// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The implementation of the [`FlightSqlService`] for LakeSoul.

use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, ActionEndTransactionRequest, Command,
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetPrimaryKeys, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate,
    DoPutPreparedStatementResult, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage, SchemaAsIpc,
    Ticket,
};
use datafusion::sql::TableReference;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::{CreateTable, SqlOption};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use futures::{Stream, StreamExt, TryStreamExt};
use lakesoul_datafusion::catalog::LakeSoulTableProperty;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_datafusion::lakesoul_table::helpers::case_fold_column_name;
use lakesoul_datafusion::serialize::arrow_java::schema_from_metadata_str;
use lakesoul_io::helpers::get_batch_memory_size;
use lakesoul_io::serde_json;
use prost::Message;
use std::env;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming, metadata::MetadataValue};

use lakesoul_io::async_writer::WriterFlushResult;
use lakesoul_metadata::MetaDataClientRef;
use uuid::Uuid;

use lakesoul_datafusion::{LakeSoulError, Result, create_lakesoul_session_ctx};

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use datafusion::logical_expr::{DdlStatement, DmlStatement, LogicalPlan, WriteOp};
use datafusion::prelude::*;

use tonic::metadata::MetadataMap;

use crate::args::Args;
use crate::jwt::JwtServer;
use crate::{Claims, datafusion_error_to_status, lakesoul_error_to_status, lakesoul_metadata_error_to_status};
use lakesoul_metadata::rbac::verify_permission_by_table_name;
use metrics::{counter, gauge, histogram};
use prost::bytes::Bytes;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

const LOG_INTERVAL: usize = 100; // Log every 100 batches
const MEGABYTE: f64 = 1_048_576.0;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

/// The transactional data of the commit operation on LakeSoul metadata.
type TransactionalData = (Arc<LakeSoulTable>, WriterFlushResult);

/// The metrics of the stream write operation.
struct StreamWriteMetrics {
    active_streams: AtomicI64,
    total_bytes: AtomicU64,
    total_rows: AtomicU64,
    start_time: Mutex<Option<Instant>>,
    throughput_limit: f64,
    last_check: Mutex<Instant>,
    bytes_since_last_check: AtomicU64,
}

impl StreamWriteMetrics {
    fn new(throughput_limit: f64) -> Self {
        Self {
            active_streams: AtomicI64::new(0),
            total_bytes: AtomicU64::new(0),
            total_rows: AtomicU64::new(0),
            start_time: Mutex::new(None),
            throughput_limit,
            last_check: Mutex::new(Instant::now()),
            bytes_since_last_check: AtomicU64::new(0),
        }
    }

    fn control_throughput(&self) {
        let mut last_check = self.last_check.lock().unwrap();
        let elapsed = last_check.elapsed().as_secs_f64();

        if elapsed >= 1.0 {
            let bytes = self.bytes_since_last_check.swap(0, Ordering::SeqCst);
            let current_mb_per_second = (bytes as f64 / MEGABYTE) / elapsed;

            if current_mb_per_second > self.throughput_limit {
                let sleep_duration =
                    std::time::Duration::from_secs_f64((current_mb_per_second / self.throughput_limit - 1.0) * elapsed);
                info!(
                    "Sleeping for {} seconds to control throughput",
                    sleep_duration.as_secs_f64()
                );
                std::thread::sleep(sleep_duration);
                info!("Waking up after sleeping");
            }

            *last_check = Instant::now();
        }
    }

    fn start_stream(&self) {
        counter!("stream_write_total").increment(1);
        self.active_streams.fetch_add(1, Ordering::SeqCst);
        gauge!("stream_write_active_stream").set(self.active_streams.load(Ordering::SeqCst) as f64);
        if self.active_streams.load(Ordering::SeqCst) == 1 {
            let mut start_time = self.start_time.lock().unwrap();
            if start_time.is_none() {
                *start_time = Some(Instant::now());
            }
        }
    }

    fn end_stream(&self) {
        self.add_batch_metrics(0, 0);
        self.report_metrics("end_stream_write");
        self.active_streams.fetch_sub(1, Ordering::SeqCst);
        gauge!("stream_write_active_stream").set(self.active_streams.load(Ordering::SeqCst) as f64);

        if self.active_streams.load(Ordering::SeqCst) == 0 {
            let mut start_time = self.start_time.lock().unwrap();
            *start_time = None;
            self.total_bytes.store(0, Ordering::SeqCst);
            self.total_rows.store(0, Ordering::SeqCst);
            self.bytes_since_last_check.store(0, Ordering::SeqCst);
        }
        self.control_throughput();
    }

    fn add_batch_metrics(&self, rows: u64, bytes: u64) {
        self.total_rows.fetch_add(rows, Ordering::SeqCst);
        self.total_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.bytes_since_last_check.fetch_add(bytes, Ordering::SeqCst);
        histogram!("stream_write_rows_of_batch").record(rows as f64);
        histogram!("stream_write_bytes_of_batch").record(bytes as f64 / MEGABYTE);
        counter!("stream_write_rows_total").increment(rows);
        counter!("stream_write_bytes_total").increment(bytes);
    }

    fn report_metrics(&self, prefix: &str) {
        let active_streams = self.active_streams.load(Ordering::SeqCst);
        let total_bytes = self.total_bytes.load(Ordering::SeqCst);
        let total_rows = self.total_rows.load(Ordering::SeqCst);
        let start_time = *self.start_time.lock().unwrap();
        if let Some(start_time) = start_time {
            let duration = start_time.elapsed().as_secs_f64();
            let rows_per_second = total_rows as f64 / duration;
            let mb_per_second = (total_bytes as f64 / MEGABYTE) / duration;
            info!(
                "{} active_streams: {}, total_bytes: {:.2}MB, total_rows: {:.2}K, rows_per_second: {:.2}K, mb_per_second: {:.2}MB",
                prefix,
                active_streams,
                total_bytes as f64 / MEGABYTE,
                total_rows as f64 / 1000.0,
                rows_per_second,
                mb_per_second
            );

            histogram!("stream_write_rows_per_second").record(rows_per_second);
            histogram!("stream_write_mb_per_second").record(mb_per_second);
        }
    }
}

/// The implementation of the [`FlightSqlService`] for LakeSoul.
pub struct FlightSqlServiceImpl {
    /// The arguments of the flight sql service.
    args: Args,
    /// The metadata client.
    client: MetaDataClientRef,
    /// The context state.
    ctx: Arc<SessionContext>,
    /// The statement state.
    statements: Arc<DashMap<String, LogicalPlan>>,
    /// The transactional data state.
    transactional_data: Arc<DashMap<String, TransactionalData>>,
    /// The metrics of the stream write operation.
    metrics: Arc<StreamWriteMetrics>,
    /// The jwt server for authentication.
    jwt_server: Arc<JwtServer>,
    /// The auth switch.
    auth_enabled: bool,
    /// The rbac authorization switch.
    rbac_enabled: bool,
    // this may OOM
    counter: DashMap<TicketWrapper, i64>,
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = Self;

    /// in lakesoul this function should not be invoked
    #[instrument(skip(self))]
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status> {
        info!("do_handshake - starting handshake");
        // no authentication actually takes place here
        // see Ballista implementation, for example of basic auth
        // in this case, we simply accept the connection and create a new SessionContext
        // the SessionContext will be re-used within this same connection/session
        let token = "dummy";
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> = Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str).map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    #[instrument(skip(self))]
    async fn get_flight_info_statement(
        &self,
        cmd: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let claims = self.verify_token(request.metadata())?;
        info!("get_flight_info_statement - query: {}", cmd.query);
        let sql = &cmd.query;

        // only need logical plan

        let dialect = self.ctx.state().config().options().sql_parser.dialect.clone();
        let stat = self
            .ctx
            .state()
            .sql_to_statement(sql, &dialect)
            .map_err(|e| status!("Error parsing SQL", e))?;

        let table_refs = self
            .ctx
            .state()
            .resolve_table_references(&stat)
            .map_err(|e| status!("Error validating statement", e))?;

        for tf in table_refs {
            // TODO may use const var
            let mut namespace: Arc<str> = Arc::from("default");
            #[allow(unused_assignments)]
            let mut table_name: Arc<str> = Arc::from("");

            match tf {
                TableReference::Bare { table } => {
                    table_name = table.clone();
                }
                TableReference::Partial { schema, table } => {
                    namespace = schema.clone();
                    table_name = table.clone();
                }
                TableReference::Full { catalog, schema, table } => {
                    let catalog = &*catalog.to_lowercase();
                    assert_eq!("lakesoul", catalog);
                    namespace = schema.clone();
                    table_name = table.clone();
                }
            }

            self.verify_rbac(&claims, &*namespace, &*table_name)
                .await
                .map_err(|e| status!("Error validating rbac", e))?;
        }

        // cache plan?
        let plan = self
            .ctx
            .state()
            .statement_to_plan(stat)
            .await
            .map_err(|e| status!("Error creating logical plan", e))?;

        let opt = SQLOptions::new().with_allow_ddl(false).with_allow_statements(false);
        opt.verify_plan(&plan)
            .map_err(|e| status!("Error validating plan", e))?;

        let schema = plan.schema().as_arrow();

        let ticket = Ticket {
            ticket: Command::TicketStatementQuery(TicketStatementQuery {
                statement_handle: sql.as_bytes().to_vec().into(),
            })
            .into_any()
            .encode_to_vec()
            .into(),
        };

        // add ticket
        {
            // FIXME naive impl
            let mut entry = self.counter.entry(TicketWrapper(ticket.ticket.clone())).or_insert(0);
            *entry = entry
                .checked_add(1)
                .ok_or_else(|| Status::cancelled("ticket counter overflow"))?;
        }

        let info = FlightInfo::new()
            // Encode the Arrow schema
            .try_with_schema(&schema)
            .expect("encoding failed")
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.verify_token(request.metadata())?;
        info!(
            "get_flight_info_prepared_statement - handle: {:?}",
            std::str::from_utf8(&cmd.prepared_statement_handle).unwrap_or("invalid utf8")
        );

        let handle =
            std::str::from_utf8(&cmd.prepared_statement_handle).map_err(|e| status!("Unable to parse uuid", e))?;

        let plan = self.get_plan(handle)?;

        let state = self.ctx.state();
        let df = DataFrame::new(state, plan);
        let result = df.collect().await.map_err(|e| status!("Error executing query", e))?;

        // if we get an empty result, create an empty schema
        let schema = match result.first() {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        let ticket = Ticket {
            ticket: Command::CommandPreparedStatementQuery(cmd)
                .into_any()
                .encode_to_vec()
                .into(),
        };

        let info = FlightInfo::new()
            // Encode the Arrow schema
            .try_with_schema(&schema)
            .expect("encoding failed")
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });
        let resp = Response::new(info);
        Ok(resp)
    }

    /// Get a FlightInfo for listing catalogs.
    async fn get_flight_info_catalogs(
        &self,
        cmd: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_catalogs");
        self.verify_token(request.metadata())?;

        let schema = Arc::new(Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]));

        let ticket = Ticket {
            ticket: Command::CommandGetCatalogs(cmd).into_any().encode_to_vec().into(),
        };

        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .expect("encoding failed")
            .with_endpoint(endpoint);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.verify_token(request.metadata())?;
        info!(
            "get_flight_info_schemas - catalog: {:?}, schema: {:?}",
            query.catalog,
            query.db_schema_filter_pattern()
        );

        // 执行查询获取所有schema
        // 修改 schema 使 catalog_name 可空
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true), // 设置 nullable=true
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));

        let ticket = Ticket {
            ticket: Command::CommandGetDbSchemas(query).into_any().encode_to_vec().into(),
        };

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .expect("encoding failed")
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });

        Ok(Response::new(info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.verify_token(request.metadata())?;
        info!("get_flight_info_tables - catalog: {:?}", query);

        let data = self
            .tables(self.ctx.clone())
            .await
            .map_err(|e| status!("Error executing query", e))?;
        let schema = data.schema();

        let ticket = Ticket {
            ticket: Command::CommandGetTables(query).into_any().encode_to_vec().into(),
        };

        let info = FlightInfo::new()
            // Encode the Arrow schema
            .try_with_schema(&schema)
            .expect("encoding failed")
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            });
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.verify_token(request.metadata())?;
        info!(
            "get_flight_info_primary_keys - catalog: {:?}, schema: {:?}, table: {:?}",
            query.catalog, query.db_schema, query.table
        );
        let table_info = self
            .get_metadata_client()
            .get_table_info_by_table_name(&query.table, query.db_schema())
            .await
            .map_err(lakesoul_metadata_error_to_status)?;
        if let Some(table_info) = table_info {
            info!("get_flight_info_primary_keys table_info: {:?}", table_info);
            let schema = schema_from_metadata_str(&table_info.table_schema);
            info!("get_flight_info_primary_keys schema: {:?}", schema);
            let ticket = Ticket {
                ticket: Command::CommandGetPrimaryKeys(query).into_any().encode_to_vec().into(),
            };
            let metadata = create_app_metadata(table_info.properties.clone(), table_info.partitions.clone());

            let info = FlightInfo::new()
                .try_with_schema(&schema)
                .expect("encoding failed")
                .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
                .with_descriptor(FlightDescriptor {
                    r#type: DescriptorType::Cmd.into(),
                    cmd: Default::default(),
                    path: vec![],
                })
                .with_app_metadata(metadata);
            Ok(Response::new(info))
        } else {
            Err(Status::internal(format!("Table '{}' not found", query.table)))
        }
    }

    #[instrument(skip(self))]
    async fn do_get_statement(
        &self,
        query: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.verify_token(request.metadata())?;
        // counter
        {
            let wrapper = TicketWrapper(request.get_ref().ticket.clone());
            let mut val = self
                .counter
                .get_mut(&wrapper)
                .ok_or_else(|| Status::cancelled("Ticket not found"))?;

            debug!("ticket val is {:?}", *val);

            if *val <= 0 {
                self.counter.remove(&wrapper);
                return Err(Status::cancelled("Ticket not found"));
            }
            *val = val
                .checked_sub(1)
                .ok_or_else(|| Status::cancelled("Ticket not found"))?;
        }
        info!("do_get_statement - query: {:?}", query.statement_handle);
        let sql = std::str::from_utf8(&query.statement_handle).map_err(|e| status!("Unable to parse uuid", e))?;
        // forbid DDL
        // FIXME from cache
        let opt = SQLOptions::new().with_allow_ddl(false).with_allow_statements(false);
        let df = self
            .ctx
            .sql_with_options(sql, opt)
            .await
            .map_err(|e| status!("Error executing query", e))?;
        let schema = Arc::new(df.schema().clone().into());
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| status!("Error executing query", e))?
            .map(|batch| {
                let batch = batch.map_err(|e| status!("Error executing query", e))?;
                Ok(batch)
            });
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.verify_token(request.metadata())?;
        info!(
            "do_get_prepared_statement - handle: {:?}",
            std::str::from_utf8(&query.prepared_statement_handle).unwrap_or("invalid utf8")
        );

        let handle =
            std::str::from_utf8(&query.prepared_statement_handle).map_err(|e| status!("Unable to parse uuid", e))?;
        let plan = self.get_plan(handle)?;
        let state = self.ctx.state();
        let df = DataFrame::new(state, plan);
        let schema = Arc::new(df.schema().clone().into());
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| status!("Error executing query", e))?
            .map(|batch| {
                let batch = batch.map_err(|e| status!("Error executing query", e))?;
                Ok(batch)
            });
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.verify_token(request.metadata())?;
        let CommandGetDbSchemas {
            catalog,
            db_schema_filter_pattern,
        } = query;
        info!(
            "do_get_schemas - catalog: {:?}, schema: {:?}",
            catalog, db_schema_filter_pattern
        );

        let namespaces = self
            .get_metadata_client()
            .get_all_namespace()
            .await
            .map_err(lakesoul_metadata_error_to_status)?;
        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![String::from("lakesoul"); namespaces.len()])),
            Arc::new(StringArray::from(
                namespaces.iter().map(|n| n.namespace.clone()).collect::<Vec<String>>(),
            )),
        ];

        // 修改 schema 使 catalog_name 可空
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true), // 设置 nullable=true
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));

        // 使用新的 schema 创建新的 RecordBatch
        let data = RecordBatch::try_new(schema.clone(), data)
            .map_err(|e| Status::internal(format!("Error creating record batch: {e}")))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(vec![Ok(data)]))
            .map_err(|e| Status::internal(format!("Error creating flight data encoder: {e}")));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_tables - query: {:?}, ticket: {:?}", query, request);
        self.verify_token(request.metadata())?;
        let table_names = self
            .get_metadata_client()
            .get_all_table_name_id_by_namespace(query.db_schema_filter_pattern())
            .await
            .map_err(lakesoul_metadata_error_to_status)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("db_schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));
        let data: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![String::from("lakesoul"); table_names.len()])),
            Arc::new(StringArray::from(
                table_names
                    .iter()
                    .map(|n| n.table_namespace.clone())
                    .collect::<Vec<String>>(),
            )),
            Arc::new(StringArray::from(
                table_names
                    .iter()
                    .map(|n| n.table_name.clone())
                    .collect::<Vec<String>>(),
            )),
            Arc::new(StringArray::from(vec![String::from("BaseTable"); table_names.len()])),
        ];
        let data = RecordBatch::try_new(schema.clone(), data)
            .map_err(|e| Status::internal(format!("Error creating record batch: {e}")))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(vec![Ok(data)]))
            .map_err(|e| Status::internal(format!("Error creating flight data encoder: {e}")));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.verify_token(request.metadata())?;
        info!(
            "do_get_primary_keys - catalog: {:?}, schema: {:?}, table: {:?}",
            query.catalog, query.db_schema, query.table
        );
        Ok(Response::new(Box::pin(futures::stream::iter(vec![]))))
    }

    async fn do_put_statement_update(
        &self,
        query: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.verify_token(request.metadata())?;
        info!("do_put_statement_update - query: {}", query.query);
        let sql = &query.query;
        let sql = normalize_sql(sql)?;
        info!("execute SQL: {}", sql);

        let _ = self
            .ctx
            .sql(&sql)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| status!("Error executing query", e))?;
        Ok(0)
    }

    #[instrument(skip(self, request))]
    async fn do_put_statement_ingest(
        &self,
        cmd: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let claims = self.verify_token(request.metadata())?;
        info!("do_put_statement_ingest");
        let CommandStatementIngest {
            table_definition_options,
            table,
            schema,
            catalog,
            temporary,
            transaction_id,
            options,
        } = cmd;
        self.verify_rbac(&claims, &schema.clone().unwrap_or("default".to_string()), &table)
            .await?;
        info!(
            "do_put_statement_ingest: table: {table}, schema: {schema:?}, catalog: {catalog:?}, temporary: {temporary}, transaction_id: {transaction_id:?}, options: {options:?} table_definition_options: {table_definition_options:?}"
        );

        // 获取输入流
        let stream = request.into_inner();

        // 创建 LakeSoulTable
        let table = Arc::new(
            LakeSoulTable::for_namespace_and_name(
                schema.unwrap_or("default".to_string()).as_str(),
                table.as_str(),
                Some(self.client.clone()),
            )
            .await
            .map_err(|e| Status::internal(format!("{}", e)))?,
        );
        let (flush_result, record_count) = self.write_stream(table.clone(), stream).await?;

        if let Some(transaction_id) = transaction_id {
            // Convert Bytes to String before passing to append_transactional_data
            let transaction_id = String::from_utf8(transaction_id.to_vec())
                .map_err(|e| Status::internal(format!("Invalid transaction ID: {}", e)))?;
            self.append_transactional_data(transaction_id, table, flush_result)?;
        } else {
            table
                .commit_flush_result(flush_result)
                .await
                .map_err(lakesoul_error_to_status)?;
        }

        Ok(record_count)
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        self.verify_token(request.metadata())?;
        info!(
            "do_put_prepared_statement_query - handle: {:?}",
            std::str::from_utf8(&query.prepared_statement_handle).unwrap_or("invalid utf8")
        );

        todo!()
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.verify_token(request.metadata())?;
        info!(
            "do_put_prepared_statement_update - handle: {:?}",
            std::str::from_utf8(&query.prepared_statement_handle).unwrap_or("invalid utf8")
        );
        info!("do_put_prepared_statement_update");
        let handle =
            std::str::from_utf8(&query.prepared_statement_handle).map_err(|e| status!("Unable to parse uuid", e))?;

        let plan = self.get_plan(handle)?;
        let table = match &plan {
            LogicalPlan::Dml(DmlStatement {
                op: WriteOp::Insert(_),
                table_name,
                ..
            }) => Arc::new(
                LakeSoulTable::for_table_reference(table_name, Some(self.get_metadata_client()))
                    .await
                    .unwrap(),
            ),
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => {
                info!("create external table: {}, {:?}", cmd.name, cmd.definition);
                Arc::new(
                    LakeSoulTable::for_table_reference(&cmd.name, Some(self.get_metadata_client()))
                        .await
                        .unwrap(),
                )
            }
            LogicalPlan::EmptyRelation(_) => Arc::new(
                LakeSoulTable::for_table_reference(
                    &TableReference::from("test_table"),
                    Some(self.get_metadata_client()),
                )
                .await
                .unwrap(),
            ),
            _ => Err(Status::internal(
                "Not a valid insert into or create external table plan",
            ))?,
        };
        let stream = request.into_inner();

        let mut stream = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| e.into()));
        let mut row_count: i64 = 0;

        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|e| status!("Error getting next batch", e))?
        {
            let now = std::time::SystemTime::now();
            let datetime: chrono::DateTime<chrono::Local> = now.into();
            info!(
                "do put prepared statement update current time: {}",
                datetime.format("%Y-%m-%d %H:%M:%S.%3f")
            );
            row_count += batch.num_rows() as i64;
            info!("batch schema: {:?}, {:?}", batch.schema(), table.schema());
            // 将列名转换为大写
            let schema = batch.schema();
            let fields = schema
                .fields()
                .iter()
                .map(|f| Field::new(case_fold_column_name(f.name()), f.data_type().clone(), f.is_nullable()))
                .collect::<Vec<_>>();
            let new_schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(new_schema, batch.columns().to_vec())
                .map_err(|e| status!("Error creating record batch with case folded column names", e))?;
            table
                .execute_upsert(batch)
                .await
                .map_err(|e| status!("Error executing upsert", e))?;
        }

        Ok(row_count)
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.verify_token(request.metadata())?;
        info!("do_action_create_prepared_statement - query: {}", query.query);
        let user_query = query.query.as_str();
        // 使用独立的函数转换查询中的占位符
        let normalized_query = normalize_sql(user_query)?;
        info!("do_action_create_prepared_statement: {normalized_query}");

        let plan = self.ctx.sql(&normalized_query).await;
        let plan = plan
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        // store a copy of the plan,  it will be used for execution
        let plan_uuid = Uuid::new_v4().hyphenated().to_string();
        // let plan_uuid = "debug_uuid".to_string();

        let plan_schema = plan.schema();

        self.statements.insert(plan_uuid.clone(), plan.clone());

        let arrow_schema = (&**plan_schema).into();
        let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let parameter_schema = Schema::empty();
        let parameter_message = SchemaAsIpc::new(&parameter_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(parameter_schema_bytes) = parameter_message;

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: plan_uuid.into(),
            dataset_schema: schema_bytes,
            parameter_schema: parameter_schema_bytes,
        };
        info!("do_action_create_prepared_statement - res: {:?}", res);
        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        handle: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        self.verify_token(request.metadata())?;
        info!(
            "do_action_close_prepared_statement - handle: {:?}",
            std::str::from_utf8(&handle.prepared_statement_handle).unwrap_or("invalid utf8")
        );
        let handle = std::str::from_utf8(&handle.prepared_statement_handle);
        if let Ok(handle) = handle {
            info!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
        }
        Ok(())
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        self.verify_token(request.metadata())?;
        let transaction_id = Uuid::new_v4().hyphenated().to_string();
        info!("do_action_begin_transaction - transaction_id: {:?}", transaction_id);
        Ok(ActionBeginTransactionResult {
            transaction_id: transaction_id.into(),
        })
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        self.verify_token(request.metadata())?;
        let ActionEndTransactionRequest { transaction_id, action } = query;
        info!(
            "do_action_end_transaction - transaction_id: {:?}, action: {:?}",
            transaction_id, action
        );
        let transaction_id = String::from_utf8(transaction_id.to_vec())
            .map_err(|e| Status::internal(format!("Invalid transaction ID: {}", e)))?;
        match action {
            1 => self.commit_transactional_data(transaction_id).await,
            2 => self.clear_transactional_data(transaction_id),
            _ => Err(Status::internal("Invalid action")),
        }
    }

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        info!("register_sql_info - id: {}, result: {:?}", id, result);
    }
}

// 独立的函数，用于转换占位符
fn convert_placeholders(query: &str) -> String {
    let mut result = String::new();
    let mut param_count = 1;
    let mut chars = query.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '?' {
            result.push('$');
            result.push_str(&param_count.to_string());
            param_count += 1;
        } else {
            result.push(c);
        }
    }
    result
}

fn normalize_sql(sql: &str) -> Result<String, Status> {
    let sql = convert_placeholders(sql);
    let statements =
        DFParser::parse_sql_with_dialect(&sql, &PostgreSqlDialect {}).map_err(|e| status!("Error parsing SQL", e))?;

    if let Some(Statement::Statement(stmt)) = statements.front() {
        // info!("stmt: {:?}", stmt);
        match stmt.as_ref() {
            datafusion::sql::sqlparser::ast::Statement::CreateTable(CreateTable {
                name,
                columns,
                partition_by,
                with_options,
                ..
            }) => {
                info!("create table: {}, {:?}, {:?}", name, partition_by, with_options);

                let mut create_table_sql = format!(
                    "CREATE EXTERNAL TABLE {} ({}) STORED AS LAKESOUL LOCATION ''",
                    name,
                    columns
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                if let Some(partition_by) = partition_by {
                    create_table_sql = format!("{} PARTITIONED BY {}", create_table_sql, partition_by);
                }
                if !with_options.is_empty() {
                    create_table_sql = format!(
                        "{} OPTIONS ({})",
                        create_table_sql,
                        with_options
                            .iter()
                            .map(|opt| match opt {
                                SqlOption::KeyValue { key, value } => format!("'{}' {}", key, value),
                                _ => format!("{:}", opt),
                            })
                            .collect::<Vec<String>>()
                            .join(", ")
                    );
                }
                Ok(create_table_sql)
            }
            _ => Ok(sql.to_string()),
        }
    } else {
        Ok(sql.to_string())
    }
}

pub fn create_app_metadata(properties: String, partitions: String) -> String {
    let mut properties = serde_json::from_str::<LakeSoulTableProperty>(&properties).unwrap();
    properties.partitions = Some(partitions);
    serde_json::to_string(&properties).unwrap()
}

impl FlightSqlServiceImpl {
    /// Create a new [`FlightSqlServiceImpl`].
    pub async fn new(metadata_client: MetaDataClientRef, args: Args) -> Result<Self> {
        let secret = metadata_client.get_client_secret();
        let jwt_server = Arc::new(JwtServer::new(secret.as_str()));
        let jwt_env = env::var("JWT_AUTH_ENABLED").unwrap_or("false".to_string());
        let rbac_env = env::var("RBAC_AUTH_ENABLED").unwrap_or("false".to_string());
        debug!("jwt: {}, rbac: {}", jwt_env, rbac_env);
        let auth_enabled = jwt_env
            .to_lowercase()
            .parse::<bool>()
            .map_err(|e| LakeSoulError::Internal(format!("{}", e)))?;
        let rbac_enabled = rbac_env
            .to_lowercase()
            .parse::<bool>()
            .map_err(|e| LakeSoulError::Internal(format!("{}", e)))?;

        if rbac_enabled && !auth_enabled {
            error!("rbac enabled but auth disabled");
            return Err(LakeSoulError::Internal("rbac enabled but auth disabled".into()));
        }

        // 从环境变量或配置中获取目标速率
        let throughput_limit = args.throughput_limit.parse::<f64>().unwrap_or(100.0);

        let ctx = create_lakesoul_session_ctx(metadata_client.clone(), &args.core)?;

        Ok(FlightSqlServiceImpl {
            client: metadata_client,
            args,
            ctx,
            statements: Default::default(),
            transactional_data: Default::default(),
            jwt_server,
            auth_enabled,
            rbac_enabled,
            metrics: Arc::new(StreamWriteMetrics::new(throughput_limit)),
            counter: DashMap::new(),
        })
    }

    /// Get the [`LogicalPlan`] from the statement state.
    fn get_plan(&self, handle: &str) -> Result<LogicalPlan, Status> {
        if let Some(plan) = self.statements.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!("Plan handle not found: {handle}")))?
        }
    }

    async fn tables(&self, ctx: Arc<SessionContext>) -> Result<RecordBatch, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("table_schema", DataType::Binary, false),
        ]));

        let mut catalogs = vec![];
        let mut schemas = vec![];
        let mut names = vec![];
        let mut types = vec![];
        let mut table_schemas: Vec<Vec<u8>> = vec![];

        for catalog in ctx.catalog_names() {
            let catalog_provider = ctx
                .catalog(&catalog)
                .ok_or_else(|| Status::internal(format!("Catalog not found: {}", catalog)))?;

            for schema in catalog_provider.schema_names() {
                let schema_provider = catalog_provider
                    .schema(&schema)
                    .ok_or_else(|| Status::internal(format!("Schema not found: {}", schema)))?;

                for table in schema_provider.table_names() {
                    let table_provider = schema_provider
                        .table(&table)
                        .await
                        .map_err(|e| Status::internal(format!("Error getting table: {}", e)))?
                        .ok_or_else(|| Status::internal(format!("Table not found: {}", table)))?;

                    let table_schema = table_provider.schema();

                    let message = SchemaAsIpc::new(&table_schema, &IpcWriteOptions::default())
                        .try_into()
                        .map_err(|e| Status::internal(format!("Error converting schema to IPC: {}", e)))?;
                    let IpcMessage(schema_bytes) = message;

                    catalogs.push(catalog.clone());
                    schemas.push(schema.clone());
                    names.push(table.clone());
                    types.push(format!("{:?}", table_provider.table_type()));
                    table_schemas.push(schema_bytes.to_vec());
                }
            }
        }

        let binary_builder = arrow::array::BinaryBuilder::new();
        let mut builder = binary_builder;
        for schema_bytes in table_schemas {
            builder.append_value(&schema_bytes);
        }
        let binary_array = builder.finish();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(binary_array),
        ];

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| Status::internal(format!("Error creating record batch: {}", e)))
    }

    /// Remove the [`LogicalPlan`] from the statement state.
    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.statements.remove(&handle.to_string());
        Ok(())
    }

    /// Get the [`MetaDataClient`] from the metadata client.
    fn get_metadata_client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    /// Append the [`TransactionalData`] to the transactional data state.
    fn append_transactional_data(
        &self,
        transaction_id: String,
        table: Arc<LakeSoulTable>,
        result: WriterFlushResult,
    ) -> Result<(), Status> {
        info!("Appending transactional data for transaction: {}", transaction_id);
        self.transactional_data.insert(transaction_id, (table, result));
        Ok(())
    }

    /// Clear the [`TransactionalData`] from the transactional data state.
    fn clear_transactional_data(&self, transaction_id: String) -> Result<(), Status> {
        self.transactional_data.remove(&transaction_id);
        Ok(())
    }

    /// Commit the [`TransactionalData`] from the transactional data state.
    async fn commit_transactional_data(&self, transaction_id: String) -> Result<(), Status> {
        // Get returns a Ref, so we need to dereference it to get the tuple
        if let Some(ref_data) = self.transactional_data.get(&transaction_id) {
            let (table, result) = &*ref_data; // Dereference the Ref to get the tuple
            table
                .commit_flush_result(result.clone())
                .await
                .map_err(lakesoul_error_to_status)?;
        }
        self.clear_transactional_data(transaction_id)?;
        Ok(())
    }

    /// Get the [`JwtServer`] from the jwt server.
    pub fn get_jwt_server(&self) -> Arc<JwtServer> {
        self.jwt_server.clone()
    }

    /// Verify the token from the metadata.
    fn verify_token(&self, metadata: &MetadataMap) -> Result<Claims, Status> {
        if !self.auth_enabled {
            return Ok(Claims::default());
        }
        let authorization = metadata
            .get("Authorization")
            .ok_or(Status::permission_denied("Missing authorization header"))?;
        info!("Verifying token header {:?}", authorization);
        let token = authorization
            .to_str()
            .map_err(|e| Status::permission_denied(format!("Invalid authorization header: {e}")))?;
        if token.len() < 8 || !token.starts_with("Bearer ") {
            return Err(Status::permission_denied(format!(
                "Invalid authorization token: {token}"
            )));
        }
        let token = token.trim_start_matches("Bearer ");
        if token.is_empty() {
            return Err(Status::permission_denied(format!(
                "Invalid authorization token: {token}"
            )));
        }
        debug!("token: {}", token);
        self.jwt_server
            .decode_token(token)
            .map_err(|e| Status::permission_denied(format!("Invalid authorization token: {e}")))
    }

    /// Verify the rbac authorization.
    #[instrument(skip(self))]
    async fn verify_rbac(&self, claims: &Claims, ns: &str, table: &str) -> Result<(), Status> {
        info!("verify_rbac, rabc_enabled:{} ", self.rbac_enabled);
        if !self.rbac_enabled {
            return Ok(());
        }
        verify_permission_by_table_name(&claims.sub, &claims.group, ns, table, self.client.clone())
            .await
            .map_err(|e| Status::permission_denied(e.to_string()))
    }

    /// Write the stream to the table.
    async fn write_stream(
        &self,
        table: Arc<LakeSoulTable>,
        stream: PeekableFlightDataStream,
    ) -> Result<(WriterFlushResult, i64), Status> {
        // 开始记录流式指标
        self.metrics.start_stream();

        let mut writer = table
            .get_writer(self.args.core.s3_options())
            .await
            .map_err(lakesoul_error_to_status)?;

        // 创建 FlightRecordBatchStream 来解码数据
        let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| e.into()));

        // 记录处理的记录数
        let mut record_count = 0i64;
        let mut batch_count = 0;

        // 处理每个批次
        while let Some(batch) = batch_stream
            .try_next()
            .await
            .map_err(|e| Status::internal(format!("Error getting next batch: {}", e)))?
        {
            let batch_rows = batch.num_rows();
            debug!("write_stream batch_rows: {}", batch_rows);
            record_count += batch_rows as i64;
            let batch_bytes = get_batch_memory_size(&batch).map_err(datafusion_error_to_status)? as u64;
            batch_count += 1;

            writer
                .write_record_batch(batch)
                .await
                .map_err(datafusion_error_to_status)?;

            self.metrics.add_batch_metrics(batch_rows as u64, batch_bytes);
            // Log metrics at intervals
            if batch_count % LOG_INTERVAL == 0 {
                self.metrics.report_metrics(
                    format!(
                        "stream_write_batch_metrics at {} batches of {}",
                        batch_count,
                        table.table_name()
                    )
                    .as_str(),
                );
                self.metrics.control_throughput();
            }
        }
        let result = writer.flush_and_close().await.map_err(datafusion_error_to_status)?;

        // 结束流式处理
        self.metrics.end_stream();

        Ok((result, record_count))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TicketWrapper(Bytes);
