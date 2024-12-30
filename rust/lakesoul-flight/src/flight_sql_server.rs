use std::env;
use std::sync::Arc;
use std::pin::Pin;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, ActionEndTransactionRequest, Command, CommandGetDbSchemas, CommandGetPrimaryKeys, CommandGetTables, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate,  SqlInfo, TicketStatementQuery, DoPutPreparedStatementResult, CommandGetCatalogs};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket
};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::CreateTable;
use datafusion::sql::TableReference;
use futures::{Stream, StreamExt, TryStreamExt};
use lakesoul_datafusion::datasource::table_factory::LakeSoulTableProviderFactory;
use lakesoul_datafusion::lakesoul_table::helpers::case_fold_column_name;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_datafusion::planner::query_planner::LakeSoulQueryPlanner;
use lakesoul_datafusion::serialize::arrow_java::schema_from_metadata_str;
use lakesoul_io::helpers::get_batch_memory_size;
use lakesoul_io::lakesoul_io_config::{register_s3_object_store, LakeSoulIOConfigBuilder};
use tonic::{Request, Response, Status, metadata::MetadataValue, Streaming};
use prost::Message;


use lakesoul_metadata::MetaDataClientRef;
use lakesoul_io::async_writer::WriterFlushResult;
use uuid::Uuid;

use lakesoul_datafusion::Result;

use arrow::array::{ArrayRef, BinaryArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray, StringArray};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{ColumnarValue, DdlStatement, DmlStatement, LogicalPlan, Volatility, WriteOp};
use datafusion::prelude::*;
use log::{error, info};

use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use lakesoul_datafusion::catalog::lakesoul_catalog::LakeSoulCatalog;
use object_store::local::LocalFileSystem;
use tonic::metadata::MetadataMap;
use url::Url;

use crate::args::Args;
use crate::jwt::{Claims, JwtServer};
use crate::{arrow_error_to_status, datafusion_error_to_status, lakesoul_error_to_status, lakesoul_metadata_error_to_status};
use metrics::{counter, gauge, histogram};
use std::time::Instant;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

const LOG_INTERVAL: usize = 100; // Log every 100 batches
const MEGABYTE: f64 = 1_048_576.0;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

type TransactionalData = (Arc<LakeSoulTable>, WriterFlushResult);

pub struct FlightSqlServiceImpl {
    args: Args,
    client: MetaDataClientRef,
    contexts: Arc<DashMap<String, Arc<SessionContext>>>,
    statements: Arc<DashMap<String, LogicalPlan>>,
    transactional_data: Arc<DashMap<String, TransactionalData>>,
    metrics: Arc<StreamWriteMetrics>,
    jwt_server: Arc<JwtServer>,
    auth_enabled: bool,
}

struct StreamWriteMetrics {
    active_streams: AtomicI64,
    total_bytes: AtomicU64,
    total_rows: AtomicU64,
    start_time: Mutex<Option<Instant>>,
    target_mb_per_second_for_write: f64,
    target_mb_per_second_for_flush: f64,
    last_check: Mutex<Instant>,
    bytes_since_last_check: AtomicU64,
}

impl StreamWriteMetrics {
    fn new(target_mb_per_second_for_write: f64, target_mb_per_second_for_flush: f64) -> Self {
        Self {
            active_streams: AtomicI64::new(0),
            total_bytes: AtomicU64::new(0),
            total_rows: AtomicU64::new(0),
            start_time: Mutex::new(None),
            target_mb_per_second_for_write,
            target_mb_per_second_for_flush,
            last_check: Mutex::new(Instant::now()),
            bytes_since_last_check: AtomicU64::new(0),
        }
    }

    fn control_throughput(&self, target_mb_per_second: f64) {
        // let mut last_check = self.last_check.lock().unwrap();
        let elapsed = {
            let last_check = self.last_check.lock().unwrap();
            last_check.elapsed().as_secs_f64()
        };
        if elapsed >= 1.0 {
            let bytes = self.bytes_since_last_check.swap(0, Ordering::SeqCst);
            let current_mb_per_second = (bytes as f64 / MEGABYTE) / elapsed;
            if current_mb_per_second > target_mb_per_second {
                let sleep_duration = std::time::Duration::from_secs_f64(
                    (current_mb_per_second / target_mb_per_second - 1.0) * elapsed
                );
                info!("Sleeping for {} seconds to control throughput", sleep_duration.as_secs_f64());
                std::thread::sleep(sleep_duration);
                info!("Waking up after sleeping");
            }
            let mut last_check = self.last_check.lock().unwrap();
            *last_check = Instant::now();
        }
    }

    fn start_stream(&self) {
        counter!("stream_write_total", 1);
        self.active_streams.fetch_add(1, Ordering::SeqCst);
        gauge!("stream_write_active_stream", self.active_streams.load(Ordering::SeqCst) as f64);
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
        gauge!("stream_write_active_stream", self.active_streams.load(Ordering::SeqCst) as f64);

        if self.active_streams.load(Ordering::SeqCst) == 0 {
            let mut start_time = self.start_time.lock().unwrap();
            *start_time = None;
            self.total_bytes.store(0, Ordering::SeqCst);
            self.total_rows.store(0, Ordering::SeqCst);
            self.bytes_since_last_check.store(0, Ordering::SeqCst);
        }
        self.control_throughput(self.target_mb_per_second_for_flush);
    }

    fn add_batch_metrics(&self, rows: u64, bytes: u64) {
        self.total_rows.fetch_add(rows, Ordering::SeqCst);
        self.total_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.bytes_since_last_check.fetch_add(bytes, Ordering::SeqCst);
        histogram!("stream_write_rows_of_batch", rows as f64);
        histogram!("stream_write_bytes_of_batch", bytes as f64 / MEGABYTE);
        counter!("stream_write_rows_total", rows);
        counter!("stream_write_bytes_total", bytes);
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
            info!("{} active_streams: {}, total_bytes: {:.2}MB, total_rows: {:.2}K, rows_per_second: {:.2}K, mb_per_second: {:.2}MB", prefix, active_streams, total_bytes as f64 / MEGABYTE, total_rows as f64 / 1000.0, rows_per_second, mb_per_second);

            histogram!("stream_write_rows_per_second", rows_per_second);
            histogram!("stream_write_mb_per_second", mb_per_second);
            
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = Self;

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) { 
        info!("register_sql_info - id: {}, result: {:?}", id, result);
    }

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status> {
        info!("do_handshake - starting handshake");
        // no authentication actually takes place here
        // see Ballista implementation for example of basic auth
        // in this case, we simply accept the connection and create a new SessionContext
        // the SessionContext will be re-used within this same connection/session
        let token = self.create_ctx().await?;

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

    async fn get_flight_info_statement(
        &self,
        cmd: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.verify_token(request.metadata())?;
        info!("get_flight_info_statement - query: {}", cmd.query);
        let sql = &cmd.query;

        let ctx = self.get_ctx(&request)?;
        let plan = ctx.sql(sql).await.map_err(|e| status!("Error executing query", e))?;
        let schema = plan.schema().clone();
        let schema = schema.into();

        let ticket = Ticket {
            ticket: Command::TicketStatementQuery(TicketStatementQuery {
                statement_handle: sql.as_bytes().to_vec().into(),
            })
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

        let ctx = self.get_ctx(&request)?;
        let plan = self.get_plan(handle)?;

        let state = ctx.state();
        // dbg!(&plan);
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
        let ctx = self.get_ctx(&request)?;

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

        let ctx = self.get_ctx(&request)?;
        let data = self.tables(ctx).await;
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
        let schema = schema_from_metadata_str(&table_info.table_schema);
        let ticket = Ticket {
            ticket: Command::CommandGetPrimaryKeys(query).into_any().encode_to_vec().into(),
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

    async fn do_get_statement(
        &self,
        query: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.verify_token(request.metadata())?;
        info!("do_get_statement - query: {:?}", query.statement_handle);
        let sql = std::str::from_utf8(&query.statement_handle).map_err(|e| status!("Unable to parse uuid", e))?;
        let ctx = self.get_ctx(&request)?;
        let df = ctx.sql(sql).await.map_err(|e| status!("Error executing query", e))?;
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
        let ctx = self.get_ctx(&request)?;
        let plan = self.get_plan(handle)?;
        let state = ctx.state();
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
        self.verify_token(request.metadata())?;
        let table_names = self
            .get_metadata_client()
            .get_all_table_name_id_by_namespace(query.catalog())
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
        dbg!(&query);
        let sql = &query.query;
        let sql = normalize_sql(sql)?;
        info!("execute SQL: {}", sql);

        let ctx = self.get_ctx(&request)?;
        let df = ctx.sql(&sql).await.map_err(|e| status!("Error executing query", e))?;
        let result = df.collect().await.map_err(|e| status!("Error executing query", e))?;
        dbg!(&result);
        Ok(0)
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
        dbg!(&query);

        let handle =
            std::str::from_utf8(&query.prepared_statement_handle).map_err(|e| status!("Unable to parse uuid", e))?;
        let plan = self.get_plan(handle)?;
        // let output = futures::stream::iter(vec![]).boxed();
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
        dbg!(&query);
        info!("do_put_prepared_statement_update");
        let handle =
            std::str::from_utf8(&query.prepared_statement_handle).map_err(|e| status!("Unable to parse uuid", e))?;

        let plan = self.get_plan(handle)?;
        let table = match &plan {
            LogicalPlan::Dml(DmlStatement {
                op: WriteOp::Insert(_),
                table_name,
                ..
            }) => Arc::new(LakeSoulTable::for_table_reference(table_name).await.unwrap()),
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => {
                info!("create external table: {}, {:?}", cmd.name, cmd.definition);
                Arc::new(LakeSoulTable::for_table_reference(&cmd.name).await.unwrap())
            }
            LogicalPlan::EmptyRelation(_) => Arc::new(
                LakeSoulTable::for_table_reference(&TableReference::from("test_table"))
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
            dbg!(&batch);
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
        // dbg!(&query, &request);
        let user_query = query.query.as_str();
        // 使用独立的函数转换查询中的占位符
        let normalized_query = normalize_sql(user_query)?;
        info!("do_action_create_prepared_statement: {normalized_query}");

        let ctx = self.get_ctx(&request)?;

        let plan = ctx.sql(&normalized_query).await;
        // dbg!(&plan);
        let plan = plan
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;
        // dbg!(&user_query, &plan);

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
        dbg!(&handle);
        let handle = std::str::from_utf8(&handle.prepared_statement_handle);
        if let Ok(handle) = handle {
            info!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
        }
        Ok(())
    }

    async fn do_put_statement_ingest(
        &self,
        cmd: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let CommandStatementIngest { table_definition_options, table, schema, catalog, temporary, transaction_id, options } = cmd;
        info!("do_put_statement_ingest: table: {table}, schema: {schema:?}, catalog: {catalog:?}, temporary: {temporary}, transaction_id: {transaction_id:?}, options: {options:?} table_definition_options: {table_definition_options:?}");

        // 获取输入流
        let stream = request.into_inner();
        
        // 创建 LakeSoulTable
        // let table = Arc::new(LakeSoulTable::for_namespace_and_name(schema.unwrap_or("default".to_string()).as_str(), &table)
        //     .await
        //     .map_err(|e| Status::internal(format!("Error creating table: {}", e)))?);
        let table_reference = TableReference::from(match &schema {
            Some(schema) => TableReference::partial(schema.as_str(), table.as_str()),
            None => TableReference::bare(table.as_str()),
        });
        let (flush_result, record_count) = self.write_stream(table_reference, stream).await?;

        // // 开始记录流式指标
        // self.metrics.start_stream();

        // let mut writer = table.get_writer(self.args.s3_options()).await.map_err(lakesoul_error_to_status)?;
        
        // // 创建 FlightRecordBatchStream 来解码数据
        // let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(
        //     stream.map_err(|e| e.into())
        // );

        // // 记录处理的记录数
        // let mut record_count = 0i64;

        // let mut batch_count = 0;

        // // 处理每个批次
        // while let Some(batch) = batch_stream.try_next().await
        //     .map_err(|e| Status::internal(format!("Error getting next batch: {}", e)))? 
        // {
        //     let batch_rows = batch.num_rows();
        //     record_count += batch_rows as i64;
        //     let batch_bytes = get_batch_memory_size(&batch).map_err(datafusion_error_to_status)?;
            
        //     batch_count += 1;


        //     // 这里可以添加实际的数据导入逻辑
        //     // 例如写入数据库等
        //     // info!("batch num_rows: {:?}", batch.num_rows());
        //     writer.write_record_batch(batch).await.map_err(datafusion_error_to_status)?;

        //     self.metrics.add_batch_metrics(batch_rows as u64, batch_bytes as u64);
        //     // Log metrics at intervals
        //     if batch_count % LOG_INTERVAL == 0 {
        //         self.metrics.report_metrics(format!("stream_write_batch_metrics at {} batches", batch_count).as_str());
        //     }

        // }
        // let result = writer.flush_and_close().await.map_err(datafusion_error_to_status)?;

        // // 结束流式处理
        // self.metrics.end_stream();
        let table = Arc::new(LakeSoulTable::for_namespace_and_name(schema.unwrap_or("default".to_string()).as_str(), table.as_str())
            .await
            .map_err(|e| Status::internal(format!("Error creating table: {}", e)))?);

        if let Some(transaction_id) = transaction_id {
            // Convert Bytes to String before passing to append_transactional_data
            let transaction_id = String::from_utf8(transaction_id.to_vec())
                .map_err(|e| Status::internal(format!("Invalid transaction ID: {}", e)))?;
            self.append_transactional_data(transaction_id, table, flush_result)?;
        } else {
            table.commit_flush_result(flush_result).await.map_err(lakesoul_error_to_status)?;
        }

        Ok(record_count)
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
            _ => {
                return Err(Status::internal("Invalid action"));
            }
        }
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
    let statements = DFParser::parse_sql(&sql).map_err(|e| status!("Error parsing SQL", e))?;

    if let Some(Statement::Statement(stmt)) = statements.front() {
        // info!("stmt: {:?}", stmt);
        match stmt.as_ref() {
            datafusion::sql::sqlparser::ast::Statement::CreateTable(CreateTable { name, columns, .. }) => Ok(format!(
                "CREATE EXTERNAL TABLE {} ({}) STORED AS LAKESOUL LOCATION ''",
                name,
                columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            )),
            _ => Ok(sql.to_string()),
        }
    } else {
        Ok(sql.to_string())
    }
}

impl FlightSqlServiceImpl {
    pub async fn new(metadata_client: MetaDataClientRef, args: Args) -> Result<Self> {
        let secret = metadata_client.get_client_secret();
        let jwt_server = Arc::new(JwtServer::new(secret.as_str()));
        let auth_enabled = std::env::var("JWT_AUTH_ENABLED")
            .unwrap_or("false".to_string())
            .parse::<bool>()
            .unwrap();
        // 从环境变量或配置中获取目标速率
        let target_mb_per_second_for_write = std::env::var("STREAM_WRITE_TARGET_MB_PER_SECOND_FOR_WRITE")
            .unwrap_or("2.0".to_string())
            .parse::<f64>()
            .unwrap_or(100.0);
        let target_mb_per_second_for_flush = std::env::var("STREAM_WRITE_TARGET_MB_PER_SECOND_FOR_FLUSH")
            .unwrap_or("2.0".to_string())
            .parse::<f64>()
            .unwrap_or(100.0);

        Ok(FlightSqlServiceImpl {
            client: metadata_client,
            args,
            contexts: Default::default(),
            statements: Default::default(),
            transactional_data: Default::default(),
            jwt_server,
            auth_enabled,
            metrics: Arc::new(StreamWriteMetrics::new(target_mb_per_second_for_write, target_mb_per_second_for_flush)),
        })
    }

    pub async fn init(&self) -> Result<(), Status> {
        self.create_ctx().await?;

        Ok(())
    }

    pub async fn create_ctx(&self) -> Result<String, Status> {
        // let uuid = Uuid::new_v4().hyphenated().to_string();
        let uuid = "1".to_string();
        let mut session_config = SessionConfig::from_env()
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(false)
            // .with_batch_size(100)
            .with_default_catalog_and_schema("LAKESOUL".to_string(), "default".to_string());
        session_config.options_mut().sql_parser.dialect = "postgresql".to_string();
        session_config.options_mut().optimizer.enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
        session_config.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
        session_config.options_mut().execution.parquet.pushdown_filters = true;
        session_config.options_mut().execution.target_partitions = 1;

        let planner = LakeSoulQueryPlanner::new_ref();

        let mut state = SessionState::new_with_config_rt(session_config, Arc::new(RuntimeEnv::default()))
            .with_query_planner(planner);
        state.table_factories_mut().insert(
            "LAKESOUL".to_string(),
            Arc::new(LakeSoulTableProviderFactory::new(self.get_metadata_client(), self.args.warehouse_prefix.clone())),
        );
        let ctx = Arc::new(SessionContext::new_with_state(state));

        let catalog = Arc::new(LakeSoulCatalog::new(self.client.clone(), ctx.clone()));

        // ctx.runtime_env().register_object_store(
        //     Url::parse("s3://").unwrap().scheme(),
        //     Arc::new(S3FileSystem::new())
        // );

        
        if let Some(warehouse_prefix) = &self.args.warehouse_prefix {
            env::set_var("LAKESOUL_WAREHOUSE_PREFIX", warehouse_prefix);
            let url = Url::parse(warehouse_prefix);
            match url {
                Ok(url) => match url.scheme() {
                "s3" | "s3a" => {
                    if let Some(s3_secret_key) = &self.args.s3_secret_key {
                        env::set_var("AWS_SECRET_ACCESS_KEY", s3_secret_key);
                    }
                    if let Some(s3_access_key) = &self.args.s3_access_key {
                        env::set_var("AWS_ACCESS_KEY_ID", s3_access_key);
                    }
                    if let Some(endpoint) = &self.args.endpoint {
                        env::set_var("AWS_ENDPOINT", endpoint);
                    }

                    if ctx.runtime_env()
                        .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath]).map_err(datafusion_error_to_status)?)
                        .is_ok()
                    {
                        return Err(Status::internal("Object store already registered"));
                    }
                    
                    
                    let mut config = self.get_io_config_builder().build();
                    register_s3_object_store(&url, &config, &ctx.runtime_env()).map_err(datafusion_error_to_status)?;

                }
                "hdfs" => {
                    todo!()
                }
                "file" => {
                    ctx.runtime_env()
                        .register_object_store(&url, Arc::new(LocalFileSystem::new()));
                }
                _ => {
                    return Err(Status::internal("Invalid scheme of warehouse prefix"));
                }
            }
            Err(_) => {
                return Err(Status::internal("Invalid warehouse prefix"));
            }
            }
        } else {
            ctx.runtime_env()
                .register_object_store(&Url::parse("file://").unwrap(), Arc::new(LocalFileSystem::new()));
        }

        ctx.state()
            .catalog_list()
            .register_catalog("LAKESOUL".to_string(), catalog);

        ctx.sql("CREATE EXTERNAL TABLE lakesoul_test_table (name STRING, id INT PRIMARY KEY, score FLOAT) STORED AS LAKESOUL LOCATION ''")
            .await.map_err(|e| {
                error!("Error creating table: {e}");
                Status::internal(format!("Error creating table: {e}"))
            })?
            .collect()
            .await
            .map_err(|e| {
                error!("Error collecting results: {e}");
                Status::internal(format!("Error collecting results: {e}"))
            })?;
        let table = LakeSoulTable::for_namespace_and_name("default", "lakesoul_test_table").await.unwrap();
        table.execute_upsert(
            RecordBatch::try_new(SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
                Field::new("score", DataType::Float32, true),
            ])), 
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Float32Array::from(vec![85.5, 90.0, 78.5])),
            ]).map_err(arrow_error_to_status)?,
        ).await.map_err(lakesoul_error_to_status)?;

        // 创建 LakeSoulTable
        let table = LakeSoulTable::for_namespace_and_name("default", "lakesoul_test_table")
            .await
            .map_err(|e| Status::internal(format!("Error creating table: {}", e)))?;

        let mut writer = table.get_writer(self.args.s3_options()).await.map_err(lakesoul_error_to_status)?;
        
        let batch = RecordBatch::try_new(SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
            Field::new("score", DataType::Float32, true),
        ])), 
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float32Array::from(vec![85.5, 90.0, 78.5])),
        ]).map_err(arrow_error_to_status)?;
        // 这里可以添加实际的数据导入逻辑
        // 例如写入数据库等
        // info!("batch num_rows: {:?}", batch.num_rows());
        writer.write_record_batch(batch).await.map_err(datafusion_error_to_status)?;

        let result = writer.flush_and_close().await.map_err(datafusion_error_to_status)?;

        table.commit_flush_result(result).await.map_err(lakesoul_error_to_status)?;

        self.contexts.insert(uuid.clone(), ctx);
        Ok(uuid)
    }

    fn get_ctx<T>(&self, _req: &Request<T>) -> Result<Arc<SessionContext>, Status> {
        // get the token from the authorization header on Request
        // let auth = req
        //     .metadata()
        //     .get("authorization")
        //     .ok_or_else(|| Status::internal("No authorization header!"))?;
        // let str = auth
        //     .to_str()
        //     .map_err(|e| Status::internal(format!("Error parsing header: {e}")))?;
        // let authorization = str.to_string();
        // let bearer = "Bearer ";
        // if !authorization.starts_with(bearer) {
        //     Err(Status::internal("Invalid auth header!"))?;
        // }
        // let auth = authorization[bearer.len()..].to_string();
        let auth = "1".to_string();

        if let Some(context) = self.contexts.get(&auth) {
            Ok(context.clone())
        } else {
            Err(Status::internal(format!("Context handle not found: {auth}")))?
        }
    }

    fn get_plan(&self, handle: &str) -> Result<LogicalPlan, Status> {
        // dbg!(&handle);
        // dbg!(&self.statements);
        if let Some(plan) = self.statements.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!("Plan handle not found: {handle}")))?
        }
    }

    async fn tables(&self, ctx: Arc<SessionContext>) -> RecordBatch {
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
            // dbg!(&catalog);
            let catalog_provider = ctx.catalog(&catalog).unwrap();
            for schema in catalog_provider.schema_names() {
                let schema_provider = catalog_provider.schema(&schema).unwrap();
                for table in schema_provider.table_names() {
                    let table_provider = schema_provider.table(&table).await.unwrap().unwrap();
                    let table_schema = table_provider.schema();

                    let message = SchemaAsIpc::new(&table_schema, &IpcWriteOptions::default())
                        .try_into()
                        .unwrap();
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

        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.statements.remove(&handle.to_string());
        Ok(())
    }

    fn get_metadata_client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

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

    fn clear_transactional_data(&self, transaction_id: String) -> Result<(), Status> {
        self.transactional_data.remove(&transaction_id);
        Ok(())
    }

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

    pub fn get_jwt_server(&self) -> Arc<JwtServer> {
        self.jwt_server.clone()
    }

    fn verify_token(&self, metadata: &MetadataMap) -> Result<(), Status> {
        if !self.auth_enabled {
            return Ok(())
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
        let _ = self
            .jwt_server
            .decode_token(token)
            .map_err(|e| Status::permission_denied(format!("Invalid authorization token: {e}")))?;
        Ok(())
    }

    fn get_io_config_builder(&self) -> LakeSoulIOConfigBuilder {
        let object_store_opttions = self.args.s3_options();
        LakeSoulIOConfigBuilder::new_with_object_store_options(object_store_opttions)
    }

    async fn write_stream(
        &self,
        table_reference: TableReference,
        stream: PeekableFlightDataStream,
    ) -> Result<(WriterFlushResult, i64), Status> {
        // 开始记录流式指标
        self.metrics.start_stream();

        let schema = table_reference.schema();
        let table_name = table_reference.table();

        let table = Arc::new(LakeSoulTable::for_namespace_and_name(schema.unwrap_or("default"), &table_name)
            .await
            .map_err(|e| Status::internal(format!("Error creating table: {}", e)))?);

        let mut writer = table.get_writer(self.args.s3_options()).await.map_err(lakesoul_error_to_status)?;
        
        let mut total_bytes = 0u64;
        // 创建 FlightRecordBatchStream 来解码数据
        let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| e.into())
        );

        // 记录处理的记录数
        let mut record_count = 0i64;
        let mut batch_count = 0;

        // 处理每个批次
        while let Some(batch) = batch_stream.try_next().await
            .map_err(|e| Status::internal(format!("Error getting next batch: {}", e)))? 
        {
            let batch_rows = batch.num_rows();
            record_count += batch_rows as i64;
            let batch_bytes = get_batch_memory_size(&batch).map_err(datafusion_error_to_status)? as u64;
            total_bytes += batch_bytes;
            batch_count += 1;

            writer.write_record_batch(batch).await.map_err(datafusion_error_to_status)?;

            self.metrics.add_batch_metrics(batch_rows as u64, batch_bytes);
            // Log metrics at intervals
            if batch_count % LOG_INTERVAL == 0 {
                self.metrics.report_metrics(format!("stream_write_batch_metrics at {} batches of {}", batch_count, table_name).as_str());
                self.metrics.control_throughput(self.metrics.target_mb_per_second_for_write);
            }
        }
        let result = writer.flush_and_close().await.map_err(datafusion_error_to_status)?;

        // 结束流式处理
        self.metrics.end_stream();

        Ok((result, record_count))
    }

}

