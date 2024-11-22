use std::sync::Arc;
use std::pin::Pin;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::util::pretty::print_batches;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetTables, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery, ProstMessageExt, SqlInfo};
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket
};
use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use lakesoul_datafusion::planner::query_planner::LakeSoulQueryPlanner;
use tonic::{Request, Response, Status, metadata::MetadataValue, Streaming};
use prost::Message;
use prost::bytes::Bytes;

use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use uuid::Uuid;

use lakesoul_datafusion::Result;

use dashmap::DashMap;
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;
use arrow::record_batch::RecordBatch;
use log::info;
use arrow::array::{ArrayRef, BinaryArray, StringArray};

use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::local::LocalFileSystem;
use lakesoul_datafusion::catalog::lakesoul_catalog::LakeSoulCatalog;
use datafusion::execution::context::SessionState;
use url::Url;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}


pub struct FlightSqlServiceImpl {
    client: MetaDataClientRef,
    contexts: Arc<DashMap<String, Arc<SessionContext>>>,
    statements: Arc<DashMap<String, LogicalPlan>>,
    results: Arc<DashMap<String, Vec<RecordBatch>>>,
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = Self;

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) { }

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        info!("do_handshake");
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
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }


    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        dbg!(&cmd, &request);
        info!("get_flight_info_prepared_statement");
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse uuid", e))?;

        let ctx = self.get_ctx(&request)?;
        let plan = self.get_plan(handle)?;

        let state = ctx.state();
        dbg!(&plan);
        let df = DataFrame::new(state, plan);
        let result = df
            .collect()
            .await
            .map_err(|e| status!("Error executing query", e))?;

        // if we get an empty result, create an empty schema
        let schema = match result.first() {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(handle.to_string(), result);

        // if we had multiple endpoints to connect to, we could use this Location
        // but in the case of standalone DataFusion, we don't
        // let loc = Location {
        //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
        // };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };

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


    async fn get_flight_info_statement(
        &self,
        cmd: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        dbg!(&cmd, &request);
        info!("get_flight_info_prepared_statement");
        let sql = &cmd.query;

        let ctx = self.get_ctx(&request)?;
        // let plan = self.get_plan(handle)?;

        // let state = ctx.state();
        let df = ctx.sql(sql).await.map_err(|e| status!("Error executing query", e))?;
        let result = df
            .collect()
            .await
            .map_err(|e| status!("Error executing query", e))?;

        // if we get an empty result, create an empty schema
        let schema = match result.first() {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(sql.to_string(), result);

        // if we had multiple endpoints to connect to, we could use this Location
        // but in the case of standalone DataFusion, we don't
        // let loc = Location {
        //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
        // };
        let fetch = FetchResults {
            handle: sql.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };

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

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any
    ) -> Result<Response<BoxStream<'static, Result<FlightData, Status>>>, Status> {
        dbg!(&message, &request);
        dbg!("do_get_fallback");
        if !message.is::<FetchResults>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let fr: FetchResults = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;

        let handle = fr.handle;

        info!("getting results for {handle}");
        let result = self.get_result(&handle)?;
        // if we get an empty result, create an empty schema
        let (schema, batches) = match result.first() {
            None => (Arc::new(Schema::empty()), vec![]),
            Some(batch) => (batch.schema(), result.clone()),
        };

        let batch_stream = futures::stream::iter(batches).map(Ok);

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))

    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        info!("do_put_prepared_statement_update");
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse uuid", e))?;

        let stream = request.into_inner();
        let batches = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| e.into()))
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| status!("Error collecting batches", e))?;
        print_batches(&batches);

        let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
        Ok(row_count)
    }
    
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        dbg!(&_query, &request);
        info!("get_flight_info_tables");
        let ctx = self.get_ctx(&request)?;
        let data = self.tables(ctx).await;
        let schema = data.schema();

        let uuid = Uuid::new_v4().hyphenated().to_string();
        self.results.insert(uuid.clone(), vec![data]);

        let fetch = FetchResults { handle: uuid };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };

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

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        dbg!(&_query, &request);
        info!("get_flight_info_schemas");
        let ctx = self.get_ctx(&request)?;
        
        // 执行查询获取所有schema
        // let sql = "SELECT catalog_name, schema_name FROM datafusion.information_schema.schemata";
        let sql = "SELECT CAST('lakesoul' AS VARCHAR) AS catalog_name, CAST('default' AS VARCHAR) AS db_schema_name";
        let df = ctx.sql(sql).await.map_err(|e| Status::internal(format!("Error executing query: {e}")))?;
        let data = df.collect().await.map_err(|e| Status::internal(format!("Error collecting results: {e}")))?[0].clone();
        
        // 修改 schema 使 catalog_name 可空
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),     // 设置 nullable=true
            Field::new("db_schema_name", DataType::Utf8, false)
        ]));
        
        // 使用新的 schema 创建新的 RecordBatch
        let data = RecordBatch::try_new(
            schema.clone(),
            data.columns().to_vec()
        ).map_err(|e| Status::internal(format!("Error creating record batch: {e}")))?;

        // 生成唯一标识并存储结果
        let uuid = Uuid::new_v4().hyphenated().to_string();
        self.results.insert(uuid.clone(), vec![data]);

        let fetch = FetchResults { handle: uuid };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };

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

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        dbg!(&query, &request);
        let user_query = query.query.as_str();
        info!("do_action_create_prepared_statement: {user_query}");

        let ctx = self.get_ctx(&request)?;

        let plan = ctx
            .sql(user_query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        // store a copy of the plan,  it will be used for execution
        let plan_uuid = Uuid::new_v4().hyphenated().to_string();
        self.statements.insert(plan_uuid.clone(), plan.clone());

        let plan_schema = plan.schema();

        let arrow_schema = (&**plan_schema).into();
        let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: plan_uuid.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(),
        };
        Ok(res)
    }

    

    async fn do_action_close_prepared_statement(
        &self,
        handle: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        dbg!(&handle);
        let handle = std::str::from_utf8(&handle.prepared_statement_handle);
        if let Ok(handle) = handle {
            info!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
            let _ = self.remove_result(handle);
        }
        Ok(())
    }

    /// Get a FlightInfo for listing catalogs.
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        dbg!(&query, &request);
        info!("get_flight_info_catalogs");
        let ctx = self.get_ctx(&request)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
        ]));

        let handle = Uuid::new_v4().hyphenated().to_string();
        let catalogs = ctx.catalog_names();
        let array = StringArray::from(catalogs);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])
            .map_err(|e| status!("Error creating record batch", e))?;

        self.results.insert(handle.clone(), vec![batch]);

        let ticket = Ticket {
            ticket: FetchResults { handle }.as_any().encode_to_vec().into(),
        };

        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .expect("encoding failed")
            .with_endpoint(endpoint);

        Ok(Response::new(flight_info))
    }

}

impl FlightSqlServiceImpl {
    pub async fn new() -> Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        Ok(FlightSqlServiceImpl {
            client,
            contexts: Default::default(),
            statements: Default::default(),
            results: Default::default(),
        })
    }


    pub async fn create_ctx(&self) -> Result<String, Status> {
        let uuid = Uuid::new_v4().hyphenated().to_string();
        let uuid = "1".to_string();
        let session_config = SessionConfig::from_env()
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?
            .with_information_schema(true);

        let planner = LakeSoulQueryPlanner::new_ref();

        let state = SessionState::new_with_config_rt(
            session_config,
            Arc::new(RuntimeEnv::default()),
        ).with_query_planner(planner);
        let ctx = Arc::new(SessionContext::new_with_state(state));

        let catalog = Arc::new(LakeSoulCatalog::new(self.client.clone(), ctx.clone()));

        // ctx.runtime_env().register_object_store(
        //     Url::parse("s3://").unwrap().scheme(),
        //     Arc::new(S3FileSystem::new())
        // );
        let file_url = Url::parse("file://").unwrap();
        ctx.runtime_env().register_object_store(
            &file_url,
            Arc::new(LocalFileSystem::new())
        );

        ctx.state().catalog_list().register_catalog(
            "lakesoul".to_string(), 
            catalog
        );
        dbg!(ctx.state().catalog_list().catalog_names());
        ctx.sql("create table example(a int, b int)")
            .await
            .map_err(|e| Status::internal(format!("Error executing query: {e}")))?
            .collect()
            .await
            .map_err(|e| Status::internal(format!("Error collecting results: {e}")))?;


        self.contexts.insert(uuid.clone(), ctx);
        Ok(uuid)
    }

    fn get_ctx<T>(&self, req: &Request<T>) -> Result<Arc<SessionContext>, Status> {
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
            Err(Status::internal(format!(
                "Context handle not found: {auth}"
            )))?
        }
    }

    fn get_plan(&self, handle: &str) -> Result<LogicalPlan, Status> {
        dbg!(&handle);
        dbg!(&self.statements);
        if let Some(plan) = self.statements.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!("Plan handle not found: {handle}")))?
        }
    }

    fn get_result(&self, handle: &str) -> Result<Vec<RecordBatch>, Status> {
        if let Some(result) = self.results.get(handle) {
            Ok(result.clone())
        } else {
            Err(Status::internal(format!(
                "Request handle not found: {handle}"
            )))?
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
            dbg!(&catalog);
            let catalog_provider = ctx.catalog(&catalog).unwrap();
            for schema in catalog_provider.schema_names() {
                let schema_provider = catalog_provider.schema(&schema).unwrap();
                for table in schema_provider.table_names() {
                    let table_provider = schema_provider.table(&table).await.unwrap();
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
            Arc::new(binary_array)
        ];

        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.statements.remove(&handle.to_string());
        Ok(())
    }

    fn remove_result(&self, handle: &str) -> Result<(), Status> {
        self.results.remove(&handle.to_string());
        Ok(())
    }


}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        ""
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}