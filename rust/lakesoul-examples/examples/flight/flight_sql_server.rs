mod token_codec;

use std::sync::Arc;

use log::info;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use lakesoul_flight::{FlightServiceServerWrapper, FlightSqlServiceImpl, JwtServer};
use lakesoul_metadata::MetaDataClient;

fn to_tonic_err(e: lakesoul_datafusion::LakeSoulError) -> Status {
    Status::internal(format!("{e:?}"))
}

pub mod token {
    include!(concat!(env!("OUT_DIR"), "/json.token.TokenServer.rs"));
}
use crate::token::token_server_server::TokenServerServer;
use crate::token_codec::{Claims, TokenResponse};
use token::token_server_server::TokenServer;

pub struct TokenService {
    jwt_server: Arc<JwtServer>,
}

#[tonic::async_trait]
impl TokenServer for TokenService {
    async fn create_token(&self, request: Request<Claims>) -> Result<Response<TokenResponse>, Status> {
        let claims = request.into_inner();
        let token = self
            .jwt_server
            .create_token(&claims)
            .map_err(|e| Status::internal(format!("Token creation failed: {e:?}")))?;
        info!("Token created {token:?} for claims {claims:?}");
        Ok(Response::new(TokenResponse { token }))
    }
}

/// This example shows how to wrap DataFusion with `FlightService` to support looking up schema information for
/// Parquet files and executing SQL queries against them on a remote server.
/// This example is run along-side the example `flight_client`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 设置详细的日志格式，包含时间、日志级别、文件位置、行号
    std::env::set_var("RUST_LOG", "info");
    // std::env::set_var("RUST_LOG", "debug");
    // 修改日志格式以包含行号
    // %l 表示日志级别
    // %m 表示日志消息
    // %f 表示文件名
    // %L 表示行号
    std::env::set_var("RUST_LOG_FORMAT", "%Y-%m-%dT%H:%M:%S%:z %l [%f:%L] %m");

    env_logger::init();
    let addr = "0.0.0.0:50051".parse()?;
    info!("Connecting to metadata server");

    let metadata_client = Arc::new(MetaDataClient::from_env().await?);
    info!("Metadata server connected");
    info!("Cleaning up metadata server");
    metadata_client.meta_cleanup().await?;
    info!("Metadata server cleaned up");

    let service = FlightSqlServiceImpl::new(metadata_client.clone())
        .await
        .map_err(to_tonic_err)?;
    service.init().await?;
    let jwt_server = service.get_jwt_server();

    let token_service = TokenService { jwt_server };

    // 使用包装器创建服务
    let svc = FlightServiceServerWrapper::new(service, metadata_client.clone());

    info!("Listening on {addr:?}");

    Server::builder()
        .add_service(svc)
        .add_service(TokenServerServer::new(token_service))
        .serve(addr)
        .await?;

    Ok(())
}
