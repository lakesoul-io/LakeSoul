use tonic::transport::Server;
use tonic::Status;
use arrow_flight::flight_service_server::FlightServiceServer;

use lakesoul_flight::FlightSqlServiceImpl;

fn to_tonic_err(e: lakesoul_datafusion::LakeSoulError) -> Status {
    Status::internal(format!("{e:?}"))
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
    let service = FlightSqlServiceImpl::new().await.map_err(to_tonic_err)?;
    service.init().await?;

    let svc = FlightServiceServer::new(service);

    println!("Listening on {addr:?}");

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}