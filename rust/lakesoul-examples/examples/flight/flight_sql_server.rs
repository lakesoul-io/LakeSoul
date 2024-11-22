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
    env_logger::init();
    let addr = "0.0.0.0:50051".parse()?;
    let service = FlightSqlServiceImpl::new().await.map_err(to_tonic_err)?;
    service.create_ctx().await?;

    let svc = FlightServiceServer::new(service);

    println!("Listening on {addr:?}");

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}