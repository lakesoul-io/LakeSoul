mod token_codec;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use metrics_exporter_prometheus::PrometheusBuilder;
use tonic::service::Interceptor;
use clap::Parser;

use lakesoul_flight::{FlightSqlServiceImpl, JwtServer, args::Args};
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

// 创建一个简单的拦截器
#[derive(Clone)]
struct GrpcInterceptor {
    total_requests: Arc<AtomicU64>,
    active_requests: Arc<AtomicU64>,
    total_bytes_in: Arc<AtomicU64>,
    start_time: Arc<Instant>,
}

impl Default for GrpcInterceptor {
    fn default() -> Self {
        Self {
            total_requests: Arc::new(AtomicU64::new(0)),
            active_requests: Arc::new(AtomicU64::new(0)),
            total_bytes_in: Arc::new(AtomicU64::new(0)),
            start_time: Arc::new(Instant::now()),
        }
    }
}

impl Interceptor for GrpcInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let start = Instant::now();
        let path = request.metadata()
            .get("grpc-path")
            .map(|v| v.to_str().unwrap_or("unknown"))
            .unwrap_or("unknown")
            .to_string();
        
        let request_size = request.metadata()
            .len() as u64;
        
        let total_bytes = self.total_bytes_in.fetch_add(request_size, Ordering::SeqCst);
        
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        let active = self.active_requests.fetch_add(1, Ordering::SeqCst);
        
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        let throughput_bytes = if elapsed_secs > 0.0 {
            (total_bytes + request_size) as f64 / elapsed_secs
        } else {
            0.0
        };
        let throughput_requests = if elapsed_secs > 0.0 {
            (self.total_requests.load(Ordering::SeqCst) + 1) as f64 / elapsed_secs
        } else {
            0.0
        };
        
        info!(
            "请求开始 - 路径: {}, 当前活跃请求数: {}, 请求大小: {} 字节, 总接收字节数: {}, 吞吐量: {:.2} 字节/秒, {:.2} 请求/秒",
            path, active + 1, request_size, total_bytes + request_size, throughput_bytes, throughput_requests
        );
        
        request.extensions_mut().insert(CallbackOnDrop {
            path,
            start,
            active_requests: self.active_requests.clone(),
        });
        
        Ok(request)
    }
}

#[derive(Clone)]
struct CallbackOnDrop {
    path: String,
    start: Instant,
    active_requests: Arc<AtomicU64>,
}

impl Drop for CallbackOnDrop {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        let active = self.active_requests.fetch_sub(1, Ordering::SeqCst);
        
        info!(
            "请求结束 - 路径: {}, 耗时: {:?}, 剩余活跃请求数: {}",
            self.path, duration, active - 1
        );
    }
}

/// This example shows how to wrap DataFusion with `FlightService` to support looking up schema information for
/// Parquet files and executing SQL queries against them on a remote server.
/// This example is run along-side the example `flight_client`.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数
    let args = Args::parse();

    // 创建并配置 tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.worker_threads)
        .enable_all()
        .build()?;

    // 使用 runtime 运行异步代码
    runtime.block_on(async {
        // 设置日志级别
        env_logger::init();

        let addr = args.addr.parse()?;
        info!("Connecting to metadata server");

        let metadata_client = Arc::new(MetaDataClient::from_env().await?);
        info!("Metadata server connected");
        info!("Cleaning up metadata server");
        metadata_client.meta_cleanup().await?;
        info!("Metadata server cleaned up");

        // 使用参数中的 metrics_addr
        let metrics_addr = {
            let re = regex::Regex::new(r"^([^:]+):(\d+)$").unwrap();
            let (host, port) = if let Some(caps) = re.captures(&args.metrics_addr) {
                (
                    caps.get(1).unwrap().as_str().parse()?,
                    caps.get(2).unwrap().as_str().parse()?
                )
            } else {
                return Err("Invalid metrics_addr format".into());
            };
            std::net::SocketAddr::new(host, port)
        };

        let builder = PrometheusBuilder::new();
        builder
            .with_http_listener(metrics_addr)
            .add_global_label("service", "lakesoul_flight")
            .install()?;

        let service = FlightSqlServiceImpl::new(metadata_client.clone(), args)
            .await?;
        service.init().await?;
        let jwt_server = service.get_jwt_server();

        let token_service = TokenService { jwt_server };

        let interceptor = GrpcInterceptor::default();
        
        let svc = FlightServiceServer::with_interceptor(service, interceptor);

        info!("Listening on {addr:?}");
        info!("Metrics server listening on {:?}", std::net::SocketAddr::from(([0, 0, 0, 0], 19000)));

        Server::builder()
            .add_service(svc)
            .add_service(TokenServerServer::new(token_service))
            .serve(addr)
            .await?;

        Ok(())
    })
}
