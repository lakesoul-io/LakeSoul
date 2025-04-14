use crate::test_utils::TestServer;
use arrow_array::{record_batch, RecordBatch};
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandStatementIngest;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::codegen::tokio_stream::Stream;
use tonic::transport::Channel;

mod test_utils {
    use assert_cmd::cargo::CommandCargoExt;
    use std::process::{Child, Command};

    pub struct TestServer {
        process: Child,
    }

    impl TestServer {
        pub fn new(args: &[&'static str]) -> Self {
            // 启动服务器进程
            println!("test server started");
            let process = Command::cargo_bin("lakesoul_arrow_flight_sql_server")
                .unwrap()
                .args(args)
                .spawn()
                .unwrap();

            //  wait 1 seconds
            std::thread::sleep(std::time::Duration::from_secs(1));

            Self { process }
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.process.kill().unwrap();
            self.process.wait().unwrap();
            println!("test server dropped");
        }
    }
}

async fn build_client() -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_static("http://localhost:50051").connect().await.unwrap();

    FlightSqlServiceClient::new(channel)
}

#[tokio::test]
async fn flight() {
    let _server = TestServer::new(&[]);
    let mut client = build_client().await;
    let x = client.handshake("", "").await.unwrap();
    // let sql = String::from("show tables");
    // let flight_info = client.execute(sql, None).await.unwrap();
    // // client.execute_ingest();
    // println!("flight_info: {:?}", flight_info);
    // for x in flight_info.endpoint {
    //     if let Some(t) = x.ticket {
    //         let stream = client.do_get(t).await.unwrap();
    //         let batches = stream.collect::<Result<Vec<_>, _>>().await.unwrap();
    //         print_batches(&batches).unwrap();
    //     }
    // }
}

// 定义一个结构体来表示流
struct RecordBatchStream {
    data: Vec<RecordBatch>,
    index: usize,
}

impl RecordBatchStream {
    fn new(data: Vec<RecordBatch>) -> Self {
        Self { data, index: 0 }
    }
}

// 实现 Stream trait
impl Stream for RecordBatchStream {
    type Item = Result<RecordBatch, FlightError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.index < this.data.len() {
            let batch = this.data[this.index].clone();
            this.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None) // 结束流
        }
    }
}

#[tokio::test]
async fn test_flight_ingest() {
    let _server = TestServer::new(&[]);
    let mut client = build_client().await;
    let sql = String::from("show tables");
    let flight_info = client.execute(sql, None).await.unwrap();

    let cmd = {
        let mut stat = CommandStatementIngest::default();
        stat
    };

    let batch = record_batch!(
        ("a", Int32, [1, 2, 3]),
        ("b", Float64, [Some(4.0), None, Some(5.0)]),
        ("c", Utf8, ["alpha", "beta", "gamma"])
    )
    .unwrap();

    let batches = vec![batch];

    let stream = RecordBatchStream::new(batches);

    let num = client.execute_ingest(cmd, stream).await.unwrap();

    println!("flight_info: {:?}", flight_info);
}
