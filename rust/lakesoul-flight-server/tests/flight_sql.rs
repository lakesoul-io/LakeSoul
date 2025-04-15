use crate::test_utils::TestServer;
use arrow_array::{record_batch, RecordBatch};
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::FlightInfo;
use futures::prelude::stream::StreamExt;
use std::collections::HashMap;
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

    // actually this strem is sync
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

async fn handle_flight_info(info: &FlightInfo, client: &mut FlightSqlServiceClient<Channel>) {
    println!("flight_info: {:#?}", info);
    for x in &info.endpoint {
        if let Some(ref t) = x.ticket {
            let mut stream = client.do_get(t.clone()).await.unwrap();
            let mut batches = vec![];
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                batches.push(batch);
            }
            // print_batches(&batches).unwrap();
        }
    }
}

async fn query_table(client: &mut FlightSqlServiceClient<Channel>) {
    let sql = "select * from test;";
    let cmd = client.execute(sql.to_string(), None).await.unwrap();
    handle_flight_info(&cmd, client).await;
}

async fn ingest(client: &mut FlightSqlServiceClient<Channel>) {
    let cmd = CommandStatementIngest {
        table_definition_options: None,
        table: String::from("test"),
        schema: Some(String::from("default")),
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: HashMap::new(),
    };

    let batch = record_batch!(("c1", Utf8, ["a", "b", "c"]), ("c2", Int32, [1, 2, 3])).unwrap();

    let batches = vec![batch];

    let stream = RecordBatchStream::new(batches);

    let num = client.execute_ingest(cmd, stream).await.unwrap();
    println!("num: {:#?}", num);
}

async fn init_test(client: &mut FlightSqlServiceClient<Channel>) {
    // drop table
    {
        let sql = String::from("drop table if exists test");
        let info = client.execute(sql, None).await.unwrap();
        handle_flight_info(&info, client).await;
        // println!("info: {:#?}", info)
    }
    // create table
    {
        let sql = String::from(
            "CREATE EXTERNAL TABLE if not exists test (c1 VARCHAR NOT NULL , c2 INT NOT NULL) STORED AS LAKESOUL LOCATION 'file:///tmp/LAKSOUL/test_data.parquet'"
        );
        let info = client.execute(sql, None).await.unwrap();
        handle_flight_info(&info, client).await;
        // println!("info: {:#?}", info)
    }
}

#[tokio::test]
async fn test_flight_sql_server() {
    let _server = TestServer::new(&[]);
    let mut client = build_client().await;
    init_test(&mut client).await;
    ingest(&mut client).await;
    query_table(&mut client).await;
}
