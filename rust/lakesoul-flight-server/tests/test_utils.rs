// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow_array::RecordBatch;
use arrow_flight::{
    error::FlightError,
    sql::{client::FlightSqlServiceClient, CommandStatementIngest},
    FlightInfo,
};
use assert_cmd::cargo::CommandCargoExt;
use futures::{Stream, StreamExt};
use tracing::info;
use std::{
    collections::HashMap,
    pin::Pin,
    process::{Child, Command},
    task::{Context, Poll},
};
use tonic::transport::Channel;

const BIN_NAME: &str = "lakesoul_arrow_flight_sql_server";

pub struct TestServer {
    process: Child,
}

impl TestServer {
    pub fn new(args: &[&'static str]) -> Self {
        info!("test server started");
        let process = Command::cargo_bin(BIN_NAME).unwrap().args(args).spawn().unwrap();
        //  wait 1 seconds
        std::thread::sleep(std::time::Duration::from_secs(1));
        Self { process }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.process.kill().unwrap();
        self.process.wait().unwrap();
        info!("test server dropped");
    }
}

struct RecordBatchStream {
    data: Vec<RecordBatch>,
    index: usize,
}

impl RecordBatchStream {
    fn new(data: Vec<RecordBatch>) -> Self {
        Self { data, index: 0 }
    }
}

// Impl Stream trait
impl Stream for RecordBatchStream {
    type Item = Result<RecordBatch, FlightError>;

    // actually this stream is sync
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.index < this.data.len() {
            let batch = this.data[this.index].clone();
            this.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

pub async fn build_client() -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_static("http://localhost:50051").connect().await.unwrap();
    FlightSqlServiceClient::new(channel)
}

async fn handle_flight_info(info: &FlightInfo, client: &mut FlightSqlServiceClient<Channel>) -> Vec<RecordBatch> {
    info!("handle_flight_info: {:#?}", info);
    let mut batches = vec![];
    for x in &info.endpoint {
        if let Some(ref t) = x.ticket {
            let mut stream = client.do_get(t.clone()).await.unwrap();
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                batches.push(batch);
            }
        }
    }
    batches
}

pub async fn ingest(client: &mut FlightSqlServiceClient<Channel>, batches: Vec<RecordBatch>, table_name: &str) -> i64 {
    let cmd = CommandStatementIngest {
        table_definition_options: None,
        table: String::from(table_name),
        schema: Some(String::from("default")),
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: HashMap::new(),
    };

    let stream = RecordBatchStream::new(batches);

    client.execute_ingest(cmd, stream).await.unwrap()
}

pub fn random_batches() -> Vec<RecordBatch> {
    // arrow::util::data_gen::create_random_batch(schema, size, null_density, true_density);
    todo!()
}

pub async fn handle_sql(client: &mut FlightSqlServiceClient<Channel>, sql: &str) -> Vec<RecordBatch> {
    let info = client.execute(sql.to_string(), None).await.unwrap();
    handle_flight_info(&info, client).await
}
