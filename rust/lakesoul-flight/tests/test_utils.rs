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
use core::panic;
use futures::{Stream, StreamExt};
use lakesoul_flight::{Claims, TokenServerClient};
use std::{
    collections::HashMap,
    pin::Pin,
    process::{Child, Command},
    task::{Context, Poll},
};
use tonic::{transport::Channel, Request};
use tracing::info;

const BIN_NAME: &str = "flight_sql_server";

pub struct TestServer {
    process: Child,
}

impl TestServer {
    pub fn new(args: &[&'static str], envs: Vec<(&'static str, &'static str)>) -> anyhow::Result<Self> {
        info!("test server started");
        let process = Command::cargo_bin(BIN_NAME)?.args(args).envs(envs).spawn()?;
        //  wait 1 seconds
        std::thread::sleep(std::time::Duration::from_secs(1));
        Ok(Self { process })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.process.kill().unwrap();
        match self.process.wait().unwrap().code() {
            Some(n) => {
                // this means error occured
                panic!("server error code: {}", n)
            }
            None => return, // kill by sig is ok
        }
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

    // actually, this stream is sync
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

pub async fn build_client(claims: Option<Claims>) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_static("http://localhost:50051").connect().await.unwrap();
    let mut client = FlightSqlServiceClient::new(channel.clone());
    if let Some(c) = claims {
        let mut token_server = TokenServerClient::new(channel);
        let token = token_server.create_token(Request::new(c)).await.unwrap();
        client.set_header("authorization", format!("Bearer {}", token.get_ref().token));
    }

    client
}

async fn handle_flight_info(
    info: &FlightInfo,
    client: &mut FlightSqlServiceClient<Channel>,
) -> anyhow::Result<Vec<RecordBatch>> {
    info!("handle_flight_info: {:#?}", info);
    let mut batches = vec![];
    for x in &info.endpoint {
        if let Some(ref t) = x.ticket {
            let mut stream = client.do_get(t.clone()).await?;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                batches.push(batch);
            }
        }
    }
    Ok(batches)
}

pub async fn ingest(
    client: &mut FlightSqlServiceClient<Channel>,
    batches: Vec<RecordBatch>,
    table_name: &str,
    schema: Option<String>,
) -> anyhow::Result<i64> {
    let cmd = CommandStatementIngest {
        table_definition_options: None,
        table: String::from(table_name),
        schema,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: HashMap::new(),
    };

    let stream = RecordBatchStream::new(batches);

    Ok(client.execute_ingest(cmd, stream).await?)
}

pub fn random_batches() -> Vec<RecordBatch> {
    // arrow::util::data_gen::create_random_batch(schema, size, null_density, true_density);
    todo!()
}

pub async fn handle_sql(client: &mut FlightSqlServiceClient<Channel>, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let info = client.execute(sql.to_string(), None).await?;
    Ok(handle_flight_info(&info, client).await?)
}
