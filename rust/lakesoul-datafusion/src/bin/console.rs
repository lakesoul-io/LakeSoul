// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! this is experimental, not for production use
use std::sync::Arc;

use clap::Parser;
use lakesoul_datafusion::{cli::CoreArgs, create_lakesoul_session_ctx};
use lakesoul_metadata::MetaDataClient;
use rustyline::error::ReadlineError;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    tracing_subscriber::fmt::init();

    let core_args = CoreArgs::parse();

    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());

    let ctx = create_lakesoul_session_ctx(meta_client, &core_args).unwrap();

    let mut rl = rustyline::DefaultEditor::new().unwrap();
    loop {
        let readline = rl.readline("lakesoul >> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();

                match ctx.sql(line.as_str()).await {
                    Ok(df) => df.show().await.unwrap(),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("bye bye! 🫡");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
