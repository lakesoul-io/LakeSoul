// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! this is experimental, not for production use

use anyhow::bail;
use clap::Parser;
use datafusion::catalog::Session;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement;
use lakesoul_datafusion::{cli::CoreArgs, create_lakesoul_session_ctx};
use lakesoul_metadata::MetaDataClient;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use sqlparser::ast::{Statement as SQLStatement, Use};
use std::sync::Arc;
use tracing::debug;
use tracing_subscriber::EnvFilter;

fn parse_use(stmt: Statement) -> Option<Use> {
    match stmt {
        Statement::Statement(sql_stmt) => {
            let sql_stmt = *sql_stmt;
            match sql_stmt {
                SQLStatement::Use(u) => Some(u),
                _ => None,
            }
        }
        _ => None,
    }
}

// simple walkaround
async fn exec_sql_and_output(ctx: &Arc<SessionContext>, sql: &str) -> anyhow::Result<()> {
    let stmt = ctx.state().sql_to_statement(sql, "MySQL")?;
    if let Some(u) = parse_use(stmt.clone()) {
        match u {
            Use::Object(name) => {
                let target = name.to_string();
                {
                    let default_schema = &ctx.state().config_mut().options_mut().catalog.default_schema.clone();
                    debug!("change the default schema from {default_schema} to {target}");
                    {
                        // write lock
                        ctx.state_ref()
                            .write()
                            .config_mut()
                            .options_mut()
                            .catalog
                            .default_schema = target;
                    }
                }
            }
            _ => {
                bail!("Not Supported");
            }
        }
        return Ok(());
    }
    let plan = ctx.state().statement_to_plan(stmt).await?;
    let df = ctx.execute_logical_plan(plan).await?;
    df.show().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    // init log
    let file_appender = tracing_appender::rolling::never("/tmp/lakesoul/logs", "console.log");
    let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let level = EnvFilter::from_default_env();
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(level)
        .with_ansi(false)
        .with_timer(timer)
        .init();
    let mut rl = DefaultEditor::new().unwrap();
    let core_args = CoreArgs::parse();
    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());

    let ctx = create_lakesoul_session_ctx(meta_client, &core_args).unwrap();

    loop {
        let readline = rl.readline("lakesoul >> ");

        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
                match exec_sql_and_output(&ctx, &line).await {
                    Err(e) => println!("Error: {:?}", e),
                    _ => {}
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
