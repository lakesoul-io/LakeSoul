// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::Command;
use crate::print::Printer;
use anyhow::bail;
use datafusion::arrow::datatypes::Schema;
use datafusion::logical_expr::sqlparser::ast::{Statement as SQLStatement, Use};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement;
use futures::stream::StreamExt;
use lakesoul_datafusion::tpch::tpch_gen_sql;
use rustyline::error::ReadlineError;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use std::{fs::File, io::BufReader};
use tokio::signal;
use tracing::debug;

pub async fn exec_command(
    commands: Command,
    ctx: &SessionContext,
    printer: &Printer,
) -> anyhow::Result<()> {
    match commands {
        Command::TpchGen {
            schema,
            path_prefix,
            scale_factor,
            num_parts,
        } => {
            let mut all = vec![];
            let all_targets = [
                "part".to_string(),
                "customer".to_string(),
                "lineitem".to_string(),
                "nation".to_string(),
                "supplier".to_string(),
                "partsupp".to_string(),
                "order".to_string(),
                "region".to_string(),
            ];

            for t in all_targets.iter() {
                let table_name = if let Some(sha) = &schema {
                    format!("{}.{}", sha, t)
                } else {
                    t.clone()
                };
                let mut buf = PathBuf::from(&path_prefix);
                buf.push(t);
                let v = tpch_gen_sql(
                    &table_name,
                    t,
                    scale_factor,
                    num_parts,
                    buf.to_str().unwrap(),
                );
                all.push(v);
            }

            for table in all.iter() {
                for sql in table.iter() {
                    exec_and_print(ctx, printer, sql).await?;
                }
            }
        }
    }
    Ok(())
}

// run and execute SQL statements and commands from a file, against a context with the given print options
async fn exec_from_lines(
    ctx: &SessionContext,
    reader: &mut BufReader<File>,
    printer: &Printer,
) -> anyhow::Result<()> {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("#!") => {
                continue;
            }
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, printer, &query).await {
                        Ok(_) => {}
                        Err(err) => eprintln!("{err}"),
                    }
                    query = "".to_string();
                } else {
                    query.push('\n');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ‘;’
    // ignore if it only consists of '\n'
    if query.contains(|c| c != '\n') {
        exec_and_print(ctx, printer, &query).await?;
    }

    Ok(())
}

pub async fn exec_from_files(
    ctx: &SessionContext,
    printer: &Printer,
    files: Vec<String>,
) -> anyhow::Result<()> {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();
    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader, printer).await?;
    }
    Ok(())
}

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
async fn exec_and_print(
    ctx: &SessionContext,
    printer: &Printer,
    sql: &str,
) -> anyhow::Result<()> {
    let now = Instant::now();
    let stmt = ctx.state().sql_to_statement(sql, "postgresql")?;
    if let Some(u) = parse_use(stmt.clone()) {
        match u {
            Use::Object(name) => {
                let target = name.to_string();
                if ctx
                    .state()
                    .catalog_list()
                    .catalog("LAKESOUL")
                    .unwrap()
                    .schema(&target)
                    .is_none()
                {
                    bail!("{} not found in LakeSoul", target)
                }
                let default_schema = &ctx
                    .state()
                    .config_mut()
                    .options_mut()
                    .catalog
                    .default_schema
                    .clone();
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
            _ => {
                bail!("Not Supported");
            }
        }
        // use finished
        return printer.print_batches(Arc::new(Schema::empty()), &[], now, 0);
    }
    let plan = ctx.state().statement_to_plan(stmt).await?;
    let df = ctx.execute_logical_plan(plan).await?;
    let physical_plan = df.create_physical_plan().await?;
    let schema = physical_plan.schema();
    let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;
    let mut row_count = 0_usize;
    let mut res = vec![];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let curr_num_rows = batch.num_rows();
        res.push(batch);
        row_count += curr_num_rows;
    }
    printer.print_batches(schema, &res, now, row_count)?;
    Ok(())
}

pub async fn exec_from_repl(
    ctx: &SessionContext,
    printer: &Printer,
) -> anyhow::Result<()> {
    let mut rl = rustyline::DefaultEditor::new()?;
    rl.load_history("console.history").ok();
    loop {
        match rl.readline("lakesoul >> ") {
            Ok(line) => {
                rl.add_history_entry(line.trim_end())?;
                tokio::select! {
                        res = exec_and_print(ctx, printer, &line) => match res {
                            Ok(_) => {}
                            Err(err) => eprintln!("{err}"),
                        },
                        _ = signal::ctrl_c() => {
                            println!("^C");
                            continue
                        },
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("bye bye! 🫡");
                break;
            }
            Err(err) => {
                eprintln!("Unknown error happened {err:?}");
                break;
            }
        }
    }

    rl.save_history("console.history")?;
    Ok(())
}
