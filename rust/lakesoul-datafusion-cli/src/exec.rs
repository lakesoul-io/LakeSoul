// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use datafusion::prelude::SessionContext;
use std::io::BufRead;
use std::{fs::File, io::BufReader};

// run and execute SQL statements and commands from a file, against a context with the given print options
async fn exec_from_lines(
    ctx: &SessionContext,
    reader: &mut BufReader<File>,
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
                    match exec_and_print(ctx, query).await {
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
        exec_and_print(ctx, query).await?;
    }

    Ok(())
}

pub async fn exec_from_files(
    ctx: &SessionContext,
    files: Vec<String>,
) -> anyhow::Result<()> {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();
    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader).await?;
    }
    Ok(())
}

async fn exec_and_print(ctx: &SessionContext, sql: String) -> anyhow::Result<()> {
    let df = ctx.sql(&sql).await?;
    let _ = df.show().await;
    Ok(())
}
