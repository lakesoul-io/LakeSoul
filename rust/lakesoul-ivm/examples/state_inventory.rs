// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Print the state inventory of an IVM view chain.
//!
//! ```sh
//! cargo run -q -p lakesoul-ivm --example state_inventory -- \
//!     example_orders --namespace example
//! ```
//!
//! The runtime reads its PostgreSQL and object-store settings from the
//! `LAKESOUL_*` environment, like the rest of the crate.  Use `--json` for a
//! machine-readable report and `--no-rows` to skip the `COUNT(*)` pass.

use lakesoul_ivm::{InventoryOptions, IvmRuntime};
use rootcause::report;

#[tokio::main]
async fn main() -> lakesoul_ivm::Result<()> {
    let mut table = None;
    let mut namespace = "default".to_string();
    let mut json = false;
    let mut include_rows = true;
    let mut top_tables = 10usize;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--namespace" | "-n" => {
                namespace = args
                    .next()
                    .ok_or_else(|| report!("--namespace needs a value"))?;
            }
            "--json" => json = true,
            "--no-rows" => include_rows = false,
            "--top" => {
                let value = args.next().ok_or_else(|| report!("--top needs a value"))?;
                top_tables = value
                    .parse::<usize>()
                    .map_err(|error| report!("invalid --top value {value:?}: {error}"))?
                    .max(1);
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            other if other.starts_with('-') => {
                return Err(report!("unknown option {other}"));
            }
            other => {
                if table.replace(other.to_string()).is_some() {
                    return Err(report!("unexpected extra table argument {other}"));
                }
            }
        }
    }
    let table = table.ok_or_else(|| {
        report!(
            "usage: state_inventory <table> [--namespace <ns>] [--json] [--no-rows] [--top <n>]"
        )
    })?;

    let runtime = IvmRuntime::from_env().await?;
    let options = InventoryOptions {
        include_rows,
        top_tables,
    };
    let inventory = runtime
        .state_inventory_for_table_with_options(&namespace, &table, &options)
        .await?;
    if json {
        println!("{}", inventory.to_json_pretty()?);
    } else {
        print!("{}", inventory.render_text());
    }
    Ok(())
}

fn print_usage() {
    println!(
        "usage: state_inventory <table> [--namespace <ns>] [--json] [--no-rows] [--top <n>]\n\
         \n\
         The runtime reads LAKESOUL_PG_* and object-store settings from the environment.\n\
         --no-rows skips COUNT(*) over every table (metadata-only report)."
    );
}
