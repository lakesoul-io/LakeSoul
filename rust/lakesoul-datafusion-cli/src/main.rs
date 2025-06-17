use std::{path::Path, process::ExitCode, sync::Arc};

use clap::Parser;
use lakesoul_datafusion::{
    MetaDataClient, cli::CoreArgs, create_lakesoul_session_ctx, tpch::register_tpch_udtfs,
};
use tokio::time::Instant;

#[derive(Parser)]
struct Cli {
    #[clap(
        short,
        long,
        num_args = 0..,
        help = "Execute commands from file(s), then exit",
        value_parser(parse_valid_file)
    )]
    file: Vec<String>,
    #[command(flatten)]
    pub core: CoreArgs,
}

mod exec;
mod print;

fn parse_valid_file(dir: &str) -> Result<String, String> {
    if Path::new(dir).is_file() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid file '{dir}'"))
    }
}

async fn main_inner(cli: Cli) -> anyhow::Result<()> {
    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let ctx = create_lakesoul_session_ctx(meta_client, &cli.core).unwrap();
    register_tpch_udtfs(&ctx).unwrap();
    let files = cli.file;
    let start = Instant::now();
    exec::exec_from_files(&ctx, files).await?;
    let finish = Instant::now();
    println!(
        "Elapsed {}",
        finish.checked_duration_since(start).unwrap().as_secs_f64()
    );
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(61)
        .enable_all()
        .build()
    else {
        eprintln!("init runtime failed");
        return ExitCode::FAILURE;
    };
    if let Err(e) = rt.block_on(main_inner(cli)) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
