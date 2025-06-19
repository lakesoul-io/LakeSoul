use std::{path::Path, process::ExitCode, sync::Arc};

use crate::exec::{exec_command, exec_from_files, exec_from_repl};
use crate::print::Printer;
use clap::{Parser, Subcommand};
use lakesoul_datafusion::{
    MetaDataClient, cli::CoreArgs, create_lakesoul_session_ctx, tpch::register_tpch_udtfs,
};
use tracing_subscriber::EnvFilter;

mod exec;
mod print;

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

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    TpchGen {
        #[arg(short, long)]
        schema: Option<String>,
        #[arg(short, long)]
        path_prefix: String,
        #[arg(short, long)]
        scale_factor: f64,
        #[arg(short, long)]
        num_parts: i32,
    },
}

fn parse_valid_file(dir: &str) -> Result<String, String> {
    if Path::new(dir).is_file() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid file '{dir}'"))
    }
}

async fn main_inner(cli: Cli) -> anyhow::Result<()> {
    // init log
    let file_appender =
        tracing_appender::rolling::never("/tmp/lakesoul/logs", "console.log");
    let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let level = EnvFilter::from_default_env();
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(level)
        .with_ansi(false)
        .with_thread_ids(true)
        .with_timer(timer)
        .init();

    let meta_client = Arc::new(MetaDataClient::from_env().await?);

    let ctx = create_lakesoul_session_ctx(meta_client, &cli.core).unwrap();

    register_tpch_udtfs(&ctx)?;

    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    let ctx = create_lakesoul_session_ctx(meta_client, &cli.core)?;
    register_tpch_udtfs(&ctx)?;
    let files = cli.file;

    let printer = Printer::default();

    if cli.command.is_some() {
        return exec_command(cli.command.unwrap(), &printer, &ctx).await;
    }

    if !files.is_empty() {
        return exec_from_files(&ctx, &printer, files).await;
    }

    exec_from_repl(&ctx, &printer).await
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.core.worker_threads)
        .enable_all()
        .build()
    else {
        eprintln!("initialize runtime failed");
        return ExitCode::FAILURE;
    };
    if let Err(e) = rt.block_on(main_inner(cli)) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
