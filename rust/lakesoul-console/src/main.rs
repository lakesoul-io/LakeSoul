// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, process::ExitCode, sync::Arc};

use clap::{Parser, Subcommand};
use lakesoul_datafusion::{
    MetaDataClient,
    cli::CoreArgs,
    distributed::{DistributedOptions, WorkerDiscovery},
    session::{LakeSoulSessionFactory, LakeSoulSessionOptions},
    tpch::register_tpch_udtfs,
};
use rand::Rng;
use rand::distr::Alphanumeric;
use rootcause::Report;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

use crate::exec::{exec_command, exec_from_files, exec_from_repl};
use crate::print::Printer;

type Result<T, E = Report> = std::result::Result<T, E>;

mod exec;
mod logo;
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

    #[arg(
        long,
        default_value = "/tmp",
        help = "log dir, end with '/' is not valid"
    )]
    log_dir: String,

    #[command(flatten)]
    pub core: CoreArgs,

    /// Worker gRPC URL. Supplying one or more workers enables distributed execution.
    #[arg(long = "worker", value_name = "URL")]
    workers: Vec<String>,

    /// Target number of partitions for distributed execution.
    #[arg(long, default_value_t = 4)]
    target_partitions: usize,

    /// Approximate bytes assigned to each distributed file-scan task.
    #[arg(long)]
    bytes_per_partition: Option<usize>,

    /// Development only: execute locally if no configured worker is available.
    #[arg(long)]
    distributed_fallback_local: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    TpchGen {
        #[arg(long)]
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

fn rand_str() -> String {
    // 创建线程本地随机数生成器
    let mut rng = rand::rng();

    // 生成 len 个随机的字母数字字符
    let s: String = (0..5)
        .map(|_| rng.sample(Alphanumeric))
        .map(char::from) // Alphanumeric 是 u8，需要转成 char
        .collect();

    s
}

fn init_log(mut log_dir: &str) -> WorkerGuard {
    if log_dir.ends_with("/") {
        log_dir = &log_dir[..log_dir.len() - 1];
    }

    let log_dir = format!("{log_dir}/lakesoul_log_{}", rand_str());
    let file_appender = tracing_appender::rolling::never(&log_dir, "console.log");
    let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    let level = EnvFilter::from_default_env();
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(level)
        .with_ansi(false)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_thread_ids(true)
        .with_timer(timer)
        .init();
    tracing::debug!("log_dir:{}", &log_dir);
    guard
}

fn print_banner() {
    println!("{}", logo::LOGO);
}

async fn main_inner(cli: Cli) -> Result<()> {
    print_banner();
    let _log_guard = init_log(&cli.log_dir);
    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    let mut session_factory = LakeSoulSessionFactory::new(meta_client, &cli.core)?;
    if !cli.workers.is_empty() {
        session_factory = session_factory.with_distributed(DistributedOptions {
            discovery: WorkerDiscovery::Static(cli.workers.clone()),
            fallback_to_local: cli.distributed_fallback_local,
            target_partitions: cli.target_partitions,
            bytes_per_partition: cli.bytes_per_partition,
        });
    }
    let ctx = session_factory.create_session(&LakeSoulSessionOptions::default())?;
    register_tpch_udtfs(&ctx)?;
    let files = cli.file;

    let printer = Printer::default();

    if let Some(cmd) = cli.command {
        return exec_command(cmd, &printer, &ctx).await;
    }

    if !files.is_empty() {
        return exec_from_files(&ctx, &printer, files).await;
    }

    exec_from_repl(&ctx, &printer).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workers_enable_distributed_execution_options() {
        let cli = Cli::try_parse_from([
            "lakesoul-console",
            "--worker",
            "http://127.0.0.1:50051",
            "--worker",
            "http://127.0.0.1:50052",
            "--target-partitions",
            "2",
            "--bytes-per-partition",
            "1",
        ])
        .unwrap();

        assert_eq!(
            cli.workers,
            [
                "http://127.0.0.1:50051".to_string(),
                "http://127.0.0.1:50052".to_string(),
            ]
        );
        assert_eq!(cli.target_partitions, 2);
        assert_eq!(cli.bytes_per_partition, Some(1));
        assert!(!cli.distributed_fallback_local);
    }

    #[test]
    fn no_workers_keeps_distributed_mode_disabled() {
        let cli = Cli::try_parse_from(["lakesoul-console"]).unwrap();

        assert!(cli.workers.is_empty());
        assert!(!cli.distributed_fallback_local);
    }
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
