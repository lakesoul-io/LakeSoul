// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, process::ExitCode, sync::Arc};

use clap::{Parser, Subcommand};
use lakesoul_datafusion::{
    MetaDataClient,
    cli::CoreArgs,
    distributed::{DistributedOptions, WorkerDiscovery},
    session::{ExecutionMemoryArgs, LakeSoulSessionFactory, LakeSoulSessionOptions},
    tpch::register_tpch_udtfs,
};
use lakesoul_observability::MemorySamplingArgs;
use rand::Rng;
use rand::distr::Alphanumeric;
use rootcause::Report;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

use crate::exec::{exec_command, exec_from_files, exec_from_repl};
use crate::print::{OutputFormat, Printer};

type Result<T, E = Report> = std::result::Result<T, E>;

mod allocator;
mod exec;
mod logo;
mod print;

// An explicit `about`: without one clap shows the doc comment of the first
// flattened argument struct as the console's description.
#[derive(Parser)]
#[command(about = "LakeSoul interactive SQL console")]
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

    /// Result output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,

    #[command(flatten)]
    pub core: CoreArgs,

    #[command(flatten)]
    memory: ExecutionMemoryArgs,

    // The console exports no metrics: samples are useful only with
    // `--memory-sample-file`, and are taken only then.
    #[command(flatten)]
    memory_sampling: MemorySamplingArgs,

    /// Worker gRPC URL. Supplying one or more workers enables distributed execution.
    #[arg(long = "worker", value_name = "URL")]
    workers: Vec<String>,

    /// Target number of partitions for distributed execution.
    #[arg(long, default_value_t = 4)]
    target_partitions: usize,

    /// Approximate bytes assigned to each distributed file-scan task.
    #[arg(long)]
    bytes_per_partition: Option<usize>,

    /// Development only: plan with the single-node planner when the
    /// distributed planner fails to plan a query, or plans one whose stages
    /// cannot be sent to a worker (a scan leaf without a wire form).
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
    allocator::log_active();
    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    cli.memory.install_vortex_decode_budget()?;
    let memory_pool = cli.memory.create_pool();
    if cli.memory_sampling.memory_sample_file.is_some() {
        let pool = Arc::clone(&memory_pool);
        cli.memory_sampling
            .spawn(allocator::stats_reader(), move || pool.reserved())?;
    }
    let mut session_factory = LakeSoulSessionFactory::new_with_memory_pool(
        meta_client,
        &cli.core,
        memory_pool,
    )?;
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

    let printer = Printer::new(cli.format);

    if let Some(cmd) = cli.command {
        return exec_command(cmd, &printer, &ctx).await;
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
