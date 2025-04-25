// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use lakesoul_datafusion::cli::CoreArgs;

// 添加命令行参数结构体
#[derive(Parser, Debug, Default)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Flight SQL 服务器监听地址
    #[arg(short, long, default_value = "0.0.0.0:50051")]
    pub addr: String,

    /// Prometheus 指标监听地址
    #[arg(short, long, default_value = "0.0.0.0:19000")]
    pub metrics_addr: String,

    /// 流写入速率限制
    #[arg(long, default_value = "100.0")]
    pub throughput_limit: String,

    #[command(flatten)]
    pub core: CoreArgs,
}
