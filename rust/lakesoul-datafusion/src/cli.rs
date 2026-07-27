// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, env};

use clap::{Parser, builder::TypedValueParser};

#[derive(Parser, Debug, Default)]
pub struct CoreArgs {
    /// LakeSoul 数据仓库前缀路径
    #[arg(long)]
    pub warehouse_prefix: Option<String>,

    /// S3 端点
    #[arg(long)]
    pub endpoint: Option<String>,

    /// S3 桶名
    #[arg(long)]
    pub s3_bucket: Option<String>,

    /// S3 访问密钥
    #[arg(long)]
    pub s3_access_key: Option<String>,

    /// S3 密钥
    #[arg(long)]
    pub s3_secret_key: Option<String>,

    #[arg(long)]
    pub s3_virtual_host_style: bool,

    /// 设置 tokio runtime 的工作线程数
    #[clap(long, default_value = "2",  value_parser = clap::value_parser!(u32).range(2..).map(|n| {
        n as usize
        }))]
    pub worker_threads: usize,
}

impl CoreArgs {
    pub fn s3_options(&self) -> HashMap<String, String> {
        let mut options = HashMap::new();
        if let Some(s3_bucket) = &self.s3_bucket {
            options.insert("fs.s3a.bucket".to_string(), s3_bucket.to_string());
        }
        if let Some(s3_access_key) = &self.s3_access_key {
            options.insert("fs.s3a.access.key".to_string(), s3_access_key.to_string());
        }
        if let Some(s3_secret_key) = &self.s3_secret_key {
            options.insert("fs.s3a.secret.key".to_string(), s3_secret_key.to_string());
        }

        // default use virtual path style
        if self.s3_virtual_host_style {
            options.insert("fs.s3a.path.style.access".to_string(), "false".to_string());
        }

        options
    }

    pub fn from_env() -> Self {
        Self {
            warehouse_prefix: env::var("LAKESOUL_WAREHOUSE").ok(),
            endpoint: env::var("AWS_ENDPOINT").ok(),
            s3_bucket: env::var("AWS_BUCKET").ok(),
            s3_access_key: env::var("AWS_ACCESS_KEY_ID").ok(),
            s3_secret_key: env::var("AWS_SECRET_ACCESS_KEY").ok(),
            s3_virtual_host_style: env::var("AWS_S3_VIRTUAL_HOST_STYLE")
                .ok()
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false),
            worker_threads: 2,
        }
    }
}
