use std::collections::HashMap;

use clap::Parser;

// 添加命令行参数结构体
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Flight SQL 服务器监听地址
    #[arg(short, long, default_value = "0.0.0.0:50051")]
    pub addr: String,

    /// Prometheus 指标监听地址
    #[arg(short, long, default_value = "0.0.0.0:19000")]
    pub metrics_addr: String,

    /// LakeSoul Meta 文件路径
    #[arg(long, default_value = "")]
    pub lakesoul_home: String,

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

    /// 设置 tokio runtime 的工作线程数
    #[clap(long, default_value = "2")]
    pub worker_threads: usize,

    /// 流写入速率限制
    #[arg(long, default_value = "100.0")]
    pub throughput_limit: String,
}

impl Args {
    pub fn s3_options(&self) -> HashMap<String, String> {
        let mut options = HashMap::new();
        if let Some(s3_bucket) = &self.s3_bucket {
            options.insert("fs.s3a.bucket".to_string(), s3_bucket.to_string());
            options.insert("fs.s3a.path.style.access".to_string(), "true".to_string());
        }
        if let Some(s3_access_key) = &self.s3_access_key {
            options.insert("fs.s3a.access.key".to_string(), s3_access_key.to_string());
        }
        if let Some(s3_secret_key) = &self.s3_secret_key {
            options.insert("fs.s3a.secret.key".to_string(), s3_secret_key.to_string());
        }
        options
    }
}
