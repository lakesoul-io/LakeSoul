use crate::cache::disk_cache::DiskCache;
use std::sync::{Arc, OnceLock};

pub mod disk_cache;
pub mod paging;
pub mod read_through;
pub mod stats;
pub use read_through::ReadThroughCache;

static LAKESOUL_CACHE: OnceLock<Arc<DiskCache>> = OnceLock::new();

/// Get and init Lakesoul Cache
pub(crate) fn get_lakesoul_cache() -> Arc<DiskCache> {
    LAKESOUL_CACHE
        .get_or_init(|| -> Arc<DiskCache> {
            let cache_size = {
                match std::env::var("LAKESOUL_CACHE_SIZE") {
                    Ok(mut s) => {
                        info!("LAKESOUL_CACHE_SIZE: {}", s);
                        match s.split_off(s.len() - 3).as_str() {
                            "KiB" => s.parse::<usize>().unwrap_or(1) * 1024,
                            "MiB" => s.parse::<usize>().unwrap_or(1) * 1024 * 1024,
                            "GiB" => {
                                info!("LAKESOUL_CACHE_SIZE: {}", s);
                                s.parse::<usize>().unwrap_or(1) * 1024 * 1024 * 1024
                            }
                            "TiB" => {
                                s.parse::<usize>().unwrap_or(1)
                                    * 1024
                                    * 1024
                                    * 1024
                                    * 1024
                            }
                            _ => {
                                info!("LAKESOUL_CACHE_SIZE: {}", s);
                                1024 * 1024 * 1024
                            }
                        }
                    }
                    Err(_) => 1024 * 1024 * 1024,
                }
            };
            info!("LAKESOUL_CACHE_SIZE: {}", cache_size);
            Arc::new(DiskCache::new(cache_size, 4 * 1024 * 1024))
        })
        .clone()
}

#[cfg(test)]
pub mod test {
    use datafusion::error::DataFusionError;
    // use datafusion::execution::context::SessionState;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::*;
    use futures::StreamExt;
    use object_store::aws;
    use std::sync::Arc;
    use std::time::Instant;
    use url::Url;

    // "--warehouse-prefix",
    // "s3://lakesoul-bucket/flight-test",
    // "--endpoint",
    // "http://localhost:9000",
    // "--s3-bucket",
    // "lakesoul-test-bucket",
    // "--s3-access-key",
    // "minioadmin1",
    // "--s3-secret-key",
    // "minioadmin1",
    // (flavor = "multi_thread", worker_threads = 10)
    #[tokio::test]
    async fn test_local_s3_cache() {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.execution.coalesce_batches", false)
            .set_u64("datafusion.execution.batch_size", 4096)
            .with_parquet_pruning(true)
            .with_repartition_joins(false);
        let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .build();
        let ctx = SessionContext::new_with_state(state);

        let s3 = aws::AmazonS3Builder::new()
            .with_bucket_name("lakesoul-test-bucket")
            .with_access_key_id("minioadmin1")
            .with_secret_access_key("minioadmin1")
            .with_allow_http(true)
            .with_endpoint("http://localhost:9000")
            .with_region("cn-southwest-2")
            .build()
            .unwrap();
        let url = Url::parse("s3://lakesoul-test-bucket/")
            .map_err(|e| DataFusionError::External(Box::new(e)))
            .unwrap();

        // 注册 S3 缓存存储
        // let cache = Arc::new(DiskCache::new( 1 * 1024 * 1024 * 1024,4 * 1024 * 1024));
        // let cache_s3_store = Arc::new(ReadThroughCache::new(Arc::new(s3), cache));
        // ctx.runtime_env().register_object_store(&url, cache_s3_store);

        ctx.runtime_env().register_object_store(&url, Arc::new(s3));

        {
            let df = ctx
                .read_parquet(
                    "s3://lakesoul-test-bucket/base-0.parquet",
                    ParquetReadOptions::default(),
                )
                .await
                .unwrap();

            let start = Instant::now();
            // let df = df.select_columns(&["uuid", "hostname", "requests"])?;

            let mut stream = df.execute_stream().await.unwrap();
            let mut total_rows = 0usize;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                total_rows += batch.num_rows();
            }

            let duration = start.elapsed();
            println!(
                "Total rows {}, Time elapsed in expensive_function() is: {:?}",
                total_rows, duration
            );
        }

        {
            let df = ctx
                .read_parquet(
                    "s3://lakesoul-test-bucket/base-0.parquet",
                    ParquetReadOptions::default(),
                )
                .await
                .unwrap();

            let start = Instant::now();
            // let df = df.select_columns(&["uuid", "hostname", "requests"])?;

            let mut stream = df.execute_stream().await.unwrap();
            let mut total_rows = 0usize;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                total_rows += batch.num_rows();
            }

            let duration = start.elapsed();

            println!(
                "Total rows {}, Time elapsed in expensive_function() is: {:?}",
                total_rows, duration
            );
        }
    }

    #[test]
    fn test_env() {
        use std::env;
        // for (key, value) in env::vars_os() {
        //     println!("{:?}: {:?}", key, value);
        // }
        let lakesoul_cache_env_value = env::var("LAKESOUL_CACHE").unwrap();
        println!("LAKESOUL_CACHE: {:?}", lakesoul_cache_env_value);
        let _lakesoul_cache_size_env_value = env::var("LAKESOUL_CACHE_SIZE")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        // println!("LAKESOUL_CACHE_SIZE: {}", lakesoul_cache_size_env_value);
    }
}
