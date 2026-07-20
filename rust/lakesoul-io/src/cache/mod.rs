// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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
    use crate::cache::ReadThroughCache;
    use crate::cache::disk_cache::DiskCache;
    use crate::cache::paging::PageCache;
    use crate::cache::stats::{AtomicIntCacheStats, CacheReadStats};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::*;
    use futures::StreamExt;
    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use url::Url;

    fn test_parquet_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    async fn count_rows(ctx: &SessionContext, path: &str) -> usize {
        let df = ctx
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .unwrap();

        let mut stream = df.execute_stream().await.unwrap();
        let mut total_rows = 0usize;
        while let Some(batch) = stream.next().await {
            total_rows += batch.unwrap().num_rows();
        }
        total_rows
    }

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

        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("base-0.parquet"), test_parquet_bytes().into())
            .await
            .unwrap();

        let cache_dir = tempfile::tempdir().unwrap();
        let disk_cache = Arc::new(DiskCache::with_path(
            64 * 1024 * 1024,
            1024,
            cache_dir.path().join("cache"),
        ));
        let stats = Arc::new(AtomicIntCacheStats::new());
        let cache_s3_store = Arc::new(ReadThroughCache::new_with_stats(
            store,
            disk_cache.clone(),
            stats.clone(),
        ));

        let url = Url::parse("s3://lakesoul-test-bucket/").unwrap();
        ctx.runtime_env()
            .register_object_store(&url, cache_s3_store);

        let path = "s3://lakesoul-test-bucket/base-0.parquet";
        assert_eq!(count_rows(&ctx, path).await, 3);
        assert!(disk_cache.size() > 0);
        let misses_after_first_read = stats.total_misses();

        assert_eq!(count_rows(&ctx, path).await, 3);
        assert_eq!(stats.total_misses(), misses_after_first_read);
    }
}
