// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! 按分片构建向量索引的构建器。
//!
//! 使用 rabitq-rs IvfRabitqBuilder 双遍流式 API：
//!   Pass 1: 读取数据文件 → insert_batch（水库抽样）
//!   Pass 2: builder.build(make_stream) → K-Means + 全量量化
//!   Flush: builder.flush() → 写入对象存储
//!
//! 不缓存全量向量到内存，每遍独立读取数据文件。

use std::collections::HashMap;
use std::sync::Arc;

use crate::rabitq::{IdAndVecBatch, IvfRabitqBuilder, ManifestStore, RabitqError};
use futures::StreamExt;
use object_store::ObjectStore;
use tracing::{info, warn};

use lakesoul_io::config::LakeSoulIOConfigBuilder;

use super::config::VectorIndexConfig;
use super::reader::extract_vector_batch;

/// 为单个分片（partition + bucket）构建向量索引。
///
/// 使用 rabitq-rs `IvfRabitqBuilder` 双遍流式构建：
/// - **Pass 1**: 读取所有数据文件，提取 PK+向量，调用 `insert_batch()` 进行水库抽样
/// - **Pass 2**: `build(make_stream)` 闭包重新创建流，执行 K-Means + 全量量化
/// - **Persist**: `flush()` 写入 manifest + segment 到对象存储
///
/// # 全量 vs 增量
///
/// - 如果 `index_prefix` 下不存在 manifest → 全量构建（Fresh 模式，两遍读取）
/// - 如果 `index_prefix` 下存在 manifest → 增量构建（Loaded 模式，单遍读取追加）
pub struct VectorShardIndexBuilder {
    /// 对象存储客户端（用于 rabitq-rs 索引 I/O）
    store: Arc<dyn ObjectStore>,
    /// 向量索引配置
    config: VectorIndexConfig,
    /// 该分片的数据文件列表（parquet 文件的完整路径或 URL）
    file_paths: Vec<String>,
    /// PK 列名（必须是 u64 或 i64 类型）
    pk_column: String,
    /// 索引在对象存储中的前缀路径
    /// 例: `s3://bucket/table/_vector_index/emb/range=1/0/`
    index_prefix: String,
    /// 对象存储配置选项（如 AWS 凭证），用于 LakeSoulReader 读取 parquet
    object_store_options: HashMap<String, String>,
    /// 默认文件系统（如 `s3://bucket` 或 `file:///`）
    default_fs: Option<String>,
}

impl VectorShardIndexBuilder {
    /// 创建新的分片索引构建器。
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: VectorIndexConfig,
        file_paths: Vec<String>,
        pk_column: String,
        index_prefix: String,
        object_store_options: HashMap<String, String>,
        default_fs: Option<String>,
    ) -> Self {
        Self {
            store,
            config,
            file_paths,
            pk_column,
            index_prefix,
            object_store_options,
            default_fs,
        }
    }

    /// 执行完整的索引构建流程。
    ///
    /// 1. 检查 index_prefix 下是否有 manifest
    /// 2. 全量或增量构建
    /// 3. flush 到对象存储
    pub async fn build(self) -> Result<(), RabitqError> {
        let mstore = ManifestStore::new(self.store.clone(), self.index_prefix.clone());
        let need_fresh = !crate::rabitq::manifest::manifest_exists(&mstore).await;

        if need_fresh {
            info!(
                "Building fresh vector index for column '{}' at '{}' ({} files)",
                self.config.column_name,
                self.index_prefix,
                self.file_paths.len()
            );
            self.build_fresh(mstore).await
        } else {
            info!(
                "Incrementally updating vector index for column '{}' at '{}' ({} files)",
                self.config.column_name,
                self.index_prefix,
                self.file_paths.len()
            );
            self.build_loaded(mstore).await
        }
    }

    /// Fresh mode: offline training, no store needed.
    async fn build_fresh(self, mstore: ManifestStore) -> Result<(), RabitqError> {
        let mut builder = IvfRabitqBuilder::new(
            self.config.dim,
            self.config.nlist,
            self.config.total_bits,
            self.config.metric,
            self.config.rotator_type,
            self.config.seed,
            self.config.use_faster_config,
        );

        // 2. Pass 1: 水库抽样
        info!(
            "Pass 1: reservoir sampling (nlist={}, capacity={})",
            self.config.nlist,
            self.config.nlist * 64
        );
        let mut stream = self.create_error_stream();
        let mut seen = 0usize;
        while let Some(batch_result) = stream.next().await {
            match batch_result {
                Ok(batch) => {
                    let n = batch.ids.len();
                    seen += n;
                    builder.insert_batch(batch)?;
                }
                Err(e) => {
                    warn!("Error reading data during Pass 1: {}", e);
                }
            }
        }
        info!("Pass 1 complete: {} vectors sampled", seen);

        if seen == 0 {
            return Err(RabitqError::InvalidPersistence(
                "no vectors found in data files",
            ));
        }

        // 3. Pass 2: builder.build(make_stream)
        //    闭包重新创建流，内部执行 K-Means + 全量量化
        info!("Pass 2: K-Means training + streaming quantization");

        // Pass 2 uses a fresh stream: create via channel + std::thread::spawn
        let make_stream = {
            let object_store_options = self.object_store_options.clone();
            let default_fs = self.default_fs.clone();
            let file_paths = self.file_paths.clone();
            let pk_column = self.pk_column.clone();
            let column_name = self.config.column_name.clone();
            let dim = self.config.dim;
            move || {
                let (tx, rx) = tokio::sync::mpsc::channel::<IdAndVecBatch>(16);
                let opts = object_store_options.clone();
                let dfs = default_fs.clone();
                let fps = file_paths.clone();
                let pk = pk_column.clone();
                let vc = column_name.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("reader runtime");
                    rt.block_on(async move {
                        for fp in &fps {
                            let mut b =
                                LakeSoulIOConfigBuilder::new_with_object_store_options(
                                    opts.clone(),
                                )
                                .with_files(vec![fp.clone()])
                                .with_batch_size(8192);
                            if let Some(ref fs) = dfs {
                                b = b.with_prefix(fs.clone());
                            }
                            let cfg = b.build();
                            let mut r =
                                match lakesoul_io::reader::LakeSoulReader::new(cfg) {
                                    Ok(r) => r,
                                    Err(_) => continue,
                                };
                            if r.start().await.is_err() {
                                continue;
                            }
                            while let Some(batch_result) = r.next_rb().await {
                                if let Ok(batch) = batch_result {
                                    if batch.num_rows() > 0 {
                                        if let Ok(idv) =
                                            extract_vector_batch(&batch, &pk, &vc, dim)
                                        {
                                            if tx.blocking_send(idv).is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                });
                tokio_stream::wrappers::ReceiverStream::new(rx)
            }
        };

        let _index = builder.build(make_stream).await?;

        // 4. 持久化到对象存储
        info!("Flushing index to object store...");
        _index.save_to_v4(&mstore).await.map_err(|e| {
            warn!("Failed to save fresh index: {:?}", e);
            e
        })?;
        info!("Fresh vector index build complete");

        Ok(())
    }

    /// Incremental: load index → insert batches → flush delta
    async fn build_loaded(self, mstore: ManifestStore) -> Result<(), RabitqError> {
        let mut builder = IvfRabitqBuilder::load(
            &mstore,
            self.config.dim,
            self.config.nlist,
            self.config.total_bits,
            self.config.metric,
            self.config.rotator_type,
            self.config.seed,
            self.config.use_faster_config,
        )
        .await?;

        info!("Reading data and inserting into loaded index...");
        let mut stream = self.create_error_stream();
        let mut inserted = 0usize;
        while let Some(batch_result) = stream.next().await {
            match batch_result {
                Ok(batch) => {
                    let n = batch.ids.len();
                    inserted += n;
                    builder.insert_batch(batch)?;
                }
                Err(e) => {
                    warn!("Error reading data during incremental insert: {}", e);
                }
            }
        }
        info!("Inserted {} vectors", inserted);

        if inserted == 0 {
            info!("No new vectors to insert, skipping flush");
            return Ok(());
        }

        info!("Flushing delta segments...");
        let _index = builder.flush(&mstore).await?;
        info!("Incremental vector index update complete");

        Ok(())
    }

    /// 创建返回 `Result<IdAndVecBatch>` 的流（用于 Pass 1，支持错误传播）。
    fn create_error_stream(
        &self,
    ) -> std::pin::Pin<
        Box<dyn futures::Stream<Item = lakesoul_io::Result<IdAndVecBatch>> + Send>,
    > {
        let (tx, rx) =
            tokio::sync::mpsc::channel::<lakesoul_io::Result<IdAndVecBatch>>(16);
        let object_store_options = self.object_store_options.clone();
        let default_fs = self.default_fs.clone();
        let file_paths = self.file_paths.clone();
        let pk_column = self.pk_column.clone();
        let vector_column = self.config.column_name.clone();
        let dim = self.config.dim;

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("reader thread runtime");
            rt.block_on(async move {
                for file_path in &file_paths {
                    let mut builder =
                        LakeSoulIOConfigBuilder::new_with_object_store_options(
                            object_store_options.clone(),
                        )
                        .with_files(vec![file_path.clone()])
                        .with_batch_size(8192);
                    if let Some(ref fs) = default_fs {
                        builder = builder.with_prefix(fs.clone());
                    }
                    let io_config = builder.build();
                    let mut reader =
                        match lakesoul_io::reader::LakeSoulReader::new(io_config) {
                            Ok(r) => r,
                            Err(e) => {
                                let _ = tx.blocking_send(Err(e));
                                continue;
                            }
                        };
                    if let Err(e) = reader.start().await {
                        let _ = tx.blocking_send(Err(e));
                        continue;
                    }
                    while let Some(batch_result) = reader.next_rb().await {
                        let batch = match batch_result {
                            Ok(b) => b,
                            Err(e) => {
                                let _ = tx.blocking_send(Err(e));
                                continue;
                            }
                        };
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        match extract_vector_batch(
                            &batch,
                            &pk_column,
                            &vector_column,
                            dim,
                        ) {
                            Ok(id_vec) => {
                                if tx.blocking_send(Ok(id_vec)).is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                let _ = tx.blocking_send(Err(e));
                            }
                        }
                    }
                }
            });
        });

        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_empty_stream() {
        let (tx, rx) =
            tokio::sync::mpsc::channel::<lakesoul_io::Result<IdAndVecBatch>>(1);
        drop(tx); // close channel immediately
        let stream: std::pin::Pin<Box<dyn futures::Stream<Item = _> + Send>> =
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        use futures::StreamExt;
        let items: Vec<_> = stream.collect().await;
        assert!(items.is_empty());
    }
}
