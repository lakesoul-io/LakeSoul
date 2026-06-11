// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul 向量索引模块
//!
//! 提供按分片（partition + hash bucket）构建 IVF+RaBitQ 向量索引的能力。
//! 使用分离式后台构建方式：不在写路径中建索引，由 compaction 任务驱动。
//!
//! # 架构
//!
//! ```text
//! VectorShardIndexBuilder
//!   ├─ Pass 1: 读取 parquet → IdAndVecBatch → IvfRabitqBuilder::insert_batch()
//!   │          （水库抽样 nlist×64 个向量）
//!   ├─ Pass 2: builder.build(make_stream)
//!   │          （重新创建流 → K-Means 15轮 + GEMM 分配质心 + RaBitQ 量化）
//!   └─ builder.flush() → 写入对象存储
//! ```

pub mod builder;
pub mod config;
pub mod rabitq;
pub mod reader;

pub use builder::VectorShardIndexBuilder;
pub use config::VectorIndexConfig;

// Re-export key rabitq types for downstream crates (lakesoul-io, python)
pub use rabitq::{
    IdAndVecBatch, IvfRabitqBuilder, IvfRabitqIndex, ManifestStore, Metric, RabitqConfig,
    RabitqError, RotatorType, SearchParams, SearchResult,
};
pub mod vector_search;
