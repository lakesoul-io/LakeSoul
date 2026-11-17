// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul 向量索引模块
//!
//! 包含 vendored rabitq-rs IVF+RaBitQ 核心实现，以及向量索引配置。
//! 构建器和检索功能在 `lakesoul-io` crate 中。

pub mod cache_config;
pub mod config;
pub mod rabitq;
/// Catalog publication of vector commits (requires the `catalog` feature).
#[cfg(feature = "catalog")]
pub mod vector_commit;

pub use cache_config::{LazyMode, VectorCacheConfig, config as cache_config};
pub use config::VectorIndexConfig;

// Re-export key rabitq types for downstream crates
pub use rabitq::{
    IdAndVecBatch, IndexHeader, IndexKey, IndexStore, IvfRabitqBuilder, IvfRabitqIndex,
    LazyIvfIndex, Metric, RabitqConfig, RabitqError, RotatorType, SearchDiagnostics,
    SearchParams, SearchResult, SegmentEntry,
};
