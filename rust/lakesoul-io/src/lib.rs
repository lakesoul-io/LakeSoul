// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul IO Library
//!
//! This library provides functionality for reading and writing data in the LakeSoul format.
//! It includes support for various file formats, optimized reading with primary keys,
//! partitioned table support, and filter pushdown capabilities.
//!
//! # Dependencies
//!
//! The following external crates are re-exported for convenience:
//!
//! - [arrow](https://docs.rs/arrow/latest/arrow/) - Apache Arrow implementation for columnar data
//! - [datafusion](https://docs.rs/datafusion/latest/datafusion/) - Query execution framework
//! - [serde_json](https://docs.rs/serde_json/latest/serde_json/) - JSON serialization/deserialization
//! - [tokio](https://docs.rs/tokio/latest/tokio/) - Asynchronous runtime
//!
//! # Examples
//!
//! ```rust
//! # tokio_test::block_on(async {
//! use lakesoul_io::lakesoul_reader::LakeSoulReader;
//! use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
//!
//! // Configure and create a reader
//! let config = LakeSoulIOConfigBuilder::new()
//!     .with_files(vec!["path/to/file.parquet"])
//!     .with_thread_num(1)
//!     .with_batch_size(256)
//!     .build();
//!
//! let mut reader = LakeSoulReader::new(config)?;
//! reader.start().await?;
//!
//! // Read data
//! while let Some(batch) = reader.next_rb().await {
//!     let record_batch = batch?;
//!     // Process the record batch
//! }
//!})
//! ```
//!
//! # Features
//!
//! - `hdfs` - Enable HDFS support (disabled by default)
//!
//! # Modules
//!
//! - `lakesoul_reader` - Core reading functionality
//! - `lakesoul_writer` - Core writing functionality
//! - `lakesoul_io_config` - Configuration types
//! - `datasource` - Data source implementations
//! - `sorted_merge` - Sorted merge operations
//! - `repartition` - Data repartitioning utilities
//! - `filter` - Filter pushdown support
//! - `helpers` - Utility functions
//! - `hash_utils` - Hash-related utilities
//! - `local_sensitive_hash` - Local sensitive hashing support

#[macro_use]
extern crate tracing;

#[deprecated(since = "3.1.0")]
pub mod async_writer;
pub mod cache;
pub mod config;
pub mod datasource;
pub mod filter;
pub mod hash_utils;
pub mod helpers;
#[deprecated(since = "3.1.0")]
pub mod lakesoul_io_config;
#[deprecated(since = "3.1.0")]
pub mod lakesoul_reader;
#[deprecated(since = "3.1.0")]
pub mod lakesoul_writer;
pub mod local_sensitive_hash;
pub mod reader;
pub mod writer;
// mod projection;
pub mod object_store;
pub mod physical_plan;
pub mod repartition;
pub mod session;
pub mod sorted_merge;

#[cfg(feature = "hdfs")]
mod hdfs;

pub mod constant;
mod default_column_stream;
mod transform;

pub type Result<T> = std::result::Result<T, Report>;

#[doc(inline)]
pub use arrow;
#[doc(inline)]
pub use datafusion::{self, arrow::error::Result as ArrowResult};
use rootcause::Report;
#[doc(inline)]
pub use serde_json;
#[doc(inline)]
pub use tokio;
