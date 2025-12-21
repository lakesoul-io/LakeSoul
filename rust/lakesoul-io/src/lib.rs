// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul IO Library
//!
//! This library provides functionality for reading and writing data in the LakeSoul format.
//! It includes support for various file formats, optimized reading with primary keys,
//! partitioned table support, and filter pushdown capabilities.
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

use rootcause::Report;

#[macro_use]
extern crate tracing;

pub type Result<T, E = Report> = std::result::Result<T, E>;

pub mod cache;
pub mod config;
pub mod constant;
// pub mod datasource;
// mod default_column_stream;
pub mod filter;
// pub mod hash_utils;
pub mod file_format;
#[cfg(feature = "hdfs")]
mod hdfs;
pub mod helpers;
pub mod local_sensitive_hash;
pub mod object_store;
pub mod physical_plan;
pub mod reader;

// pub mod repartition;
pub mod session;
// pub mod sorted_merge;
mod stream;
pub mod utils;
pub mod writer;
