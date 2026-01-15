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
//! - `cache` - Object store cache impl
//! - `config` - Configuration types
//! - `constant` - constants used for lakesoul
//! - `file_format` - datafusion file format impl
//! - `filter` - Filter pushdown support
//! - `helpers` - Helper functions for physical plan construction and execution
//! - `local_sensitive_hash` - Local sensitive hashing support
//! - `object_store` - obejct store configuration
//! - `physical_plan` - Physical Plan implementations
//! - `reader` - Core reading functionality
//! - `session` - dafusion session impl
//! - `utils` -  Common utilities
//! - `writer` - Core writing functionality

#[macro_use]
extern crate tracing;

pub type Result<T, E = rootcause::Report> = std::result::Result<T, E>;

#[cfg(feature = "hdfs")]
mod hdfs;

pub mod cache;
pub mod config;
pub mod constant;
pub mod file_format;
pub mod filter;
pub mod helpers;
pub mod local_sensitive_hash;
pub mod object_store;
pub mod physical_plan;
pub mod reader;
pub mod session;
pub mod utils;
pub mod writer;

mod mem;
mod stream;
