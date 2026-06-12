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
//! ```no_run
//! # fn main() -> lakesoul_io::Result<()> {
//! # tokio_test::block_on(async {
//! use lakesoul_io::config::LakeSoulIOConfig;
//! use lakesoul_io::reader::LakeSoulReader;
//!
//! let config = LakeSoulIOConfig::builder()
//!     .with_file("path/to/file.parquet")
//!     .with_thread_num(1)
//!     .with_batch_size(256)
//!     .build();
//!
//! let mut reader = LakeSoulReader::new(config)?;
//! reader.start().await?;
//!
//! while let Some(batch) = reader.next_rb().await {
//!     let record_batch = batch?;
//!     let _row_count = record_batch.num_rows();
//! }
//! # Ok::<(), rootcause::Report>(())
//! # })?;
//! # Ok(())
//! # }
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

pub mod mem;
mod stream;
