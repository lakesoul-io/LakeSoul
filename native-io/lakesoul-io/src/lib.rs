/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#![feature(new_uninit)]
#![feature(get_mut_unchecked)]
#![feature(io_error_more)]
#![feature(sync_unsafe_cell)]

pub mod lakesoul_reader;
pub mod filter;
pub mod lakesoul_writer;
pub mod lakesoul_io_config;
pub use datafusion::arrow::error::Result;
pub mod sorted_merge;

#[cfg(feature = "hdfs")]
mod hdfs;

pub mod default_column_stream;
pub mod constant;
