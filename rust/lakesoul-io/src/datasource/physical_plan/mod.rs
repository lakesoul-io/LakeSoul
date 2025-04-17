// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource::physical_plan] implementation of LakeSoul.

pub use empty_schema::EmptySchemaScanExec;
pub use merge::MergeParquetExec;

pub mod defatul_column;
mod empty_schema;
pub mod merge;

pub mod self_incremental_index_column;
