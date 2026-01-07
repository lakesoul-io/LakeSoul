// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod merge;
pub mod repartition;
pub mod self_incremental_index_column;

mod datasource {
    // todo!("Implement the datasource module")
    // mod parquet;
}

#[allow(dead_code)]
pub(crate) mod empty_schema;

mod api {
    pub use super::merge::MergeParquetExec;
    pub use super::repartition::RepartitionByRangeAndHashExec;
    pub use super::self_incremental_index_column::SelfIncrementalIndexColumnExec;
}

pub use api::*;
