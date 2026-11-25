// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Coordinator state and the complete input for a single partition version.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use lakesoul_metadata_proto::entity::{PartitionInfo, TableInfo};

use super::compaction::CompactionFile;
use super::compaction_discovery::{DiscoveryConfig, register_mirror_functions};

#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    pub table: TableInfo,
    pub partition: PartitionInfo,
    pub active_files: Vec<CompactionFile>,
}

pub struct CompactionCoordinator {
    pub(super) metadata_session: Arc<SessionContext>,
    pub(super) config: DiscoveryConfig,
}

impl CompactionCoordinator {
    pub fn new(metadata_session: Arc<SessionContext>, config: DiscoveryConfig) -> Self {
        register_mirror_functions(&metadata_session);
        Self {
            metadata_session,
            config,
        }
    }
}
