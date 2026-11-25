// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! File metadata shared by compaction discovery and execution.

use lakesoul_metadata::transfusion::DataFileInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionFile {
    pub file: DataFileInfo,
    pub level: u32,
}
