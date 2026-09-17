// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Shared index management framework.
//!
//! Every index kind configures itself through a `{kind}_index_columns` table
//! property and is built/garbage collected with the same orchestration:
//! group the committed files by index shard, resolve the shard's current
//! commit from the catalog, decide between delta/rebuild, commit the built
//! segments and optionally sweep superseded files.  This module holds the
//! kind-agnostic parts; each kind keeps its own config parameters, builder
//! invocation and file naming.

pub mod build;
pub mod config;
pub mod gc;
pub mod store;

pub use config::{IndexManagementConfig, IndexTableConfig};
pub use gc::{IndexGcOptions, IndexGcReport};
pub use store::store_for_files;
