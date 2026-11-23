// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Single integration-test binary for `rust/lakesoul-metadata`.
//!
//! Each suite used to be its own non-cacheable test executable.
//! Declaring them as modules links once and keeps the build cacheable.

#[path = "metadata_contracts/acl_mutation.rs"]
mod acl_mutation;

#[path = "metadata_contracts/acl_resolution.rs"]
mod acl_resolution;

#[path = "metadata_contracts/alter_table_definition.rs"]
mod alter_table_definition;

#[path = "metadata_contracts/commit_occ.rs"]
mod commit_occ;

#[path = "metadata_contracts/data_commit_by_table_id.rs"]
mod data_commit_by_table_id;

#[path = "metadata_contracts/incremental_read.rs"]
mod incremental_read;

#[path = "metadata_contracts/list_tables_filter.rs"]
mod list_tables_filter;

#[path = "metadata_contracts/partition_as_of.rs"]
mod partition_as_of;

#[path = "metadata_contracts/table_changelog.rs"]
mod table_changelog;

#[path = "metadata_contracts/transaction_commit.rs"]
mod transaction_commit;

#[path = "metadata_contracts/update_commit.rs"]
mod update_commit;

#[path = "metadata_contracts/vector_index_catalog.rs"]
mod vector_index_catalog;
