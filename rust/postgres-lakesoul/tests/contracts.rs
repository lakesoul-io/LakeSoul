// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Single integration-test binary for the `postgres-lakesoul` contracts.
//!
//! Each contract used to be its own test target. Every target compiled the
//! heavyweight `support` harness, which dominated link time and was
//! non-cacheable (sccache cannot cache executables). Declaring the contracts
//! as modules of one binary compiles the harness once and links once.

mod support;

#[path = "contracts/acl_contract.rs"]
mod acl_contract;

#[path = "contracts/bi_contract.rs"]
mod bi_contract;

#[path = "contracts/catalog_contract.rs"]
mod catalog_contract;

#[path = "contracts/copy_contract.rs"]
mod copy_contract;

#[path = "contracts/ddl_contract.rs"]
mod ddl_contract;

#[path = "contracts/delete_truncate_contract.rs"]
mod delete_truncate_contract;

#[path = "contracts/detached_query_contract.rs"]
mod detached_query_contract;

#[path = "contracts/index_ddl_contract.rs"]
mod index_ddl_contract;

#[path = "contracts/insert_contract.rs"]
mod insert_contract;

#[path = "contracts/ivm_contract.rs"]
mod ivm_contract;

#[path = "contracts/rbac_contract.rs"]
mod rbac_contract;

#[path = "contracts/read_snapshot_contract.rs"]
mod read_snapshot_contract;

#[path = "contracts/session_contract.rs"]
mod session_contract;

#[path = "contracts/snapshot_tag_contract.rs"]
mod snapshot_tag_contract;

#[path = "contracts/sql_compat.rs"]
mod sql_compat;

#[path = "contracts/storage_contract.rs"]
mod storage_contract;

#[path = "contracts/time_travel_contract.rs"]
mod time_travel_contract;

#[path = "contracts/tls_contract.rs"]
mod tls_contract;

#[path = "contracts/tpch_contract.rs"]
mod tpch_contract;

#[path = "contracts/transaction_contract.rs"]
mod transaction_contract;

#[path = "contracts/transaction_lock_contract.rs"]
mod transaction_lock_contract;

#[path = "contracts/type_contract.rs"]
mod type_contract;

#[path = "contracts/update_contract.rs"]
mod update_contract;

#[path = "contracts/upsert_contract.rs"]
mod upsert_contract;

#[path = "contracts/vector_contract.rs"]
mod vector_contract;
