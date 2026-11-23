// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Focused, PostgreSQL-free tests for the distributed worker discovery and
//! the CLI/environment priority rules.
//!
//! Unlike the crate's lib test harness (`src/tests/mod.rs`, whose `#[ctor]`
//! connects to the metadata database at process start), this integration
//! binary runs without any database: pure EndpointSlice JSON parsing,
//! protocol probing against a local worker gRPC endpoint, a mock Kubernetes
//! API server over plain HTTP, the one-resolver-per-process sharing invariant
//! and subprocess-probe CLI priority checks.

#[path = "discovery_tests/cli.rs"]
mod cli;
#[path = "discovery_tests/resolver.rs"]
mod resolver;
