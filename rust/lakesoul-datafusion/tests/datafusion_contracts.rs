// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Single integration-test binary for `lakesoul-datafusion`.
//!
//! Each suite used to be its own non-cacheable test executable. Declaring
//! them as modules links once and keeps the shared dependency build small.

#[path = "datafusion_contracts/discovery_tests.rs"]
mod discovery_tests;

#[path = "datafusion_contracts/session_config_env.rs"]
mod session_config_env;
