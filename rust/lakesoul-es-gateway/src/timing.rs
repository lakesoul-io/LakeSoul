// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Optional per-phase timing for the write and search paths.
//!
//! Every request already reports an end-to-end `took`; diagnosing where the
//! time goes needs per-phase numbers.  They are emitted at `info` level when
//! `LAKESOUL_ES_GATEWAY_TIMING` is set, so normal deployments stay quiet.

use std::sync::OnceLock;

/// Whether per-phase timing logs are enabled for this process.
pub fn timing_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("LAKESOUL_ES_GATEWAY_TIMING").is_ok())
}
