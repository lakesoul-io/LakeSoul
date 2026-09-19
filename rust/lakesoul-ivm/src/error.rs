// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The error type used across the IVM runtime.

/// The IVM result type; errors are carried as [`rootcause::Report`] so any
/// underlying error keeps its context chain.
pub type Result<T, E = rootcause::Report> = std::result::Result<T, E>;
