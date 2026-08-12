// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul product version and native build provenance.

use std::fmt;

/// LakeSoul Core version inherited from the Cargo workspace.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Source commit supplied through `LAKESOUL_GIT_COMMIT` or detected by `git`.
pub const GIT_COMMIT: &str = env!("LAKESOUL_GIT_COMMIT");
/// Rust target triple used for this build.
pub const TARGET: &str = env!("LAKESOUL_BUILD_TARGET");
/// Cargo build profile used for this build.
pub const PROFILE: &str = env!("LAKESOUL_BUILD_PROFILE");
/// Human-readable build identity embedded in native binaries that reference it.
pub const BUILD_INFO: &str = env!("LAKESOUL_BUILD_INFO");
/// NUL-terminated Core version for native FFI exports.
pub const VERSION_NUL: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
/// NUL-terminated build identity for native FFI exports.
pub const BUILD_INFO_NUL: &[u8] = concat!(env!("LAKESOUL_BUILD_INFO"), "\0").as_bytes();

/// Structured LakeSoul build identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildInfo {
    pub version: &'static str,
    pub git_commit: &'static str,
    pub target: &'static str,
    pub profile: &'static str,
}

/// Build identity for the current LakeSoul binary.
pub const BUILD: BuildInfo = BuildInfo {
    version: VERSION,
    git_commit: GIT_COMMIT,
    target: TARGET,
    profile: PROFILE,
};

impl fmt::Display for BuildInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LakeSoul {} (commit {}, target {}, profile {})",
            self.version, self.git_commit, self.target, self.profile
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_contains_all_fields() {
        assert!(BUILD_INFO.contains(VERSION));
        assert!(BUILD_INFO.contains(GIT_COMMIT));
        assert!(BUILD_INFO.contains(TARGET));
        assert!(BUILD_INFO.contains(PROFILE));
        assert_eq!(BUILD.to_string(), BUILD_INFO);
        assert_eq!(VERSION_NUL.last(), Some(&0));
        assert_eq!(BUILD_INFO_NUL.last(), Some(&0));
        assert!(!VERSION_NUL[..VERSION_NUL.len() - 1].contains(&0));
        assert!(!BUILD_INFO_NUL[..BUILD_INFO_NUL.len() - 1].contains(&0));
    }
}
