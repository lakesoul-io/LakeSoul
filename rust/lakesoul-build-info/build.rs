// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=LAKESOUL_GIT_COMMIT");
    emit_git_rerun_paths();

    let git_commit = env::var("LAKESOUL_GIT_COMMIT")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(git_commit);
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned());
    let version =
        env::var("CARGO_PKG_VERSION").expect("Cargo must set CARGO_PKG_VERSION");
    let build_info = format!(
        "LakeSoul {version} (commit {git_commit}, target {target}, profile {profile})"
    );

    println!("cargo:rustc-env=LAKESOUL_GIT_COMMIT={git_commit}");
    println!("cargo:rustc-env=LAKESOUL_BUILD_TARGET={target}");
    println!("cargo:rustc-env=LAKESOUL_BUILD_PROFILE={profile}");
    println!("cargo:rustc-env=LAKESOUL_BUILD_INFO={build_info}");
}

fn emit_git_rerun_paths() {
    if let Some(head_path) = git_output(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");
    }
    if let Some(reference) = git_output(&["symbolic-ref", "-q", "HEAD"])
        && let Some(reference_path) = git_output(&["rev-parse", "--git-path", &reference])
    {
        println!("cargo:rerun-if-changed={reference_path}");
    }
}

fn git_commit() -> String {
    git_output(&["rev-parse", "--short=12", "HEAD"])
        .unwrap_or_else(|| "unknown".to_owned())
}

fn git_output(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|output| output.trim().to_owned())
        .filter(|output| !output.is_empty())
}
