use std::process::{Command, ExitStatus};

const GPRC_VERSION: &str = "1.70";

fn main() {
    emit_build_info();

    // first time for uv
    if gen_py().is_err() {
        println!("cargo::warning=try using sys python to gen proto");
        Command::new("python3")
            .args(["-m", "venv", ".venv"])
            .status()
            .unwrap();
        Command::new(".venv/bin/pip")
            .args([
                "install",
                format!("grpcio-tools=={}", GPRC_VERSION).as_str(),
            ])
            .status()
            .unwrap();
        gen_py().unwrap();
        return;
    }
    println!("cargo::warning=try using uv's python to gen proto");
}

fn emit_build_info() {
    println!("cargo:rerun-if-env-changed=LAKESOUL_GIT_COMMIT");
    emit_git_rerun_paths();

    let git_commit = std::env::var("LAKESOUL_GIT_COMMIT")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .or_else(|| git_output(&["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_owned());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned());
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned());
    let version = std::env::var("CARGO_PKG_VERSION").expect("Cargo must set CARGO_PKG_VERSION");
    let build_info = format!(
        "LakeSoul Python extension {version} (commit {git_commit}, target {target}, profile {profile})"
    );

    println!("cargo:rustc-env=LAKESOUL_PYTHON_BUILD_INFO={build_info}");
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

fn gen_py() -> std::io::Result<ExitStatus> {
    let vpy = ".venv/bin/python3";
    let proto_dir = "../rust/lakesoul-metadata-proto/src";
    let proto_path = "../rust/lakesoul-metadata-proto/src/entity.proto";
    let out = "src/lakesoul/metadata/generated";

    Command::new(vpy)
        .args([
            "-m",
            "grpc.tools.protoc",
            &format!("-I={}", proto_dir),
            &format!("--python_out={}", out),
            &format!("--pyi_out={}", out),
            proto_path,
        ])
        .status()
}
