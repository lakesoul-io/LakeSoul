use std::process::{Command, ExitStatus};

const GPRC_VERSION: &str = "1.70";

fn main() {
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

fn gen_py() -> std::io::Result<ExitStatus> {
    let vpy = ".venv/bin/python3";
    let proto_dir = "../rust/proto/src";
    let proto_path = "../rust/proto/src/entity.proto";
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
