use std::process::Command;

fn main() {
    let vpy = ".venv/bin/python3";
    let proto_dir = "rust/proto/src";
    let proto_path = "rust/proto/src/entity.proto";
    let out = "python/src/lakesoul/metadata/generated";
    // ignore error
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
        .unwrap();
}
