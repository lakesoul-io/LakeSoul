// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

extern crate prost_build;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }
    prost_build::compile_protos(&["src/entity.proto"], &["src/"])?;
    Ok(())
}
