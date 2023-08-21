// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

extern crate prost_build;

fn main() {
    #[cfg(target_os = "linux")] 
    {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }
    prost_build::compile_protos(&["src/entity.proto"],
                                &["src/"]).unwrap();
}