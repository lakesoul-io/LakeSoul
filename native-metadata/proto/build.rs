// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["src/entity.proto"],
                                &["src/"]).unwrap();
}