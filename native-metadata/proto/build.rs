extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["src/entity.proto"],
                                &["src/"]).unwrap();
}