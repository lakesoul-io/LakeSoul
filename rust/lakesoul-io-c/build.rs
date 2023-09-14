extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_include_guard("LAKESOUL_C_BINDINGS_H")
        .with_include("stddef.h")
        .with_namespace("lakesoul")
        .with_after_include("\nnamespace lakesoul {\ntypedef ptrdiff_t c_ptrdiff_t;\ntypedef size_t c_size_t;\n}")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("lakesoul_c_bindings.h");
}