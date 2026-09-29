// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

extern crate cbindgen;

use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = env::var("CARGO_MANIFEST_DIR")?;

    // The bindings depend on this crate's sources only. Declare them explicitly: without any
    // `rerun-if-changed` cargo falls back to watching the whole package directory, which
    // includes the header written below -- so this script, this crate and every consumer of
    // it would be rebuilt on every single cargo invocation.
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=Cargo.toml");

    let bindings = cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_include_guard("LAKESOUL_C_BINDINGS_H")
        .with_include("stddef.h")
        .with_namespace("lakesoul")
        .with_after_include("\nnamespace lakesoul {\ntypedef ptrdiff_t c_ptrdiff_t;\ntypedef size_t c_size_t;\n}")
        .generate()
        .expect("Unable to generate bindings");

    // CResult and CStatus are defined in lakesoul-common::ffi and re-exported.
    // cbindgen cannot follow pub-use re-exports from dependency crates,
    // so we inject the template definitions here.
    let extra = r#"
template<typename OpaqueT>
struct CResult;

template<>
struct CResult<void> {
    void *ptr;
    const char *err;
};

template<typename OpaqueT>
struct CResult {
    OpaqueT *ptr;
    const char *err;
};

struct CStatus {
    const char *err;
    int32_t status;
};
"#;

    let mut header = Vec::new();
    bindings.write(&mut header);
    let mut header = String::from_utf8(header)?;
    // Inject extra types after the namespace opening brace
    header = header.replacen(
        "namespace lakesoul {\ntypedef ptrdiff_t c_ptrdiff_t;",
        &format!(
            "namespace lakesoul {{{}\ntypedef ptrdiff_t c_ptrdiff_t;",
            extra
        ),
        1,
    );

    let header_path = format!("{}/lakesoul_c_bindings.h", crate_dir);
    // Only rewrite when the contents actually change, so the mtime does not churn and the
    // generated file stays quiet for editors, diffs and the tooling watching this tree.
    if std::fs::read_to_string(&header_path).unwrap_or_default() != header {
        std::fs::write(&header_path, &header)?;
    }

    // Smoke test: verify the generated header compiles as C++
    match std::process::Command::new("c++")
        .args(["-std=c++17", "-fsyntax-only", "-x", "c++", &header_path])
        .status()
    {
        Ok(status) if status.success() => {
            println!("cargo:warning=Header smoke test passed");
        }
        Ok(status) => {
            println!(
                "cargo:warning=Header smoke test FAILED. Run: c++ -std=c++17 -fsyntax-only -x c++ {}",
                header_path
            );
            return Err(
                format!("Header smoke test failed with exit code {status}").into()
            );
        }
        Err(e) => {
            println!("cargo:warning=Header smoke test skipped (c++ not found): {e}");
        }
    }

    Ok(())
}
