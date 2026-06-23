// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::str::FromStr;

use pyo3::prelude::*;
use rootcause::Report;

mod metadata;
mod reader;
mod utils;
mod writer;

type Result<T, E = Report> = std::result::Result<T, E>;

#[pymodule]
fn _lib(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    let level_str = std::env::var("RUST_LOG").unwrap_or_else(|_| "off".to_string());
    let level = log::LevelFilter::from_str(&level_str).unwrap_or(log::LevelFilter::Off);

    pyo3_log::Logger::default()
        .filter(level) // trace may cause deadlocks
        .install()
        .unwrap();
    metadata::init(py, m)?;
    reader::init(py, m)?;
    writer::init(py, m)?;
    utils::init(py, m)?;
    Ok(())
}

/// Initialize a module and add it to `sys.modules`.
///
/// Without this, it's not possible to use native submodules as "packages". For example:
///
/// ```pycon
/// >>> from lakesoul._lib.dataset import sync_reader  # This fails
/// ModuleNotFoundError: No module named 'lakesoul._lib.dataset'; 'lakesoul._lib' is not a package
/// ```
///
/// After this, we can import submodules both as modules:
///
/// ```pycon
/// >>> from lakesoul._lib import dataset
/// ```
///
/// And have direct import access to functions and classes in the submodule:
///
/// ```pycon
/// >>> from lakesoul._lib.dataset import sync_reader
/// ```
///
/// See <https://github.com/PyO3/pyo3/issues/759#issuecomment-1811992321>.
pub fn install_module(name: &str, module: &Bound<PyModule>) -> PyResult<()> {
    module
        .py()
        .import("sys")?
        .getattr("modules")?
        .set_item(name, module)?;
    // needs to be set *after* `add_submodule()`
    module.setattr("__name__", name)?;
    Ok(())
}
