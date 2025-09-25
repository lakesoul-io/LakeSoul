// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
use pyo3::prelude::*;

mod dataset;
mod metadata;

#[pymodule]
fn _lib(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    metadata::init(py, m)?;
    dataset::init(py, m)?;
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
