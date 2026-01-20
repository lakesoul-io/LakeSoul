// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

use std::sync::Arc;

use crate::{install_module, metadata::METADATA_CLIENT_REF};
use datafusion_python::context::PySessionContext;
use pyo3::{exceptions::PyValueError, prelude::*};

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_context")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._context", &m)?;
    m.add_function(wrap_pyfunction!(create_lakesoul_session_ctx_internal, &m)?)?;
    Ok(())
}

#[pyfunction]
fn create_lakesoul_session_ctx_internal() -> PyResult<PySessionContext> {
    let args = lakesoul_datafusion::cli::CoreArgs::default();
    let raw = lakesoul_datafusion::create_lakesoul_session_ctx(METADATA_CLIENT_REF.clone(), &args)
        .map_err(|e| PyValueError::new_err(format!("Failed to create session context: {}", e)))?
        .as_ref()
        .clone();

    Ok(PySessionContext { ctx: raw })
}
