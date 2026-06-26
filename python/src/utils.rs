use std::collections::HashSet;

use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use lakesoul_common::ser::arrow_java::schema_from_metadata_str;
use pyo3::{
    Bound, PyResult, Python,
    exceptions::PyRuntimeError,
    pyfunction,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction,
};

use crate::install_module;

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_utils")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._utils", &m)?;
    m.add_function(wrap_pyfunction!(_schema_from_metadata_str, &m)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (schema_json_str, exclude_columns=None))]
fn _schema_from_metadata_str(
    schema_json_str: &str,
    exclude_columns: Option<HashSet<String>>,
) -> PyResult<PyArrowType<Schema>> {
    let schema = schema_from_metadata_str(schema_json_str)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let exclude_columns = exclude_columns.unwrap_or_default();

    let fields = schema
        .fields()
        .iter()
        .filter(|field| !exclude_columns.contains(field.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();

    let new_schema = Schema::new_with_metadata(fields, schema.metadata().clone());

    Ok(PyArrowType(new_schema))
}
