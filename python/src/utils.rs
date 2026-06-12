use std::collections::HashSet;

use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use lakesoul_common::ser::arrow_java::schema_from_metadata_str;
use pyo3::{
    Bound, PyResult, Python, pyfunction,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction,
};

use crate::install_module;

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_utils")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._utils", &m)?;
    m.add_function(wrap_pyfunction!(to_arrow_schema, &m)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (schema_json_str, exclude_columns=None))]
fn to_arrow_schema(
    schema_json_str: &str,
    exclude_columns: Option<HashSet<String>>,
) -> PyArrowType<Schema> {
    let schema = schema_from_metadata_str(schema_json_str);
    let exclude_columns = exclude_columns.unwrap_or_default();

    let fields = schema
        .fields()
        .iter()
        .filter(|field| !exclude_columns.contains(field.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();

    let new_schema = Schema::new_with_metadata(fields, schema.metadata().clone());

    PyArrowType(new_schema)
}
