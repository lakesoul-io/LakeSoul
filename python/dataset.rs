// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use crate::install_module;
use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use lakesoul_io::{arrow::array::RecordBatchReader, lakesoul_io_config::LakeSoulIOConfigBuilder};
use pyo3::{ffi::PyFrameObject, prelude::*, types::PyList};

#[pyfunction]
fn double(x: usize) -> usize {
    x * 2
}

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_dataset")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._dataset", &m)?;
    m.add_class::<PyArrowDataset>()?;
    m.add_function(wrap_pyfunction!(double, &m)?)?;
    Ok(())
}

#[pyclass(name = "ArrowDataSet", module = "_dataset")]
pub struct PyArrowDataset {
    batch_size: usize,
    thread_num: usize,
    target_schema: Schema,
    file_urls: Vec<Vec<String>>,
    primary_keys: Vec<Vec<String>>,
    partition_info: Vec<(String, String)>,
    oss_conf: Vec<(String, String)>,
    builder: LakeSoulIOConfigBuilder,
}

#[pymethods]
impl PyArrowDataset {
    #[new]
    fn new(
        batch_size: usize,
        thread_num: usize,
        schema: PyArrowType<Schema>,
        file_urls: Vec<Vec<String>>,
        primary_keys: Vec<Vec<String>>,
        partition_info: Vec<(String, String)>,
        oss_conf: Vec<(String, String)>,
    ) -> Self {
        let target_schema = schema.0;
        Self {
            batch_size,
            thread_num,
            target_schema,
            file_urls,
            primary_keys,
            partition_info,
            oss_conf,
            builder: LakeSoulIOConfigBuilder::default(),
        }
    }

    fn to_reader(&self) -> PyArrowType<Box<dyn RecordBatchReader + Send>> {
        todo!()
    }

    fn fragments(&self) -> Vec<PyArrowFragment> {
        todo!()
    }

    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.target_schema.clone())
    }
}

#[pyclass(name = "ArrowFrament", module = "_dataset")]
pub struct PyArrowFragment {
    builder: LakeSoulIOConfigBuilder,
}

impl PyArrowFragment {
    fn new() -> Self {
        todo!()
    }
}

#[pymethods]
impl PyArrowFragment {
    fn to_reader(&self) -> PyArrowType<Box<dyn RecordBatchReader + Send>> {
        todo!()
    }
}
