// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow_array::RecordBatch;
use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use lakesoul_io::{
    config::LakeSoulIOConfigBuilder,
    file_format::PhysicalFormat,
    writer::{SyncSendableMutableLakeSoulWriter, async_writer::FlushOutput},
};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};

use crate::install_module;

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_writer")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._writer", &m)?;
    m.add_class::<NativeFileInfo>()?;
    m.add_class::<NativeWriter>()?;
    Ok(())
}

#[pyclass(
    module = "lakesoul._lib._writer",
    name = "_NativeFileInfo",
    frozen,
    get_all
)]
#[derive(Clone)]
pub struct NativeFileInfo {
    partition: String,
    path: String,
    size: u64,
    existing_columns: Vec<String>,
    row_count: usize,
    other_info: HashMap<String, String>,
}

impl From<FlushOutput> for NativeFileInfo {
    fn from(output: FlushOutput) -> Self {
        Self {
            partition: output.partition_desc,
            path: output.file_path,
            size: output.object_meta.size,
            existing_columns: output.file_exist_cols,
            row_count: output.row_count,
            other_info: output.other_info,
        }
    }
}

#[pyclass(module = "lakesoul._lib._writer", name = "_NativeWriter")]
pub struct NativeWriter {
    writer: Option<SyncSendableMutableLakeSoulWriter>,
    aborted: bool,
}

#[pymethods]
impl NativeWriter {
    #[new]
    #[pyo3(signature = (
        *,
        path,
        schema,
        format = "vortex-compact",
        primary_keys = Vec::new(),
        partition_by = Vec::new(),
        hash_bucket_num = 1,
        batch_size = 8192,
        thread_num = 1,
        max_file_size = None,
        max_row_group_size = 250_000,
        object_store_options = None,
        options = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python,
        path: String,
        schema: PyArrowType<Schema>,
        format: &str,
        primary_keys: Vec<String>,
        partition_by: Vec<String>,
        hash_bucket_num: usize,
        batch_size: usize,
        thread_num: usize,
        max_file_size: Option<u64>,
        max_row_group_size: usize,
        object_store_options: Option<HashMap<String, String>>,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        validate_config(
            &path,
            &schema.0,
            &primary_keys,
            &partition_by,
            hash_bucket_num,
            batch_size,
            thread_num,
            max_file_size,
            max_row_group_size,
        )?;

        let physical_format = PhysicalFormat::from_str(format)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        let mut builder = LakeSoulIOConfigBuilder::new();

        // requires_partitioning_writer
        if !partition_by.is_empty() || !primary_keys.is_empty() {
            builder = builder.set_dynamic_partition(true);
        }

        builder = builder
            .with_prefix(path)
            .with_schema(Arc::new(schema.0))
            .with_primary_keys(primary_keys)
            .with_range_partitions(partition_by.clone())
            .with_hash_bucket_num(hash_bucket_num.to_string())
            .with_batch_size(batch_size)
            .with_thread_num(thread_num)
            .with_max_row_group_size(max_row_group_size);

        if let Some(max_file_size) = max_file_size {
            builder = builder.with_max_file_size(max_file_size);
        }
        for (key, value) in object_store_options.unwrap_or_default() {
            builder = builder.with_object_store_option(key, value);
        }
        for (key, value) in options.unwrap_or_default() {
            builder = builder.with_option(key, value);
        }
        builder = builder.with_physical_format(physical_format);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(thread_num)
            .build()
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let config = builder.build();
        let writer = py
            .detach(move || SyncSendableMutableLakeSoulWriter::from_io_config(config, runtime))
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

        Ok(Self {
            writer: Some(writer),
            aborted: false,
        })
    }

    fn write(&mut self, py: Python, batch: PyArrowType<RecordBatch>) -> PyResult<usize> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            PyRuntimeError::new_err(if self.aborted {
                "writer has been aborted"
            } else {
                "writer has already been finished"
            })
        })?;
        let row_count = batch.0.num_rows();
        py.detach(|| writer.write_batch(batch.0))
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        Ok(row_count)
    }

    fn finish(&mut self, py: Python) -> PyResult<Vec<NativeFileInfo>> {
        let writer = self.writer.take().ok_or_else(|| {
            PyRuntimeError::new_err(if self.aborted {
                "writer has been aborted"
            } else {
                "writer has already been finished"
            })
        })?;
        let outputs = py
            .detach(move || writer.finish())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        Ok(outputs.into_iter().map(NativeFileInfo::from).collect())
    }

    fn abort(&mut self, py: Python) -> PyResult<()> {
        if let Some(writer) = self.writer.take() {
            py.detach(move || writer.abort_and_close())
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        }
        self.aborted = true;
        Ok(())
    }

    #[getter]
    fn closed(&self) -> bool {
        self.writer.is_none()
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_config(
    path: &str,
    schema: &Schema,
    primary_keys: &[String],
    partition_by: &[String],
    hash_bucket_num: usize,
    batch_size: usize,
    thread_num: usize,
    max_file_size: Option<u64>,
    max_row_group_size: usize,
) -> PyResult<()> {
    if path.is_empty() {
        return Err(PyValueError::new_err("path must not be empty"));
    }
    if hash_bucket_num == 0 {
        return Err(PyValueError::new_err(
            "hash_bucket_num must be greater than zero",
        ));
    }
    if batch_size == 0 {
        return Err(PyValueError::new_err(
            "batch_size must be greater than zero",
        ));
    }
    if thread_num == 0 {
        return Err(PyValueError::new_err(
            "thread_num must be greater than zero",
        ));
    }
    if max_row_group_size == 0 {
        return Err(PyValueError::new_err(
            "max_row_group_size must be greater than zero",
        ));
    }
    if max_file_size.is_some_and(|size| size == 0) {
        return Err(PyValueError::new_err(
            "max_file_size must be greater than zero",
        ));
    }

    for column in primary_keys.iter().chain(partition_by) {
        schema
            .field_with_name(column)
            .map_err(|_| PyValueError::new_err(format!("column not in schema: {column}")))?;
    }
    Ok(())
}
