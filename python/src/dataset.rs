// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use crate::install_module;
use arrow_array::RecordBatch;
use arrow_pyarrow::PyArrowType;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use futures::{StreamExt, stream::SelectAll};
use lakesoul_io::{
    arrow::array::RecordBatchReader,
    datafusion::{error::DataFusionError, execution::SendableRecordBatchStream},
    lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder},
    lakesoul_reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader},
};
use pyo3::{exceptions::PyRuntimeError, prelude::*};

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_dataset")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._dataset", &m)?;
    m.add_function(wrap_pyfunction!(sync_reader, &m)?)?;
    m.add_function(wrap_pyfunction!(one_reader, &m)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (batch_size,thread_num,schema,file_urls,primary_keys,partition_info,oss_conf,partition_schema=None,filter = None))]
fn sync_reader(
    batch_size: usize,
    thread_num: usize,
    schema: PyArrowType<Schema>,
    file_urls: Vec<String>,
    primary_keys: Vec<String>,
    partition_info: Vec<(String, String)>,
    oss_conf: Vec<(String, String)>,
    partition_schema: Option<PyArrowType<Schema>>,
    filter: Option<Vec<u8>>,
) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
    let schema = Arc::new(schema.0);
    let partition_schema = partition_schema.map(|s| Arc::new(s.0));
    let config = build_io_config(
        batch_size,
        thread_num,
        schema,
        partition_schema,
        file_urls,
        primary_keys,
        &partition_info,
        &oss_conf,
        filter,
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(thread_num)
        .build()?;

    let reader = LakeSoulReader::new(config).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
    reader
        .start_blocked()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyArrowType(Box::new(reader)))
}

#[pyfunction]
#[pyo3(signature = (batch_size,thread_num,schema,file_urls,primary_keys,partition_info,oss_conf,partition_schema=None,filter=None))]
fn one_reader(
    batch_size: usize,
    thread_num: usize,
    schema: PyArrowType<Schema>,
    file_urls: Vec<Vec<String>>,
    primary_keys: Vec<Vec<String>>,
    partition_info: Vec<(String, String)>,
    oss_conf: Vec<(String, String)>,
    partition_schema: Option<PyArrowType<Schema>>,
    filter: Option<Vec<u8>>,
) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
    let schema = Arc::new(schema.0);
    let partition_schema = partition_schema.map(|s| Arc::new(s.0));
    let readers = file_urls
        .into_iter()
        .zip(primary_keys.into_iter())
        .map(|(files, pks)| {
            LakeSoulReader::new(build_io_config(
                batch_size,
                thread_num,
                schema.clone(),
                partition_schema.clone(),
                files,
                pks,
                &partition_info,
                &oss_conf,
                filter.clone(),
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
        .collect::<PyResult<Vec<LakeSoulReader>>>()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(thread_num)
        .build()?;
    let one = OneReader::try_new(schema, readers, runtime)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(PyArrowType(Box::new(one)))
}

fn build_io_config(
    batch_size: usize,
    thread_num: usize,
    schema: SchemaRef,
    partition_schema: Option<SchemaRef>,
    file_urls: Vec<String>,
    primary_keys: Vec<String>,
    partition_info: &[(String, String)],
    oss_conf: &[(String, String)],
    filter: Option<Vec<u8>>,
) -> LakeSoulIOConfig {
    let mut builder = LakeSoulIOConfigBuilder::default()
        .with_batch_size(batch_size)
        .with_thread_num(thread_num)
        .with_files(file_urls)
        .with_primary_keys(primary_keys)
        .with_schema(Arc::clone(&schema));

    for (k, v) in partition_info {
        builder = builder.with_default_column_value(k.clone(), v.clone());
    }

    if let Some(p_schema) = partition_schema {
        builder = builder.with_partition_schema(Arc::clone(&p_schema));
    }

    let mut has_path_style_config = false;

    for (k, v) in oss_conf {
        if k == "fs.s3a.path.style.access" {
            has_path_style_config = true;
        }
        builder = builder.with_object_store_option(k, v);
    }
    if !has_path_style_config {
        // if this config is not specified by user, we always set it to true
        builder = builder.with_object_store_option("fs.s3a.path.style.access", "true");
    }

    if let Some(buf) = filter {
        builder = builder.with_filter_buf(buf);
    }

    builder.build()
}

struct OneReader {
    schema: SchemaRef,
    runtime: Arc<tokio::runtime::Runtime>,
    stream: SelectAll<SendableRecordBatchStream>,
}

impl OneReader {
    fn try_new(
        schema: SchemaRef,
        mut readers: Vec<LakeSoulReader>,
        runtime: tokio::runtime::Runtime,
    ) -> Result<Self, DataFusionError> {
        let mut ss = vec![];
        for reader in readers.iter_mut() {
            // start reader
            runtime.block_on(reader.start())?;

            if let Some(s) = reader.stream() {
                ss.push(s);
            }
        }
        let s = futures::stream::select_all(ss);
        Ok(Self {
            schema,
            // readers,
            runtime: Arc::new(runtime),
            stream: s,
        })
    }
}

impl Iterator for OneReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime.block_on(self.stream.next()).map(|res| {
            res.map_err(|e| match e {
                DataFusionError::ArrowError(arrow_error, _) => arrow_error,
                _ => ArrowError::ExternalError(Box::new(e)),
            })
        })
    }
}

impl RecordBatchReader for OneReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}
