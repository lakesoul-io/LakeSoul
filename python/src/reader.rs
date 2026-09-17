// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2025 LakeSoul contributors
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_pyarrow::PyArrowType;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use datafusion_common::DataFusionError;
use datafusion_execution::SendableRecordBatchStream;
use futures::{StreamExt, stream::SelectAll};
use lakesoul_common::IndexKind;
use lakesoul_io::{
    config::LakeSoulIOConfigBuilder,
    index::IndexLease,
    index::commit::ResolvedIndex,
    reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader},
};
use lakesoul_metadata::index_catalog::VectorCatalog;
use lakesoul_vector::SegmentEntry;
use pyo3::{exceptions::PyRuntimeError, prelude::*};

use crate::Result;
use crate::install_module;

pub(crate) fn init(py: Python, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_reader")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._reader", &m)?;
    m.add_function(wrap_pyfunction!(_sync_reader, &m)?)?;
    m.add_function(wrap_pyfunction!(_one_reader, &m)?)?;
    Ok(())
}

/// Process-wide index catalog (PostgreSQL); `None` when the metadata
/// database is not reachable, in which case vector search falls back to the
/// reader's normal "no index" behavior.
static CATALOG: tokio::sync::OnceCell<Option<VectorCatalog>> = tokio::sync::OnceCell::const_new();

async fn vector_catalog() -> Option<VectorCatalog> {
    CATALOG
        .get_or_init(|| async {
            match lakesoul_metadata::MetaDataClient::from_env().await {
                Ok(client) => Some(client.vector_index_catalog()),
                Err(error) => {
                    log::warn!("vector index catalog unavailable: {error}");
                    None
                }
            }
        })
        .await
        .clone()
}

/// Resolve the index commits of the shards behind `file_urls` and hold
/// reader leases for them, so the native reader can search without any
/// catalog access of its own.
async fn resolve_vector_shards(
    file_urls: &[String],
    options: &Option<Vec<(String, String)>>,
) -> (Vec<ResolvedIndex>, Vec<Arc<IndexLease>>) {
    let Some(catalog) = vector_catalog().await else {
        return (Vec::new(), Vec::new());
    };
    crate::index::resolve_index_shards(IndexKind::Vector, &catalog, file_urls, options, |view| {
        let segments: Vec<SegmentEntry> = view
            .segments
            .iter()
            .map(|segment| SegmentEntry {
                cluster_id: segment.cluster_id,
                segment_version: segment.segment_version,
                segment_filename: segment.filename.clone(),
                num_vectors: segment.num_vectors,
                file_size: segment.file_size,
            })
            .collect();
        serde_json::to_value(&segments).ok()
    })
    .await
}

#[pyfunction]
#[pyo3(signature = (batch_size,thread_num,schema,file_urls,primary_keys,partition_info,oss_conf,partition_schema=None,filter=None,options=None))]
fn _sync_reader(
    batch_size: usize,
    thread_num: usize,
    schema: PyArrowType<Schema>,
    file_urls: Vec<String>,
    primary_keys: Vec<String>,
    partition_info: Vec<(String, String)>,
    oss_conf: Vec<(String, String)>,
    partition_schema: Option<PyArrowType<Schema>>,
    filter: Option<Vec<u8>>,
    options: Option<Vec<(String, String)>>,
) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
    let schema = Arc::new(schema.0);
    let partition_schema = partition_schema.map(|s| Arc::new(s.0));
    let builder = build_io_config_builder(
        batch_size,
        thread_num,
        schema,
        partition_schema,
        file_urls.clone(),
        primary_keys,
        &partition_info,
        &oss_conf,
        filter,
        options.clone(),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(thread_num)
        .build()?;

    let (shards, leases) = runtime.block_on(resolve_vector_shards(&file_urls, &options));
    let config = builder
        .with_resolved_index_shards(shards)
        .with_index_leases(leases)
        .build();

    let reader = LakeSoulReader::new(config).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
    reader
        .start_blocked()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyArrowType(Box::new(reader)))
}

#[pyfunction]
#[pyo3(signature = (batch_size,thread_num,schema,file_urls,primary_keys,partition_info,oss_conf,partition_schema=None,filter=None,options=None))]
fn _one_reader(
    batch_size: usize,
    thread_num: usize,
    schema: PyArrowType<Schema>,
    file_urls: Vec<Vec<String>>,
    primary_keys: Vec<Vec<String>>,
    partition_info: Vec<Vec<(String, String)>>,
    oss_conf: Vec<(String, String)>,
    partition_schema: Option<PyArrowType<Schema>>,
    filter: Option<Vec<u8>>,
    options: Option<Vec<(String, String)>>,
) -> PyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
    let schema = Arc::new(schema.0);
    log::debug!("schema: {:?}", schema);
    let partition_schema = partition_schema.map(|s| Arc::new(s.0));
    if file_urls.len() != primary_keys.len() || file_urls.len() != partition_info.len() {
        return Err(PyRuntimeError::new_err(format!(
            "file_urls, primary_keys and partition_info must have the same length, got {}, {} and {}",
            file_urls.len(),
            primary_keys.len(),
            partition_info.len()
        )));
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(thread_num)
        .build()?;

    let readers = file_urls
        .into_iter()
        .zip(primary_keys)
        .zip(partition_info)
        .map(|((files, pks), part_info)| {
            let builder = build_io_config_builder(
                batch_size,
                thread_num,
                schema.clone(),
                partition_schema.clone(),
                files.clone(),
                pks,
                &part_info,
                &oss_conf,
                filter.clone(),
                options.clone(),
            );
            let (shards, leases) = runtime.block_on(resolve_vector_shards(&files, &options));
            let config = builder
                .with_resolved_index_shards(shards)
                .with_index_leases(leases)
                .build();
            LakeSoulReader::new(config).map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
        .collect::<PyResult<Vec<LakeSoulReader>>>()?;

    let one = OneReader::try_new(schema, readers, runtime)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(PyArrowType(Box::new(one)))
}

#[allow(clippy::too_many_arguments)]
fn build_io_config_builder(
    batch_size: usize,
    thread_num: usize,
    schema: SchemaRef,
    partition_schema: Option<SchemaRef>,
    file_urls: Vec<String>,
    primary_keys: Vec<String>,
    partition_info: &[(String, String)],
    oss_conf: &[(String, String)],
    filter: Option<Vec<u8>>,
    options: Option<Vec<(String, String)>>,
) -> LakeSoulIOConfigBuilder {
    // Derive prefix from the first file's parent directory.
    // Preserve URL scheme + authority for S3 paths; std::path::Path
    // would strip the authority (e.g. s3://bucket → s3:/bucket).
    let prefix = file_urls
        .first()
        .map(|u| crate::index::derive_prefix_from_url(u))
        .unwrap_or_default();

    let mut builder = LakeSoulIOConfigBuilder::default()
        .with_batch_size(batch_size)
        .with_thread_num(thread_num)
        .with_files(file_urls)
        .with_primary_keys(primary_keys)
        .with_schema(Arc::clone(&schema))
        .with_prefix(prefix);

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

    if let Some(opts) = options {
        for (k, v) in opts {
            builder = builder.with_option(k, v);
        }
    }

    builder
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
    ) -> Result<Self> {
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
                DataFusionError::ArrowError(arrow_error, _) => *arrow_error,
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
