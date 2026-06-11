// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PyO3 bindings for lakesoul-vector crate.
//!
//! Exposes `build_shard_vector_index()` to Python — builds an IVF+RaBitQ
//! vector index for a single shard (partition + hash bucket) and persists
//! it to object storage.

use std::collections::HashMap;
use std::sync::Arc;

use lakesoul_vector::Metric;
use lakesoul_vector::{VectorIndexConfig, VectorShardIndexBuilder};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

/// Build a vector index for a single shard (one partition + one hash bucket).
///
/// Reads parquet files, extracts PK + vector columns, builds an IVF+RaBitQ
/// index using two-pass streaming, and persists to object storage under
/// ``index_prefix``.
///
/// Args:
///     store_config: dict with S3 credentials. Keys:
///         ``type`` ("s3" or "local"),
///         ``bucket``, ``region`` (default "us-east-1"),
///         ``access_key_id``, ``secret_access_key``,
///         ``endpoint`` (optional)
///     file_paths: list of parquet file paths for this shard
///     pk_column: name of the u64 primary key column
///     vector_column: name of the vector column (FixedSizeList<Float32>)
///     dim: vector dimension
///     nlist: number of IVF clusters (default 256)
///     total_bits: RaBitQ total bits (default 7)
///     metric: distance metric, "L2" or "IP" (InnerProduct)
///     index_prefix: object store prefix for the index,
///         e.g. ``s3://bucket/table/_vector_index/emb/range=1/0/``
///
/// Returns:
///     "ok" on success, raises RuntimeError on failure
#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn build_shard_vector_index(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    pk_column: String,
    vector_column: String,
    dim: usize,
    nlist: usize,
    total_bits: usize,
    metric: String,
    index_prefix: String,
) -> PyResult<String> {
    let store = create_object_store(&store_config)?;

    let metric = match metric.to_lowercase().as_str() {
        "l2" => Metric::L2,
        "ip" | "innerproduct" => Metric::InnerProduct,
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unknown metric '{}', expected 'L2' or 'IP'",
                other
            )));
        }
    };

    let config = VectorIndexConfig {
        column_name: vector_column,
        dim,
        nlist,
        total_bits,
        metric,
        rotator_type: lakesoul_vector::RotatorType::FhtKacRotator,
        seed: 42,
        use_faster_config: true,
    };

    // Build object_store_options for LakeSoulReader (converted from store_config)
    let object_store_options = store_config.clone();
    let default_fs = store_config
        .get("default_fs")
        .cloned()
        .or_else(|| store_config.get("bucket").map(|b| format!("s3://{}", b)));

    let builder = VectorShardIndexBuilder::new(
        store,
        config,
        file_paths,
        pk_column,
        index_prefix,
        object_store_options,
        default_fs,
    );

    let runtime = Runtime::new().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create tokio runtime: {}",
            e
        ))
    })?;

    runtime.block_on(async move {
        builder
            .build()
            .await
            .map(|_| "ok".to_string())
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index build failed: {:?}",
                    e
                ))
            })
    })
}

/// Create an ObjectStore from a Python configuration dict.
fn create_object_store(config: &HashMap<String, String>) -> PyResult<Arc<dyn ObjectStore>> {
    let store_type = config.get("type").map(|s| s.as_str()).unwrap_or("s3");

    match store_type {
        "s3" => create_s3_store(config),
        "local" | "file" => {
            // Use local filesystem — the paths are file:// URLs
            // We can use the object_store's local filesystem adapter
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "local file store not yet supported, use s3",
            ))
        }
        other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "unknown store type '{}', expected 's3' or 'local'",
            other
        ))),
    }
}

/// Create an S3 ObjectStore from config dict.
fn create_s3_store(config: &HashMap<String, String>) -> PyResult<Arc<dyn ObjectStore>> {
    let region = config
        .get("region")
        .map(|s| s.as_str())
        .unwrap_or("us-east-1");
    let bucket = config.get("bucket").ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>("missing 'bucket' in store_config")
    })?;

    let mut builder = AmazonS3Builder::new()
        .with_region(region)
        .with_bucket_name(bucket)
        .with_allow_http(true);

    if let (Some(k), Some(s)) = (config.get("access_key_id"), config.get("secret_access_key")) {
        builder = builder.with_access_key_id(k).with_secret_access_key(s);
    }

    if let Some(ep) = config.get("endpoint") {
        builder = builder.with_endpoint(ep);
    }

    let store = builder.build().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create S3 object store: {}",
            e
        ))
    })?;

    Ok(Arc::new(store))
}

/// Register the vector submodule.
pub fn init(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    let submodule = PyModule::new(m.py(), "vector")?;
    submodule.add_function(wrap_pyfunction!(build_shard_vector_index, &submodule)?)?;
    m.add_submodule(&submodule)?;
    let full_name = format!("{}.vector", m.name()?);
    crate::install_module(&full_name, &submodule)?;
    Ok(())
}
