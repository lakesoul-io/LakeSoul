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

use lakesoul_io::vector::builder::VectorShardIndexBuilder;
use lakesoul_vector::{Metric, VectorIndexConfig};
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
            let prefix = config.get("prefix").map(|s| s.as_str()).unwrap_or("/");
            let store: object_store::local::LocalFileSystem =
                object_store::local::LocalFileSystem::new_with_prefix(prefix).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "failed to create local file store: {}",
                        e
                    ))
                })?;
            Ok(Arc::new(store))
        }
        other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "unknown store type '{}', expected 's3' or 'local'",
            other
        ))),
    }
}

/// Create an S3 ObjectStore from config dict and environment variables.
/// Env vars (AWS_ACCESS_KEY_ID, …) take priority over config keys.
/// Config keys use the Hadoop fs.s3a.* naming convention.
fn create_s3_store(config: &HashMap<String, String>) -> PyResult<Arc<dyn ObjectStore>> {
    // Env vars first, config keys second (matches object_store.rs).
    let key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .or_else(|| config.get("fs.s3a.access.key").cloned());
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .or_else(|| config.get("fs.s3a.secret.key").cloned());
    let region = std::env::var("AWS_REGION").ok().or_else(|| {
        std::env::var("AWS_DEFAULT_REGION").ok().or_else(|| {
            config.get("fs.s3a.endpoint.region").cloned()
        })
    });
    let endpoint = std::env::var("AWS_ENDPOINT")
        .ok()
        .or_else(|| config.get("fs.s3a.endpoint").cloned());
    let bucket = config.get("fs.s3a.bucket").cloned().or_else(|| {
        std::env::var("LAKESOUL_S3_BUCKET").ok()
    }).ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "missing 'fs.s3a.bucket' in store_config",
        )
    })?;
    let virtual_hosted = config
        .get("fs.s3a.path.style.access")
        .map(|s| s != "true")
        .unwrap_or(true);

    let mut builder = AmazonS3Builder::new()
        .with_region(region.unwrap_or_else(|| "us-east-1".to_owned()))
        .with_bucket_name(bucket)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(virtual_hosted);

    if let Some(k) = key {
        builder = builder.with_access_key_id(k);
    }
    if let Some(s) = secret {
        builder = builder.with_secret_access_key(s);
    }
    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep);
    }

    let store = builder.build().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create S3 object store: {}", e,
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
