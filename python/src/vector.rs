// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PyO3 bindings for lakesoul-vector crate.
//!
//! Exposes vector-index configuration parsing and `build_shard_vector_index()`
//! to Python — builds an IVF+RaBitQ vector index for a single shard
//! (partition + hash bucket) and persists it to object storage.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_pyarrow::PyArrowType;
use arrow_schema::Schema;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use lakesoul_common::IndexKind;
use lakesoul_common::misc::SecretMap;
#[cfg(feature = "ci-unify")]
use lakesoul_datafusion::vector_index::{
    DropGcReport, DropVectorIndexOptions, DropVectorIndexReport, VectorIndexTableConfig,
    declare_vector_index as declare_vector_index_impl, drop_vector_index_with_store,
    gc_dropped_vector_index_objects as gc_dropped_objects_impl,
};
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::key::{KeyCodec, KeyLayout};
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_io::vector::builder::VectorShardIndexBuilder;
#[cfg(feature = "ci-unify")]
use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::index_catalog::{CommitMode, IndexCommitView, VectorSegmentEntry};
use lakesoul_vector::{IndexKey, Metric, RotatorType, SegmentEntry, VectorIndexConfig};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use tokio::runtime::Runtime;

/// Parse a table's ``vector_index_columns`` property into a list of config dicts.
///
/// The Rust side is the single source of truth for parsing
/// (``VectorIndexConfig::parse_json``). Each returned dict has keys:
/// ``column``, ``dim``, ``nlist``, ``total_bits``, ``metric``,
/// ``rotator_type``, ``seed``, ``use_faster_config``, ``rebuild_mode``,
/// ``max_delta_ratio``.
///
/// Args:
///     value: JSON (single object or array) from the ``vector_index_columns``
///         table property.
///
/// Returns:
///     List of config dicts. Empty list for empty input.
#[pyfunction]
fn parse_vector_index_configs(py: Python<'_>, value: String) -> PyResult<Vec<Py<PyAny>>> {
    use pyo3::types::PyDict;

    let configs = VectorIndexConfig::parse_json(&value).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "invalid vector_index_columns property: {}",
            e
        ))
    })?;

    let mut result = Vec::with_capacity(configs.len());
    for c in configs {
        let d = PyDict::new(py);
        d.set_item("column", c.column_name)?;
        d.set_item("dim", c.dim)?;
        d.set_item("nlist", c.nlist)?;
        d.set_item("total_bits", c.total_bits)?;
        d.set_item("metric", metric_str(c.metric))?;
        d.set_item("rotator_type", rotator_type_str(c.rotator_type))?;
        d.set_item("seed", c.seed)?;
        d.set_item("use_faster_config", c.use_faster_config)?;
        d.set_item("rebuild_mode", c.rebuild_mode)?;
        d.set_item("max_delta_ratio", c.max_delta_ratio)?;
        result.push(d.into_any().unbind());
    }
    Ok(result)
}

fn metric_str(metric: Metric) -> &'static str {
    match metric {
        Metric::L2 => "L2",
        Metric::InnerProduct => "IP",
    }
}

fn rotator_type_str(rotator: RotatorType) -> &'static str {
    match rotator {
        RotatorType::FhtKacRotator => "FhtKac",
        RotatorType::MatrixRotator => "Matrix",
    }
}

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
///     pk_columns: names of the primary key columns (in key order)
///     vector_column: name of the vector column (FixedSizeList<Float32>)
///     dim: vector dimension
///     nlist: number of IVF clusters (default 256)
///     total_bits: RaBitQ total bits (default 7)
///     metric: distance metric, "L2" or "IP" (InnerProduct)
///     rotator_type: rotation type, "FhtKac" or "Matrix" (default "FhtKac")
///     seed: random seed (default 42)
///     use_faster_config: fast-quantization mode (default True)
///
/// Returns:
///     "ok" on success, raises RuntimeError on failure
#[pyfunction]
#[pyo3(signature = (
    store_config,
    file_paths,
    pk_columns,
    vector_column,
    dim,
    nlist = 256,
    total_bits = 7,
    metric = String::from("L2"),
    rotator_type = String::from("FhtKac"),
    seed = 42,
    use_faster_config = true,
))]
#[allow(clippy::too_many_arguments)]
fn build_shard_vector_index(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    pk_columns: Vec<String>,
    vector_column: String,
    dim: usize,
    nlist: usize,
    total_bits: usize,
    metric: String,
    rotator_type: String,
    seed: u64,
    use_faster_config: bool,
) -> PyResult<String> {
    run_shard_vector_index(
        store_config,
        file_paths,
        pk_columns,
        vector_column,
        dim,
        nlist,
        total_bits,
        metric,
        rotator_type,
        seed,
        use_faster_config,
        false,
    )
}

/// Rebuild a vector index shard from scratch (fresh IVF k-means over all of
/// the shard's data files, published as a new index generation).
///
/// *file_paths* must contain **all** active data files of the shard (base +
/// earlier deltas + the latest batch), not just newly written ones.
///
/// Returns:
///     "ok" on success, raises RuntimeError on failure
#[pyfunction]
#[pyo3(signature = (
    store_config,
    file_paths,
    pk_columns,
    vector_column,
    dim,
    nlist = 256,
    total_bits = 7,
    metric = String::from("L2"),
    rotator_type = String::from("FhtKac"),
    seed = 42,
    use_faster_config = true,
))]
#[allow(clippy::too_many_arguments)]
fn rebuild_shard_vector_index(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    pk_columns: Vec<String>,
    vector_column: String,
    dim: usize,
    nlist: usize,
    total_bits: usize,
    metric: String,
    rotator_type: String,
    seed: u64,
    use_faster_config: bool,
) -> PyResult<String> {
    run_shard_vector_index(
        store_config,
        file_paths,
        pk_columns,
        vector_column,
        dim,
        nlist,
        total_bits,
        metric,
        rotator_type,
        seed,
        use_faster_config,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_shard_vector_index(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    pk_columns: Vec<String>,
    vector_column: String,
    dim: usize,
    nlist: usize,
    total_bits: usize,
    metric: String,
    rotator_type: String,
    seed: u64,
    use_faster_config: bool,
    force_rebuild: bool,
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
    let rotator_type = match rotator_type.to_lowercase().as_str() {
        "fhtkac" | "fht" => RotatorType::FhtKacRotator,
        "matrix" => RotatorType::MatrixRotator,
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unknown rotator_type '{}', expected 'FhtKac' or 'Matrix'",
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
        rotator_type,
        seed,
        use_faster_config,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    };

    // Build object_store_options for LakeSoulReader (converted from store_config)
    let object_store_options = store_config.clone();
    let default_fs = store_config
        .get("default_fs")
        .cloned()
        .or_else(|| store_config.get("bucket").map(|b| format!("s3://{}", b)));

    let index_prefix = shard_index_prefix(&file_paths, IndexKind::Vector, &config.column_name);
    let covered_files = file_paths.clone();
    let builder = VectorShardIndexBuilder::new(
        store.clone(),
        config,
        file_paths,
        pk_columns,
        SecretMap::from(object_store_options),
        default_fs,
    );

    let runtime = Runtime::new().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create tokio runtime: {}",
            e
        ))
    })?;

    runtime.block_on(async move {
        let client = lakesoul_metadata::MetaDataClient::from_env()
            .await
            .map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index build requires metadata access: {error}"
                ))
            })?;
        let catalog = client.vector_index_catalog();
        let resolved = catalog.resolve(&index_prefix).await.map_err(|error| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to resolve vector index at '{index_prefix}': {error}"
            ))
        })?;

        let (mode, builder) = match &resolved {
            Some(view) if !force_rebuild => {
                let resolved = to_resolved_shard(&index_prefix, view).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "failed to serialize vector index segments: {e}"
                    ))
                })?;
                (CommitMode::Delta, builder.with_base(resolved))
            }
            _ => (CommitMode::Rebuild, builder),
        };
        let outcome = if mode == CommitMode::Rebuild {
            builder.rebuild().await
        } else {
            builder.build().await
        }
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "vector index build failed: {:?}",
                e
            ))
        })?;

        if !outcome.new_segments.is_empty() {
            let segments = to_catalog_segments(&outcome.new_segments, &covered_files);
            lakesoul_vector::vector_commit::commit_vector_build(
                &store,
                &catalog,
                &outcome.index_prefix,
                &outcome.header,
                segments,
                outcome.upserted_keys,
                mode,
            )
            .await
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index commit failed: {e}"
                ))
            })?;
        }
        Ok("ok".to_string())
    })
}

fn to_resolved_shard(
    prefix: &str,
    view: &IndexCommitView<VectorSegmentEntry>,
) -> Result<ResolvedIndex, serde_json::Error> {
    let segments: Vec<SegmentEntry> = view
        .segments
        .iter()
        .map(|segment| SegmentEntry {
            cluster_id: segment.cluster_id,
            segment_version: segment.segment_version,
            segment_filename: segment.filename.clone(),
            num_vectors: segment.num_vectors,
            file_size: segment.file_size,
            data_files: segment.data_files.clone(),
            birth_version: segment.birth_version,
        })
        .collect();
    Ok(ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: prefix.to_string(),
        commit_id: view.commit_id,
        generation: view.generation,
        version: view.version,
        header: view.header.clone(),
        segments: serde_json::to_value(&segments)?,
        mutation_head: view.mutation_head.clone(),
        mutation_records: view.mutation_records,
    })
}

fn to_catalog_segments(
    segments: &[SegmentEntry],
    data_files: &[String],
) -> Vec<VectorSegmentEntry> {
    segments
        .iter()
        .map(|segment| VectorSegmentEntry {
            cluster_id: segment.cluster_id,
            segment_version: segment.segment_version,
            filename: segment.segment_filename.clone(),
            num_vectors: segment.num_vectors,
            file_size: segment.file_size,
            data_files: data_files.to_vec(),
            birth_version: segment.birth_version,
        })
        .collect()
}

/// Create an ObjectStore from a Python configuration dict.
pub(crate) fn create_object_store(
    config: &HashMap<String, String>,
) -> PyResult<Arc<dyn ObjectStore>> {
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
        std::env::var("AWS_DEFAULT_REGION")
            .ok()
            .or_else(|| config.get("fs.s3a.endpoint.region").cloned())
    });
    let endpoint = std::env::var("AWS_ENDPOINT")
        .ok()
        .or_else(|| config.get("fs.s3a.endpoint").cloned());
    let bucket = config
        .get("fs.s3a.bucket")
        .cloned()
        .or_else(|| std::env::var("LAKESOUL_S3_BUCKET").ok())
        .ok_or_else(|| {
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
            "failed to create S3 object store: {}",
            e,
        ))
    })?;

    Ok(Arc::new(store))
}

/// Register the vector submodule.
/// Durable vector-index delete of arrow-row-encoded keys.
///
/// Deletes are tombstone commits: no segment file is rewritten and the
/// deleted keys disappear from the next search.  The rows must be deleted
/// from the table data as well, otherwise a later rebuild re-indexes them.
///
/// Args:
///     store_config: object-store credentials (as for the build binding)
///     file_paths: data files of the shard
///     vector_column: indexed vector column (selects the index prefix)
///     keys_b64: base64-encoded arrow-row primary-key bytes
///
/// Returns:
///     Number of keys published (all of them), raises on failure
#[pyfunction]
fn delete_vector_index_keys_base64(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    vector_column: String,
    keys_b64: Vec<String>,
) -> PyResult<usize> {
    if keys_b64.is_empty() {
        return Ok(0);
    }
    let keys: Vec<IndexKey> = keys_b64
        .iter()
        .map(|key| {
            BASE64.decode(key).map(IndexKey::new).map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "invalid base64 index key: {error}"
                ))
            })
        })
        .collect::<PyResult<_>>()?;
    let store = create_object_store(&store_config)?;
    let index_prefix = shard_index_prefix(&file_paths, IndexKind::Vector, &vector_column);
    let runtime = Runtime::new().map_err(|error| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create tokio runtime: {error}"
        ))
    })?;
    let count = keys.len();
    runtime.block_on(async move {
        let client = lakesoul_metadata::MetaDataClient::from_env()
            .await
            .map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index delete requires metadata access: {error}"
                ))
            })?;
        let catalog = client.vector_index_catalog();
        lakesoul_vector::vector_commit::delete_vector_keys(&store, &catalog, &index_prefix, &keys)
            .await
            .map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index delete failed: {error}"
                ))
            })?;
        Ok::<usize, PyErr>(count)
    })
}

/// Encode primary-key rows as base64 arrow-row index keys.
///
/// The batch must contain every primary-key column; its Arrow schema must
/// match the table's schema for the encoded bytes to match the index keys.
#[pyfunction]
fn encode_index_keys_base64(
    schema: PyArrowType<Schema>,
    pk_columns: Vec<String>,
    batch: PyArrowType<RecordBatch>,
) -> PyResult<Vec<String>> {
    let layout = KeyLayout::from_schema(&schema.0, &pk_columns)
        .map_err(|error| PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string()))?;
    let codec = KeyCodec::new(layout)
        .map_err(|error| PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string()))?;
    let keys = codec
        .encode_batch(&batch.0)
        .map_err(|error| PyErr::new::<pyo3::exceptions::PyValueError, _>(error.to_string()))?;
    Ok(keys
        .iter()
        .map(|key| BASE64.encode(key.as_bytes()))
        .collect())
}

/// Drop a vector index for an existing table.
///
/// This is the Python binding of the Rust `drop_vector_index` implementation.
/// It removes the `vector_index_columns` declaration (unless
/// `keep_declaration=True`), deletes the index control-plane rows, optionally
/// deletes the `_vector_index/...` objects, and invalidates the process-local
/// caches.
///
/// Args:
///     store_config: object-store configuration (same as
///         `build_shard_vector_index`; `type` + `fs.s3a.*` keys).
///     table_name: LakeSoul table name.
///     namespace: LakeSoul namespace.
///     columns: columns to drop; `None` means all declared vector columns.
///     partition_desc: drop one partition's index (keeps the declaration).
///     keep_declaration: keep the `vector_index_columns` entry.
///     purge_files: delete `_vector_index/...` objects.
///     dry_run: only report what would be deleted.
///     if_exists: do not fail when the index is absent.
///
/// Returns:
///     Dict with `table_name`, `namespace`, `columns`, `partitions`,
///     `shards_dropped`, `objects_deleted`, `bytes_deleted`,
///     `declaration_removed`, `dry_run`.
#[cfg(feature = "ci-unify")]
#[pyfunction]
#[pyo3(signature = (
    store_config,
    table_name,
    namespace,
    columns = None,
    partition_desc = None,
    keep_declaration = false,
    purge_files = true,
    dry_run = false,
    if_exists = true,
    grace_seconds = 0.0,
    wait_for_cleanup = true,
))]
#[allow(clippy::too_many_arguments)]
fn drop_vector_index(
    py: Python<'_>,
    store_config: HashMap<String, String>,
    table_name: String,
    namespace: String,
    columns: Option<Vec<String>>,
    partition_desc: Option<String>,
    keep_declaration: bool,
    purge_files: bool,
    dry_run: bool,
    if_exists: bool,
    grace_seconds: f64,
    wait_for_cleanup: bool,
) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyDict;

    if !grace_seconds.is_finite() || grace_seconds < 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "grace_seconds must be a finite non-negative number",
        ));
    }

    let store = create_object_store(&store_config)?;
    let runtime = Runtime::new().map_err(|error| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create tokio runtime: {error}"
        ))
    })?;
    let report = runtime.block_on(async {
        let client = MetaDataClient::from_env().await.map_err(|error| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "vector index drop requires metadata access: {error}"
            ))
        })?;
        let options = DropVectorIndexOptions {
            columns,
            partition_desc,
            keep_declaration,
            purge_files,
            dry_run,
            if_exists,
            grace: std::time::Duration::from_secs_f64(grace_seconds),
            wait_for_cleanup,
        };
        drop_vector_index_with_store(&client, store, &table_name, &namespace, options)
            .await
            .map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index drop failed: {error}"
                ))
            })
    })?;
    let dict = PyDict::new(py);
    fill_drop_report(py, &dict, &report)?;
    Ok(dict.into_any().unbind())
}

#[cfg(feature = "ci-unify")]
#[pyfunction]
#[pyo3(signature = (table_name, namespace, configs))]
fn declare_vector_index(table_name: String, namespace: String, configs: String) -> PyResult<()> {
    let configs: Vec<VectorIndexTableConfig> = serde_json::from_str(&configs).map_err(|error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "invalid vector index config JSON: {error}"
        ))
    })?;
    let runtime = Runtime::new().map_err(|error| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create tokio runtime: {error}"
        ))
    })?;
    runtime.block_on(async {
        let client = MetaDataClient::from_env().await.map_err(|error| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "vector index declaration requires metadata access: {error}"
            ))
        })?;
        declare_vector_index_impl(&client, &table_name, &namespace, configs)
            .await
            .map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index declaration failed: {error}"
                ))
            })
    })
}

#[cfg(feature = "ci-unify")]
#[pyfunction]
#[pyo3(signature = (store_config, table_name, namespace))]
fn gc_dropped_vector_index_objects(
    py: Python<'_>,
    store_config: HashMap<String, String>,
    table_name: String,
    namespace: String,
) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyDict;

    let runtime = Runtime::new().map_err(|error| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create tokio runtime: {error}"
        ))
    })?;
    let report = runtime.block_on(async {
        let client = MetaDataClient::from_env().await.map_err(|error| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "vector index GC requires metadata access: {error}"
            ))
        })?;
        let report = gc_dropped_objects_impl(&client, &table_name, &namespace, &store_config)
            .await
            .map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "vector index GC failed: {error}"
                ))
            })?;
        Ok::<_, PyErr>((report, client))
    })?;
    let dict = PyDict::new(py);
    fill_drop_gc_report(py, &dict, &report.0)?;
    Ok(dict.into_any().unbind())
}

#[cfg(feature = "ci-unify")]
fn fill_drop_gc_report(
    py: Python<'_>,
    dict: &Bound<pyo3::types::PyDict>,
    report: &DropGcReport,
) -> PyResult<()> {
    dict.set_item("table_name", &report.table_name)?;
    dict.set_item("namespace", &report.namespace)?;
    dict.set_item("tombstones", report.tombstones)?;
    dict.set_item("objects_deleted", report.objects_deleted)?;
    dict.set_item("bytes_deleted", report.bytes_deleted)?;
    let _ = py;
    Ok(())
}

#[cfg(feature = "ci-unify")]
fn fill_drop_report(
    py: Python<'_>,
    dict: &Bound<pyo3::types::PyDict>,
    report: &DropVectorIndexReport,
) -> PyResult<()> {
    use pyo3::types::PyList;
    dict.set_item("table_name", &report.table_name)?;
    dict.set_item("namespace", &report.namespace)?;
    dict.set_item("columns", PyList::new(py, &report.columns)?)?;
    dict.set_item("partitions", PyList::new(py, &report.partitions)?)?;
    dict.set_item("shards_dropped", report.shards_dropped)?;
    dict.set_item("objects_deleted", report.objects_deleted)?;
    dict.set_item("bytes_deleted", report.bytes_deleted)?;
    dict.set_item("declaration_removed", report.declaration_removed)?;
    dict.set_item("dry_run", report.dry_run)?;
    dict.set_item("epoch", report.epoch)?;
    dict.set_item("drop_id", report.drop_id.as_deref())?;
    dict.set_item("cleanup", &report.cleanup)?;
    Ok(())
}

pub fn init(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    let submodule = PyModule::new(m.py(), "vector")?;
    submodule.add_function(wrap_pyfunction!(build_shard_vector_index, &submodule)?)?;
    submodule.add_function(wrap_pyfunction!(rebuild_shard_vector_index, &submodule)?)?;
    submodule.add_function(wrap_pyfunction!(parse_vector_index_configs, &submodule)?)?;
    submodule.add_function(wrap_pyfunction!(
        delete_vector_index_keys_base64,
        &submodule
    )?)?;
    submodule.add_function(wrap_pyfunction!(encode_index_keys_base64, &submodule)?)?;
    #[cfg(feature = "ci-unify")]
    submodule.add_function(wrap_pyfunction!(drop_vector_index, &submodule)?)?;
    #[cfg(feature = "ci-unify")]
    submodule.add_function(wrap_pyfunction!(declare_vector_index, &submodule)?)?;
    #[cfg(feature = "ci-unify")]
    submodule.add_function(wrap_pyfunction!(
        gc_dropped_vector_index_objects,
        &submodule
    )?)?;
    m.add_submodule(&submodule)?;
    let full_name = format!("{}.vector", m.name()?);
    crate::install_module(&full_name, &submodule)?;
    Ok(())
}
