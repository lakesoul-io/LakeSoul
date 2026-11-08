// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! PyO3 binding for a LakeSoul query session.
//!
//! Every vector-search interface the Python package offers is a query on this
//! session: `table.search()` builds the same `ORDER BY <distance> LIMIT k`
//! statement a user would write in SQL, so the Python path runs through the
//! exact planner rules, physical operators and merge-on-read semantics the
//! Rust engine uses (including all vector-index fast paths).  The session is a
//! read-only client: it owns a DataFusion `SessionContext`, a metadata client
//! and the index/payload/metadata caches, and never writes table data, index
//! commits or metadata.
//!
//! This module is gated behind the `ci-unify` feature because it links the
//! `lakesoul-datafusion` crate: build the extension with
//! `maturin develop --features ci-unify` to expose it.  (A dedicated feature
//! name is planned; the code is identical.)

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_pyarrow::PyArrowType;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::create_lakesoul_session_config;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::vector_batch::VectorBatchRequest;
use lakesoul_datafusion::create_lakesoul_session_ctx_with_config;
use lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchConfig;
use lakesoul_io::session::GLOBAL_RUNTIME;
use lakesoul_metadata::MetaDataClient;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::install_module;

/// A LakeSoul query session: one DataFusion context over the LakeSoul catalog.
///
/// Use it for repeated queries (it keeps the index, payload and metadata
/// caches warm) and for any lakehouse SQL, including vector search:
///
/// ```python
/// from lakesoul import LakeSoulSession
///
/// with LakeSoulSession.from_env() as session:
///     rows = session.sql(
///         'SELECT id FROM "lakesoul".default.docs '
///         'ORDER BY array_distance(embedding, ARRAY[0.1, 0.2]) LIMIT 10'
///     )
/// ```
///
/// Args:
///     metadata_config: PostgreSQL connection string in the native
///         `host=... dbname=... user=... password=...` form (the same value
///         `lakesoul.metadata.NativeMetadataClient` accepts).  When omitted
///         the `LAKESOUL_PG_URL`/`LAKESOUL_PG_USERNAME`/`LAKESOUL_PG_PASSWORD`
///         environment variables are used.
///     secondary_config: Optional read-replica connection string.
///     max_retry: Metadata statement retry budget.
///     config: Optional session settings dict:
///
///         - ``warehouse_prefix`` (str): table-root prefix handed to the reader;
///         - ``target_partitions`` (int): DataFusion target partitions;
///         - ``object_store_options`` (dict): subset of the S3 options
///           (``fs.s3a.endpoint``/``bucket``/``access.key``/``secret.key``/
///           ``path.style.access``);
///         - ``vector_search`` (dict): defaults for
///           ``lakesoul_vector_search.*`` (``nprobe``, ``rerank``,
///           ``parallelism``, ``deferred_payload``, ``exact_overfetch``).
#[pyclass(module = "lakesoul._lib._session", name = "NativeSession")]
pub struct NativeSession {
    ctx: Arc<SessionContext>,
}

#[pymethods]
impl NativeSession {
    #[new]
    #[pyo3(signature = (
        metadata_config = None,
        secondary_config = None,
        max_retry = 3,
        config = None
    ))]
    fn new(
        py: Python<'_>,
        metadata_config: Option<String>,
        secondary_config: Option<String>,
        max_retry: usize,
        config: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let meta_client = py
            .detach(move || {
                GLOBAL_RUNTIME.block_on(async move {
                    match metadata_config {
                        Some(config) => {
                            MetaDataClient::from_config_and_max_retry(
                                config,
                                secondary_config,
                                max_retry,
                            )
                            .await
                        }
                        None => MetaDataClient::from_env().await,
                    }
                })
            })
            .map_err(|error| {
                PyRuntimeError::new_err(format!(
                    "failed to create the LakeSoul metadata client: {error}"
                ))
            })?;

        let mut args = CoreArgs::from_env();
        let mut vector_search = LakeSoulVectorSearchConfig::default();
        let mut target_partitions: Option<usize> = None;
        if let Some(config) = config.as_ref() {
            if let Some(value) = config_string(config, "warehouse_prefix")? {
                args.warehouse_prefix = Some(value);
            }
            if let Some(value) = config_usize(config, "target_partitions")? {
                if value == 0 {
                    return Err(PyValueError::new_err(
                        "target_partitions must be >= 1",
                    ));
                }
                target_partitions = Some(value);
            }
            if let Some(options) = config.get_item("object_store_options")? {
                let options = options
                    .downcast::<PyDict>()
                    .map_err(|_| PyValueError::new_err("object_store_options must be a dict"))?;
                for (key, value) in options.iter() {
                    let key: String = key.extract()?;
                    let value: String = value.extract()?;
                    match key.as_str() {
                        "fs.s3a.endpoint" => args.endpoint = Some(value),
                        "fs.s3a.bucket" => args.s3_bucket = Some(value),
                        "fs.s3a.access.key" => args.s3_access_key = Some(value),
                        "fs.s3a.secret.key" => args.s3_secret_key = Some(value),
                        "fs.s3a.path.style.access" => {
                            args.s3_virtual_host_style =
                                !value.eq_ignore_ascii_case("true");
                        }
                        // Other options are not part of the reader's S3
                        // configuration surface; ignoring them keeps the
                        // session usable while `target_partitions`/caches are
                        // configured through their dedicated settings.
                        _ => {}
                    }
                }
            }
            if let Some(options) = config.get_item("vector_search")? {
                let options = options
                    .downcast::<PyDict>()
                    .map_err(|_| PyValueError::new_err("vector_search must be a dict"))?;
                for (key, value) in options.iter() {
                    let key: String = key.extract()?;
                    match key.as_str() {
                        "nprobe" => vector_search.nprobe = value.extract()?,
                        "rerank" => vector_search.rerank = value.extract()?,
                        "parallelism" => vector_search.parallelism = value.extract()?,
                        "deferred_payload" => {
                            vector_search.deferred_payload = value.extract()?
                        }
                        "exact_overfetch" => {
                            vector_search.exact_overfetch = value.extract()?
                        }
                        other => {
                            return Err(PyValueError::new_err(format!(
                                "unknown vector_search option {other:?}; \
                                 expected one of nprobe, rerank, parallelism, \
                                 deferred_payload, exact_overfetch"
                            )));
                        }
                    }
                }
            }
        }

        let mut session_config = create_lakesoul_session_config().map_err(|error| {
            PyRuntimeError::new_err(format!(
                "failed to build the DataFusion session config: {error}"
            ))
        })?;
        session_config
            .options_mut()
            .extensions
            .insert(vector_search);
        if let Some(partitions) = target_partitions {
            session_config = session_config.with_target_partitions(partitions);
        }
        let ctx = create_lakesoul_session_ctx_with_config(
            Arc::new(meta_client),
            &args,
            session_config,
        )
        .map_err(|error| {
            PyRuntimeError::new_err(format!(
                "failed to create the LakeSoul query session: {error}"
            ))
        })?;
        Ok(Self { ctx })
    }

    /// Execute one SQL statement and return the result as pyarrow record batches.
    ///
    /// The statement runs on the DataFusion engine with the LakeSoul catalog,
    /// so it can combine lakehouse SQL (filters, joins, aggregates, window
    /// functions) with vector search (`ORDER BY array_distance(...) LIMIT k`).
    /// The GIL is released while the query runs.
    fn sql(
        &self,
        py: Python<'_>,
        sql: String,
    ) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
        self.collect_arrow(py, &sql)
            .map(|batches| batches.into_iter().map(PyArrowType).collect())
    }

    /// Render `EXPLAIN` / `EXPLAIN ANALYZE` for a statement.
    ///
    /// Use it to confirm that a vector search was pushed down: the plan should
    /// contain `LakeSoulVectorSearchExec(vec=..., metric=..., order_by=...)`.
    /// When `analyze` is set the statement is executed to collect per-operator
    /// metrics, so side-effect-free queries only.
    #[pyo3(signature = (sql, verbose = true, analyze = false))]
    fn explain(
        &self,
        py: Python<'_>,
        sql: String,
        verbose: bool,
        analyze: bool,
    ) -> PyResult<String> {
        let prefix = if analyze {
            "EXPLAIN ANALYZE "
        } else if verbose {
            "EXPLAIN VERBOSE "
        } else {
            "EXPLAIN "
        };
        let batches = self.collect_arrow(py, &format!("{prefix}{sql}"))?;
        pretty_format_batches(&batches)
            .map(|table| table.to_string())
            .map_err(|error| {
                PyRuntimeError::new_err(format!("failed to format the plan: {error}"))
            })
    }

    /// Update the session's `lakesoul_vector_search.*` options.
    ///
    /// The values take effect for the following statements on this session
    /// (nearest-statement semantics, like `SET` in SQL):
    ///
    /// ```python
    /// session.set_vector_search(rerank="index", nprobe=16)
    /// ```
    fn set_vector_search(
        &self,
        py: Python<'_>,
        options: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        for (key, value) in options.iter() {
            let key: String = key.extract()?;
            let literal = if let Ok(flag) = value.extract::<bool>() {
                flag.to_string()
            } else if let Ok(number) = value.extract::<i64>() {
                number.to_string()
            } else {
                let text: String = value.extract()?;
                format!("'{}'", text.replace('\'', "''"))
            };
            self.collect_arrow(
                py,
                &format!("SET lakesoul_vector_search.{key} = {literal}"),
            )?;
        }
        Ok(())
    }

    /// The current `nprobe` of the session (checked by the Python wrapper).
    #[getter]
    fn nprobe(&self) -> usize {
        self.ctx
            .state()
            .config_options()
            .extensions
            .get::<LakeSoulVectorSearchConfig>()
            .map(|options| options.nprobe)
            .unwrap_or(64)
    }

    /// Run the snapshot batch entry: many query vectors, one snapshot.
    ///
    /// Returns one list of primary keys per query.  This is the fast batch
    /// path (it resolves each bucket's index shard once for the whole call and
    /// runs the index-only or exact ranking natively).  It follows the same
    /// snapshot and merge-on-read semantics as the SQL path.
    ///
    /// The native batch entry supports a single Int64 primary key named
    /// ``id``; the Python wrapper falls back to per-query SQL for other key
    /// layouts.  A query vector may be any length the index accepts (the
    /// index dimension is enforced by the engine).
    #[pyo3(signature = (
        table,
        namespace,
        column,
        queries,
        top_k = 10,
        nprobe = 64,
        parallelism = 1,
        batch_size = 0,
        exact = true,
        exact_overfetch = 10,
        metric = "L2".to_string()
    ))]
    #[allow(clippy::too_many_arguments)]
    fn vector_batch_search(
        &self,
        py: Python<'_>,
        table: String,
        namespace: String,
        column: String,
        queries: Vec<Vec<f32>>,
        top_k: usize,
        nprobe: usize,
        parallelism: usize,
        batch_size: usize,
        exact: bool,
        exact_overfetch: usize,
        metric: String,
    ) -> PyResult<Vec<Vec<i64>>> {
        if top_k == 0 {
            return Err(PyValueError::new_err("top_k must be >= 1"));
        }
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let metric = if metric.eq_ignore_ascii_case("L2")
            || metric.eq_ignore_ascii_case("IP")
            || metric.eq_ignore_ascii_case("INNERPRODUCT")
        {
            if metric.eq_ignore_ascii_case("L2") {
                "L2".to_string()
            } else {
                "IP".to_string()
            }
        } else {
            return Err(PyValueError::new_err(format!(
                "metric must be 'L2' or 'IP', got {metric:?}"
            )));
        };
        let ctx = Arc::clone(&self.ctx);
        py.detach(move || {
            GLOBAL_RUNTIME.block_on(async move {
                let catalog = ctx
                    .catalog("lakesoul")
                    .ok_or_else(|| "the lakesoul catalog is not registered".to_string())?;
                let schema = catalog.schema(&namespace).ok_or_else(|| {
                    format!("the namespace {namespace:?} is not registered")
                })?;
                let provider = schema
                    .table(&table)
                    .await
                    .map_err(|error| {
                        format!(
                            "failed to resolve the table {namespace}.{table}: {error}"
                        )
                    })?
                    .ok_or_else(|| {
                        format!("the table {namespace}.{table} is not registered")
                    })?;
                let provider = provider
                    .downcast_ref::<LakeSoulTableProvider>()
                    .ok_or_else(|| {
                        format!("{namespace}.{table} is not a LakeSoul table")
                    })?;
                let request = VectorBatchRequest {
                    column,
                    queries,
                    top_k,
                    nprobe: nprobe.max(1),
                    parallelism: parallelism.max(1),
                    batch_size,
                    exact,
                    exact_overfetch: exact_overfetch.max(1),
                    metric,
                };
                provider
                    .vector_batch_search(&ctx.state(), &request)
                    .await
                    .map_err(|error| format!("batch vector search failed: {error}"))
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    /// Release the session (the underlying context is dropped when Python
    /// releases the object; this method exists for use in `__exit__`).
    fn close(&self) {}

    fn __repr__(&self) -> String {
        "LakeSoulSession(<native query session>)".to_string()
    }
}

impl NativeSession {
    /// Run one statement and return its Arrow batches.
    ///
    /// The closure runs without the GIL; the error is converted after the
    /// thread joins so no Python object is touched inside it.
    fn collect_arrow(&self, py: Python<'_>, sql: &str) -> PyResult<Vec<RecordBatch>> {
        let ctx = Arc::clone(&self.ctx);
        let sql = sql.to_string();
        py.detach(move || {
            GLOBAL_RUNTIME.block_on(async move {
                let dataframe = ctx.sql(&sql).await.map_err(|error| {
                    format!("query planning failed: {error}; SQL: {sql}")
                })?;
                let schema = dataframe.schema().as_arrow().clone();
                let batches = dataframe
                    .collect()
                    .await
                    .map_err(|error| format!("query execution failed: {error}; SQL: {sql}"))?;
                if batches.is_empty() {
                    // Keep the result schema so `pyarrow.Table.from_batches`
                    // yields a typed, empty table instead of a schema-less one.
                    Ok::<_, String>(vec![RecordBatch::new_empty(Arc::new(schema))])
                } else {
                    Ok(batches)
                }
            })
        })
        .map_err(PyRuntimeError::new_err)
    }
}

fn config_string(config: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<String>> {
    config
        .get_item(key)?
        .map(|value| {
            value
                .extract::<String>()
                .map_err(|_| PyValueError::new_err(format!("{key} must be a string")))
        })
        .transpose()
}

fn config_usize(config: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<usize>> {
    config
        .get_item(key)?
        .map(|value| {
            value
                .extract::<usize>()
                .map_err(|_| PyValueError::new_err(format!("{key} must be an integer")))
        })
        .transpose()
}

/// Register the `lakesoul._lib._session` submodule.
pub(crate) fn init(py: Python<'_>, parent: &Bound<PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "_session")?;
    parent.add_submodule(&m)?;
    install_module("lakesoul._lib._session", &m)?;
    m.add_class::<NativeSession>()?;
    Ok(())
}
