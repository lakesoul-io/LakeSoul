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
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_pyarrow::PyArrowType;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion_execution::SendableRecordBatchStream;
use futures::StreamExt;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::create_lakesoul_session_config;
use lakesoul_datafusion::datasource::table_provider::{LakeSoulTableProvider, TableReadSnapshot};
use lakesoul_datafusion::distributed::{DistributedOptions, WorkerDiscovery};
use lakesoul_datafusion::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchConfig;
use lakesoul_datafusion::vector_batch::VectorBatchRequest;
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
static SNAPSHOT_ALIAS: AtomicU64 = AtomicU64::new(0);
const SNAPSHOT_CATALOG: &str = "__lakesoul_snapshots";
const SNAPSHOT_SCHEMA: &str = "public";

/// Iterator over a DataFusion query's Arrow batches.
///
/// The stream is pulled lazily, one batch per `__next__`, so large query
/// results do not have to be materialised as one Arrow table first.
#[pyclass(
    module = "lakesoul._lib._session",
    name = "NativeBatchStream",
    unsendable
)]
pub struct NativeBatchStream {
    stream: Option<SendableRecordBatchStream>,
}

#[pymethods]
impl NativeBatchStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyArrowType<RecordBatch>>> {
        let Some(mut stream) = self.stream.take() else {
            return Ok(None);
        };
        let next = py.detach(|| GLOBAL_RUNTIME.block_on(stream.next()));
        match next {
            Some(Ok(batch)) => {
                self.stream = Some(stream);
                Ok(Some(PyArrowType(batch)))
            }
            Some(Err(error)) => Err(PyRuntimeError::new_err(format!(
                "streaming query failed: {error}"
            ))),
            None => Ok(None),
        }
    }
}

#[pyclass(module = "lakesoul._lib._session", name = "NativeSession")]
pub struct NativeSession {
    ctx: Arc<SessionContext>,
    /// In-memory schema where pinned (time-travel) table views are registered.
    snapshot_schema: Arc<MemorySchemaProvider>,
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
        let mut distributed: Option<DistributedOptions> = None;
        if let Some(config) = config.as_ref() {
            if let Some(value) = config_string(config, "warehouse_prefix")? {
                args.warehouse_prefix = Some(value);
            }
            if let Some(value) = config_usize(config, "target_partitions")? {
                if value == 0 {
                    return Err(PyValueError::new_err("target_partitions must be >= 1"));
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
                            args.s3_virtual_host_style = !value.eq_ignore_ascii_case("true");
                        }
                        // Other options are not part of the reader's S3
                        // configuration surface; ignoring them keeps the
                        // session usable while `target_partitions`/caches are
                        // configured through their dedicated settings.
                        _ => {}
                    }
                }
            }
            if let Some(options) = config.get_item("distributed")? {
                let options = options
                    .downcast::<PyDict>()
                    .map_err(|_| PyValueError::new_err("distributed must be a dict"))?;
                let worker_urls: Vec<String> = options
                    .get_item("worker_urls")?
                    .ok_or_else(|| {
                        PyValueError::new_err(
                            "distributed.worker_urls is required for a distributed session",
                        )
                    })?
                    .extract()
                    .map_err(|_| {
                        PyValueError::new_err(
                            "distributed.worker_urls must be a list of URL strings",
                        )
                    })?;
                if worker_urls.is_empty() {
                    return Err(PyValueError::new_err(
                        "distributed.worker_urls must not be empty",
                    ));
                }
                let distributed_partitions =
                    config_usize(options, "target_partitions")?.unwrap_or(8);
                if distributed_partitions < 2 {
                    return Err(PyValueError::new_err(
                        "distributed.target_partitions must be >= 2",
                    ));
                }
                let fallback_to_local = config_bool(options, "fallback_to_local")?.unwrap_or(false);
                let bytes_per_partition = config_usize(options, "bytes_per_partition")?;
                distributed = Some(DistributedOptions {
                    discovery: WorkerDiscovery::Static(worker_urls),
                    fallback_to_local,
                    target_partitions: distributed_partitions,
                    bytes_per_partition,
                    // Python options do not expose task placement yet.
                    ..DistributedOptions::default()
                });
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
                        "deferred_payload" => vector_search.deferred_payload = value.extract()?,
                        "exact_overfetch" => vector_search.exact_overfetch = value.extract()?,
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
        let factory =
            LakeSoulSessionFactory::new(Arc::new(meta_client), &args).map_err(|error| {
                PyRuntimeError::new_err(format!(
                    "failed to create the LakeSoul session factory: {error}"
                ))
            })?;
        let factory = match distributed {
            Some(options) => factory.with_distributed(options),
            None => factory,
        };
        let factory = match target_partitions {
            Some(partitions) => factory.with_target_partitions(partitions),
            None => factory,
        };
        let ctx = factory
            .with_session_template(session_config)
            .create_session(&LakeSoulSessionOptions::default())
            .map_err(|error| {
                PyRuntimeError::new_err(format!(
                    "failed to create the LakeSoul query session: {error}"
                ))
            })?;
        // Pinned snapshot views live in their own in-memory catalog: they are
        // per-session query artifacts, not catalog tables, and registering
        // them next to the LakeSoul catalog would hit the metadata write path.
        let snapshot_schema = Arc::new(MemorySchemaProvider::new());
        let snapshot_catalog = Arc::new(MemoryCatalogProvider::new());
        let schema_provider: Arc<dyn SchemaProvider> = snapshot_schema.clone();
        snapshot_catalog
            .register_schema(SNAPSHOT_SCHEMA, schema_provider)
            .map_err(|error| {
                PyValueError::new_err(format!("failed to create the snapshot schema: {error}"))
            })?;
        ctx.register_catalog(
            SNAPSHOT_CATALOG,
            snapshot_catalog as Arc<dyn CatalogProvider>,
        );
        Ok(Self {
            ctx,
            snapshot_schema,
        })
    }

    /// Execute one SQL statement and return the result as pyarrow record batches.
    ///
    /// The statement runs on the DataFusion engine with the LakeSoul catalog,
    /// so it can combine lakehouse SQL (filters, joins, aggregates, window
    /// functions) with vector search (`ORDER BY array_distance(...) LIMIT k`).
    /// The GIL is released while the query runs.
    fn sql(&self, py: Python<'_>, sql: String) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
        self.collect_arrow(py, &sql)
            .map(|batches| batches.into_iter().map(PyArrowType).collect())
    }

    /// Execute one SQL statement and return a lazy iterator of Arrow batches.
    ///
    /// Unlike `sql`, this does not collect the whole result: callers can stop
    /// early, and large results stay bounded by the consumer's pace.
    fn sql_stream(&self, py: Python<'_>, sql: String) -> PyResult<NativeBatchStream> {
        let ctx = Arc::clone(&self.ctx);
        let stream = py
            .detach(move || {
                GLOBAL_RUNTIME.block_on(async move {
                    let dataframe = ctx
                        .sql(&sql)
                        .await
                        .map_err(|error| format!("query planning failed: {error}; SQL: {sql}"))?;
                    dataframe
                        .execute_stream()
                        .await
                        .map_err(|error| format!("query execution failed: {error}; SQL: {sql}"))
                })
            })
            .map_err(PyRuntimeError::new_err)?;
        Ok(NativeBatchStream {
            stream: Some(stream),
        })
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
            .map_err(|error| PyRuntimeError::new_err(format!("failed to format the plan: {error}")))
    }

    /// Update the session's `lakesoul_vector_search.*` options.
    ///
    /// The values take effect for the following statements on this session
    /// (nearest-statement semantics, like `SET` in SQL):
    ///
    /// ```python
    /// session.set_vector_search(rerank="index", nprobe=16)
    /// ```
    fn set_vector_search(&self, py: Python<'_>, options: Bound<'_, PyDict>) -> PyResult<()> {
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
            self.collect_arrow(py, &format!("SET lakesoul_vector_search.{key} = {literal}"))?;
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
                let schema = catalog
                    .schema(&namespace)
                    .ok_or_else(|| format!("the namespace {namespace:?} is not registered"))?;
                let provider = schema
                    .table(&table)
                    .await
                    .map_err(|error| {
                        format!("failed to resolve the table {namespace}.{table}: {error}")
                    })?
                    .ok_or_else(|| format!("the table {namespace}.{table} is not registered"))?;
                let provider = provider
                    .downcast_ref::<LakeSoulTableProvider>()
                    .ok_or_else(|| format!("{namespace}.{table} is not a LakeSoul table"))?;
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

    /// Register a pinned (time-travel) view of a table for SQL queries.
    ///
    /// Exactly one of ``snapshot`` (a snapshot id from
    /// ``LakeSoulCatalog.create_snapshot``), ``tag`` or ``as_of`` (Unix
    /// milliseconds) must be given.  The view is registered under a unique
    /// alias and is readable as an unqualified table name (or via the alias);
    /// the query sees the partition versions frozen at that point, with the
    /// regular merge-on-read semantics.  The vector/text index pushdowns are
    /// disabled for pinned reads because those indexes describe the latest
    /// files; the distance ordering is still exact.
    ///
    /// Drop the alias with :meth:`unregister_table` when done (the Python
    /// wrapper does this automatically).
    #[pyo3(signature = (table, namespace = "default".to_string(), snapshot = None, tag = None, as_of = None))]
    fn register_snapshot_table(
        &self,
        py: Python<'_>,
        table: String,
        namespace: String,
        snapshot: Option<i64>,
        tag: Option<String>,
        as_of: Option<i64>,
    ) -> PyResult<String> {
        let requested = [snapshot.is_some(), tag.is_some(), as_of.is_some()]
            .into_iter()
            .filter(|given| *given)
            .count();
        if requested != 1 {
            return Err(PyValueError::new_err(
                "exactly one of snapshot, tag or as_of must be given",
            ));
        }
        let spec = match (snapshot, tag, as_of) {
            (Some(snapshot_id), None, None) => {
                if snapshot_id < 0 {
                    return Err(PyValueError::new_err("snapshot must be a non-negative id"));
                }
                TableReadSnapshot::SnapshotId(snapshot_id)
            }
            (None, Some(tag), None) => {
                if tag.trim().is_empty() {
                    return Err(PyValueError::new_err("tag must not be empty"));
                }
                TableReadSnapshot::Tag(tag)
            }
            (None, None, Some(as_of_ms)) => {
                if as_of_ms < 0 {
                    return Err(PyValueError::new_err(
                        "as_of must be a Unix millisecond timestamp",
                    ));
                }
                TableReadSnapshot::AsOfMs(as_of_ms)
            }
            _ => unreachable!("exactly one snapshot selector is validated above"),
        };
        let alias = format!(
            "__lakesoul_snapshot_{}",
            SNAPSHOT_ALIAS.fetch_add(1, Ordering::Relaxed)
        );
        let ctx = Arc::clone(&self.ctx);
        let snapshot_schema = Arc::clone(&self.snapshot_schema);
        let alias_for_task = alias.clone();
        py.detach(move || {
            GLOBAL_RUNTIME.block_on(async move {
                let catalog = ctx
                    .catalog("lakesoul")
                    .ok_or_else(|| "the lakesoul catalog is not registered".to_string())?;
                let schema = catalog
                    .schema(&namespace)
                    .ok_or_else(|| format!("the namespace {namespace:?} is not registered"))?;
                let provider = schema
                    .table(&table)
                    .await
                    .map_err(|error| {
                        format!("failed to resolve the table {namespace}.{table}: {error}")
                    })?
                    .ok_or_else(|| format!("the table {namespace}.{table} is not registered"))?;
                let provider = provider
                    .downcast_ref::<LakeSoulTableProvider>()
                    .ok_or_else(|| format!("{namespace}.{table} is not a LakeSoul table"))?;
                let view: Arc<dyn TableProvider> = Arc::new(provider.snapshot_view(spec));
                snapshot_schema
                    .register_table(alias_for_task.clone(), view)
                    .map_err(|error| format!("failed to register the snapshot view: {error}"))?;
                Ok::<String, String>(format!(
                    "\"{SNAPSHOT_CATALOG}\".\"{SNAPSHOT_SCHEMA}\".\"{alias_for_task}\""
                ))
            })
        })
        .map_err(PyRuntimeError::new_err)
    }

    /// Remove a view registered by :meth:`register_snapshot_table`.
    ///
    /// Accepts either the returned qualified reference or the bare alias.
    fn unregister_table(&self, name: String) -> PyResult<()> {
        let alias = name
            .rsplit('.')
            .next()
            .unwrap_or(name.as_str())
            .trim_matches('"')
            .to_string();
        self.snapshot_schema
            .deregister_table(alias.as_str())
            .map_err(|error| {
                PyRuntimeError::new_err(format!("failed to unregister the table view: {error}"))
            })?;
        Ok(())
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
                let dataframe = ctx
                    .sql(&sql)
                    .await
                    .map_err(|error| format!("query planning failed: {error}; SQL: {sql}"))?;
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

fn config_bool(config: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<bool>> {
    config
        .get_item(key)?
        .map(|value| {
            value
                .extract::<bool>()
                .map_err(|_| PyValueError::new_err(format!("{key} must be a boolean")))
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
    m.add_class::<NativeBatchStream>()?;
    m.add_class::<NativeSession>()?;
    Ok(())
}
