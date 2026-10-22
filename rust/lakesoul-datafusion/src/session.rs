// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Reusable LakeSoul DataFusion session construction.
//!
//! [`LakeSoulSessionFactory`] owns resources shared by all sessions (metadata
//! client, session config template, object-store configuration, warehouse
//! location and execution memory pool) and builds an independent
//! [`SessionContext`] on each call. PostgreSQL connection identity and settings
//! are intentionally kept in the `postgres-lakesoul` crate.

use datafusion::catalog::{CatalogProvider, TableProviderFactory};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool,
    MemoryReservation, TrackConsumersPool, UnboundedMemoryPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{Distribution, ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    BroadcastExec, DesiredTaskCountEvent, DesiredTaskCountEventResponse,
    DistributedConfig, DistributedExt, SessionStateBuilderExt, WorkerResolver,
};
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::object_store::{register_hdfs_object_store, register_s3_object_store};
use lakesoul_io::object_store_metrics::MonitoredObjectStore;
use lakesoul_io::physical_plan::MergeParquetExec;
use object_store::local::LocalFileSystem;
use rootcause::{bail, report};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use url::Url;

use crate::Result;
use crate::catalog::{
    CatalogSnapshot, DEFAULT_CATALOG_REFRESH_INTERVAL, LakeSoulCatalog,
    LakeSoulProviderOptions,
};
use crate::cli::CoreArgs;
use crate::datasource::table_factory::LakeSoulTableProviderFactory;
use crate::distributed::DistributedOptions;
use crate::distributed::planner::LakeSoulDistributedQueryPlanner;
use crate::distributed::resolver::{
    KubernetesWorkerResolver, StaticWorkerResolver, WorkerDiscovery,
};
use crate::planner::LakeSoulQueryPlanner;

pub const DEFAULT_CATALOG: &str = "lakesoul";
pub const DEFAULT_SCHEMA: &str = "default";

/// Execution memory policy shared by every connection/task in a server process.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct ExecutionMemoryArgs {
    /// Process-wide execution budget in bytes (K/M/G or Ki/Mi/Gi accepted); 0 is unbounded.
    #[arg(long, env = "LAKESOUL_MEMORY_LIMIT", default_value = "0", value_parser = parse_memory_limit)]
    pub memory_limit: usize,

    /// Allocation policy used when the execution memory budget is nonzero.
    #[arg(long, env = "LAKESOUL_MEMORY_POOL", value_enum, default_value = "fair")]
    pub memory_pool: ExecutionMemoryPool,

    /// Process-wide bytes of decoded Vortex data in flight (K/M/G or Ki/Mi/Gi
    /// accepted). Defaults to a quarter of `--memory-limit` when that is
    /// nonzero, and to unbounded otherwise; 0 is unbounded.
    #[arg(long, env = "LAKESOUL_VORTEX_DECODE_BUDGET", value_parser = parse_memory_limit)]
    pub vortex_decode_budget: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum ExecutionMemoryPool {
    #[default]
    Fair,
    Greedy,
}

/// Parse a byte budget without rounding or silently overflowing `usize`.
pub fn parse_memory_limit(value: &str) -> std::result::Result<usize, String> {
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (digits, suffix) = value.split_at(split);
    let bytes = digits
        .parse::<usize>()
        .map_err(|_| format!("invalid memory limit: {value:?}"))?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" => 1,
        "k" => 1_000,
        "m" => 1_000_000,
        "g" => 1_000_000_000,
        "ki" => 1_024,
        "mi" => 1_048_576,
        "gi" => 1_073_741_824,
        _ => return Err(format!("invalid memory limit suffix: {value:?}")),
    };
    bytes
        .checked_mul(multiplier)
        .ok_or_else(|| format!("memory limit overflows usize: {value:?}"))
}

impl ExecutionMemoryArgs {
    /// Create once at startup, after installing the metrics recorder.
    pub fn create_pool(&self) -> Arc<dyn MemoryPool> {
        let top = NonZeroUsize::new(5).unwrap();
        let (kind, inner): (&str, Arc<dyn MemoryPool>) = if self.memory_limit == 0 {
            ("unbounded", Arc::new(UnboundedMemoryPool::default()))
        } else {
            match self.memory_pool {
                ExecutionMemoryPool::Fair => (
                    "fair",
                    Arc::new(TrackConsumersPool::new(
                        FairSpillPool::new(self.memory_limit),
                        top,
                    )),
                ),
                ExecutionMemoryPool::Greedy => (
                    "greedy",
                    Arc::new(TrackConsumersPool::new(
                        GreedyMemoryPool::new(self.memory_limit),
                        top,
                    )),
                ),
            }
        };
        let reserved = metrics::gauge!("lakesoul_memory_pool_reserved_bytes");
        reserved.set(0.0);
        metrics::gauge!("lakesoul_memory_pool_limit_bytes").set(self.memory_limit as f64);
        // Exported at 0 from startup so the first failures show in
        // `increase()`; `increment(0)` registers without resetting a counter
        // an earlier pool in this process already advanced.
        let failures = metrics::counter!("lakesoul_memory_reservation_failures_total");
        failures.increment(0);
        tracing::info!(
            pool = kind,
            limit_bytes = self.memory_limit,
            "execution memory pool configured"
        );
        Arc::new(MonitoredMemoryPool {
            inner,
            reserved,
            failures,
        })
    }

    /// The effective [`vortex_decode_budget`](Self::vortex_decode_budget), if
    /// decoding is bounded.
    pub fn vortex_decode_budget_bytes(&self) -> Option<usize> {
        match self.vortex_decode_budget {
            Some(0) => None,
            Some(bytes) => Some(bytes),
            None => (self.memory_limit > 0).then_some(self.memory_limit / 4),
        }
    }

    /// Installs the process-wide Vortex decode budget. Call once at startup,
    /// before building session factories or workers.
    pub fn install_vortex_decode_budget(&self) -> Result<()> {
        let Some(bytes) = self.vortex_decode_budget_bytes() else {
            tracing::info!("vortex decode budget unbounded");
            return Ok(());
        };
        lakesoul_io::file_format::install_vortex_decode_budget(bytes)?;
        tracing::info!(limit_bytes = bytes, "vortex decode budget configured");
        Ok(())
    }
}

/// Cached metric handles avoid recorder lookup/allocation on the hot path.
/// Gauge deltas (not `set(inner.reserved())`) avoid stale snapshots when tasks
/// concurrently grow and shrink the shared pool. Baseline uses this same thin
/// wrapper, but deliberately omits consumer tracking and its locking overhead.
#[derive(Debug)]
struct MonitoredMemoryPool {
    inner: Arc<dyn MemoryPool>,
    reserved: metrics::Gauge,
    failures: metrics::Counter,
}

impl fmt::Display for MonitoredMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl MemoryPool for MonitoredMemoryPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.reserved.increment(additional as f64);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.reserved.decrement(shrink as f64);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> DataFusionResult<()> {
        match self.inner.try_grow(reservation, additional) {
            Ok(()) => {
                self.reserved.increment(additional as f64);
                Ok(())
            }
            Err(error) => {
                self.failures.increment(1);
                Err(error)
            }
        }
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Configuration used to create object-store clients.
pub type ObjectStoreConfig = LakeSoulIOConfig;

/// Generic options applied to one newly-created LakeSoul session.
#[derive(Debug, Clone)]
pub struct LakeSoulSessionOptions {
    /// Schema used to resolve unqualified table names.
    pub default_schema: String,
    /// Optional DataFusion execution time zone. When omitted, the value from
    /// the factory's [`SessionConfig`] template is preserved.
    pub time_zone: Option<String>,
}

impl Default for LakeSoulSessionOptions {
    fn default() -> Self {
        Self {
            default_schema: DEFAULT_SCHEMA.to_string(),
            time_zone: None,
        }
    }
}

/// Parsed warehouse prefix used to register an object store in each session's
/// [`RuntimeEnv`].
#[derive(Debug, Clone)]
pub enum WarehouseConfig {
    /// `s3://`/`s3a://` prefix, trimmed to scheme + authority.
    S3(Url),
    /// `hdfs://` prefix; the authority is used as the HDFS default FS.
    Hdfs(Url),
    /// `file://` prefix or no prefix at all.
    Local,
}

type CatalogDecorator =
    dyn Fn(Arc<LakeSoulCatalog>) -> Arc<dyn CatalogProvider> + Send + Sync;

/// Pins every `MergeParquetExec` to a single distributed task.
///
/// Every bucket work unit in a range merge must see all its version files.
/// This distributed-task cap does not limit the merge's local output lanes.
/// Without it the distributed planner derives the merge subtree's task
/// count from its file scans (`Desired(N)` for large file sets or a small
/// `bytes_per_partition`), the children-isolator union allocates several tasks
/// to one merge, and the leaf scale-up slices every version file into byte
/// ranges — so the same primary key's versions merge in different tasks and
/// the scan emits stale or duplicate rows. `Maximum(1)` wins over the scans'
/// `Desired(N)` during task-count reconciliation, for both the union and the
/// no-union (single range partition) plan shape; parallelism across merges of
/// different range partitions and across local bucket lanes is preserved.
fn merge_parquet_exec_single_task(
    ev: DesiredTaskCountEvent<'_>,
) -> Option<DataFusionResult<DesiredTaskCountEventResponse>> {
    ev.plan.downcast_ref::<MergeParquetExec>()?;
    Some(Ok(DesiredTaskCountEventResponse::maximum(1)))
}

/// Caps a stage at one task when splitting its scans would break the input
/// distribution an operator in it was planned against.
///
/// DataFusion satisfies an operator's `required_input_distribution` with the
/// partitioning its input declares, and a one-partition input — an
/// append-only work unit is one file group — satisfies `SinglePartition` and
/// every `HashPartitioned` requirement on its own. So an `AggregateExec` in
/// `Single` or `SinglePartitioned` mode, a global `SortExec`, a
/// `GlobalLimitExec`, a window or a sort-merge join lands directly on the
/// scan. The distributed planner then splits the scan's files over the
/// stage's tasks without consulting those requirements, and every task runs
/// the operator over its share: a scalar subquery's `avg` is broadcast as one
/// row per task (TPC-H q22), a `LIMIT` holds per task, a join misses the
/// matches in other tasks.
///
/// An input keeps its distribution across tasks only when it arrives through
/// a stage boundary: a hash shuffle (above a hash `RepartitionExec`), a
/// broadcast, or a coalesce (`CoalescePartitionsExec`,
/// `SortPreservingMergeExec`), whose consumer runs in one task. An operator
/// with a distribution requirement on an input that reaches a leaf without
/// crossing one of them therefore answers `Maximum(1)`, which wins over the
/// scans' `Desired(N)` for the whole stage: the stage reads every row in one
/// task instead of splitting them. This is a placement policy, not a
/// fallback: the stage stays a stage of the distributed plan when it borders
/// another one, and only a plan left without any boundary is the original
/// single-node plan, which the distributed planner returns as is.
fn input_distribution_single_task(
    ev: DesiredTaskCountEvent<'_>,
) -> Option<DataFusionResult<DesiredTaskCountEventResponse>> {
    let plan = ev.plan;
    plan.required_input_distribution()
        .iter()
        .zip(plan.children())
        .any(|(required, child)| {
            !matches!(required, Distribution::UnspecifiedDistribution)
                && reaches_leaf_within_stage(child)
        })
        .then(|| Ok(DesiredTaskCountEventResponse::maximum(1)))
}

/// Whether `plan` reaches a leaf without crossing a stage boundary of the
/// distributed planner.
fn reaches_leaf_within_stage(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if closes_stage(plan.as_ref()) {
        return false;
    }
    let children = plan.children();
    children.is_empty() || children.into_iter().any(reaches_leaf_within_stage)
}

/// Whether the distributed planner ends a stage at `plan`, handing its
/// consumer an input that is distributed across tasks as declared.
fn closes_stage(plan: &dyn ExecutionPlan) -> bool {
    plan.is::<BroadcastExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<SortPreservingMergeExec>()
        || plan
            .downcast_ref::<RepartitionExec>()
            .is_some_and(|repartition| {
                matches!(repartition.partitioning(), Partitioning::Hash(..))
            })
}

/// Caps `datafusion.optimizer.repartition_file_min_size` at the scan size
/// from which the distributed planner splits a scan over several tasks.
///
/// The two split decisions are taken independently. `EnforceDistribution`
/// byte-splits a file scan into `target_partitions` partitions once it holds
/// `repartition_file_min_size` bytes (1 MiB by default); the distributed
/// planner gives a scan `ceil(ceil(bytes / bytes_per_partition) /
/// target_partitions)` tasks, i.e. several once it holds more than
/// `bytes_per_partition * target_partitions` bytes. A scan the distributed
/// planner splits but DataFusion kept whole was planned as one partition, so
/// a `Single`-mode aggregate (or a global sort, limit, window) sits directly
/// on it and [`input_distribution_single_task`] must run the stage in one
/// task. With the cap, DataFusion plans such a scan multi-partition — a
/// `Partial` aggregate on the scan and its `Final` half behind a coalesce or
/// hash shuffle — and the stage keeps its tasks.
///
/// A plan-shape optimization, not a correctness mechanism: DataFusion keeps a
/// scan whole for reasons this threshold does not reach (a source labelled
/// compressed — LakeSoul's Parquet files — or without byte-range support,
/// exact row statistics of at most one batch, a declared output
/// partitioning such as a merge-on-read input's, or a `SET` of either knob
/// after the session was created), and `input_distribution_single_task`
/// keeps those plans correct. Only ever lowers the threshold, so a template
/// asking for a smaller one keeps it.
fn align_repartition_file_min_size(
    config: &mut SessionConfig,
    bytes_per_partition: usize,
) {
    let options = config.options_mut();
    let distributed_split_size =
        bytes_per_partition.saturating_mul(options.execution.target_partitions);
    options.optimizer.repartition_file_min_size = options
        .optimizer
        .repartition_file_min_size
        .min(distributed_split_size);
}

/// Factory for independent LakeSoul [`SessionContext`]s.
///
/// Every [`create_session`](Self::create_session) call clones the session
/// config template, builds a fresh [`RuntimeEnv`] sharing the factory's memory
/// pool, and wires the LakeSoul planner, table factory and catalog into a fresh
/// context. Object-store registries and TMPDIR-backed spill managers remain
/// independent per session.
pub struct LakeSoulSessionFactory {
    meta_client: crate::MetaDataClientRef,
    session_template: SessionConfig,
    object_store_config: ObjectStoreConfig,
    warehouse: WarehouseConfig,
    memory_pool: Arc<dyn MemoryPool>,
    planner: Arc<dyn QueryPlanner + Send + Sync>,
    table_factory: Arc<LakeSoulTableProviderFactory>,
    catalog_decorator: Option<Arc<CatalogDecorator>>,
    /// Distributed planning options; `None` keeps sessions single-node.
    distributed: Option<DistributedOptions>,
    /// `target_partitions` of single-node sessions; distributed sessions use
    /// [`DistributedOptions::target_partitions`] instead.
    target_partitions: Option<usize>,
    /// Metadata view shared by every session of this factory.
    ///
    /// One snapshot (and therefore one periodic refresher) per factory instead
    /// of one per session: per-session snapshots would multiply the periodic
    /// metadata load by the number of connections.
    catalog_snapshot: Arc<CatalogSnapshot>,
}

impl LakeSoulSessionFactory {
    pub fn new(meta_client: crate::MetaDataClientRef, args: &CoreArgs) -> Result<Self> {
        Self::new_with_memory_pool(
            meta_client,
            args,
            ExecutionMemoryArgs::default().create_pool(),
        )
    }

    /// Build a factory using a pool allocated once by the server process.
    pub fn new_with_memory_pool(
        meta_client: crate::MetaDataClientRef,
        args: &CoreArgs,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let session_template = Self::session_template()?;
        let object_store_config =
            LakeSoulIOConfigBuilder::new_with_object_store_options(args.s3_options())
                .build();
        let warehouse = parse_warehouse(args.warehouse_prefix.as_deref())?;
        let table_factory = Arc::new(LakeSoulTableProviderFactory::new(
            Arc::clone(&meta_client),
            args.warehouse_prefix.clone(),
        ));
        let catalog_snapshot = CatalogSnapshot::new(
            Arc::clone(&meta_client),
            DEFAULT_CATALOG_REFRESH_INTERVAL,
        );
        Ok(Self {
            meta_client,
            session_template,
            object_store_config,
            warehouse,
            memory_pool,
            planner: LakeSoulQueryPlanner::new_ref(),
            table_factory,
            catalog_decorator: None,
            distributed: None,
            target_partitions: None,
            catalog_snapshot,
        })
    }

    /// Install a decorator applied to the `lakesoul` catalog of every session
    /// created by this factory.
    pub fn with_catalog_decorator<F>(mut self, decorate_catalog: F) -> Self
    where
        F: Fn(Arc<LakeSoulCatalog>) -> Arc<dyn CatalogProvider> + Send + Sync + 'static,
    {
        self.catalog_decorator = Some(Arc::new(decorate_catalog));
        self
    }

    /// Replace the session config template shared by every session.
    ///
    /// Used by [`crate::create_lakesoul_session_ctx_with_config`] to attach
    /// caller-supplied extensions (e.g. vector-search options).
    pub fn with_session_template(mut self, session_template: SessionConfig) -> Self {
        self.session_template = session_template;
        self
    }

    /// Build a fresh [`SessionContext`].
    pub fn create_session(
        &self,
        options: &LakeSoulSessionOptions,
    ) -> Result<Arc<SessionContext>> {
        let mut session_config = self
            .session_template
            .clone()
            .with_default_catalog_and_schema(
                DEFAULT_CATALOG.to_string(),
                options.default_schema.clone(),
            );
        if let Some(time_zone) = &options.time_zone {
            session_config.options_mut().execution.time_zone = Some(time_zone.clone());
        }

        let distributed = self.distributed.clone();
        // `target_partitions` is parameterized rather than fixed at 1:
        // distributed planning relies on hash `RepartitionExec` stage
        // boundaries, which the physical planner only emits when
        // `target_partitions > 1`. A session template supplied by the caller
        // keeps its own value unless the factory overrides it.
        let target_partitions = match &distributed {
            Some(distributed) => distributed.target_partitions.max(2),
            None => self
                .target_partitions
                .unwrap_or(session_config.options().execution.target_partitions)
                .max(1),
        };
        session_config.options_mut().execution.target_partitions = target_partitions;
        if let Some(distributed) = &distributed {
            let bytes_per_partition =
                distributed.bytes_per_partition.unwrap_or_else(|| {
                    DistributedConfig::default().file_scan_config_bytes_per_partition
                });
            align_repartition_file_min_size(&mut session_config, bytes_per_partition);
        }

        // Object-store options (`fs.s3a.*`, `fs.defaultFS`, …) must be
        // reachable beyond the warehouse store registered on the session
        // runtime: readers that build their own io session — the vector
        // search scan — and the vector index auto-build after a commit
        // construct their stores from the table's io config, which would
        // otherwise carry no credentials at all.
        let session_config =
            session_config.with_extension(Arc::new(self.object_store_config.clone()));

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&self.memory_pool))
            .build_arc()?;
        register_warehouse_object_store(
            &self.warehouse,
            &self.object_store_config,
            &runtime,
        )?;

        // `DistributedOptions::discovery` is one resolver per process: the
        // shared variant (used by coordinator binaries after start-up
        // initialization) clones the same Arc for every session, so exactly
        // one discovery loop exists.
        let resolver: Option<Arc<dyn WorkerResolver>> = distributed
            .as_ref()
            .map(|dist| build_worker_resolver(&dist.discovery))
            .transpose()?;

        // Distributed sessions must install the LakeSoul planner *before*
        // `with_distributed_planner()`: the latter wraps the planner already
        // present on the builder, and the reverse order would let the
        // LakeSoul planner overwrite the distributed one.
        let mut builder = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::clone(&self.planner))
            .with_optimizer_rule(Arc::new(
                crate::planner::vector_search_rule::VectorSearchPushdownRule,
            ))
            .with_optimizer_rule(Arc::new(
                crate::planner::text_search_rule::TextSearchPushdownRule,
            ))
            .with_physical_optimizer_rule(Arc::new(
                crate::planner::text_score_rule::TextScoreProjectionRule,
            ));
        if let (Some(dist), Some(resolver)) = (&distributed, &resolver) {
            builder = builder
                .with_distributed_worker_resolver(Arc::clone(resolver))
                .with_distributed_metrics_collection(true)?
                .with_distributed_desired_task_count_handler(
                    merge_parquet_exec_single_task,
                )
                .with_distributed_desired_task_count_handler(
                    input_distribution_single_task,
                );
            // The codec list is position-addressed on the wire, so it comes
            // from one function the workers call too; see `codec::user_codecs`.
            for codec in crate::distributed::codec::user_codecs() {
                builder = builder.with_distributed_user_codec_arc(codec);
            }
            // Merge-work-unit atomicity: a merge-on-read work unit (one
            // partition's complete file set) is one `MergeParquetExec`, i.e. one
            // child of the scan's union. The children-isolator union runs each
            // child whole inside a single task, so a merge never sees only a
            // subset of its versions — which would duplicate primary keys or
            // lose the version that should win. Enabled explicitly rather than
            // relying on the library default.
            builder = builder.with_distributed_children_isolator_unions(true)?;
            if let Some(bytes_per_partition) = dist.bytes_per_partition {
                builder = builder.with_distributed_file_scan_config_bytes_per_partition(
                    bytes_per_partition,
                )?;
            }
            builder = builder.with_distributed_planner();

            // Availability policy gate in front of the distributed planner:
            // fast-fail in production, optional single-node fallback in
            // development.
            let distributed_planner = std::mem::take(builder.query_planner())
                .expect("with_distributed_planner installed a query planner");
            builder = builder.with_query_planner(Arc::new(
                LakeSoulDistributedQueryPlanner::new(
                    distributed_planner,
                    dist.fallback_to_local,
                    Some(Arc::clone(resolver)),
                ),
            ));
        }

        let mut state = builder.build();
        state.table_factories_mut().insert(
            "LAKESOUL".to_string(),
            Arc::clone(&self.table_factory) as Arc<dyn TableProviderFactory>,
        );
        let provider_options = LakeSoulProviderOptions::from_session(&state);
        let ctx = Arc::new(SessionContext::new_with_state(state));
        ctx.register_udf((*crate::udf::vector_search_marker::marker_udf()).clone());
        ctx.register_udf((*crate::udf::vector_search_marker::multi_marker_udf()).clone());
        ctx.register_udf((*crate::udf::text_search_marker::marker_udf()).clone());
        ctx.register_udf((*crate::udf::text_search_marker::text_match_udf()).clone());
        ctx.register_udf((*crate::udf::text_search_marker::text_score_udf()).clone());

        let lakesoul_catalog = Arc::new(LakeSoulCatalog::with_snapshot(
            Arc::clone(&self.meta_client),
            provider_options,
            Arc::clone(&self.catalog_snapshot),
        ));
        let catalog = match &self.catalog_decorator {
            Some(decorate) => decorate(lakesoul_catalog),
            None => lakesoul_catalog,
        };
        ctx.state()
            .catalog_list()
            .register_catalog(DEFAULT_CATALOG.to_string(), catalog);

        info!(
            "created LakeSoul session, catalogs: {:?}",
            ctx.catalog_names()
        );
        Ok(ctx)
    }

    /// Sets `target_partitions` for sessions created by this factory.
    ///
    /// Unset by default, so the session template's value is kept (1 for the
    /// default template, and whatever a caller supplied through
    /// [`Self::with_session_template`]). Distributed sessions ignore this and
    /// use [`DistributedOptions::target_partitions`] instead, which is forced
    /// to at least 2.
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = Some(target_partitions);
        self
    }

    /// The metadata view shared by this factory's sessions.
    ///
    /// Call [`CatalogSnapshot::load`] on it once at start-up to warm the view
    /// before serving queries.
    pub fn catalog_snapshot(&self) -> &Arc<CatalogSnapshot> {
        &self.catalog_snapshot
    }

    /// Enable distributed planning for sessions created by this factory.
    pub fn with_distributed(mut self, options: DistributedOptions) -> Self {
        self.distributed = Some(options);
        self
    }

    /// Constant parts of the session config, shared by every session.
    /// Single source of truth: [`crate::create_lakesoul_session_config`].
    fn session_template() -> Result<SessionConfig> {
        crate::create_lakesoul_session_config()
    }
}

pub(crate) fn parse_warehouse(prefix: Option<&str>) -> Result<WarehouseConfig> {
    let Some(prefix) = prefix else {
        return Ok(WarehouseConfig::Local);
    };
    let url = Url::parse(prefix).map_err(|_| report!("Invalid warehouse prefix"))?;
    match url.scheme() {
        "s3" | "s3a" => {
            // Register under scheme + authority; paths resolve through the same store.
            let url = Url::parse(&url[..url::Position::BeforePath])
                .map_err(|_| report!("Invalid warehouse prefix"))?;
            Ok(WarehouseConfig::S3(url))
        }
        "hdfs" => Ok(WarehouseConfig::Hdfs(url)),
        "file" => Ok(WarehouseConfig::Local),
        _ => bail!("Invalid scheme of warehouse prefix"),
    }
}

/// Register the warehouse object store on a session-private runtime.
fn register_warehouse_object_store(
    warehouse: &WarehouseConfig,
    object_store_config: &ObjectStoreConfig,
    runtime: &RuntimeEnv,
) -> Result<()> {
    match warehouse {
        WarehouseConfig::S3(url) => {
            register_s3_object_store(url, object_store_config, runtime)?
        }
        WarehouseConfig::Hdfs(url) => {
            if !url.has_host() {
                bail!("HDFS warehouse prefix without host is not supported");
            }
            register_hdfs_object_store(
                url,
                &url[url::Position::BeforeHost..url::Position::BeforePath],
                object_store_config,
                runtime,
            )?;
        }
        WarehouseConfig::Local => {
            runtime.register_object_store(
                &Url::parse("file://").expect("file:// is a valid URL"),
                Arc::new(MonitoredObjectStore::new(
                    Arc::new(LocalFileSystem::new()),
                    "local",
                )),
            );
        }
    }
    Ok(())
}

/// Build a [`RuntimeEnv`] carrying the warehouse object store registered the
/// same way a coordinator session does it.
///
/// Workers and coordinators must observe identical object-store configuration
/// for encoded plans to resolve their stores on either side.
pub fn build_worker_runtime_env(
    args: &CoreArgs,
    memory: &ExecutionMemoryArgs,
) -> Result<Arc<RuntimeEnv>> {
    let warehouse = parse_warehouse(args.warehouse_prefix.as_deref())?;
    let object_store_config =
        LakeSoulIOConfigBuilder::new_with_object_store_options(args.s3_options()).build();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(memory.create_pool())
        .build_arc()?;
    register_warehouse_object_store(&warehouse, &object_store_config, &runtime)?;
    Ok(runtime)
}

/// Instantiates the worker resolver for the configured discovery backend.
///
/// Public for the coordinator binaries' tests (and the session factory): with
/// [`WorkerDiscovery::Shared`] this returns the same single instance every
/// session of the process passes its resolver from, which is how
/// "one discovery loop per process" is preserved across sessions.
pub fn build_worker_resolver(
    discovery: &WorkerDiscovery,
) -> Result<Arc<dyn WorkerResolver>> {
    match discovery {
        WorkerDiscovery::Static(urls) => {
            Ok(Arc::new(StaticWorkerResolver::new(urls.clone())?))
        }
        WorkerDiscovery::Kubernetes(config) => {
            Ok(Arc::new(KubernetesWorkerResolver::start(config)?))
        }
        // A shared resolver is handed out by the factory and is never rebuilt:
        // cloning the Arc preserves "one discovery loop per process" across
        // sessions and would never be constructed here (parser-resolved
        // options always go through `DistributedArgs::resolve_distributed_options`).
        WorkerDiscovery::Shared(resolver) => Ok(Arc::clone(resolver)),
    }
}
