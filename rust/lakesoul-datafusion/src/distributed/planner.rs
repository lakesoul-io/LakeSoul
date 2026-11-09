// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Policy gate in front of the distributed planner.
//!
//! The gate decides per query what a distributed session does when the
//! distributed planner cannot produce a *distributable* plan:
//!
//! - a table write whose plan carries a runtime subquery (`Expr::ScalarSubquery`
//!   and its siblings) runs on the coordinator: the statement-local
//!   result/cardinality state of such a subquery has no stage contract, and a
//!   decorrelated `LEFT JOIN ... ON true` would silently answer a multi-row
//!   subquery with duplicate rows instead of PostgreSQL's `21000`. This is a
//!   required execution placement, not fallback: it applies whether or not
//!   `fallback_to_local` is set;
//! - the worker resolver fails (Kubernetes discovery without an eligible
//!   worker, or a stale snapshot) → the query fails before planning, whatever
//!   `fallback_to_local` says;
//! - the distributed planner plans the query and every stage it would send to a
//!   worker has a wire form → its plan is used, whether or not it contains a
//!   distributed stage: the planner itself executes a query it is not worth
//!   distributing - including any query while a static worker list is empty -
//!   on the coordinator, and that is not a failure;
//! - the distributed planner fails, or plans a query whose worker stages cannot
//!   cross the wire, `fallback_to_local = true` (development) → plan the query
//!   with the plain LakeSoul planner and execute it single-node on the
//!   coordinator;
//! - either planner case with `fallback_to_local = false` (production) → the
//!   error fails the query instead of silently degrading it to coordinator-only
//!   execution.
//!
//! The wire check is part of the gate because a stage is serialized when it is
//! *sent* to a worker: a stage whose plan DataFusion cannot encode fails the
//! query after this gate has returned, and the coordinator's encoding error
//! surfaces to the user as a worker-side timeout. Refusing such a plan while
//! planning keeps the failure where `fallback_to_local` can still act on it.
//! Only worker stages are checked: the coordinator's head stage runs in place,
//! so a plan the distributed planner kept single-node scans the same file
//! locally and is not a failure.
//!
//! `fallback_to_local` therefore covers *planning failures only*; it does not
//! require every query to produce a distributed stage.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{
    DistributedConfig, DistributedLeafExec, NetworkBoundaryExt, WorkerResolver,
    get_distributed_worker_resolver,
};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::LakeSoulScanExec;

use crate::distributed::codec::{lazy_scan_has_wire_form, source_has_wire_form};
use crate::planner::LakeSoulQueryPlanner;
use crate::planner::query_planner::end_current_otel_span;

/// Restricts this planning attempt to coordinator execution: an explicitly
/// local key scan, a write with statement-local subquery state, or a configured
/// fallback after distributed planning fails. Other statements keep their workers.
#[derive(Debug)]
struct CoordinatorPlanning;

/// How many distributed tasks the stage that runs a table scan may get.
///
/// Mirrors the cap the distributed planner puts on every stage:
/// `distributed.max_tasks_per_stage`, or, when that is 0, the number of
/// workers the session's resolver currently lists. 1 for a session planned
/// on one node: one without a worker resolver, one whose resolver lists no
/// worker or fails (the distributed planner fails such a query, and a
/// fallback plans it locally), and the local fallback itself.
///
/// A merge-on-read scan packs whole (range partition, bucket) units into at
/// most this many merges, one per task; see `LakeSoulTableProvider::scan`. It
/// only shapes the plan: a resolver whose answer changes before the
/// distributed planner asks again yields more or fewer merges per task, never
/// a merge split across tasks.
pub(crate) fn scan_task_budget(config: &SessionConfig) -> usize {
    if config.get_extension::<CoordinatorPlanning>().is_some() {
        return 1;
    }
    let Ok(resolver) = get_distributed_worker_resolver(config) else {
        return 1;
    };
    match DistributedConfig::from_config_options(config.options()) {
        Ok(distributed) if distributed.max_tasks_per_stage > 0 => {
            distributed.max_tasks_per_stage
        }
        _ => resolver.get_urls().map_or(1, |urls| urls.len().max(1)),
    }
}

/// Whether this planning attempt may put writes on workers. Coordinator-only
/// planning retains the resolver but must not create remote write tasks.
pub(crate) fn distributed_writes_enabled(config: &SessionConfig) -> bool {
    config.get_extension::<CoordinatorPlanning>().is_none()
        && get_distributed_worker_resolver(config)
            .is_ok_and(|resolver| resolver.get_urls().is_ok_and(|urls| !urls.is_empty()))
}

/// Build one locally packed historical-key scan. Its captured plan is sent
/// intact to each writer, not split again by the distributed stage planner.
pub(crate) fn local_key_scan_session(session: &dyn Session) -> DFResult<SessionState> {
    let mut state = session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "LakeSoul distributed writes require a SessionState".into(),
            )
        })?
        .clone();
    state
        .config_mut()
        .set_extension(Arc::new(CoordinatorPlanning));
    Ok(state)
}

/// Wraps the distributed [`QueryPlanner`] installed by
/// `SessionStateBuilderExt::with_distributed_planner` and decides per query
/// whether distributed execution is possible.
pub struct LakeSoulDistributedQueryPlanner {
    /// The distributed planner to delegate to when workers are available.
    distributed: Arc<dyn QueryPlanner + Send + Sync>,
    local: Arc<dyn QueryPlanner + Send + Sync>,
    resolver: Option<Arc<dyn WorkerResolver>>,
    /// Whether a query the distributed planner cannot plan - or plans with a
    /// worker stage that has no wire form - is planned locally instead of
    /// failing.
    fallback_to_local: bool,
}

impl std::fmt::Debug for LakeSoulDistributedQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulDistributedQueryPlanner")
            .field("fallback_to_local", &self.fallback_to_local)
            .finish_non_exhaustive()
    }
}

impl LakeSoulDistributedQueryPlanner {
    /// Builds the gate. `distributed` must be the planner produced by
    /// `with_distributed_planner()`.
    pub fn new(
        distributed: Arc<dyn QueryPlanner + Send + Sync>,
        fallback_to_local: bool,
        resolver: Option<Arc<dyn WorkerResolver>>,
    ) -> Self {
        Self {
            distributed,
            local: LakeSoulQueryPlanner::new_ref(),
            resolver,
            fallback_to_local,
        }
    }

    /// Plans the query with the plain LakeSoul planner, so it runs on the
    /// coordinator.
    ///
    /// `placement` is recorded on the trace, because two very different things
    /// lead here: a required execution placement (a write with runtime
    /// subquery state) and the configured fallback after the distributed
    /// planner failed.
    ///
    /// The session still lists the workers, so it is marked with
    /// [`CoordinatorPlanning`]: scans then plan for the one node that runs
    /// them ([`scan_task_budget`]) and writes are never sent to workers.
    async fn plan_locally(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
        reason: &'static str,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        info!(placement = reason, "planning on the coordinator");
        let Some(state) = session.as_any().downcast_ref::<SessionState>() else {
            return self.local.create_physical_plan(logical_plan, session).await;
        };
        let mut state = state.clone();
        state
            .config_mut()
            .set_extension(Arc::new(CoordinatorPlanning));
        // A runtime subquery can only be planned by the coordinator's own
        // planner: a distributed session clears this option so the optimizer
        // flattens uncorrelated subqueries the stage could not ship. The
        // placement is already coordinator-only here, so put the option back
        // and let `DefaultPhysicalPlanner` build the `ScalarSubqueryExec` the
        // statement needs (including its typed cardinality error).
        if plan_contains_subquery(logical_plan) {
            state
                .config_mut()
                .options_mut()
                .optimizer
                .enable_physical_uncorrelated_scalar_subquery = true;
        }
        self.local.create_physical_plan(logical_plan, &state).await
    }
}

/// Whether any node of `plan` carries a subquery expression.
///
/// These are the expressions that are evaluated at *runtime* by a
/// `ScalarSubqueryExec`/`SubqueryExec` and share statement-local
/// result/cardinality state; a subquery the optimizer already decorrelated
/// into a join is an ordinary join and no longer matches.
///
/// A distributed session turns physical uncorrelated scalar subqueries off
/// (`with_distributed_planner` clears
/// `optimizer.enable_physical_uncorrelated_scalar_subquery` because a stage
/// cannot ship `ScalarSubqueryExec`), which makes the optimizer flatten such a
/// subquery into `LEFT JOIN ... ON true` *before* a planner can see it. A
/// write planned from `lakesoul_datafusion::planner::update` therefore keeps
/// the option enabled for the statement (`RuntimeSubqueryGuard`), so the
/// subquery survives to here and can be recognized.
pub(crate) fn plan_contains_subquery(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if matches!(
                    expr,
                    Expr::ScalarSubquery(_)
                        | Expr::InSubquery(_)
                        | Expr::Exists(_)
                        | Expr::SetComparison(_)
                ) {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

/// Whether a write plan carries a runtime subquery.
///
/// Runtime subqueries have statement-local result/cardinality state. Keep
/// these writes on the coordinator until that state has a distributed stage
/// contract: a worker stage cannot share the subquery result, and a
/// decorrelated `LEFT JOIN ... ON true` does not reproduce PostgreSQL's
/// `21000` for a multi-row subquery. Already-decorrelated joins and ordinary
/// writes remain distributable.
fn write_has_subquery(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Dml(_)) && plan_contains_subquery(plan)
}

/// The first scan or write node a worker stage cannot receive.
///
/// Only the plan below a network boundary is sent to a worker: the
/// coordinator's own head stage runs in place, and a plan the distributed
/// planner decided to keep single-node has no boundary at all. The walk
/// therefore checks the children of every network boundary rather than the
/// whole plan, so a leaf that stays on the coordinator is not a failure.
fn unshippable_stage(plan: &Arc<dyn ExecutionPlan>) -> Option<DataFusionError> {
    let mut found = None;
    plan.apply(|node| {
        if node.is_network_boundary() {
            // Everything below the boundary is the plan of one worker stage.
            // The boundary itself is encodable: a worker consumes another
            // stage's output through it, it does not execute it.
            for stage in node.children() {
                if let Some(leaf) = unshippable_node(stage) {
                    found = Some(leaf);
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a physical plan cannot fail");
    found
}

/// Preflight the scan and write nodes of one worker stage.
///
/// The scan leaf of a distributed stage is a [`DistributedLeafExec`], which
/// holds one variant per task and exposes none of them as a child, so its
/// variants - the plans a task receives; the coordinator specializes the leaf
/// to its variant before sending it - are walked explicitly.
///
/// Within a stage, `DataSourceExec`s over non-file sources (memory, …) are
/// skipped - they serialize themselves and have no files to read.
///
/// A `MergeParquetExec` is refused: its payload only described spans into the
/// scan children it merges, and those children are the plan, which a worker
/// does not have. A distributed stage plans its scans as metadata-only
/// [`LakeSoulScanExec`] leaves and builds the merge on the worker instead.
///
/// A metadata-only [`LakeSoulScanExec`] is an opaque leaf: its files are
/// descriptors carried by [`crate::distributed::codec::LakeSoulScanCodec`],
/// not a `FileSource` the `DataSourceExec` arm would see, so it is checked
/// explicitly instead of being waved through as a plan node with no children.
fn unshippable_node(stage: &Arc<dyn ExecutionPlan>) -> Option<DataFusionError> {
    let mut pending = vec![Arc::clone(stage)];
    while let Some(node) = pending.pop() {
        if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
            pending.extend(leaf.variants().iter().cloned());
            continue;
        }
        if node.is::<lakesoul_io::physical_plan::MergeParquetExec>() {
            return Some(DataFusionError::Plan(
                "MergeParquetExec has no wire form: a distributed stage ships its scans as \
                 metadata-only LakeSoulScanExec leaves and merges on the worker"
                    .into(),
            ));
        }
        if let Some(exec) = node.downcast_ref::<LakeSoulScanExec>() {
            if !lazy_scan_has_wire_form(exec) {
                return Some(unshippable_stage_error(&describe_lazy_scan_leaf(exec)));
            }
            // The node is a leaf and carries its own descriptors; its
            // predicate/pk-locator state is part of the same payload.
            continue;
        }
        use crate::datasource::file_format::{LakeSoulCommitExec, LakeSoulWriteExec};
        if node.is::<LakeSoulCommitExec>() {
            return Some(DataFusionError::Plan(
                "LakeSoulCommitExec must stay on the coordinator".into(),
            ));
        }
        if node.is::<LakeSoulWriteExec>() {
            use datafusion_proto::physical_plan::{
                DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
            };
            if let Err(error) = crate::distributed::codec::LakeSoulWriteCodec.try_encode(
                Arc::clone(&node),
                &mut Vec::new(),
                &DefaultPhysicalProtoConverter {},
            ) {
                return Some(error);
            }
        }
        if node
            .downcast_ref::<crate::datasource::upsert_exec::UpsertExec>()
            .is_some()
        {
            use datafusion_proto::physical_plan::{
                DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
            };
            if let Err(error) = crate::distributed::codec::UpsertExecCodec.try_encode(
                Arc::clone(&node),
                &mut Vec::new(),
                &DefaultPhysicalProtoConverter {},
            ) {
                return Some(error);
            }
        }
        if node
            .downcast_ref::<crate::federation::FederationWriteExec>()
            .is_some()
        {
            use datafusion_proto::physical_plan::{
                DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
            };
            if let Err(error) = crate::federation::FederationPlanCodec.try_encode(
                Arc::clone(&node),
                &mut Vec::new(),
                &DefaultPhysicalProtoConverter {},
            ) {
                return Some(error);
            }
        }
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
            && !source_has_wire_form(config.file_source().as_ref())
        {
            return Some(unshippable_stage_error(&describe_scan_leaf(config)));
        }
        pending.extend(node.children().into_iter().cloned());
    }
    None
}

/// Names the first file of a metadata-only scan the gate refuses.
fn describe_lazy_scan_leaf(exec: &LakeSoulScanExec) -> String {
    exec.work_units()
        .iter()
        .flat_map(|unit| unit.files.iter())
        .next()
        .map(|file| format!("'{}' ({})", file.path, file.physical_format))
        .unwrap_or_else(|| "a metadata-only scan without a readable format".to_string())
}

/// Names a scan leaf for the error: the first file it reads, plus the physical
/// format of that file when its extension says.
fn describe_scan_leaf(config: &FileScanConfig) -> String {
    let path = config
        .file_groups
        .iter()
        .flat_map(|group| group.files())
        .next()
        .map(|file| file.object_meta.location.to_string());
    match path {
        Some(path) => match PhysicalFormat::from_extension(&path) {
            Ok(format) => format!("'{path}' ({format})"),
            Err(_) => format!("'{path}'"),
        },
        None => "a file source without a plan codec".to_string(),
    }
}

/// The error a plan gets when no worker could execute a stage of it.
fn unshippable_stage_error(leaf: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "distributed execution requires a wire-encodable scan leaf, but a \
         worker stage scans {leaf}: that file source has no plan codec, so a \
         worker cannot decode it; tables written as parquet or vortex do have \
         one, so check the table's file_format, or run without distributed \
         execution"
    ))
}

/// Records the plan root and outcome on the current `distributed_plan` span,
/// so a distributed plan and a local fallback are told apart in the trace.
fn record_distributed_plan(plan: &Arc<dyn ExecutionPlan>, outcome: &'static str) {
    tracing::Span::current().record("physical_plan_root", plan.name());
    tracing::Span::current().record("outcome", outcome);
}

#[async_trait::async_trait]
impl QueryPlanner for LakeSoulDistributedQueryPlanner {
    #[tracing::instrument(
        name = "distributed_plan",
        level = "info",
        skip_all,
        fields(
            output_field_count = logical_plan.schema().fields().len(),
            physical_plan_root = tracing::field::Empty,
            outcome = tracing::field::Empty,
        ),
        err
    )]
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let started = std::time::Instant::now();
        if let Some(resolver) = &self.resolver {
            let available_workers = resolver.get_urls()?;
            info!(
                available_workers = available_workers.len(),
                ?available_workers,
                "distributed planning worker availability snapshot"
            );
        }
        // This is a required execution placement, not recovery from a planning
        // error: a runtime subquery's shared result/cardinality state must stay
        // in one coordinator plan. Do not split it into independent worker
        // writes, which would lose that state and can report `UPDATE 0`
        // instead of the subquery's typed error. The update planner keeps the
        // subquery as an expression (instead of a decorrelated join) for the
        // duration of the statement, so this check sees it; a query without
        // surviving subqueries keeps its workers.
        if write_has_subquery(logical_plan) {
            let plan = self
                .plan_locally(logical_plan, session, "write with runtime subquery")
                .await?;
            record_distributed_plan(&plan, "subquery_write_coordinator");
            end_current_otel_span();
            return Ok(plan);
        }

        let plan = match self
            .distributed
            .create_physical_plan(logical_plan, session)
            .await
        {
            Ok(plan) => plan,
            Err(e) => {
                error!("use distributed planner: {}", e);
                if !self.fallback_to_local {
                    return Err(e);
                }
                let plan = self
                    .plan_locally(logical_plan, session, "distributed planning failed")
                    .await?;
                record_distributed_plan(&plan, "local_fallback");
                end_current_otel_span();
                return Ok(plan);
            }
        };
        info!(
            wall_ms = started.elapsed().as_secs_f64() * 1000.0,
            "scan planning distributed conversion complete"
        );
        // A stage no worker could decode is a planning failure, not an
        // execution failure: the coordinator only fails when it sends the
        // stage, and the user then sees a worker-side timeout instead.
        if let Some(error) = unshippable_stage(&plan) {
            if !self.fallback_to_local {
                error!("{error}");
                return Err(error);
            }
            warn!("{error}; planning the query locally instead");
            let plan = self
                .plan_locally(logical_plan, session, "unshippable worker stage")
                .await?;
            record_distributed_plan(&plan, "local_fallback");
            end_current_otel_span();
            return Ok(plan);
        }
        record_distributed_plan(&plan, "distributed");
        info!(
            wall_ms = started.elapsed().as_secs_f64() * 1000.0,
            "scan planning distributed planner summary"
        );
        end_current_otel_span();
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
    };
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::NetworkShuffleExec;
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_io::file_format::LakeSoulFormatRegistry;
    use lakesoul_io::physical_plan::merge::MergeWorkUnit;
    use lakesoul_io::physical_plan::{
        LakeSoulScanConfig, MergeParquetExec, ScanFile, ScanWorkUnit,
    };

    use super::*;
    use crate::federation::external_file::{
        ExternalFileScanExec, ExternalFileStoreConfig,
    };

    /// Stands in for a distributed planner that cannot plan the query at all —
    /// the failure `fallback_to_local` covers.
    #[derive(Debug)]
    struct FailingPlanner;

    #[async_trait::async_trait]
    impl QueryPlanner for FailingPlanner {
        async fn create_physical_plan(
            &self,
            _: &LogicalPlan,
            _: &dyn Session,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::Execution(
                "distributed planner failed".to_string(),
            ))
        }
    }

    /// A distributed planner that plans every query as `plan`, so the gate can
    /// be tested against the plan shapes a worker would receive.
    #[derive(Debug)]
    struct StubPlanner(Arc<dyn ExecutionPlan>);

    #[async_trait::async_trait]
    impl QueryPlanner for StubPlanner {
        async fn create_physical_plan(
            &self,
            _: &LogicalPlan,
            _: &dyn Session,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::clone(&self.0))
        }
    }

    /// Plans `SELECT 1` through a gate wrapping `distributed`.
    async fn plan_through_gate(
        distributed: Arc<dyn QueryPlanner + Send + Sync>,
        fallback_to_local: bool,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let ctx = SessionContext::new();
        let frame = ctx.sql("SELECT 1 AS one").await?;
        let planner =
            LakeSoulDistributedQueryPlanner::new(distributed, fallback_to_local, None);
        planner
            .create_physical_plan(frame.logical_plan(), &ctx.state())
            .await
    }

    /// A scan leaf of `format` reading `file`, built through the same format
    /// registry the provider uses.
    fn scan_leaf(format: PhysicalFormat, file: &str) -> Arc<dyn ExecutionPlan> {
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_files(vec![file.to_string()])
            .build();
        let registry =
            LakeSoulFormatRegistry::new(io_config, false).expect("format registry");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let source = registry
            .file_format(format)
            .file_source(TableSchema::new(schema, Vec::new()));
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").expect("object store url"),
            source,
        )
        .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
            file.to_string(),
            1024,
        )])])
        .build();
        DataSourceExec::from_data_source(config)
    }

    /// A file source of a format no codec knows: a stand-in for a format added
    /// later, which the gate must refuse until it gets a wire form (see
    /// `codec::source_has_wire_form`).
    #[derive(Debug)]
    struct ForeignSource {
        table_schema: TableSchema,
        metrics: ExecutionPlanMetricsSet,
    }

    impl FileSource for ForeignSource {
        fn create_file_opener(
            &self,
            _object_store: Arc<dyn object_store::ObjectStore>,
            _base_config: &FileScanConfig,
            _partition: usize,
        ) -> DFResult<Arc<dyn datafusion_datasource::file_stream::FileOpener>> {
            unreachable!("the gate only inspects a foreign source, never opens one")
        }

        fn table_schema(&self) -> &TableSchema {
            &self.table_schema
        }

        fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
            Arc::new(ForeignSource {
                table_schema: self.table_schema.clone(),
                metrics: self.metrics.clone(),
            })
        }

        fn metrics(&self) -> &ExecutionPlanMetricsSet {
            &self.metrics
        }

        fn file_type(&self) -> &str {
            "foreign"
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
        ) -> DFResult<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
    }

    /// A scan leaf of a source with no wire form, reading `file`.
    fn foreign_leaf(file: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let source = ForeignSource {
            table_schema: TableSchema::new(schema, Vec::new()),
            metrics: ExecutionPlanMetricsSet::default(),
        };
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").expect("object store url"),
            Arc::new(source),
        )
        .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
            file.to_string(),
            1024,
        )])])
        .build();
        DataSourceExec::from_data_source(config)
    }

    /// A metadata-only scan leaf of `format` reading `file`, the leaf the
    /// table provider now plans: it has no children and carries its files as
    /// descriptors, so the gate cannot see it as a `DataSourceExec`.
    fn lazy_leaf(format: PhysicalFormat, file: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let config = LakeSoulScanConfig {
            table_schema: Arc::clone(&schema),
            output_schema: Arc::clone(&schema),
            partition_schema: Arc::new(Schema::empty()),
            io_config: LakeSoulIOConfigBuilder::new().build(),
            predicate: None,
            key_constraint: None,
            parquet_force_view_types: false,
            read_options: HashMap::new(),
            limit: None,
            min_index_rows: lakesoul_io::pk_locator::MIN_INDEX_ROWS,
        };
        let units = vec![ScanWorkUnit {
            files: vec![ScanFile {
                path: file.to_string(),
                size: Some(1024),
                physical_format: format,
                range: None,
            }],
            default_column_value: HashMap::new(),
            lane: 0,
        }];
        Arc::new(
            LakeSoulScanExec::try_new(config, units, 1)
                .expect("a valid metadata-only scan leaf"),
        )
    }

    /// Every physical format the metadata-only leaf reads has a worker
    /// reader, and the leaf carries its own codec, so a lazy scan stage is
    /// shippable. This is also the failure the opaque leaf would otherwise
    /// hide: it has no children, so only the explicit
    /// `lazy_scan_has_wire_form` arm of the gate sees it.
    #[tokio::test]
    async fn lazy_scan_stages_pass_the_gate() {
        for format in [
            PhysicalFormat::Parquet,
            PhysicalFormat::Vortex,
            PhysicalFormat::VortexCompact,
        ] {
            let leaf = stage(lazy_leaf(
                format,
                &format!("s3://bucket/t/a.{}", format.extension()),
            ));
            let plan = plan_through_gate(Arc::new(StubPlanner(Arc::clone(&leaf))), false)
                .await
                .unwrap_or_else(|err| panic!("{format}: {err}"));
            assert!(
                Arc::ptr_eq(&plan, &leaf),
                "{format}: the gate must keep the plan"
            );
        }
    }

    /// The per-task variants of a lazy scan are the plans a worker decodes, so
    /// the gate must inspect them as well.
    #[tokio::test]
    async fn lazy_scan_task_variants_pass_the_gate() {
        let variants = vec![
            lazy_leaf(PhysicalFormat::Parquet, "s3://bucket/t/a.parquet"),
            lazy_leaf(PhysicalFormat::Vortex, "s3://bucket/t/b.vortex"),
        ];
        let original = Arc::clone(&variants[0]);
        let variants = Arc::new(
            DistributedLeafExec::try_new(original, variants)
                .expect("the variants agree on schema and partitioning"),
        );
        let stage = stage(variants);
        let plan = plan_through_gate(Arc::new(StubPlanner(Arc::clone(&stage))), false)
            .await
            .expect("lazy task variants have a wire form");
        assert!(Arc::ptr_eq(&plan, &stage), "the gate must keep the plan");
    }

    /// A `MergeParquetExec` over `inputs`, the shape a primary-key work unit
    /// has: the merge node itself is encodable, its scan children may not.
    fn merge_over(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://bucket/t/file-0.parquet".to_string()])
            .with_primary_keys(vec!["v".to_string()])
            .with_merge_op("v".to_string(), "UseLast".to_string())
            .build();
        let work_units = vec![MergeWorkUnit {
            input_range: 0..inputs.len(),
            files: io_config.files_slice().to_vec(),
            default_column_value: Arc::new(HashMap::new()),
            lane: 0,
        }];
        Arc::new(
            MergeParquetExec::from_parts(
                schema,
                Arc::new(vec!["v".to_string()]),
                Arc::new(HashMap::new()),
                inputs,
                io_config,
                work_units,
                1,
            )
            .expect("valid single merge work unit"),
        )
    }

    /// A worker stage over `plan`: a hash `RepartitionExec` below a real
    /// `NetworkShuffleExec`, the node the distributed planner builds a stage
    /// boundary from.
    fn stage(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let partitioning = Partitioning::Hash(vec![Arc::new(Column::new("v", 0))], 2);
        let repartition =
            RepartitionExec::try_new(plan, partitioning).expect("hash repartition");
        Arc::new(
            NetworkShuffleExec::try_new(Arc::new(repartition), 2)
                .expect("a hash repartition is a valid boundary input"),
        )
    }

    /// A `DistributedLeafExec` over `variants`: the scan leaf of a distributed
    /// stage, whose per-task plans are not children.
    fn task_variants(variants: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        let original = scan_leaf(PhysicalFormat::Parquet, "s3://bucket/t/a.parquet");
        Arc::new(
            DistributedLeafExec::try_new(original, variants)
                .expect("the variants agree on schema and partitioning"),
        )
    }

    /// The development fallback: a distributed planner failure plans the query
    /// with the plain LakeSoul planner, so it runs on the coordinator.
    #[tokio::test]
    async fn fallback_plans_locally_when_the_distributed_planner_fails() {
        let plan = plan_through_gate(Arc::new(FailingPlanner), true)
            .await
            .expect("the fallback must plan the query");
        assert_eq!(plan.schema().field(0).name(), "one");
    }

    /// Production: the same failure fails the query, so it can never run
    /// unnoticed as a single-node query.
    #[tokio::test]
    async fn the_distributed_planner_error_propagates_without_the_fallback() {
        let err = plan_through_gate(Arc::new(FailingPlanner), false)
            .await
            .expect_err("no fallback must propagate the failure");
        assert!(
            err.to_string().contains("distributed planner failed"),
            "unexpected error: {err}"
        );
    }

    /// Positive control: a parquet stage is what the distributed codec ships,
    /// so the gate must pass the distributed plan through unchanged.
    #[tokio::test]
    async fn parquet_stages_pass_the_gate() {
        let leaf = stage(scan_leaf(
            PhysicalFormat::Parquet,
            "s3://bucket/t/a.parquet",
        ));
        let plan = plan_through_gate(Arc::new(StubPlanner(Arc::clone(&leaf))), false)
            .await
            .expect("a parquet stage must pass");
        assert!(Arc::ptr_eq(&plan, &leaf), "the gate must keep the plan");
    }

    /// A vortex stage crosses the wire through the registered vortex codec
    /// (`codec::user_codecs`), so the gate must pass the distributed plan
    /// through: `codec`'s roundtrip test pins that a vortex leaf really does
    /// encode and decode.
    #[tokio::test]
    async fn vortex_stages_pass_the_gate() {
        let leaf = stage(scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/a.vortex"));
        let plan = plan_through_gate(Arc::new(StubPlanner(Arc::clone(&leaf))), false)
            .await
            .expect("a vortex stage has a wire form");
        assert!(Arc::ptr_eq(&plan, &leaf), "the gate must keep the plan");
    }

    /// A `MergeParquetExec` stage is refused while planning: its payload only
    /// described spans into the children it merges, so it has no wire form the
    /// worker could decode. The refusal has to happen here, where
    /// `fallback_to_local` can still act on it, instead of when the stage is
    /// sent.
    #[tokio::test]
    async fn a_merge_stage_is_refused() {
        let leaf = scan_leaf(PhysicalFormat::VortexCompact, "s3://bucket/t/a.vortex");
        let stage = stage(merge_over(vec![leaf]));
        let error = plan_through_gate(Arc::new(StubPlanner(Arc::clone(&stage))), false)
            .await
            .expect_err("a merge stage has no wire form");
        assert!(
            error
                .to_string()
                .contains("MergeParquetExec has no wire form"),
            "{error}"
        );
    }

    /// A distributed stage hides the plan of each task inside a
    /// `DistributedLeafExec`, which exposes no children: the variants are what
    /// a worker receives, so they are what the check must inspect. The
    /// coordinator only ever executes the original (the single-task case), and
    /// here it is parquet while the task variants are vortex.
    #[tokio::test]
    async fn vortex_task_variants_pass_the_gate() {
        let variants = task_variants(vec![
            scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/a.vortex"),
            scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/b.vortex"),
        ]);
        let stage = stage(variants);
        let plan = plan_through_gate(Arc::new(StubPlanner(Arc::clone(&stage))), false)
            .await
            .expect("vortex task variants have a wire form");
        assert!(Arc::ptr_eq(&plan, &stage), "the gate must keep the plan");
    }

    /// Only stages are checked: a plan the distributed planner kept
    /// single-node runs on the coordinator, so its vortex leaf never reaches
    /// the wire and must not fail the query.
    #[tokio::test]
    async fn a_vortex_leaf_outside_any_stage_passes_the_gate() {
        let leaf = scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/a.vortex");
        let plan = plan_through_gate(Arc::new(StubPlanner(Arc::clone(&leaf))), false)
            .await
            .expect("a coordinator-only plan must pass");
        assert!(Arc::ptr_eq(&plan, &leaf), "the gate must keep the plan");
    }

    /// Production: a stage scanning a source no codec knows fails the query
    /// while planning instead of when the stage is sent to a worker.
    #[tokio::test]
    async fn unencodable_stage_is_refused_without_the_fallback() {
        let leaf = foreign_leaf("s3://bucket/t/a.foreign");
        let err = plan_through_gate(Arc::new(StubPlanner(stage(leaf))), false)
            .await
            .expect_err("a stage without a wire form must be refused");
        let message = err.to_string();
        assert!(message.contains("wire-encodable scan leaf"), "{message}");
        assert!(message.contains("a.foreign"), "{message}");
    }

    /// An external-file scan is a self-contained distributed leaf (its
    /// credential-carrying payload carries a nested Parquet scan). The gate
    /// must allow it even though it does not expose the inner `DataSourceExec`
    /// as a child.
    fn external_file_leaf(file: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let source = ParquetSource::new(TableSchema::new(schema, Vec::new()));
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("s3://bucket").expect("object store url"),
            Arc::new(source),
        )
        .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
            file.to_string(),
            1024,
        )])])
        .build();
        let inner = DataSourceExec::from_data_source(config);
        Arc::new(
            ExternalFileScanExec::try_new(
                inner,
                ObjectStoreUrl::parse("s3://bucket").expect("object store url"),
                ExternalFileStoreConfig {
                    endpoint: Some("http://127.0.0.1:9000".to_string()),
                    access_key_id: Some("access".to_string()),
                    secret_access_key: Some("secret".to_string()),
                    session_token: None,
                    region: Some("us-east-1".to_string()),
                    virtual_host_style: false,
                    allow_http: true,
                },
                "external".to_string(),
                file.to_string(),
            )
            .expect("external file scan"),
        )
    }

    #[test]
    fn external_file_wrapper_is_shippable() {
        let leaf = external_file_leaf("s3://bucket/t/a.parquet");
        assert!(unshippable_node(&leaf).is_none());
    }

    /// Development: the same stage is planned with the plain LakeSoul planner,
    /// so the query still runs on the coordinator.
    #[tokio::test]
    async fn unencodable_stage_falls_back_to_the_local_planner() {
        let leaf = foreign_leaf("s3://bucket/t/a.foreign");
        let plan = plan_through_gate(Arc::new(StubPlanner(stage(leaf))), true)
            .await
            .expect("the fallback must plan the query");
        assert_eq!(plan.schema().field(0).name(), "one");
    }

    /// Lists `.0` workers, or fails when it is `None`.
    struct Workers(Option<usize>);

    impl WorkerResolver for Workers {
        fn get_urls(&self) -> DFResult<Vec<url::Url>> {
            let Some(count) = self.0 else {
                return Err(DataFusionError::Execution("discovery failed".into()));
            };
            Ok((0..count)
                .map(|i| url::Url::parse(&format!("http://worker-{i}:50051")).unwrap())
                .collect())
        }
    }

    /// A scan packs its ranges for as many tasks as the distributed planner
    /// gives a stage: the configured per-stage cap, else the listed workers;
    /// one wherever the query is planned for one node.
    #[test]
    fn scan_task_budget_follows_the_stage_task_cap() {
        use datafusion_distributed::DistributedExt;

        assert_eq!(scan_task_budget(&SessionConfig::new()), 1, "no resolver");
        let workers =
            |count| SessionConfig::new().with_distributed_worker_resolver(Workers(count));
        assert_eq!(scan_task_budget(&workers(Some(3))), 3);
        assert_eq!(scan_task_budget(&workers(Some(0))), 1, "no worker listed");
        assert_eq!(scan_task_budget(&workers(None)), 1, "discovery failed");
        let capped = workers(Some(3))
            .with_distributed_max_tasks_per_stage(2)
            .unwrap();
        assert_eq!(scan_task_budget(&capped), 2, "max_tasks_per_stage");
        let fallback = workers(Some(3)).with_extension(Arc::new(CoordinatorPlanning));
        assert_eq!(scan_task_budget(&fallback), 1, "local fallback");
    }
}
