// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Policy gate in front of the distributed planner.
//!
//! The gate decides per query what a distributed session does when the
//! distributed planner cannot produce a *distributable* plan:
//!
//! - the distributed planner plans the query and every stage it would send to a
//!   worker has a wire form → its plan is used, whether or not it contains a
//!   distributed stage: the planner itself executes a query it is not worth
//!   distributing - including any query while no worker is ready - on the
//!   coordinator, and that is not a failure;
//! - the distributed planner fails, or plans a query whose worker stages cannot
//!   cross the wire, `fallback_to_local = true` (development) → plan the query
//!   with the plain LakeSoul planner and execute it single-node on the
//!   coordinator;
//! - either case with `fallback_to_local = false` (production) → the error
//!   fails the query instead of silently degrading it to coordinator-only
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
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource, ParquetSource};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{DistributedLeafExec, NetworkBoundaryExt};
use lakesoul_io::file_format::PhysicalFormat;

use crate::planner::LakeSoulQueryPlanner;

/// Wraps the distributed [`QueryPlanner`] installed by
/// `SessionStateBuilderExt::with_distributed_planner` and decides per query
/// whether distributed execution is possible.
pub struct LakeSoulDistributedQueryPlanner {
    /// The distributed planner to delegate to when workers are available.
    distributed: Arc<dyn QueryPlanner + Send + Sync>,
    local: Arc<dyn QueryPlanner + Send + Sync>,
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
    ) -> Self {
        Self {
            distributed,
            local: LakeSoulQueryPlanner::new_ref(),
            fallback_to_local,
        }
    }

    /// Plans the query with the plain LakeSoul planner, so it runs on the
    /// coordinator.
    async fn plan_locally(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        info!("fallback to local planner");
        self.local.create_physical_plan(logical_plan, session).await
    }
}

/// Whether a file source can be sent to a worker.
///
/// A stage travels as protobuf: its [`DataSourceExec`]s serialize themselves
/// through their data source, a [`FileScanConfig`] through its [`FileSource`],
/// and a source without a `try_to_proto` hook has no wire form. Parquet is the
/// only source LakeSoul builds that implements one, so it is the only source a
/// worker can decode. The check is a whitelist rather than "not a vortex path"
/// so a leaf of a future format is refused until it gets a codec, instead of
/// failing at execution time.
fn wire_encodable_source(source: &dyn FileSource) -> bool {
    source.is::<ParquetSource>()
}

/// Describes the first scan leaf of a worker stage that no worker could
/// receive.
///
/// Only the plan below a network boundary is sent to a worker: the
/// coordinator's own head stage runs in place, and a plan the distributed
/// planner decided to keep single-node has no boundary at all. The walk
/// therefore checks the children of every network boundary rather than the
/// whole plan, so a leaf that stays on the coordinator is not a failure.
fn unshippable_scan_leaf(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
    let mut found = None;
    plan.apply(|node| {
        if node.is_network_boundary() {
            // Everything below the boundary is the plan of one worker stage.
            // The boundary itself is encodable: a worker consumes another
            // stage's output through it, it does not execute it.
            for stage in node.children() {
                if let Some(leaf) = unshippable_file_scan(stage) {
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

/// The first file scan of `stage` whose source has no wire form, if any.
///
/// The scan leaf of a distributed stage is a [`DistributedLeafExec`], which
/// holds one variant per task and exposes none of them as a child, so its
/// variants - the plans a task receives; the coordinator specializes the leaf
/// to its variant before sending it - are walked explicitly.
///
/// Within a stage, `DataSourceExec`s over non-file sources (memory, …) are
/// skipped - they serialize themselves and have no files to read - and a leaf
/// below a `MergeParquetExec` (a primary-key work unit) is reached as well.
fn unshippable_file_scan(stage: &Arc<dyn ExecutionPlan>) -> Option<String> {
    let mut pending = vec![Arc::clone(stage)];
    while let Some(node) = pending.pop() {
        if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
            pending.extend(leaf.variants().iter().cloned());
            continue;
        }
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
            && !wire_encodable_source(config.file_source().as_ref())
        {
            return Some(describe_scan_leaf(config));
        }
        pending.extend(node.children().into_iter().cloned());
    }
    None
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
         worker cannot decode it; write the table as parquet (file_format \
         'parquet') or run without distributed execution"
    ))
}

#[async_trait::async_trait]
impl QueryPlanner for LakeSoulDistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
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
                return self.plan_locally(logical_plan, session).await;
            }
        };
        // A stage no worker could decode is a planning failure, not an
        // execution failure: the coordinator only fails when it sends the
        // stage, and the user then sees a worker-side timeout instead.
        if let Some(leaf) = unshippable_scan_leaf(&plan) {
            let error = unshippable_stage_error(&leaf);
            if !self.fallback_to_local {
                error!("{error}");
                return Err(error);
            }
            warn!("{error}; planning the query locally instead");
            return self.plan_locally(logical_plan, session).await;
        }
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::NetworkShuffleExec;
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_io::file_format::LakeSoulFormatRegistry;
    use lakesoul_io::physical_plan::MergeParquetExec;

    use super::*;

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
            LakeSoulDistributedQueryPlanner::new(distributed, fallback_to_local);
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

    /// A `MergeParquetExec` over `inputs`, the shape a primary-key work unit
    /// has: the merge node itself is encodable, its scan children may not be.
    fn merge_over(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let io_config = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://bucket/t/file-0.parquet".to_string()])
            .with_primary_keys(vec!["v".to_string()])
            .with_merge_op("v".to_string(), "UseLast".to_string())
            .build();
        Arc::new(MergeParquetExec::from_parts(
            schema,
            Arc::new(vec!["v".to_string()]),
            Arc::new(HashMap::new()),
            Arc::new(HashMap::new()),
            inputs,
            io_config,
        ))
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

    /// Production: a vortex stage has no wire form, so the query fails while
    /// planning instead of when the stage is sent to a worker.
    #[tokio::test]
    async fn vortex_stage_is_refused_without_the_fallback() {
        let leaf = scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/a.vortex");
        let err = plan_through_gate(Arc::new(StubPlanner(stage(leaf))), false)
            .await
            .expect_err("a vortex stage must be refused");
        let message = err.to_string();
        assert!(message.contains("wire-encodable scan leaf"), "{message}");
        assert!(message.contains("a.vortex"), "{message}");
    }

    /// The check walks a whole stage: a vortex leaf below the merge node of a
    /// primary-key work unit is found too.
    #[tokio::test]
    async fn vortex_leaf_below_a_merge_node_is_refused() {
        let leaf = scan_leaf(PhysicalFormat::VortexCompact, "s3://bucket/t/a.vortex");
        let err = plan_through_gate(
            Arc::new(StubPlanner(stage(merge_over(vec![leaf])))),
            false,
        )
        .await
        .expect_err("a vortex leaf below a merge must be refused");
        assert!(
            err.to_string().contains("wire-encodable scan leaf"),
            "{err}"
        );
    }

    /// A distributed stage hides the plan of each task inside a
    /// `DistributedLeafExec`, which exposes no children: the variants are what
    /// a worker receives, so they are what the check must inspect. The
    /// coordinator only ever executes the original (the single-task case), and
    /// here it is parquet while the task variants are vortex.
    #[tokio::test]
    async fn vortex_task_variant_is_refused() {
        let variants = task_variants(vec![
            scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/a.vortex"),
            scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/b.vortex"),
        ]);
        let err = plan_through_gate(Arc::new(StubPlanner(stage(variants))), false)
            .await
            .expect_err("a vortex task variant must be refused");
        assert!(
            err.to_string().contains("wire-encodable scan leaf"),
            "{err}"
        );
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

    /// Development: the same stage is planned with the plain LakeSoul planner,
    /// so a vortex table still runs on the coordinator.
    #[tokio::test]
    async fn vortex_stage_falls_back_to_the_local_planner() {
        let leaf = scan_leaf(PhysicalFormat::Vortex, "s3://bucket/t/a.vortex");
        let plan = plan_through_gate(Arc::new(StubPlanner(stage(leaf))), true)
            .await
            .expect("the fallback must plan the query");
        assert_eq!(plan.schema().field(0).name(), "one");
    }
}
