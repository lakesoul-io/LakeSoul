// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The dedicated incremental SQL entry.
//!
//! [`IvmSqlExecutor::execute`] interprets a single
//! `INSERT INTO <mv> SELECT ...` as **incremental maintenance** of the target
//! table: the first execution builds the view from the current source state,
//! later executions consume the source changelog and apply only the delta.
//! `INSERT OVERWRITE` keeps its full-overwrite semantics and recomputes the
//! view from scratch.  Ordinary SQL execution is untouched; callers opt in by
//! going through this executor.
//!
//! The target table must already exist and its schema must match the view the
//! SELECT defines.  A changed definition rebuilds the view: the generation is
//! bumped, cursors are reset and generated state tables are dropped and
//! recreated.

use std::collections::HashMap;
use std::fmt;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, SubqueryAlias};
use datafusion::prelude::{Expr, SessionContext};
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::ast::{
    ObjectName, ObjectNamePart, Query, Statement, TableObject, Visit, Visitor,
};

use crate::error::Result;
use crate::metadata::StateRole;
use crate::runtime::{
    ChainPolicy, IVM_AVG_COLUMN, IVM_COUNT_COLUMN, IVM_EPOCH_COLUMN,
    IVM_GROUP_ROW_COLUMN, IVM_NONNULL_COUNT_COLUMN, IVM_PAIR_IDENTITY_COLUMN,
    IVM_ROW_KINDS_COLUMN, IVM_SOURCE_COLUMN, IVM_SUM_COLUMN, IVM_VALUE_COLUMN,
    IvmRuntime, JoinOutputColumn, JoinSide, SinkColumn, ViewSpec,
    approx_distinct_groups_mv_schema_for, approx_percentile_groups_mv_schema_for,
    array_agg_groups_mv_schema_for, bool_agg_groups_mv_schema_for,
    computed_agg_mv_schema_for, cross_join_view_schema_for, decode_data_type,
    distinct_agg_groups_mv_schema_for, encode_data_type, full_join_view_schema_with_keys,
    grouping_sets_mv_schema_for, join_view_schema_with_keys,
    keyed_join_output_primary_keys, keyed_join_view_schema_with_keys,
    left_aggregate_mv_schema_with_outputs, left_join_view_schema_with_keys,
    lookup_chain_mv_schema_for, lookup_chain_step_id_column, lookup_join_view_schema_for,
    median_groups_mv_schema_for, min_max_groups_mv_schema_for, multi_agg_mv_schema_for,
    multi_agg_rows_state_schema_for, multi_agg_state_schema_for,
    multi_join_append_schema_for, multi_join_mv_schema_for, multi_join_primary_keys,
    multi_window_mv_schema_for_with_payload, nullable_join_key_schema, parse_filter,
    refresh_session, row_expr_mv_schema_for, semi_anti_mv_schema_for,
    string_agg_groups_mv_schema_for, sum_count_groups_mv_schema_for,
    top_k_expr_mv_schema_for, union_all_mv_schema_with_source,
    union_distinct_mv_schema_for, union_output_schema_for,
    value_count_groups_state_schema_for, variance_groups_mv_schema_for,
    wide_full_join_view_schema_with_identity_and_keys, wide_join_view_schema_with_keys,
    wide_keyed_join_view_schema_for, wide_keyed_join_view_schema_with_keys,
    wide_left_join_view_schema_with_identity_and_keys, wide_lookup_join_view_schema_for,
    wide_outer_join_view_schema_with_keys, window_columns_mv_schema_for_with_payload,
};
use crate::sql::{
    AnalyzeRequest, DerivedSource, UnsupportedShape, analyze_select, definition_hash,
    peel, plan_label,
};
use crate::table::{
    IvmTable, IvmTableOptions, create_ivm_table, internal_file_format,
    keyed_state_file_format, normalize_schema, state_range_partition_columns,
};

/// Placeholder state table id used to probe the query shape before the real
/// state table exists.
const STATE_PLACEHOLDER: &str = "__ivm_state_placeholder__";

/// Placeholder identity of a derived spec: the derived table does not exist
/// while its subplan is analyzed, so the executor patches the real ids
/// afterwards ([`ViewSpec::set_derived_identity`]).
const DERIVED_PLACEHOLDER: &str = "__ivm_derived__";

/// The maximum number of derived views one statement may materialize.
///
/// Production CTEs chain several compound sources (a union, joins and
/// aggregates) before the target shape is reachable.  The limit keeps a
/// pathological statement bounded without rejecting the common 7-18 CTE
/// shapes; a statement that really needs more should be split into explicit
/// views.
const DEFAULT_MAX_DERIVED_STEPS: usize = 32;

/// The derived-step budget.  A production statement with many CTEs may need
/// more than the default; the environment override keeps the limit explicit.
fn max_derived_steps() -> usize {
    std::env::var("LAKESOUL_IVM_MAX_DERIVED_STEPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_DERIVED_STEPS)
}

/// The hash bucket columns of an internal copy of a view.
///
/// The writer falls back to hashing the primary keys when no bucket columns
/// are set, so leaving the list empty produces bucket files that a keyed
/// lookup cannot address.  Record the same keys explicitly: the lookup then
/// routes a contribution to the bucket files that can contain it instead of
/// scanning the whole table.  Hashing the full key also keeps the physical
/// layout of tables written before this metadata was recorded.
fn bucket_columns_for(primary_keys: &[String]) -> Vec<String> {
    primary_keys.to_vec()
}

/// The table name of a derived step.
fn derived_step_name(target_name: &str, index: usize) -> String {
    format!("{target_name}__ivm_step{index}")
}

/// The table path of a derived step (a sibling directory of the target MV).
fn derived_step_path(target_path: &str, index: usize) -> String {
    format!("{}/__ivm_step{index}", target_path.trim_end_matches('/'))
}

/// A derived step materialized during one analysis, reusable when the same
/// subplan appears again (a CTE referenced several times).
#[derive(Clone)]
struct CachedDerived {
    table: IvmTable,
    view_id: String,
    mapping: Vec<(String, String)>,
}

fn plan_key(plan: &LogicalPlan) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in format!("{plan:?}").bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// The compact join shapes name their payload columns `left_value` /
/// `right_value`; a derived view must expose the subplan's column names, so
/// the compact shape is widened to one named output column per payload.  A
/// payload the select list renamed keeps that name (the analyzer recorded it
/// in `payload_names`), because the consuming plan references it.
fn widen_derived_join(spec: &mut ViewSpec) {
    let (output_columns, left_value, right_value, payload_names) = match spec {
        ViewSpec::Join {
            output_columns,
            left_value,
            right_value,
            payload_names,
            ..
        }
        | ViewSpec::LookupJoin {
            output_columns,
            left_value,
            right_value,
            payload_names,
            ..
        }
        | ViewSpec::CrossJoin {
            output_columns,
            left_value,
            right_value,
            payload_names,
            ..
        }
        | ViewSpec::LeftJoin {
            output_columns,
            left_value,
            right_value,
            payload_names,
            ..
        }
        | ViewSpec::FullJoin {
            output_columns,
            left_value,
            right_value,
            payload_names,
            ..
        } => (output_columns, left_value, right_value, payload_names),
        _ => return,
    };
    if !output_columns.is_empty() {
        return;
    }
    let names = if payload_names.len() == 2 {
        (payload_names[0].clone(), payload_names[1].clone())
    } else {
        (left_value.clone(), right_value.clone())
    };
    *output_columns = vec![
        JoinOutputColumn {
            side: JoinSide::Left,
            column: left_value.clone(),
            name: names.0,
        },
        JoinOutputColumn {
            side: JoinSide::Right,
            column: right_value.clone(),
            name: names.1,
        },
    ];
}

/// The subplan of a derived source in an analyzer error, if any.
fn derived_source(error: &rootcause::Report) -> Option<LogicalPlan> {
    error
        .attachments()
        .iter()
        .find_map(|attachment| attachment.downcast_inner::<DerivedSource>())
        .map(|source| source.plan.clone())
}

/// Whether the analysis failed because the analyzer does not maintain the
/// statement's shape (as opposed to a broken statement, an unknown table or a
/// schema mismatch), which the Tier-2 fallback may recompute in full.
fn unsupported_shape(error: &rootcause::Report) -> bool {
    error
        .attachments()
        .iter()
        .any(|attachment| attachment.downcast_inner::<UnsupportedShape>().is_some())
}

/// The one-line reason a statement fell back, for the view metadata.
fn rejection_reason(error: &rootcause::Report) -> String {
    error
        .to_string()
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.trim_start_matches('●').trim().to_string())
        .unwrap_or_default()
}

/// The MV schema a Tier-2 fallback statement must produce: its own output
/// columns plus the row kind and epoch columns every MV carries.
fn fallback_mv_schema(plan: &LogicalPlan) -> Result<SchemaRef> {
    let mut fields = plan
        .schema()
        .fields()
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable(),
            )) as arrow_schema::FieldRef
        })
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        IVM_ROW_KINDS_COLUMN,
        DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// A scan of a derived MV table, for the rewritten plan.
fn derived_scan(table: &IvmTable, runtime: &IvmRuntime) -> Result<LogicalPlan> {
    let provider: Arc<dyn TableProvider> = Arc::new(runtime.table_provider(table));
    Ok(LogicalPlanBuilder::scan(
        table.table_name.as_str(),
        provider_as_source(provider),
        None,
    )?
    .build()?)
}

/// Rename the column references of a rewritten plan: the materialized
/// column of a derived view can differ from the subplan output it replaces
/// (the aggregate layouts), so every reference to a subplan field is pointed
/// at the MV column.  The mapping only holds non-identity entries.
fn rename_derived_columns(
    plan: LogicalPlan,
    mapping: &HashMap<String, String>,
) -> Result<LogicalPlan> {
    let transformed = plan
        .transform_up(|node| {
            node.map_expressions(|expr| {
                let renamed = expr.transform_down(|node| match &node {
                    Expr::Column(column) => match mapping.get(&column.name) {
                        Some(name) => Ok(Transformed::yes(Expr::Column(
                            datafusion::common::Column::from_name(name),
                        ))),
                        None => Ok(Transformed::no(node)),
                    },
                    _ => Ok(Transformed::no(node)),
                })?;
                Ok(renamed)
            })
        })
        .map_err(|error| rootcause::report!("renaming the derived columns: {error}"))?;
    Ok(transformed.data)
}

/// Whether a subplan can be materialized as a derived view: the join shapes
/// and any aggregate the analyzer itself maintains (a mixed aggregate becomes
/// a multi-aggregate step).  A rejected shape carries the fallback marker, so
/// a Tier-2 target recomputes the statement in full instead of failing.
fn derived_step_supported(subplan: &LogicalPlan) -> Result<()> {
    match peel(subplan) {
        // A projection over a join is the subquery form of a join side; the
        // analyzer reads it for the output columns and the projection stays
        // part of the step, so the parent's references keep resolving.
        // A projection over a materializable subplan is the subquery form of
        // an operator source; the projection stays part of the step, so the
        // parent's references keep resolving.
        LogicalPlan::Projection(projection) => derived_step_supported(&projection.input),
        // Every join type works as a derived side: an outer join materializes
        // the non-null row identity its consumer needs.  A `Sort` with a fetch
        // and a `Limit` are the top-k sources, and a `Distinct` subquery is a
        // row-distinct source.
        LogicalPlan::Join(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::TableScan(_) => Ok(()),
        LogicalPlan::Unnest(_) => Err(crate::sql::unnest_unsupported()),
        other => Err(rootcause::report!(
            "a derived view over {} is not materialized yet",
            plan_label(other)
        )
        .attach(UnsupportedShape)),
    }
}

/// The materialized column of one aggregate output of a sum-count step.
fn sum_count_column(expr: &Expr) -> Result<&'static str> {
    let Expr::AggregateFunction(function) = expr else {
        return Err(rootcause::report!(
            "a derived view over a non-aggregate output is not materialized yet"
        )
        .attach(UnsupportedShape));
    };
    match function.func.name() {
        "sum" => Ok(IVM_SUM_COLUMN),
        "count" => {
            if matches!(
                function.params.args.as_slice(),
                [Expr::Literal(ScalarValue::Int64(Some(1)), _)]
            ) {
                Ok(IVM_COUNT_COLUMN)
            } else {
                Ok(IVM_NONNULL_COUNT_COLUMN)
            }
        }
        "avg" => Ok(IVM_AVG_COLUMN),
        other => Err(rootcause::report!(
            "a derived view over {other}(...) is not materialized yet"
        )),
    }
}

/// The rename mapping of a derived step: every subplan output field and the
/// MV column that materializes it.
fn derived_output_mapping(
    spec: &ViewSpec,
    subplan: &LogicalPlan,
) -> Result<Vec<(String, String)>> {
    let fields = subplan.schema().fields();
    if matches!(spec, ViewSpec::SumCount { .. }) {
        // The optimizer may prune a group key above the aggregate while the
        // MV still materializes it; peel plain projections and map fields by
        // their aggregate output name.
        let mut node = peel(subplan);
        let mut projection: Option<&datafusion::logical_expr::Projection> = None;
        while let LogicalPlan::Projection(inner) = node {
            if !inner.expr.iter().all(|expr| {
                let mut value = expr;
                while let Expr::Alias(alias) = value {
                    value = &alias.expr;
                }
                matches!(value, Expr::Column(_))
            }) {
                break;
            }
            projection = Some(inner);
            node = peel(&inner.input);
        }
        let LogicalPlan::Aggregate(aggregate) = node else {
            return Err(rootcause::report!(
                "a derived sum-count view needs an aggregate input"
            )
            .attach(UnsupportedShape));
        };
        let groups = aggregate.group_expr.len();
        let mut targets = HashMap::new();
        for field in aggregate.schema.fields().iter().take(groups) {
            targets.insert(field.name().clone(), field.name().clone());
        }
        for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
            targets.insert(
                expr.schema_name().to_string(),
                sum_count_column(expr)?.to_string(),
            );
            if index + groups >= aggregate.schema.fields().len() {
                break;
            }
        }
        let mut mapping = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            let source =
                match projection.and_then(|projection| projection.expr.get(index)) {
                    Some(expr) => {
                        let mut value = expr;
                        while let Expr::Alias(alias) = value {
                            value = &alias.expr;
                        }
                        match value {
                            Expr::Column(column) => column.name.clone(),
                            _ => field.name().clone(),
                        }
                    }
                    None => field.name().clone(),
                };
            let column = targets
                .get(&source)
                .cloned()
                .unwrap_or_else(|| source.clone());
            mapping.push((field.name().clone(), column));
        }
        return Ok(mapping);
    }
    if let ViewSpec::Window { columns, .. } = spec {
        // The trailing subplan fields are the window outputs; the leading
        // input columns that the MV does not keep stay unmapped and fail
        // naturally when a consumer references them.
        let outputs = columns.len();
        let start = fields.len().saturating_sub(outputs);
        let mut mapping = Vec::with_capacity(outputs);
        for (index, field) in fields.iter().enumerate().skip(start) {
            mapping.push((field.name().clone(), columns[index - start].column.clone()));
        }
        return Ok(mapping);
    }
    if let ViewSpec::MultiWindow { windows, .. } = spec {
        // The chain nodes are materialized top-down: the analyzer collected
        // them from the subplan's top window, and every node's trailing
        // outputs are that clause's window columns.  The leading input columns
        // the MV does not keep stay unmapped.
        let mut mapping = Vec::new();
        let mut node = peel(subplan);
        for group in windows {
            let LogicalPlan::Window(window) = node else {
                return Err(rootcause::report!(
                    "a derived multi-window view needs its window chain"
                )
                .attach(UnsupportedShape));
            };
            let fields = node.schema().fields();
            let outputs = group.columns.len();
            if fields.len() < outputs {
                return Err(rootcause::report!(
                    "a derived multi-window view does not expose its columns"
                )
                .attach(UnsupportedShape));
            }
            let start = fields.len() - outputs;
            for (index, field) in fields.iter().enumerate().skip(start) {
                mapping.push((
                    field.name().clone(),
                    group.columns[index - start].column.clone(),
                ));
            }
            node = match peel(&window.input) {
                LogicalPlan::Projection(projection) => peel(&projection.input),
                other => other,
            };
        }
        return Ok(mapping);
    }
    if let ViewSpec::MultiAgg {
        group_keys,
        aggregates,
        ..
    } = spec
    {
        // A mixed aggregate step keeps the group keys by name and materializes
        // one column per aggregate.  A computed select list may drop a group
        // key or put a computed output before a raw aggregate, so resolve by
        // name first and use the positional layout only as a fallback.
        let mut mapping = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            let name = field.name();
            let column = if group_keys.contains(name) {
                name.clone()
            } else if let Some(aggregate) = aggregates
                .iter()
                .find(|aggregate| &aggregate.column == name)
            {
                aggregate.column.clone()
            } else if let Some(aggregate) =
                aggregates.iter().find(|aggregate| &aggregate.call == name)
            {
                aggregate.column.clone()
            } else {
                match index.checked_sub(group_keys.len()) {
                    None => group_keys
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| name.clone()),
                    Some(position) => aggregates
                        .get(position)
                        .map(|aggregate| aggregate.column.clone())
                        .unwrap_or_else(|| name.clone()),
                }
            };
            mapping.push((name.clone(), column));
        }
        return Ok(mapping);
    }
    if let ViewSpec::MinMax { group_keys, .. }
    | ViewSpec::DistinctAgg { group_keys, .. } = spec
    {
        // The value-count layouts materialize the single aggregate under
        // `value`; the group keys keep their names.  The distinct rewrite's
        // internal dedup key is neither and stays unmapped.
        let mut mapping = Vec::with_capacity(fields.len());
        let last = fields.len().saturating_sub(1);
        for (index, field) in fields.iter().enumerate() {
            if index == last {
                mapping.push((field.name().clone(), IVM_VALUE_COLUMN.to_string()));
            } else if group_keys.contains(field.name()) {
                mapping.push((field.name().clone(), field.name().clone()));
            }
        }
        return Ok(mapping);
    }
    if let ViewSpec::LookupChain { output_columns, .. } = spec
        && fields.len() == output_columns.len()
    {
        // The chain materializes the projection in order, so a column whose
        // layout name was uniquified still maps from the subplan's field.
        return Ok(fields
            .iter()
            .zip(output_columns)
            .map(|(field, column)| (field.name().clone(), column.name.clone()))
            .collect());
    }
    Ok(fields
        .iter()
        .map(|field| (field.name().clone(), field.name().clone()))
        .collect())
}

/// Replace every occurrence of `target` in `plan` with `replacement`.
///
/// Identical subplans map to the same derived view, so replacing all of their
/// occurrences is the intended behavior.
fn replace_plan(
    plan: &LogicalPlan,
    target: &LogicalPlan,
    replacement: &LogicalPlan,
) -> Result<LogicalPlan> {
    let transformed = plan
        .clone()
        .transform_up(|node| {
            if &node == target {
                // A marker may cover a subquery alias (`(SELECT ...) x`);
                // keep the alias around the replacement so the parent's
                // references like `x.k` still resolve.
                if let LogicalPlan::SubqueryAlias(alias) = &node {
                    let aliased = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                        Arc::new(replacement.clone()),
                        alias.alias.clone(),
                    )?);
                    return Ok(Transformed::yes(aliased));
                }
                Ok(Transformed::yes(replacement.clone()))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map_err(|error| rootcause::report!("rewriting the derived source: {error}"))?;
    Ok(transformed.data)
}

/// What an execution did to the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IvmExecutionAction {
    /// The view did not exist yet; it was built from the current source state.
    Bootstrap,
    /// The view existed with the same definition; only the delta was applied.
    Incremental,
    /// The definition changed (or an overwrite hit a new definition); the view
    /// was rebuilt from the current source state.
    Rebuild,
    /// `INSERT OVERWRITE` with an unchanged definition: the view was recomputed
    /// from the current source state.
    Overwrite,
    /// The statement's shape is not maintained incrementally: the view was
    /// recomputed in full (Tier-2).
    Fallback,
}

impl IvmExecutionAction {
    /// A stable lowercase label for the action, used by the metrics.
    pub fn label(self) -> &'static str {
        match self {
            IvmExecutionAction::Bootstrap => "bootstrap",
            IvmExecutionAction::Incremental => "incremental",
            IvmExecutionAction::Rebuild => "rebuild",
            IvmExecutionAction::Overwrite => "overwrite",
            IvmExecutionAction::Fallback => "fallback",
        }
    }
}

impl fmt::Display for IvmExecutionAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            IvmExecutionAction::Bootstrap => "bootstrap",
            IvmExecutionAction::Incremental => "incremental",
            IvmExecutionAction::Rebuild => "rebuild",
            IvmExecutionAction::Overwrite => "overwrite",
            IvmExecutionAction::Fallback => "fallback",
        })
    }
}

/// The outcome of one [`IvmSqlExecutor::execute`] call.
#[derive(Debug, Clone)]
pub struct IvmExecution {
    /// What happened to the view.
    pub action: IvmExecutionAction,
    /// The epoch of the executed refresh window, when one ran.
    pub epoch: Option<i64>,
    /// The view id (the target table id).
    pub view_id: String,
    /// The definition identity that is now registered.
    pub definition_hash: String,
}

/// The dedicated `INSERT INTO`/`INSERT OVERWRITE` entry for incremental views.
pub struct IvmSqlExecutor {
    runtime: IvmRuntime,
    session: Option<SessionContext>,
    refresh_upstream: bool,
}

impl IvmSqlExecutor {
    /// Build an executor that plans SQL in an internal DataFusion session.
    pub fn new(runtime: IvmRuntime) -> Self {
        Self {
            runtime,
            session: None,
            refresh_upstream: false,
        }
    }

    /// Refresh the views the statement reads (upstream first) before running
    /// it.  Off by default: a statement only reads their current state, and a
    /// scheduler drives the chain through
    /// [`IvmRuntime::refresh_view_chain`](crate::IvmRuntime::refresh_view_chain).
    ///
    /// When enabled, the referenced views are brought up to date in one
    /// traversal with the default [`ChainPolicy`]: retried incrementally,
    /// rebuilt on a persistent failure or when marked dirty, and the
    /// statement is rejected when a view cannot be brought up to date.
    pub fn with_refresh_upstream(mut self, enabled: bool) -> Self {
        self.refresh_upstream = enabled;
        self
    }

    /// Use the caller's session for planning (its catalog must be able to
    /// resolve the tables the statement references).
    pub fn with_session(mut self, session: SessionContext) -> Self {
        self.session = Some(session);
        self
    }

    /// The runtime behind the executor.
    pub fn runtime(&self) -> &IvmRuntime {
        &self.runtime
    }

    /// Execute one incremental statement.
    pub async fn execute(&self, sql: &str) -> Result<IvmExecution> {
        let started = std::time::Instant::now();
        // Boxed: the statement future nests the refresh, the lakesoul-io
        // reader and its moka file cache by value, which puts its layout just
        // under rustc's default query depth limit (128). Without the box any
        // caller awaiting `execute` overflows it.
        let result = Box::pin(self.execute_inner(sql)).await;
        let action = match &result {
            Ok(execution) => execution.action.label(),
            Err(_) => "error",
        };
        crate::observability::record_statement(action, started.elapsed());
        result
    }

    async fn execute_inner(&self, sql: &str) -> Result<IvmExecution> {
        // Flink task SQL may carry temporal-join syntax; these tasks use a
        // primary-key dimension lookup, so it is planned as a normal join.
        let normalized = crate::sql::normalize_flink_sql(sql);
        let sql = normalized.as_str();
        let insert = parse_insert(sql)?;
        let (namespace, table_name) = split_object_name(insert_target(&insert)?)?;
        let mv = self.runtime.open_table(&table_name, &namespace).await?;
        let view_id = mv.table_id.clone();

        let source = insert.source.as_deref().ok_or_else(|| {
            rootcause::report!(
                "only `INSERT INTO <table> SELECT ...` is incremental; INSERT ... VALUES is not"
            )
        })?;
        let select_sql = source.to_string();

        // Open every relation the SELECT references and, without a caller
        // session, register it in an internal one.
        let mut relations = Vec::new();
        let _ = source.visit(&mut RelationCollector::new(&mut relations));
        let session = self.session.clone().unwrap_or_default();
        let mut tables = HashMap::new();
        for relation in relations {
            let (namespace, name) = split_object_name(&relation)?;
            let table = self.runtime.open_table(&name, &namespace).await?;
            if self.session.is_none() {
                let provider: Arc<dyn TableProvider> =
                    Arc::new(self.runtime.table_provider(&table));
                let _ = session.register_table(name.as_str(), provider.clone());
                let _ = session.register_table(relation.to_string().as_str(), provider);
            }
            tables.insert(name, table);
        }

        let plan = session
            .state()
            .create_logical_plan(&select_sql)
            .await
            .map_err(|error| rootcause::report!("plan `{select_sql}`: {error}"))?;
        let plan = crate::sql::normalize_quantified_comparisons(&plan)?;
        let plan = session
            .state()
            .optimize(&plan)
            .map_err(|error| rootcause::report!("optimize `{select_sql}`: {error}"))?;

        let request = AnalyzeRequest {
            view_id: view_id.clone(),
            mv_table_id: mv.table_id.clone(),
            state_table_id: Some(STATE_PLACEHOLDER.to_string()),
            derived: false,
        };
        // Compound sources the statement reads are materialized as derived
        // views (internal keyed MVs) before the outer analysis succeeds; the
        // derived steps are idempotent by name, while the target checks below
        // still run before the target itself is touched.
        let mut derived = Vec::new();
        let mut derived_cache = HashMap::new();
        let analyzed = self
            .analyze_with_derived(
                &plan,
                &mut tables,
                &request,
                &mv,
                &mut derived,
                &mut derived_cache,
            )
            .await;
        let (probe, fallback, expected, plan) = match analyzed {
            Ok((probe, rewritten)) => {
                let expected = expected_mv_schema(&probe, &tables)?;
                (probe, false, expected, rewritten)
            }
            Err(error) if unsupported_shape(&error) && mv.allow_fallback => {
                // Tier-2: the analyzer does not maintain the shape, so the
                // statement is recomputed in full on every refresh.  The
                // derived steps a failed analysis materialized are not part
                // of the fallback definition.
                if !derived.is_empty() {
                    self.drop_stale_derived(&mv, 0).await?;
                }
                // The definition hash must not depend on the map's iteration
                // order: the source set is a set.
                let mut source_ids = tables
                    .values()
                    .filter(|table| !derived.contains(&table.table_id))
                    .map(|table| table.table_id.clone())
                    .collect::<Vec<_>>();
                source_ids.sort();
                // Whether a refresh can recompute single partitions is a
                // property of the statement; record the decision (and why not)
                // with the shape rejection.
                let scope = crate::runtime::fallback_partition_scope(&plan, &tables, &mv);
                let probe = ViewSpec::Fallback {
                    view_id: view_id.clone(),
                    statement: select_sql.clone(),
                    tables: source_ids,
                    reason: format!("{}; {}", rejection_reason(&error), scope.note),
                    partition_scope: scope.scope,
                    mv_table_id: mv.table_id.clone(),
                };
                let expected = fallback_mv_schema(&plan)?;
                (probe, true, expected, plan.clone())
            }
            Err(error) => return Err(error),
        };
        // A target whose schema differs from the maintenance state, or whose
        // range partition columns are not part of its primary key, cannot be
        // written directly.  Materialize the maintenance state and project it
        // through a sink instead.
        if target_needs_sink(&mv, &expected) {
            if fallback {
                let reason = match &probe {
                    ViewSpec::Fallback { reason, .. } => {
                        format!(" (the shape is not maintained incrementally: {reason})")
                    }
                    _ => String::new(),
                };
                return Err(rootcause::report!(
                    "target table `{}` does not match the view schema of this statement{reason}",
                    mv.table_name
                ));
            }
            return self
                .execute_split_target(
                    &mv,
                    &view_id,
                    plan,
                    tables,
                    sql,
                    insert.overwrite,
                    derived,
                )
                .await;
        }
        // The definition hash ignores generated table ids, so the probe is a
        // valid identity even before the state table exists.
        let definition = definition_hash(&probe)?;
        let stored = self.runtime.metadata().view_definition(&view_id).await?;
        let same_definition =
            stored.as_ref().is_some_and(|(hash, _)| hash == &definition);

        // Validate before the target is touched: a statement that cannot
        // maintain this target must not create or drop its state tables.
        if !schema_matches(&mv.schema, &expected) {
            // A fallback reports the shape rejection too: the user needs to
            // know why the statement is recomputed in full (and why its
            // output columns must match the MV exactly).
            let reason = match &probe {
                ViewSpec::Fallback { reason, .. } => {
                    format!(" (the shape is not maintained incrementally: {reason})")
                }
                _ => String::new(),
            };
            return Err(rootcause::report!(
                "target table `{}` does not match the view schema of this statement{reason}",
                mv.table_name
            ));
        }
        // A range-partitioned MV must carry its partition columns in the
        // primary key: merge-on-read runs within a partition, so a key that
        // spans partitions would read back as several rows.
        if !mv.range_partition_columns.is_empty()
            && !mv
                .range_partition_columns
                .iter()
                .all(|column| mv.primary_keys.contains(column))
        {
            return Err(rootcause::report!(
                "target table `{}` is range partitioned by [{}]: its primary keys must contain every partition column",
                mv.table_name,
                mv.range_partition_columns.join(", ")
            ));
        }
        // The keyed contract: the MV's primary key is the merge key that
        // resolves the delete markers a refresh writes, so a keyed statement
        // needs one.  A global aggregate rewrites its single row wholesale and
        // needs no key; a Tier-2 fallback truncates and rewrites the whole MV,
        // so it may be append-only.
        if !fallback && mv.primary_keys.is_empty() && !is_global_aggregate(&probe) {
            return Err(rootcause::report!(
                "target table `{}` needs a primary key: the view's refresh writes \
                 delete markers that the MV's primary key merges; declare the key \
                 columns when creating the table",
                mv.table_name
            ));
        }

        // A changed definition invalidates the generated state: drop it before
        // the new spec creates (and registers) its own.
        if stored.is_some() && !same_definition {
            self.drop_generated_state(&view_id).await?;
        }
        let mut generated_state_id = None;
        let spec = if !fallback && needs_state_table(&probe) {
            let state_table_id = self.ensure_state_table(&mv, &probe).await?;
            generated_state_id = Some(state_table_id.clone());
            let request = AnalyzeRequest {
                state_table_id: Some(state_table_id),
                ..request
            };
            self.analyze_with_derived(
                &plan,
                &mut tables,
                &request,
                &mv,
                &mut derived,
                &mut derived_cache,
            )
            .await?
            .0
        } else {
            probe
        };
        let mut spec = spec;
        if let Some(state_table_id) = generated_state_id {
            spec.set_derived_state_table(&state_table_id)?;
        }
        if definition_hash(&spec)? != definition {
            return Err(rootcause::report!(
                "internal error: the definition identity changed during analysis"
            ));
        }

        // A cascading view reads other materialized views.  By default the
        // statement only reads their current state; with the opt-in flag the
        // views it reads are brought up to date first, in one traversal (the
        // policy retries, rebuilds and marks a stuck view dirty).
        if self.refresh_upstream {
            let roots = tables
                .values()
                .map(|table| table.table_id.clone())
                .collect::<Vec<_>>();
            self.runtime
                .refresh_view_chains(&roots, &ChainPolicy::default())
                .await?;
        }

        // A view marked dirty (a failed chain refresh) has no trusted
        // incremental state: it is rebuilt like a changed definition.
        let dirty = self
            .runtime
            .metadata()
            .view_status(&view_id)
            .await?
            .as_deref()
            == Some("dirty");
        let (action, epoch) = if fallback {
            // Every fallback refresh recomputes the statement; the action says
            // so even when the definition is unchanged.  A no-op refresh (no
            // source change) reports no epoch.
            if same_definition && !insert.overwrite && !dirty {
                match self.runtime.refresh_spec(&spec).await {
                    Ok(epoch) => (IvmExecutionAction::Fallback, epoch),
                    // A dropped source partition whose value the view carries
                    // is retracted deterministically; everything else rebuilds.
                    Err(error) if IvmRuntime::rebuild_required(&error) => {
                        match self
                            .runtime
                            .cleanup_dropped_partitions(&spec, &error)
                            .await?
                        {
                            Some(epoch) => (IvmExecutionAction::Fallback, Some(epoch)),
                            None => {
                                let epoch = self.runtime.rebuild_spec(&spec).await?;
                                (IvmExecutionAction::Fallback, Some(epoch))
                            }
                        }
                    }
                    Err(error) => return Err(error),
                }
            } else {
                let epoch = self.runtime.rebuild_spec(&spec).await?;
                self.runtime
                    .metadata()
                    .set_view_definition(&view_id, &definition, sql)
                    .await?;
                (IvmExecutionAction::Fallback, Some(epoch))
            }
        } else if insert.overwrite {
            if same_definition {
                let epoch = self.runtime.rebuild_spec(&spec).await?;
                (IvmExecutionAction::Overwrite, Some(epoch))
            } else {
                let epoch = self.runtime.rebuild_spec(&spec).await?;
                self.runtime
                    .metadata()
                    .set_view_definition(&view_id, &definition, sql)
                    .await?;
                (IvmExecutionAction::Rebuild, Some(epoch))
            }
        } else if same_definition && !dirty {
            match self.runtime.refresh_spec(&spec).await {
                Ok(epoch) => (IvmExecutionAction::Incremental, epoch),
                // A dropped source partition (or a source window that needs a
                // snapshot) cannot be retried through the file window: rebuild
                // the view from the current state.
                Err(error) if IvmRuntime::rebuild_required(&error) => {
                    // A dropped source partition whose columns are part of the
                    // view's keys is removed deterministically; everything else
                    // still rebuilds.
                    match self
                        .runtime
                        .cleanup_dropped_partitions(&spec, &error)
                        .await?
                    {
                        Some(epoch) => (IvmExecutionAction::Incremental, Some(epoch)),
                        None => {
                            let epoch = self.runtime.rebuild_spec(&spec).await?;
                            (IvmExecutionAction::Rebuild, Some(epoch))
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        } else {
            let action = if stored.is_some() {
                IvmExecutionAction::Rebuild
            } else {
                IvmExecutionAction::Bootstrap
            };
            let epoch = self.runtime.rebuild_spec(&spec).await?;
            self.runtime
                .metadata()
                .set_view_definition(&view_id, &definition, sql)
                .await?;
            (action, Some(epoch))
        };
        if dirty {
            // The rebuild recovered the target; clear its failure marker.
            self.runtime
                .metadata()
                .set_view_status(&view_id, "active")
                .await?;
            self.runtime
                .metadata()
                .set_view_error(&view_id, None)
                .await?;
        }
        // A changed definition may leave derived steps behind: drop them
        // (their content is regenerated when a later definition needs the
        // step again).
        if stored.is_some() && !same_definition {
            self.drop_stale_derived(&mv, derived.len()).await?;
        }

        Ok(IvmExecution {
            action,
            epoch,
            view_id,
            definition_hash: definition,
        })
    }

    /// Execute a statement through an internal maintenance state and a target
    /// projection sink.  This path is used when the user target schema (or its
    /// primary key/partition layout) cannot be the maintenance schema itself.
    async fn execute_split_target(
        &self,
        target: &IvmTable,
        view_id: &str,
        plan: LogicalPlan,
        mut tables: HashMap<String, IvmTable>,
        sql: &str,
        overwrite: bool,
        mut derived: Vec<String>,
    ) -> Result<IvmExecution> {
        if self.refresh_upstream {
            let roots = tables
                .values()
                .map(|table| table.table_id.clone())
                .collect::<Vec<_>>();
            self.runtime
                .refresh_view_chains(&roots, &ChainPolicy::default())
                .await?;
        }
        // A maintenance state is an internal state view, so analyze it like a
        // derived step.  This keeps select aliases and widens the state with
        // the row identities the sink must not expose.
        let request = AnalyzeRequest {
            view_id: target.table_id.clone(),
            mv_table_id: target.table_id.clone(),
            state_table_id: Some(STATE_PLACEHOLDER.to_string()),
            derived: true,
        };
        let mut derived_cache = HashMap::new();
        let (state_spec, _) = self
            .analyze_with_derived(
                &plan,
                &mut tables,
                &request,
                target,
                &mut derived,
                &mut derived_cache,
            )
            .await?;
        let expected = expected_mv_schema(&state_spec, &tables)?;
        // Flink `INSERT INTO` maps the select list to the target by position;
        // the names may differ (`spec_model` -> `spec_model_no`).
        let internal =
            |name: &str| name == IVM_ROW_KINDS_COLUMN || name.starts_with("__ivm_");
        let target_fields = target
            .schema
            .fields()
            .iter()
            .filter(|field| !internal(field.name()))
            .collect::<Vec<_>>();
        let select_fields = plan.schema().fields();
        if target_fields.len() != select_fields.len() {
            return Err(rootcause::report!(
                "target table `{}` has {} user columns {:?} but the select list has {} {:?}; \
                 an INSERT INTO maps them by position",
                target.table_name,
                target_fields.len(),
                target_fields
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
                select_fields.len(),
                select_fields
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            ));
        }
        let state_map = derived_output_mapping(&state_spec, &plan)?
            .into_iter()
            .collect::<HashMap<_, _>>();
        let mut positional = HashMap::new();
        for (target, select) in target_fields.iter().zip(select_fields) {
            if crate::table::normalize_data_type(target.data_type())
                != crate::table::normalize_data_type(select.data_type())
            {
                return Err(rootcause::report!(
                    "target column {} ({}) does not match select column {} ({}); \
                     an INSERT INTO maps by position and the types must match",
                    target.name(),
                    target.data_type(),
                    select.name(),
                    select.data_type()
                ));
            }
            let source = state_map
                .get(select.name())
                .cloned()
                .unwrap_or_else(|| select.name().clone());
            if expected.field_with_name(&source).is_err() {
                return Err(rootcause::report!(
                    "the maintenance state does not materialize select column {}",
                    select.name()
                ));
            }
            positional.insert(target.name().clone(), source);
        }
        if target.schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_err()
            || target.schema.field_with_name(IVM_EPOCH_COLUMN).is_err()
        {
            return Err(rootcause::report!(
                "target table `{}` needs the internal rowKinds/__ivm_epoch columns",
                target.table_name
            ));
        }
        if target.primary_keys.is_empty() {
            return Err(rootcause::report!(
                "target table `{}` needs a primary key for the projection sink: \
                 its refresh writes delete markers that the target key merges",
                target.table_name
            ));
        }

        let mut state_spec = state_spec;
        let state_table = self
            .ensure_internal_state_table(target, &state_spec, expected.clone())
            .await?;
        state_spec.set_derived_identity(&state_table.table_id, &state_table.table_id)?;
        if needs_state_table(&state_spec) {
            let state_table_id =
                self.ensure_state_table(&state_table, &state_spec).await?;
            state_spec.set_derived_state_table(&state_table_id)?;
        }
        let state_definition = definition_hash(&state_spec)?;
        let stored_state = self
            .runtime
            .metadata()
            .view_definition(&state_table.table_id)
            .await?;
        let same_state = stored_state
            .as_ref()
            .is_some_and(|(hash, _)| hash == &state_definition);
        let state_dirty = self
            .runtime
            .metadata()
            .view_status(&state_table.table_id)
            .await?
            .as_deref()
            == Some("dirty");

        let state_generation_before = self
            .runtime
            .metadata()
            .view_generation(&state_table.table_id)
            .await
            .unwrap_or(0);
        let mut state_rebuilt =
            overwrite || stored_state.is_none() || !same_state || state_dirty;
        let _state_epoch = if state_rebuilt {
            let epoch = self.runtime.rebuild_spec(&state_spec).await?;
            self.runtime
                .metadata()
                .set_view_definition(&state_table.table_id, &state_definition, sql)
                .await?;
            Some(epoch)
        } else {
            match self.runtime.refresh_spec(&state_spec).await {
                Ok(epoch) => epoch,
                Err(error) if IvmRuntime::rebuild_required(&error) => {
                    state_rebuilt = true;
                    let epoch = self.runtime.rebuild_spec(&state_spec).await?;
                    self.runtime
                        .metadata()
                        .set_view_definition(
                            &state_table.table_id,
                            &state_definition,
                            sql,
                        )
                        .await?;
                    Some(epoch)
                }
                Err(error) => return Err(error),
            }
        };
        let state_generation = self
            .runtime
            .metadata()
            .view_generation(&state_table.table_id)
            .await
            .unwrap_or(0);
        if state_generation != state_generation_before {
            // A rebuild of the state (even with the same definition) resets
            // its changelog; the target must be rebuilt from the snapshot.
            state_rebuilt = true;
        }
        if state_rebuilt || state_dirty {
            self.runtime
                .metadata()
                .set_view_status(&state_table.table_id, "active")
                .await?;
            self.runtime
                .metadata()
                .set_view_error(&state_table.table_id, None)
                .await?;
        }

        let columns = target
            .schema
            .fields()
            .iter()
            .map(|field| {
                let source = positional
                    .get(field.name())
                    .cloned()
                    .unwrap_or_else(|| field.name().clone());
                Ok(SinkColumn {
                    name: field.name().clone(),
                    source: if source == *field.name() {
                        String::new()
                    } else {
                        source
                    },
                    result: encode_data_type(field.data_type())?,
                    nullable: field.is_nullable(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let source_generation = state_generation;
        let sink_spec = ViewSpec::Sink {
            view_id: view_id.to_string(),
            source_table_id: state_table.table_id.clone(),
            mv_table_id: target.table_id.clone(),
            columns,
            primary_keys: target.primary_keys.clone(),
            range_partition_columns: target.range_partition_columns.clone(),
            source_definition: state_definition.clone(),
            source_generation,
        };
        let sink_definition = definition_hash(&sink_spec)?;
        let stored_sink = self.runtime.metadata().view_definition(view_id).await?;
        let stored_source_generation =
            match self.runtime.metadata().get_view_spec(view_id).await? {
                Some(value) => match serde_json::from_value::<ViewSpec>(value) {
                    Ok(ViewSpec::Sink {
                        source_generation, ..
                    }) => Some(source_generation),
                    _ => None,
                },
                None => None,
            };
        let same_sink = stored_sink
            .as_ref()
            .is_some_and(|(hash, _)| hash == &sink_definition);
        let sink_dirty = self
            .runtime
            .metadata()
            .view_status(view_id)
            .await?
            .as_deref()
            == Some("dirty");
        let mut sink_rebuilt = overwrite
            || stored_sink.is_none()
            || !same_sink
            || stored_source_generation != Some(source_generation)
            || state_rebuilt
            || sink_dirty;
        let sink_epoch = if sink_rebuilt {
            let epoch = self.runtime.rebuild_spec(&sink_spec).await?;
            self.runtime
                .metadata()
                .set_view_definition(view_id, &sink_definition, sql)
                .await?;
            Some(epoch)
        } else {
            match self.runtime.refresh_spec(&sink_spec).await {
                Ok(epoch) => epoch,
                Err(error) if IvmRuntime::rebuild_required(&error) => {
                    sink_rebuilt = true;
                    let epoch = self.runtime.rebuild_spec(&sink_spec).await?;
                    self.runtime
                        .metadata()
                        .set_view_definition(view_id, &sink_definition, sql)
                        .await?;
                    Some(epoch)
                }
                Err(error) => return Err(error),
            }
        };
        if sink_rebuilt || sink_dirty {
            self.runtime
                .metadata()
                .set_view_status(view_id, "active")
                .await?;
            self.runtime
                .metadata()
                .set_view_error(view_id, None)
                .await?;
        }
        if stored_sink.is_some() && !same_sink {
            self.drop_stale_derived(target, derived.len()).await?;
        }

        let action = if overwrite {
            IvmExecutionAction::Overwrite
        } else if stored_sink.is_none() {
            IvmExecutionAction::Bootstrap
        } else if sink_rebuilt {
            IvmExecutionAction::Rebuild
        } else {
            IvmExecutionAction::Incremental
        };
        Ok(IvmExecution {
            action,
            epoch: sink_epoch,
            view_id: view_id.to_string(),
            definition_hash: sink_definition,
        })
    }

    /// Create or reuse the hidden maintenance state table of a split view.
    async fn ensure_internal_state_table(
        &self,
        target: &IvmTable,
        spec: &ViewSpec,
        expected: SchemaRef,
    ) -> Result<IvmTable> {
        let primary_keys = self.derived_primary_keys(spec).await?;
        if primary_keys.is_empty() && !is_global_aggregate(spec) {
            return Err(rootcause::report!(
                "the maintenance state of target `{}` needs a primary key",
                target.table_name
            ));
        }
        let name = format!("{}__ivm_state", target.table_name);
        let path = format!("{}/__ivm_state", target.table_path.trim_end_matches('/'));
        let bucket_columns = bucket_columns_for(&primary_keys);
        if let Ok(existing) = self.runtime.open_table(&name, &target.namespace).await {
            if schema_matches(&existing.schema, &expected)
                && existing.primary_keys == primary_keys
                && (existing.bucket_columns.is_empty()
                    || existing.bucket_columns == bucket_columns)
                && existing.file_format == internal_file_format()
            {
                return Ok(existing);
            }
            for state in self
                .runtime
                .metadata()
                .list_states(&existing.table_id)
                .await?
            {
                if state.role == StateRole::Mv {
                    continue;
                }
                self.runtime
                    .metadata()
                    .unregister_state(&existing.table_id, state.role)
                    .await?;
                let _ = self
                    .runtime
                    .client()
                    .drop_table(&state.table_name, &state.namespace)
                    .await;
            }
            let _ = self
                .runtime
                .metadata()
                .delete_view(&existing.table_id)
                .await;
            let _ = self
                .runtime
                .client()
                .drop_table(&name, &target.namespace)
                .await;
        }
        let mut options = IvmTableOptions::new(name, path, expected)
            .with_namespace(target.namespace.clone())
            .with_primary_keys(primary_keys)
            .with_bucket_columns(bucket_columns)
            .with_file_format(internal_file_format());
        options.hash_bucket_num = target.hash_bucket_num.clone();
        self.runtime.create_table(options).await
    }

    /// Analyze `plan`, materializing the derived views its compound sources
    /// need (bottom-up), and return the outer spec with the rewritten plan.
    async fn analyze_with_derived(
        &self,
        plan: &LogicalPlan,
        tables: &mut HashMap<String, IvmTable>,
        request: &AnalyzeRequest,
        target: &IvmTable,
        steps: &mut Vec<String>,
        cache: &mut HashMap<String, CachedDerived>,
    ) -> Result<(ViewSpec, LogicalPlan)> {
        let mut plan = plan.clone();
        loop {
            match analyze_select(&plan, tables, request) {
                Ok(analyzed) => return Ok((analyzed.spec, plan)),
                Err(error) => {
                    let Some(source) = derived_source(&error) else {
                        return Err(error);
                    };
                    if steps.len() >= max_derived_steps() {
                        return Err(rootcause::report!(
                            "the statement needs more than {} derived views; \
                             split it into explicit views (raise \
                             LAKESOUL_IVM_MAX_DERIVED_STEPS to allow more)",
                            max_derived_steps()
                        )
                        .attach(UnsupportedShape));
                    }
                    let key = plan_key(&source);
                    let reused = cache.get(&key).cloned();
                    let (table, view_id, mapping, fresh) = match reused {
                        Some(step) => (step.table, step.view_id, step.mapping, false),
                        None => {
                            let (table, view_id, mapping) = self
                                .materialize_derived(
                                    &source, target, tables, steps, cache,
                                )
                                .await?;
                            cache.insert(
                                key,
                                CachedDerived {
                                    table: table.clone(),
                                    view_id: view_id.clone(),
                                    mapping: mapping.clone(),
                                },
                            );
                            (table, view_id, mapping, true)
                        }
                    };
                    let scan = derived_scan(&table, &self.runtime)?;
                    plan = replace_plan(&plan, &source, &scan)?;
                    let renames = mapping
                        .into_iter()
                        .filter(|(field, column)| field != column)
                        .collect::<HashMap<_, _>>();
                    if !renames.is_empty() {
                        plan = rename_derived_columns(plan, &renames)?;
                    }
                    tables.insert(table.table_name.clone(), table);
                    if fresh {
                        steps.push(view_id);
                    }
                }
            }
        }
    }

    /// Materialize the subplan of a derived source as a registered view (with
    /// its own internal MV table) and return the table and view id.
    async fn materialize_derived(
        &self,
        subplan: &LogicalPlan,
        target: &IvmTable,
        tables: &mut HashMap<String, IvmTable>,
        steps: &mut Vec<String>,
        cache: &mut HashMap<String, CachedDerived>,
    ) -> Result<(IvmTable, String, Vec<(String, String)>)> {
        derived_step_supported(subplan)?;
        let request = AnalyzeRequest {
            view_id: DERIVED_PLACEHOLDER.to_string(),
            mv_table_id: DERIVED_PLACEHOLDER.to_string(),
            state_table_id: Some(STATE_PLACEHOLDER.to_string()),
            derived: true,
        };
        // The subplan may itself need derived views; they are materialized
        // first and take the lower step indices.
        let (mut spec, _) = Box::pin(
            self.analyze_with_derived(subplan, tables, &request, target, steps, cache),
        )
        .await?;
        // A derived outer-join step gets a non-null row identity, so a
        // consumer can read its MV as a keyed source; the keys must be
        // encodable into that identity.
        self.runtime.validate_identity_keys(&spec).await?;
        spec.set_derived_identity_column()?;
        // A compact join names its payloads left_value / right_value; the
        // derived view widens them to the subplan's column names so the
        // rewritten plan resolves its columns.
        widen_derived_join(&mut spec);
        // Two occurrences of the same CTE may differ only in relation aliases;
        // their normalized specs are identical, so reuse the step table (and
        // skip creating a new one).
        let spec_key = format!("spec:{}", definition_hash(&spec)?);
        if let Some(step) = cache.get(&spec_key).cloned() {
            let mapping = derived_output_mapping(&spec, subplan)?;
            return Ok((step.table, step.view_id, mapping));
        }
        // The materialized columns must cover every subplan output field
        // (the replacement projection renames them back).
        let mapping = derived_output_mapping(&spec, subplan)?;
        let schema = expected_mv_schema(&spec, tables)?;
        for (_, column) in &mapping {
            if schema.field_with_name(column).is_err() {
                return Err(rootcause::report!(
                    "a derived view does not expose the column {column}; \
                     split the statement into explicit views"
                )
                .attach(UnsupportedShape));
            }
        }
        let index = steps.len();
        let table = self
            .ensure_derived_table(target, index, &spec, tables)
            .await?;
        // The derived view is registered under its MV table id (the view
        // identity of every view), so the chain graph finds it when the outer
        // spec lists the derived table as a source.
        let view_id = table.table_id.clone();
        spec.set_derived_identity(&view_id, &view_id)?;
        // A `MIN`/`MAX` or `DISTINCT` aggregate step carries its own
        // value-count state table, created next to the step MV.
        if needs_state_table(&spec) {
            let state_table_id = self.ensure_state_table(&table, &spec).await?;
            spec.set_derived_state_table(&state_table_id)?;
        }
        let definition = definition_hash(&spec)?;
        let stored = self.runtime.metadata().view_definition(&view_id).await?;
        let same_definition =
            stored.as_ref().is_some_and(|(hash, _)| hash == &definition);
        if same_definition {
            self.runtime.refresh_spec(&spec).await?;
        } else {
            self.runtime.rebuild_spec(&spec).await?;
            self.runtime
                .metadata()
                .set_view_definition(&view_id, &definition, "")
                .await?;
        }
        cache.insert(
            spec_key,
            CachedDerived {
                table: table.clone(),
                view_id: view_id.clone(),
                mapping: mapping.clone(),
            },
        );
        Ok((table, view_id, mapping))
    }

    /// Create or reuse the internal MV table of a derived step.  The table is
    /// reused by name while its schema and keys match; a changed step shape
    /// recreates it (the dependent views rebuild through their new table id).
    async fn ensure_derived_table(
        &self,
        target: &IvmTable,
        index: usize,
        spec: &ViewSpec,
        tables: &HashMap<String, IvmTable>,
    ) -> Result<IvmTable> {
        let name = derived_step_name(&target.table_name, index);
        let path = derived_step_path(&target.table_path, index);
        let mut schema = expected_mv_schema(spec, tables)?;
        // A filter `key IS NOT NULL` guarantees a derived key is non-null even
        // when the source column is declared nullable.  Carry that into the
        // step schema so a consumer can read the step as a keyed source.
        let non_null = self.spec_non_null_columns(spec, &schema).await?;
        if !non_null.is_empty() {
            let fields = schema
                .fields()
                .iter()
                .map(|field| {
                    if non_null.contains(field.name()) {
                        Arc::new(field.as_ref().clone().with_nullable(false))
                    } else {
                        field.clone()
                    }
                })
                .collect::<Vec<_>>();
            schema =
                Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        }
        let primary_keys = self.derived_primary_keys(spec).await?;
        let bucket_columns = bucket_columns_for(&primary_keys);
        if let Ok(existing) = self.runtime.open_table(&name, &target.namespace).await {
            // A derived step inherits the target's bucket layout: keeping the
            // default bucket count would serialize a 1M-row window/join step
            // into one oversized hash partition.  It must record the bucket
            // columns as well: a keyed lookup routes by them, and an empty
            // list silently turns every before-image lookup into a full scan.
            if schema_matches(&existing.schema, &schema)
                && existing.primary_keys == primary_keys
                && (existing.bucket_columns.is_empty()
                    || existing.bucket_columns == bucket_columns)
                && existing.file_format == internal_file_format()
                && existing.hash_bucket_num == target.hash_bucket_num
            {
                return Ok(existing);
            }
            self.runtime
                .metadata()
                .delete_view(&existing.table_id)
                .await?;
            self.runtime
                .client()
                .drop_table(&name, &target.namespace)
                .await?;
        }
        let mut options = IvmTableOptions::new(name, path, schema)
            .with_namespace(target.namespace.clone())
            .with_primary_keys(primary_keys)
            .with_bucket_columns(bucket_columns)
            .with_file_format(internal_file_format());
        options.hash_bucket_num = target.hash_bucket_num.clone();
        self.runtime.create_table(options).await
    }

    /// The columns a derived step's own filter, or every keyed UNION branch's
    /// filter, guarantees non-null.
    async fn spec_non_null_columns(
        &self,
        spec: &ViewSpec,
        schema: &Schema,
    ) -> Result<Vec<String>> {
        // A step filter runs against the source table, not against the
        // step's (possibly pruned) output schema.
        let filter_schema = match spec {
            ViewSpec::Row {
                source_table_id, ..
            }
            | ViewSpec::MultiAgg {
                source_table_id, ..
            } => self.runtime.open_table_by_id(source_table_id).await?.schema,
            _ => Arc::new(schema.clone()),
        };
        let mut columns = spec_filter(spec)
            .map(|filter| filter_non_null_columns(filter, &filter_schema))
            .unwrap_or_default();
        if let ViewSpec::UnionAll { sources, .. } = spec {
            let mut guaranteed: Option<Vec<String>> = None;
            for source in sources {
                let table = self.runtime.open_table_by_id(&source.table_id).await?;
                let mut branch = source
                    .filter
                    .as_deref()
                    .map(|filter| filter_non_null_columns(filter, &table.schema))
                    .unwrap_or_default();
                // A branch table that already carries a non-null column and
                // projects it unchanged guarantees it as well.
                if source.columns.is_empty() {
                    for field in table.schema.fields() {
                        if !field.is_nullable() {
                            branch.push(field.name().clone());
                        }
                    }
                } else {
                    for (index, column) in source.columns.iter().enumerate() {
                        let expression = source
                            .exprs
                            .get(index)
                            .cloned()
                            .unwrap_or_else(|| column.clone());
                        if expression == *column
                            && table
                                .schema
                                .field_with_name(column)
                                .is_ok_and(|field| !field.is_nullable())
                        {
                            branch.push(column.clone());
                        }
                    }
                }
                guaranteed = Some(match guaranteed {
                    None => branch,
                    Some(previous) => previous
                        .into_iter()
                        .filter(|column| branch.contains(column))
                        .collect(),
                });
            }
            columns.extend(guaranteed.unwrap_or_default());
        }
        Ok(columns)
    }

    /// The primary keys a derived MV is created with.  The join shapes, the
    /// sum/count/avg aggregates and the `MIN`/`MAX` and `DISTINCT` aggregates
    /// are materialized for now; the other kinds are rejected explicitly.
    async fn derived_primary_keys(&self, spec: &ViewSpec) -> Result<Vec<String>> {
        Ok(match spec {
            ViewSpec::Join {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::CrossJoin {
                left_table_id,
                right_table_id,
                ..
            } => {
                let left = self.runtime.open_table_by_id(left_table_id).await?;
                let right = self.runtime.open_table_by_id(right_table_id).await?;
                keyed_join_output_primary_keys(&left.primary_keys, &right.primary_keys)
            }
            ViewSpec::FullJoin {
                left_table_id,
                right_table_id,
                identity_column,
                ..
            } => {
                let left = self.runtime.open_table_by_id(left_table_id).await?;
                let right = self.runtime.open_table_by_id(right_table_id).await?;
                if *identity_column {
                    // Both sides may be unmatched, so only the encoded
                    // identity is non-null.
                    vec![IVM_PAIR_IDENTITY_COLUMN.to_string()]
                } else {
                    keyed_join_output_primary_keys(
                        &left.primary_keys,
                        &right.primary_keys,
                    )
                }
            }
            ViewSpec::SumCount { group_keys, .. }
            | ViewSpec::MinMax { group_keys, .. }
            | ViewSpec::DistinctAgg { group_keys, .. }
            | ViewSpec::MultiAgg { group_keys, .. } => group_keys.clone(),
            ViewSpec::LeftJoin {
                left_table_id,
                right_table_id,
                identity_column,
                ..
            } => {
                let left = self.runtime.open_table_by_id(left_table_id).await?;
                let right = self.runtime.open_table_by_id(right_table_id).await?;
                // The MV's key columns are the aliased identities, so the
                // pair helper names them.
                let mut keys = keyed_join_output_primary_keys(&left.primary_keys, &[]);
                if *identity_column {
                    // The right identities may be NULL; the encoded column
                    // keeps the key non-null so a consumer can read the step.
                    keys.push(IVM_PAIR_IDENTITY_COLUMN.to_string());
                } else {
                    keys.extend(keyed_join_output_primary_keys(&[], &right.primary_keys));
                }
                keys
            }
            ViewSpec::Window {
                source_table_id,
                partition_keys,
                ..
            } => {
                let source = self.runtime.open_table_by_id(source_table_id).await?;
                let mut keys = partition_keys.clone();
                keys.extend(source.primary_keys.clone());
                keys
            }
            ViewSpec::MultiWindow {
                source_table_id, ..
            } => {
                // The multi-window MV is keyed by the source primary keys.
                let source = self.runtime.open_table_by_id(source_table_id).await?;
                source.primary_keys.clone()
            }
            ViewSpec::UnionAll {
                sources,
                source_column,
                ..
            } => {
                // A keyed union is keyed by `(source column, branch keys)`;
                // every keyed branch keeps its primary keys in the output.
                // An append-only union stays keyless.  A nested union's inner
                // marker is a branch primary key and stays in `keys` too.
                let mut keys = Vec::new();
                for source in sources {
                    let table = self.runtime.open_table_by_id(&source.table_id).await?;
                    if table.primary_keys.is_empty() {
                        continue;
                    }
                    if keys.is_empty() {
                        keys.push(source_column.clone());
                    }
                    for key in &table.primary_keys {
                        let output = crate::sql::union_key_output_name(source, key)
                            .unwrap_or_else(|| key.clone());
                        if !keys.contains(&output) {
                            keys.push(output);
                        }
                    }
                }
                keys
            }
            ViewSpec::UnionDistinct { sources, .. } => {
                let mut keys = Vec::new();
                for source in sources {
                    let table = self.runtime.open_table_by_id(&source.table_id).await?;
                    if table.primary_keys.is_empty() {
                        continue;
                    }
                    if keys.is_empty() {
                        keys.push(IVM_SOURCE_COLUMN.to_string());
                    }
                    for key in &table.primary_keys {
                        if !keys.contains(key) {
                            keys.push(key.clone());
                        }
                    }
                }
                keys
            }
            ViewSpec::LookupChain { sources, steps, .. } => {
                // The chain MV is keyed by the base keys plus the matched-row
                // identity of every 1:N step.  Computed-output derived steps
                // only accept unique steps, but the general branch keeps the
                // key layout valid if this helper is reused later.
                let mut keys = self
                    .runtime
                    .open_table_by_id(
                        &sources
                            .first()
                            .ok_or_else(|| {
                                rootcause::report!(
                                    "a derived lookup chain has no base source"
                                )
                            })?
                            .table_id,
                    )
                    .await?
                    .primary_keys
                    .clone();
                for step in steps {
                    if !step.unique {
                        keys.push(lookup_chain_step_id_column(step.source));
                    }
                }
                keys
            }
            ViewSpec::LookupJoin { left_table_id, .. }
            | ViewSpec::SemiAnti { left_table_id, .. } => self
                .runtime
                .open_table_by_id(left_table_id)
                .await?
                .primary_keys
                .clone(),
            // A projection step keeps the source row identity.  A keyed
            // consumer reads the step by it; an append-only source stays
            // keyless.
            ViewSpec::Row {
                source_table_id, ..
            } => self
                .runtime
                .open_table_by_id(source_table_id)
                .await?
                .primary_keys
                .clone(),
            // A top-k row's identity is its source primary key.
            // A top-1 view has exactly one row per group, so the partition
            // keys are its identity; a larger limit keeps the source keys.
            ViewSpec::TopK {
                group_keys,
                limit,
                offset,
                ..
            } if *limit == 1 && *offset == 0 => group_keys.clone(),
            ViewSpec::TopK {
                source_table_id, ..
            } => self
                .runtime
                .open_table_by_id(source_table_id)
                .await?
                .primary_keys
                .clone(),
            ViewSpec::MultiJoin { sources, .. } => {
                let mut keys = Vec::with_capacity(sources.len());
                for source in sources {
                    keys.push(
                        self.runtime
                            .open_table_by_id(&source.table_id)
                            .await?
                            .primary_keys
                            .clone(),
                    );
                }
                multi_join_primary_keys(&keys)
            }
            other => {
                return Err(rootcause::report!(
                    "a derived view over {} is not materialized yet",
                    other.kind()
                ));
            }
        })
    }

    /// Drop the derived steps beyond `needed` (a changed definition): the
    /// step tables are addressed by name, and their views are registered
    /// under the table id.
    async fn drop_stale_derived(&self, target: &IvmTable, needed: usize) -> Result<()> {
        for index in needed..=max_derived_steps() {
            let name = derived_step_name(&target.table_name, index);
            let Ok(table) = self.runtime.open_table(&name, &target.namespace).await
            else {
                continue;
            };
            self.runtime.metadata().delete_view(&table.table_id).await?;
            let _ = self
                .runtime
                .client()
                .drop_table(&name, &target.namespace)
                .await;
        }
        Ok(())
    }

    /// The value-count state table of the view, reusing the registered one when
    /// its schema still matches and recreating it otherwise.
    async fn ensure_state_table(&self, mv: &IvmTable, spec: &ViewSpec) -> Result<String> {
        if matches!(spec, ViewSpec::Join { .. } | ViewSpec::LookupJoin { .. }) {
            let schema = mv.schema.clone();
            let primary_keys = mv.primary_keys.clone();
            let bucket_columns = if primary_keys.is_empty() {
                Vec::new()
            } else if !mv.bucket_columns.is_empty() {
                mv.bucket_columns
                    .iter()
                    .filter(|column| primary_keys.contains(column))
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                vec![primary_keys[0].clone()]
            };
            if let Some(existing) = self
                .runtime
                .metadata()
                .get_state(spec.view_id(), StateRole::State)
                .await?
            {
                let table = self.runtime.open_table_by_id(&existing.table_id).await?;
                if schema_matches(&table.schema, &schema)
                    && table.primary_keys == primary_keys
                {
                    return Ok(table.table_id);
                }
                self.runtime
                    .metadata()
                    .unregister_state(spec.view_id(), StateRole::State)
                    .await?;
                let _ = self
                    .runtime
                    .client()
                    .drop_table(&table.table_name, &table.namespace)
                    .await;
            }
            let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
            let mut options = IvmTableOptions::new(
                format!("{}__ivm_state_{suffix}", mv.table_name),
                format!("{}__ivm_state_{suffix}", mv.table_path),
                schema,
            )
            .with_namespace(mv.namespace.clone())
            .with_primary_keys(primary_keys)
            .with_bucket_columns(bucket_columns)
            .with_file_format(keyed_state_file_format());
            if options.schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_ok() {
                options = options.with_cdc_column(IVM_ROW_KINDS_COLUMN);
            }
            options.hash_bucket_num = mv.hash_bucket_num.clone();
            let table = create_ivm_table(self.runtime.client(), options).await?;
            return Ok(table.table_id);
        }
        if !multi_agg_value_state_eligible(spec)
            && let ViewSpec::MultiAgg {
                source_table_id,
                group_keys,
                ..
            } = spec
        {
            let source = self.runtime.open_table_by_id(source_table_id).await?;
            if source.primary_keys.is_empty() {
                return Err(rootcause::report!(
                    "multi-agg row state needs a keyed source"
                ));
            }
            let schema = multi_agg_rows_state_schema_for(&source.schema, group_keys)?;
            let mut primary_keys = group_keys.clone();
            for key in &source.primary_keys {
                if !primary_keys.contains(key) {
                    primary_keys.push(key.clone());
                }
            }
            let bucket_columns = group_keys.clone();
            let range_partitions = state_range_partition_columns(&source, &primary_keys);
            if let Some(existing) = self
                .runtime
                .metadata()
                .get_state(spec.view_id(), StateRole::MultiAggRows)
                .await?
            {
                let table = self.runtime.open_table_by_id(&existing.table_id).await?;
                if schema_matches(&table.schema, &schema)
                    && table.primary_keys == primary_keys
                {
                    return Ok(table.table_id);
                }
                self.runtime
                    .metadata()
                    .unregister_state(spec.view_id(), StateRole::MultiAggRows)
                    .await?;
                let _ = self
                    .runtime
                    .client()
                    .drop_table(&table.table_name, &table.namespace)
                    .await;
            }
            // A compact aggregate state from an older definition would make
            // the refresh pick the wrong path; remove it when switching to
            // row-level state.
            if let Some(existing) = self
                .runtime
                .metadata()
                .get_state(spec.view_id(), StateRole::State)
                .await?
            {
                self.runtime
                    .metadata()
                    .unregister_state(spec.view_id(), StateRole::State)
                    .await?;
                let table = self.runtime.open_table_by_id(&existing.table_id).await?;
                let _ = self
                    .runtime
                    .client()
                    .drop_table(&table.table_name, &table.namespace)
                    .await;
            }
            let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
            let mut options = IvmTableOptions::new(
                format!("{}__ivm_multi_agg_rows_{suffix}", mv.table_name),
                format!("{}__ivm_multi_agg_rows_{suffix}", mv.table_path),
                schema,
            )
            .with_namespace(mv.namespace.clone())
            .with_primary_keys(primary_keys)
            .with_bucket_columns(bucket_columns)
            .with_cdc_column(IVM_ROW_KINDS_COLUMN)
            .with_file_format(keyed_state_file_format());
            if !range_partitions.is_empty() {
                options = options.with_range_partition_columns(range_partitions);
            }
            options.hash_bucket_num = mv.hash_bucket_num.clone();
            let table = create_ivm_table(self.runtime.client(), options).await?;
            self.runtime
                .metadata()
                .register_state(spec.view_id(), StateRole::MultiAggRows, &table)
                .await?;
            return Ok(table.table_id);
        }
        if let ViewSpec::MultiAgg {
            source_table_id,
            group_keys,
            aggregates,
            ..
        } = spec
        {
            let source = self.runtime.open_table_by_id(source_table_id).await?;
            let columns = aggregates
                .iter()
                .map(|aggregate| {
                    Ok((
                        aggregate.column.clone(),
                        decode_data_type(&aggregate.result)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let schema =
                multi_agg_state_schema_for(&source.schema, group_keys, &columns)?;
            if let Some(existing) = self
                .runtime
                .metadata()
                .get_state(spec.view_id(), StateRole::State)
                .await?
            {
                let table = self.runtime.open_table_by_id(&existing.table_id).await?;
                if schema_matches(&table.schema, &schema) {
                    return Ok(table.table_id);
                }
                self.runtime
                    .metadata()
                    .unregister_state(spec.view_id(), StateRole::State)
                    .await?;
                let _ = self
                    .runtime
                    .client()
                    .drop_table(&table.table_name, &table.namespace)
                    .await;
            }
            let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
            let mut options = IvmTableOptions::new(
                format!("{}__ivm_state_{suffix}", mv.table_name),
                format!("{}__ivm_state_{suffix}", mv.table_path),
                schema,
            )
            .with_namespace(mv.namespace.clone())
            .with_primary_keys(vec![IVM_GROUP_ROW_COLUMN.to_string()])
            .with_bucket_columns(vec![IVM_GROUP_ROW_COLUMN.to_string()])
            .with_file_format(keyed_state_file_format());
            if options.schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_ok() {
                options = options.with_cdc_column(IVM_ROW_KINDS_COLUMN);
            }
            options.hash_bucket_num = mv.hash_bucket_num.clone();
            let table = create_ivm_table(self.runtime.client(), options).await?;
            return Ok(table.table_id);
        }
        let (source_table_id, group_keys, group_exprs, value_column, value_expr) =
            match spec {
                ViewSpec::MinMax {
                    source_table_id,
                    group_keys,
                    group_exprs,
                    value_column,
                    value_expr,
                    ..
                } => (
                    source_table_id,
                    group_keys,
                    group_exprs,
                    value_column.as_deref(),
                    value_expr.as_deref(),
                ),
                ViewSpec::DistinctAgg {
                    source_table_id,
                    group_keys,
                    group_exprs,
                    value_column,
                    ..
                } => (
                    source_table_id,
                    group_keys,
                    group_exprs,
                    Some(value_column.as_str()),
                    None,
                ),
                _ => {
                    return Err(rootcause::report!(
                        "view {} does not use a value-count state table",
                        spec.view_id()
                    ));
                }
            };
        let source = self.runtime.open_table_by_id(source_table_id).await?;
        let schema = value_count_groups_state_schema_for(
            &source.schema,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
        )?;

        if let Some(existing) = self
            .runtime
            .metadata()
            .get_state(spec.view_id(), StateRole::State)
            .await?
        {
            let table = self.runtime.open_table_by_id(&existing.table_id).await?;
            if schema_matches(&table.schema, &schema) {
                return Ok(table.table_id);
            }
            self.runtime
                .metadata()
                .unregister_state(spec.view_id(), StateRole::State)
                .await?;
            let _ = self
                .runtime
                .client()
                .drop_table(&table.table_name, &table.namespace)
                .await;
        }

        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let mut primary_keys = group_keys.to_vec();
        primary_keys.push(IVM_VALUE_COLUMN.to_string());
        let mut options = IvmTableOptions::new(
            format!("{}__ivm_state_{suffix}", mv.table_name),
            format!("{}__ivm_state_{suffix}", mv.table_path),
            schema,
        )
        .with_primary_keys(primary_keys.clone())
        .with_bucket_columns(group_keys.to_vec())
        .with_file_format(keyed_state_file_format());
        let range_partitions = state_range_partition_columns(&source, &primary_keys);
        if !range_partitions.is_empty() {
            options = options.with_range_partition_columns(range_partitions);
        }
        let table = create_ivm_table(self.runtime.client(), options).await?;
        Ok(table.table_id)
    }

    /// Drop the generated state tables of a view (the MV is recycled by the
    /// rebuild, not dropped).
    async fn drop_generated_state(&self, view_id: &str) -> Result<()> {
        for state in self.runtime.metadata().list_states(view_id).await? {
            if state.role == StateRole::Mv {
                continue;
            }
            self.runtime
                .metadata()
                .unregister_state(view_id, state.role)
                .await?;
            let _ = self
                .runtime
                .client()
                .drop_table(&state.table_name, &state.namespace)
                .await;
        }
        Ok(())
    }
}

/// The INSERT statement of the entry, validated for the supported subset.
fn parse_insert(sql: &str) -> Result<datafusion::sql::sqlparser::ast::Insert> {
    let mut statements = DFParser::parse_sql(sql)
        .map_err(|error| rootcause::report!("parse SQL: {error}"))?;
    if statements.len() != 1 {
        return Err(rootcause::report!(
            "the incremental entry takes exactly one statement, got {}",
            statements.len()
        ));
    }
    let statement = statements.pop_front().expect("length checked");
    let datafusion::sql::parser::Statement::Statement(statement) = statement else {
        return Err(rootcause::report!(
            "the incremental entry accepts INSERT INTO/OVERWRITE ... SELECT only"
        ));
    };
    let Statement::Insert(insert) = *statement else {
        return Err(rootcause::report!(
            "the incremental entry accepts INSERT INTO/OVERWRITE ... SELECT only"
        ));
    };
    if !insert.columns.is_empty() {
        return Err(rootcause::report!(
            "an explicit INSERT column list is not supported; the target schema is the view schema"
        ));
    }
    if !insert.assignments.is_empty()
        || insert.on.is_some()
        || insert.returning.is_some()
        || insert.replace_into
    {
        return Err(rootcause::report!(
            "only plain INSERT INTO/OVERWRITE ... SELECT is incremental"
        ));
    }
    Ok(insert)
}

fn insert_target(
    insert: &datafusion::sql::sqlparser::ast::Insert,
) -> Result<&ObjectName> {
    match &insert.table {
        TableObject::TableName(name) => Ok(name),
        _ => Err(rootcause::report!(
            "INSERT target must be a plain table name"
        )),
    }
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
            _ => Err(rootcause::report!("table names must be plain identifiers")),
        })
        .collect()
}

fn split_object_name(name: &ObjectName) -> Result<(String, String)> {
    let parts = object_name_parts(name)?;
    match parts.as_slice() {
        [table] => Ok(("default".to_string(), table.clone())),
        [namespace, table] => Ok((namespace.clone(), table.clone())),
        // Ignore the catalog: LakeSoul lives in one PostgreSQL database.
        [_catalog, namespace, table] => Ok((namespace.clone(), table.clone())),
        _ => Err(rootcause::report!(
            "table names with more than three parts are not supported"
        )),
    }
}

struct RelationCollector<'a> {
    names: &'a mut Vec<ObjectName>,
    /// The CTE aliases in scope; a reference to one of them is not a table.
    ctes: Vec<String>,
}

impl RelationCollector<'_> {
    fn new(names: &mut Vec<ObjectName>) -> RelationCollector<'_> {
        RelationCollector {
            names,
            ctes: Vec::new(),
        }
    }
}

impl Visitor for RelationCollector<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes.push(cte.alias.name.value.clone());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
        // A single-part name that matches a CTE alias refers to the CTE, not
        // to a table.
        if relation.0.len() == 1
            && let Some(ObjectNamePart::Identifier(identifier)) = relation.0.first()
            && self
                .ctes
                .iter()
                .any(|cte| cte.eq_ignore_ascii_case(&identifier.value))
        {
            return ControlFlow::Continue(());
        }
        self.names.push(relation.clone());
        ControlFlow::Continue(())
    }
}

/// Whether a multi-aggregate view can use the compact aggregate-value state
/// (one row per group) instead of the row-level source state.
fn multi_agg_value_state_eligible(spec: &ViewSpec) -> bool {
    let ViewSpec::MultiAgg {
        group_exprs,
        aggregates,
        having,
        having_lookup,
        ..
    } = spec
    else {
        return false;
    };
    group_exprs.is_empty()
        && having.is_none()
        && having_lookup.is_none()
        && aggregates.iter().all(|aggregate| {
            let call = aggregate.call.trim();
            call.get(..4)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SUM("))
                && call.ends_with(')')
                && {
                    let expression = &call[4..call.len() - 1];
                    let expression = expression.trim();
                    !expression.is_empty()
                        && !expression.contains(',')
                        && !expression.to_ascii_uppercase().contains("DISTINCT")
                }
        })
}

fn needs_state_table(spec: &ViewSpec) -> bool {
    match spec {
        ViewSpec::MinMax { .. } | ViewSpec::DistinctAgg { .. } => true,
        ViewSpec::MultiAgg {
            group_keys,
            group_exprs,
            having_lookup,
            ..
        } => group_exprs.is_empty() && !group_keys.is_empty() && having_lookup.is_none(),
        _ => false,
    }
}

/// Whether a spec is a global aggregate: its single MV row is rewritten
/// wholesale on every refresh, so it needs no primary key.
fn is_global_aggregate(spec: &ViewSpec) -> bool {
    match spec {
        ViewSpec::SumCount { group_keys, .. }
        | ViewSpec::Variance { group_keys, .. }
        | ViewSpec::Median { group_keys, .. }
        | ViewSpec::BoolAgg { group_keys, .. }
        | ViewSpec::ApproxDistinct { group_keys, .. }
        | ViewSpec::ApproxPercentile { group_keys, .. }
        | ViewSpec::ComputedAgg { group_keys, .. }
        | ViewSpec::StringAgg { group_keys, .. }
        | ViewSpec::ArrayAgg { group_keys, .. }
        | ViewSpec::MultiAgg { group_keys, .. }
        | ViewSpec::MinMax { group_keys, .. }
        | ViewSpec::DistinctAgg { group_keys, .. } => group_keys.is_empty(),
        _ => false,
    }
}

fn find_table<'a>(
    tables: &'a HashMap<String, IvmTable>,
    table_id: &str,
) -> Result<&'a IvmTable> {
    tables
        .values()
        .find(|table| table.table_id == table_id)
        .ok_or_else(|| rootcause::report!("table {table_id} was not opened for analysis"))
}

/// The schema the target MV must have for this definition.
fn expected_mv_schema(
    spec: &ViewSpec,
    tables: &HashMap<String, IvmTable>,
) -> Result<SchemaRef> {
    let schema = match spec {
        // A Tier-2 fallback is validated against its planned statement, never
        // against the spec.
        ViewSpec::Fallback { .. } => {
            return Err(rootcause::report!(
                "a fallback view's schema comes from its statement"
            ));
        }
        ViewSpec::Sink { columns, .. } => Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| {
                    Ok(Field::new(
                        &column.name,
                        decode_data_type(&column.result)?,
                        column.nullable,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        ViewSpec::LookupChain {
            sources,
            steps,
            output_columns,
            ..
        } => {
            let mut schemas = Vec::with_capacity(sources.len());
            for source in sources {
                schemas.push(find_table(tables, &source.table_id)?.schema.clone());
            }
            lookup_chain_mv_schema_for(&schemas, steps, output_columns)?
        }
        ViewSpec::LeftAggregate {
            left_table_id,
            right_table_id,
            right_aggregate,
            aggregate_column,
            output_columns,
            output_exprs,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let right = find_table(tables, right_table_id)?;
            let (aggregate_type, _) =
                crate::runtime::expression_type(&right.schema, right_aggregate)?;
            left_aggregate_mv_schema_with_outputs(
                &left.schema,
                output_columns,
                aggregate_column,
                &aggregate_type,
                &output_exprs
                    .iter()
                    .map(|output| {
                        Ok((output.column.clone(), decode_data_type(&output.result)?))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )?
        }
        ViewSpec::SumCount {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            average,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            sum_count_groups_mv_schema_for(
                &source.schema,
                group_keys,
                group_exprs,
                value_column.as_deref(),
                value_expr.as_deref(),
                *average,
            )?
        }
        ViewSpec::FullJoin {
            left_table_id,
            right_table_id,
            join_keys,
            key_exprs,
            left_value,
            right_value,
            output_columns,
            identity_column,
            null_safe,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let right = find_table(tables, right_table_id)?;
            let schema = if *identity_column {
                wide_full_join_view_schema_with_identity_and_keys(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    key_exprs,
                    output_columns,
                )?
            } else if output_columns.is_empty() {
                full_join_view_schema_with_keys(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    key_exprs,
                    left_value,
                    right_value,
                )?
            } else {
                wide_outer_join_view_schema_with_keys(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    key_exprs,
                    output_columns,
                )?
            };
            if *null_safe && !join_keys.is_empty() {
                nullable_join_key_schema(&schema, join_keys.len())
            } else {
                schema
            }
        }
        ViewSpec::LeftJoin {
            left_table_id,
            right_table_id,
            join_keys,
            key_exprs,
            left_value,
            right_value,
            output_columns,
            identity_column,
            null_safe,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let right = find_table(tables, right_table_id)?;
            let schema = if *identity_column {
                wide_left_join_view_schema_with_identity_and_keys(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    key_exprs,
                    output_columns,
                )?
            } else if output_columns.is_empty() {
                left_join_view_schema_with_keys(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    key_exprs,
                    left_value,
                    right_value,
                )?
            } else {
                wide_outer_join_view_schema_with_keys(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    key_exprs,
                    output_columns,
                )?
            };
            if *null_safe && !join_keys.is_empty() {
                nullable_join_key_schema(&schema, join_keys.len())
            } else {
                schema
            }
        }
        ViewSpec::LookupJoin {
            left_table_id,
            right_table_id,
            join_keys,
            key_exprs,
            left_value,
            right_value,
            output_columns,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let right = find_table(tables, right_table_id)?;
            if output_columns.is_empty() {
                lookup_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    key_exprs,
                    left_value,
                    right_value,
                )?
            } else {
                wide_lookup_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    join_keys,
                    key_exprs,
                    output_columns,
                )?
            }
        }
        ViewSpec::Variance {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            statistic,
            ..
        } => variance_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
            *statistic,
        )?,
        ViewSpec::StringAgg {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            ..
        } => string_agg_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
        )?,
        ViewSpec::ArrayAgg {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            ..
        } => array_agg_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
        )?,
        ViewSpec::Median {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            ..
        } => median_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
        )?,
        ViewSpec::MinMax {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            min_max,
            ..
        } => min_max_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
            *min_max,
        )?,
        ViewSpec::DistinctAgg {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            agg,
            ..
        } => distinct_agg_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column,
            *agg,
        )?,
        ViewSpec::GroupingSets {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            average,
            grouping_columns,
            aggregates,
            ..
        } => {
            let columns = aggregates
                .iter()
                .map(|aggregate| {
                    Ok((
                        aggregate.column.clone(),
                        decode_data_type(&aggregate.result)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            grouping_sets_mv_schema_for(
                &find_table(tables, source_table_id)?.schema,
                group_keys,
                group_exprs,
                value_column.as_deref(),
                value_expr.as_deref(),
                *average,
                grouping_columns,
                &columns,
            )?
        }
        ViewSpec::BoolAgg {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            bool_agg,
            ..
        } => bool_agg_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
            *bool_agg,
        )?,
        ViewSpec::ApproxDistinct {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            ..
        } => approx_distinct_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
        )?,
        ViewSpec::ApproxPercentile {
            source_table_id,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            ..
        } => approx_percentile_groups_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            value_column.as_deref(),
            value_expr.as_deref(),
        )?,
        ViewSpec::ComputedAgg {
            source_table_id,
            group_keys,
            group_exprs,
            column,
            result,
            arguments,
            ..
        } => computed_agg_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            group_exprs,
            column,
            *result,
            arguments.first(),
        )?,
        ViewSpec::MultiWindow {
            source_table_id,
            windows,
            payload_columns,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            multi_window_mv_schema_for_with_payload(
                &source.schema,
                &source.primary_keys,
                payload_columns,
                windows,
            )?
        }
        ViewSpec::Window {
            source_table_id,
            partition_keys,
            columns,
            payload_columns,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            window_columns_mv_schema_for_with_payload(
                &source.schema,
                partition_keys,
                &source.primary_keys,
                payload_columns,
                columns,
            )?
        }
        ViewSpec::TopK {
            source_table_id,
            output_columns,
            output_exprs,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            top_k_expr_mv_schema_for(
                &source.schema,
                &source.primary_keys,
                output_columns,
                output_exprs,
            )?
        }
        ViewSpec::SemiAnti {
            left_table_id,
            output_columns,
            ..
        } => semi_anti_mv_schema_for(
            &find_table(tables, left_table_id)?.schema,
            output_columns,
        )?,
        ViewSpec::Row {
            source_table_id,
            output_columns,
            output_exprs,
            ..
        } => row_expr_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            output_columns,
            output_exprs,
        )?,
        ViewSpec::UnionAll {
            sources,
            source_column,
            ..
        } => {
            let spec = sources
                .first()
                .ok_or_else(|| rootcause::report!("UNION ALL view without sources"))?;
            let source = find_table(tables, &spec.table_id)?;
            let output =
                union_output_schema_for(&source.schema, &spec.columns, &spec.exprs)?;
            union_all_mv_schema_with_source(&output, source_column)?
        }
        ViewSpec::UnionDistinct { sources, .. } => {
            let spec = sources
                .first()
                .ok_or_else(|| rootcause::report!("UNION view without sources"))?;
            let source = find_table(tables, &spec.table_id)?;
            let output =
                union_output_schema_for(&source.schema, &spec.columns, &spec.exprs)?;
            union_distinct_mv_schema_for(&output)
        }
        ViewSpec::MultiAgg {
            source_table_id,
            group_keys,
            group_exprs,
            aggregates,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            let columns = aggregates
                .iter()
                .map(|aggregate| {
                    Ok((
                        aggregate.column.clone(),
                        decode_data_type(&aggregate.result)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            multi_agg_mv_schema_for(&source.schema, group_keys, group_exprs, &columns)?
        }
        ViewSpec::MultiJoin {
            sources, columns, ..
        } => {
            let tables = sources
                .iter()
                .map(|source| find_table(tables, &source.table_id))
                .collect::<Result<Vec<_>>>()?;
            let schemas = tables
                .iter()
                .map(|table| table.schema.clone())
                .collect::<Vec<_>>();
            let keyed = tables
                .first()
                .map(|table| !table.primary_keys.is_empty())
                .unwrap_or(false);
            if tables
                .iter()
                .any(|table| table.primary_keys.is_empty() == keyed)
            {
                return Err(rootcause::report!(
                    "a multi join view needs all sources keyed or all append-only"
                ));
            }
            if keyed {
                let primary_keys = tables
                    .iter()
                    .map(|table| table.primary_keys.clone())
                    .collect::<Vec<_>>();
                multi_join_mv_schema_for(&schemas, &primary_keys, columns)?
            } else {
                multi_join_append_schema_for(&schemas, columns)?
            }
        }
        ViewSpec::CrossJoin {
            left_table_id,
            right_table_id,
            left_value,
            right_value,
            output_columns,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let right = find_table(tables, right_table_id)?;
            if output_columns.is_empty() {
                cross_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    left_value,
                    right_value,
                )?
            } else {
                wide_keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    &[],
                    output_columns,
                )?
            }
        }
        ViewSpec::Join {
            left_table_id,
            right_table_id,
            join_keys,
            key_exprs,
            left_value,
            right_value,
            output_columns,
            null_safe,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let right = find_table(tables, right_table_id)?;
            let schema =
                match (left.primary_keys.is_empty(), right.primary_keys.is_empty()) {
                    (false, false) => {
                        if output_columns.is_empty() {
                            keyed_join_view_schema_with_keys(
                                &left.schema,
                                &right.schema,
                                &left.primary_keys,
                                &right.primary_keys,
                                join_keys,
                                key_exprs,
                                left_value,
                                right_value,
                            )?
                        } else {
                            wide_keyed_join_view_schema_with_keys(
                                &left.schema,
                                &right.schema,
                                &left.primary_keys,
                                &right.primary_keys,
                                join_keys,
                                key_exprs,
                                output_columns,
                            )?
                        }
                    }
                    (true, true) => {
                        if output_columns.is_empty() {
                            join_view_schema_with_keys(
                                &left.schema,
                                &right.schema,
                                join_keys,
                                key_exprs,
                                left_value,
                                right_value,
                            )?
                        } else {
                            wide_join_view_schema_with_keys(
                                &left.schema,
                                &right.schema,
                                join_keys,
                                key_exprs,
                                output_columns,
                            )?
                        }
                    }
                    _ => {
                        return Err(rootcause::report!(
                            "a join view needs both sources keyed or both append-only"
                        ));
                    }
                };
            if *null_safe && !join_keys.is_empty() {
                // A null-safe join matches NULL keys with NULL keys, so its
                // output can carry a NULL join key.
                nullable_join_key_schema(&schema, join_keys.len())
            } else {
                schema
            }
        }
    };
    Ok(normalize_schema(&schema))
}

/// The filter a normalized view applies to its contributing rows, when the
/// spec keeps one.
fn spec_filter(spec: &ViewSpec) -> Option<&str> {
    match spec {
        ViewSpec::Row { filter, .. } => filter.as_deref(),
        ViewSpec::MultiAgg { filter, .. } => filter.as_deref(),
        _ => None,
    }
}

/// The columns a filter guarantees non-null (`col IS NOT NULL` conjuncts).
fn filter_non_null_columns(filter: &str, schema: &Schema) -> Vec<String> {
    let context = refresh_session();
    let schema: SchemaRef = Arc::new(schema.clone());
    let Ok(predicate) = parse_filter(&context, &schema, filter) else {
        return Vec::new();
    };
    datafusion::logical_expr::utils::split_conjunction(&predicate)
        .into_iter()
        .filter_map(|conjunct| match conjunct {
            Expr::IsNotNull(inner) => non_null_column(inner),
            _ => None,
        })
        .collect()
}

/// The column under a cast chain, for `CAST(col AS t) IS NOT NULL`.
fn non_null_column(expr: &Expr) -> Option<String> {
    let mut inner = expr;
    while let Expr::Cast(cast) = inner {
        inner = cast.expr.as_ref();
    }
    match inner {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn target_needs_sink(target: &IvmTable, expected: &Schema) -> bool {
    if !schema_matches(&target.schema, expected) {
        return true;
    }
    !target.range_partition_columns.is_empty()
        && !target
            .range_partition_columns
            .iter()
            .all(|column| target.primary_keys.contains(column))
}

fn schema_matches(actual: &Schema, expected: &Schema) -> bool {
    actual.fields().len() == expected.fields().len()
        && actual
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(actual, expected)| {
                actual.name() == expected.name()
                    && actual.data_type() == expected.data_type()
                    && actual.is_nullable() == expected.is_nullable()
            })
}
