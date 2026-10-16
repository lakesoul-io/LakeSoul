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
    IVM_NONNULL_COUNT_COLUMN, IVM_PAIR_IDENTITY_COLUMN, IVM_ROW_KINDS_COLUMN,
    IVM_SOURCE_COLUMN, IVM_SUM_COLUMN, IVM_VALUE_COLUMN, IvmRuntime, JoinOutputColumn,
    JoinSide, ViewSpec, approx_distinct_groups_mv_schema_for,
    approx_percentile_groups_mv_schema_for, array_agg_groups_mv_schema_for,
    bool_agg_groups_mv_schema_for, computed_agg_mv_schema_for,
    cross_join_view_schema_for, decode_data_type, distinct_agg_groups_mv_schema_for,
    full_join_view_schema_for, grouping_sets_mv_schema_for, join_view_schema_with_keys,
    keyed_join_output_primary_keys, keyed_join_view_schema_with_keys,
    left_aggregate_mv_schema_with_outputs, left_join_view_schema_for,
    lookup_chain_mv_schema_for, lookup_join_view_schema_for, median_groups_mv_schema_for,
    min_max_groups_mv_schema_for, multi_agg_mv_schema_for, multi_join_append_schema_for,
    multi_join_mv_schema_for, multi_join_primary_keys, multi_window_mv_schema_for,
    nullable_join_key_schema, row_expr_mv_schema_for, semi_anti_mv_schema_for,
    string_agg_groups_mv_schema_for, sum_count_groups_mv_schema_for,
    top_k_expr_mv_schema_for, union_all_mv_schema_for, union_distinct_mv_schema_for,
    union_output_schema_for, value_count_groups_state_schema_for,
    variance_groups_mv_schema_for, wide_full_join_view_schema_with_identity,
    wide_join_view_schema_with_keys, wide_keyed_join_view_schema_for,
    wide_keyed_join_view_schema_with_keys, wide_left_join_view_schema_with_identity,
    wide_lookup_join_view_schema_for, wide_outer_join_view_schema_for,
    window_columns_mv_schema_for,
};
use crate::sql::{
    AnalyzeRequest, DerivedSource, UnsupportedShape, analyze_select, definition_hash,
    peel, plan_label,
};
use crate::table::{IvmTable, IvmTableOptions, create_ivm_table};

/// Placeholder state table id used to probe the query shape before the real
/// state table exists.
const STATE_PLACEHOLDER: &str = "__ivm_state_placeholder__";

/// Placeholder identity of a derived spec: the derived table does not exist
/// while its subplan is analyzed, so the executor patches the real ids
/// afterwards ([`ViewSpec::set_derived_identity`]).
const DERIVED_PLACEHOLDER: &str = "__ivm_derived__";

/// The maximum number of derived views one statement may materialize.
const MAX_DERIVED_STEPS: usize = 4;

/// The table name of a derived step.
fn derived_step_name(target_name: &str, index: usize) -> String {
    format!("{target_name}__ivm_step{index}")
}

/// The table path of a derived step (a sibling directory of the target MV).
fn derived_step_path(target_path: &str, index: usize) -> String {
    format!("{}/__ivm_step{index}", target_path.trim_end_matches('/'))
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
        | LogicalPlan::Limit(_) => Ok(()),
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
        let LogicalPlan::Aggregate(aggregate) = peel(subplan) else {
            return Err(rootcause::report!(
                "a derived sum-count view needs an aggregate input"
            )
            .attach(UnsupportedShape));
        };
        let groups = aggregate.group_expr.len();
        let mut mapping = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            let column = if index < groups {
                field.name().clone()
            } else {
                sum_count_column(&aggregate.aggr_expr[index - groups])?.to_string()
            };
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
        // one column per aggregate, in select order.
        let mut mapping = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            let column = match index.checked_sub(group_keys.len()) {
                None => group_keys
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| field.name().clone()),
                Some(position) => aggregates
                    .get(position)
                    .map(|aggregate| aggregate.column.clone())
                    .unwrap_or_else(|| field.name().clone()),
            };
            mapping.push((field.name().clone(), column));
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
        let result = self.execute_inner(sql).await;
        let action = match &result {
            Ok(execution) => execution.action.label(),
            Err(_) => "error",
        };
        crate::observability::record_statement(action, started.elapsed());
        result
    }

    async fn execute_inner(&self, sql: &str) -> Result<IvmExecution> {
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
        let analyzed = self
            .analyze_with_derived(&plan, &mut tables, &request, &mv, &mut derived)
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
        let spec = if !fallback && needs_state_table(&probe) {
            let state_table_id = self.ensure_state_table(&mv, &probe).await?;
            let request = AnalyzeRequest {
                state_table_id: Some(state_table_id),
                ..request
            };
            self.analyze_with_derived(&plan, &mut tables, &request, &mv, &mut derived)
                .await?
                .0
        } else {
            probe
        };
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

    /// Analyze `plan`, materializing the derived views its compound sources
    /// need (bottom-up), and return the outer spec with the rewritten plan.
    async fn analyze_with_derived(
        &self,
        plan: &LogicalPlan,
        tables: &mut HashMap<String, IvmTable>,
        request: &AnalyzeRequest,
        target: &IvmTable,
        steps: &mut Vec<String>,
    ) -> Result<(ViewSpec, LogicalPlan)> {
        let mut plan = plan.clone();
        loop {
            match analyze_select(&plan, tables, request) {
                Ok(analyzed) => return Ok((analyzed.spec, plan)),
                Err(error) => {
                    let Some(source) = derived_source(&error) else {
                        return Err(error);
                    };
                    if steps.len() >= MAX_DERIVED_STEPS {
                        return Err(rootcause::report!(
                            "the statement needs more than {MAX_DERIVED_STEPS} \
                             derived views; split it into explicit views"
                        )
                        .attach(UnsupportedShape));
                    }
                    let (table, view_id, mapping) = self
                        .materialize_derived(&source, target, tables, steps)
                        .await?;
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
                    steps.push(view_id);
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
        let (mut spec, _) =
            Box::pin(self.analyze_with_derived(subplan, tables, &request, target, steps))
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
        let schema = expected_mv_schema(spec, tables)?;
        let primary_keys = self.derived_primary_keys(spec).await?;
        if let Ok(existing) = self.runtime.open_table(&name, &target.namespace).await {
            if schema_matches(&existing.schema, &schema)
                && existing.primary_keys == primary_keys
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
        self.runtime
            .create_table(
                IvmTableOptions::new(name, path, schema)
                    .with_namespace(target.namespace.clone())
                    .with_primary_keys(primary_keys),
            )
            .await
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
            ViewSpec::UnionAll { sources, .. }
            | ViewSpec::UnionDistinct { sources, .. } => {
                // A keyed union is keyed by `(__ivm_source, primary keys)`;
                // every keyed branch keeps its primary keys in the output.
                // An append-only union stays keyless.
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
            ViewSpec::LookupJoin { left_table_id, .. } => self
                .runtime
                .open_table_by_id(left_table_id)
                .await?
                .primary_keys
                .clone(),
            // A top-k row's identity is its source primary key.
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
        for index in needed..=MAX_DERIVED_STEPS {
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
        let table = create_ivm_table(
            self.runtime.client(),
            IvmTableOptions::new(
                format!("{}__ivm_state_{suffix}", mv.table_name),
                format!("{}__ivm_state_{suffix}", mv.table_path),
                schema,
            )
            .with_primary_keys(primary_keys)
            .with_bucket_columns(group_keys.to_vec())
            .with_file_format(mv.file_format),
        )
        .await?;
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

fn needs_state_table(spec: &ViewSpec) -> bool {
    matches!(spec, ViewSpec::MinMax { .. } | ViewSpec::DistinctAgg { .. })
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
    Ok(match spec {
        // A Tier-2 fallback is validated against its planned statement, never
        // against the spec.
        ViewSpec::Fallback { .. } => {
            return Err(rootcause::report!(
                "a fallback view's schema comes from its statement"
            ));
        }
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
                wide_full_join_view_schema_with_identity(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    output_columns,
                )?
            } else if output_columns.is_empty() {
                full_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    left_value,
                    right_value,
                )?
            } else {
                wide_outer_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
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
                wide_left_join_view_schema_with_identity(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    output_columns,
                )?
            } else if output_columns.is_empty() {
                left_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    left_value,
                    right_value,
                )?
            } else {
                wide_outer_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
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
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            multi_window_mv_schema_for(&source.schema, &source.primary_keys, windows)?
        }
        ViewSpec::Window {
            source_table_id,
            partition_keys,
            columns,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            window_columns_mv_schema_for(
                &source.schema,
                partition_keys,
                &source.primary_keys,
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
        ViewSpec::UnionAll { sources, .. } => {
            let spec = sources
                .first()
                .ok_or_else(|| rootcause::report!("UNION ALL view without sources"))?;
            let source = find_table(tables, &spec.table_id)?;
            let output =
                union_output_schema_for(&source.schema, &spec.columns, &spec.exprs)?;
            union_all_mv_schema_for(&output)?
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
    })
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
