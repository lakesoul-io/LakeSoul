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
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
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
    IvmRuntime, JoinOutputColumn, JoinSide, MultiAggPartialPlan, MultiAggSpec,
    MultiLookupColumn, MultiLookupRight, SinkColumn, ViewSpec,
    approx_distinct_groups_mv_schema_for, approx_percentile_groups_mv_schema_for,
    array_agg_groups_mv_schema_for, bool_agg_groups_mv_schema_for,
    computed_agg_mv_schema_for, cross_join_view_schema_for, decode_data_type,
    distinct_agg_groups_mv_schema_for, encode_data_type, full_join_view_schema_with_keys,
    grouping_sets_mv_schema_for, join_view_schema_with_keys,
    keyed_join_output_primary_keys, keyed_join_view_schema_with_keys,
    left_aggregate_mv_schema_with_outputs, left_join_view_schema_with_keys,
    lookup_chain_mv_schema_for, lookup_chain_step_id_column, lookup_join_view_schema_for,
    median_groups_mv_schema_for, min_max_groups_mv_schema_for, multi_agg_mv_schema_for,
    multi_agg_partial_plan, multi_agg_rows_state_schema_for, multi_agg_state_schema_for,
    multi_join_append_schema_for, multi_join_mv_schema_for, multi_join_primary_keys,
    multi_lookup_join_mv_schema_for, multi_window_mv_schema_for_with_payload,
    nullable_join_key_schema, parse_filter, refresh_session, row_expr_mv_schema_for,
    semi_anti_mv_schema_for, string_agg_groups_mv_schema_for,
    sum_count_groups_mv_schema_for, top_k_expr_mv_schema_for,
    union_all_mv_schema_with_source, union_distinct_mv_schema_for,
    union_output_schema_for, value_count_groups_state_schema_for,
    variance_groups_mv_schema_for, wide_full_join_view_schema_with_identity_and_keys,
    wide_join_view_schema_with_keys, wide_keyed_join_view_schema_for,
    wide_keyed_join_view_schema_with_keys,
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

/// The derived-step budget from [`IvmOptions`](crate::config::IvmOptions).
///
/// A production statement with many CTEs may need a higher
/// `IvmOptions::max_derived_steps`; a statement that really needs more should
/// be split into explicit views.
fn max_derived_steps() -> usize {
    crate::config::options().max_derived_steps.max(1)
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

/// The identity of a multi-aggregate shape that several derived steps can
/// share: same source, grouping and filter.
#[derive(Clone, PartialEq, Eq, Hash)]
struct MultiAggKey {
    source_table_id: String,
    group_keys: Vec<String>,
    group_exprs: Vec<String>,
    filter: Option<String>,
}

/// The first materialized multi-aggregate step of a shape.  A later step with
/// the same source/grouping whose aggregates are a subset can reuse its MV and
/// become a cheap row projection instead of a second row state.
#[derive(Clone)]
struct CachedMultiAgg {
    table: IvmTable,
    aggregates: Vec<MultiAggSpec>,
}

/// Caches shared by the two analyses of one statement (the probe and the
/// widened state) and by the split-target path.
#[derive(Default)]
struct DerivedCaches {
    derived: HashMap<String, CachedDerived>,
    multi_agg: HashMap<MultiAggKey, CachedMultiAgg>,
    rows: HashMap<String, ViewSpec>,
    /// How often each derived view is read by the statement being analyzed.
    /// A fused lookup owns its inner view only when that view has one reader,
    /// so a shared step keeps its own refresh.
    uses: HashMap<String, usize>,
    /// Inner lookup steps replaced by a fused step.  Their tables are released
    /// after the statement commits so they do not keep a second materialized
    /// copy of the same rows.
    fused_inner: Vec<String>,
}

fn multi_agg_key(spec: &ViewSpec) -> Option<MultiAggKey> {
    let ViewSpec::MultiAgg {
        source_table_id,
        group_keys,
        group_exprs,
        filter,
        ..
    } = spec
    else {
        return None;
    };
    Some(MultiAggKey {
        source_table_id: source_table_id.clone(),
        group_keys: group_keys.clone(),
        group_exprs: group_exprs.clone(),
        filter: filter.clone(),
    })
}

/// Build a row projection over an existing multi-aggregate MV when every
/// aggregate of `spec` already exists there.  A HAVING predicate is rewritten
/// onto the owner's column names and becomes the row filter; computed columns
/// (e.g. a hash key) are matched by their aggregate call, not by name.
fn merge_multi_agg_row(
    spec: &ViewSpec,
    owner_table_id: &str,
    owner_schema: &SchemaRef,
    owner_aggregates: &[MultiAggSpec],
    mv_schema: &SchemaRef,
) -> Option<ViewSpec> {
    let ViewSpec::MultiAgg {
        group_keys,
        aggregates,
        having,
        having_lookup,
        ..
    } = spec
    else {
        return None;
    };
    if having_lookup.is_some() {
        return None;
    }
    let mut output_columns = Vec::new();
    let mut output_exprs = Vec::new();
    let mut column_map = HashMap::new();
    for field in mv_schema.fields() {
        let name = field.name();
        if name == IVM_ROW_KINDS_COLUMN || name == IVM_EPOCH_COLUMN {
            continue;
        }
        if group_keys.iter().any(|key| key == name) {
            output_columns.push(name.clone());
            output_exprs.push(name.clone());
            continue;
        }
        let aggregate = aggregates
            .iter()
            .find(|aggregate| &aggregate.column == name)
            .or_else(|| aggregates.iter().find(|aggregate| &aggregate.call == name))?;
        let owner_column = owner_aggregates
            .iter()
            .find(|candidate| candidate.call == aggregate.call)
            .map(|candidate| candidate.column.clone())?;
        column_map.insert(aggregate.column.clone(), owner_column.clone());
        column_map.insert(name.clone(), owner_column.clone());
        output_columns.push(name.clone());
        output_exprs.push(owner_column);
    }
    let filter = match having {
        None => None,
        Some(having) => {
            let translated = rewrite_filter_columns(having, &column_map);
            parse_filter(&refresh_session(), owner_schema, &translated).ok()?;
            Some(translated)
        }
    };
    Some(ViewSpec::Row {
        view_id: DERIVED_PLACEHOLDER.to_string(),
        source_table_id: owner_table_id.to_string(),
        mv_table_id: DERIVED_PLACEHOLDER.to_string(),
        output_columns,
        output_exprs,
        filter,
        scalar: None,
    })
}

/// Rewrite the identifiers of a rendered HAVING predicate through `mapping`.
/// String literals are copied verbatim; quoted and unquoted column identifiers
/// are replaced by the quoted owner column.
fn rewrite_filter_columns(filter: &str, mapping: &HashMap<String, String>) -> String {
    fn quoted(name: &str) -> String {
        format!("\"{}\"", name.replace('\"', "\"\""))
    }
    let bytes = filter.as_bytes();
    let mut out = String::with_capacity(filter.len());
    let mut index = 0;
    while index < bytes.len() {
        let ch = filter[index..].chars().next().expect("in bounds");
        if ch == '\'' {
            // Copy a SQL string literal, including doubled quotes.
            out.push(ch);
            index += 1;
            while index < bytes.len() {
                let next = filter[index..].chars().next().expect("in bounds");
                out.push(next);
                index += next.len_utf8();
                if next == '\'' {
                    if filter[index..].starts_with('\'') {
                        out.push('\'');
                        index += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if ch == '\"' {
            let start = index;
            index += 1;
            let mut name = String::new();
            while index < bytes.len() {
                let next = filter[index..].chars().next().expect("in bounds");
                index += next.len_utf8();
                if next == '\"' {
                    if filter[index..].starts_with('\"') {
                        name.push('\"');
                        index += 1;
                    } else {
                        break;
                    }
                } else {
                    name.push(next);
                }
            }
            if let Some(owner) = mapping.get(&name) {
                out.push_str(&quoted(owner));
            } else {
                out.push_str(&filter[start..index]);
            }
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = index;
            index += ch.len_utf8();
            while index < bytes.len() {
                let next = filter[index..].chars().next().expect("in bounds");
                if next.is_ascii_alphanumeric() || next == '_' {
                    index += next.len_utf8();
                } else {
                    break;
                }
            }
            let name = &filter[start..index];
            if let Some(owner) = mapping.get(name) {
                out.push_str(&quoted(owner));
            } else {
                out.push_str(name);
            }
            continue;
        }
        out.push(ch);
        index += ch.len_utf8();
    }
    out
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

/// Whether a table's primary key is exactly the lookup key set.
fn same_key_set(primary_keys: &[String], keys: &[String]) -> bool {
    let mut primary = primary_keys.to_vec();
    let mut keys = keys.to_vec();
    primary.sort();
    keys.sort();
    primary == keys
}

/// The source table of a row view that is a pure identity projection.  Such a
/// view can be skipped: a consumer may read the upstream MV directly and keep
/// its output columns, which are a subset of the upstream schema.
fn identity_row_source(spec: &ViewSpec) -> Option<&str> {
    let ViewSpec::Row {
        source_table_id,
        output_columns,
        output_exprs,
        filter,
        ..
    } = spec
    else {
        return None;
    };
    if filter.is_some() {
        return None;
    }
    for (index, expression) in output_exprs.iter().enumerate() {
        if !expression.is_empty()
            && output_columns
                .get(index)
                .is_none_or(|column| column != expression)
        {
            return None;
        }
    }
    Some(source_table_id)
}

/// Replace row-view sources of a consumer with the row's upstream table.  The
/// row is a projection, so the consumer's referenced columns still resolve;
/// the caller validates the expected schema and reverts on a mismatch.
fn inline_identity_rows(
    spec: &mut ViewSpec,
    rows: &HashMap<String, ViewSpec>,
) -> Vec<String> {
    let mut replaced = Vec::new();
    let mut inline = |table_id: &mut String| {
        let Some(source) = rows.get(table_id.as_str()).and_then(identity_row_source)
        else {
            return;
        };
        let source = source.to_string();
        replaced.push(std::mem::replace(table_id, source));
    };
    match spec {
        ViewSpec::LookupJoin {
            left_table_id,
            right_table_id,
            ..
        }
        | ViewSpec::LeftJoin {
            left_table_id,
            right_table_id,
            ..
        }
        | ViewSpec::FullJoin {
            left_table_id,
            right_table_id,
            ..
        }
        | ViewSpec::Join {
            left_table_id,
            right_table_id,
            ..
        } => {
            inline(left_table_id);
            inline(right_table_id);
        }
        ViewSpec::MultiAgg {
            source_table_id, ..
        }
        | ViewSpec::SumCount {
            source_table_id, ..
        }
        | ViewSpec::MinMax {
            source_table_id, ..
        }
        | ViewSpec::DistinctAgg {
            source_table_id, ..
        }
        | ViewSpec::Variance {
            source_table_id, ..
        }
        | ViewSpec::Median {
            source_table_id, ..
        }
        | ViewSpec::BoolAgg {
            source_table_id, ..
        }
        | ViewSpec::StringAgg {
            source_table_id, ..
        }
        | ViewSpec::ArrayAgg {
            source_table_id, ..
        }
        | ViewSpec::ApproxDistinct {
            source_table_id, ..
        }
        | ViewSpec::ApproxPercentile {
            source_table_id, ..
        }
        | ViewSpec::ComputedAgg {
            source_table_id, ..
        }
        | ViewSpec::Window {
            source_table_id, ..
        }
        | ViewSpec::SemiAnti {
            left_table_id: source_table_id,
            ..
        }
        | ViewSpec::TopK {
            source_table_id, ..
        } => inline(source_table_id),
        _ => {}
    }
    replaced
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

/// A fused multi-right lookup and the inner view whose join-key index it
/// adopts, so the same left rows are not indexed twice.
struct FusedMultiLookup {
    spec: ViewSpec,
    inner_view_id: String,
}

/// Count the nodes a derived-source rewrite replaces.  A plan may read the
/// same derived source twice; the fusion below only owns the inner view when
/// it has exactly one reader.
fn plan_occurrences(plan: &LogicalPlan, target: &LogicalPlan) -> usize {
    let mut count = 0;
    let _ = plan.apply(|node| {
        if node == target {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    count.max(1)
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
    runtime: Arc<IvmRuntime>,
    session: Option<SessionContext>,
    refresh_upstream: bool,
    /// Namespace used for a one-part table name and for PostgreSQL's
    /// `public` schema.  The Rust API leaves it unset (`default`).
    default_namespace: Option<String>,
}

impl IvmSqlExecutor {
    /// Build an executor that plans SQL in an internal DataFusion session.
    pub fn new(runtime: IvmRuntime) -> Self {
        Self::with_shared_runtime(Arc::new(runtime))
    }

    /// Build an executor over a runtime shared by the process, such as the
    /// PostgreSQL front end's service.
    pub fn with_shared_runtime(runtime: Arc<IvmRuntime>) -> Self {
        Self {
            runtime,
            session: None,
            refresh_upstream: false,
            default_namespace: None,
        }
    }

    /// Resolve unqualified names and PostgreSQL's `public` schema against
    /// `namespace` instead of the LakeSoul `default` namespace.
    pub fn with_default_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.default_namespace = Some(namespace.into());
        self
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
        self.runtime.as_ref()
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
        let (namespace, table_name) = split_object_name(
            insert_target(&insert)?,
            self.default_namespace.as_deref(),
        )?;
        let mv = self.runtime.open_table(&table_name, &namespace).await?;
        let view_id = mv.table_id.clone();
        let lock_owner = uuid::Uuid::new_v4().to_string();
        let lock_ttl_ms = crate::config::options().refresh_lock_ttl_ms.max(1000) as i64;
        // ViewLockGuard::drop releases the lease from a spawned task, so a
        // back-to-back statement can still observe the previous statement's
        // lease for a few milliseconds.  Wait briefly for it instead of
        // failing a statement that is about to become valid; a genuinely
        // concurrent refresh still fails after the bounded wait.
        let mut lock_waits = 0;
        while !self
            .runtime
            .metadata()
            .try_lock_view(&view_id, &lock_owner, lock_ttl_ms)
            .await?
        {
            lock_waits += 1;
            if lock_waits >= 40 {
                return Err(rootcause::report!(
                    "view {view_id} is being refreshed by another statement"
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let heartbeat = spawn_view_lock_heartbeat(
            self.runtime.metadata().clone(),
            view_id.clone(),
            lock_owner.clone(),
            lock_ttl_ms,
        );
        let _lock = ViewLockGuard {
            metadata: self.runtime.metadata().clone(),
            view_id: view_id.clone(),
            owner: lock_owner,
            heartbeat,
        };

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
            let (namespace, name) =
                split_object_name(&relation, self.default_namespace.as_deref())?;
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
        let mut deferred = Vec::new();
        let mut caches = DerivedCaches::default();
        let analyzed = self
            .analyze_with_derived(
                &plan,
                &mut tables,
                &request,
                &mv,
                &mut derived,
                &mut deferred,
                &mut caches,
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
        let needs_sink = target_needs_sink(&mv, &expected);
        if needs_sink && fallback {
            // Report why the shape is not maintained before a target schema
            // problem masks it; the fallback cannot run on a mismatched target.
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
        // Validate the target's IVM bookkeeping after the statement analysis:
        // an unsupported shape must report its own reason first.  A missing
        // `__ivm_epoch` is added to the schema and forces a rebuild below.
        let (mv, epoch_added) = self.runtime.ensure_target_epoch_column(&mv).await?;
        if needs_sink {
            return self
                .execute_split_target(
                    &mv,
                    &view_id,
                    plan,
                    tables,
                    sql,
                    insert.overwrite,
                    derived,
                    deferred,
                    caches,
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
            match self.ensure_state_table(&mv, &probe).await? {
                Some(state_table_id) => {
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
                        &mut deferred,
                        &mut caches,
                    )
                    .await?
                    .0
                }
                None => probe,
            }
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
        if fallback {
            // A fallback statement recomputes its whole output; the derived
            // steps of a failed analysis are not part of its definition.
            deferred.clear();
        } else {
            self.flush_deferred(&mut deferred).await?;
        }
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
        let dirty = epoch_added
            || self
                .runtime
                .metadata()
                .view_status(&view_id)
                .await?
                .as_deref()
                == Some("dirty");
        let target_id = mv.table_id.clone();
        let (action, epoch) = if fallback {
            // Every fallback refresh recomputes the statement; the action says
            // so even when the definition is unchanged.  A no-op refresh (no
            // source change) reports no epoch.
            if same_definition && !insert.overwrite && !dirty {
                match self.runtime.refresh_spec_target(&spec, &target_id).await {
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
                                let epoch = self
                                    .runtime
                                    .rebuild_spec_target(&spec, &target_id)
                                    .await?;
                                (IvmExecutionAction::Fallback, Some(epoch))
                            }
                        }
                    }
                    Err(error) => return Err(error),
                }
            } else {
                let epoch = self.runtime.rebuild_spec_target(&spec, &target_id).await?;
                self.runtime
                    .metadata()
                    .set_view_definition(&view_id, &definition, sql)
                    .await?;
                (IvmExecutionAction::Fallback, Some(epoch))
            }
        } else if insert.overwrite {
            if same_definition {
                let epoch = self.runtime.rebuild_spec_target(&spec, &target_id).await?;
                (IvmExecutionAction::Overwrite, Some(epoch))
            } else {
                let epoch = self.runtime.rebuild_spec_target(&spec, &target_id).await?;
                self.runtime
                    .metadata()
                    .set_view_definition(&view_id, &definition, sql)
                    .await?;
                (IvmExecutionAction::Rebuild, Some(epoch))
            }
        } else if same_definition && !dirty {
            match self.runtime.refresh_spec_target(&spec, &target_id).await {
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
                            let epoch = self
                                .runtime
                                .rebuild_spec_target(&spec, &target_id)
                                .await?;
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
            let epoch = self.runtime.rebuild_spec_target(&spec, &target_id).await?;
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

        self.release_fused_inner_views(&caches).await?;
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
        mut deferred: Vec<String>,
        mut caches: DerivedCaches,
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
        let (state_spec, _) = self
            .analyze_with_derived(
                &plan,
                &mut tables,
                &request,
                target,
                &mut derived,
                &mut deferred,
                &mut caches,
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
        if needs_state_table(&state_spec)
            && let Some(state_table_id) =
                self.ensure_state_table(&state_table, &state_spec).await?
        {
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
        self.flush_deferred(&mut deferred).await?;
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
            let epoch = self
                .runtime
                .rebuild_spec_target(&sink_spec, &target.table_id)
                .await?;
            self.runtime
                .metadata()
                .set_view_definition(view_id, &sink_definition, sql)
                .await?;
            Some(epoch)
        } else {
            match self
                .runtime
                .refresh_spec_target(&sink_spec, &target.table_id)
                .await
            {
                Ok(epoch) => epoch,
                Err(error) if IvmRuntime::rebuild_required(&error) => {
                    sink_rebuilt = true;
                    let epoch = self
                        .runtime
                        .rebuild_spec_target(&sink_spec, &target.table_id)
                        .await?;
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
        self.release_fused_inner_views(&caches).await?;
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

    /// Refresh the derived views whose unchanged definitions were deferred
    /// during analysis.  The chain scheduler discovers their dependencies and
    /// runs independent views concurrently when
    /// [`IvmOptions::view_parallelism`](crate::config::IvmOptions::view_parallelism)
    /// is above one.
    async fn flush_deferred(&self, deferred: &mut Vec<String>) -> Result<()> {
        if deferred.is_empty() {
            return Ok(());
        }
        let roots = std::mem::take(deferred);
        let refreshed = self
            .runtime
            .refresh_view_chains(&roots, &ChainPolicy::default())
            .await?;
        tracing::debug!(views = refreshed.len(), "deferred derived views refreshed");
        Ok(())
    }

    /// Fuse `outer LookupJoin(left = inner LookupJoin)` into one
    /// MultiLookupJoin when both lookups use the same plain join keys and both
    /// rights are unique by key.  The shared left index remains the only
    /// right-change fanout path; no base scan is introduced.
    async fn fuse_multi_lookup(
        &self,
        spec: &ViewSpec,
        tables: &HashMap<String, IvmTable>,
        deferred: &mut Vec<String>,
        caches: &DerivedCaches,
    ) -> Result<Option<FusedMultiLookup>> {
        let ViewSpec::LookupJoin {
            left_table_id,
            right_table_id,
            join_keys,
            right_keys,
            key_exprs,
            left_filter,
            right_filter,
            output_columns,
            state_table_id,
            ..
        } = spec
        else {
            return Ok(None);
        };
        if output_columns.is_empty()
            || left_filter.is_some()
            || state_table_id.is_some()
            || key_exprs
                .left
                .iter()
                .chain(&key_exprs.right)
                .any(|expr| !expr.is_empty())
        {
            return Ok(None);
        }
        let outer_right_keys = if right_keys.is_empty() {
            join_keys.clone()
        } else {
            right_keys.clone()
        };
        let outer_right = self.runtime.open_table_by_id(right_table_id).await?;
        if !same_key_set(&outer_right.primary_keys, &outer_right_keys)
            || !same_key_set(&outer_right.primary_keys, join_keys)
        {
            return Ok(None);
        }
        if caches.uses.get(left_table_id).copied() != Some(1) {
            return Ok(None);
        }
        let Some(value) = self.runtime.metadata().get_view_spec(left_table_id).await?
        else {
            return Ok(None);
        };
        let inner: ViewSpec = serde_json::from_value(value)?;
        let ViewSpec::LookupJoin {
            left_table_id: base_id,
            right_table_id: inner_right_id,
            join_keys: inner_keys,
            right_keys: inner_right_keys,
            key_exprs: inner_key_exprs,
            left_filter: inner_left_filter,
            right_filter: inner_right_filter,
            output_columns: inner_columns,
            state_table_id: inner_state,
            ..
        } = inner
        else {
            return Ok(None);
        };
        if inner_keys != *join_keys
            || inner_left_filter.is_some()
            || inner_state.is_some()
            || inner_columns.is_empty()
            || inner_key_exprs
                .left
                .iter()
                .chain(&inner_key_exprs.right)
                .any(|expr| !expr.is_empty())
        {
            return Ok(None);
        }
        let inner_right_keys = if inner_right_keys.is_empty() {
            inner_keys.clone()
        } else {
            inner_right_keys
        };
        if inner_right_keys != outer_right_keys {
            return Ok(None);
        }
        let base = self.runtime.open_table_by_id(&base_id).await?;
        let inner_right = self.runtime.open_table_by_id(&inner_right_id).await?;
        if !same_key_set(&inner_right.primary_keys, &inner_right_keys)
            || !same_key_set(&inner_right.primary_keys, join_keys)
        {
            return Ok(None);
        }
        let mut columns = Vec::new();
        for key in join_keys {
            columns.push(MultiLookupColumn {
                source: 0,
                column: key.clone(),
                name: key.clone(),
            });
        }
        for column in output_columns {
            if column.side == JoinSide::Right {
                columns.push(MultiLookupColumn {
                    source: 2,
                    column: column.column.clone(),
                    name: column.name.clone(),
                });
                continue;
            }
            let Some(inner_column) = inner_columns
                .iter()
                .find(|candidate| candidate.name == column.column)
            else {
                return Ok(None);
            };
            columns.push(MultiLookupColumn {
                source: if inner_column.side == JoinSide::Right {
                    1
                } else {
                    0
                },
                column: inner_column.column.clone(),
                name: column.name.clone(),
            });
        }
        for key in &base.primary_keys {
            if !join_keys.contains(key)
                && !columns
                    .iter()
                    .any(|column| column.column == *key && column.name == *key)
            {
                columns.push(MultiLookupColumn {
                    source: 0,
                    column: key.clone(),
                    name: key.clone(),
                });
            }
        }
        let chain = ViewSpec::MultiLookupJoin {
            view_id: DERIVED_PLACEHOLDER.to_string(),
            left_table_id: base_id,
            mv_table_id: DERIVED_PLACEHOLDER.to_string(),
            join_keys: join_keys.clone(),
            rights: vec![
                MultiLookupRight {
                    table_id: inner_right_id,
                    filter: inner_right_filter,
                },
                MultiLookupRight {
                    table_id: right_table_id.clone(),
                    filter: right_filter.clone(),
                },
            ],
            output_columns: columns,
            refresh_interval_ms: 0,
        };
        let Ok(expected) = expected_mv_schema(spec, tables) else {
            return Ok(None);
        };
        let Ok(actual) = expected_mv_schema(&chain, tables) else {
            return Ok(None);
        };
        let schema_ok = schema_matches(actual.as_ref(), expected.as_ref());
        let keys_ok = self.derived_primary_keys(spec).await?
            == self.derived_primary_keys(&chain).await?;
        if !schema_ok || !keys_ok {
            return Ok(None);
        }
        deferred.retain(|view_id| view_id != left_table_id);
        Ok(Some(FusedMultiLookup {
            spec: chain,
            inner_view_id: left_table_id.clone(),
        }))
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
        deferred: &mut Vec<String>,
        caches: &mut DerivedCaches,
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
                             IvmOptions::max_derived_steps to allow more)",
                            max_derived_steps()
                        )
                        .attach(UnsupportedShape));
                    }
                    let key = plan_key(&source);
                    let reused = caches.derived.get(&key).cloned();
                    let (table, view_id, mapping, fresh) = match reused {
                        Some(step) => (step.table, step.view_id, step.mapping, false),
                        None => {
                            let (table, view_id, mapping) = self
                                .materialize_derived(
                                    &source, target, tables, steps, deferred, caches,
                                )
                                .await?;
                            caches.derived.insert(
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
                    let uses = plan_occurrences(&plan, &source);
                    plan = replace_plan(&plan, &source, &scan)?;
                    let renames = mapping
                        .into_iter()
                        .filter(|(field, column)| field != column)
                        .collect::<HashMap<_, _>>();
                    if !renames.is_empty() {
                        plan = rename_derived_columns(plan, &renames)?;
                    }
                    tables.insert(table.table_name.clone(), table);
                    *caches.uses.entry(view_id.clone()).or_default() += uses;
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
        deferred: &mut Vec<String>,
        caches: &mut DerivedCaches,
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
        let (mut spec, _) = Box::pin(self.analyze_with_derived(
            subplan, tables, &request, target, steps, deferred, caches,
        ))
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
        let mut adopt_state_from = None;
        if let Some(fused) = self
            .fuse_multi_lookup(&spec, tables, deferred, caches)
            .await?
        {
            if !caches.fused_inner.contains(&fused.inner_view_id) {
                caches.fused_inner.push(fused.inner_view_id.clone());
            }
            spec = fused.spec;
            adopt_state_from = Some(fused.inner_view_id);
        }
        // Two occurrences of the same CTE may differ only in relation aliases;
        // their normalized specs are identical, so reuse the step table (and
        // skip creating a new one).
        let spec_key = format!("spec:{}", definition_hash(&spec)?);
        if let Some(step) = caches.derived.get(&spec_key).cloned() {
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
        // A pure identity row step can be skipped: let this consumer read the
        // row's upstream MV directly, and remove the row from the deferred
        // refresh set.  The expected schema check makes the rewrite safe.
        let before_inline = spec.clone();
        let before_schema = schema.clone();
        let replaced_rows = inline_identity_rows(&mut spec, &caches.rows);
        if !replaced_rows.is_empty() {
            let after_schema = expected_mv_schema(&spec, tables)?;
            if schema_matches(after_schema.as_ref(), before_schema.as_ref()) {
                for row_id in replaced_rows {
                    deferred.retain(|view_id| view_id != &row_id);
                }
            } else {
                spec = before_inline;
            }
        }
        let index = steps.len();
        let original_spec = spec.clone();
        let merge_key = multi_agg_key(&original_spec);
        let merge_owner = merge_key
            .as_ref()
            .and_then(|key| caches.multi_agg.get(key))
            .cloned();
        let merged = merge_owner.as_ref().and_then(|owner| {
            merge_multi_agg_row(
                &original_spec,
                &owner.table.table_id,
                &owner.table.schema,
                &owner.aggregates,
                &schema,
            )
        });
        // A cardinality-preserving step can carry its source's real range
        // partition columns through its own MV.  The column is materialized
        // for the internal step only; the business output mapping ignores it,
        // while a downstream state table can mirror the source partitioning.
        // A merged multi-aggregate row reuses the owner's layout, so it is not
        // widened here.
        let partitions = if merged.is_some() {
            DerivedPartitions::default()
        } else {
            propagate_derived_partitions(&mut spec, tables)?
        };
        tracing::debug!(
            target = %target.table_name,
            index,
            kind = spec.kind(),
            carried = ?partitions.carried,
            internal = ?partitions.internal,
            "derived step partitions"
        );
        let table = self
            .ensure_derived_table(target, index, &spec, tables, &partitions)
            .await?;
        // The derived view is registered under its MV table id (the view
        // identity of every view), so the chain graph finds it when the outer
        // spec lists the derived table as a source.
        let view_id = table.table_id.clone();
        let mut merged_into_owner = false;
        if let Some(mut row) = merged {
            row.set_derived_identity(&view_id, &view_id)?;
            let row_schema = expected_mv_schema(&row, tables)?;
            if schema_matches(row_schema.as_ref(), schema.as_ref()) {
                // The existing MV carries every aggregate this step needs;
                // a row projection (with the HAVING as a filter) replaces the
                // second multi-aggregate state.
                spec = row;
                merged_into_owner = true;
                self.drop_generated_state(&view_id).await?;
            }
        }
        if !merged_into_owner
            && let Some(key) = merge_key
            && let ViewSpec::MultiAgg {
                aggregates,
                having,
                having_lookup,
                ..
            } = &original_spec
            && having.is_none()
            && having_lookup.is_none()
        {
            caches
                .multi_agg
                .entry(key)
                .or_insert_with(|| CachedMultiAgg {
                    table: table.clone(),
                    aggregates: aggregates.clone(),
                });
        }
        spec.set_derived_identity(&view_id, &view_id)?;
        if let Some(inner_view_id) = adopt_state_from {
            self.adopt_lookup_join_state(&inner_view_id, &view_id)
                .await?;
        }
        if matches!(spec, ViewSpec::Row { .. }) {
            caches.rows.insert(view_id.clone(), spec.clone());
        }
        // A `MIN`/`MAX` or `DISTINCT` aggregate step carries its own
        // value-count state table, created next to the step MV.
        if needs_state_table(&spec)
            && let Some(state_table_id) = self.ensure_state_table(&table, &spec).await?
        {
            spec.set_derived_state_table(&state_table_id)?;
        }
        let definition = definition_hash(&spec)?;
        let stored = self.runtime.metadata().view_definition(&view_id).await?;
        let same_definition =
            stored.as_ref().is_some_and(|(hash, _)| hash == &definition);
        if same_definition {
            // Defer the incremental refresh to the chain scheduler: all
            // derived steps of this statement are materialized first, then
            // refreshed together in dependency order and in parallel.
            if !deferred.contains(&view_id) {
                deferred.push(view_id.clone());
            }
        } else {
            // A changed definition must see current upstreams; flush the
            // deferred steps before rebuilding it.
            self.flush_deferred(deferred).await?;
            self.runtime.rebuild_spec(&spec).await?;
            self.runtime
                .metadata()
                .set_view_definition(&view_id, &definition, "")
                .await?;
        }
        caches.derived.insert(
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
        partitions: &DerivedPartitions,
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
                && existing.range_partition_columns == partitions.carried
                && existing.internal_columns == partitions.internal
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
        if !partitions.carried.is_empty() {
            options = options.with_range_partition_columns(partitions.carried.clone());
        }
        if !partitions.internal.is_empty() {
            options = options.with_internal_columns(partitions.internal.clone());
        }
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
            | ViewSpec::MultiLookupJoin { left_table_id, .. }
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

    /// The source columns the row-level state of a multi-aggregate view must
    /// keep: the group keys, every column an aggregate call or filter references,
    /// and the source primary keys (added by the projected source).  Columns the
    /// view never reads are not copied into the state.
    fn multi_agg_rows_state_columns(
        context: &datafusion::prelude::SessionContext,
        source_schema: &arrow_schema::SchemaRef,
        group_keys: &[String],
        group_exprs: &[String],
        aggregates: &[crate::runtime::MultiAggSpec],
        filter: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut columns = group_keys.to_vec();
        let df_schema =
            datafusion::common::DFSchema::try_from(source_schema.as_ref().clone())
                .map_err(|error| rootcause::report!("invalid source schema: {error}"))?;
        let mut add = |expression: &str| -> Result<()> {
            let expression = context
                .state()
                .create_logical_expr(expression, &df_schema)
                .map_err(|error| {
                    rootcause::report!(
                        "invalid aggregate expression {expression:?}: {error}"
                    )
                })?;
            for column in expression.column_refs() {
                if !columns.contains(&column.name) {
                    columns.push(column.name.clone());
                }
            }
            Ok(())
        };
        for (key, expression) in group_keys.iter().zip(group_exprs) {
            if !expression.is_empty() && expression != key {
                add(expression)?;
            }
        }
        for aggregate in aggregates {
            add(&aggregate.call)?;
        }
        if let Some(filter) = filter {
            for column in crate::runtime::filter_columns(context, source_schema, filter)?
            {
                if !columns.contains(&column) {
                    columns.push(column);
                }
            }
        }
        Ok(columns)
    }

    /// Ensure the partial state of a decomposable multi-aggregate view.
    ///
    /// The partial state stores one row per `(group key, source partition)`
    /// with the aggregates of that partition, so a refresh can recompute only
    /// the changed partitions and then merge all partials for the affected
    /// groups into the final MV.  It is only needed when the row state is
    /// range-partitioned and the view has group keys.
    async fn ensure_multi_agg_partial_state(
        &self,
        view_id: &str,
        mv: &IvmTable,
        state: &IvmTable,
        group_keys: &[String],
        range_partitions: &[String],
        plan: &MultiAggPartialPlan,
    ) -> Result<Option<IvmTable>> {
        if group_keys.is_empty() || range_partitions.is_empty() {
            return Ok(None);
        }
        let mut keys = group_keys.to_vec();
        for column in range_partitions {
            if !keys.contains(column) {
                keys.push(column.clone());
            }
        }
        let mut fields = Vec::with_capacity(keys.len() + plan.columns.len() + 2);
        for column in &keys {
            let field = state.schema.field_with_name(column)?;
            fields.push(Arc::new(Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable(),
            )));
        }
        for (column, data_type) in plan.partial_columns() {
            fields.push(Arc::new(Field::new(column, data_type, true)));
        }
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
        let schema = Arc::new(Schema::new(fields));
        let name = format!("{}__partials", state.table_name);
        let path = format!("{}__partials", state.table_path);
        if let Ok(existing) = self.runtime.open_table(&name, &mv.namespace).await {
            if schema_matches(&existing.schema, &schema)
                && existing.primary_keys == keys
                && existing.range_partition_columns == range_partitions
                && existing.bucket_columns == group_keys
            {
                self.runtime
                    .metadata()
                    .register_state(view_id, StateRole::MultiAggPartials, &existing)
                    .await?;
                return Ok(Some(existing));
            }
            let _ = self
                .runtime
                .metadata()
                .unregister_state(view_id, StateRole::MultiAggPartials)
                .await;
            let _ = self
                .runtime
                .metadata()
                .delete_view(&existing.table_id)
                .await;
            let _ = self.runtime.client().drop_table(&name, &mv.namespace).await;
        }
        let mut options = IvmTableOptions::new(name, path, schema)
            .with_namespace(mv.namespace.clone())
            .with_primary_keys(keys)
            .with_bucket_columns(group_keys.to_vec())
            .with_range_partition_columns(range_partitions.to_vec())
            .with_cdc_column(IVM_ROW_KINDS_COLUMN)
            .with_file_format(keyed_state_file_format());
        options.hash_bucket_num = state.hash_bucket_num.clone();
        let table = create_ivm_table(self.runtime.client(), options).await?;
        self.runtime
            .metadata()
            .register_state(view_id, StateRole::MultiAggPartials, &table)
            .await?;
        Ok(Some(table))
    }

    /// The value-count state table of the view, reusing the registered one when
    /// its schema still matches and recreating it otherwise.
    async fn ensure_state_table(
        &self,
        mv: &IvmTable,
        spec: &ViewSpec,
    ) -> Result<Option<String>> {
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
            let mut replaced = false;
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
                    return Ok(Some(table.table_id));
                }
                self.runtime
                    .metadata()
                    .unregister_state(spec.view_id(), StateRole::State)
                    .await?;
                // Keep the old physical table: a stored spec may still carry
                // its id until the re-analysis persists the replacement.
                replaced = true;
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
            if replaced {
                // The state starts empty, so the next refresh must rebuild the
                // view instead of applying a delta to an empty state.
                self.runtime
                    .metadata()
                    .set_view_status(spec.view_id(), "dirty")
                    .await?;
            }
            return Ok(Some(table.table_id));
        }
        if !multi_agg_value_state_eligible(spec)
            && let ViewSpec::MultiAgg {
                source_table_id,
                group_keys,
                group_exprs,
                aggregates,
                filter,
                ..
            } = spec
        {
            let source = self.runtime.open_table_by_id(source_table_id).await?;
            if source.primary_keys.is_empty() {
                return Err(rootcause::report!(
                    "multi-agg row state needs a keyed source"
                ));
            }
            if group_exprs.iter().zip(group_keys).any(|(expr, key)| {
                !expr.is_empty()
                    && expr != key
                    && source.schema.field_with_name(key).is_ok()
            }) {
                tracing::debug!(
                    table = %source.table_name,
                    "multi-agg group expression reuses a source column; using full recompute"
                );
                return Ok(None);
            }
            let state_columns = Self::multi_agg_rows_state_columns(
                &refresh_session(),
                &source.schema,
                group_keys,
                group_exprs,
                aggregates,
                filter.as_deref(),
            )?;
            let state_source = source.projected_state_source(&state_columns)?;
            let schema = multi_agg_rows_state_schema_for(
                &state_source.schema,
                group_keys,
                group_exprs,
            )?;
            let mut primary_keys = group_keys.clone();
            // A source range partition column is a real state key even when
            // the SQL does not group by it.  Keeping it in the key makes a
            // row that moves between source partitions distinct, and lets the
            // state table mirror the source partitioning.
            for column in &source.range_partition_columns {
                if source.schema.field_with_name(column).is_ok()
                    && !primary_keys.contains(column)
                {
                    primary_keys.push(column.clone());
                }
            }
            for key in &state_source.primary_keys {
                if !primary_keys.contains(key) {
                    primary_keys.push(key.clone());
                }
            }
            let bucket_columns = group_keys.clone();
            let range_partitions =
                state_range_partition_columns(&state_source, &primary_keys);
            let partial_plan = {
                let aggregates = aggregates
                    .iter()
                    .map(|aggregate| {
                        Ok((
                            aggregate.call.clone(),
                            aggregate.column.clone(),
                            decode_data_type(&aggregate.result)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                multi_agg_partial_plan(group_keys, &aggregates, &source.schema)?
            };
            let mut replaced = false;
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
                    if let Some(plan) = &partial_plan {
                        self.ensure_multi_agg_partial_state(
                            spec.view_id(),
                            mv,
                            &table,
                            group_keys,
                            &range_partitions,
                            plan,
                        )
                        .await?;
                    }
                    return Ok(Some(table.table_id));
                }
                self.runtime
                    .metadata()
                    .unregister_state(spec.view_id(), StateRole::MultiAggRows)
                    .await?;
                replaced = true;
                // Keep the old physical table: a stored view spec may still
                // reference its id until the re-analysis below persists the
                // new state id.  It stays unregistered and inert afterwards.
            }
            // A compact aggregate state from an older definition would make
            // the refresh pick the wrong path; remove it when switching to
            // row-level state.
            if self
                .runtime
                .metadata()
                .get_state(spec.view_id(), StateRole::State)
                .await?
                .is_some()
            {
                self.runtime
                    .metadata()
                    .unregister_state(spec.view_id(), StateRole::State)
                    .await?;
                // The same physical table can be registered under both roles;
                // leave it in place for the same migration reason as above.
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
                options = options.with_range_partition_columns(range_partitions.clone());
            }
            options.hash_bucket_num = mv.hash_bucket_num.clone();
            let table = create_ivm_table(self.runtime.client(), options).await?;
            self.runtime
                .metadata()
                .register_state(spec.view_id(), StateRole::MultiAggRows, &table)
                .await?;
            if let Some(plan) = &partial_plan {
                self.ensure_multi_agg_partial_state(
                    spec.view_id(),
                    mv,
                    &table,
                    group_keys,
                    &range_partitions,
                    plan,
                )
                .await?;
            }
            if replaced {
                // An empty recreated row state cannot answer the old groups;
                // rebuild the view from the source instead.
                self.runtime
                    .metadata()
                    .set_view_status(spec.view_id(), "dirty")
                    .await?;
            }
            return Ok(Some(table.table_id));
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
                    return Ok(Some(table.table_id));
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
            return Ok(Some(table.table_id));
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
                return Ok(Some(table.table_id));
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
        Ok(Some(table.table_id))
    }

    /// Drop the generated state tables of a view (the MV is recycled by the
    /// rebuild, not dropped).
    /// Move the inner lookup's join-key index to the fused view.  Both views
    /// have the same left source and join keys, so the rows are the same; the
    /// inner step is no longer refreshed after fusion.
    async fn adopt_lookup_join_state(&self, from: &str, to: &str) -> Result<()> {
        if from == to {
            return Ok(());
        }
        let Some(state) = self
            .runtime
            .metadata()
            .get_state(from, StateRole::JoinLeft)
            .await?
        else {
            return Ok(());
        };
        if self
            .runtime
            .metadata()
            .get_state(to, StateRole::JoinLeft)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let table = self.runtime.open_table_by_id(&state.table_id).await?;
        self.runtime
            .metadata()
            .register_state(to, StateRole::JoinLeft, &table)
            .await?;
        self.runtime
            .metadata()
            .unregister_state(from, StateRole::JoinLeft)
            .await?;
        Ok(())
    }

    /// Release the inner lookup steps that a fused step replaced.  The view
    /// metadata stays so a later execution reuses its shape, but its table is
    /// emptied and its cursors are gone; if it is ever maintained on its own
    /// again the dirty marker forces a rebuild.
    async fn release_fused_inner_views(&self, caches: &DerivedCaches) -> Result<()> {
        for view_id in &caches.fused_inner {
            let Ok(table) = self.runtime.open_table_by_id(view_id).await else {
                continue;
            };
            self.drop_generated_state(view_id).await?;
            table.truncate(self.runtime.client()).await?;
            self.runtime.metadata().delete_cursors(view_id).await?;
            self.runtime
                .metadata()
                .set_view_status(view_id, "dirty")
                .await?;
        }
        Ok(())
    }

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

/// Releases the per-view refresh lease when a statement ends, including on an
/// early error or a dropped future.
struct ViewLockGuard {
    metadata: crate::metadata::IvmMetadata,
    view_id: String,
    owner: String,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl Drop for ViewLockGuard {
    fn drop(&mut self) {
        self.heartbeat.abort();
        let metadata = self.metadata.clone();
        let view_id = self.view_id.clone();
        let owner = self.owner.clone();
        tokio::spawn(async move {
            let _ = metadata.unlock_view(&view_id, &owner).await;
        });
    }
}

/// Renew a view's refresh lease while the statement is running.  A killed
/// coordinator process stops renewing; the lease then expires and another
/// coordinator can resume the persisted commit plan.
fn spawn_view_lock_heartbeat(
    metadata: crate::metadata::IvmMetadata,
    view_id: String,
    owner: String,
    ttl_ms: i64,
) -> tokio::task::JoinHandle<()> {
    let interval = std::time::Duration::from_millis((ttl_ms as u64 / 3).max(50));
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match metadata.renew_view_lock(&view_id, &owner, ttl_ms).await {
                Ok(true) => {}
                Ok(false) | Err(_) => break,
            }
        }
    })
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

fn split_object_name(
    name: &ObjectName,
    default_namespace: Option<&str>,
) -> Result<(String, String)> {
    let parts = object_name_parts(name)?;
    let default = default_namespace.unwrap_or("default");
    match parts.as_slice() {
        [table] => Ok((default.to_string(), table.clone())),
        [schema, table]
            if default_namespace.is_some() && schema.eq_ignore_ascii_case("public") =>
        {
            Ok((default.to_string(), table.clone()))
        }
        [namespace, table] => Ok((namespace.clone(), table.clone())),
        // Ignore the catalog: LakeSoul lives in one PostgreSQL database.
        [_catalog, schema, table]
            if default_namespace.is_some() && schema.eq_ignore_ascii_case("public") =>
        {
            Ok((default.to_string(), table.clone()))
        }
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
            having_lookup,
            ..
        } => !group_keys.is_empty() && having_lookup.is_none(),
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

/// Carry a source's real range partition columns through a derived step that
/// preserves the source's cardinality.
///
/// A range-partitioned table treats the same key in two partitions as two
/// rows.  A projection-like step (a row, a semi/anti filter, a union of rows
/// or a window that already carries payload columns) therefore has to keep the
/// partition value: dropping it would merge rows the source keeps apart, and a
/// downstream state table could not mirror the source partitioning.  The
/// column is appended to the step's materialized columns only; the derived
/// output mapping ignores it, so the business output is unchanged.
///
/// Aggregate and join steps are not widened: grouping by a partition changes
/// the row cardinality, and a join can multiply rows on the other side.
fn propagate_derived_partitions(
    spec: &mut ViewSpec,
    tables: &HashMap<String, IvmTable>,
) -> Result<DerivedPartitions> {
    match spec {
        ViewSpec::Row {
            source_table_id,
            output_columns,
            output_exprs,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            let mut partitions = DerivedPartitions::default();
            for column in &source.range_partition_columns {
                if source.schema.field_with_name(column).is_err() {
                    continue;
                }
                if output_columns.is_empty() {
                    // Every source column is materialized already.  A column
                    // the source only carries for its own internal
                    // partitioning stays internal here too.
                    partitions.carried.push(column.clone());
                    if source.internal_columns.contains(column) {
                        partitions.internal.push(column.clone());
                    }
                    continue;
                }
                match output_columns
                    .iter()
                    .position(|candidate| candidate == column)
                {
                    Some(position) => {
                        let unchanged = output_exprs.is_empty()
                            || output_exprs.get(position) == Some(column);
                        if unchanged {
                            partitions.carried.push(column.clone());
                        }
                    }
                    None => {
                        output_columns.push(column.clone());
                        if !output_exprs.is_empty() {
                            output_exprs.push(column.clone());
                        }
                        partitions.carried.push(column.clone());
                        partitions.internal.push(column.clone());
                    }
                }
            }
            Ok(partitions)
        }
        ViewSpec::SemiAnti {
            left_table_id,
            output_columns,
            ..
        } => {
            let source = find_table(tables, left_table_id)?;
            let mut partitions = DerivedPartitions::default();
            for column in &source.range_partition_columns {
                if source.schema.field_with_name(column).is_err() {
                    continue;
                }
                if output_columns.is_empty() {
                    // Every left column is materialized.  A column the left
                    // source only carries for its own internal partitioning
                    // stays internal here too.
                    if source.internal_columns.contains(column) {
                        partitions.internal.push(column.clone());
                    }
                } else if !output_columns.contains(column) {
                    output_columns.push(column.clone());
                    partitions.internal.push(column.clone());
                }
                partitions.carried.push(column.clone());
            }
            Ok(partitions)
        }
        ViewSpec::Window {
            source_table_id,
            partition_keys,
            payload_columns,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            let mut partitions = DerivedPartitions::default();
            for column in &source.range_partition_columns {
                if source.schema.field_with_name(column).is_err() {
                    continue;
                }
                // The MV keys are materialized first; anything else has to be
                // a payload column to survive the window projection.
                if !partition_keys.contains(column)
                    && !source.primary_keys.contains(column)
                    && !payload_columns.contains(column)
                {
                    payload_columns.push(column.clone());
                    partitions.internal.push(column.clone());
                }
                partitions.carried.push(column.clone());
            }
            Ok(partitions)
        }
        ViewSpec::MultiWindow {
            source_table_id,
            windows,
            payload_columns,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            let mut partitions = DerivedPartitions::default();
            for column in &source.range_partition_columns {
                if source.schema.field_with_name(column).is_err() {
                    continue;
                }
                let keyed = windows
                    .iter()
                    .any(|window| window.partition_keys.contains(column));
                if !keyed
                    && !source.primary_keys.contains(column)
                    && !payload_columns.contains(column)
                {
                    payload_columns.push(column.clone());
                    partitions.internal.push(column.clone());
                }
                partitions.carried.push(column.clone());
            }
            Ok(partitions)
        }
        ViewSpec::UnionAll { sources, .. } => {
            if sources.is_empty()
                || sources.iter().any(|source| source.columns.is_empty())
            {
                // An implicit `SELECT *` branch has the whole source schema in
                // its own order; adding a column to the other branches would
                // make the branch schemas diverge.
                return Ok(DerivedPartitions::default());
            }
            let mut candidates: Option<Vec<String>> = None;
            for source in sources.iter() {
                let table = find_table(tables, &source.table_id)?;
                let available = table
                    .range_partition_columns
                    .iter()
                    .filter(|column| table.schema.field_with_name(column).is_ok())
                    .cloned()
                    .collect::<Vec<_>>();
                candidates = Some(match candidates {
                    None => available,
                    Some(previous) => previous
                        .into_iter()
                        .filter(|column| available.contains(column))
                        .collect(),
                });
            }
            let mut partitions = DerivedPartitions::default();
            for column in candidates.unwrap_or_default() {
                let positions = sources
                    .iter()
                    .map(|source| {
                        source
                            .columns
                            .iter()
                            .position(|candidate| candidate == &column)
                    })
                    .collect::<Vec<_>>();
                if positions.iter().all(Option::is_some) {
                    // Every branch projects it; it must be unchanged and in
                    // the same position so the branch schemas still agree.
                    let first = positions[0];
                    if positions.iter().any(|position| *position != first) {
                        continue;
                    }
                    let unchanged = sources.iter().all(|source| {
                        source.exprs.is_empty()
                            || first.and_then(|index| source.exprs.get(index))
                                == Some(&column)
                    });
                    if unchanged {
                        partitions.carried.push(column);
                    }
                    continue;
                }
                if positions.iter().any(Option::is_some) {
                    // Only some branches expose it; inserting it in the same
                    // position everywhere would reorder their other columns.
                    continue;
                }
                for source in sources.iter_mut() {
                    source.columns.push(column.clone());
                    if !source.exprs.is_empty() {
                        source.exprs.push(column.clone());
                    }
                }
                partitions.internal.push(column.clone());
                partitions.carried.push(column);
            }
            Ok(partitions)
        }
        _ => Ok(DerivedPartitions::default()),
    }
}

/// The range partitions a derived step MV mirrors and the payload columns
/// materialized only for that purpose.
#[derive(Debug, Default)]
struct DerivedPartitions {
    /// Real source range partition columns the step MV carries.
    carried: Vec<String>,
    /// The carried columns that are not part of the business output and stay
    /// hidden from an implicit `SELECT *` over the step.
    internal: Vec<String>,
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
        ViewSpec::MultiLookupJoin {
            left_table_id,
            rights,
            join_keys,
            output_columns,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let mut right_schemas = Vec::with_capacity(rights.len());
            for right in rights {
                right_schemas.push(find_table(tables, &right.table_id)?.schema.clone());
            }
            multi_lookup_join_mv_schema_for(
                &left.schema,
                &right_schemas,
                &left.primary_keys,
                join_keys,
                output_columns,
            )?
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn multi_agg(call: &str, column: &str, having: Option<&str>) -> ViewSpec {
        let mut spec = serde_json::json!({
            "kind": "multi_agg",
            "view_id": "new",
            "source_table_id": "src",
            "mv_table_id": "new_mv",
            "group_keys": ["g"],
            "aggregates": [{
                "call": call,
                "column": column,
                "result": "int64",
            }],
        });
        if let Some(having) = having {
            spec["having"] = serde_json::Value::String(having.to_string());
        }
        serde_json::from_value(spec).unwrap()
    }

    fn owner_aggregates() -> Vec<MultiAggSpec> {
        vec![
            MultiAggSpec {
                call: "sum(v)".to_string(),
                column: "net_v".to_string(),
                result: "int64".to_string(),
            },
            MultiAggSpec {
                call: "count(*)".to_string(),
                column: "cnt_v".to_string(),
                result: "int64".to_string(),
            },
        ]
    }

    #[test]
    fn rewrites_having_columns_but_not_string_literals() {
        let mapping = HashMap::from([("sum_v".to_string(), "net_v".to_string())]);
        let rewritten = rewrite_filter_columns("sum_v <> 0 AND note = 'sum_v'", &mapping);
        assert!(rewritten.contains("\"net_v\" <> 0"), "{rewritten}");
        assert!(rewritten.contains("'sum_v'"), "{rewritten}");
    }

    #[test]
    fn merges_a_subset_multi_agg_into_a_row_projection() {
        let source = source_schema();
        let owner_schema = multi_agg_mv_schema_for(
            &source,
            &["g".to_string()],
            &[],
            &[
                ("net_v".to_string(), DataType::Int64),
                ("cnt_v".to_string(), DataType::Int64),
            ],
        )
        .unwrap();
        let new_schema = multi_agg_mv_schema_for(
            &source,
            &["g".to_string()],
            &[],
            &[("sum_v".to_string(), DataType::Int64)],
        )
        .unwrap();
        let spec = multi_agg("sum(v)", "sum_v", Some("\"sum_v\" <> 0"));
        let row = merge_multi_agg_row(
            &spec,
            "owner",
            &owner_schema,
            &owner_aggregates(),
            &new_schema,
        )
        .expect("the SUM aggregate is present in the owner");
        let ViewSpec::Row {
            source_table_id,
            output_columns,
            output_exprs,
            filter,
            ..
        } = row
        else {
            panic!("expected a row projection");
        };
        assert_eq!(source_table_id, "owner");
        assert_eq!(output_columns, ["g", "sum_v"]);
        assert_eq!(output_exprs, ["g", "net_v"]);
        assert!(filter.unwrap().contains("net_v"));
    }

    #[test]
    fn recognizes_an_identity_row_and_inlines_its_source() {
        let row: ViewSpec = serde_json::from_value(serde_json::json!({
            "kind": "row",
            "view_id": "row_view",
            "source_table_id": "upstream",
            "mv_table_id": "row_view",
            "output_columns": ["g", "v"],
            "output_exprs": ["g", "v"],
        }))
        .unwrap();
        assert_eq!(identity_row_source(&row), Some("upstream"));

        let mut consumer: ViewSpec = serde_json::from_value(serde_json::json!({
            "kind": "multi_agg",
            "view_id": "consumer",
            "source_table_id": "row_view",
            "mv_table_id": "consumer",
            "group_keys": ["g"],
            "aggregates": [{
                "call": "sum(v)",
                "column": "sum_v",
                "result": "int64",
            }],
        }))
        .unwrap();
        let rows = HashMap::from([("row_view".to_string(), row)]);
        assert_eq!(inline_identity_rows(&mut consumer, &rows), ["row_view"]);
        let ViewSpec::MultiAgg {
            source_table_id, ..
        } = consumer
        else {
            panic!("expected a multi aggregate");
        };
        assert_eq!(source_table_id, "upstream");
    }

    #[test]
    fn keeps_a_filtering_row_as_a_source() {
        let row: ViewSpec = serde_json::from_value(serde_json::json!({
            "kind": "row",
            "view_id": "row_view",
            "source_table_id": "upstream",
            "mv_table_id": "row_view",
            "output_columns": ["g", "v"],
            "filter": "v <> 0",
        }))
        .unwrap();
        assert!(identity_row_source(&row).is_none());
    }

    #[test]
    fn refuses_a_missing_aggregate() {
        let source = source_schema();
        let owner_schema = multi_agg_mv_schema_for(
            &source,
            &["g".to_string()],
            &[],
            &[("net_v".to_string(), DataType::Int64)],
        )
        .unwrap();
        let new_schema = multi_agg_mv_schema_for(
            &source,
            &["g".to_string()],
            &[],
            &[("max_v".to_string(), DataType::Int64)],
        )
        .unwrap();
        let spec = multi_agg("max(v)", "max_v", None);
        assert!(
            merge_multi_agg_row(
                &spec,
                "owner",
                &owner_schema,
                &owner_aggregates(),
                &new_schema,
            )
            .is_none()
        );
    }
}
