// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL shape analysis: derive a [`ViewSpec`] from the SELECT of an
//! `INSERT INTO <mv> SELECT ...` statement.
//!
//! The SQL frontend has no DDL and no table marker: callers hand the statement
//! to the dedicated executor, which analyzes the logical plan here and then
//! drives [`crate::IvmRuntime::refresh_spec`].  Only shapes that map exactly
//! onto an existing [`ViewSpec`] are accepted; everything else is rejected
//! with an explicit message, because silently appending the full result would
//! duplicate data on the next refresh.
//!
//! Covered shapes: projections/filters, the aggregate family (SUM/COUNT,
//! MIN/MAX, COUNT(DISTINCT)/SUM(DISTINCT)), windows (ranking and aggregates),
//! per-group top-k, inner joins, semi/anti joins (pass an **optimized** plan
//! so EXISTS/IN are decorrelated into left-semi/left-anti joins) and
//! `UNION ALL` of identical schemas.

use std::collections::HashMap;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, ScalarValue, TableReference};
use datafusion::logical_expr::expr::{AggregateFunction, NullTreatment};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    Aggregate, Distinct, Expr, Filter, Join, JoinType, LogicalPlan, Operator, Projection,
    SortExpr, Union, Window, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;

use arrow_schema::Schema;

use crate::error::Result;
use crate::runtime::{
    CompareOp, DistinctAggKind, IVM_AVG_COLUMN, IVM_COUNT_COLUMN, IVM_MEDIAN_COLUMN,
    IVM_NONNULL_COUNT_COLUMN, IVM_SUM_COLUMN, IVM_VALUE_COLUMN, MinMaxKind,
    SemiAntiCondition, UnionSourceSpec, VarianceKind, ViewSpec, WindowColumn,
    WindowFunction, WindowGroupSpec, string_agg_output_column, union_output_schema_for,
};
use crate::table::IvmTable;

/// The inputs the executor knows before analyzing a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeRequest {
    /// The id the view is registered under (the target MV table).
    pub view_id: String,
    /// The materialized view table id (must already exist).
    pub mv_table_id: String,
    /// The value-count state table to use for MIN/MAX and DISTINCT views.
    ///
    /// The executor creates it (and replaces it on a definition change), so it
    /// is only known after the shape is known.
    pub state_table_id: Option<String>,
}

impl AnalyzeRequest {
    fn state_table(&self) -> Result<String> {
        self.state_table_id.clone().ok_or_else(|| {
            rootcause::report!(
                "this view shape needs a value-count state table; the executor must create one and pass state_table_id"
            )
        })
    }
}

/// The analyzed definition of one `INSERT INTO ... SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedView {
    /// The derived view definition, ready for `refresh_spec`.
    pub spec: ViewSpec,
    /// Stable hash of the definition; a different hash means the view must be
    /// rebuilt (old state tables dropped).
    pub definition_hash: String,
}

/// Analyze the SELECT plan of an `INSERT INTO <mv> SELECT ...` statement.
///
/// `tables` resolves the source tables referenced by the plan; keys may be
/// qualified (`schema.table`, `catalog.schema.table`) or bare table names.
pub fn analyze_select(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<AnalyzedView> {
    if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
        return analyze_select(peel(plan), tables, request);
    }
    let spec = match plan {
        LogicalPlan::Projection(projection) => match peel(&projection.input) {
            LogicalPlan::Aggregate(aggregate) => {
                if !is_plain_projection(projection) {
                    return Err(unsupported(
                        "computed columns above an aggregate are not supported",
                    ));
                }
                analyze_aggregate(aggregate, Some(projection), &[], tables, request)?
            }
            LogicalPlan::Window(window) => {
                analyze_window(projection, window, tables, request)?
            }
            LogicalPlan::Join(join) => {
                analyze_join(join, Some(projection), tables, request)?
            }
            LogicalPlan::Union(union) => {
                analyze_union(union, Some(projection), tables, request)?
            }
            LogicalPlan::Filter(filter) => {
                // `HAVING` is one or more filters directly above the
                // aggregate; a filter on a window rank column is top-k.
                if let Some((aggregate, having)) = having_aggregate(&projection.input) {
                    if !is_plain_projection(projection) {
                        return Err(unsupported(
                            "computed columns above an aggregate are not supported",
                        ));
                    }
                    analyze_aggregate(
                        aggregate,
                        Some(projection),
                        &having,
                        tables,
                        request,
                    )?
                } else {
                    match try_analyze_top_k(Some(projection), filter, tables, request)? {
                        Some(spec) => spec,
                        None => analyze_row(plan, tables, request)?,
                    }
                }
            }
            LogicalPlan::TableScan(_) => analyze_row(plan, tables, request)?,
            other => {
                return Err(unsupported(format!(
                    "projection over {}",
                    plan_label(other)
                )));
            }
        },
        LogicalPlan::Filter(_) => {
            // The projection above an aggregate may be optimized away (e.g.
            // for MIN/MAX), leaving a bare `Filter -> Aggregate` plan.
            if let Some((aggregate, having)) = having_aggregate(plan) {
                analyze_aggregate(aggregate, None, &having, tables, request)?
            } else {
                analyze_row(plan, tables, request)?
            }
        }
        LogicalPlan::TableScan(_) => analyze_row(plan, tables, request)?,
        LogicalPlan::Aggregate(aggregate) => {
            analyze_aggregate(aggregate, None, &[], tables, request)?
        }
        LogicalPlan::Join(join) => analyze_join(join, None, tables, request)?,
        LogicalPlan::Union(union) => analyze_union(union, None, tables, request)?,
        LogicalPlan::Distinct(distinct) => {
            analyze_distinct_rows(distinct, tables, request)?
        }
        other => {
            return Err(unsupported(plan_label(other)));
        }
    };
    Ok(AnalyzedView {
        definition_hash: definition_hash(&spec)?,
        spec,
    })
}

/// The aggregate under one or more filters (a `HAVING` clause) together with
/// the predicates, when the plan has that shape.
fn having_aggregate(plan: &LogicalPlan) -> Option<(&Aggregate, Vec<Expr>)> {
    let mut node = peel(plan);
    let mut conjuncts = Vec::new();
    while let LogicalPlan::Filter(filter) = node {
        conjuncts.extend(split_conjunction(&filter.predicate).into_iter().cloned());
        node = peel(&filter.input);
    }
    match node {
        LogicalPlan::Aggregate(aggregate) if !conjuncts.is_empty() => {
            Some((aggregate, conjuncts))
        }
        _ => None,
    }
}

/// Plain `SELECT DISTINCT`: a grouping without aggregate functions, which the
/// count-only sum/count shape maintains (`count_v = 0` drops the group).
fn analyze_distinct_rows(
    distinct: &Distinct,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let Distinct::All(input) = distinct else {
        return Err(unsupported("DISTINCT ON"));
    };
    if let LogicalPlan::Union(union) = peel(input) {
        return analyze_union_distinct(union, tables, request);
    }
    let (source, filter) = collect_filtered_source(input, tables, "SELECT DISTINCT")?;
    let group_keys = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    if group_keys.is_empty() {
        return Err(unsupported("SELECT DISTINCT needs at least one column"));
    }
    Ok(ViewSpec::SumCount {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        group_keys,
        group_exprs: Vec::new(),
        value_column: None,
        value_expr: None,
        count_column: None,
        aggregate_filter: None,
        filter,
        having: None,
        average: false,
    })
}

/// A projection (and optional filters) of one source table.
fn analyze_row(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let (source, output_columns, output_exprs, filter) = collect_row(plan, tables)?;
    Ok(ViewSpec::Row {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        output_columns: output_columns.unwrap_or_default(),
        output_exprs,
        filter,
    })
}

type RowParts = (IvmTable, Option<Vec<String>>, Vec<String>, Option<String>);

fn collect_row(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
) -> Result<RowParts> {
    match peel(plan) {
        LogicalPlan::Projection(projection) => {
            let (source, _, _, filter) = collect_row(&projection.input, tables)?;
            let mut names = Vec::with_capacity(projection.expr.len());
            let mut exprs = Vec::with_capacity(projection.expr.len());
            for expr in &projection.expr {
                // Display aliases wrap the actual expression; the outermost
                // alias names the materialized column.
                let mut inner = expr;
                let mut alias = None;
                while let Expr::Alias(nested) = inner {
                    if alias.is_none() {
                        alias = Some(nested.name.clone());
                    }
                    inner = &nested.expr;
                }
                match inner {
                    Expr::Column(column) => {
                        let name = alias.unwrap_or_else(|| column.name.clone());
                        names.push(name);
                        exprs.push(column.name.clone());
                    }
                    other => {
                        let name = alias.ok_or_else(|| {
                            unsupported("computed projection columns need an alias")
                        })?;
                        names.push(name);
                        exprs.push(render_filter(other)?);
                    }
                }
            }
            // Plain projections stay compact: the expressions default to the
            // output columns themselves.
            if names == exprs {
                exprs.clear();
            }
            Ok((source, Some(names), exprs, filter))
        }
        LogicalPlan::Filter(filter) => {
            let (source, output_columns, output_exprs, previous) =
                collect_row(&filter.input, tables)?;
            let filter =
                Some(combine_filter(previous, render_filter(&filter.predicate)?));
            Ok((source, output_columns, output_exprs, filter))
        }
        LogicalPlan::TableScan(scan) => {
            let source = resolve_table(tables, &scan.table_name)?.clone();
            // The optimizer pushes the projection into the scan, so the
            // scanned columns define the row view's output columns.
            let output_columns = scan.projection.as_ref().map(|_| {
                scan.projected_schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>()
            });
            Ok((
                source,
                output_columns,
                Vec::new(),
                scan_filter(&scan.filters)?,
            ))
        }
        other => Err(unsupported(format!("row shape over {}", plan_label(other)))),
    }
}

/// The predicates DataFusion pushed into a table scan.
fn scan_filter(filters: &[Expr]) -> Result<Option<String>> {
    let mut filter: Option<String> = None;
    for predicate in filters {
        filter = Some(combine_filter(filter, render_filter(predicate)?));
    }
    Ok(filter)
}

/// The source and the filter of the plan below an aggregate, window or top-k
/// node.  The optimizer may push the projection (and, with a capable
/// provider, the filter) into the table scan, so plain projections and
/// filters are both collected.
fn collect_filtered_source(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    shape: &str,
) -> Result<(IvmTable, Option<String>)> {
    match peel(plan) {
        LogicalPlan::Projection(projection) => {
            // The optimizer hoists a variance argument or a shared aggregate
            // FILTER predicate into a projection (`CAST(value AS Float64) AS
            // __common_expr_1`, `value > 5 AS __common_expr_2`), so scalar
            // projections are allowed next to plain ones.
            if !is_plain_projection(projection) && !is_scalar_projection(projection) {
                return Err(unsupported(format!(
                    "computed columns below {shape} are not supported"
                )));
            }
            collect_filtered_source(&projection.input, tables, shape)
        }
        LogicalPlan::Filter(filter) => {
            let (source, previous) =
                collect_filtered_source(&filter.input, tables, shape)?;
            let filter =
                Some(combine_filter(previous, render_filter(&filter.predicate)?));
            Ok((source, filter))
        }
        LogicalPlan::TableScan(scan) => {
            let source = resolve_table(tables, &scan.table_name)?.clone();
            Ok((source, scan_filter(&scan.filters)?))
        }
        other => Err(unsupported(format!("{shape} over {}", plan_label(other)))),
    }
}

/// `(previous) AND (rendered)` when a filter was already collected.
fn combine_filter(previous: Option<String>, rendered: String) -> String {
    match previous {
        Some(previous) => format!("({previous}) AND ({rendered})"),
        None => rendered,
    }
}

/// The materialized columns a `HAVING` predicate may reference.
#[derive(Clone, Copy)]
enum HavingColumns<'a> {
    /// `sum_v`, `count_v`, the non-NULL count and, for AVG views, `avg_v`.
    SumCount {
        value: Option<&'a AggValue>,
        count_column: Option<&'a str>,
        average: bool,
        /// The aggregate `FILTER` the view materializes; `None` when its
        /// aggregates are unfiltered.
        aggregate_filter: Option<&'a str>,
    },
    /// `value` for a MIN/MAX view.
    MinMax {
        kind: MinMaxKind,
        value: &'a AggValue,
    },
    /// `value` for a COUNT(DISTINCT)/SUM(DISTINCT) view.
    Distinct {
        kind: DistinctAggKind,
        value_column: &'a str,
        /// The placeholder column of a split distinct aggregate (`alias1`).
        alias: Option<&'a str>,
    },
    /// `variance_v` / `stddev_v` for a variance view.
    Variance { statistic: VarianceKind },
    /// `median_v` for a median view.
    Median,
    /// `string_agg_<value>` for a `STRING_AGG` view.
    StringAgg {
        value: &'a AggValue,
        delimiter: &'a str,
        order_by: &'a [String],
    },
}

/// Rewrite the `HAVING` predicates over the materialized MV columns and render
/// them to canonical SQL, so the runtime can evaluate them on the aggregate
/// the refresh just computed.
///
/// The planner refers to an aggregate in `HAVING` by a hidden column named
/// after the aggregate expression (`sum(src.v)`), so the mapping is built from
/// the aggregate node's expressions.
fn render_having(
    exprs: &[Expr],
    columns: HavingColumns<'_>,
    aggregate: &Aggregate,
    group_keys: &[String],
) -> Result<Option<String>> {
    if exprs.is_empty() {
        return Ok(None);
    }
    // A group expression is referenced by the name of its output field.
    let mut group_aliases = HashMap::new();
    for (index, key) in group_keys.iter().enumerate() {
        if let Some(field) = aggregate.schema.fields().get(index)
            && field.name() != key
        {
            group_aliases.insert(field.name().clone(), key.clone());
        }
    }
    let hoisted = hoisted_expressions(&aggregate.input);
    let mut mapping: HashMap<String, String> = HashMap::new();
    let group_count = aggregate.group_expr.len();
    for (index, aggr) in aggregate.aggr_expr.iter().enumerate() {
        let mut inner = aggr;
        while let Expr::Alias(alias) = inner {
            inner = &alias.expr;
        }
        let Expr::AggregateFunction(function) = inner else {
            continue;
        };
        if let Ok(column) = having_column(function, columns, &hoisted) {
            // The HAVING filter refers to the aggregate by the name of its
            // output field.
            if let Some(field) = aggregate.schema.fields().get(group_count + index) {
                mapping.insert(field.name().clone(), column.clone());
            }
            // The HAVING filter refers to the aggregate by the name the
            // optimizer gave its output column: with the hoisted FILTER
            // aliases resolved and the argument casts dropped, so map both.
            let resolved =
                resolve_hoisted(&Expr::AggregateFunction(function.clone()), &hoisted);
            mapping.insert(format!("{resolved}"), column.clone());
            mapping.insert(cast_normalized(&resolved), column);
        }
    }
    let group_keys = group_keys
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut parts = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let rewritten = rewrite_having(
            expr,
            columns,
            &mapping,
            &group_keys,
            &group_aliases,
            &hoisted,
        )?;
        parts.push(render_filter(&rewritten)?);
    }
    Ok(Some(parts.join(" AND ")))
}

/// The display of an expression with argument casts removed, matching the
/// hidden column names the planner uses in `HAVING`.
fn cast_normalized(expr: &Expr) -> String {
    let normalized = expr
        .clone()
        .transform_down(|node| match node {
            Expr::Cast(cast) => Ok(Transformed::yes(*cast.expr)),
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
        .unwrap_or_else(|_| expr.clone());
    format!("{normalized}")
}

fn rewrite_having(
    expr: &Expr,
    columns: HavingColumns<'_>,
    mapping: &HashMap<String, String>,
    group_keys: &std::collections::HashSet<&str>,
    group_aliases: &HashMap<String, String>,
    hoisted: &HashMap<String, Expr>,
) -> Result<Expr> {
    expr.clone()
        .transform_down(|node| match node {
            Expr::Column(column) if !group_keys.contains(column.name.as_str()) => {
                if let Some(key) = group_aliases.get(&column.name) {
                    return Ok(Transformed::yes(Expr::Column(Column::from_name(
                        key.clone(),
                    ))));
                }
                match mapping.get(&column.name) {
                    Some(mv_column) => Ok(Transformed::yes(Expr::Column(
                        Column::from_name(mv_column.clone()),
                    ))),
                    None => Err(datafusion::error::DataFusionError::Plan(format!(
                        "HAVING over {}, which the view does not materialize",
                        column.name
                    ))),
                }
            }
            Expr::AggregateFunction(function) => {
                match having_column(&function, columns, hoisted) {
                    Ok(column) => {
                        Ok(Transformed::yes(Expr::Column(Column::from_name(column))))
                    }
                    Err(message) => {
                        Err(datafusion::error::DataFusionError::Plan(message))
                    }
                }
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
        .map_err(|error| unsupported(format!("HAVING {error}")))
}

/// The column an aggregate argument refers to, unwrapping the numeric cast
/// the optimizer adds (`avg(v)` becomes `avg(CAST(v AS Float64))`).
fn aggregate_column_of(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::Column(column) => Some(column.name.as_str()),
            _ => None,
        },
        _ => None,
    }
}

/// The MV column holding the aggregate a `HAVING` function refers to.
fn having_column(
    function: &AggregateFunction,
    columns: HavingColumns<'_>,
    hoisted: &HashMap<String, Expr>,
) -> std::result::Result<String, String> {
    // A `STRING_AGG` is defined by its in-aggregate ordering.
    let string_agg = matches!(columns, HavingColumns::StringAgg { .. });
    if !function.params.order_by.is_empty() && !string_agg {
        return Err(format!(
            "{} with ORDER BY is not maintained",
            function.func.name()
        ));
    }
    let name = function.func.name();
    let column_arg = |column: &str| matches!(function.params.args.as_slice(), [arg] if aggregate_column_of(arg) == Some(column));
    let counts_all = function.params.args.is_empty()
        || matches!(function.params.args.as_slice(), [Expr::Literal(..)]);
    let function_filter = match &function.params.filter {
        Some(filter) => {
            let filter = resolve_hoisted(filter, hoisted);
            Some(render_filter(&filter).map_err(|error| error.to_string())?)
        }
        None => None,
    };
    let value_matches = |value: &AggValue, strip_cast: bool| {
        matches!(function.params.args.as_slice(), [arg]
            if value_argument(arg, hoisted, strip_cast)
                .map(|parsed| parsed == *value)
                .unwrap_or(false))
    };
    match columns {
        HavingColumns::SumCount {
            value,
            count_column,
            average,
            aggregate_filter,
        } => {
            if function_filter.as_deref() != aggregate_filter {
                return Err(format!(
                    "{} with a different FILTER is not materialized",
                    name
                ));
            }
            match name {
                "avg" if !function.params.distinct => {
                    if average && value.is_some_and(|value| value_matches(value, true)) {
                        Ok(IVM_AVG_COLUMN.to_string())
                    } else {
                        Err(not_materialized(name))
                    }
                }
                "sum" if !function.params.distinct => match value {
                    Some(value) if value_matches(value, true) => {
                        Ok(IVM_SUM_COLUMN.to_string())
                    }
                    _ => Err(not_materialized(name)),
                },
                "count" if !function.params.distinct => {
                    if counts_all {
                        Ok(IVM_COUNT_COLUMN.to_string())
                    } else if count_column.is_some_and(&column_arg) {
                        Ok(IVM_NONNULL_COUNT_COLUMN.to_string())
                    } else if let Some(AggValue::Column(column)) = value
                        && column_arg(column)
                    {
                        Ok(IVM_NONNULL_COUNT_COLUMN.to_string())
                    } else {
                        Err(not_materialized(name))
                    }
                }
                _ => Err(not_materialized(name)),
            }
        }
        HavingColumns::MinMax { .. } if function_filter.is_some() => {
            Err(not_materialized(name))
        }
        HavingColumns::MinMax { kind, value } => {
            let expected = match kind {
                MinMaxKind::Min => "min",
                MinMaxKind::Max => "max",
            };
            if name == expected
                && !function.params.distinct
                && function.params.args.len() == 1
                && value_matches(value, false)
            {
                Ok(IVM_VALUE_COLUMN.to_string())
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::StringAgg {
            value,
            delimiter,
            order_by,
        } => {
            if function_filter.is_some() || function.params.distinct {
                return Err(not_materialized(name));
            }
            let same_value = matches!(function.params.args.as_slice(), [value_arg, _]
                if value_argument(value_arg, hoisted, false)
                    .map(|parsed| parsed == *value)
                    .unwrap_or(false));
            let same_delimiter = matches!(function.params.args.as_slice(), [_, delimiter_arg]
                if render_string_literal(delimiter_arg).as_deref() == Some(delimiter));
            let same_order = render_order_items(&function.params.order_by)
                .map(|items| items.as_slice() == order_by)
                .unwrap_or(false);
            if name == "string_agg" && same_value && same_delimiter && same_order {
                Ok(string_agg_output_column(match value {
                    AggValue::Column(column) => Some(column.as_str()),
                    AggValue::Expr(_) => None,
                }))
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::Median if function_filter.is_some() => Err(not_materialized(name)),
        HavingColumns::Median => {
            if name == "median"
                && !function.params.distinct
                && function.params.args.len() == 1
            {
                Ok(IVM_MEDIAN_COLUMN.to_string())
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::Variance { .. } if function_filter.is_some() => {
            Err(not_materialized(name))
        }
        HavingColumns::Variance { statistic } => {
            if name == statistic.sql_name()
                && !function.params.distinct
                && function.params.args.len() == 1
            {
                Ok(statistic.column_name().to_string())
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::Distinct {
            kind,
            value_column,
            alias,
        } => {
            let expected = match kind {
                DistinctAggKind::Count => "count",
                DistinctAggKind::Sum => "sum",
            };
            let refers_to_alias = alias.is_some_and(|alias| {
                matches!(
                    function.params.args.as_slice(),
                    [Expr::Column(column)] if column.name == alias
                )
            });
            let matches = if function.params.distinct {
                column_arg(value_column)
            } else {
                refers_to_alias
            };
            if name == expected && matches {
                Ok(IVM_VALUE_COLUMN.to_string())
            } else {
                Err(not_materialized(name))
            }
        }
    }
}

fn not_materialized(name: &str) -> String {
    format!("HAVING over {name}, which the view does not materialize")
}

/// An aggregate `FILTER (WHERE ...)` predicate is evaluated against the source
/// rows the view reads, so it may only refer to source columns.
fn validate_filter_columns(source: &IvmTable, filter: &Expr) -> Result<()> {
    let stripped = strip_relations(filter.clone())?;
    stripped
        .apply(|node| {
            if let Expr::Column(column) = node
                && source.schema.field_with_name(&column.name).is_err()
            {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "FILTER over {} does not refer to a source column",
                    column.name
                )));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| unsupported(format!("FILTER {error}")))?;
    Ok(())
}

/// Validate a filter and render it to canonical SQL over unqualified columns,
/// so it can be embedded in the runtime's queries under any table alias.
fn render_filter(expr: &Expr) -> Result<String> {
    validate_filter(expr)?;
    let stripped = strip_relations(expr.clone())?;
    let ast = Unparser::default()
        .expr_to_sql(&stripped)
        .map_err(|error| unsupported(format!("filter {error}")))?;
    Ok(ast.to_string())
}

fn strip_relations(expr: Expr) -> Result<Expr> {
    expr.transform_down(|node| match node {
        Expr::Column(column) if column.relation.is_some() => Ok(Transformed::yes(
            Expr::Column(Column::from_name(column.name.clone())),
        )),
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
    .map_err(|error| unsupported(format!("filter {error}")))
}

fn validate_filter(expr: &Expr) -> Result<()> {
    expr.apply(|node| {
        match node {
            Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_) => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "filters may not contain aggregates, windows or subqueries"
                        .to_string(),
                ));
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|error| unsupported(format!("filter {error}")))?;
    Ok(())
}

/// The two-level plan of a single `DISTINCT` aggregate: the optimizer groups
/// the source by `(group keys, value)` first and then aggregates the
/// placeholder column (`alias1`).
fn distinct_split(aggregate: &Aggregate) -> Result<Option<(&Aggregate, String, String)>> {
    let LogicalPlan::Aggregate(inner) = peel(&aggregate.input) else {
        return Ok(None);
    };
    if !inner.aggr_expr.is_empty()
        || inner.group_expr.len() != aggregate.group_expr.len() + 1
    {
        return Ok(None);
    }
    // The inner grouping repeats the outer keys plus one aliased value.
    let outer = aggregate
        .group_expr
        .iter()
        .map(|expr| format!("{expr}"))
        .collect::<Vec<_>>();
    let mut alias: Option<(String, String)> = None;
    let mut others = Vec::new();
    for expr in &inner.group_expr {
        match expr {
            // A hoisted group expression keeps its alias in the inner
            // grouping; the outer grouping references that alias.
            Expr::Alias(inner_alias) if outer.contains(&inner_alias.name) => {
                others.push(inner_alias.name.clone());
            }
            Expr::Alias(inner_alias) => {
                if alias.is_some() {
                    return Ok(None);
                }
                let Expr::Column(value) = inner_alias.expr.as_ref() else {
                    return Ok(None);
                };
                alias = Some((inner_alias.name.clone(), value.name.clone()));
            }
            other => others.push(format!("{other}")),
        }
    }
    let Some((alias, value_column)) = alias else {
        return Ok(None);
    };
    if others != outer {
        return Ok(None);
    }
    Ok(Some((inner, alias, value_column)))
}

/// The aggregate family: SUM/COUNT, MIN/MAX and COUNT(DISTINCT)/SUM(DISTINCT).
/// A projection of plain columns and the scalar aliases the optimizer hoists
/// above the scan: `CAST(column)` casts and shared aggregate `FILTER`
/// predicates.
fn is_scalar_projection(projection: &Projection) -> bool {
    projection.expr.iter().all(|expr| match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::Cast(cast) => matches!(cast.expr.as_ref(), Expr::Column(_)),
            hoisted => validate_filter(hoisted).is_ok(),
        },
        _ => false,
    })
}

/// The aliases the optimizer hoisted above the aggregate input, mapped to
/// their expressions.
fn hoisted_expressions(plan: &LogicalPlan) -> HashMap<String, Expr> {
    let mut exprs = HashMap::new();
    if let LogicalPlan::Projection(projection) = peel(plan) {
        for expr in &projection.expr {
            if let Expr::Alias(alias) = expr {
                exprs.insert(alias.name.clone(), alias.expr.as_ref().clone());
            }
        }
    }
    exprs
}

/// Replace the hoisted aliases a predicate refers to by their expressions.
fn resolve_hoisted(expr: &Expr, hoisted: &HashMap<String, Expr>) -> Expr {
    expr.clone()
        .transform_down(|node| {
            // The optimizer wraps the hoisted column in a display alias.
            let mut inner = &node;
            while let Expr::Alias(alias) = inner {
                inner = &alias.expr;
            }
            if let Expr::Column(column) = inner
                && let Some(hoisted_expr) = hoisted.get(&column.name)
            {
                return Ok(Transformed::yes(hoisted_expr.clone()));
            }
            Ok(Transformed::no(node))
        })
        .map(|transformed| transformed.data)
        .unwrap_or_else(|_| expr.clone())
}

/// The argument of a materialized `SUM`/`AVG`: a plain column or a rendered
/// scalar expression.
#[derive(Clone, PartialEq)]
enum AggValue {
    Column(String),
    Expr(String),
}

/// Parse a value-aggregate argument, resolving hoisted aliases.  The numeric
/// aggregates drop the coercion cast the optimizer adds (`avg(v)` becomes
/// `avg(CAST(v AS Float64))`); MIN/MAX keep user casts.
fn value_argument(
    arg: &Expr,
    hoisted: &HashMap<String, Expr>,
    strip_cast: bool,
) -> Result<AggValue> {
    let resolved = resolve_hoisted(arg, hoisted);
    let inner = if strip_cast {
        match &resolved {
            Expr::Cast(cast) => cast.expr.as_ref(),
            other => other,
        }
    } else {
        &resolved
    };
    match inner {
        Expr::Column(column) => Ok(AggValue::Column(column.name.clone())),
        other => Ok(AggValue::Expr(render_filter(other)?)),
    }
}

/// A `SUM`/`AVG` argument (numeric coercion casts are dropped).
fn sum_value_argument(arg: &Expr, hoisted: &HashMap<String, Expr>) -> Result<AggValue> {
    value_argument(arg, hoisted, true)
}

fn analyze_aggregate(
    aggregate: &Aggregate,
    projection: Option<&Projection>,
    having_exprs: &[Expr],
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    // `SELECT ... UNION SELECT ...` deduplicates the union output; the
    // optimizer plans it as a bare group-by over every union column.
    if aggregate.aggr_expr.is_empty()
        && let LogicalPlan::Union(union) = peel(&aggregate.input)
    {
        return analyze_union_distinct(union, tables, request);
    }
    let mut hoisted_exprs = hoisted_expressions(&aggregate.input);
    // The optimizer rewrites a single DISTINCT aggregate into an inner
    // grouping over `(group keys, value)` and an outer `count(alias)` /
    // `sum(alias)`.
    let distinct_split = distinct_split(aggregate)?;
    // The DISTINCT split hoists a computed group key into the inner grouping
    // aliased as `group_alias_N`; the outer grouping references that alias, so
    // resolve it back to its expression.
    if let Some((inner, _, _)) = &distinct_split {
        for expr in &inner.group_expr {
            if let Expr::Alias(alias) = expr {
                hoisted_exprs
                    .entry(alias.name.clone())
                    .or_insert((*alias.expr).clone());
            }
        }
    }
    let input = match &distinct_split {
        Some((inner, _, _)) => &inner.input,
        None => &aggregate.input,
    };
    let (source, filter) = collect_filtered_source(input, tables, "an aggregate")?;
    let source = &source;

    let mut group_keys = Vec::with_capacity(aggregate.group_expr.len());
    let mut group_exprs = Vec::with_capacity(aggregate.group_expr.len());
    for expr in &aggregate.group_expr {
        // The select alias names the materialized key column.
        let alias = projection.and_then(|projection| projection_alias(projection, expr));
        let mut inner = expr;
        while let Expr::Alias(nested) = inner {
            inner = &nested.expr;
        }
        if matches!(inner, Expr::GroupingSet(_)) {
            return Err(unsupported(
                "GROUPING SETS / ROLLUP / CUBE are not supported yet",
            ));
        }
        match inner {
            Expr::Column(column) if hoisted_exprs.contains_key(&column.name) => {
                let name = alias
                    .ok_or_else(|| unsupported("GROUP BY expressions need an alias"))?;
                let resolved = resolve_hoisted(inner, &hoisted_exprs);
                group_keys.push(name);
                group_exprs.push(render_filter(&resolved)?);
            }
            Expr::Column(column) => {
                group_keys.push(alias.unwrap_or_else(|| column.name.clone()));
                group_exprs.push(column.name.clone());
            }
            other => {
                let name = alias
                    .ok_or_else(|| unsupported("GROUP BY expressions need an alias"))?;
                let resolved = resolve_hoisted(other, &hoisted_exprs);
                group_keys.push(name);
                group_exprs.push(render_filter(&resolved)?);
            }
        }
    }
    // A plain column grouping stays compact.
    let group_exprs = if group_keys == group_exprs {
        Vec::new()
    } else {
        group_exprs
    };

    // `SELECT DISTINCT` plans as a group-by without aggregate functions; the
    // count-only sum/count shape drops groups as soon as their count reaches
    // zero, which is exactly the distinct rows.
    if aggregate.aggr_expr.is_empty() {
        if !having_exprs.is_empty() {
            return Err(unsupported("HAVING with SELECT DISTINCT"));
        }
        if group_keys.is_empty() {
            return Err(unsupported("SELECT DISTINCT needs at least one column"));
        }
        return Ok(ViewSpec::SumCount {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column: None,
            value_expr: None,
            count_column: None,
            aggregate_filter: None,
            filter,
            having: None,
            average: false,
        });
    }

    let mut count = false;
    let mut count_column: Option<String> = None;
    let mut sum: Option<AggValue> = None;
    let mut avg: Option<AggValue> = None;
    let mut variance: Option<(VarianceKind, AggValue)> = None;
    let mut median: Option<AggValue> = None;
    // `(value column, rendered delimiter, rendered aggregate ordering)`.
    let mut string_agg: Option<(AggValue, String, Vec<String>)> = None;
    // `(value column, rendered aggregate ordering)`.
    let mut array_agg: Option<(AggValue, Vec<String>)> = None;
    let mut min_max: Option<(MinMaxKind, AggValue)> = None;
    let mut distinct: Option<(DistinctAggKind, Vec<String>)> = None;
    // `(aggregate name, FILTER predicate, whether it is a value aggregate)`.
    let mut aggregate_filters: Vec<(&str, Option<String>, bool)> = Vec::new();
    for expr in &aggregate.aggr_expr {
        let mut inner = expr;
        while let Expr::Alias(alias) = inner {
            inner = &alias.expr;
        }
        let Expr::AggregateFunction(function) = inner else {
            return Err(unsupported("non-aggregate expression in the select list"));
        };
        let name = function.func.name();
        // `STRING_AGG`/`ARRAY_AGG` are defined by their in-aggregate ordering.
        if !function.params.order_by.is_empty()
            && !matches!(name, "string_agg" | "array_agg")
        {
            return Err(unsupported(format!("{name} with ORDER BY")));
        }
        let function_filter = match &function.params.filter {
            Some(filter) => {
                let filter = resolve_hoisted(filter, &hoisted_exprs);
                validate_filter_columns(source, &filter)?;
                Some(render_filter(&filter)?)
            }
            None => None,
        };
        // A split DISTINCT aggregate is the only aggregate and refers to its
        // placeholder column (e.g. `count(alias1)` over `v AS alias1`).
        if let Some((_, alias, value_column)) = &distinct_split {
            let refers_to_alias = matches!(
                function.params.args.as_slice(),
                [Expr::Column(column)] if &column.name == alias
            );
            if refers_to_alias && !function.params.distinct {
                if function_filter.is_some() {
                    return Err(unsupported(
                        "FILTER over a DISTINCT aggregate is not maintained",
                    ));
                }
                if !matches!(name, "count" | "sum") {
                    return Err(unsupported(format!(
                        "aggregate function {name}(DISTINCT ...)"
                    )));
                }
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                {
                    return Err(unsupported(
                        "mixing DISTINCT aggregates with other aggregates",
                    ));
                }
                let kind = if name == "count" {
                    DistinctAggKind::Count
                } else {
                    DistinctAggKind::Sum
                };
                distinct = Some((kind, vec![value_column.clone()]));
                continue;
            }
        }
        match (name, function.params.distinct) {
            ("count", true) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                {
                    return Err(unsupported(
                        "mixing DISTINCT aggregates with other aggregates",
                    ));
                }
                let value_columns = multi_column_args(&function.params.args)?;
                if value_columns.len() > 1 {
                    if function_filter.is_some() {
                        return Err(unsupported(
                            "FILTER over a multi-column COUNT(DISTINCT) is not maintained",
                        ));
                    }
                    if group_keys.is_empty() {
                        return Err(unsupported(
                            "a multi-column COUNT(DISTINCT) needs a GROUP BY",
                        ));
                    }
                }
                distinct = Some((DistinctAggKind::Count, value_columns));
                aggregate_filters.push((name, function_filter, false));
            }
            ("sum", true) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                {
                    return Err(unsupported(
                        "mixing DISTINCT aggregates with other aggregates",
                    ));
                }
                distinct = Some((
                    DistinctAggKind::Sum,
                    vec![single_column_arg(&function.params.args)?],
                ));
                aggregate_filters.push((name, function_filter, false));
            }
            ("count", false) => {
                let counts_all = function.params.args.is_empty()
                    || matches!(function.params.args.as_slice(), [Expr::Literal(..)]);
                if counts_all && function_filter.is_some() {
                    return Err(unsupported(
                        "COUNT(*) FILTER is not maintained; use COUNT(column) or SUM",
                    ));
                }
                if min_max.is_some()
                    || distinct.is_some()
                    || variance.is_some()
                    || median.is_some()
                {
                    return Err(unsupported("mixing COUNT with other aggregate kinds"));
                }
                if counts_all {
                    if count {
                        return Err(unsupported("duplicate COUNT aggregate"));
                    }
                    // SUM + COUNT(*) is the standard sum/count shape.
                    count = true;
                } else {
                    // COUNT(column) counts non-NULL values; the sum/count
                    // state keeps that as the non-NULL count of a column.
                    let column = single_column_arg(&function.params.args)?;
                    if count_column.as_ref().is_some_and(|count| count != &column) {
                        return Err(unsupported("duplicate COUNT aggregate"));
                    }
                    count_column = Some(column);
                }
                aggregate_filters.push(("count", function_filter, !counts_all));
            }
            ("sum", false) => {
                if min_max.is_some()
                    || distinct.is_some()
                    || variance.is_some()
                    || median.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                if sum.is_some() {
                    return Err(unsupported("duplicate SUM aggregate"));
                }
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported(
                        "aggregates take exactly one value argument",
                    ));
                };
                let value = sum_value_argument(arg, &hoisted_exprs)?;
                if avg.as_ref().is_some_and(|avg| avg != &value) {
                    return Err(unsupported("AVG and SUM must use the same value"));
                }
                sum = Some(value);
                aggregate_filters.push(("sum", function_filter, true));
            }
            ("avg", false) => {
                if min_max.is_some()
                    || distinct.is_some()
                    || variance.is_some()
                    || median.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                if avg.is_some() {
                    return Err(unsupported("duplicate AVG aggregate"));
                }
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported(
                        "aggregates take exactly one value argument",
                    ));
                };
                let value = sum_value_argument(arg, &hoisted_exprs)?;
                if sum.as_ref().is_some_and(|sum| sum != &value) {
                    return Err(unsupported("AVG and SUM must use the same value"));
                }
                avg = Some(value);
                aggregate_filters.push(("avg", function_filter, true));
            }
            ("min", false) | ("max", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                let kind = if name == "min" {
                    MinMaxKind::Min
                } else {
                    MinMaxKind::Max
                };
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported(
                        "aggregates take exactly one value argument",
                    ));
                };
                let value = value_argument(arg, &hoisted_exprs, false)?;
                min_max = Some((kind, value));
                aggregate_filters.push((name, function_filter, false));
            }
            ("var", false)
            | ("var_pop", false)
            | ("stddev", false)
            | ("stddev_pop", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                let statistic = match name {
                    "var_pop" => VarianceKind::VarPop,
                    "stddev" => VarianceKind::StddevSamp,
                    "stddev_pop" => VarianceKind::StddevPop,
                    _ => VarianceKind::VarSamp,
                };
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported(
                        "variance aggregates take exactly one argument",
                    ));
                };
                let value = value_argument(arg, &hoisted_exprs, true)?;
                variance = Some((statistic, value));
                aggregate_filters.push((name, function_filter, false));
            }
            ("median", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported("median takes exactly one argument"));
                };
                let value = value_argument(arg, &hoisted_exprs, true)?;
                median = Some(value);
                aggregate_filters.push((name, function_filter, false));
            }
            ("string_agg", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                    || string_agg.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                if function.params.order_by.is_empty() {
                    return Err(unsupported(
                        "STRING_AGG needs an ORDER BY inside the aggregate to be deterministic",
                    ));
                }
                let [value, delimiter_arg] = function.params.args.as_slice() else {
                    return Err(unsupported(
                        "STRING_AGG takes a value column and a delimiter",
                    ));
                };
                let string_value = value_argument(value, &hoisted_exprs, false)?;
                let delimiter =
                    render_string_literal(delimiter_arg).ok_or_else(|| {
                        unsupported("STRING_AGG delimiter must be a string literal")
                    })?;
                let order_by = render_order_items(&function.params.order_by)?;
                string_agg = Some((string_value, delimiter, order_by));
                aggregate_filters.push((name, function_filter, false));
            }
            ("array_agg", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                    || string_agg.is_some()
                    || array_agg.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                if function.params.order_by.is_empty() {
                    return Err(unsupported(
                        "ARRAY_AGG needs an ORDER BY inside the aggregate to be deterministic",
                    ));
                }
                let [value] = function.params.args.as_slice() else {
                    return Err(unsupported("ARRAY_AGG takes a value column"));
                };
                let array_value = value_argument(value, &hoisted_exprs, false)?;
                let order_by = render_order_items(&function.params.order_by)?;
                array_agg = Some((array_value, order_by));
                aggregate_filters.push((name, function_filter, false));
            }
            _ => {
                return Err(unsupported(format!(
                    "aggregate function {name}{}",
                    if function.params.distinct {
                        "(DISTINCT)"
                    } else {
                        ""
                    }
                )));
            }
        }
    }

    // Aggregate FILTER predicates: only the value aggregates (SUM, AVG and
    // COUNT(column)) may be filtered, they must share one predicate, and the
    // row count stays unfiltered so a group without matching rows keeps
    // existing with a NULL value.
    let mut aggregate_filter: Option<String> = None;
    {
        let value_filters = aggregate_filters
            .iter()
            .filter(|(_, _, is_value)| *is_value)
            .map(|(_, filter, _)| filter.as_deref())
            .collect::<Vec<Option<&str>>>();
        let present = value_filters
            .iter()
            .filter_map(|filter| *filter)
            .collect::<Vec<&str>>();
        if let Some(first) = present.first() {
            if present.iter().any(|filter| filter != first) {
                return Err(unsupported(
                    "aggregates with different FILTER predicates are not maintained",
                ));
            }
            if present.len() != value_filters.len() {
                return Err(unsupported(
                    "every SUM/AVG/COUNT(column) must carry the same FILTER",
                ));
            }
            aggregate_filter = Some(first.to_string());
        }
        for (name, filter, is_value) in &aggregate_filters {
            if !is_value && filter.is_some() {
                return Err(unsupported(format!("FILTER over {name} is not maintained")));
            }
        }
    }

    if (string_agg.is_some() || array_agg.is_some())
        && (count
            || sum.is_some()
            || avg.is_some()
            || variance.is_some()
            || median.is_some()
            || min_max.is_some()
            || distinct.is_some())
    {
        return Err(unsupported("mixing aggregate kinds"));
    }

    // A single non-NULL count accumulator is shared with SUM/AVG.
    if let Some(count_column) = &count_column {
        match sum.as_ref().or(avg.as_ref()) {
            Some(AggValue::Expr(_)) => {
                return Err(unsupported(
                    "COUNT(column) cannot be combined with an aggregated expression",
                ));
            }
            Some(AggValue::Column(column)) if column != count_column => {
                return Err(unsupported(
                    "COUNT and SUM/AVG must use the same value column",
                ));
            }
            _ => {}
        }
    }

    let having = if let Some((agg, value_columns)) = &distinct {
        if value_columns.len() > 1 {
            if !having_exprs.is_empty() {
                return Err(unsupported(
                    "HAVING with a multi-column COUNT(DISTINCT) is not maintained",
                ));
            }
            None
        } else {
            render_having(
                having_exprs,
                HavingColumns::Distinct {
                    kind: *agg,
                    value_column: &value_columns[0],
                    alias: distinct_split.as_ref().map(|(_, alias, _)| alias.as_str()),
                },
                aggregate,
                &group_keys,
            )?
        }
    } else if let Some((min_max, value)) = &min_max {
        render_having(
            having_exprs,
            HavingColumns::MinMax {
                kind: *min_max,
                value,
            },
            aggregate,
            &group_keys,
        )?
    } else if let Some((statistic, _)) = &variance {
        render_having(
            having_exprs,
            HavingColumns::Variance {
                statistic: *statistic,
            },
            aggregate,
            &group_keys,
        )?
    } else if let Some((_, order_by)) = &array_agg {
        if !having_exprs.is_empty() {
            return Err(unsupported("HAVING with ARRAY_AGG"));
        }
        let _ = order_by;
        None
    } else if let Some((value, delimiter, order_by)) = &string_agg {
        render_having(
            having_exprs,
            HavingColumns::StringAgg {
                value,
                delimiter,
                order_by,
            },
            aggregate,
            &group_keys,
        )?
    } else if median.is_some() {
        render_having(having_exprs, HavingColumns::Median, aggregate, &group_keys)?
    } else if sum.is_some() || count || count_column.is_some() || avg.is_some() {
        render_having(
            having_exprs,
            HavingColumns::SumCount {
                value: sum.as_ref().or(avg.as_ref()),
                count_column: count_column.as_deref(),
                average: avg.is_some(),
                aggregate_filter: aggregate_filter.as_deref(),
            },
            aggregate,
            &group_keys,
        )?
    } else {
        return Err(unsupported(
            "GROUP BY without a supported aggregate (use SUM/COUNT/MIN/MAX or DISTINCT)",
        ));
    };
    let average = avg.is_some();
    let value = sum.or(avg);
    let spec = if let Some((statistic, value)) = variance {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::Variance {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            statistic,
            filter,
            having,
        }
    } else if let Some((value, order_by)) = array_agg {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::ArrayAgg {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            order_by,
            filter,
        }
    } else if let Some((value, delimiter, order_by)) = string_agg {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::StringAgg {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            delimiter,
            order_by,
            filter,
            having,
        }
    } else if let Some(value) = median {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::Median {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            filter,
            having,
        }
    } else if let Some((agg, distinct_columns)) = distinct {
        let mut distinct_columns = distinct_columns;
        let value_column = distinct_columns.remove(0);
        // A single-column distinct is maintained through the value-count
        // state table; a multi-column one recomputes the affected groups.
        let value_columns = if distinct_columns.is_empty() {
            Vec::new()
        } else {
            let mut all = vec![value_column.clone()];
            all.extend(distinct_columns);
            all
        };
        ViewSpec::DistinctAgg {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            state_table_id: request.state_table()?,
            group_keys,
            group_exprs,
            value_column,
            value_columns,
            agg,
            filter,
            having,
        }
    } else if let Some((min_max, value)) = min_max {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::MinMax {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            state_table_id: request.state_table()?,
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            min_max,
            filter,
            having,
        }
    } else if value.is_some() || count || count_column.is_some() {
        let (value_column, value_expr) = match &value {
            Some(AggValue::Column(column)) => (Some(column.clone()), None),
            Some(AggValue::Expr(expression)) => (None, Some(expression.clone())),
            None => (None, None),
        };
        ViewSpec::SumCount {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            count_column,
            aggregate_filter,
            filter,
            having,
            average,
        }
    } else {
        return Err(unsupported(
            "GROUP BY without a supported aggregate (use SUM/COUNT/MIN/MAX or DISTINCT)",
        ));
    };
    Ok(spec)
}

/// A window view: one window function over one source.
fn analyze_window(
    projection: &Projection,
    window: &Window,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    // A statement with several window clauses plans as chained `WindowAggr`
    // nodes (with an intermediate projection when the upper clause needs the
    // lower columns); collect the chain down to the source.
    let mut chain = vec![window];
    let mut node = peel(&window.input);
    loop {
        let inner = match node {
            LogicalPlan::Projection(projection) => peel(&projection.input),
            other => other,
        };
        let LogicalPlan::Window(inner) = inner else {
            break;
        };
        chain.push(inner);
        node = peel(&inner.input);
    }
    let (source, filter) = collect_filtered_source(node, tables, "a window")?;
    // Selecting a window expression itself (aliased or not) is fine;
    // computing on top of it would be silently dropped otherwise.
    let window_exprs = chain
        .iter()
        .flat_map(|window| window.window_expr.iter())
        .collect::<Vec<_>>();
    for expr in &projection.expr {
        let mut inner = expr;
        while let Expr::Alias(alias) = inner {
            inner = &alias.expr;
        }
        if column_name(inner).is_none() && !window_exprs.contains(&inner) {
            return Err(unsupported(
                "computed columns above a window function are not supported",
            ));
        }
    }
    // Every clause is parsed against the top projection, which names the
    // materialized columns.
    let parts = chain
        .iter()
        .map(|window| window_function_spec(window, Some(projection)))
        .collect::<Result<Vec<_>>>()?;
    if parts.len() == 1 {
        let WindowParts {
            partition_keys,
            order_keys,
            order_by,
            columns,
        } = parts.into_iter().next().expect("length checked");
        return Ok(ViewSpec::Window {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            partition_keys,
            order_keys,
            order_by,
            columns,
            filter,
        });
    }
    Ok(ViewSpec::MultiWindow {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        windows: parts
            .into_iter()
            .map(|parts| WindowGroupSpec {
                partition_keys: parts.partition_keys,
                order_keys: parts.order_keys,
                order_by: parts.order_by,
                columns: parts.columns,
            })
            .collect(),
        filter,
    })
}

/// The top `k` rows per group: a filter on the `row_number()` column of a
/// window expression.
fn try_analyze_top_k(
    projection: Option<&Projection>,
    filter: &Filter,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<Option<ViewSpec>> {
    let mut node = peel(&filter.input);
    loop {
        match peel(node) {
            LogicalPlan::Projection(inner) => node = &inner.input,
            LogicalPlan::Window(_) => break,
            _ => return Ok(None),
        }
    }
    let LogicalPlan::Window(window) = peel(node) else {
        return Ok(None);
    };
    let WindowParts {
        partition_keys,
        order_keys,
        order_by,
        columns,
    } = window_function_spec(window, None)?;
    if columns.len() != 1
        || columns[0].function != WindowFunction::RowNumber
        || columns[0].value_column.is_some()
    {
        return Ok(None);
    }
    // The filter references the window column through the subquery scope: it
    // is the single column the filter's input adds on top of the source.
    let source_fields = window
        .input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<std::collections::HashSet<_>>();
    let rank_columns = filter
        .input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !source_fields.contains(name))
        .collect::<Vec<_>>();
    if rank_columns.len() != 1 {
        return Ok(None);
    }
    let rank_column = &rank_columns[0];

    let conjuncts = split_conjunction(&filter.predicate);
    if conjuncts.len() != 1 {
        return Ok(None);
    }
    let limit = match conjuncts[0] {
        Expr::BinaryExpr(binary) => match (&*binary.left, binary.op, &*binary.right) {
            (Expr::Column(column), Operator::LtEq, right)
                if &column.name == rank_column =>
            {
                int_literal(right)
            }
            (left, Operator::GtEq, Expr::Column(column))
                if &column.name == rank_column =>
            {
                int_literal(left)
            }
            _ => None,
        },
        _ => None,
    };
    let Some(limit) = limit.filter(|limit| *limit > 0) else {
        return Ok(None);
    };
    if partition_keys.is_empty() || order_keys.is_empty() {
        return Err(unsupported("top-k needs PARTITION BY and ORDER BY columns"));
    }
    let output_columns = match projection {
        Some(projection) => {
            let columns = projection
                .expr
                .iter()
                .map(column_name)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| unsupported("computed columns above a top-k filter"))?;
            if columns.iter().any(|column| column == rank_column) {
                return Err(unsupported(
                    "top-k output may not include the row-number column",
                ));
            }
            columns
        }
        None => Vec::new(),
    };
    let (source, filter) = collect_filtered_source(&window.input, tables, "top-k")?;
    Ok(Some(ViewSpec::TopK {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        group_keys: partition_keys,
        order_keys,
        order_by,
        output_columns,
        limit,
        filter,
    }))
}

/// The window columns of a window node plus their shared partition and
/// ordering.
struct WindowParts {
    partition_keys: Vec<String>,
    order_keys: Vec<String>,
    /// The rendered ordering; empty when it is the plain ascending default.
    order_by: Vec<String>,
    columns: Vec<WindowColumn>,
}

/// The window columns of a window node, sharing one `PARTITION BY`/`ORDER BY`
/// clause.
fn window_function_spec(
    window: &Window,
    projection: Option<&Projection>,
) -> Result<WindowParts> {
    if window.window_expr.is_empty() {
        return Err(unsupported("window node without window functions"));
    }
    let mut partition_keys: Option<Vec<String>> = None;
    let mut order_keys: Option<Vec<String>> = None;
    let mut order_by: Option<Vec<String>> = None;
    let mut columns = Vec::with_capacity(window.window_expr.len());
    for expr in &window.window_expr {
        let Expr::WindowFunction(function) = expr else {
            return Err(unsupported("non-window expression in a window node"));
        };
        let params = &function.params;
        let expr_partition = params
            .partition_by
            .iter()
            .map(column_name)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| unsupported("PARTITION BY expressions must be columns"))?;
        let mut expr_order = Vec::with_capacity(params.order_by.len());
        let mut expr_order_by = Vec::with_capacity(params.order_by.len());
        for sort in &params.order_by {
            match column_name(&sort.expr) {
                Some(column) => {
                    expr_order_by.push(render_order_key(
                        &column,
                        sort.asc,
                        sort.nulls_first,
                    ));
                    expr_order.push(column);
                }
                None => {
                    // A rendered expression: the keys hold the plain
                    // expression (for validation), the ordering adds the
                    // direction and wraps the expression in parentheses.
                    expr_order_by.push(render_order_expr(
                        &sort.expr,
                        sort.asc,
                        sort.nulls_first,
                    )?);
                    expr_order.push(render_filter(&sort.expr)?);
                }
            }
        }
        match (&partition_keys, &order_keys, &order_by) {
            (Some(partition), Some(order), Some(rendered)) => {
                if partition != &expr_partition
                    || order != &expr_order
                    || rendered != &expr_order_by
                {
                    return Err(unsupported(
                        "multiple window functions with different PARTITION BY/ORDER BY",
                    ));
                }
            }
            _ => {
                partition_keys = Some(expr_partition.clone());
                order_keys = Some(expr_order.clone());
                order_by = Some(expr_order_by.clone());
            }
        }
        let mut column = window_column(function, &expr_order)?;
        column.column = projection
            .and_then(|projection| projection_alias(projection, expr))
            .unwrap_or_else(|| column.function.column_name().to_string());
        columns.push(column);
    }
    let mut names = std::collections::HashSet::new();
    for column in &columns {
        if !names.insert(column.column.clone()) {
            return Err(unsupported(format!(
                "duplicate window column {}; alias the repeated functions",
                column.column
            )));
        }
    }
    let order_by = order_by.unwrap_or_default();
    let order_keys = order_keys.unwrap_or_default();
    // The plain ascending ordering is the default; keep the spec compact.
    let order_by = if order_by == order_keys {
        Vec::new()
    } else {
        order_by
    };
    Ok(WindowParts {
        partition_keys: partition_keys.unwrap_or_default(),
        order_keys,
        order_by,
        columns,
    })
}

/// Render one `ORDER BY` item: the ascending `NULLS LAST` default stays the
/// bare column, anything else is spelled out.
fn render_order_key(column: &str, asc: bool, nulls_first: bool) -> String {
    match (asc, nulls_first) {
        (true, false) => column.to_string(),
        (true, true) => format!("{} asc nulls first", quote_order_identifier(column)),
        (false, true) => format!("{} desc nulls first", quote_order_identifier(column)),
        (false, false) => format!("{} desc nulls last", quote_order_identifier(column)),
    }
}

/// Quote an identifier for a rendered ordering item.
fn quote_order_identifier(column: &str) -> String {
    format!("\"{}\"", column.replace('"', "\"\""))
}

/// Render the `ORDER BY` items of a window or aggregate expression.
fn render_order_items(order_by: &[SortExpr]) -> Result<Vec<String>> {
    order_by
        .iter()
        .map(|sort| match column_name(&sort.expr) {
            Some(column) => Ok(render_order_key(&column, sort.asc, sort.nulls_first)),
            None => render_order_expr(&sort.expr, sort.asc, sort.nulls_first),
        })
        .collect()
}

/// Render one `ORDER BY` item whose expression is not a plain column.
fn render_order_expr(expr: &Expr, asc: bool, nulls_first: bool) -> Result<String> {
    let rendered = render_filter(expr)?;
    Ok(match (asc, nulls_first) {
        (true, false) => format!("({rendered})"),
        (true, true) => format!("({rendered}) asc nulls first"),
        (false, false) => format!("({rendered}) desc nulls last"),
        (false, true) => format!("({rendered}) desc nulls first"),
    })
}

/// Render a string literal argument (`','`).
fn render_string_literal(expr: &Expr) -> Option<String> {
    let value = match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _) => value.as_str(),
        Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) => value.as_str(),
        Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => value.as_str(),
        _ => return None,
    };
    Some(format!("'{}'", value.replace('\'', "''")))
}

/// One window column parsed from its function expression.
fn window_column(
    function: &datafusion::logical_expr::expr::WindowFunction,
    order_keys: &[String],
) -> Result<WindowColumn> {
    let params = &function.params;
    let (function, value_column, window_args) = match &function.fun {
        WindowFunctionDefinition::WindowUDF(udf) => match udf.name() {
            "row_number" => (WindowFunction::RowNumber, None, None),
            "rank" => (WindowFunction::Rank, None, None),
            "dense_rank" => (WindowFunction::DenseRank, None, None),
            "lag" | "lead" => {
                let function = if udf.name() == "lag" {
                    WindowFunction::Lag
                } else {
                    WindowFunction::Lead
                };
                let (value, args) = window_shift_args(&params.args)?;
                (function, Some(value), args)
            }
            "first_value" | "last_value" => {
                let function = if udf.name() == "first_value" {
                    WindowFunction::FirstValue
                } else {
                    WindowFunction::LastValue
                };
                (function, Some(window_value_arg(&params.args)?), None)
            }
            "nth_value" => {
                let (value, args) = window_nth_value_args(&params.args)?;
                (WindowFunction::NthValue, Some(value), args)
            }
            "ntile" => (
                WindowFunction::Ntile,
                None,
                Some(positive_integer_arg(
                    &params.args,
                    "the NTILE bucket count",
                )?),
            ),
            "percent_rank" => (WindowFunction::PercentRank, None, None),
            "cume_dist" => (WindowFunction::CumeDist, None, None),
            other => {
                return Err(unsupported(format!("window function {other}")));
            }
        },
        WindowFunctionDefinition::AggregateUDF(udf) => match udf.name() {
            "sum" => (
                WindowFunction::Sum,
                Some(single_column_arg(&params.args)?),
                None,
            ),
            "count" => {
                let counts_all = params.args.is_empty()
                    || matches!(params.args.as_slice(), [Expr::Literal(..)]);
                (
                    WindowFunction::Count,
                    if counts_all {
                        None
                    } else {
                        Some(single_column_arg(&params.args)?)
                    },
                    None,
                )
            }
            other => {
                return Err(unsupported(format!("window aggregate {other}")));
            }
        },
    };
    let window_frame = window_frame_sql(&params.window_frame, !order_keys.is_empty())?;
    if !function.is_aggregate() && order_keys.is_empty() {
        return Err(unsupported("ranking window functions need ORDER BY"));
    }
    let window_filter = match &params.filter {
        Some(filter) => {
            if !function.is_aggregate() {
                return Err(unsupported("FILTER on a non-aggregate window function"));
            }
            Some(render_filter(filter)?)
        }
        None => None,
    };
    // `IGNORE NULLS` only changes the value functions; for ranking and
    // aggregate windows it is a no-op, so it is normalized away.
    let ignore_nulls = function.is_value()
        && matches!(params.null_treatment, Some(NullTreatment::IgnoreNulls));
    Ok(WindowColumn {
        function,
        value_column,
        window_args,
        window_filter,
        ignore_nulls,
        window_frame,
        column: String::new(),
    })
}

/// The alias the projection gives a window expression, when it has one (the
/// outermost alias wins).
fn projection_alias(projection: &Projection, target: &Expr) -> Option<String> {
    let target_display = format!("{target}");
    for expr in &projection.expr {
        let mut name = None;
        let mut inner = expr;
        while let Expr::Alias(alias) = inner {
            if name.is_none() {
                name = Some(alias.name.clone());
            }
            inner = &alias.expr;
        }
        // The optimized plan references the window node's output by a column
        // named after the window expression.
        let matches = inner == target
            || matches!(inner, Expr::Column(column) if column.name == target_display);
        if matches {
            // Ignore the alias DataFusion generates from the expression text
            // when the user did not name the column.
            return match name {
                Some(name) if name != format!("{inner}") => Some(name),
                _ => None,
            };
        }
    }
    None
}

/// The value column of a value window function (`FIRST_VALUE`/`LAST_VALUE`).
fn window_value_arg(args: &[Expr]) -> Result<String> {
    match args {
        [arg] => column_name(arg)
            .ok_or_else(|| unsupported("window values must be plain columns")),
        _ => Err(unsupported(
            "the window function takes exactly one value column",
        )),
    }
}

/// The value column and the row number of an `NTH_VALUE` call.
fn window_nth_value_args(args: &[Expr]) -> Result<(String, Option<String>)> {
    let [value, n] = args else {
        return Err(unsupported(
            "NTH_VALUE takes a value column and a row number",
        ));
    };
    let value = column_name(value)
        .ok_or_else(|| unsupported("NTH_VALUE values must be plain columns"))?;
    let n = integer_literal(n).ok_or_else(|| {
        unsupported("the NTH_VALUE row number must be an integer literal")
    })?;
    if n < 1 {
        return Err(unsupported("the NTH_VALUE row number must be positive"));
    }
    Ok((value, Some(n.to_string())))
}

/// A single positive integer literal argument, rendered to SQL text.
fn positive_integer_arg(args: &[Expr], what: &str) -> Result<String> {
    let [arg] = args else {
        return Err(unsupported(format!("{what} must be a single literal")));
    };
    let value = integer_literal(arg)
        .ok_or_else(|| unsupported(format!("{what} must be an integer literal")))?;
    if value < 1 {
        return Err(unsupported(format!("{what} must be positive")));
    }
    Ok(value.to_string())
}

/// An integer literal of any width.
fn integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => i64::try_from(*value).ok(),
        _ => None,
    }
}

/// The SQL text of the declared window frame, or `None` when it is the
/// default frame of the declared ordering (the runtime then omits it).
fn window_frame_sql(frame: &WindowFrame, has_order: bool) -> Result<Option<String>> {
    let is_default = if has_order {
        frame.units == WindowFrameUnits::Range
            && frame.start_bound.is_unbounded()
            && matches!(frame.end_bound, WindowFrameBound::CurrentRow)
    } else {
        frame.units == WindowFrameUnits::Rows
            && frame.start_bound.is_unbounded()
            && frame.end_bound.is_unbounded()
    };
    if is_default {
        return Ok(None);
    }
    let units = match frame.units {
        WindowFrameUnits::Rows => "rows",
        WindowFrameUnits::Range => "range",
        WindowFrameUnits::Groups => "groups",
    };
    Ok(Some(format!(
        "{units} between {} and {}",
        window_bound_sql(&frame.start_bound)?,
        window_bound_sql(&frame.end_bound)?,
    )))
}

fn window_bound_sql(bound: &WindowFrameBound) -> Result<String> {
    Ok(match bound {
        WindowFrameBound::Preceding(value) if value.is_null() => {
            "unbounded preceding".to_string()
        }
        WindowFrameBound::Preceding(value) => {
            format!("{} preceding", window_literal_sql(value)?)
        }
        WindowFrameBound::CurrentRow => "current row".to_string(),
        WindowFrameBound::Following(value) if value.is_null() => {
            "unbounded following".to_string()
        }
        WindowFrameBound::Following(value) => {
            format!("{} following", window_literal_sql(value)?)
        }
    })
}

fn window_literal_sql(value: &ScalarValue) -> Result<String> {
    let ast = Unparser::default()
        .expr_to_sql(&Expr::Literal(value.clone(), None))
        .map_err(|error| unsupported(format!("window literal {error}")))?;
    Ok(ast.to_string())
}

/// The value column and the optional extra arguments (`offset`, `default`)
/// of a `LAG`/`LEAD` call; the extra arguments are rendered to SQL text.
fn window_shift_args(args: &[Expr]) -> Result<(String, Option<String>)> {
    let Some((value, extras)) = args.split_first() else {
        return Err(unsupported("LAG/LEAD need a value column"));
    };
    let value = column_name(value)
        .ok_or_else(|| unsupported("the LAG/LEAD value must be a plain column"))?;
    if extras.is_empty() {
        return Ok((value, None));
    }
    if extras.len() > 2 {
        return Err(unsupported("LAG/LEAD take at most an offset and a default"));
    }
    let offset = integer_literal(&extras[0])
        .ok_or_else(|| unsupported("the LAG/LEAD offset must be an integer literal"))?;
    if offset < 0 {
        return Err(unsupported("the LAG/LEAD offset must not be negative"));
    }
    if extras.iter().any(|arg| !matches!(arg, Expr::Literal(..))) {
        return Err(unsupported("the LAG/LEAD default must be a literal"));
    }
    let mut rendered = Vec::with_capacity(extras.len());
    for arg in extras {
        let ast = Unparser::default()
            .expr_to_sql(arg)
            .map_err(|error| unsupported(format!("window argument {error}")))?;
        rendered.push(ast.to_string());
    }
    Ok((value, Some(rendered.join(", "))))
}

/// The side of a join a column belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

/// An inner join or a semi/anti join.
///
/// The plan is expected to be optimized (EXISTS/IN decorrelated into
/// left-semi/left-anti joins by DataFusion).
fn analyze_join(
    join: &Join,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    // `INTERSECT`/`EXCEPT` plan as null-aware joins (a NULL row matches a
    // NULL row), and `IS NOT DISTINCT FROM` predicates are null-aware too.
    // The maintained joins compare with equality (a NULL key never matches)
    // and the set operations also need the per-row match counts, so these
    // shapes are rejected instead of silently producing different rows.
    if join.null_equality == NullEquality::NullEqualsNull {
        return Err(unsupported(
            "INTERSECT/EXCEPT (or a null-aware join predicate) is not maintained",
        ));
    }
    // `FROM a, b` / `CROSS JOIN` plans as an inner join without an `ON`
    // clause; a residual filter would need a non-equi condition and stays
    // unsupported.
    if join.on.is_empty() && join.join_type == JoinType::Inner {
        if join.filter.is_none() {
            return analyze_cross_join(join, projection, tables, request);
        }
        return Err(unsupported(
            "a CROSS JOIN with a WHERE clause is not supported",
        ));
    }
    let left_input = join_input(&join.left, tables)?;
    let right_input = join_input(&join.right, tables)?;
    let left = left_input.table;
    let right = right_input.table;
    let (left_alias, right_alias) =
        (left_input.alias.as_deref(), right_input.alias.as_deref());
    let left_filter = left_input.filter.clone();
    let right_filter = right_input.filter.clone();

    // The equality keys as (left, right) name pairs; differently named keys
    // are supported by the lookup join.
    let mut key_pairs = Vec::new();
    let mut conditions = Vec::new();
    for (left_expr, right_expr) in &join.on {
        let left_column = column_of(left_expr)
            .ok_or_else(|| unsupported("join keys must be plain columns"))?;
        let right_column = column_of(right_expr)
            .ok_or_else(|| unsupported("join keys must be plain columns"))?;
        key_pairs.push((left_column.name.clone(), right_column.name.clone()));
    }
    if let Some(filter) = &join.filter {
        for conjunct in split_conjunction(filter) {
            if let Some((first, second)) = equi_columns(conjunct) {
                let first_side = side_of(first, left_alias, right_alias, left, right);
                let second_side = side_of(second, left_alias, right_alias, left, right);
                let (left_column, right_column) = match (first_side, second_side) {
                    (Some(Side::Left), Some(Side::Right)) => (first, second),
                    (Some(Side::Right), Some(Side::Left)) => (second, first),
                    _ => {
                        return Err(unsupported(
                            "join conditions between the same side are not supported",
                        ));
                    }
                };
                key_pairs.push((left_column.name.clone(), right_column.name.clone()));
            } else {
                let (left_column, right_column, op) =
                    column_compare(conjunct, left_alias, right_alias, left, right)?;
                conditions.push(SemiAntiCondition {
                    left_column,
                    right_column,
                    op,
                });
            }
        }
    }
    if key_pairs.is_empty() {
        return Err(unsupported("join without an equality key"));
    }
    key_pairs.sort();
    key_pairs.dedup();
    let join_keys = key_pairs
        .iter()
        .map(|(left, _)| left.clone())
        .collect::<Vec<_>>();
    let right_keys = key_pairs
        .iter()
        .map(|(_, right)| right.clone())
        .collect::<Vec<_>>();
    let same_names = join_keys == right_keys;

    match join.join_type {
        JoinType::Inner => {
            if !conditions.is_empty() {
                return Err(unsupported("inner join with non-equality conditions"));
            }
            if !same_names {
                return Err(unsupported(
                    "differently named join keys are only supported by a lookup LEFT JOIN",
                ));
            }
            let (left_value, right_value) = join_values(
                projection,
                left_alias,
                right_alias,
                left,
                right,
                &join_keys,
            )?;
            Ok(ViewSpec::Join {
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                output_table_id: request.mv_table_id.clone(),
                join_keys,
                left_value,
                right_value,
                left_filter,
                right_filter,
            })
        }
        JoinType::Left => {
            if !conditions.is_empty() {
                return Err(unsupported("left join with non-equality conditions"));
            }
            analyze_outer_join(
                "LEFT",
                projection,
                left,
                right,
                left_alias,
                right_alias,
                join_keys,
                right_keys,
                left_filter,
                right_filter,
                request,
            )
        }
        JoinType::Right => {
            if !conditions.is_empty() {
                return Err(unsupported("right join with non-equality conditions"));
            }
            // `A RIGHT JOIN B` keeps every row of `B`, i.e. `B LEFT JOIN A`;
            // the swapped sides swap their key names too.
            analyze_outer_join(
                "RIGHT",
                projection,
                right,
                left,
                right_alias,
                left_alias,
                right_keys,
                join_keys,
                right_filter,
                left_filter,
                request,
            )
        }
        JoinType::Full => {
            if !conditions.is_empty() {
                return Err(unsupported("full join with non-equality conditions"));
            }
            if left_filter.is_some() || right_filter.is_some() {
                return Err(unsupported(
                    "a join input with a WHERE clause (or a filtered derived table) is not supported yet",
                ));
            }
            if !same_names {
                return Err(unsupported(
                    "differently named join keys are only supported by a lookup LEFT JOIN",
                ));
            }
            if left.primary_keys.is_empty() || right.primary_keys.is_empty() {
                return Err(unsupported("FULL JOIN needs a primary key on both sources"));
            }
            let (left_value, right_value) = join_values(
                projection,
                left_alias,
                right_alias,
                left,
                right,
                &join_keys,
            )?;
            Ok(ViewSpec::FullJoin {
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                output_table_id: request.mv_table_id.clone(),
                join_keys,
                left_value,
                right_value,
            })
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            if left_filter.is_some() || right_filter.is_some() {
                return Err(unsupported(
                    "a join input with a WHERE clause (or a filtered derived table) is not supported yet",
                ));
            }
            let output_columns = match projection {
                Some(projection) => {
                    if !is_plain_projection(projection) {
                        return Err(unsupported(
                            "computed columns in a semi/anti join output",
                        ));
                    }
                    let mut columns = Vec::new();
                    for expr in &projection.expr {
                        let column = column_of(expr).ok_or_else(|| {
                            unsupported("semi/anti output must be plain columns")
                        })?;
                        let on_left =
                            side_of(column, left_alias, right_alias, left, right)
                                == Some(Side::Left)
                                || (left.schema.field_with_name(&column.name).is_ok()
                                    && right
                                        .schema
                                        .field_with_name(&column.name)
                                        .is_err());
                        if !on_left {
                            return Err(unsupported(format!(
                                "semi/anti join output column {} is not a left column",
                                column.name
                            )));
                        }
                        columns.push(column.name.clone());
                    }
                    columns
                }
                // No projection left above the join: the join output is the
                // left input's schema (a pushed-down projection included).
                None => join
                    .left
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
            };
            Ok(ViewSpec::SemiAnti {
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                mv_table_id: request.mv_table_id.clone(),
                join_keys,
                conditions,
                output_columns,
                anti: join.join_type == JoinType::LeftAnti,
            })
        }
        other => Err(unsupported(format!("join type {other:?}"))),
    }
}

/// A `UNION ALL` of sources with identical schemas.
fn analyze_union(
    union: &Union,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    if let Some(projection) = projection {
        if !is_plain_projection(projection) {
            return Err(unsupported("computed columns above UNION ALL"));
        }
        let columns = projection
            .expr
            .iter()
            .map(column_name)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| unsupported("UNION ALL output must be plain columns"))?;
        let union_columns = union
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        if columns != union_columns {
            return Err(unsupported(
                "UNION ALL output must keep the column order and names",
            ));
        }
    }
    let mut sources = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        sources.push(union_branch(peel(input), tables)?);
    }
    if sources.len() < 2 {
        return Err(unsupported("UNION ALL needs at least two branches"));
    }
    let first = &sources[0];
    let first_schema =
        union_output_schema_for(&first.source.schema, &first.columns, &first.exprs)?;
    for branch in &sources[1..] {
        let schema = union_output_schema_for(
            &branch.source.schema,
            &branch.columns,
            &branch.exprs,
        )?;
        if !same_union_schema(&first_schema, &schema) {
            return Err(unsupported(
                "UNION ALL branches must have identical output schemas",
            ));
        }
    }
    let keyed = !first.source.primary_keys.is_empty();
    for branch in &sources {
        let source = &branch.source;
        let columns = &branch.columns;
        let exprs = &branch.exprs;
        if keyed != !source.primary_keys.is_empty() {
            return Err(unsupported(
                "UNION ALL branches must be all keyed or all append-only",
            ));
        }
        if keyed {
            // The keyed maintenance matches the MV rows by the source primary
            // keys, so a keyed branch must project them unchanged.
            let projected = if columns.is_empty() {
                source
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>()
            } else {
                columns.clone()
            };
            for key in &source.primary_keys {
                let position = projected.iter().position(|column| column == key);
                let unchanged = match position {
                    Some(_) if exprs.is_empty() => true,
                    Some(position) => exprs[position] == *key,
                    None => false,
                };
                if !unchanged {
                    return Err(unsupported(format!(
                        "a keyed UNION ALL branch must keep the primary key \
                         {key} as a plain column"
                    )));
                }
            }
        }
    }
    Ok(ViewSpec::UnionAll {
        view_id: request.view_id.clone(),
        sources: sources
            .into_iter()
            .map(|branch| UnionSourceSpec {
                table_id: branch.source.table_id,
                filter: branch.filter,
                columns: branch.columns,
                exprs: branch.exprs,
            })
            .collect(),
        mv_table_id: request.mv_table_id.clone(),
    })
}

/// A `UNION` (distinct) view: `SELECT ... UNION SELECT ...` deduplicates the
/// union output.  The optimizer plans it as a group-by over every column (the
/// raw plan keeps a `Distinct` node), so the analyzer accepts both shapes.
fn analyze_union_distinct(
    union: &Union,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let mut sources = Vec::with_capacity(union.inputs.len());
    for input in &union.inputs {
        sources.push(union_distinct_branch(peel(input), tables)?);
    }
    if sources.len() < 2 {
        return Err(unsupported("UNION needs at least two branches"));
    }
    let first = &sources[0];
    let first_schema =
        union_output_schema_for(&first.source.schema, &first.columns, &first.exprs)?;
    for branch in &sources[1..] {
        let schema = union_output_schema_for(
            &branch.source.schema,
            &branch.columns,
            &branch.exprs,
        )?;
        if !same_union_schema(&first_schema, &schema) {
            return Err(unsupported("UNION branches must have identical schemas"));
        }
    }
    let keyed = !first.source.primary_keys.is_empty();
    for branch in &sources {
        if keyed != !branch.source.primary_keys.is_empty() {
            return Err(unsupported(
                "UNION branches must be all keyed or all append-only",
            ));
        }
    }
    Ok(ViewSpec::UnionDistinct {
        view_id: request.view_id.clone(),
        sources: sources
            .into_iter()
            .map(|branch| UnionSourceSpec {
                table_id: branch.source.table_id,
                filter: branch.filter,
                columns: branch.columns,
                exprs: branch.exprs,
            })
            .collect(),
        mv_table_id: request.mv_table_id.clone(),
    })
}

/// One parsed union branch: the source, the branch filter, the projected
/// output columns (empty means every source column) and the rendered
/// projection expressions (empty means plain columns).
struct UnionBranch {
    source: IvmTable,
    filter: Option<String>,
    columns: Vec<String>,
    exprs: Vec<String>,
}

/// The source of one `UNION ALL` branch.
fn union_branch(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
) -> Result<UnionBranch> {
    let (source, output_columns, output_exprs, filter) = collect_row(plan, tables)?;
    Ok(UnionBranch {
        source,
        filter,
        columns: output_columns.unwrap_or_default(),
        exprs: output_exprs,
    })
}

/// Whether a rendered projection expression references `column`.
fn projection_uses_column(
    schema: &Schema,
    expression: &str,
    column: &str,
) -> Result<bool> {
    let context = SessionContext::new();
    let df_schema = DFSchema::try_from(schema.clone())
        .map_err(|error| unsupported(format!("invalid source schema: {error}")))?;
    let expression = context
        .state()
        .create_logical_expr(expression, &df_schema)
        .map_err(|error| {
            unsupported(format!("invalid union projection {expression:?}: {error}"))
        })?;
    let mut uses = false;
    expression
        .apply(|node| {
            if let Expr::Column(column_ref) = node
                && column_ref.name == column
            {
                uses = true;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| unsupported(format!("invalid union projection: {error}")))?;
    Ok(uses)
}

/// Two union outputs match when their column names and types match; the
/// nullability may differ between branches.
fn same_union_schema(left: &Schema, right: &Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| {
                left.name() == right.name() && left.data_type() == right.data_type()
            })
}

/// The source of one `UNION` (distinct) branch.  The CDC change column is
/// maintained internally and therefore never part of the distinct key: a
/// branch over a CDC source must project plain data columns.
fn union_distinct_branch(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
) -> Result<UnionBranch> {
    let (source, output_columns, output_exprs, filter) = collect_row(plan, tables)?;
    let change = source.cdc_column.as_deref();
    let columns = match output_columns {
        Some(columns) => columns,
        None => source
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| Some(name.as_str()) != change)
            .collect(),
    };
    if let Some(change) = change {
        if output_exprs.is_empty() {
            if columns.iter().any(|column| column == change) {
                return Err(unsupported(
                    "the CDC change column cannot be part of a UNION output",
                ));
            }
        } else {
            for expression in &output_exprs {
                if projection_uses_column(&source.schema, expression, change)? {
                    return Err(unsupported(
                        "the CDC change column cannot be part of a UNION output",
                    ));
                }
            }
        }
    }
    Ok(UnionBranch {
        source,
        filter,
        columns,
        exprs: output_exprs,
    })
}

/// The table of one join input, plus its alias when it has one.
/// A `CROSS JOIN` of two keyed sources: every pair of rows, keyed by both row
/// identities.
fn analyze_cross_join(
    join: &Join,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let left = join_input(&join.left, tables)?;
    let right = join_input(&join.right, tables)?;
    if left.filter.is_some() || right.filter.is_some() {
        return Err(unsupported(
            "a join input with a WHERE clause (or a filtered derived table) is not supported yet",
        ));
    }
    let (left, left_alias) = (left.table, left.alias.as_deref());
    let (right, right_alias) = (right.table, right.alias.as_deref());
    if left.primary_keys.is_empty() || right.primary_keys.is_empty() {
        return Err(unsupported(
            "a cross join needs primary keys on both sources",
        ));
    }
    let (left_value, right_value) = match projection {
        Some(projection) => {
            join_values(Some(projection), left_alias, right_alias, left, right, &[])?
        }
        None => {
            // The optimizer drops the projection when the pruned join output
            // is exactly the select list: every column of the output belongs
            // to one side, one of them is the payload of that side.
            let mut left_value = None;
            let mut right_value = None;
            for field in join.schema.fields() {
                let column = datafusion::common::Column::new_unqualified(field.name());
                match side_of(&column, left_alias, right_alias, left, right) {
                    Some(Side::Left) => {
                        if left_value.replace(field.name().clone()).is_some() {
                            return Err(unsupported(
                                "a cross join takes one left payload column",
                            ));
                        }
                    }
                    Some(Side::Right) => {
                        if right_value.replace(field.name().clone()).is_some() {
                            return Err(unsupported(
                                "a cross join takes one right payload column",
                            ));
                        }
                    }
                    None => {
                        return Err(unsupported(format!(
                            "cross join output column {} is ambiguous; use distinct names \
                             or aliases",
                            field.name()
                        )));
                    }
                }
            }
            (
                left_value.ok_or_else(|| {
                    unsupported("a cross join needs a left payload column")
                })?,
                right_value.ok_or_else(|| {
                    unsupported("a cross join needs a right payload column")
                })?,
            )
        }
    };
    Ok(ViewSpec::CrossJoin {
        view_id: request.view_id.clone(),
        left_table_id: left.table_id.clone(),
        right_table_id: right.table_id.clone(),
        output_table_id: request.mv_table_id.clone(),
        left_value,
        right_value,
    })
}

/// An outer join that keeps every row of `left`: a lookup join when the right
/// side is keyed by the join keys (at most one match per left row), otherwise
/// a pair-keyed left join over two keyed sources.
#[allow(clippy::too_many_arguments)]
fn analyze_outer_join(
    kind: &str,
    projection: Option<&Projection>,
    left: &IvmTable,
    right: &IvmTable,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    join_keys: Vec<String>,
    right_keys: Vec<String>,
    left_filter: Option<String>,
    right_filter: Option<String>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    // Same-named keys stay compact (the right keys default to the left ones).
    let right_keys = if join_keys == right_keys {
        Vec::new()
    } else {
        right_keys
    };
    let effective_right_keys = if right_keys.is_empty() {
        join_keys.clone()
    } else {
        right_keys.clone()
    };
    // The join keys never become payloads, under either name.
    let mut key_names = join_keys.clone();
    for key in &effective_right_keys {
        if !key_names.contains(key) {
            key_names.push(key.clone());
        }
    }
    let (left_value, right_value) =
        join_values(projection, left_alias, right_alias, left, right, &key_names)?;
    let lookup = {
        let mut expected = right.primary_keys.clone();
        expected.sort();
        let mut keys = effective_right_keys;
        keys.sort();
        !expected.is_empty() && expected == keys
    };
    if lookup {
        if right_filter.is_some() {
            return Err(unsupported(
                "a join input with a WHERE clause (or a filtered derived table) is not supported yet",
            ));
        }
        Ok(ViewSpec::LookupJoin {
            view_id: request.view_id.clone(),
            left_table_id: left.table_id.clone(),
            right_table_id: right.table_id.clone(),
            output_table_id: request.mv_table_id.clone(),
            join_keys,
            right_keys,
            left_filter,
            left_value,
            right_value,
        })
    } else if left_filter.is_some() || right_filter.is_some() {
        Err(unsupported(
            "a join input with a WHERE clause (or a filtered derived table) is not supported yet",
        ))
    } else if !right_keys.is_empty() {
        Err(unsupported(
            "differently named join keys are only supported by a lookup LEFT JOIN",
        ))
    } else if !left.primary_keys.is_empty() && !right.primary_keys.is_empty() {
        Ok(ViewSpec::LeftJoin {
            view_id: request.view_id.clone(),
            left_table_id: left.table_id.clone(),
            right_table_id: right.table_id.clone(),
            output_table_id: request.mv_table_id.clone(),
            join_keys,
            left_value,
            right_value,
        })
    } else {
        Err(unsupported(format!(
            "{kind} JOIN needs keyed sources (or a right source keyed by the join keys)"
        )))
    }
}

/// One join input: the source table, its alias and an optional side filter.
struct JoinInput<'a> {
    table: &'a IvmTable,
    alias: Option<String>,
    filter: Option<String>,
}

fn join_input<'a>(
    plan: &'a LogicalPlan,
    tables: &'a HashMap<String, IvmTable>,
) -> Result<JoinInput<'a>> {
    let alias = if let LogicalPlan::SubqueryAlias(alias) = plan {
        Some(alias.alias.table().to_string())
    } else {
        None
    };
    let input = match plan {
        LogicalPlan::SubqueryAlias(alias) => &alias.input,
        other => other,
    };
    // A side filter is pushed below the join as a `Filter` (or into the
    // scan's pushed filters).
    let mut filter = None;
    let mut node = peel(input);
    if let LogicalPlan::Filter(predicate) = node {
        filter = Some(render_filter(&predicate.predicate)?);
        node = peel(&predicate.input);
    }
    let LogicalPlan::TableScan(scan) = node else {
        return Err(unsupported("join input must be a table"));
    };
    if let Some(pushed) = scan_filter(&scan.filters)? {
        filter = Some(combine_filter(filter, pushed));
    }
    let source = resolve_table(tables, &scan.table_name)?;
    Ok(JoinInput {
        table: source,
        alias,
        filter,
    })
}

fn column_of(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => column_of(&alias.expr),
        _ => None,
    }
}

/// `col = col` equality, used for join keys.
fn equi_columns(expr: &Expr) -> Option<(&Column, &Column)> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let left = column_of(&binary.left)?;
    let right = column_of(&binary.right)?;
    Some((left, right))
}

/// `left.col op right.col`, normalized so the first column is on the left side
/// of the join.
fn column_compare(
    expr: &Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    left: &IvmTable,
    right: &IvmTable,
) -> Result<(String, String, CompareOp)> {
    let Expr::BinaryExpr(binary) = expr else {
        return Err(unsupported("join conditions must compare columns"));
    };
    let first = column_of(&binary.left)
        .ok_or_else(|| unsupported("join conditions must compare columns"))?;
    let second = column_of(&binary.right)
        .ok_or_else(|| unsupported("join conditions must compare columns"))?;
    let op = compare_op(binary.op)?;
    let first_side = side_of(first, left_alias, right_alias, left, right);
    let second_side = side_of(second, left_alias, right_alias, left, right);
    match (first_side, second_side) {
        (Some(Side::Left), Some(Side::Right)) => {
            Ok((first.name.clone(), second.name.clone(), op))
        }
        (Some(Side::Right), Some(Side::Left)) => {
            Ok((second.name.clone(), first.name.clone(), flip(op)))
        }
        _ => Err(unsupported(
            "join conditions must compare a left and a right column",
        )),
    }
}

fn side_of(
    column: &Column,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    left: &IvmTable,
    right: &IvmTable,
) -> Option<Side> {
    if let Some(relation) = &column.relation {
        let relation = relation.table();
        if left_alias == Some(relation)
            || (left_alias.is_none() && relation == left.table_name)
        {
            return Some(Side::Left);
        }
        if right_alias == Some(relation)
            || (right_alias.is_none() && relation == right.table_name)
        {
            return Some(Side::Right);
        }
    }
    let on_left = left.schema.field_with_name(&column.name).is_ok();
    let on_right = right.schema.field_with_name(&column.name).is_ok();
    match (on_left, on_right) {
        (true, false) => Some(Side::Left),
        (false, true) => Some(Side::Right),
        _ => None,
    }
}

/// The single payload column of each join side, taken from the select list.
fn join_values(
    projection: Option<&Projection>,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    left: &IvmTable,
    right: &IvmTable,
    join_keys: &[String],
) -> Result<(String, String)> {
    let projection = projection.ok_or_else(|| {
        unsupported("a join view needs one payload column from each side")
    })?;
    if !is_plain_projection(projection) {
        return Err(unsupported("computed columns in a join output"));
    }
    let mut left_value = None;
    let mut right_value = None;
    for expr in &projection.expr {
        let column = column_of(expr)
            .ok_or_else(|| unsupported("join output must be plain columns"))?;
        if join_keys.contains(&column.name) {
            continue;
        }
        match side_of(column, left_alias, right_alias, left, right) {
            Some(Side::Left) => {
                if left_value.replace(column.name.clone()).is_some() {
                    return Err(unsupported("a join view takes one left payload column"));
                }
            }
            Some(Side::Right) => {
                if right_value.replace(column.name.clone()).is_some() {
                    return Err(unsupported(
                        "a join view takes one right payload column",
                    ));
                }
            }
            None => {
                return Err(unsupported(format!(
                    "join output column {} is ambiguous",
                    column.name
                )));
            }
        }
    }
    let left_value = left_value
        .ok_or_else(|| unsupported("a join view needs a left payload column"))?;
    let right_value = right_value
        .ok_or_else(|| unsupported("a join view needs a right payload column"))?;
    Ok((left_value, right_value))
}

/// Stable definition identity.  FNV-1a over the canonical spec JSON: a change
/// in columns, keys or aggregate kind changes the hash and triggers a rebuild.
///
/// The generated value-count state table id is normalised away, so recreating
/// that table during a rebuild does not change the identity again.
pub fn definition_hash(spec: &ViewSpec) -> Result<String> {
    let mut normalized = spec.clone();
    match &mut normalized {
        ViewSpec::MinMax { state_table_id, .. }
        | ViewSpec::DistinctAgg { state_table_id, .. } => {
            *state_table_id = String::new();
        }
        _ => {}
    }
    let bytes = serde_json::to_vec(&normalized)?;
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    Ok(format!("{hash:016x}"))
}

fn peel(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => peel(&alias.input),
        _ => plan,
    }
}

fn resolve_table<'a>(
    tables: &'a HashMap<String, IvmTable>,
    name: &TableReference,
) -> Result<&'a IvmTable> {
    let full = name.to_string();
    if let Some(table) = tables.get(&full) {
        return Ok(table);
    }
    if let Some(schema) = name.schema()
        && let Some(table) = tables.get(&format!("{schema}.{}", name.table()))
    {
        return Ok(table);
    }
    tables.get(name.table()).ok_or_else(|| {
        rootcause::report!("source table {full} is not available to the analyzer")
    })
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Alias(alias) => column_name(&alias.expr),
        _ => None,
    }
}

fn is_plain_projection(projection: &Projection) -> bool {
    projection
        .expr
        .iter()
        .all(|expr| column_name(expr).is_some())
}

/// The plain columns of every argument of a multi-column aggregate
/// (`COUNT(DISTINCT a, b)`).
fn multi_column_args(args: &[Expr]) -> Result<Vec<String>> {
    if args.is_empty() {
        return Err(unsupported("COUNT(DISTINCT ...) needs an argument"));
    }
    args.iter()
        .map(|arg| single_column_arg(std::slice::from_ref(arg)))
        .collect()
}

fn single_column_arg(args: &[Expr]) -> Result<String> {
    match args {
        [expr] => column_name(expr)
            .ok_or_else(|| unsupported("aggregate arguments must be plain columns")),
        _ => Err(unsupported("aggregates take exactly one column argument")),
    }
}

/// An integer literal, for the top-k rank comparison.
fn int_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn compare_op(op: Operator) -> Result<CompareOp> {
    match op {
        Operator::Eq => Ok(CompareOp::Eq),
        Operator::NotEq => Ok(CompareOp::Ne),
        Operator::Lt => Ok(CompareOp::Lt),
        Operator::LtEq => Ok(CompareOp::Le),
        Operator::Gt => Ok(CompareOp::Gt),
        Operator::GtEq => Ok(CompareOp::Ge),
        _ => Err(unsupported(format!("comparison {op}"))),
    }
}

fn flip(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

fn plan_label(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Projection(_) => "a projection",
        LogicalPlan::Filter(_) => "a filter",
        LogicalPlan::Aggregate(_) => "an aggregate",
        LogicalPlan::Window(_) => "a window",
        LogicalPlan::Sort(_) => "a sort",
        LogicalPlan::Limit(_) => "a limit",
        LogicalPlan::Join(_) => "a join",
        LogicalPlan::Union(_) => "a union",
        LogicalPlan::Distinct(_) => "SELECT DISTINCT",
        LogicalPlan::TableScan(_) => "a table scan",
        LogicalPlan::SubqueryAlias(_) => "a subquery alias",
        LogicalPlan::Subquery(_) => "a subquery",
        LogicalPlan::Values(_) => "VALUES",
        LogicalPlan::Repartition(_) => "a repartition",
        LogicalPlan::EmptyRelation(_) => "an empty relation",
        _ => "an unsupported plan node",
    }
}

fn unsupported(what: impl std::fmt::Display) -> rootcause::Report {
    rootcause::report!("unsupported incremental view shape: {what}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemTable;
    use datafusion::prelude::SessionContext;
    use lakesoul_io::file_format::PhysicalFormat;

    use super::*;
    use crate::runtime::{CompareOp, DistinctAggKind, MinMaxKind, WindowFunction};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn source_table(name: &str) -> IvmTable {
        IvmTable {
            table_id: format!("table_{name}"),
            table_name: name.to_string(),
            namespace: "default".to_string(),
            table_path: format!("file:///tmp/{name}"),
            schema: schema(),
            primary_keys: vec!["k".to_string()],
            bucket_columns: Vec::new(),
            hash_bucket_num: "1".to_string(),
            cdc_column: None,
            file_format: PhysicalFormat::Parquet,
        }
    }

    async fn plan(sql: &str) -> LogicalPlan {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        ctx.register_table("src", Arc::new(table)).unwrap();
        ctx.sql(sql).await.unwrap().logical_plan().clone()
    }

    fn request() -> AnalyzeRequest {
        AnalyzeRequest {
            view_id: "view_1".to_string(),
            mv_table_id: "table_mv".to_string(),
            state_table_id: Some("table_state".to_string()),
        }
    }

    async fn analyze(sql: &str) -> Result<AnalyzedView> {
        let tables = HashMap::from([("src".to_string(), source_table("src"))]);
        analyze_select(&plan(sql).await, &tables, &request())
    }

    /// The rendered predicate without grouping parentheses or identifier
    /// quotes, so the assertions do not depend on the unparser's formatting.
    fn normalized(filter: Option<&str>) -> Option<String> {
        filter.map(|filter| filter.replace(['(', ')', '"'], ""))
    }

    #[tokio::test]
    async fn analyzes_sum_count() {
        let analyzed = analyze("select g, sum(v), count(*) from src group by g")
            .await
            .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::SumCount {
                view_id: "view_1".to_string(),
                source_table_id: "table_src".to_string(),
                mv_table_id: "table_mv".to_string(),
                group_keys: vec!["g".to_string()],
                group_exprs: Vec::new(),
                value_column: Some("v".to_string()),
                value_expr: None,
                count_column: None,
                aggregate_filter: None,
                filter: None,
                having: None,
                average: false,
            }
        );
    }

    #[tokio::test]
    async fn analyzes_array_agg() {
        let analyzed = analyze("select k, array_agg(g order by v) from src group by k")
            .await
            .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::ArrayAgg {
                view_id: "view_1".to_string(),
                source_table_id: "table_src".to_string(),
                mv_table_id: "table_mv".to_string(),
                group_keys: vec!["k".to_string()],
                group_exprs: Vec::new(),
                value_column: Some("g".to_string()),
                value_expr: None,
                order_by: vec!["v".to_string()],
                filter: None,
            }
        );

        // Directions render inside the aggregate.
        let analyzed =
            analyze("select k, array_agg(g order by v desc) from src group by k")
                .await
                .unwrap();
        let ViewSpec::ArrayAgg { order_by, .. } = analyzed.spec else {
            panic!("expected an array_agg spec");
        };
        assert_eq!(order_by, vec!["\"v\" desc nulls first".to_string()]);

        // The aggregate ordering is required and mixing is rejected.
        assert!(
            analyze("select k, array_agg(g) from src group by k")
                .await
                .is_err()
        );
        assert!(
            analyze("select k, array_agg(g order by v), sum(v) from src group by k")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_sum_expressions() {
        let analyzed = analyze("select g, sum(v * 2) from src group by g")
            .await
            .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::SumCount {
                view_id: "view_1".to_string(),
                source_table_id: "table_src".to_string(),
                mv_table_id: "table_mv".to_string(),
                group_keys: vec!["g".to_string()],
                group_exprs: Vec::new(),
                value_column: None,
                value_expr: Some("(v * 2)".to_string()),
                count_column: None,
                aggregate_filter: None,
                filter: None,
                having: None,
                average: false,
            }
        );

        // SUM and AVG share the same expression, hoisted by the optimizer.
        let analyzed = analyze("select g, sum(v * 2), avg(v * 2) from src group by g")
            .await
            .unwrap();
        let ViewSpec::SumCount {
            value_expr,
            average,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(value_expr.as_deref(), Some("(v * 2)"));
        assert!(average);

        // HAVING over the same expression maps onto sum_v.
        let analyzed =
            analyze("select g, sum(v * 2) from src group by g having sum(v * 2) > 10")
                .await
                .unwrap();
        let ViewSpec::SumCount { having, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("sum_v > 10"));

        // The aggregates must agree on one value.
        assert!(
            analyze("select g, sum(v), avg(v * 2) from src group by g")
                .await
                .is_err()
        );
        // COUNT(column) cannot count an aggregated expression.
        assert!(
            analyze("select g, sum(v * 2), count(v) from src group by g")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_row_expressions() {
        let analyzed = analyze(
            "select k, v * 2 as v2, case when v > 5 then 'big' else 'small' end as bucket from src",
        )
        .await
        .unwrap();
        let ViewSpec::Row {
            output_columns,
            output_exprs,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a row spec");
        };
        assert_eq!(output_columns, vec!["k", "v2", "bucket"]);
        assert_eq!(output_exprs.len(), 3);
        let exprs = output_exprs
            .iter()
            .map(|expr| expr.replace(['(', ')'], ""))
            .collect::<Vec<_>>();
        assert_eq!(exprs[0], "k");
        assert_eq!(exprs[1], "v * 2");
        assert!(exprs[2].to_uppercase().starts_with("CASE"));
        assert_eq!(filter, None);

        // A plain projection stays compact; a rename keeps its expression.
        let analyzed = analyze("select k, v from src").await.unwrap();
        let ViewSpec::Row { output_exprs, .. } = analyzed.spec else {
            panic!("expected a row spec");
        };
        assert!(output_exprs.is_empty());

        let analyzed = analyze("select k, v as value from src").await.unwrap();
        let ViewSpec::Row {
            output_columns,
            output_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a row spec");
        };
        assert_eq!(output_columns, vec!["k", "value"]);
        assert_eq!(output_exprs, vec!["k", "v"]);

        // WHERE and expressions combine.
        let analyzed = analyze("select k, v + 1 as v1 from src where v > 5")
            .await
            .unwrap();
        let ViewSpec::Row {
            output_exprs,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a row spec");
        };
        assert_eq!(output_exprs, vec!["k", "(v + 1)"]);
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn analyzes_string_agg() {
        let analyzed =
            analyze("select k, string_agg(g, ',' order by v) from src group by k")
                .await
                .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::StringAgg {
                view_id: "view_1".to_string(),
                source_table_id: "table_src".to_string(),
                mv_table_id: "table_mv".to_string(),
                group_keys: vec!["k".to_string()],
                group_exprs: Vec::new(),
                value_column: Some("g".to_string()),
                value_expr: None,
                delimiter: "','".to_string(),
                order_by: vec!["v".to_string()],
                filter: None,
                having: None,
            }
        );

        // Directions render inside the aggregate; HAVING maps the column.
        let analyzed = analyze(
            "select k, string_agg(g, '|' order by v desc) from src group by k \
             having string_agg(g, '|' order by v desc) <> ''",
        )
        .await
        .unwrap();
        let ViewSpec::StringAgg {
            order_by, having, ..
        } = analyzed.spec
        else {
            panic!("expected a string_agg spec");
        };
        assert_eq!(order_by, vec!["\"v\" desc nulls first".to_string()]);
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("string_agg_g <> ''")
        );

        // The aggregate ordering is required, the delimiter must be a string
        // literal, and the aggregate cannot be mixed.
        assert!(
            analyze("select k, string_agg(g, ',') from src group by k")
                .await
                .is_err()
        );
        assert!(
            analyze("select k, string_agg(g, v order by v) from src group by k")
                .await
                .is_err()
        );
        assert!(
            analyze(
                "select k, string_agg(g, ',' order by v), sum(v) from src group by k"
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_aggregate_filter() {
        let analyzed =
            analyze("select g, sum(v) filter (where v > 5) from src group by g")
                .await
                .unwrap();
        let ViewSpec::SumCount {
            value_column,
            aggregate_filter,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(
            normalized(aggregate_filter.as_deref()).as_deref(),
            Some("v > 5")
        );
        assert_eq!(filter, None);

        // WHERE and FILTER combine; COUNT(*) stays unfiltered machinery.
        let analyzed = analyze(
            "select g, sum(v) filter (where v > 5), count(*) from src \
             where g <> 'x' group by g",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount {
            aggregate_filter,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(
            normalized(aggregate_filter.as_deref()).as_deref(),
            Some("v > 5")
        );
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("g <> 'x'"));

        // HAVING over the same filtered aggregate maps onto sum_v.
        let analyzed = analyze(
            "select g, sum(v) filter (where v > 5) from src group by g \
             having sum(v) filter (where v > 5) > 10",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount { having, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("sum_v > 10"));

        // Every value aggregate must carry the same FILTER.
        assert!(
            analyze("select g, sum(v) filter (where v > 5), avg(v) from src group by g")
                .await
                .is_err()
        );
        assert!(
            analyze(
                "select g, sum(v) filter (where v > 5), sum(v) filter (where v < 0) from src group by g"
            )
            .await
            .is_err()
        );
        // COUNT(*) FILTER and a HAVING with a different FILTER are not
        // maintained.
        assert!(
            analyze("select g, count(*) filter (where v > 5) from src group by g")
                .await
                .is_err()
        );
        assert!(
            analyze(
                "select g, sum(v) filter (where v > 5) from src group by g having sum(v) > 10"
            )
            .await
            .is_err()
        );
        // So is a FILTER over another aggregate family.
        assert!(
            analyze("select g, min(v) filter (where v > 5) from src group by g")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_count_column() {
        // A count-only view maintains the non-NULL count of the column.
        let analyzed = analyze("select g, count(v) from src group by g")
            .await
            .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::SumCount {
                view_id: "view_1".to_string(),
                source_table_id: "table_src".to_string(),
                mv_table_id: "table_mv".to_string(),
                group_keys: vec!["g".to_string()],
                group_exprs: Vec::new(),
                value_column: None,
                value_expr: None,
                count_column: Some("v".to_string()),
                aggregate_filter: None,
                filter: None,
                having: None,
                average: false,
            }
        );

        // SUM and COUNT over the same column share the count state.
        let analyzed = analyze("select g, sum(v), count(v) from src group by g")
            .await
            .unwrap();
        let ViewSpec::SumCount {
            value_column,
            count_column,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(count_column, Some("v".to_string()));

        // The non-NULL count is shared, so SUM and COUNT must agree.
        assert!(
            analyze("select g, sum(v), count(k) from src group by g")
                .await
                .is_err()
        );
        // A repeated COUNT(column) is rejected by the planner before the
        // analyzer sees it, so only the mixed-column case is asserted here.

        // HAVING maps COUNT(column) onto the maintained column.
        let analyzed =
            analyze("select g, count(v) from src group by g having count(v) > 0")
                .await
                .unwrap();
        let ViewSpec::SumCount { having, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("__ivm_nonnull_count > 0")
        );
    }

    #[tokio::test]
    async fn analyzes_count_only_and_global_aggregates() {
        let analyzed = analyze("select g, count(*) from src group by g")
            .await
            .unwrap();
        let ViewSpec::SumCount {
            value_column,
            group_keys,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(value_column, None);
        assert_eq!(group_keys, vec!["g".to_string()]);

        let analyzed = analyze("select sum(v) from src").await.unwrap();
        let ViewSpec::SumCount { group_keys, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert!(group_keys.is_empty());
    }

    #[tokio::test]
    async fn analyzes_multi_window_clauses() {
        // Different clauses chain into several WindowAggr nodes; the top node
        // is the last select item.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) as rn, \
             sum(v) over (partition by v) as cnt from src",
        )
        .await
        .unwrap();
        let ViewSpec::MultiWindow { windows, .. } = analyzed.spec else {
            panic!("expected a multi-window spec");
        };
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].partition_keys, vec!["v".to_string()]);
        assert!(windows[0].order_keys.is_empty());
        assert_eq!(windows[0].columns.len(), 1);
        assert_eq!(windows[0].columns[0].column, "cnt");
        assert_eq!(windows[1].partition_keys, vec!["g".to_string()]);
        assert_eq!(windows[1].order_keys, vec!["v".to_string()]);
        assert_eq!(windows[1].columns[0].column, "rn");

        // Different partitions put an intermediate projection between the
        // clauses.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) as rn, \
             sum(v) over (partition by v order by k) as cnt from src",
        )
        .await
        .unwrap();
        let ViewSpec::MultiWindow { windows, .. } = analyzed.spec else {
            panic!("expected a multi-window spec");
        };
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].partition_keys, vec!["v".to_string()]);
        assert_eq!(windows[0].order_keys, vec!["k".to_string()]);
        assert_eq!(windows[1].partition_keys, vec!["g".to_string()]);
        assert_eq!(windows[1].order_keys, vec!["v".to_string()]);

        // One shared clause stays a plain window view.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) as rn, \
             rank() over (partition by g order by v) as rk from src",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::Window { .. }));
    }

    #[tokio::test]
    async fn analyzes_value_count_group_expressions() {
        // MIN/MAX and DISTINCT carry computed group keys like the other
        // aggregate families.
        let analyzed =
            analyze("select v % 10 as bucket, min(v) from src group by bucket")
                .await
                .unwrap();
        let ViewSpec::MinMax {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a min/max spec");
        };
        assert_eq!(group_keys, vec!["bucket".to_string()]);
        assert_eq!(group_exprs, vec!["(v % 10)".to_string()]);

        let analyzed = analyze(
            "select v % 10 as bucket, count(distinct g) from src group by bucket",
        )
        .await
        .unwrap();
        let ViewSpec::DistinctAgg {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a distinct spec");
        };
        assert_eq!(group_keys, vec!["bucket".to_string()]);
        assert_eq!(group_exprs, vec!["(v % 10)".to_string()]);

        // A plain key stays compact next to an expression.
        let analyzed =
            analyze("select g, v % 10 as bucket, max(v) from src group by g, bucket")
                .await
                .unwrap();
        let ViewSpec::MinMax {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a min/max spec");
        };
        assert_eq!(group_keys, vec!["g".to_string(), "bucket".to_string()]);
        assert_eq!(group_exprs, vec!["g".to_string(), "(v % 10)".to_string()]);
    }

    #[tokio::test]
    async fn analyzes_semi_anti_subqueries() {
        // `WHERE EXISTS` / `NOT EXISTS` / `IN` decorrelate into LeftSemi /
        // LeftAnti joins over a correlated subquery alias.
        let analyzed = analyze_multi(
            "select a.k, a.g from a where exists (select 1 from b where b.k = a.k)",
            vec![source_table("a"), source_table("b")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            join_keys,
            conditions,
            output_columns,
            anti,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert_eq!(join_keys, vec!["k".to_string()]);
        assert!(conditions.is_empty());
        assert_eq!(output_columns, vec!["k".to_string(), "g".to_string()]);
        assert!(!anti);

        let analyzed = analyze_multi(
            "select a.k, a.g from a where not exists (select 1 from b where b.k = a.k)",
            vec![source_table("a"), source_table("b")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti { anti, .. } = analyzed.spec else {
            panic!("expected a semi/anti spec");
        };
        assert!(anti);

        let analyzed = analyze_multi(
            "select a.k, a.g from a where a.k in (select b.k from b)",
            vec![source_table("a"), source_table("b")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti { anti, .. } = analyzed.spec else {
            panic!("expected a semi/anti spec");
        };
        assert!(!anti);

        // An extra comparison condition is kept.
        let analyzed = analyze_multi(
            "select a.k, a.g from a where exists \
             (select 1 from b where b.k = a.k and b.v > a.v)",
            vec![source_table("a"), source_table("b")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti { conditions, .. } = analyzed.spec else {
            panic!("expected a semi/anti spec");
        };
        assert!(!conditions.is_empty());
    }

    #[tokio::test]
    async fn analyzes_cross_joins() {
        // `CROSS JOIN` / `FROM a, b` plans as an inner join without `ON`.
        // The two sides need distinguishable column names (the payloads are
        // resolved by schema membership).
        let mut right = source_table("b");
        right.schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("rg", DataType::Utf8, false),
            Field::new("rv", DataType::Int64, false),
        ]));
        let analyzed = analyze_multi(
            "select a.v, b.rv from a cross join b",
            vec![source_table("a"), right.clone()],
        )
        .await
        .unwrap();
        let ViewSpec::CrossJoin {
            left_value,
            right_value,
            ..
        } = analyzed.spec
        else {
            panic!("expected a cross join spec");
        };
        assert_eq!(left_value, "v");
        assert_eq!(right_value, "rv");

        // Both sources must be keyed.
        let mut left = source_table("a");
        left.primary_keys.clear();
        assert!(
            analyze_multi(
                "select a.v, b.rv from a cross join b",
                vec![left, right.clone()]
            )
            .await
            .is_err()
        );

        // The comma spelling is the same shape.
        let analyzed =
            analyze_multi("select a.v, b.rv from a, b", vec![source_table("a"), right])
                .await
                .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::CrossJoin { .. }));
    }

    #[tokio::test]
    async fn analyzes_union_distinct() {
        // `UNION` deduplicates the whole union row; the source fixture is
        // keyed, so this covers the keyed branch.
        let analyzed = analyze("select k, g, v from src union select k, g, v from src")
            .await
            .unwrap();
        let ViewSpec::UnionDistinct { sources, .. } = analyzed.spec else {
            panic!("expected a union-distinct spec");
        };
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].table_id, "table_src");
        assert_eq!(sources[1].table_id, "table_src");
        assert!(sources.iter().all(|source| source.filter.is_none()));

        // Per-branch predicates are kept.
        let analyzed = analyze(
            "select k, g, v from src where v > 5 \
             union select k, g, v from src where v < 3",
        )
        .await
        .unwrap();
        let ViewSpec::UnionDistinct { sources, .. } = analyzed.spec else {
            panic!("expected a union-distinct spec");
        };
        assert!(sources.iter().all(|source| source.filter.is_some()));

        // A UNION of a keyed and an append-only branch is rejected.
        let ctx = SessionContext::new();
        for name in ["src", "src2"] {
            let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
            ctx.register_table(name, Arc::new(table)).unwrap();
        }
        let plan = ctx
            .sql("select k, g, v from src union select k, g, v from src2")
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let mut append_only = source_table("src2");
        append_only.primary_keys.clear();
        let tables = HashMap::from([
            ("src".to_string(), source_table("src")),
            ("src2".to_string(), append_only),
        ]);
        assert!(analyze_select(&plan, &tables, &request()).is_err());
    }

    #[tokio::test]
    async fn analyzes_group_expressions() {
        let analyzed =
            analyze("select v % 10 as bucket, sum(v) from src group by bucket")
                .await
                .unwrap();
        let ViewSpec::SumCount {
            group_keys,
            group_exprs,
            value_column,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(group_keys, vec!["bucket".to_string()]);
        assert_eq!(group_exprs, vec!["(v % 10)".to_string()]);
        assert_eq!(value_column, Some("v".to_string()));

        // A plain key mixed with an expression keeps both entries.
        let analyzed =
            analyze("select g, v % 10 as bucket, sum(v) from src group by g, bucket")
                .await
                .unwrap();
        let ViewSpec::SumCount {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(group_keys, vec!["g".to_string(), "bucket".to_string()]);
        assert_eq!(group_exprs, vec!["g".to_string(), "(v % 10)".to_string()]);

        // HAVING over the group alias maps onto the key column.
        let analyzed = analyze(
            "select v % 10 as bucket, sum(v) from src group by bucket having bucket > 2",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount { having, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("bucket > 2"));

        // A group expression needs an alias.
        assert!(
            analyze("select v % 10, sum(v) from src group by v % 10")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_recompute_group_expressions() {
        // The recompute families carry computed group keys the same way
        // SUM/COUNT does.
        let analyzed =
            analyze("select v % 10 as bucket, var_samp(v) from src group by bucket")
                .await
                .unwrap();
        let ViewSpec::Variance {
            group_keys,
            group_exprs,
            value_column,
            ..
        } = analyzed.spec
        else {
            panic!("expected a variance spec");
        };
        assert_eq!(group_keys, vec!["bucket".to_string()]);
        assert_eq!(group_exprs, vec!["(v % 10)".to_string()]);
        assert_eq!(value_column.as_deref(), Some("v"));

        let analyzed =
            analyze("select v % 10 as bucket, median(v) from src group by bucket")
                .await
                .unwrap();
        let ViewSpec::Median {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a median spec");
        };
        assert_eq!(group_keys, vec!["bucket".to_string()]);
        assert_eq!(group_exprs, vec!["(v % 10)".to_string()]);

        let analyzed = analyze(
            "select v % 10 as bucket, string_agg(g, ',' order by k) from src group by bucket",
        )
        .await
        .unwrap();
        let ViewSpec::StringAgg {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a string_agg spec");
        };
        assert_eq!(group_keys, vec!["bucket".to_string()]);
        assert_eq!(group_exprs, vec!["(v % 10)".to_string()]);

        let analyzed = analyze(
            "select v % 10 as bucket, array_agg(g order by k) from src group by bucket",
        )
        .await
        .unwrap();
        let ViewSpec::ArrayAgg {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected an array_agg spec");
        };
        assert_eq!(group_keys, vec!["bucket".to_string()]);
        assert_eq!(group_exprs, vec!["(v % 10)".to_string()]);

        // A plain column key keeps the compact form next to an expression.
        let analyzed = analyze(
            "select g, v % 10 as bucket, var_samp(v) from src group by g, bucket",
        )
        .await
        .unwrap();
        let ViewSpec::Variance {
            group_keys,
            group_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a variance spec");
        };
        assert_eq!(group_keys, vec!["g".to_string(), "bucket".to_string()]);
        assert_eq!(group_exprs, vec!["g".to_string(), "(v % 10)".to_string()]);
    }

    #[tokio::test]
    async fn analyzes_window_order_expressions() {
        // ORDER BY may be a scalar expression: the keys keep the plain
        // expression (for validation) and the ordering adds the direction.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v % 10) as rn from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(order_keys, vec!["(v % 10)".to_string()]);
        assert_eq!(order_by, vec!["((v % 10))".to_string()]);

        // A mixed list of plain columns and expressions.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v % 10 desc, k) as rn from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(order_keys, vec!["(v % 10)".to_string(), "k".to_string()]);
        assert_eq!(
            order_by,
            vec!["((v % 10)) desc nulls first".to_string(), "k".to_string()]
        );

        // The aggregate ordering of STRING_AGG/ARRAY_AGG accepts expressions
        // the same way.
        let analyzed =
            analyze("select g, string_agg(g, ',' order by v % 10) from src group by g")
                .await
                .unwrap();
        let ViewSpec::StringAgg { order_by, .. } = analyzed.spec else {
            panic!("expected a string_agg spec");
        };
        assert_eq!(order_by, vec!["((v % 10))".to_string()]);

        let analyzed =
            analyze("select g, array_agg(g order by v % 10, k) from src group by g")
                .await
                .unwrap();
        let ViewSpec::ArrayAgg { order_by, .. } = analyzed.spec else {
            panic!("expected an array_agg spec");
        };
        assert_eq!(order_by, vec!["((v % 10))".to_string(), "k".to_string()]);

        // The same ordering parser drives TOP-K.
        let analyzed = analyze(
            "select k, v from (select k, g, v, \
             row_number() over (partition by g order by v % 10) as rn from src) t \
             where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(order_keys, vec!["(v % 10)".to_string()]);
        assert_eq!(order_by, vec!["((v % 10))".to_string()]);
    }

    #[tokio::test]
    async fn analyzes_recompute_value_expressions() {
        // Variance/median arguments may be scalar expressions; the optimizer
        // numeric-coercion cast is dropped like for SUM/AVG.
        let analyzed = analyze("select g, var_samp(v * 2) from src group by g")
            .await
            .unwrap();
        let ViewSpec::Variance {
            value_column,
            value_expr,
            ..
        } = analyzed.spec
        else {
            panic!("expected a variance spec");
        };
        assert_eq!(value_column, None);
        assert_eq!(value_expr.as_deref(), Some("(v * 2)"));

        let analyzed = analyze("select g, median(v * 2) from src group by g")
            .await
            .unwrap();
        let ViewSpec::Median {
            value_column,
            value_expr,
            ..
        } = analyzed.spec
        else {
            panic!("expected a median spec");
        };
        assert_eq!(value_column, None);
        assert_eq!(value_expr.as_deref(), Some("(v * 2)"));

        // A plain column keeps the column form.
        let analyzed = analyze("select g, stddev_samp(v) from src group by g")
            .await
            .unwrap();
        let ViewSpec::Variance {
            value_column,
            value_expr,
            ..
        } = analyzed.spec
        else {
            panic!("expected a variance spec");
        };
        assert_eq!(value_column.as_deref(), Some("v"));
        assert_eq!(value_expr, None);
    }

    #[tokio::test]
    async fn analyzes_agg_argument_expressions() {
        // STRING_AGG over a cast keeps the cast, so numbers can be joined.
        let analyzed = analyze_optimized(
            "select g, string_agg(cast(v as varchar), '|' order by k) from src group by g",
        )
        .await
        .unwrap();
        let ViewSpec::StringAgg {
            value_column,
            value_expr,
            delimiter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a string_agg spec");
        };
        assert_eq!(value_column, None);
        assert!(
            value_expr
                .as_deref()
                .is_some_and(|expr| expr.to_uppercase().contains("CAST"))
        );
        assert_eq!(delimiter, "'|'");

        // ARRAY_AGG over an arithmetic expression.
        let analyzed =
            analyze("select g, array_agg(v * 2 order by k) from src group by g")
                .await
                .unwrap();
        let ViewSpec::ArrayAgg {
            value_column,
            value_expr,
            ..
        } = analyzed.spec
        else {
            panic!("expected an array_agg spec");
        };
        assert_eq!(value_column, None);
        assert_eq!(value_expr.as_deref(), Some("(v * 2)"));
    }

    #[tokio::test]
    async fn analyzes_min_max_expressions() {
        let analyzed = analyze("select g, min(v * 2) from src group by g")
            .await
            .unwrap();
        let ViewSpec::MinMax {
            value_column,
            value_expr,
            min_max,
            ..
        } = analyzed.spec
        else {
            panic!("expected a min/max spec");
        };
        assert_eq!(value_column, None);
        assert_eq!(value_expr.as_deref(), Some("(v * 2)"));
        assert_eq!(min_max, MinMaxKind::Min);

        // A conditional aggregate is materialized as written.
        let analyzed = analyze(
            "select g, max(case when v > 5 then v else 0 end) from src group by g",
        )
        .await
        .unwrap();
        let ViewSpec::MinMax { value_expr, .. } = analyzed.spec else {
            panic!("expected a min/max spec");
        };
        assert!(
            value_expr
                .as_deref()
                .is_some_and(|expr| expr.to_uppercase().contains("CASE"))
        );

        // HAVING must reference the same argument.
        let analyzed =
            analyze("select g, min(v * 2) from src group by g having min(v * 2) > 10")
                .await
                .unwrap();
        let ViewSpec::MinMax { having, .. } = analyzed.spec else {
            panic!("expected a min/max spec");
        };
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("value > 10"));
        assert!(
            analyze("select g, min(v * 2) from src group by g having min(v) > 10")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_min_max_and_requires_a_state_table() {
        let analyzed = analyze("select g, max(v) from src group by g")
            .await
            .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::MinMax {
                view_id: "view_1".to_string(),
                source_table_id: "table_src".to_string(),
                mv_table_id: "table_mv".to_string(),
                state_table_id: "table_state".to_string(),
                group_keys: vec!["g".to_string()],
                group_exprs: Vec::new(),
                value_column: Some("v".to_string()),
                value_expr: None,
                min_max: MinMaxKind::Max,
                filter: None,
                having: None,
            }
        );

        let tables = HashMap::from([("src".to_string(), source_table("src"))]);
        let mut request = request();
        request.state_table_id = None;
        assert!(
            analyze_select(
                &plan("select g, min(v) from src group by g").await,
                &tables,
                &request,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_distinct_aggregates() {
        let analyzed = analyze("select g, count(distinct v) from src group by g")
            .await
            .unwrap();
        let ViewSpec::DistinctAgg {
            agg, value_column, ..
        } = analyzed.spec
        else {
            panic!("expected a distinct spec");
        };
        assert_eq!(agg, DistinctAggKind::Count);
        assert_eq!(value_column, "v");

        let analyzed = analyze("select g, sum(distinct v) from src group by g")
            .await
            .unwrap();
        let ViewSpec::DistinctAgg { agg, .. } = analyzed.spec else {
            panic!("expected a distinct spec");
        };
        assert_eq!(agg, DistinctAggKind::Sum);
    }

    #[tokio::test]
    async fn analyzes_multi_column_distinct_aggregates() {
        let analyzed =
            analyze_optimized("select g, count(distinct v, k) from src group by g")
                .await
                .unwrap();
        let ViewSpec::DistinctAgg {
            agg,
            value_column,
            value_columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a distinct spec");
        };
        assert_eq!(agg, DistinctAggKind::Count);
        assert_eq!(value_column, "v");
        assert_eq!(value_columns, vec!["v".to_string(), "k".to_string()]);

        // A multi-column distinct count cannot carry HAVING or FILTER.
        assert!(
            analyze_optimized(
                "select g, count(distinct v, k) from src group by g \
                 having count(distinct v, k) > 1",
            )
            .await
            .is_err()
        );
        assert!(
            analyze_optimized(
                "select g, count(distinct v, k) filter (where v > 1) from src \
                 group by g",
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_split_distinct_aggregates() {
        // The optimized plan splits COUNT(DISTINCT v) into two aggregates.
        let analyzed =
            analyze_optimized("select g, count(distinct v) from src group by g")
                .await
                .unwrap();
        let ViewSpec::DistinctAgg {
            agg,
            value_column,
            filter,
            having,
            ..
        } = analyzed.spec
        else {
            panic!("expected a distinct spec");
        };
        assert_eq!(agg, DistinctAggKind::Count);
        assert_eq!(value_column, "v");
        assert_eq!(filter, None);
        assert_eq!(having, None);

        // WHERE below the inner aggregate and SUM(DISTINCT).
        let analyzed = analyze_optimized(
            "select g, sum(distinct v) from src where v > 5 group by g",
        )
        .await
        .unwrap();
        let ViewSpec::DistinctAgg {
            agg,
            value_column,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a distinct spec");
        };
        assert_eq!(agg, DistinctAggKind::Sum);
        assert_eq!(value_column, "v");
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));

        // HAVING over the split distinct aggregate maps to `value`.
        let analyzed = analyze_optimized(
            "select g, count(distinct v) from src group by g having count(distinct v) > 1",
        )
        .await
        .unwrap();
        let ViewSpec::DistinctAgg { having, .. } = analyzed.spec else {
            panic!("expected a distinct spec");
        };
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("value > 1"));

        // The raw and the optimized plan describe the same view.
        let raw = analyze("select g, count(distinct v) from src group by g")
            .await
            .unwrap();
        let optimized =
            analyze_optimized("select g, count(distinct v) from src group by g")
                .await
                .unwrap();
        assert_eq!(raw.spec, optimized.spec);

        // Mixing distinct aggregates stays rejected.
        assert!(
            analyze_optimized(
                "select g, count(distinct v), sum(distinct v) from src group by g"
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_projection_and_filters() {
        let analyzed = analyze("select v, k from src where v > 5 and g = 'a'")
            .await
            .unwrap();
        let ViewSpec::Row {
            output_columns,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a row spec");
        };
        assert_eq!(output_columns, vec!["v".to_string(), "k".to_string()]);
        assert_eq!(
            normalized(filter.as_deref()).as_deref(),
            Some("v > 5 AND g = 'a'")
        );
    }

    #[tokio::test]
    async fn analyzes_is_null_filters_and_reversed_comparisons() {
        let analyzed = analyze("select k from src where g is null and 5 < v")
            .await
            .unwrap();
        let ViewSpec::Row { filter, .. } = analyzed.spec else {
            panic!("expected a row spec");
        };
        assert_eq!(
            normalized(filter.as_deref()).as_deref(),
            Some("g IS NULL AND 5 < v")
        );
    }

    #[tokio::test]
    async fn analyzes_ctes_and_derived_tables() {
        // The optimizer inlines plain CTEs, so they analyze as their inner
        // plan (the executor always analyzes the optimized plan).
        let analyzed = analyze_optimized(
            "with filtered as (select g, v from src where v > 5) \
             select g, sum(v) from filtered group by g",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount {
            group_keys, filter, ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(group_keys, vec!["g".to_string()]);
        assert!(filter.is_some());

        // A CTE whose body is an aggregate keeps the aggregate shape.
        let analyzed = analyze_optimized(
            "with totals as (select g, sum(v) as s from src group by g) \
             select g, s from totals",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::SumCount { .. }));

        // A derived table with a predicate feeds the aggregate.
        let analyzed = analyze_optimized(
            "select t.g, sum(t.v) from (select g, v from src where v > 5) t \
             group by t.g",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount { filter, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert!(filter.is_some());

        // A window under a derived table keeps the window shape.
        let analyzed = analyze_optimized(
            "select g, rn from (select g, row_number() over (partition by g order by v) as rn from src) t",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::Window { .. }));
    }

    #[tokio::test]
    async fn analyzes_select_distinct() {
        // The raw and the optimized plan describe the same count-only shape.
        let expected = ViewSpec::SumCount {
            view_id: "view_1".to_string(),
            source_table_id: "table_src".to_string(),
            mv_table_id: "table_mv".to_string(),
            group_keys: vec!["g".to_string()],
            group_exprs: Vec::new(),
            value_column: None,
            value_expr: None,
            count_column: None,
            aggregate_filter: None,
            filter: None,
            having: None,
            average: false,
        };
        let raw = analyze("select distinct g from src").await.unwrap();
        assert_eq!(raw.spec, expected);
        let optimized = analyze_optimized("select distinct g from src")
            .await
            .unwrap();
        assert_eq!(optimized.spec, expected);

        // Several columns and a WHERE clause.
        let analyzed = analyze_optimized("select distinct k, g from src where v > 5")
            .await
            .unwrap();
        let ViewSpec::SumCount {
            group_keys,
            value_column,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(group_keys, vec!["k".to_string(), "g".to_string()]);
        assert_eq!(value_column, None);
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));

        // The definition equals an explicit count-only group by.
        let counted = analyze_optimized("select g, count(*) from src group by g")
            .await
            .unwrap();
        assert_eq!(optimized.spec, counted.spec);
        assert_eq!(optimized.definition_hash, counted.definition_hash);

        // Computed distinct expressions are rejected.
        assert!(
            analyze_optimized("select distinct v + 1 from src")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_having() {
        let analyzed =
            analyze("select g, sum(v), count(*) from src group by g having sum(v) > 10")
                .await
                .unwrap();
        let ViewSpec::SumCount { having, filter, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(filter, None);
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("sum_v > 10"));

        // Aggregates that only appear in HAVING are materialized too.
        let analyzed = analyze(
            "select g, sum(v) from src group by g having count(*) >= 2 and sum(v) <= 100",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount { having, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("count_v >= 2 AND sum_v <= 100")
        );

        // WHERE and HAVING combine, including a group-key predicate.
        let analyzed = analyze(
            "select g, count(*) from src where v > 5 group by g having count(*) > 1 and g <> 'x'",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount { having, filter, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("count_v > 1 AND g <> 'x'")
        );

        // MIN/MAX map to the `value` column.
        let analyzed = analyze("select g, min(v) from src group by g having min(v) > 3")
            .await
            .unwrap();
        let ViewSpec::MinMax { having, .. } = analyzed.spec else {
            panic!("expected a min/max spec");
        };
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("value > 3"));
    }

    #[tokio::test]
    async fn analyzes_having_on_the_optimized_plan() {
        let analyzed = analyze_optimized(
            "select g, sum(v) from src group by g having sum(v) > 10 or g = 'x'",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount { having, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("sum_v > 10 OR g = 'x'")
        );

        // A pure group-key HAVING is pushed below the aggregate and becomes a
        // WHERE filter instead.
        let analyzed =
            analyze_optimized("select g, sum(v) from src group by g having g <> 'x'")
                .await
                .unwrap();
        let ViewSpec::SumCount { having, filter, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(having, None);
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("g <> 'x'"));
    }

    #[tokio::test]
    async fn rejects_unmaterialized_having() {
        // MAX is not maintained by a SUM/COUNT view.
        assert!(
            analyze("select g, sum(v) from src group by g having max(v) > 5")
                .await
                .is_err()
        );
        // COUNT(column) over another column would need a second non-NULL
        // count; over the summed column it is maintained.
        assert!(
            analyze("select g, sum(v) from src group by g having count(k) > 1")
                .await
                .is_err()
        );
        assert!(
            analyze("select g, sum(v) from src group by g having count(v) > 1")
                .await
                .is_ok()
        );
        // A second value column would need a second SUM state.
        assert!(
            analyze("select g, sum(v) from src group by g having sum(k) > 1")
                .await
                .is_err()
        );
        // A different aggregate kind for a MIN view.
        assert!(
            analyze("select g, min(v) from src group by g having sum(v) > 5")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_variance_aggregates() {
        for (sql, statistic) in [
            (
                "select g, var_samp(v) from src group by g",
                VarianceKind::VarSamp,
            ),
            (
                "select g, var_pop(v) from src group by g",
                VarianceKind::VarPop,
            ),
            (
                "select g, stddev_samp(v) from src group by g",
                VarianceKind::StddevSamp,
            ),
            (
                "select g, stddev_pop(v) from src group by g",
                VarianceKind::StddevPop,
            ),
        ] {
            let analyzed = analyze_optimized(sql).await.unwrap();
            let ViewSpec::Variance {
                statistic: got,
                value_column,
                ..
            } = analyzed.spec
            else {
                panic!("expected a variance spec");
            };
            assert_eq!(got, statistic);
            assert_eq!(value_column.as_deref(), Some("v"));
        }

        // WHERE and HAVING.
        let analyzed = analyze_optimized(
            "select g, var_samp(v) from src where v > 5 group by g having var_samp(v) > 1",
        )
        .await
        .unwrap();
        let ViewSpec::Variance { filter, having, .. } = analyzed.spec else {
            panic!("expected a variance spec");
        };
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("variance_v > 1.0")
        );

        // Mixing with other aggregate kinds is rejected.
        assert!(
            analyze("select g, var_samp(v), sum(v) from src group by g")
                .await
                .is_err()
        );
        assert!(
            analyze("select g, var_samp(v), var_pop(v) from src group by g")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_ignore_nulls_windows() {
        // IGNORE NULLS is kept for the value functions.
        for (sql, function) in [
            (
                "select k, lag(v) ignore nulls over (partition by g order by v) from src",
                WindowFunction::Lag,
            ),
            (
                "select k, first_value(v) ignore nulls over (partition by g order by v) from src",
                WindowFunction::FirstValue,
            ),
        ] {
            let analyzed = analyze(sql).await.unwrap();
            let column = only_window_column(&analyzed.spec);
            let got = column.function;
            let ignore_nulls = column.ignore_nulls;

            assert_eq!(got, function);
            assert!(ignore_nulls);
        }

        // The default and the explicit RESPECT NULLS stay false.
        let analyzed = analyze(
            "select k, lag(v) respect nulls over (partition by g order by v) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let ignore_nulls = column.ignore_nulls;

        assert!(!ignore_nulls);

        // For ranking and aggregate windows it is a no-op and normalized away.
        let analyzed =
            analyze("select k, row_number() ignore nulls over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let ignore_nulls = column.ignore_nulls;

        assert!(!ignore_nulls);
        let analyzed =
            analyze("select k, sum(v) ignore nulls over (partition by g) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let ignore_nulls = column.ignore_nulls;

        assert!(!ignore_nulls);
    }

    #[tokio::test]
    async fn probe_union_distinct() {
        for sql in [
            "select k, v from src union select k, v from src",
            "select k, v from src union all select k, v from src",
            "select k, v from src union select k, g from src",
        ] {
            let ctx = SessionContext::new();
            let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
            ctx.register_table("src", Arc::new(table)).unwrap();
            match ctx.sql(sql).await {
                Ok(frame) => {
                    let plan = frame.logical_plan().clone();
                    println!("SQL: {sql}\nRAW:\n{plan}");
                    match ctx.state().optimize(&plan) {
                        Ok(optimized) => println!("OPT:\n{optimized}\n----"),
                        Err(error) => println!("OPT ERROR: {error}\n----"),
                    }
                }
                Err(error) => println!("SQL: {sql}\nPLAN ERROR: {error}\n----"),
            }
        }
    }

    #[tokio::test]
    async fn analyzes_avg() {
        let analyzed = analyze("select g, avg(v) from src group by g")
            .await
            .unwrap();
        let ViewSpec::SumCount {
            value_column,
            average,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(value_column, Some("v".to_string()));
        assert!(average);

        // AVG alongside SUM/COUNT over the same value column.
        let analyzed = analyze("select g, avg(v), sum(v), count(*) from src group by g")
            .await
            .unwrap();
        let ViewSpec::SumCount {
            value_column,
            average,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(value_column, Some("v".to_string()));
        assert!(average);

        // WHERE and HAVING work on the AVG view.
        let analyzed = analyze(
            "select g, avg(v) from src where v > 5 group by g having avg(v) > 10 or sum(v) > 100",
        )
        .await
        .unwrap();
        let ViewSpec::SumCount {
            filter,
            having,
            average,
            ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert!(average);
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("avg_v > 10 OR sum_v > 100")
        );

        // The optimized plan keeps the same shape.
        let analyzed =
            analyze_optimized("select g, avg(v) from src group by g having avg(v) > 10")
                .await
                .unwrap();
        let ViewSpec::SumCount {
            having, average, ..
        } = analyzed.spec
        else {
            panic!("expected a sum/count spec");
        };
        assert!(average);
        // The optimized plan coerces the literal to Float64.
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("avg_v > 10.0")
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_avg() {
        // AVG mixes with MIN/MAX or DISTINCT.
        assert!(
            analyze("select g, avg(v), min(v) from src group by g")
                .await
                .is_err()
        );
        // AVG and SUM must share the value column.
        assert!(
            analyze("select g, avg(v), sum(k) from src group by g")
                .await
                .is_err()
        );
        assert!(
            analyze("select g, sum(k), avg(v) from src group by g")
                .await
                .is_err()
        );
        // AVG over a non-numeric column is rejected when the MV is created.
        assert!(
            crate::runtime::avg_mv_schema_for(&schema(), &["g".to_string()], "g")
                .is_err()
        );
        assert!(
            crate::runtime::avg_mv_schema_for(&schema(), &["g".to_string()], "v").is_ok()
        );
    }

    #[tokio::test]
    async fn analyzes_aggregate_where() {
        let analyzed = analyze("select g, sum(v) from src where v > 5 group by g")
            .await
            .unwrap();
        let ViewSpec::SumCount { filter, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));

        let analyzed = analyze("select g, max(v) from src where v > 5 group by g")
            .await
            .unwrap();
        let ViewSpec::MinMax { filter, .. } = analyzed.spec else {
            panic!("expected a min/max spec");
        };
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));

        let analyzed =
            analyze("select g, count(distinct v) from src where v > 5 group by g")
                .await
                .unwrap();
        let ViewSpec::DistinctAgg { filter, .. } = analyzed.spec else {
            panic!("expected a distinct spec");
        };
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn analyzes_aggregate_where_on_the_optimized_plan() {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        ctx.register_table("src", Arc::new(table)).unwrap();
        let dataframe = ctx
            .sql("select g, sum(v) from src where v > 5 and g <> 'x' group by g")
            .await
            .unwrap();
        let optimized = dataframe.into_optimized_plan().unwrap();
        let tables = HashMap::from([("src".to_string(), source_table("src"))]);
        let analyzed = analyze_select(&optimized, &tables, &request()).unwrap();
        let ViewSpec::SumCount { filter, .. } = analyzed.spec else {
            panic!("expected a sum/count spec");
        };
        assert_eq!(
            normalized(filter.as_deref()).as_deref(),
            Some("v > 5 AND g <> 'x'")
        );
    }

    #[tokio::test]
    async fn analyzes_row_where_on_the_optimized_plan() {
        // The optimizer pushes the projection and (with an exact pushdown
        // provider) the filter into the table scan.
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        ctx.register_table("src", Arc::new(table)).unwrap();
        let dataframe = ctx
            .sql("select v, k from src where v > 5 and g = 'a'")
            .await
            .unwrap();
        let optimized = dataframe.into_optimized_plan().unwrap();
        let tables = HashMap::from([("src".to_string(), source_table("src"))]);
        let analyzed = analyze_select(&optimized, &tables, &request()).unwrap();
        let ViewSpec::Row {
            output_columns,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a row spec");
        };
        assert_eq!(output_columns, vec!["v".to_string(), "k".to_string()]);
        assert_eq!(
            normalized(filter.as_deref()).as_deref(),
            Some("v > 5 AND g = 'a'")
        );
    }

    #[tokio::test]
    async fn rejects_subquery_filters() {
        assert!(
            analyze(
                "select g, sum(v) from src where v > (select max(k) from src) group by g"
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_shapes() {
        // AVG does not mix with MIN/MAX.
        assert!(
            analyze("select g, avg(v), min(v) from src group by g")
                .await
                .is_err()
        );
        // DISTINCT ON has no view kind.
        assert!(
            analyze("select distinct on (g) k, g from src")
                .await
                .is_err()
        );
        // HAVING mixing aggregate kinds.
        assert!(
            analyze("select g, sum(v) from src group by g having max(v) > 0")
                .await
                .is_err()
        );
        // Joins and computed projections are later slices.
        assert!(analyze("select k + 1 from src").await.is_err());
        assert!(
            analyze("select * from src a join src b on a.k = b.k")
                .await
                .is_err()
        );
    }

    /// The only window column of an analyzed window view.
    fn only_window_column(spec: &ViewSpec) -> &WindowColumn {
        match spec {
            ViewSpec::Window { columns, .. } if columns.len() == 1 => &columns[0],
            other => panic!("expected a single-column window spec, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn analyzes_ranking_windows() {
        for (sql, function) in [
            (
                "select k, row_number() over (partition by g order by v) from src",
                WindowFunction::RowNumber,
            ),
            (
                "select k, rank() over (partition by g order by v) from src",
                WindowFunction::Rank,
            ),
            (
                "select k, dense_rank() over (partition by g order by v) from src",
                WindowFunction::DenseRank,
            ),
        ] {
            let analyzed = analyze(sql).await.unwrap();
            assert_eq!(
                analyzed.spec,
                ViewSpec::Window {
                    view_id: "view_1".to_string(),
                    source_table_id: "table_src".to_string(),
                    mv_table_id: "table_mv".to_string(),
                    partition_keys: vec!["g".to_string()],
                    order_keys: vec!["v".to_string()],
                    order_by: Vec::new(),
                    columns: vec![WindowColumn::new(function)],
                    filter: None,
                }
            );
        }
    }

    #[tokio::test]
    async fn analyzes_multiple_window_columns() {
        let analyzed = analyze(
            "select k, sum(v) over w as total, count(*) over w as n from src window w as (partition by g)",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            partition_keys,
            columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(partition_keys, vec!["g".to_string()]);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].function, WindowFunction::Sum);
        assert_eq!(columns[0].value_column.as_deref(), Some("v"));
        assert_eq!(columns[0].column, "total");
        assert_eq!(columns[1].function, WindowFunction::Count);
        assert_eq!(columns[1].column, "n");

        // Columns may carry their own frames.
        let analyzed = analyze(
            "select k, sum(v) over (partition by g order by v) as a, sum(k) over (partition by g order by v rows between unbounded preceding and current row) as b from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window { columns, .. } = analyzed.spec else {
            panic!("expected a window spec");
        };
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].window_frame, None);
        assert_eq!(
            columns[1].window_frame.as_deref(),
            Some("rows between unbounded preceding and current row")
        );
    }

    #[tokio::test]
    async fn analyzes_descending_orderings() {
        // A descending ordering is rendered explicitly.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v desc) from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(order_by, vec!["\"v\" desc nulls first".to_string()]);

        // `DESC NULLS LAST` and `ASC NULLS FIRST` keep their null placement.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v desc nulls last, k asc nulls first) from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window { order_by, .. } = analyzed.spec else {
            panic!("expected a window spec");
        };
        assert_eq!(
            order_by,
            vec![
                "\"v\" desc nulls last".to_string(),
                "\"k\" asc nulls first".to_string(),
            ]
        );

        // The ascending default stays the bare column.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v asc nulls last) from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window { order_by, .. } = analyzed.spec else {
            panic!("expected a window spec");
        };
        assert!(order_by.is_empty());

        // Top-k carries the same rendering.
        let analyzed = analyze(
            "select k, v from (select k, v, row_number() over (partition by g order by v desc) as rn from src) t where rn <= 2",
        )
        .await
        .unwrap();
        let ViewSpec::TopK {
            order_keys,
            order_by,
            limit,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(order_by, vec!["\"v\" desc nulls first".to_string()]);
        assert_eq!(limit, 2);
    }

    #[tokio::test]
    async fn analyzes_lag_lead_windows() {
        let analyzed =
            analyze("select k, lag(v) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(function, WindowFunction::Lag);
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(window_args, None);
        assert_eq!(order_keys, vec!["v".to_string()]);

        // Offset and default render to SQL text; the optimized plan keeps the
        // same spec.
        let analyzed = analyze_optimized(
            "select k, lead(v, 2, 0) over (partition by g order by v) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();

        assert_eq!(function, WindowFunction::Lead);
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(window_args.as_deref(), Some("2, 0"));

        // LAG/LEAD need an ordering.
        assert!(
            analyze("select k, lag(v) over (partition by g) from src")
                .await
                .is_err()
        );
        // The offset must be an integer literal, the value a plain column.
        assert!(
            analyze("select k, lag(v, k) over (partition by g order by v) from src")
                .await
                .is_err()
        );
        assert!(
            analyze("select k, lag(v + 1) over (partition by g order by v) from src")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_frame_value_windows() {
        let analyzed = analyze(
            "select k, first_value(v) over (partition by g order by v, k rows between unbounded preceding and unbounded following) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();
        let window_frame = column.window_frame.clone();

        assert_eq!(function, WindowFunction::FirstValue);
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(window_args, None);
        assert_eq!(
            window_frame.as_deref(),
            Some("rows between unbounded preceding and unbounded following")
        );

        // NTH_VALUE takes a positive integer row number.
        let analyzed = analyze(
            "select k, nth_value(v, 2) over (partition by g order by v, k) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let window_args = column.window_args.clone();
        let window_frame = column.window_frame.clone();

        assert_eq!(function, WindowFunction::NthValue);
        assert_eq!(window_args.as_deref(), Some("2"));
        assert_eq!(window_frame, None);

        // LAST_VALUE under the default frame.
        let analyzed = analyze(
            "select k, last_value(v) over (partition by g order by v, k) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;

        assert_eq!(function, WindowFunction::LastValue);

        // Rejections: a missing or non-literal row number, no ordering, and
        // a computed value.
        assert!(
            analyze("select k, nth_value(v) over (partition by g order by v) from src")
                .await
                .is_err()
        );
        assert!(
            analyze(
                "select k, nth_value(v, k) over (partition by g order by v) from src"
            )
            .await
            .is_err()
        );
        assert!(
            analyze("select k, first_value(v) over (partition by g) from src")
                .await
                .is_err()
        );
        assert!(
            analyze(
                "select k, first_value(v + 1) over (partition by g order by v) from src"
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_distribution_windows() {
        let analyzed =
            analyze("select k, ntile(4) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();

        assert_eq!(function, WindowFunction::Ntile);
        assert_eq!(value_column, None);
        assert_eq!(window_args.as_deref(), Some("4"));

        let analyzed =
            analyze("select k, percent_rank() over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;

        assert_eq!(function, WindowFunction::PercentRank);

        let analyzed =
            analyze("select k, cume_dist() over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;

        assert_eq!(function, WindowFunction::CumeDist);

        // NTILE needs a positive integer literal bucket count.
        assert!(
            analyze("select k, ntile(v) over (partition by g order by v) from src")
                .await
                .is_err()
        );
        // Ranking functions need an ordering.
        assert!(
            analyze("select k, ntile(4) over (partition by g) from src")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_window_filter() {
        let analyzed = analyze(
            "select k, sum(v) filter (where v > 5) over (partition by g) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let window_filter = column.window_filter.clone();

        assert_eq!(function, WindowFunction::Sum);
        assert_eq!(
            normalized(window_filter.as_deref()).as_deref(),
            Some("v > 5")
        );

        let analyzed = analyze(
            "select k, count(*) filter (where g = 'a') over (partition by g) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_filter = column.window_filter.clone();

        assert_eq!(function, WindowFunction::Count);
        assert_eq!(value_column, None);
        assert_eq!(
            normalized(window_filter.as_deref()).as_deref(),
            Some("g = 'a'")
        );
    }

    #[tokio::test]
    async fn analyzes_global_windows() {
        let analyzed = analyze("select k, row_number() over (order by v, k) from src")
            .await
            .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let partition_keys = match &analyzed.spec {
            ViewSpec::Window { partition_keys, .. } => partition_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(function, WindowFunction::RowNumber);
        assert!(partition_keys.is_empty());
        assert_eq!(order_keys, vec!["v".to_string(), "k".to_string()]);

        let analyzed = analyze("select k, sum(v) over () from src").await.unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let partition_keys = match &analyzed.spec {
            ViewSpec::Window { partition_keys, .. } => partition_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let value_column = column.value_column.clone();

        assert_eq!(function, WindowFunction::Sum);
        assert_eq!(value_column, Some("v".to_string()));
        assert!(partition_keys.is_empty());
        assert!(order_keys.is_empty());
    }

    #[tokio::test]
    async fn analyzes_aggregate_windows() {
        let analyzed = analyze("select k, sum(v) over (partition by g) from src")
            .await
            .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let value_column = column.value_column.clone();

        assert_eq!(function, WindowFunction::Sum);
        assert_eq!(value_column, Some("v".to_string()));
        assert!(order_keys.is_empty());

        let analyzed =
            analyze("select k, count(*) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let value_column = column.value_column.clone();

        assert_eq!(function, WindowFunction::Count);
        assert_eq!(value_column, None);
        assert_eq!(order_keys, vec!["v".to_string()]);
    }

    #[tokio::test]
    async fn analyzes_union_projections() {
        // Append-only branches may prune, rename and compute columns as long
        // as their output schemas match.
        let mut left = source_table("a");
        left.primary_keys.clear();
        let mut right = source_table("b");
        right.primary_keys.clear();
        let analyzed = analyze_multi(
            "select k as id, upper(g) as name from a \
             union all select k as id, g as name from b",
            vec![left, right],
        )
        .await
        .unwrap();
        let ViewSpec::UnionAll { sources, .. } = analyzed.spec else {
            panic!("expected a union all spec");
        };
        assert_eq!(
            sources[0].columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(sources[0].exprs.len(), 2);
        assert_eq!(sources[0].exprs[0], "k");
        assert!(sources.iter().all(|source| source.filter.is_none()));

        // A keyed UNION ALL branch must keep its primary keys unchanged.
        let analyzed = analyze_multi(
            "select k as id, g as name from a \
             union all select k as id, g as name from b",
            vec![source_table("a"), source_table("b")],
        )
        .await;
        assert!(analyzed.is_err());

        // UNION (distinct) accepts projected columns.
        let analyzed = analyze_multi(
            "select k as id, g as name from a \
             union select k as id, g as name from b",
            vec![source_table("a"), source_table("b")],
        )
        .await
        .unwrap();
        let ViewSpec::UnionDistinct { sources, .. } = analyzed.spec else {
            panic!("expected a union-distinct spec");
        };
        assert_eq!(
            sources[0].columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(sources[0].exprs[0], "k");

        // The CDC change column cannot be part of a UNION output.
        let mut left = source_table("a");
        left.cdc_column = Some("g".to_string());
        let mut right = source_table("b");
        right.cdc_column = Some("g".to_string());
        let analyzed = analyze_multi(
            "select k, g from a union select k, g from b",
            vec![left, right],
        )
        .await;
        assert!(analyzed.is_err());
    }

    #[tokio::test]
    async fn analyzes_top_k() {
        let analyzed = analyze(
            "select k, v from (select k, g, v, row_number() over (partition by g order by v) as rn from src) t where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK {
            group_keys,
            order_keys,
            output_columns,
            limit,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(group_keys, vec!["g".to_string()]);
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(output_columns, vec!["k".to_string(), "v".to_string()]);
        assert_eq!(limit, 3);
        assert_eq!(filter, None);
    }

    #[tokio::test]
    async fn analyzes_top_k_where() {
        let analyzed = analyze(
            "select k, v from (select k, g, v, row_number() over (partition by g order by v) as rn from src where v > 5) t where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK { limit, filter, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(limit, 3);
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn analyzes_window_where() {
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) from src where v > 5",
        )
        .await
        .unwrap();
        let filter = match &analyzed.spec {
            ViewSpec::Window { filter, .. } => filter.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn analyzes_window_and_top_k_where_on_the_optimized_plan() {
        // The optimizer pushes the filter below the window.
        let analyzed = analyze_optimized(
            "select k, sum(v) over (partition by g) from src where v > 5 and g <> 'x'",
        )
        .await
        .unwrap();
        let filter = match &analyzed.spec {
            ViewSpec::Window { filter, .. } => filter.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(
            normalized(filter.as_deref()).as_deref(),
            Some("v > 5 AND g <> 'x'")
        );

        let analyzed = analyze_optimized(
            "select k, v from (select k, g, v, row_number() over (partition by g order by v) as rn from src where v > 5) t where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK { filter, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn rejects_unsupported_window_shapes() {
        // A frame is only stored when it is not the default.
        let analyzed =
            analyze("select k, sum(v) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let window_frame = column.window_frame.clone();

        assert_eq!(window_frame, None);
        let analyzed = analyze(
            "select k, sum(v) over (partition by g order by v rows between 1 preceding and current row) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let window_frame = column.window_frame.clone();

        assert_eq!(
            window_frame.as_deref(),
            Some("rows between 1 preceding and current row")
        );
        // Ranking needs an ordering.
        assert!(
            analyze("select k, row_number() over (partition by g) from src")
                .await
                .is_err()
        );
        // Multiple windows with different partitionings chain into a
        // multi-window view.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v), rank() over (partition by k order by v) from src",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::MultiWindow { .. }));
        // Repeated functions need distinct column names (an alias).
        assert!(
            analyze(
                "select k, sum(v) over (partition by g order by v), sum(k) over (partition by g order by v rows between unbounded preceding and current row) from src"
            )
            .await
            .is_err()
        );
        // Computing on top of the window column is not maintained.
        assert!(
            analyze(
                "select k, row_number() over (partition by g order by v) + 1 from src"
            )
            .await
            .is_err()
        );
        // Filtering a rank column is not top-k.
        assert!(
            analyze(
                "select k from (select k, rank() over (partition by g order by v) as rn from src) t where rn <= 3"
            )
            .await
            .is_err()
        );
        // Global ORDER BY ... LIMIT is not maintained.
        assert!(
            analyze("select k, v from src order by v limit 3")
                .await
                .is_err()
        );
    }

    async fn analyze_optimized(sql: &str) -> Result<AnalyzedView> {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        ctx.register_table("src", Arc::new(table)).unwrap();
        let tables = HashMap::from([("src".to_string(), source_table("src"))]);
        let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let optimized = ctx.state().optimize(&plan).unwrap();
        analyze_select(&optimized, &tables, &request())
    }

    async fn analyze_multi(sql: &str, tables: Vec<IvmTable>) -> Result<AnalyzedView> {
        let ctx = SessionContext::new();
        let mut map = HashMap::new();
        for table in &tables {
            let mem = MemTable::try_new(table.schema.clone(), vec![vec![]]).unwrap();
            ctx.register_table(table.table_name.as_str(), Arc::new(mem))
                .unwrap();
            map.insert(table.table_name.clone(), table.clone());
        }
        let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let optimized = ctx.state().optimize(&plan).unwrap();
        analyze_select(&optimized, &map, &request())
    }

    #[tokio::test]
    async fn analyzes_inner_join() {
        let analyzed =
            analyze_optimized("select a.k, a.v, b.v from src a join src b on a.k = b.k")
                .await
                .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::Join {
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_src".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                left_filter: None,
                right_filter: None,
            }
        );
    }

    #[tokio::test]
    async fn rejects_unmaintained_shapes() {
        // A computed column (or scalar subquery) above an aggregate.
        assert!(
            analyze_optimized("select g, sum(v) + 1 from src group by g")
                .await
                .is_err()
        );
        assert!(
            analyze_optimized("select g, (select max(v) from src) from src group by g",)
                .await
                .is_err()
        );
        // GROUPING SETS have a dedicated message.
        for sql in [
            "select g, sum(v) from src group by rollup(g)",
            "select g, sum(v) from src group by cube(g)",
            "select g, sum(v) from src group by grouping sets ((g), ())",
        ] {
            let error = analyze_optimized(sql).await.unwrap_err().to_string();
            assert!(error.contains("GROUPING SETS"), "{sql}: {error}");
        }
        // A multi-column DISTINCT needs a GROUP BY.
        assert!(
            analyze_optimized("select count(distinct g, v) from src")
                .await
                .is_err()
        );
        // DISTINCT ON plans as first_value aggregates, which are not
        // maintained.
        assert!(
            analyze_optimized("select distinct on (g) g, v from src")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_set_operations() {
        // INTERSECT/EXCEPT plan as null-aware semi/anti joins: the maintained
        // semi/anti views match with equality and keep one row per left row,
        // which differs from the set-operation semantics (NULL rows match,
        // and the ALL variants count the matches on both sides).
        for sql in [
            "select k, v from src intersect select k, v from dim",
            "select k, v from src except select k, v from dim",
            "select k, v from src intersect all select k, v from dim",
            "select k, v from src except all select k, v from dim",
            "select a.k, b.v from src a join dim b \
             on a.k is not distinct from b.k",
            "select k from src where exists (select 1 from dim \
             where dim.k is not distinct from src.k)",
        ] {
            let error =
                analyze_multi(sql, vec![source_table("src"), source_table("dim")])
                    .await
                    .unwrap_err()
                    .to_string();
            assert!(error.contains("INTERSECT/EXCEPT"), "{sql}: {error}");
        }
    }

    #[tokio::test]
    async fn analyzes_filtered_inner_join_inputs() {
        // Both sides of an inner join can be filtered; the predicates are
        // pushed below the join and kept on the spec.
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v from src a join src b on a.k = b.k \
             where a.v > 1 and b.v > 2",
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            left_filter,
            right_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected an inner join spec");
        };
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 1"));
        assert_eq!(
            normalized(right_filter.as_deref()).as_deref(),
            Some("v > 2")
        );

        // The same through a filtered derived table.
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v from (select * from src where v > 1) a \
             join src b on a.k = b.k",
        )
        .await
        .unwrap();
        let ViewSpec::Join { left_filter, .. } = analyzed.spec else {
            panic!("expected an inner join spec");
        };
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 1"));
    }

    #[tokio::test]
    async fn rejects_filtered_join_inputs() {
        // SEMI/ANTI join side filters are pushed below the join and are not
        // maintained: the analyzer must reject them rather than ignore the
        // filter.
        assert!(
            analyze_optimized(
                "select a.v from src a left semi join src b on a.k = b.k where a.v > 1",
            )
            .await
            .is_err()
        );
        assert!(
            analyze_optimized(
                "select a.v from src a left anti join src b on a.k = b.k where a.v > 1",
            )
            .await
            .is_err()
        );
        // A cross-side predicate on a CROSS JOIN stays unsupported.
        assert!(
            analyze_optimized(
                "select a.v, b.v from src a cross join src b where a.v < b.v"
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_lookup_left_join() {
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v from src a left join src b on a.k = b.k",
        )
        .await
        .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::LookupJoin {
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_src".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                right_keys: Vec::new(),
                left_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
            }
        );

        // A right side keyed by something else keeps the pairs keyed by both
        // row identities.
        let dim = {
            let mut table = source_table("dim");
            table.primary_keys = vec!["v".to_string()];
            table
        };
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v from src a left join dim b on a.k = b.k",
            vec![source_table("src"), dim.clone()],
        )
        .await
        .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::LeftJoin {
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_dim".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                left_value: "v".to_string(),
                right_value: "v".to_string(),
            }
        );

        // The right key may have a different name: a lookup join by a
        // foreign key.
        let mut names = source_table("dim");
        names.schema = Arc::new(Schema::new(vec![
            Field::new("rk", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        names.primary_keys = vec!["rk".to_string()];
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v from src a left join dim b on a.k = b.rk",
            vec![source_table("src"), names.clone()],
        )
        .await
        .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::LookupJoin {
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_dim".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                right_keys: vec!["rk".to_string()],
                left_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
            }
        );

        // A left-side filter is kept for the lookup join.
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v from src a left join src b on a.k = b.k where a.v > 1",
        )
        .await
        .unwrap();
        let ViewSpec::LookupJoin { left_filter, .. } = analyzed.spec else {
            panic!("expected a lookup join spec");
        };
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 1"));

        // Differently named keys are rejected elsewhere.
        assert!(
            analyze_multi(
                "select a.k, a.v, b.v from src a join dim b on a.k = b.rk",
                vec![source_table("src"), names],
            )
            .await
            .is_err()
        );

        // An unkeyed side is rejected.
        let mut unkeyed = source_table("dim2");
        unkeyed.primary_keys = Vec::new();
        assert!(
            analyze_multi(
                "select a.k, a.v, b.v from src a left join dim2 b on a.k = b.k",
                vec![source_table("src"), unkeyed],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_right_join() {
        // `A RIGHT JOIN B` keeps B's rows: the swapped lookup join.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v from src a right join dim b on a.k = b.k",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::LookupJoin {
                view_id: "view_1".to_string(),
                left_table_id: "table_dim".to_string(),
                right_table_id: "table_src".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                right_keys: Vec::new(),
                left_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
            }
        );

        // When the swapped right side is not keyed by the join keys the
        // pair-keyed left join is used.
        let dim = {
            let mut table = source_table("dim");
            table.primary_keys = vec!["v".to_string()];
            table
        };
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v from dim b right join src a on a.k = b.k",
            vec![source_table("src"), dim],
        )
        .await
        .unwrap();
        let ViewSpec::LeftJoin {
            left_table_id,
            right_table_id,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left join spec");
        };
        assert_eq!(left_table_id, "table_src");
        assert_eq!(right_table_id, "table_dim");
    }

    #[tokio::test]
    async fn analyzes_full_join() {
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v from src a full join src b on a.k = b.k",
        )
        .await
        .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::FullJoin {
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_src".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                left_value: "v".to_string(),
                right_value: "v".to_string(),
            }
        );

        // Both sides must be keyed.
        let mut unkeyed = source_table("dim");
        unkeyed.primary_keys = Vec::new();
        assert!(
            analyze_multi(
                "select a.k, a.v, b.v from src a full join dim b on a.k = b.k",
                vec![source_table("src"), unkeyed],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_semi_and_anti_joins() {
        let analyzed = analyze_optimized(
            "select a.k, a.v from src a where exists (select 1 from src b where b.k = a.k)",
        )
        .await
        .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::SemiAnti {
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_src".to_string(),
                mv_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                conditions: Vec::new(),
                output_columns: vec!["k".to_string(), "v".to_string()],
                anti: false,
            }
        );

        let analyzed = analyze_optimized(
            "select a.k, a.v from src a where not exists (select 1 from src b where b.k = a.k)",
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti { anti, .. } = analyzed.spec else {
            panic!("expected a semi/anti spec");
        };
        assert!(anti);
    }

    #[tokio::test]
    async fn analyzes_semi_join_with_extra_conditions() {
        let analyzed = analyze_optimized(
            "select a.k, a.v from src a where exists (select 1 from src b where b.k = a.k and b.v < a.v)",
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti { conditions, .. } = analyzed.spec else {
            panic!("expected a semi/anti spec");
        };
        // `b.v < a.v` is normalized to `a.v > b.v`.
        assert_eq!(
            conditions,
            vec![SemiAntiCondition {
                left_column: "v".to_string(),
                right_column: "v".to_string(),
                op: CompareOp::Gt,
            }]
        );
    }

    #[tokio::test]
    async fn analyzes_union_all() {
        let left = source_table("a");
        let right = source_table("b");
        let analyzed = analyze_multi(
            "select * from a union all select * from b",
            vec![left, right],
        )
        .await
        .unwrap();
        assert_eq!(
            analyzed.spec,
            ViewSpec::UnionAll {
                view_id: "view_1".to_string(),
                sources: vec![
                    UnionSourceSpec {
                        table_id: "table_a".to_string(),
                        filter: None,
                        columns: vec!["k".to_string(), "g".to_string(), "v".to_string()],
                        exprs: Vec::new(),
                    },
                    UnionSourceSpec {
                        table_id: "table_b".to_string(),
                        filter: None,
                        columns: vec!["k".to_string(), "g".to_string(), "v".to_string()],
                        exprs: Vec::new(),
                    },
                ],
                mv_table_id: "table_mv".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn analyzes_union_all_where() {
        let left = source_table("a");
        let right = source_table("b");
        let analyzed = analyze_multi(
            "select * from a where v > 5 union all select * from b where g = 'x'",
            vec![left, right],
        )
        .await
        .unwrap();
        let ViewSpec::UnionAll { sources, .. } = analyzed.spec else {
            panic!("expected a union spec");
        };
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].table_id, "table_a");
        assert_eq!(
            normalized(sources[0].filter.as_deref()).as_deref(),
            Some("v > 5")
        );
        assert_eq!(sources[1].table_id, "table_b");
        assert_eq!(
            normalized(sources[1].filter.as_deref()).as_deref(),
            Some("g = 'x'")
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_join_and_union_shapes() {
        // Outer joins are not maintained incrementally.
        assert!(
            analyze_optimized("select a.k from src a left join src b on a.k = b.k")
                .await
                .is_err()
        );
        // The runtime keys a join on same-named columns.
        assert!(
            analyze_optimized("select a.k, a.v, b.v from src a join src b on a.k = b.v")
                .await
                .is_err()
        );
        // Inner joins take no extra conditions.
        assert!(
            analyze_optimized(
                "select a.k, a.v, b.v from src a join src b on a.k = b.k and a.v > b.v"
            )
            .await
            .is_err()
        );
        // UNION ALL branches must keep the primary keys of keyed sources and
        // have matching output schemas.
        let left = source_table("a");
        let right = source_table("b");
        assert!(
            analyze_multi(
                "select g from a union all select g from b",
                vec![left.clone(), right.clone()]
            )
            .await
            .is_err()
        );
        assert!(
            analyze_multi(
                "select k, g from a union all select k, v from b",
                vec![left, right]
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn definition_hash_is_stable_and_shape_sensitive() {
        let first = analyze("select g, sum(v) from src group by g")
            .await
            .unwrap();
        let second = analyze("select g, sum(v) from src group by g")
            .await
            .unwrap();
        assert_eq!(first.definition_hash, second.definition_hash);

        let other = analyze("select g, sum(k) from src group by g")
            .await
            .unwrap();
        assert_ne!(first.definition_hash, other.definition_hash);
    }
}
