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
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    Aggregate, Expr, Filter, Join, JoinType, LogicalPlan, Operator, Projection, Union,
    Window, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::sql::unparser::Unparser;

use crate::error::Result;
use crate::runtime::{
    CompareOp, DistinctAggKind, IVM_AVG_COLUMN, IVM_COUNT_COLUMN,
    IVM_NONNULL_COUNT_COLUMN, IVM_SUM_COLUMN, IVM_VALUE_COLUMN, MinMaxKind,
    SemiAntiCondition, UnionSourceSpec, ViewSpec, WindowFunction,
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
                analyze_aggregate(aggregate, &[], tables, request)?
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
                    analyze_aggregate(aggregate, &having, tables, request)?
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
                analyze_aggregate(aggregate, &having, tables, request)?
            } else {
                analyze_row(plan, tables, request)?
            }
        }
        LogicalPlan::TableScan(_) => analyze_row(plan, tables, request)?,
        LogicalPlan::Aggregate(aggregate) => {
            analyze_aggregate(aggregate, &[], tables, request)?
        }
        LogicalPlan::Join(join) => analyze_join(join, None, tables, request)?,
        LogicalPlan::Union(union) => analyze_union(union, None, tables, request)?,
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

/// A projection (and optional filters) of one source table.
fn analyze_row(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let (source, output_columns, filter) = collect_row(plan, tables)?;
    Ok(ViewSpec::Row {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        output_columns: output_columns.unwrap_or_default(),
        filter,
    })
}

type RowParts = (IvmTable, Option<Vec<String>>, Option<String>);

fn collect_row(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
) -> Result<RowParts> {
    match peel(plan) {
        LogicalPlan::Projection(projection) => {
            let output_columns = projection
                .expr
                .iter()
                .map(column_name)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    unsupported("computed projection columns are not supported")
                })?;
            let (source, _, filter) = collect_row(&projection.input, tables)?;
            Ok((source, Some(output_columns), filter))
        }
        LogicalPlan::Filter(filter) => {
            let (source, output_columns, previous) = collect_row(&filter.input, tables)?;
            let filter =
                Some(combine_filter(previous, render_filter(&filter.predicate)?));
            Ok((source, output_columns, filter))
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
            Ok((source, output_columns, scan_filter(&scan.filters)?))
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
            if !is_plain_projection(projection) {
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
        value_column: Option<&'a str>,
        average: bool,
    },
    /// `value` for a MIN/MAX view.
    MinMax {
        kind: MinMaxKind,
        value_column: &'a str,
    },
    /// `value` for a COUNT(DISTINCT)/SUM(DISTINCT) view.
    Distinct {
        kind: DistinctAggKind,
        value_column: &'a str,
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
    let mut mapping = HashMap::new();
    for aggr in &aggregate.aggr_expr {
        let Expr::AggregateFunction(function) = aggr else {
            continue;
        };
        if let Ok(column) = having_column(function, columns) {
            // The HAVING filter refers to the aggregate by the name it had
            // before the optimizer added argument casts, so map both names.
            mapping.insert(format!("{aggr}"), column);
            mapping.insert(cast_normalized(aggr), column);
        }
    }
    let group_keys = group_keys
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut parts = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let rewritten = rewrite_having(expr, columns, &mapping, &group_keys)?;
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
    mapping: &HashMap<String, &'static str>,
    group_keys: &std::collections::HashSet<&str>,
) -> Result<Expr> {
    expr.clone()
        .transform_down(|node| match node {
            Expr::Column(column) if !group_keys.contains(column.name.as_str()) => {
                match mapping.get(&column.name) {
                    Some(mv_column) => Ok(Transformed::yes(Expr::Column(
                        Column::from_name(*mv_column),
                    ))),
                    None => Err(datafusion::error::DataFusionError::Plan(format!(
                        "HAVING over {}, which the view does not materialize",
                        column.name
                    ))),
                }
            }
            Expr::AggregateFunction(function) => {
                match having_column(&function, columns) {
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
) -> std::result::Result<&'static str, String> {
    if function.params.filter.is_some() || !function.params.order_by.is_empty() {
        return Err(format!(
            "{} with FILTER/ORDER BY is not maintained",
            function.func.name()
        ));
    }
    let name = function.func.name();
    let column_arg = |column: &str| matches!(function.params.args.as_slice(), [arg] if aggregate_column_of(arg) == Some(column));
    let counts_all = function.params.args.is_empty()
        || matches!(function.params.args.as_slice(), [Expr::Literal(..)]);
    match columns {
        HavingColumns::SumCount {
            value_column,
            average,
        } => match name {
            "avg" if !function.params.distinct => {
                if average && value_column.is_some_and(&column_arg) {
                    Ok(IVM_AVG_COLUMN)
                } else {
                    Err(not_materialized(name))
                }
            }
            "sum" if !function.params.distinct => match value_column {
                Some(value) if column_arg(value) => Ok(IVM_SUM_COLUMN),
                _ => Err(not_materialized(name)),
            },
            "count" if !function.params.distinct => {
                if counts_all {
                    Ok(IVM_COUNT_COLUMN)
                } else if value_column.is_some_and(&column_arg) {
                    Ok(IVM_NONNULL_COUNT_COLUMN)
                } else {
                    Err(not_materialized(name))
                }
            }
            _ => Err(not_materialized(name)),
        },
        HavingColumns::MinMax { kind, value_column } => {
            let expected = match kind {
                MinMaxKind::Min => "min",
                MinMaxKind::Max => "max",
            };
            if name == expected && !function.params.distinct && column_arg(value_column) {
                Ok(IVM_VALUE_COLUMN)
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::Distinct { kind, value_column } => {
            let expected = match kind {
                DistinctAggKind::Count => "count",
                DistinctAggKind::Sum => "sum",
            };
            if name == expected && function.params.distinct && column_arg(value_column) {
                Ok(IVM_VALUE_COLUMN)
            } else {
                Err(not_materialized(name))
            }
        }
    }
}

fn not_materialized(name: &str) -> String {
    format!("HAVING over {name}, which the view does not materialize")
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

/// The aggregate family: SUM/COUNT, MIN/MAX and COUNT(DISTINCT)/SUM(DISTINCT).
fn analyze_aggregate(
    aggregate: &Aggregate,
    having_exprs: &[Expr],
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let (source, filter) =
        collect_filtered_source(&aggregate.input, tables, "an aggregate")?;
    let source = &source;

    let mut group_keys = Vec::with_capacity(aggregate.group_expr.len());
    for expr in &aggregate.group_expr {
        group_keys.push(
            column_name(expr).ok_or_else(|| {
                unsupported("GROUP BY expressions must be plain columns")
            })?,
        );
    }

    let mut count = false;
    let mut sum: Option<String> = None;
    let mut avg: Option<String> = None;
    let mut min_max: Option<(MinMaxKind, String)> = None;
    let mut distinct: Option<(DistinctAggKind, String)> = None;
    for expr in &aggregate.aggr_expr {
        let Expr::AggregateFunction(function) = expr else {
            return Err(unsupported("non-aggregate expression in the select list"));
        };
        if function.params.filter.is_some() || !function.params.order_by.is_empty() {
            return Err(unsupported(format!(
                "{} with FILTER/ORDER BY",
                function.func.name()
            )));
        }
        let name = function.func.name();
        match (name, function.params.distinct) {
            ("count", true) | ("sum", true) => {
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
                distinct = Some((kind, single_column_arg(&function.params.args)?));
            }
            ("count", false) => {
                let counts_all = function.params.args.is_empty()
                    || matches!(function.params.args.as_slice(), [Expr::Literal(..)]);
                if !counts_all {
                    return Err(unsupported(
                        "COUNT(column) is not supported; use COUNT(*)",
                    ));
                }
                if min_max.is_some() || distinct.is_some() {
                    return Err(unsupported("mixing COUNT with other aggregate kinds"));
                }
                if count {
                    return Err(unsupported("duplicate COUNT aggregate"));
                }
                // SUM + COUNT(*) is the standard sum/count shape.
                count = true;
            }
            ("sum", false) => {
                if min_max.is_some() || distinct.is_some() {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                if sum.is_some() {
                    return Err(unsupported("duplicate SUM aggregate"));
                }
                let column = single_column_arg(&function.params.args)?;
                if avg.as_ref().is_some_and(|avg| avg != &column) {
                    return Err(unsupported(
                        "AVG and SUM must use the same value column",
                    ));
                }
                sum = Some(column);
            }
            ("avg", false) => {
                if min_max.is_some() || distinct.is_some() {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                if avg.is_some() {
                    return Err(unsupported("duplicate AVG aggregate"));
                }
                // The optimizer coerces the argument: `avg(CAST(v AS Float64))`.
                let column =
                    match function.params.args.as_slice() {
                        [arg] => {
                            aggregate_column_of(arg).map(str::to_string).ok_or_else(
                                || unsupported("AVG arguments must be plain columns"),
                            )?
                        }
                        _ => {
                            return Err(unsupported(
                                "aggregates take exactly one column argument",
                            ));
                        }
                    };
                if sum.as_ref().is_some_and(|sum| sum != &column) {
                    return Err(unsupported(
                        "AVG and SUM must use the same value column",
                    ));
                }
                avg = Some(column);
            }
            ("min", false) | ("max", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
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
                min_max = Some((kind, single_column_arg(&function.params.args)?));
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

    let having = if let Some((agg, value_column)) = &distinct {
        render_having(
            having_exprs,
            HavingColumns::Distinct {
                kind: *agg,
                value_column,
            },
            aggregate,
            &group_keys,
        )?
    } else if let Some((min_max, value_column)) = &min_max {
        render_having(
            having_exprs,
            HavingColumns::MinMax {
                kind: *min_max,
                value_column,
            },
            aggregate,
            &group_keys,
        )?
    } else if sum.is_some() || count || avg.is_some() {
        render_having(
            having_exprs,
            HavingColumns::SumCount {
                value_column: sum.as_deref().or(avg.as_deref()),
                average: avg.is_some(),
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
    let value_column = sum.or(avg);
    let spec = if let Some((agg, value_column)) = distinct {
        ViewSpec::DistinctAgg {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            state_table_id: request.state_table()?,
            group_keys,
            value_column,
            agg,
            filter,
            having,
        }
    } else if let Some((min_max, value_column)) = min_max {
        ViewSpec::MinMax {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            state_table_id: request.state_table()?,
            group_keys,
            value_column,
            min_max,
            filter,
            having,
        }
    } else if value_column.is_some() || count {
        ViewSpec::SumCount {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            value_column,
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
    let (source, filter) = collect_filtered_source(&window.input, tables, "a window")?;
    // Selecting the window expression itself is fine; computing on top of it
    // would be silently dropped otherwise.
    for expr in &projection.expr {
        if column_name(expr).is_none() && !window.window_expr.contains(expr) {
            return Err(unsupported(
                "computed columns above a window function are not supported",
            ));
        }
    }
    let (function, partition_keys, order_keys, value_column) =
        window_function_spec(window)?;
    Ok(ViewSpec::Window {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        partition_keys,
        order_keys,
        function,
        value_column,
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
    let (function, partition_keys, order_keys, value_column) =
        window_function_spec(window)?;
    if function != WindowFunction::RowNumber || value_column.is_some() {
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
        output_columns,
        limit,
        filter,
    }))
}

/// `(function, partition keys, order keys, aggregate column)`.
type WindowParts = (WindowFunction, Vec<String>, Vec<String>, Option<String>);

/// The single window function of a window node.
fn window_function_spec(window: &Window) -> Result<WindowParts> {
    let [expr] = window.window_expr.as_slice() else {
        return Err(unsupported("multiple window functions in one view"));
    };
    let Expr::WindowFunction(function) = expr else {
        return Err(unsupported("non-window expression in a window node"));
    };
    let params = &function.params;
    if params.filter.is_some() {
        return Err(unsupported("FILTER on a window function"));
    }
    let partition_keys = params
        .partition_by
        .iter()
        .map(column_name)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| unsupported("PARTITION BY expressions must be columns"))?;
    let order_keys = params
        .order_by
        .iter()
        .map(|sort| column_name(&sort.expr))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| unsupported("ORDER BY expressions must be columns"))?;
    let (function, value_column) = match &function.fun {
        WindowFunctionDefinition::WindowUDF(udf) => match udf.name() {
            "row_number" => (WindowFunction::RowNumber, None),
            "rank" => (WindowFunction::Rank, None),
            "dense_rank" => (WindowFunction::DenseRank, None),
            other => {
                return Err(unsupported(format!("window function {other}")));
            }
        },
        WindowFunctionDefinition::AggregateUDF(udf) => match udf.name() {
            "sum" => (WindowFunction::Sum, Some(single_column_arg(&params.args)?)),
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
                )
            }
            other => {
                return Err(unsupported(format!("window aggregate {other}")));
            }
        },
    };
    if partition_keys.is_empty() {
        return Err(unsupported("window views need PARTITION BY columns"));
    }
    if function.is_aggregate() {
        if !frame_supported(&params.window_frame, !order_keys.is_empty()) {
            return Err(unsupported(
                "window frame; only the whole partition or the SQL default running frame is maintained",
            ));
        }
    } else if order_keys.is_empty() {
        return Err(unsupported("ranking window functions need ORDER BY"));
    }
    Ok((function, partition_keys, order_keys, value_column))
}

/// The frames the runtime maintains: the whole partition (no ORDER BY) or the
/// SQL default running frame (with ORDER BY).
fn frame_supported(frame: &WindowFrame, has_order: bool) -> bool {
    if has_order {
        frame.units == WindowFrameUnits::Range
            && frame.start_bound.is_unbounded()
            && matches!(frame.end_bound, WindowFrameBound::CurrentRow)
    } else {
        frame.units == WindowFrameUnits::Rows
            && frame.start_bound.is_unbounded()
            && frame.end_bound.is_unbounded()
    }
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
    let (left, left_alias) = join_input(&join.left, tables)?;
    let (right, right_alias) = join_input(&join.right, tables)?;

    let mut join_keys = Vec::new();
    let mut conditions = Vec::new();
    for (left_expr, right_expr) in &join.on {
        let left_column = column_of(left_expr)
            .ok_or_else(|| unsupported("join keys must be plain columns"))?;
        let right_column = column_of(right_expr)
            .ok_or_else(|| unsupported("join keys must be plain columns"))?;
        if left_column.name != right_column.name {
            return Err(unsupported(format!(
                "join keys must have the same name on both sides ({} vs {})",
                left_column.name, right_column.name
            )));
        }
        join_keys.push(left_column.name.clone());
    }
    if let Some(filter) = &join.filter {
        for conjunct in split_conjunction(filter) {
            if let Some((left_column, right_column)) = equi_columns(conjunct) {
                if left_column.name != right_column.name {
                    return Err(unsupported(format!(
                        "join keys must have the same name on both sides ({} vs {})",
                        left_column.name, right_column.name
                    )));
                }
                let side = side_of(
                    left_column,
                    left_alias.as_deref(),
                    right_alias.as_deref(),
                    left,
                    right,
                )
                .or_else(|| {
                    side_of(
                        right_column,
                        left_alias.as_deref(),
                        right_alias.as_deref(),
                        left,
                        right,
                    )
                });
                if side.is_none() {
                    return Err(unsupported(
                        "join conditions between the same side are not supported",
                    ));
                }
                join_keys.push(left_column.name.clone());
            } else {
                let (left_column, right_column, op) = column_compare(
                    conjunct,
                    left_alias.as_deref(),
                    right_alias.as_deref(),
                    left,
                    right,
                )?;
                conditions.push(SemiAntiCondition {
                    left_column,
                    right_column,
                    op,
                });
            }
        }
    }
    if join_keys.is_empty() {
        return Err(unsupported("join without an equality key"));
    }
    join_keys.sort();
    join_keys.dedup();

    match join.join_type {
        JoinType::Inner => {
            if !conditions.is_empty() {
                return Err(unsupported("inner join with non-equality conditions"));
            }
            let (left_value, right_value) = join_values(
                projection,
                left_alias.as_deref(),
                right_alias.as_deref(),
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
            })
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
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
                        let on_left = side_of(
                            column,
                            left_alias.as_deref(),
                            right_alias.as_deref(),
                            left,
                            right,
                        ) == Some(Side::Left)
                            || (left.schema.field_with_name(&column.name).is_ok()
                                && right.schema.field_with_name(&column.name).is_err());
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
    for (source, _) in &sources[1..] {
        if source.schema != sources[0].0.schema {
            return Err(unsupported(
                "UNION ALL branches must have identical schemas",
            ));
        }
    }
    Ok(ViewSpec::UnionAll {
        view_id: request.view_id.clone(),
        sources: sources
            .into_iter()
            .map(|(table, filter)| UnionSourceSpec {
                table_id: table.table_id,
                filter,
            })
            .collect(),
        mv_table_id: request.mv_table_id.clone(),
    })
}

/// The source of one UNION ALL branch, with the branch filter.  The branch
/// must select every source column in order.
fn union_branch(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
) -> Result<(IvmTable, Option<String>)> {
    let (source, filter) = collect_filtered_source(plan, tables, "a UNION ALL branch")?;
    // `plan.schema()` is the branch output, `source.schema` the full table:
    // a pruned or reordered projection is rejected.
    let source_fields = source.schema.fields();
    let branch_fields = plan.schema().fields();
    if branch_fields.len() != source_fields.len()
        || branch_fields
            .iter()
            .zip(source_fields.iter())
            .any(|(left, right)| {
                left.name() != right.name() || left.data_type() != right.data_type()
            })
    {
        return Err(unsupported(
            "UNION ALL branches must select all source columns in order",
        ));
    }
    Ok((source, filter))
}

/// The table of one join input, plus its alias when it has one.
fn join_input<'a>(
    plan: &'a LogicalPlan,
    tables: &'a HashMap<String, IvmTable>,
) -> Result<(&'a IvmTable, Option<String>)> {
    if let LogicalPlan::SubqueryAlias(alias) = plan {
        let LogicalPlan::TableScan(scan) = peel(&alias.input) else {
            return Err(unsupported("join input must be a table"));
        };
        let source = resolve_table(tables, &scan.table_name)?;
        return Ok((source, Some(alias.alias.table().to_string())));
    }
    let LogicalPlan::TableScan(scan) = peel(plan) else {
        return Err(unsupported("join input must be a table"));
    };
    let source = resolve_table(tables, &scan.table_name)?;
    Ok((source, None))
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
                value_column: Some("v".to_string()),
                filter: None,
                having: None,
                average: false,
            }
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
                value_column: "v".to_string(),
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
        // SELECT DISTINCT has no view kind.
        assert!(analyze("select distinct g from src").await.is_err());
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
                    function,
                    value_column: None,
                    filter: None,
                }
            );
        }
    }

    #[tokio::test]
    async fn analyzes_aggregate_windows() {
        let analyzed = analyze("select k, sum(v) over (partition by g) from src")
            .await
            .unwrap();
        let ViewSpec::Window {
            function,
            order_keys,
            value_column,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(function, WindowFunction::Sum);
        assert_eq!(value_column, Some("v".to_string()));
        assert!(order_keys.is_empty());

        let analyzed =
            analyze("select k, count(*) over (partition by g order by v) from src")
                .await
                .unwrap();
        let ViewSpec::Window {
            function,
            order_keys,
            value_column,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(function, WindowFunction::Count);
        assert_eq!(value_column, None);
        assert_eq!(order_keys, vec!["v".to_string()]);
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
        let ViewSpec::Window { filter, .. } = analyzed.spec else {
            panic!("expected a window spec");
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
        let ViewSpec::Window { filter, .. } = analyzed.spec else {
            panic!("expected a window spec");
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
        // A frame the runtime does not maintain.
        assert!(
            analyze(
                "select k, sum(v) over (partition by g order by v rows between 1 preceding and current row) from src"
            )
            .await
            .is_err()
        );
        // Ranking needs an ordering.
        assert!(
            analyze("select k, row_number() over (partition by g) from src")
                .await
                .is_err()
        );
        // PARTITION BY is required.
        assert!(
            analyze("select k, row_number() over (order by v) from src")
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
            }
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
                    },
                    UnionSourceSpec {
                        table_id: "table_b".to_string(),
                        filter: None,
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
        // UNION ALL branches must select all source columns in order.
        let left = source_table("a");
        let right = source_table("b");
        assert!(
            analyze_multi(
                "select k from a union all select k from b",
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
