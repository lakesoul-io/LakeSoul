// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The aggregate analyzer: `SUM`/`COUNT`/`AVG`, `MIN`/`MAX`,
//! `DISTINCT` aggregates, `HAVING`, `GROUPING SETS` and the mixed-kind
//! statements.

use super::*;

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
    /// `value` for a multi-column `COUNT(DISTINCT a, b)` view.
    MultiDistinct { columns: &'a [String] },
    /// `variance_v` / `stddev_v` for a variance view.
    Variance { statistic: VarianceKind },
    /// `median_v` for a median view.
    Median,
    /// `approx_distinct_<value>` for an APPROX_DISTINCT view.
    ApproxDistinct { value: &'a AggValue },
    /// `approx_percentile_cont_<value>` for an APPROX_PERCENTILE_CONT view.
    ApproxPercentile {
        value: &'a AggValue,
        percentile: &'a str,
    },
    /// `<function>_<args>` for a computed-aggregate view.
    ComputedAgg {
        function: &'a str,
        arguments: &'a [ComputedAggArg],
        percentile: Option<&'a str>,
    },
    /// `bool_and_<value>` / `bool_or_<value>` for a BOOL_AND/BOOL_OR view.
    BoolAgg {
        kind: BoolAggKind,
        value: &'a AggValue,
    },
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
    // Hidden columns (for example the grouping-set id) may sit between the
    // group columns and the aggregates, so the aggregate outputs are located
    // from the end of the schema.
    let aggregate_offset = aggregate
        .schema
        .fields()
        .len()
        .saturating_sub(aggregate.aggr_expr.len());
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
            if let Some(field) = aggregate.schema.fields().get(aggregate_offset + index) {
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
                    } else if count_column.is_some_and(column_arg) {
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
        HavingColumns::ComputedAgg { .. } if function_filter.is_some() => {
            Err(not_materialized(name))
        }
        HavingColumns::ComputedAgg {
            function: expected_function,
            arguments,
            percentile,
        } => {
            let argument_matches = function.params.args.len()
                == arguments.len() + usize::from(percentile.is_some())
                && arguments
                    .iter()
                    .zip(&function.params.args)
                    .all(|(expected, arg)| computed_arg_matches(expected, arg, hoisted));
            let percentile_matches = percentile.is_none_or(|expected| {
                function.params.args.last().is_some_and(|literal| {
                    render_filter(literal).is_ok_and(|rendered| rendered == expected)
                })
            });
            if name == expected_function
                && !function.params.distinct
                && argument_matches
                && percentile_matches
            {
                Ok(format!(
                    "{name}_{}",
                    arguments
                        .iter()
                        .map(ComputedAggArg::label)
                        .collect::<Vec<_>>()
                        .join("_")
                ))
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::ApproxPercentile { .. } if function_filter.is_some() => {
            Err(not_materialized(name))
        }
        HavingColumns::ApproxPercentile { value, percentile } => {
            // The aggregate takes two arguments, so compare the value and the
            // percentile separately.
            let first_matches = function.params.args.first().is_some_and(|arg| {
                value_argument(arg, hoisted, true)
                    .map(|parsed| parsed == *value)
                    .unwrap_or(false)
            });
            let percentile_matches = function.params.args.get(1).is_some_and(|p| {
                render_filter(p).is_ok_and(|rendered| rendered == *percentile)
            });
            let matches = name == "approx_percentile_cont"
                && !function.params.distinct
                && function.params.args.len() == 2
                && first_matches
                && percentile_matches;
            if matches {
                Ok(approx_percentile_output_column(match value {
                    AggValue::Column(column) => Some(column.as_str()),
                    AggValue::Expr(_) => None,
                }))
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::ApproxDistinct { .. } if function_filter.is_some() => {
            Err(not_materialized(name))
        }
        HavingColumns::ApproxDistinct { value } => {
            if name == "approx_distinct"
                && !function.params.distinct
                && function.params.args.len() == 1
                && value_matches(value, false)
            {
                Ok(approx_distinct_output_column(match value {
                    AggValue::Column(column) => Some(column.as_str()),
                    AggValue::Expr(_) => None,
                }))
            } else {
                Err(not_materialized(name))
            }
        }
        HavingColumns::BoolAgg { .. } if function_filter.is_some() => {
            Err(not_materialized(name))
        }
        HavingColumns::BoolAgg { kind, value } => {
            if name == kind.sql_name()
                && !function.params.distinct
                && function.params.args.len() == 1
                && value_matches(value, false)
            {
                Ok(bool_agg_output_column(
                    kind,
                    match value {
                        AggValue::Column(column) => Some(column.as_str()),
                        AggValue::Expr(_) => None,
                    },
                ))
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
        HavingColumns::MultiDistinct { columns } => {
            let matches = name == "count"
                && function.params.distinct
                && function.params.args.len() == columns.len()
                && function
                    .params
                    .args
                    .iter()
                    .zip(columns)
                    .all(|(arg, column)| {
                        aggregate_column_of(arg) == Some(column.as_str())
                    });
            if matches {
                Ok(IVM_VALUE_COLUMN.to_string())
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

/// The flat key index of a `GROUPING(key)` projection column: the optimizer
/// rewrites it to `CAST(__grouping_id & <single bit> AS Int32)`.
fn grouping_bit(expr: &Expr, keys: usize) -> Option<usize> {
    let Expr::Cast(cast) = expr else {
        return None;
    };
    let Expr::BinaryExpr(binary) = cast.expr.as_ref() else {
        return None;
    };
    if binary.op != Operator::BitwiseAnd {
        return None;
    }
    let (mask, column) = match (&*binary.left, &*binary.right) {
        (Expr::Literal(mask, _), Expr::Column(column)) => (mask, column),
        (Expr::Column(column), Expr::Literal(mask, _)) => (mask, column),
        _ => return None,
    };
    if column.name != "__grouping_id" {
        return None;
    }
    let mask = match mask {
        ScalarValue::UInt8(Some(mask)) => u64::from(*mask),
        ScalarValue::UInt16(Some(mask)) => u64::from(*mask),
        ScalarValue::UInt32(Some(mask)) => u64::from(*mask),
        ScalarValue::UInt64(Some(mask)) => *mask,
        _ => return None,
    };
    if mask.count_ones() != 1 {
        return None;
    }
    let bit = mask.trailing_zeros() as usize;
    (bit < keys).then_some(bit)
}

/// Whether every non-plain projection column is a `GROUPING()` rewrite (an
/// expression over the hidden grouping id).
pub(super) fn is_grouping_projection(projection: &Projection) -> bool {
    projection.expr.iter().all(|expr| {
        column_of(expr).is_some() || expr_mentions_grouping_id(strip_alias(expr))
    })
}

/// Whether an expression references the hidden grouping id.
fn expr_mentions_grouping_id(expr: &Expr) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node
            && column.name == "__grouping_id"
        {
            found = true;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// The shape of a deterministic scalar aggregate maintained by the computed
/// aggregate family: `(arguments, takes a percentile literal, result type)`.
fn computed_agg_shape(name: &str) -> Option<(usize, bool, ComputedAggResult)> {
    Some(match name {
        "bit_and" | "bit_or" | "bit_xor" => (1, false, ComputedAggResult::Value),
        "corr" | "covar_samp" | "covar_pop" => (2, false, ComputedAggResult::Float64),
        "regr_slope" | "regr_intercept" | "regr_r2" | "regr_avgx" | "regr_avgy"
        | "regr_sxx" | "regr_syy" | "regr_sxy" => (2, false, ComputedAggResult::Float64),
        "regr_count" => (2, false, ComputedAggResult::UInt64),
        "percentile_cont" => (1, true, ComputedAggResult::Float64),
        "approx_median" => (1, false, ComputedAggResult::Float64),
        "approx_percentile_cont_with_weight" => (2, true, ComputedAggResult::Float64),
        _ => return None,
    })
}

/// Whether an aggregate argument matches a stored computed-aggregate argument.
fn computed_arg_matches(
    expected: &ComputedAggArg,
    arg: &Expr,
    hoisted: &HashMap<String, Expr>,
) -> bool {
    match sum_value_argument(arg, hoisted) {
        Ok(AggValue::Column(column)) => {
            expected.column.as_deref() == Some(column.as_str()) && expected.expr.is_none()
        }
        Ok(AggValue::Expr(expression)) => {
            expected.expr.as_deref() == Some(expression.as_str())
                && expected.column.is_none()
        }
        Err(_) => false,
    }
}

/// `GROUP BY GROUPING SETS` / `ROLLUP` / `CUBE` over a keyed source with the
/// `SUM`/`COUNT`/`AVG` aggregates.
///
/// The flat key columns are the distinct grouping columns in first-seen
/// order; every grouping set is recorded as indices into them, and the view
/// materializes one row per (set index, key tuple) with NULLs for the keys a
/// set does not group by.
fn analyze_grouping_sets(
    aggregate: &Aggregate,
    projection: Option<&Projection>,
    having_exprs: &[Expr],
    source: &IvmTable,
    filter: Option<String>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    if source.primary_keys.is_empty() {
        return Err(unsupported(
            "GROUPING SETS / ROLLUP / CUBE need a keyed source",
        ));
    }
    let mut grouping = None;
    for expr in &aggregate.group_expr {
        match strip_alias(expr) {
            Expr::GroupingSet(sets) => {
                if grouping.replace(sets.clone()).is_some() {
                    return Err(unsupported(
                        "several GROUPING SETS expressions in one GROUP BY",
                    ));
                }
            }
            _ => {
                return Err(unsupported(
                    "GROUP BY mixing plain keys with GROUPING SETS / ROLLUP / CUBE",
                ));
            }
        }
    }
    let Some(grouping) = grouping else {
        return Err(unsupported("GROUP BY without a grouping set"));
    };
    let member_sets: Vec<Vec<Expr>> = match &grouping {
        GroupingSet::Rollup(exprs) => (0..=exprs.len())
            .rev()
            .map(|length| exprs[..length].to_vec())
            .collect(),
        GroupingSet::Cube(exprs) => {
            let mut sets = Vec::new();
            for mask in (0..1u64 << exprs.len()).rev() {
                let mut set = Vec::new();
                for (index, expr) in exprs.iter().enumerate() {
                    if mask & (1 << index) != 0 {
                        set.push(expr.clone());
                    }
                }
                sets.push(set);
            }
            sets
        }
        GroupingSet::GroupingSets(sets) => sets.clone(),
    };
    // The flat keys, in first-seen order.
    let mut group_keys: Vec<String> = Vec::new();
    let mut groupings: Vec<Vec<usize>> = Vec::new();
    for set in &member_sets {
        let mut indices = Vec::with_capacity(set.len());
        for expr in set {
            let Expr::Column(column) = strip_alias(expr) else {
                return Err(unsupported("GROUPING SETS keys must be plain columns"));
            };
            let index = match group_keys.iter().position(|key| key == &column.name) {
                Some(index) => index,
                None => {
                    group_keys.push(column.name.clone());
                    group_keys.len() - 1
                }
            };
            if !indices.contains(&index) {
                indices.push(index);
            }
        }
        indices.sort_unstable();
        groupings.push(indices);
    }

    let hoisted = hoisted_expressions(&aggregate.input);
    let mut value: Option<AggValue> = None;
    let mut count = false;
    let mut count_column: Option<String> = None;
    let mut average = false;
    for expr in &aggregate.aggr_expr {
        let mut inner = expr;
        while let Expr::Alias(alias) = inner {
            inner = &alias.expr;
        }
        let Expr::AggregateFunction(function) = inner else {
            return Err(unsupported("non-aggregate expression in the select list"));
        };
        let name = function.func.name();
        if function.params.distinct {
            return Err(unsupported(format!(
                "{name}(DISTINCT ...) with GROUPING SETS"
            )));
        }
        if function.params.filter.is_some() {
            return Err(unsupported("FILTER with GROUPING SETS is not maintained"));
        }
        if !function.params.order_by.is_empty() {
            return Err(unsupported(format!("{name} with ORDER BY")));
        }
        match name {
            "count" => {
                let counts_all = function.params.args.is_empty()
                    || matches!(function.params.args.as_slice(), [Expr::Literal(..)]);
                if counts_all {
                    if count {
                        return Err(unsupported("duplicate COUNT aggregate"));
                    }
                    count = true;
                } else {
                    let [arg] = function.params.args.as_slice() else {
                        return Err(unsupported("COUNT takes one argument"));
                    };
                    let column = match sum_value_argument(arg, &hoisted)? {
                        AggValue::Column(column) => column,
                        AggValue::Expr(_) => {
                            return Err(unsupported(
                                "COUNT over an expression with GROUPING SETS",
                            ));
                        }
                    };
                    if count_column
                        .as_ref()
                        .is_some_and(|existing| existing != &column)
                    {
                        return Err(unsupported("duplicate COUNT aggregate"));
                    }
                    count_column = Some(column);
                }
            }
            "sum" | "avg" => {
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported(format!("{name} takes one argument")));
                };
                let candidate = sum_value_argument(arg, &hoisted)?;
                if let Some(existing) = &value
                    && existing != &candidate
                {
                    return Err(unsupported("SUM/AVG over different arguments"));
                }
                value = Some(candidate);
                if name == "avg" {
                    average = true;
                }
            }
            other => {
                return Err(unsupported(format!(
                    "aggregate function {other} with GROUPING SETS (only SUM/COUNT/AVG)"
                )));
            }
        }
    }
    if value.is_none() && !count && count_column.is_none() {
        return Err(unsupported("GROUPING SETS needs SUM/COUNT/AVG"));
    }
    let having = render_having(
        having_exprs,
        HavingColumns::SumCount {
            value: value.as_ref(),
            count_column: count_column.as_deref(),
            average,
            aggregate_filter: None,
        },
        aggregate,
        &group_keys,
    )?;
    let (value_column, value_expr) = match value {
        Some(AggValue::Column(column)) => (Some(column), None),
        Some(AggValue::Expr(expression)) => (None, Some(expression)),
        None => (None, None),
    };
    // `GROUPING(key)` is rewritten to a projection over the hidden
    // `__grouping_id`; materialize the supported single-key shape per set.
    let mut grouping_columns = Vec::new();
    if let Some(projection) = projection {
        for expr in &projection.expr {
            let inner = strip_alias(expr);
            let Some(key) = grouping_bit(inner, group_keys.len()) else {
                if expr_mentions_grouping_id(inner) {
                    return Err(unsupported(
                        "only a single-key GROUPING(key) column is maintained",
                    ));
                }
                continue;
            };
            // The optimizer auto-names an unaliased `GROUPING()` column after
            // the SQL expression; fall back to a plain derived name then.
            let name = match expr {
                Expr::Alias(alias)
                    if alias.name.chars().all(|character| {
                        character.is_alphanumeric() || character == '_'
                    }) =>
                {
                    alias.name.clone()
                }
                _ => format!("grouping_{}", group_keys[key]),
            };
            if grouping_columns
                .iter()
                .any(|column: &GroupingColumn| column.name == name)
            {
                return Err(unsupported("duplicate GROUPING() column"));
            }
            grouping_columns.push(GroupingColumn { key, name });
        }
    }
    Ok(ViewSpec::GroupingSets {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        group_keys,
        group_exprs: Vec::new(),
        grouping_columns,
        groupings,
        value_column,
        value_expr,
        count_column,
        aggregate_filter: None,
        average,
        filter,
        having,
    })
}

#[allow(clippy::type_complexity)]
pub(super) fn analyze_aggregate(
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
    // A DISTINCT aggregate beside another aggregate optimizes into a nested
    // grouping the general path does not model; reject it explicitly (a lone
    // DISTINCT aggregate keeps its dedicated view).
    if aggregate.aggr_expr.len() > 1
        && aggregate.aggr_expr.iter().any(|expr| {
            matches!(
                strip_alias(expr),
                Expr::AggregateFunction(function) if function.params.distinct
            )
        })
    {
        return Err(unsupported(
            "a mixed statement does not maintain DISTINCT aggregates",
        ));
    }
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
    // `GROUP BY GROUPING SETS`/`ROLLUP`/`CUBE` has its own maintenance.
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(strip_alias(expr), Expr::GroupingSet(_)))
    {
        return analyze_grouping_sets(
            aggregate,
            projection,
            having_exprs,
            source,
            filter,
            request,
        );
    }

    let mut group_keys = Vec::with_capacity(aggregate.group_expr.len());
    let mut group_exprs = Vec::with_capacity(aggregate.group_expr.len());
    for expr in &aggregate.group_expr {
        // The select alias names the materialized key column.
        let alias = projection.and_then(|projection| projection_alias(projection, expr));
        let mut inner = expr;
        while let Expr::Alias(nested) = inner {
            inner = &nested.expr;
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

    // Mixed aggregate kinds (e.g. `SUM(v), MIN(v)`) are recomputed with the
    // general multi-aggregate view; a SUM/COUNT/AVG-only mix keeps the
    // incremental sum/count view.
    let selected = selected_aggregates(aggregate, projection);
    let first_value = selected.iter().any(|expr| is_first_value_aggregate(expr));
    if first_value || (selected.len() > 1 && !single_family_shapes(&selected)) {
        return analyze_multi_agg(
            aggregate,
            group_keys,
            group_exprs,
            having_exprs,
            source,
            filter,
            request,
        );
    }

    let mut count = false;
    let mut count_column: Option<String> = None;
    let mut sum: Option<AggValue> = None;
    let mut avg: Option<AggValue> = None;
    let mut variance: Option<(VarianceKind, AggValue)> = None;
    let mut bool_agg: Option<(BoolAggKind, AggValue)> = None;
    let mut approx_distinct: Option<AggValue> = None;
    let mut approx_percentile: Option<(AggValue, String)> = None;
    let mut computed_agg: Option<(
        String,
        Vec<ComputedAggArg>,
        Option<String>,
        ComputedAggResult,
        String,
    )> = None;
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
            ("approx_percentile_cont", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                    || string_agg.is_some()
                    || array_agg.is_some()
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                let [arg, percentile] = function.params.args.as_slice() else {
                    return Err(unsupported(
                        "approx_percentile_cont takes a value and a percentile",
                    ));
                };
                if !matches!(percentile, Expr::Literal(..)) {
                    return Err(unsupported(
                        "APPROX_PERCENTILE_CONT needs a literal percentile",
                    ));
                }
                // The planner coerces the value to Float64; drop that cast like
                // SUM/AVG so a plain column stays a column.
                let value = sum_value_argument(arg, &hoisted_exprs)?;
                approx_percentile = Some((value, render_filter(percentile)?));
                aggregate_filters.push((name, function_filter, false));
            }
            ("approx_distinct", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                    || string_agg.is_some()
                    || array_agg.is_some()
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported(
                        "approx_distinct takes exactly one argument",
                    ));
                };
                let value = value_argument(arg, &hoisted_exprs, false)?;
                approx_distinct = Some(value);
                aggregate_filters.push((name, function_filter, false));
            }
            ("bool_and", false) | ("bool_or", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                    || string_agg.is_some()
                    || array_agg.is_some()
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                let [arg] = function.params.args.as_slice() else {
                    return Err(unsupported(format!(
                        "{name} takes exactly one argument"
                    )));
                };
                let value = value_argument(arg, &hoisted_exprs, false)?;
                let kind = if name == "bool_and" {
                    BoolAggKind::BoolAnd
                } else {
                    BoolAggKind::BoolOr
                };
                bool_agg = Some((kind, value));
                aggregate_filters.push((name, function_filter, false));
            }
            ("median", false) => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
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
            (name, false) if computed_agg_shape(name).is_some() => {
                if count
                    || sum.is_some()
                    || avg.is_some()
                    || variance.is_some()
                    || median.is_some()
                    || min_max.is_some()
                    || distinct.is_some()
                    || string_agg.is_some()
                    || array_agg.is_some()
                    || bool_agg.is_some()
                    || approx_distinct.is_some()
                    || approx_percentile.is_some()
                    || computed_agg.is_some()
                {
                    return Err(unsupported("mixing aggregate kinds"));
                }
                let (arity, percentile_literal, result) =
                    computed_agg_shape(name).expect("checked above");
                let expected = arity + usize::from(percentile_literal);
                if function.params.args.len() != expected {
                    return Err(unsupported(format!(
                        "{name} takes {expected} argument(s)"
                    )));
                }
                let mut arguments = Vec::with_capacity(arity);
                for arg in &function.params.args[..arity] {
                    let value = sum_value_argument(arg, &hoisted_exprs)?;
                    arguments.push(match value {
                        AggValue::Column(column) => ComputedAggArg {
                            column: Some(column),
                            expr: None,
                        },
                        AggValue::Expr(expression) => ComputedAggArg {
                            column: None,
                            expr: Some(expression),
                        },
                    });
                }
                let percentile = if percentile_literal {
                    let literal = &function.params.args[arity];
                    if !matches!(literal, Expr::Literal(..)) {
                        return Err(unsupported(format!(
                            "{name} needs a literal percentile"
                        )));
                    }
                    Some(render_filter(literal)?)
                } else {
                    None
                };
                let column = format!(
                    "{name}_{}",
                    arguments
                        .iter()
                        .map(ComputedAggArg::label)
                        .collect::<Vec<_>>()
                        .join("_")
                );
                computed_agg =
                    Some((name.to_string(), arguments, percentile, result, column));
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
            || bool_agg.is_some()
            || approx_distinct.is_some()
            || approx_percentile.is_some()
            || computed_agg.is_some()
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
            render_having(
                having_exprs,
                HavingColumns::MultiDistinct {
                    columns: value_columns,
                },
                aggregate,
                &group_keys,
            )?
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
    } else if let Some((function, arguments, percentile, _, _)) = &computed_agg {
        render_having(
            having_exprs,
            HavingColumns::ComputedAgg {
                function,
                arguments,
                percentile: percentile.as_deref(),
            },
            aggregate,
            &group_keys,
        )?
    } else if let Some((value, percentile)) = &approx_percentile {
        render_having(
            having_exprs,
            HavingColumns::ApproxPercentile { value, percentile },
            aggregate,
            &group_keys,
        )?
    } else if let Some(value) = &approx_distinct {
        render_having(
            having_exprs,
            HavingColumns::ApproxDistinct { value },
            aggregate,
            &group_keys,
        )?
    } else if let Some((kind, value)) = &bool_agg {
        render_having(
            having_exprs,
            HavingColumns::BoolAgg { kind: *kind, value },
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
    } else if let Some((function, arguments, percentile, result, column)) = computed_agg {
        ViewSpec::ComputedAgg {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            function,
            arguments,
            percentile,
            column,
            result,
            filter,
            having,
        }
    } else if let Some((value, percentile)) = approx_percentile {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::ApproxPercentile {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            percentile,
            filter,
            having,
        }
    } else if let Some(value) = approx_distinct {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::ApproxDistinct {
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
    } else if let Some((kind, value)) = bool_agg {
        let (value_column, value_expr) = match value {
            AggValue::Column(column) => (Some(column), None),
            AggValue::Expr(expression) => (None, Some(expression)),
        };
        ViewSpec::BoolAgg {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            group_exprs,
            value_column,
            value_expr,
            bool_agg: kind,
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

/// The aggregate expressions the select list references (the planner may add
/// `HAVING`-only aggregates beside them).
fn selected_aggregates<'a>(
    aggregate: &'a Aggregate,
    projection: Option<&Projection>,
) -> Vec<&'a Expr> {
    // The optimizer drops the projection when the pruned aggregate output is
    // exactly the select list: every aggregate is selected then.
    let Some(projection) = projection else {
        return aggregate.aggr_expr.iter().collect();
    };
    let mut referenced_columns = Vec::new();
    let mut referenced_exprs = Vec::new();
    for expr in &projection.expr {
        let inner = strip_alias(expr);
        if let Some(column) = column_of(inner) {
            referenced_columns.push(column.name.clone());
        }
        referenced_exprs.push(format!("{inner}"));
    }
    let offset = aggregate
        .schema
        .fields()
        .len()
        .saturating_sub(aggregate.aggr_expr.len());
    aggregate
        .aggr_expr
        .iter()
        .enumerate()
        .filter(|(index, expr)| {
            let inner = strip_alias(expr);
            aggregate
                .schema
                .fields()
                .get(offset + index)
                .is_some_and(|field| referenced_columns.contains(field.name()))
                || referenced_exprs.contains(&format!("{inner}"))
        })
        .map(|(_, expr)| expr)
        .collect()
}

/// Whether every aggregate fits the incremental `SUM`/`COUNT`/`AVG` view
/// (at most one of each).
fn single_family_shapes(aggregates: &[&Expr]) -> bool {
    let mut sum = 0usize;
    let mut count = 0usize;
    let mut avg = 0usize;
    for expr in aggregates {
        let Expr::AggregateFunction(function) = strip_alias(expr) else {
            return false;
        };
        if function.params.distinct {
            return false;
        }
        match function.func.name() {
            "sum" => sum += 1,
            "count" => count += 1,
            "avg" => avg += 1,
            _ => return false,
        }
    }
    sum <= 1 && count <= 1 && avg <= 1
}

/// The aggregate functions a mixed-statement view can recompute.
fn multi_agg_supported(name: &str) -> bool {
    matches!(
        name,
        "sum"
            | "count"
            | "avg"
            | "min"
            | "max"
            | "var_samp"
            | "var_pop"
            | "stddev"
            | "stddev_samp"
            | "stddev_pop"
            | "median"
            | "approx_median"
            | "string_agg"
            | "array_agg"
            | "bool_and"
            | "bool_or"
            | "approx_distinct"
            | "approx_percentile_cont"
            | "percentile_cont"
            | "approx_percentile_cont_with_weight"
            | "first_value"
    ) || computed_agg_shape(name).is_some()
}

/// Whether an expression is a `FIRST_VALUE` aggregate (`DISTINCT ON` plans
/// pick the first row per group with it).
pub(super) fn is_first_value_aggregate(expr: &Expr) -> bool {
    matches!(
        strip_alias(expr),
        Expr::AggregateFunction(function) if function.func.name() == "first_value"
    )
}

/// Render `FIRST_VALUE(arg)` with the source primary keys appended to its
/// ordering, so the picked row is deterministic for a keyed source.
fn render_first_value_call(
    function: &AggregateFunction,
    source: &IvmTable,
) -> Result<String> {
    if function.params.args.len() != 1 {
        return Err(unsupported("FIRST_VALUE needs a single argument"));
    }
    if source.primary_keys.is_empty() {
        return Err(unsupported(
            "FIRST_VALUE needs a source with a primary key so the picked row is deterministic",
        ));
    }
    let argument = render_filter(&function.params.args[0])?;
    let mut order = render_order_items(&function.params.order_by)?;
    // The primary keys break ties, like the runtime appends them to a TOP-K
    // ordering.
    for key in &source.primary_keys {
        let item = render_order_items(&[datafusion::logical_expr::SortExpr::new(
            Expr::Column(Column::from_name(key.clone())),
            true,
            false,
        )])?
        .into_iter()
        .next()
        .ok_or_else(|| unsupported("empty ordering"))?;
        if !order.contains(&item) {
            order.push(item);
        }
    }
    Ok(format!(
        "first_value({argument} order by {})",
        order.join(", ")
    ))
}

/// Render an aggregate expression as executable SQL over the source columns.
fn render_aggregate_call(expr: &Expr) -> Result<String> {
    let stripped = strip_relations(expr.clone())?;
    let ast = Unparser::default()
        .expr_to_sql(&stripped)
        .map_err(|error| unsupported(format!("aggregate {error}")))?;
    Ok(ast.to_string())
}

/// The name fragment an aggregate argument contributes to the MV column.
fn aggregate_arg_label(expr: &Expr) -> String {
    match strip_alias(expr) {
        Expr::Column(column) => column.name.clone(),
        Expr::Cast(cast) => aggregate_arg_label(&cast.expr),
        _ => "value".to_string(),
    }
}

/// Render `HAVING` over a mixed-aggregate view: every aggregate reference is
/// replaced by its materialized column.
///
/// The optimizer names an aggregate's output column after its expression
/// (`sum(src.v)`), and a filter below the projection carries the aggregate
/// itself, so both shapes are mapped.
fn render_multi_agg_having(
    having_exprs: &[Expr],
    aggregate: &Aggregate,
    group_keys: &[String],
    aggregates: &[(String, String, DataType)],
) -> Result<Option<String>> {
    if having_exprs.is_empty() {
        return Ok(None);
    }
    let mut group_aliases = HashMap::new();
    for (index, key) in group_keys.iter().enumerate() {
        if let Some(field) = aggregate.schema.fields().get(index)
            && field.name() != key
        {
            group_aliases.insert(field.name().clone(), key.clone());
        }
    }
    let group_set = group_keys
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let offset = aggregate
        .schema
        .fields()
        .len()
        .saturating_sub(aggregate.aggr_expr.len());
    let mut mapping: HashMap<String, String> = HashMap::new();
    for (index, ((call, column, _), expr)) in
        aggregates.iter().zip(&aggregate.aggr_expr).enumerate()
    {
        if let Some(field) = aggregate.schema.fields().get(offset + index) {
            mapping.insert(field.name().clone(), column.clone());
        }
        let mut inner = expr;
        while let Expr::Alias(alias) = inner {
            inner = &alias.expr;
        }
        mapping.insert(format!("{inner}"), column.clone());
        mapping.insert(cast_normalized(inner), column.clone());
        mapping.insert(call.clone(), column.clone());
    }
    let mut parts = Vec::with_capacity(having_exprs.len());
    for expr in having_exprs {
        let rewritten = expr
            .clone()
            .transform_down(|node| match node {
                Expr::Column(column) if !group_set.contains(column.name.as_str()) => {
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
                    let rendered =
                        match render_aggregate_call(&Expr::AggregateFunction(function)) {
                            Ok(rendered) => rendered,
                            Err(error) => {
                                return Err(datafusion::error::DataFusionError::Plan(
                                    format!("HAVING {error}"),
                                ));
                            }
                        };
                    match mapping.get(&rendered) {
                        Some(mv_column) => Ok(Transformed::yes(Expr::Column(
                            Column::from_name(mv_column.clone()),
                        ))),
                        None => Err(datafusion::error::DataFusionError::Plan(format!(
                            "HAVING over {rendered}, which the view does not materialize"
                        ))),
                    }
                }
                other => Ok(Transformed::no(other)),
            })
            .map(|transformed| transformed.data)
            .map_err(|error| unsupported(format!("HAVING {error}")))?;
        parts.push(render_filter(&rewritten)?);
    }
    Ok(Some(parts.join(" AND ")))
}

/// An aggregate view over one source with any mix of supported aggregate
/// functions in a single statement (recomputed from the affected groups).
#[allow(clippy::too_many_arguments)]
fn analyze_multi_agg(
    aggregate: &Aggregate,
    group_keys: Vec<String>,
    group_exprs: Vec<String>,
    having_exprs: &[Expr],
    source: &IvmTable,
    filter: Option<String>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let mut aggregates: Vec<(String, String, DataType)> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
        let inner = strip_alias(expr);
        let Expr::AggregateFunction(function) = inner else {
            return Err(unsupported("non-aggregate expression in the select list"));
        };
        let name = function.func.name();
        if function.params.filter.is_some() {
            return Err(unsupported(format!(
                "aggregate FILTER over {name} is not maintained in a mixed statement"
            )));
        }
        if !function.params.order_by.is_empty()
            && !matches!(name, "string_agg" | "array_agg" | "first_value")
        {
            return Err(unsupported(
                "an aggregate ordering is only supported inside STRING_AGG, ARRAY_AGG and FIRST_VALUE",
            ));
        }
        if !multi_agg_supported(name) {
            return Err(unsupported(format!("aggregate function {name}")));
        }
        // A `FIRST_VALUE` over a group key is the key itself.
        if name == "first_value"
            && function.params.args.len() == 1
            && column_of(&function.params.args[0])
                .is_some_and(|column| group_keys.contains(&column.name))
        {
            continue;
        }
        let call = if name == "first_value" {
            render_first_value_call(function, source)?
        } else {
            render_aggregate_call(inner)?
        };
        if function.params.distinct
            && (!matches!(name, "count" | "sum") || function.params.args.len() != 1)
        {
            return Err(unsupported(
                "a mixed statement only maintains DISTINCT over a single argument",
            ));
        }
        if name == "count" && function.params.args.len() > 1 {
            return Err(unsupported("COUNT(...) needs a single argument"));
        }
        let column = if name == "count"
            && !function.params.distinct
            && matches!(call.as_str(), "count(1)" | "count(*)")
        {
            "count".to_string()
        } else {
            let labels = function
                .params
                .args
                .iter()
                .map(aggregate_arg_label)
                .collect::<Vec<_>>()
                .join("_");
            if labels.is_empty() {
                name.to_string()
            } else {
                format!("{name}_{labels}")
            }
        };
        if names.contains(&column) {
            return Err(unsupported(format!(
                "aggregate column {column} is materialized twice; use distinct arguments"
            )));
        }
        names.push(column.clone());
        let data_type = aggregate
            .schema
            .field(aggregate.group_expr.len() + index)
            .data_type()
            .clone();
        encode_data_type(&data_type)?;
        aggregates.push((call, column, data_type));
    }
    if aggregates.is_empty() {
        return Err(unsupported(
            "DISTINCT ON needs at least one non-key column; use SELECT DISTINCT for plain key deduplication",
        ));
    }
    let having =
        render_multi_agg_having(having_exprs, aggregate, &group_keys, &aggregates)?;
    Ok(ViewSpec::MultiAgg {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        group_keys,
        group_exprs,
        aggregates: aggregates
            .iter()
            .map(|(call, column, data_type)| {
                Ok(MultiAggSpec {
                    call: call.clone(),
                    column: column.clone(),
                    result: encode_data_type(data_type)?,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        filter,
        having,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::test_helpers::*;

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

        // The aggregate ordering is required for ARRAY_AGG; mixing with
        // another kind recomputes through the multi-aggregate view.
        assert!(
            analyze("select k, array_agg(g) from src group by k")
                .await
                .is_err()
        );
        let mixed =
            analyze("select k, array_agg(g order by v), sum(v) from src group by k")
                .await
                .unwrap();
        assert!(matches!(mixed.spec, ViewSpec::MultiAgg { .. }));
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
        let mixed = analyze(
            "select k, string_agg(g, ',' order by v), sum(v) from src group by k",
        )
        .await
        .unwrap();
        assert!(matches!(mixed.spec, ViewSpec::MultiAgg { .. }));
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

        // HAVING over the multi-column count maps to the `value` column.
        let analyzed = analyze_optimized(
            "select g, count(distinct v, k) from src group by g \
             having count(distinct v, k) > 1",
        )
        .await
        .unwrap();
        let ViewSpec::DistinctAgg { having, .. } = analyzed.spec else {
            panic!("expected a distinct spec");
        };
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("value > 1"));

        // A multi-column distinct count cannot carry FILTER.
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
        // AVG mixes with MIN/MAX through the multi-aggregate view and rejects
        // DISTINCT beside a plain aggregate.
        let mixed = analyze("select g, avg(v), min(v) from src group by g")
            .await
            .unwrap();
        assert!(matches!(mixed.spec, ViewSpec::MultiAgg { .. }));
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
    async fn analyzes_distinct_on() {
        // `DISTINCT ON (...) ... ORDER BY` plans as FIRST_VALUE aggregates;
        // the source primary keys are appended to the ordering so the picked
        // row is deterministic.
        let analyzed =
            analyze_optimized("select distinct on (g) g, v from src order by g, v desc")
                .await
                .unwrap();
        let ViewSpec::MultiAgg {
            group_keys,
            aggregates,
            ..
        } = analyzed.spec
        else {
            panic!("expected a multi-aggregate spec");
        };
        assert_eq!(group_keys, vec!["g".to_string()]);
        assert_eq!(aggregates.len(), 1);
        assert_eq!(aggregates[0].column, "first_value_v");
        assert!(
            aggregates[0].call.starts_with("first_value(v order by "),
            "{}",
            aggregates[0].call
        );
        assert!(
            aggregates[0].call.ends_with(", k)"),
            "{}",
            aggregates[0].call
        );

        // Without an ORDER BY the primary key ordering picks the row.
        let analyzed = analyze_optimized("select distinct on (g) g, v from src")
            .await
            .unwrap();
        let ViewSpec::MultiAgg { aggregates, .. } = analyzed.spec else {
            panic!("expected a multi-aggregate spec");
        };
        assert_eq!(aggregates[0].call, "first_value(v order by k)");

        // Several picked columns, none of them the group key.
        let analyzed =
            analyze_optimized("select distinct on (g) g, k, v from src order by g, v")
                .await
                .unwrap();
        let ViewSpec::MultiAgg { aggregates, .. } = analyzed.spec else {
            panic!("expected a multi-aggregate spec");
        };
        assert_eq!(
            aggregates
                .iter()
                .map(|aggregate| aggregate.column.clone())
                .collect::<Vec<_>>(),
            vec!["first_value_k".to_string(), "first_value_v".to_string(),]
        );

        // A key-only DISTINCT ON has nothing to pick.
        assert!(
            analyze_optimized("select distinct on (k) k from src")
                .await
                .is_err()
        );

        // An append-only source has no deterministic tie-break.
        let mut append_only = source_table("dim");
        append_only.primary_keys = Vec::new();
        assert!(
            analyze_multi(
                "select distinct on (k) k, v from dim order by k, v",
                vec![append_only],
            )
            .await
            .is_err()
        );

        // A plain ORDER BY above an aggregate stays rejected.
        assert!(
            analyze_optimized("select g, sum(v) from src group by g order by g")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_mixed_aggregates() {
        // Mixed aggregate kinds recompute through the multi-aggregate view.
        let analyzed = analyze_optimized(
            "select g, sum(v), min(v), count(*) from src group by g \
             having sum(v) > 1 and min(v) < 9",
        )
        .await
        .unwrap();
        let ViewSpec::MultiAgg {
            group_keys,
            aggregates,
            having,
            ..
        } = analyzed.spec
        else {
            panic!("expected a multi-aggregate spec");
        };
        assert_eq!(group_keys, vec!["g".to_string()]);
        assert_eq!(
            aggregates
                .iter()
                .map(|aggregate| aggregate.column.clone())
                .collect::<Vec<_>>(),
            vec![
                "sum_v".to_string(),
                "min_v".to_string(),
                "count".to_string(),
            ]
        );
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("sum_v > 1 AND min_v < 9")
        );

        // MIN, MAX and MEDIAN together.
        let analyzed =
            analyze_optimized("select g, min(v), max(v), median(v) from src group by g")
                .await
                .unwrap();
        let ViewSpec::MultiAgg { aggregates, .. } = analyzed.spec else {
            panic!("expected a multi-aggregate spec");
        };
        assert_eq!(
            aggregates
                .iter()
                .map(|aggregate| aggregate.column.clone())
                .collect::<Vec<_>>(),
            vec![
                "min_v".to_string(),
                "max_v".to_string(),
                "median_v".to_string(),
            ]
        );

        // A SUM/COUNT/AVG-only mix keeps the incremental sum/count view.
        let analyzed =
            analyze_optimized("select g, sum(v), count(*), avg(v) from src group by g")
                .await
                .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::SumCount { .. }));

        // DISTINCT aggregates and aggregate FILTER stay rejected in a mix.
        assert!(
            analyze_optimized("select g, count(distinct v), sum(v) from src group by g",)
                .await
                .is_err()
        );
        assert!(
            analyze_optimized(
                "select g, sum(v) filter (where v > 1), min(v) from src group by g",
            )
            .await
            .is_err()
        );
        assert!(
            analyze_optimized(
                "select g, count(distinct v, k), sum(v) from src group by g",
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_computed_aggregates() {
        let cases = [
            ("select g, bit_and(v) from src group by g", "bit_and_v"),
            ("select g, bit_or(v) from src group by g", "bit_or_v"),
            ("select g, bit_xor(v) from src group by g", "bit_xor_v"),
            ("select g, corr(v, k) from src group by g", "corr_v_k"),
            (
                "select g, covar_samp(v, k) from src group by g",
                "covar_samp_v_k",
            ),
            (
                "select g, covar_pop(v, k) from src group by g",
                "covar_pop_v_k",
            ),
            (
                "select g, regr_slope(v, k) from src group by g",
                "regr_slope_v_k",
            ),
            (
                "select g, regr_intercept(v, k) from src group by g",
                "regr_intercept_v_k",
            ),
            (
                "select g, regr_count(v, k) from src group by g",
                "regr_count_v_k",
            ),
            ("select g, regr_r2(v, k) from src group by g", "regr_r2_v_k"),
            (
                "select g, regr_avgx(v, k) from src group by g",
                "regr_avgx_v_k",
            ),
            (
                "select g, regr_avgy(v, k) from src group by g",
                "regr_avgy_v_k",
            ),
            (
                "select g, regr_sxx(v, k) from src group by g",
                "regr_sxx_v_k",
            ),
            (
                "select g, regr_syy(v, k) from src group by g",
                "regr_syy_v_k",
            ),
            (
                "select g, regr_sxy(v, k) from src group by g",
                "regr_sxy_v_k",
            ),
            (
                "select g, percentile_cont(v, 0.5) from src group by g",
                "percentile_cont_v",
            ),
            (
                "select g, approx_median(v) from src group by g",
                "approx_median_v",
            ),
            (
                "select g, approx_percentile_cont_with_weight(v, k, 0.5) \
                 from src group by g",
                "approx_percentile_cont_with_weight_v_k",
            ),
        ];
        for (sql, column) in cases {
            let analyzed = analyze_optimized(sql)
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            let ViewSpec::ComputedAgg {
                column: analyzed_column,
                ..
            } = analyzed.spec
            else {
                panic!("{sql}: expected a computed aggregate spec");
            };
            assert_eq!(analyzed_column, column, "{sql}");
        }

        // HAVING maps to the derived column.
        let analyzed = analyze_optimized(
            "select g, bit_and(v) from src group by g having bit_and(v) > 0",
        )
        .await
        .unwrap();
        let ViewSpec::ComputedAgg { having, .. } = analyzed.spec else {
            panic!("expected a computed aggregate spec");
        };
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("bit_and_v > 0")
        );

        // Literal percentiles and mixing are validated (an arity mismatch is
        // already rejected by the planner).
        assert!(
            analyze_optimized("select g, percentile_cont(v, k) from src group by g")
                .await
                .is_err()
        );
        let mixed = analyze_optimized("select g, bit_and(v), sum(v) from src group by g")
            .await
            .unwrap();
        assert!(matches!(mixed.spec, ViewSpec::MultiAgg { .. }));
    }

    #[tokio::test]
    async fn analyzes_approx_percentile() {
        let analyzed = analyze_optimized(
            "select g, approx_percentile_cont(v, 0.5) from src group by g",
        )
        .await
        .unwrap();
        let ViewSpec::ApproxPercentile {
            percentile,
            value_column,
            ..
        } = analyzed.spec
        else {
            panic!("expected an approx-percentile spec");
        };
        assert_eq!(percentile, "0.5");
        assert_eq!(value_column.as_deref(), Some("v"));

        // HAVING maps to the derived column.
        let analyzed = analyze_optimized(
            "select g, approx_percentile_cont(v, 0.5) from src group by g \
             having approx_percentile_cont(v, 0.5) > 10.0",
        )
        .await
        .unwrap();
        let ViewSpec::ApproxPercentile { having, .. } = analyzed.spec else {
            panic!("expected an approx-percentile spec");
        };
        assert!(
            having
                .as_deref()
                .is_some_and(|having| having.contains("approx_percentile_cont_v")),
            "{having:?}"
        );

        // The percentile must be a literal and mixing is rejected.
        assert!(
            analyze_optimized(
                "select g, approx_percentile_cont(v, v) from src group by g"
            )
            .await
            .is_err()
        );
        let mixed = analyze_optimized(
            "select g, approx_percentile_cont(v, 0.5), sum(v) from src group by g",
        )
        .await
        .unwrap();
        assert!(matches!(mixed.spec, ViewSpec::MultiAgg { .. }));
    }

    #[tokio::test]
    async fn analyzes_approx_distinct() {
        let analyzed =
            analyze_optimized("select g, approx_distinct(v) from src group by g")
                .await
                .unwrap();
        let ViewSpec::ApproxDistinct { value_column, .. } = analyzed.spec else {
            panic!("expected an approx-distinct spec");
        };
        assert_eq!(value_column.as_deref(), Some("v"));

        // HAVING maps to the derived column.
        let analyzed = analyze_optimized(
            "select g, approx_distinct(v) from src group by g \
             having approx_distinct(v) > 1",
        )
        .await
        .unwrap();
        let ViewSpec::ApproxDistinct { having, .. } = analyzed.spec else {
            panic!("expected an approx-distinct spec");
        };
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("approx_distinct_v > 1")
        );

        // A value expression is kept.
        let analyzed =
            analyze_optimized("select g, approx_distinct(v * 2) from src group by g")
                .await
                .unwrap();
        let ViewSpec::ApproxDistinct {
            value_column,
            value_expr,
            ..
        } = analyzed.spec
        else {
            panic!("expected an approx-distinct spec");
        };
        assert!(value_column.is_none());
        assert!(value_expr.is_some());

        // Mixing with other aggregates recomputes through the
        // multi-aggregate view.
        let mixed =
            analyze_optimized("select g, approx_distinct(v), sum(v) from src group by g")
                .await
                .unwrap();
        assert!(matches!(mixed.spec, ViewSpec::MultiAgg { .. }));
    }

    #[tokio::test]
    async fn analyzes_bool_aggregates() {
        // A boolean aggregate over an expression.
        let analyzed = analyze_optimized("select g, bool_and(v > 1) from src group by g")
            .await
            .unwrap();
        let ViewSpec::BoolAgg {
            bool_agg,
            value_column,
            value_expr,
            ..
        } = analyzed.spec
        else {
            panic!("expected a bool-agg spec");
        };
        assert_eq!(bool_agg, BoolAggKind::BoolAnd);
        assert!(value_column.is_none());
        assert!(value_expr.is_some());

        // HAVING maps to the derived column.
        let analyzed = analyze_optimized(
            "select g, bool_or(v > 1) from src group by g having bool_or(v > 1)",
        )
        .await
        .unwrap();
        let ViewSpec::BoolAgg { having, .. } = analyzed.spec else {
            panic!("expected a bool-agg spec");
        };
        assert_eq!(
            normalized(having.as_deref()).as_deref(),
            Some("bool_or_value")
        );

        // Only booleans are supported.
        assert!(
            crate::runtime::bool_agg_mv_schema_for(
                &schema(),
                &["g".to_string()],
                "v",
                BoolAggKind::BoolAnd,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_grouping_sets_aggregates() {
        let analyzed =
            analyze_optimized("select g, sum(v), count(*) from src group by rollup(g)")
                .await
                .unwrap();
        let ViewSpec::GroupingSets {
            group_keys,
            groupings,
            value_column,
            average,
            ..
        } = analyzed.spec
        else {
            panic!("expected a grouping-sets spec");
        };
        assert_eq!(group_keys, vec!["g".to_string()]);
        assert_eq!(groupings, vec![vec![0], vec![]]);
        assert_eq!(value_column.as_deref(), Some("v"));
        assert!(!average);

        // CUBE of two keys expands to all four subsets.
        let analyzed =
            analyze_optimized("select g, v, sum(v) from src group by cube(g, v)")
                .await
                .unwrap();
        let ViewSpec::GroupingSets {
            group_keys,
            groupings,
            ..
        } = analyzed.spec
        else {
            panic!("expected a grouping-sets spec");
        };
        assert_eq!(group_keys, vec!["g".to_string(), "v".to_string()]);
        assert_eq!(groupings.len(), 4);
        assert!(groupings.contains(&vec![0, 1]));
        assert!(groupings.contains(&vec![0]));
        assert!(groupings.contains(&vec![1]));
        assert!(groupings.contains(&Vec::new()));

        // Explicit sets with HAVING and AVG.
        let analyzed = analyze_optimized(
            "select g, sum(v) from src \
             group by grouping sets ((g), ()) having sum(v) > 10",
        )
        .await
        .unwrap();
        let ViewSpec::GroupingSets {
            groupings, having, ..
        } = analyzed.spec
        else {
            panic!("expected a grouping-sets spec");
        };
        assert_eq!(groupings, vec![vec![0], vec![]]);
        assert_eq!(normalized(having.as_deref()).as_deref(), Some("sum_v > 10"));

        let analyzed = analyze_optimized("select g, avg(v) from src group by rollup(g)")
            .await
            .unwrap();
        let ViewSpec::GroupingSets { average, .. } = analyzed.spec else {
            panic!("expected a grouping-sets spec");
        };
        assert!(average);

        // A plain key next to a ROLLUP is rewritten into the expanded sets.
        let analyzed =
            analyze_optimized("select g, v, sum(v) from src group by g, rollup(v)")
                .await
                .unwrap();
        let ViewSpec::GroupingSets {
            group_keys,
            groupings,
            ..
        } = analyzed.spec
        else {
            panic!("expected a grouping-sets spec");
        };
        assert_eq!(group_keys, vec!["g".to_string(), "v".to_string()]);
        assert_eq!(groupings, vec![vec![0], vec![0, 1]]);

        // GROUPING(key) columns are materialized per set.
        let analyzed = analyze_optimized(
            "select g, grouping(g) as is_total, sum(v) from src \
             group by grouping sets ((g), ())",
        )
        .await
        .unwrap();
        let ViewSpec::GroupingSets {
            grouping_columns, ..
        } = analyzed.spec
        else {
            panic!("expected a grouping-sets spec");
        };
        assert_eq!(
            grouping_columns,
            vec![GroupingColumn {
                key: 0,
                name: "is_total".to_string(),
            }]
        );
        // An unaliased GROUPING() column falls back to a plain derived name.
        let analyzed = analyze_optimized(
            "select g, grouping(g), sum(v) from src \
             group by grouping sets ((g), ())",
        )
        .await
        .unwrap();
        let ViewSpec::GroupingSets {
            grouping_columns, ..
        } = analyzed.spec
        else {
            panic!("expected a grouping-sets spec");
        };
        assert_eq!(grouping_columns[0].name, "grouping_g");

        // Only SUM/COUNT/AVG and plain columns are maintained.
        assert!(
            analyze_optimized("select g, min(v) from src group by rollup(g)")
                .await
                .is_err()
        );
        assert!(
            analyze_optimized(
                "select v + 1 as bucket, sum(v) from src group by rollup(v + 1)"
            )
            .await
            .is_err()
        );
    }
}
