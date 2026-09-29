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
//! This first slice covers projections/filters and the aggregate family
//! (SUM/COUNT, MIN/MAX, COUNT(DISTINCT)/SUM(DISTINCT)); window, top-k, join,
//! semi/anti and UNION ALL follow.

use std::collections::HashMap;

use datafusion::common::{ScalarValue, TableReference};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan, Operator, Projection};

use crate::error::Result;
use crate::runtime::{
    CompareOp, DistinctAggKind, FilterCondition, LiteralValue, MinMaxKind, ViewSpec,
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
                analyze_aggregate(aggregate, tables, request)?
            }
            LogicalPlan::Filter(_) | LogicalPlan::TableScan(_) => {
                analyze_row(plan, tables, request)?
            }
            other => {
                return Err(unsupported(format!(
                    "projection over {}",
                    plan_label(other)
                )));
            }
        },
        LogicalPlan::Filter(_) | LogicalPlan::TableScan(_) => {
            analyze_row(plan, tables, request)?
        }
        LogicalPlan::Aggregate(aggregate) => {
            analyze_aggregate(aggregate, tables, request)?
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

/// A projection (and optional filters) of one source table.
fn analyze_row(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let (source, output_columns, filters) = collect_row(plan, tables)?;
    Ok(ViewSpec::Row {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        output_columns: output_columns.unwrap_or_default(),
        filters,
    })
}

type RowParts = (IvmTable, Option<Vec<String>>, Vec<FilterCondition>);

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
            let (source, _, mut filters) = collect_row(&projection.input, tables)?;
            let _ = &mut filters;
            Ok((source, Some(output_columns), filters))
        }
        LogicalPlan::Filter(filter) => {
            let (source, output_columns, mut filters) =
                collect_row(&filter.input, tables)?;
            for conjunct in split_conjunction(&filter.predicate) {
                filters.push(filter_condition(conjunct)?);
            }
            Ok((source, output_columns, filters))
        }
        LogicalPlan::TableScan(scan) => {
            let source = resolve_table(tables, &scan.table_name)?.clone();
            Ok((source, None, Vec::new()))
        }
        other => Err(unsupported(format!("row shape over {}", plan_label(other)))),
    }
}

/// The aggregate family: SUM/COUNT, MIN/MAX and COUNT(DISTINCT)/SUM(DISTINCT).
fn analyze_aggregate(
    aggregate: &Aggregate,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let scan = match peel(&aggregate.input) {
        LogicalPlan::TableScan(scan) => scan,
        LogicalPlan::Filter(_) => {
            return Err(unsupported(
                "WHERE on an aggregate view (use an intermediate filtered view)",
            ));
        }
        other => {
            return Err(unsupported(format!("aggregate over {}", plan_label(other))));
        }
    };
    let source = resolve_table(tables, &scan.table_name)?;

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
                if count || sum.is_some() || min_max.is_some() || distinct.is_some() {
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
                sum = Some(single_column_arg(&function.params.args)?);
            }
            ("min", false) | ("max", false) => {
                if count || sum.is_some() || min_max.is_some() || distinct.is_some() {
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

    let spec = if let Some((agg, value_column)) = distinct {
        ViewSpec::DistinctAgg {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            state_table_id: request.state_table()?,
            group_keys,
            value_column,
            agg,
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
        }
    } else if sum.is_some() || count {
        ViewSpec::SumCount {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            group_keys,
            value_column: sum,
        }
    } else {
        return Err(unsupported(
            "GROUP BY without a supported aggregate (use SUM/COUNT/MIN/MAX or DISTINCT)",
        ));
    };
    Ok(spec)
}

/// Stable definition identity.  FNV-1a over the canonical spec JSON: a change
/// in columns, keys or aggregate kind changes the hash and triggers a rebuild.
fn definition_hash(spec: &ViewSpec) -> Result<String> {
    let bytes = serde_json::to_vec(spec)?;
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

fn filter_condition(expr: &Expr) -> Result<FilterCondition> {
    match expr {
        Expr::BinaryExpr(binary) => {
            let (column, op, value) = match (binary.left.as_ref(), binary.right.as_ref())
            {
                (Expr::Column(column), literal) => (
                    column.name.clone(),
                    compare_op(binary.op)?,
                    literal_value(literal)?,
                ),
                (literal, Expr::Column(column)) => (
                    column.name.clone(),
                    flip(compare_op(binary.op)?),
                    literal_value(literal)?,
                ),
                _ => {
                    return Err(unsupported(
                        "filters comparing two columns are not supported",
                    ));
                }
            };
            Ok(FilterCondition { column, op, value })
        }
        Expr::IsNull(inner) => Ok(FilterCondition {
            column: column_name(inner)
                .ok_or_else(|| unsupported("IS NULL on a non-column expression"))?,
            op: CompareOp::Eq,
            value: LiteralValue::Null,
        }),
        Expr::IsNotNull(inner) => Ok(FilterCondition {
            column: column_name(inner)
                .ok_or_else(|| unsupported("IS NOT NULL on a non-column expression"))?,
            op: CompareOp::Ne,
            value: LiteralValue::Null,
        }),
        _ => Err(unsupported(format!("filter {expr}"))),
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

fn literal_value(expr: &Expr) -> Result<LiteralValue> {
    let Expr::Literal(scalar, _) = expr else {
        return Err(unsupported("comparisons against non-literals"));
    };
    match scalar {
        ScalarValue::Null => Ok(LiteralValue::Null),
        ScalarValue::Boolean(Some(value)) => Ok(LiteralValue::Bool(*value)),
        ScalarValue::Int8(Some(value)) => Ok(LiteralValue::Int(i64::from(*value))),
        ScalarValue::Int16(Some(value)) => Ok(LiteralValue::Int(i64::from(*value))),
        ScalarValue::Int32(Some(value)) => Ok(LiteralValue::Int(i64::from(*value))),
        ScalarValue::Int64(Some(value)) => Ok(LiteralValue::Int(*value)),
        ScalarValue::UInt8(Some(value)) => Ok(LiteralValue::Int(i64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Ok(LiteralValue::Int(i64::from(*value))),
        ScalarValue::UInt32(Some(value)) => Ok(LiteralValue::Int(i64::from(*value))),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value)
            .map(LiteralValue::Int)
            .map_err(|_| unsupported("integer literal out of i64 range")),
        ScalarValue::Float32(Some(value)) => Ok(LiteralValue::Float(f64::from(*value))),
        ScalarValue::Float64(Some(value)) => Ok(LiteralValue::Float(*value)),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Ok(LiteralValue::String(value.clone())),
        other => Err(unsupported(format!("literal {other}"))),
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
    use crate::runtime::{CompareOp, DistinctAggKind, MinMaxKind};

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
        let analyzed =
            analyze("select v, k from src where v > 5 and g = 'a' and 5 <= v or false")
                .await;
        // The trailing `or false` makes the whole predicate unsupported: the
        // analyzer never drops part of a predicate.
        assert!(analyzed.is_err());

        let analyzed = analyze("select v, k from src where v > 5 and g = 'a'")
            .await
            .unwrap();
        let ViewSpec::Row {
            output_columns,
            filters,
            ..
        } = analyzed.spec
        else {
            panic!("expected a row spec");
        };
        assert_eq!(output_columns, vec!["v".to_string(), "k".to_string()]);
        let mut got = filters
            .iter()
            .map(|filter| (filter.column.clone(), filter.op, filter.value.clone()))
            .collect::<Vec<_>>();
        got.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            got,
            vec![
                (
                    "g".to_string(),
                    CompareOp::Eq,
                    LiteralValue::String("a".to_string())
                ),
                ("v".to_string(), CompareOp::Gt, LiteralValue::Int(5)),
            ]
        );
    }

    #[tokio::test]
    async fn analyzes_is_null_filters_and_reversed_comparisons() {
        let analyzed = analyze("select k from src where g is null and 5 < v")
            .await
            .unwrap();
        let ViewSpec::Row { filters, .. } = analyzed.spec else {
            panic!("expected a row spec");
        };
        let mut got = filters
            .iter()
            .map(|filter| (filter.column.clone(), filter.op, filter.value.clone()))
            .collect::<Vec<_>>();
        got.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            got,
            vec![
                ("g".to_string(), CompareOp::Eq, LiteralValue::Null),
                ("v".to_string(), CompareOp::Gt, LiteralValue::Int(5)),
            ]
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_shapes() {
        // AVG is not maintained incrementally.
        assert!(
            analyze("select g, avg(v) from src group by g")
                .await
                .is_err()
        );
        // SELECT DISTINCT has no view kind.
        assert!(analyze("select distinct g from src").await.is_err());
        // WHERE on an aggregate needs an intermediate view.
        assert!(
            analyze("select g, sum(v) from src where v > 0 group by g")
                .await
                .is_err()
        );
        // HAVING filters above the aggregate.
        assert!(
            analyze("select g, sum(v) from src group by g having sum(v) > 10")
                .await
                .is_err()
        );
        // Window, join and computed projections are later slices.
        assert!(
            analyze("select k, row_number() over (partition by g order by v) from src")
                .await
                .is_err()
        );
        assert!(analyze("select k + 1 from src").await.is_err());
        assert!(
            analyze("select * from src a join src b on a.k = b.k")
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
