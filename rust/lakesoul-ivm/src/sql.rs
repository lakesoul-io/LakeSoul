use crate::error::Result;
use crate::runtime::{
    BoolAggKind, CompareOp, ComputedAggArg, ComputedAggResult, DistinctAggKind,
    GroupingColumn, IVM_AVG_COLUMN, IVM_COUNT_COLUMN, IVM_MEDIAN_COLUMN,
    IVM_NONNULL_COUNT_COLUMN, IVM_RIGHT_AGG_COLUMN, IVM_SUM_COLUMN, IVM_VALUE_COLUMN,
    JoinOutputColumn, JoinSide, LookupChainColumn, LookupChainSource, LookupChainStep,
    MinMaxKind, MultiAggSpec, MultiJoinColumn, MultiJoinCondition, MultiJoinKey,
    MultiJoinSource, RowScalarSpec, ScalarTableSpec, SemiAntiCondition, UnionSourceSpec,
    VarianceKind, ViewSpec, WindowColumn, WindowFunction, WindowGroupSpec,
    approx_distinct_output_column, approx_percentile_output_column,
    bool_agg_output_column, encode_data_type, string_agg_output_column,
    union_output_schema_for, wide_pair_alias,
};
use crate::table::IvmTable;
use arrow_schema::{DataType, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, ScalarValue, TableReference};
use datafusion::logical_expr::expr::{AggregateFunction, GroupingSet, NullTreatment};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    Aggregate, Distinct, Expr, Filter, Join, JoinType, LogicalPlan, Operator, Projection,
    SortExpr, Subquery, Union, Window, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;
use std::collections::HashMap;

mod aggregates;
mod joins;
mod multi_join;
mod rows;
mod set_ops;
mod unions;
mod windows;

use self::aggregates::*;
use self::joins::*;
use self::multi_join::*;
use self::rows::*;
use self::set_ops::*;
use self::unions::*;
use self::windows::*;

#[cfg(test)]
pub(crate) mod test_helpers;
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
                // A `GROUPING()` column is a computed column over the grouping
                // sets, which the grouping-sets analyzer materializes.
                let grouping_projection = aggregate
                    .group_expr
                    .iter()
                    .any(|expr| matches!(strip_alias(expr), Expr::GroupingSet(_)))
                    && is_grouping_projection(projection);
                if !is_plain_projection(projection) && !grouping_projection {
                    return Err(unsupported(
                        "computed columns above an aggregate are not supported",
                    ));
                }
                analyze_aggregate(aggregate, Some(projection), &[], tables, request)?
            }
            LogicalPlan::Sort(sort) => {
                // `DISTINCT ON (...) ... ORDER BY` keeps the final ordering in
                // a Sort above the aggregate; the ordering is not
                // materialized, but the `FIRST_VALUE` aggregates under it
                // define the picked rows.
                if let LogicalPlan::Aggregate(aggregate) = peel(&sort.input)
                    && aggregate.aggr_expr.iter().any(is_first_value_aggregate)
                {
                    analyze_aggregate(aggregate, Some(projection), &[], tables, request)?
                } else {
                    return Err(unsupported("projection over a sort"));
                }
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
                    let grouping_projection =
                        aggregate.group_expr.iter().any(|expr| {
                            matches!(strip_alias(expr), Expr::GroupingSet(_))
                        }) && is_grouping_projection(projection);
                    if !is_plain_projection(projection) && !grouping_projection {
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
    // The optimizer may keep a plain projection between the filter and the
    // aggregate (the projection only reorders the materialized columns).
    if let LogicalPlan::Projection(projection) = node
        && is_plain_projection(projection)
    {
        node = peel(&projection.input);
    }
    match node {
        LogicalPlan::Aggregate(aggregate) if !conjuncts.is_empty() => {
            Some((aggregate, conjuncts))
        }
        _ => None,
    }
}
type RowParts = (IvmTable, Option<Vec<String>>, Vec<String>, Option<String>);
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
/// Render an expression to SQL over unqualified columns, for the runtime to
/// parse back (aggregate calls and join predicates included).
fn render_expression(expr: &Expr) -> Result<String> {
    let stripped = strip_relations(expr.clone())?;
    let ast = Unparser::default()
        .expr_to_sql(&stripped)
        .map_err(|error| unsupported(format!("expression {error}")))?;
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
/// The expression under any aliases.
fn strip_alias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}
fn column_of(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => column_of(&alias.expr),
        _ => None,
    }
}
/// The SQL operator of a comparison.
pub(crate) fn compare_op_sql(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
    }
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
    use super::*;
    use crate::sql::test_helpers::*;

    #[tokio::test]
    async fn probe_scalar_subqueries() {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        let dim = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        ctx.register_table("src", Arc::new(table)).unwrap();
        ctx.register_table("dim", Arc::new(dim)).unwrap();
        for sql in [
            "select k, v from src where v > (select avg(v) from src)",
            "select k, v from src s where v > (select avg(v) from dim)",
            "select k, v, (select max(v) from dim) as m from src",
            "select k, v from src s where v > (select avg(v) from dim u where u.k = s.k)",
            "select k, v from src s where exists (select 1 from dim u where u.k = s.k)",
        ] {
            match ctx.sql(sql).await {
                Ok(df) => {
                    let plan = ctx.state().optimize(&df.logical_plan().clone()).unwrap();
                    println!("=== {sql}");
                    println!("{plan}");
                }
                Err(error) => println!("PLAN_ERR {sql}: {error}"),
            }
        }
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
        // AVG mixes with MIN/MAX through the multi-aggregate view.
        let mixed = analyze("select g, avg(v), min(v) from src group by g")
            .await
            .unwrap();
        assert!(matches!(mixed.spec, ViewSpec::MultiAgg { .. }));
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
        // A multi-column DISTINCT needs a GROUP BY.
        assert!(
            analyze_optimized("select count(distinct g, v) from src")
                .await
                .is_err()
        );
        // DISTINCT ON plans as first_value aggregates and is maintained by
        // the recompute view (see analyzes_distinct_on).
        let distinct_on = analyze_optimized("select distinct on (g) g, v from src")
            .await
            .unwrap();
        assert!(matches!(distinct_on.spec, ViewSpec::MultiAgg { .. }));
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
        // Inner joins take no extra conditions over columns the pair does
        // not carry (payload comparisons are maintained, see
        // `analyzes_theta_joins`).
        assert!(
            analyze_optimized(
                "select a.k, a.v, b.v from src a join src b on a.k = b.k and a.g < b.g"
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
