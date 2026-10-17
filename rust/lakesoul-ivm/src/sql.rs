use crate::error::Result;
use crate::runtime::{
    BoolAggKind, CompareOp, ComputedAggArg, ComputedAggResult, DistinctAggKind,
    GroupingColumn, HavingLookupSpec, IVM_AGG_VALUE_COLUMN, IVM_AVG_COLUMN,
    IVM_COUNT_COLUMN, IVM_HAVING_LOOKUP_COLUMN, IVM_MEDIAN_COLUMN,
    IVM_NONNULL_COUNT_COLUMN, IVM_RIGHT_AGG_COLUMN, IVM_SUM_COLUMN, IVM_VALUE_COLUMN,
    JoinKeyExprs, JoinOutputColumn, JoinSide, LeftAggregateOutput, LookupChainColumn,
    LookupChainSource, LookupChainStep, MinMaxKind, MultiAggSpec, MultiJoinColumn,
    MultiJoinCondition, MultiJoinKey, MultiJoinSource, RowScalarSpec, ScalarTableSpec,
    SemiAntiCondition, UnionSourceSpec, VarianceKind, ViewSpec, WindowColumn,
    WindowFunction, WindowGroupSpec, approx_distinct_output_column,
    approx_percentile_output_column, bool_agg_output_column, encode_data_type,
    join_condition_column_name, join_key_expression_name, string_agg_output_column,
    union_output_schema_for, wide_pair_alias,
};
use crate::table::IvmTable;
use arrow_schema::{DataType, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{
    Column, DFSchema, ExprSchema, NullEquality, ScalarValue, TableReference,
};
use datafusion::logical_expr::expr::{AggregateFunction, GroupingSet, NullTreatment};
use datafusion::logical_expr::expr::{Alias, SetQuantifier};
use datafusion::logical_expr::expr::{BinaryExpr, Exists, InSubquery, SetComparison};
use datafusion::logical_expr::utils::{merge_schema, split_conjunction};
use datafusion::logical_expr::{
    Aggregate, Case, Distinct, Expr, Filter, Join, JoinType, LogicalPlan,
    LogicalPlanBuilder, Operator, Projection, Sort, SortExpr, Subquery, Union, Window,
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;
use rootcause::report;
use std::collections::HashMap;
use std::sync::Arc;

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
    /// Whether the statement is an internal derived step.  A derived step
    /// widens a union branch with the primary keys the maintenance needs when
    /// the consuming operator does not select them; a user statement keeps
    /// them an explicit part of the output contract.
    pub derived: bool,
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
    // `x = ANY (S)` is `x IN (S)` and `x <> ALL (S)` is `x NOT IN (S)` by the
    // SQL standard; normalizing them before planning makes DataFusion plan the
    // semi/anti joins the analyzer models instead of mark joins.
    let plan = normalize_quantified_comparisons(plan)?;
    // The optimizer keeps a projection over a projection around a pruned
    // subquery select list (a filtered window subquery, say); every shape
    // below reads one projection.
    if let Some(collapsed) = collapse_projections(&plan) {
        return analyze_select(&collapsed, tables, request);
    }
    let plan = &plan;
    // Incremental views need keyed sources: the refresh folds the changelog
    // through merge-on-read and retracts by key.  Append-only sources (with or
    // without a change column) are rejected explicitly.
    for table in tables.values() {
        if table.primary_keys.is_empty() {
            return Err(unsupported(format!(
                "source table {} has no primary key: incremental views need \
                 keyed sources (append-only sources are not supported)",
                table.table_name
            )));
        }
    }
    if matches!(plan, LogicalPlan::SubqueryAlias(_)) {
        return analyze_select(peel(plan), tables, request);
    }
    let spec = match plan {
        LogicalPlan::Projection(projection) => match peel(&projection.input) {
            LogicalPlan::Aggregate(aggregate) => {
                // Computed columns above the aggregate (a `GROUPING()` column
                // or an expression over the aggregate outputs) are materialized
                // by the aggregate analyzers.
                analyze_aggregate(aggregate, Some(projection), &[], tables, request)?
            }
            LogicalPlan::Limit(limit) => {
                // `ORDER BY ... LIMIT n OFFSET m` keeps a Limit (skip, fetch)
                // above the Sort's fetch.
                let offset = limit_bound(&limit.skip, "OFFSET")?.unwrap_or(0);
                let Some(fetch) = limit_bound(&limit.fetch, "LIMIT")? else {
                    return Err(unsupported(
                        "LIMIT without ORDER BY is not maintained (order the rows \
                         with ORDER BY ... LIMIT k)",
                    ));
                };
                // The optimizer may or may not push the fetch into the Sort,
                // so any Sort input is a top-k here.
                match peel(&limit.input) {
                    LogicalPlan::Sort(sort) => analyze_order_by_limit(
                        Some(projection),
                        sort,
                        tables,
                        request,
                        offset,
                        Some(fetch),
                    )?,
                    _ => {
                        return Err(unsupported(
                            "LIMIT without ORDER BY is not maintained (order \
                             the rows with ORDER BY ... LIMIT k)",
                        ));
                    }
                }
            }
            LogicalPlan::Sort(sort) => {
                // `DISTINCT ON (...) ... ORDER BY` keeps the final ordering in
                // a Sort above the aggregate; the ordering is not
                // materialized, but the `FIRST_VALUE` aggregates under it
                // define the picked rows.  A `fetch` is a `LIMIT`: a global
                // top-k.
                if sort.fetch.is_some() {
                    analyze_order_by_limit(
                        Some(projection),
                        sort,
                        tables,
                        request,
                        0,
                        None,
                    )?
                } else if let LogicalPlan::Aggregate(aggregate) = peel(&sort.input)
                    && aggregate.aggr_expr.iter().any(is_first_value_aggregate)
                {
                    analyze_aggregate(aggregate, Some(projection), &[], tables, request)?
                } else {
                    return Err(unsupported("projection over a sort"));
                }
            }
            LogicalPlan::Window(window) => {
                analyze_window(Some(projection), window, tables, request)?
            }
            LogicalPlan::Join(join) => {
                analyze_join(join, Some(projection), tables, request)?
            }
            LogicalPlan::Union(union) => {
                analyze_union(union, Some(projection), tables, request)?
            }
            LogicalPlan::Filter(filter) => {
                // `HAVING` is one or more filters directly above the
                // aggregate; a filter on a window rank column is top-k; a
                // guarded COUNT comparison is a correlated scalar subquery.
                if let Some(spec) =
                    try_analyze_guarded_count(Some(projection), filter, tables, request)?
                {
                    spec
                } else if let Some((aggregate, having)) =
                    having_aggregate(&projection.input)
                {
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
        LogicalPlan::Filter(filter) => {
            // The projection above an aggregate may be optimized away (e.g.
            // for MIN/MAX), leaving a bare `Filter -> Aggregate` plan.
            if let Some(spec) = try_analyze_guarded_count(None, filter, tables, request)?
            {
                spec
            } else if let Some((aggregate, having)) = having_aggregate(plan) {
                analyze_aggregate(aggregate, None, &having, tables, request)?
            } else {
                analyze_row(plan, tables, request)?
            }
        }
        LogicalPlan::TableScan(_) => analyze_row(plan, tables, request)?,
        LogicalPlan::Sort(sort) => {
            analyze_order_by_limit(None, sort, tables, request, 0, None)?
        }
        LogicalPlan::Window(window) => analyze_window(None, window, tables, request)?,
        LogicalPlan::Limit(limit) => {
            let offset = limit_bound(&limit.skip, "OFFSET")?.unwrap_or(0);
            let Some(fetch) = limit_bound(&limit.fetch, "LIMIT")? else {
                return Err(unsupported(
                    "LIMIT without ORDER BY is not maintained (order the rows \
                     with ORDER BY ... LIMIT k)",
                ));
            };
            match peel(&limit.input) {
                LogicalPlan::Sort(sort) => analyze_order_by_limit(
                    None,
                    sort,
                    tables,
                    request,
                    offset,
                    Some(fetch),
                )?,
                _ => {
                    return Err(unsupported(
                        "LIMIT without ORDER BY is not maintained (order the rows \
                         with ORDER BY ... LIMIT k)",
                    ));
                }
            }
        }
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
        // A plain `ORDER BY` (without a limit) does not change the multiset a
        // source contributes, so an operator below it reads through.
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
            collect_filtered_source(&sort.input, tables, shape)
        }
        other if derived_sourceable(other) => Err(derived_source_error(shape, other)),
        other => Err(unsupported(format!("{shape} over {}", plan_label(other)))),
    }
}

/// The attachment the analyzer leaves on a compound source that must be
/// materialized as a derived view: the executor catches it, materializes the
/// subplan and retries the analysis against the derived MV.
#[derive(Debug)]
pub(crate) struct DerivedSource {
    /// The subplan to materialize.
    pub(crate) plan: LogicalPlan,
}

impl std::fmt::Display for DerivedSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a derived source")
    }
}

/// The attachment every unsupported-shape rejection carries, so the executor
/// can tell a shape the analyzer does not maintain (which may fall back to a
/// full recompute, Tier-2) from a plain error (a broken statement, a schema
/// mismatch) that must stay fatal.
#[derive(Debug)]
pub(crate) struct UnsupportedShape;

impl std::fmt::Display for UnsupportedShape {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an unsupported view shape")
    }
}

/// Whether a compound source can be materialized as a derived view.  The
/// executor materializes the subplan exactly like a registered view (its
/// spec, MV table and refresh are the normal ones), so any shape the
/// analyzer itself maintains qualifies.
pub(crate) fn derived_sourceable(plan: &LogicalPlan) -> bool {
    matches!(
        peel(plan),
        LogicalPlan::Join(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
    )
}

/// The error asking the executor to materialize `plan` as a derived view.
pub(crate) fn derived_source_error(shape: &str, plan: &LogicalPlan) -> rootcause::Report {
    report!(
        "{shape} reads {}: it is materialized as a derived view",
        plan_label(plan)
    )
    .attach(DerivedSource { plan: plan.clone() })
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
/// Rewrite the quantified comparisons into the maintained semi/anti shapes.
///
/// `x = ANY (S)` is exactly `x IN (S)` and `x <> ALL (S)` is exactly
/// `x NOT IN (S)`, so those keep the cheaper `IN` forms.  The remaining
/// quantifiers are rewritten into `EXISTS` predicates whose filter sees the
/// comparison truth: `x op ANY (S)` keeps a left row exactly when some right
/// row satisfies `x op y`, and `x op ALL (S)` exactly when no right row makes
/// `x op y` false or unknown.  A `NOT` directly above a quantifier is pushed
/// into it with the complementary operator (`NOT (x op ANY S)` is
/// `x complement(op) ALL S`), so a quantifier below a wider `NOT` keeps its
/// mark-join plan and is rejected by the analyzer rather than mistranslated.
pub(crate) fn normalize_quantified_comparisons(
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    plan.clone()
        .transform_down(|node| {
            // The expressions of a node resolve against its inputs.
            let schema = merge_schema(&node.inputs());
            node.map_expressions(|expr| {
                expr.transform_down(|expr| {
                    rewrite_set_comparison(expr, &schema).map_err(|error| {
                        datafusion::error::DataFusionError::Plan(error.to_string())
                    })
                })
            })
        })
        .map(|transformed| transformed.data)
        .map_err(|error| {
            unsupported(format!("normalize quantified comparisons: {error}"))
        })
}

/// The complementary operator: `NOT (x op y)` is `x complement(op) y`.
fn complement_operator(op: Operator) -> Result<Operator> {
    Ok(match op {
        Operator::Eq => Operator::NotEq,
        Operator::NotEq => Operator::Eq,
        Operator::Lt => Operator::GtEq,
        Operator::LtEq => Operator::Gt,
        Operator::Gt => Operator::LtEq,
        Operator::GtEq => Operator::Lt,
        other => {
            return Err(unsupported(format!("quantified comparison {other}")));
        }
    })
}

fn rewrite_set_comparison(
    expr: Expr,
    outer_schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    match expr {
        Expr::Not(inner) if matches!(inner.as_ref(), Expr::SetComparison(_)) => {
            let Expr::SetComparison(comparison) = *inner else {
                unreachable!("the guard checked the variant");
            };
            let comparison = SetComparison {
                quantifier: match comparison.quantifier {
                    SetQuantifier::Any => SetQuantifier::All,
                    SetQuantifier::All => SetQuantifier::Any,
                },
                op: complement_operator(comparison.op)?,
                ..comparison
            };
            Ok(Transformed::yes(build_set_comparison(
                comparison,
                outer_schema,
            )?))
        }
        Expr::SetComparison(comparison) => Ok(Transformed::yes(build_set_comparison(
            comparison,
            outer_schema,
        )?)),
        other => Ok(Transformed::no(other)),
    }
}

fn build_set_comparison(
    comparison: SetComparison,
    outer_schema: &DFSchema,
) -> Result<Expr> {
    let SetComparison {
        expr,
        subquery,
        op,
        quantifier,
    } = comparison;
    match (quantifier, op) {
        (SetQuantifier::Any, Operator::Eq) => {
            return Ok(Expr::InSubquery(InSubquery::new(expr, subquery, false)));
        }
        (SetQuantifier::All, Operator::NotEq) => {
            return Ok(Expr::InSubquery(InSubquery::new(expr, subquery, true)));
        }
        _ => {}
    }
    let left = to_outer_reference(*expr, outer_schema)?;
    let subquery_schema = subquery.subquery.schema();
    if subquery_schema.fields().is_empty() {
        return Err(unsupported(
            "a quantified comparison needs a single subquery column",
        ));
    }
    let right = Expr::Column(Column::from(subquery_schema.qualified_field(0)));
    let comparison =
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)));
    // The EXISTS filter holds where the comparison is true (`ANY`) or where
    // no row makes it false or unknown (`ALL`, as an anti join).
    let (filter, negated) = match quantifier {
        SetQuantifier::Any => (comparison, false),
        SetQuantifier::All => (Expr::IsNotTrue(Box::new(comparison)), true),
    };
    let plan = LogicalPlanBuilder::from(subquery.subquery.as_ref().clone())
        .filter(filter)?
        .build()?;
    let outer_ref_columns = plan.all_out_ref_exprs();
    Ok(Expr::Exists(Exists {
        subquery: Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns,
            spans: subquery.spans,
        },
        negated,
    }))
}

/// Mark every outer column of a quantified comparison's left expression, so
/// the subquery can reference it (the same form DataFusion's own
/// `RewriteSetComparison` rule builds).
fn to_outer_reference(expr: Expr, outer_schema: &DFSchema) -> Result<Expr> {
    expr.transform_up(|expr| match expr {
        Expr::Column(column) => {
            let field = outer_schema.field_from_column(&column).map_err(|error| {
                datafusion::error::DataFusionError::Plan(format!(
                    "quantified comparison {error}"
                ))
            })?;
            Ok(Transformed::yes(Expr::OuterReferenceColumn(
                Arc::clone(field),
                column,
            )))
        }
        Expr::OuterReferenceColumn(_, _) => Ok(Transformed::no(expr)),
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
    .map_err(|error| unsupported(format!("quantified comparison {error}")))
}

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
            Expr::ScalarSubquery(_) => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "a filter may not contain a subquery; a HAVING subquery \
                     needs two views: group the source in one view, then \
                     filter it against the subquery in a second view"
                        .to_string(),
                ));
            }
            Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::Exists { .. }
            | Expr::InSubquery(_) => {
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
        // The rejection reason depends on the analyzer's message, not on the
        // view's meaning.
        ViewSpec::Fallback {
            reason,
            partition_scope,
            ..
        } => {
            reason.clear();
            *partition_scope = None;
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
/// Collapse `Projection (outer)` over `Projection (inner)` when the outer
/// one only selects or renames columns.  The optimizer keeps both around a
/// pruned subquery select list, and the shape analyzers expect a single
/// projection over the shape's input.
fn collapse_projections(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Projection(outer) = plan else {
        return None;
    };
    let LogicalPlan::Projection(inner) = peel(&outer.input) else {
        return None;
    };
    let mut exprs = Vec::with_capacity(outer.expr.len());
    for expr in &outer.expr {
        let mut target = expr;
        let mut alias = None;
        while let Expr::Alias(aliased) = target {
            if alias.is_none() {
                alias = Some(aliased.name.clone());
            }
            target = &aliased.expr;
        }
        let Expr::Column(column) = target else {
            return None;
        };
        let (field, expression) = inner
            .schema
            .fields()
            .iter()
            .zip(&inner.expr)
            .find(|(field, _)| field.name() == &column.name)?;
        let expression = match &alias {
            Some(name) if name != field.name() => Expr::Alias(Alias::new(
                expression.clone(),
                None::<TableReference>,
                name.clone(),
            )),
            _ => expression.clone(),
        };
        exprs.push(expression);
    }
    // Keep the outer schema: above a derived step the inner input can carry
    // a schema that the rewrite has not refreshed, and the analyzer only
    // reads names and types from this projection.
    let projection =
        Projection::try_new_with_schema(exprs, inner.input.clone(), outer.schema.clone())
            .ok()?;
    Some(LogicalPlan::Projection(projection))
}

pub(crate) fn peel(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => peel(&alias.input),
        _ => plan,
    }
}
pub(crate) fn resolve_table<'a>(
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
/// The integer a `Limit` bound holds (`LIMIT` / `OFFSET` are expressions in
/// the plan).
fn limit_bound(expr: &Option<Box<Expr>>, what: &str) -> Result<Option<i64>> {
    match expr.as_deref().map(int_literal) {
        None => Ok(None),
        Some(Some(value)) => Ok(Some(value)),
        Some(None) => Err(unsupported(format!(
            "the {what} must be an integer literal"
        ))),
    }
}

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
pub(crate) fn plan_label(plan: &LogicalPlan) -> &'static str {
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
pub(crate) fn unsupported(what: impl std::fmt::Display) -> rootcause::Report {
    rootcause::report!("unsupported incremental view shape: {what}")
        .attach(UnsupportedShape)
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
    async fn analyzes_multi_aggregate_scalar_subqueries() {
        let tables = vec![source_table("src"), source_table("dim")];
        // WHERE: the threshold is one expression over two aggregates.
        let analyzed = analyze_multi(
            "select k, v from src s where v > \
             (select sum(u.v) + max(u.v) from dim u where u.k = s.k)",
            tables.clone(),
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            right_aggregate,
            match_predicate,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi join");
        };
        assert_eq!(right_aggregate.as_deref(), Some("(sum(v) + max(v))"));
        assert_eq!(
            normalized(match_predicate.as_deref()).as_deref(),
            Some("v > __ivm_right_agg")
        );

        // Select list: the direct value and a computed one.
        let analyzed = analyze_multi(
            "select k, \
             (select sum(u.v) + max(u.v) from dim u where u.k = s.k) as m \
             from src s",
            tables.clone(),
        )
        .await
        .unwrap();
        let ViewSpec::LeftAggregate {
            right_aggregate,
            aggregate_column,
            output_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left aggregate");
        };
        assert_eq!(right_aggregate, "(sum(v) + max(v))");
        assert_eq!(aggregate_column, "m");
        assert!(output_exprs.is_empty());

        let analyzed = analyze_multi(
            "select k, \
             (select sum(u.v) + max(u.v) from dim u where u.k = s.k) + 1 as m \
             from src s",
            tables.clone(),
        )
        .await
        .unwrap();
        let ViewSpec::LeftAggregate {
            right_aggregate,
            output_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left aggregate");
        };
        assert_eq!(right_aggregate, "(sum(v) + max(v))");
        assert_eq!(output_exprs.len(), 1);
        assert_eq!(
            normalized(Some(output_exprs[0].call.as_str())).as_deref(),
            Some("__ivm_right_agg + 1")
        );

        // HAVING: a global and a correlated lookup, both over two aggregates.
        for sql in [
            "select g, sum(v) from src group by g \
             having sum(v) > (select max(v) + min(v) from dim)",
            "select g, sum(v) from src group by g \
             having sum(v) > (select max(v) + min(v) from dim u where u.g = g)",
        ] {
            let analyzed = analyze_multi(sql, tables.clone()).await.unwrap();
            let ViewSpec::MultiAgg {
                having,
                having_lookup,
                ..
            } = analyzed.spec
            else {
                panic!("expected a multi aggregate");
            };
            assert_eq!(
                normalized(having.as_deref()).as_deref(),
                Some("sum_v > __ivm_lookup")
            );
            assert_eq!(
                having_lookup.expect("the lookup").aggregate,
                "(max(v) + min(v))"
            );
        }

        // A value over COUNT aggregates only has a non-NULL empty result, so
        // a per-key join cannot express it.
        let error = analyze_multi(
            "select k, v from src s where v > \
             (select count(u.v) + count(u.k) from dim u where u.k = s.k)",
            tables,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("several aggregates"), "{error}");
    }

    #[test]
    fn fallback_definition_hash_ignores_the_reason() {
        let first = ViewSpec::Fallback {
            view_id: "view_1".to_string(),
            statement: "select 1".to_string(),
            tables: vec!["table_src".to_string()],
            reason: "unsupported incremental view shape: one".to_string(),
            partition_scope: None,
            mv_table_id: "table_mv".to_string(),
        };
        let mut second = first.clone();
        if let ViewSpec::Fallback { reason, .. } = &mut second {
            *reason = "unsupported incremental view shape: two".to_string();
        }
        assert_eq!(
            definition_hash(&first).unwrap(),
            definition_hash(&second).unwrap()
        );
        let mut third = first.clone();
        if let ViewSpec::Fallback { statement, .. } = &mut third {
            *statement = "select 2".to_string();
        }
        assert_ne!(
            definition_hash(&first).unwrap(),
            definition_hash(&third).unwrap()
        );
    }

    #[tokio::test]
    async fn rejects_distinct_correlated_aggregates() {
        async fn optimized(ctx: &SessionContext, sql: &str) -> LogicalPlan {
            let frame = ctx.sql(sql).await.unwrap();
            ctx.state().optimize(&frame.logical_plan().clone()).unwrap()
        }
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        let dim = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        ctx.register_table("src", Arc::new(table)).unwrap();
        ctx.register_table("dim", Arc::new(dim)).unwrap();
        let tables = HashMap::from([
            ("src".to_string(), source_table("src")),
            ("dim".to_string(), source_table("dim")),
        ]);
        // The non-distinct correlated aggregates keep analyzing.
        for sql in [
            "select k, v from src s where v > (select sum(v) from dim u where u.k = s.k)",
            "select k, (select sum(v) from dim u where u.k = s.k) as m from src s",
        ] {
            let plan = optimized(&ctx, sql).await;
            assert!(analyze_select(&plan, &tables, &request()).is_ok(), "{sql}");
        }
        // A DISTINCT aggregate is rejected explicitly instead of failing on
        // the distinct rewrite's internals later.
        for sql in [
            "select k, v from src s where v > (select sum(DISTINCT v) from dim u where u.k = s.k)",
            "select k, v from src s where v > (select avg(DISTINCT v) from dim u where u.k = s.k)",
            "select k, (select sum(DISTINCT v) from dim u where u.k = s.k) as m from src s",
            "select k, (select avg(DISTINCT v) from dim u where u.k = s.k) as m from src s",
            "select k, (select count(DISTINCT v) from dim u where u.k = s.k) as m from src s",
        ] {
            let plan = optimized(&ctx, sql).await;
            let Err(error) = analyze_select(&plan, &tables, &request()) else {
                panic!("{sql} should be rejected");
            };
            let error = error.to_string();
            assert!(error.contains("DISTINCT"), "{sql}: {error}");
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
        // HAVING mixing aggregate kinds falls back to the multi-aggregate
        // view (the mixed statement above is already asserted).
        assert!(matches!(
            analyze("select g, sum(v) from src group by g having max(v) > 0")
                .await
                .unwrap()
                .spec,
            ViewSpec::MultiAgg { .. }
        ));
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
        // A join whose select list takes no payload from either side cannot
        // be materialized.
        assert!(
            analyze_optimized("select a.k from src a left join src b on a.k = b.k")
                .await
                .is_err()
        );
        // A null-safe join needs plain columns as its keys: a computed key
        // would be compared null-safely too.
        assert!(
            analyze_optimized(
                "select a.k, a.v, b.v from src a join src b \
                 on a.v + 1 is not distinct from b.v"
            )
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
