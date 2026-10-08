// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The row projection / `SELECT DISTINCT` analyzer.

use super::*;

/// Plain `SELECT DISTINCT`: a grouping without aggregate functions, which the
/// count-only sum/count shape maintains (`count_v = 0` drops the group).
pub(super) fn analyze_distinct_rows(
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
pub(super) fn analyze_row(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let mut scalars = Vec::new();
    let (source, output_columns, output_exprs, filter) =
        collect_row(plan, tables, &mut scalars)?;
    if !scalars.is_empty() && source.primary_keys.is_empty() {
        return Err(unsupported(
            "a scalar subquery in a filter needs a keyed source",
        ));
    }
    Ok(ViewSpec::Row {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        output_columns: output_columns.unwrap_or_default(),
        output_exprs,
        filter,
        scalar: if scalars.is_empty() {
            None
        } else {
            Some(RowScalarSpec { tables: scalars })
        },
    })
}

pub(super) fn collect_row(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    scalars: &mut Vec<ScalarTableSpec>,
) -> Result<RowParts> {
    match peel(plan) {
        LogicalPlan::Projection(projection) => {
            let (source, _, _, filter) = collect_row(&projection.input, tables, scalars)?;
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
                        if contains_scalar_subquery(other) {
                            return Err(unsupported(
                                "a scalar subquery in the select list",
                            ));
                        }
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
                collect_row(&filter.input, tables, scalars)?;
            let filter = Some(combine_filter(
                previous,
                render_scalar_filter(&filter.predicate, tables, scalars)?,
            ));
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
                scalar_scan_filter(&scan.filters, tables, scalars)?,
            ))
        }
        other => Err(unsupported(format!("row shape over {}", plan_label(other)))),
    }
}

/// Whether an expression contains an uncorrelated scalar subquery.
fn contains_scalar_subquery(expr: &Expr) -> bool {
    expr.exists(|node| Ok(matches!(node, Expr::ScalarSubquery(_))))
        .unwrap_or(false)
}

/// The predicates DataFusion pushed into a table scan, with scalar subqueries.
fn scalar_scan_filter(
    filters: &[Expr],
    tables: &HashMap<String, IvmTable>,
    scalars: &mut Vec<ScalarTableSpec>,
) -> Result<Option<String>> {
    let mut filter: Option<String> = None;
    for predicate in filters {
        filter = Some(combine_filter(
            filter,
            render_scalar_filter(predicate, tables, scalars)?,
        ));
    }
    Ok(filter)
}

/// Render a filter whose conjuncts may compare against an uncorrelated scalar
/// subquery (`v > (SELECT AVG(v) FROM dim)`).
fn render_scalar_filter(
    expr: &Expr,
    tables: &HashMap<String, IvmTable>,
    scalars: &mut Vec<ScalarTableSpec>,
) -> Result<String> {
    let mut terms = Vec::new();
    for conjunct in split_conjunction(expr) {
        terms.push(render_scalar_term(conjunct, tables, scalars)?);
    }
    Ok(terms.join(" AND "))
}

/// One filter conjunct: either a plain predicate or a comparison against one
/// scalar subquery.
fn render_scalar_term(
    expr: &Expr,
    tables: &HashMap<String, IvmTable>,
    scalars: &mut Vec<ScalarTableSpec>,
) -> Result<String> {
    if !contains_scalar_subquery(expr) {
        return render_filter(expr);
    }
    let Expr::BinaryExpr(binary) = expr else {
        return Err(unsupported(
            "a scalar subquery is only maintained in a comparison or a conjunction",
        ));
    };
    let operator = match binary.op {
        Operator::Eq => "=",
        Operator::NotEq => "<>",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => {
            return Err(unsupported(
                "a scalar subquery comparison must be =, <>, <, <=, > or >=",
            ));
        }
    };
    match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::ScalarSubquery(subquery), other) if !contains_scalar_subquery(other) => {
            Ok(format!(
                "({}) {operator} {}",
                render_scalar_subquery(subquery, tables, scalars)?,
                render_filter(other)?
            ))
        }
        (other, Expr::ScalarSubquery(subquery)) if !contains_scalar_subquery(other) => {
            Ok(format!(
                "{} {operator} ({})",
                render_filter(other)?,
                render_scalar_subquery(subquery, tables, scalars)?
            ))
        }
        _ => Err(unsupported(
            "a scalar subquery is only maintained standalone in a comparison",
        )),
    }
}

/// Render one scalar subquery to self-contained SQL over its tables, which the
/// refresh registers under the names the SQL refers to.
fn render_scalar_subquery(
    subquery: &Subquery,
    tables: &HashMap<String, IvmTable>,
    scalars: &mut Vec<ScalarTableSpec>,
) -> Result<String> {
    if !subquery.outer_ref_columns.is_empty() {
        return Err(unsupported("a correlated scalar subquery"));
    }
    let inner = peel(&subquery.subquery);
    let LogicalPlan::Aggregate(aggregate) = inner else {
        return Err(unsupported(format!(
            "a scalar subquery over {} (a global aggregate only)",
            plan_label(inner)
        )));
    };
    if !aggregate.group_expr.is_empty() {
        return Err(unsupported("a scalar subquery with GROUP BY"));
    }
    let mut scans = Vec::new();
    subquery
        .subquery
        .apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                scans.push(scan.table_name.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| unsupported(format!("a scalar subquery {error}")))?;
    for name in scans {
        let source = resolve_table(tables, &name)?;
        if !scalars
            .iter()
            .any(|table| table.table_id == source.table_id)
        {
            scalars.push(ScalarTableSpec {
                name: name.to_string(),
                table_id: source.table_id.clone(),
            });
        }
    }
    let statement = datafusion::sql::unparser::plan_to_sql(&subquery.subquery)
        .map_err(|error| unsupported(format!("a scalar subquery {error}")))?;
    Ok(statement.to_string())
}

/// The aggregate family: SUM/COUNT, MIN/MAX and COUNT(DISTINCT)/SUM(DISTINCT).
/// A projection of plain columns and the scalar aliases the optimizer hoists
/// above the scan: `CAST(column)` casts and shared aggregate `FILTER`
/// predicates.
pub(super) fn is_scalar_projection(projection: &Projection) -> bool {
    projection.expr.iter().all(|expr| match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::Cast(cast) => matches!(cast.expr.as_ref(), Expr::Column(_)),
            hoisted => validate_filter(hoisted).is_ok(),
        },
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::test_helpers::*;

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
    async fn analyzes_scalar_subquery_filter() {
        let dim = source_table("dim");
        let analyzed = analyze_multi(
            "select k, v from src where v > (select avg(v) from dim)",
            vec![source_table("src"), dim.clone()],
        )
        .await
        .unwrap();
        let ViewSpec::Row { filter, scalar, .. } = analyzed.spec else {
            panic!("expected a row spec");
        };
        let raw = filter.as_deref().unwrap();
        assert!(raw.contains("avg("), "{raw}");
        assert!(raw.contains("dim"), "{raw}");
        let filter = normalized(filter.as_deref()).unwrap();
        assert!(filter.contains(" > "), "{filter}");
        let scalar = scalar.expect("scalar inputs");
        assert_eq!(scalar.tables.len(), 1);
        assert_eq!(scalar.tables[0].name, "dim");
        assert_eq!(scalar.tables[0].table_id, dim.table_id);

        // Two subqueries over the same table register it once.
        let analyzed = analyze_multi(
            "select k from src where v > (select avg(v) from dim) \
             and v < (select max(v) from dim)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::Row { scalar, .. } = analyzed.spec else {
            panic!("expected a row spec");
        };
        assert_eq!(scalar.expect("scalar inputs").tables.len(), 1);

        // The other comparison side, conjunctions and shared subqueries work.
        let analyzed = analyze_multi(
            "select k from src where (select max(v) from dim) < v and k > 1",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::Row { filter, scalar, .. } = analyzed.spec else {
            panic!("expected a row spec");
        };
        let filter = normalized(filter.as_deref()).unwrap();
        assert!(filter.contains(" < v"), "{filter}");
        assert!(filter.contains("k > 1"), "{filter}");
        assert_eq!(scalar.expect("scalar inputs").tables.len(), 1);

        // Rejections: GROUP BY, a non-aggregate subquery, a select-list
        // subquery and a correlated subquery (which plans as a semi join).
        for sql in [
            "select k from src where v > (select avg(v) from dim group by k)",
            "select k from src where v > (select v from dim)",
            "select k, (select max(v) from dim) as m from src",
            "select k from src s where v > (select avg(u.v) from dim u where u.k = s.k)",
        ] {
            assert!(
                analyze_multi(sql, vec![source_table("src"), source_table("dim")])
                    .await
                    .is_err(),
                "{sql}"
            );
        }
    }
}
