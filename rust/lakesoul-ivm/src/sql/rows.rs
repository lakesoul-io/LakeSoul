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

pub(super) fn collect_row(
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
}
