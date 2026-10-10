// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The `UNION ALL` / `UNION` (distinct) analyzer.

use super::*;

/// A `UNION ALL` of sources with identical schemas.
pub(super) fn analyze_union(
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
pub(super) fn analyze_union_distinct(
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
    let mut scalars = Vec::new();
    let (source, output_columns, output_exprs, filter) =
        collect_row(plan, tables, &mut scalars)?;
    if !scalars.is_empty() {
        return Err(unsupported("a scalar subquery inside a UNION branch"));
    }
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
    let mut scalars = Vec::new();
    let (source, output_columns, output_exprs, filter) =
        collect_row(plan, tables, &mut scalars)?;
    if !scalars.is_empty() {
        return Err(unsupported("a scalar subquery inside a UNION branch"));
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::test_helpers::*;

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
    async fn analyzes_union_projections() {
        // A UNION ALL branch over an append-only source is rejected: views
        // need keyed sources.
        let mut keyless = source_table("a");
        keyless.primary_keys.clear();
        assert!(
            analyze_multi(
                "select k as id, upper(g) as name from a \
                 union all select k as id, g as name from b",
                vec![keyless, source_table("b")],
            )
            .await
            .is_err()
        );

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
}
