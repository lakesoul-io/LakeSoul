// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The `INTERSECT`/`EXCEPT` analyzer.

use super::*;

/// `INTERSECT`/`EXCEPT` and null-aware semi/anti predicates.
///
/// These plan as null-aware joins. A NULL-capable join column is matched
/// null-safely (`NULL` matches `NULL`), and the `ALL` variants count the
/// matches on both sides. The match counting still coincides with the
/// semi/anti semantics when the left rows are unique on the join columns
/// (their primary key is covered), which is the subset maintained here;
/// everything else is rejected.
pub(super) fn analyze_set_operation(
    join: &Join,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let anti = match join.join_type {
        JoinType::LeftSemi => false,
        JoinType::LeftAnti => true,
        _ => {
            return Err(unsupported(
                "a null-aware join predicate is only maintained for semi/anti joins",
            ));
        }
    };
    if join.filter.is_some() {
        return Err(unsupported("INTERSECT/EXCEPT with a residual condition"));
    }
    // The distinct variants wrap the left side in a distinct aggregate.
    let left_source = peel(&join.left);
    let left_plan = if let LogicalPlan::Aggregate(aggregate) = left_source {
        if !aggregate.aggr_expr.is_empty() {
            return Err(unsupported("INTERSECT/EXCEPT with aggregates on the left"));
        }
        peel(&aggregate.input)
    } else {
        left_source
    };
    let left_input = join_input(left_plan, tables)?;
    let right_input = join_input(&join.right, tables)?;
    let left = left_input.table;
    let right = right_input.table;
    let (left_alias, right_alias) =
        (left_input.alias.as_deref(), right_input.alias.as_deref());
    let mut join_keys = Vec::new();
    for (left_expr, right_expr) in &join.on {
        let left_column = column_of(left_expr)
            .ok_or_else(|| unsupported("set-operation keys must be plain columns"))?;
        let right_column = column_of(right_expr)
            .ok_or_else(|| unsupported("set-operation keys must be plain columns"))?;
        if left_column.name != right_column.name {
            return Err(unsupported(
                "set-operation keys must share their names on both sides",
            ));
        }
        join_keys.push(left_column.name.clone());
    }
    join_keys.sort();
    join_keys.dedup();
    if join_keys.is_empty() {
        return Err(unsupported("a set operation needs at least one key"));
    }
    if left.primary_keys.is_empty() {
        return Err(unsupported("a set operation needs a keyed left source"));
    }
    for key in &left.primary_keys {
        if !join_keys.contains(key) {
            return Err(unsupported(
                "a set operation left rows must be unique on the join columns",
            ));
        }
    }
    // A NULL-capable join column makes the join null-aware: NULL matches
    // NULL, exactly as the set operations do.
    let mut null_safe = false;
    for key in &join_keys {
        let left_field = left.schema.field_with_name(key).map_err(|_| {
            unsupported(format!("set-operation key {key} is not in the left source"))
        })?;
        let right_field = right.schema.field_with_name(key).map_err(|_| {
            unsupported(format!(
                "set-operation key {key} is not in the right source"
            ))
        })?;
        null_safe = null_safe || left_field.is_nullable() || right_field.is_nullable();
    }
    // The peeled distinct must not have grouped on extra columns: the
    // projected tuples must be the distinct ones.
    if let LogicalPlan::Aggregate(aggregate) = left_source {
        let mut distinct = Vec::new();
        for expr in &aggregate.group_expr {
            let column = column_of(expr).ok_or_else(|| {
                unsupported("set-operation distinct keys must be plain columns")
            })?;
            distinct.push(column.name.clone());
        }
        distinct.sort();
        distinct.dedup();
        if distinct != join_keys {
            return Err(unsupported(
                "a set operation over a distinct projection of other columns",
            ));
        }
    }
    let output_columns =
        semi_anti_output_columns(join, projection, left_alias, right_alias, left, right)?;
    Ok(ViewSpec::SemiAnti {
        view_id: request.view_id.clone(),
        left_table_id: left.table_id.clone(),
        right_table_id: right.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        join_keys,
        conditions: Vec::new(),
        output_columns,
        anti,
        left_filter: left_input.filter.clone(),
        right_filter: right_input.filter.clone(),
        null_safe,
        right_aggregate: None,
        right_keys: Vec::new(),
        match_predicate: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::test_helpers::*;

    #[tokio::test]
    async fn analyzes_set_operations() {
        // INTERSECT/EXCEPT are maintained when NULL can never appear in a
        // join column and the left rows are unique per join tuple: the
        // semi/anti semantics then coincide with the set operations.
        for (sql, anti) in [
            ("select k, v from src intersect select k, v from dim", false),
            ("select k, v from src except select k, v from dim", true),
            (
                "select k, v from src intersect all select k, v from dim",
                false,
            ),
            ("select k, v from src except all select k, v from dim", true),
        ] {
            let analyzed =
                analyze_multi(sql, vec![source_table("src"), source_table("dim")])
                    .await
                    .unwrap();
            let ViewSpec::SemiAnti {
                join_keys,
                output_columns,
                anti: analyzed_anti,
                null_safe,
                ..
            } = analyzed.spec
            else {
                panic!("expected a semi/anti spec");
            };
            assert_eq!(join_keys, vec!["k".to_string(), "v".to_string()]);
            assert_eq!(output_columns, vec!["k".to_string(), "v".to_string()]);
            assert_eq!(analyzed_anti, anti);
            assert!(!null_safe);
        }

        // A null-aware EXISTS over non-nullable keys is the same semi join.
        let analyzed = analyze_multi(
            "select k from src where exists (select 1 from dim \
             where dim.k is not distinct from src.k)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti { anti, .. } = analyzed.spec else {
            panic!("expected a semi/anti spec");
        };
        assert!(!anti);

        // A NULL-capable join column makes the view null-safe.
        let mut nullable = source_table("dim");
        nullable.schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let analyzed = analyze_multi(
            "select k, v from src intersect select k, v from dim",
            vec![source_table("src"), nullable],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti { null_safe, .. } = analyzed.spec else {
            panic!("expected a semi/anti spec");
        };
        assert!(null_safe);
    }

    #[tokio::test]
    async fn rejects_unsafe_set_operations() {
        // The left rows must be unique on the join columns.
        assert!(
            analyze_multi(
                "select g from src intersect select g from dim",
                vec![source_table("src"), source_table("dim")],
            )
            .await
            .is_err()
        );
        // A distinct over extra columns is not the projected distinct.
        assert!(
            analyze_multi(
                "select k from (select distinct k, v from src) t \
                 intersect select k from dim",
                vec![source_table("src"), source_table("dim")],
            )
            .await
            .is_err()
        );
        // A null-aware inner join is not a semi/anti join.
        assert!(
            analyze_multi(
                "select a.k, b.v from src a join dim b \
                 on a.k is not distinct from b.k",
                vec![source_table("src"), source_table("dim")],
            )
            .await
            .is_err()
        );
    }
}
