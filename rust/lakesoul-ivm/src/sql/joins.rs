// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The pair-join analyzer: inner, lookup `LEFT`, pair-keyed left/right/full,
//! cross and the semi/anti arms.

use super::*;

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
pub(super) fn analyze_join(
    join: &Join,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    // `INTERSECT`/`EXCEPT` plan as null-aware joins (a NULL row matches a
    // NULL row), and `IS NOT DISTINCT FROM` predicates are null-aware too.
    // The set operations need the per-row match counts and stay in the
    // semi/anti analyzer; a single two-source inner or outer join is
    // maintained with null-safe keys.  Every other null-aware shape (chains,
    // computed keys) is rejected instead of silently producing different
    // rows.
    let null_safe = join.null_equality == NullEquality::NullEqualsNull;
    if null_safe
        && !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
        )
    {
        return analyze_set_operation(join, projection, tables, request);
    }
    // A correlated scalar subquery plans as a left-semi join against one
    // aggregate row per correlated key.
    if let Some((aggregate, _)) = aggregate_input(&join.right) {
        // The correlated `HAVING` form: a semi join between the grouped
        // source and the per-key lookup aggregate.
        if join.join_type == JoinType::LeftSemi
            && let LogicalPlan::Aggregate(left_aggregate) = peel(&join.left)
        {
            return analyze_having_join(
                join,
                left_aggregate,
                aggregate,
                projection,
                tables,
                request,
            );
        }
    }
    if let Some((aggregate, subquery_projection)) = aggregate_input(&join.right) {
        return match join.join_type {
            JoinType::LeftSemi => analyze_correlated_scalar(
                join,
                aggregate,
                subquery_projection,
                projection,
                tables,
                request,
            ),
            JoinType::Left => {
                let Some(projection) = projection else {
                    return Err(unsupported(
                        "a select-list scalar subquery needs its projection",
                    ));
                };
                analyze_left_aggregate(
                    join,
                    aggregate,
                    subquery_projection,
                    projection,
                    tables,
                    request,
                )
            }
            _ => Err(unsupported(
                if subtree_has_distinct_aggregate(&join.right) {
                    "a correlated scalar subquery with a DISTINCT aggregate"
                } else {
                    "a join against an aggregate input (a correlated scalar subquery) \
                 is only maintained as a WHERE comparison or a select-list value"
                },
            )),
        };
    }
    // Three or more joined tables flatten into one chain: an outer step is a
    // left-deep lookup chain, anything else an inner chain.
    if join_tree_has_multiple_sources(join) {
        if join_tree_has_null_safe_step(join) {
            return Err(unsupported(
                "null-safe join keys are only maintained for a single inner join",
            ));
        }
        if join.join_type != JoinType::Inner
            || join_tree_has_outer_step(&join.left)
            || join_tree_has_outer_step(&join.right)
        {
            return analyze_lookup_chain(join, projection, tables, request);
        }
        return analyze_multi_join(join, projection, tables, request);
    }
    // `FROM a, b` / `CROSS JOIN` plans as an inner join without an `ON`
    // clause; a residual filter would need a non-equi condition and stays
    // unsupported.
    if join.on.is_empty() && join.join_type == JoinType::Inner {
        // A residual filter is a cross-side predicate over the pair payloads.
        return analyze_cross_join(join, projection, tables, request);
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
    // are supported by the lookup join.  A key that is not a plain column is
    // a rendered expression evaluated on each side (`ON a.x + 1 = b.y`).
    let mut key_pairs = Vec::new();
    let mut expression_pairs: Vec<(String, String)> = Vec::new();
    let mut conditions = Vec::new();
    for (left_expr, right_expr) in &join.on {
        match (column_of(left_expr), column_of(right_expr)) {
            (Some(left_column), Some(right_column)) => {
                key_pairs.push((left_column.name.clone(), right_column.name.clone()));
            }
            _ => expression_pairs.push((
                render_expression(left_expr)?,
                render_expression(right_expr)?,
            )),
        }
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
                if null_safe {
                    // The extracted `=` keeps its strict (NULL-rejecting)
                    // semantics; the maintained join compares every key
                    // null-safely, so this shape is rejected instead of
                    // silently matching more rows.
                    return Err(unsupported(
                        "a null-safe join with an extra equality condition in \
                         the residual filter is not supported",
                    ));
                }
                key_pairs.push((left_column.name.clone(), right_column.name.clone()));
            } else if let Expr::IsNotTrue(inner) = conjunct {
                // `(x op y) IS NOT TRUE`: the match condition of an `ALL`
                // quantifier (a false or unknown comparison disqualifies the
                // left row).
                let (left_column, right_column, op) =
                    column_compare(inner, left_alias, right_alias, left, right)?;
                conditions.push(SemiAntiCondition {
                    left_column,
                    right_column,
                    op,
                    is_not_true: true,
                });
            } else {
                let (left_column, right_column, op) =
                    column_compare(conjunct, left_alias, right_alias, left, right)?;
                conditions.push(SemiAntiCondition {
                    left_column,
                    right_column,
                    op,
                    is_not_true: false,
                });
            }
        }
    }
    if key_pairs.is_empty() && expression_pairs.is_empty() {
        // A theta semi/anti join (the maintained `x op ANY/ALL` quantifiers)
        // has no equality key at all: the condition is the whole predicate.
        let theta_semi_anti = !conditions.is_empty()
            && matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti);
        if !theta_semi_anti {
            return Err(unsupported("join without an equality key"));
        }
    }
    if null_safe && !expression_pairs.is_empty() {
        // A computed key would be compared null-safely too; only plain
        // columns are maintained for now.
        return Err(unsupported(
            "a null-safe join needs plain columns as its keys",
        ));
    }
    key_pairs.sort();
    key_pairs.dedup();
    let mut join_keys = key_pairs
        .iter()
        .map(|(left, _)| left.clone())
        .collect::<Vec<_>>();
    let mut right_keys = key_pairs
        .iter()
        .map(|(_, right)| right.clone())
        .collect::<Vec<_>>();
    let mut key_exprs = JoinKeyExprs::default();
    if !expression_pairs.is_empty() {
        // `key_exprs` is positional over `join_keys`: the plain keys that come
        // first get an empty entry.
        let plain = join_keys.len();
        let mut left_exprs = vec![String::new(); plain];
        let mut right_exprs = vec![String::new(); plain];
        for (index, (left_expr, right_expr)) in expression_pairs.iter().enumerate() {
            let (left_type, _) =
                crate::runtime::expression_type(&left.schema, left_expr)?;
            let (right_type, _) =
                crate::runtime::expression_type(&right.schema, right_expr)?;
            if left_type != right_type {
                return Err(unsupported(format!(
                    "join key expressions {left_expr:?} and {right_expr:?} have different types"
                )));
            }
            let name = join_key_expression_name(index);
            join_keys.push(name.clone());
            right_keys.push(name);
            left_exprs.push(left_expr.clone());
            right_exprs.push(right_expr.clone());
        }
        key_exprs = JoinKeyExprs {
            left: left_exprs,
            right: right_exprs,
        };
    }
    let same_names = join_keys == right_keys;

    match join.join_type {
        JoinType::Inner => {
            // Differently named keys join on both names but output the left
            // ones; the right names are not payloads either.
            let mut key_names = join_keys.clone();
            for key in &right_keys {
                if !key_names.contains(key) {
                    key_names.push(key.clone());
                }
            }
            let payloads = match projection {
                Some(projection) => join_payloads(
                    Some(projection),
                    left_alias,
                    right_alias,
                    left,
                    right,
                    &join_keys,
                    &right_keys,
                )?,
                // The optimizer drops the projection when the pruned join
                // output is exactly the select list.
                None => join_payloads_from_fields(
                    &join.schema,
                    left_alias,
                    right_alias,
                    left,
                    right,
                    &join_keys,
                    &right_keys,
                )?,
            };
            let right_keys = if same_names { Vec::new() } else { right_keys };
            // A single-sided inner join whose right side is keyed by something
            // other than the join keys is a pair join: several matches
            // duplicate the left row, which the semi shape a membership test
            // compiles to cannot express.  The right row identity becomes the
            // right payload, so its pairs stay apart.
            let effective_right_keys = if right_keys.is_empty() {
                &join_keys
            } else {
                &right_keys
            };
            let right_is_keyed = right.primary_keys.len() == effective_right_keys.len()
                && right
                    .primary_keys
                    .iter()
                    .all(|key| effective_right_keys.contains(key));
            let mut payloads = payloads;
            let synthetic = if payloads.left_count > 0
                && payloads.right_count == 0
                && !right_is_keyed
            {
                right
                    .primary_keys
                    .iter()
                    .find(|key| !effective_right_keys.contains(key))
                    .or_else(|| right.primary_keys.first())
                    .cloned()
            } else {
                None
            };
            if let Some(column) = synthetic {
                payloads.push(Side::Right, column, "right_value".to_string());
            }
            // One payload per side keeps the compact `left_value` /
            // `right_value` shape; anything else materializes the selected
            // columns under their select names.  A non-equality condition over
            // a column the select list does not carry materializes that column
            // as a hidden payload too, so the pair filter can compare it.
            let compact = payloads.left_count == 1
                && payloads.right_count == 1
                && conditions.iter().all(|condition| {
                    pair_compatible(
                        condition,
                        payloads.left_value.as_deref().expect("one left payload"),
                        payloads.right_value.as_deref().expect("one right payload"),
                    )
                });
            let (left_value, right_value, output_columns, pair_filter, payload_names) =
                if compact {
                    let left_value =
                        payloads.left_value.clone().expect("one left payload");
                    let right_value =
                        payloads.right_value.clone().expect("one right payload");
                    let pair_filter = render_pair_conditions(
                        &conditions,
                        &left_value,
                        &right_value,
                        "inner join",
                    )?;
                    let names =
                        compact_payload_names(&payloads, &left_value, &right_value);
                    (left_value, right_value, Vec::new(), pair_filter, names)
                } else {
                    if payloads.left_count == 0 || payloads.right_count == 0 {
                        // A single-sided inner join is a membership test: with
                        // the right side keyed by the join keys the match is
                        // unique, so the view is exactly the left rows that
                        // have one - the semi shape, which the semi/anti view
                        // already maintains.
                        if payloads.left_count > 0 && right_is_keyed {
                            let output_columns = semi_anti_output_columns(
                                join,
                                projection,
                                left_alias,
                                right_alias,
                                left,
                                right,
                            )?;
                            return Ok(ViewSpec::SemiAnti {
                                view_id: request.view_id.clone(),
                                left_table_id: left.table_id.clone(),
                                right_table_id: right.table_id.clone(),
                                mv_table_id: request.mv_table_id.clone(),
                                join_keys: join_keys.clone(),
                                conditions,
                                output_columns,
                                anti: false,
                                left_filter,
                                right_filter,
                                null_safe: false,
                                count_mode: false,
                                right_aggregate: None,
                                right_keys: Vec::new(),
                                match_predicate: None,
                            });
                        }
                        return Err(unsupported(
                            "a join view needs at least one column from each side",
                        ));
                    }
                    let left_value =
                        payloads.left_value.clone().expect("one left payload");
                    let right_value =
                        payloads.right_value.clone().expect("one right payload");
                    let mut output_columns = payloads.output_columns;
                    materialize_condition_columns(&mut output_columns, &conditions);
                    check_wide_output_names(&output_columns, &key_names)?;
                    let pair_filter = render_wide_pair_conditions(
                        &conditions,
                        &output_columns,
                        "inner join",
                    )?;
                    (
                        left_value,
                        right_value,
                        output_columns,
                        pair_filter,
                        Vec::new(),
                    )
                };
            Ok(ViewSpec::Join {
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                output_table_id: request.mv_table_id.clone(),
                join_keys,
                right_keys,
                key_exprs,
                left_value,
                right_value,
                output_columns,
                payload_names,
                left_filter,
                right_filter,
                pair_filter,
                null_safe,
            })
        }
        JoinType::Left => {
            if !conditions.is_empty() {
                return Err(unsupported("left join with non-equality conditions"));
            }
            analyze_outer_join(
                "LEFT",
                &join.schema,
                projection,
                left,
                right,
                left_alias,
                right_alias,
                join_keys,
                right_keys,
                &key_exprs,
                left_filter,
                right_filter,
                null_safe,
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
                &join.schema,
                projection,
                right,
                left,
                right_alias,
                left_alias,
                right_keys,
                join_keys,
                &JoinKeyExprs {
                    left: key_exprs.right.clone(),
                    right: key_exprs.left.clone(),
                },
                right_filter,
                left_filter,
                null_safe,
                request,
            )
        }
        JoinType::Full => {
            if !conditions.is_empty() {
                return Err(unsupported("full join with non-equality conditions"));
            }
            if left.primary_keys.is_empty() || right.primary_keys.is_empty() {
                return Err(unsupported("FULL JOIN needs a primary key on both sources"));
            }
            let right_keys = if same_names { Vec::new() } else { right_keys };
            // The join keys, under either name, never become payloads.
            let mut key_names = join_keys.clone();
            for key in &right_keys {
                if !key_names.contains(key) {
                    key_names.push(key.clone());
                }
            }
            let right_key_list = if right_keys.is_empty() {
                &join_keys
            } else {
                &right_keys
            };
            let payloads = match projection {
                Some(projection) => join_payloads(
                    Some(projection),
                    left_alias,
                    right_alias,
                    left,
                    right,
                    &join_keys,
                    right_key_list,
                )?,
                None => join_payloads_from_fields(
                    &join.schema,
                    left_alias,
                    right_alias,
                    left,
                    right,
                    &join_keys,
                    right_key_list,
                )?,
            };
            let (left_value, right_value, output_columns, payload_names) =
                compact_or_wide(payloads, &key_names, "FULL JOIN")?;
            Ok(ViewSpec::FullJoin {
                identity_column: false,
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                output_table_id: request.mv_table_id.clone(),
                join_keys,
                right_keys,
                key_exprs: key_exprs.clone(),
                left_value,
                right_value,
                output_columns,
                payload_names,
                left_filter,
                right_filter,
                null_safe,
            })
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            let output_columns = semi_anti_output_columns(
                join,
                projection,
                left_alias,
                right_alias,
                left,
                right,
            )?;
            Ok(ViewSpec::SemiAnti {
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                mv_table_id: request.mv_table_id.clone(),
                join_keys,
                conditions,
                output_columns,
                anti: join.join_type == JoinType::LeftAnti,
                left_filter,
                right_filter,
                null_safe: false,
                count_mode: false,
                right_aggregate: None,
                right_keys: Vec::new(),
                match_predicate: None,
            })
        }
        other => Err(unsupported(format!("join type {other:?}"))),
    }
}

/// The table of one join input, plus its alias when it has one.
/// A `CROSS JOIN` of two keyed sources: every pair of rows, keyed by both row
/// identities.
/// The left columns a semi/anti join materializes: the plain left columns the
/// projection selects, or the left input's schema when the optimizer pruned
/// the projection away.
/// The aggregate node and its projection of a join input that is a derived
/// aggregate (`SubqueryAlias -> Projection -> Aggregate`).
/// Whether an expression is a `DISTINCT` aggregate.
pub(super) fn is_distinct_aggregate(expr: &Expr) -> bool {
    matches!(expr, Expr::AggregateFunction(function) if function.params.distinct)
}

/// Whether a plan's subtree contains a `DISTINCT` aggregate, either written
/// directly or in the dedup group-by the optimizer splits a lone `DISTINCT`
/// aggregate into (an aggregate with no aggregate functions feeding the
/// aggregate that consumes its output).
pub(super) fn subtree_has_distinct_aggregate(plan: &LogicalPlan) -> bool {
    fn walk(plan: &LogicalPlan, under_aggregate: bool) -> bool {
        if let LogicalPlan::Aggregate(aggregate) = plan {
            if aggregate.aggr_expr.iter().any(is_distinct_aggregate)
                || (under_aggregate && aggregate.aggr_expr.is_empty())
            {
                return true;
            }
            return walk(&aggregate.input, true);
        }
        plan.inputs()
            .iter()
            .any(|input| walk(input, under_aggregate))
    }
    walk(plan, false)
}

fn aggregate_input(plan: &LogicalPlan) -> Option<(&Aggregate, &Projection)> {
    let plan = peel(plan);
    let LogicalPlan::Projection(projection) = plan else {
        return None;
    };
    let inner = peel(&projection.input);
    let LogicalPlan::Aggregate(aggregate) = inner else {
        return None;
    };
    Some((aggregate, projection))
}

/// A correlated scalar subquery: `WHERE v OP (SELECT AGG(w) FROM dim u WHERE
/// u.k = s.k)` is maintained as a semi join against one aggregate row per
/// correlated key.
fn analyze_correlated_scalar(
    join: &Join,
    aggregate: &Aggregate,
    subquery_projection: &Projection,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let filter = join
        .filter
        .as_ref()
        .ok_or_else(|| unsupported("a scalar subquery comparison needs a filter"))?;
    if aggregate.aggr_expr.len() != 1 {
        return Err(unsupported("a scalar subquery with several aggregates"));
    }
    if subtree_has_distinct_aggregate(&join.right) {
        return Err(unsupported(
            "a correlated scalar subquery with a DISTINCT aggregate",
        ));
    }
    validate_decimal_accumulators(aggregate)?;
    // The correlated keys are the aggregate's group columns.
    let mut right_keys = Vec::new();
    for expr in &aggregate.group_expr {
        let column = column_of(expr).ok_or_else(|| {
            unsupported("a correlated scalar subquery needs plain key columns")
        })?;
        right_keys.push(column.name.clone());
    }
    if right_keys.is_empty() {
        return Err(unsupported("an uncorrelated scalar subquery"));
    }
    // The subquery returns one column: the projection field that is not a key.
    let mut aggregate_columns = Vec::new();
    for field in subquery_projection.schema.fields() {
        if !right_keys.contains(field.name()) {
            aggregate_columns.push(field.name().clone());
        }
    }
    if aggregate_columns.len() != 1 {
        return Err(unsupported("a scalar subquery must return one column"));
    }
    // The join pairs the correlated keys.
    let mut join_keys = Vec::new();
    let mut on_right_keys = Vec::new();
    let mut right_qualifier = None;
    for (left_expr, right_expr) in &join.on {
        let left_column = column_of(left_expr)
            .ok_or_else(|| unsupported("a scalar-subquery key must be a column"))?;
        let right_column = column_of(right_expr)
            .ok_or_else(|| unsupported("a scalar-subquery key must be a column"))?;
        join_keys.push(left_column.name.clone());
        on_right_keys.push(right_column.name.clone());
        if right_column.relation.is_some() {
            right_qualifier = right_column.relation.clone();
        }
    }
    if join_keys.is_empty() {
        return Err(unsupported("a scalar subquery needs a correlated key"));
    }
    let mut sorted_group = right_keys.clone();
    sorted_group.sort();
    sorted_group.dedup();
    let mut sorted_on = on_right_keys;
    sorted_on.sort();
    sorted_on.dedup();
    if sorted_group != sorted_on {
        return Err(unsupported(
            "a scalar subquery correlated on keys other than its grouping",
        ));
    }
    let left_input = join_input(&join.left, tables)?;
    let right_input = join_input(&aggregate.input, tables)?;
    let left = left_input.table;
    let right = right_input.table;
    let (left_alias, right_alias) =
        (left_input.alias.as_deref(), right_input.alias.as_deref());
    let predicate =
        render_match_predicate(filter, &aggregate_columns, right_qualifier.as_ref())?;
    let call = render_expression(&aggregate.aggr_expr[0])?;
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
        anti: false,
        left_filter: left_input.filter.clone(),
        right_filter: right_input.filter.clone(),
        null_safe: false,
        count_mode: false,
        right_aggregate: Some(call),
        right_keys,
        match_predicate: Some(predicate),
    })
}

/// The guarded COUNT comparison DataFusion decorrelates for the COUNT family:
/// a comparison against `CASE WHEN guard IS NULL THEN 0 ELSE count(...) END`
/// (the aggregate of no rows).  The rewrite returns the predicate with the
/// CASE evaluated over the maintained aggregate column — a key without
/// aggregate rows pads it with NULL, so the empty-set branch still fires —
/// plus the aggregate output column the predicate refers to.
fn rewrite_count_guard(predicate: &Expr) -> Option<(Expr, String)> {
    let mut aggregate_column: Option<String> = None;
    let rewritten = predicate
        .clone()
        .transform_down(|node| {
            if aggregate_column.is_some() {
                return Ok(Transformed::no(node));
            }
            let Expr::Case(case) = &node else {
                return Ok(Transformed::no(node));
            };
            if case.expr.is_some() || case.when_then_expr.len() != 1 {
                return Ok(Transformed::no(node));
            }
            let (when, then) = &case.when_then_expr[0];
            let Some(else_expr) = case.else_expr.as_deref() else {
                return Ok(Transformed::no(node));
            };
            // `CASE WHEN guard IS NULL THEN 0 ELSE count END` and its mirrored
            // `CASE WHEN guard IS NOT NULL THEN count ELSE 0 END`.
            let (guard, empty, value) = match (when.as_ref(), then.as_ref(), else_expr) {
                (Expr::IsNull(guard), Expr::Literal(empty, _), Expr::Column(value)) => {
                    (guard, empty, value)
                }
                (
                    Expr::IsNotNull(guard),
                    Expr::Column(value),
                    Expr::Literal(empty, _),
                ) => (guard, empty, value),
                _ => return Ok(Transformed::no(node)),
            };
            if !matches!(guard.as_ref(), Expr::Column(_)) {
                return Ok(Transformed::no(node));
            }
            aggregate_column = Some(value.name.clone());
            let aggregate = Expr::Column(Column::from_name(IVM_RIGHT_AGG_COLUMN));
            Ok(Transformed::yes(Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(Expr::IsNull(Box::new(aggregate.clone()))),
                    Box::new(Expr::Literal(empty.clone(), None)),
                )],
                Some(Box::new(aggregate)),
            ))))
        })
        .ok()?
        .data;
    aggregate_column.map(|column| (rewritten, column))
}

/// The aggregate call of a guarded COUNT comparison, the group keys that pair
/// with the correlated keys, and the plan the aggregate reads.
///
/// `COUNT(DISTINCT x)` decorrelates into a two-level aggregate (a dedup
/// aggregate under the counting one) with an always-true group guard; the
/// guard is dropped here and the counting argument resolved back to the
/// distinct expression.
fn resolve_count_aggregate<'a>(
    aggregate: &'a Aggregate,
    subquery_projection: &'a Projection,
    aggregate_column: &str,
) -> Result<Option<(String, Vec<String>, &'a LogicalPlan)>> {
    if aggregate.aggr_expr.len() != 1 {
        return Err(unsupported("a scalar subquery with several aggregates"));
    }
    if subquery_projection
        .schema
        .field_with_name(None, aggregate_column)
        .is_err()
    {
        return Ok(None);
    }
    let mut group_names = Vec::new();
    for expr in &aggregate.group_expr {
        let Some(column) = column_of(expr) else {
            return Ok(None);
        };
        group_names.push(column.name.clone());
    }
    let mut right_plan: &LogicalPlan = &aggregate.input;
    let mut call = None;
    if let LogicalPlan::Projection(inner_projection) = peel(&aggregate.input)
        && let LogicalPlan::Aggregate(dedup) = peel(&inner_projection.input)
    {
        let Expr::AggregateFunction(function) = &aggregate.aggr_expr[0] else {
            return Ok(None);
        };
        let Some(argument) = function.params.args.first().and_then(column_of) else {
            return Ok(None);
        };
        let Some(distinct) = dedup
            .group_expr
            .iter()
            .find(|expr| expr.schema_name().to_string() == argument.name)
        else {
            return Ok(None);
        };
        let distinct = match distinct {
            Expr::Alias(alias) => &alias.expr,
            other => other,
        };
        call = Some(format!("count(distinct {})", render_expression(distinct)?));
        // Drop the always-true group guard of the rewrite.
        for expr in &inner_projection.expr {
            if let Expr::Alias(alias) = expr
                && matches!(
                    alias.expr.as_ref(),
                    Expr::Literal(ScalarValue::Boolean(Some(true)), _)
                )
            {
                group_names.retain(|name| name != &alias.name);
            }
        }
        right_plan = &dedup.input;
    }
    let call = match call {
        Some(call) => call,
        None => render_expression(&aggregate.aggr_expr[0])?,
    };
    Ok(Some((call, group_names, right_plan)))
}

/// A correlated COUNT-family comparison: DataFusion wraps the aggregate in an
/// empty-set guard, so the shape arrives as a filter above a projection above
/// a left-semi join.  The maintained view is the same semi join as the plain
/// correlated scalar, with the guard evaluated over the aggregate column.
pub(super) fn try_analyze_guarded_count(
    projection: Option<&Projection>,
    filter: &Filter,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<Option<ViewSpec>> {
    let Some((predicate, aggregate_column)) = rewrite_count_guard(&filter.predicate)
    else {
        return Ok(None);
    };
    let mut node = peel(&filter.input);
    if let LogicalPlan::Projection(mid) = node
        && is_plain_projection(mid)
    {
        node = peel(&mid.input);
    }
    let LogicalPlan::Join(join) = node else {
        return Ok(None);
    };
    // The decorrelated plan is a left join with the guarded comparison left
    // in the filter above it; a semi join with the comparison pushed into its
    // filter is the same shape (and the plain correlated scalar's).
    if join.filter.is_some()
        || !matches!(join.join_type, JoinType::Left | JoinType::LeftSemi)
    {
        return Ok(None);
    }
    let Some((aggregate, subquery_projection)) = aggregate_input(&join.right) else {
        return Ok(None);
    };
    let Some((call, group_names, right_plan)) =
        resolve_count_aggregate(aggregate, subquery_projection, &aggregate_column)?
    else {
        return Ok(None);
    };
    let mut join_keys = Vec::new();
    let mut on_right_keys = Vec::new();
    let mut right_qualifier = None;
    for (left_expr, right_expr) in &join.on {
        let left_column = column_of(left_expr)
            .ok_or_else(|| unsupported("a scalar-subquery key must be a column"))?;
        let right_column = column_of(right_expr)
            .ok_or_else(|| unsupported("a scalar-subquery key must be a column"))?;
        join_keys.push(left_column.name.clone());
        on_right_keys.push(right_column.name.clone());
        if right_column.relation.is_some() {
            right_qualifier = right_column.relation.clone();
        }
    }
    if join_keys.is_empty() {
        return Err(unsupported("a scalar subquery needs a correlated key"));
    }
    let mut sorted_group = group_names.clone();
    sorted_group.sort();
    sorted_group.dedup();
    let mut sorted_on = on_right_keys.clone();
    sorted_on.sort();
    sorted_on.dedup();
    if sorted_group != sorted_on {
        return Err(unsupported(
            "a scalar subquery correlated on keys other than its grouping",
        ));
    }
    let left_input = join_input(&join.left, tables)?;
    let right_input = join_input(right_plan, tables)?;
    let left = left_input.table;
    let right = right_input.table;
    let (left_alias, right_alias) =
        (left_input.alias.as_deref(), right_input.alias.as_deref());
    let predicate = render_match_predicate(
        &predicate,
        &[aggregate_column],
        right_qualifier.as_ref(),
    )?;
    let output_columns =
        semi_anti_output_columns(join, projection, left_alias, right_alias, left, right)?;
    Ok(Some(ViewSpec::SemiAnti {
        view_id: request.view_id.clone(),
        left_table_id: left.table_id.clone(),
        right_table_id: right.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        join_keys,
        conditions: Vec::new(),
        output_columns,
        anti: false,
        left_filter: left_input.filter.clone(),
        right_filter: right_input.filter.clone(),
        null_safe: false,
        count_mode: false,
        right_aggregate: Some(call),
        right_keys: on_right_keys,
        match_predicate: Some(predicate),
    }))
}

/// The aggregate column of DataFusion's scalar-COUNT plan, if `expr` is the
/// `CASE WHEN <subquery>.__always_true IS NULL THEN 0 ELSE <aggregate> END`
/// the decorrelation builds: the else branch is the aggregate's output.
fn scalar_count_value(expr: &Expr) -> Option<&Column> {
    let Expr::Case(case) = expr else {
        return None;
    };
    if case.expr.is_some() || case.when_then_expr.len() != 1 {
        return None;
    }
    let (when, then) = &case.when_then_expr[0];
    let Expr::IsNull(marker) = &**when else {
        return None;
    };
    let Expr::Column(marker) = &**marker else {
        return None;
    };
    if marker.name != "__always_true" || !matches!(&**then, Expr::Literal(..)) {
        return None;
    }
    match case.else_expr.as_deref()? {
        Expr::Column(column) => Some(column),
        _ => None,
    }
}

/// A select-list correlated scalar subquery: `SELECT k, (SELECT AGG(w) FROM
/// dim u WHERE u.k = s.k) AS m FROM src s` is maintained as a left join
/// against one aggregate row per correlated key.
fn analyze_left_aggregate(
    join: &Join,
    aggregate: &Aggregate,
    subquery_projection: &Projection,
    projection: &Projection,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    if join.filter.is_some() {
        return Err(unsupported(
            "a select-list scalar subquery with a residual condition",
        ));
    }
    if aggregate.aggr_expr.len() != 1 {
        return Err(unsupported("a scalar subquery with several aggregates"));
    }
    if subtree_has_distinct_aggregate(&join.right) {
        // DataFusion plans `count(DISTINCT ..)` as two aggregate levels, so
        // the maintained subquery would correlate on the distinct key too.
        return Err(unsupported(
            "a correlated scalar subquery with a DISTINCT aggregate",
        ));
    }
    validate_decimal_accumulators(aggregate)?;
    // The correlated keys are the aggregate's group columns.
    let mut right_keys = Vec::new();
    for expr in &aggregate.group_expr {
        let column = column_of(expr).ok_or_else(|| {
            unsupported("a correlated scalar subquery needs plain key columns")
        })?;
        right_keys.push(column.name.clone());
    }
    if right_keys.is_empty() {
        return Err(unsupported("an uncorrelated scalar subquery"));
    }
    // The subquery returns one column: the projection field that is not a
    // key.  DataFusion's decorrelation adds helper columns (`__always_true`)
    // the maintained subquery does not materialize.
    let mut aggregate_columns = Vec::new();
    for field in subquery_projection.schema.fields() {
        if field.name().starts_with("__") {
            continue;
        }
        if !right_keys.contains(field.name()) {
            aggregate_columns.push(field.name().clone());
        }
    }
    if aggregate_columns.len() != 1 {
        return Err(unsupported("a scalar subquery must return one column"));
    }
    // The join pairs the correlated keys.
    let mut join_keys = Vec::new();
    let mut on_right_keys = Vec::new();
    let mut right_qualifier = None;
    for (left_expr, right_expr) in &join.on {
        let left_column = column_of(left_expr)
            .ok_or_else(|| unsupported("a scalar-subquery key must be a column"))?;
        let right_column = column_of(right_expr)
            .ok_or_else(|| unsupported("a scalar-subquery key must be a column"))?;
        join_keys.push(left_column.name.clone());
        on_right_keys.push(right_column.name.clone());
        if right_column.relation.is_some() {
            right_qualifier = right_column.relation.clone();
        }
    }
    if join_keys.is_empty() {
        return Err(unsupported("a scalar subquery needs a correlated key"));
    }
    let mut sorted_group = right_keys.clone();
    sorted_group.sort();
    sorted_group.dedup();
    let mut sorted_on = on_right_keys;
    sorted_on.sort();
    sorted_on.dedup();
    if sorted_group != sorted_on {
        return Err(unsupported(
            "a scalar subquery correlated on keys other than its grouping",
        ));
    }
    let left_input = join_input(&join.left, tables)?;
    let right_input = join_input(&aggregate.input, tables)?;
    let left = left_input.table;
    let right = right_input.table;
    // The outer projection: plain left columns plus the scalar value, or
    // computed expressions over the scalar value (`(SELECT AGG(w) ...) + 1`).
    let mut output_columns = Vec::new();
    let mut aggregate_column: Option<String> = None;
    let mut output_exprs = Vec::new();
    let mut count_zero = false;
    // The decorrelation names the scalar output per shape, so a reference is
    // the subquery value when it carries the right qualifier or exactly the
    // subquery's output name.
    let aggregate_name = aggregate_columns[0].clone();
    let on_right = |column: &Column| {
        column.relation.is_some()
            && (column.relation.as_ref() == right_qualifier.as_ref()
                || column.name == aggregate_name)
    };
    for (index, expr) in projection.expr.iter().enumerate() {
        let mut inner = expr;
        let mut alias = None;
        while let Expr::Alias(nested) = inner {
            if alias.is_none() {
                alias = Some(nested.name.clone());
            }
            inner = &nested.expr;
        }
        // DataFusion plans a scalar COUNT as
        // `CASE WHEN <subquery>.__always_true IS NULL THEN 0 ELSE
        // <subquery>.count(...) END`: the maintained value is the aggregate
        // and a key without right rows counts zero (the `count_zero` flag).
        let mut inner = inner.clone();
        if let Some(value) = scalar_count_value(&inner) {
            count_zero = true;
            inner = Expr::Column(value.clone());
        } else {
            inner = inner
                .transform_down(|node| {
                    if let Some(value) = scalar_count_value(&node) {
                        count_zero = true;
                        return Ok(Transformed::yes(Expr::Column(value.clone())));
                    }
                    Ok(Transformed::no(node))
                })
                .map_err(|error| {
                    unsupported(format!("a scalar subquery output {error}"))
                })?
                .data;
        }
        if let Expr::Column(column) = &inner {
            if on_right(column) {
                if aggregate_column.is_some() {
                    return Err(unsupported("the scalar subquery value appears twice"));
                }
                aggregate_column = Some(alias.unwrap_or_else(|| column.name.clone()));
            } else {
                output_columns.push(alias.unwrap_or_else(|| column.name.clone()));
            }
            continue;
        }
        // A computed output: it must compute over the scalar subquery value
        // and nothing else, and it is rendered against the runtime's raw
        // aggregate column.
        let Expr::Alias(output_alias) = expr else {
            return Err(unsupported(
                "a computed scalar subquery output needs an alias",
            ));
        };
        if !output_alias
            .name
            .chars()
            .all(|character| character.is_alphanumeric() || character == '_')
        {
            return Err(unsupported(
                "a computed scalar subquery output needs an alias",
            ));
        }
        let mut references = 0usize;
        inner
            .apply(|node| {
                if let Expr::Column(column) = node
                    && on_right(column)
                {
                    references += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .map_err(|error| unsupported(format!("a scalar subquery output {error}")))?;
        if references != 1 {
            return Err(unsupported(
                "a computed scalar subquery output must read the subquery \
                 value once (it may also read the row's own columns)",
            ));
        }
        let rewritten = inner
            .clone()
            .transform_down(|node| {
                if let Expr::Column(column) = &node
                    && on_right(column)
                {
                    return Ok(Transformed::yes(Expr::Column(Column::from_name(
                        IVM_RIGHT_AGG_COLUMN,
                    ))));
                }
                Ok(Transformed::no(node))
            })
            .map_err(|error| unsupported(format!("a scalar subquery output {error}")))?
            .data;
        let data_type = projection.schema.field(index).data_type().clone();
        output_exprs.push(LeftAggregateOutput {
            call: render_expression(&rewritten)?,
            column: output_alias.name.clone(),
            result: encode_data_type(&data_type)?,
        });
    }
    // The raw subquery value stays materialized (it is what the runtime joins
    // and sorts on); a computed-only select list names it internally.
    let aggregate_column =
        aggregate_column.unwrap_or_else(|| IVM_AGG_VALUE_COLUMN.to_string());
    for key in &left.primary_keys {
        if !output_columns.contains(key) {
            return Err(unsupported(format!(
                "a select-list scalar subquery must keep the left key {key}"
            )));
        }
    }
    let call = render_expression(&aggregate.aggr_expr[0])?;
    // A COUNT counts zero for a key without rows; every other aggregate is
    // NULL there.
    let count_zero = count_zero
        || matches!(
            &aggregate.aggr_expr[0],
            Expr::AggregateFunction(function)
                if function.func.name().eq_ignore_ascii_case("count")
        );
    Ok(ViewSpec::LeftAggregate {
        view_id: request.view_id.clone(),
        left_table_id: left.table_id.clone(),
        right_table_id: right.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        join_keys,
        right_keys,
        right_aggregate: call,
        aggregate_column,
        count_zero,
        output_columns,
        left_filter: left_input.filter.clone(),
        right_filter: right_input.filter.clone(),
        output_exprs,
    })
}

/// Render the comparison of a correlated scalar subquery: the aggregate
/// output columns become [`IVM_RIGHT_AGG_COLUMN`] and the left columns lose
/// their relation names.
fn render_match_predicate(
    filter: &Expr,
    aggregate_columns: &[String],
    right_qualifier: Option<&TableReference>,
) -> Result<String> {
    let mut right_column = None;
    filter
        .apply(|node| {
            if let Expr::Column(column) = node
                && column.relation.as_ref() == right_qualifier
                && !aggregate_columns.contains(&column.name)
                && right_column.is_none()
            {
                right_column = Some(column.name.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| unsupported(format!("a scalar subquery condition {error}")))?;
    if let Some(column) = right_column {
        return Err(unsupported(format!(
            "a correlated condition over the right column {column}"
        )));
    }
    let transformed = filter
        .clone()
        .transform_down(|node| match node {
            Expr::Column(column) if aggregate_columns.contains(&column.name) => Ok(
                Transformed::yes(Expr::Column(Column::from_name(IVM_RIGHT_AGG_COLUMN))),
            ),
            Expr::Column(column) if column.relation.is_some() => Ok(Transformed::yes(
                Expr::Column(Column::from_name(column.name.clone())),
            )),
            other => Ok(Transformed::no(other)),
        })
        .map_err(|error| unsupported(format!("a scalar subquery condition {error}")))?;
    let ast = Unparser::default()
        .expr_to_sql(&transformed.data)
        .map_err(|error| unsupported(format!("a scalar subquery condition {error}")))?;
    Ok(ast.to_string())
}

pub(super) fn semi_anti_output_columns(
    join: &Join,
    projection: Option<&Projection>,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    left: &IvmTable,
    right: &IvmTable,
) -> Result<Vec<String>> {
    match projection {
        Some(projection) => {
            if !is_plain_projection(projection) {
                return Err(unsupported("computed columns in a semi/anti join output"));
            }
            let mut columns = Vec::new();
            for expr in &projection.expr {
                let column = column_of(expr).ok_or_else(|| {
                    unsupported("semi/anti output must be plain columns")
                })?;
                let on_left = side_of(column, left_alias, right_alias, left, right)
                    == Some(Side::Left)
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
            Ok(columns)
        }
        None => Ok(join
            .left
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()),
    }
}

fn analyze_cross_join(
    join: &Join,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let left = join_input(&join.left, tables)?;
    let right = join_input(&join.right, tables)?;
    let left_filter = left.filter.clone();
    let right_filter = right.filter.clone();
    let (left, left_alias) = (left.table, left.alias.as_deref());
    let (right, right_alias) = (right.table, right.alias.as_deref());
    if left.primary_keys.is_empty() || right.primary_keys.is_empty() {
        return Err(unsupported(
            "a cross join needs primary keys on both sources",
        ));
    }
    let payloads = match projection {
        Some(projection) => join_payloads(
            Some(projection),
            left_alias,
            right_alias,
            left,
            right,
            &[],
            &[],
        )?,
        None => {
            // The optimizer drops the projection when the pruned join output
            // is exactly the select list: every column of the output belongs
            // to one side.  The join schema keeps each side's qualifier, which
            // disambiguates columns the two sides share a name with.
            let mut payloads = JoinPayloads::default();
            for (qualifier, field) in join.schema.iter() {
                let column = match qualifier {
                    Some(qualifier) => datafusion::common::Column::new(
                        Some(qualifier.clone()),
                        field.name(),
                    ),
                    None => datafusion::common::Column::new_unqualified(field.name()),
                };
                let side =
                    side_of(&column, left_alias, right_alias, left, right).ok_or_else(|| {
                        unsupported(format!(
                            "cross join output column {} is ambiguous; use distinct names \
                             or aliases",
                            field.name()
                        ))
                    })?;
                payloads.push(side, field.name().clone(), field.name().clone());
            }
            payloads
        }
    };
    let mut conditions = Vec::new();
    if let Some(filter) = &join.filter {
        for conjunct in split_conjunction(filter) {
            let (left_column, right_column, op) =
                column_compare(conjunct, left_alias, right_alias, left, right)?;
            conditions.push(SemiAntiCondition {
                left_column,
                right_column,
                op,
                is_not_true: false,
            });
        }
    }
    // Like the inner join, a cross join materializes the columns its pair
    // condition compares when the select list does not carry them.
    let compact = payloads.left_count == 1
        && payloads.right_count == 1
        && conditions.iter().all(|condition| {
            pair_compatible(
                condition,
                payloads.left_value.as_deref().expect("one left payload"),
                payloads.right_value.as_deref().expect("one right payload"),
            )
        });
    let (left_value, right_value, output_columns, pair_filter) = if compact {
        let left_value = payloads.left_value.clone().expect("one left payload");
        let right_value = payloads.right_value.clone().expect("one right payload");
        let pair_filter =
            render_pair_conditions(&conditions, &left_value, &right_value, "cross join")?;
        (left_value, right_value, Vec::new(), pair_filter)
    } else {
        if payloads.left_count == 0 || payloads.right_count == 0 {
            return Err(unsupported(
                "a cross join needs at least one column from each side",
            ));
        }
        let left_value = payloads.left_value.clone().expect("one left payload");
        let right_value = payloads.right_value.clone().expect("one right payload");
        let mut output_columns = payloads.output_columns;
        materialize_condition_columns(&mut output_columns, &conditions);
        check_wide_output_names(&output_columns, &[])?;
        let pair_filter =
            render_wide_pair_conditions(&conditions, &output_columns, "cross join")?;
        (left_value, right_value, output_columns, pair_filter)
    };
    Ok(ViewSpec::CrossJoin {
        view_id: request.view_id.clone(),
        left_table_id: left.table_id.clone(),
        right_table_id: right.table_id.clone(),
        output_table_id: request.mv_table_id.clone(),
        left_value,
        right_value,
        output_columns,
        payload_names: Vec::new(),
        left_filter,
        right_filter,
        pair_filter,
    })
}

/// An outer join that keeps every row of `left`: a lookup join when the right
/// side is keyed by the join keys (at most one match per left row), otherwise
/// a pair-keyed left join over two keyed sources.
#[allow(clippy::too_many_arguments)]
fn analyze_outer_join(
    kind: &str,
    join_schema: &datafusion::common::DFSchema,
    projection: Option<&Projection>,
    left: &IvmTable,
    right: &IvmTable,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    join_keys: Vec<String>,
    right_keys: Vec<String>,
    key_exprs: &JoinKeyExprs,
    left_filter: Option<String>,
    right_filter: Option<String>,
    null_safe: bool,
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
    let payloads = match projection {
        Some(projection) => join_payloads(
            Some(projection),
            left_alias,
            right_alias,
            left,
            right,
            &join_keys,
            &effective_right_keys,
        )?,
        None => join_payloads_from_fields(
            join_schema,
            left_alias,
            right_alias,
            left,
            right,
            &join_keys,
            &effective_right_keys,
        )?,
    };
    let lookup = {
        let mut expected = right.primary_keys.clone();
        expected.sort();
        // A computed key is materialized under a hidden name; the right side
        // is still keyed by the plain column its expression renders, so the
        // key names read through the expressions.
        let mut keys = effective_right_keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                match key_exprs
                    .right
                    .get(index)
                    .filter(|expr| !expr.is_empty())
                    .filter(|expr| right.schema.field_with_name(expr).is_ok())
                {
                    Some(expr) => expr.clone(),
                    None => key.clone(),
                }
            })
            .collect::<Vec<_>>();
        keys.sort();
        !expected.is_empty() && expected == keys
    };
    let mut payloads = payloads;
    if lookup {
        // The lookup layout materializes the left primary keys itself, so
        // selecting one does not widen the payloads.
        payloads.drop_carried_left(&left.primary_keys);
    }
    let (left_value, right_value, output_columns, payload_names) =
        compact_or_wide(payloads, &key_names, kind)?;
    if lookup {
        // The right side is keyed by the join keys, so its keys are a
        // non-nullable primary key: a NULL left key never matches and the
        // lookup needs no null-safe flag.
        Ok(ViewSpec::LookupJoin {
            view_id: request.view_id.clone(),
            left_table_id: left.table_id.clone(),
            right_table_id: right.table_id.clone(),
            output_table_id: request.mv_table_id.clone(),
            join_keys,
            right_keys,
            key_exprs: key_exprs.clone(),
            left_filter,
            right_filter,
            left_value,
            right_value,
            output_columns,
            payload_names,
        })
    } else if !left.primary_keys.is_empty() && !right.primary_keys.is_empty() {
        Ok(ViewSpec::LeftJoin {
            view_id: request.view_id.clone(),
            left_table_id: left.table_id.clone(),
            right_table_id: right.table_id.clone(),
            output_table_id: request.mv_table_id.clone(),
            join_keys,
            right_keys,
            key_exprs: key_exprs.clone(),
            left_value,
            right_value,
            output_columns,
            payload_names,
            identity_column: false,
            left_filter,
            right_filter,
            null_safe,
        })
    } else {
        Err(unsupported(format!(
            "{kind} JOIN needs keyed sources (or a right source keyed by the join keys)"
        )))
    }
}

/// One join input: the source table, its alias and an optional side filter.
pub(super) struct JoinInput<'a> {
    pub(super) table: &'a IvmTable,
    pub(super) alias: Option<String>,
    pub(super) filter: Option<String>,
}

pub(super) fn join_input<'a>(
    plan: &'a LogicalPlan,
    tables: &'a HashMap<String, IvmTable>,
) -> Result<JoinInput<'a>> {
    // A side filter is pushed below the join as a `Filter`; column pruning
    // may add a plain projection above it. Scan-level filters land in the
    // scan's pushed filters. The alias may sit anywhere in the chain.
    //
    // The derived marker carries the outermost projection (when there is
    // one): the step's analysis reads it for the output columns and the
    // projection stays part of the step, so the parent's references keep
    // resolving.
    let mut projection_plan: Option<&LogicalPlan> = None;
    let mut alias = None;
    let mut filter = None;
    let mut node = plan;
    loop {
        match node {
            LogicalPlan::SubqueryAlias(inner) => {
                if alias.is_none() {
                    alias = Some(inner.alias.table().to_string());
                }
                node = &inner.input;
            }
            LogicalPlan::Filter(predicate) => {
                filter =
                    Some(combine_filter(filter, render_filter(&predicate.predicate)?));
                node = &predicate.input;
            }
            LogicalPlan::Projection(projection) => {
                // A plain column projection may alias its columns
                // (`SELECT b.k AS k FROM b`), which is what a subquery over a
                // join side normally looks like.
                if !projection
                    .expr
                    .iter()
                    .all(|expr| matches!(strip_alias(expr), Expr::Column(_)))
                {
                    return Err(unsupported(
                        "join input must be a table or a plain column projection",
                    ));
                }
                if projection_plan.is_none() {
                    projection_plan = Some(node);
                }
                node = &projection.input;
            }
            LogicalPlan::TableScan(scan) => {
                if let Some(pushed) = scan_filter(&scan.filters)? {
                    filter = Some(combine_filter(filter, pushed));
                }
                let source = resolve_table(tables, &scan.table_name)?;
                return Ok(JoinInput {
                    table: source,
                    alias,
                    filter,
                });
            }
            LogicalPlan::Aggregate(_) => {
                return Err(unsupported(
                    "a HAVING subquery over a grouped source needs two views: \
                     group the source in one view, then filter it against the \
                     subquery in a second view",
                ));
            }
            _ if derived_sourceable(node) => {
                let plan = projection_plan.unwrap_or(node);
                return Err(derived_source_error("a join side", plan));
            }
            _ => return Err(unsupported("join input must be a table")),
        }
    }
}

/// Render non-equality join conditions over the pair payload aliases
/// (`left_value` / `right_value`).
///
/// Each condition must compare the two payload columns, so the runtime can
/// evaluate it on the joined pair; conditions over other columns cannot be
/// evaluated because those columns are not materialized.
fn render_pair_conditions(
    conditions: &[SemiAntiCondition],
    left_value: &str,
    right_value: &str,
    shape: &str,
) -> Result<Option<String>> {
    if conditions.is_empty() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(conditions.len());
    for condition in conditions {
        let op = if condition.left_column == left_value
            && condition.right_column == right_value
        {
            condition.op
        } else if condition.left_column == right_value
            && condition.right_column == left_value
        {
            flip(condition.op)
        } else {
            return Err(unsupported(format!(
                "a {shape} condition must compare the left and right payload columns"
            )));
        };
        parts.push(format!("left_value {} right_value", compare_op_sql(op),));
    }
    Ok(Some(parts.join(" AND ")))
}

/// Render non-equality pair conditions over a wide join's output columns,
/// which the layout carries as `__left_*` / `__right_*` fields.
fn render_wide_pair_conditions(
    conditions: &[SemiAntiCondition],
    output_columns: &[JoinOutputColumn],
    shape: &str,
) -> Result<Option<String>> {
    if conditions.is_empty() {
        return Ok(None);
    }
    let mut parts = Vec::with_capacity(conditions.len());
    for condition in conditions {
        let find = |side: JoinSide, column: &str| {
            output_columns
                .iter()
                .find(|output| output.side == side && output.column == column)
        };
        let left = find(JoinSide::Left, &condition.left_column).ok_or_else(|| {
            unsupported(format!(
                "a {shape} condition must compare materialized left and right columns"
            ))
        })?;
        let right = find(JoinSide::Right, &condition.right_column).ok_or_else(|| {
            unsupported(format!(
                "a {shape} condition must compare materialized left and right columns"
            ))
        })?;
        parts.push(format!(
            "{} {} {}",
            wide_pair_alias(left),
            compare_op_sql(condition.op),
            wide_pair_alias(right)
        ));
    }
    Ok(Some(parts.join(" AND ")))
}

/// Whether a non-equality condition compares exactly the compact pair
/// payloads (in either order).
fn pair_compatible(
    condition: &SemiAntiCondition,
    left_value: &str,
    right_value: &str,
) -> bool {
    (condition.left_column == left_value && condition.right_column == right_value)
        || (condition.left_column == right_value && condition.right_column == left_value)
}

/// Materialize the columns a non-equality condition compares but the select
/// list does not carry as hidden wide payloads, so the pair filter can
/// evaluate them; [`join_condition_column_name`] names them and the MV schema
/// helper spells the same columns.
fn materialize_condition_columns(
    output_columns: &mut Vec<JoinOutputColumn>,
    conditions: &[SemiAntiCondition],
) {
    for condition in conditions {
        for (side, column) in [
            (JoinSide::Left, &condition.left_column),
            (JoinSide::Right, &condition.right_column),
        ] {
            if !output_columns
                .iter()
                .any(|output| output.side == side && &output.column == column)
            {
                output_columns.push(JoinOutputColumn {
                    side,
                    column: column.clone(),
                    name: join_condition_column_name(side, column),
                });
            }
        }
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

/// The payload columns of a join select list, split by side.
#[derive(Default)]
struct JoinPayloads {
    /// The wide output columns in select order, join keys removed.
    output_columns: Vec<JoinOutputColumn>,
    /// The first left output column (the compact payload fallback).
    left_value: Option<String>,
    /// The first right output column (the compact payload fallback).
    right_value: Option<String>,
    /// The selected name of the first left payload.
    left_name: Option<String>,
    /// The selected name of the first right payload.
    right_name: Option<String>,
    /// How many payload columns the left side contributes.
    left_count: usize,
    /// How many payload columns the right side contributes.
    right_count: usize,
}

impl JoinPayloads {
    /// Drop the left payloads the layout carries by itself (a lookup join
    /// materializes the left primary keys), so a statement that selects them
    /// stays compact. The drop keeps at least one left payload.
    fn drop_carried_left(&mut self, carried: &[String]) {
        if self.left_count < 2 {
            return;
        }
        let dropped = self
            .output_columns
            .iter()
            .filter(|column| {
                column.side == JoinSide::Left && carried.contains(&column.column)
            })
            .count();
        if dropped == 0 {
            return;
        }
        self.output_columns.retain(|column| {
            !(column.side == JoinSide::Left && carried.contains(&column.column))
        });
        self.left_count -= dropped;
        let first = self
            .output_columns
            .iter()
            .find(|column| column.side == JoinSide::Left);
        self.left_value = first.map(|column| column.column.clone());
        self.left_name = first.map(|column| column.name.clone());
    }

    /// Record one payload column from `side`.
    fn push(&mut self, side: Side, column: String, name: String) {
        match side {
            Side::Left => {
                self.left_count += 1;
                if self.left_value.is_none() {
                    self.left_value = Some(column.clone());
                    self.left_name = Some(name.clone());
                }
            }
            Side::Right => {
                self.right_count += 1;
                if self.right_value.is_none() {
                    self.right_value = Some(column.clone());
                    self.right_name = Some(name.clone());
                }
            }
        }
        self.output_columns.push(JoinOutputColumn {
            side: match side {
                Side::Left => JoinSide::Left,
                Side::Right => JoinSide::Right,
            },
            column,
            name,
        });
    }
}

/// Classify the join select list: every non-key column is a plain column of
/// one side, and its select alias (or source name) becomes the output name.
fn join_payloads(
    projection: Option<&Projection>,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    left: &IvmTable,
    right: &IvmTable,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<JoinPayloads> {
    let projection = projection.ok_or_else(|| {
        unsupported("a join view needs payload columns from the select list")
    })?;
    if !is_plain_projection(projection) {
        return Err(unsupported("computed columns in a join output"));
    }
    let mut payloads = JoinPayloads::default();
    for expr in &projection.expr {
        let column = column_of(expr)
            .ok_or_else(|| unsupported("join output must be plain columns"))?;
        let side =
            side_of(column, left_alias, right_alias, left, right).ok_or_else(|| {
                unsupported(format!(
                    "join output column {} is ambiguous; use distinct names or aliases",
                    column.name
                ))
            })?;
        let keys = match side {
            Side::Left => left_keys,
            Side::Right => right_keys,
        };
        if keys.contains(&column.name) {
            continue;
        }
        let name = match expr {
            Expr::Alias(alias) => alias.name.clone(),
            _ => column.name.clone(),
        };
        payloads.push(side, column.name.clone(), name);
    }
    Ok(payloads)
}

/// Classify a pruned join schema whose projection the optimizer removed:
/// every field outside the join keys is a payload named after the field.
fn join_payloads_from_fields(
    schema: &datafusion::common::DFSchema,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    left: &IvmTable,
    right: &IvmTable,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<JoinPayloads> {
    let mut payloads = JoinPayloads::default();
    for (qualifier, field) in schema.iter() {
        let column = datafusion::common::Column::new(
            qualifier.map(|qualifier| qualifier.table().to_string()),
            field.name(),
        );
        let side =
            side_of(&column, left_alias, right_alias, left, right).ok_or_else(|| {
                unsupported(format!(
                    "join output column {} is ambiguous; use distinct names or aliases",
                    field.name()
                ))
            })?;
        let keys = match side {
            Side::Left => left_keys,
            Side::Right => right_keys,
        };
        if keys.contains(field.name()) {
            continue;
        }
        payloads.push(side, field.name().clone(), field.name().clone());
    }
    Ok(payloads)
}

/// Split the classified payloads into the compact payload pair or the wide
/// output column list.
fn compact_or_wide(
    payloads: JoinPayloads,
    key_names: &[String],
    shape: &str,
) -> Result<(String, String, Vec<JoinOutputColumn>, Vec<String>)> {
    if payloads.left_count == 1 && payloads.right_count == 1 {
        let left_value = payloads.left_value.clone().expect("one left payload");
        let right_value = payloads.right_value.clone().expect("one right payload");
        // A renamed payload keeps its select name (a derived step materializes
        // it under that name); the untouched compact layout keeps the fixed
        // `left_value` / `right_value` names.
        let names = match (payloads.left_name.clone(), payloads.right_name.clone()) {
            (Some(left), Some(right)) if left != left_value || right != right_value => {
                vec![left, right]
            }
            _ => Vec::new(),
        };
        return Ok((left_value, right_value, Vec::new(), names));
    }
    if payloads.left_count == 0 || payloads.right_count == 0 {
        return Err(unsupported(format!(
            "a {shape} needs at least one column from each side"
        )));
    }
    check_wide_output_names(&payloads.output_columns, key_names)?;
    Ok((
        payloads.left_value.clone().expect("one left payload"),
        payloads.right_value.clone().expect("one right payload"),
        payloads.output_columns,
        Vec::new(),
    ))
}

/// The compact payload names the statement selected, when a rename makes
/// them differ from the payload columns: the derived-step rewrite then
/// materializes the columns under the select names.
fn compact_payload_names(
    payloads: &JoinPayloads,
    left_value: &str,
    right_value: &str,
) -> Vec<String> {
    match (payloads.left_name.clone(), payloads.right_name.clone()) {
        (Some(left), Some(right)) if left != left_value || right != right_value => {
            vec![left, right]
        }
        _ => Vec::new(),
    }
}

/// The wide output names must be unique and must not shadow the join keys.
fn check_wide_output_names(
    output_columns: &[JoinOutputColumn],
    join_keys: &[String],
) -> Result<()> {
    let mut names = join_keys.to_vec();
    for column in output_columns {
        if names.contains(&column.name) {
            return Err(unsupported(format!(
                "join output column {} is materialized twice; add distinct aliases",
                column.name
            )));
        }
        names.push(column.name.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::test_helpers::*;

    #[tokio::test]
    async fn analyzes_quantified_comparisons() {
        // `x > ANY (S)` keeps a left row when some right row satisfies the
        // comparison: a theta semi join with no equality key.
        let analyzed = analyze_multi(
            "select k from src where v > any (select v from dim)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            join_keys,
            conditions,
            anti,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert!(join_keys.is_empty());
        assert!(!anti);
        assert_eq!(
            conditions,
            vec![SemiAntiCondition {
                left_column: "v".to_string(),
                right_column: "v".to_string(),
                op: CompareOp::Gt,
                is_not_true: false,
            }]
        );

        // `x > ALL (S)` keeps a left row when no right row makes the
        // comparison false or unknown: an anti join on `IS NOT TRUE`.
        let analyzed = analyze_multi(
            "select k from src where v > all (select v from dim)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            conditions, anti, ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert!(anti);
        assert_eq!(conditions[0].op, CompareOp::Gt);
        assert!(conditions[0].is_not_true);

        // `NOT (x > ANY S)` is exactly `x <= ALL S`.
        let analyzed = analyze_multi(
            "select k from src where not (v > any (select v from dim))",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            conditions, anti, ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert!(anti);
        assert_eq!(conditions[0].op, CompareOp::Le);
        assert!(conditions[0].is_not_true);

        // A side filter and a correlated key beside the quantifier are kept.
        for (sql, keys) in [
            (
                "select k from src where g = 'x' and v > any (select v from dim)",
                Vec::<String>::new(),
            ),
            (
                "select k from src where v > any (select v from dim where dim.g = src.g)",
                vec!["g".to_string()],
            ),
        ] {
            let analyzed =
                analyze_multi(sql, vec![source_table("src"), source_table("dim")])
                    .await
                    .unwrap();
            let ViewSpec::SemiAnti {
                join_keys,
                left_filter,
                ..
            } = analyzed.spec
            else {
                panic!("expected a semi/anti spec");
            };
            assert_eq!(join_keys, keys, "{sql}");
            if keys.is_empty() {
                assert_eq!(
                    normalized(left_filter.as_deref()),
                    Some("g = 'x'".to_string())
                );
            }
        }

        // A quantifier below a disjunction keeps its mark-join plan: it is
        // rejected rather than mistranslated.
        assert!(
            analyze_multi(
                "select k from src where v > any (select v from dim) or g = 'x'",
                vec![source_table("src"), source_table("dim")],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_left_aggregate_outputs() {
        // A computed select-list expression over the scalar subquery value:
        // the raw value stays materialized (named internally when the select
        // list does not carry it) and the expression appends a view column.
        let analyzed = analyze_multi(
            "select s.k, (select max(d.v) from dim d where d.k = s.k) * 2 + 1 \
             as m from src s",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::LeftAggregate {
            aggregate_column,
            output_columns,
            output_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left aggregate spec");
        };
        assert_eq!(aggregate_column, "__ivm_agg");
        assert_eq!(output_columns, vec!["k".to_string()]);
        assert_eq!(
            output_exprs,
            vec![LeftAggregateOutput {
                call: "((__ivm_right_agg * 2) + 1)".to_string(),
                column: "m".to_string(),
                result: "int64".to_string(),
            }]
        );

        // A computed output may also read the row's own columns; a left
        // column it reads is materialized by the runtime.
        let analyzed = analyze_multi(
            "select s.k, s.v + (select max(d.v) from dim d where d.k = s.k) \
             as m from src s",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::LeftAggregate {
            output_columns,
            output_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left aggregate spec");
        };
        assert_eq!(output_columns, vec!["k".to_string()]);
        assert_eq!(output_exprs.len(), 1);
        assert!(
            output_exprs[0].call.contains("__ivm_right_agg")
                && output_exprs[0].call.contains('v'),
            "{:?}",
            output_exprs[0].call
        );
        assert_eq!(output_exprs[0].column, "m");

        // The plain value keeps the user's alias.
        let analyzed = analyze_multi(
            "select s.k, (select max(d.v) from dim d where d.k = s.k) as m \
             from src s",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::LeftAggregate {
            aggregate_column,
            output_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left aggregate spec");
        };
        assert_eq!(aggregate_column, "m");
        assert!(output_exprs.is_empty());

        // A computed output must be aliased and may only compute over the
        // subquery value.
        for sql in [
            "select s.k, (select max(d.v) from dim d where d.k = s.k) + 1 \
             from src s",
            "select s.k, s.v + 1 as x, \
             (select max(d.v) from dim d where d.k = s.k) + 1 as m from src s",
        ] {
            assert!(
                analyze_multi(sql, vec![source_table("src"), source_table("dim")])
                    .await
                    .is_err(),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn probe_having_native() {
        for sql in [
            "select g, sum(v) from src group by g having sum(v) > (select max(v) from dim)",
            "select g, sum(v) from src group by g having sum(v) > (select max(d.v) from dim d where d.g = src.g)",
            "select g, sum(v) from src group by g having sum(v) > (select max(d.v) from dim d where d.g = src.g and d.v > 0)",
            "select g, sum(v), count(*) from src group by g having count(*) > (select count(*) from dim)",
            "select sum(v) from src having sum(v) > (select max(v) from dim)",
        ] {
            match analyze_multi(sql, vec![source_table("src"), source_table("dim")]).await
            {
                Ok(analyzed) => eprintln!("HAVING_OK {sql}:\n{:#?}\n", analyzed.spec),
                Err(error) => eprintln!("HAVING_ERR {sql}:\n{error}\n"),
            }
        }
    }

    #[tokio::test]
    async fn analyzes_having_subqueries() {
        // A global lookup aggregate (a cross join) and a per-key one (a
        // keyed join) both feed the recomputed groups before the predicate.
        let analyzed = analyze_multi(
            "select g, sum(v) from src group by g \
             having sum(v) > (select max(v) from dim)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::MultiAgg {
            having,
            having_lookup,
            ..
        } = analyzed.spec
        else {
            panic!("expected a multi-aggregate spec");
        };
        assert!(having.is_some());
        let lookup = having_lookup.expect("a HAVING lookup");
        assert_eq!(lookup.aggregate, "max(v)");
        assert!(lookup.join_keys.is_empty());
        assert_eq!(lookup.column, "__ivm_lookup");

        let analyzed = analyze_multi(
            "select g, sum(v) from src group by g \
             having sum(v) > (select max(d.v) from dim d where d.g = src.g)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::MultiAgg {
            group_keys,
            having_lookup,
            ..
        } = analyzed.spec
        else {
            panic!("expected a multi-aggregate spec");
        };
        assert_eq!(group_keys, vec!["g".to_string()]);
        let lookup = having_lookup.expect("a HAVING lookup");
        assert_eq!(lookup.aggregate, "max(v)");
        assert_eq!(lookup.join_keys, vec!["g".to_string()]);
        assert_eq!(lookup.right_keys, vec!["g".to_string()]);

        // An aliased group key keeps the alias in the lookup key.
        let analyzed = analyze_multi(
            "select g as bucket, sum(v) from src group by g \
             having sum(v) > (select max(d.v) from dim d where d.g = src.g)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::MultiAgg { having_lookup, .. } = analyzed.spec else {
            panic!("expected a multi-aggregate spec");
        };
        let lookup = having_lookup.expect("a HAVING lookup");
        assert_eq!(lookup.join_keys, vec!["bucket".to_string()]);

        // A lookup correlated on a non-group key stays rejected.
        for sql in [
            "select g, sum(v) from src group by g \
             having sum(v) > (select max(d.v) from dim d where d.k = src.k)",
            "select g, sum(v) from src group by g \
             having sum(v) > (select max(d.v) from dim d where d.g = src.g \
             and d.v > (select min(v) from dim))",
        ] {
            assert!(
                analyze_multi(sql, vec![source_table("src"), source_table("dim")])
                    .await
                    .is_err(),
                "{sql}"
            );
        }
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
    async fn probe_setop_vs_predicate_plans() {
        for sql in [
            "select g from src intersect all select g from dim",
            "select g from src except all select g from dim",
            "select g from src intersect select g from dim",
            "select g from src where exists (select 1 from dim where dim.g is not distinct from src.g)",
            "select g from src where not exists (select 1 from dim where dim.g is not distinct from src.g)",
        ] {
            let ctx = SessionContext::new();
            let table = || {
                Arc::new(MemTable::try_new(schema(), vec![vec![]]).unwrap())
                    as Arc<dyn datafusion::catalog::TableProvider>
            };
            ctx.register_table("src", table()).unwrap();
            ctx.register_table("dim", table()).unwrap();
            let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
            let optimized = ctx.state().optimize(&plan).unwrap();
            eprintln!("PROBE {sql}\n{}\n", optimized.display_indent());
        }
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
                right_keys: Vec::new(),
                key_exprs: JoinKeyExprs::default(),
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                payload_names: Vec::new(),
                left_filter: None,
                right_filter: None,
                pair_filter: None,
                null_safe: false,
            }
        );
    }

    /// A source whose value column is nullable, so the optimizer keeps the
    /// `IS NOT DISTINCT FROM` keys instead of simplifying them away.
    fn nullable_key_source(name: &str) -> IvmTable {
        let mut table = source_table(name);
        table.schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        table
    }

    #[tokio::test]
    async fn analyzes_null_safe_inner_join() {
        // `IS NOT DISTINCT FROM` keys plan as a null-aware inner join: the
        // maintained pair join matches NULL keys with NULL keys.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.k from a join b on a.v is not distinct from b.v",
            vec![nullable_key_source("a"), nullable_key_source("b")],
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            join_keys,
            null_safe,
            ..
        } = analyzed.spec
        else {
            panic!("expected a null-safe join spec");
        };
        assert_eq!(join_keys, vec!["v".to_string()]);
        assert!(null_safe);

        // A computed null-safe key is not maintained.
        assert!(
            analyze_multi(
                "select a.k, a.v, b.k from a join b \
                 on a.v + 1 is not distinct from b.v",
                vec![nullable_key_source("a"), nullable_key_source("b")],
            )
            .await
            .is_err()
        );

        // A null-safe step inside a multi-table chain is not maintained.
        assert!(
            analyze_multi(
                "select a.k from a join b on a.k = b.k \
                 join c on a.v is not distinct from c.v",
                vec![
                    nullable_key_source("a"),
                    nullable_key_source("b"),
                    nullable_key_source("c"),
                ],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_null_safe_outer_joins() {
        // A LEFT JOIN whose right source is not keyed by the join keys is the
        // pair-keyed shape and keeps the null-safe flag.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.k from a left join b \
             on a.v is not distinct from b.v",
            vec![nullable_key_source("a"), nullable_key_source("b")],
        )
        .await
        .unwrap();
        let ViewSpec::LeftJoin { null_safe, .. } = analyzed.spec else {
            panic!("expected a left join spec");
        };
        assert!(null_safe);

        // A right source keyed by the join keys is the lookup shape: its keys
        // are a non-nullable primary key, so a NULL left key never matches and
        // the lookup keeps the strict semantics.
        let mut lookup = nullable_key_source("b");
        lookup.primary_keys = vec!["v".to_string()];
        let analyzed = analyze_multi(
            "select a.k, a.v, b.k from a left join b \
             on a.v is not distinct from b.v",
            vec![nullable_key_source("a"), lookup],
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::LookupJoin { .. }));

        // A FULL JOIN keeps the flag too.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.k from a full join b \
             on a.v is not distinct from b.v",
            vec![nullable_key_source("a"), nullable_key_source("b")],
        )
        .await
        .unwrap();
        let ViewSpec::FullJoin { null_safe, .. } = analyzed.spec else {
            panic!("expected a full join spec");
        };
        assert!(null_safe);

        // A RIGHT JOIN is analyzed as the swapped LEFT join.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.k from a right join b \
             on a.v is not distinct from b.v",
            vec![nullable_key_source("a"), nullable_key_source("b")],
        )
        .await
        .unwrap();
        let ViewSpec::LeftJoin { null_safe, .. } = analyzed.spec else {
            panic!("expected a swapped left join spec");
        };
        assert!(null_safe);

        // A computed null-safe key stays rejected.
        assert!(
            analyze_multi(
                "select a.k, a.v, b.k from a left join b \
                 on a.v + 1 is not distinct from b.v",
                vec![nullable_key_source("a"), nullable_key_source("b")],
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_inner_join_with_right_key_names() {
        // An inner join may use differently named keys; the output keeps the
        // left names and neither key name is a payload.
        let mut right = source_table("b");
        right.primary_keys = vec!["rk".to_string()];
        right.schema = Arc::new(Schema::new(vec![
            Field::new("rk", DataType::Int64, false),
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let analyzed = analyze_multi(
            "select a.g, b.v from a join b on a.k = b.rk",
            vec![source_table("a"), right],
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            join_keys,
            right_keys,
            left_value,
            right_value,
            ..
        } = analyzed.spec
        else {
            panic!("expected an inner join spec");
        };
        assert_eq!(join_keys, vec!["k".to_string()]);
        assert_eq!(right_keys, vec!["rk".to_string()]);
        assert_eq!(left_value, "g");
        assert_eq!(right_value, "v");

        // Same-named keys keep the compact form.
        let analyzed =
            analyze_optimized("select a.k, a.v, b.v from src a join src b on a.k = b.k")
                .await
                .unwrap();
        let ViewSpec::Join { right_keys, .. } = analyzed.spec else {
            panic!("expected an inner join spec");
        };
        assert!(right_keys.is_empty());
    }

    #[tokio::test]
    async fn analyzes_multi_cross_join() {
        // A keyless chain (`FROM a, b, c`) flattens into cross joins.
        let analyzed = analyze_optimized(
            "select a.k, b.g as bg, c.v as cv from src a, src b, src c",
        )
        .await
        .unwrap();
        let ViewSpec::MultiJoin {
            sources,
            keys,
            columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a multi join spec");
        };
        assert_eq!(sources.len(), 3);
        assert!(keys.is_empty());
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[1].source, 1);
        assert_eq!(columns[2].name, "cv");

        // A mixed chain keeps the keyed step and cross joins the rest.
        let analyzed = analyze_optimized(
            "select a.k, b.g as bg, c.v as cv from src a join src b on a.k = b.k, src c",
        )
        .await
        .unwrap();
        let ViewSpec::MultiJoin { keys, .. } = analyzed.spec else {
            panic!("expected a multi join spec");
        };
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].right_source, 1);
    }

    #[tokio::test]
    async fn analyzes_wide_outer_joins() {
        // A wide lookup join materializes several columns per side.
        let analyzed = analyze_optimized(
            "select a.k, a.g, a.v, b.g as bg, b.v as bv from src a \
             left join src b on a.k = b.k",
        )
        .await
        .unwrap();
        let ViewSpec::LookupJoin {
            left_value,
            right_value,
            output_columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a lookup join spec");
        };
        assert_eq!(left_value, "g");
        assert_eq!(right_value, "g");
        assert_eq!(output_columns.len(), 4);
        assert_eq!(output_columns[2].name, "bg");
        assert_eq!(output_columns[3].side, JoinSide::Right);

        // A wide full join.
        let analyzed = analyze_optimized(
            "select a.k, a.g, a.v, b.g as bg, b.v as bv from src a \
             full join src b on a.k = b.k",
        )
        .await
        .unwrap();
        let ViewSpec::FullJoin { output_columns, .. } = analyzed.spec else {
            panic!("expected a full join spec");
        };
        assert_eq!(output_columns.len(), 4);

        // A pair-keyed wide left join (the right side keyed by another
        // column).
        let dim = {
            let mut table = source_table("dim");
            table.primary_keys = vec!["v".to_string()];
            table
        };
        let analyzed = analyze_multi(
            "select a.k, a.g, a.v, b.g as bg, b.v as bv from src a \
             left join dim b on a.k = b.k",
            vec![source_table("src"), dim],
        )
        .await
        .unwrap();
        let ViewSpec::LeftJoin { output_columns, .. } = analyzed.spec else {
            panic!("expected a left join spec");
        };
        assert_eq!(output_columns.len(), 4);

        // One payload per side stays compact.
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v from src a left join src b on a.k = b.k",
        )
        .await
        .unwrap();
        let ViewSpec::LookupJoin { output_columns, .. } = analyzed.spec else {
            panic!("expected a lookup join spec");
        };
        assert!(output_columns.is_empty());
    }

    #[tokio::test]
    async fn analyzes_lookup_chain() {
        // A left-deep chain of keyed 1:1 lookups over a base table.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v as bv, c.v as cv from src a \
             left join src b on b.k = a.k \
             join src c on c.k = a.k",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::LookupChain {
            sources,
            steps,
            output_columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a lookup chain spec");
        };
        assert_eq!(sources.len(), 3);
        assert_eq!(steps.len(), 2);
        assert!(steps[0].left && !steps[1].left);
        assert_eq!(steps[0].keys, vec!["k".to_string()]);
        assert_eq!(steps[0].right_keys, vec!["k".to_string()]);
        assert_eq!(output_columns.len(), 4);
        assert_eq!(output_columns[0].source, 0);
        assert_eq!(output_columns[2].source, 1);
        assert_eq!(output_columns[2].name, "bv");
        assert_eq!(output_columns[3].source, 2);
        assert_eq!(output_columns[3].name, "cv");

        // A step key may reference an earlier step's column.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v as bv, c.v as cv from src a \
             left join src b on b.k = a.k \
             left join src c on c.k = b.k",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::LookupChain { steps, .. } = analyzed.spec else {
            panic!("expected a lookup chain spec");
        };
        assert_eq!(steps[1].keys, vec!["k".to_string()]);
        assert_eq!(steps[1].key_sources, vec![1]);

        // A step joined on a non-key column is a 1:N lookup.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v as bv, c.v as cv from src a \
             left join src b on b.g = a.g \
             join src c on c.k = a.k",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::LookupChain { steps, .. } = analyzed.spec else {
            panic!("expected a lookup chain spec");
        };
        assert!(!steps[0].unique);
        assert!(steps[1].unique);

        // The key of a 1:N step may reference an earlier step.
        let analyzed = analyze_multi(
            "select a.k, b.v as bv, c.v as cv from src a \
             join src b on b.k = a.k \
             left join src c on c.v = b.v",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::LookupChain { steps, .. } = analyzed.spec else {
            panic!("expected a lookup chain spec");
        };
        assert!(steps[0].unique);
        assert!(!steps[1].unique);
        assert_eq!(steps[1].key_sources, vec![1]);

        // A right/full step stays rejected.
        for sql in [
            "select a.k, b.v as bv, c.v as cv from src a join src b on b.k = a.k \
             right join src c on c.k = a.k",
            "select a.k, b.v as bv, c.v as cv from src a join src b on b.k = a.k \
             full join src c on c.k = a.k",
        ] {
            let error = analyze_multi(sql, vec![source_table("src")])
                .await
                .unwrap_err();
            let message = format!("{error}");
            assert!(message.contains("lookup chain"), "{sql}: {message}");
        }
    }

    #[tokio::test]
    async fn analyzes_quantified_in_equivalents() {
        // `= ANY` is `IN` (a semi join) and `<> ALL` / `!= ALL` is `NOT IN`
        // (an anti join) by the SQL standard.
        let analyzed = analyze_multi(
            "select k from src where k = any (select k from dim)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            anti, join_keys, ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert!(!anti);
        assert_eq!(join_keys, vec!["k".to_string()]);

        for sql in [
            "select k from src where k <> all (select k from dim)",
            "select k from src where k != all (select k from dim)",
        ] {
            let analyzed =
                analyze_multi(sql, vec![source_table("src"), source_table("dim")])
                    .await
                    .unwrap();
            let ViewSpec::SemiAnti { anti, .. } = analyzed.spec else {
                panic!("expected a semi/anti spec");
            };
            assert!(anti, "{sql}");
        }

        // The remaining quantifiers are rewritten into EXISTS predicates and
        // maintained as theta semi/anti joins (see
        // `analyzes_quantified_comparisons`).
        let analyzed = analyze_multi(
            "select k from src where k > any (select k from dim)",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::SemiAnti { .. }));
    }

    #[tokio::test]
    async fn analyzes_multi_join() {
        let analyzed = analyze_optimized(
            "select a.k, a.g, b.g as bg, c.v as cv from src a \
             join src b on a.k = b.k join src c on b.k = c.k",
        )
        .await
        .unwrap();
        let ViewSpec::MultiJoin {
            sources,
            keys,
            columns,
            conditions,
            ..
        } = analyzed.spec
        else {
            panic!("expected a multi join spec");
        };
        assert_eq!(sources.len(), 3);
        assert_eq!(
            keys,
            vec![
                MultiJoinKey {
                    left_source: 0,
                    left_column: "k".to_string(),
                    right_source: 1,
                    right_column: "k".to_string(),
                },
                MultiJoinKey {
                    left_source: 1,
                    left_column: "k".to_string(),
                    right_source: 2,
                    right_column: "k".to_string(),
                },
            ]
        );
        assert_eq!(
            columns,
            vec![
                MultiJoinColumn {
                    source: 0,
                    column: "k".to_string(),
                    name: "k".to_string(),
                },
                MultiJoinColumn {
                    source: 0,
                    column: "g".to_string(),
                    name: "g".to_string(),
                },
                MultiJoinColumn {
                    source: 1,
                    column: "g".to_string(),
                    name: "bg".to_string(),
                },
                MultiJoinColumn {
                    source: 2,
                    column: "v".to_string(),
                    name: "cv".to_string(),
                },
            ]
        );

        // Up to sixteen sources: nine aliases of the same table flatten into
        // one multi-way join.
        let mut sql = String::from("select t0.k");
        for index in 1..9 {
            sql.push_str(&format!(", t{index}.k as k{index}"));
        }
        sql.push_str(" from src t0");
        for index in 1..9 {
            sql.push_str(&format!(" join src t{index} on t{index}.k = t0.k"));
        }
        let analyzed = analyze_optimized(&sql).await.unwrap();
        let ViewSpec::MultiJoin { sources, .. } = analyzed.spec else {
            panic!("expected a multi join spec");
        };
        assert_eq!(sources.len(), 9);
        assert!(conditions.is_empty());

        // A cross-source condition and a side filter.
        let analyzed = analyze_optimized(
            "select a.k, b.g, c.v from src a join src b on a.k = b.k \
             join src c on b.k = c.k where a.g <> 'skip' and a.v < c.v",
        )
        .await
        .unwrap();
        let ViewSpec::MultiJoin {
            sources,
            conditions,
            ..
        } = analyzed.spec
        else {
            panic!("expected a multi join spec");
        };
        assert_eq!(sources[0].filter.as_deref(), Some("(g <> 'skip')"));
        assert_eq!(
            conditions,
            vec![MultiJoinCondition {
                left_source: 0,
                left_column: "v".to_string(),
                right_source: 2,
                right_column: "v".to_string(),
                op: CompareOp::Lt,
            }]
        );

        // A non-inner join in the tree becomes a lookup chain (see
        // analyzes_lookup_chain); a right or full step stays rejected.
        let analyzed = analyze_optimized(
            "select a.k, b.g, c.v from src a join src b on a.k = b.k \
             left join src c on b.k = c.k",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::LookupChain { .. }));
        assert!(
            analyze_optimized(
                "select a.k, b.g, c.v from src a join src b on a.k = b.k \
                 full join src c on b.k = c.k",
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_wide_inner_join() {
        // More than one payload column per side materializes the select
        // columns under their select names.
        let analyzed = analyze_optimized(
            "select a.k, a.g, a.v, b.g as bg, b.v as bv from src a join src b on a.k = b.k",
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            join_keys,
            left_value,
            right_value,
            output_columns,
            pair_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected an inner join spec");
        };
        assert_eq!(join_keys, vec!["k".to_string()]);
        assert_eq!(left_value, "g");
        assert_eq!(right_value, "g");
        assert_eq!(
            output_columns,
            vec![
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "g".to_string(),
                    name: "g".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "v".to_string(),
                    name: "v".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "g".to_string(),
                    name: "bg".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "v".to_string(),
                    name: "bv".to_string(),
                },
            ]
        );
        assert!(pair_filter.is_none());

        // A non-equality condition over the wide payloads maps to the
        // intermediate column names.
        let analyzed = analyze_optimized(
            "select a.k, a.g, a.v, b.g as bg, b.v as bv from src a \
             join src b on a.k = b.k and a.v < b.v",
        )
        .await
        .unwrap();
        let ViewSpec::Join { pair_filter, .. } = analyzed.spec else {
            panic!("expected an inner join spec");
        };
        assert_eq!(
            normalized(pair_filter.as_deref()).as_deref(),
            Some("__left_v < __right_bv")
        );

        // A side without a payload becomes the membership shape when the
        // right side is keyed by the join key: the match is unique, so the
        // view is exactly the left rows that have one (the select list's
        // `b.k` equals the left's `k` under the join condition).
        let analyzed = analyze_optimized(
            "select a.k, a.g, a.v, b.k from src a join src b on a.k = b.k",
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            anti,
            output_columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected the semi shape");
        };
        assert!(!anti);
        assert_eq!(output_columns, vec!["k", "g", "v"]);
    }

    #[tokio::test]
    async fn analyzes_wide_cross_join() {
        let analyzed = analyze_optimized(
            "select a.g, a.v, b.g as bg, b.v as bv from src a cross join src b",
        )
        .await
        .unwrap();
        let ViewSpec::CrossJoin { output_columns, .. } = analyzed.spec else {
            panic!("expected a cross join spec");
        };
        assert_eq!(output_columns.len(), 4);
        assert_eq!(output_columns[2].name, "bg");

        // A cross-side predicate maps to the wide pair filter.
        let analyzed = analyze_optimized(
            "select a.g, a.v, b.g as bg, b.v as bv from src a cross join src b \
             where a.v < b.v",
        )
        .await
        .unwrap();
        let ViewSpec::CrossJoin { pair_filter, .. } = analyzed.spec else {
            panic!("expected a cross join spec");
        };
        assert_eq!(
            normalized(pair_filter.as_deref()).as_deref(),
            Some("__left_v < __right_bv")
        );
    }

    #[tokio::test]
    async fn analyzes_theta_joins() {
        // An inner join's non-equality condition over the payloads becomes a
        // pair filter on the joined pair.
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v from src a join src b on a.k = b.k and a.v < b.v",
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            pair_filter,
            left_filter,
            right_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected an inner join spec");
        };
        assert_eq!(
            normalized(pair_filter.as_deref()).as_deref(),
            Some("left_value < right_value")
        );
        assert!(left_filter.is_none() && right_filter.is_none());

        // A cross join's residual cross-side predicate is a pair filter too.
        let mut right = source_table("b");
        right.schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("rg", DataType::Utf8, false),
            Field::new("rv", DataType::Int64, false),
        ]));
        let analyzed = analyze_multi(
            "select a.v, b.rv from a cross join b where a.v < b.rv",
            vec![source_table("a"), right],
        )
        .await
        .unwrap();
        let ViewSpec::CrossJoin { pair_filter, .. } = analyzed.spec else {
            panic!("expected a cross join spec");
        };
        assert_eq!(
            normalized(pair_filter.as_deref()).as_deref(),
            Some("left_value < right_value")
        );

        // An equality key that is not a plain column becomes a computed key:
        // both sides are rendered, evaluated and stored under a hidden name.
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v as bv from src a join src b \
             on a.k = b.k and a.v + 1 = b.v",
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            join_keys,
            key_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected an inner join spec");
        };
        assert_eq!(join_keys.len(), 2);
        assert_eq!(join_keys[1], join_key_expression_name(0));
        assert_eq!(key_exprs.left.len(), 2);
        assert!(key_exprs.left[0].is_empty() && key_exprs.right[0].is_empty());
        assert!(key_exprs.left[1].contains("v + 1"), "{:?}", key_exprs.left);
        assert_eq!(key_exprs.right[1], "v");

        // A condition over a column the select list does not carry
        // materializes that column as a hidden wide payload.
        let analyzed = analyze_optimized(
            "select a.k, a.v, b.v as bv from src a join src b \
             on a.k = b.k and a.g < b.g",
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            output_columns,
            pair_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected an inner join spec");
        };
        assert_eq!(output_columns.len(), 4);
        assert!(output_columns.iter().any(|column| {
            column.side == JoinSide::Left
                && column.column == "g"
                && column.name == join_condition_column_name(JoinSide::Left, "g")
        }));
        assert!(output_columns.iter().any(|column| {
            column.side == JoinSide::Right
                && column.column == "g"
                && column.name == join_condition_column_name(JoinSide::Right, "g")
        }));
        assert_eq!(
            normalized(pair_filter.as_deref()).as_deref(),
            Some("__left___ivm_cond_left_g < __right___ivm_cond_right_g")
        );
    }

    #[tokio::test]
    async fn analyzes_filtered_pair_joins() {
        // A CROSS JOIN may filter either side; the right fixture keeps the
        // column names distinguishable.
        let mut right = source_table("b");
        right.schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("rg", DataType::Utf8, false),
            Field::new("rv", DataType::Int64, false),
        ]));
        let analyzed = analyze_multi(
            "select a.v, b.rv from a cross join b where a.v > 1 and b.rv > 2",
            vec![source_table("a"), right.clone()],
        )
        .await
        .unwrap();
        let ViewSpec::CrossJoin {
            left_filter,
            right_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a cross join spec");
        };
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 1"));
        assert_eq!(
            normalized(right_filter.as_deref()).as_deref(),
            Some("rv > 2")
        );

        // A pair-keyed LEFT JOIN keeps its left-side filter (a right-side
        // WHERE would become an inner join); the right source must not be
        // keyed by the join keys for the pair shape.
        let mut pair_right = source_table("b");
        pair_right.primary_keys = vec!["v".to_string()];
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v from a left join b on a.k = b.k where a.v > 1",
            vec![source_table("a"), pair_right],
        )
        .await
        .unwrap();
        let ViewSpec::LeftJoin {
            left_filter,
            right_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left join spec");
        };
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 1"));
        assert!(right_filter.is_none());
    }

    #[tokio::test]
    async fn analyzes_filtered_semi_anti_inputs() {
        // The outer WHERE filters the left side and the subquery predicate
        // the right side; both are pushed below the semi/anti join.
        let analyzed = analyze_multi(
            "select a.k, a.v from a where a.v > 1 \
             and exists (select 1 from b where b.k = a.k and b.v > 2)",
            vec![source_table("a"), source_table("b")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            left_filter,
            right_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 1"));
        assert_eq!(
            normalized(right_filter.as_deref()).as_deref(),
            Some("v > 2")
        );

        // The `IN` form with filters on both sides.
        let analyzed = analyze_multi(
            "select a.k from a where a.v > 5 \
             and a.k in (select b.k from b where b.v > 2)",
            vec![source_table("a"), source_table("b")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            left_filter,
            right_filter,
            anti,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert!(!anti);
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 5"));
        assert_eq!(
            normalized(right_filter.as_deref()).as_deref(),
            Some("v > 2")
        );
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
    async fn analyzes_a_filtered_cross_join() {
        // A cross-side predicate on a CROSS JOIN is evaluated on the joined
        // pair: every column of the join output classifies by its qualifier,
        // even when the two sides share the name.
        let analyzed = analyze_optimized(
            "select a.v, b.v from src a cross join src b where a.v < b.v",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::CrossJoin { .. }));
    }

    #[tokio::test]
    async fn a_lookup_key_that_is_also_the_primary_key_stays_a_lookup() {
        // The right side is keyed by the join key, so the shape is a lookup
        // even when the join key is also the left primary key: the layout
        // materializes it once, as the key (PLAN 10.143).
        let analyzed =
            analyze_optimized("select a.g, b.v from src a left join src b on b.k = a.k")
                .await
                .unwrap();
        let ViewSpec::LookupJoin {
            join_keys,
            left_value,
            right_value,
            ..
        } = analyzed.spec
        else {
            panic!("expected a lookup join spec");
        };
        assert_eq!(join_keys, vec!["k".to_string()]);
        assert_eq!(left_value, "g");
        assert_eq!(right_value, "v");
    }

    #[tokio::test]
    async fn analyzes_a_lookup_join_with_a_computed_key() {
        // A computed left key still looks the right side up: it is
        // materialized as a hidden key while the right side is keyed by the
        // plain column its expression renders.
        let analyzed = analyze_optimized(
            "select a.k, a.g, b.v from src a left join src b on b.k = a.v + 1",
        )
        .await
        .unwrap();
        let ViewSpec::LookupJoin {
            join_keys,
            key_exprs,
            left_value,
            right_value,
            ..
        } = analyzed.spec
        else {
            panic!("expected a lookup join spec");
        };
        assert_eq!(join_keys, vec![join_key_expression_name(0)]);
        assert!(key_exprs.left[0].contains("v + 1"), "{:?}", key_exprs.left);
        assert_eq!(key_exprs.right[0], "k");
        assert_eq!(left_value, "g");
        assert_eq!(right_value, "v");

        // A right side keyed by several columns, one of them computed on the
        // left, still looks up.
        let mut dim = source_table("dim");
        dim.primary_keys = vec!["k".to_string(), "v".to_string()];
        let analyzed = analyze_multi(
            "select a.k, a.g, b.g from src a left join dim b \
             on a.k = b.k and b.v = a.v + 1",
            vec![source_table("src"), dim],
        )
        .await
        .unwrap();
        let ViewSpec::LookupJoin {
            join_keys,
            key_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a lookup join spec");
        };
        assert_eq!(
            join_keys,
            vec!["k".to_string(), join_key_expression_name(0)]
        );
        assert!(key_exprs.left[0].is_empty() && key_exprs.right[0].is_empty());
        assert!(key_exprs.left[1].contains("v + 1"), "{:?}", key_exprs.left);
        assert_eq!(key_exprs.right[1], "v");

        // A computed key whose right side is not keyed by it is a pair left
        // join; it maintains computed keys too (see
        // `analyzes_a_left_join_with_a_computed_key`).
        let analyzed = analyze_optimized(
            "select a.k, a.g, b.v from src a left join src b on b.v = a.k + 1",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::LeftJoin { .. }));

        // A wide lookup maintains the computed key too.
        let analyzed = analyze_optimized(
            "select a.k, a.g, a.v, b.g as bg, b.v as bv from src a \
             left join src b on b.k = a.v + 1",
        )
        .await
        .unwrap();
        let ViewSpec::LookupJoin {
            join_keys,
            key_exprs,
            output_columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a lookup join spec");
        };
        assert_eq!(join_keys, vec![join_key_expression_name(0)]);
        assert!(key_exprs.left[0].contains("v + 1"), "{:?}", key_exprs.left);
        assert_eq!(output_columns.len(), 4);
    }

    #[tokio::test]
    async fn analyzes_a_single_sided_join_with_a_non_unique_right() {
        // The right side is keyed, but not by the join key: several matches
        // duplicate the left row, so the shape is a pair join that
        // materializes the right row identity as the right payload.
        let mut dim = source_table("dim");
        dim.primary_keys = vec!["v".to_string()];
        let analyzed = analyze_multi(
            "select a.k, a.v from src a join dim b on a.k = b.k",
            vec![source_table("src"), dim],
        )
        .await
        .unwrap();
        let ViewSpec::Join {
            left_value,
            right_value,
            ..
        } = analyzed.spec
        else {
            panic!("expected a pair join spec");
        };
        assert_eq!(left_value, "v");
        assert_eq!(right_value, "v");

        // The right side keyed by the join key stays the membership shape.
        let analyzed = analyze_optimized(
            "select a.k, a.g, a.v, b.k from src a join src b on a.k = b.k",
        )
        .await
        .unwrap();
        assert!(matches!(
            analyzed.spec,
            ViewSpec::SemiAnti { anti: false, .. }
        ));
    }

    #[tokio::test]
    async fn analyzes_a_left_join_with_a_computed_key() {
        // The right side is not keyed by the join keys, so the shape is a
        // pair left join; the computed key is materialized on its side.
        let mut dim = source_table("dim");
        dim.primary_keys = vec!["v".to_string()];
        let analyzed = analyze_multi(
            "select a.g, b.g from src a left join dim b on a.v + 1 = b.k",
            vec![source_table("src"), dim],
        )
        .await
        .unwrap();
        let ViewSpec::LeftJoin {
            join_keys,
            key_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left join spec");
        };
        assert_eq!(join_keys, vec![join_key_expression_name(0)]);
        assert!(key_exprs.left[0].contains("v + 1"), "{:?}", key_exprs.left);
        assert_eq!(key_exprs.right[0], "k");

        // The wide pair left join keeps the computed key too.
        let analyzed = analyze_multi(
            "select a.k, a.g, b.g as bg, b.v as bv from src a \
             left join dim b on a.v + 1 = b.k",
            vec![source_table("src"), {
                let mut dim = source_table("dim");
                dim.primary_keys = vec!["v".to_string()];
                dim
            }],
        )
        .await
        .unwrap();
        let ViewSpec::LeftJoin {
            key_exprs,
            output_columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left join spec");
        };
        assert!(key_exprs.left[0].contains("v + 1"), "{:?}", key_exprs.left);
        assert_eq!(output_columns.len(), 4);
    }

    #[tokio::test]
    async fn analyzes_a_full_join_with_a_computed_key() {
        let mut dim = source_table("dim");
        dim.primary_keys = vec!["v".to_string()];
        let analyzed = analyze_multi(
            "select a.g, b.g from src a full join dim b on a.v + 1 = b.k",
            vec![source_table("src"), dim],
        )
        .await
        .unwrap();
        let ViewSpec::FullJoin {
            join_keys,
            key_exprs,
            ..
        } = analyzed.spec
        else {
            panic!("expected a full join spec");
        };
        assert_eq!(join_keys, vec![join_key_expression_name(0)]);
        assert!(key_exprs.left[0].contains("v + 1"), "{:?}", key_exprs.left);
        assert_eq!(key_exprs.right[0], "k");
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
                key_exprs: JoinKeyExprs::default(),
                left_filter: None,
                right_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                payload_names: Vec::new(),
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
                right_keys: Vec::new(),
                key_exprs: JoinKeyExprs::default(),
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                payload_names: Vec::new(),
                identity_column: false,
                left_filter: None,
                right_filter: None,
                null_safe: false,
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
                key_exprs: JoinKeyExprs::default(),
                left_filter: None,
                right_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                payload_names: Vec::new(),
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

        // A filtered right input is kept as well.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v from src a left join \
             (select * from dim where v > 1) b on a.k = b.k",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::LookupJoin { right_filter, .. } = analyzed.spec else {
            panic!("expected a lookup join spec");
        };
        assert_eq!(
            normalized(right_filter.as_deref()).as_deref(),
            Some("v > 1")
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
                key_exprs: JoinKeyExprs::default(),
                left_filter: None,
                right_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                payload_names: Vec::new(),
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
                identity_column: false,
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_src".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                right_keys: Vec::new(),
                key_exprs: JoinKeyExprs::default(),
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                payload_names: Vec::new(),
                left_filter: None,
                right_filter: None,
                null_safe: false,
            }
        );

        // Differently named keys and filtered derived tables are supported.
        let analyzed = analyze_multi(
            "select a.k, a.v, b.g from src a full join dim b on a.k = b.v",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::FullJoin {
            join_keys,
            right_keys,
            ..
        } = analyzed.spec
        else {
            panic!("expected a full join spec");
        };
        assert_eq!(join_keys, vec!["k".to_string()]);
        assert_eq!(right_keys, vec!["v".to_string()]);
        let analyzed = analyze_multi(
            "select a.k, a.v, b.v from (select * from src where v > 1) a \
             full join (select * from dim where v < 9) b on a.k = b.k",
            vec![source_table("src"), source_table("dim")],
        )
        .await
        .unwrap();
        let ViewSpec::FullJoin {
            left_filter,
            right_filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a full join spec");
        };
        assert_eq!(normalized(left_filter.as_deref()).as_deref(), Some("v > 1"));
        assert_eq!(
            normalized(right_filter.as_deref()).as_deref(),
            Some("v < 9")
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
    async fn analyzes_correlated_scalar_subquery() {
        // A correlated scalar subquery plans as a semi join against one
        // aggregate row per correlated key.
        let analyzed = analyze_multi(
            "select k, v from src s where v > \
             (select avg(u.v) from src u where u.k = s.k)",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            join_keys,
            right_keys,
            right_aggregate,
            match_predicate,
            anti,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert_eq!(join_keys, vec!["k".to_string()]);
        assert_eq!(right_keys, vec!["k".to_string()]);
        assert!(!anti);
        let call = right_aggregate.expect("aggregate call");
        assert!(call.contains("avg"), "{call}");
        let predicate = normalized(match_predicate.as_deref()).unwrap();
        assert!(predicate.contains(IVM_RIGHT_AGG_COLUMN), "{predicate}");
        assert!(predicate.contains('>'), "{predicate}");

        // The correlated key may have another name and both sides may be
        // computed.
        let analyzed = analyze_multi(
            "select k, v from src s where v * 2 > \
             (select max(u.v) from src u where u.g = s.g) + 1",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            join_keys,
            right_keys,
            right_aggregate,
            match_predicate,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert_eq!(join_keys, vec!["g".to_string()]);
        assert_eq!(right_keys, vec!["g".to_string()]);
        assert!(right_aggregate.unwrap().contains("max"));
        assert!(
            normalized(match_predicate.as_deref())
                .unwrap()
                .contains(IVM_RIGHT_AGG_COLUMN)
        );

        // A subquery filter is kept; the computed select-list form is not a
        // semi join.
        assert!(
            analyze_multi(
                "select k, v from src s where v > \
                 (select avg(u.v) from src u where u.k = s.k and u.v > 1)",
                vec![source_table("src")],
            )
            .await
            .is_ok()
        );
        assert!(
            analyze_multi(
                "select (select avg(u.v) from src u where u.k = s.k) + 1 as m from src s",
                vec![source_table("src")],
            )
            .await
            .is_err()
        );

        // A select-list correlated scalar subquery is a left aggregate view.
        let analyzed = analyze_multi(
            "select k, v, (select avg(u.v) from src u where u.k = s.k) as m from src s",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::LeftAggregate {
            join_keys,
            right_keys,
            aggregate_column,
            output_columns,
            right_aggregate,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left aggregate spec");
        };
        assert_eq!(join_keys, vec!["k".to_string()]);
        assert_eq!(right_keys, vec!["k".to_string()]);
        assert_eq!(aggregate_column, "m");
        assert_eq!(output_columns, vec!["k".to_string(), "v".to_string()]);
        assert!(right_aggregate.contains("avg"), "{right_aggregate}");
    }

    #[tokio::test]
    async fn analyzes_correlated_count_subqueries() {
        // A COUNT comparison: DataFusion guards the empty set, and the
        // maintained predicate evaluates the guard over the aggregate column,
        // which a key without rows pads with NULL.
        let analyzed = analyze_multi(
            "select s.k, s.g, s.v from src s \
             where s.v > (select count(u.v) from src u where u.k = s.k)",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            right_aggregate,
            match_predicate,
            ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert_eq!(right_aggregate.as_deref(), Some("count(v)"));
        let predicate = match_predicate.expect("a match predicate");
        assert!(predicate.contains("__ivm_right_agg"), "{predicate}");
        assert!(predicate.contains("IS NULL"), "{predicate}");

        // COUNT(*): the empty set is zero rows.
        let analyzed = analyze_multi(
            "select s.k, s.g, s.v from src s \
             where s.v > (select count(*) from src u where u.k = s.k)",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            right_aggregate, ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert_eq!(right_aggregate.as_deref(), Some("count(1)"));

        // COUNT(DISTINCT): the two-level rewrite resolves back to the
        // distinct expression.
        let analyzed = analyze_multi(
            "select s.k, s.g, s.v from src s \
             where s.v > (select count(distinct u.v) from src u where u.k = s.k)",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::SemiAnti {
            right_aggregate, ..
        } = analyzed.spec
        else {
            panic!("expected a semi/anti spec");
        };
        assert_eq!(right_aggregate.as_deref(), Some("count(distinct v)"));

        // The select-list COUNT family is maintained by the left-aggregate
        // view: the value counts zero for a key without rows.
        let analyzed = analyze_multi(
            "select s.k, (select count(u.v) from src u where u.k = s.k) as c \
             from src s",
            vec![source_table("src")],
        )
        .await
        .unwrap();
        let ViewSpec::LeftAggregate {
            count_zero,
            aggregate_column,
            ..
        } = analyzed.spec
        else {
            panic!("expected a left aggregate spec");
        };
        assert!(count_zero);
        assert_eq!(aggregate_column, "c");
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
                left_filter: None,
                right_filter: None,
                null_safe: false,
                count_mode: false,
                right_aggregate: None,
                right_keys: Vec::new(),
                match_predicate: None,
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
                is_not_true: false,
            }]
        );
    }
}
