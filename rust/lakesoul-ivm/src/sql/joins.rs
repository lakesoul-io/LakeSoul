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
    // The maintained joins compare with equality (a NULL key never matches)
    // and the set operations also need the per-row match counts, so these
    // shapes are rejected instead of silently producing different rows.
    if join.null_equality == NullEquality::NullEqualsNull {
        return analyze_set_operation(join, projection, tables, request);
    }
    // Three or more joined tables flatten into one chained inner join.
    if join_tree_has_multiple_sources(join) {
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
    // are supported by the lookup join.
    let mut key_pairs = Vec::new();
    let mut conditions = Vec::new();
    for (left_expr, right_expr) in &join.on {
        let left_column = column_of(left_expr)
            .ok_or_else(|| unsupported("join keys must be plain columns"))?;
        let right_column = column_of(right_expr)
            .ok_or_else(|| unsupported("join keys must be plain columns"))?;
        key_pairs.push((left_column.name.clone(), right_column.name.clone()));
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
                key_pairs.push((left_column.name.clone(), right_column.name.clone()));
            } else {
                let (left_column, right_column, op) =
                    column_compare(conjunct, left_alias, right_alias, left, right)?;
                conditions.push(SemiAntiCondition {
                    left_column,
                    right_column,
                    op,
                });
            }
        }
    }
    if key_pairs.is_empty() {
        return Err(unsupported("join without an equality key"));
    }
    key_pairs.sort();
    key_pairs.dedup();
    let join_keys = key_pairs
        .iter()
        .map(|(left, _)| left.clone())
        .collect::<Vec<_>>();
    let right_keys = key_pairs
        .iter()
        .map(|(_, right)| right.clone())
        .collect::<Vec<_>>();
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
            // One payload per side keeps the compact `left_value` /
            // `right_value` shape; anything else materializes the selected
            // columns under their select names.
            let (left_value, right_value, output_columns, pair_filter) =
                if payloads.left_count == 1 && payloads.right_count == 1 {
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
                    (left_value, right_value, Vec::new(), pair_filter)
                } else {
                    if payloads.left_count == 0 || payloads.right_count == 0 {
                        return Err(unsupported(
                            "a join view needs at least one column from each side",
                        ));
                    }
                    check_wide_output_names(&payloads.output_columns, &key_names)?;
                    let pair_filter = render_wide_pair_conditions(
                        &conditions,
                        &payloads.output_columns,
                        "inner join",
                    )?;
                    (
                        payloads.left_value.clone().expect("one left payload"),
                        payloads.right_value.clone().expect("one right payload"),
                        payloads.output_columns,
                        pair_filter,
                    )
                };
            Ok(ViewSpec::Join {
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                output_table_id: request.mv_table_id.clone(),
                join_keys,
                right_keys,
                left_value,
                right_value,
                output_columns,
                left_filter,
                right_filter,
                pair_filter,
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
                left_filter,
                right_filter,
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
                right_filter,
                left_filter,
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
            let (left_value, right_value, output_columns) =
                compact_or_wide(payloads, &key_names, "FULL JOIN")?;
            Ok(ViewSpec::FullJoin {
                view_id: request.view_id.clone(),
                left_table_id: left.table_id.clone(),
                right_table_id: right.table_id.clone(),
                output_table_id: request.mv_table_id.clone(),
                join_keys,
                right_keys,
                left_value,
                right_value,
                output_columns,
                left_filter,
                right_filter,
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
            // to one side.
            let mut payloads = JoinPayloads::default();
            for field in join.schema.fields() {
                let column = datafusion::common::Column::new_unqualified(field.name());
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
            });
        }
    }
    let (left_value, right_value, output_columns, pair_filter) = if payloads.left_count
        == 1
        && payloads.right_count == 1
    {
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
        check_wide_output_names(&payloads.output_columns, &[])?;
        let pair_filter = render_wide_pair_conditions(
            &conditions,
            &payloads.output_columns,
            "cross join",
        )?;
        (
            payloads.left_value.clone().expect("one left payload"),
            payloads.right_value.clone().expect("one right payload"),
            payloads.output_columns,
            pair_filter,
        )
    };
    Ok(ViewSpec::CrossJoin {
        view_id: request.view_id.clone(),
        left_table_id: left.table_id.clone(),
        right_table_id: right.table_id.clone(),
        output_table_id: request.mv_table_id.clone(),
        left_value,
        right_value,
        output_columns,
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
    left_filter: Option<String>,
    right_filter: Option<String>,
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
    let (left_value, right_value, output_columns) =
        compact_or_wide(payloads, &key_names, kind)?;
    let lookup = {
        let mut expected = right.primary_keys.clone();
        expected.sort();
        let mut keys = effective_right_keys;
        keys.sort();
        !expected.is_empty() && expected == keys
    };
    if lookup {
        Ok(ViewSpec::LookupJoin {
            view_id: request.view_id.clone(),
            left_table_id: left.table_id.clone(),
            right_table_id: right.table_id.clone(),
            output_table_id: request.mv_table_id.clone(),
            join_keys,
            right_keys,
            left_filter,
            right_filter,
            left_value,
            right_value,
            output_columns,
        })
    } else if !left.primary_keys.is_empty() && !right.primary_keys.is_empty() {
        Ok(ViewSpec::LeftJoin {
            view_id: request.view_id.clone(),
            left_table_id: left.table_id.clone(),
            right_table_id: right.table_id.clone(),
            output_table_id: request.mv_table_id.clone(),
            join_keys,
            right_keys,
            left_value,
            right_value,
            output_columns,
            left_filter,
            right_filter,
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
                if !projection
                    .expr
                    .iter()
                    .all(|expr| matches!(expr, Expr::Column(_)))
                {
                    return Err(unsupported(
                        "join input must be a table or a plain column projection",
                    ));
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
    /// How many payload columns the left side contributes.
    left_count: usize,
    /// How many payload columns the right side contributes.
    right_count: usize,
}

impl JoinPayloads {
    /// Record one payload column from `side`.
    fn push(&mut self, side: Side, column: String, name: String) {
        match side {
            Side::Left => {
                self.left_count += 1;
                if self.left_value.is_none() {
                    self.left_value = Some(column.clone());
                }
            }
            Side::Right => {
                self.right_count += 1;
                if self.right_value.is_none() {
                    self.right_value = Some(column.clone());
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
) -> Result<(String, String, Vec<JoinOutputColumn>)> {
    if payloads.left_count == 1 && payloads.right_count == 1 {
        return Ok((
            payloads.left_value.clone().expect("one left payload"),
            payloads.right_value.clone().expect("one right payload"),
            Vec::new(),
        ));
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
    ))
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
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                left_filter: None,
                right_filter: None,
                pair_filter: None,
            }
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

        // A non-inner join in the tree is rejected.
        assert!(
            analyze_optimized(
                "select a.k, b.g, c.v from src a join src b on a.k = b.k \
                 left join src c on b.k = c.k",
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

        // A side without a payload is rejected (duplicate or ambiguous
        // select names are already rejected by the planner).
        let error = analyze_optimized(
            "select a.k, a.g, a.v, b.k from src a join src b on a.k = b.k",
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("each side"), "{error}");
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

        // A condition over a column the pair does not carry is rejected.
        assert!(
            analyze_optimized(
                "select a.k, a.v, b.v from src a join src b \
                 on a.k = b.k and a.g < b.g",
            )
            .await
            .is_err()
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
    async fn rejects_filtered_join_inputs() {
        // The same through an EXISTS subquery whose sides carry filters is
        // maintained (see `analyzes_filtered_semi_anti_inputs`).
        // A cross-side predicate on a CROSS JOIN stays unsupported.
        assert!(
            analyze_optimized(
                "select a.v, b.v from src a cross join src b where a.v < b.v"
            )
            .await
            .is_err()
        );
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
                left_filter: None,
                right_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
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
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                left_filter: None,
                right_filter: None,
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
                left_filter: None,
                right_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
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
                left_filter: None,
                right_filter: None,
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
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
                view_id: "view_1".to_string(),
                left_table_id: "table_src".to_string(),
                right_table_id: "table_src".to_string(),
                output_table_id: "table_mv".to_string(),
                join_keys: vec!["k".to_string()],
                right_keys: Vec::new(),
                left_value: "v".to_string(),
                right_value: "v".to_string(),
                output_columns: Vec::new(),
                left_filter: None,
                right_filter: None,
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
}
