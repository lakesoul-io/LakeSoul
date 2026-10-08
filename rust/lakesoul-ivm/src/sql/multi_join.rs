// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The multi-way join analyzer: flattening a join tree into one chain.

use super::*;

/// Whether the join tree has more than the two sources of a plain pair join.
pub(super) fn join_tree_has_multiple_sources(join: &Join) -> bool {
    matches!(peel_join_node(&join.left), LogicalPlan::Join(_))
}

/// Peel the wrappers a join subtree may carry: aliases, the plain projections
/// column pruning adds and filters.
fn peel_join_node(mut plan: &LogicalPlan) -> &LogicalPlan {
    loop {
        plan = match plan {
            LogicalPlan::SubqueryAlias(alias) => &alias.input,
            LogicalPlan::Projection(projection) => &projection.input,
            LogicalPlan::Filter(filter) => &filter.input,
            other => return other,
        };
    }
}

/// The flattened shape of a multi-way inner join.
struct FlatJoin<'a> {
    inputs: Vec<JoinInput<'a>>,
    keys: Vec<MultiJoinKey>,
    conditions: Vec<MultiJoinCondition>,
}

/// Flatten a join tree (three or more sources) into a linear chain: the
/// sources are visited in post order, so every key pair connects a source to
/// an earlier one.
fn flatten_join_tree<'a>(
    plan: &'a LogicalPlan,
    tables: &'a HashMap<String, IvmTable>,
) -> Result<FlatJoin<'a>> {
    let mut flat = FlatJoin {
        inputs: Vec::new(),
        keys: Vec::new(),
        conditions: Vec::new(),
    };
    flatten_join_node(plan, tables, &mut flat)?;
    Ok(flat)
}

fn flatten_join_node<'a>(
    plan: &'a LogicalPlan,
    tables: &'a HashMap<String, IvmTable>,
    flat: &mut FlatJoin<'a>,
) -> Result<()> {
    // Peel aliases and the plain projections column pruning adds.
    let mut node = plan;
    loop {
        match node {
            LogicalPlan::SubqueryAlias(alias) => node = &alias.input,
            LogicalPlan::Projection(projection)
                if projection
                    .expr
                    .iter()
                    .all(|expr| matches!(expr, Expr::Column(_))) =>
            {
                node = &projection.input;
            }
            _ => break,
        }
    }
    match node {
        LogicalPlan::Filter(filter) => {
            if matches!(peel_join_node(&filter.input), LogicalPlan::Join(_)) {
                flatten_join_node(&filter.input, tables, flat)?;
                let limit = flat.inputs.len();
                for conjunct in split_conjunction(&filter.predicate) {
                    classify_multi_conjunct(conjunct, flat, limit)?;
                }
                Ok(())
            } else {
                flat.inputs.push(join_input(plan, tables)?);
                Ok(())
            }
        }
        LogicalPlan::Join(join) => {
            if join.join_type != JoinType::Inner {
                return Err(unsupported(
                    "three or more table joins only support inner joins",
                ));
            }
            // Flatten the left side, then the right side; the keys of this
            // node connect the two sides.
            flatten_join_node(&join.left, tables, flat)?;
            let right_start = flat.inputs.len();
            if matches!(peel_join_node(&join.right), LogicalPlan::Join(_)) {
                flatten_join_node(&join.right, tables, flat)?;
            } else {
                flat.inputs.push(join_input(&join.right, tables)?);
            }
            let limit = flat.inputs.len();
            for (left_expr, right_expr) in &join.on {
                let first = column_of(left_expr)
                    .ok_or_else(|| unsupported("join keys must be plain columns"))?;
                let second = column_of(right_expr)
                    .ok_or_else(|| unsupported("join keys must be plain columns"))?;
                let first_source = source_of_multi(&flat.inputs, first, limit)?;
                let second_source = source_of_multi(&flat.inputs, second, limit)?;
                let in_right = |source: usize| source >= right_start;
                let (left_source, left_column, right_source, right_column) = match (
                    in_right(first_source),
                    in_right(second_source),
                ) {
                    (false, true) => (
                        first_source,
                        first.name.clone(),
                        second_source,
                        second.name.clone(),
                    ),
                    (true, false) => (
                        second_source,
                        second.name.clone(),
                        first_source,
                        first.name.clone(),
                    ),
                    _ => {
                        return Err(unsupported(
                            "each join key must connect the joined source to an earlier one",
                        ));
                    }
                };
                flat.keys.push(MultiJoinKey {
                    left_source,
                    left_column,
                    right_source,
                    right_column,
                });
            }
            if let Some(filter) = &join.filter {
                for conjunct in split_conjunction(filter) {
                    classify_multi_conjunct(conjunct, flat, limit)?;
                }
            }
            Ok(())
        }
        _ => {
            flat.inputs.push(join_input(plan, tables)?);
            Ok(())
        }
    }
}

/// Resolve a join column to one of the flattened sources.
fn source_of_multi(
    inputs: &[JoinInput<'_>],
    column: &Column,
    limit: usize,
) -> Result<usize> {
    if let Some(relation) = &column.relation {
        let relation = relation.table();
        for (index, input) in inputs[..limit].iter().enumerate() {
            let matches = match &input.alias {
                Some(alias) => relation == alias,
                None => relation == input.table.table_name,
            };
            if matches {
                return Ok(index);
            }
        }
        return Err(unsupported(format!(
            "join column {} does not belong to any joined source",
            column.name
        )));
    }
    let mut found = None;
    for (index, input) in inputs[..limit].iter().enumerate() {
        if input.table.schema.field_with_name(&column.name).is_ok() {
            if found.is_some() {
                return Err(unsupported(format!(
                    "join column {} is ambiguous; qualify it with its table alias",
                    column.name
                )));
            }
            found = Some(index);
        }
    }
    found.ok_or_else(|| {
        unsupported(format!(
            "join column {} is not in any joined source",
            column.name
        ))
    })
}

/// Classify a join condition of a multi-way join: a column comparison becomes
/// a pair condition, a single-source predicate becomes a side filter.
fn classify_multi_conjunct(
    conjunct: &Expr,
    flat: &mut FlatJoin<'_>,
    limit: usize,
) -> Result<()> {
    if let Expr::BinaryExpr(binary) = conjunct
        && let (Some(first), Some(second)) =
            (column_of(&binary.left), column_of(&binary.right))
        && let Ok(op) = compare_op(binary.op)
    {
        let first_source = source_of_multi(&flat.inputs, first, limit)?;
        let second_source = source_of_multi(&flat.inputs, second, limit)?;
        if first_source == second_source {
            return Err(unsupported(
                "join conditions between the same source are not supported",
            ));
        }
        let (left_source, left_column, right_source, right_column, op) =
            if first_source < second_source {
                (
                    first_source,
                    first.name.clone(),
                    second_source,
                    second.name.clone(),
                    op,
                )
            } else {
                (
                    second_source,
                    second.name.clone(),
                    first_source,
                    first.name.clone(),
                    flip(op),
                )
            };
        flat.conditions.push(MultiJoinCondition {
            left_source,
            left_column,
            right_source,
            right_column,
            op,
        });
        return Ok(());
    }
    // Every referenced column must belong to one source.
    let mut referenced: Vec<usize> = Vec::new();
    let mut failure: Option<rootcause::Report> = None;
    let _ = conjunct.apply(|node| {
        if let Expr::Column(column) = node {
            match source_of_multi(&flat.inputs, column, limit) {
                Ok(index) => {
                    if !referenced.contains(&index) {
                        referenced.push(index);
                    }
                }
                Err(error) => failure = Some(error),
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if let Some(error) = failure {
        return Err(error);
    }
    if referenced.len() == 1 {
        let rendered = render_filter(conjunct)?;
        let index = referenced[0];
        flat.inputs[index].filter =
            Some(combine_filter(flat.inputs[index].filter.take(), rendered));
        Ok(())
    } else {
        Err(unsupported(
            "a three or more table join condition must compare two columns",
        ))
    }
}

/// A three-or-more-table inner join over keyed or append-only sources.
pub(super) fn analyze_multi_join(
    join: &Join,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let plan = LogicalPlan::Join(join.clone());
    let flat = flatten_join_tree(&plan, tables)?;
    if flat.inputs.len() < 3 {
        return Err(unsupported(
            "a multi-table join needs at least three sources",
        ));
    }
    if flat.inputs.len() > 8 {
        return Err(unsupported(
            "a multi-table join supports at most eight sources",
        ));
    }
    if projection.is_some_and(|projection| !is_plain_projection(projection)) {
        return Err(unsupported("computed columns in a multi-table join output"));
    }
    let keyed = !flat.inputs[0].table.primary_keys.is_empty();
    for input in &flat.inputs {
        if keyed != !input.table.primary_keys.is_empty() {
            return Err(unsupported(
                "a multi-table join needs all sources keyed or all append-only",
            ));
        }
    }
    let mut columns = Vec::new();
    let mut names = Vec::new();
    let mut push_column = |source: usize, column: String, name: String| -> Result<()> {
        if names.contains(&name) {
            return Err(unsupported(format!(
                "multi-table join output column {name} is materialized twice; \
                 add distinct aliases"
            )));
        }
        names.push(name.clone());
        columns.push(MultiJoinColumn {
            source,
            column,
            name,
        });
        Ok(())
    };
    match projection {
        Some(projection) => {
            for expr in &projection.expr {
                let column = column_of(expr).ok_or_else(|| {
                    unsupported("multi-table join output must be plain columns")
                })?;
                let source = source_of_multi(&flat.inputs, column, flat.inputs.len())?;
                let name = match expr {
                    Expr::Alias(alias) => alias.name.clone(),
                    _ => column.name.clone(),
                };
                push_column(source, column.name.clone(), name)?;
            }
        }
        None => {
            // The optimizer drops the projection when the pruned join output
            // is exactly the select list.
            for (qualifier, field) in join.schema.iter() {
                let column = datafusion::common::Column::new(
                    qualifier.map(|qualifier| qualifier.table().to_string()),
                    field.name(),
                );
                let source = source_of_multi(&flat.inputs, &column, flat.inputs.len())?;
                push_column(source, field.name().clone(), field.name().clone())?;
            }
        }
    }
    if columns.is_empty() {
        return Err(unsupported(
            "a multi-table join needs at least one output column",
        ));
    }
    Ok(ViewSpec::MultiJoin {
        view_id: request.view_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        sources: flat
            .inputs
            .iter()
            .map(|input| MultiJoinSource {
                table_id: input.table.table_id.clone(),
                filter: input.filter.clone(),
            })
            .collect(),
        keys: flat.keys,
        columns,
        conditions: flat.conditions,
    })
}
