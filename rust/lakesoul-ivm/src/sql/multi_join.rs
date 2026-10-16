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
    /// The number of computed join keys materialized so far (they are named
    /// by their index, see [`join_key_expression_name`]).
    expression_keys: usize,
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
        expression_keys: 0,
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
                return Err(unsupported(format!(
                    "an outer {} inside a three-or-more-table chain is not \
                     maintained (the flattened chain only supports inner \
                     joins; a two-table outer join and per-step lookups are \
                     maintained separately)",
                    join.join_type,
                )));
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
            let mut key_counter = flat.expression_keys;
            for (left_expr, right_expr) in &join.on {
                let first =
                    multi_key_operand(left_expr, &flat.inputs, limit, &mut key_counter)?;
                let second =
                    multi_key_operand(right_expr, &flat.inputs, limit, &mut key_counter)?;
                let in_right = |source: usize| source >= right_start;
                let (left, right) = match (
                    in_right(first.source),
                    in_right(second.source),
                ) {
                    (false, true) => (first, second),
                    (true, false) => (second, first),
                    _ => {
                        return Err(unsupported(
                            "each join key must connect the joined source to an earlier one",
                        ));
                    }
                };
                flat.keys.push(MultiJoinKey {
                    left_source: left.source,
                    left_column: left.column,
                    left_expr: left.expr,
                    right_source: right.source,
                    right_column: right.column,
                    right_expr: right.expr,
                });
            }
            flat.expression_keys = key_counter;
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

/// One operand of a flattened join key: a plain column of one source, or a
/// scalar expression over exactly one source (materialized under a hidden
/// name by the runtime).
struct MultiKeyOperand {
    source: usize,
    column: String,
    expr: Option<String>,
}

/// Resolve one side of a join key into the source it reads.
fn multi_key_operand(
    operand: &Expr,
    inputs: &[JoinInput<'_>],
    limit: usize,
    counter: &mut usize,
) -> Result<MultiKeyOperand> {
    if let Some(column) = column_of(operand) {
        return Ok(MultiKeyOperand {
            source: source_of_multi(inputs, column, limit)?,
            column: column.name.clone(),
            expr: None,
        });
    }
    // An expression key reads exactly one of the flattened sources; the
    // runtime materializes it under a hidden name before the join.
    let mut columns = Vec::new();
    operand
        .apply(|node| {
            if let Expr::Column(column) = node {
                columns.push(column.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| unsupported(format!("a join key expression {error}")))?;
    let mut source = None;
    for column in &columns {
        let found = source_of_multi(inputs, column, limit)?;
        match source {
            None => source = Some(found),
            Some(existing) if existing == found => {}
            Some(_) => {
                return Err(unsupported(
                    "a join key expression must read one of the joined sources",
                ));
            }
        }
    }
    let source = source.ok_or_else(|| {
        unsupported("a join key expression must read one of the joined sources")
    })?;
    let column = join_key_expression_name(*counter);
    *counter += 1;
    Ok(MultiKeyOperand {
        source,
        column,
        expr: Some(render_expression(operand)?),
    })
}

/// The source a chain column belongs to: its relation when it has one, else
/// the single source whose schema carries the name.
fn chain_source_of(
    column: &Column,
    chain: &[&IvmTable],
    aliases: &[Option<String>],
) -> Option<usize> {
    if let Some(relation) = &column.relation {
        for (candidate, table) in chain.iter().enumerate() {
            let name = aliases[candidate]
                .clone()
                .unwrap_or_else(|| table.table_name.clone());
            if relation.table() == name.as_str() {
                return Some(candidate);
            }
        }
        return None;
    }
    let matches = chain
        .iter()
        .enumerate()
        .filter(|(_, table)| table.schema.field_with_name(&column.name).is_ok())
        .map(|(candidate, _)| candidate)
        .collect::<Vec<_>>();
    (matches.len() == 1).then_some(matches[0])
}

/// The single source a chain key expression reads.
fn chain_expression_source(
    operand: &Expr,
    chain: &[&IvmTable],
    aliases: &[Option<String>],
) -> Result<usize> {
    let mut columns = Vec::new();
    operand
        .apply(|node| {
            if let Expr::Column(column) = node {
                columns.push(column.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|error| unsupported(format!("a lookup chain key expression {error}")))?;
    let mut source = None;
    for column in &columns {
        let found = chain_source_of(column, chain, aliases).ok_or_else(|| {
            unsupported(format!(
                "lookup chain step key {} is ambiguous",
                column.name
            ))
        })?;
        match source {
            None => source = Some(found),
            Some(existing) if existing == found => {}
            Some(_) => {
                return Err(unsupported(
                    "a lookup chain key expression must read one source",
                ));
            }
        }
    }
    source.ok_or_else(|| {
        unsupported("a lookup chain key expression must read one of the joined sources")
    })
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
    let limit = crate::runtime::multi_join_source_limit(keyed);
    if flat.inputs.len() > limit {
        return Err(unsupported(format!(
            "a multi-table join supports at most {limit} {} sources",
            if keyed { "keyed" } else { "append-only" }
        )));
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

/// Peel the aliases and plain projections a chain node may carry.
fn peel_chain_node(plan: &LogicalPlan) -> &LogicalPlan {
    let mut node = peel(plan);
    loop {
        match node {
            LogicalPlan::Projection(projection)
                if projection
                    .expr
                    .iter()
                    .all(|expr| matches!(expr, Expr::Column(_))) =>
            {
                node = peel(&projection.input)
            }
            _ => return node,
        }
    }
}

/// Whether any step of a join tree matches its keys null-safely.
///
/// The flattened chain and the lookup chain compare with strict equality, so
/// a null-safe step (including a nested one) is rejected instead of silently
/// producing different rows.
pub(super) fn join_tree_has_null_safe_step(join: &Join) -> bool {
    /// One subtree: peel the wrappers a join node may carry, then recurse.
    fn step(plan: &LogicalPlan) -> bool {
        let mut node = peel(plan);
        loop {
            match node {
                LogicalPlan::SubqueryAlias(alias) => node = &alias.input,
                LogicalPlan::Projection(projection) => node = &projection.input,
                LogicalPlan::Filter(filter) => node = &filter.input,
                _ => break,
            }
        }
        match node {
            LogicalPlan::Join(join) => {
                join.null_equality == NullEquality::NullEqualsNull
                    || step(&join.left)
                    || step(&join.right)
            }
            _ => false,
        }
    }
    join.null_equality == NullEquality::NullEqualsNull
        || step(&join.left)
        || step(&join.right)
}

/// Whether a join tree contains a step that is not an inner join.
pub(super) fn join_tree_has_outer_step(plan: &LogicalPlan) -> bool {
    let mut node = peel(plan);
    loop {
        match node {
            LogicalPlan::SubqueryAlias(alias) => node = &alias.input,
            LogicalPlan::Projection(projection) => node = &projection.input,
            LogicalPlan::Filter(filter) => node = &filter.input,
            _ => break,
        }
    }
    match node {
        LogicalPlan::Join(join) => {
            join.join_type != JoinType::Inner
                || join_tree_has_outer_step(&join.left)
                || join_tree_has_outer_step(&join.right)
        }
        _ => false,
    }
}

/// Whether a chain side is a compound subplan (a derived source) rather than
/// a plain table: the projection above it may keep the columns under aliases.
fn chain_side_is_derived(plan: &LogicalPlan) -> bool {
    if derived_sourceable(plan) {
        return true;
    }
    match peel(plan) {
        LogicalPlan::Projection(projection) => derived_sourceable(&projection.input),
        _ => false,
    }
}

/// A left-deep chain of keyed 1:1 lookups over a base table: the shape a
/// star-schema `[LEFT] JOIN` chain flattens to.
pub(super) fn analyze_lookup_chain(
    outer: &Join,
    projection: Option<&Projection>,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    let Some(projection) = projection else {
        return Err(unsupported("a lookup chain needs a projection"));
    };
    if !is_plain_projection(projection) {
        return Err(unsupported("computed columns in a lookup chain output"));
    }
    // A chain whose outermost join is a RIGHT or FULL join is not a
    // left-deep lookup chain: materialize its left side as a derived step
    // and let the two-source outer-join paths handle the outer join.
    if matches!(outer.join_type, JoinType::Right | JoinType::Full)
        && chain_side_is_derived(&outer.left)
    {
        return Err(derived_source_error("a lookup chain step", &outer.left));
    }
    // Collect the left-deep steps from the outermost join inward.  The
    // aliases are kept: the base alias names its columns in the step keys.
    let mut joins = vec![outer];
    let mut node: &LogicalPlan = &outer.left;
    loop {
        match node {
            LogicalPlan::Projection(projection)
                if projection
                    .expr
                    .iter()
                    .all(|expr| matches!(expr, Expr::Column(_))) =>
            {
                node = &projection.input;
            }
            LogicalPlan::Join(join) => {
                // An inner step that is a RIGHT or FULL join is not part of
                // a left-deep lookup chain: materialize it as a derived step
                // for the two-source outer-join paths.
                if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                    return Err(derived_source_error("a lookup chain step", node));
                }
                joins.push(join);
                node = &join.left;
            }
            // A chain side that is a compound subplan (for example a join
            // under a projection) is materialized as a derived step first;
            // the two-source join paths then handle the outer step.
            _ if chain_side_is_derived(node) => {
                return Err(derived_source_error("a lookup chain side", node));
            }
            _ => break,
        }
    }
    if joins.len() < 2 {
        return Err(unsupported("a lookup chain needs at least three sources"));
    }
    joins.reverse();
    // The base of the chain.
    let base_input = join_input(node, tables)?;
    let base = base_input.table;
    if base.primary_keys.is_empty() {
        return Err(unsupported("a lookup chain needs a keyed base"));
    }
    let mut aliases = vec![base_input.alias.clone()];
    let mut sources = vec![LookupChainSource {
        table_id: base.table_id.clone(),
        filter: base_input.filter.clone(),
    }];
    let mut chain = vec![base];
    let mut steps = Vec::new();
    for (index, join) in joins.iter().enumerate() {
        if !matches!(join.join_type, JoinType::Inner | JoinType::Left) {
            return Err(unsupported(
                "a lookup chain step must be an inner or left join",
            ));
        }
        if join.filter.is_some() {
            return Err(unsupported("a lookup chain step with a residual condition"));
        }
        if matches!(peel_chain_node(&join.right), LogicalPlan::Join(_)) {
            return Err(unsupported("a lookup chain must be left-deep"));
        }
        let step_input = join_input(&join.right, tables)?;
        let step = step_input.table;
        if step.primary_keys.is_empty() {
            return Err(unsupported(format!(
                "a lookup chain step source {} needs a primary key",
                step.table_name
            )));
        }
        let mut keys = Vec::new();
        let mut right_keys = Vec::new();
        let mut key_sources = Vec::new();
        let mut left_key_exprs = Vec::new();
        let mut right_key_exprs = Vec::new();
        let mut expression_keys = 0usize;
        for (left_expr, right_expr) in &join.on {
            // The left key is a column of an earlier source or a scalar
            // expression over one of them; the right key is a column of the
            // step source or an expression over it.  A computed key is
            // materialized under a hidden name.
            let (left_name, left_source, left_rendered) = match column_of(left_expr) {
                Some(column) => {
                    let source =
                        chain_source_of(column, &chain, &aliases).ok_or_else(|| {
                            unsupported(format!(
                                "lookup chain step key {} is ambiguous",
                                column.name
                            ))
                        })?;
                    if source > index {
                        return Err(unsupported(format!(
                            "lookup chain step key {} must come from an earlier source",
                            column.name
                        )));
                    }
                    (column.name.clone(), source, String::new())
                }
                None => {
                    let source = chain_expression_source(left_expr, &chain, &aliases)?;
                    if source > index {
                        return Err(unsupported(
                            "a lookup chain step key must come from an earlier source",
                        ));
                    }
                    let name = join_key_expression_name(expression_keys);
                    expression_keys += 1;
                    (name, source, render_expression(left_expr)?)
                }
            };
            let (right_name, right_rendered) = match column_of(right_expr) {
                Some(column) => (column.name.clone(), String::new()),
                None => {
                    // A step's own computed key is materialized on the step's
                    // frame, so it must read that source only (the step source
                    // is not part of the chain yet).
                    let mut with_step = chain.clone();
                    with_step.push(step);
                    let mut step_aliases = aliases.clone();
                    step_aliases.push(step_input.alias.clone());
                    let source =
                        chain_expression_source(right_expr, &with_step, &step_aliases)?;
                    // The step source is `index + 1` in the chain.
                    if source != index + 1 {
                        return Err(unsupported(
                            "a lookup chain step's computed key must read the \
                             step source",
                        ));
                    }
                    let name = join_key_expression_name(expression_keys);
                    expression_keys += 1;
                    (name, render_expression(right_expr)?)
                }
            };
            keys.push(left_name);
            right_keys.push(right_name);
            key_sources.push(left_source);
            left_key_exprs.push(left_rendered);
            right_key_exprs.push(right_rendered);
        }
        if keys.is_empty() {
            return Err(unsupported("a lookup chain step needs a key"));
        }
        let mut pks = step.primary_keys.clone();
        pks.sort();
        pks.dedup();
        let mut step_keys = right_keys.clone();
        step_keys.sort();
        step_keys.dedup();
        // The join keys equal to the source key make the step a 1:1 lookup;
        // any other key is a 1:N lookup, whose matched row identity the MV
        // materializes.
        let unique = pks == step_keys;
        sources.push(LookupChainSource {
            table_id: step.table_id.clone(),
            filter: step_input.filter.clone(),
        });
        steps.push(LookupChainStep {
            source: index + 1,
            left: join.join_type == JoinType::Left,
            keys,
            right_keys,
            key_sources,
            key_exprs: JoinKeyExprs {
                left: left_key_exprs,
                right: right_key_exprs,
            },
            unique,
        });
        aliases.push(step_input.alias.clone());
        chain.push(step);
    }
    // The materialized columns.  Their names must stay unique: a generated
    // identity column or a same-named column of another source takes part in
    // the MV schema, so a collision is disambiguated deterministically (the
    // first occurrence keeps its name).
    let mut names = steps
        .iter()
        .filter(|step| !step.unique)
        .map(|step| crate::runtime::lookup_chain_step_id_column(step.source))
        .chain([
            crate::runtime::IVM_ROW_KINDS_COLUMN.to_string(),
            crate::runtime::IVM_EPOCH_COLUMN.to_string(),
        ])
        .collect::<Vec<_>>();
    let mut output_columns = Vec::new();
    for expr in &projection.expr {
        let mut inner = expr;
        let mut alias = None;
        while let Expr::Alias(nested) = inner {
            if alias.is_none() {
                alias = Some(nested.name.clone());
            }
            inner = &nested.expr;
        }
        let Expr::Column(column) = inner else {
            return Err(unsupported("computed columns in a lookup chain output"));
        };
        let mut source = None;
        if let Some(relation) = &column.relation {
            for (index, table) in chain.iter().enumerate() {
                let name = aliases[index]
                    .clone()
                    .unwrap_or_else(|| table.table_name.clone());
                if relation.table() == name.as_str() {
                    source = Some(index);
                    break;
                }
            }
        } else {
            let matches = chain
                .iter()
                .enumerate()
                .filter(|(_, table)| table.schema.field_with_name(&column.name).is_ok())
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if matches.len() == 1 {
                source = Some(matches[0]);
            }
        }
        let source = source.ok_or_else(|| {
            unsupported(format!(
                "lookup chain output column {} is ambiguous",
                column.name
            ))
        })?;
        let mut name = alias.unwrap_or_else(|| column.name.clone());
        if names.contains(&name) {
            let base = format!("{name}_{source}");
            name = base.clone();
            let mut index = 2;
            while names.contains(&name) {
                name = format!("{base}_{index}");
                index += 1;
            }
        }
        names.push(name.clone());
        output_columns.push(LookupChainColumn {
            source,
            column: column.name.clone(),
            name,
        });
    }
    Ok(ViewSpec::LookupChain {
        view_id: request.view_id.clone(),
        sources,
        steps,
        output_columns,
        mv_table_id: request.mv_table_id.clone(),
    })
}
