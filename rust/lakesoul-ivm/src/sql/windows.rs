// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The window and top-k analyzer.

use super::*;

/// A window view: one window function over one source.
pub(super) fn analyze_window(
    projection: Option<&Projection>,
    window: &Window,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<ViewSpec> {
    // A statement with several window clauses plans as chained `WindowAggr`
    // nodes (with an intermediate projection when the upper clause needs the
    // lower columns); collect the chain down to the source.
    let mut chain = vec![window];
    let mut node = peel(&window.input);
    loop {
        let inner = match node {
            LogicalPlan::Projection(projection) => peel(&projection.input),
            other => other,
        };
        let LogicalPlan::Window(inner) = inner else {
            break;
        };
        chain.push(inner);
        node = peel(&inner.input);
    }
    let (source, filter) = collect_filtered_source(node, tables, "a window")?;
    // Selecting a window expression itself (aliased or not) is fine;
    // computing on top of it would be silently dropped otherwise.
    let window_exprs = chain
        .iter()
        .flat_map(|window| window.window_expr.iter())
        .collect::<Vec<_>>();
    if let Some(projection) = projection {
        for expr in &projection.expr {
            let mut inner = expr;
            while let Expr::Alias(alias) = inner {
                inner = &alias.expr;
            }
            if column_name(inner).is_none() && !window_exprs.contains(&inner) {
                return Err(unsupported(
                    "computed columns above a window function are not supported",
                ));
            }
        }
    }
    // Every clause is parsed against the top projection, which names the
    // materialized columns; a derived window node (a compound source) has no
    // projection and keeps the function names.
    let parts = chain
        .iter()
        .map(|window| window_function_spec(window, projection))
        .collect::<Result<Vec<_>>>()?;
    if parts.len() == 1 {
        let WindowParts {
            partition_keys,
            order_keys,
            order_by,
            columns,
        } = parts.into_iter().next().expect("length checked");
        // A derived window step carries the input columns a consumer may
        // read: the window node's input fields that are not already keys or
        // window columns.
        let payload_columns = if request.derived {
            let mut payloads = Vec::new();
            for field in window.input.schema().fields() {
                let name = field.name().clone();
                // A step's own bookkeeping columns are never payloads.
                let internal = matches!(
                    name.as_str(),
                    crate::runtime::IVM_ROW_KINDS_COLUMN
                        | crate::runtime::IVM_EPOCH_COLUMN
                        | crate::runtime::IVM_SOURCE_COLUMN
                );
                let carried = internal
                    || partition_keys.contains(&name)
                    || source.primary_keys.contains(&name)
                    || columns.iter().any(|column| column.column == name);
                if !carried && !payloads.contains(&name) {
                    payloads.push(name);
                }
            }
            payloads
        } else {
            Vec::new()
        };
        return Ok(ViewSpec::Window {
            view_id: request.view_id.clone(),
            source_table_id: source.table_id.clone(),
            mv_table_id: request.mv_table_id.clone(),
            partition_keys,
            order_keys,
            order_by,
            columns,
            payload_columns,
            filter,
        });
    }
    Ok(ViewSpec::MultiWindow {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        windows: parts
            .into_iter()
            .map(|parts| WindowGroupSpec {
                partition_keys: parts.partition_keys,
                order_keys: parts.order_keys,
                order_by: parts.order_by,
                columns: parts.columns,
            })
            .collect(),
        filter,
    })
}

/// A global top-k over one keyed source: `ORDER BY ... LIMIT k`.
///
/// DataFusion plans the statement as a `Sort` with a `fetch`; the view keeps
/// the rows whose `ROW_NUMBER()` over the ordering (the runtime appends the
/// source primary keys as the final tie-break) is within the limit.  The raw
/// ordering decides the ranking; the primary-key tie-break only makes the
/// boundary deterministic, because the statement's own tie-break is
/// arbitrary.
pub(super) fn analyze_order_by_limit(
    projection: Option<&Projection>,
    sort: &Sort,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
    offset: i64,
    limit: Option<i64>,
) -> Result<ViewSpec> {
    let fetch = match limit {
        Some(limit) => limit,
        None => {
            let Some(fetch) = sort.fetch else {
                return Err(unsupported(
                    "ORDER BY without LIMIT is not materialized (add a LIMIT)",
                ));
            };
            i64::try_from(fetch).map_err(|_| unsupported("the LIMIT is too large"))?
        }
    };
    if fetch == 0 {
        return Err(unsupported("LIMIT 0 is not maintained"));
    }
    if offset < 0 {
        return Err(unsupported("a negative OFFSET is not maintained"));
    }
    // The rank window is `(offset, offset + limit]`.
    offset
        .checked_add(fetch)
        .ok_or_else(|| unsupported("the LIMIT plus OFFSET is too large"))?;
    let (source, filter) = collect_filtered_source(&sort.input, tables, "a top-k")?;
    // The select list may sit below the Sort when the statement does not add
    // a projection of its own.
    let select_projection = match projection {
        Some(projection) => Some(projection),
        None => match peel(&sort.input) {
            LogicalPlan::Projection(projection) => Some(projection),
            _ => None,
        },
    };
    // The projected columns; a select item that is an expression (rather
    // than a plain column) is materialized by its rendered form.
    let (output_columns, output_exprs) = match select_projection {
        Some(projection) => {
            let columns = projection
                .expr
                .iter()
                .zip(projection.schema.iter())
                .map(|(_, (_, field))| field.name().clone())
                .collect::<Vec<_>>();
            let computed = projection
                .expr
                .iter()
                .any(|expr| column_name(expr).is_none());
            let exprs = if computed {
                projection
                    .expr
                    .iter()
                    .map(|expr| match column_name(expr) {
                        Some(column) => Ok(column),
                        None => render_filter(expr),
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            (columns, exprs)
        }
        // The select list was pruned into the input; its schema is the
        // projected column set.
        None => (
            sort.input
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            Vec::new(),
        ),
    };
    let mut order_keys = Vec::with_capacity(sort.expr.len());
    let mut order_by = Vec::with_capacity(sort.expr.len());
    for item in &sort.expr {
        match column_name(&item.expr) {
            Some(column) => {
                // An `ORDER BY` over a computed select alias orders by the
                // expression the alias stands for.
                let asked_for_alias = source.schema.field_with_name(&column).is_err()
                    && !output_exprs.is_empty();
                if asked_for_alias
                    && let Some(index) =
                        output_columns.iter().position(|output| output == &column)
                {
                    let rendered = output_exprs[index].clone();
                    order_by.push(render_order_rendered(
                        &rendered,
                        item.asc,
                        item.nulls_first,
                    ));
                    order_keys.push(rendered);
                } else {
                    order_by.push(render_order_key(&column, item.asc, item.nulls_first));
                    order_keys.push(column);
                }
            }
            None => {
                order_by.push(render_order_expr(&item.expr, item.asc, item.nulls_first)?);
                order_keys.push(render_filter(&item.expr)?);
            }
        }
    }
    if order_keys.is_empty() {
        return Err(unsupported("ORDER BY ... LIMIT needs an ordering"));
    }
    let order_by = if order_by == order_keys {
        Vec::new()
    } else {
        order_by
    };
    Ok(ViewSpec::TopK {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        group_keys: Vec::new(),
        order_keys,
        order_by,
        output_columns,
        output_exprs,
        limit: fetch,
        offset,
        filter,
    })
}

/// The top `k` rows per group: a filter on the `row_number()` column of a
/// window expression.
pub(super) fn try_analyze_top_k(
    projection: Option<&Projection>,
    filter: &Filter,
    tables: &HashMap<String, IvmTable>,
    request: &AnalyzeRequest,
) -> Result<Option<ViewSpec>> {
    let mut node = peel(&filter.input);
    loop {
        match peel(node) {
            LogicalPlan::Projection(inner) => node = &inner.input,
            LogicalPlan::Window(_) => break,
            _ => return Ok(None),
        }
    }
    let LogicalPlan::Window(window) = peel(node) else {
        return Ok(None);
    };
    let WindowParts {
        partition_keys,
        order_keys,
        order_by,
        columns,
    } = window_function_spec(window, None)?;
    if columns.len() != 1
        || columns[0].function != WindowFunction::RowNumber
        || columns[0].value_column.is_some()
    {
        return Ok(None);
    }
    // The filter references the window column through the subquery scope: it
    // is the single column the filter's input adds on top of the source.
    let source_fields = window
        .input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<std::collections::HashSet<_>>();
    let rank_columns = filter
        .input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !source_fields.contains(name))
        .collect::<Vec<_>>();
    if rank_columns.len() != 1 {
        return Ok(None);
    }
    let rank_column = &rank_columns[0];

    let conjuncts = split_conjunction(&filter.predicate);
    if conjuncts.len() != 1 {
        return Ok(None);
    }
    let limit = match conjuncts[0] {
        Expr::BinaryExpr(binary) => match (&*binary.left, binary.op, &*binary.right) {
            (Expr::Column(column), Operator::LtEq, right)
                if &column.name == rank_column =>
            {
                int_literal(right)
            }
            (left, Operator::GtEq, Expr::Column(column))
                if &column.name == rank_column =>
            {
                int_literal(left)
            }
            _ => None,
        },
        _ => None,
    };
    let Some(limit) = limit.filter(|limit| *limit > 0) else {
        return Ok(None);
    };
    if partition_keys.is_empty() || order_keys.is_empty() {
        return Err(unsupported("top-k needs PARTITION BY and ORDER BY columns"));
    }
    let output_columns = match projection {
        Some(projection) => {
            let columns = projection
                .expr
                .iter()
                .map(column_name)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| unsupported("computed columns above a top-k filter"))?;
            if columns.iter().any(|column| column == rank_column) {
                return Err(unsupported(
                    "top-k output may not include the row-number column",
                ));
            }
            columns
        }
        None => Vec::new(),
    };
    let (source, filter) = collect_filtered_source(&window.input, tables, "top-k")?;
    Ok(Some(ViewSpec::TopK {
        view_id: request.view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: request.mv_table_id.clone(),
        group_keys: partition_keys,
        order_keys,
        order_by,
        output_columns,
        output_exprs: Vec::new(),
        limit,
        offset: 0,
        filter,
    }))
}

/// The window columns of a window node plus their shared partition and
/// ordering.
struct WindowParts {
    partition_keys: Vec<String>,
    order_keys: Vec<String>,
    /// The rendered ordering; empty when it is the plain ascending default.
    order_by: Vec<String>,
    columns: Vec<WindowColumn>,
}

/// The window columns of a window node, sharing one `PARTITION BY`/`ORDER BY`
/// clause.
fn window_function_spec(
    window: &Window,
    projection: Option<&Projection>,
) -> Result<WindowParts> {
    if window.window_expr.is_empty() {
        return Err(unsupported("window node without window functions"));
    }
    let mut partition_keys: Option<Vec<String>> = None;
    let mut order_keys: Option<Vec<String>> = None;
    let mut order_by: Option<Vec<String>> = None;
    let mut columns = Vec::with_capacity(window.window_expr.len());
    for expr in &window.window_expr {
        let Expr::WindowFunction(function) = expr else {
            return Err(unsupported("non-window expression in a window node"));
        };
        let params = &function.params;
        let expr_partition = params
            .partition_by
            .iter()
            .map(column_name)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| unsupported("PARTITION BY expressions must be columns"))?;
        let mut expr_order = Vec::with_capacity(params.order_by.len());
        let mut expr_order_by = Vec::with_capacity(params.order_by.len());
        for sort in &params.order_by {
            match column_name(&sort.expr) {
                Some(column) => {
                    expr_order_by.push(render_order_key(
                        &column,
                        sort.asc,
                        sort.nulls_first,
                    ));
                    expr_order.push(column);
                }
                None => {
                    // A rendered expression: the keys hold the plain
                    // expression (for validation), the ordering adds the
                    // direction and wraps the expression in parentheses.
                    expr_order_by.push(render_order_expr(
                        &sort.expr,
                        sort.asc,
                        sort.nulls_first,
                    )?);
                    expr_order.push(render_filter(&sort.expr)?);
                }
            }
        }
        match (&partition_keys, &order_keys, &order_by) {
            (Some(partition), Some(order), Some(rendered)) => {
                if partition != &expr_partition
                    || order != &expr_order
                    || rendered != &expr_order_by
                {
                    return Err(unsupported(
                        "multiple window functions with different PARTITION BY/ORDER BY",
                    ));
                }
            }
            _ => {
                partition_keys = Some(expr_partition.clone());
                order_keys = Some(expr_order.clone());
                order_by = Some(expr_order_by.clone());
            }
        }
        let mut column = window_column(function, &expr_order)?;
        column.column = projection
            .and_then(|projection| projection_alias(projection, expr))
            .unwrap_or_else(|| column.function.column_name().to_string());
        columns.push(column);
    }
    let mut names = std::collections::HashSet::new();
    for column in &columns {
        if !names.insert(column.column.clone()) {
            return Err(unsupported(format!(
                "duplicate window column {}; alias the repeated functions",
                column.column
            )));
        }
    }
    let order_by = order_by.unwrap_or_default();
    let order_keys = order_keys.unwrap_or_default();
    // The plain ascending ordering is the default; keep the spec compact.
    let order_by = if order_by == order_keys {
        Vec::new()
    } else {
        order_by
    };
    Ok(WindowParts {
        partition_keys: partition_keys.unwrap_or_default(),
        order_keys,
        order_by,
        columns,
    })
}

/// Render one `ORDER BY` item: the ascending `NULLS LAST` default stays the
/// bare column, anything else is spelled out.
pub(super) fn render_order_key(column: &str, asc: bool, nulls_first: bool) -> String {
    match (asc, nulls_first) {
        (true, false) => column.to_string(),
        (true, true) => format!("{} asc nulls first", quote_order_identifier(column)),
        (false, true) => format!("{} desc nulls first", quote_order_identifier(column)),
        (false, false) => format!("{} desc nulls last", quote_order_identifier(column)),
    }
}

/// Quote an identifier for a rendered ordering item.
fn quote_order_identifier(column: &str) -> String {
    format!("\"{}\"", column.replace('"', "\"\""))
}

/// Render an `ORDER BY` item from an already rendered expression.
pub(super) fn render_order_rendered(
    rendered: &str,
    asc: bool,
    nulls_first: bool,
) -> String {
    match (asc, nulls_first) {
        (true, false) => format!("({rendered})"),
        (true, true) => format!("({rendered}) asc nulls first"),
        (false, false) => format!("({rendered}) desc nulls last"),
        (false, true) => format!("({rendered}) desc nulls first"),
    }
}

/// Render one `ORDER BY` item whose expression is not a plain column.
pub(super) fn render_order_expr(
    expr: &Expr,
    asc: bool,
    nulls_first: bool,
) -> Result<String> {
    let rendered = render_filter(expr)?;
    Ok(match (asc, nulls_first) {
        (true, false) => format!("({rendered})"),
        (true, true) => format!("({rendered}) asc nulls first"),
        (false, false) => format!("({rendered}) desc nulls last"),
        (false, true) => format!("({rendered}) desc nulls first"),
    })
}

/// One window column parsed from its function expression.
fn window_column(
    function: &datafusion::logical_expr::expr::WindowFunction,
    order_keys: &[String],
) -> Result<WindowColumn> {
    let params = &function.params;
    let (function, value_column, window_args) = match &function.fun {
        WindowFunctionDefinition::WindowUDF(udf) => match udf.name() {
            "row_number" => (WindowFunction::RowNumber, None, None),
            "rank" => (WindowFunction::Rank, None, None),
            "dense_rank" => (WindowFunction::DenseRank, None, None),
            "lag" | "lead" => {
                let function = if udf.name() == "lag" {
                    WindowFunction::Lag
                } else {
                    WindowFunction::Lead
                };
                let (value, args) = window_shift_args(&params.args)?;
                (function, Some(value), args)
            }
            "first_value" | "last_value" => {
                let function = if udf.name() == "first_value" {
                    WindowFunction::FirstValue
                } else {
                    WindowFunction::LastValue
                };
                (function, Some(window_value_arg(&params.args)?), None)
            }
            "nth_value" => {
                let (value, args) = window_nth_value_args(&params.args)?;
                (WindowFunction::NthValue, Some(value), args)
            }
            "ntile" => (
                WindowFunction::Ntile,
                None,
                Some(positive_integer_arg(
                    &params.args,
                    "the NTILE bucket count",
                )?),
            ),
            "percent_rank" => (WindowFunction::PercentRank, None, None),
            "cume_dist" => (WindowFunction::CumeDist, None, None),
            other => {
                return Err(unsupported(format!("window function {other}")));
            }
        },
        WindowFunctionDefinition::AggregateUDF(udf) => match udf.name() {
            "sum" => (
                WindowFunction::Sum,
                Some(single_column_arg(&params.args)?),
                None,
            ),
            "count" => {
                let counts_all = params.args.is_empty()
                    || matches!(params.args.as_slice(), [Expr::Literal(..)]);
                (
                    WindowFunction::Count,
                    if counts_all {
                        None
                    } else {
                        Some(single_column_arg(&params.args)?)
                    },
                    None,
                )
            }
            other => {
                return Err(unsupported(format!("window aggregate {other}")));
            }
        },
    };
    let window_frame = window_frame_sql(&params.window_frame, !order_keys.is_empty())?;
    if !function.is_aggregate() && order_keys.is_empty() {
        return Err(unsupported("ranking window functions need ORDER BY"));
    }
    let window_filter = match &params.filter {
        Some(filter) => {
            if !function.is_aggregate() {
                return Err(unsupported("FILTER on a non-aggregate window function"));
            }
            Some(render_filter(filter)?)
        }
        None => None,
    };
    // `IGNORE NULLS` only changes the value functions; for ranking and
    // aggregate windows it is a no-op, so it is normalized away.
    let ignore_nulls = function.is_value()
        && matches!(params.null_treatment, Some(NullTreatment::IgnoreNulls));
    Ok(WindowColumn {
        function,
        value_column,
        window_args,
        window_filter,
        ignore_nulls,
        window_frame,
        column: String::new(),
    })
}

/// The value column of a value window function (`FIRST_VALUE`/`LAST_VALUE`).
fn window_value_arg(args: &[Expr]) -> Result<String> {
    match args {
        [arg] => column_name(arg)
            .ok_or_else(|| unsupported("window values must be plain columns")),
        _ => Err(unsupported(
            "the window function takes exactly one value column",
        )),
    }
}

/// The value column and the row number of an `NTH_VALUE` call.
fn window_nth_value_args(args: &[Expr]) -> Result<(String, Option<String>)> {
    let [value, n] = args else {
        return Err(unsupported(
            "NTH_VALUE takes a value column and a row number",
        ));
    };
    let value = column_name(value)
        .ok_or_else(|| unsupported("NTH_VALUE values must be plain columns"))?;
    let n = integer_literal(n).ok_or_else(|| {
        unsupported("the NTH_VALUE row number must be an integer literal")
    })?;
    if n < 1 {
        return Err(unsupported("the NTH_VALUE row number must be positive"));
    }
    Ok((value, Some(n.to_string())))
}

/// A single positive integer literal argument, rendered to SQL text.
fn positive_integer_arg(args: &[Expr], what: &str) -> Result<String> {
    let [arg] = args else {
        return Err(unsupported(format!("{what} must be a single literal")));
    };
    let value = integer_literal(arg)
        .ok_or_else(|| unsupported(format!("{what} must be an integer literal")))?;
    if value < 1 {
        return Err(unsupported(format!("{what} must be positive")));
    }
    Ok(value.to_string())
}

/// An integer literal of any width.
fn integer_literal(expr: &Expr) -> Option<i64> {
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

/// The SQL text of the declared window frame, or `None` when it is the
/// default frame of the declared ordering (the runtime then omits it).
fn window_frame_sql(frame: &WindowFrame, has_order: bool) -> Result<Option<String>> {
    let is_default = if has_order {
        frame.units == WindowFrameUnits::Range
            && frame.start_bound.is_unbounded()
            && matches!(frame.end_bound, WindowFrameBound::CurrentRow)
    } else {
        frame.units == WindowFrameUnits::Rows
            && frame.start_bound.is_unbounded()
            && frame.end_bound.is_unbounded()
    };
    if is_default {
        return Ok(None);
    }
    let units = match frame.units {
        WindowFrameUnits::Rows => "rows",
        WindowFrameUnits::Range => "range",
        WindowFrameUnits::Groups => "groups",
    };
    Ok(Some(format!(
        "{units} between {} and {}",
        window_bound_sql(&frame.start_bound)?,
        window_bound_sql(&frame.end_bound)?,
    )))
}

fn window_bound_sql(bound: &WindowFrameBound) -> Result<String> {
    Ok(match bound {
        WindowFrameBound::Preceding(value) if value.is_null() => {
            "unbounded preceding".to_string()
        }
        WindowFrameBound::Preceding(value) => {
            format!("{} preceding", window_literal_sql(value)?)
        }
        WindowFrameBound::CurrentRow => "current row".to_string(),
        WindowFrameBound::Following(value) if value.is_null() => {
            "unbounded following".to_string()
        }
        WindowFrameBound::Following(value) => {
            format!("{} following", window_literal_sql(value)?)
        }
    })
}

fn window_literal_sql(value: &ScalarValue) -> Result<String> {
    let ast = Unparser::default()
        .expr_to_sql(&Expr::Literal(value.clone(), None))
        .map_err(|error| unsupported(format!("window literal {error}")))?;
    Ok(ast.to_string())
}

/// The value column and the optional extra arguments (`offset`, `default`)
/// of a `LAG`/`LEAD` call; the extra arguments are rendered to SQL text.
fn window_shift_args(args: &[Expr]) -> Result<(String, Option<String>)> {
    let Some((value, extras)) = args.split_first() else {
        return Err(unsupported("LAG/LEAD need a value column"));
    };
    let value = column_name(value)
        .ok_or_else(|| unsupported("the LAG/LEAD value must be a plain column"))?;
    if extras.is_empty() {
        return Ok((value, None));
    }
    if extras.len() > 2 {
        return Err(unsupported("LAG/LEAD take at most an offset and a default"));
    }
    let offset = integer_literal(&extras[0])
        .ok_or_else(|| unsupported("the LAG/LEAD offset must be an integer literal"))?;
    if offset < 0 {
        return Err(unsupported("the LAG/LEAD offset must not be negative"));
    }
    if extras.iter().any(|arg| !matches!(arg, Expr::Literal(..))) {
        return Err(unsupported("the LAG/LEAD default must be a literal"));
    }
    let mut rendered = Vec::with_capacity(extras.len());
    for arg in extras {
        let ast = Unparser::default()
            .expr_to_sql(arg)
            .map_err(|error| unsupported(format!("window argument {error}")))?;
        rendered.push(ast.to_string());
    }
    Ok((value, Some(rendered.join(", "))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::test_helpers::*;

    #[tokio::test]
    async fn analyzes_order_by_limit_top_k() {
        // `ORDER BY ... LIMIT k` plans as a `Sort` with a fetch: a global
        // top-k whose identity is the source primary key.
        let analyzed = analyze_optimized("select k, v from src order by v desc limit 3")
            .await
            .unwrap();
        let ViewSpec::TopK {
            group_keys,
            order_keys,
            order_by,
            output_columns,
            limit,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert!(group_keys.is_empty());
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(order_by, vec!["\"v\" desc nulls first".to_string()]);
        assert_eq!(output_columns, vec!["k".to_string(), "v".to_string()]);
        assert_eq!(limit, 3);
        assert!(filter.is_none());

        // A filter and a computed ordering.
        let analyzed = analyze_optimized(
            "select k, v from src where g = 'g0' order by v % 10 desc, k limit 2",
        )
        .await
        .unwrap();
        let ViewSpec::TopK {
            order_keys,
            order_by,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(order_keys, vec!["(v % 10)".to_string(), "k".to_string()]);
        assert_eq!(
            order_by,
            vec!["((v % 10)) desc nulls first".to_string(), "k".to_string()]
        );
        assert!(filter.is_some());

        // A projection above the sort carries the select list.
        let analyzed = analyze_optimized("select k from src order by v desc limit 3")
            .await
            .unwrap();
        let ViewSpec::TopK { output_columns, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(output_columns, vec!["k".to_string()]);

        // The shapes outside the maintained subset.
        for sql in [
            // A limit without an ordering is arbitrary.
            "select k, v from src limit 3",
            // Ordering alone is not materialized.
            "select k, v from src order by v desc",
            // A top-k over an aggregate is a view chain, not one statement.
            "select g, sum(v) as s from src group by g order by s desc limit 2",
        ] {
            assert!(analyze_optimized(sql).await.is_err(), "{sql}");
        }
    }

    #[tokio::test]
    async fn analyzes_computed_select_list_columns() {
        // A computed select item is materialized by its rendered expression,
        // and ordering by its alias orders by that expression.
        let analyzed =
            analyze_optimized("select k, v * 2 as w from src order by w desc limit 3")
                .await
                .unwrap();
        let ViewSpec::TopK {
            output_columns,
            output_exprs,
            order_keys,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(output_columns, vec!["k".to_string(), "w".to_string()]);
        assert_eq!(output_exprs.len(), 2);
        assert!(output_exprs[1].contains('*'), "{output_exprs:?}");
        assert_eq!(order_keys.len(), 1);
        assert!(order_keys[0].contains('*'), "{order_keys:?}");

        let schema = crate::runtime::top_k_expr_mv_schema_for(
            &crate::sql::test_helpers::schema(),
            &["k".to_string()],
            &output_columns,
            &output_exprs,
        )
        .unwrap();
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "k",
                "w",
                crate::runtime::IVM_ROW_KINDS_COLUMN,
                crate::runtime::IVM_EPOCH_COLUMN
            ]
        );
        // The computed column is typed by its expression.
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Int64);
    }

    #[tokio::test]
    async fn analyzes_multi_window_clauses() {
        // Different clauses chain into several WindowAggr nodes; the top node
        // is the last select item.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) as rn, \
             sum(v) over (partition by v) as cnt from src",
        )
        .await
        .unwrap();
        let ViewSpec::MultiWindow { windows, .. } = analyzed.spec else {
            panic!("expected a multi-window spec");
        };
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].partition_keys, vec!["v".to_string()]);
        assert!(windows[0].order_keys.is_empty());
        assert_eq!(windows[0].columns.len(), 1);
        assert_eq!(windows[0].columns[0].column, "cnt");
        assert_eq!(windows[1].partition_keys, vec!["g".to_string()]);
        assert_eq!(windows[1].order_keys, vec!["v".to_string()]);
        assert_eq!(windows[1].columns[0].column, "rn");

        // Different partitions put an intermediate projection between the
        // clauses.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) as rn, \
             sum(v) over (partition by v order by k) as cnt from src",
        )
        .await
        .unwrap();
        let ViewSpec::MultiWindow { windows, .. } = analyzed.spec else {
            panic!("expected a multi-window spec");
        };
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].partition_keys, vec!["v".to_string()]);
        assert_eq!(windows[0].order_keys, vec!["k".to_string()]);
        assert_eq!(windows[1].partition_keys, vec!["g".to_string()]);
        assert_eq!(windows[1].order_keys, vec!["v".to_string()]);

        // One shared clause stays a plain window view.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) as rn, \
             rank() over (partition by g order by v) as rk from src",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::Window { .. }));
    }

    #[tokio::test]
    async fn analyzes_window_order_expressions() {
        // ORDER BY may be a scalar expression: the keys keep the plain
        // expression (for validation) and the ordering adds the direction.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v % 10) as rn from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(order_keys, vec!["(v % 10)".to_string()]);
        assert_eq!(order_by, vec!["((v % 10))".to_string()]);

        // A mixed list of plain columns and expressions.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v % 10 desc, k) as rn from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(order_keys, vec!["(v % 10)".to_string(), "k".to_string()]);
        assert_eq!(
            order_by,
            vec!["((v % 10)) desc nulls first".to_string(), "k".to_string()]
        );

        // The aggregate ordering of STRING_AGG/ARRAY_AGG accepts expressions
        // the same way.
        let analyzed =
            analyze("select g, string_agg(g, ',' order by v % 10) from src group by g")
                .await
                .unwrap();
        let ViewSpec::StringAgg { order_by, .. } = analyzed.spec else {
            panic!("expected a string_agg spec");
        };
        assert_eq!(order_by, vec!["((v % 10))".to_string()]);

        let analyzed =
            analyze("select g, array_agg(g order by v % 10, k) from src group by g")
                .await
                .unwrap();
        let ViewSpec::ArrayAgg { order_by, .. } = analyzed.spec else {
            panic!("expected an array_agg spec");
        };
        assert_eq!(order_by, vec!["((v % 10))".to_string(), "k".to_string()]);

        // The same ordering parser drives TOP-K.
        let analyzed = analyze(
            "select k, v from (select k, g, v, \
             row_number() over (partition by g order by v % 10) as rn from src) t \
             where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(order_keys, vec!["(v % 10)".to_string()]);
        assert_eq!(order_by, vec!["((v % 10))".to_string()]);
    }

    #[tokio::test]
    async fn analyzes_ignore_nulls_windows() {
        // IGNORE NULLS is kept for the value functions.
        for (sql, function) in [
            (
                "select k, lag(v) ignore nulls over (partition by g order by v) from src",
                WindowFunction::Lag,
            ),
            (
                "select k, first_value(v) ignore nulls over (partition by g order by v) from src",
                WindowFunction::FirstValue,
            ),
        ] {
            let analyzed = analyze(sql).await.unwrap();
            let column = only_window_column(&analyzed.spec);
            let got = column.function;
            let ignore_nulls = column.ignore_nulls;

            assert_eq!(got, function);
            assert!(ignore_nulls);
        }

        // The default and the explicit RESPECT NULLS stay false.
        let analyzed = analyze(
            "select k, lag(v) respect nulls over (partition by g order by v) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let ignore_nulls = column.ignore_nulls;

        assert!(!ignore_nulls);

        // For ranking and aggregate windows it is a no-op and normalized away.
        let analyzed =
            analyze("select k, row_number() ignore nulls over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let ignore_nulls = column.ignore_nulls;

        assert!(!ignore_nulls);
        let analyzed =
            analyze("select k, sum(v) ignore nulls over (partition by g) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let ignore_nulls = column.ignore_nulls;

        assert!(!ignore_nulls);
    }

    /// The only window column of an analyzed window view.
    fn only_window_column(spec: &ViewSpec) -> &WindowColumn {
        match spec {
            ViewSpec::Window { columns, .. } if columns.len() == 1 => &columns[0],
            other => panic!("expected a single-column window spec, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn analyzes_ranking_windows() {
        for (sql, function) in [
            (
                "select k, row_number() over (partition by g order by v) from src",
                WindowFunction::RowNumber,
            ),
            (
                "select k, rank() over (partition by g order by v) from src",
                WindowFunction::Rank,
            ),
            (
                "select k, dense_rank() over (partition by g order by v) from src",
                WindowFunction::DenseRank,
            ),
        ] {
            let analyzed = analyze(sql).await.unwrap();
            assert_eq!(
                analyzed.spec,
                ViewSpec::Window {
                    view_id: "view_1".to_string(),
                    source_table_id: "table_src".to_string(),
                    mv_table_id: "table_mv".to_string(),
                    partition_keys: vec!["g".to_string()],
                    order_keys: vec!["v".to_string()],
                    order_by: Vec::new(),
                    columns: vec![WindowColumn::new(function)],
                    payload_columns: Vec::new(),
                    filter: None,
                }
            );
        }
    }

    #[tokio::test]
    async fn analyzes_multiple_window_columns() {
        let analyzed = analyze(
            "select k, sum(v) over w as total, count(*) over w as n from src window w as (partition by g)",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            partition_keys,
            columns,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(partition_keys, vec!["g".to_string()]);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].function, WindowFunction::Sum);
        assert_eq!(columns[0].value_column.as_deref(), Some("v"));
        assert_eq!(columns[0].column, "total");
        assert_eq!(columns[1].function, WindowFunction::Count);
        assert_eq!(columns[1].column, "n");

        // Columns may carry their own frames.
        let analyzed = analyze(
            "select k, sum(v) over (partition by g order by v) as a, sum(k) over (partition by g order by v rows between unbounded preceding and current row) as b from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window { columns, .. } = analyzed.spec else {
            panic!("expected a window spec");
        };
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].window_frame, None);
        assert_eq!(
            columns[1].window_frame.as_deref(),
            Some("rows between unbounded preceding and current row")
        );
    }

    #[tokio::test]
    async fn analyzes_descending_orderings() {
        // A descending ordering is rendered explicitly.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v desc) from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window {
            order_keys,
            order_by,
            ..
        } = analyzed.spec
        else {
            panic!("expected a window spec");
        };
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(order_by, vec!["\"v\" desc nulls first".to_string()]);

        // `DESC NULLS LAST` and `ASC NULLS FIRST` keep their null placement.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v desc nulls last, k asc nulls first) from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window { order_by, .. } = analyzed.spec else {
            panic!("expected a window spec");
        };
        assert_eq!(
            order_by,
            vec![
                "\"v\" desc nulls last".to_string(),
                "\"k\" asc nulls first".to_string(),
            ]
        );

        // The ascending default stays the bare column.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v asc nulls last) from src",
        )
        .await
        .unwrap();
        let ViewSpec::Window { order_by, .. } = analyzed.spec else {
            panic!("expected a window spec");
        };
        assert!(order_by.is_empty());

        // Top-k carries the same rendering.
        let analyzed = analyze(
            "select k, v from (select k, v, row_number() over (partition by g order by v desc) as rn from src) t where rn <= 2",
        )
        .await
        .unwrap();
        let ViewSpec::TopK {
            order_keys,
            order_by,
            limit,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(order_by, vec!["\"v\" desc nulls first".to_string()]);
        assert_eq!(limit, 2);
    }

    #[tokio::test]
    async fn analyzes_lag_lead_windows() {
        let analyzed =
            analyze("select k, lag(v) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(function, WindowFunction::Lag);
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(window_args, None);
        assert_eq!(order_keys, vec!["v".to_string()]);

        // Offset and default render to SQL text; the optimized plan keeps the
        // same spec.
        let analyzed = analyze_optimized(
            "select k, lead(v, 2, 0) over (partition by g order by v) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();

        assert_eq!(function, WindowFunction::Lead);
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(window_args.as_deref(), Some("2, 0"));

        // LAG/LEAD need an ordering.
        assert!(
            analyze("select k, lag(v) over (partition by g) from src")
                .await
                .is_err()
        );
        // The offset must be an integer literal, the value a plain column.
        assert!(
            analyze("select k, lag(v, k) over (partition by g order by v) from src")
                .await
                .is_err()
        );
        assert!(
            analyze("select k, lag(v + 1) over (partition by g order by v) from src")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_frame_value_windows() {
        let analyzed = analyze(
            "select k, first_value(v) over (partition by g order by v, k rows between unbounded preceding and unbounded following) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();
        let window_frame = column.window_frame.clone();

        assert_eq!(function, WindowFunction::FirstValue);
        assert_eq!(value_column, Some("v".to_string()));
        assert_eq!(window_args, None);
        assert_eq!(
            window_frame.as_deref(),
            Some("rows between unbounded preceding and unbounded following")
        );

        // NTH_VALUE takes a positive integer row number.
        let analyzed = analyze(
            "select k, nth_value(v, 2) over (partition by g order by v, k) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let window_args = column.window_args.clone();
        let window_frame = column.window_frame.clone();

        assert_eq!(function, WindowFunction::NthValue);
        assert_eq!(window_args.as_deref(), Some("2"));
        assert_eq!(window_frame, None);

        // LAST_VALUE under the default frame.
        let analyzed = analyze(
            "select k, last_value(v) over (partition by g order by v, k) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;

        assert_eq!(function, WindowFunction::LastValue);

        // Rejections: a missing or non-literal row number, no ordering, and
        // a computed value.
        assert!(
            analyze("select k, nth_value(v) over (partition by g order by v) from src")
                .await
                .is_err()
        );
        assert!(
            analyze(
                "select k, nth_value(v, k) over (partition by g order by v) from src"
            )
            .await
            .is_err()
        );
        assert!(
            analyze("select k, first_value(v) over (partition by g) from src")
                .await
                .is_err()
        );
        assert!(
            analyze(
                "select k, first_value(v + 1) over (partition by g order by v) from src"
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_distribution_windows() {
        let analyzed =
            analyze("select k, ntile(4) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_args = column.window_args.clone();

        assert_eq!(function, WindowFunction::Ntile);
        assert_eq!(value_column, None);
        assert_eq!(window_args.as_deref(), Some("4"));

        let analyzed =
            analyze("select k, percent_rank() over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;

        assert_eq!(function, WindowFunction::PercentRank);

        let analyzed =
            analyze("select k, cume_dist() over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;

        assert_eq!(function, WindowFunction::CumeDist);

        // NTILE needs a positive integer literal bucket count.
        assert!(
            analyze("select k, ntile(v) over (partition by g order by v) from src")
                .await
                .is_err()
        );
        // Ranking functions need an ordering.
        assert!(
            analyze("select k, ntile(4) over (partition by g) from src")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn analyzes_window_filter() {
        let analyzed = analyze(
            "select k, sum(v) filter (where v > 5) over (partition by g) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let window_filter = column.window_filter.clone();

        assert_eq!(function, WindowFunction::Sum);
        assert_eq!(
            normalized(window_filter.as_deref()).as_deref(),
            Some("v > 5")
        );

        let analyzed = analyze(
            "select k, count(*) filter (where g = 'a') over (partition by g) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let value_column = column.value_column.clone();
        let window_filter = column.window_filter.clone();

        assert_eq!(function, WindowFunction::Count);
        assert_eq!(value_column, None);
        assert_eq!(
            normalized(window_filter.as_deref()).as_deref(),
            Some("g = 'a'")
        );
    }

    #[tokio::test]
    async fn analyzes_global_windows() {
        let analyzed = analyze("select k, row_number() over (order by v, k) from src")
            .await
            .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let partition_keys = match &analyzed.spec {
            ViewSpec::Window { partition_keys, .. } => partition_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(function, WindowFunction::RowNumber);
        assert!(partition_keys.is_empty());
        assert_eq!(order_keys, vec!["v".to_string(), "k".to_string()]);

        let analyzed = analyze("select k, sum(v) over () from src").await.unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let partition_keys = match &analyzed.spec {
            ViewSpec::Window { partition_keys, .. } => partition_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let value_column = column.value_column.clone();

        assert_eq!(function, WindowFunction::Sum);
        assert_eq!(value_column, Some("v".to_string()));
        assert!(partition_keys.is_empty());
        assert!(order_keys.is_empty());
    }

    #[tokio::test]
    async fn analyzes_aggregate_windows() {
        let analyzed = analyze("select k, sum(v) over (partition by g) from src")
            .await
            .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let value_column = column.value_column.clone();

        assert_eq!(function, WindowFunction::Sum);
        assert_eq!(value_column, Some("v".to_string()));
        assert!(order_keys.is_empty());

        let analyzed =
            analyze("select k, count(*) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let function = column.function;
        let order_keys = match &analyzed.spec {
            ViewSpec::Window { order_keys, .. } => order_keys.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };
        let value_column = column.value_column.clone();

        assert_eq!(function, WindowFunction::Count);
        assert_eq!(value_column, None);
        assert_eq!(order_keys, vec!["v".to_string()]);
    }

    #[tokio::test]
    async fn analyzes_order_by_limit_with_offset() {
        // OFFSET keeps a Limit (skip, fetch) above the Sort's fetch; both the
        // optimized and the raw shape are accepted.
        let analyzed =
            analyze_optimized("select k, v from src order by v desc limit 3 offset 2")
                .await
                .unwrap();
        let ViewSpec::TopK {
            order_keys,
            limit,
            offset,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(limit, 3);
        assert_eq!(offset, 2);

        // The select list may be pruned into the sort.
        let analyzed = analyze("select * from src order by v limit 3 offset 2")
            .await
            .unwrap();
        let ViewSpec::TopK { limit, offset, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(limit, 3);
        assert_eq!(offset, 2);

        let analyzed = analyze_optimized("select * from src order by v limit 3 offset 2")
            .await
            .unwrap();
        let ViewSpec::TopK { limit, offset, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(limit, 3);
        assert_eq!(offset, 2);

        // A global ORDER BY ... LIMIT without OFFSET is a top-k too.
        let analyzed = analyze("select k, v from src order by v limit 3")
            .await
            .unwrap();
        let ViewSpec::TopK {
            group_keys,
            limit,
            offset,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert!(group_keys.is_empty());
        assert_eq!(limit, 3);
        assert_eq!(offset, 0);

        // A select list that omits the source primary key is materialized
        // with it appended (the view identifies a top-k row by it).
        let analyzed = analyze_optimized("select v from src order by v desc limit 3")
            .await
            .unwrap();
        let ViewSpec::TopK { output_columns, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(output_columns, vec!["v".to_string()]);
        let schema = crate::runtime::top_k_mv_schema_for(
            &crate::sql::test_helpers::schema(),
            &["k".to_string()],
            &output_columns,
        )
        .unwrap();
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "v",
                "k",
                crate::runtime::IVM_ROW_KINDS_COLUMN,
                crate::runtime::IVM_EPOCH_COLUMN
            ]
        );
    }

    #[tokio::test]
    async fn analyzes_top_k() {
        let analyzed = analyze(
            "select k, v from (select k, g, v, row_number() over (partition by g order by v) as rn from src) t where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK {
            group_keys,
            order_keys,
            output_columns,
            limit,
            filter,
            ..
        } = analyzed.spec
        else {
            panic!("expected a top-k spec");
        };
        assert_eq!(group_keys, vec!["g".to_string()]);
        assert_eq!(order_keys, vec!["v".to_string()]);
        assert_eq!(output_columns, vec!["k".to_string(), "v".to_string()]);
        assert_eq!(limit, 3);
        assert_eq!(filter, None);
    }

    #[tokio::test]
    async fn analyzes_top_k_where() {
        let analyzed = analyze(
            "select k, v from (select k, g, v, row_number() over (partition by g order by v) as rn from src where v > 5) t where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK { limit, filter, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(limit, 3);
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn analyzes_window_where() {
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v) from src where v > 5",
        )
        .await
        .unwrap();
        let filter = match &analyzed.spec {
            ViewSpec::Window { filter, .. } => filter.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn analyzes_window_and_top_k_where_on_the_optimized_plan() {
        // The optimizer pushes the filter below the window.
        let analyzed = analyze_optimized(
            "select k, sum(v) over (partition by g) from src where v > 5 and g <> 'x'",
        )
        .await
        .unwrap();
        let filter = match &analyzed.spec {
            ViewSpec::Window { filter, .. } => filter.clone(),
            other => panic!("expected a window spec, got {other:?}"),
        };

        assert_eq!(
            normalized(filter.as_deref()).as_deref(),
            Some("v > 5 AND g <> 'x'")
        );

        let analyzed = analyze_optimized(
            "select k, v from (select k, g, v, row_number() over (partition by g order by v) as rn from src where v > 5) t where rn <= 3",
        )
        .await
        .unwrap();
        let ViewSpec::TopK { filter, .. } = analyzed.spec else {
            panic!("expected a top-k spec");
        };
        assert_eq!(normalized(filter.as_deref()).as_deref(), Some("v > 5"));
    }

    #[tokio::test]
    async fn rejects_unsupported_window_shapes() {
        // A frame is only stored when it is not the default.
        let analyzed =
            analyze("select k, sum(v) over (partition by g order by v) from src")
                .await
                .unwrap();
        let column = only_window_column(&analyzed.spec);
        let window_frame = column.window_frame.clone();

        assert_eq!(window_frame, None);
        let analyzed = analyze(
            "select k, sum(v) over (partition by g order by v rows between 1 preceding and current row) from src",
        )
        .await
        .unwrap();
        let column = only_window_column(&analyzed.spec);
        let window_frame = column.window_frame.clone();

        assert_eq!(
            window_frame.as_deref(),
            Some("rows between 1 preceding and current row")
        );
        // Ranking needs an ordering.
        assert!(
            analyze("select k, row_number() over (partition by g) from src")
                .await
                .is_err()
        );
        // Multiple windows with different partitionings chain into a
        // multi-window view.
        let analyzed = analyze(
            "select k, row_number() over (partition by g order by v), rank() over (partition by k order by v) from src",
        )
        .await
        .unwrap();
        assert!(matches!(analyzed.spec, ViewSpec::MultiWindow { .. }));
        // Repeated functions need distinct column names (an alias).
        assert!(
            analyze(
                "select k, sum(v) over (partition by g order by v), sum(k) over (partition by g order by v rows between unbounded preceding and current row) from src"
            )
            .await
            .is_err()
        );
        // Computing on top of the window column is not maintained.
        assert!(
            analyze(
                "select k, row_number() over (partition by g order by v) + 1 from src"
            )
            .await
            .is_err()
        );
        // Filtering a rank column is not top-k.
        assert!(
            analyze(
                "select k from (select k, rank() over (partition by g order by v) as rn from src) t where rn <= 3"
            )
            .await
            .is_err()
        );
    }
}
