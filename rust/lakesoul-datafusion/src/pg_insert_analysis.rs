// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL's `INSERT` source analysis, shared by every SQL front end.
//!
//! Both the plain `INSERT` / `ON CONFLICT DO NOTHING` path and the
//! `ON CONFLICT DO UPDATE` planner need the same rules:
//!
//! - resolve the insert column list (duplicates are `42701`, unknown columns
//!   are `42703`);
//! - compare the source width with the target columns (`42601`), where a
//!   no-list source may be narrower and is completed with NULL;
//! - check every untyped literal against its target column (`22P02` for an
//!   invalid string, `42804` for a type mismatch) and every typed `VALUES`
//!   or `SELECT` expression with PostgreSQL's assignment matrix.
//!
//! Keeping this in one place means the replacement `DO UPDATE` path cannot
//! drift from the ordinary `INSERT` contract.
//!
//! The analysis only inspects the source shape; it does not rewrite the
//! statement. [`pad_insert_source`] applies the no-list NULL completion when a
//! consumer (the DataFusion `INSERT` planner) needs an explicitly padded
//! source.

use std::collections::BTreeSet;
use std::fmt;

use arrow::datatypes::{Field, Schema};
use datafusion::common::DFSchemaRef;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, Ident, Insert, ObjectName, Query, SelectItem, SetExpr, Statement, Value, Values,
};

use crate::pg_assignment::{self, AssignmentError};

/// SQLSTATE `undefined_column`.
pub const UNDEFINED_COLUMN: &str = "42703";
/// SQLSTATE `duplicate_column`.
pub const DUPLICATE_COLUMN: &str = "42701";
/// SQLSTATE `syntax_error` (source/target column count mismatch).
pub const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE `feature_not_supported`.
pub const FEATURE_NOT_SUPPORTED: &str = "0A000";

/// A rejected `INSERT` source, carrying the PostgreSQL SQLSTATE and message.
#[derive(Debug, Clone)]
pub struct InsertAnalysisError {
    pub sqlstate: &'static str,
    pub message: String,
}

impl fmt::Display for InsertAnalysisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InsertAnalysisError {}

impl From<AssignmentError> for InsertAnalysisError {
    fn from(error: AssignmentError) -> Self {
        Self {
            sqlstate: error.sqlstate,
            message: error.message,
        }
    }
}

fn syntax(message: &str) -> InsertAnalysisError {
    InsertAnalysisError {
        sqlstate: SYNTAX_ERROR,
        message: message.to_string(),
    }
}

fn unsupported(message: String) -> InsertAnalysisError {
    InsertAnalysisError {
        sqlstate: FEATURE_NOT_SUPPORTED,
        message,
    }
}

/// The resolved shape of one `INSERT` source.
#[derive(Debug, Clone)]
pub struct SourceAnalysis {
    /// Schema index of the target column for each source position.
    pub targets: Vec<usize>,
    /// Number of source expressions, or `None` when a `SELECT` source cannot
    /// be planned without the target context yet (for example an unresolved
    /// parameter inside a function call). An unresolvable `SELECT` only
    /// disables the static width/type checks, exactly like the previous
    /// front-end analysis.
    pub source_width: Option<usize>,
    /// Whether the statement spelled an explicit column list.
    pub explicit: bool,
    /// Number of target columns the source is assigned to.
    pub expected: usize,
    /// Number of columns in the target table.
    pub schema_len: usize,
}

impl SourceAnalysis {
    /// The source position that feeds each target column, in schema order.
    ///
    /// `None` means the statement does not supply that column; PostgreSQL's
    /// `INSERT` rule completes it with NULL (LakeSoul has no `DEFAULT`).
    pub fn column_sources(&self) -> Vec<Option<usize>> {
        let mut sources = vec![None; self.schema_len];
        let width = self.source_width.unwrap_or(0);
        for (position, &index) in self.targets.iter().enumerate().take(width) {
            sources[index] = Some(position);
        }
        sources
    }
}

/// Analyse the source of `insert` against the target table `schema`.
pub async fn analyse(
    insert: &Insert,
    schema: &Schema,
    session: &SessionContext,
) -> Result<SourceAnalysis, InsertAnalysisError> {
    let source = insert.source.as_ref().ok_or_else(|| {
        unsupported("INSERT without a source is not supported".to_string())
    })?;
    let explicit = !insert.columns.is_empty();
    let targets = resolve_targets(insert, schema)?;
    let expected = targets.len();

    let widths = values(source).map(|values| {
        values
            .rows
            .iter()
            .map(|row| row.content.len())
            .collect::<Vec<_>>()
    });

    let (source_width, planned) = match widths {
        Some(widths) => {
            let width = widths.first().copied().unwrap_or(0);
            if let Some(mismatch) = widths.iter().find(|&&other| other != width) {
                return Err(syntax(&format!(
                    "VALUES lists must all be the same length ({width} and {mismatch})"
                )));
            }
            check_width(width, expected, explicit)?;
            // A `VALUES` source only needs its own plan when one expression is
            // typed: untyped literals are resolved against the target column,
            // while the source query's own plan would infer a different type
            // (or fail to cast the literal).
            let planned =
                if values_have_typed(values(source).expect("VALUES stays VALUES")) {
                    plan_source(source, session).await
                } else {
                    None
                };
            (Some(width), planned)
        }
        None => {
            let planned = plan_source(source, session).await;
            if let Some(schema) = &planned {
                check_width(schema.fields().len(), expected, explicit)?;
            }
            (
                planned.as_ref().map(|schema| schema.fields().len()),
                planned,
            )
        }
    };

    if let Some(values) = values(source) {
        for (position, &index) in targets.iter().enumerate() {
            let target = schema.field(index);
            let column = values
                .rows
                .iter()
                .filter_map(|row| row.content.get(position))
                .collect::<Vec<_>>();
            validate_values_column(position, &column, target, planned.as_ref())?;
        }
    } else if let Some(planned) = &planned {
        let width = source_width.unwrap_or(0);
        for (position, &index) in targets.iter().enumerate() {
            if position >= width {
                // The column is completed with NULL; nothing to check.
                continue;
            }
            validate_select_column(
                source.body.as_ref(),
                position,
                width,
                schema.field(index),
                planned,
            )?;
        }
    }

    Ok(SourceAnalysis {
        targets,
        source_width,
        explicit,
        expected,
        schema_len: schema.fields().len(),
    })
}

/// Complete a no-list source to the table's width the way PostgreSQL fills the
/// unlisted columns with their default (NULL, since LakeSoul has no
/// `DEFAULT`): `VALUES` rows get trailing NULL expressions and `SELECT`
/// projections get trailing NULL columns.
pub fn pad_insert_source(statement: &mut Insert, analysis: &SourceAnalysis) {
    if analysis.explicit {
        return;
    }
    let Some(width) = analysis.source_width else {
        return;
    };
    if width >= analysis.expected {
        return;
    }
    let Some(source) = statement.source.as_mut() else {
        return;
    };
    pad_body(
        source.body.as_mut(),
        analysis.expected,
        analysis.expected - width,
    );
}

fn pad_body(body: &mut SetExpr, width: usize, added: usize) {
    match body {
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                while row.content.len() < width {
                    row.content.push(Expr::Value(Value::Null.into()));
                }
            }
        }
        SetExpr::Select(select) => {
            for _ in 0..added {
                select
                    .projection
                    .push(SelectItem::UnnamedExpr(Expr::Value(Value::Null.into())));
            }
        }
        SetExpr::Query(query) => pad_body(query.body.as_mut(), width, added),
        SetExpr::SetOperation { left, right, .. } => {
            pad_body(left, width, added);
            pad_body(right, width, added);
        }
        _ => {}
    }
}

/// The schema index of the target column for each source position: the
/// explicit column list, otherwise the table's schema order.
fn resolve_targets(
    insert: &Insert,
    schema: &Schema,
) -> Result<Vec<usize>, InsertAnalysisError> {
    if insert.columns.is_empty() {
        return Ok((0..schema.fields().len()).collect());
    }
    let mut targets = Vec::with_capacity(insert.columns.len());
    let mut seen = BTreeSet::new();
    for name in &insert.columns {
        let column = single_column(name)?;
        if !seen.insert(column.clone()) {
            return Err(InsertAnalysisError {
                sqlstate: DUPLICATE_COLUMN,
                message: format!("column \"{column}\" specified more than once"),
            });
        }
        let index = schema
            .fields()
            .iter()
            .position(|field| field.name() == &column)
            .ok_or_else(|| InsertAnalysisError {
                sqlstate: UNDEFINED_COLUMN,
                message: format!("column \"{column}\" of relation does not exist"),
            })?;
        targets.push(index);
    }
    Ok(targets)
}

fn single_column(name: &ObjectName) -> Result<String, InsertAnalysisError> {
    let [part] = name.0.as_slice() else {
        return Err(unsupported(format!(
            "multi-part column name {name} in INSERT"
        )));
    };
    normalize_column(
        part.as_ident()
            .ok_or_else(|| unsupported(format!("object name part {part} in INSERT")))?,
    )
}

/// PostgreSQL folds an unquoted identifier to lower case. LakeSoul stores
/// lower-case names; a quoted mixed-case column is outside its naming
/// contract (the CDC `rowKinds` exception is preserved).
pub fn normalize_column(ident: &Ident) -> Result<String, InsertAnalysisError> {
    let folded = ident.value.to_ascii_lowercase();
    if ident.quote_style.is_some() && ident.value != folded {
        if ident.value == "rowKinds" {
            return Ok(ident.value.clone());
        }
        return Err(unsupported(format!(
            "quoted identifier \"{}\" is not supported: LakeSoul stores table \
             and column names in lower case, so it would be renamed to \
             \"{folded}\"; declare it unquoted or in lower case",
            ident.value
        )));
    }
    Ok(folded)
}

/// `more expressions` when the source is wider than the target, `more target
/// columns` when it is narrower and padding is not allowed.
fn check_width(
    width: usize,
    expected: usize,
    explicit: bool,
) -> Result<(), InsertAnalysisError> {
    if width > expected {
        return Err(syntax("INSERT has more expressions than target columns"));
    }
    if width < expected && explicit {
        return Err(syntax("INSERT has more target columns than expressions"));
    }
    Ok(())
}

fn values_have_typed(values: &Values) -> bool {
    values
        .rows
        .iter()
        .flat_map(|row| row.content.iter())
        .any(|expr| pg_assignment::literal(expr).is_none())
}

fn validate_values_column(
    index: usize,
    column: &[&Expr],
    target: &Field,
    source_schema: Option<&DFSchemaRef>,
) -> Result<(), InsertAnalysisError> {
    let mut literals = Vec::with_capacity(column.len());
    let mut typed = false;
    for expr in column {
        match pg_assignment::literal(expr) {
            Some(value) => literals.push(value),
            None => typed = true,
        }
    }
    if typed {
        // A typed expression resolves the column's type; the literals in the
        // other rows are coerced to it, so the planned type decides.
        if let Some(source) = source_schema
            .and_then(|schema| schema.fields().get(index))
            .map(|field| field.data_type())
        {
            return pg_assignment::ensure_assignment(source, target).map_err(Into::into);
        }
        return Ok(());
    }
    for value in literals {
        pg_assignment::check_literal(value, target).map_err(InsertAnalysisError::from)?;
    }
    Ok(())
}

/// Check one exported column of an `INSERT ... SELECT` source. A projection
/// item that is an untyped literal is resolved against the target column,
/// exactly as in a `VALUES` row; every other expression is checked by the
/// type the source query plans to. Nested queries and set operations are
/// walked like the padding above, so every branch's expression at the
/// column's position is checked.
fn validate_select_column(
    body: &SetExpr,
    index: usize,
    width: usize,
    target: &Field,
    planned: &DFSchemaRef,
) -> Result<(), InsertAnalysisError> {
    match body {
        SetExpr::Select(select) => {
            if let Some(expr) = projected_expression(&select.projection, index, width)
                && let Some(value) = pg_assignment::literal(expr)
            {
                return pg_assignment::check_literal(value, target).map_err(Into::into);
            }
            ensure_planned_type(index, target, planned)
        }
        SetExpr::Query(query) => {
            validate_select_column(query.body.as_ref(), index, width, target, planned)
        }
        SetExpr::SetOperation { left, right, .. } => {
            validate_select_column(left, index, width, target, planned)?;
            validate_select_column(right, index, width, target, planned)
        }
        SetExpr::Values(values) => {
            let column = values
                .rows
                .iter()
                .filter_map(|row| row.content.get(index))
                .collect::<Vec<_>>();
            validate_values_column(index, &column, target, Some(planned))
        }
        _ => ensure_planned_type(index, target, planned),
    }
}

/// The expression a `SELECT` projects at `index`, when its projection list is
/// fully explicit. A wildcard expands to an unknown number of columns, so the
/// AST position of the items after it cannot be mapped; those projections are
/// left to the planned type.
fn projected_expression(
    projection: &[SelectItem],
    index: usize,
    width: usize,
) -> Option<&Expr> {
    let explicit = projection.iter().all(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
        )
    });
    if !explicit || projection.len() < width {
        return None;
    }
    match projection.get(index)? {
        SelectItem::UnnamedExpr(expr) => Some(expr),
        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }
}

fn ensure_planned_type(
    index: usize,
    target: &Field,
    planned: &DFSchemaRef,
) -> Result<(), InsertAnalysisError> {
    match planned.fields().get(index) {
        Some(source) => pg_assignment::ensure_assignment(source.data_type(), target)
            .map_err(Into::into),
        None => Ok(()),
    }
}

/// The source query's own logical schema, or `None` when it cannot be planned
/// on its own (for example while a parameter type is still unknown). The
/// statement's plan reports that error itself, so a missing schema only
/// disables the static check.
async fn plan_source(query: &Query, session: &SessionContext) -> Option<DFSchemaRef> {
    session
        .state()
        .statement_to_plan(DfStatement::Statement(Box::new(Statement::Query(
            Box::new(query.clone()),
        ))))
        .await
        .ok()
        .map(|plan| plan.schema().clone())
}

fn values(query: &Query) -> Option<&Values> {
    match query.body.as_ref() {
        SetExpr::Values(values) => Some(values),
        _ => None,
    }
}
