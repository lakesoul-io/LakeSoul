// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `INSERT` for the SQL front end.
//!
//! The statement is claimed at Parse (so the plan is built from a rewritten
//! AST) and at Execute (so the parameters are substituted, the rows are
//! executed and the outcome is mapped to PostgreSQL's SQLSTATEs). The write
//! itself is the native LakeSoul sink: a plain `INSERT` into a primary-key
//! table is insert-only and rejects duplicate keys, while `ON CONFLICT DO
//! NOTHING` drops the conflicting rows and `ON CONFLICT ... DO UPDATE SET
//! col = EXCLUDED.col` is planned as a LakeSoul replace.
//!
//! Two clauses are refused because LakeSoul cannot express them: `RETURNING`
//! (the sink commits after the statement's rows are written, and there is no
//! row stream to return), and `ON CONFLICT DO UPDATE` with assignments that
//! are not `column = EXCLUDED.column` (LakeSoul replace rewrites a whole row;
//! an expression over the old row would need read-modify-write).
//!
//! Writes are refused inside a transaction block and inside a multi-statement
//! simple-query message, for the same reason DDL is: LakeSoul commits
//! immediately, so a later `ROLLBACK` or a failing statement in the batch
//! cannot undo the rows.
//!
//! Two outcomes need care. A statement that loses a race with a concurrent
//! writer is reported as `40001` (serialization failure, retryable), because
//! LakeSoul detects the lost race at snapshot validation and cannot tell
//! whether the winner inserted the same key; a retry reports `23505` once the
//! conflict is a plain duplicate key. Index auto-maintenance runs after the
//! metadata commit, so a failed vector/text index build still returns an
//! error even though the rows are committed and visible.
//!
//! Before planning, the statement is analysed the way PostgreSQL analyses it:
//! the source must line up with the target columns (`42601`), a no-list
//! source shorter than the table is completed with NULLs for the unlisted
//! columns, an untyped string literal is converted to the target type
//! (`22P02` when it does not convert), and every other expression must be
//! assignment-castable (`42804`). `SELECT` expression casts keep their own
//! documented boundary.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::common::{DFSchemaRef, ParamValues, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::dml::{InsertOp, WriteOp};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, ConflictTarget, Expr, Insert, ObjectName, ObjectNamePart,
    OnConflictAction, OnInsert, Query, SelectItem, SetExpr, SqliteOnConflict, Statement,
    TableObject, Value, Values,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::response::TransactionStatus;

use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::write_mode::{
    ConflictMode, LakeSoulWriteMode, WriteConflict, WriteViolation,
};

use crate::catalog::PUBLIC_SCHEMA;
use crate::ddl::{feature_not_supported, normalize_ident};
use crate::server::ConnectionSession;

/// SQLSTATE `undefined_table`.
const UNDEFINED_TABLE: &str = "42P01";
/// SQLSTATE `invalid_schema_name`.
const INVALID_SCHEMA_NAME: &str = "3F000";
/// SQLSTATE `invalid_catalog_name`.
const INVALID_CATALOG_NAME: &str = "3D000";
/// SQLSTATE `invalid_column_reference` (no unique constraint for the target).
const NO_UNIQUE_CONSTRAINT: &str = "42P10";
/// SQLSTATE `syntax_error` (the source does not match the target column count).
const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE `datatype_mismatch` (the source expression is not assignable).
const DATATYPE_MISMATCH: &str = "42804";
/// SQLSTATE `invalid_text_representation` (a string literal does not convert).
const INVALID_TEXT_REPRESENTATION: &str = "22P02";
/// SQLSTATE `cardinality_violation` (an upsert source repeats a key).
const CARDINALITY_VIOLATION: &str = "21000";

pub(crate) struct InsertStatementHook;

#[async_trait]
impl QueryHook for InsertStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let insert = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                run(insert, session_context, status, &database).await
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let insert = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                let target = prepare(insert, session_context, status, &database).await?;
                plan_target(&target, session_context).await
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let insert = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                let target = prepare(insert, session_context, status, &database).await?;
                // The mode is read while the write plan is built, which happens in
                // `execute_logical_plan` below.
                write_mode(session_context)?.set(target.mode);
                execute_bound(logical_plan, params, session_context).await
            }
            .await,
        )
    }
}

pub(crate) fn extract(statement: &Statement) -> Option<&Insert> {
    match statement {
        Statement::Insert(insert) => Some(insert),
        _ => None,
    }
}

/// Everything the statement's execution depends on, resolved once.
struct Target {
    /// What the sink does with a key that is already present.
    mode: ConflictMode,
    /// The statement to plan: `ON CONFLICT` is dropped (DataFusion refuses
    /// it), `DO UPDATE` selects the replace write operation, and a no-list
    /// `VALUES` source shorter than the table is padded with NULLs, which is
    /// PostgreSQL's default semantics for the unlisted columns.
    statement: Insert,
}

/// Connection state a hook may hold across an `await`.
pub(crate) fn request_context(
    client: &(impl ClientInfo + ?Sized),
) -> PgWireResult<(TransactionStatus, String)> {
    let session = client
        .session_extensions()
        .get::<ConnectionSession>()
        .ok_or_else(|| internal("the connection has no session"))?;
    Ok((
        client.transaction_status(),
        session.session.identity.database.clone(),
    ))
}

async fn prepare(
    insert: &Insert,
    session_context: &SessionContext,
    status: TransactionStatus,
    database: &str,
) -> PgWireResult<Target> {
    reject_transaction(status)?;
    reject_unsupported(insert)?;
    let (table_ref, table_name) = target_reference(insert, database)?;
    let provider = session_context
        .table_provider(table_ref.clone())
        .await
        .map_err(|error| {
            user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table_name}\" does not exist: {error}"),
            )
        })?;
    let provider: &dyn std::any::Any = provider.as_ref();
    let provider = provider
        .downcast_ref::<LakeSoulTableProvider>()
        .ok_or_else(|| {
            feature_not_supported(format!(
                "relation \"{table_name}\" is not a LakeSoul table"
            ))
        })?;

    let table_info = provider.table_info();
    let (range, primary_keys) = split_partitions(&table_info.partitions);
    let fields = provider
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_lowercase())
        .collect::<BTreeSet<_>>();

    let (replace, mode) = conflict_plan(insert, &fields, &primary_keys, &range)?;
    let statement =
        checked_statement(insert, &provider.schema(), session_context, replace).await?;
    Ok(Target { mode, statement })
}

/// PostgreSQL analyses an `INSERT` before planning it: the source must have
/// one expression per target column, a no-list source shorter than the table
/// is completed with NULLs for the unlisted columns, and every exported
/// column is checked against the assignment cast matrix. This performs that
/// analysis on the AST so the errors carry PostgreSQL's SQLSTATEs instead of
/// the planner's internal `XX000`, and returns the statement to plan (with a
/// possibly padded source).
async fn checked_statement(
    insert: &Insert,
    schema: &SchemaRef,
    session_context: &SessionContext,
    replace: bool,
) -> PgWireResult<Insert> {
    let mut statement = insert.clone();
    statement.on = None;
    if replace {
        statement.or = Some(SqliteOnConflict::Replace);
    }
    let targets = exported_columns(insert, schema)?;
    validate_source(&mut statement, &targets, session_context).await?;
    Ok(statement)
}

/// The table columns the statement's source is assigned to, in source order:
/// an explicit column list, otherwise the table's schema order. A column the
/// planner cannot resolve stays `None` (the planner reports its own error).
fn exported_columns<'a>(
    insert: &Insert,
    schema: &'a SchemaRef,
) -> PgWireResult<Vec<Option<&'a Field>>> {
    if insert.columns.is_empty() {
        return Ok(schema
            .fields()
            .iter()
            .map(|field| Some(field.as_ref()))
            .collect());
    }
    let mut columns = Vec::with_capacity(insert.columns.len());
    for name in &insert.columns {
        let column = single_column(name)?;
        columns.push(
            schema
                .fields()
                .iter()
                .find(|field| field.name().to_lowercase() == column)
                .map(|field| field.as_ref()),
        );
    }
    Ok(columns)
}

/// Column-count and assignment rules of PostgreSQL's `INSERT` analysis.
async fn validate_source(
    statement: &mut Insert,
    targets: &[Option<&Field>],
    session_context: &SessionContext,
) -> PgWireResult<()> {
    let explicit = !statement.columns.is_empty();
    let expected = targets.len();
    let widths: Option<Vec<usize>> = statement
        .source
        .as_ref()
        .and_then(|source| values(source))
        .map(|values| values.rows.iter().map(|row| row.content.len()).collect());

    // A `VALUES` source only needs a plan when one of its expressions is
    // typed: untyped literals are resolved here instead, because PostgreSQL
    // coerces them against the target column while the source query's own
    // plan would infer a different type (or fail to cast the literal).
    let source_schema = match &widths {
        Some(_) => {
            let typed = statement
                .source
                .as_ref()
                .and_then(|source| values(source))
                .is_some_and(values_have_typed);
            match statement.source.as_ref() {
                Some(source) if typed => plan_source(source, session_context).await,
                _ => None,
            }
        }
        None => match statement.source.as_ref() {
            Some(source) => plan_source(source, session_context).await,
            None => None,
        },
    };

    match &widths {
        Some(widths) => {
            for &width in widths {
                check_width(width, expected, !explicit)?;
            }
        }
        None => {
            if let Some(schema) = &source_schema {
                check_width(schema.fields().len(), expected, !explicit)?;
            }
        }
    }

    if !explicit {
        match &widths {
            Some(_) => pad_source(statement, expected, 0),
            None => {
                if let Some(schema) = &source_schema {
                    let width = schema.fields().len();
                    if width < expected {
                        pad_source(statement, expected, expected - width);
                    }
                }
            }
        }
    }

    match &widths {
        Some(_) => {
            let values = statement
                .source
                .as_ref()
                .and_then(|source| values(source))
                .expect("a VALUES source stays a VALUES source");
            for (index, target) in targets.iter().enumerate() {
                let Some(target) = target else { continue };
                let column = values
                    .rows
                    .iter()
                    .map(|row| &row.content[index])
                    .collect::<Vec<_>>();
                validate_values_column(index, &column, target, source_schema.as_ref())?;
            }
        }
        None => {
            let Some(schema) = &source_schema else {
                return Ok(());
            };
            let width = schema.fields().len();
            let source = statement
                .source
                .as_ref()
                .expect("reject_unsupported guarantees a source");
            for (index, target) in targets.iter().enumerate() {
                if index >= width {
                    // The column was added by the NULL completion above.
                    continue;
                }
                let Some(target) = target else { continue };
                validate_select_column(source, index, width, target, schema)?;
            }
        }
    }
    Ok(())
}

/// `more expressions` when the source is wider than the target, `more target
/// columns` when it is narrower and padding is not allowed.
fn check_width(width: usize, expected: usize, pad: bool) -> PgWireResult<()> {
    if width > expected {
        return Err(user_error(
            SYNTAX_ERROR,
            "INSERT has more expressions than target columns".to_string(),
        ));
    }
    if width < expected && !pad {
        return Err(user_error(
            SYNTAX_ERROR,
            "INSERT has more target columns than expressions".to_string(),
        ));
    }
    Ok(())
}

/// Complete a no-list source to the table's width the way PostgreSQL fills
/// the unlisted columns with their default (NULL, since LakeSoul has no
/// `DEFAULT`): `VALUES` rows get trailing NULL expressions and `SELECT`
/// projections get trailing NULL columns.
fn pad_source(statement: &mut Insert, width: usize, added: usize) {
    let Some(source) = statement.source.as_mut() else {
        return;
    };
    pad_body(source.body.as_mut(), width, added);
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

/// The source query's own logical schema, or `None` when it cannot be planned
/// on its own (for example while a parameter type is still unknown). The
/// statement's plan reports that error itself, so a missing schema only
/// disables the static check.
async fn plan_source(
    query: &Query,
    session_context: &SessionContext,
) -> Option<DFSchemaRef> {
    session_context
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

fn values_have_typed(values: &Values) -> bool {
    values
        .rows
        .iter()
        .flat_map(|row| row.content.iter())
        .any(|expr| literal(expr).is_none())
}

/// A literal PostgreSQL treats as `unknown` (or a parameter whose type is
/// supplied by the target column), as opposed to a typed expression.
enum Literal<'a> {
    Null,
    String(&'a str),
    Number(&'a str),
    Boolean,
    Placeholder,
}

fn literal(expr: &Expr) -> Option<Literal<'_>> {
    match expr {
        Expr::Nested(inner) => literal(inner),
        Expr::Value(value) => match &value.value {
            Value::Null => Some(Literal::Null),
            Value::SingleQuotedString(text) => Some(Literal::String(text)),
            Value::Number(text, _) => Some(Literal::Number(text)),
            Value::Boolean(_) => Some(Literal::Boolean),
            Value::Placeholder(_) => Some(Literal::Placeholder),
            _ => None,
        },
        _ => None,
    }
}

fn validate_values_column(
    index: usize,
    column: &[&Expr],
    target: &Field,
    source_schema: Option<&DFSchemaRef>,
) -> PgWireResult<()> {
    let mut literals = Vec::with_capacity(column.len());
    let mut typed = false;
    for expr in column {
        match literal(expr) {
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
            return ensure_assignment(source, target);
        }
        return Ok(());
    }
    for value in literals {
        check_literal(value, target)?;
    }
    Ok(())
}

fn check_literal(value: Literal<'_>, target: &Field) -> PgWireResult<()> {
    let target_type = target.data_type();
    match value {
        Literal::Null | Literal::Placeholder => Ok(()),
        Literal::String(text) => {
            if is_string(target_type) {
                return Ok(());
            }
            if is_numeric(target_type)
                || is_temporal(target_type)
                || *target_type == DataType::Boolean
                || is_binary(target_type)
            {
                ScalarValue::Utf8(Some(text.to_string()))
                    .cast_to(target_type)
                    .map_err(|_| {
                        user_error(
                            INVALID_TEXT_REPRESENTATION,
                            format!(
                                "invalid input syntax for type {}: \"{text}\"",
                                pg_type_name(target_type)
                            ),
                        )
                    })?;
            }
            Ok(())
        }
        Literal::Number(text) => {
            if is_numeric(target_type) || is_string(target_type) {
                Ok(())
            } else {
                Err(datatype_mismatch(target, number_type(text)))
            }
        }
        Literal::Boolean => {
            if *target_type == DataType::Boolean || is_string(target_type) {
                Ok(())
            } else {
                Err(datatype_mismatch(target, "boolean"))
            }
        }
    }
}

/// Check one exported column of an `INSERT ... SELECT` source. A projection
/// item that is an untyped literal is resolved against the target column,
/// exactly as in a `VALUES` row; every other expression is checked by the
/// type the source query plans to. Nested queries and set operations are
/// walked like the padding above, so every branch's expression at the
/// column's position is checked.
fn validate_select_column(
    query: &Query,
    index: usize,
    width: usize,
    target: &Field,
    planned: &DFSchemaRef,
) -> PgWireResult<()> {
    validate_body_column(query.body.as_ref(), index, width, target, planned)
}

fn validate_body_column(
    body: &SetExpr,
    index: usize,
    width: usize,
    target: &Field,
    planned: &DFSchemaRef,
) -> PgWireResult<()> {
    match body {
        SetExpr::Select(select) => {
            if let Some(expr) = projected_expression(&select.projection, index, width)
                && let Some(value) = literal(expr)
            {
                return check_literal(value, target);
            }
            ensure_planned_type(index, target, planned)
        }
        SetExpr::Query(query) => {
            validate_body_column(query.body.as_ref(), index, width, target, planned)
        }
        SetExpr::SetOperation { left, right, .. } => {
            validate_body_column(left, index, width, target, planned)?;
            validate_body_column(right, index, width, target, planned)
        }
        SetExpr::Values(values) => {
            let column = values
                .rows
                .iter()
                .filter_map(|row| row.content.get(index))
                .collect::<Vec<_>>();
            let mut literals = Vec::with_capacity(column.len());
            let mut typed = false;
            for expr in column {
                match literal(expr) {
                    Some(value) => literals.push(value),
                    None => typed = true,
                }
            }
            if typed {
                return ensure_planned_type(index, target, planned);
            }
            for value in literals {
                check_literal(value, target)?;
            }
            Ok(())
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
) -> PgWireResult<()> {
    match planned.fields().get(index) {
        Some(source) => ensure_assignment(source.data_type(), target),
        None => Ok(()),
    }
}

/// PostgreSQL's assignment cast matrix for the types a LakeSoul column can
/// have. Everything else is a `42804`.
fn ensure_assignment(source: &DataType, target: &Field) -> PgWireResult<()> {
    if *source == DataType::Null {
        // An unresolved parameter takes its type from the target column.
        return Ok(());
    }
    if assignment_allowed(source, target.data_type()) {
        return Ok(());
    }
    Err(datatype_mismatch(target, pg_type_name(source)))
}

fn assignment_allowed(source: &DataType, target: &DataType) -> bool {
    if is_numeric(source) {
        is_numeric(target) || is_string(target)
    } else if *source == DataType::Boolean {
        *target == DataType::Boolean || is_string(target)
    } else if is_string(source) {
        is_string(target)
    } else if is_temporal(source) {
        is_temporal(target) || is_string(target)
    } else if is_binary(source) {
        is_binary(target) || is_string(target)
    } else {
        false
    }
}

fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
    )
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

fn pg_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => {
            "smallint"
        }
        DataType::Int32 | DataType::UInt32 => "integer",
        DataType::Int64 | DataType::UInt64 => "bigint",
        DataType::Float16 | DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "text",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, timezone) => match timezone {
            Some(_) => "timestamp with time zone",
            None => "timestamp without time zone",
        },
        DataType::Decimal128(..) | DataType::Decimal256(..) => "numeric",
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "bytea",
        _ => "unknown",
    }
}

/// The PostgreSQL type an untyped numeric literal gets.
fn number_type(text: &str) -> &'static str {
    if text.contains(['.', 'e', 'E']) {
        "numeric"
    } else if text.parse::<i32>().is_ok() {
        "integer"
    } else if text.parse::<i64>().is_ok() {
        "bigint"
    } else {
        "numeric"
    }
}

fn datatype_mismatch(target: &Field, source: &str) -> PgWireError {
    user_error(
        DATATYPE_MISMATCH,
        format!(
            "column \"{}\" is of type {} but expression is of type {source}",
            target.name(),
            pg_type_name(target.data_type())
        ),
    )
}

/// `ON CONFLICT` mapped onto LakeSoul's write modes.
fn conflict_plan(
    insert: &Insert,
    fields: &BTreeSet<String>,
    primary_keys: &[String],
    range: &[String],
) -> PgWireResult<(bool, ConflictMode)> {
    let Some(OnInsert::OnConflict(conflict)) = &insert.on else {
        return Ok((false, ConflictMode::Reject));
    };
    // LakeSoul keys are partition-local: a range-partitioned table has one
    // unique key per partition, which a PostgreSQL conflict target cannot
    // express.
    if !range.is_empty() {
        return Err(feature_not_supported(
            "ON CONFLICT is not supported on a range-partitioned LakeSoul \
             table: LakeSoul unique keys are partition-local"
                .to_string(),
        ));
    }
    let primary_keys = primary_keys
        .iter()
        .map(|column| column.to_lowercase())
        .collect::<BTreeSet<_>>();
    if primary_keys.is_empty() {
        return Err(user_error(
            NO_UNIQUE_CONSTRAINT,
            format!(
                "there is no unique or exclusion constraint matching the \
                 ON CONFLICT specification on table \"{}\"",
                table_name(insert)
            ),
        ));
    }
    match &conflict.conflict_target {
        None => {}
        Some(ConflictTarget::Columns(columns)) => {
            let mut target = BTreeSet::new();
            for column in columns {
                target.insert(normalize_ident(column)?);
            }
            if target != primary_keys {
                return Err(feature_not_supported(format!(
                    "ON CONFLICT must name the table's primary key columns: \
                     LakeSoul keys on {}",
                    primary_keys.iter().cloned().collect::<Vec<_>>().join(", ")
                )));
            }
        }
        Some(ConflictTarget::OnConstraint(name)) => {
            return Err(feature_not_supported(format!(
                "ON CONFLICT ON CONSTRAINT {name} is not supported: LakeSoul \
                 has no named unique constraints"
            )));
        }
    }

    match &conflict.action {
        OnConflictAction::DoNothing => Ok((false, ConflictMode::Skip)),
        OnConflictAction::DoUpdate(update) => {
            if update.selection.is_some() {
                return Err(feature_not_supported(
                    "ON CONFLICT DO UPDATE ... WHERE is not supported by \
                     LakeSoul"
                        .to_string(),
                ));
            }
            let mut assigned = BTreeSet::new();
            for assignment in &update.assignments {
                let AssignmentTarget::ColumnName(column) = &assignment.target else {
                    return Err(do_update_unsupported("a tuple assignment"));
                };
                let column = single_column(column)?;
                let Expr::CompoundIdentifier(parts) = &assignment.value else {
                    return Err(do_update_unsupported(&format!(
                        "the value assigned to \"{column}\""
                    )));
                };
                let [excluded, value] = parts.as_slice() else {
                    return Err(do_update_unsupported(&format!(
                        "the value assigned to \"{column}\""
                    )));
                };
                let excluded = normalize_ident(excluded)?;
                let value = normalize_ident(value)?;
                if excluded != "excluded" || value != column {
                    return Err(do_update_unsupported(&format!(
                        "the value assigned to \"{column}\""
                    )));
                }
                if !assigned.insert(column.clone()) {
                    return Err(do_update_unsupported(&format!(
                        "\"{column}\" assigned twice"
                    )));
                }
            }
            // LakeSoul replace rewrites the whole inserted row. It matches
            // PostgreSQL's `DO UPDATE` only when every non-key column of the
            // table is written and assigned: an inserted-but-unassigned column
            // would be reset to NULL instead of keeping the old value, and an
            // unassigned column the statement does not insert at all would be
            // padded with NULL by the planner.
            let inserted = inserted_columns(insert, fields)?;
            if inserted != *fields {
                return Err(do_update_unsupported(
                    "an INSERT column list that does not name every column",
                ));
            }
            let expected = fields
                .difference(&primary_keys)
                .cloned()
                .collect::<BTreeSet<_>>();
            if assigned != expected {
                return Err(do_update_unsupported(
                    "an assignment list that does not set every non-key column",
                ));
            }
            Ok((true, ConflictMode::Reject))
        }
    }
}

fn do_update_unsupported(what: &str) -> PgWireError {
    feature_not_supported(format!(
        "ON CONFLICT DO UPDATE requires {what}; LakeSoul replace rewrites the \
         whole row, so only `SET column = EXCLUDED.column` for every non-key \
         column is supported"
    ))
}

/// The columns the statement inserts, normalized; an empty column list means
/// every table column.
fn inserted_columns(
    insert: &Insert,
    fields: &BTreeSet<String>,
) -> PgWireResult<BTreeSet<String>> {
    if insert.columns.is_empty() {
        return Ok(fields.clone());
    }
    let mut columns = BTreeSet::new();
    for name in &insert.columns {
        if name.0.len() != 1 {
            return Err(feature_not_supported(format!(
                "multi-part column name {name} in INSERT"
            )));
        }
        columns.insert(single_column(name)?);
    }
    Ok(columns)
}

fn single_column(name: &ObjectName) -> PgWireResult<String> {
    let [part] = name.0.as_slice() else {
        return Err(feature_not_supported(format!(
            "multi-part column name {name} in INSERT"
        )));
    };
    normalize_ident(part.as_ident().ok_or_else(|| {
        feature_not_supported(format!("object name part {part} in INSERT"))
    })?)
}

/// Refuse the clauses LakeSoul cannot honour, with a stable `0A000` instead
/// of the planner's internal error.
fn reject_unsupported(insert: &Insert) -> PgWireResult<()> {
    if insert.returning.is_some() {
        return Err(feature_not_supported(
            "INSERT ... RETURNING is not supported: LakeSoul commits the write \
             after the rows are written and returns no row stream"
                .to_string(),
        ));
    }
    for (present, clause) in [
        (insert.overwrite, "INSERT OVERWRITE"),
        (insert.table_alias.is_some(), "a table alias"),
        (insert.ignore, "INSERT IGNORE"),
        (insert.replace_into, "REPLACE INTO"),
        (insert.partitioned.is_some(), "a PARTITION clause"),
        (!insert.after_columns.is_empty(), "POST-PARTITION columns"),
        (!insert.assignments.is_empty(), "INSERT ... SET"),
        (insert.settings.is_some(), "a SETTINGS clause"),
        (insert.format_clause.is_some(), "a FORMAT clause"),
        (insert.output.is_some(), "an OUTPUT clause"),
        (insert.priority.is_some(), "a PRIORITY clause"),
        (insert.insert_alias.is_some(), "INSERT aliases"),
        (!insert.optimizer_hints.is_empty(), "optimizer hints"),
        (
            insert.multi_table_insert_type.is_some()
                || !insert.multi_table_into_clauses.is_empty()
                || !insert.multi_table_when_clauses.is_empty()
                || insert.multi_table_else_clause.is_some(),
            "a multi-table INSERT",
        ),
    ] {
        if present {
            return Err(feature_not_supported(format!(
                "INSERT with {clause} is not supported"
            )));
        }
    }
    match insert.table {
        TableObject::TableName(_) => {}
        _ => {
            return Err(feature_not_supported(
                "INSERT into a table function or subquery is not supported".to_string(),
            ));
        }
    }
    if insert.source.is_none() {
        return Err(feature_not_supported(
            "INSERT without a source is not supported".to_string(),
        ));
    }
    if insert.or.is_some() {
        return Err(feature_not_supported(
            "INSERT OR REPLACE/IGNORE is not SQL; use ON CONFLICT".to_string(),
        ));
    }
    if let Some(OnInsert::DuplicateKeyUpdate(_)) = &insert.on {
        return Err(feature_not_supported(
            "INSERT ... ON DUPLICATE KEY UPDATE is not supported".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn reject_transaction(status: TransactionStatus) -> PgWireResult<()> {
    if status == TransactionStatus::Idle {
        return Ok(());
    }
    Err(feature_not_supported(
        "INSERT is not supported inside a transaction block: LakeSoul commits \
         every statement immediately and cannot roll it back"
            .to_string(),
    ))
}

fn table_name(insert: &Insert) -> String {
    match &insert.table {
        TableObject::TableName(name) => name.to_string(),
        _ => String::new(),
    }
}

/// The table reference PostgreSQL's catalog resolves, and the name a client
/// wrote (for messages).
pub(crate) fn target_reference(
    insert: &Insert,
    database: &str,
) -> PgWireResult<(datafusion::common::TableReference, String)> {
    let TableObject::TableName(name) = &insert.table else {
        return Err(feature_not_supported(
            "INSERT into a table function or subquery is not supported".to_string(),
        ));
    };
    let written = name.to_string();
    let mut parts = Vec::new();
    for part in &name.0 {
        match part {
            ObjectNamePart::Identifier(ident) => parts.push(normalize_ident(ident)?),
            other => {
                return Err(feature_not_supported(format!(
                    "object name part {other} in INSERT"
                )));
            }
        }
    }
    use datafusion::common::TableReference;
    match parts.as_slice() {
        [table] => Ok((TableReference::bare(table.clone()), written)),
        [schema, table] => {
            if schema != PUBLIC_SCHEMA {
                return Err(user_error(
                    INVALID_SCHEMA_NAME,
                    format!("schema \"{schema}\" does not exist"),
                ));
            }
            Ok((
                TableReference::partial(PUBLIC_SCHEMA, table.clone()),
                written,
            ))
        }
        [catalog, schema, table] => {
            if catalog != database {
                return Err(user_error(
                    INVALID_CATALOG_NAME,
                    format!(
                        "cross-database reference \"{catalog}\": this connection \
                         uses database \"{database}\""
                    ),
                ));
            }
            if schema != PUBLIC_SCHEMA {
                return Err(user_error(
                    INVALID_SCHEMA_NAME,
                    format!("schema \"{schema}\" does not exist"),
                ));
            }
            Ok((
                TableReference::full(database, PUBLIC_SCHEMA, table.clone()),
                written,
            ))
        }
        _ => Err(feature_not_supported(format!(
            "object name {written} has too many parts"
        ))),
    }
}

/// `partitions` is `range1,range2;pk1,pk2`.
fn split_partitions(partitions: &str) -> (Vec<String>, Vec<String>) {
    let (range, primary_keys) = match partitions.split_once(';') {
        Some((range, primary_keys)) => (range, primary_keys),
        None => ("", partitions),
    };
    let split = |values: &str| {
        values
            .split(',')
            .map(|value| value.trim().to_lowercase())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
    };
    (split(range), split(primary_keys))
}

/// Plan the checked statement: `ON CONFLICT` is already dropped and `DO
/// UPDATE` already rewritten to a replace.
async fn plan_target(
    target: &Target,
    session_context: &SessionContext,
) -> PgWireResult<LogicalPlan> {
    // The namespace is resolved by the catalog while planning, so the
    // statement's own spelling is kept.
    session_context
        .state()
        .statement_to_plan(DfStatement::Statement(Box::new(Statement::Insert(
            target.statement.clone(),
        ))))
        .await
        .map_err(|error| PgWireError::ApiError(Box::new(error)))
}

fn write_mode(session_context: &SessionContext) -> PgWireResult<Arc<LakeSoulWriteMode>> {
    session_context
        .state()
        .config()
        .get_extension::<LakeSoulWriteMode>()
        .ok_or_else(|| internal("the session has no LakeSoul write mode"))
}

async fn run(
    insert: &Insert,
    session_context: &SessionContext,
    status: TransactionStatus,
    database: &str,
) -> PgWireResult<Response> {
    let target = prepare(insert, session_context, status, database).await?;
    write_mode(session_context)?.set(target.mode);
    let plan = plan_target(&target, session_context).await?;
    reject_duplicate_upsert_rows(&plan, session_context).await?;
    let dataframe = session_context
        .execute_logical_plan(plan)
        .await
        .map_err(insert_error)?;
    let batches = dataframe.collect().await.map_err(insert_error)?;
    Ok(Response::Execution(
        Tag::new("INSERT")
            .with_oid(0)
            .with_rows(rows_affected(&batches)?),
    ))
}

async fn execute_bound(
    logical_plan: &LogicalPlan,
    params: &ParamValues,
    session_context: &SessionContext,
) -> PgWireResult<Response> {
    let plan = logical_plan
        .clone()
        .replace_params_with_values(params)
        .map_err(insert_error)?;
    reject_duplicate_upsert_rows(&plan, session_context).await?;
    let dataframe = session_context
        .execute_logical_plan(plan)
        .await
        .map_err(insert_error)?;
    let batches = dataframe.collect().await.map_err(insert_error)?;
    Ok(Response::Execution(
        Tag::new("INSERT")
            .with_oid(0)
            .with_rows(rows_affected(&batches)?),
    ))
}

/// The sink reports the written row count in its single `count` batch.
fn rows_affected(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> PgWireResult<usize> {
    let mut rows = 0usize;
    for batch in batches {
        let Some(column) = batch.column_by_name("count") else {
            continue;
        };
        let values = column
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .ok_or_else(|| internal("the INSERT row count has an unexpected type"))?;
        rows += values
            .values()
            .iter()
            .map(|value| *value as usize)
            .sum::<usize>();
    }
    Ok(rows)
}

/// PostgreSQL rejects an `INSERT ... ON CONFLICT DO UPDATE` whose source
/// produces the same key twice: the command would affect one row twice
/// (`21000`). LakeSoul replace is last-wins, so the source is grouped by key
/// before the write and one duplicated key rejects the statement before any
/// file is written.
///
/// The check only runs for a replace plan. A plain `INSERT` reports `23505`
/// from the sink, and `DO NOTHING` drops the duplicate as PostgreSQL does.
async fn reject_duplicate_upsert_rows(
    plan: &LogicalPlan,
    session_context: &SessionContext,
) -> PgWireResult<()> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(());
    };
    if dml.op != WriteOp::Insert(InsertOp::Replace) {
        return Ok(());
    }
    let provider = session_context
        .table_provider(dml.table_name.clone())
        .await
        .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    let provider: &dyn std::any::Any = provider.as_ref();
    let Some(provider) = provider.downcast_ref::<LakeSoulTableProvider>() else {
        return Ok(());
    };
    let (_, primary_keys) = split_partitions(&provider.table_info().partitions);
    if primary_keys.is_empty() {
        return Ok(());
    }
    let groups = primary_keys
        .iter()
        .map(|column| col(column.as_str()))
        .collect::<Vec<_>>();
    let counted = LogicalPlanBuilder::from(dml.input.as_ref().clone())
        .aggregate(
            groups,
            vec![count(lit(1i64)).alias("lakesoul_duplicate_keys")],
        )
        .map_err(insert_error)?;
    let duplicated = counted
        .filter(col("lakesoul_duplicate_keys").gt(lit(1i64)))
        .map_err(insert_error)?
        .limit(0, Some(1))
        .map_err(insert_error)?
        .build()
        .map_err(insert_error)?;
    let batches = session_context
        .execute_logical_plan(duplicated)
        .await
        .map_err(insert_error)?
        .collect()
        .await
        .map_err(insert_error)?;
    if batches.iter().any(|batch| batch.num_rows() > 0) {
        return Err(user_error(
            CARDINALITY_VIOLATION,
            "ON CONFLICT DO UPDATE command cannot affect row a second time".to_string(),
        ));
    }
    Ok(())
}

/// Map a write violation to its PostgreSQL SQLSTATE; other errors keep the
/// server's generic internal error, like every other DataFusion failure.
fn insert_error(error: DataFusionError) -> PgWireError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(violation) = current.downcast_ref::<WriteViolation>() {
            return user_error(violation.sqlstate(), violation.to_string());
        }
        if let Some(conflict) = current.downcast_ref::<WriteConflict>() {
            return user_error(conflict.sqlstate(), conflict.to_string());
        }
        if let Some(violation) =
            current.downcast_ref::<lakesoul_datafusion::write_mode::NotNullViolation>()
        {
            return user_error("23502", violation.to_string());
        }
        source = current.source();
    }
    PgWireError::ApiError(Box::new(error))
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message,
    )))
}

fn internal(message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        message.to_string(),
    )))
}

/// Position of the first `INSERT` in a multi-statement simple-query message.
///
/// PostgreSQL runs a simple-query message in an implicit transaction, so a
/// statement after the `INSERT` failing would roll the insert back. LakeSoul
/// commits each statement immediately and cannot undo it, so such a batch is
/// refused at the `INSERT` itself: statements before it still stream their
/// results, and no `INSERT` runs.
pub(crate) fn first_insert_in_batch(statements: &[String]) -> Option<usize> {
    if statements.len() < 2 {
        return None;
    }
    let parser = PostgresCompatibilityParser::new();
    statements.iter().position(|sql| {
        parser.parse(sql).is_ok_and(|parsed| {
            parsed
                .iter()
                .any(|statement| matches!(statement, Statement::Insert(_)))
        })
    })
}

/// The failure for an `INSERT` that shares its simple-query message.
pub(crate) fn refuse_insert_in_batch() -> PgWireError {
    feature_not_supported(
        "INSERT cannot be combined with other statements in one simple-query \
         message: PostgreSQL would run the batch in an implicit transaction, \
         and LakeSoul commits each INSERT immediately"
            .to_string(),
    )
}
