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

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::ParamValues;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, ConflictTarget, Expr, Insert, ObjectName, ObjectNamePart,
    OnConflictAction, OnInsert, Statement, TableObject,
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
                plan_target(insert, &target, session_context).await
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

fn extract(statement: &Statement) -> Option<&Insert> {
    match statement {
        Statement::Insert(insert) => Some(insert),
        _ => None,
    }
}

/// Everything the statement's execution depends on, resolved once.
struct Target {
    /// Whether the AST must be planned as a LakeSoul replace (`DO UPDATE`).
    replace: bool,
    /// What the sink does with a key that is already present.
    mode: ConflictMode,
}

/// Connection state a hook may hold across an `await`.
fn request_context(
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
    Ok(Target { replace, mode })
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

fn reject_transaction(status: TransactionStatus) -> PgWireResult<()> {
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
fn target_reference(
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

/// Build the plan from a rewritten statement: the `ON CONFLICT` clause is
/// dropped (DataFusion refuses it) and `DO UPDATE` selects the replace write
/// operation.
async fn plan_target(
    insert: &Insert,
    target: &Target,
    session_context: &SessionContext,
) -> PgWireResult<LogicalPlan> {
    let mut rewritten = insert.clone();
    rewritten.on = None;
    if target.replace {
        rewritten.or = Some(datafusion::sql::sqlparser::ast::SqliteOnConflict::Replace);
    }
    // The namespace is resolved by the catalog while planning, so the
    // statement's own spelling is kept.
    session_context
        .state()
        .statement_to_plan(DfStatement::Statement(Box::new(Statement::Insert(
            rewritten,
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
    let plan = plan_target(insert, &target, session_context).await?;
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
