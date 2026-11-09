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
//! NOTHING` drops the conflicting rows. `ON CONFLICT ... DO UPDATE` is
//! planned by [`lakesoul_datafusion::planner::upsert`]; this module only
//! routes it there.
//!
//! `RETURNING` is refused because the sink commits after the statement's rows
//! are written and returns no row stream.
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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::ParamValues;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    ConflictTarget, Insert, ObjectNamePart, OnConflictAction, OnInsert, Statement,
    TableObject,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::response::{
    CommandComplete, TransactionStatus,
};

use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::federation::FederationTableProvider;
use lakesoul_datafusion::federation::external_file::ExternalFileTableProvider;
use lakesoul_datafusion::pg_insert_analysis;
use lakesoul_datafusion::planner::upsert::{
    SnapshotMode as UpsertSnapshotMode, UpsertPlanError, plan_upsert,
};
use lakesoul_datafusion::write_mode::{
    ConflictMode, LakeSoulWriteMode, WriteConflict, WriteViolation,
};

use crate::catalog::PUBLIC_SCHEMA;
use crate::ddl::{feature_not_supported, normalize_column_ident, normalize_ident};
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
                if is_do_update(insert) {
                    run_upsert(insert, session_context, status, &database).await
                } else {
                    run(insert, session_context, status, &database).await
                }
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
                if is_do_update(insert) {
                    reject_transaction(status, "INSERT")?;
                    let planned = prepare_upsert(
                        insert,
                        session_context,
                        &database,
                        UpsertSnapshotMode::PlanOnly,
                    )
                    .await?;
                    return Ok(planned.plan.clone());
                }
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
                if is_do_update(insert) {
                    reject_transaction(status, "INSERT")?;
                    verify_upsert_identity(
                        insert,
                        logical_plan,
                        session_context,
                        &database,
                    )
                    .await?;
                    let planned = prepare_upsert(
                        insert,
                        session_context,
                        &database,
                        UpsertSnapshotMode::Capture,
                    )
                    .await?;
                    return execute_upsert(&planned, Some(params), session_context).await;
                }
                let target = prepare(insert, session_context, status, &database).await?;
                // The mode is read while the write plan is built, which happens in
                // `execute_logical_plan` below.
                if let TargetKind::LakeSoul(mode) = target.kind {
                    write_mode(session_context)?.set(mode);
                }
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

/// Whether the statement is the general upsert path (`DO UPDATE`).
fn is_do_update(insert: &Insert) -> bool {
    matches!(
        &insert.on,
        Some(OnInsert::OnConflict(conflict))
            if matches!(conflict.action, OnConflictAction::DoUpdate(_))
    )
}

async fn prepare_upsert(
    insert: &Insert,
    session_context: &SessionContext,
    database: &str,
    mode: UpsertSnapshotMode,
) -> PgWireResult<lakesoul_datafusion::planner::upsert::PlannedUpsert> {
    let (table_ref, _) = target_reference(&insert.table, database)?;
    plan_upsert(session_context, insert, table_ref, mode)
        .await
        .map_err(upsert_plan_error)
}

async fn run_upsert(
    insert: &Insert,
    session_context: &SessionContext,
    status: TransactionStatus,
    database: &str,
) -> PgWireResult<Response> {
    reject_transaction(status, "INSERT")?;
    let planned = prepare_upsert(
        insert,
        session_context,
        database,
        UpsertSnapshotMode::Capture,
    )
    .await?;
    execute_upsert(&planned, None, session_context).await
}

async fn execute_upsert(
    planned: &lakesoul_datafusion::planner::upsert::PlannedUpsert,
    params: Option<&ParamValues>,
    session_context: &SessionContext,
) -> PgWireResult<Response> {
    let (rows, tag) = execute_upsert_plan(planned, params, session_context).await?;
    Ok(Response::Execution(tag.with_rows(rows)))
}

/// Runs the prepared upsert and returns the written rows with the command tag.
///
/// Shared with detached jobs, which have no pgwire `Response` to fill.
async fn execute_upsert_plan(
    planned: &lakesoul_datafusion::planner::upsert::PlannedUpsert,
    params: Option<&ParamValues>,
    session_context: &SessionContext,
) -> PgWireResult<(usize, Tag)> {
    let plan = match params {
        Some(params) => planned.plan.clone().replace_params_with_values(params),
        None => Ok(planned.plan.clone()),
    }
    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    // `planned` keeps the captured snapshot installed through execution.
    let dataframe = session_context
        .execute_logical_plan(plan)
        .await
        .map_err(upsert_exec_error)?;
    let batches = dataframe.collect().await.map_err(upsert_exec_error)?;
    Ok((rows_affected(&batches)?, Tag::new("INSERT").with_oid(0)))
}

/// Reject a prepared upsert whose target name now resolves to another table.
async fn verify_upsert_identity(
    insert: &Insert,
    logical_plan: &LogicalPlan,
    session_context: &SessionContext,
    database: &str,
) -> PgWireResult<()> {
    let Some(parsed) = plan_table_id(logical_plan) else {
        return Ok(());
    };
    let (table_ref, table_name) = target_reference(&insert.table, database)?;
    let current = resolved_table_id(session_context, &table_ref).await?;
    if current.as_deref() != Some(parsed.as_str()) {
        return Err(user_error(
            UNDEFINED_TABLE,
            format!(
                "relation \"{table_name}\" was dropped and recreated; reparse the statement"
            ),
        ));
    }
    Ok(())
}

fn plan_table_id(plan: &LogicalPlan) -> Option<String> {
    let LogicalPlan::Dml(datafusion::logical_expr::dml::DmlStatement { target, .. }) =
        plan
    else {
        return None;
    };
    let provider = datafusion::datasource::source_as_provider(target).ok()?;
    let provider: &dyn std::any::Any = provider.as_ref();
    provider
        .downcast_ref::<LakeSoulTableProvider>()
        .map(|provider| provider.table_info().table_id.clone())
}

async fn resolved_table_id(
    session_context: &SessionContext,
    table_ref: &datafusion::common::TableReference,
) -> PgWireResult<Option<String>> {
    let provider = match session_context.table_provider(table_ref.clone()).await {
        Ok(provider) => provider,
        Err(error) => {
            let message = error.to_string();
            if message.contains("No table named")
                || message.contains("does not exist")
                || message.contains("not found")
            {
                return Ok(None);
            }
            return Err(insert_error(error));
        }
    };
    let provider: &dyn std::any::Any = provider.as_ref();
    Ok(provider
        .downcast_ref::<LakeSoulTableProvider>()
        .map(|provider| provider.table_info().table_id.clone()))
}

fn upsert_plan_error(error: UpsertPlanError) -> PgWireError {
    match error {
        UpsertPlanError::DataFusion(error) => PgWireError::ApiError(Box::new(error)),
        other => user_error(other.sqlstate(), other.to_string()),
    }
}

/// Map execution failures of the upsert plan. Runtime casts are PostgreSQL
/// `22P02` even though the plan may raise them from a worker stage.
fn upsert_exec_error(error: DataFusionError) -> PgWireError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(arrow) =
            current.downcast_ref::<datafusion::arrow::error::ArrowError>()
        {
            match arrow {
                datafusion::arrow::error::ArrowError::CastError(_)
                | datafusion::arrow::error::ArrowError::ParseError(_) => {
                    return user_error("22P02", arrow.to_string());
                }
                datafusion::arrow::error::ArrowError::DivideByZero => {
                    return user_error("22012", arrow.to_string());
                }
                _ => {}
            }
        }
        source = current.source();
    }
    insert_error(error)
}

/// Everything the statement's execution depends on, resolved once.
struct Target {
    kind: TargetKind,
    /// The statement to plan: `ON CONFLICT` is dropped (DataFusion refuses
    /// it) and a no-list `VALUES`/`SELECT` source shorter than the table is
    /// padded with NULLs, which is PostgreSQL's default semantics for the
    /// unlisted columns. `DO UPDATE` never reaches this path.
    statement: Insert,
}

enum TargetKind {
    LakeSoul(ConflictMode),
    Foreign,
    ExternalFile,
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
    reject_transaction(status, "INSERT")?;
    reject_unsupported(insert)?;
    let (table_ref, table_name) = target_reference(&insert.table, database)?;
    let provider = session_context
        .table_provider(table_ref.clone())
        .await
        .map_err(|error| {
            user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table_name}\" does not exist: {error}"),
            )
        })?;
    let provider_any: &dyn std::any::Any = provider.as_ref();
    if let Some(provider) = provider_any.downcast_ref::<LakeSoulTableProvider>() {
        let table_info = provider.table_info();
        let (range, primary_keys) = split_partitions(&table_info.partitions);
        let mode = conflict_plan(insert, &primary_keys, &range)?;
        let statement =
            checked_statement(insert, &provider.schema(), session_context).await?;
        return Ok(Target {
            kind: TargetKind::LakeSoul(mode),
            statement,
        });
    }

    if let Some(provider) = provider_any.downcast_ref::<FederationTableProvider>() {
        if insert.on.is_some() {
            return Err(feature_not_supported(
                "INSERT ... ON CONFLICT is not supported on a foreign table; \
                 configure the foreign table with \
                 OPTIONS (primary_key '...', on_conflict 'update'|'nothing')"
                    .to_string(),
            ));
        }
        let statement =
            checked_statement(insert, &provider.schema(), session_context).await?;
        return Ok(Target {
            kind: TargetKind::Foreign,
            statement,
        });
    }

    if let Some(provider) = provider_any.downcast_ref::<ExternalFileTableProvider>() {
        if insert.on.is_some() {
            return Err(feature_not_supported(
                "INSERT ... ON CONFLICT is not supported on an external file table; \
                 append-only inserts have no key conflict handling"
                    .to_string(),
            ));
        }
        let statement =
            checked_statement(insert, &provider.schema(), session_context).await?;
        return Ok(Target {
            kind: TargetKind::ExternalFile,
            statement,
        });
    }

    Err(feature_not_supported(format!(
        "relation \"{table_name}\" is not a LakeSoul or foreign table"
    )))
}

/// PostgreSQL analyses an `INSERT` before planning it: the source must line
/// up with the target columns (`42601`), a no-list source shorter than the
/// table is completed with NULLs for the unlisted columns, and every exported
/// column is checked against the assignment cast matrix (`42804`/`22P02`).
/// The rules live in [`pg_insert_analysis`] so the `ON CONFLICT DO UPDATE`
/// planner cannot drift from the plain `INSERT` contract. This returns the
/// statement to plan (with a possibly padded source).
async fn checked_statement(
    insert: &Insert,
    schema: &SchemaRef,
    session_context: &SessionContext,
) -> PgWireResult<Insert> {
    let mut statement = insert.clone();
    statement.on = None;
    let analysis = pg_insert_analysis::analyse(insert, schema.as_ref(), session_context)
        .await
        .map_err(insert_analysis_error)?;
    pg_insert_analysis::pad_insert_source(&mut statement, &analysis);
    Ok(statement)
}

fn insert_analysis_error(error: pg_insert_analysis::InsertAnalysisError) -> PgWireError {
    user_error(error.sqlstate, error.to_string())
}

/// `ON CONFLICT` mapped onto LakeSoul's write modes.
///
/// `DO UPDATE` is handled by [`prepare_upsert`]; this path only sees a plain
/// `INSERT` and `ON CONFLICT DO NOTHING`.
fn conflict_plan(
    insert: &Insert,
    primary_keys: &[String],
    range: &[String],
) -> PgWireResult<ConflictMode> {
    let Some(OnInsert::OnConflict(conflict)) = &insert.on else {
        return Ok(ConflictMode::Reject);
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
                target.insert(normalize_column_ident(column)?);
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
        OnConflictAction::DoNothing => Ok(ConflictMode::Skip),
        OnConflictAction::DoUpdate(_) => Err(internal(
            "ON CONFLICT DO UPDATE must be planned by the upsert path",
        )),
    }
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

pub(crate) fn reject_transaction(
    status: TransactionStatus,
    verb: &str,
) -> PgWireResult<()> {
    if status == TransactionStatus::Idle {
        return Ok(());
    }
    Err(feature_not_supported(format!(
        "{verb} is not supported inside a transaction block: LakeSoul commits \
         every statement immediately and cannot roll it back"
    )))
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
    table: &TableObject,
    database: &str,
) -> PgWireResult<(datafusion::common::TableReference, String)> {
    let TableObject::TableName(name) = table else {
        return Err(feature_not_supported(
            "a write target that is not a table is not supported".to_string(),
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
    let (rows, tag) = execute_write(insert, session_context, status, database).await?;
    Ok(Response::Execution(tag.with_rows(rows)))
}

/// Runs one `INSERT` for a detached job, outside a client connection.
///
/// The write is the same one the wire path performs, including its single
/// commit; the caller gets the row count and the command tag instead of a
/// pgwire response.
pub(crate) async fn run_for_job(
    insert: &Insert,
    session_context: &SessionContext,
    database: &str,
) -> PgWireResult<(usize, String)> {
    if is_do_update(insert) {
        let planned = prepare_upsert(
            insert,
            session_context,
            database,
            UpsertSnapshotMode::Capture,
        )
        .await?;
        let (rows, tag) = execute_upsert_plan(&planned, None, session_context).await?;
        return Ok((rows, CommandComplete::from(tag).tag));
    }
    let (rows, tag) =
        execute_write(insert, session_context, TransactionStatus::Idle, database).await?;
    let tag = CommandComplete::from(tag.with_rows(rows)).tag;
    Ok((rows, tag))
}

async fn execute_write(
    insert: &Insert,
    session_context: &SessionContext,
    status: TransactionStatus,
    database: &str,
) -> PgWireResult<(usize, Tag)> {
    let target = prepare(insert, session_context, status, database).await?;
    if let TargetKind::LakeSoul(mode) = target.kind {
        write_mode(session_context)?.set(mode);
    }
    let plan = plan_target(&target, session_context).await?;
    let dataframe = session_context
        .execute_logical_plan(plan)
        .await
        .map_err(insert_error)?;
    let batches = dataframe.collect().await.map_err(insert_error)?;
    Ok((rows_affected(&batches)?, Tag::new("INSERT").with_oid(0)))
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
pub(crate) fn rows_affected(
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
pub(crate) fn insert_error(error: DataFusionError) -> PgWireError {
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
        if let Some(
            error @ lakesoul_metadata::error::LakeSoulMetaDataError::CommitConflict(_),
        ) = current.downcast_ref::<lakesoul_metadata::error::LakeSoulMetaDataError>()
        {
            return user_error("40001", error.to_string());
        }
        source = current.source();
    }
    // A distributed stage re-encodes an error as text, so the typed cause is
    // gone. The encoder tags its message with the SQLSTATE marker.
    let flattened = error.to_string();
    if let Some(sqlstate) =
        lakesoul_datafusion::write_mode::EncodedViolation::sqlstate_of(&flattened)
    {
        return user_error(sqlstate, flattened);
    }
    PgWireError::ApiError(Box::new(error))
}

pub(crate) fn user_error(code: &str, message: String) -> PgWireError {
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
