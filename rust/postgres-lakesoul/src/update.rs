// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `UPDATE` for the SQL front end.
//!
//! The statement is claimed at Parse and at Execute. Parse builds the plan the
//! way the query would run *now* so DataFusion can infer the parameter types,
//! but Execute never uses that plan: it re-resolves the target, re-checks the
//! table's effective configuration, captures a new primary metadata snapshot
//! and plans again. A prepared statement therefore cannot bypass a table that
//! became CDC/append-only, and cannot publish into a recreated table
//! identity: a target whose `table_id` changed since Parse is rejected with
//! `42P01`.
//!
//! Execution is a LakeSoul read-modify-write: the planner (see
//! `lakesoul_datafusion::planner::update`) turns the statement into
//! `INSERT ... SELECT` with the replace write operation, binds one partition
//! snapshot to every scan of the target, scans the merge-on-read live rows,
//! evaluates `WHERE` and then the full new row, appends the new versions and
//! publishes them with an exact version comparison. A concurrent commit makes
//! the statement fail with `40001` instead of silently rebasing it.
//!
//! Refused with `0A000`: CDC tables (by effective configuration, before any
//! scan, so an empty table or `WHERE false` does not slip through), tables
//! without a primary key, `appendOnly` tables, tables with a non-overwrite
//! merge operator, assignments to a primary-key or range-partition column,
//! `RETURNING`, `UPDATE ... FROM`, and writes inside a transaction block or a
//! multi-statement simple-query batch. `UPDATE ... RETURNING` has no row
//! stream, and changing a key would leave the old row behind without a delete
//! path.

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{LogicalPlan, dml::DmlStatement};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Statement, TableFactor, TableObject, TableWithJoins, Update,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};

use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::planner::update::{
    PlannedUpdate, SnapshotMode, UpdatePlanError, plan_update,
};

use crate::insert::{
    insert_error, request_context, rows_affected, target_reference, user_error,
};

/// SQLSTATE `undefined_table` (a prepared statement's target identity is gone).
const UNDEFINED_TABLE: &str = "42P01";

pub(crate) struct UpdateStatementHook;

#[async_trait]
impl QueryHook for UpdateStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let update = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                crate::insert::reject_transaction(status, "UPDATE")?;
                let planned = prepare(update, session_context, &database).await?;
                execute(&planned, None, session_context).await
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
        let update = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                crate::insert::reject_transaction(status, "UPDATE")?;
                // Analysis only: the plan supplies parameter types, but no
                // partition snapshot is pinned at Parse time.
                let planned = prepare_with(
                    update,
                    session_context,
                    &database,
                    SnapshotMode::PlanOnly,
                )
                .await?;
                Ok(planned.plan)
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
        let update = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                crate::insert::reject_transaction(status, "UPDATE")?;
                // Execute re-plans from the statement and the live catalog.
                // Compare the prepared statement's target identity first: a
                // recreated table (whatever its new configuration) must not be
                // written through the old binding.
                if let Some(parsed) = plan_identity(logical_plan) {
                    let (table_ref, table_name) =
                        target_reference(&table_object(&update.table)?, &database)?;
                    let current = resolved_table_id(session_context, &table_ref).await?;
                    if current.as_deref() != Some(parsed.as_str()) {
                        return Err(user_error(
                            UNDEFINED_TABLE,
                            format!(
                                "relation \"{table_name}\" was dropped and recreated; \
                                 reparse the statement"
                            ),
                        ));
                    }
                }
                let planned = prepare_with(
                    update,
                    session_context,
                    &database,
                    SnapshotMode::Capture,
                )
                .await?;
                execute(&planned, Some(params), session_context).await
            }
            .await,
        )
    }
}

pub(crate) fn extract(statement: &Statement) -> Option<&Update> {
    match statement {
        Statement::Update(update) => Some(update),
        _ => None,
    }
}

async fn prepare(
    update: &Update,
    session_context: &SessionContext,
    database: &str,
) -> PgWireResult<PlannedUpdate> {
    prepare_with(update, session_context, database, SnapshotMode::Capture).await
}

async fn prepare_with(
    update: &Update,
    session_context: &SessionContext,
    database: &str,
    mode: SnapshotMode,
) -> PgWireResult<PlannedUpdate> {
    let (table_ref, _table_name) =
        target_reference(&table_object(&update.table)?, database)?;
    plan_update(session_context, update, table_ref, mode)
        .await
        .map_err(plan_error)
}

async fn execute(
    planned: &PlannedUpdate,
    params: Option<&ParamValues>,
    session_context: &SessionContext,
) -> PgWireResult<Response> {
    let plan = match params {
        Some(params) => planned.plan.clone().replace_params_with_values(params),
        None => Ok(planned.plan.clone()),
    }
    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    // `planned` still holds the snapshot guard here: planning, execution and
    // the metadata commit all see the same partition versions.
    let dataframe = session_context
        .execute_logical_plan(plan)
        .await
        .map_err(update_error)?;
    let batches = dataframe.collect().await.map_err(update_error)?;
    Ok(Response::Execution(
        // PostgreSQL's tag is `UPDATE <rows>`; the `0` oid field belongs to
        // INSERT only.
        Tag::new("UPDATE").with_rows(rows_affected(&batches)?),
    ))
}

/// Map execution failures: a value that failed to convert at runtime is
/// PostgreSQL's `22P02`, everything else keeps the INSERT write mapping.
fn update_error(error: datafusion::error::DataFusionError) -> PgWireError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(arrow) =
            current.downcast_ref::<datafusion::arrow::error::ArrowError>()
            && matches!(
                arrow,
                datafusion::arrow::error::ArrowError::CastError(_)
                    | datafusion::arrow::error::ArrowError::ParseError(_)
            )
        {
            return user_error("22P02", arrow.to_string());
        }
        source = current.source();
    }
    insert_error(error)
}

fn plan_error(error: UpdatePlanError) -> PgWireError {
    match error {
        UpdatePlanError::DataFusion(error) => PgWireError::ApiError(Box::new(error)),
        other => user_error(other.sqlstate(), other.to_string()),
    }
}

/// The `table_id` the target name resolves to right now, if it is a LakeSoul
/// table. A missing table is `None`, which the caller reports as the prepared
/// identity being gone.
async fn resolved_table_id(
    session_context: &SessionContext,
    table_ref: &datafusion::common::TableReference,
) -> PgWireResult<Option<String>> {
    let provider = match session_context.table_provider(table_ref.clone()).await {
        Ok(provider) => provider,
        Err(datafusion::error::DataFusionError::Plan(message))
            if message.contains("No table named")
                || message.contains("does not exist")
                || message.contains("not found") =>
        {
            return Ok(None);
        }
        Err(error) => return Err(insert_error(error)),
    };
    let provider: &dyn std::any::Any = provider.as_ref();
    Ok(provider
        .downcast_ref::<LakeSoulTableProvider>()
        .map(|provider| provider.table_info().table_id.clone()))
}

/// The table identity a Parse-time plan was built against.
fn plan_identity(plan: &LogicalPlan) -> Option<String> {
    let LogicalPlan::Dml(DmlStatement { target, .. }) = plan else {
        return None;
    };
    let provider = source_as_provider(target).ok()?;
    let provider: &dyn std::any::Any = provider.as_ref();
    provider
        .downcast_ref::<LakeSoulTableProvider>()
        .map(|provider| provider.table_info().table_id.clone())
}

/// Position of the first `UPDATE` in a multi-statement simple-query message.
///
/// A simple-query batch runs in an implicit transaction, which LakeSoul cannot
/// honour: the UPDATE commits immediately and a later failure in the batch
/// could not roll it back. The statement itself is refused, so statements
/// before it still stream their rows.
pub(crate) fn first_update_in_batch(statements: &[String]) -> Option<usize> {
    if statements.len() < 2 {
        return None;
    }
    let parser = PostgresCompatibilityParser::new();
    statements.iter().position(|sql| {
        parser.parse(sql).is_ok_and(|parsed| {
            parsed
                .iter()
                .any(|statement| matches!(statement, Statement::Update(_)))
        })
    })
}

/// The failure for an `UPDATE` that shares its simple-query message.
pub(crate) fn refuse_update_in_batch() -> PgWireError {
    crate::ddl::feature_not_supported(
        "UPDATE cannot be combined with other statements in one simple-query \
         message: PostgreSQL would run the batch in an implicit transaction, \
         and LakeSoul commits each UPDATE immediately"
            .to_string(),
    )
}

/// `target_reference` takes a table object; `Update.table` is a
/// `TableWithJoins`, so expose its relation that way.
fn table_object(table: &TableWithJoins) -> PgWireResult<TableObject> {
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(TableObject::TableName(name.clone())),
        _ => Err(user_error(
            "0A000",
            "UPDATE of a table function or subquery is not supported".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_with_update_is_detected() {
        let statements = vec!["select 1".to_string(), "update t set n = 1".to_string()];
        assert_eq!(first_update_in_batch(&statements), Some(1));
    }

    #[test]
    fn a_single_update_is_not_a_batch() {
        assert_eq!(
            first_update_in_batch(&["update t set n = 1".to_string()]),
            None
        );
    }

    #[test]
    fn batch_refusal_is_feature_not_supported() {
        let error = refuse_update_in_batch();
        match error {
            PgWireError::UserError(info) => assert_eq!(info.code, "0A000"),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
