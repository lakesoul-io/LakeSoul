// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `TRUNCATE` for the SQL front end.
//!
//! `TRUNCATE` publishes an empty snapshot for every selected partition. It is
//! deliberately **not** a file deletion: the old commits and files stay in
//! place, so a pinned snapshot can still read the pre-truncate relation, and
//! GC reclaims the objects after the retention window. This is the same
//! lifecycle the clean job applies to expired partitions.
//!
//! The statement is a metadata-only operation. Every target's definition is
//! re-checked under its `table_info` row lock inside one metadata transaction,
//! so a concurrent DROP or `ALTER` aborts the whole statement instead of
//! truncating a stale view.
//!
//! Refused with `0A000`: foreign/external tables, subscription targets, IVM
//! internal tables, a `PARTITION` spec that does not name every range
//! partition column, and a transaction block (the metadata commit is not
//! staged yet).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Statement, TableObject, Truncate, TruncateTableTarget, Value,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_metadata::{MetaDataClientRef, TruncateTarget};
use lakesoul_metadata_proto::entity::PartitionInfo;
use rootcause::compat::boxed_error::IntoBoxedError;

use crate::insert::{
    insert_error, reject_transaction_strict, request_context, target_reference,
    user_error,
};
use crate::server::ConnectionSession;

const UNDEFINED_TABLE: &str = "42P01";

pub(crate) struct TruncateStatementHook;

#[async_trait]
impl QueryHook for TruncateStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let truncate = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                reject_transaction_strict(status, "TRUNCATE")?;
                let meta_client = meta_client(client)?;
                let planned =
                    prepare(truncate, session_context, &database, &meta_client).await?;
                execute(&planned, &meta_client).await
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        extract(statement)?;
        // `TRUNCATE` has no parameters and no result columns. Resolving the
        // target is deferred to Execute, where the table is re-read anyway.
        Some(Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let truncate = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                reject_transaction_strict(status, "TRUNCATE")?;
                let meta_client = meta_client(client)?;
                let planned =
                    prepare(truncate, session_context, &database, &meta_client).await?;
                execute(&planned, &meta_client).await
            }
            .await,
        )
    }
}

fn extract(statement: &Statement) -> Option<&Truncate> {
    match statement {
        Statement::Truncate(truncate) => Some(truncate),
        _ => None,
    }
}

fn meta_client(client: &(impl ClientInfo + ?Sized)) -> PgWireResult<MetaDataClientRef> {
    let session = client
        .session_extensions()
        .get::<ConnectionSession>()
        .ok_or_else(|| {
            user_error("XX000", "the connection has no session".to_string())
        })?;
    Ok(Arc::clone(&session.meta_client))
}

struct PlannedTruncate {
    targets: Vec<TruncateTarget>,
}

async fn prepare(
    truncate: &Truncate,
    session_context: &SessionContext,
    database: &str,
    meta_client: &MetaDataClientRef,
) -> PgWireResult<PlannedTruncate> {
    let mut targets = Vec::with_capacity(truncate.table_names.len());
    for target in &truncate.table_names {
        if let Some(target) =
            prepare_table(target, truncate, session_context, database, meta_client)
                .await?
        {
            targets.push(target);
        }
    }
    Ok(PlannedTruncate { targets })
}

async fn prepare_table(
    target: &TruncateTableTarget,
    truncate: &Truncate,
    session_context: &SessionContext,
    database: &str,
    meta_client: &MetaDataClientRef,
) -> PgWireResult<Option<TruncateTarget>> {
    let (table_ref, table_name) =
        target_reference(&TableObject::TableName(target.name.clone()), database)?;
    let provider = match session_context.table_provider(table_ref).await {
        Ok(provider) => provider,
        Err(error) if truncate.if_exists && is_table_not_found(&error) => {
            return Ok(None);
        }
        Err(error) => {
            return Err(user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table_name}\" does not exist: {error}"),
            ));
        }
    };
    let provider: &dyn std::any::Any = provider.as_ref();
    let provider = provider
        .downcast_ref::<LakeSoulTableProvider>()
        .ok_or_else(|| {
            crate::ddl::feature_not_supported(format!(
                "TRUNCATE is not supported on \"{table_name}\": not a LakeSoul table"
            ))
        })?;
    crate::acl::require_table_write(session_context, database, &table_name).await?;

    let table_info = provider.primary_table_info().await.map_err(|error| {
        user_error(
            UNDEFINED_TABLE,
            format!("relation \"{table_name}\" does not exist: {error}"),
        )
    })?;
    if crate::alter::subscription_of(meta_client, &table_info.table_id)
        .await?
        .is_some()
    {
        return Err(crate::ddl::feature_not_supported(format!(
            "TRUNCATE is not supported on \"{table_name}\": it is the target of a subscription"
        )));
    }
    if crate::alter::is_ivm_table(&table_info) {
        return Err(crate::ddl::feature_not_supported(format!(
            "TRUNCATE is not supported on \"{table_name}\": IVM owns the table"
        )));
    }

    let snapshot = provider.primary_snapshot().await.map_err(|error| {
        PgWireError::ApiError(Box::new(datafusion::error::DataFusionError::External(
            error.into_boxed_error(),
        )))
    })?;
    let selected = select_partitions(
        &table_info,
        snapshot.as_slice(),
        truncate.partitions.as_deref(),
    )?;
    Ok(Some(TruncateTarget {
        table_info,
        partitions: selected,
    }))
}

fn is_table_not_found(error: &datafusion::error::DataFusionError) -> bool {
    let message = error.to_string();
    message.contains("No table named")
        || message.contains("does not exist")
        || message.contains("not found")
}

/// Resolve `TRUNCATE ... PARTITION (...)` to the matching partition versions.
///
/// Only equality on every declared range partition column is accepted; a
/// missing or unknown column is `0A000` rather than a silent whole-table
/// truncate.
fn select_partitions(
    table_info: &lakesoul_metadata_proto::entity::TableInfo,
    all: &[PartitionInfo],
    spec: Option<&[Expr]>,
) -> PgWireResult<Vec<PartitionInfo>> {
    let Some(spec) = spec else {
        return Ok(all.to_vec());
    };
    let (range_columns, _) = crate::insert::split_partitions(&table_info.partitions);
    if range_columns.is_empty() {
        return Err(crate::ddl::feature_not_supported(
            "TRUNCATE ... PARTITION is not supported on a table without range partitions"
                .to_string(),
        ));
    }

    let mut values: HashMap<String, String> = HashMap::with_capacity(spec.len());
    for expr in spec {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = expr
        else {
            return Err(crate::ddl::feature_not_supported(
                "TRUNCATE ... PARTITION accepts only `column = value` terms".to_string(),
            ));
        };
        let column = partition_column(left)?;
        let value = partition_value(right)?;
        if values.insert(column.clone(), value).is_some() {
            return Err(user_error(
                "42701",
                format!("partition column \"{column}\" is specified more than once"),
            ));
        }
    }

    let mut parts = Vec::with_capacity(range_columns.len());
    for column in &range_columns {
        let Some(value) = values.remove(column) else {
            return Err(crate::ddl::feature_not_supported(format!(
                "TRUNCATE ... PARTITION must name every range partition column; \
                 \"{column}\" is missing"
            )));
        };
        parts.push(format!("{column}={value}"));
    }
    if let Some(unknown) = values.keys().next() {
        return Err(user_error(
            "42703",
            format!("\"{unknown}\" is not a partition column of this table"),
        ));
    }
    let desc = parts.join(",");
    Ok(all
        .iter()
        .filter(|partition| partition.partition_desc == desc)
        .cloned()
        .collect())
}

fn partition_column(expr: &Expr) -> PgWireResult<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|ident| ident.value.to_ascii_lowercase())
            .ok_or_else(|| user_error("42703", "empty partition column".to_string())),
        other => Err(crate::ddl::feature_not_supported(format!(
            "partition column {other} is not supported in TRUNCATE"
        ))),
    }
}

fn partition_value(expr: &Expr) -> PgWireResult<String> {
    let Expr::Value(value) = expr else {
        return Err(crate::ddl::feature_not_supported(
            "partition value must be a literal".to_string(),
        ));
    };
    match &value.value {
        Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
            Ok(value.clone())
        }
        Value::Number(value, _) => Ok(value.clone()),
        Value::Boolean(value) => Ok(value.to_string()),
        Value::Null => Ok("null".to_string()),
        other => Err(crate::ddl::feature_not_supported(format!(
            "partition value {other} is not supported in TRUNCATE"
        ))),
    }
}

async fn execute(
    planned: &PlannedTruncate,
    meta_client: &MetaDataClientRef,
) -> PgWireResult<Response> {
    meta_client
        .commit_truncate(planned.targets.clone())
        .await
        .map_err(|error| match error {
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(message) => {
                user_error(UNDEFINED_TABLE, message)
            }
            lakesoul_metadata::LakeSoulMetaDataError::CommitConflict(message) => {
                user_error("40001", message)
            }
            other => insert_error(datafusion::error::DataFusionError::External(
                Box::new(other),
            )),
        })?;
    Ok(Response::Execution(Tag::new("TRUNCATE TABLE")))
}

/// Position of the first `TRUNCATE` in a multi-statement simple-query message.
///
/// A simple-query batch runs in an implicit transaction. `TRUNCATE` publishes
/// its metadata immediately, so a later failure in the batch could not undo
/// it; the statement is refused while statements before it still run.
pub(crate) fn first_truncate_in_batch(statements: &[String]) -> Option<usize> {
    if statements.len() < 2 {
        return None;
    }
    let parser =
        datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser::new(
        );
    statements.iter().position(|sql| {
        parser.parse(sql).is_ok_and(|parsed| {
            parsed
                .iter()
                .any(|statement| matches!(statement, Statement::Truncate { .. }))
        })
    })
}

/// The failure for a `TRUNCATE` that shares its simple-query message.
pub(crate) fn refuse_truncate_in_batch() -> PgWireError {
    crate::ddl::feature_not_supported(
        "TRUNCATE cannot be combined with other statements in one simple-query \
         message: implicit transaction support for multi-statement batches is not available yet; \
         split the statements or use an explicit BEGIN block"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_with_truncate_is_detected() {
        let statements = vec!["select 1".to_string(), "truncate table t".to_string()];
        assert_eq!(first_truncate_in_batch(&statements), Some(1));
    }

    #[test]
    fn a_single_truncate_is_not_a_batch() {
        assert_eq!(
            first_truncate_in_batch(&["truncate table t".to_string()]),
            None
        );
    }

    #[test]
    fn batch_refusal_is_feature_not_supported() {
        let error = refuse_truncate_in_batch();
        match error {
            PgWireError::UserError(info) => assert_eq!(info.code, "0A000"),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
