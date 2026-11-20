// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `DELETE` for the SQL front end.
//!
//! The statement is claimed at Parse and at Execute. Parse builds the plan for
//! parameter typing and `Describe`; Execute re-resolves the target, checks the
//! table identity, captures a fresh primary snapshot and plans again. A
//! prepared statement therefore cannot write through a table that was dropped
//! and recreated.
//!
//! CDC tables write a tombstone (`change column = 'delete'`) through the
//! ordinary INSERT sink. Every other table rewrites the affected partitions:
//! the survivor query writes the current relation without the matching rows
//! and the partitions are replaced with an `UpdateCommit`. A predicate that
//! only names range-partition columns clears whole partitions through an empty
//! snapshot instead.
//!
//! Rewrite and partition-clear statements are refused inside a transaction
//! block in Phase 1: their publication is not staged yet. A CDC tombstone is a
//! normal append and works in a block, including read-your-writes.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{ParamValues, TableReference};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::dml::DmlStatement;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Delete, FromTable, Statement, TableFactor, TableObject,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{
    FieldInfo, QueryResponse, Response, Tag,
};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::response::TransactionStatus;
use futures::stream;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::planner::delete::{
    DeletePlanError, DeleteStrategy, PlannedDelete, plan_delete,
};
use lakesoul_datafusion::planner::update::SnapshotMode;
use lakesoul_datafusion::write_mode::{
    LakeSoulDeferredCommit, StagedWrite, StagedWriteSink, discard_staged_writes,
};
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClientRef};
use lakesoul_metadata_proto::entity::{DataCommitInfo, FileOp, PartitionInfo};

use crate::insert::{
    current_result_format, insert_error, reject_transaction, reject_transaction_strict,
    request_context, target_reference, user_error,
};
use crate::server::ConnectionSession;

const UNDEFINED_TABLE: &str = "42P01";

pub(crate) struct DeleteStatementHook;

#[async_trait]
impl QueryHook for DeleteStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let delete = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                let meta_client = meta_client(client)?;
                let planned = plan_with(
                    delete,
                    session_context,
                    &database,
                    SnapshotMode::Capture,
                    &meta_client,
                )
                .await?;
                reject_for_strategy(status, session_context, planned.strategy)?;
                let format = current_result_format(client);
                execute(&planned, None, &format, session_context, &meta_client).await
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
        let delete = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                let meta_client = meta_client(client)?;
                // Analysis only: the returned plan supplies parameter types
                // and the Describe columns, but no snapshot is pinned.
                let planned = plan_with(
                    delete,
                    session_context,
                    &database,
                    SnapshotMode::PlanOnly,
                    &meta_client,
                )
                .await?;
                reject_for_strategy(status, session_context, planned.strategy)?;
                Ok(planned.describe)
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
        let delete = extract(statement)?;
        Some(
            async {
                let (status, database) = request_context(client)?;
                let meta_client = meta_client(client)?;
                // Execute re-plans from the live catalog. Compare the prepared
                // statement's target identity first: a recreated table must
                // not be written through the old binding.
                let (table_ref, table_name) =
                    target_reference(&table_object(delete)?, &database)?;
                if let Some(parsed) = plan_identity(logical_plan, &table_ref) {
                    let current =
                        resolved_table_id(&meta_client, &table_ref, &database).await?;
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
                let planned = plan_with(
                    delete,
                    session_context,
                    &database,
                    SnapshotMode::Capture,
                    &meta_client,
                )
                .await?;
                reject_for_strategy(status, session_context, planned.strategy)?;
                let format = current_result_format(client);
                execute(
                    &planned,
                    Some(params),
                    &format,
                    session_context,
                    &meta_client,
                )
                .await
            }
            .await,
        )
    }
}

pub(crate) fn extract(statement: &Statement) -> Option<&Delete> {
    match statement {
        Statement::Delete(delete) => Some(delete),
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

async fn plan_with(
    delete: &Delete,
    session_context: &SessionContext,
    database: &str,
    mode: SnapshotMode,
    meta_client: &MetaDataClientRef,
) -> PgWireResult<PlannedDelete> {
    let (table_ref, table_name) = target_reference(&table_object(delete)?, database)?;
    crate::acl::require_table_write(session_context, database, &table_name).await?;
    let planned = plan_delete(session_context, delete, table_ref, mode)
        .await
        .map_err(plan_error)?;
    if crate::alter::subscription_of(meta_client, &planned.identity.table_id)
        .await?
        .is_some()
    {
        return Err(crate::ddl::feature_not_supported(format!(
            "DELETE is not supported on \"{table_name}\": it is the target of a subscription"
        )));
    }
    if crate::alter::is_ivm_table(&planned.table_info) {
        return Err(crate::ddl::feature_not_supported(format!(
            "DELETE is not supported on \"{table_name}\": IVM owns the table"
        )));
    }
    Ok(planned)
}

fn reject_for_strategy(
    status: TransactionStatus,
    session_context: &SessionContext,
    strategy: DeleteStrategy,
) -> PgWireResult<()> {
    match strategy {
        // A tombstone is an ordinary append and stages into an open block.
        DeleteStrategy::CdcTombstone => {
            reject_transaction(status, session_context, "DELETE")
        }
        // Rewrite/partition-clear publish an UpdateCommit, which the block's
        // staged append path cannot express yet.
        DeleteStrategy::PartitionClear | DeleteStrategy::Rewrite => {
            reject_transaction_strict(status, "DELETE (rewrite)")
        }
    }
}

fn plan_error(error: DeletePlanError) -> PgWireError {
    match error {
        DeletePlanError::DataFusion(error) => PgWireError::ApiError(Box::new(error)),
        other => user_error(other.sqlstate(), other.to_string()),
    }
}

async fn execute(
    planned: &PlannedDelete,
    params: Option<&ParamValues>,
    format: &Format,
    session_context: &SessionContext,
    meta_client: &MetaDataClientRef,
) -> PgWireResult<Response> {
    let count = session_context
        .execute_logical_plan(bind(&planned.count, params)?)
        .await
        .map_err(insert_error)?;
    let count_batches = count.collect().await.map_err(insert_error)?;
    let deleted = count_rows(&count_batches)?;

    let returning = match &planned.returning {
        Some(plan) => {
            let dataframe = session_context
                .execute_logical_plan(bind(plan, params)?)
                .await
                .map_err(insert_error)?;
            let fields = Arc::new(
                datafusion_postgres::arrow_pg::datatypes::arrow_schema_to_pg_fields(
                    dataframe.schema().as_arrow(),
                    format,
                    None,
                )?,
            );
            let batches = dataframe.collect().await.map_err(insert_error)?;
            Some((fields, batches))
        }
        None => None,
    };

    if deleted == 0 {
        return finish(deleted, returning);
    }

    match planned.strategy {
        DeleteStrategy::CdcTombstone => {
            let write = planned.write.as_ref().ok_or_else(|| {
                user_error("XX000", "DELETE has no write plan".to_string())
            })?;
            let dataframe = session_context
                .execute_logical_plan(bind(write, params)?)
                .await
                .map_err(insert_error)?;
            dataframe.collect().await.map_err(insert_error)?;
        }
        DeleteStrategy::Rewrite => {
            let writes = execute_rewrite(planned, params, session_context).await?;
            publish_rewrite(planned, writes, session_context, meta_client).await?;
        }
        DeleteStrategy::PartitionClear => {
            let expected = planned
                .snapshot
                .as_ref()
                .map(|partitions| partitions.as_slice())
                .unwrap_or(&[]);
            meta_client
                .commit_replace(
                    &planned.table_info,
                    expected,
                    Vec::new(),
                    planned.affected.clone(),
                )
                .await
                .map_err(commit_error)?;
        }
    }

    finish(deleted, returning)
}

fn bind(plan: &LogicalPlan, params: Option<&ParamValues>) -> PgWireResult<LogicalPlan> {
    match params {
        Some(params) => plan
            .clone()
            .replace_params_with_values(params)
            .map_err(|error| PgWireError::ApiError(Box::new(error))),
        None => Ok(plan.clone()),
    }
}

fn finish(
    deleted: usize,
    returning: Option<(Arc<Vec<FieldInfo>>, Vec<RecordBatch>)>,
) -> PgWireResult<Response> {
    match returning {
        Some((fields, batches)) => {
            let rows = batches
                .into_iter()
                .flat_map(|batch| {
                    datafusion_postgres::arrow_pg::encode_recordbatch(
                        Arc::clone(&fields),
                        batch,
                    )
                    .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let mut response = QueryResponse::new(fields, stream::iter(rows));
            response.set_command_tag(&format!("DELETE {deleted}"));
            Ok(Response::Query(response))
        }
        None => Ok(Response::Execution(Tag::new("DELETE").with_rows(deleted))),
    }
}

fn count_rows(batches: &[RecordBatch]) -> PgWireResult<usize> {
    let mut total = 0usize;
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }
        let column = batch.column(0);
        if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            for index in 0..values.len() {
                if !values.is_null(index) {
                    total += values.value(index) as usize;
                }
            }
        } else if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
            for index in 0..values.len() {
                if !values.is_null(index) {
                    total += values.value(index) as usize;
                }
            }
        } else {
            return Err(user_error(
                "XX000",
                "DELETE count has an unexpected type".to_string(),
            ));
        }
    }
    Ok(total)
}

#[derive(Default)]
struct CollectingSink {
    writes: parking_lot::Mutex<Vec<StagedWrite>>,
}

impl CollectingSink {
    fn take(&self) -> Vec<StagedWrite> {
        std::mem::take(&mut *self.writes.lock())
    }
}

impl StagedWriteSink for CollectingSink {
    fn stage(&self, write: StagedWrite) -> datafusion::error::Result<()> {
        self.writes.lock().push(write);
        Ok(())
    }

    fn staged_commits(&self, _table_id: &str) -> Vec<DataCommitInfo> {
        // The rewrite's SELECT must read the published relation, not the files
        // it is producing.
        Vec::new()
    }

    fn has_staged_writes(&self) -> bool {
        false
    }
}

async fn execute_rewrite(
    planned: &PlannedDelete,
    params: Option<&ParamValues>,
    session_context: &SessionContext,
) -> PgWireResult<Vec<StagedWrite>> {
    let handle = session_context
        .state()
        .config()
        .get_extension::<LakeSoulDeferredCommit>()
        .ok_or_else(|| {
            user_error(
                "XX000",
                "the session has no deferred-write handle".to_string(),
            )
        })?;
    let previous = handle.get();
    let sink = Arc::new(CollectingSink::default());
    handle.set(sink.clone() as Arc<dyn StagedWriteSink>);

    let result: PgWireResult<()> = async {
        let write = planned
            .write
            .as_ref()
            .ok_or_else(|| user_error("XX000", "DELETE has no write plan".to_string()))?;
        let dataframe = session_context
            .execute_logical_plan(bind(write, params)?)
            .await
            .map_err(insert_error)?;
        dataframe.collect().await.map_err(insert_error)?;
        Ok(())
    }
    .await;

    handle.clear();
    if let Some(previous) = previous {
        handle.set(previous);
    }

    if let Err(error) = result {
        let writes = sink.take();
        if !writes.is_empty() {
            let _ = discard_staged_writes(session_context, &writes).await;
        }
        return Err(error);
    }
    Ok(sink.take())
}

async fn publish_rewrite(
    planned: &PlannedDelete,
    writes: Vec<StagedWrite>,
    session_context: &SessionContext,
    meta_client: &MetaDataClientRef,
) -> PgWireResult<()> {
    let files = staged_files(&writes);
    let present = files
        .iter()
        .map(|file| file.partition_desc.clone())
        .collect::<HashSet<_>>();
    let empty: Vec<PartitionInfo> = planned
        .affected
        .iter()
        .filter(|partition| !present.contains(&partition.partition_desc))
        .cloned()
        .collect();
    let expected = planned
        .snapshot
        .as_ref()
        .map(|partitions| partitions.as_slice())
        .unwrap_or(&[]);
    match meta_client
        .commit_replace(&planned.table_info, expected, files, empty)
        .await
    {
        Ok(_) => {
            // The rewrite wrote the whole affected partitions, so their new
            // files are not covered by the old index shards. Rebuild them when
            // the table declares vector/text indexes; a failure only leaves
            // the search path on its exact fallback, never the data.
            rebuild_indexes_after_delete(meta_client, &planned.table_info).await;
            Ok(())
        }
        Err(error) => {
            if !matches!(error, LakeSoulMetaDataError::InsertCommitOutcomeUnknown(_)) {
                let _ = discard_staged_writes(session_context, &writes).await;
            }
            Err(commit_error(error))
        }
    }
}

/// Rebuild the table's pending vector/text index shards after a rewrite.
///
/// The native builder's future is not `Send`, so it is driven on a blocking
/// thread with its own current-thread runtime, mirroring the writer's inline
/// index maintenance. A failure is logged: the data is already published and
/// the search path falls back to the exact scan.
async fn rebuild_indexes_after_delete(
    meta_client: &MetaDataClientRef,
    table_info: &lakesoul_metadata_proto::entity::TableInfo,
) {
    let client = Arc::clone(meta_client);
    let table_name = table_info.table_name.clone();
    let namespace = table_info.table_namespace.clone();
    let result = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                datafusion::error::DataFusionError::Execution(error.to_string())
            })?;
        runtime.block_on(
            lakesoul_datafusion::index_maintenance::build_pending_indices(
                client,
                &table_name,
                &namespace,
            ),
        )
    })
    .await;
    match result {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => tracing::warn!(
            table = %table_info.table_name,
            %error,
            "building indexes after DELETE failed; search will use the exact path"
        ),
        Err(error) => tracing::warn!(
            table = %table_info.table_name,
            %error,
            "index task after DELETE failed; search will use the exact path"
        ),
    }
}

fn staged_files(writes: &[StagedWrite]) -> Vec<DataFileInfo> {
    let mut files = Vec::new();
    for write in writes {
        for commit in &write.data_commits {
            for op in &commit.file_ops {
                if op.file_op() == FileOp::Add {
                    files.push(DataFileInfo {
                        partition_desc: commit.partition_desc.clone(),
                        path: op.path.clone(),
                        file_op: "add".to_string(),
                        size: op.size,
                        bucket_id: None,
                        modification_time: commit.timestamp,
                        file_exist_cols: op.file_exist_cols.clone(),
                    });
                }
            }
        }
    }
    files
}

fn commit_error(error: LakeSoulMetaDataError) -> PgWireError {
    match &error {
        LakeSoulMetaDataError::CommitConflict(_)
        | LakeSoulMetaDataError::InsertConflict { .. } => {
            user_error("40001", error.to_string())
        }
        LakeSoulMetaDataError::NotFound(_) => {
            user_error(UNDEFINED_TABLE, error.to_string())
        }
        _ => insert_error(datafusion::error::DataFusionError::External(Box::new(
            error,
        ))),
    }
}

/// The `table_id` the target name resolves to right now, from the primary.
///
/// The session catalog can still hold the provider a prepared statement was
/// planned with; the metadata store is the authority for "was this name
/// dropped and recreated?".
async fn resolved_table_id(
    meta_client: &MetaDataClientRef,
    table_ref: &TableReference,
    database: &str,
) -> PgWireResult<Option<String>> {
    meta_client
        .primary_table_id_by_name(table_ref.table(), database)
        .await
        .map_err(|error| {
            insert_error(datafusion::error::DataFusionError::External(Box::new(
                error,
            )))
        })
}

/// The table identity a Parse-time plan was built against.
fn plan_identity(plan: &LogicalPlan, target: &TableReference) -> Option<String> {
    if let LogicalPlan::Dml(DmlStatement { target: source, .. }) = plan {
        let provider = source_as_provider(source).ok()?;
        let provider: &dyn std::any::Any = provider.as_ref();
        return provider
            .downcast_ref::<LakeSoulTableProvider>()
            .map(|provider| provider.table_info().table_id.clone());
    }
    find_scan_identity(plan, target)
}

/// Find the target table scan inside a returning projection.
fn find_scan_identity(plan: &LogicalPlan, target: &TableReference) -> Option<String> {
    if let LogicalPlan::TableScan(scan) = plan
        && scan.table_name.table() == target.table()
        && (scan.table_name.schema().is_none()
            || target.schema().is_none()
            || scan.table_name.schema() == target.schema())
    {
        let provider = source_as_provider(&scan.source).ok()?;
        let provider: &dyn std::any::Any = provider.as_ref();
        if let Some(provider) = provider.downcast_ref::<LakeSoulTableProvider>() {
            return Some(provider.table_info().table_id.clone());
        }
    }
    for input in plan.inputs() {
        if let Some(identity) = find_scan_identity(input, target) {
            return Some(identity);
        }
    }
    None
}

fn table_object(delete: &Delete) -> PgWireResult<TableObject> {
    let from = match &delete.from {
        FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
    };
    let [table] = from.as_slice() else {
        return Err(user_error(
            "0A000",
            "DELETE of more than one FROM item is not supported".to_string(),
        ));
    };
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(TableObject::TableName(name.clone())),
        _ => Err(user_error(
            "0A000",
            "DELETE of a table function or subquery is not supported".to_string(),
        )),
    }
}

/// Position of the first `DELETE` in a multi-statement simple-query message.
pub(crate) fn first_delete_in_batch(statements: &[String]) -> Option<usize> {
    if statements.len() < 2 {
        return None;
    }
    let parser = PostgresCompatibilityParser::new();
    statements.iter().position(|sql| {
        parser.parse(sql).is_ok_and(|parsed| {
            parsed
                .iter()
                .any(|statement| matches!(statement, Statement::Delete(_)))
        })
    })
}

/// The failure for a `DELETE` that shares its simple-query message.
pub(crate) fn refuse_delete_in_batch() -> PgWireError {
    crate::ddl::feature_not_supported(
        "DELETE cannot be combined with other statements in one simple-query \
         message: PostgreSQL would run the batch in an implicit transaction, \
         and LakeSoul publishes each DELETE immediately"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_with_delete_is_detected() {
        let statements = vec![
            "select 1".to_string(),
            "delete from t where id = 1".to_string(),
        ];
        assert_eq!(first_delete_in_batch(&statements), Some(1));
    }

    #[test]
    fn a_single_delete_is_not_a_batch() {
        assert_eq!(first_delete_in_batch(&["delete from t".to_string()]), None);
    }
}
