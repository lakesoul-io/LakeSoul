// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL transaction blocks for the LakeSoul front end.
//!
//! `BEGIN` opens a connection-local [`TransactionState`]. Statements inside the
//! block run as they do outside it, except that a write stages its files in the
//! block instead of publishing them: the writer hands them to the block's sink,
//! nothing becomes visible, and `COMMIT` publishes every staged write in one
//! metadata transaction. Table DDL is staged the same way, with a
//! connection-local catalog overlay for later statements. `ROLLBACK` (and any
//! ending that publishes nothing) deletes the staged files.
//!
//! This hook replaces the upstream `TransactionStatementHook`, which only flips
//! pgwire's transaction status. The differences are the ones PostgreSQL clients
//! observe:
//!
//! * a failing `COMMIT` ends the block - the connection reports `I`, not `E`,
//!   so the next statement runs instead of failing with `25P02`;
//! * every portal is destroyed when the block ends, so a named portal created
//!   inside the block cannot execute its plan afterwards;
//! * savepoints snapshot the block's staged state; `ROLLBACK TO SAVEPOINT`
//!   truncates it and recovers a failed block. `COMMIT`/`ROLLBACK AND CHAIN`
//!   and `DISCARD ALL` inside a block are still refused with PostgreSQL's
//!   SQLSTATEs instead of reaching DataFusion;
//! * the block's statements read their own staged writes: a scan appends them
//!   to the published snapshot as its newest versions, so a later statement
//!   sees an earlier one's rows and validates its keys against them.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::common::{DFSchema, ParamValues};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{DiscardObject, Statement};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_datafusion::catalog::LakeSoulProviderOptions;
use lakesoul_datafusion::write_mode::{
    LakeSoulDeferredCommit, StagedWrite, StagedWriteSink, WriteViolation,
};
use lakesoul_metadata::{
    LakeSoulMetaDataError, MetaDataClientRef, TransactionTableCommit,
};
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;
use tracing::{debug, warn};

use crate::ddl_transaction::{DdlChanges, PendingAcl, PendingCatalog};
use crate::server::ConnectionSession;
use crate::session::PgSession;

/// SQLSTATE for statements this front end does not implement.
const FEATURE_NOT_SUPPORTED: &str = "0A000";
/// `active_sql_transaction`, what PostgreSQL reports for `DISCARD ALL` inside a
/// transaction block.
const ACTIVE_SQL_TRANSACTION: &str = "25001";
/// `serialization_failure`: the block lost a race and must be retried.
const SERIALIZATION_FAILURE: &str = "40001";
/// `internal_error`: the block's outcome is unknown.
const INTERNAL_ERROR: &str = "XX000";
/// `no_active_sql_transaction`: a savepoint command outside a block.
const NO_ACTIVE_SQL_TRANSACTION: &str = "25P01";
/// `invalid_savepoint_specification`: the named savepoint does not exist.
const INVALID_SAVEPOINT_SPECIFICATION: &str = "3B001";

/// Isolation a block runs under.
///
/// Only `READ COMMITTED` exists: `ReadCommittedOnlyGuard` runs in front of this
/// hook and refuses every stronger level, so the state records what the block
/// actually gets, not what the client asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Isolation {
    ReadCommitted,
}

/// One open transaction block on a connection.
#[derive(Debug)]
pub(crate) struct TransactionState {
    /// The isolation the block runs under.
    pub isolation: Isolation,
    /// When `BEGIN` opened the block.
    pub started: Instant,
    /// Writes waiting for `COMMIT`, in statement order.
    pub pending: Vec<StagedWrite>,
    /// Table DDL waiting for `COMMIT`.
    pub ddl: DdlChanges,
    /// The block's session-local catalog view.
    pub catalog: PendingCatalog,
    /// Permissions for tables the block created but has not published.
    pub pending_acl: PendingAcl,
    /// Savepoints in creation order; duplicate names resolve to the latest.
    pub savepoints: Vec<Savepoint>,
    /// Files the block holds, for its limits.
    pub staged_files: usize,
    /// Bytes the block holds, for its limits.
    pub staged_bytes: u64,
}

/// One `SAVEPOINT` marker.
#[derive(Debug, Clone)]
pub(crate) struct Savepoint {
    pub name: String,
    /// Length of `TransactionState::pending` at the savepoint.
    pub pending_len: usize,
    pub ddl: DdlChanges,
    pub catalog: PendingCatalog,
    pub pending_acl: PendingAcl,
    pub staged_files: usize,
    pub staged_bytes: u64,
}

impl TransactionState {
    fn new(isolation: Isolation) -> Self {
        Self {
            isolation,
            started: Instant::now(),
            pending: Vec::new(),
            ddl: DdlChanges::default(),
            catalog: PendingCatalog::default(),
            pending_acl: PendingAcl::default(),
            savepoints: Vec::new(),
            staged_files: 0,
            staged_bytes: 0,
        }
    }

    fn savepoint(&self, name: String) -> Savepoint {
        Savepoint {
            name,
            pending_len: self.pending.len(),
            ddl: self.ddl.clone(),
            catalog: self.catalog.clone(),
            pending_acl: self.pending_acl.clone(),
            staged_files: self.staged_files,
            staged_bytes: self.staged_bytes,
        }
    }

    /// Restore the state captured by `savepoint`; returns the writes that were
    /// staged after it and must be deleted.
    fn rollback_to(&mut self, index: usize) -> Vec<StagedWrite> {
        let savepoint = self.savepoints[index].clone();
        let discarded = self.pending.split_off(savepoint.pending_len);
        self.ddl = savepoint.ddl;
        self.catalog = savepoint.catalog;
        self.pending_acl = savepoint.pending_acl;
        self.staged_files = savepoint.staged_files;
        self.staged_bytes = savepoint.staged_bytes;
        self.savepoints.truncate(index + 1);
        discarded
    }
}

/// The connection's block, shared with the write path.
///
/// It lives on [`ConnectionSession`] so every hook and the deferred-commit sink
/// see the same state; nothing here holds the session itself, which would be an
/// `Arc` cycle through its `SessionContext`.
pub(crate) type SharedTransaction = Arc<parking_lot::Mutex<Option<TransactionState>>>;

pub(crate) fn shared() -> SharedTransaction {
    Arc::new(parking_lot::Mutex::new(None))
}

/// Whether the connection is really inside a block.
///
/// The block state and the client's transaction status are written at
/// different moments: pgwire reports a `BEGIN` (and updates the status) only
/// after its response is sent. A send interrupted by a cancel or a deadline
/// therefore leaves a block the client never saw - `ReadyForQuery` says Idle -
/// and nothing may be staged into it. Such a block is not usable: the gates
/// that read this ask about both, and the next `BEGIN` replaces it.
pub(crate) fn is_open(session: &ConnectionSession, status: TransactionStatus) -> bool {
    status != TransactionStatus::Idle && session.transaction.lock().is_some()
}

/// Collects one block's staged writes on its connection.
struct BlockSink {
    transaction: SharedTransaction,
    limits: crate::limits::Limits,
}

impl StagedWriteSink for BlockSink {
    fn stage(&self, write: StagedWrite) -> DataFusionResult<()> {
        let mut block = self.transaction.lock();
        let Some(state) = block.as_mut() else {
            return Err(DataFusionError::Execution(
                "the transaction block ended while the statement was writing".to_string(),
            ));
        };
        let files = write
            .data_commits
            .iter()
            .map(|commit| commit.file_ops.len())
            .sum::<usize>();
        let bytes = write
            .data_commits
            .iter()
            .flat_map(|commit| commit.file_ops.iter())
            .map(|file| file.size.max(0) as u64)
            .sum::<u64>();
        // The block holds unreferenced files until `COMMIT`: bound both count
        // and size, so an open block cannot fill the warehouse.
        let over_files = self.limits.max_staged_files_per_transaction > 0
            && state.staged_files + files > self.limits.max_staged_files_per_transaction;
        let over_bytes = self.limits.max_staged_bytes_per_transaction > 0
            && state.staged_bytes + bytes
                > self.limits.max_staged_bytes_per_transaction as u64;
        if over_files || over_bytes {
            let (what, limit) = if over_files {
                ("files", self.limits.max_staged_files_per_transaction as u64)
            } else {
                ("bytes", self.limits.max_staged_bytes_per_transaction as u64)
            };
            metrics::counter!(
                crate::limits::STAGED_LIMIT_REJECTED_TOTAL,
                "resource" => what.to_string()
            )
            .increment(1);
            return Err(DataFusionError::External(Box::new(
                WriteViolation::StagedLimit {
                    what: what.to_string(),
                    limit,
                },
            )));
        }
        state.staged_files += files;
        state.staged_bytes += bytes;
        state.pending.push(write);
        Ok(())
    }

    fn has_staged_writes(&self) -> bool {
        let block = self.transaction.lock();
        block
            .as_ref()
            .is_some_and(|state| !state.pending.is_empty())
    }

    /// The block's staged commits for one table: a scan inside the block reads
    /// them as the newest versions, so a later statement sees what an earlier
    /// one wrote (and validates its keys against it).
    fn staged_commits(
        &self,
        table_id: &str,
    ) -> Vec<lakesoul_metadata_proto::entity::DataCommitInfo> {
        let block = self.transaction.lock();
        let Some(state) = block.as_ref() else {
            return Vec::new();
        };
        state
            .pending
            .iter()
            .filter(|write| write.table_info.table_id == table_id)
            .flat_map(|write| write.data_commits.iter().cloned())
            .collect()
    }
}

/// The session's deferred-commit handle, if this session has one.
fn deferred_commit(session: &ConnectionSession) -> Option<Arc<LakeSoulDeferredCommit>> {
    session
        .session
        .context
        .state()
        .config()
        .get_extension::<LakeSoulDeferredCommit>()
}

/// Routes the block's writes into its own state.
fn install_sink(session: &ConnectionSession) {
    if let Some(handle) = deferred_commit(session) {
        handle.set(Arc::new(BlockSink {
            transaction: Arc::clone(&session.transaction),
            limits: session.limits,
        }));
    }
}

fn clear_sink(session: &ConnectionSession) {
    if let Some(handle) = deferred_commit(session) {
        handle.clear();
    }
}

/// Owns the files a block staged until the block's publication outcome is
/// known.
///
/// A `COMMIT` can be cancelled or time out while it waits for the metadata
/// transaction, which drops the statement's future; the block's state is gone
/// by then, so this is the last owner. The destructor deletes the files unless
/// the caller declared them published (or possibly published).
pub(crate) struct StagedFiles {
    context: SessionContext,
    writes: Option<Vec<StagedWrite>>,
}

impl StagedFiles {
    pub(crate) fn new(context: &SessionContext, writes: Vec<StagedWrite>) -> Self {
        Self {
            context: context.clone(),
            writes: Some(writes),
        }
    }

    /// Disarms the destructor and returns what the files are.
    ///
    /// Called before the publication decides their fate, so an unwind inside
    /// the publication keeps them (GC collects what was never published)
    /// instead of deleting files a committed metadata transaction references.
    /// The caller also needs the writes afterwards: index maintenance runs on
    /// what they describe, after the files stopped being ours to delete.
    pub(crate) fn into_parts(mut self) -> (SessionContext, Vec<StagedWrite>) {
        (self.context.clone(), self.writes.take().unwrap_or_default())
    }

    /// The files are unreferenced: delete them now, and keep the destructor as
    /// the fallback if this future is dropped while the deletion runs.
    async fn discard(mut self) {
        if let Some(writes) = &self.writes
            && !writes.is_empty()
        {
            Self::delete(&self.context, writes).await;
        }
        self.writes = None;
    }

    async fn delete(context: &SessionContext, writes: &[StagedWrite]) {
        if let Err(error) =
            lakesoul_datafusion::write_mode::discard_staged_writes(context, writes).await
        {
            // Unreferenced files are recoverable; a deleted committed file is
            // not, so a failure here is reported and left to the GC.
            warn!(
                %error,
                "could not delete the transaction's staged files; GC will collect them"
            );
        }
    }
}

impl Drop for StagedFiles {
    fn drop(&mut self) {
        let Some(writes) = self.writes.take() else {
            return;
        };
        if writes.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // No runtime to delete on: the files are unreferenced and GC is
            // the backstop.
            return;
        };
        let context = self.context.clone();
        runtime.spawn(async move { StagedFiles::delete(&context, &writes).await });
    }
}

/// Deletes the files of writes whose block ended without deciding their fate.
///
/// The last owner: a connection closed with an open block, or a block the
/// client never learned about. A failure is logged; GC collects the rest.
pub(crate) async fn discard_staged_files(
    context: &datafusion::prelude::SessionContext,
    writes: Vec<StagedWrite>,
) {
    StagedFiles::delete(context, &writes).await;
}

/// Ends a block the client never learned about, deleting what it staged.
///
/// `BEGIN` installs the block before pgwire reports the transaction, so a send
/// interrupted by a cancel or a deadline leaves a block that `ReadyForQuery`
/// says is not there. The block and its sink are undone at the next statement
/// boundary, before the statement could stage into them.
pub(crate) async fn reconcile(session: &ConnectionSession, status: TransactionStatus) {
    if status != TransactionStatus::Idle {
        return;
    }
    let state = session.transaction.lock().take();
    let Some(state) = state else {
        return;
    };
    warn!("dropping a transaction block the client never learned about");
    clear_sink(session);
    if !state.ddl.is_empty() {
        session.settings.bump();
    }
    StagedFiles::new(&session.session.context, state.pending)
        .discard()
        .await;
}

/// PostgreSQL transaction statements, answered here instead of upstream.
pub(crate) struct LakeSoulTransactionHook;

impl LakeSoulTransactionHook {
    /// The isolation a block gets: the guard in front of this hook already
    /// refused everything stronger than READ COMMITTED.
    const ISOLATION: Isolation = Isolation::ReadCommitted;

    /// Destroys the block's portals, as PostgreSQL does at a transaction end.
    ///
    /// `pgwire` holds its own reference to the portal of the statement that is
    /// executing, and
    /// [`QueryCancellation::forget_all_portals`] drops that portal's
    /// bookkeeping without ending the statement, which reports its own outcome
    /// once its response is sent.
    ///
    /// [`QueryCancellation::forget_all_portals`]: crate::cancel::QueryCancellation::forget_all_portals
    fn close_portals(session: &ConnectionSession, client: &mut dyn HookClient) {
        client.portal_store().clear_portals();
        session.cancellation.forget_all_portals();
        // Non-holdable cursors do not survive the transaction that declared
        // them. `WITH HOLD` is refused, so the whole store goes.
        crate::cursor::clear(client);
    }

    /// Publishes every staged write and table DDL in one metadata transaction.
    async fn publish(
        writes: &[StagedWrite],
        ddl: &DdlChanges,
        client: &MetaDataClientRef,
    ) -> Result<(Vec<StagedWrite>, Vec<StagedWrite>), LakeSoulMetaDataError> {
        // Writes against a table the block dropped (or created and dropped
        // again) are never published; their files are deleted by the caller.
        let mut published_writes = Vec::new();
        let mut discarded_writes = Vec::new();
        for write in writes {
            if ddl.is_dropped_id(&write.table_info.table_id) {
                discarded_writes.push(write.clone());
            } else {
                published_writes.push(write.clone());
            }
        }
        if published_writes.is_empty() && ddl.is_empty() {
            return Ok((published_writes, discarded_writes));
        }
        // One entry per table, with its staged commits in statement order: the
        // metadata commit folds them into one partition version each.
        //
        // The expectation is per touched partition, not a union of lists: each
        // statement validated the partitions *it* wrote against the versions it
        // saw (a partition it wrote but did not list did not exist yet, i.e.
        // version -1). Two statements that saw the same partition at different
        // versions cannot both be right - a peer committed in between - so the
        // block gives up with a serialization failure instead of publishing one
        // statement's rows over the peer's. A table with any append-only write
        // is never compared: its files are disjoint from every writer's.
        struct TablePlan {
            table_info: lakesoul_metadata_proto::entity::TableInfo,
            data_commits: Vec<lakesoul_metadata_proto::entity::DataCommitInfo>,
            /// `partition_desc -> version every statement saw`, `None` when the
            /// table is append-only.
            expected: Option<std::collections::HashMap<String, i32>>,
            /// One descriptor per expected partition, for the commit's CAS.
            expected_info: std::collections::HashMap<
                String,
                lakesoul_metadata_proto::entity::PartitionInfo,
            >,
        }

        let mut plans: Vec<TablePlan> = Vec::new();
        for write in &published_writes {
            let effective = ddl
                .creates
                .get(&write.table_info.table_name)
                .filter(|create| create.table_info.table_id == write.table_info.table_id)
                .map(|create| create.table_info.clone())
                .or_else(|| {
                    ddl.alters
                        .get(&write.table_info.table_name)
                        .filter(|alter| {
                            alter.final_info.table_id == write.table_info.table_id
                        })
                        .map(|alter| alter.final_info.clone())
                })
                .unwrap_or_else(|| (*write.table_info).clone());
            let index = match plans
                .iter()
                .position(|plan| plan.table_info.table_id == write.table_info.table_id)
            {
                Some(index) => index,
                None => {
                    plans.push(TablePlan {
                        table_info: effective,
                        data_commits: Vec::new(),
                        expected: write.expected.as_ref().map(|_| Default::default()),
                        expected_info: Default::default(),
                    });
                    plans.len() - 1
                }
            };
            let plan = &mut plans[index];
            plan.data_commits.extend(write.data_commits.iter().cloned());
            let Some(expected) = plan.expected.as_mut() else {
                continue;
            };
            let Some(seen) = &write.expected else {
                // A table cannot be both append-only and compared.
                plan.expected = None;
                continue;
            };
            let captured = seen
                .iter()
                .map(|partition| (partition.partition_desc.as_str(), partition))
                .collect::<std::collections::HashMap<_, _>>();
            for commit in &write.data_commits {
                let version = captured
                    .get(commit.partition_desc.as_str())
                    .map_or(-1, |partition| partition.version);
                match expected.get(&commit.partition_desc) {
                    Some(known) if *known != version => {
                        return Err(LakeSoulMetaDataError::InsertConflict {
                            table_id: write.table_info.table_id.clone(),
                            partition_desc: commit.partition_desc.clone(),
                            expected_version: *known,
                            actual_version: version,
                        });
                    }
                    Some(_) => {}
                    None => {
                        expected.insert(commit.partition_desc.clone(), version);
                        let descriptor =
                            captured.get(commit.partition_desc.as_str()).map_or_else(
                                || lakesoul_metadata_proto::entity::PartitionInfo {
                                    table_id: write.table_info.table_id.clone(),
                                    partition_desc: commit.partition_desc.clone(),
                                    version: -1,
                                    ..Default::default()
                                },
                                |partition| (*partition).clone(),
                            );
                        plan.expected_info
                            .insert(commit.partition_desc.clone(), descriptor);
                    }
                }
            }
        }
        let tables = plans
            .into_iter()
            .map(|plan| TransactionTableCommit {
                table_info: plan.table_info,
                data_commits: plan.data_commits,
                expected: plan
                    .expected
                    .map(|_| plan.expected_info.into_values().collect()),
            })
            .collect();
        client
            .commit_metadata_transaction(ddl.metadata_changeset(tables))
            .await?;
        Ok((published_writes, discarded_writes))
    }

    /// Publishes the block without letting the statement's cancellation decide
    /// the files' fate.
    ///
    /// The hook runs inside `with_cancellation`, which drops this future on a
    /// cancel request or a statement timeout. The publication is a metadata
    /// round trip that cannot be aborted half-way: if the future were dropped
    /// while it runs, the files' owner would delete files the metadata
    /// transaction may have just published. The task therefore runs to
    /// completion and is the single place that keeps the files (published) or
    /// deletes them (failed), returning the writes when it kept them.
    fn publish_detached(
        staged: StagedFiles,
        ddl: DdlChanges,
        client: MetaDataClientRef,
        session: Arc<PgSession>,
    ) -> tokio::task::JoinHandle<Result<Vec<StagedWrite>, LakeSoulMetaDataError>> {
        let (context, writes) = staged.into_parts();
        tokio::spawn(async move {
            match Self::publish(&writes, &ddl, &client).await {
                Ok((published, discarded)) => {
                    if !discarded.is_empty() {
                        StagedFiles::delete(&context, &discarded).await;
                    }
                    if !ddl.is_empty() {
                        session.refresh_after_ddl(&ddl).await;
                    }
                    Ok(published)
                }
                Err(error) => {
                    // Only a definitive failure deletes the files: an unknown
                    // outcome may be visible, and an unwind out of the
                    // publication (a panic) keeps them for GC as well.
                    if !matches!(
                        error,
                        LakeSoulMetaDataError::InsertCommitOutcomeUnknown(_)
                    ) {
                        StagedFiles::delete(&context, &writes).await;
                    }
                    Err(error)
                }
            }
        })
    }

    /// Builds the indexes of every table the block published.
    ///
    /// The block is committed at this point, and its writes never ran the
    /// inline maintenance of the write path, so it runs now on the published
    /// file set. A failure here is logged and never turns a successful
    /// `COMMIT` into an error: the data is published and readable, only the
    /// acceleration structures are missing (a later rebuild recreates them).
    async fn maintain_indexes(
        writes: &[StagedWrite],
        client: &MetaDataClientRef,
        context: &SessionContext,
    ) {
        /// Everything one table's maintenance needs, gathered over the
        /// block's statements.
        struct Target {
            name: String,
            primary_keys: Vec<String>,
            files: std::collections::HashMap<String, (Vec<String>, u64)>,
        }

        let mut targets: Vec<(String, Target)> = Vec::new();
        for write in writes {
            let index = match targets
                .iter()
                .position(|(table_id, _)| *table_id == write.table_info.table_id)
            {
                Some(index) => index,
                None => {
                    let (_, primary_keys) =
                        lakesoul_metadata::transfusion::parse_table_info_partitions(
                            &write.table_info.partitions,
                        );
                    targets.push((
                        write.table_info.table_id.clone(),
                        Target {
                            name: format!(
                                "{}.{}",
                                write.table_info.table_namespace,
                                write.table_info.table_name
                            ),
                            primary_keys,
                            files: Default::default(),
                        },
                    ));
                    targets.len() - 1
                }
            };
            let target = &mut targets[index].1;
            // Several statements may now write one partition: the index has to
            // cover every file they published, not just the last statement's.
            for commit in &write.data_commits {
                let paths = commit
                    .file_ops
                    .iter()
                    .map(|op| op.path.clone())
                    .collect::<Vec<_>>();
                match target.files.get_mut(&commit.partition_desc) {
                    Some((staged_paths, _)) => staged_paths.extend(paths),
                    None => {
                        target
                            .files
                            .insert(commit.partition_desc.clone(), (paths, 0));
                    }
                }
            }
            for (partition_desc, rows) in &write.rows {
                if let Some((_, staged_rows)) = target.files.get_mut(partition_desc) {
                    *staged_rows += *rows;
                }
            }
        }
        if targets.is_empty() {
            return;
        }
        let object_store_options =
            LakeSoulProviderOptions::from_session(&context.state()).object_store_options;
        for (_, target) in targets {
            if let Err(error) =
                lakesoul_datafusion::datasource::file_format::maintain_indexes(
                    client,
                    &target.name,
                    &target.primary_keys,
                    &object_store_options,
                    &target.files,
                )
                .await
            {
                warn!(
                    table = %target.name,
                    %error,
                    "index maintenance after COMMIT failed; the block is still committed"
                );
            }
        }
    }

    fn log_end(state: &TransactionState, outcome: &str) {
        debug!(
            isolation = ?state.isolation,
            elapsed_ms = state.started.elapsed().as_millis() as u64,
            staged_files = state.staged_files,
            staged_bytes = state.staged_bytes,
            outcome,
            "transaction block ended"
        );
    }

    /// The response for a transaction-family statement, `None` for anything
    /// this hook does not answer.
    async fn respond(
        statement: &Statement,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Option<Response>> {
        let Some(session) = client.session_extensions().get::<ConnectionSession>() else {
            // Startup failed or a test drives the hooks without a session:
            // leaving the statement to the other hooks would turn a
            // transaction command into a planner error, so say so.
            warn!(
                statement = %statement,
                "transaction statement without a connection session"
            );
            return Ok(None);
        };
        match statement {
            Statement::StartTransaction { .. } => {
                let status = client.transaction_status();
                let stale = {
                    let mut block = session.transaction.lock();
                    if status != TransactionStatus::Idle && block.is_some() {
                        // PostgreSQL ignores a nested BEGIN with a warning.
                        warn!("BEGIN command ignored: already in transaction block");
                        return Ok(Some(Response::Execution(Tag::new("BEGIN"))));
                    }
                    // A block the client never saw - its `BEGIN` response was
                    // interrupted before pgwire reported the transaction - is
                    // replaced, not reported as nested: this BEGIN opens the
                    // transaction the client is asking for. Whatever it staged
                    // was never acknowledged, so it is deleted.
                    block.take()
                };
                if let Some(stale) = stale {
                    warn!("replacing a transaction block the client never learned about");
                    clear_sink(&session);
                    if !stale.ddl.is_empty() {
                        session.settings.bump();
                    }
                    StagedFiles::new(&session.session.context, stale.pending)
                        .discard()
                        .await;
                }
                *session.transaction.lock() =
                    Some(TransactionState::new(Self::ISOLATION));
                install_sink(&session);
                Ok(Some(Response::TransactionStart(Tag::new("BEGIN"))))
            }
            Statement::Commit { chain: true, .. } => Err(unsupported("COMMIT AND CHAIN")),
            Statement::Rollback { chain: true, .. } => {
                Err(unsupported("ROLLBACK AND CHAIN"))
            }
            Statement::Savepoint { name } => {
                let name = savepoint_name(name);
                let mut block = session.transaction.lock();
                let Some(state) = block.as_mut() else {
                    return Err(no_active_transaction("SAVEPOINT"));
                };
                let savepoint = state.savepoint(name);
                state.savepoints.push(savepoint);
                Ok(Some(Response::Execution(Tag::new("SAVEPOINT"))))
            }
            Statement::ReleaseSavepoint { name } => {
                let name = savepoint_name(name);
                let mut block = session.transaction.lock();
                let Some(state) = block.as_mut() else {
                    return Err(no_active_transaction("RELEASE SAVEPOINT"));
                };
                let Some(index) = state
                    .savepoints
                    .iter()
                    .rposition(|savepoint| savepoint.name == name)
                else {
                    return Err(savepoint_not_found(&name));
                };
                state.savepoints.truncate(index);
                Ok(Some(Response::Execution(Tag::new("RELEASE"))))
            }
            Statement::Rollback {
                savepoint: Some(name),
                ..
            } => {
                let name = savepoint_name(name);
                let discarded = {
                    let mut block = session.transaction.lock();
                    let Some(state) = block.as_mut() else {
                        return Err(no_active_transaction("ROLLBACK TO SAVEPOINT"));
                    };
                    let Some(index) = state
                        .savepoints
                        .iter()
                        .rposition(|savepoint| savepoint.name == name)
                    else {
                        return Err(savepoint_not_found(&name));
                    };
                    state.rollback_to(index)
                };
                if !discarded.is_empty() {
                    StagedFiles::delete(&session.session.context, &discarded).await;
                }
                // `ROLLBACK TO` recovers a failed block: PostgreSQL leaves the
                // transaction usable and reports no error state.
                session.settings.bump();
                client.set_transaction_status(TransactionStatus::Transaction);
                Ok(Some(Response::Execution(Tag::new("ROLLBACK"))))
            }
            Statement::Commit { .. } => {
                // `END` parses as a `Commit` too. A failed block never
                // publishes: PostgreSQL answers its `COMMIT` with the ROLLBACK
                // tag and ends the transaction.
                let failed = client.transaction_status() == TransactionStatus::Error;
                let state = session.transaction.lock().take();
                clear_sink(&session);
                Self::close_portals(&session, client);
                let Some(state) = state else {
                    return Ok(Some(Response::TransactionEnd(Tag::new("COMMIT"))));
                };
                Self::log_end(&state, "commit");
                let TransactionState { pending, ddl, .. } = state;
                let has_ddl = !ddl.is_empty();
                // The files stay owned until the publication's outcome is
                // known. The publication itself runs detached: a cancelled
                // `COMMIT` must not delete files it may have published.
                let staged = StagedFiles::new(&session.session.context, pending);
                if failed {
                    // The block is gone before its files are; report idle first
                    // so a cancel during the deletion cannot leave the client
                    // failed in a transaction that no longer exists.
                    client.set_transaction_status(TransactionStatus::Idle);
                    if has_ddl {
                        session.settings.bump();
                    }
                    staged.discard().await;
                    return Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK"))));
                }
                let publication = Self::publish_detached(
                    staged,
                    ddl,
                    Arc::clone(&session.meta_client),
                    Arc::clone(&session.session),
                );
                // The block is over the moment its publication starts, and the
                // task decides the files' fate on its own. Report the
                // connection idle before waiting for it (and before the index
                // build): a cancel while either runs must not leave the client
                // in a failed transaction it never entered, and a failure
                // reports idle just as PostgreSQL does for a failed COMMIT.
                client.set_transaction_status(TransactionStatus::Idle);
                match publication.await {
                    Ok(Ok(writes)) => {
                        // Published, and the files are no longer ours to
                        // delete. Index maintenance is cancellable: a cancel
                        // here costs shards, never data.
                        Self::maintain_indexes(
                            &writes,
                            &session.meta_client,
                            &session.session.context,
                        )
                        .await;
                        if has_ddl {
                            session.settings.bump();
                        }
                        Ok(Some(Response::TransactionEnd(Tag::new("COMMIT"))))
                    }
                    Ok(Err(error)) => {
                        if has_ddl {
                            session.settings.bump();
                        }
                        Err(commit_error(error))
                    }
                    Err(join) => {
                        // The publication task panicked: the block's outcome is
                        // unknown, so its files are not deleted here either.
                        warn!(%join, "the COMMIT publication task failed");
                        if has_ddl {
                            session.settings.bump();
                        }
                        Err(user_error(INTERNAL_ERROR, format!("COMMIT failed: {join}")))
                    }
                }
            }
            Statement::Rollback { .. } => {
                let state = session.transaction.lock().take();
                clear_sink(&session);
                Self::close_portals(&session, client);
                if let Some(state) = state {
                    Self::log_end(&state, "rollback");
                    // As for a failed `COMMIT`: the block is gone before its
                    // files, so the client must not stay in a transaction it
                    // cannot see if the deletion is cancelled.
                    client.set_transaction_status(TransactionStatus::Idle);
                    if !state.ddl.is_empty() {
                        session.settings.bump();
                    }
                    StagedFiles::new(&session.session.context, state.pending)
                        .discard()
                        .await;
                }
                Ok(Some(Response::TransactionEnd(Tag::new("ROLLBACK"))))
            }
            Statement::Discard {
                object_type: DiscardObject::ALL,
            } if is_open(&session, client.transaction_status()) => Err(user_error(
                ACTIVE_SQL_TRANSACTION,
                "DISCARD ALL cannot run inside a transaction block".to_string(),
            )),
            _ => Ok(None),
        }
    }

    /// Whether `handle` answers this statement, so the extended protocol gives
    /// it a plan instead of handing it to DataFusion.
    fn claims(statement: &Statement) -> bool {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
        )
    }
}

/// The placeholder plan a transaction statement is parsed into: it is never
/// executed, the response comes from [`LakeSoulTransactionHook::respond`].
fn transaction_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

#[async_trait]
impl QueryHook for LakeSoulTransactionHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Self::respond(statement, client).await.transpose()
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        Self::claims(statement).then(|| Ok(transaction_plan()))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Self::respond(statement, client).await.transpose()
    }
}

/// Fold an unquoted savepoint name the way PostgreSQL folds identifiers.
fn savepoint_name(name: &datafusion::sql::sqlparser::ast::Ident) -> String {
    if name.quote_style.is_some() {
        name.value.clone()
    } else {
        name.value.to_ascii_lowercase()
    }
}

/// PostgreSQL's error for a savepoint command outside a transaction block.
fn no_active_transaction(statement: &str) -> PgWireError {
    user_error(
        NO_ACTIVE_SQL_TRANSACTION,
        format!("{statement} can only be used in transaction blocks"),
    )
}

/// PostgreSQL's error for `RELEASE`/`ROLLBACK TO` naming an unknown savepoint.
fn savepoint_not_found(name: &str) -> PgWireError {
    user_error(
        INVALID_SAVEPOINT_SPECIFICATION,
        format!("savepoint \"{name}\" does not exist"),
    )
}

/// A statement this front end refuses inside a block.
fn unsupported(statement: &str) -> PgWireError {
    user_error(
        FEATURE_NOT_SUPPORTED,
        format!("{statement} is not supported in a LakeSoul transaction block"),
    )
}

/// The SQLSTATE a failed publication reports.
pub(crate) fn commit_error(error: LakeSoulMetaDataError) -> PgWireError {
    let sqlstate = match &error {
        // The block lost a race with a concurrent writer, or the table it was
        // written against was redefined underneath it: either way the client
        // retries the transaction.
        LakeSoulMetaDataError::CommitConflict(_)
        | LakeSoulMetaDataError::InsertConflict { .. }
        | LakeSoulMetaDataError::NotFound(_) => SERIALIZATION_FAILURE,
        _ => INTERNAL_ERROR,
    };
    user_error(sqlstate, error.to_string())
}

fn user_error(sqlstate: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        sqlstate.to_string(),
        message,
    )))
}

#[cfg(test)]
mod staged_limit_tests {
    use super::*;
    use lakesoul_datafusion::write_mode::{StagedWrite, WriteViolation};

    /// One staged write holding a single added file of `size` bytes.
    fn staged_write(size: i64) -> StagedWrite {
        use lakesoul_metadata_proto::entity::{
            CommitOp, DataCommitInfo, DataFileOp, FileOp, TableInfo,
        };
        StagedWrite {
            table_info: Arc::new(TableInfo {
                table_id: "staged-limit".to_string(),
                ..Default::default()
            }),
            data_commits: vec![DataCommitInfo {
                table_id: "staged-limit".to_string(),
                partition_desc: "p".to_string(),
                file_ops: vec![DataFileOp {
                    path: "file:///tmp/staged-limit.parquet".to_string(),
                    file_op: FileOp::Add as i32,
                    size,
                    ..Default::default()
                }],
                commit_op: CommitOp::AppendCommit as i32,
                ..Default::default()
            }],
            expected: None,
            rows: vec![("p".to_string(), 1)],
        }
    }

    fn violation(error: DataFusionError) -> WriteViolation {
        match error {
            DataFusionError::External(error) => *error
                .downcast::<WriteViolation>()
                .expect("a write violation"),
            other => panic!("expected a write violation, got {other:?}"),
        }
    }

    /// The block refuses a write that would exceed either bound, and keeps the
    /// writes it already accepted.
    #[test]
    fn a_staged_write_over_the_limit_is_refused() {
        for (limits, expected) in [
            (
                crate::limits::Limits {
                    max_staged_files_per_transaction: 1,
                    ..Default::default()
                },
                "files",
            ),
            (
                crate::limits::Limits {
                    max_staged_bytes_per_transaction: 150,
                    ..Default::default()
                },
                "bytes",
            ),
        ] {
            let transaction = shared();
            *transaction.lock() = Some(TransactionState::new(Isolation::ReadCommitted));
            let sink = BlockSink {
                transaction: Arc::clone(&transaction),
                limits,
            };
            let size = if expected == "bytes" { 100 } else { 1 };
            sink.stage(staged_write(size))
                .expect("the first write fits");
            let error = sink
                .stage(staged_write(size))
                .expect_err("the second write exceeds the bound");
            assert_eq!(violation(error).sqlstate(), "54000");
            assert_eq!(
                transaction.lock().as_ref().expect("open").pending.len(),
                1,
                "the accepted write stays staged"
            );
        }
    }
}
