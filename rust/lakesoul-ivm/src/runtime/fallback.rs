// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The Tier-2 fallback view: a statement the incremental analyzer does not
//! maintain, recomputed in full on every refresh.
//!
//! The view has no state tables.  A refresh collects the change window of
//! every source table; when none of them changed the refresh is a no-op,
//! otherwise the statement runs against the current source state, the MV is
//! truncated and the result is written with the row kinds and epoch columns
//! the other views write.  The result is computed before the MV is touched,
//! so a failing statement leaves the previous state in place (the chain
//! policy still rebuilds or marks the view dirty when the epoch cannot
//! commit).
//!
//! A fallback MV may be append-only (no primary keys).  When it declares
//! primary keys the recomputed result must be unique on them; a repeated key
//! would be collapsed by merge-on-read, so it is rejected explicitly instead.

use super::*;
use crate::metadata::StateRole;

/// A [`ViewSpec::Fallback`] view with its tables opened.
#[derive(Debug, Clone)]
pub struct FallbackView {
    /// The view id.
    pub view_id: String,
    /// The `SELECT` that produces the MV rows.
    pub statement: String,
    /// The tables the statement reads.
    pub tables: Vec<IvmTable>,
    /// Why the incremental analyzer rejected the shape (metadata only; it is
    /// not part of the definition identity).
    pub reason: String,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The registered refresh interval.
    pub refresh_interval_ms: i64,
}

impl FallbackView {
    /// The persisted spec of the view.
    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Fallback {
            view_id: self.view_id.clone(),
            statement: self.statement.clone(),
            tables: self
                .tables
                .iter()
                .map(|table| table.table_id.clone())
                .collect(),
            reason: self.reason.clone(),
            mv_table_id: self.mv.table_id.clone(),
        }
    }
}

impl IvmRuntime {
    /// Register the fallback view's MV and its persisted spec.
    pub async fn register_fallback_view(&self, view: &FallbackView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a fallback view: recompute the whole statement when any source
    /// produced a change window, otherwise do nothing.
    pub async fn refresh_fallback(&self, view: &FallbackView) -> Result<Option<i64>> {
        self.register_fallback_view(view).await?;
        let mut windows = Vec::with_capacity(view.tables.len());
        for table in &view.tables {
            windows.push(self.collect_source_window(&view.view_id, table).await?);
        }
        if windows.iter().all(|window| window.added_files.is_empty()) {
            return Ok(None);
        }
        let mut identity = Vec::new();
        for window in &windows {
            identity.extend(window.identity.iter().cloned());
        }
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window_cursors(&windows))
                    .await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let commit_ids = self.recompute_fallback(view, epoch).await?;
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window_cursors(&windows))
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(Some(epoch))
    }

    /// Rebuild a fallback view from the current source state.
    pub async fn rebuild_fallback(&self, view: &FallbackView) -> Result<i64> {
        self.register_fallback_view(view).await?;
        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;

        let mut baselines = Vec::with_capacity(view.tables.len());
        for table in &view.tables {
            baselines.push(self.source_baseline(table).await?);
        }
        let to_versions = baselines
            .iter()
            .flat_map(|baseline| baseline.to_versions.iter().cloned())
            .collect::<Vec<_>>();
        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                &view.view_id,
                &format!("rebuild:{generation}"),
                &to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                for baseline in &baselines {
                    self.advance_cursors(&view.view_id, baseline.cursors.clone())
                        .await?;
                }
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;
        let commit_ids = self.recompute_fallback(view, epoch).await?;
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for baseline in &baselines {
            self.advance_cursors(&view.view_id, baseline.cursors.clone())
                .await?;
        }
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Run the view's statement against the current source state, check the
    /// result against the MV keys, truncate the MV and write the rows with
    /// their row kinds and epoch.  Returns the produced commit ids.
    async fn recompute_fallback(
        &self,
        view: &FallbackView,
        epoch: i64,
    ) -> Result<Vec<String>> {
        let context = SessionContext::new();
        for table in &view.tables {
            let provider: Arc<dyn datafusion::catalog::TableProvider> =
                Arc::new(self.table_provider(table));
            let _ = context.register_table(table.table_name.as_str(), provider.clone());
            let qualified = format!("{}.{}", table.namespace, table.table_name);
            let _ = context.register_table(qualified.as_str(), provider);
        }
        let frame = context
            .sql(&view.statement)
            .await
            .map_err(|error| report!("fallback `{}`: {error}", view.statement))?;
        let batches = frame
            .collect()
            .await
            .map_err(|error| report!("fallback `{}`: {error}", view.statement))?;
        self.ensure_fallback_keys(view, &batches).await?;

        let rows = rows_with_epoch(&context, batches, epoch).await?;
        view.mv.truncate(&self.client).await?;
        let mut commit_ids = Vec::new();
        for batch in rows {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }
        Ok(commit_ids)
    }

    /// A keyed fallback MV must be unique on its primary keys: the write path
    /// merges repeated keys, which would silently collapse rows.
    async fn ensure_fallback_keys(
        &self,
        view: &FallbackView,
        batches: &[RecordBatch],
    ) -> Result<()> {
        if view.mv.primary_keys.is_empty() || batches.is_empty() {
            return Ok(());
        }
        let context = SessionContext::new();
        let schema = batches[0].schema();
        let mem = datafusion::datasource::memory::MemTable::try_new(
            schema,
            vec![batches.to_vec()],
        )?;
        context.register_table("__ivm_fallback", Arc::new(mem))?;
        let keys = quoted_list(&view.mv.primary_keys);
        let frame = context
            .sql(&format!(
                "select 1 from __ivm_fallback group by {keys} \
                 having count(*) > 1 limit 1"
            ))
            .await
            .map_err(|error| report!("fallback key check: {error}"))?;
        if !frame
            .collect()
            .await
            .map_err(|error| report!("fallback key check: {error}"))?
            .is_empty()
        {
            return Err(report!(
                "the fallback result repeats the primary key ({keys}) of the \
                 target table; create the materialized view without primary \
                 keys or add a distinguishing column to the statement"
            ));
        }
        Ok(())
    }
}

/// The cursors every source window advanced to.
fn window_cursors(windows: &[SourceWindow]) -> Vec<Cursor> {
    windows
        .iter()
        .flat_map(|window| window.cursors.iter().cloned())
        .collect()
}

/// Attach the row kind and epoch columns the MV write expects.
async fn rows_with_epoch(
    context: &SessionContext,
    batches: Vec<RecordBatch>,
    epoch: i64,
) -> Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let mem =
        datafusion::datasource::memory::MemTable::try_new(first.schema(), vec![batches])?;
    context.register_table("__ivm_fallback_rows", Arc::new(mem))?;
    // The names are quoted: SQL would lowercase `rowKinds` and the writer
    // looks the column up by its exact name.
    let frame = context
        .sql(&format!(
            "select *, 'insert' as \"{IVM_ROW_KINDS_COLUMN}\", {epoch} as \"{IVM_EPOCH_COLUMN}\" \
             from __ivm_fallback_rows"
        ))
        .await
        .map_err(|error| report!("fallback epoch columns: {error}"))?;
    frame
        .collect()
        .await
        .map_err(|error| report!("fallback epoch columns: {error}"))
}
