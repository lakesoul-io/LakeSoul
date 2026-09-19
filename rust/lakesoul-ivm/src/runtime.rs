// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The IVM refresh runtime.
//!
//! A view is described by a [`ViewSpec`] persisted in `ivm.views`; the runtime
//! consumes the source changelog by partition version (P0-2) and writes the
//! resulting deltas into the materialized view table as one
//! `delete(old) + insert(new)` commit per affected key. Refreshes are
//! idempotent per cursor: the cursor only advances once its delta has been
//! committed.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::functions_aggregate::{count::count, sum::sum};
use datafusion::prelude::{DataFrame, JoinType, SessionContext, col, lit};
use lakesoul_io::constant::DEFAULT_PARTITION_DESC;
use lakesoul_metadata::MetaDataClient;
use rootcause::report;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::metadata::{
    BeginEpoch, Cursor, EpochRecord, IvmMetadata, PartitionVersion, SourceVersionRange,
};
use crate::table::{
    IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN, IvmTable, IvmTableOptions, create_ivm_table,
};

/// The `SUM` column of a [`sum_count_mv_schema`] materialized view.
pub const IVM_SUM_COLUMN: &str = "sum_v";
/// The `COUNT` column of a [`sum_count_mv_schema`] materialized view.
pub const IVM_COUNT_COLUMN: &str = "count_v";

/// The persisted description of a view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ViewSpec {
    /// `group_key`, `SUM(value_column)` and `COUNT(*)` over the append-only
    /// changelog of the source table.
    SumCount {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key column.
        group_key: String,
        /// The summed column; `None` means `SUM(0)`, i.e. count only.
        value_column: Option<String>,
    },
    /// Inner equi-join of the append-only changelogs of two sources, appended
    /// to an append-only output table.
    Join {
        /// The view id.
        view_id: String,
        /// The left source table id.
        left_table_id: String,
        /// The right source table id.
        right_table_id: String,
        /// The append-only output table id.
        output_table_id: String,
        /// The equi-join key, present in both sources.
        join_key: String,
        /// The payload column of the left source.
        left_value: String,
        /// The payload column of the right source.
        right_value: String,
    },
}

/// A `SUM`/`COUNT` view over a source table.
#[derive(Debug, Clone)]
pub struct SumCountView {
    /// The view id.
    pub view_id: String,
    /// The append-only source table.
    pub source: IvmTable,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The group key column (must be `Int64`).
    pub group_key: String,
    /// The summed column (must be `Int64`); `None` counts rows only.
    pub value_column: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl SumCountView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: Option<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_key: group_key.into(),
            value_column,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::SumCount {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_key: self.group_key.clone(),
            value_column: self.value_column.clone(),
        }
    }
}

/// The schema of a [`SumCountView`] materialized view.
pub fn sum_count_mv_schema(group_key: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(group_key, DataType::Int64, false),
        Field::new(IVM_SUM_COLUMN, DataType::Int64, false),
        Field::new(IVM_COUNT_COLUMN, DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]))
}

/// An inner equi-join view over two append-only sources.
///
/// The output is append-only: as long as both sides only grow, every joined
/// pair is produced exactly once across refreshes. Each refresh computes the
/// inclusion-exclusion delta
/// `ΔL ⋈ R_before + L_before ⋈ ΔR + ΔL ⋈ ΔR`, so the accumulated output always
/// equals `L_now ⋈ R_now`.
#[derive(Debug, Clone)]
pub struct JoinView {
    /// The view id.
    pub view_id: String,
    /// The left append-only source.
    pub left: IvmTable,
    /// The right append-only source.
    pub right: IvmTable,
    /// The append-only output table.
    pub output: IvmTable,
    /// The equi-join key, present in both sources (must be `Int64`).
    pub join_key: String,
    /// The `Int64` payload column of the left source.
    pub left_value: String,
    /// The `Int64` payload column of the right source.
    pub right_value: String,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl JoinView {
    /// A new join view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        left: IvmTable,
        right: IvmTable,
        output: IvmTable,
        join_key: impl Into<String>,
        left_value: impl Into<String>,
        right_value: impl Into<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            left,
            right,
            output,
            join_key: join_key.into(),
            left_value: left_value.into(),
            right_value: right_value.into(),
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Join {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            output_table_id: self.output.table_id.clone(),
            join_key: self.join_key.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
        }
    }
}

/// The schema of a [`JoinView`] output.
pub fn join_view_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("join_key", DataType::Int64, false),
        Field::new("left_value", DataType::Int64, false),
        Field::new("right_value", DataType::Int64, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]))
}

/// The IVM runtime: a metadata client plus the `ivm` schema access layer.
pub struct IvmRuntime {
    client: MetaDataClient,
    metadata: IvmMetadata,
}

impl IvmRuntime {
    /// Build the runtime from the `LAKESOUL_PG_*` configuration.
    pub async fn from_env() -> Result<Self> {
        Ok(Self {
            client: MetaDataClient::from_env().await?,
            metadata: IvmMetadata::from_env().await?,
        })
    }

    /// The underlying LakeSoul metadata client.
    pub fn client(&self) -> &MetaDataClient {
        &self.client
    }

    /// The `ivm` schema access layer.
    pub fn metadata(&self) -> &IvmMetadata {
        &self.metadata
    }

    /// Create the `ivm` schema and tables if they do not exist.
    pub async fn init_schema(&self) -> Result<()> {
        self.metadata.init_schema().await
    }

    /// Create an internal LakeSoul table managed by the runtime.
    pub async fn create_table(&self, options: IvmTableOptions) -> Result<IvmTable> {
        create_ivm_table(&self.client, options).await
    }

    /// Persist a sum/count view spec (idempotent).
    pub async fn register_view(&self, view: &SumCountView) -> Result<()> {
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a join view spec (idempotent).
    pub async fn register_join_view(&self, view: &JoinView) -> Result<()> {
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a `SUM`/`COUNT` view over the source changelog.
    ///
    /// Returns the epoch written, or `None` when the source had no new rows.
    /// The source must be append-only; an update/delete in the consumed window
    /// (or a missing baseline) is reported as an error so the caller can
    /// rebuild the view.
    pub async fn refresh_sum_count(&self, view: &SumCountView) -> Result<Option<i64>> {
        self.register_view(view).await?;
        ensure_append_only(&view.source, &view.view_id)?;

        let window = self
            .collect_source_window(&view.view_id, &view.source)
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }

        let record = match self
            .begin_window(&view.view_id, &window.identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;

        let delta_batches = view.source.read_files(window.added_files).await?;
        let delta = aggregate_groups(view, delta_batches).await?;
        if !delta.is_empty() {
            let state_batches = view.mv.read_current(&self.client).await?;
            let state = current_state(view, state_batches)?;
            let batch = build_mv_batch(view, &delta, &state, epoch)?;
            view.mv.append_batch(&self.client, batch).await?;
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Refresh an inner equi-join view over two append-only sources.
    ///
    /// Returns the epoch written, or `None` when neither source had new rows.
    /// Each refresh appends `ΔL ⋈ R_before + L_before ⋈ ΔR + ΔL ⋈ ΔR`, so the
    /// accumulated output equals `L_now ⋈ R_now`; `R_before`/`L_before` are
    /// read as of each side's cursor with the as-of API (P0-1).
    pub async fn refresh_join(&self, view: &JoinView) -> Result<Option<i64>> {
        self.register_join_view(view).await?;
        ensure_append_only(&view.left, &view.view_id)?;
        ensure_append_only(&view.right, &view.view_id)?;
        self.ensure_unpartitioned(&view.left).await?;
        self.ensure_unpartitioned(&view.right).await?;

        let left_window = self
            .collect_source_window(&view.view_id, &view.left)
            .await?;
        let right_window = self
            .collect_source_window(&view.view_id, &view.right)
            .await?;
        if left_window.added_files.is_empty() && right_window.added_files.is_empty() {
            return Ok(None);
        }

        let identity = left_window
            .identity
            .iter()
            .chain(right_window.identity.iter())
            .cloned()
            .collect::<Vec<_>>();
        let record = match self
            .begin_window(&view.view_id, &identity, &view.output)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, left_window.cursors)
                    .await?;
                self.advance_cursors(&view.view_id, right_window.cursors)
                    .await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;

        let left_delta = view
            .left
            .read_files(left_window.added_files.clone())
            .await?;
        let right_delta = view
            .right
            .read_files(right_window.added_files.clone())
            .await?;
        let left_before = view
            .left
            .read_as_of(&self.client, left_window.before_timestamp)
            .await?;
        let right_before = view
            .right
            .read_as_of(&self.client, right_window.before_timestamp)
            .await?;

        if let Some(batch) = compute_join_delta(
            view,
            &left_delta,
            &right_delta,
            &left_before,
            &right_before,
            epoch,
        )
        .await?
        {
            view.output.append_batch(&self.client, batch).await?;
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Rebuild a `SUM`/`COUNT` view from the full source state.
    ///
    /// Used when a refresh reports `requires_rebuild` (an update/delete inside
    /// the window or a missing baseline) or when a cursor rewind is not aligned
    /// with the last window. The MV is truncated, recomputed from the current
    /// source state and published as the epoch `rebuild:<generation>`; cursors
    /// are re-baselined to the latest source version. The generation bump
    /// isolates the epochs of the previous incarnation.
    pub async fn rebuild_sum_count(&self, view: &SumCountView) -> Result<i64> {
        self.register_view(view).await?;
        ensure_append_only(&view.source, &view.view_id)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
        let full = aggregate_groups(view, baseline.batches).await?;

        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                &view.view_id,
                &format!("rebuild:{generation}"),
                &baseline.to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                self.advance_cursors(&view.view_id, baseline.cursors)
                    .await?;
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;

        let batch = build_full_batch(view, &full, epoch)?;
        view.mv.append_batch(&self.client, batch).await?;

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Rebuild an inner-join view from the full state of both sources.
    ///
    /// The output is truncated and refilled with the full join, published as
    /// the epoch `rebuild:<generation>`.
    pub async fn rebuild_join(&self, view: &JoinView) -> Result<i64> {
        self.register_join_view(view).await?;
        ensure_append_only(&view.left, &view.view_id)?;
        ensure_append_only(&view.right, &view.view_id)?;
        self.ensure_unpartitioned(&view.left).await?;
        self.ensure_unpartitioned(&view.right).await?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.output.truncate(&self.client).await?;

        let left_baseline = self.source_baseline(&view.left).await?;
        let right_baseline = self.source_baseline(&view.right).await?;
        let to_versions = left_baseline
            .to_versions
            .iter()
            .chain(right_baseline.to_versions.iter())
            .cloned()
            .collect::<Vec<_>>();

        let mv_versions_before =
            output_partition_versions(&self.client, &view.output).await?;
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
                self.advance_cursors(&view.view_id, left_baseline.cursors)
                    .await?;
                self.advance_cursors(&view.view_id, right_baseline.cursors)
                    .await?;
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;

        let context = SessionContext::new();
        let joined = join_term(
            &context,
            &left_baseline.batches,
            &right_baseline.batches,
            view,
        )?;
        if let Some(batch) = build_join_batch(&joined.collect().await?, epoch)? {
            view.output.append_batch(&self.client, batch).await?;
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, left_baseline.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// The read side of an epoch: the MV state pinned to the partition
    /// versions the epoch produced.
    pub async fn view_state_at_epoch(
        &self,
        output: &IvmTable,
        record: &EpochRecord,
    ) -> Result<Vec<RecordBatch>> {
        output
            .read_at_versions(&self.client, &record.mv_versions)
            .await
    }

    /// The latest committed epoch of the current generation, if any.
    pub async fn latest_epoch(&self, view_id: &str) -> Result<Option<EpochRecord>> {
        let generation = self.metadata.view_generation(view_id).await?;
        self.metadata
            .latest_committed_epoch(view_id, generation)
            .await
    }

    /// Read the full current state of a source and the cursors / source ranges
    /// that represent it (a rebuild baseline).
    async fn source_baseline(&self, source: &IvmTable) -> Result<SourceBaseline> {
        let mut files = Vec::new();
        let mut cursors = Vec::new();
        let mut to_versions = Vec::new();
        for partition in self.client.get_all_partition_info(&source.table_id).await? {
            files.extend(
                self.client
                    .get_data_files_of_single_partition(&partition)
                    .await?,
            );
            cursors.push(Cursor {
                source_table_id: source.table_id.clone(),
                partition_desc: partition.partition_desc.clone(),
                last_version: i64::from(partition.version),
                last_timestamp: partition.timestamp,
            });
            to_versions.push(SourceVersionRange {
                source_table_id: source.table_id.clone(),
                partition_desc: partition.partition_desc.clone(),
                from_version: -1,
                to_version: i64::from(partition.version),
            });
        }
        let batches = source.read_files(files).await?;
        Ok(SourceBaseline {
            batches,
            cursors,
            to_versions,
        })
    }

    /// Read the changelog window of every partition of a source.
    async fn collect_source_window(
        &self,
        view_id: &str,
        source: &IvmTable,
    ) -> Result<SourceWindow> {
        let cursors = self
            .metadata
            .list_cursors(view_id)
            .await?
            .into_iter()
            .filter(|cursor| cursor.source_table_id == source.table_id)
            .map(|cursor| (cursor.partition_desc.clone(), cursor))
            .collect::<HashMap<String, Cursor>>();
        let before_timestamp = cursors
            .values()
            .map(|cursor| cursor.last_timestamp)
            .max()
            .unwrap_or(0);

        let mut added_files = Vec::new();
        let mut new_cursors = Vec::new();
        let mut identity = Vec::new();
        for partition in self.client.get_all_partition_info(&source.table_id).await? {
            let last_version = cursors
                .get(&partition.partition_desc)
                .map(|cursor| cursor.last_version)
                .unwrap_or(-1);
            if i64::from(partition.version) <= last_version {
                continue;
            }

            let window = self
                .client
                .get_partition_changelog(
                    &source.table_id,
                    &partition.partition_desc,
                    last_version,
                    i64::from(partition.version),
                )
                .await?;
            if window.requires_rebuild {
                return Err(report!(
                    "view {view_id} source partition {} requires a rebuild",
                    partition.partition_desc
                ));
            }
            if window.partition_deleted {
                continue;
            }

            added_files.extend(window.added_files.iter().map(|file| file.path.clone()));
            identity.push((
                source.table_id.clone(),
                partition.partition_desc.clone(),
                last_version,
                window.to_version,
            ));
            new_cursors.push(Cursor {
                source_table_id: source.table_id.clone(),
                partition_desc: partition.partition_desc.clone(),
                last_version: window.to_version,
                last_timestamp: window.to_timestamp,
            });
        }

        Ok(SourceWindow {
            added_files,
            cursors: new_cursors,
            identity,
            before_timestamp,
        })
    }

    /// Gate a refresh window through `ivm.epochs`.
    ///
    /// Returns [`WindowStart::Apply`] when the window still has to be applied,
    /// [`WindowStart::AlreadyApplied`] when a previous attempt (or an earlier
    /// replay) already wrote it. A pending row whose MV versions moved past
    /// `mv_versions_before` is the crash case "data written, epoch not marked";
    /// the window is then marked committed without touching the data.
    ///
    /// The window must start where the last committed window ended, otherwise
    /// the recomputed window would overlap applied history (a cursor rewind
    /// that is not aligned with a previous window boundary) and a rebuild is
    /// required.
    async fn begin_window(
        &self,
        view_id: &str,
        identity: &[(String, String, i64, i64)],
        output: &IvmTable,
    ) -> Result<WindowStart> {
        let window_key = window_key(identity);
        let to_versions = identity
            .iter()
            .map(
                |(source_table_id, partition_desc, from_version, to_version)| {
                    SourceVersionRange {
                        source_table_id: source_table_id.clone(),
                        partition_desc: partition_desc.clone(),
                        from_version: *from_version,
                        to_version: *to_version,
                    }
                },
            )
            .collect::<Vec<_>>();
        let mv_versions_before = output_partition_versions(&self.client, output).await?;

        let record = match self
            .metadata
            .begin_epoch(view_id, &window_key, &to_versions, &mv_versions_before)
            .await?
        {
            BeginEpoch::Committed(record) => {
                return Ok(WindowStart::AlreadyApplied(record.epoch));
            }
            BeginEpoch::Created(record) => record,
            BeginEpoch::Pending(record) => {
                let current = output_partition_versions(&self.client, output).await?;
                if current != record.mv_versions_before {
                    self.metadata
                        .mark_epoch_committed(&record, &current)
                        .await?;
                    return Ok(WindowStart::AlreadyApplied(record.epoch));
                }
                record
            }
        };

        let max_to = self
            .metadata
            .max_committed_to_versions(view_id, record.generation)
            .await?;
        for range in &record.to_versions {
            let expected = max_to
                .get(&(range.source_table_id.clone(), range.partition_desc.clone()))
                .copied()
                .unwrap_or(-1);
            if range.from_version != expected {
                return Err(report!(
                    "view {view_id} window for source {} partition {} starts at version {}, \
                     but the last committed window ended at {expected}; a rebuild is required",
                    range.source_table_id,
                    range.partition_desc,
                    range.from_version
                ));
            }
        }

        Ok(WindowStart::Apply(record))
    }

    async fn advance_cursors(&self, view_id: &str, cursors: Vec<Cursor>) -> Result<()> {
        for cursor in cursors {
            self.metadata
                .upsert_cursor(
                    view_id,
                    &cursor.source_table_id,
                    &cursor.partition_desc,
                    cursor.last_version,
                    cursor.last_timestamp,
                )
                .await?;
        }
        Ok(())
    }

    /// Join sources must be unpartitioned: the inclusion-exclusion terms use
    /// one as-of timestamp per side and a per-partition cursor mix would make
    /// the before-state inconsistent.
    async fn ensure_unpartitioned(&self, source: &IvmTable) -> Result<()> {
        for partition in self.client.get_all_partition_info(&source.table_id).await? {
            if partition.partition_desc != DEFAULT_PARTITION_DESC {
                return Err(report!(
                    "join source {} must not be range partitioned yet (partition {})",
                    source.table_name,
                    partition.partition_desc
                ));
            }
        }
        Ok(())
    }
}

/// The changelog window of every partition of one source.
struct SourceWindow {
    added_files: Vec<String>,
    cursors: Vec<Cursor>,
    /// `(source_table_id, partition_desc, from_version, to_version)` of every
    /// consumed partition; this is the window identity the epoch is keyed by.
    identity: Vec<(String, String, i64, i64)>,
    before_timestamp: i64,
}

/// The full current state of one source, as read by a rebuild.
struct SourceBaseline {
    batches: Vec<RecordBatch>,
    cursors: Vec<Cursor>,
    to_versions: Vec<SourceVersionRange>,
}

/// The canonical identity of a refresh window.
///
/// Entries are `(source_table_id, partition_desc, from_version, to_version)`
/// and are sorted, so the same consumed window always produces the same key;
/// the key is persisted as `ivm.epochs.window_key` and is what makes a retried
/// window recognizable without touching the MV data.
pub fn window_key(identity: &[(String, String, i64, i64)]) -> String {
    let mut sorted = identity.to_vec();
    sorted.sort();
    sorted
        .iter()
        .map(|(source, partition, from, to)| format!("{source}|{partition}|{from}|{to}"))
        .collect::<Vec<_>>()
        .join(";")
}

/// Whether the refresh window still has to be applied.
enum WindowStart {
    /// The caller must apply the window with `EpochRecord::epoch`.
    Apply(EpochRecord),
    /// The window was already applied; only the cursor has to advance.
    AlreadyApplied(i64),
}

/// The current partition versions of a table, sorted by partition.
async fn output_partition_versions(
    client: &MetaDataClient,
    table: &IvmTable,
) -> Result<Vec<PartitionVersion>> {
    let mut versions = client
        .get_all_partition_info(&table.table_id)
        .await?
        .into_iter()
        .map(|partition| PartitionVersion {
            partition_desc: partition.partition_desc,
            version: i64::from(partition.version),
        })
        .collect::<Vec<_>>();
    versions.sort_by(|left, right| left.partition_desc.cmp(&right.partition_desc));
    Ok(versions)
}

fn ensure_append_only(source: &IvmTable, view_id: &str) -> Result<()> {
    if !source.primary_keys.is_empty() {
        return Err(report!(
            "view {view_id} source {} must be append-only (has primary keys)",
            source.table_name
        ));
    }
    Ok(())
}

/// Build the inclusion-exclusion delta of an inner join.
#[allow(clippy::too_many_arguments)]
async fn compute_join_delta(
    view: &JoinView,
    left_delta: &[RecordBatch],
    right_delta: &[RecordBatch],
    left_before: &[RecordBatch],
    right_before: &[RecordBatch],
    epoch: i64,
) -> Result<Option<RecordBatch>> {
    let context = SessionContext::new();
    let mut terms: Vec<DataFrame> = Vec::new();
    if !left_delta.is_empty() && !right_before.is_empty() {
        terms.push(join_term(&context, left_delta, right_before, view)?);
    }
    if !left_before.is_empty() && !right_delta.is_empty() {
        terms.push(join_term(&context, left_before, right_delta, view)?);
    }
    if !left_delta.is_empty() && !right_delta.is_empty() {
        terms.push(join_term(&context, left_delta, right_delta, view)?);
    }
    if terms.is_empty() {
        return Ok(None);
    }

    let mut collected = Vec::new();
    for term in terms {
        collected.extend(term.collect().await?);
    }

    build_join_batch(&collected, epoch)
}

/// Turn joined `(join_key, left_value, right_value)` batches into the
/// append-only output batch stamped with `epoch`.
fn build_join_batch(batches: &[RecordBatch], epoch: i64) -> Result<Option<RecordBatch>> {
    let mut keys = Vec::new();
    let mut left_values = Vec::new();
    let mut right_values = Vec::new();
    for batch in batches {
        let batch_keys = int64_column(batch, 0, "join_key")?;
        let batch_left = int64_column(batch, 1, "left_value")?;
        let batch_right = int64_column(batch, 2, "right_value")?;
        for row in 0..batch.num_rows() {
            keys.push(batch_keys.value(row));
            left_values.push(batch_left.value(row));
            right_values.push(batch_right.value(row));
        }
    }
    if keys.is_empty() {
        return Ok(None);
    }

    let row_count = keys.len();
    Ok(Some(RecordBatch::try_new(
        join_view_schema(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(left_values)),
            Arc::new(Int64Array::from(right_values)),
            Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                epoch, row_count,
            ))),
        ],
    )?))
}

/// Join two batch sets on the view key, projecting to the output columns.
fn join_term(
    context: &SessionContext,
    left: &[RecordBatch],
    right: &[RecordBatch],
    view: &JoinView,
) -> Result<DataFrame> {
    let left = context.read_batches(left.to_vec())?.select(vec![
        col(view.join_key.as_str()).alias("join_key"),
        col(view.left_value.as_str()).alias("left_value"),
    ])?;
    let right = context.read_batches(right.to_vec())?.select(vec![
        col(view.join_key.as_str()).alias("right_key"),
        col(view.right_value.as_str()).alias("right_value"),
    ])?;
    Ok(left
        .join(right, JoinType::Inner, &["join_key"], &["right_key"], None)?
        .select(vec![col("join_key"), col("left_value"), col("right_value")])?)
}

/// Aggregate sum/count batches into `group_key -> (sum, count)`.
async fn aggregate_groups(
    view: &SumCountView,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<HashMap<i64, (i64, i64)>> {
    if batches.is_empty() {
        return Ok(HashMap::new());
    }

    let context = SessionContext::new();
    let frame = context.read_batches(batches)?;
    let value_expr = match &view.value_column {
        Some(column) => sum(col(column.as_str())),
        None => sum(lit(0_i64)),
    };
    let aggregated = frame.aggregate(
        vec![col(view.group_key.as_str())],
        vec![
            value_expr.alias("delta_sum"),
            count(lit(1_i64)).alias("delta_count"),
        ],
    )?;

    let mut delta = HashMap::new();
    for batch in aggregated.collect().await? {
        let keys = int64_column(&batch, 0, &view.group_key)?;
        let sums = int64_column(&batch, 1, "delta_sum")?;
        let counts = int64_column(&batch, 2, "delta_count")?;
        for row in 0..batch.num_rows() {
            delta.insert(keys.value(row), (sums.value(row), counts.value(row)));
        }
    }
    Ok(delta)
}

/// Read the current merge-on-read state as `group_key -> (sum, count, epoch)`.
fn current_state(
    view: &SumCountView,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<HashMap<i64, (i64, i64, i64)>> {
    let mut state = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let key_index = schema.index_of(&view.group_key)?;
        let sum_index = schema.index_of(IVM_SUM_COLUMN)?;
        let count_index = schema.index_of(IVM_COUNT_COLUMN)?;
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN)?;
        let epoch_index = schema.index_of(IVM_EPOCH_COLUMN)?;

        let keys = int64_column(&batch, key_index, &view.group_key)?;
        let sums = int64_column(&batch, sum_index, IVM_SUM_COLUMN)?;
        let counts = int64_column(&batch, count_index, IVM_COUNT_COLUMN)?;
        let epochs = int64_column(&batch, epoch_index, IVM_EPOCH_COLUMN)?;
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| report!("{IVM_ROW_KINDS_COLUMN} must be a Utf8 column"))?;

        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            if kinds.value(row) == "insert" {
                state
                    .insert(key, (sums.value(row), counts.value(row), epochs.value(row)));
            } else if !state.contains_key(&key) {
                state.remove(&key);
            }
        }
    }
    Ok(state)
}

/// Build the `delete(old) + insert(new)` batch for the affected groups.
///
/// Groups whose state already carries `epoch` were written by a previous
/// attempt of the same window (a refresh that crashed before advancing its
/// cursors) and are skipped, which makes the refresh idempotent.
fn build_mv_batch(
    view: &SumCountView,
    delta: &HashMap<i64, (i64, i64)>,
    state: &HashMap<i64, (i64, i64, i64)>,
    epoch: i64,
) -> Result<arrow::record_batch::RecordBatch> {
    let mut keys = Vec::new();
    let mut sums = Vec::new();
    let mut counts = Vec::new();
    let mut kinds = Vec::new();
    let mut epochs = Vec::new();

    let mut affected = delta.keys().copied().collect::<Vec<_>>();
    affected.sort_unstable();
    for key in affected {
        let (delta_sum, delta_count) = delta[&key];
        let previous = state.get(&key).copied();
        if previous.is_some_and(|(_, _, state_epoch)| state_epoch == epoch) {
            continue;
        }
        let previous = previous.map(|(sum, count, _)| (sum, count));

        if let Some((old_sum, old_count)) = previous {
            keys.push(key);
            sums.push(old_sum);
            counts.push(old_count);
            kinds.push("delete");
            epochs.push(epoch);
        }

        let (old_sum, old_count) = previous.unwrap_or((0, 0));
        let new_count = old_count + delta_count;
        if new_count != 0 {
            keys.push(key);
            sums.push(old_sum + delta_sum);
            counts.push(new_count);
            kinds.push("insert");
            epochs.push(epoch);
        }
    }

    Ok(arrow::record_batch::RecordBatch::try_new(
        view.mv.schema.clone(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(sums)),
            Arc::new(Int64Array::from(counts)),
            Arc::new(StringArray::from(kinds)),
            Arc::new(Int64Array::from(epochs)),
        ],
    )?)
}

/// Build the `insert`-only batch of a full rebuild.
fn build_full_batch(
    view: &SumCountView,
    full: &HashMap<i64, (i64, i64)>,
    epoch: i64,
) -> Result<RecordBatch> {
    let mut keys = Vec::new();
    let mut sums = Vec::new();
    let mut counts = Vec::new();
    let mut kinds = Vec::new();
    let mut epochs = Vec::new();

    let mut affected = full.keys().copied().collect::<Vec<_>>();
    affected.sort_unstable();
    for key in affected {
        let (sum, count) = full[&key];
        if count == 0 {
            continue;
        }
        keys.push(key);
        sums.push(sum);
        counts.push(count);
        kinds.push("insert");
        epochs.push(epoch);
    }

    Ok(RecordBatch::try_new(
        view.mv.schema.clone(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(sums)),
            Arc::new(Int64Array::from(counts)),
            Arc::new(StringArray::from(kinds)),
            Arc::new(Int64Array::from(epochs)),
        ],
    )?)
}

fn int64_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| report!("column {name} must be Int64"))
}
