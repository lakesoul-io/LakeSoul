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

use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::functions_aggregate::{count::count, sum::sum};
use datafusion::prelude::{SessionContext, col, lit};
use lakesoul_metadata::MetaDataClient;
use rootcause::report;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::metadata::{Cursor, IvmMetadata};
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

    /// Persist the view spec (idempotent).
    pub async fn register_view(&self, view: &SumCountView) -> Result<()> {
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

        let cursors = self
            .metadata
            .list_cursors(&view.view_id)
            .await?
            .into_iter()
            .map(|cursor| (cursor.partition_desc.clone(), cursor))
            .collect::<HashMap<String, Cursor>>();

        let mut added_files = Vec::new();
        let mut new_cursors = Vec::new();
        for partition in self
            .client
            .get_all_partition_info(&view.source.table_id)
            .await?
        {
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
                    &view.source.table_id,
                    &partition.partition_desc,
                    last_version,
                    i64::from(partition.version),
                )
                .await?;
            if window.requires_rebuild {
                return Err(report!(
                    "view {} source partition {} requires a rebuild",
                    view.view_id,
                    partition.partition_desc
                ));
            }
            if window.partition_deleted {
                continue;
            }

            added_files.extend(window.added_files.iter().map(|file| file.path.clone()));
            new_cursors.push(Cursor {
                source_table_id: view.source.table_id.clone(),
                partition_desc: partition.partition_desc.clone(),
                last_version: window.to_version,
                last_timestamp: window.to_timestamp,
            });
        }

        if added_files.is_empty() {
            return Ok(None);
        }

        let epoch = new_cursors
            .iter()
            .map(|cursor| cursor.last_timestamp)
            .max()
            .unwrap_or_else(crate::now_ms);

        let delta_batches = view.source.read_files(added_files).await?;
        let delta = aggregate_delta(view, delta_batches).await?;
        if !delta.is_empty() {
            let state_batches = view.mv.read_current(&self.client).await?;
            let state = current_state(view, state_batches)?;
            let batch = build_mv_batch(view, &delta, &state, epoch)?;
            view.mv.append_batch(&self.client, batch).await?;
        }

        for cursor in new_cursors {
            self.metadata
                .upsert_cursor(
                    &view.view_id,
                    &cursor.source_table_id,
                    &cursor.partition_desc,
                    cursor.last_version,
                    cursor.last_timestamp,
                )
                .await?;
        }

        Ok(Some(epoch))
    }
}

/// Aggregate the changelog batch into `group_key -> (sum, count)`.
async fn aggregate_delta(
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

/// Read the current merge-on-read state as `group_key -> (sum, count)`.
fn current_state(
    view: &SumCountView,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<HashMap<i64, (i64, i64)>> {
    let mut state = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let key_index = schema.index_of(&view.group_key)?;
        let sum_index = schema.index_of(IVM_SUM_COLUMN)?;
        let count_index = schema.index_of(IVM_COUNT_COLUMN)?;
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN)?;

        let keys = int64_column(&batch, key_index, &view.group_key)?;
        let sums = int64_column(&batch, sum_index, IVM_SUM_COLUMN)?;
        let counts = int64_column(&batch, count_index, IVM_COUNT_COLUMN)?;
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| report!("{IVM_ROW_KINDS_COLUMN} must be a Utf8 column"))?;

        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            if kinds.value(row) == "insert" {
                state.insert(key, (sums.value(row), counts.value(row)));
            } else if !state.contains_key(&key) {
                state.remove(&key);
            }
        }
    }
    Ok(state)
}

/// Build the `delete(old) + insert(new)` batch for the affected groups.
fn build_mv_batch(
    view: &SumCountView,
    delta: &HashMap<i64, (i64, i64)>,
    state: &HashMap<i64, (i64, i64)>,
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
