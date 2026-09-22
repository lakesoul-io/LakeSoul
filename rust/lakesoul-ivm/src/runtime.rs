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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
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
/// The value column of a [`min_max_mv_schema`] materialized view and of the
/// value-count state table.
pub const IVM_VALUE_COLUMN: &str = "value";
/// The value-count column of the MIN/MAX state table.
pub const IVM_VALUE_COUNT_COLUMN: &str = "value_count";
/// The row-number column of a [`WindowView`] materialized view.
pub const IVM_ROW_NUMBER_COLUMN: &str = "row_number";

/// Whether a [`MinMaxView`] maintains the minimum or the maximum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MinMaxKind {
    /// The minimum value per group.
    Min,
    /// The maximum value per group.
    Max,
}

/// The distinct aggregate a [`DistinctAggView`] maintains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistinctAggKind {
    /// `COUNT(DISTINCT value_column)` per group.
    Count,
    /// `SUM(DISTINCT value_column)` per group.
    Sum,
}

/// The window function a [`WindowView`] maintains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowFunction {
    /// `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`. The source primary
    /// keys are appended to the ordering so ties are broken deterministically.
    RowNumber,
}

/// The persisted description of a view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ViewSpec {
    /// `group_key`, `SUM(value_column)` and `COUNT(*)` over the source
    /// changelog. Append-only sources contribute every row; a source with a
    /// primary key is treated as upsert and retracts the previous version of
    /// each changed row (`rowKinds='delete'` rows retract only).
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
    /// `group_key`, `MIN(value_column)` or `MAX(value_column)` over the source
    /// changelog, backed by a value-count state table.
    MinMax {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The value-count state table id.
        state_table_id: String,
        /// The group key column.
        group_key: String,
        /// The min/max column.
        value_column: String,
        /// Whether the minimum or the maximum is maintained.
        min_max: MinMaxKind,
    },
    /// `group_key`, `COUNT(DISTINCT value_column)` or
    /// `SUM(DISTINCT value_column)` over the source changelog, backed by the
    /// same value-count state table as [`ViewSpec::MinMax`].
    DistinctAgg {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The value-count state table id.
        state_table_id: String,
        /// The group key column.
        group_key: String,
        /// The distinct value column.
        value_column: String,
        /// Whether the distinct count or the distinct sum is maintained.
        agg: DistinctAggKind,
    },
    /// `ROW_NUMBER()` over a source, maintained by recomputing the affected
    /// partitions.
    Window {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The `PARTITION BY` columns.
        partition_keys: Vec<String>,
        /// The `ORDER BY` columns.
        order_keys: Vec<String>,
        /// The window function.
        function: WindowFunction,
    },
    /// `SEMI`/`ANTI` join of a keyed left source against a right source,
    /// maintained by recomputing the affected left rows.
    SemiAnti {
        /// The view id.
        view_id: String,
        /// The left source table id.
        left_table_id: String,
        /// The right source table id.
        right_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The equi-join key, present in both sources.
        join_keys: Vec<String>,
        /// `true` for `ANTI` (rows without a match), `false` for `SEMI`.
        anti: bool,
    },
}

/// A `SUM`/`COUNT` view over a source table.
#[derive(Debug, Clone)]
pub struct SumCountView {
    /// The view id.
    pub view_id: String,
    /// The source table: append-only, or keyed (upsert) when it has primary
    /// keys.
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

/// The aggregation a value-count view derives from a group's value counts.
#[derive(Debug, Clone, Copy)]
enum ValueAgg {
    Min,
    Max,
    DistinctCount,
    DistinctSum,
}

impl ValueAgg {
    fn apply(self, group_values: Option<&BTreeMap<i64, i64>>) -> Option<i64> {
        let values = group_values.filter(|values| !values.is_empty());
        match self {
            ValueAgg::Min => values.and_then(|values| values.keys().next().copied()),
            ValueAgg::Max => values.and_then(|values| values.keys().next_back().copied()),
            ValueAgg::DistinctCount => values.map(|values| values.len() as i64),
            ValueAgg::DistinctSum => values.map(|values| values.keys().sum()),
        }
    }
}

impl From<MinMaxKind> for ValueAgg {
    fn from(kind: MinMaxKind) -> Self {
        match kind {
            MinMaxKind::Min => ValueAgg::Min,
            MinMaxKind::Max => ValueAgg::Max,
        }
    }
}

impl From<DistinctAggKind> for ValueAgg {
    fn from(kind: DistinctAggKind) -> Self {
        match kind {
            DistinctAggKind::Count => ValueAgg::DistinctCount,
            DistinctAggKind::Sum => ValueAgg::DistinctSum,
        }
    }
}

/// A borrowed description of a value-count view, shared by the MIN/MAX and
/// DISTINCT aggregate refresh paths.
struct ValueCountView<'a> {
    view_id: &'a str,
    source: &'a IvmTable,
    mv: &'a IvmTable,
    state: &'a IvmTable,
    group_key: &'a str,
    value_column: &'a str,
    agg: ValueAgg,
}

/// A `MIN`/`MAX` view over a source table.
///
/// The materialized view holds one row per group; the value distribution of
/// every group is kept in a value-count state table (`(group, value) -> count`)
/// so updates and deletes can retract the previous value and recompute the
/// affected groups.
#[derive(Debug, Clone)]
pub struct MinMaxView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The value-count state table, created from [`min_max_state_schema`] with
    /// `(group_key, value)` as merge key.
    pub state: IvmTable,
    /// The group key column (must be `Int64`).
    pub group_key: String,
    /// The min/max value column (must be `Int64`).
    pub value_column: String,
    /// Whether the minimum or the maximum is maintained.
    pub min_max: MinMaxKind,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl MinMaxView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        state: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
        min_max: MinMaxKind,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            state,
            group_key: group_key.into(),
            value_column: value_column.into(),
            min_max,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::MinMax {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            state_table_id: self.state.table_id.clone(),
            group_key: self.group_key.clone(),
            value_column: self.value_column.clone(),
            min_max: self.min_max,
        }
    }
}

/// A `COUNT(DISTINCT)`/`SUM(DISTINCT)` view over a source table.
///
/// It shares the value-count state table with [`MinMaxView`]: every distinct
/// value of a group is kept with its row count, and the materialized view
/// holds the number (or the sum) of the distinct values with a positive count.
#[derive(Debug, Clone)]
pub struct DistinctAggView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The value-count state table, created from
    /// [`value_count_state_schema`] with `(group_key, value)` as merge key.
    pub state: IvmTable,
    /// The group key column (must be `Int64`).
    pub group_key: String,
    /// The distinct value column (must be `Int64`).
    pub value_column: String,
    /// Whether the distinct count or the distinct sum is maintained.
    pub agg: DistinctAggKind,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl DistinctAggView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        state: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
        agg: DistinctAggKind,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            state,
            group_key: group_key.into(),
            value_column: value_column.into(),
            agg,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::DistinctAgg {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            state_table_id: self.state.table_id.clone(),
            group_key: self.group_key.clone(),
            value_column: self.value_column.clone(),
            agg: self.agg,
        }
    }
}

/// A `ROW_NUMBER()` view over a source table.
///
/// The materialized view stores one row per source row — keyed by the
/// partition keys and the source primary keys — with its row number. A refresh
/// recomputes the affected partitions from the current source state, so an
/// order-value change, a delete or a partition move shifts the ranks of the
/// whole partition.
#[derive(Debug, Clone)]
pub struct WindowView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert); it must have a primary
    /// key.
    pub source: IvmTable,
    /// The materialized view table, created from [`window_mv_schema`] with the
    /// partition keys plus the source primary keys as merge key and the
    /// partition keys as bucket prefix.
    pub mv: IvmTable,
    /// The `PARTITION BY` columns.
    pub partition_keys: Vec<String>,
    /// The `ORDER BY` columns.
    pub order_keys: Vec<String>,
    /// The window function.
    pub function: WindowFunction,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl WindowView {
    /// A `ROW_NUMBER()` view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        partition_keys: Vec<String>,
        order_keys: Vec<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            partition_keys,
            order_keys,
            function: WindowFunction::RowNumber,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Window {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            partition_keys: self.partition_keys.clone(),
            order_keys: self.order_keys.clone(),
            function: self.function,
        }
    }
}

/// A `SEMI`/`ANTI` join view.
///
/// The materialized view holds the left rows that have (SEMI) or do not have
/// (ANTI) a match on `join_keys`, keyed by the left primary keys. A refresh
/// recomputes only the affected left rows: the left rows in the delta, plus
/// the left rows whose join keys changed on the right side. This supports
/// keyed sources with updates and deletes on both sides.
#[derive(Debug, Clone)]
pub struct SemiAntiView {
    /// The view id.
    pub view_id: String,
    /// The left source; it must have a primary key.
    pub left: IvmTable,
    /// The right source (append-only or keyed).
    pub right: IvmTable,
    /// The materialized view table, created from [`semi_anti_mv_schema`] with
    /// the left primary keys as merge key.
    pub mv: IvmTable,
    /// The equi-join key, present in both sources.
    pub join_keys: Vec<String>,
    /// `true` for `ANTI` (rows without a match), `false` for `SEMI`.
    pub anti: bool,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl SemiAntiView {
    /// A `SEMI` join view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        left: IvmTable,
        right: IvmTable,
        mv: IvmTable,
        join_keys: Vec<String>,
        anti: bool,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            left,
            right,
            mv,
            join_keys,
            anti,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::SemiAnti {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            join_keys: self.join_keys.clone(),
            anti: self.anti,
        }
    }
}

/// The schema of a value-count materialized view (MIN/MAX,
/// COUNT(DISTINCT), SUM(DISTINCT)): one row per group with the aggregated
/// value.
pub fn value_count_mv_schema(group_key: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(group_key, DataType::Int64, false),
        Field::new(IVM_VALUE_COLUMN, DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]))
}

/// The schema of a [`MinMaxView`] materialized view. Alias of
/// [`value_count_mv_schema`].
pub fn min_max_mv_schema(group_key: &str) -> SchemaRef {
    value_count_mv_schema(group_key)
}

/// The schema of a [`DistinctAggView`] materialized view. Alias of
/// [`value_count_mv_schema`].
pub fn distinct_agg_mv_schema(group_key: &str) -> SchemaRef {
    value_count_mv_schema(group_key)
}

/// The schema of a [`WindowView`] materialized view: the partition keys, the
/// source primary keys, the row number, the row kind and the epoch.
pub fn window_mv_schema(partition_keys: &[String], row_keys: &[String]) -> SchemaRef {
    let mut fields = Vec::new();
    for column in partition_keys.iter().chain(row_keys.iter()) {
        fields.push(Field::new(column, DataType::Int64, false));
    }
    fields.push(Field::new(IVM_ROW_NUMBER_COLUMN, DataType::Int64, false));
    fields.push(Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false));
    fields.push(Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

/// The schema of a [`SemiAntiView`] materialized view: the left columns plus
/// the row kind and the epoch.
pub fn semi_anti_mv_schema(left_schema: &Schema) -> SchemaRef {
    let mut fields = left_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        IVM_ROW_KINDS_COLUMN,
        DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Arc::new(Schema::new(fields))
}

/// The schema of a value-count state table: `(group, value) -> count`.
///
/// Create it with `(group_key, value)` as primary keys and the group key as its
/// bucket prefix so a group can be probed by key.
pub fn value_count_state_schema(group_key: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(group_key, DataType::Int64, false),
        Field::new(IVM_VALUE_COLUMN, DataType::Int64, false),
        Field::new(IVM_VALUE_COUNT_COLUMN, DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]))
}

/// The schema of the [`MinMaxView`] value-count state table. Alias of
/// [`value_count_state_schema`].
pub fn min_max_state_schema(group_key: &str) -> SchemaRef {
    value_count_state_schema(group_key)
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
    /// Append-only sources are aggregated directly. A source with a primary key
    /// is treated as an upsert stream: the new version of every changed row is
    /// retracted against the row's state at the window start, so updates and
    /// `rowKinds='delete'` rows are handled without a full source scan.
    ///
    /// An update/delete commit that the changelog cannot express incrementally
    /// (a missing baseline) is reported as an error so the caller can rebuild.
    pub async fn refresh_sum_count(&self, view: &SumCountView) -> Result<Option<i64>> {
        self.register_view(view).await?;

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
        let delta = if view.source.primary_keys.is_empty() {
            aggregate_groups(view, delta_batches).await?
        } else {
            let old_batches = view
                .source
                .read_as_of(&self.client, window.before_timestamp)
                .await?;
            aggregate_upsert_delta(view, delta_batches, old_batches).await?
        };
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

    /// Persist a min/max view spec (idempotent).
    pub async fn register_min_max_view(&self, view: &MinMaxView) -> Result<()> {
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a distinct aggregate view spec (idempotent).
    pub async fn register_distinct_agg_view(&self, view: &DistinctAggView) -> Result<()> {
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a window view spec (idempotent).
    pub async fn register_window_view(&self, view: &WindowView) -> Result<()> {
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a semi/anti join view spec (idempotent).
    pub async fn register_semi_anti_view(&self, view: &SemiAntiView) -> Result<()> {
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
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

    /// Refresh a `MIN`/`MAX` view through its value-count state table.
    pub async fn refresh_min_max(&self, view: &MinMaxView) -> Result<Option<i64>> {
        self.register_min_max_view(view).await?;
        self.refresh_value_count(&ValueCountView {
            view_id: &view.view_id,
            source: &view.source,
            mv: &view.mv,
            state: &view.state,
            group_key: &view.group_key,
            value_column: &view.value_column,
            agg: ValueAgg::from(view.min_max),
        })
        .await
    }

    /// Refresh a `COUNT(DISTINCT)`/`SUM(DISTINCT)` view through the shared
    /// value-count state table.
    pub async fn refresh_distinct_agg(
        &self,
        view: &DistinctAggView,
    ) -> Result<Option<i64>> {
        self.register_distinct_agg_view(view).await?;
        self.refresh_value_count(&ValueCountView {
            view_id: &view.view_id,
            source: &view.source,
            mv: &view.mv,
            state: &view.state,
            group_key: &view.group_key,
            value_column: &view.value_column,
            agg: ValueAgg::from(view.agg),
        })
        .await
    }

    /// Refresh a `ROW_NUMBER()` view by recomputing its affected partitions.
    ///
    /// A row number depends on every row of its partition, so the delta only
    /// selects which partitions to recompute: their current source state is
    /// ranked again and the MV rows that changed (including rows that
    /// disappeared or moved to another partition) are rewritten. The per-row
    /// epoch keeps a replay from applying a window twice.
    pub async fn refresh_window(&self, view: &WindowView) -> Result<Option<i64>> {
        self.register_window_view(view).await?;
        validate_window_view(view)?;

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
        let delta_rows = key_set(&delta_batches, &view.source.primary_keys)?;
        let mut affected = key_set(&delta_batches, &view.partition_keys)?;

        let mv_batches = view.mv.read_current(&self.client).await?;
        let current_mv = read_window_state(view, mv_batches)?;
        // A changed row may have left its previous partition, which then needs
        // a recomputation too.
        for row_key in &delta_rows {
            if let Some(entry) = current_mv.get(row_key) {
                affected.insert(entry.partition.clone());
            }
        }

        let source_batches = view.source.read_current(&self.client).await?;
        let computed = compute_row_numbers(view, source_batches, Some(&affected)).await?;

        let mut mv_rows = WindowRows::default();
        for (row_key, (partition, number)) in &computed {
            match current_mv.get(row_key) {
                Some(entry) if entry.epoch == epoch => continue,
                Some(entry) => {
                    mv_rows.push_delete(&entry.partition, row_key, entry.number, epoch)
                }
                None => {}
            }
            mv_rows.push_insert(partition, row_key, *number, epoch);
        }
        for (row_key, entry) in &current_mv {
            if entry.epoch == epoch {
                continue;
            }
            if affected.contains(&entry.partition) && !computed.contains_key(row_key) {
                mv_rows.push_delete(&entry.partition, row_key, entry.number, epoch);
            }
        }
        if !mv_rows.is_empty() {
            view.mv
                .append_batch(&self.client, mv_rows.into_batch(view)?)
                .await?;
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Refresh a `SEMI`/`ANTI` join by recomputing the affected left rows.
    ///
    /// The affected rows are the left rows in the delta plus the left rows
    /// whose join keys changed on the right side. For those rows the join is
    /// re-evaluated against both current states and the MV is rewritten
    /// (`delete(affected) + insert(current)`), so updates, deletes and join
    /// key changes on either side are handled. Rows already written by this
    /// epoch are skipped, which makes a replay a no-op.
    pub async fn refresh_semi_anti(&self, view: &SemiAntiView) -> Result<Option<i64>> {
        self.register_semi_anti_view(view).await?;
        validate_semi_anti_view(view)?;
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
            .begin_window(&view.view_id, &identity, &view.mv)
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

        let delta_left = view.left.read_files(left_window.added_files).await?;
        let delta_right = view.right.read_files(right_window.added_files).await?;
        let left_before = view
            .left
            .read_as_of(&self.client, left_window.before_timestamp)
            .await?;
        let left_now = view.left.read_current(&self.client).await?;
        let right_now = view.right.read_current(&self.client).await?;
        let mv_batches = view.mv.read_current(&self.client).await?;

        let context = SessionContext::new();
        let delta_left = dataframe(&context, delta_left, &view.left.schema)?;
        let delta_right = dataframe(&context, delta_right, &view.right.schema)?;
        let left_before = dataframe(&context, left_before, &view.left.schema)?;
        let left_now = filter_deletes(
            dataframe(&context, left_now, &view.left.schema)?,
            change_column(&view.left),
        )?;
        let right_now = filter_deletes(
            dataframe(&context, right_now, &view.right.schema)?,
            change_column(&view.right),
        )?;
        let mv = dataframe(&context, mv_batches, &view.mv.schema)?;

        let join_keys = view
            .join_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let left_key_names = view
            .left
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let left_key_exprs = view
            .left
            .primary_keys
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();

        let affected_from_left = delta_left.select(left_key_exprs.clone())?.distinct()?;
        let affected_from_right = left_before
            .clone()
            .join(
                delta_right,
                JoinType::LeftSemi,
                &join_keys,
                &join_keys,
                None,
            )?
            .select(left_key_exprs.clone())?
            .distinct()?;
        let affected = affected_from_left.union(affected_from_right)?.distinct()?;

        let matched_now = left_now
            .clone()
            .join(right_now, JoinType::LeftSemi, &join_keys, &join_keys, None)?
            .select(left_key_exprs.clone())?
            .distinct()?;

        let output_columns = view
            .left
            .schema
            .fields()
            .iter()
            .map(|field| col(field.name().as_str()))
            .collect::<Vec<_>>();
        let join_type = if view.anti {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        let insert_base = left_now
            .join(
                affected.clone(),
                JoinType::LeftSemi,
                &left_key_names,
                &left_key_names,
                None,
            )?
            .join(
                matched_now,
                join_type,
                &left_key_names,
                &left_key_names,
                None,
            )?;
        let mv_epoch_keys = mv
            .clone()
            .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
            .select(left_key_exprs.clone())?
            .distinct()?;
        let inserts = insert_base
            .select(output_columns.clone())?
            .join(
                mv_epoch_keys,
                JoinType::LeftAnti,
                &left_key_names,
                &left_key_names,
                None,
            )?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let deletes = mv
            .join(
                affected,
                JoinType::LeftSemi,
                &left_key_names,
                &left_key_names,
                None,
            )?
            .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
            .select(output_columns)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

        for batch in inserts.union(deletes)?.collect().await? {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Refresh a value-count view.
    ///
    /// The window delta is turned into `(group, value) -> count` changes (new
    /// rows add, the previous version of an upserted row retracts), applied to
    /// the state table, and the affected groups are recomputed and written to
    /// the MV. State rows carry the window epoch, so a replay after a crash
    /// between the state and the MV writes is a no-op for the state.
    async fn refresh_value_count(
        &self,
        view: &ValueCountView<'_>,
    ) -> Result<Option<i64>> {
        let window = self
            .collect_source_window(view.view_id, view.source)
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }

        let record = match self
            .begin_window(view.view_id, &window.identity, view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;

        let delta_batches = view.source.read_files(window.added_files).await?;
        let mut count_changes = count_rows_by_group_value(view, &delta_batches).await?;
        if !view.source.primary_keys.is_empty() {
            let old_batches = view
                .source
                .read_as_of(&self.client, window.before_timestamp)
                .await?;
            subtract_changed_old_counts(
                view,
                &delta_batches,
                &old_batches,
                &mut count_changes,
            )
            .await?;
        }
        count_changes.retain(|_, count| *count != 0);

        let state_batches = view.state.read_current(&self.client).await?;
        let mut state = read_value_count_state(view, state_batches)?;

        let mut state_rows = StateDeltaRows::default();
        for ((group, value), delta) in &count_changes {
            let key = (*group, *value);
            if state.get(&key).is_some_and(|entry| entry.epoch == epoch) {
                // The state already carries this window (a crashed attempt
                // wrote it before the MV was committed).
                continue;
            }
            let old_count = state
                .get(&key)
                .filter(|entry| entry.row_kinds == "insert")
                .map(|entry| entry.count)
                .unwrap_or(0);
            let new_count = old_count + delta;
            if old_count > 0 {
                state_rows.push_delete(key, old_count, epoch);
            }
            if new_count > 0 {
                state_rows.push_insert(key, new_count, epoch);
                state.insert(
                    key,
                    StateEntry {
                        count: new_count,
                        row_kinds: "insert".to_string(),
                        epoch,
                    },
                );
            } else {
                state.remove(&key);
            }
        }
        if !state_rows.is_empty() {
            view.state
                .append_batch(&self.client, state_rows.into_batch(view.state)?)
                .await?;
        }

        let values_by_group = values_by_group(&state);
        let mv_batches = view.mv.read_current(&self.client).await?;
        let current_mv = read_value_count_mv(view, mv_batches)?;
        let mut mv_rows = ValueRows::default();
        for group in count_changes
            .keys()
            .map(|(group, _)| *group)
            .collect::<std::collections::HashSet<_>>()
        {
            let new_value = view.agg.apply(values_by_group.get(&group));
            match current_mv.get(&group) {
                Some(entry) if entry.epoch == epoch => continue,
                Some(entry) => mv_rows.push_delete(group, entry.value, epoch),
                None => {}
            }
            if let Some(value) = new_value {
                mv_rows.push_insert(group, value, epoch);
            }
        }
        if !mv_rows.is_empty() {
            view.mv
                .append_batch(&self.client, mv_rows.into_batch(view.mv)?)
                .await?;
        }

        let mv_versions = output_partition_versions(&self.client, view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(view.view_id, window.cursors).await?;
        Ok(Some(epoch))
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

    /// Rebuild a `MIN`/`MAX` view from the full source state.
    pub async fn rebuild_min_max(&self, view: &MinMaxView) -> Result<i64> {
        self.register_min_max_view(view).await?;
        self.rebuild_value_count(&ValueCountView {
            view_id: &view.view_id,
            source: &view.source,
            mv: &view.mv,
            state: &view.state,
            group_key: &view.group_key,
            value_column: &view.value_column,
            agg: ValueAgg::from(view.min_max),
        })
        .await
    }

    /// Rebuild a `COUNT(DISTINCT)`/`SUM(DISTINCT)` view from the full source
    /// state.
    pub async fn rebuild_distinct_agg(&self, view: &DistinctAggView) -> Result<i64> {
        self.register_distinct_agg_view(view).await?;
        self.rebuild_value_count(&ValueCountView {
            view_id: &view.view_id,
            source: &view.source,
            mv: &view.mv,
            state: &view.state,
            group_key: &view.group_key,
            value_column: &view.value_column,
            agg: ValueAgg::from(view.agg),
        })
        .await
    }

    /// Rebuild a `ROW_NUMBER()` view from the full source state.
    pub async fn rebuild_window(&self, view: &WindowView) -> Result<i64> {
        self.register_window_view(view).await?;
        validate_window_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
        let computed = compute_row_numbers(view, baseline.batches, None).await?;

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

        let mut rows = WindowRows::default();
        for (row_key, (partition, number)) in &computed {
            rows.push_insert(partition, row_key, *number, epoch);
        }
        if !rows.is_empty() {
            view.mv
                .append_batch(&self.client, rows.into_batch(view)?)
                .await?;
        }

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

    /// Rebuild a `SEMI`/`ANTI` join from the full state of both sources.
    pub async fn rebuild_semi_anti(&self, view: &SemiAntiView) -> Result<i64> {
        self.register_semi_anti_view(view).await?;
        validate_semi_anti_view(view)?;
        self.ensure_unpartitioned(&view.left).await?;
        self.ensure_unpartitioned(&view.right).await?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let left_baseline = self.source_baseline(&view.left).await?;
        let right_baseline = self.source_baseline(&view.right).await?;
        let to_versions = left_baseline
            .to_versions
            .iter()
            .chain(right_baseline.to_versions.iter())
            .cloned()
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
        let left = filter_deletes(
            dataframe(&context, left_baseline.batches, &view.left.schema)?,
            change_column(&view.left),
        )?;
        let right = filter_deletes(
            dataframe(&context, right_baseline.batches, &view.right.schema)?,
            change_column(&view.right),
        )?;
        let join_keys = view
            .join_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let join_type = if view.anti {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        let output_columns = view
            .left
            .schema
            .fields()
            .iter()
            .map(|field| col(field.name().as_str()))
            .collect::<Vec<_>>();
        let rows = left
            .join(right, join_type, &join_keys, &join_keys, None)?
            .select(output_columns)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        for batch in rows.collect().await? {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
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

    /// Rebuild a value-count view.
    ///
    /// Both the value-count state table and the MV are truncated and refilled
    /// from the current source state, published as `rebuild:<generation>`.
    async fn rebuild_value_count(&self, view: &ValueCountView<'_>) -> Result<i64> {
        self.metadata
            .set_view_status(view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(view.view_id).await?;
        self.metadata.delete_cursors(view.view_id).await?;
        view.mv.truncate(&self.client).await?;
        view.state.truncate(&self.client).await?;

        let baseline = self.source_baseline(view.source).await?;
        let counts = count_rows_by_group_value(view, &baseline.batches).await?;

        let mv_versions_before = output_partition_versions(&self.client, view.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                view.view_id,
                &format!("rebuild:{generation}"),
                &baseline.to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                self.advance_cursors(view.view_id, baseline.cursors).await?;
                self.metadata
                    .set_view_status(view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;

        let mut state = HashMap::new();
        let mut state_rows = StateDeltaRows::default();
        for ((group, value), count) in &counts {
            if *count <= 0 {
                continue;
            }
            state_rows.push_insert((*group, *value), *count, epoch);
            state.insert(
                (*group, *value),
                StateEntry {
                    count: *count,
                    row_kinds: "insert".to_string(),
                    epoch,
                },
            );
        }
        if !state_rows.is_empty() {
            view.state
                .append_batch(&self.client, state_rows.into_batch(view.state)?)
                .await?;
        }

        let values_by_group = values_by_group(&state);
        let mut mv_rows = ValueRows::default();
        let mut groups = values_by_group.keys().copied().collect::<Vec<_>>();
        groups.sort_unstable();
        for group in groups {
            if let Some(value) = view.agg.apply(values_by_group.get(&group)) {
                mv_rows.push_insert(group, value, epoch);
            }
        }
        if !mv_rows.is_empty() {
            view.mv
                .append_batch(&self.client, mv_rows.into_batch(view.mv)?)
                .await?;
        }

        let mv_versions = output_partition_versions(&self.client, view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(view.view_id, baseline.cursors).await?;
        self.metadata
            .set_view_status(view.view_id, "active")
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

/// Column names are case sensitive; `col()` would normalize the identifier to
/// lower case, so the column is built from its exact name.
fn column_expr(name: &str) -> datafusion::logical_expr::Expr {
    datafusion::logical_expr::Expr::Column(datafusion::common::Column::from_name(name))
}

/// The CDC change column of a source, when the table declares one and it is
/// part of the schema. `insert` / `delete` values are interpreted as changes;
/// update markers collapse through the primary-key merge before we inspect
/// them.
fn change_column(source: &IvmTable) -> Option<&str> {
    if let Some(column) = source.cdc_column.as_deref() {
        return source.schema.field_with_name(column).ok().map(|_| column);
    }
    // Sources written through the internal writer carry `rowKinds` even when
    // the table does not declare a CDC column.
    source
        .schema
        .field_with_name(IVM_ROW_KINDS_COLUMN)
        .ok()
        .map(|_| IVM_ROW_KINDS_COLUMN)
}

/// Drop `delete` rows from a source frame when the source has a change column.
fn filter_deletes(frame: DataFrame, change_column: Option<&str>) -> Result<DataFrame> {
    match change_column {
        Some(column) => Ok(frame.filter(column_expr(column).not_eq(lit("delete")))?),
        None => Ok(frame),
    }
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
    let frame = filter_deletes(frame, change_column(&view.source))?;
    aggregate_dataframe(view, frame).await
}

/// Aggregate one [`DataFrame`] into `group_key -> (sum, count)`.
async fn aggregate_dataframe(
    view: &SumCountView,
    frame: DataFrame,
) -> Result<HashMap<i64, (i64, i64)>> {
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

/// The delta of an upsert source:
/// `aggregate(new rows) - aggregate(old rows whose key changed)`.
///
/// The changelog of a keyed source contains the new version of every changed
/// row but no retraction of its previous version, so the previous version is
/// read as of the window start and matched by primary key. Rows whose
/// `rowKinds` says `delete` only contribute their retraction.
async fn aggregate_upsert_delta(
    view: &SumCountView,
    delta: Vec<RecordBatch>,
    old: Vec<RecordBatch>,
) -> Result<HashMap<i64, (i64, i64)>> {
    if delta.is_empty() {
        return Ok(HashMap::new());
    }

    let context = SessionContext::new();
    let delta_frame = context.read_batches(delta)?;
    let new_rows = filter_deletes(delta_frame.clone(), change_column(&view.source))?;
    let mut delta_groups = aggregate_dataframe(view, new_rows).await?;

    if !old.is_empty() {
        // Deleted keys survive merge-on-read as tombstones; they must not be
        // retracted again.
        let old_frame =
            filter_deletes(context.read_batches(old)?, change_column(&view.source))?;
        let pk_columns = view
            .source
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let changed_keys = delta_frame
            .select(
                pk_columns
                    .iter()
                    .map(|column| col(*column))
                    .collect::<Vec<_>>(),
            )?
            .distinct()?;
        let changed_old = old_frame.join(
            changed_keys,
            JoinType::LeftSemi,
            &pk_columns,
            &pk_columns,
            None,
        )?;
        for (group, (sum, count)) in aggregate_dataframe(view, changed_old).await? {
            let entry = delta_groups.entry(group).or_insert((0, 0));
            entry.0 -= sum;
            entry.1 -= count;
        }
    }

    Ok(delta_groups)
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

/// One surviving row of the MIN/MAX value-count state table.
struct StateEntry {
    count: i64,
    row_kinds: String,
    epoch: i64,
}

/// One surviving row of a MIN/MAX materialized view.
struct MvEntry {
    value: i64,
    epoch: i64,
}

/// Read the value-count state.
fn read_value_count_state(
    view: &ValueCountView<'_>,
    batches: Vec<RecordBatch>,
) -> Result<HashMap<(i64, i64), StateEntry>> {
    let mut state = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let group_index = schema.index_of(view.group_key)?;
        let value_index = schema.index_of(IVM_VALUE_COLUMN)?;
        let count_index = schema.index_of(IVM_VALUE_COUNT_COLUMN)?;
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN)?;
        let epoch_index = schema.index_of(IVM_EPOCH_COLUMN)?;

        let groups = int64_column(&batch, group_index, view.group_key)?;
        let values = int64_column(&batch, value_index, IVM_VALUE_COLUMN)?;
        let counts = int64_column(&batch, count_index, IVM_VALUE_COUNT_COLUMN)?;
        let epochs = int64_column(&batch, epoch_index, IVM_EPOCH_COLUMN)?;
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| report!("{IVM_ROW_KINDS_COLUMN} must be a Utf8 column"))?;

        for row in 0..batch.num_rows() {
            state.insert(
                (groups.value(row), values.value(row)),
                StateEntry {
                    count: counts.value(row),
                    row_kinds: kinds.value(row).to_string(),
                    epoch: epochs.value(row),
                },
            );
        }
    }
    Ok(state)
}

/// Read the current state of a value-count materialized view.
fn read_value_count_mv(
    view: &ValueCountView<'_>,
    batches: Vec<RecordBatch>,
) -> Result<HashMap<i64, MvEntry>> {
    let mut mv = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let group_index = schema.index_of(view.group_key)?;
        let value_index = schema.index_of(IVM_VALUE_COLUMN)?;
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN)?;
        let epoch_index = schema.index_of(IVM_EPOCH_COLUMN)?;

        let groups = int64_column(&batch, group_index, view.group_key)?;
        let values = int64_column(&batch, value_index, IVM_VALUE_COLUMN)?;
        let epochs = int64_column(&batch, epoch_index, IVM_EPOCH_COLUMN)?;
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| report!("{IVM_ROW_KINDS_COLUMN} must be a Utf8 column"))?;

        for row in 0..batch.num_rows() {
            let group = groups.value(row);
            if kinds.value(row) == "insert" {
                mv.insert(
                    group,
                    MvEntry {
                        value: values.value(row),
                        epoch: epochs.value(row),
                    },
                );
            } else {
                mv.remove(&group);
            }
        }
    }
    Ok(mv)
}

/// `(group, value) -> count` over the non-delete rows of the batches.
async fn count_rows_by_group_value(
    view: &ValueCountView<'_>,
    batches: &[RecordBatch],
) -> Result<HashMap<(i64, i64), i64>> {
    if batches.is_empty() {
        return Ok(HashMap::new());
    }

    let context = SessionContext::new();
    let frame = context.read_batches(batches.to_vec())?;
    let frame = filter_deletes(frame, change_column(view.source))?;
    let aggregated = frame.aggregate(
        vec![col(view.group_key), col(view.value_column)],
        vec![count(lit(1_i64)).alias("rows")],
    )?;

    let mut counts = HashMap::new();
    for batch in aggregated.collect().await? {
        let groups = int64_column(&batch, 0, view.group_key)?;
        let values = int64_column(&batch, 1, view.value_column)?;
        let rows = int64_column(&batch, 2, "rows")?;
        for row in 0..batch.num_rows() {
            counts.insert((groups.value(row), values.value(row)), rows.value(row));
        }
    }
    Ok(counts)
}

/// Retract the previous version of every key in `delta` from `counts`.
async fn subtract_changed_old_counts(
    view: &ValueCountView<'_>,
    delta: &[RecordBatch],
    old: &[RecordBatch],
    counts: &mut HashMap<(i64, i64), i64>,
) -> Result<()> {
    if old.is_empty() || delta.is_empty() {
        return Ok(());
    }

    let context = SessionContext::new();
    let delta_frame = context.read_batches(delta.to_vec())?;
    let old_frame = context.read_batches(old.to_vec())?;
    let pk_columns = view
        .source
        .primary_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let changed_keys = delta_frame
        .select(
            pk_columns
                .iter()
                .map(|column| col(*column))
                .collect::<Vec<_>>(),
        )?
        .distinct()?;
    let changed_old = old_frame.join(
        changed_keys,
        JoinType::LeftSemi,
        &pk_columns,
        &pk_columns,
        None,
    )?;
    for (key, count) in
        count_rows_by_group_value(view, &changed_old.collect().await?).await?
    {
        *counts.entry(key).or_insert(0) -= count;
    }
    Ok(())
}

/// The positive-count values of every group in the state table.
fn values_by_group(
    state: &HashMap<(i64, i64), StateEntry>,
) -> HashMap<i64, BTreeMap<i64, i64>> {
    let mut by_group = HashMap::new();
    for ((group, value), entry) in state {
        if entry.row_kinds == "insert" && entry.count > 0 {
            by_group
                .entry(*group)
                .or_insert_with(BTreeMap::new)
                .insert(*value, entry.count);
        }
    }
    by_group
}

/// Rows to write into the MIN/MAX value-count state table.
#[derive(Default)]
struct StateDeltaRows {
    groups: Vec<i64>,
    values: Vec<i64>,
    counts: Vec<i64>,
    kinds: Vec<&'static str>,
    epochs: Vec<i64>,
}

impl StateDeltaRows {
    fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    fn push_delete(&mut self, (group, value): (i64, i64), count: i64, epoch: i64) {
        self.groups.push(group);
        self.values.push(value);
        self.counts.push(count);
        self.kinds.push("delete");
        self.epochs.push(epoch);
    }

    fn push_insert(&mut self, (group, value): (i64, i64), count: i64, epoch: i64) {
        self.groups.push(group);
        self.values.push(value);
        self.counts.push(count);
        self.kinds.push("insert");
        self.epochs.push(epoch);
    }

    fn into_batch(self, table: &IvmTable) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            table.schema.clone(),
            vec![
                Arc::new(Int64Array::from(self.groups)),
                Arc::new(Int64Array::from(self.values)),
                Arc::new(Int64Array::from(self.counts)),
                Arc::new(StringArray::from(self.kinds)),
                Arc::new(Int64Array::from(self.epochs)),
            ],
        )?)
    }
}

/// Rows to write into a MIN/MAX materialized view.
#[derive(Default)]
struct ValueRows {
    groups: Vec<i64>,
    values: Vec<i64>,
    kinds: Vec<&'static str>,
    epochs: Vec<i64>,
}

impl ValueRows {
    fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    fn push_delete(&mut self, group: i64, value: i64, epoch: i64) {
        self.groups.push(group);
        self.values.push(value);
        self.kinds.push("delete");
        self.epochs.push(epoch);
    }

    fn push_insert(&mut self, group: i64, value: i64, epoch: i64) {
        self.groups.push(group);
        self.values.push(value);
        self.kinds.push("insert");
        self.epochs.push(epoch);
    }

    fn into_batch(self, table: &IvmTable) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            table.schema.clone(),
            vec![
                Arc::new(Int64Array::from(self.groups)),
                Arc::new(Int64Array::from(self.values)),
                Arc::new(StringArray::from(self.kinds)),
                Arc::new(Int64Array::from(self.epochs)),
            ],
        )?)
    }
}

/// Validate that a window view can be maintained.
fn validate_window_view(view: &WindowView) -> Result<()> {
    if view.source.primary_keys.is_empty() {
        return Err(report!(
            "window view {} needs a source with a primary key",
            view.view_id
        ));
    }
    if view.partition_keys.is_empty() || view.order_keys.is_empty() {
        return Err(report!(
            "window view {} needs partition and order keys",
            view.view_id
        ));
    }
    for column in view
        .partition_keys
        .iter()
        .chain(view.source.primary_keys.iter())
    {
        let field = view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "window view {}: column {column} is not in the source",
                view.view_id
            )
        })?;
        if *field.data_type() != DataType::Int64 {
            return Err(report!(
                "window view {}: partition/key column {column} must be Int64",
                view.view_id
            ));
        }
    }
    for column in &view.order_keys {
        view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "window view {}: order column {column} is not in the source",
                view.view_id
            )
        })?;
    }
    Ok(())
}

/// The distinct `columns`-tuples of the batches.
fn key_set(batches: &[RecordBatch], columns: &[String]) -> Result<HashSet<Vec<i64>>> {
    let mut keys = HashSet::new();
    for batch in batches {
        let mut arrays = Vec::with_capacity(columns.len());
        for column in columns {
            let index = batch.schema().index_of(column)?;
            arrays.push(int64_column(batch, index, column)?);
        }
        for row in 0..batch.num_rows() {
            keys.insert(arrays.iter().map(|array| array.value(row)).collect());
        }
    }
    Ok(keys)
}

/// One surviving row of a window materialized view.
struct WindowEntry {
    partition: Vec<i64>,
    number: i64,
    epoch: i64,
}

/// Read the current state of a window materialized view.
fn read_window_state(
    view: &WindowView,
    batches: Vec<RecordBatch>,
) -> Result<HashMap<Vec<i64>, WindowEntry>> {
    let mut state = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let number_index = schema.index_of(IVM_ROW_NUMBER_COLUMN)?;
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN)?;
        let epoch_index = schema.index_of(IVM_EPOCH_COLUMN)?;

        let mut partition_arrays = Vec::with_capacity(view.partition_keys.len());
        for column in &view.partition_keys {
            partition_arrays.push(int64_column(
                &batch,
                schema.index_of(column)?,
                column,
            )?);
        }
        let mut row_arrays = Vec::with_capacity(view.source.primary_keys.len());
        for column in &view.source.primary_keys {
            row_arrays.push(int64_column(&batch, schema.index_of(column)?, column)?);
        }
        let numbers = int64_column(&batch, number_index, IVM_ROW_NUMBER_COLUMN)?;
        let epochs = int64_column(&batch, epoch_index, IVM_EPOCH_COLUMN)?;
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| report!("{IVM_ROW_KINDS_COLUMN} must be a Utf8 column"))?;

        for row in 0..batch.num_rows() {
            let row_key = row_arrays
                .iter()
                .map(|array| array.value(row))
                .collect::<Vec<_>>();
            if kinds.value(row) == "insert" {
                state.insert(
                    row_key,
                    WindowEntry {
                        partition: partition_arrays
                            .iter()
                            .map(|array| array.value(row))
                            .collect(),
                        number: numbers.value(row),
                        epoch: epochs.value(row),
                    },
                );
            } else {
                state.remove(&row_key);
            }
        }
    }
    Ok(state)
}

/// Rank the source rows with `ROW_NUMBER()`.
///
/// `filter_partitions` keeps only the partitions that have to be recomputed;
/// `None` ranks every partition (a rebuild).
async fn compute_row_numbers(
    view: &WindowView,
    batches: Vec<RecordBatch>,
    filter_partitions: Option<&HashSet<Vec<i64>>>,
) -> Result<HashMap<Vec<i64>, (Vec<i64>, i64)>> {
    if batches.is_empty() {
        return Ok(HashMap::new());
    }

    let context = SessionContext::new();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(datafusion::datasource::memory::MemTable::try_new(
            view.source.schema.clone(),
            vec![batches],
        )?);
    context.register_table("src", table)?;

    let quoted = |columns: &[String]| {
        columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let order_columns = view
        .order_keys
        .iter()
        .chain(view.source.primary_keys.iter())
        .cloned()
        .collect::<Vec<_>>();
    let filter = match change_column(&view.source) {
        Some(column) => format!(" where \"{column}\" != 'delete'"),
        None => String::new(),
    };
    let frame = context
        .sql(&format!(
            "select {}, {}, cast(row_number() over (partition by {} order by {}) as bigint) as \"{IVM_ROW_NUMBER_COLUMN}\" from src{}",
            quoted(&view.source.primary_keys),
            quoted(&view.partition_keys),
            quoted(&view.partition_keys),
            quoted(&order_columns),
            filter,
        ))
        .await?;

    let row_key_count = view.source.primary_keys.len();
    let mut computed = HashMap::new();
    for batch in frame.collect().await? {
        let mut row_arrays = Vec::with_capacity(row_key_count);
        for (index, column) in view.source.primary_keys.iter().enumerate() {
            row_arrays.push(int64_column(&batch, index, column)?);
        }
        let mut partition_arrays = Vec::with_capacity(view.partition_keys.len());
        for (index, column) in view.partition_keys.iter().enumerate() {
            partition_arrays.push(int64_column(&batch, row_key_count + index, column)?);
        }
        let numbers = int64_column(
            &batch,
            row_key_count + view.partition_keys.len(),
            IVM_ROW_NUMBER_COLUMN,
        )?;
        for row in 0..batch.num_rows() {
            let partition = partition_arrays
                .iter()
                .map(|array| array.value(row))
                .collect::<Vec<_>>();
            if filter_partitions.is_some_and(|filter| !filter.contains(&partition)) {
                continue;
            }
            let row_key = row_arrays
                .iter()
                .map(|array| array.value(row))
                .collect::<Vec<_>>();
            computed.insert(row_key, (partition, numbers.value(row)));
        }
    }
    Ok(computed)
}

/// Rows to write into a window materialized view.
#[derive(Default)]
struct WindowRows {
    partitions: Vec<Vec<i64>>,
    row_keys: Vec<Vec<i64>>,
    numbers: Vec<i64>,
    kinds: Vec<&'static str>,
    epochs: Vec<i64>,
}

impl WindowRows {
    fn is_empty(&self) -> bool {
        self.row_keys.is_empty()
    }

    fn push_delete(
        &mut self,
        partition: &[i64],
        row_key: &[i64],
        number: i64,
        epoch: i64,
    ) {
        self.push(partition, row_key, number, "delete", epoch);
    }

    fn push_insert(
        &mut self,
        partition: &[i64],
        row_key: &[i64],
        number: i64,
        epoch: i64,
    ) {
        self.push(partition, row_key, number, "insert", epoch);
    }

    fn push(
        &mut self,
        partition: &[i64],
        row_key: &[i64],
        number: i64,
        kind: &'static str,
        epoch: i64,
    ) {
        self.partitions.push(partition.to_vec());
        self.row_keys.push(row_key.to_vec());
        self.numbers.push(number);
        self.kinds.push(kind);
        self.epochs.push(epoch);
    }

    fn into_batch(self, view: &WindowView) -> Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(
            view.partition_keys.len() + view.source.primary_keys.len() + 3,
        );
        for index in 0..view.partition_keys.len() {
            arrays.push(Arc::new(Int64Array::from(
                self.partitions
                    .iter()
                    .map(|partition| partition[index])
                    .collect::<Vec<_>>(),
            )));
        }
        for index in 0..view.source.primary_keys.len() {
            arrays.push(Arc::new(Int64Array::from(
                self.row_keys
                    .iter()
                    .map(|row_key| row_key[index])
                    .collect::<Vec<_>>(),
            )));
        }
        arrays.push(Arc::new(Int64Array::from(self.numbers)));
        arrays.push(Arc::new(StringArray::from(self.kinds)));
        arrays.push(Arc::new(Int64Array::from(self.epochs)));
        Ok(RecordBatch::try_new(view.mv.schema.clone(), arrays)?)
    }
}

/// Build a [`DataFrame`] over the batches (possibly empty) with `schema`.
fn dataframe(
    context: &SessionContext,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<DataFrame> {
    let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        datafusion::datasource::memory::MemTable::try_new(schema.clone(), vec![batches])?,
    );
    Ok(context.read_table(table)?)
}

/// Validate that a semi/anti view can be maintained.
fn validate_semi_anti_view(view: &SemiAntiView) -> Result<()> {
    if view.left.primary_keys.is_empty() {
        return Err(report!(
            "semi/anti view {} needs a left source with a primary key",
            view.view_id
        ));
    }
    if view.join_keys.is_empty() {
        return Err(report!(
            "semi/anti view {} needs at least one join key",
            view.view_id
        ));
    }
    for key in &view.join_keys {
        view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "semi/anti view {}: join key {key} is not in the left source",
                view.view_id
            )
        })?;
        view.right.schema.field_with_name(key).map_err(|_| {
            report!(
                "semi/anti view {}: join key {key} is not in the right source",
                view.view_id
            )
        })?;
    }
    Ok(())
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
