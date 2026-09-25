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

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::prelude::{DataFrame, Expr, JoinType, SessionContext, col, lit};
use lakesoul_io::constant::DEFAULT_PARTITION_DESC;
use lakesoul_metadata::MetaDataClient;
use rootcause::report;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::metadata::{
    BeginEpoch, Consumer, Cursor, EpochRecord, IvmMetadata, PartitionVersion,
    SourceVersionRange, StateRole, StateTable,
};
use crate::table::{
    IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN, IvmTable, IvmTableOptions, create_ivm_table,
};

/// The `SUM` column of a [`sum_count_mv_schema`] materialized view.
pub const IVM_SUM_COLUMN: &str = "sum_v";
/// The `COUNT` column of a [`sum_count_mv_schema`] materialized view.
pub const IVM_COUNT_COLUMN: &str = "count_v";
/// The count of non-NULL values in a [`sum_count_mv_schema`] state row. It is
/// what lets an all-NULL group sum to NULL instead of 0, matching SQL `SUM`.
pub const IVM_NONNULL_COUNT_COLUMN: &str = "__ivm_nonnull_count";
/// The value column of a [`min_max_mv_schema`] materialized view and of the
/// value-count state table.
pub const IVM_VALUE_COLUMN: &str = "value";
/// The value-count column of the MIN/MAX state table.
pub const IVM_VALUE_COUNT_COLUMN: &str = "value_count";
/// The row-number column of a `ROW_NUMBER()` [`WindowView`] materialized view.
pub const IVM_ROW_NUMBER_COLUMN: &str = "row_number";
/// The source index column of a [`UnionAllView`] materialized view.
pub const IVM_SOURCE_COLUMN: &str = "__ivm_source";
/// The internal rank column of a [`TopKView`] computation (not materialized).
const IVM_TOP_K_RANK_COLUMN: &str = "__ivm_rank";
/// The rank column of a `RANK()` [`WindowView`] materialized view.
pub const IVM_RANK_COLUMN: &str = "rank";
/// The rank column of a `DENSE_RANK()` [`WindowView`] materialized view.
pub const IVM_DENSE_RANK_COLUMN: &str = "dense_rank";

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowFunction {
    /// `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`. The source primary
    /// keys are appended to the ordering so ties are broken deterministically.
    #[default]
    RowNumber,
    /// `RANK() OVER (PARTITION BY ... ORDER BY ...)`. Ties share the smallest
    /// rank and the next rank is skipped.
    Rank,
    /// `DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)`. Ties share a rank
    /// and ranks are consecutive.
    DenseRank,
    /// `SUM(value_column) OVER (PARTITION BY ... [ORDER BY ...])`; without
    /// order keys the whole partition is summed, otherwise the SQL default
    /// running frame is used. The value is nullable.
    Sum,
    /// `COUNT(*)` or `COUNT(value_column) OVER (PARTITION BY ... [ORDER BY
    /// ...])`. Never NULL.
    Count,
}

impl WindowFunction {
    /// The SQL window function name.
    pub fn sql_name(self) -> &'static str {
        match self {
            WindowFunction::RowNumber => "row_number",
            WindowFunction::Rank => "rank",
            WindowFunction::DenseRank => "dense_rank",
            WindowFunction::Sum => "sum",
            WindowFunction::Count => "count",
        }
    }

    /// The materialized view column holding the computed value.
    pub fn column_name(self) -> &'static str {
        match self {
            WindowFunction::RowNumber => IVM_ROW_NUMBER_COLUMN,
            WindowFunction::Rank => IVM_RANK_COLUMN,
            WindowFunction::DenseRank => IVM_DENSE_RANK_COLUMN,
            WindowFunction::Sum => IVM_SUM_COLUMN,
            WindowFunction::Count => IVM_COUNT_COLUMN,
        }
    }

    /// Whether the function aggregates a value column over the partition.
    pub fn is_aggregate(self) -> bool {
        matches!(self, WindowFunction::Sum | WindowFunction::Count)
    }

    /// Whether the source primary keys are appended to the ordering. Only
    /// `ROW_NUMBER` needs it: for `RANK`/`DENSE_RANK` appending them would
    /// break ties that must share a rank, and aggregate windows ignore the
    /// ordering of peers.
    fn breaks_ties_with_primary_keys(self) -> bool {
        matches!(self, WindowFunction::RowNumber)
    }
}

/// The persisted description of a view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
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
        /// The equi-join keys, present in both sources.
        #[serde(default, alias = "join_key", deserialize_with = "de_group_keys")]
        join_keys: Vec<String>,
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
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
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
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
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
        /// The `ORDER BY` columns; empty for whole-partition aggregates.
        order_keys: Vec<String>,
        /// The window function.
        #[serde(default)]
        function: WindowFunction,
        /// The aggregated column of a `SUM`/`COUNT` window function.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_column: Option<String>,
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
        /// Extra comparison conditions between a left and a right column.
        #[serde(default)]
        conditions: Vec<SemiAntiCondition>,
        /// The left columns materialized in the view; empty means all of them.
        #[serde(default)]
        output_columns: Vec<String>,
        /// `true` for `ANTI` (rows without a match), `false` for `SEMI`.
        anti: bool,
    },
    /// A projection (and optional filter) of one source.
    Row {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The projected source columns; empty means all of them.
        #[serde(default)]
        output_columns: Vec<String>,
        /// The filter conditions, all of which must hold.
        #[serde(default)]
        filters: Vec<FilterCondition>,
    },
    /// `UNION ALL` of several sources with the same schema.
    UnionAll {
        /// The view id.
        view_id: String,
        /// The source table ids, in output order.
        source_table_ids: Vec<String>,
        /// The materialized view table id.
        mv_table_id: String,
    },
    /// The top `limit` rows of every group, ordered by `order_keys`.
    TopK {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group (`PARTITION BY`) columns.
        group_keys: Vec<String>,
        /// The `ORDER BY` columns.
        order_keys: Vec<String>,
        /// The projected source columns; empty means all of them.
        #[serde(default)]
        output_columns: Vec<String>,
        /// How many rows to keep per group.
        limit: i64,
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
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The summed column; `None` counts rows only.
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
            group_keys: vec![group_key.into()],
            value_column,
            refresh_interval_ms: 0,
        }
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        value_column: Option<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            value_column,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::SumCount {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
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
        Field::new(IVM_NONNULL_COUNT_COLUMN, DataType::Int64, false),
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
    /// The equi-join keys, present in both sources (any equality-comparable
    /// types).
    pub join_keys: Vec<String>,
    /// The payload column of the left source (any type).
    pub left_value: String,
    /// The payload column of the right source (any type).
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
        Self::new_with_join_keys(
            view_id,
            left,
            right,
            output,
            vec![join_key.into()],
            left_value,
            right_value,
        )
    }

    /// A new join view over several equi-join keys.
    pub fn new_with_join_keys(
        view_id: impl Into<String>,
        left: IvmTable,
        right: IvmTable,
        output: IvmTable,
        join_keys: Vec<String>,
        left_value: impl Into<String>,
        right_value: impl Into<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            left,
            right,
            output,
            join_keys,
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
            join_keys: self.join_keys.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
        }
    }
}

/// The schema of a [`JoinView`] output, deriving the join key and payload
/// types from the source schemas.
pub fn join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    join_keys: &[String],
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    for key in join_keys {
        fields.push(Arc::new(Field::new(
            key,
            field_type(left_schema, key)?,
            false,
        )));
    }
    fields.push(Arc::new(Field::new(
        "left_value",
        field_type(left_schema, left_value)?,
        left_schema
            .field_with_name(left_value)
            .map(|field| field.is_nullable())
            .unwrap_or(true),
    )));
    fields.push(Arc::new(Field::new(
        "right_value",
        field_type(right_schema, right_value)?,
        right_schema
            .field_with_name(right_value)
            .map(|field| field.is_nullable())
            .unwrap_or(true),
    )));
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    let _ = right_schema;
    Ok(Arc::new(Schema::new(fields)))
}

/// The hidden column holding a left primary key in a keyed join output.
fn left_pk_alias(key: &str) -> String {
    format!("__left_pk_{key}")
}

/// The hidden column holding a right primary key in a keyed join output.
fn right_pk_alias(key: &str) -> String {
    format!("__right_pk_{key}")
}

/// The primary keys of a keyed [`JoinView`] output, in schema order.
pub fn keyed_join_output_primary_keys(
    left_primary_keys: &[String],
    right_primary_keys: &[String],
) -> Vec<String> {
    left_primary_keys
        .iter()
        .map(|key| left_pk_alias(key))
        .chain(right_primary_keys.iter().map(|key| right_pk_alias(key)))
        .collect()
}

/// The schema of a [`JoinView`] output over two keyed (upsert/delete) sources.
///
/// The output is keyed by the pair of row identities (`__left_pk_*`,
/// `__right_pk_*`), so the view can retract and rewrite individual pairs as
/// either side changes. The join keys are non-nullable because NULL join keys
/// never match, and the payloads keep the nullability of their source columns.
pub fn keyed_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "a keyed join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for key in join_keys {
        fields.push(Arc::new(Field::new(
            key,
            field_type(left_schema, key)?,
            false,
        )));
    }
    fields.push(Arc::new(Field::new(
        "left_value",
        field_type(left_schema, left_value)?,
        left_schema
            .field_with_name(left_value)
            .map(|field| field.is_nullable())
            .unwrap_or(true),
    )));
    fields.push(Arc::new(Field::new(
        "right_value",
        field_type(right_schema, right_value)?,
        right_schema
            .field_with_name(right_value)
            .map(|field| field.is_nullable())
            .unwrap_or(true),
    )));
    for key in left_primary_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            left_pk_alias(key),
            field.data_type().clone(),
            false,
        )));
    }
    for key in right_primary_keys {
        let field = right_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            right_pk_alias(key),
            field.data_type().clone(),
            false,
        )));
    }
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
    Ok(Arc::new(Schema::new(fields)))
}

/// Accepts either a single `group_key` string or a `group_keys` array when
/// deserializing persisted view specs.
#[derive(Deserialize)]
#[serde(untagged)]
enum GroupKeysRepr {
    One(String),
    Many(Vec<String>),
}

fn de_group_keys<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<GroupKeysRepr>::deserialize(deserializer)? {
        None => Vec::new(),
        Some(GroupKeysRepr::One(key)) => vec![key],
        Some(GroupKeysRepr::Many(keys)) => keys,
    })
}

/// The aggregation a value-count view derives from a group's value counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueAgg {
    Min,
    Max,
    DistinctCount,
    DistinctSum,
}

impl ValueAgg {
    fn result_type(self, value_type: &DataType) -> Result<DataType> {
        match self {
            ValueAgg::Min | ValueAgg::Max => Ok(value_type.clone()),
            ValueAgg::DistinctCount => Ok(DataType::Int64),
            ValueAgg::DistinctSum => sum_result_type(value_type),
        }
    }

    fn sql(self, value_column: &str) -> String {
        let column = quote_ident(value_column);
        match self {
            ValueAgg::Min => format!("min({column})"),
            ValueAgg::Max => format!("max({column})"),
            // SQL distinct aggregates ignore NULL values; `count(distinct ...)`
            // also yields 0 for a group that only holds NULLs.
            ValueAgg::DistinctCount => format!("count(distinct {column})"),
            ValueAgg::DistinctSum => format!("sum(distinct {column})"),
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
    group_keys: &'a [String],
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
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The min/max value column.
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
            group_keys: vec![group_key.into()],
            value_column: value_column.into(),
            min_max,
            refresh_interval_ms: 0,
        }
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        state: IvmTable,
        group_keys: Vec<String>,
        value_column: impl Into<String>,
        min_max: MinMaxKind,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            state,
            group_keys,
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
            group_keys: self.group_keys.clone(),
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
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The distinct value column.
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
            group_keys: vec![group_key.into()],
            value_column: value_column.into(),
            agg,
            refresh_interval_ms: 0,
        }
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        state: IvmTable,
        group_keys: Vec<String>,
        value_column: impl Into<String>,
        agg: DistinctAggKind,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            state,
            group_keys,
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
            group_keys: self.group_keys.clone(),
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
    /// The aggregated column of a `SUM`/`COUNT` window function.
    pub value_column: Option<String>,
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
        Self::new_with_function(
            view_id,
            source,
            mv,
            partition_keys,
            order_keys,
            WindowFunction::RowNumber,
        )
    }

    /// A window view for an explicit ranking function.
    pub fn new_with_function(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        partition_keys: Vec<String>,
        order_keys: Vec<String>,
        function: WindowFunction,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            partition_keys,
            order_keys,
            function,
            value_column: None,
            refresh_interval_ms: 0,
        }
    }

    /// An aggregate window view (`SUM`/`COUNT` over a partition, optionally
    /// running when `order_keys` is not empty).
    pub fn new_aggregate(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        partition_keys: Vec<String>,
        order_keys: Vec<String>,
        function: WindowFunction,
        value_column: Option<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            partition_keys,
            order_keys,
            function,
            value_column,
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
            value_column: self.value_column.clone(),
        }
    }
}

/// A comparison operator of a [`SemiAntiCondition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    /// `=`
    Eq,
    /// `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

/// A literal value of a [`FilterCondition`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum LiteralValue {
    /// `NULL` (only meaningful with `=` / `<>`).
    Null,
    /// A boolean.
    Bool(bool),
    /// An integer.
    Int(i64),
    /// A float.
    Float(f64),
    /// A string.
    String(String),
}

/// One filter condition of a [`RowView`]: `{column} {op} {value}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterCondition {
    /// The source column.
    pub column: String,
    /// The comparison operator.
    pub op: CompareOp,
    /// The literal value.
    pub value: LiteralValue,
}

/// One condition of a [`SemiAntiView`]:
/// `left.{left_column} {op} right.{right_column}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemiAntiCondition {
    /// The left column.
    pub left_column: String,
    /// The right column.
    pub right_column: String,
    /// The comparison operator.
    pub op: CompareOp,
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
    /// Extra comparison conditions between a left and a right column.
    pub conditions: Vec<SemiAntiCondition>,
    /// The left columns materialized in the view; empty means all of them.
    /// The left primary keys are always contained.
    pub output_columns: Vec<String>,
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
        Self::new_with_conditions(view_id, left, right, mv, join_keys, Vec::new(), anti)
    }

    /// A `SEMI`/`ANTI` view with additional comparison conditions.
    pub fn new_with_conditions(
        view_id: impl Into<String>,
        left: IvmTable,
        right: IvmTable,
        mv: IvmTable,
        join_keys: Vec<String>,
        conditions: Vec<SemiAntiCondition>,
        anti: bool,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            left,
            right,
            mv,
            join_keys,
            conditions,
            output_columns: Vec::new(),
            anti,
            refresh_interval_ms: 0,
        }
    }

    /// Materialize only `output_columns` of the left source (the left primary
    /// keys are added automatically).
    pub fn with_output_columns(mut self, output_columns: Vec<String>) -> Self {
        self.output_columns = output_columns;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::SemiAnti {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            join_keys: self.join_keys.clone(),
            conditions: self.conditions.clone(),
            output_columns: self.output_columns.clone(),
            anti: self.anti,
        }
    }
}

/// A projection (and optional filter) of one source.
///
/// A keyed source is maintained with `delete(old) + insert(current)` per
/// changed primary key; an append-only source simply appends the rows that
/// pass the filter.
#[derive(Debug, Clone)]
pub struct RowView {
    /// The view id.
    pub view_id: String,
    /// The source table (keyed or append-only).
    pub source: IvmTable,
    /// The materialized view table: the projected columns plus the row kind
    /// and the epoch.
    pub mv: IvmTable,
    /// The projected source columns; empty means all of them.
    pub output_columns: Vec<String>,
    /// The filter conditions, all of which must hold.
    pub filters: Vec<FilterCondition>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl RowView {
    /// A view that mirrors the source (all columns, no filter).
    pub fn new(view_id: impl Into<String>, source: IvmTable, mv: IvmTable) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            output_columns: Vec::new(),
            filters: Vec::new(),
            refresh_interval_ms: 0,
        }
    }

    /// Project only `output_columns` (keyed sources must include their keys).
    pub fn with_output_columns(mut self, output_columns: Vec<String>) -> Self {
        self.output_columns = output_columns;
        self
    }

    /// Add filter conditions (all of which must hold).
    pub fn with_filters(mut self, filters: Vec<FilterCondition>) -> Self {
        self.filters = filters;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Row {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            output_columns: self.output_columns.clone(),
            filters: self.filters.clone(),
        }
    }
}

/// `UNION ALL` of several sources with the same schema.
///
/// With keyed sources the output is keyed by `(__ivm_source, primary keys)`,
/// so updates and deletes are retracted per source; with append-only sources
/// the output is append-only.
#[derive(Debug, Clone)]
pub struct UnionAllView {
    /// The view id.
    pub view_id: String,
    /// The source tables, in output order.
    pub sources: Vec<IvmTable>,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl UnionAllView {
    /// A new union-all view over `sources`.
    pub fn new(view_id: impl Into<String>, sources: Vec<IvmTable>, mv: IvmTable) -> Self {
        Self {
            view_id: view_id.into(),
            sources,
            mv,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::UnionAll {
            view_id: self.view_id.clone(),
            source_table_ids: self
                .sources
                .iter()
                .map(|source| source.table_id.clone())
                .collect(),
            mv_table_id: self.mv.table_id.clone(),
        }
    }
}

/// The top `limit` rows of every group, ordered by `order_keys`.
///
/// Ties are broken by the source primary keys, so the result is deterministic
/// and exactly `limit` rows are kept per group (when the group has enough
/// rows). A refresh recomputes the affected groups only.
#[derive(Debug, Clone)]
pub struct TopKView {
    /// The view id.
    pub view_id: String,
    /// The source table; it must have a primary key.
    pub source: IvmTable,
    /// The materialized view table: the projected columns plus the row kind
    /// and the epoch.
    pub mv: IvmTable,
    /// The group (`PARTITION BY`) columns.
    pub group_keys: Vec<String>,
    /// The `ORDER BY` columns.
    pub order_keys: Vec<String>,
    /// The projected source columns; empty means all of them. Must contain the
    /// group keys and the source primary keys.
    pub output_columns: Vec<String>,
    /// How many rows to keep per group.
    pub limit: i64,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl TopKView {
    /// A new top-k view.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        order_keys: Vec<String>,
        limit: i64,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            order_keys,
            output_columns: Vec::new(),
            limit,
            refresh_interval_ms: 0,
        }
    }

    /// Project only `output_columns` (must contain the group keys and the
    /// source primary keys).
    pub fn with_output_columns(mut self, output_columns: Vec<String>) -> Self {
        self.output_columns = output_columns;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::TopK {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            order_keys: self.order_keys.clone(),
            output_columns: self.output_columns.clone(),
            limit: self.limit,
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

/// The schema of a [`WindowView`] materialized view, deriving the partition
/// and row key types from the source schema.
pub fn window_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
) -> Result<SchemaRef> {
    window_ranking_mv_schema_for(
        source_schema,
        partition_keys,
        row_keys,
        WindowFunction::RowNumber,
    )
}

/// The schema of a [`WindowView`] materialized view for a ranking function:
/// the partition keys, the source primary keys, the rank column (named after
/// the function), the row kind and the epoch.
pub fn window_ranking_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
    function: WindowFunction,
) -> Result<SchemaRef> {
    let keys = partition_keys
        .iter()
        .chain(row_keys.iter())
        .cloned()
        .collect::<Vec<_>>();
    let mut fields = key_fields(source_schema, &keys)?;
    fields.push(Arc::new(Field::new(
        function.column_name(),
        DataType::Int64,
        false,
    )));
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of an aggregate [`WindowView`] (`SUM`/`COUNT` over a
/// partition): the partition keys, the source primary keys and the aggregate
/// value, named after the function (`sum_v` / `count_v`). A `SUM` is nullable
/// (the frame may hold no non-NULL value), a `COUNT` never is.
pub fn window_aggregate_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
    function: WindowFunction,
    value_column: Option<&str>,
) -> Result<SchemaRef> {
    let keys = partition_keys
        .iter()
        .chain(row_keys.iter())
        .cloned()
        .collect::<Vec<_>>();
    let mut fields = key_fields(source_schema, &keys)?;
    let (value_type, nullable) = match function {
        WindowFunction::Count => (DataType::Int64, false),
        WindowFunction::Sum => {
            let value = value_column.ok_or_else(|| {
                rootcause::report!("a SUM window view needs a value column")
            })?;
            (sum_result_type(&field_type(source_schema, value)?)?, true)
        }
        other => {
            return Err(rootcause::report!(
                "{} is not an aggregate window function",
                other.sql_name()
            ));
        }
    };
    fields.push(Arc::new(Field::new(
        function.column_name(),
        value_type,
        nullable,
    )));
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a [`SemiAntiView`] materialized view: the left columns plus
/// the row kind and the epoch.
pub fn semi_anti_mv_schema(left_schema: &Schema) -> SchemaRef {
    semi_anti_mv_schema_for(left_schema, &[]).expect("all left columns exist")
}

/// The schema of a [`SemiAntiView`] materialized view projected to
/// `output_columns` (empty means all left columns): the projected left columns
/// plus the row kind and the epoch.
pub fn semi_anti_mv_schema_for(
    left_schema: &Schema,
    output_columns: &[String],
) -> Result<SchemaRef> {
    let mut fields = if output_columns.is_empty() {
        left_schema.fields().iter().cloned().collect::<Vec<_>>()
    } else {
        output_columns
            .iter()
            .map(|column| {
                left_schema
                    .field_with_name(column)
                    .map(|field| Arc::new(field.clone()))
                    .map_err(|_| {
                        rootcause::report!(
                            "output column {column} is not in the left schema"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?
    };
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a [`RowView`] materialized view: the projected source
/// columns plus the row kind and the epoch.
pub fn row_mv_schema_for(
    source_schema: &Schema,
    output_columns: &[String],
) -> Result<SchemaRef> {
    let columns = if output_columns.is_empty() {
        source_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    } else {
        output_columns.to_vec()
    };
    let mut fields = project_schema(source_schema, &columns)?
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a [`UnionAllView`] materialized view: the source columns
/// plus the source index, the row kind and the epoch. Keyed sources are keyed
/// by `(__ivm_source, primary keys)`.
pub fn union_all_mv_schema_for(source_schema: &Schema) -> Result<SchemaRef> {
    let mut fields = source_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        IVM_SOURCE_COLUMN,
        DataType::Int32,
        false,
    )));
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a [`TopKView`] materialized view: the projected source
/// columns plus the row kind and the epoch (the same shape as [`RowView`]).
pub fn top_k_mv_schema_for(
    source_schema: &Schema,
    output_columns: &[String],
) -> Result<SchemaRef> {
    row_mv_schema_for(source_schema, output_columns)
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

/// The result type of a distinct aggregate's materialized value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueResultKind {
    /// `MIN(value)`: the value type.
    Min,
    /// `MAX(value)`: the value type.
    Max,
    /// `COUNT(DISTINCT value)`: `Int64`.
    DistinctCount,
    /// `SUM(DISTINCT value)`: the sum result type.
    DistinctSum,
}

impl From<MinMaxKind> for ValueResultKind {
    fn from(kind: MinMaxKind) -> Self {
        match kind {
            MinMaxKind::Min => ValueResultKind::Min,
            MinMaxKind::Max => ValueResultKind::Max,
        }
    }
}

impl From<DistinctAggKind> for ValueResultKind {
    fn from(kind: DistinctAggKind) -> Self {
        match kind {
            DistinctAggKind::Count => ValueResultKind::DistinctCount,
            DistinctAggKind::Sum => ValueResultKind::DistinctSum,
        }
    }
}

/// The non-nullable group key fields taken from the source schema.
fn key_fields(source_schema: &Schema, group_keys: &[String]) -> Result<Vec<Arc<Field>>> {
    group_keys
        .iter()
        .map(|key| {
            let field = source_schema.field_with_name(key)?;
            // NULL is a regular group value, so keep the source nullability.
            Ok(Arc::new(field.clone()))
        })
        .collect()
}

/// The schema of a `SUM`/`COUNT` materialized view, deriving the key and sum
/// types from the source schema.
pub fn sum_count_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: Option<&str>,
) -> Result<SchemaRef> {
    let mut fields = key_fields(source_schema, group_keys)?;
    let sum_type = match value_column {
        Some(column) => sum_result_type(&field_type(source_schema, column)?)?,
        None => DataType::Int64,
    };
    // An all-NULL group sums to NULL.
    fields.push(Arc::new(Field::new(IVM_SUM_COLUMN, sum_type, true)));
    fields.push(Arc::new(Field::new(
        IVM_COUNT_COLUMN,
        DataType::Int64,
        false,
    )));
    fields.push(Arc::new(Field::new(
        IVM_NONNULL_COUNT_COLUMN,
        DataType::Int64,
        false,
    )));
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a value-count state table, deriving the key and value types
/// from the source schema.
pub fn value_count_state_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    let value_field = source_schema.field_with_name(value_column)?;
    let mut fields = key_fields(source_schema, group_keys)?;
    fields.push(Arc::new(Field::new(
        IVM_VALUE_COLUMN,
        value_field.data_type().clone(),
        value_field.is_nullable(),
    )));
    fields.push(Arc::new(Field::new(
        IVM_VALUE_COUNT_COLUMN,
        DataType::Int64,
        false,
    )));
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a value-count materialized view for a distinct aggregate.
pub fn value_count_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
    result: ValueResultKind,
) -> Result<SchemaRef> {
    // MIN/MAX over an all-NULL group is NULL; DISTINCT SUM is NULL when the
    // group has no distinct value.
    let (value_type, value_nullable) = match result {
        ValueResultKind::Min | ValueResultKind::Max => {
            (field_type(source_schema, value_column)?, true)
        }
        ValueResultKind::DistinctCount => (DataType::Int64, false),
        ValueResultKind::DistinctSum => (
            sum_result_type(&field_type(source_schema, value_column)?)?,
            true,
        ),
    };
    let mut fields = key_fields(source_schema, group_keys)?;
    fields.push(Arc::new(Field::new(
        IVM_VALUE_COLUMN,
        value_type,
        value_nullable,
    )));
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
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a [`MinMaxView`] materialized view, deriving the types from
/// the source schema.
pub fn min_max_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
    kind: MinMaxKind,
) -> Result<SchemaRef> {
    value_count_mv_schema_for(source_schema, group_keys, value_column, kind.into())
}

/// The schema of a [`DistinctAggView`] materialized view, deriving the types
/// from the source schema.
pub fn distinct_agg_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
    kind: DistinctAggKind,
) -> Result<SchemaRef> {
    value_count_mv_schema_for(source_schema, group_keys, value_column, kind.into())
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
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// The internal tables registered for a view (see `ivm.states`).
    pub async fn list_states(&self, view_id: &str) -> Result<Vec<StateTable>> {
        self.metadata.list_states(view_id).await
    }

    /// Register or update a consumer watermark (see `EPOCH.md` §9).
    pub async fn register_consumer(
        &self,
        view_id: &str,
        consumer_id: &str,
        last_epoch: i64,
    ) -> Result<()> {
        self.metadata
            .upsert_consumer(view_id, consumer_id, last_epoch)
            .await
    }

    /// Remove a consumer.
    pub async fn delete_consumer(&self, view_id: &str, consumer_id: &str) -> Result<()> {
        self.metadata.delete_consumer(view_id, consumer_id).await
    }

    /// The consumers of a view.
    pub async fn list_consumers(&self, view_id: &str) -> Result<Vec<Consumer>> {
        self.metadata.list_consumers(view_id).await
    }

    /// The oldest epoch the view's consumers still need.
    pub async fn consumer_watermark(&self, view_id: &str) -> Result<Option<i64>> {
        self.metadata.consumer_watermark(view_id).await
    }

    /// Garbage collect committed epochs below the watermark minus `grace`.
    pub async fn gc_epochs(&self, view_id: &str, grace: i64) -> Result<u64> {
        self.metadata.gc_epochs(view_id, grace).await
    }

    /// Bind the view's internal tables in `ivm.states` before persisting the
    /// spec, so a conflicting state table fails without changing the view row.
    async fn register_state_tables(
        &self,
        view_id: &str,
        tables: &[(StateRole, &IvmTable)],
    ) -> Result<()> {
        for (role, table) in tables {
            self.metadata.register_state(view_id, *role, table).await?;
        }
        Ok(())
    }

    /// Persist a join view spec (idempotent).
    pub async fn register_join_view(&self, view: &JoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.output)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a `SUM`/`COUNT` view over the source changelog.
    ///
    /// The delta is aggregated in SQL (so any numeric value type works), the
    /// previous versions of changed keys are retracted through the as-of read
    /// and the result is merged with the current MV state: rows whose key was
    /// already written by this epoch are skipped, which makes a replay a
    /// no-op.
    pub async fn refresh_sum_count(&self, view: &SumCountView) -> Result<Option<i64>> {
        self.register_view(view).await?;
        validate_group_keys(&view.source, &view.group_keys, &view.view_id)?;
        if let Some(value_column) = &view.value_column {
            sum_result_type(&field_type(&view.source.schema, value_column)?)?;
        }

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

        let context = SessionContext::new();
        register_table(
            &context,
            "delta",
            view.source.read_files(window.added_files).await?,
            &view.source.schema,
        )?;
        let keyed = !view.source.primary_keys.is_empty();
        if keyed {
            register_table(
                &context,
                "old",
                view.source
                    .read_as_of(&self.client, window.before_timestamp)
                    .await?,
                &view.source.schema,
            )?;
        }
        register_table(
            &context,
            "mv",
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        let sql = sum_count_refresh_sql(view, keyed, epoch);
        for batch in context.sql(&sql).await?.collect().await? {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
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
        self.register_state_tables(
            &view.view_id,
            &[(StateRole::Mv, &view.mv), (StateRole::State, &view.state)],
        )
        .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a distinct aggregate view spec (idempotent).
    pub async fn register_distinct_agg_view(&self, view: &DistinctAggView) -> Result<()> {
        self.register_state_tables(
            &view.view_id,
            &[(StateRole::Mv, &view.mv), (StateRole::State, &view.state)],
        )
        .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a window view spec (idempotent).
    pub async fn register_window_view(&self, view: &WindowView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a semi/anti join view spec (idempotent).
    pub async fn register_semi_anti_view(&self, view: &SemiAntiView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a projection/filter view spec (idempotent).
    pub async fn register_row_view(&self, view: &RowView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a union-all view spec (idempotent).
    pub async fn register_union_all_view(&self, view: &UnionAllView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a top-k view spec (idempotent).
    pub async fn register_top_k_view(&self, view: &TopKView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
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
    /// read as of each side's cursor with the as-of API (P0-1). Join keys and
    /// payload columns may be of any equality-comparable type.
    pub async fn refresh_join(&self, view: &JoinView) -> Result<Option<i64>> {
        self.register_join_view(view).await?;
        validate_join_view(view)?;
        let keyed = join_is_keyed(view);
        if !keyed {
            ensure_append_only(&view.left, &view.view_id)?;
            ensure_append_only(&view.right, &view.view_id)?;
        }
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

        let context = SessionContext::new();
        if keyed {
            let left_now = filter_deletes(
                dataframe(
                    &context,
                    view.left.read_current(&self.client).await?,
                    &view.left.schema,
                )?,
                change_column(&view.left),
            )?;
            let right_now = filter_deletes(
                dataframe(
                    &context,
                    view.right.read_current(&self.client).await?,
                    &view.right.schema,
                )?,
                change_column(&view.right),
            )?;
            let delta_left = dataframe(
                &context,
                view.left
                    .read_files(left_window.added_files.clone())
                    .await?,
                &view.left.schema,
            )?;
            let delta_right = dataframe(
                &context,
                view.right
                    .read_files(right_window.added_files.clone())
                    .await?,
                &view.right.schema,
            )?;
            let mv = dataframe(
                &context,
                view.output.read_current(&self.client).await?,
                &view.output.schema,
            )?;

            let left_key_names = view
                .left
                .primary_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let right_key_names = view
                .right
                .primary_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let left_key_exprs = left_key_names
                .iter()
                .map(|key| col(*key))
                .collect::<Vec<_>>();
            let right_key_exprs = right_key_names
                .iter()
                .map(|key| col(*key))
                .collect::<Vec<_>>();

            // Pairs whose left or right row changed in this window.
            let affected_left = delta_left.select(left_key_exprs.clone())?.distinct()?;
            let affected_right =
                delta_right.select(right_key_exprs.clone())?.distinct()?;
            let left_rows = left_now.clone().join(
                affected_left.clone(),
                JoinType::LeftSemi,
                &left_key_names,
                &left_key_names,
                None,
            )?;
            let right_rows = right_now.clone().join(
                affected_right.clone(),
                JoinType::LeftSemi,
                &right_key_names,
                &right_key_names,
                None,
            )?;

            // The current matches of the affected rows, deduplicated for
            // pairs whose both sides changed.
            let current = keyed_join_projection(left_rows, right_now, view)?
                .union(keyed_join_projection(left_now, right_rows, view)?)?
                .distinct()?;

            let pair_keys = view
                .left
                .primary_keys
                .iter()
                .map(|key| left_pk_alias(key))
                .chain(
                    view.right
                        .primary_keys
                        .iter()
                        .map(|key| right_pk_alias(key)),
                )
                .collect::<Vec<_>>();
            let pair_refs = pair_keys.iter().map(String::as_str).collect::<Vec<_>>();
            let pair_exprs = pair_keys
                .iter()
                .map(|key| col(key.as_str()))
                .collect::<Vec<_>>();

            // Pair rows already written by this epoch make a replay a no-op.
            let already = mv
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(pair_exprs.clone())?
                .distinct()?;
            let inserts = current
                .join(already, JoinType::LeftAnti, &pair_refs, &pair_refs, None)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

            // Retract the previous pairs of the affected rows; rows written by
            // this epoch are kept so a replay is idempotent. The output keys
            // pairs by the two row identities, so match the aliases.
            let left_alias_names = view
                .left
                .primary_keys
                .iter()
                .map(|key| left_pk_alias(key))
                .collect::<Vec<_>>();
            let left_alias_refs = left_alias_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let right_alias_names = view
                .right
                .primary_keys
                .iter()
                .map(|key| right_pk_alias(key))
                .collect::<Vec<_>>();
            let right_alias_refs = right_alias_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let active = mv
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?;
            let deletes = active
                .clone()
                .join(
                    affected_left,
                    JoinType::LeftSemi,
                    &left_alias_refs,
                    &left_key_names,
                    None,
                )?
                .union(active.join(
                    affected_right,
                    JoinType::LeftSemi,
                    &right_alias_refs,
                    &right_key_names,
                    None,
                )?)?
                .distinct()?
                .select(keyed_join_output_columns(view))?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

            // Delete rows must arrive before the replacement insert of the
            // same pair for merge-on-read.
            let mut sort_exprs = pair_exprs;
            sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
            for batch in inserts
                .union(deletes)?
                .sort_by(sort_exprs)?
                .collect()
                .await?
            {
                if batch.num_rows() > 0 {
                    view.output.append_batch(&self.client, batch).await?;
                }
            }
        } else {
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

            let mut terms = Vec::new();
            if !left_delta.is_empty() && !right_before.is_empty() {
                terms.push(join_projection(
                    dataframe(&context, left_delta.clone(), &view.left.schema)?,
                    dataframe(&context, right_before.clone(), &view.right.schema)?,
                    view,
                )?);
            }
            if !left_before.is_empty() && !right_delta.is_empty() {
                terms.push(join_projection(
                    dataframe(&context, left_before.clone(), &view.left.schema)?,
                    dataframe(&context, right_delta.clone(), &view.right.schema)?,
                    view,
                )?);
            }
            if !left_delta.is_empty() && !right_delta.is_empty() {
                terms.push(join_projection(
                    dataframe(&context, left_delta.clone(), &view.left.schema)?,
                    dataframe(&context, right_delta.clone(), &view.right.schema)?,
                    view,
                )?);
            }
            if let Some(first) = terms.pop() {
                let mut combined = first;
                for term in terms {
                    combined = combined.union(term)?;
                }
                for batch in combined
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
                    .collect()
                    .await?
                {
                    if batch.num_rows() > 0 {
                        view.output.append_batch(&self.client, batch).await?;
                    }
                }
            }
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
    pub async fn rebuild_sum_count(&self, view: &SumCountView) -> Result<i64> {
        self.register_view(view).await?;
        validate_group_keys(&view.source, &view.group_keys, &view.view_id)?;
        if let Some(value_column) = &view.value_column {
            sum_result_type(&field_type(&view.source.schema, value_column)?)?;
        }

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
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

        let context = SessionContext::new();
        register_table(&context, "src", baseline.batches, &view.source.schema)?;
        for batch in context
            .sql(&sum_count_rebuild_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
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

    /// Refresh a `MIN`/`MAX` view through its value-count state table.
    pub async fn refresh_min_max(&self, view: &MinMaxView) -> Result<Option<i64>> {
        self.register_min_max_view(view).await?;
        self.refresh_value_count(&ValueCountView {
            view_id: &view.view_id,
            source: &view.source,
            mv: &view.mv,
            state: &view.state,
            group_keys: &view.group_keys,
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
            group_keys: &view.group_keys,
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
    /// epoch keeps a replay from applying a window twice; any column types are
    /// supported because the ranking runs in SQL.
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
        let mv_batches = view.mv.read_current(&self.client).await?;

        // Phase 1: the affected partitions follow from the delta and the MV
        // alone, so the source scan can be pruned to them.
        let affected_context = SessionContext::new();
        register_table(
            &affected_context,
            "delta",
            delta_batches.clone(),
            &view.source.schema,
        )?;
        register_table(&affected_context, "mv", mv_batches.clone(), &view.mv.schema)?;
        let mut affected_batches = Vec::new();
        for batch in affected_context
            .sql(&window_affected_sql(view))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                affected_batches.push(batch);
            }
        }
        let filters = partition_filters(&view.partition_keys, &affected_batches)?;
        let src_batches = view
            .source
            .read_current_filtered(&self.client, filters)
            .await?;

        // Phase 2: recompute the affected partitions from their pruned
        // current source rows.
        let context = SessionContext::new();
        register_table(&context, "delta", delta_batches, &view.source.schema)?;
        register_table(&context, "mv", mv_batches, &view.mv.schema)?;
        register_table(&context, "src", src_batches, &view.source.schema)?;
        for batch in context
            .sql(&window_refresh_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Refresh a value-count view.
    ///
    /// The window delta is turned into `(group, value) -> count` changes in
    /// SQL (new rows add, the previous version of an upserted row retracts),
    /// applied to the state table, and the affected groups are recomputed and
    /// written to the MV. State rows carry the window epoch, so a replay after
    /// a crash between the state and the MV writes is a no-op for the state.
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

        // Project both sides to the columns the view actually needs.
        let left_projection =
            project_schema(&view.left.schema, &semi_anti_left_columns(view))?;
        let right_projection =
            project_schema(&view.right.schema, &semi_anti_right_columns(view))?;

        let delta_left = view
            .left
            .read_files_projected(left_window.added_files, Some(&left_projection))
            .await?;
        let delta_right = view
            .right
            .read_files_projected(right_window.added_files, Some(&right_projection))
            .await?;
        let left_before = view
            .left
            .read_as_of_projected(
                &self.client,
                left_window.before_timestamp,
                Some(&left_projection),
            )
            .await?;
        let right_changed = !delta_right.is_empty();
        let right_before = if !right_changed {
            Vec::new()
        } else {
            view.right
                .read_as_of_projected(
                    &self.client,
                    right_window.before_timestamp,
                    Some(&right_projection),
                )
                .await?
        };
        let left_now = view
            .left
            .read_current_projected(&self.client, Some(&left_projection))
            .await?;
        let right_now = view
            .right
            .read_current_projected(&self.client, Some(&right_projection))
            .await?;
        let mv_batches = view.mv.read_current(&self.client).await?;

        let context = SessionContext::new();
        let delta_left = dataframe(&context, delta_left, &left_projection)?;
        let delta_right = dataframe(&context, delta_right, &right_projection)?;
        let left_before = filter_deletes(
            dataframe(&context, left_before, &left_projection)?,
            change_column(&view.left),
        )?;
        let right_before = filter_deletes(
            dataframe(&context, right_before, &right_projection)?,
            change_column(&view.right),
        )?;
        let left_now = filter_deletes(
            dataframe(&context, left_now, &left_projection)?,
            change_column(&view.left),
        )?;
        let right_now = filter_deletes(
            dataframe(&context, right_now, &right_projection)?,
            change_column(&view.right),
        )?;
        let mv = dataframe(&context, mv_batches, &view.mv.schema)?;

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
        let right_key_names = view
            .right
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        // Left rows in the delta are always affected.
        let affected_from_left = delta_left.select(left_key_exprs.clone())?.distinct()?;
        // A right change affects the left rows matching the previous or the
        // current version of the changed right rows: the predicate can flip on
        // an update and a changed equality key can drop an old match.
        let affected = if !right_changed {
            affected_from_left
        } else {
            let changed_pks = delta_right
                .clone()
                .select(
                    view.right
                        .primary_keys
                        .iter()
                        .map(|column| col(column.as_str()))
                        .collect::<Vec<_>>(),
                )?
                .distinct()?;
            let changed_before = right_before.join(
                changed_pks,
                JoinType::LeftSemi,
                &right_key_names,
                &right_key_names,
                None,
            )?;
            let changed = changed_before.union(delta_right)?.distinct()?;
            let affected_from_right =
                semi_anti_join(left_before, changed, view, JoinType::LeftSemi)?
                    .select(left_key_exprs.clone())?
                    .distinct()?;
            affected_from_left.union(affected_from_right)?.distinct()?
        };

        let join_type = if view.anti {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        // Rows that match right now; the ANTI insert takes the difference.
        let matched_now =
            semi_anti_join(left_now.clone(), right_now, view, JoinType::LeftSemi)?
                .select(left_key_exprs.clone())?
                .distinct()?;

        let output_columns = semi_anti_output_columns(view)
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();
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

        // Delete rows must arrive before the replacement insert of the same
        // key for merge-on-read.
        let mut sort_exprs = view
            .left
            .primary_keys
            .iter()
            .map(|key| column_expr(key))
            .collect::<Vec<_>>();
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        for batch in inserts
            .union(deletes)?
            .sort_by(sort_exprs)?
            .collect()
            .await?
        {
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

    async fn refresh_value_count(
        &self,
        view: &ValueCountView<'_>,
    ) -> Result<Option<i64>> {
        validate_group_keys(view.source, view.group_keys, view.view_id)?;
        let value_type = field_type(&view.source.schema, view.value_column)?;
        view.agg.result_type(&value_type)?;

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

        let context = SessionContext::new();
        register_table(
            &context,
            "delta",
            view.source.read_files(window.added_files).await?,
            &view.source.schema,
        )?;
        let keyed = !view.source.primary_keys.is_empty();
        if keyed {
            register_table(
                &context,
                "old",
                view.source
                    .read_as_of(&self.client, window.before_timestamp)
                    .await?,
                &view.source.schema,
            )?;
        }
        register_table(
            &context,
            "state",
            view.state.read_current(&self.client).await?,
            &view.state.schema,
        )?;

        let cte = value_count_refresh_cte(view, keyed, epoch);
        for batch in context
            .sql(&format!(
                "{cte} select * from state_del union all select * from state_ins \
                 order by {}, \"rowKinds\"",
                quoted_list(
                    &view
                        .group_keys
                        .iter()
                        .cloned()
                        .chain(std::iter::once(IVM_VALUE_COLUMN.to_string()))
                        .collect::<Vec<_>>(),
                )
            ))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.state.append_batch(&self.client, batch).await?;
            }
        }

        let affected = context
            .sql(&format!(
                "{cte} select distinct {} from counts",
                quoted_list(view.group_keys)
            ))
            .await?
            .collect()
            .await?;
        register_table(
            &context,
            "affected",
            affected,
            &key_schema(&view.source.schema, view.group_keys)?,
        )?;
        register_table(
            &context,
            "state_now",
            view.state.read_current(&self.client).await?,
            &view.state.schema,
        )?;
        register_table(
            &context,
            "mv",
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        for batch in context
            .sql(&value_count_mv_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
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
        validate_join_view(view)?;
        let keyed = join_is_keyed(view);
        if !keyed {
            ensure_append_only(&view.left, &view.view_id)?;
            ensure_append_only(&view.right, &view.view_id)?;
        }
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
        if !left_baseline.batches.is_empty() && !right_baseline.batches.is_empty() {
            let joined = if keyed {
                let left = filter_deletes(
                    dataframe(&context, left_baseline.batches, &view.left.schema)?,
                    change_column(&view.left),
                )?;
                let right = filter_deletes(
                    dataframe(&context, right_baseline.batches, &view.right.schema)?,
                    change_column(&view.right),
                )?;
                keyed_join_projection(left, right, view)?
                    .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            } else {
                join_projection(
                    dataframe(&context, left_baseline.batches, &view.left.schema)?,
                    dataframe(&context, right_baseline.batches, &view.right.schema)?,
                    view,
                )?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            };
            for batch in joined.collect().await? {
                if batch.num_rows() > 0 {
                    view.output.append_batch(&self.client, batch).await?;
                }
            }
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
            group_keys: &view.group_keys,
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
            group_keys: &view.group_keys,
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

        let context = SessionContext::new();
        register_table(&context, "src", baseline.batches, &view.source.schema)?;
        for batch in context
            .sql(&window_rebuild_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
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
        let join_type = if view.anti {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        let output_columns = semi_anti_output_columns(view)
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();
        let rows = semi_anti_join(left, right, view, join_type)?
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

    /// Refresh a projection/filter view.
    ///
    /// A keyed source is maintained per changed primary key (`delete(old) +
    /// insert(current)` when the row passes the filter); an append-only source
    /// simply appends the delta rows that pass.
    pub async fn refresh_row(&self, view: &RowView) -> Result<Option<i64>> {
        self.register_row_view(view).await?;
        validate_row_view(view)?;
        self.ensure_unpartitioned(&view.source).await?;

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
        let keyed = !view.source.primary_keys.is_empty();

        let context = SessionContext::new();
        let delta = dataframe(
            &context,
            view.source.read_files(window.added_files).await?,
            &view.source.schema,
        )?;
        let output_columns = row_output_columns(view);
        let output_exprs = output_columns
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();
        let predicate = row_filter_predicate(view);

        if keyed {
            let source_now = filter_deletes(
                dataframe(
                    &context,
                    view.source.read_current(&self.client).await?,
                    &view.source.schema,
                )?,
                change_column(&view.source),
            )?;
            let mv = dataframe(
                &context,
                view.mv.read_current(&self.client).await?,
                &view.mv.schema,
            )?;
            let key_names = view
                .source
                .primary_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let key_exprs = view
                .source
                .primary_keys
                .iter()
                .map(|column| col(column.as_str()))
                .collect::<Vec<_>>();
            let affected = delta.select(key_exprs.clone())?.distinct()?;
            let mut passing = source_now.join(
                affected.clone(),
                JoinType::LeftSemi,
                &key_names,
                &key_names,
                None,
            )?;
            if let Some(predicate) = predicate {
                passing = passing.filter(predicate)?;
            }
            let already = mv
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(key_exprs.clone())?
                .distinct()?;
            let inserts = passing
                .select(output_exprs.clone())?
                .join(already, JoinType::LeftAnti, &key_names, &key_names, None)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let deletes = mv
                .join(affected, JoinType::LeftSemi, &key_names, &key_names, None)?
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
                .select(output_exprs)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let mut sort_exprs = view
                .source
                .primary_keys
                .iter()
                .map(|key| column_expr(key))
                .collect::<Vec<_>>();
            sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
            for batch in inserts
                .union(deletes)?
                .sort_by(sort_exprs)?
                .collect()
                .await?
            {
                if batch.num_rows() > 0 {
                    view.mv.append_batch(&self.client, batch).await?;
                }
            }
        } else {
            let mut rows = delta;
            if let Some(predicate) = predicate {
                rows = rows.filter(predicate)?;
            }
            for batch in rows
                .select(output_exprs)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
                .collect()
                .await?
            {
                if batch.num_rows() > 0 {
                    view.mv.append_batch(&self.client, batch).await?;
                }
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a projection/filter view from the full source state.
    pub async fn rebuild_row(&self, view: &RowView) -> Result<i64> {
        self.register_row_view(view).await?;
        validate_row_view(view)?;
        self.ensure_unpartitioned(&view.source).await?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
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

        let context = SessionContext::new();
        let mut rows = filter_deletes(
            dataframe(&context, baseline.batches, &view.source.schema)?,
            change_column(&view.source),
        )?;
        if let Some(predicate) = row_filter_predicate(view) {
            rows = rows.filter(predicate)?;
        }
        let output_exprs = row_output_columns(view)
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();
        for batch in rows
            .select(output_exprs)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
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

    /// Refresh a `UNION ALL` view over keyed or append-only sources.
    pub async fn refresh_union_all(&self, view: &UnionAllView) -> Result<Option<i64>> {
        self.register_union_all_view(view).await?;
        validate_union_all_view(view)?;
        for source in &view.sources {
            self.ensure_unpartitioned(source).await?;
        }

        let mut windows = Vec::new();
        for source in &view.sources {
            windows.push(self.collect_source_window(&view.view_id, source).await?);
        }
        if windows.iter().all(|window| window.added_files.is_empty()) {
            return Ok(None);
        }
        let identity = windows
            .iter()
            .flat_map(|window| window.identity.iter().cloned())
            .collect::<Vec<_>>();
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                for window in &windows {
                    self.advance_cursors(&view.view_id, window.cursors.clone())
                        .await?;
                }
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let keyed = !view.sources[0].primary_keys.is_empty();

        let context = SessionContext::new();
        let mut inserts = Vec::new();
        let mut changed = Vec::new();
        for (index, source) in view.sources.iter().enumerate() {
            let delta = dataframe(
                &context,
                source
                    .read_files(windows[index].added_files.clone())
                    .await?,
                &source.schema,
            )?;
            if keyed {
                let source_now = filter_deletes(
                    dataframe(
                        &context,
                        source.read_current(&self.client).await?,
                        &source.schema,
                    )?,
                    change_column(source),
                )?;
                let key_names = source
                    .primary_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let key_exprs = source
                    .primary_keys
                    .iter()
                    .map(|column| col(column.as_str()))
                    .collect::<Vec<_>>();
                let affected = delta
                    .select(key_exprs)?
                    .distinct()?
                    .with_column(IVM_SOURCE_COLUMN, lit(index as i32))?;
                let rows = source_now
                    .join(
                        affected.clone(),
                        JoinType::LeftSemi,
                        &key_names,
                        &key_names,
                        None,
                    )?
                    .select(union_all_projection(source, index)?)?;
                inserts.push(rows);
                changed.push(affected);
            } else {
                inserts.push(delta.select(union_all_projection(source, index)?)?);
            }
        }

        let output_exprs = view.sources[0]
            .schema
            .fields()
            .iter()
            .map(|field| col(field.name().as_str()))
            .chain(std::iter::once(col(IVM_SOURCE_COLUMN)))
            .collect::<Vec<_>>();

        if keyed {
            let mv = dataframe(
                &context,
                view.mv.read_current(&self.client).await?,
                &view.mv.schema,
            )?;
            let mut pair_names = vec![IVM_SOURCE_COLUMN.to_string()];
            pair_names.extend(view.sources[0].primary_keys.iter().cloned());
            let pair_refs = pair_names.iter().map(String::as_str).collect::<Vec<_>>();
            let pair_exprs = pair_names
                .iter()
                .map(|column| col(column.as_str()))
                .collect::<Vec<_>>();
            let already = mv
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(pair_exprs.clone())?
                .distinct()?;
            let inserts = union_frames(inserts)?
                .join(already, JoinType::LeftAnti, &pair_refs, &pair_refs, None)?
                .select(output_exprs.clone())?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let changed = union_frames(changed)?;
            let deletes = mv
                .join(changed, JoinType::LeftSemi, &pair_refs, &pair_refs, None)?
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
                .select(output_exprs)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let mut sort_exprs = vec![column_expr(IVM_SOURCE_COLUMN)];
            sort_exprs.extend(
                view.sources[0]
                    .primary_keys
                    .iter()
                    .map(|key| column_expr(key)),
            );
            sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
            for batch in inserts
                .union(deletes)?
                .sort_by(sort_exprs)?
                .collect()
                .await?
            {
                if batch.num_rows() > 0 {
                    view.mv.append_batch(&self.client, batch).await?;
                }
            }
        } else {
            for batch in union_frames(inserts)?
                .select(output_exprs)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
                .collect()
                .await?
            {
                if batch.num_rows() > 0 {
                    view.mv.append_batch(&self.client, batch).await?;
                }
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        for window in &windows {
            self.advance_cursors(&view.view_id, window.cursors.clone())
                .await?;
        }
        Ok(Some(epoch))
    }

    /// Rebuild a `UNION ALL` view from the full states of its sources.
    pub async fn rebuild_union_all(&self, view: &UnionAllView) -> Result<i64> {
        self.register_union_all_view(view).await?;
        validate_union_all_view(view)?;
        for source in &view.sources {
            self.ensure_unpartitioned(source).await?;
        }

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let mut baselines = Vec::new();
        for source in &view.sources {
            baselines.push(self.source_baseline(source).await?);
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

        let context = SessionContext::new();
        let mut frames = Vec::new();
        for (index, source) in view.sources.iter().enumerate() {
            let rows = filter_deletes(
                dataframe(&context, baselines[index].batches.clone(), &source.schema)?,
                change_column(source),
            )?;
            frames.push(rows.select(union_all_projection(source, index)?)?);
        }
        let output_exprs = view.sources[0]
            .schema
            .fields()
            .iter()
            .map(|field| col(field.name().as_str()))
            .chain(std::iter::once(col(IVM_SOURCE_COLUMN)))
            .collect::<Vec<_>>();
        for batch in union_frames(frames)?
            .select(output_exprs)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
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

    /// Refresh a top-k view by recomputing its affected groups.
    pub async fn refresh_top_k(&self, view: &TopKView) -> Result<Option<i64>> {
        self.register_top_k_view(view).await?;
        validate_top_k_view(view)?;
        self.ensure_unpartitioned(&view.source).await?;

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

        let context = SessionContext::new();
        register_table(
            &context,
            "delta",
            view.source.read_files(window.added_files).await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "src",
            view.source.read_current(&self.client).await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "mv",
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        for batch in context
            .sql(&top_k_refresh_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a top-k view from the full source state.
    pub async fn rebuild_top_k(&self, view: &TopKView) -> Result<i64> {
        self.register_top_k_view(view).await?;
        validate_top_k_view(view)?;
        self.ensure_unpartitioned(&view.source).await?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
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

        let context = SessionContext::new();
        register_table(&context, "src", baseline.batches, &view.source.schema)?;
        for batch in context
            .sql(&top_k_rebuild_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
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

    /// Rebuild a value-count view.
    ///
    /// Both the value-count state table and the MV are truncated and refilled
    /// from the current source state, published as `rebuild:<generation>`.
    async fn rebuild_value_count(&self, view: &ValueCountView<'_>) -> Result<i64> {
        validate_group_keys(view.source, view.group_keys, view.view_id)?;
        let value_type = field_type(&view.source.schema, view.value_column)?;
        view.agg.result_type(&value_type)?;

        self.metadata
            .set_view_status(view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(view.view_id).await?;
        self.metadata.delete_cursors(view.view_id).await?;
        view.mv.truncate(&self.client).await?;
        view.state.truncate(&self.client).await?;

        let baseline = self.source_baseline(view.source).await?;
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

        let context = SessionContext::new();
        register_table(&context, "src", baseline.batches, &view.source.schema)?;
        let state_sql = format!(
            "select {}, {} as {}, count(1) as {}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
             from src where {} group by {}, {}",
            quoted_list(view.group_keys),
            quote_ident(view.value_column),
            quote_ident(IVM_VALUE_COLUMN),
            quote_ident(IVM_VALUE_COUNT_COLUMN),
            source_delete_filter("src", change_column(view.source)),
            quoted_list(view.group_keys),
            quote_ident(view.value_column),
        );
        for batch in context.sql(&state_sql).await?.collect().await? {
            if batch.num_rows() > 0 {
                view.state.append_batch(&self.client, batch).await?;
            }
        }

        register_table(
            &context,
            "state_now",
            view.state.read_current(&self.client).await?,
            &view.state.schema,
        )?;
        let mv_sql = format!(
            "select {}, {} as {}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
             from state_now where \"rowKinds\" = 'insert' and {} > 0 group by {}",
            quoted_list(view.group_keys),
            view.agg.sql(IVM_VALUE_COLUMN),
            quote_ident(IVM_VALUE_COLUMN),
            quote_ident(IVM_VALUE_COUNT_COLUMN),
            quoted_list(view.group_keys),
        );
        for batch in context.sql(&mv_sql).await?.collect().await? {
            if batch.num_rows() > 0 {
                view.mv.append_batch(&self.client, batch).await?;
            }
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

/// Validate that a join view can be maintained.
fn validate_join_view(view: &JoinView) -> Result<()> {
    if view.join_keys.is_empty() {
        return Err(report!(
            "join view {} needs at least one join key",
            view.view_id
        ));
    }
    for key in &view.join_keys {
        let left = view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "join view {}: join key {key} is not in the left source",
                view.view_id
            )
        })?;
        let right = view.right.schema.field_with_name(key).map_err(|_| {
            report!(
                "join view {}: join key {key} is not in the right source",
                view.view_id
            )
        })?;
        if left.data_type() != right.data_type() {
            return Err(report!(
                "join view {}: join key {key} has different types on the two sources",
                view.view_id
            ));
        }
    }
    field_type(&view.left.schema, &view.left_value)?;
    field_type(&view.right.schema, &view.right_value)?;

    if join_is_keyed(view) {
        if view.left.primary_keys.is_empty() || view.right.primary_keys.is_empty() {
            return Err(report!(
                "join view {}: both sources must be keyed, or both append-only",
                view.view_id
            ));
        }
        // The row identities must be non-NULL and present in both schemas.
        for (side, source) in [("left", &view.left), ("right", &view.right)] {
            for key in &source.primary_keys {
                let field = source.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "join view {}: {side} key {key} is not in the source",
                        view.view_id
                    )
                })?;
                if field.is_nullable() {
                    return Err(report!(
                        "join view {}: {side} key {key} must be non-nullable",
                        view.view_id
                    ));
                }
            }
        }
        let expected = keyed_join_view_schema_for(
            &view.left.schema,
            &view.right.schema,
            &view.left.primary_keys,
            &view.right.primary_keys,
            &view.join_keys,
            &view.left_value,
            &view.right_value,
        )?;
        for field in expected.fields() {
            let actual =
                view.output
                    .schema
                    .field_with_name(field.name())
                    .map_err(|_| {
                        report!(
                            "join view {}: output table {} is missing column {}",
                            view.view_id,
                            view.output.table_name,
                            field.name()
                        )
                    })?;
            if actual.data_type() != field.data_type() {
                return Err(report!(
                    "join view {}: output column {} has type {} instead of {}",
                    view.view_id,
                    field.name(),
                    actual.data_type(),
                    field.data_type()
                ));
            }
        }
        let expected_keys = keyed_join_output_primary_keys(
            &view.left.primary_keys,
            &view.right.primary_keys,
        );
        if view.output.primary_keys != expected_keys {
            return Err(report!(
                "join view {}: output primary keys must be [{}]",
                view.view_id,
                expected_keys.join(", ")
            ));
        }
    } else if !view.output.primary_keys.is_empty() {
        return Err(report!(
            "join view {}: an append-only join output must not have primary keys",
            view.view_id
        ));
    }
    Ok(())
}

/// Project an inner equi-join onto `(join keys, left value, right value)`.
///
/// The right side's key columns are aliased before the join because DataFusion
/// rejects duplicate qualified fields for the same name.
fn join_projection(
    left: DataFrame,
    right: DataFrame,
    view: &JoinView,
) -> Result<DataFrame> {
    let left_keys = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    let right_keys = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()).alias(format!("__right_{key}")))
        .collect::<Vec<_>>();
    let mut left_columns = left_keys.clone();
    left_columns.push(col(view.left_value.as_str()).alias("left_value"));
    let left = left.select(left_columns)?;
    let mut right_columns = right_keys;
    right_columns.push(col(view.right_value.as_str()).alias("right_value"));
    let right = right.select(right_columns)?;

    let key_names = view
        .join_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let right_key_names = view
        .join_keys
        .iter()
        .map(|key| format!("__right_{key}"))
        .collect::<Vec<_>>();
    let right_key_refs = right_key_names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let joined = left.join(right, JoinType::Inner, &key_names, &right_key_refs, None)?;

    let mut output = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    output.push(col("left_value"));
    output.push(col("right_value"));
    Ok(joined.select(output)?)
}

/// Project an inner equi-join of two keyed sources onto
/// `(join keys, left value, right value, left pk*, right pk*)`.
fn keyed_join_projection(
    left: DataFrame,
    right: DataFrame,
    view: &JoinView,
) -> Result<DataFrame> {
    let mut left_columns = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    left_columns.push(col(view.left_value.as_str()).alias("left_value"));
    for key in &view.left.primary_keys {
        left_columns.push(col(key.as_str()).alias(left_pk_alias(key)));
    }
    let left = left.select(left_columns)?;

    let mut right_columns = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()).alias(format!("__right_{key}")))
        .collect::<Vec<_>>();
    right_columns.push(col(view.right_value.as_str()).alias("right_value"));
    for key in &view.right.primary_keys {
        right_columns.push(col(key.as_str()).alias(right_pk_alias(key)));
    }
    let right = right.select(right_columns)?;

    let key_names = view
        .join_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let right_key_names = view
        .join_keys
        .iter()
        .map(|key| format!("__right_{key}"))
        .collect::<Vec<_>>();
    let right_key_refs = right_key_names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let joined = left.join(right, JoinType::Inner, &key_names, &right_key_refs, None)?;

    let mut output = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    output.push(col("left_value"));
    output.push(col("right_value"));
    for key in &view.left.primary_keys {
        output.push(col(left_pk_alias(key)));
    }
    for key in &view.right.primary_keys {
        output.push(col(right_pk_alias(key)));
    }
    Ok(joined.select(output)?)
}

/// The output columns of a keyed join in schema order.
fn keyed_join_output_columns(view: &JoinView) -> Vec<datafusion::logical_expr::Expr> {
    let mut output = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    output.push(col("left_value"));
    output.push(col("right_value"));
    for key in &view.left.primary_keys {
        output.push(col(left_pk_alias(key).as_str()));
    }
    for key in &view.right.primary_keys {
        output.push(col(right_pk_alias(key).as_str()));
    }
    output
}

/// Whether a join view takes the keyed (upsert) path.
fn join_is_keyed(view: &JoinView) -> bool {
    !view.left.primary_keys.is_empty() || !view.right.primary_keys.is_empty()
}

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
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quoted_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn field_type(schema: &Schema, column: &str) -> Result<DataType> {
    Ok(schema.field_with_name(column)?.data_type().clone())
}

/// The result type of `SUM` over `value_type`, matching DataFusion's rules.
fn sum_result_type(value_type: &DataType) -> Result<DataType> {
    Ok(match value_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            DataType::Int64
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            DataType::UInt64
        }
        DataType::Float32 | DataType::Float64 => value_type.clone(),
        DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
        DataType::Decimal256(_, scale) => DataType::Decimal256(76, *scale),
        other => {
            return Err(report!("SUM is not supported for value type {other}"));
        }
    })
}

fn key_schema(source_schema: &Schema, group_keys: &[String]) -> Result<SchemaRef> {
    Ok(Arc::new(Schema::new(key_fields(
        source_schema,
        group_keys,
    )?)))
}

fn validate_group_keys(
    source: &IvmTable,
    group_keys: &[String],
    view_id: &str,
) -> Result<()> {
    if group_keys.is_empty() {
        return Err(report!("view {view_id} needs at least one group key"));
    }
    for key in group_keys {
        source.schema.field_with_name(key).map_err(|_| {
            report!("view {view_id}: group key {key} is not in the source")
        })?;
    }
    Ok(())
}

fn register_table(
    context: &SessionContext,
    name: &str,
    mut batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<()> {
    // SQL results may carry different nullability than the declared schema;
    // use the batches' own schema when there is one, and an empty batch with
    // the declared schema otherwise.
    let actual_schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| schema.clone());
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(actual_schema.clone()));
    }
    let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        datafusion::datasource::memory::MemTable::try_new(actual_schema, vec![batches])
            .map_err(|error| report!("registering {name}: {error}"))?,
    );
    context.register_table(name, table)?;
    Ok(())
}

/// `alias.column <> 'delete'` when the source has a change column.
fn source_delete_filter(alias: &str, change_column: Option<&str>) -> String {
    match change_column {
        Some(column) => format!("{alias}.{} <> 'delete'", quote_ident(column)),
        None => "true".to_string(),
    }
}

fn key_join_condition(left: &str, right: &str, keys: &[String]) -> String {
    keys.iter()
        .map(|key| format!("{left}.{} = {right}.{}", quote_ident(key), quote_ident(key)))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Null-safe key equality (`IS NOT DISTINCT FROM`), used wherever NULL is a
/// valid group/partition value (SQL groups NULLs together).
fn key_join_condition_null_safe(left: &str, right: &str, keys: &[String]) -> String {
    keys.iter()
        .map(|key| {
            format!(
                "({left}.{} IS NOT DISTINCT FROM {right}.{})",
                quote_ident(key),
                quote_ident(key)
            )
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// SQL for one `SUM`/`COUNT` refresh window: a `delete` part (the previous
/// accumulator of changed groups) unioned with an `insert` part (the new
/// accumulator). Keys already written for this epoch are skipped.
fn sum_count_refresh_sql(view: &SumCountView, keyed: bool, epoch: i64) -> String {
    let keys = quoted_list(&view.group_keys);
    let sum_expr = match &view.value_column {
        Some(column) => format!("sum({})", quote_ident(column)),
        None => "sum(0)".to_string(),
    };
    let nonnull_expr = match &view.value_column {
        Some(column) => format!("count({})", quote_ident(column)),
        None => "count(1)".to_string(),
    };
    let delta_filter = source_delete_filter("delta", change_column(&view.source));
    let old_filter = source_delete_filter("o", change_column(&view.source));
    let pk_match = key_join_condition("o", "p", &view.source.primary_keys);
    let delta_part = format!(
        "new_agg as (select {keys}, {sum_expr} as dsum, count(1) as dcount, \
                            {nonnull_expr} as dnonnull \
                     from delta where {delta_filter} group by {keys})"
    );
    let d2 = if keyed {
        format!(
            "old_changed as (select o.* from old o where {old_filter} \
                            and exists (select 1 from delta_pks p where {pk_match})), \
             old_agg as (select {keys}, {sum_expr} as osum, count(1) as ocount, \
                                 {nonnull_expr} as ononnull \
                         from old_changed group by {keys}), \
             raw as (select {coalesced_keys}, n.dsum as new_sum, o.osum as old_sum, \
                            n.dcount as new_count, o.ocount as old_count, \
                            n.dnonnull as new_nonnull, o.ononnull as old_nonnull \
                     from new_agg n full join old_agg o on {agg_match}), \
             d2 as (select {keys}, \
                           coalesce(new_sum - old_sum, new_sum, -old_sum) as dsum, \
                           coalesce(new_count - old_count, new_count, -old_count) as dcount, \
                           coalesce(new_nonnull - old_nonnull, new_nonnull, -old_nonnull) as dnonnull \
                    from raw \
                    where coalesce(new_sum - old_sum, new_sum, -old_sum) <> 0 \
                       or coalesce(new_count - old_count, new_count, -old_count) <> 0 \
                       or coalesce(new_nonnull - old_nonnull, new_nonnull, -old_nonnull) <> 0)",
            coalesced_keys = view
                .group_keys
                .iter()
                .map(|key| format!("coalesce(n.{q}, o.{q}) as {q}", q = quote_ident(key)))
                .collect::<Vec<_>>()
                .join(", "),
            agg_match = key_join_condition_null_safe("n", "o", &view.group_keys),
        )
    } else {
        format!(
            "d2 as (select {keys}, dsum, dcount, dnonnull from new_agg \
                    where dsum <> 0 or dcount <> 0 or dnonnull <> 0)"
        )
    };
    let active_match = key_join_condition_null_safe("s", "d2", &view.group_keys);
    let coalesced_keys = view
        .group_keys
        .iter()
        .map(|key| format!("coalesce(s.{q}, d2.{q}) as {q}", q = quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let already_match = view
        .group_keys
        .iter()
        .map(|key| format!("(a.{q} IS NOT DISTINCT FROM {q})", q = quote_ident(key)))
        .collect::<Vec<_>>()
        .join(" and ");
    format!(
        "with delta_pks as (select distinct {pks} from delta), \
         {delta_part}, {d2}, \
         already as (select distinct {keys} from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         merged as (select {coalesced_keys}, s.sum_v, s.count_v, s.{nonnull_c} as s_nonnull, \
                           s.\"__ivm_epoch\" as s_epoch, d2.dsum, d2.dcount, d2.dnonnull, \
                           coalesce(s.{nonnull_c} + d2.dnonnull, s.{nonnull_c}, d2.dnonnull) as n_nonnull \
                    from active s full join d2 on {active_match}), \
         deletes as (select {keys}, sum_v, count_v, s_nonnull as {nonnull_c}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from merged where s_epoch is not null and dcount is not null), \
         inserts as (select {keys}, \
                            case when n_nonnull > 0 then coalesce(sum_v + dsum, sum_v, dsum) else null end as sum_v, \
                            coalesce(count_v + dcount, count_v, dcount) as count_v, \
                            n_nonnull as {nonnull_c}, \
                            'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from merged \
                     where dcount is not null \
                       and coalesce(count_v + dcount, count_v, dcount) <> 0 \
                       and not exists (select 1 from already a where {already_match})) \
         select * from deletes union all select * from inserts \
         order by {keys}, \"rowKinds\"",
        pks = quoted_list(&view.source.primary_keys),
        nonnull_c = quote_ident(IVM_NONNULL_COUNT_COLUMN),
    )
}

/// SQL for a full `SUM`/`COUNT` rebuild.
fn sum_count_rebuild_sql(view: &SumCountView, epoch: i64) -> String {
    let keys = quoted_list(&view.group_keys);
    let sum_expr = match &view.value_column {
        Some(column) => format!("sum({})", quote_ident(column)),
        None => "sum(0)".to_string(),
    };
    let nonnull_expr = match &view.value_column {
        Some(column) => format!("count({})", quote_ident(column)),
        None => "count(1)".to_string(),
    };
    format!(
        "select {keys}, {sum_expr} as {}, count(1) as {}, {nonnull_expr} as {}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from src where {} group by {keys}",
        quote_ident(IVM_SUM_COLUMN),
        quote_ident(IVM_COUNT_COLUMN),
        quote_ident(IVM_NONNULL_COUNT_COLUMN),
        source_delete_filter("src", change_column(&view.source)),
    )
}

/// The CTE chain of one value-count refresh window. It defines `counts`
/// (`(group, value) -> dcount`), `state_del` / `state_ins` (the state rows to
/// write) and `merged` (the post-window state with `n_count`).
fn value_count_refresh_cte(view: &ValueCountView<'_>, keyed: bool, epoch: i64) -> String {
    let keys = quoted_list(view.group_keys);
    let source_value = quote_ident(view.value_column);
    let state_value = quote_ident(IVM_VALUE_COLUMN);
    let delta_filter = source_delete_filter("delta", change_column(view.source));
    let old_filter = source_delete_filter("o", change_column(view.source));
    let pk_match = key_join_condition("o", "p", &view.source.primary_keys);
    let new_select = format!("{keys}, {source_value} as {state_value}");
    let new_group = format!("{keys}, {source_value}");
    let state_value_keys = view
        .group_keys
        .iter()
        .cloned()
        .chain(std::iter::once(IVM_VALUE_COLUMN.to_string()))
        .collect::<Vec<_>>();
    let state_list = quoted_list(&state_value_keys);
    let counts = if keyed {
        let agg_match = key_join_condition_null_safe("n", "o", &state_value_keys);
        let coalesced = state_value_keys
            .iter()
            .map(|key| format!("coalesce(n.{q}, o.{q}) as {q}", q = quote_ident(key)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "delta_pks as (select distinct {pks} from delta), \
             old_changed as (select o.* from old o where {old_filter} \
                             and exists (select 1 from delta_pks p where {pk_match})), \
             old_agg as (select {new_select}, count(1) as ocount \
                         from old_changed group by {new_group}), \
             raw as (select {coalesced}, n.dcount as new_c, o.ocount as old_c \
                     from new_agg n full join old_agg o on {agg_match}), \
             counts as (select {state_list}, \
                               coalesce(new_c - old_c, new_c, -old_c) as dcount \
                        from raw \
                        where coalesce(new_c - old_c, new_c, -old_c) <> 0)",
            pks = quoted_list(&view.source.primary_keys),
        )
    } else {
        format!("counts as (select {state_list}, dcount from new_agg where dcount <> 0)")
    };
    let merged_match = key_join_condition_null_safe("s", "c", &state_value_keys);
    let coalesced = state_value_keys
        .iter()
        .map(|key| format!("coalesce(s.{q}, c.{q}) as {q}", q = quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let already_match = state_value_keys
        .iter()
        .map(|key| format!("(a.{q} IS NOT DISTINCT FROM {q})", q = quote_ident(key)))
        .collect::<Vec<_>>()
        .join(" and ");
    format!(
        "with new_agg as (select {new_select}, count(1) as dcount from delta \
                          where {delta_filter} group by {new_group}), \
         {counts}, \
         already as (select distinct {state_list} from state \
                     where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from state \
                    where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         merged as (select {coalesced}, s.{value_count}, s.\"__ivm_epoch\" as s_epoch, c.dcount, \
                           coalesce(s.{value_count} + c.dcount, s.{value_count}, c.dcount) as n_count \
                    from active s full join counts c on {merged_match}), \
         state_del as (select {state_list}, {value_count}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                       from merged where s_epoch is not null and dcount is not null), \
         state_ins as (select {state_list}, n_count as {value_count}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                       from merged where dcount is not null and n_count > 0 \
                         and not exists (select 1 from already a where {already_match}))",
        value_count = quote_ident(IVM_VALUE_COUNT_COLUMN),
    )
}

/// SQL comparing the affected groups' recomputed extreme with the MV.
fn value_count_mv_sql(view: &ValueCountView<'_>, epoch: i64) -> String {
    let keys = quoted_list(view.group_keys);
    let value = quote_ident(IVM_VALUE_COLUMN);
    let affected_match = key_join_condition_null_safe("a", "state_now", view.group_keys);
    let active_match = key_join_condition_null_safe("a", "active", view.group_keys);
    let agg_match = key_join_condition_null_safe("a", "agg", view.group_keys);
    format!(
        "with agg as (select {keys}, {agg} as {value} from state_now \
                      where \"rowKinds\" = 'insert' and {value_count} > 0 \
                        and exists (select 1 from affected a where {affected_match}) \
                      group by {keys}), \
         mv_rows as (select * from mv where \"rowKinds\" = 'insert'), \
         already as (select distinct {keys} from mv_rows where \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv_rows where \"__ivm_epoch\" <> {epoch}), \
         mv_del as (select {keys}, {value} as {value}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                    from active where exists (select 1 from affected a where {active_match})), \
         mv_ins as (select {keys}, {value}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                    from agg where not exists (select 1 from already a where {agg_match})) \
         select * from mv_del union all select * from mv_ins order by {keys}, \"rowKinds\"",
        agg = view.agg.sql(IVM_VALUE_COLUMN),
        value_count = quote_ident(IVM_VALUE_COUNT_COLUMN),
    )
}

/// Validate that a window view can be maintained.
fn validate_window_view(view: &WindowView) -> Result<()> {
    if view.source.primary_keys.is_empty() {
        return Err(report!(
            "window view {} needs a source with a primary key",
            view.view_id
        ));
    }
    if view.partition_keys.is_empty() {
        return Err(report!("window view {} needs partition keys", view.view_id));
    }
    // Ranking functions need an ordering; aggregates may span the whole
    // partition (empty order keys).
    if !view.function.is_aggregate() && view.order_keys.is_empty() {
        return Err(report!(
            "window view {}: {} needs order keys",
            view.view_id,
            view.function.sql_name()
        ));
    }
    for column in &view.partition_keys {
        view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "window view {}: partition column {column} is not in the source",
                view.view_id
            )
        })?;
    }
    // The primary keys identify a row, so they must not be NULL.
    for column in &view.source.primary_keys {
        let field = view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "window view {}: key column {column} is not in the source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "window view {}: key column {column} must be non-nullable",
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
    match view.function {
        WindowFunction::Sum => {
            let value = view.value_column.as_deref().ok_or_else(|| {
                report!("window view {}: SUM needs a value column", view.view_id)
            })?;
            let field = view.source.schema.field_with_name(value).map_err(|_| {
                report!(
                    "window view {}: value column {value} is not in the source",
                    view.view_id
                )
            })?;
            sum_result_type(field.data_type())?;
        }
        WindowFunction::Count => {
            if let Some(value) = view.value_column.as_deref() {
                view.source.schema.field_with_name(value).map_err(|_| {
                    report!(
                        "window view {}: value column {value} is not in the source",
                        view.view_id
                    )
                })?;
            }
        }
        function => {
            if view.value_column.is_some() {
                return Err(report!(
                    "window view {}: {} does not take a value column",
                    view.view_id,
                    function.sql_name()
                ));
            }
        }
    }
    Ok(())
}

/// The `computed` CTE: the window function over the current source state.
/// `ROW_NUMBER` appends the primary keys to the ordering for deterministic
/// ties; `RANK`/`DENSE_RANK` keep the declared ordering so ties share a rank,
/// and aggregate windows use the SQL default frame (whole partition without
/// order keys, running with them).
fn window_function_cte(view: &WindowView, source_alias: &str) -> String {
    let parts = quoted_list(&view.partition_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let mut order = view.order_keys.clone();
    if view.function.breaks_ties_with_primary_keys() {
        order.extend(view.source.primary_keys.iter().cloned());
    }
    let over = if order.is_empty() {
        format!("partition by {parts}")
    } else {
        format!("partition by {parts} order by {}", quoted_list(&order))
    };
    let filter = source_delete_filter(source_alias, change_column(&view.source));
    let value = quote_ident(view.function.column_name());
    let computed = match view.function {
        WindowFunction::Sum => format!(
            "sum({}) over ({over})",
            quote_ident(view.value_column.as_deref().unwrap_or_default())
        ),
        WindowFunction::Count => match view.value_column.as_deref() {
            Some(column) => format!("count({}) over ({over})", quote_ident(column)),
            None => format!("count(1) over ({over})"),
        },
        function => format!("cast({}() over ({over}) as bigint)", function.sql_name()),
    };
    format!(
        "computed as (select {pks}, {parts}, {computed} as {value} \
         from {source_alias} where {filter})"
    )
}

/// The CTE prefix computing the affected partitions of one refresh window:
/// the partitions in the delta plus the partitions of the MV rows whose
/// primary keys changed.
fn window_affected_cte(view: &WindowView) -> String {
    let parts = quoted_list(&view.partition_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let mv_pks = view
        .source
        .primary_keys
        .iter()
        .map(|key| {
            format!(
                "{} as {}",
                quote_ident(key),
                quote_ident(&format!("__mv_{key}"))
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mv_delta_match = view
        .source
        .primary_keys
        .iter()
        .map(|key| {
            format!(
                "m.{} = d.{}",
                quote_ident(&format!("__mv_{key}")),
                quote_ident(key)
            )
        })
        .collect::<Vec<_>>()
        .join(" and ");
    format!(
        "delta_parts as (select distinct {parts} from delta), \
         delta_rows as (select distinct {pks} from delta), \
         old_parts as (select distinct {parts} \
                       from (select {parts}, {mv_pks} from mv) m \
                       join delta_rows d on {mv_delta_match}), \
         affected as (select * from delta_parts union select * from old_parts)"
    )
}

/// SQL returning the affected partitions of one window refresh.
fn window_affected_sql(view: &WindowView) -> String {
    format!("with {} select * from affected", window_affected_cte(view))
}

/// Equality/`IN` filters over the partition keys of the affected rows, used
/// to read only those partitions of the source.
fn partition_filters(
    partition_keys: &[String],
    batches: &[RecordBatch],
) -> Result<Vec<datafusion::prelude::Expr>> {
    let mut filters = Vec::new();
    for key in partition_keys {
        let mut values: Vec<ScalarValue> = Vec::new();
        let mut has_null = false;
        for batch in batches {
            let index = batch.schema().index_of(key).map_err(|_| {
                report!("partition column {key} is not in the affected partitions")
            })?;
            let array = batch.column(index);
            for row in 0..array.len() {
                if array.is_null(row) {
                    has_null = true;
                } else {
                    let scalar = ScalarValue::try_from_array(array, row)?;
                    if !values.contains(&scalar) {
                        values.push(scalar);
                    }
                }
            }
        }
        let mut filter = if values.is_empty() {
            None
        } else {
            Some(
                col(key.as_str())
                    .in_list(values.iter().cloned().map(lit).collect(), false),
            )
        };
        if has_null {
            let null_filter = col(key.as_str()).is_null();
            filter = Some(match filter {
                Some(filter) => filter.or(null_filter),
                None => null_filter,
            });
        }
        filters.push(filter.unwrap_or_else(|| lit(false)));
    }
    Ok(filters)
}

/// SQL for one `ROW_NUMBER()` refresh window: recompute the affected
/// partitions and rewrite their MV rows (`delete` then `insert`).
fn window_refresh_sql(view: &WindowView, epoch: i64) -> String {
    let parts = quoted_list(&view.partition_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let value = quote_ident(view.function.column_name());
    let computed_pks = view
        .source
        .primary_keys
        .iter()
        .map(|key| format!("c.{}", quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let computed_parts = view
        .partition_keys
        .iter()
        .map(|key| format!("c.{}", quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let part_match_computed =
        key_join_condition_null_safe("c", "a", &view.partition_keys);
    let pk_match_computed = key_join_condition("c", "a", &view.source.primary_keys);
    let part_match_active =
        key_join_condition_null_safe("active", "a", &view.partition_keys);
    format!(
        "with {affected}, {computed}, \
         already as (select distinct {pks} from mv \
                     where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv \
                    where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         inserts as (select {computed_parts}, {computed_pks}, c.{value}, \
                            'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from computed c \
                     where exists (select 1 from affected a where {part_match_computed}) \
                       and not exists (select 1 from already a where {pk_match_computed})), \
         deletes as (select {parts}, {pks}, {value}, \
                            'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from active \
                     where exists (select 1 from affected a where {part_match_active})) \
         select * from deletes union all select * from inserts \
         order by {pks}, \"rowKinds\"",
        affected = window_affected_cte(view),
        computed = window_function_cte(view, "src"),
    )
}

/// The `affected` CTE of a top-k refresh: the groups in the delta plus the
/// groups of the MV rows whose primary keys changed.
fn top_k_affected_cte(view: &TopKView) -> String {
    let groups = quoted_list(&view.group_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let pk_match = key_join_condition("m", "d", &view.source.primary_keys);
    format!(
        "delta_groups as (select distinct {groups} from delta), \
         delta_rows as (select distinct {pks} from delta), \
         old_groups as (select distinct {groups} from mv m \
                        join delta_rows d on {pk_match}), \
         affected as (select * from delta_groups union select * from old_groups)"
    )
}

/// The `computed` CTE of a top-k view: the projected rows with their row
/// number inside the group (ties broken by the source primary keys).
fn top_k_computed_cte(view: &TopKView, source_alias: &str) -> String {
    let groups = quoted_list(&view.group_keys);
    let mut order = view.order_keys.clone();
    order.extend(view.source.primary_keys.iter().cloned());
    let orders = quoted_list(&order);
    let output = quoted_list(&top_k_output_columns(view));
    let filter = source_delete_filter(source_alias, change_column(&view.source));
    let rank = quote_ident(IVM_TOP_K_RANK_COLUMN);
    format!(
        "computed as (select {output}, \
         cast(row_number() over (partition by {groups} order by {orders}) as bigint) \
             as {rank} \
         from {source_alias} where {filter})"
    )
}

/// SQL for one top-k refresh window: recompute the affected groups and rewrite
/// their MV rows (`delete` then `insert`).
fn top_k_refresh_sql(view: &TopKView, epoch: i64) -> String {
    let output = quoted_list(&top_k_output_columns(view));
    let pks = quoted_list(&view.source.primary_keys);
    let rank = quote_ident(IVM_TOP_K_RANK_COLUMN);
    let group_match_computed = key_join_condition_null_safe("c", "a", &view.group_keys);
    let pk_match_computed = key_join_condition("c", "a", &view.source.primary_keys);
    let group_match_active =
        key_join_condition_null_safe("active", "a", &view.group_keys);
    format!(
        "with {affected}, {computed}, \
         already as (select distinct {pks} from mv \
                     where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv \
                    where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         inserts as (select {output}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from computed c \
                     where c.{rank} <= {limit} \
                       and exists (select 1 from affected a where {group_match_computed}) \
                       and not exists (select 1 from already a where {pk_match_computed})), \
         deletes as (select {output}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from active \
                     where exists (select 1 from affected a where {group_match_active})) \
         select * from deletes union all select * from inserts \
         order by {pks}, \"rowKinds\"",
        affected = top_k_affected_cte(view),
        computed = top_k_computed_cte(view, "src"),
        limit = view.limit,
    )
}

/// SQL for a full top-k rebuild.
fn top_k_rebuild_sql(view: &TopKView, epoch: i64) -> String {
    let output = quoted_list(&top_k_output_columns(view));
    let rank = quote_ident(IVM_TOP_K_RANK_COLUMN);
    format!(
        "with {computed} \
         select {output}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from computed where {rank} <= {limit}",
        computed = top_k_computed_cte(view, "src"),
        limit = view.limit,
    )
}

/// SQL for a full `ROW_NUMBER()` rebuild.
fn window_rebuild_sql(view: &WindowView, epoch: i64) -> String {
    let parts = quoted_list(&view.partition_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let value = quote_ident(view.function.column_name());
    format!(
        "with {computed} \
         select {parts}, {pks}, {value}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from computed",
        computed = window_function_cte(view, "src"),
    )
}

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
    for key in &view.left.primary_keys {
        let field = view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "semi/anti view {}: key column {key} is not in the left source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "semi/anti view {}: key column {key} must be non-nullable",
                view.view_id
            ));
        }
    }
    if view.join_keys.is_empty() && view.conditions.is_empty() {
        return Err(report!(
            "semi/anti view {} needs at least one join key or condition",
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
    for condition in &view.conditions {
        view.left
            .schema
            .field_with_name(&condition.left_column)
            .map_err(|_| {
                report!(
                    "semi/anti view {}: condition column {} is not in the left source",
                    view.view_id,
                    condition.left_column
                )
            })?;
        view.right
            .schema
            .field_with_name(&condition.right_column)
            .map_err(|_| {
                report!(
                    "semi/anti view {}: condition column {} is not in the right source",
                    view.view_id,
                    condition.right_column
                )
            })?;
    }
    for column in &view.output_columns {
        view.left.schema.field_with_name(column).map_err(|_| {
            report!(
                "semi/anti view {}: output column {column} is not in the left source",
                view.view_id
            )
        })?;
    }
    if !view.output_columns.is_empty() {
        for key in &view.left.primary_keys {
            if !view.output_columns.contains(key) {
                return Err(report!(
                    "semi/anti view {}: output columns must contain the left key {key}",
                    view.view_id
                ));
            }
        }
    }
    Ok(())
}

/// The left columns materialized by a semi/anti view.
fn semi_anti_output_columns(view: &SemiAntiView) -> Vec<String> {
    if view.output_columns.is_empty() {
        view.left
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        view.output_columns.clone()
    }
}

/// The left columns a semi/anti refresh must read: the output, the left
/// primary keys, the condition columns and the change column.
fn semi_anti_left_columns(view: &SemiAntiView) -> Vec<String> {
    let mut columns = semi_anti_output_columns(view);
    for column in view.left.primary_keys.iter().chain(
        view.conditions
            .iter()
            .map(|condition| &condition.left_column),
    ) {
        if !columns.contains(column) {
            columns.push(column.clone());
        }
    }
    if let Some(change) = change_column(&view.left)
        && !columns.iter().any(|column| column == change)
    {
        columns.push(change.to_string());
    }
    columns
}

/// The right columns a semi/anti refresh must read: the equality keys, the
/// condition columns, the right primary keys and the change column.
fn semi_anti_right_columns(view: &SemiAntiView) -> Vec<String> {
    let mut columns = Vec::new();
    for column in view
        .join_keys
        .iter()
        .chain(
            view.conditions
                .iter()
                .map(|condition| &condition.right_column),
        )
        .chain(view.right.primary_keys.iter())
    {
        if !columns.contains(column) {
            columns.push(column.clone());
        }
    }
    if let Some(change) = change_column(&view.right)
        && !columns.iter().any(|column| column == change)
    {
        columns.push(change.to_string());
    }
    columns
}

/// An Arrow schema with `columns` in the given order.
fn project_schema(schema: &Schema, columns: &[String]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| {
            schema
                .field_with_name(column)
                .map(|field| Arc::new(field.clone()))
                .map_err(|_| report!("column {column} is not part of the schema"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// The alias of a right column in a semi/anti join, so conditions can name
/// both sides unambiguously.
fn semi_anti_right_alias(column: &str) -> String {
    format!("__ivm_right_{column}")
}

/// The comparison expression of one semi/anti condition.
fn semi_anti_compare(left: Expr, right: Expr, op: CompareOp) -> Expr {
    match op {
        CompareOp::Eq => left.eq(right),
        CompareOp::Ne => left.not_eq(right),
        CompareOp::Lt => left.lt(right),
        CompareOp::Le => left.lt_eq(right),
        CompareOp::Gt => left.gt(right),
        CompareOp::Ge => left.gt_eq(right),
    }
}

/// Join `left` against `right` with a semi/anti view's predicate.
///
/// `right` is projected to the referenced columns with `__ivm_right_*` aliases
/// so the conditions can reference both sides; equality conditions become join
/// keys (so DataFusion can hash them) and the rest a join filter.
fn semi_anti_join(
    left: DataFrame,
    right: DataFrame,
    view: &SemiAntiView,
    join_type: JoinType,
) -> Result<DataFrame> {
    let right_columns = semi_anti_right_columns(view);
    let right = right.select(
        right_columns
            .iter()
            .map(|column| col(column.as_str()).alias(semi_anti_right_alias(column)))
            .collect::<Vec<_>>(),
    )?;
    let mut on_pairs: Vec<(String, String)> = view
        .join_keys
        .iter()
        .map(|key| (key.clone(), semi_anti_right_alias(key)))
        .collect();
    let mut filter: Option<Expr> = None;
    for condition in &view.conditions {
        let alias = semi_anti_right_alias(&condition.right_column);
        if condition.op == CompareOp::Eq {
            let pair = (condition.left_column.clone(), alias);
            if !on_pairs.contains(&pair) {
                on_pairs.push(pair);
            }
        } else {
            let expr = semi_anti_compare(
                col(condition.left_column.as_str()),
                col(alias.as_str()),
                condition.op,
            );
            filter = Some(match filter {
                Some(filter) => filter.and(expr),
                None => expr,
            });
        }
    }
    let left_on = on_pairs
        .iter()
        .map(|(left, _)| left.as_str())
        .collect::<Vec<_>>();
    let right_on = on_pairs
        .iter()
        .map(|(_, right)| right.as_str())
        .collect::<Vec<_>>();
    Ok(left.join(right, join_type, &left_on, &right_on, filter)?)
}

/// The expression of one filter condition.
fn filter_expr(condition: &FilterCondition) -> Expr {
    let column = col(condition.column.as_str());
    match (&condition.value, condition.op) {
        (LiteralValue::Null, CompareOp::Eq) => column.is_null(),
        (LiteralValue::Null, CompareOp::Ne) => column.is_not_null(),
        (LiteralValue::Bool(value), op) => semi_anti_compare(column, lit(*value), op),
        (LiteralValue::Int(value), op) => semi_anti_compare(column, lit(*value), op),
        (LiteralValue::Float(value), op) => semi_anti_compare(column, lit(*value), op),
        (LiteralValue::String(value), op) => {
            semi_anti_compare(column, lit(value.as_str()), op)
        }
        // A comparison with NULL is unknown; such a filter matches nothing.
        _ => lit(false),
    }
}

/// The conjunction of a row view's filter conditions.
fn row_filter_predicate(view: &RowView) -> Option<Expr> {
    view.filters
        .iter()
        .map(filter_expr)
        .reduce(|left, right| left.and(right))
}

/// The source columns a [`RowView`] materializes.
fn row_output_columns(view: &RowView) -> Vec<String> {
    if view.output_columns.is_empty() {
        view.source
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        view.output_columns.clone()
    }
}

/// Validate that a projection/filter view can be maintained.
fn validate_row_view(view: &RowView) -> Result<()> {
    project_schema(&view.source.schema, &row_output_columns(view))?;
    if !view.source.primary_keys.is_empty() {
        for key in &view.source.primary_keys {
            let field = view.source.schema.field_with_name(key).map_err(|_| {
                report!(
                    "row view {}: key column {key} is not in the source",
                    view.view_id
                )
            })?;
            if field.is_nullable() {
                return Err(report!(
                    "row view {}: key column {key} must be non-nullable",
                    view.view_id
                ));
            }
            if !row_output_columns(view).contains(key) {
                return Err(report!(
                    "row view {}: output columns must contain the source key {key}",
                    view.view_id
                ));
            }
        }
    }
    for condition in &view.filters {
        view.source
            .schema
            .field_with_name(&condition.column)
            .map_err(|_| {
                report!(
                    "row view {}: filter column {} is not in the source",
                    view.view_id,
                    condition.column
                )
            })?;
        if condition.value == LiteralValue::Null
            && !matches!(condition.op, CompareOp::Eq | CompareOp::Ne)
        {
            return Err(report!(
                "row view {}: NULL can only be compared with = or <>",
                view.view_id
            ));
        }
    }
    Ok(())
}

/// Validate that a union-all view can be maintained.
fn validate_union_all_view(view: &UnionAllView) -> Result<()> {
    if view.sources.is_empty() {
        return Err(report!(
            "union all view {} needs at least one source",
            view.view_id
        ));
    }
    let keyed = !view.sources[0].primary_keys.is_empty();
    let first = &view.sources[0].schema;
    for source in &view.sources {
        if source.schema.fields().len() != first.fields().len()
            || source
                .schema
                .fields()
                .iter()
                .zip(first.fields())
                .any(|(left, right)| {
                    left.name() != right.name() || left.data_type() != right.data_type()
                })
        {
            return Err(report!(
                "union all view {}: source {} does not have the same schema",
                view.view_id,
                source.table_name
            ));
        }
        if keyed != !source.primary_keys.is_empty() {
            return Err(report!(
                "union all view {}: sources must be all keyed or all append-only",
                view.view_id
            ));
        }
        if keyed {
            for key in &source.primary_keys {
                let field = source.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "union all view {}: key column {key} is not in source {}",
                        view.view_id,
                        source.table_name
                    )
                })?;
                if field.is_nullable() {
                    return Err(report!(
                        "union all view {}: key column {key} must be non-nullable",
                        view.view_id
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Frames of the projection of a source onto `(columns..., __ivm_source)`.
fn union_all_projection(source: &IvmTable, index: usize) -> Result<Vec<Expr>> {
    let mut exprs = source
        .schema
        .fields()
        .iter()
        .map(|field| col(field.name().as_str()))
        .collect::<Vec<_>>();
    exprs.push(lit(index as i32).alias(IVM_SOURCE_COLUMN));
    Ok(exprs)
}

/// `UNION ALL` of several frames.
fn union_frames(mut frames: Vec<DataFrame>) -> Result<DataFrame> {
    let first = frames.remove(0);
    let mut combined = first;
    for frame in frames {
        combined = combined.union(frame)?;
    }
    Ok(combined)
}

/// The source columns a [`TopKView`] materializes.
fn top_k_output_columns(view: &TopKView) -> Vec<String> {
    if view.output_columns.is_empty() {
        view.source
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        view.output_columns.clone()
    }
}

/// Validate that a top-k view can be maintained.
fn validate_top_k_view(view: &TopKView) -> Result<()> {
    if view.limit <= 0 {
        return Err(report!(
            "top-k view {} needs a positive limit",
            view.view_id
        ));
    }
    if view.group_keys.is_empty() || view.order_keys.is_empty() {
        return Err(report!(
            "top-k view {} needs group and order keys",
            view.view_id
        ));
    }
    if view.source.primary_keys.is_empty() {
        return Err(report!(
            "top-k view {} needs a source with a primary key",
            view.view_id
        ));
    }
    for key in &view.source.primary_keys {
        let field = view.source.schema.field_with_name(key).map_err(|_| {
            report!(
                "top-k view {}: key column {key} is not in the source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "top-k view {}: key column {key} must be non-nullable",
                view.view_id
            ));
        }
    }
    for column in view.group_keys.iter().chain(view.order_keys.iter()) {
        view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "top-k view {}: column {column} is not in the source",
                view.view_id
            )
        })?;
    }
    let output = top_k_output_columns(view);
    project_schema(&view.source.schema, &output)?;
    for column in view
        .source
        .primary_keys
        .iter()
        .chain(view.group_keys.iter())
    {
        if !output.contains(column) {
            return Err(report!(
                "top-k view {}: output columns must contain {column}",
                view.view_id
            ));
        }
    }
    Ok(())
}
