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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use arrow_array::Array;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, ScalarValue};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::when;
use datafusion::logical_expr::{ExprSchemable, Operator};
use datafusion::prelude::{
    DataFrame, Expr, JoinType, SessionConfig, SessionContext, col, lit,
};
use lakesoul_io::constant::DEFAULT_PARTITION_DESC;
use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::transfusion::DataFileInfo;
use rootcause::report;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::metadata::{
    BeginEpoch, Consumer, Cursor, EpochRecord, IvmMetadata, PartitionVersion,
    SourceVersionRange, StateRole, StateTable, StateVersion,
};
use crate::provider::IvmTableProvider;
pub use crate::table::{IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN};
pub const IVM_GROUP_ROW_COLUMN: &str = "__ivm_group_row";
pub const IVM_ROW_COUNT_COLUMN: &str = "__ivm_row_count";
use crate::table::{
    IvmTable, IvmTableOptions, PartitionFiles, create_ivm_table, parse_partition_values,
};

mod aggregates;
mod chain;
mod fallback;
pub(crate) mod join_state;
mod joins;
pub(crate) mod keyed_plan;
mod lookup_chain;
mod multi_join;
mod recompute;
mod rows;
mod semi_anti;
mod sink;
mod unions;
mod windows;

pub use self::aggregates::*;
pub use self::chain::*;
pub use self::fallback::*;
pub use self::joins::*;
pub use self::lookup_chain::*;
pub use self::multi_join::*;
pub use self::recompute::*;
pub use self::rows::*;
pub use self::semi_anti::*;
pub use self::sink::*;
pub use self::unions::*;
pub use self::windows::*;

pub(crate) use self::joins::wide_pair_alias;

/// The `SUM` column of a [`sum_count_mv_schema`] materialized view.
pub const IVM_SUM_COLUMN: &str = "sum_v";

/// The `COUNT` column of a [`sum_count_mv_schema`] materialized view.
pub const IVM_COUNT_COLUMN: &str = "count_v";

/// The average column of an [`avg_mv_schema_for`] materialized view
/// (`AVG(value_column)`).
pub const IVM_AVG_COLUMN: &str = "avg_v";

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

/// The bucket column of an `NTILE()` [`WindowView`] materialized view.
pub const IVM_NTILE_COLUMN: &str = "ntile";

/// The column of a `PERCENT_RANK()` [`WindowView`] materialized view.
pub const IVM_PERCENT_RANK_COLUMN: &str = "percent_rank";

/// The column of a `CUME_DIST()` [`WindowView`] materialized view.
pub const IVM_CUME_DIST_COLUMN: &str = "cume_dist";

/// The variance column of a [`VarianceView`] materialized view.
pub const IVM_VARIANCE_COLUMN: &str = "variance_v";

/// The standard-deviation column of a [`VarianceView`] materialized view.
pub const IVM_STDDEV_COLUMN: &str = "stddev_v";

/// The median column of a [`MedianView`] materialized view.
pub const IVM_MEDIAN_COLUMN: &str = "median_v";

/// The source index column of a [`UnionAllView`] materialized view.
pub const IVM_SOURCE_COLUMN: &str = "__ivm_source";

/// The non-null row identity a derived outer-join step materializes: the
/// right side's primary keys encoded as a collision-free string (the left
/// keys are already non-null).  A consumer can then read the step as a keyed
/// source.
pub const IVM_PAIR_IDENTITY_COLUMN: &str = "__ivm_pair_id";

/// The grouping-set index of a `GROUPING SETS` row.
pub const IVM_GROUPING_COLUMN: &str = "__ivm_grouping";

/// The internal rank column of a [`TopKView`] computation (not materialized).
const IVM_TOP_K_RANK_COLUMN: &str = "__ivm_rank";

/// The rank column of a `RANK()` [`WindowView`] materialized view.
pub const IVM_RANK_COLUMN: &str = "rank";

/// The rank column of a `DENSE_RANK()` [`WindowView`] materialized view.
pub const IVM_DENSE_RANK_COLUMN: &str = "dense_rank";

/// The value column of a `LAG()` [`WindowView`] materialized view.
pub const IVM_LAG_COLUMN: &str = "lag_v";

/// The value column of a `LEAD()` [`WindowView`] materialized view.
pub const IVM_LEAD_COLUMN: &str = "lead_v";

/// The value column of a `FIRST_VALUE()` [`WindowView`] materialized view.
pub const IVM_FIRST_VALUE_COLUMN: &str = "first_value_v";

/// The value column of a `LAST_VALUE()` [`WindowView`] materialized view.
pub const IVM_LAST_VALUE_COLUMN: &str = "last_value_v";

/// The value column of a `NTH_VALUE()` [`WindowView`] materialized view.
pub const IVM_NTH_VALUE_COLUMN: &str = "nth_value_v";

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
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The summed column; `None` means `SUM(0)`, i.e. count only.
        value_column: Option<String>,
        /// The summed expression, when the argument is not a plain column
        /// (e.g. `SUM(v * 2)`); mutually exclusive with `value_column`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// The non-NULL count column of a `COUNT(column)` aggregate; `None`
        /// falls back to `value_column`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count_column: Option<String>,
        /// The `FILTER (WHERE ...)` predicate of the materialized aggregates
        /// (SUM/AVG/COUNT(column)); `None` when they are unfiltered.  Row
        /// counts (and so the groups themselves) stay unfiltered.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aggregate_filter: Option<String>,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized aggregate
        /// columns (`sum_v`, `count_v`, the non-NULL count and the group
        /// keys).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
        /// `true` for `AVG(value_column)`: the MV additionally exposes the
        /// average as [`IVM_AVG_COLUMN`].
        #[serde(default)]
        average: bool,
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
        /// The right source's key columns when they differ from the left
        /// ones; empty means the keys share their names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        right_keys: Vec<String>,
        /// The rendered key expressions aligned with `join_keys`; empty means
        /// every key is a same-named column of both sources.
        #[serde(default, skip_serializing_if = "JoinKeyExprs::is_empty")]
        key_exprs: JoinKeyExprs,
        /// The payload column of the left source.
        left_value: String,
        /// The payload column of the right source.
        right_value: String,
        /// The wide output columns beyond the join keys; empty means the
        /// compact `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
        /// The compact payload names the select list renamed, in
        /// `left_value` / `right_value` order; empty means the payload
        /// columns keep their own names.  A derived step materializes
        /// them under these names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        payload_names: Vec<String>,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
        /// An optional filter over the joined pair's payload columns
        /// (`left_value` / `right_value` or the wide output columns), for
        /// non-equality join conditions.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pair_filter: Option<String>,
        /// Optional keyed output state table, mirroring the output schema and keys.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state_table_id: Option<String>,
        /// Whether a NULL join key matches another NULL join key
        /// (`IS NOT DISTINCT FROM` keys).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        null_safe: bool,
    },
    /// A keyed lookup join with several 1:1 right sources that share one
    /// set of join keys.  The left side keeps one join-key index, so a right
    /// change maps back to affected left rows without scanning the left table.
    MultiLookupJoin {
        /// The view id.
        view_id: String,
        /// The left source table id (keyed).
        left_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The join keys, shared by every right source.
        join_keys: Vec<String>,
        /// The right sources in output order.
        rights: Vec<MultiLookupRight>,
        /// The materialized columns; source 0 is the left, source `i + 1`
        /// is `rights[i]`.
        output_columns: Vec<MultiLookupColumn>,
        /// The refresh interval hint persisted with the view.
        #[serde(default)]
        refresh_interval_ms: i64,
    },
    /// `LEFT JOIN` over two keyed sources: every left row with all its
    /// matching right rows (a NULL-padded row when nothing matches).
    ///
    /// The output is keyed by the pair of row identities, so the right primary
    /// keys are NULL for an unmatched left row.
    LeftJoin {
        /// The view id.
        view_id: String,
        /// The left source table id (keyed).
        left_table_id: String,
        /// The right source table id (keyed).
        right_table_id: String,
        /// The output table id, keyed by both row identities.
        output_table_id: String,
        /// The equi-join keys, present in both sources.
        #[serde(default, alias = "join_key", deserialize_with = "de_group_keys")]
        join_keys: Vec<String>,
        /// The right source's key columns when they differ from the left
        /// ones; empty means the keys share their names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        right_keys: Vec<String>,
        /// The rendered key expressions, aligned with `join_keys`; an empty
        /// entry means that key is a plain column of that side.
        #[serde(default, skip_serializing_if = "JoinKeyExprs::is_empty")]
        key_exprs: JoinKeyExprs,
        /// The payload column of the left source.
        left_value: String,
        /// The payload column of the right source; NULL without a match.
        right_value: String,
        /// The wide output columns; empty means the compact
        /// `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
        /// The compact payload names the select list renamed, in
        /// `left_value` / `right_value` order; empty means the payload
        /// columns keep their own names.  A derived step materializes
        /// them under these names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        payload_names: Vec<String>,
        /// Whether the view materializes a non-null row identity
        /// ([`IVM_PAIR_IDENTITY_COLUMN`], the encoded right primary keys), so
        /// its MV can be read as a keyed source: a derived outer-join step.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        identity_column: bool,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
        /// Whether a NULL join key matches another NULL join key
        /// (`IS NOT DISTINCT FROM` keys).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        null_safe: bool,
    },
    /// `CROSS JOIN` of two keyed sources: every pair of rows, keyed by both
    /// row identities. A change on either side rewrites only the pairs of the
    /// changed identities.
    CrossJoin {
        /// The view id.
        view_id: String,
        /// The left source table id (keyed).
        left_table_id: String,
        /// The right source table id (keyed).
        right_table_id: String,
        /// The output table id, keyed by both row identities.
        output_table_id: String,
        /// The payload column of the left source.
        left_value: String,
        /// The payload column of the right source.
        right_value: String,
        /// The wide output columns; empty means the compact
        /// `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
        /// The compact payload names the select list renamed, in
        /// `left_value` / `right_value` order; empty means the payload
        /// columns keep their own names.  A derived step materializes
        /// them under these names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        payload_names: Vec<String>,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
        /// An optional filter over the joined pair's payload columns
        /// (or the wide output columns), for cross-side join predicates.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pair_filter: Option<String>,
    },
    /// An inner equi-join of three or more sources.
    ///
    /// The join tree is flattened into a chain in post order
    /// (`((S0 ⋈ S1) ⋈ S2) ...`); every key pair connects two sources and its
    /// left endpoint precedes the right one in the chain. The output is keyed
    /// by every source's row identity (keyed sources) or append-only.
    MultiJoin {
        /// The view id.
        view_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The joined sources, in chain order.
        sources: Vec<MultiJoinSource>,
        /// The equality key pairs.
        keys: Vec<MultiJoinKey>,
        /// The output columns.
        columns: Vec<MultiJoinColumn>,
        /// The non-equality pair conditions.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        conditions: Vec<MultiJoinCondition>,
        /// Every key comparison matches NULL with NULL (`IS NOT DISTINCT
        /// FROM`); the optimizer keeps the marker uniform across a chain.
        #[serde(default)]
        null_safe: bool,
    },
    /// An aggregate view over one source with any mix of supported aggregate
    /// functions in a single statement.
    ///
    /// The aggregates are recomputed from the affected groups' current rows
    /// (like the other unmergeable aggregates), so the mix does not need a
    /// shared incremental state.
    MultiAgg {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The aggregates, in select order.
        aggregates: Vec<MultiAggSpec>,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized columns.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
        /// The extra source a `HAVING` subquery reads, when the statement has
        /// one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having_lookup: Option<HavingLookupSpec>,
        /// Optional keyed state table for the plain SUM shape.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state_table_id: Option<String>,
    },
    /// `FULL JOIN` over two keyed sources: all matching pairs plus the
    /// unmatched rows of either side (NULL-padded).
    FullJoin {
        /// The view id.
        view_id: String,
        /// The left source table id (keyed).
        left_table_id: String,
        /// The right source table id (keyed).
        right_table_id: String,
        /// The output table id, keyed by both row identities.
        output_table_id: String,
        /// The equi-join keys, present in both sources.
        #[serde(default, alias = "join_key", deserialize_with = "de_group_keys")]
        join_keys: Vec<String>,
        /// The payload column of the left source; NULL without a match.
        left_value: String,
        /// The payload column of the right source; NULL without a match.
        right_value: String,
        /// The right source's key columns when they differ from the left
        /// ones; empty means the keys share their names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        right_keys: Vec<String>,
        /// The rendered key expressions, aligned with `join_keys`; an empty
        /// entry means that key is a plain column of that side.
        #[serde(default, skip_serializing_if = "JoinKeyExprs::is_empty")]
        key_exprs: JoinKeyExprs,
        /// The wide output columns; empty means the compact
        /// `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
        /// The compact payload names the select list renamed, in
        /// `left_value` / `right_value` order; empty means the payload
        /// columns keep their own names.  A derived step materializes
        /// them under these names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        payload_names: Vec<String>,
        /// Whether the view materializes a non-null row identity
        /// ([`IVM_PAIR_IDENTITY_COLUMN`], both sides' encoded primary keys),
        /// so its MV can be read as a keyed source: a derived full-join step.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        identity_column: bool,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
        /// Whether a NULL join key matches another NULL join key
        /// (`IS NOT DISTINCT FROM` keys).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        null_safe: bool,
    },
    /// `LEFT JOIN` lookup: every left row with the right row its join keys
    /// reference (or NULL).
    ///
    /// The right source must be keyed by the join keys, so every left row has
    /// at most one match and the output is keyed by the left row identities.
    LookupJoin {
        /// The view id.
        view_id: String,
        /// The left source table id (keyed).
        left_table_id: String,
        /// The right source table id (keyed by the join keys).
        right_table_id: String,
        /// The output table id, keyed by the left primary keys.
        output_table_id: String,
        /// The left equi-join keys; the output key columns.
        #[serde(default, alias = "join_key", deserialize_with = "de_group_keys")]
        join_keys: Vec<String>,
        /// The right equi-join keys, parallel to `join_keys`; empty means the
        /// keys share their names with the left side.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        right_keys: Vec<String>,
        /// The rendered key expressions, aligned with `join_keys`; an empty
        /// entry means that key is a plain column of that side.
        #[serde(default, skip_serializing_if = "JoinKeyExprs::is_empty")]
        key_exprs: JoinKeyExprs,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the referenced right rows must satisfy; a left
        /// row whose match is filtered out keeps NULLs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
        /// The payload column of the left source.
        left_value: String,
        /// The payload column of the right source; NULL without a match.
        right_value: String,
        /// The wide output columns; empty means the compact
        /// `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
        /// The compact payload names the select list renamed, in
        /// `left_value` / `right_value` order; empty means the payload
        /// columns keep their own names.  A derived step materializes
        /// them under these names.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        payload_names: Vec<String>,
        /// Optional keyed output state table, mirroring the output schema and keys.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state_table_id: Option<String>,
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
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The min/max column; `None` when the argument is an expression.
        value_column: Option<String>,
        /// The rendered min/max expression.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// Whether the minimum or the maximum is maintained.
        min_max: MinMaxKind,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized aggregate
        /// columns (`value` and the group keys).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
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
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The distinct value column.
        value_column: String,
        /// The distinct value columns of a multi-column
        /// `COUNT(DISTINCT a, b)`; empty for the single-column shape, which
        /// uses `value_column` and the value-count state table. A
        /// multi-column distinct count has no signed per-value state and is
        /// maintained by recomputing the affected groups.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        value_columns: Vec<String>,
        /// Whether the distinct count or the distinct sum is maintained.
        agg: DistinctAggKind,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized aggregate
        /// columns (`value` and the group keys).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// `VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP` of a value column over
    /// a group.
    ///
    /// DataFusion computes the statistic with Welford's algorithm, which
    /// cannot be merged from signed deltas, so a refresh recomputes the
    /// affected groups from their current source rows.
    Variance {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The value column; `None` when the argument is an expression.
        #[serde(default)]
        value_column: Option<String>,
        /// The rendered value expression.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// Which statistic is maintained.
        statistic: VarianceKind,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// `MEDIAN(value_column)` over a group.
    ///
    /// The median cannot be merged from signed deltas either, so it is
    /// recomputed from the affected groups' current source rows like the
    /// variance family.
    Median {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The value column; `None` when the argument is an expression.
        #[serde(default)]
        value_column: Option<String>,
        /// The rendered value expression.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// `group_key`, `BOOL_AND(value)` or `BOOL_OR(value)` over the source
    /// rows; the statistic is recomputed from the affected groups.
    BoolAgg {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The boolean value column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_column: Option<String>,
        /// A rendered value expression; mutually exclusive with
        /// `value_column`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// Whether `BOOL_AND` or `BOOL_OR` is maintained.
        bool_agg: BoolAggKind,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// `group_key`, `APPROX_DISTINCT(value)` over the source rows; the
    /// statistic is recomputed from the affected groups (the sketch update is
    /// order independent, so a rebuild is consistent).
    ApproxDistinct {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The value column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_column: Option<String>,
        /// A rendered value expression; mutually exclusive with
        /// `value_column`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// `group_key`, `APPROX_PERCENTILE_CONT(value, percentile)` over the
    /// source rows; the statistic is recomputed from the affected groups.
    ApproxPercentile {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The value column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_column: Option<String>,
        /// A rendered value expression; mutually exclusive with
        /// `value_column`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// The rendered percentile literal (e.g. `0.5`).
        percentile: String,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// A deterministic scalar-aggregate function over the source rows:
    /// `BIT_AND`/`BIT_OR`/`BIT_XOR`, `CORR`/`COVAR_*`, the `REGR_*` family,
    /// `PERCENTILE_CONT` and the approximate medians/weighted percentiles.
    /// The statistic is recomputed from the affected groups.
    ComputedAgg {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The SQL aggregate function name.
        function: String,
        /// The aggregate arguments, in order.
        arguments: Vec<ComputedAggArg>,
        /// The rendered percentile literal of the ordered-set / weighted
        /// aggregates, appended after the arguments.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        percentile: Option<String>,
        /// The MV column holding the statistic.
        column: String,
        /// The result type of the aggregate.
        result: ComputedAggResult,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// `STRING_AGG(value, delimiter ORDER BY keys)` over a source, maintained
    /// by recomputing the affected groups.
    StringAgg {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The concatenated column; `None` when the argument is an
        /// expression.
        value_column: Option<String>,
        /// The rendered concatenated expression.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// The delimiter, rendered as a SQL string literal (e.g. `','`).
        delimiter: String,
        /// The rendered `ORDER BY` items inside the aggregate (required, so
        /// the concatenation is deterministic).
        #[serde(default)]
        order_by: Vec<String>,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// An optional `HAVING` predicate over the materialized column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
    },
    /// `ARRAY_AGG(value ORDER BY keys)` over a source, maintained by
    /// recomputing the affected groups.
    ArrayAgg {
        /// The view id.
        view_id: String,
        /// The source table id.
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The group key columns.
        #[serde(default, alias = "group_key", deserialize_with = "de_group_keys")]
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// The collected column; `None` when the argument is an expression.
        value_column: Option<String>,
        /// The rendered collected expression.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// The rendered `ORDER BY` items inside the aggregate (required, so
        /// the collected list is deterministic).
        #[serde(default)]
        order_by: Vec<String>,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
    },
    /// Window functions over a source, maintained by recomputing the
    /// affected partitions.
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
        /// The rendered `ORDER BY` items (e.g. `v desc`), when they differ
        /// from the plain ascending `order_keys`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        order_by: Vec<String>,
        /// The window columns, sharing the partition and ordering.
        columns: Vec<WindowColumn>,
        /// Input columns the step also carries (a derived window step a
        /// consumer reads); empty for a window over the user's source.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        payload_columns: Vec<String>,
        /// An optional filter applied before windowing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
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
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
        /// Whether a NULL join key matches another NULL join key, as the set
        /// operations and null-aware predicates do.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        null_safe: bool,
        /// Whether left rows sharing the join keys compete for the right
        /// multiplicity: `INTERSECT ALL` keeps the first `count_r` copies and
        /// `EXCEPT ALL` the `count_l - count_r` surplus.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        count_mode: bool,
        /// The rendered aggregate call over the right columns; when present
        /// the right side contributes one value per join key (a correlated
        /// scalar subquery) instead of a row match.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_aggregate: Option<String>,
        /// The right group keys, aligned with `join_keys`; empty means the
        /// same names as `join_keys`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        right_keys: Vec<String>,
        /// The rendered comparison predicate over the left row and the
        /// aggregate output column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        match_predicate: Option<String>,
    },
    /// A left join against one aggregate row per join key (a select-list
    /// correlated scalar subquery): the MV keeps one row per left key with the
    /// aggregate value appended (NULL when the key has no right rows).
    LeftAggregate {
        /// The view id.
        view_id: String,
        /// The left source table id (keyed).
        left_table_id: String,
        /// The right source table id.
        right_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The left join keys.
        join_keys: Vec<String>,
        /// The right group keys, aligned with `join_keys`; empty means the
        /// same names as `join_keys`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        right_keys: Vec<String>,
        /// The rendered aggregate call over the right columns.
        right_aggregate: String,
        /// The MV column the aggregate value is materialized into.
        aggregate_column: String,
        /// Whether the aggregate is a COUNT, which counts zero for a left row
        /// whose key has no right rows (every other aggregate is NULL).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        count_zero: bool,
        /// The left columns materialized; the left primary keys are always
        /// contained.
        output_columns: Vec<String>,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
        /// The computed select-list expressions over the scalar subquery
        /// value, appended after the aggregate column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_exprs: Vec<LeftAggregateOutput>,
    },
    /// A left-deep chain of keyed 1:1 lookups over a base table: every base
    /// row appears once, with NULL payloads for the steps it does not match.
    LookupChain {
        /// The view id.
        view_id: String,
        /// The chain sources; index 0 is the base.
        sources: Vec<LookupChainSource>,
        /// The join steps in chain order; step `k` joins source `k + 1`.
        steps: Vec<LookupChainStep>,
        /// The materialized columns.
        output_columns: Vec<LookupChainColumn>,
        /// The materialized view table id.
        mv_table_id: String,
        /// Every step compares its keys null-safely (`IS NOT DISTINCT FROM`);
        /// the optimizer keeps the marker uniform across a chain.
        #[serde(default)]
        null_safe: bool,
    },
    /// `GROUP BY GROUPING SETS`/`ROLLUP`/`CUBE` over a keyed source with
    /// `SUM`/`COUNT`/`AVG`: the MV keeps one row per (grouping index, key
    /// tuple), and the keys a set does not group by are NULL.
    GroupingSets {
        /// The view id.
        view_id: String,
        /// The source table id (keyed).
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The flat key columns, in the aggregate's output order.
        group_keys: Vec<String>,
        /// The rendered group expressions, parallel to `group_keys`; empty
        /// means every key is a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_exprs: Vec<String>,
        /// Each grouping set as indices into `group_keys`.
        groupings: Vec<Vec<usize>>,
        /// The summed column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_column: Option<String>,
        /// A rendered value expression; mutually exclusive with
        /// `value_column`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_expr: Option<String>,
        /// A `COUNT(column)` column.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        count_column: Option<String>,
        /// The shared aggregate `FILTER (WHERE ...)` predicate.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aggregate_filter: Option<String>,
        /// Whether the MV also carries `avg_v`.
        #[serde(default)]
        average: bool,
        /// An optional filter the contributing rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// Materialized `GROUPING(key)` columns: the flat key index and the
        /// output column name.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        grouping_columns: Vec<GroupingColumn>,
        /// A general aggregate list (any mix of the supported functions);
        /// empty keeps the incremental SUM/COUNT/AVG layout.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        aggregates: Vec<MultiAggSpec>,
        /// An optional `HAVING` predicate over the MV columns.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        having: Option<String>,
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
        /// The rendered projection expressions, parallel to `output_columns`;
        /// empty means every column is projected as a plain column.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_exprs: Vec<String>,
        /// An optional filter the source rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
        /// The scalar subquery inputs of the filter, when it has any; their
        /// tables are watched alongside the source.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scalar: Option<RowScalarSpec>,
    },
    /// `UNION ALL` of several sources with the same schema.
    UnionAll {
        /// The view id.
        view_id: String,
        /// The sources, in output order.
        sources: Vec<UnionSourceSpec>,
        /// The materialized view table id.
        mv_table_id: String,
        /// The branch-index column name.  A nested union carries its inner
        /// branch index as a data column, so every union names its own marker
        /// and the two levels cannot collide.
        #[serde(default = "default_union_source_column")]
        source_column: String,
    },
    /// `UNION` (distinct) of several sources with the same schema: every
    /// distinct row with its occurrence count.
    ///
    /// The materialized view keeps one row per distinct value together with
    /// the number of contributing source rows; a refresh adjusts the count by
    /// the signed delta and drops the row when the count reaches zero.
    UnionDistinct {
        /// The view id.
        view_id: String,
        /// The sources, in output order.
        sources: Vec<UnionSourceSpec>,
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
        /// The rendered `ORDER BY` items (e.g. `v desc`), when they differ
        /// from the plain ascending `order_keys`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        order_by: Vec<String>,
        /// The projected source columns; empty means all of them.
        #[serde(default)]
        output_columns: Vec<String>,
        /// The rendered projection expressions, parallel to `output_columns`
        /// (empty means every column is projected unchanged).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_exprs: Vec<String>,
        /// How many rows to keep per group.
        limit: i64,
        /// How many rows to skip before keeping them (a `LIMIT ... OFFSET`).
        #[serde(default)]
        offset: i64,
        /// An optional filter applied before ranking.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
    },
    /// Several window clauses (the chained `WindowAggr` nodes of one
    /// statement) over one source: every clause's window columns keyed by the
    /// source primary keys, with each clause's partition keys materialized as
    /// value columns.
    MultiWindow {
        /// The view id.
        view_id: String,
        /// The source table id (keyed).
        source_table_id: String,
        /// The materialized view table id.
        mv_table_id: String,
        /// The window clauses, in select-list order.
        windows: Vec<WindowGroupSpec>,
        /// Input columns the step also carries (a derived multi-window step a
        /// consumer reads); empty for a view over the user's source.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        payload_columns: Vec<String>,
        /// An optional filter applied before windowing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
    },
    /// A statement the incremental analyzer does not maintain: every refresh
    /// recomputes it in full (Tier-2) against the current source state.  The
    /// view has no state tables and its definition identity is the statement.
    Fallback {
        /// The view id.
        view_id: String,
        /// The `SELECT` that produces the MV rows, exactly as written.
        statement: String,
        /// The source table ids the statement reads.
        tables: Vec<String>,
        /// Why the incremental analyzer rejected the shape, for the metadata.
        /// It is not part of the definition identity.
        reason: String,
        /// The partition scope a refresh can recompute on its own, when the
        /// statement is partition local.  Derived metadata: it is not part of
        /// the definition identity.
        partition_scope: Option<PartitionScope>,
        /// The materialized view table id.
        mv_table_id: String,
    },
    /// A projection sink from an internal maintenance state to a user target
    /// table.  The source is the keyed maintenance MV; the sink writes the
    /// user target by the target primary key.
    Sink {
        /// The user view id (the target table id).
        view_id: String,
        /// The maintenance state MV.
        source_table_id: String,
        /// The user target table id.
        mv_table_id: String,
        /// The target output columns, in target schema order.
        #[serde(default)]
        columns: Vec<SinkColumn>,
        /// The target primary keys, so a key change rebuilds the target.
        #[serde(default)]
        primary_keys: Vec<String>,
        /// The target range partition columns, so a partition-layout change
        /// rebuilds the target.
        #[serde(default)]
        range_partition_columns: Vec<String>,
        /// The definition identity of the source maintenance view, so a
        /// changed source shape rebuilds the target.
        #[serde(default)]
        source_definition: String,
        /// The source view generation the sink last consumed.  A rebuild of
        /// the maintenance state increments it and forces a target rebuild.
        #[serde(default)]
        source_generation: i64,
    },
}

fn default_union_source_column() -> String {
    IVM_SOURCE_COLUMN.to_string()
}

/// One output column of a [`ViewSpec::Sink`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkColumn {
    /// The target column name.
    pub name: String,
    /// The maintenance-state output column carrying the value; empty when it
    /// has the target name.  A Flink `INSERT INTO` maps the select list by
    /// position, so the names may differ.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    /// The encoded arrow data type.
    pub result: String,
    /// Whether the target column is nullable.
    pub nullable: bool,
}

impl SinkColumn {
    /// The maintenance-state column this target column reads.
    pub fn source(&self) -> &str {
        if self.source.is_empty() {
            &self.name
        } else {
            &self.source
        }
    }
}

impl ViewSpec {
    /// The view id this spec is registered under.
    pub fn view_id(&self) -> &str {
        match self {
            ViewSpec::SumCount { view_id, .. }
            | ViewSpec::Variance { view_id, .. }
            | ViewSpec::Median { view_id, .. }
            | ViewSpec::BoolAgg { view_id, .. }
            | ViewSpec::ApproxDistinct { view_id, .. }
            | ViewSpec::ApproxPercentile { view_id, .. }
            | ViewSpec::ComputedAgg { view_id, .. }
            | ViewSpec::StringAgg { view_id, .. }
            | ViewSpec::ArrayAgg { view_id, .. }
            | ViewSpec::Join { view_id, .. }
            | ViewSpec::MultiJoin { view_id, .. }
            | ViewSpec::MultiAgg { view_id, .. }
            | ViewSpec::LookupJoin { view_id, .. }
            | ViewSpec::MultiLookupJoin { view_id, .. }
            | ViewSpec::LeftJoin { view_id, .. }
            | ViewSpec::FullJoin { view_id, .. }
            | ViewSpec::CrossJoin { view_id, .. }
            | ViewSpec::MinMax { view_id, .. }
            | ViewSpec::DistinctAgg { view_id, .. }
            | ViewSpec::Window { view_id, .. }
            | ViewSpec::SemiAnti { view_id, .. }
            | ViewSpec::LeftAggregate { view_id, .. }
            | ViewSpec::LookupChain { view_id, .. }
            | ViewSpec::GroupingSets { view_id, .. }
            | ViewSpec::Row { view_id, .. }
            | ViewSpec::UnionAll { view_id, .. }
            | ViewSpec::UnionDistinct { view_id, .. }
            | ViewSpec::MultiWindow { view_id, .. }
            | ViewSpec::TopK { view_id, .. }
            | ViewSpec::Fallback { view_id, .. }
            | ViewSpec::Sink { view_id, .. } => view_id,
        }
    }

    /// The source table ids the view reads, for refreshing cascading views in
    /// dependency order.
    pub fn source_table_ids(&self) -> Vec<&str> {
        let mut ids = Vec::new();
        match self {
            ViewSpec::SumCount {
                source_table_id, ..
            }
            | ViewSpec::Variance {
                source_table_id, ..
            }
            | ViewSpec::Median {
                source_table_id, ..
            }
            | ViewSpec::BoolAgg {
                source_table_id, ..
            }
            | ViewSpec::ApproxDistinct {
                source_table_id, ..
            }
            | ViewSpec::ApproxPercentile {
                source_table_id, ..
            }
            | ViewSpec::ComputedAgg {
                source_table_id, ..
            }
            | ViewSpec::StringAgg {
                source_table_id, ..
            }
            | ViewSpec::ArrayAgg {
                source_table_id, ..
            }
            | ViewSpec::MinMax {
                source_table_id, ..
            }
            | ViewSpec::DistinctAgg {
                source_table_id, ..
            }
            | ViewSpec::Window {
                source_table_id, ..
            }
            | ViewSpec::MultiWindow {
                source_table_id, ..
            }
            | ViewSpec::TopK {
                source_table_id, ..
            }
            | ViewSpec::GroupingSets {
                source_table_id, ..
            }
            | ViewSpec::Row {
                source_table_id, ..
            }
            | ViewSpec::Sink {
                source_table_id, ..
            } => {
                ids.push(source_table_id.as_str());
            }
            ViewSpec::MultiAgg {
                source_table_id,
                having_lookup,
                ..
            } => {
                ids.push(source_table_id.as_str());
                if let Some(lookup) = having_lookup {
                    ids.push(lookup.table_id.as_str());
                }
            }
            ViewSpec::Fallback { tables, .. } => {
                ids.extend(tables.iter().map(String::as_str));
            }
            ViewSpec::MultiLookupJoin {
                left_table_id,
                rights,
                ..
            } => {
                ids.push(left_table_id.as_str());
                ids.extend(rights.iter().map(|right| right.table_id.as_str()));
            }
            ViewSpec::Join {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::LookupJoin {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::LeftJoin {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::FullJoin {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::CrossJoin {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::SemiAnti {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::LeftAggregate {
                left_table_id,
                right_table_id,
                ..
            } => {
                ids.push(left_table_id.as_str());
                ids.push(right_table_id.as_str());
            }
            ViewSpec::MultiJoin { sources, .. } => {
                ids.extend(sources.iter().map(|source| source.table_id.as_str()));
            }
            ViewSpec::UnionAll { sources, .. }
            | ViewSpec::UnionDistinct { sources, .. } => {
                ids.extend(sources.iter().map(|source| source.table_id.as_str()));
            }
            ViewSpec::LookupChain { sources, .. } => {
                ids.extend(sources.iter().map(|source| source.table_id.as_str()));
            }
        }
        ids
    }

    /// Patch the identity of a derived spec: the analysis of a derived source
    /// uses placeholder ids (the derived MV table does not exist yet), and the
    /// executor sets the real view and MV table ids once it created the
    /// table.
    pub(crate) fn set_derived_identity(
        &mut self,
        view_id: &str,
        mv_table_id: &str,
    ) -> Result<()> {
        match self {
            ViewSpec::Join {
                view_id: id,
                output_table_id: mv,
                ..
            }
            | ViewSpec::LookupJoin {
                view_id: id,
                output_table_id: mv,
                ..
            }
            | ViewSpec::LeftJoin {
                view_id: id,
                output_table_id: mv,
                ..
            }
            | ViewSpec::FullJoin {
                view_id: id,
                output_table_id: mv,
                ..
            }
            | ViewSpec::CrossJoin {
                view_id: id,
                output_table_id: mv,
                ..
            }
            | ViewSpec::MultiJoin {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::MultiLookupJoin {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::SumCount {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::MultiAgg {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::MinMax {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::DistinctAgg {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::Window {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::MultiWindow {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::UnionAll {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::UnionDistinct {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::LookupChain {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::SemiAnti {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::TopK {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::Row {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::Variance {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::Median {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::BoolAgg {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::ApproxDistinct {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::ApproxPercentile {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::ComputedAgg {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::StringAgg {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::ArrayAgg {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::LeftAggregate {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::GroupingSets {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::Fallback {
                view_id: id,
                mv_table_id: mv,
                ..
            }
            | ViewSpec::Sink {
                view_id: id,
                mv_table_id: mv,
                ..
            } => {
                *id = view_id.to_string();
                *mv = mv_table_id.to_string();
                Ok(())
            }
        }
    }

    /// Mark a derived outer-join step to materialize the non-null row
    /// identity column ([`IVM_PAIR_IDENTITY_COLUMN`]), so a consumer can read
    /// the step's MV as a keyed source.  Other kinds are left untouched.
    pub(crate) fn set_derived_identity_column(&mut self) -> Result<()> {
        match self {
            ViewSpec::LeftJoin {
                identity_column, ..
            }
            | ViewSpec::FullJoin {
                identity_column, ..
            } => *identity_column = true,
            _ => {}
        }
        Ok(())
    }

    /// Patch the state table id of a derived spec whose value-count view
    /// needs one (`MIN`/`MAX` and the `DISTINCT` aggregates).
    pub(crate) fn set_derived_state_table(&mut self, state_table_id: &str) -> Result<()> {
        match self {
            ViewSpec::MinMax {
                state_table_id: state,
                ..
            }
            | ViewSpec::DistinctAgg {
                state_table_id: state,
                ..
            } => {
                *state = state_table_id.to_string();
                Ok(())
            }
            ViewSpec::Join {
                state_table_id: state,
                ..
            }
            | ViewSpec::LookupJoin {
                state_table_id: state,
                ..
            }
            | ViewSpec::MultiAgg {
                state_table_id: state,
                ..
            } => {
                *state = Some(state_table_id.to_string());
                Ok(())
            }
            other => Err(report!(
                "a derived {} view does not use a state table",
                other.kind()
            )),
        }
    }

    /// A stable label for the view kind, used by the metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            ViewSpec::SumCount { .. } => "sum_count",
            ViewSpec::Variance { .. } => "variance",
            ViewSpec::Median { .. } => "median",
            ViewSpec::BoolAgg { .. } => "bool_agg",
            ViewSpec::ApproxDistinct { .. } => "approx_distinct",
            ViewSpec::ApproxPercentile { .. } => "approx_percentile",
            ViewSpec::ComputedAgg { .. } => "computed_agg",
            ViewSpec::StringAgg { .. } => "string_agg",
            ViewSpec::ArrayAgg { .. } => "array_agg",
            ViewSpec::Join { .. } => "join",
            ViewSpec::MultiJoin { .. } => "multi_join",
            ViewSpec::MultiAgg { .. } => "multi_agg",
            ViewSpec::LookupJoin { .. } => "lookup_join",
            ViewSpec::MultiLookupJoin { .. } => "multi_lookup_join",
            ViewSpec::LeftJoin { .. } => "left_join",
            ViewSpec::FullJoin { .. } => "full_join",
            ViewSpec::CrossJoin { .. } => "cross_join",
            ViewSpec::MinMax { .. } => "min_max",
            ViewSpec::DistinctAgg { .. } => "distinct_agg",
            ViewSpec::Window { .. } => "window",
            ViewSpec::SemiAnti { .. } => "semi_anti",
            ViewSpec::LeftAggregate { .. } => "left_aggregate",
            ViewSpec::LookupChain { .. } => "lookup_chain",
            ViewSpec::GroupingSets { .. } => "grouping_sets",
            ViewSpec::Row { .. } => "row",
            ViewSpec::UnionAll { .. } => "union_all",
            ViewSpec::UnionDistinct { .. } => "union_distinct",
            ViewSpec::TopK { .. } => "top_k",
            ViewSpec::MultiWindow { .. } => "multi_window",
            ViewSpec::Fallback { .. } => "fallback",
            ViewSpec::Sink { .. } => "sink",
        }
    }
}

/// One right source of a [`ViewSpec::MultiLookupJoin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiLookupRight {
    /// The right table id; its primary key must equal `join_keys`.
    pub table_id: String,
    /// An optional filter the right rows must satisfy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// One materialized column of a [`ViewSpec::MultiLookupJoin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiLookupColumn {
    /// Source index: 0 is the left, `i + 1` is `rights[i]`.
    pub source: usize,
    /// The source column.
    pub column: String,
    /// The MV column name.
    pub name: String,
}

/// One source of a [`ViewSpec::LookupChain`] (index 0 is the base).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupChainSource {
    /// The table id.
    pub table_id: String,
    /// An optional filter this source's rows must satisfy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// One `[LEFT] JOIN` step of a [`ViewSpec::LookupChain`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupChainStep {
    /// The joined source index (1-based into the sources).
    pub source: usize,
    /// `true` for a LEFT join (the row keeps NULL payloads when unmatched).
    pub left: bool,
    /// The base columns the step joins on.
    pub keys: Vec<String>,
    /// The joined source's key columns aligned with [`Self::keys`].
    pub right_keys: Vec<String>,
    /// The source index of each key (0 is the base); empty means every key
    /// comes from the base.  A key may reference any earlier source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_sources: Vec<usize>,
    /// The rendered key expressions, aligned with `keys`; an empty entry
    /// means that key is a plain column of its source.
    #[serde(default, skip_serializing_if = "JoinKeyExprs::is_empty")]
    pub key_exprs: JoinKeyExprs,
    /// `true` when the joined source's primary key is exactly `right_keys`,
    /// so one row matches at most one chain row (a 1:1 lookup).  `false` for
    /// a 1:N lookup, which materializes the matched row's identity.
    #[serde(default = "lookup_chain_step_unique_default")]
    pub unique: bool,
}

/// `LookupChainStep::unique` defaults to `true`, so specs written before 1:N
/// steps existed keep their 1:1 meaning.
fn lookup_chain_step_unique_default() -> bool {
    true
}

/// One materialized column of a [`ViewSpec::LookupChain`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupChainColumn {
    /// The source index (0 is the base).
    pub source: usize,
    /// The source column.
    pub column: String,
    /// The MV column name.
    pub name: String,
}

/// A typed view reconstructed from a persisted [`ViewSpec`].
enum SpecView {
    SumCount(SumCountView),
    Variance(VarianceView),
    Median(MedianView),
    BoolAgg(BoolAggView),
    ApproxDistinct(ApproxDistinctView),
    ApproxPercentile(ApproxPercentileView),
    ComputedAgg(ComputedAggView),
    StringAgg(StringAggView),
    ArrayAgg(ArrayAggView),
    Join(JoinView),
    MultiJoin(MultiJoinView),
    MultiAgg(MultiAggView),
    LookupJoin(LookupJoinView),
    MultiLookupJoin(MultiLookupJoinView),
    LeftJoin(LeftJoinView),
    FullJoin(FullJoinView),
    CrossJoin(CrossJoinView),
    MinMax(MinMaxView),
    DistinctAgg(DistinctAggView),
    Window(WindowView),
    SemiAnti(SemiAntiView),
    LeftAggregate(LeftAggregateView),
    LookupChain(LookupChainView),
    GroupingSets(GroupingSetsView),
    Row(RowView),
    UnionAll(UnionAllView),
    UnionDistinct(UnionDistinctView),
    TopK(TopKView),
    MultiWindow(MultiWindowView),
    Fallback(FallbackView),
    Sink(SinkView),
}

/// The extra source a `HAVING` subquery reads: its rows are aggregated and
/// joined into the affected groups before the predicate is evaluated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HavingLookupSpec {
    /// The lookup source table id.
    pub table_id: String,
    /// The rendered aggregate over the lookup rows (for example `max("v")`).
    pub aggregate: String,
    /// The group keys joined to the lookup keys; empty means the aggregate is
    /// global and joined with a cross join.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub join_keys: Vec<String>,
    /// The lookup keys, aligned with `join_keys` (empty means the same names).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub right_keys: Vec<String>,
    /// The name the joined lookup value carries in the recompute SQL; the
    /// rendered `having` references it.
    pub column: String,
    /// An optional filter the lookup rows must satisfy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// One computed output of a [`ViewSpec::LeftAggregate`] view:
/// the rendered expression over the scalar subquery value
/// ([`crate::runtime::semi_anti::IVM_RIGHT_AGG_COLUMN`]) and the MV column
/// holding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeftAggregateOutput {
    /// The rendered expression over the right aggregate column.
    pub call: String,
    /// The MV column holding the value.
    pub column: String,
    /// The encoded result type of the expression.
    pub result: String,
}

/// One aggregate of a [`ViewSpec::MultiAgg`] view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiAggSpec {
    /// The rendered aggregate call over the source columns.
    pub call: String,
    /// The MV column holding the aggregate.
    pub column: String,
    /// The encoded result type of the aggregate.
    pub result: String,
}

/// A portable encoding of the view column types.
///
/// Scalars use their short names.  Nested types recurse through JSON field
/// descriptors (`{"name":…,"nullable":…,"type":…}`), so element and field
/// names survive the round trip exactly: a list carries its element
/// descriptor, a struct an array of descriptors and a map the descriptor of
/// its entries struct.
pub(crate) fn encode_data_type(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "largeutf8".to_string(),
        DataType::Utf8View => "utf8view".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::LargeBinary => "largebinary".to_string(),
        DataType::BinaryView => "binaryview".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Timestamp(unit, timezone) => format!(
            "timestamp:{}:{}",
            match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            },
            timezone.as_deref().unwrap_or("")
        ),
        DataType::Decimal128(precision, scale) => {
            format!("decimal128:{precision}:{scale}")
        }
        DataType::Decimal256(precision, scale) => {
            format!("decimal256:{precision}:{scale}")
        }
        DataType::List(field) => format!("list:{}", encode_field(field)?),
        DataType::LargeList(field) => format!("large_list:{}", encode_field(field)?),
        DataType::FixedSizeList(field, size) => {
            format!("fixed_list:{size}:{}", encode_field(field)?)
        }
        DataType::Struct(fields) => format!(
            "struct:[{}]",
            fields
                .iter()
                .map(|field| encode_field(field))
                .collect::<Result<Vec<_>>>()?
                .join(",")
        ),
        DataType::Map(field, sorted) => {
            format!(
                "map:{}:{}",
                if *sorted { 1 } else { 0 },
                encode_field(field)?
            )
        }
        other => {
            return Err(crate::sql::unsupported(format!(
                "the view column type {other} is not supported"
            )));
        }
    })
}

/// One field of a nested type as a JSON descriptor: its name, nullability and
/// the recursive type encoding.
fn encode_field(field: &Field) -> Result<String> {
    Ok(serde_json::json!({
        "name": field.name(),
        "nullable": field.is_nullable(),
        "type": encode_data_type(field.data_type())?,
    })
    .to_string())
}

pub(crate) fn decode_data_type(text: &str) -> Result<DataType> {
    Ok(match text {
        "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "largeutf8" => DataType::LargeUtf8,
        "utf8view" => DataType::Utf8View,
        "binary" => DataType::Binary,
        "largebinary" => DataType::LargeBinary,
        "binaryview" => DataType::BinaryView,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        other => {
            if let Some(rest) = other.strip_prefix("timestamp:") {
                let (unit, timezone) = rest
                    .split_once(':')
                    .ok_or_else(|| report!("invalid view column type {other}"))?;
                let unit = match unit {
                    "s" => TimeUnit::Second,
                    "ms" => TimeUnit::Millisecond,
                    "us" => TimeUnit::Microsecond,
                    "ns" => TimeUnit::Nanosecond,
                    _ => {
                        return Err(report!("invalid view column type {other}"));
                    }
                };
                let timezone = if timezone.is_empty() {
                    None
                } else {
                    Some(Arc::from(timezone))
                };
                DataType::Timestamp(unit, timezone)
            } else if let Some(rest) = other.strip_prefix("decimal128:") {
                let (precision, scale) = rest
                    .split_once(':')
                    .ok_or_else(|| report!("invalid view column type {other}"))?;
                DataType::Decimal128(
                    precision
                        .parse()
                        .map_err(|_| report!("invalid view column type {other}"))?,
                    scale
                        .parse()
                        .map_err(|_| report!("invalid view column type {other}"))?,
                )
            } else if let Some(rest) = other.strip_prefix("decimal256:") {
                let (precision, scale) = rest
                    .split_once(':')
                    .ok_or_else(|| report!("invalid view column type {other}"))?;
                DataType::Decimal256(
                    precision
                        .parse()
                        .map_err(|_| report!("invalid view column type {other}"))?,
                    scale
                        .parse()
                        .map_err(|_| report!("invalid view column type {other}"))?,
                )
            } else if let Some(rest) = other.strip_prefix("list:") {
                DataType::List(Arc::new(decode_field(rest)?))
            } else if let Some(rest) = other.strip_prefix("large_list:") {
                DataType::LargeList(Arc::new(decode_field(rest)?))
            } else if let Some(rest) = other.strip_prefix("fixed_list:") {
                let (size, field) = rest
                    .split_once(':')
                    .ok_or_else(|| report!("invalid view column type {other}"))?;
                DataType::FixedSizeList(
                    Arc::new(decode_field(field)?),
                    size.parse()
                        .map_err(|_| report!("invalid view column type {other}"))?,
                )
            } else if let Some(rest) = other.strip_prefix("struct:") {
                DataType::Struct(decode_fields(rest)?)
            } else if let Some(rest) = other.strip_prefix("map:") {
                let (sorted, entries) = rest
                    .split_once(':')
                    .ok_or_else(|| report!("invalid view column type {other}"))?;
                DataType::Map(Arc::new(decode_field(entries)?), sorted == "1")
            } else {
                return Err(report!("invalid view column type {other}"));
            }
        }
    })
}

/// A struct's field list, decoded from an array of JSON field descriptors.
fn decode_fields(text: &str) -> Result<Fields> {
    let descriptors: Vec<serde_json::Value> = serde_json::from_str(text)
        .map_err(|error| report!("invalid view struct type {text}: {error}"))?;
    let fields = descriptors
        .iter()
        .map(decode_field_value)
        .collect::<Result<Vec<_>>>()?;
    Ok(Fields::from(fields))
}

/// One JSON field descriptor.
fn decode_field(text: &str) -> Result<Field> {
    let descriptor: serde_json::Value = serde_json::from_str(text)
        .map_err(|error| report!("invalid view field {text}: {error}"))?;
    decode_field_value(&descriptor)
}

fn decode_field_value(descriptor: &serde_json::Value) -> Result<Field> {
    let invalid = || report!("invalid view field {descriptor}");
    let name = descriptor
        .get("name")
        .and_then(|name| name.as_str())
        .ok_or_else(invalid)?;
    let nullable = descriptor
        .get("nullable")
        .and_then(|nullable| nullable.as_bool())
        .ok_or_else(invalid)?;
    let data_type = descriptor
        .get("type")
        .and_then(|data_type| data_type.as_str())
        .ok_or_else(invalid)?;
    Ok(Field::new(name, decode_data_type(data_type)?, nullable))
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

/// The variance a [`VarianceView`] maintains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VarianceKind {
    /// `VAR_POP(value)`.
    VarPop,
    /// `VAR_SAMP(value)` (DataFusion's `var`).
    VarSamp,
    /// `STDDEV_POP(value)`.
    StddevPop,
    /// `STDDEV_SAMP(value)` (DataFusion's `stddev`).
    StddevSamp,
}

impl VarianceKind {
    /// The SQL aggregate name, as DataFusion normalizes it.
    pub fn sql_name(self) -> &'static str {
        match self {
            VarianceKind::VarPop => "var_pop",
            VarianceKind::VarSamp => "var",
            VarianceKind::StddevPop => "stddev_pop",
            VarianceKind::StddevSamp => "stddev",
        }
    }

    /// The materialized view column holding the statistic.
    pub fn column_name(self) -> &'static str {
        match self {
            VarianceKind::VarPop | VarianceKind::VarSamp => IVM_VARIANCE_COLUMN,
            VarianceKind::StddevPop | VarianceKind::StddevSamp => IVM_STDDEV_COLUMN,
        }
    }
}

/// A `MEDIAN(value_column)` view over a source table.
///
/// The median is recomputed from the affected groups' current source rows,
/// which keeps the result identical to the native aggregate.
/// The boolean aggregate a [`BoolAggView`] maintains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoolAggKind {
    /// `BOOL_AND(value)`.
    BoolAnd,
    /// `BOOL_OR(value)`.
    BoolOr,
}

impl BoolAggKind {
    /// The SQL function name.
    pub fn sql_name(self) -> &'static str {
        match self {
            BoolAggKind::BoolAnd => "bool_and",
            BoolAggKind::BoolOr => "bool_or",
        }
    }
}

/// The rendered value of an aggregate whose argument is a plain column or a
/// stored expression.
fn aggregate_value_sql(value_column: Option<&str>, value_expr: Option<&str>) -> String {
    match (value_expr, value_column) {
        (Some(expression), _) => expression.to_string(),
        (None, Some(column)) => quote_ident(column),
        (None, None) => String::new(),
    }
}

/// The value type of an aggregate whose argument is a plain column or a
/// stored expression.
fn aggregate_value_type(
    source_schema: &Schema,
    value_column: Option<&str>,
    value_expr: Option<&str>,
) -> Result<DataType> {
    match (value_expr, value_column) {
        (Some(expression), _) => Ok(expression_type(source_schema, expression)?.0),
        (None, Some(column)) => field_type(source_schema, column),
        (None, None) => Err(report!("the aggregate has no value")),
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

/// A materialized `GROUPING(key)` column: the flat key index and the output
/// column name. The value is `0` for the sets that group by the key and `1`
/// for the sets that aggregate it away.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupingColumn {
    /// The index of the key in the flat key list.
    pub key: usize,
    /// The output column name.
    pub name: String,
}

/// The type and nullability of a stored scalar expression over a schema.
pub(crate) fn expression_type(
    source_schema: &Schema,
    expression: &str,
) -> Result<(DataType, bool)> {
    let context = refresh_session();
    let df_schema = DFSchema::try_from(source_schema.clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    let expression = context
        .state()
        .create_logical_expr(expression, &df_schema)
        .map_err(|error| report!("invalid expression {expression:?}: {error}"))?;
    let data_type = expression
        .get_type(&df_schema)
        .map_err(|error| report!("invalid expression {expression:?}: {error}"))?;
    let nullable = expression
        .nullable(&df_schema)
        .map_err(|error| report!("invalid expression {expression:?}: {error}"))?;
    Ok((data_type, nullable))
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
/// The materialized key fields of a view: plain source columns or planned
/// group expressions.
fn group_key_fields(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
) -> Result<Vec<arrow_schema::FieldRef>> {
    if group_exprs.is_empty() {
        return key_fields(source_schema, group_keys);
    }
    if group_exprs.len() != group_keys.len() {
        return Err(report!("the group needs one expression per key column"));
    }
    group_keys
        .iter()
        .zip(group_exprs)
        .map(|(key, expression)| {
            // A plain column among computed keys is its own expression.  A
            // computed key may deliberately reuse an input column's name
            // (`CASE ... spec_name ... END AS spec_name`); the expression is
            // evaluated against the input and its result type defines the
            // materialized key.
            if let Ok(field) = source_schema.field_with_name(key)
                && expression == key
            {
                return Ok(Arc::new(field.clone()) as arrow_schema::FieldRef);
            }
            let (data_type, nullable) = expression_type(source_schema, expression)?;
            Ok(Arc::new(Field::new(key, data_type, nullable)) as arrow_schema::FieldRef)
        })
        .collect()
}

/// One condition on a range-partition column extracted from a view filter.
#[derive(Debug, Clone)]
enum PartitionCondition {
    In {
        column: String,
        values: std::collections::BTreeSet<String>,
    },
    Cmp {
        column: String,
        op: Operator,
        value: String,
        number: Option<i128>,
    },
}

impl PartitionCondition {
    fn column(&self) -> &str {
        match self {
            Self::In { column, .. } | Self::Cmp { column, .. } => column,
        }
    }

    fn matches(&self, value: &str) -> Option<bool> {
        match self {
            Self::In { values, .. } => Some(values.contains(value)),
            Self::Cmp {
                op,
                value: expected,
                number,
                ..
            } => {
                if let Some(number) = number {
                    let actual = value.parse::<i128>().ok()?;
                    Some(match op {
                        Operator::Gt => actual > *number,
                        Operator::GtEq => actual >= *number,
                        Operator::Lt => actual < *number,
                        Operator::LtEq => actual <= *number,
                        _ => return None,
                    })
                } else {
                    Some(match op {
                        Operator::Gt => value > expected.as_str(),
                        Operator::GtEq => value >= expected.as_str(),
                        Operator::Lt => value < expected.as_str(),
                        Operator::LtEq => value <= expected.as_str(),
                        _ => return None,
                    })
                }
            }
        }
    }
}

/// A conjunction of constant partition conditions from a view filter.
#[derive(Debug, Clone, Default)]
struct PartitionPredicate {
    conditions: Vec<PartitionCondition>,
}

impl PartitionPredicate {
    fn matches(&self, descriptor: &str, columns: &[String]) -> Option<bool> {
        let values = crate::table::parse_partition_values(descriptor, columns)?;
        for condition in &self.conditions {
            let index = columns
                .iter()
                .position(|column| column == condition.column())?;
            let value = values.get(index)?;
            if !condition.matches(value)? {
                return Some(false);
            }
        }
        Some(true)
    }
}

/// The IVM runtime: a metadata client plus the `ivm` schema access layer.
pub struct IvmRuntime {
    client: MetaDataClient,
    metadata: IvmMetadata,
    /// Estimated live row count per view, used by the cost-based refresh
    /// fallback.  Updated on bootstrap, rebuild and incremental refreshes of
    /// unique-key chains; unknown after a process restart until the next
    /// rebuild/bootstrap.
    view_rows: Mutex<HashMap<String, u64>>,
    /// Right-key existence shared by the plain SEMI/ANTI views of one right
    /// window.  The key is `(right table, right join keys)`, and the value is
    /// the set of encoded keys already probed plus the subset that exists.
    /// It is deliberately scoped to one source window: a right change starts
    /// a new window and invalidates every previous answer.
    semi_anti_right_keys: Mutex<SemiAntiRightKeyCache>,
}

/// The shared right-key answers of one source window.
#[derive(Default)]
pub(crate) struct SemiAntiRightKeyCache {
    window: String,
    tables: HashMap<String, SemiAntiRightKeySet>,
}

/// The encoded keys a plain SEMI/ANTI view already asked the right side for.
#[derive(Default)]
pub(crate) struct SemiAntiRightKeySet {
    probed: HashSet<Vec<u8>>,
    present: HashSet<Vec<u8>>,
}

impl SemiAntiRightKeyCache {
    /// Start a new window, dropping answers from the previous one.
    pub(crate) fn begin_window(&mut self, window: &str) {
        if self.window != window {
            self.window = window.to_string();
            self.tables.clear();
        }
    }

    /// The keys known present and the keys that still need a probe.
    pub(crate) fn lookup(
        &mut self,
        table: &str,
        requested: &HashSet<Vec<u8>>,
    ) -> (HashSet<Vec<u8>>, HashSet<Vec<u8>>) {
        let entry = self.tables.entry(table.to_string()).or_default();
        let present = requested
            .intersection(&entry.present)
            .cloned()
            .collect::<HashSet<_>>();
        let unknown = requested
            .difference(&entry.probed)
            .cloned()
            .collect::<HashSet<_>>();
        (present, unknown)
    }

    /// Record one probe of `unknown` that found `present`.
    pub(crate) fn record(
        &mut self,
        table: &str,
        unknown: &HashSet<Vec<u8>>,
        present: &HashSet<Vec<u8>>,
    ) {
        let entry = self.tables.entry(table.to_string()).or_default();
        entry.probed.extend(unknown.iter().cloned());
        entry.present.extend(present.iter().cloned());
    }
}

/// The attachment a refresh failure carries when a source partition was
/// dropped: the incremental window cannot retract the rows it contributed.
/// The executor removes the view rows that belong to the partition when the
/// view's keys carry its columns
/// ([`IvmRuntime::cleanup_dropped_partitions`]), and rebuilds from the current
/// source state otherwise.
#[derive(Debug, Clone)]
pub(crate) struct PartitionDropped {
    /// The table whose partitions were dropped.
    pub(crate) source_table_id: String,
    /// The dropped partitions: `(partition_desc, to_version, to_timestamp)`,
    /// in changelog order. The cursor is advanced to the deletion's version so
    /// the partition is not reported again.
    pub(crate) partitions: Vec<(String, i64, i64)>,
}

/// The attachment a refresh failure carries when the incremental path cannot
/// continue (a source window that needs a snapshot, a cursor window that no
/// longer aligns with the committed history): the caller rebuilds.
#[derive(Debug)]
pub(crate) struct RebuildRequired;

impl std::fmt::Display for RebuildRequired {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a window that requires a rebuild")
    }
}

impl std::fmt::Display for PartitionDropped {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a dropped source partition")
    }
}

/// The changelog window of every partition of one source.
pub(crate) struct SourceWindow {
    added_files: Vec<PartitionFiles>,
    cursors: Vec<Cursor>,
    /// `(source_table_id, partition_desc, from_version, to_version)` of every
    /// consumed partition; this is the window identity the epoch is keyed by.
    identity: Vec<(String, String, i64, i64)>,
    before_timestamp: i64,
    /// `partition_desc -> last consumed version before this window`, for every
    /// partition the window touched (`None` = the partition is first consumed
    /// by this window). Used to read the before state per partition.
    before_versions: HashMap<String, Option<i64>>,
}

/// The full current state of one source, as read by a rebuild.
struct SourceBaseline {
    groups: Vec<PartitionFiles>,
    cursors: Vec<Cursor>,
    to_versions: Vec<SourceVersionRange>,
}

/// The metadata half of [`SourceBaseline`]: current partition files, cursors
/// and versions, without reading the data.  Rebuild paths that read the base
/// bucket by bucket use it to avoid materializing the whole source once more.
struct SourceBaselineMeta {
    groups: Vec<PartitionFiles>,
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
    Apply(Box<EpochRecord>),
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

/// Apply an optional side filter to a join input frame.
fn apply_side_filter(
    context: &SessionContext,
    frame: DataFrame,
    schema: &SchemaRef,
    filter: Option<&str>,
) -> Result<DataFrame> {
    match filter {
        Some(filter) => Ok(frame.filter(parse_filter(context, schema, filter)?)?),
        None => Ok(frame),
    }
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

/// Drop the rows retracted by their change marker (`delete`) when the source
/// has a change column.
fn filter_deletes(frame: DataFrame, change_column: Option<&str>) -> Result<DataFrame> {
    match change_column {
        // A projected frame may not carry the change column; its source was
        // already read in current/before form, so there is no tombstone to
        // filter.
        Some(column) if frame.schema().field_with_name(None, column).is_err() => {
            Ok(frame)
        }
        Some(column) => Ok(frame.filter(column_expr(column).not_eq(lit("delete")))?),
        None => Ok(frame),
    }
}

/// Aggregate sum/count batches into `group_key -> (sum, count)`.
/// A SQL string literal.
fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The `key, ` select prefix of a group key list; empty for a global
/// aggregate.
fn key_select(columns: &[String]) -> String {
    if columns.is_empty() {
        String::new()
    } else {
        format!("{}, ", quoted_list(columns))
    }
}

/// The ` group by ...` suffix of a group key list; empty for a global
/// aggregate.
fn group_by_clause(columns: &[String]) -> String {
    if columns.is_empty() {
        String::new()
    } else {
        format!(" group by {}", quoted_list(columns))
    }
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

/// The result type of `AVG(value_column)`; only numeric inputs are supported.
fn avg_result_type(value_type: &DataType) -> Result<DataType> {
    Ok(match value_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => DataType::Float64,
        other => {
            return Err(report!("AVG is not supported for value type {other}"));
        }
    })
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

/// `alias.column <> 'delete'` when the source has a change column.
fn source_delete_filter(alias: &str, change_column: Option<&str>) -> String {
    match change_column {
        Some(column) => format!("{alias}.{} <> 'delete'", quote_ident(column)),
        None => "true".to_string(),
    }
}

/// The SQL predicate matching rows whose marker retracts them (`delete`).
fn source_retract_condition(alias: &str, change_column: Option<&str>) -> String {
    match change_column {
        Some(column) => format!("{alias}.{} = 'delete'", quote_ident(column)),
        None => "false".to_string(),
    }
}

/// The signed aggregate expressions of an append-only CDC delta: rows with a
/// retraction marker subtract their contribution, the others add it.
fn signed_delta_exprs(
    alias: &str,
    value: Option<&str>,
    count_column: Option<&str>,
    aggregate_filter: Option<&str>,
    change_column: Option<&str>,
) -> (String, String, String) {
    let retract = source_retract_condition(alias, change_column);
    // The row count drives the groups, so it is never filtered.
    let count = format!("sum(case when {retract} then -1 else 1 end)");
    let filter = aggregate_filter
        .map(|filter| format!(" filter (where {filter})"))
        .unwrap_or_default();
    let sum = match value {
        Some(value) => {
            format!("sum(case when {retract} then -({value}) else ({value}) end){filter}")
        }
        None => "sum(0)".to_string(),
    };
    // The non-NULL count belongs to the count column, falling back to the
    // summed value.
    let nonnull = match count_column
        .map(quote_ident)
        .or_else(|| value.map(str::to_string))
    {
        Some(value) => {
            format!(
                "sum(case when {value} is null then 0 when {retract} then -1 else 1 end){filter}"
            )
        }
        None => count.clone(),
    };
    (sum, count, nonnull)
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

/// Add computed group-key columns to a lazy frame.  The frame stays lazy, so
/// a rebuild can register it without materializing the source.
fn project_group_keys_frame(
    context: &SessionContext,
    mut frame: DataFrame,
    schema: &SchemaRef,
    group_keys: &[String],
    group_exprs: &[String],
) -> Result<DataFrame> {
    if group_exprs.is_empty() {
        return Ok(frame);
    }
    let df_schema = DFSchema::try_from(schema.as_ref().clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    for (key, expression) in group_keys.iter().zip(group_exprs) {
        let expression = context
            .state()
            .create_logical_expr(expression, &df_schema)
            .map_err(|error| {
                report!("invalid group expression {expression:?}: {error}")
            })?;
        frame = frame.with_column(key.as_str(), expression)?;
    }
    Ok(frame)
}

/// Cast the given columns of a frame to the types the materialized view
/// stores.  A source read may expose an optimized string view type
/// (`Utf8View`) while the state/MV schema stores `Utf8`; hash joins on the
/// group keys require both sides to use the same physical type.
pub(crate) fn cast_frame_columns(
    frame: DataFrame,
    target_schema: &arrow_schema::Schema,
    columns: &[String],
) -> Result<DataFrame> {
    let exprs = frame
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let expr = Expr::Column(Column::from_name(field.name()));
            if columns.iter().any(|column| column == field.name())
                && let Ok(target) = target_schema.field_with_name(field.name())
                && target.data_type() != field.data_type()
            {
                return datafusion::logical_expr::Expr::Cast(
                    datafusion::logical_expr::expr::Cast::new(
                        Box::new(expr),
                        target.data_type().clone(),
                    ),
                );
            }
            expr
        })
        .collect::<Vec<_>>();
    frame.select(exprs).map_err(Into::into)
}

/// The session used by full and incremental refreshes.  Keep the partition
/// count identical on both paths so a bootstrap and its first incremental
/// windows run with the same parallelism.
pub(crate) fn refresh_session() -> SessionContext {
    let target_partitions = std::env::var("LAKESOUL_IVM_TARGET_PARTITIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4);
    let batch_size = std::env::var("LAKESOUL_IVM_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8192)
        .max(1);
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(batch_size);
    let context = SessionContext::new_with_config_rt(config, refresh_runtime());
    tracing::debug!(
        target_partitions,
        batch_size,
        "IVM refresh session configured"
    );
    context
}

/// The single SQL execution pool shared by every refresh session.
///
/// A refresh creates a session per view and a few more for helper plans.  A
/// pool per session made memory scale with the number of live plans; sharing
/// one pool keeps the configured budget process-wide and lets DataFusion spill
/// instead of multiplying it.  This pool is intentionally the largest one:
/// joins, windows and sorts benefit from memory much more than streaming IO
/// readers do.
fn refresh_runtime() -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
    static RUNTIME: std::sync::OnceLock<
        Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    > = std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            let limit_mb = std::env::var("LAKESOUL_IVM_MEMORY_LIMIT_MB")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(8192);
            let spill_dir = std::env::var("LAKESOUL_IVM_SPILL_DIR")
                .unwrap_or_else(|_| "/tmp/lakesoul/ivm-spill".to_string());
            let _ = std::fs::create_dir_all(&spill_dir);
            let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                .with_memory_limit(limit_mb * 1024 * 1024, 0.8)
                .with_temp_file_path(&spill_dir)
                .with_max_temp_directory_size(100 * 1024 * 1024 * 1024)
                .build()
                .expect("ivm refresh runtime env");
            tracing::debug!(
                limit_mb,
                %spill_dir,
                "IVM refresh runtime configured"
            );
            Arc::new(runtime)
        })
        .clone()
}

/// Encodes the group-key columns of a batch into Arrow's canonical row
/// bytes.  Equal group keys produce identical bytes (nulls included), so a
/// single Binary column can be used as a hash-join key or as the primary key
/// of an internal state table; the pk locator can map those bytes to row
/// numbers directly.
pub(crate) struct GroupKeyRowEncoder {
    converter: RowConverter,
}

impl GroupKeyRowEncoder {
    pub(crate) fn new(schema: &Schema, group_keys: &[String]) -> Result<Self> {
        let mut sort_fields = Vec::with_capacity(group_keys.len());
        for key in group_keys {
            let field = schema.field_with_name(key)?;
            sort_fields.push(SortField::new(field.data_type().clone()));
        }
        Ok(Self {
            converter: RowConverter::new(sort_fields)?,
        })
    }

    /// Encode the group-key columns directly, for a DataFusion scalar UDF.
    pub(crate) fn encode_columns(&self, columns: &[ArrayRef]) -> Result<BinaryArray> {
        let rows = self.converter.convert_columns(columns)?;
        let mut builder = BinaryBuilder::new();
        for row in 0..rows.num_rows() {
            builder.append_value(rows.row(row).as_ref());
        }
        Ok(builder.finish())
    }
}

/// The groups a refresh window can touch: the groups in the delta plus, for a
/// keyed source, the previous groups of the rows that changed.
fn affected_groups_sql(
    source: &IvmTable,
    group_keys: &[String],
    keyed: bool,
    filter: Option<&str>,
    changelog: bool,
) -> String {
    let keys = quoted_list(group_keys);
    let plain_where = filter_where(filter);
    if !keyed || changelog {
        return format!("select distinct {keys} from delta{plain_where}");
    }
    let pks = quoted_list(&source.primary_keys);
    let old_filter = format!(
        "{}{}",
        source_delete_filter("o", change_column(source)),
        filter_clause(filter),
    );
    let pk_match = key_join_condition("o", "p", &source.primary_keys);
    format!(
        "with delta_groups as (select distinct {keys} from delta{plain_where}), \
         delta_pks as (select distinct {pks} from delta), \
         old_groups as (select distinct {keys} from old o \
                        where {old_filter} \
                          and exists (select 1 from delta_pks p where {pk_match})) \
         select * from delta_groups union select * from old_groups"
    )
}

/// The MV range partitions an affected source row can live in.
///
/// Sound when the MV's partition columns are source partition columns, the MV
/// keys carry them and the source primary keys carry the source's partition
/// columns (a key cannot move between partitions).  Callers must have verified
/// that the MV materializes the partition column unchanged: the window/top-k
/// and row views carry the source primary keys, so a partition column drawn
/// from them is copied through as-is.
fn mv_partitions_for_source(
    mv: &IvmTable,
    source: &IvmTable,
    window: &SourceWindow,
) -> Option<Vec<(String, Vec<String>)>> {
    let mapping = mv
        .range_partition_columns
        .iter()
        .map(|column| (column.clone(), column.clone()))
        .collect::<Vec<_>>();
    mv_partitions_for_source_mapped(mv, source, window, &mapping)
}

/// The MV range partitions an affected source row can live in.
///
/// `mapping` maps a source column to the MV column that carries it (the same
/// name for most views, an alias for join outputs).  Sound when every MV
/// partition column maps to a source partition column, the source primary keys
/// carry its partition columns (a key cannot move between partitions) and the
/// MV keys carry the MV partition columns.  Callers must have verified that
/// the MV materializes the mapped column unchanged.
fn mv_partitions_for_source_mapped(
    mv: &IvmTable,
    source: &IvmTable,
    window: &SourceWindow,
    mapping: &[(String, String)],
) -> Option<Vec<(String, Vec<String>)>> {
    let mv_partitions = &mv.range_partition_columns;
    if mv_partitions.is_empty() {
        return None;
    }
    if !source
        .range_partition_columns
        .iter()
        .all(|column| source.primary_keys.contains(column))
    {
        return None;
    }
    let mut restriction = Vec::with_capacity(mv_partitions.len());
    for partition in mv_partitions {
        let (source_column, _) = mapping
            .iter()
            .find(|(_, mv_column)| mv_column == partition)?;
        if !source.range_partition_columns.contains(source_column)
            || !mv.primary_keys.contains(partition)
        {
            return None;
        }
        restriction.push((partition.clone(), Vec::new()));
    }
    for files in &window.added_files {
        let values = parse_partition_values(
            &files.partition_desc,
            &source.range_partition_columns,
        )?;
        for (mv_column, allowed) in restriction.iter_mut() {
            let source_column = mapping
                .iter()
                .find(|(_, mapped)| mapped == mv_column)
                .map(|(source, _)| source)?;
            let index = source
                .range_partition_columns
                .iter()
                .position(|candidate| candidate == source_column)?;
            let value = values.get(index)?.clone();
            if !allowed.contains(&value) {
                allowed.push(value);
            }
        }
    }
    restriction
        .iter()
        .all(|(_, allowed)| !allowed.is_empty())
        .then_some(restriction)
}

/// The result of streaming a refresh output into a table.
pub(crate) struct AppendOutcome {
    pub(crate) commit_ids: Vec<String>,
    /// Rows appended in total.
    pub(crate) rows: u64,
    /// Insert-kind rows.
    pub(crate) inserts: u64,
    /// Delete-kind rows.
    pub(crate) deletes: u64,
}

impl IvmRuntime {
    /// The partition versions of every auxiliary state table of a view.
    ///
    /// The MV is represented separately by `mv_versions`; state roles are
    /// sorted so the persisted snapshot is deterministic across retries.
    pub(crate) async fn state_versions(
        &self,
        view_id: &str,
    ) -> Result<Vec<StateVersion>> {
        let mut versions = Vec::new();
        for state in self.metadata.list_states(view_id).await? {
            if state.role == StateRole::Mv {
                continue;
            }
            let table = self.open_table_by_id(&state.table_id).await?;
            versions.push(StateVersion {
                role: state.role,
                table_id: state.table_id,
                versions: output_partition_versions(&self.client, &table).await?,
            });
        }
        versions.sort_by(|left, right| {
            (left.role.role_name(), &left.table_id)
                .cmp(&(right.role.role_name(), &right.table_id))
        });
        Ok(versions)
    }

    /// Rebuild mirror-style auxiliary state from the current MV.
    ///
    /// Window/top-k partition indexes, join pair indexes and the optional
    /// join output mirror all have the MV schema.  If a pending epoch committed
    /// the MV but missed one of these writes, they can be repaired in place
    /// without rebuilding the whole view.  Source-derived state (value-count,
    /// join side state, rank state) is intentionally left to the rebuild path.
    pub(crate) async fn repair_mirror_states(
        &self,
        view_id: &str,
        output: &IvmTable,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut commit_ids = Vec::new();
        let mut repaired_tables = Vec::new();
        for state in self.metadata.list_states(view_id).await? {
            if state.table_id == output.table_id {
                continue;
            }
            let table = self.open_table_by_id(&state.table_id).await?;
            let mirror = state.role == StateRole::JoinPairIndex
                || (state.role == StateRole::State
                    && table.schema.as_ref() == output.schema.as_ref());
            if !mirror {
                continue;
            }
            let context = refresh_session();
            let frame = self.current_table_frame(&context, output)?;
            table.truncate(&self.client).await?;
            commit_ids.extend(
                append_dataframe(&table, &self.client, frame)
                    .await?
                    .commit_ids,
            );
            repaired_tables.push(state.table_id);
        }
        Ok((commit_ids, repaired_tables))
    }

    /// Record the current auxiliary state versions on a pending epoch.
    ///
    /// This is called immediately after the state portion of a refresh so a
    /// crash after that write but before the final commit remains detectable.
    pub(crate) async fn checkpoint_epoch_states(
        &self,
        record: &EpochRecord,
    ) -> Result<()> {
        let state_versions = self.state_versions(&record.view_id).await?;
        self.metadata
            .checkpoint_epoch_state_versions(record, &state_versions)
            .await
    }

    /// Mark an epoch committed together with the current auxiliary state
    /// versions.  All refresh paths go through this runtime method so a
    /// pending epoch can later verify that every state write completed.
    pub(crate) async fn mark_epoch_committed(
        &self,
        record: &EpochRecord,
        mv_versions: &[PartitionVersion],
        commit_ids: &[String],
    ) -> Result<()> {
        let state_versions = self.state_versions(&record.view_id).await?;
        self.metadata
            .mark_epoch_committed_with_states(
                record,
                mv_versions,
                commit_ids,
                &state_versions,
            )
            .await
    }

    /// The estimated live row count of a view, when known.
    pub(crate) fn known_view_rows(&self, view_id: &str) -> Option<u64> {
        self.view_rows
            .lock()
            .ok()
            .and_then(|rows| rows.get(view_id).copied())
    }

    /// Replace the estimated live row count of a view.
    pub(crate) fn set_view_rows(&self, view_id: &str, rows: u64) {
        if let Ok(mut counts) = self.view_rows.lock() {
            counts.insert(view_id.to_string(), rows);
        }
    }

    /// Apply an incremental round to the estimated live row count.
    pub(crate) fn adjust_view_rows(&self, view_id: &str, inserts: u64, deletes: u64) {
        if let Ok(mut counts) = self.view_rows.lock()
            && let Some(rows) = counts.get_mut(view_id)
        {
            *rows = rows.saturating_sub(deletes).saturating_add(inserts);
        }
    }
}

/// The files a refresh output produced, before publication.
pub(crate) struct WrittenFiles {
    pub(crate) files: Vec<DataFileInfo>,
    /// Insert-kind plus delete-kind rows.
    pub(crate) rows: u64,
    /// Insert-kind rows.
    pub(crate) inserts: u64,
    /// Delete-kind rows.
    pub(crate) deletes: u64,
}

/// Execute a refresh data frame and write its batches as they arrive.
///
/// The driver never holds the whole output: DataFusion streams batches into
/// one rolling writer, which flushes a bounded sort/write window at a time.
/// Publication is left to the caller, so a distributed refresh can have
/// workers write and the driver commit every file once.
pub(crate) async fn write_dataframe(
    table: &IvmTable,
    frame: DataFrame,
) -> Result<WrittenFiles> {
    use futures::StreamExt;
    let mut stream = frame.execute_stream().await?;
    let mut writer = table.writer().await?;
    let mut written_rows = 0u64;
    let mut inserts = 0u64;
    let mut deletes = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        written_rows += batch.num_rows() as u64;
        if let Ok(index) = batch.schema().index_of(IVM_ROW_KINDS_COLUMN)
            && let Some(kinds) = batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
        {
            for row in 0..kinds.len() {
                match kinds.value(row) {
                    "insert" => inserts += 1,
                    "delete" => deletes += 1,
                    _ => {}
                }
            }
        }
        // DataFusion plans string expressions as `Utf8View`; the physical
        // table stores `Utf8`.  Every write path funnels through here, so cast
        // once instead of relying on each view kind.
        let batch = table.cast_batch(batch)?;
        let started = std::time::Instant::now();
        writer.write_record_batch(batch).await?;
        crate::observability::add_write_time(started.elapsed());
    }
    let started = std::time::Instant::now();
    let outputs = writer.flush_and_close().await?;
    crate::observability::add_write_time(started.elapsed());
    let files = table.output_files(outputs, written_rows);
    Ok(WrittenFiles {
        files,
        rows: inserts + deletes,
        inserts,
        deletes,
    })
}

/// Write a refresh data frame and publish its files in one commit.
pub(crate) async fn append_dataframe(
    table: &IvmTable,
    client: &MetaDataClient,
    frame: DataFrame,
) -> Result<AppendOutcome> {
    let written = write_dataframe(table, frame).await?;
    let commit_ids = table.commit_files(client, written.files).await?;
    Ok(AppendOutcome {
        commit_ids,
        rows: written.rows,
        inserts: written.inserts,
        deletes: written.deletes,
    })
}

fn apply_filters(mut frame: DataFrame, filters: Vec<Expr>) -> Result<DataFrame> {
    for filter in filters {
        frame = frame.filter(filter)?;
    }
    Ok(frame)
}

/// Project a lazy frame to `columns`, in order, by name.
fn project_frame(frame: DataFrame, columns: &[String]) -> Result<DataFrame> {
    if columns.is_empty() {
        return Ok(frame);
    }
    // `col()` lower-cases, but internal columns such as `rowKinds` are
    // case-sensitive.
    let exprs: Vec<Expr> = columns
        .iter()
        .map(|column| {
            Expr::Column(datafusion::common::Column::from_name(column.as_str()))
        })
        .collect();
    frame.select(exprs).map_err(Into::into)
}

/// Count a lazy frame's rows by draining its stream; nothing is collected.
/// Log the process RSS at a debug checkpoint when `IVM_DEBUG_RSS` is set.
pub(crate) fn trace_rss(label: &str) {
    if std::env::var("IVM_DEBUG_RSS").is_err() {
        return;
    }
    let rss_mb = std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| {
            value
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
        / 1024;
    tracing::debug!(rss_mb, label, "rss checkpoint");
}

pub(crate) async fn count_frame_rows(frame: DataFrame) -> Result<u64> {
    use futures::StreamExt;
    let mut stream = frame.execute_stream().await?;
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows() as u64;
    }
    Ok(rows)
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

/// The columns a refresh batch orders its deletes and inserts by: the MV's
/// primary keys, so the delete a refresh writes always precedes the insert
/// that replaces the same row — whatever the group keys look like and
/// whether or not their payloads are NULL.  Falls back to the group keys for
/// an MV declared without a primary key (or one outside the batch's columns);
/// callers append `"rowKinds"` after these columns.
fn refresh_order_columns(mv: &IvmTable, keys: &[String]) -> Vec<String> {
    let order = mv
        .primary_keys
        .iter()
        .filter(|key| keys.contains(key))
        .cloned()
        .collect::<Vec<_>>();
    if order.is_empty() {
        keys.to_vec()
    } else {
        order
    }
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

/// The plain columns a rendered filter references.
pub(crate) fn filter_columns(
    context: &SessionContext,
    schema: &SchemaRef,
    filter: &str,
) -> Result<Vec<String>> {
    let expression = parse_filter(context, schema, filter)?;
    Ok(expression
        .column_refs()
        .into_iter()
        .map(|column| column.name.clone())
        .collect())
}

/// Parse a persisted filter predicate into a logical expression.
pub(crate) fn parse_filter(
    context: &SessionContext,
    schema: &SchemaRef,
    filter: &str,
) -> Result<Expr> {
    let df_schema = DFSchema::try_from(schema.as_ref().clone())
        .map_err(|error| report!("invalid filter schema: {error}"))?;
    context
        .state()
        .create_logical_expr(filter, &df_schema)
        .map_err(|error| report!("invalid filter {filter:?}: {error}"))
}

/// Validate a `HAVING` predicate against the MV columns it references.
fn validate_having(
    view_id: &str,
    mv_schema: &SchemaRef,
    having: Option<&str>,
) -> Result<()> {
    let Some(having) = having else {
        return Ok(());
    };
    let context = refresh_session();
    parse_filter(&context, mv_schema, having)
        .map_err(|error| report!("view {view_id}: invalid HAVING: {error}"))?;
    Ok(())
}

/// The temporary registration the scalar filter is planned against.
const ROW_FILTER_SOURCE: &str = "__ivm_row_filter_source";

/// The conjunction of a row view's filter, when it has one.
async fn row_filter_predicate(
    context: &SessionContext,
    view: &RowView,
) -> Result<Option<Expr>> {
    let Some(filter) = view.filter.as_deref() else {
        return Ok(None);
    };
    if view.scalar.is_none() {
        return Ok(Some(parse_filter(context, &view.source.schema, filter)?));
    }
    parse_scalar_filter(context, view, filter).await.map(Some)
}

/// Plan a row filter that contains scalar subqueries.
///
/// The source is registered under a temporary name so the subquery tables
/// resolve through the session; the predicate's relation qualifiers are then
/// stripped, because the frames it is applied to are unnamed.
async fn parse_scalar_filter(
    context: &SessionContext,
    view: &RowView,
    filter: &str,
) -> Result<Expr> {
    let provider: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(datafusion::datasource::memory::MemTable::try_new(
            view.source.schema.clone(),
            vec![vec![]],
        )?);
    context
        .register_table(ROW_FILTER_SOURCE, provider)
        .map_err(|error| report!("invalid filter {filter:?}: {error}"))?;
    let sql = format!("select * from {ROW_FILTER_SOURCE} where ({filter})");
    let plan = context
        .state()
        .create_logical_plan(&sql)
        .await
        .map_err(|error| report!("invalid filter {filter:?}: {error}"))?;
    let mut predicate = None;
    plan.apply(|node| {
        if let LogicalPlan::Filter(filter) = node
            && predicate.is_none()
        {
            predicate = Some(filter.predicate.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map_err(|error| report!("invalid filter {filter:?}: {error}"))?;
    let predicate =
        predicate.ok_or_else(|| report!("invalid filter {filter:?}: no predicate"))?;
    predicate
        .transform_down(|node| match node {
            Expr::Column(column) if column.relation.is_some() => Ok(Transformed::yes(
                Expr::Column(Column::from_name(column.name.clone())),
            )),
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
        .map_err(|error| report!("invalid filter {filter:?}: {error}"))
}

/// ` and (<filter>)`, to append to an existing WHERE clause.
fn filter_clause(filter: Option<&str>) -> String {
    filter
        .map(|filter| format!(" and ({filter})"))
        .unwrap_or_default()
}

/// ` where (<filter>)`, for a query that has no other predicate.
fn filter_where(filter: Option<&str>) -> String {
    filter
        .map(|filter| format!(" where ({filter})"))
        .unwrap_or_default()
}

impl IvmRuntime {
    /// Build the runtime from the `LAKESOUL_PG_*` configuration.
    pub async fn from_env() -> Result<Self> {
        Ok(Self {
            client: MetaDataClient::from_env().await?,
            metadata: IvmMetadata::from_env().await?,
            view_rows: Mutex::new(HashMap::new()),
            semi_anti_right_keys: Mutex::new(SemiAntiRightKeyCache::default()),
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

    /// The shared right-key cache used by plain SEMI/ANTI views.
    pub(crate) fn semi_anti_right_key_cache(&self) -> &Mutex<SemiAntiRightKeyCache> {
        &self.semi_anti_right_keys
    }

    /// Create the `ivm` schema and tables if they do not exist.
    pub async fn init_schema(&self) -> Result<()> {
        self.metadata.init_schema().await
    }

    /// Create an internal LakeSoul table managed by the runtime.
    pub async fn create_table(&self, options: IvmTableOptions) -> Result<IvmTable> {
        create_ivm_table(&self.client, options).await
    }

    /// Compact a table through the streaming provider instead of collecting
    /// its entire current state in the driver.
    ///
    /// This is used by the pilot source loader: loading appends bounded
    /// batches, so source tables accumulate many small files even though the
    /// loaded snapshot is logically compactable.  The reader and writer share
    /// the process-wide LakeSoulIO runtime and the writer still enforces its
    /// normal memory/file bounds.
    pub async fn compact_table_streaming(&self, table: &IvmTable) -> Result<Vec<String>> {
        // `IvmTable::compact` reads the current state as partitioned streams and
        // writes through the rolling writer; keep this name for the pilot/tool
        // entry point.
        table.compact(&self.client).await
    }

    /// Open an existing table by name.
    pub async fn open_table(
        &self,
        table_name: &str,
        namespace: &str,
    ) -> Result<IvmTable> {
        let info = self
            .client
            .get_table_info_by_table_name(table_name, namespace)
            .await?
            .ok_or_else(|| {
                rootcause::report!("table {namespace}.{table_name} not found")
            })?;
        IvmTable::from_table_info(&info)
    }

    /// Open an existing table by id.
    pub async fn open_table_by_id(&self, table_id: &str) -> Result<IvmTable> {
        let info = self
            .client
            .get_table_info_by_table_id(table_id)
            .await?
            .ok_or_else(|| rootcause::report!("table {table_id} not found"))?;
        IvmTable::from_table_info(&info)
    }

    /// Open the tables of a spec and build the typed view it describes.
    async fn spec_view(&self, spec: &ViewSpec) -> Result<SpecView> {
        let refresh_interval_ms = self
            .metadata
            .view_refresh_interval_ms(spec.view_id())
            .await?
            .unwrap_or(0);
        Ok(match spec {
            ViewSpec::SumCount {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                count_column,
                aggregate_filter,
                filter,
                having,
                average,
            } => SpecView::SumCount(SumCountView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                count_column: count_column.clone(),
                aggregate_filter: aggregate_filter.clone(),
                filter: filter.clone(),
                having: having.clone(),
                average: *average,
                refresh_interval_ms,
            }),
            ViewSpec::GroupingSets {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                groupings,
                value_column,
                value_expr,
                count_column,
                aggregate_filter,
                average,
                grouping_columns,
                aggregates,
                filter,
                having,
            } => SpecView::GroupingSets(GroupingSetsView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                groupings: groupings.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                count_column: count_column.clone(),
                aggregate_filter: aggregate_filter.clone(),
                average: *average,
                grouping_columns: grouping_columns.clone(),
                aggregates: aggregates
                    .iter()
                    .map(|aggregate| {
                        Ok((
                            aggregate.call.clone(),
                            aggregate.column.clone(),
                            decode_data_type(&aggregate.result)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::MultiAgg {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                aggregates,
                filter,
                having,
                having_lookup,
                state_table_id,
            } => SpecView::MultiAgg(MultiAggView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                aggregates: aggregates
                    .iter()
                    .map(|aggregate| {
                        Ok((
                            aggregate.call.clone(),
                            aggregate.column.clone(),
                            decode_data_type(&aggregate.result)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
                filter: filter.clone(),
                having: having.clone(),
                having_lookup: match having_lookup {
                    Some(lookup) => Some(HavingLookup {
                        table: self.open_table_by_id(&lookup.table_id).await?,
                        aggregate: lookup.aggregate.clone(),
                        join_keys: lookup.join_keys.clone(),
                        right_keys: lookup.right_keys.clone(),
                        column: lookup.column.clone(),
                        filter: lookup.filter.clone(),
                    }),
                    None => None,
                },
                state: match state_table_id {
                    Some(id) => Some(self.open_table_by_id(id).await?),
                    None => None,
                },
                refresh_interval_ms,
            }),
            ViewSpec::MultiJoin {
                view_id,
                mv_table_id,
                sources,
                keys,
                columns,
                conditions,
                null_safe,
            } => {
                let mut tables = Vec::with_capacity(sources.len());
                for source in sources {
                    tables.push(self.open_table_by_id(&source.table_id).await?);
                }
                SpecView::MultiJoin(MultiJoinView {
                    view_id: view_id.clone(),
                    sources: tables,
                    filters: sources.iter().map(|source| source.filter.clone()).collect(),
                    mv: self.open_table_by_id(mv_table_id).await?,
                    keys: keys.clone(),
                    columns: columns.clone(),
                    conditions: conditions.clone(),
                    null_safe: *null_safe,
                    refresh_interval_ms,
                })
            }
            ViewSpec::Join {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                join_keys,
                right_keys,
                key_exprs,
                left_value,
                right_value,
                output_columns,
                payload_names: _,
                left_filter,
                right_filter,
                pair_filter,
                state_table_id,
                null_safe,
            } => SpecView::Join(JoinView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                key_exprs: key_exprs.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                pair_filter: pair_filter.clone(),
                state: match state_table_id {
                    Some(id) => Some(self.open_table_by_id(id).await?),
                    None => None,
                },
                null_safe: *null_safe,
                refresh_interval_ms,
            }),
            ViewSpec::MultiLookupJoin {
                view_id,
                left_table_id,
                mv_table_id,
                join_keys,
                rights,
                output_columns,
                refresh_interval_ms,
            } => {
                let mut opened = Vec::with_capacity(rights.len());
                for right in rights {
                    opened.push((
                        self.open_table_by_id(&right.table_id).await?,
                        right.filter.clone(),
                    ));
                }
                SpecView::MultiLookupJoin(MultiLookupJoinView {
                    view_id: view_id.clone(),
                    left: self.open_table_by_id(left_table_id).await?,
                    rights: opened,
                    mv: self.open_table_by_id(mv_table_id).await?,
                    join_keys: join_keys.clone(),
                    output_columns: output_columns.clone(),
                    refresh_interval_ms: *refresh_interval_ms,
                })
            }
            ViewSpec::LeftJoin {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                join_keys,
                right_keys,
                key_exprs,
                left_value,
                right_value,
                output_columns,
                payload_names: _,
                left_filter,
                right_filter,
                null_safe,
                identity_column,
            } => SpecView::LeftJoin(LeftJoinView {
                identity_column: *identity_column,
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                key_exprs: key_exprs.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                null_safe: *null_safe,
                refresh_interval_ms,
            }),
            ViewSpec::FullJoin {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                join_keys,
                right_keys,
                key_exprs,
                identity_column,
                left_value,
                right_value,
                output_columns,
                payload_names: _,
                left_filter,
                right_filter,
                null_safe,
            } => SpecView::FullJoin(FullJoinView {
                identity_column: *identity_column,
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                key_exprs: key_exprs.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                null_safe: *null_safe,
                refresh_interval_ms,
            }),
            ViewSpec::LookupJoin {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                join_keys,
                right_keys,
                key_exprs,
                left_filter,
                right_filter,
                left_value,
                right_value,
                output_columns,
                payload_names: _,
                state_table_id,
            } => SpecView::LookupJoin(LookupJoinView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                key_exprs: key_exprs.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                state: match state_table_id {
                    Some(id) => Some(self.open_table_by_id(id).await?),
                    None => None,
                },
                refresh_interval_ms,
            }),
            ViewSpec::CrossJoin {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                left_value,
                right_value,
                output_columns,
                payload_names: _,
                left_filter,
                right_filter,
                pair_filter,
            } => SpecView::CrossJoin(CrossJoinView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                pair_filter: pair_filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::MinMax {
                view_id,
                source_table_id,
                mv_table_id,
                state_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                min_max,
                filter,
                having,
            } => SpecView::MinMax(MinMaxView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                state: self.open_table_by_id(state_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                min_max: *min_max,
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::DistinctAgg {
                view_id,
                source_table_id,
                mv_table_id,
                state_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_columns,
                agg,
                filter,
                having,
            } => SpecView::DistinctAgg(DistinctAggView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                state: self.open_table_by_id(state_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_columns: value_columns.clone(),
                agg: *agg,
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::Variance {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                statistic,
                filter,
                having,
            } => SpecView::Variance(VarianceView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                statistic: *statistic,
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::Median {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                filter,
                having,
            } => SpecView::Median(MedianView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::BoolAgg {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                bool_agg,
                filter,
                having,
            } => SpecView::BoolAgg(BoolAggView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                kind: *bool_agg,
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::ApproxDistinct {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                filter,
                having,
            } => SpecView::ApproxDistinct(ApproxDistinctView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::ApproxPercentile {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                percentile,
                filter,
                having,
            } => SpecView::ApproxPercentile(ApproxPercentileView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                percentile: percentile.clone(),
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::ComputedAgg {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                function,
                arguments,
                percentile,
                column,
                result,
                filter,
                having,
            } => SpecView::ComputedAgg(ComputedAggView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                function: function.clone(),
                arguments: arguments.clone(),
                percentile: percentile.clone(),
                column: column.clone(),
                result: *result,
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::StringAgg {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                delimiter,
                order_by,
                filter,
                having,
            } => SpecView::StringAgg(StringAggView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                delimiter: delimiter.clone(),
                order_by: order_by.clone(),
                filter: filter.clone(),
                having: having.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::ArrayAgg {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                group_exprs,
                value_column,
                value_expr,
                order_by,
                filter,
            } => SpecView::ArrayAgg(ArrayAggView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                group_exprs: group_exprs.clone(),
                value_column: value_column.clone(),
                value_expr: value_expr.clone(),
                order_by: order_by.clone(),
                filter: filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::Window {
                view_id,
                source_table_id,
                mv_table_id,
                partition_keys,
                order_keys,
                order_by,
                columns,
                payload_columns,
                filter,
            } => SpecView::Window(WindowView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                partition_keys: partition_keys.clone(),
                order_keys: order_keys.clone(),
                order_by: order_by.clone(),
                columns: columns.clone(),
                payload_columns: payload_columns.clone(),
                filter: filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::SemiAnti {
                view_id,
                left_table_id,
                right_table_id,
                mv_table_id,
                join_keys,
                conditions,
                output_columns,
                anti,
                left_filter,
                right_filter,
                null_safe,
                count_mode,
                right_aggregate,
                right_keys,
                match_predicate,
            } => SpecView::SemiAnti(SemiAntiView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                join_keys: join_keys.clone(),
                conditions: conditions.clone(),
                output_columns: output_columns.clone(),
                anti: *anti,
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                null_safe: *null_safe,
                count_mode: *count_mode,
                right_aggregate: right_aggregate.clone(),
                right_keys: right_keys.clone(),
                match_predicate: match_predicate.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::LookupChain {
                view_id,
                sources,
                steps,
                output_columns,
                mv_table_id,
                null_safe,
            } => {
                let mut opened = Vec::with_capacity(sources.len());
                for source in sources {
                    opened.push((
                        self.open_table_by_id(&source.table_id).await?,
                        source.filter.clone(),
                    ));
                }
                SpecView::LookupChain(LookupChainView {
                    view_id: view_id.clone(),
                    sources: opened,
                    steps: steps.clone(),
                    output_columns: output_columns.clone(),
                    mv: self.open_table_by_id(mv_table_id).await?,
                    null_safe: *null_safe,
                    refresh_interval_ms,
                })
            }
            ViewSpec::LeftAggregate {
                view_id,
                left_table_id,
                right_table_id,
                mv_table_id,
                join_keys,
                right_keys,
                right_aggregate,
                aggregate_column,
                count_zero,
                output_columns,
                left_filter,
                right_filter,
                output_exprs,
            } => SpecView::LeftAggregate(LeftAggregateView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                right_aggregate: right_aggregate.clone(),
                aggregate_column: aggregate_column.clone(),
                count_zero: *count_zero,
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                output_exprs: output_exprs
                    .iter()
                    .map(|output| {
                        Ok((
                            output.call.clone(),
                            output.column.clone(),
                            decode_data_type(&output.result)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
                refresh_interval_ms,
            }),
            ViewSpec::Row {
                view_id,
                source_table_id,
                mv_table_id,
                output_columns,
                output_exprs,
                filter,
                scalar,
            } => {
                let scalar = match scalar {
                    Some(spec) => {
                        let mut tables = Vec::with_capacity(spec.tables.len());
                        for table in &spec.tables {
                            tables.push((
                                table.name.clone(),
                                self.open_table_by_id(&table.table_id).await?,
                            ));
                        }
                        Some(RowScalarView { tables })
                    }
                    None => None,
                };
                SpecView::Row(RowView {
                    view_id: view_id.clone(),
                    source: self.open_table_by_id(source_table_id).await?,
                    mv: self.open_table_by_id(mv_table_id).await?,
                    output_columns: output_columns.clone(),
                    output_exprs: output_exprs.clone(),
                    filter: filter.clone(),
                    scalar,
                    refresh_interval_ms,
                })
            }
            ViewSpec::UnionAll {
                view_id,
                sources,
                mv_table_id,
                source_column,
            } => {
                let mut opened = Vec::with_capacity(sources.len());
                for source in sources {
                    opened.push(UnionSource {
                        table: self.open_table_by_id(&source.table_id).await?,
                        filter: source.filter.clone(),
                        columns: source.columns.clone(),
                        exprs: source.exprs.clone(),
                    });
                }
                SpecView::UnionAll(UnionAllView {
                    view_id: view_id.clone(),
                    sources: opened,
                    mv: self.open_table_by_id(mv_table_id).await?,
                    source_column: source_column.clone(),
                    refresh_interval_ms,
                })
            }
            ViewSpec::UnionDistinct {
                view_id,
                sources,
                mv_table_id,
            } => {
                let mut opened = Vec::with_capacity(sources.len());
                for source in sources {
                    opened.push(UnionSource {
                        table: self.open_table_by_id(&source.table_id).await?,
                        filter: source.filter.clone(),
                        columns: source.columns.clone(),
                        exprs: source.exprs.clone(),
                    });
                }
                SpecView::UnionDistinct(UnionDistinctView {
                    view_id: view_id.clone(),
                    sources: opened,
                    mv: self.open_table_by_id(mv_table_id).await?,
                    refresh_interval_ms,
                })
            }
            ViewSpec::MultiWindow {
                view_id,
                source_table_id,
                mv_table_id,
                windows,
                payload_columns,
                filter,
            } => SpecView::MultiWindow(MultiWindowView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                windows: windows.clone(),
                payload_columns: payload_columns.clone(),
                filter: filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::TopK {
                view_id,
                source_table_id,
                mv_table_id,
                group_keys,
                order_keys,
                order_by,
                output_columns,
                output_exprs,
                limit,
                offset,
                filter,
            } => SpecView::TopK(TopKView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                order_keys: order_keys.clone(),
                order_by: order_by.clone(),
                output_columns: output_columns.clone(),
                output_exprs: output_exprs.clone(),
                limit: *limit,
                offset: *offset,
                filter: filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::Fallback {
                view_id,
                statement,
                tables,
                reason,
                partition_scope,
                mv_table_id,
            } => {
                let mut opened = Vec::with_capacity(tables.len());
                for table_id in tables {
                    opened.push(self.open_table_by_id(table_id).await?);
                }
                SpecView::Fallback(FallbackView {
                    view_id: view_id.clone(),
                    statement: statement.clone(),
                    tables: opened,
                    reason: reason.clone(),
                    partition_scope: partition_scope.clone(),
                    mv: self.open_table_by_id(mv_table_id).await?,
                    refresh_interval_ms,
                })
            }
            ViewSpec::Sink {
                view_id,
                source_table_id,
                mv_table_id,
                columns,
                primary_keys,
                range_partition_columns,
                source_definition,
                source_generation,
            } => SpecView::Sink(SinkView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                target: self.open_table_by_id(mv_table_id).await?,
                columns: columns.clone(),
                primary_keys: primary_keys.clone(),
                range_partition_columns: range_partition_columns.clone(),
                source_definition: source_definition.clone(),
                source_generation: *source_generation,
                refresh_interval_ms,
            }),
        })
    }

    /// A Tier-1 view must not read a Tier-2 fallback: the fallback rewrites its
    /// whole MV, so a downstream incremental refresh cannot see the rows its
    /// truncate removed.  A fallback view itself may read other fallbacks.
    async fn ensure_incremental_upstreams(&self, spec: &ViewSpec) -> Result<()> {
        if matches!(spec, ViewSpec::Fallback { .. }) {
            return Ok(());
        }
        for table_id in spec.source_table_ids() {
            let Some(value) = self.metadata.get_view_spec(table_id).await? else {
                continue;
            };
            let upstream: ViewSpec = serde_json::from_value(value)?;
            if matches!(upstream, ViewSpec::Fallback { .. }) {
                return Err(report!(
                    "view {} reads the fallback view {table_id}: a Tier-2 fallback \
                     rewrites its materialized view, so a downstream incremental \
                     view cannot track what it removed",
                    spec.view_id()
                ));
            }
        }
        Ok(())
    }

    /// Refresh a view from its persisted [`ViewSpec`], opening every table by
    pub async fn refresh_spec(&self, spec: &ViewSpec) -> Result<Option<i64>> {
        self.ensure_incremental_upstreams(spec).await?;
        let kind = spec.kind();
        let started = std::time::Instant::now();
        tracing::debug!(
            kind,
            view_id = %spec.view_id(),
            "view refresh started"
        );
        let view = self.spec_view(spec).await;
        let (result, io) = crate::observability::scope_refresh_io(kind, async {
            match view {
                Ok(SpecView::SumCount(view)) => self.refresh_sum_count(&view).await,
                Ok(SpecView::Variance(view)) => self.refresh_variance(&view).await,
                Ok(SpecView::Median(view)) => self.refresh_median(&view).await,
                Ok(SpecView::BoolAgg(view)) => self.refresh_bool_agg(&view).await,
                Ok(SpecView::ApproxDistinct(view)) => {
                    self.refresh_approx_distinct(&view).await
                }
                Ok(SpecView::ApproxPercentile(view)) => {
                    self.refresh_approx_percentile(&view).await
                }
                Ok(SpecView::ComputedAgg(view)) => self.refresh_computed_agg(&view).await,
                Ok(SpecView::StringAgg(view)) => self.refresh_string_agg(&view).await,
                Ok(SpecView::ArrayAgg(view)) => self.refresh_array_agg(&view).await,
                Ok(SpecView::Join(view)) => self.refresh_join(&view).await,
                Ok(SpecView::MultiJoin(view)) => self.refresh_multi_join(&view).await,
                Ok(SpecView::MultiAgg(view)) => self.refresh_multi_agg(&view).await,
                Ok(SpecView::LookupJoin(view)) => self.refresh_lookup_join(&view).await,
                Ok(SpecView::MultiLookupJoin(view)) => {
                    self.refresh_multi_lookup_join(&view).await
                }
                Ok(SpecView::LeftJoin(view)) => self.refresh_left_join(&view).await,
                Ok(SpecView::FullJoin(view)) => self.refresh_full_join(&view).await,
                Ok(SpecView::CrossJoin(view)) => self.refresh_cross_join(&view).await,
                Ok(SpecView::MinMax(view)) => self.refresh_min_max(&view).await,
                Ok(SpecView::DistinctAgg(view)) => self.refresh_distinct_agg(&view).await,
                Ok(SpecView::Window(view)) => self.refresh_window(&view).await,
                Ok(SpecView::SemiAnti(view)) => self.refresh_semi_anti(&view).await,
                Ok(SpecView::LeftAggregate(view)) => {
                    self.refresh_left_aggregate(&view).await
                }
                Ok(SpecView::LookupChain(view)) => self.refresh_lookup_chain(&view).await,
                Ok(SpecView::GroupingSets(view)) => {
                    self.refresh_grouping_sets(&view).await
                }
                Ok(SpecView::Row(view)) => self.refresh_row(&view).await,
                Ok(SpecView::UnionAll(view)) => self.refresh_union_all(&view).await,
                Ok(SpecView::UnionDistinct(view)) => {
                    self.refresh_union_distinct(&view).await
                }
                Ok(SpecView::TopK(view)) => self.refresh_top_k(&view).await,
                Ok(SpecView::MultiWindow(view)) => self.refresh_multi_window(&view).await,
                Ok(SpecView::Fallback(view)) => self.refresh_fallback(&view).await,
                Ok(SpecView::Sink(view)) => self.refresh_sink(&view).await,
                Err(error) => Err(error),
            }
        })
        .await;
        crate::observability::record_refresh(
            kind,
            matches!(result, Ok(Some(_))),
            started.elapsed(),
        );
        tracing::debug!(
            kind,
            view_id = %spec.view_id(),
            applied = matches!(result, Ok(Some(_))),
            elapsed_ms = started.elapsed().as_millis() as u64,
            full_scan = io.full_scans() > 0,
            full_scans = io.full_scans(),
            full_scan_tables = ?io.full_scan_tables(),
            files_read = io.files_read(),
            rows_scanned = io.rows_scanned(),
            rows_written = io.rows_written(),
            lookup_ms = io.lookup_ms(),
            lookup_calls = io.lookup_calls(),
            write_ms = io.write_ms(),
            write_calls = io.write_calls(),
            "view refresh finished"
        );
        result
    }

    /// Rebuild a view from its persisted [`ViewSpec`]: the full source state is
    /// recomputed, the MV is replaced and the cursors are reset to the current
    /// source versions.
    pub async fn rebuild_spec(&self, spec: &ViewSpec) -> Result<i64> {
        self.ensure_incremental_upstreams(spec).await?;
        let kind = spec.kind();
        let started = std::time::Instant::now();
        let view = self.spec_view(spec).await;
        let (result, io) = crate::observability::scope_refresh_io(kind, async {
            match view {
                Ok(SpecView::SumCount(view)) => self.rebuild_sum_count(&view).await,
                Ok(SpecView::Variance(view)) => self.rebuild_variance(&view).await,
                Ok(SpecView::Median(view)) => self.rebuild_median(&view).await,
                Ok(SpecView::BoolAgg(view)) => self.rebuild_bool_agg(&view).await,
                Ok(SpecView::ApproxDistinct(view)) => {
                    self.rebuild_approx_distinct(&view).await
                }
                Ok(SpecView::ApproxPercentile(view)) => {
                    self.rebuild_approx_percentile(&view).await
                }
                Ok(SpecView::ComputedAgg(view)) => self.rebuild_computed_agg(&view).await,
                Ok(SpecView::StringAgg(view)) => self.rebuild_string_agg(&view).await,
                Ok(SpecView::ArrayAgg(view)) => self.rebuild_array_agg(&view).await,
                Ok(SpecView::Join(view)) => self.rebuild_join(&view).await,
                Ok(SpecView::MultiJoin(view)) => self.rebuild_multi_join(&view).await,
                Ok(SpecView::MultiAgg(view)) => self.rebuild_multi_agg(&view).await,
                Ok(SpecView::LookupJoin(view)) => self.rebuild_lookup_join(&view).await,
                Ok(SpecView::MultiLookupJoin(view)) => {
                    self.rebuild_multi_lookup_join(&view).await
                }
                Ok(SpecView::LeftJoin(view)) => self.rebuild_left_join(&view).await,
                Ok(SpecView::FullJoin(view)) => self.rebuild_full_join(&view).await,
                Ok(SpecView::CrossJoin(view)) => self.rebuild_cross_join(&view).await,
                Ok(SpecView::MinMax(view)) => self.rebuild_min_max(&view).await,
                Ok(SpecView::DistinctAgg(view)) => self.rebuild_distinct_agg(&view).await,
                Ok(SpecView::Window(view)) => self.rebuild_window(&view).await,
                Ok(SpecView::SemiAnti(view)) => self.rebuild_semi_anti(&view).await,
                Ok(SpecView::LeftAggregate(view)) => {
                    self.rebuild_left_aggregate(&view).await
                }
                Ok(SpecView::LookupChain(view)) => self.rebuild_lookup_chain(&view).await,
                Ok(SpecView::GroupingSets(view)) => {
                    self.rebuild_grouping_sets(&view).await
                }
                Ok(SpecView::Row(view)) => self.rebuild_row(&view).await,
                Ok(SpecView::UnionAll(view)) => self.rebuild_union_all(&view).await,
                Ok(SpecView::UnionDistinct(view)) => {
                    self.rebuild_union_distinct(&view).await
                }
                Ok(SpecView::TopK(view)) => self.rebuild_top_k(&view).await,
                Ok(SpecView::MultiWindow(view)) => self.rebuild_multi_window(&view).await,
                Ok(SpecView::Fallback(view)) => self.rebuild_fallback(&view).await,
                Ok(SpecView::Sink(view)) => self.rebuild_sink(&view).await,
                Err(error) => Err(error),
            }
        })
        .await;
        tracing::debug!(
            kind,
            view_id = %spec.view_id(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            full_scan = io.full_scans() > 0,
            full_scans = io.full_scans(),
            full_scan_tables = ?io.full_scan_tables(),
            files_read = io.files_read(),
            rows_scanned = io.rows_scanned(),
            rows_written = io.rows_written(),
            lookup_ms = io.lookup_ms(),
            lookup_calls = io.lookup_calls(),
            write_ms = io.write_ms(),
            write_calls = io.write_calls(),
            "view rebuild finished"
        );
        if result.is_ok() {
            crate::observability::record_rebuild(kind, started.elapsed());
        }
        result
    }

    /// The internal tables registered for a view (see `ivm.states`).
    pub async fn list_states(&self, view_id: &str) -> Result<Vec<StateTable>> {
        self.metadata.list_states(view_id).await
    }

    /// A DataFusion provider over the current state of an internal table.
    /// A provider that returns the raw merge-on-read state, keeping the CDC
    /// tombstone rows; the regular [`IvmRuntime::table_provider`] hides them.
    pub fn table_provider_raw(&self, table: &IvmTable) -> IvmTableProvider {
        IvmTableProvider::raw(table.clone(), self.client.clone())
    }

    pub fn table_provider(&self, table: &IvmTable) -> IvmTableProvider {
        IvmTableProvider::current(table.clone(), self.client.clone())
    }

    /// A DataFusion provider over the state of an internal table as of
    /// `as_of_ms`.
    pub fn table_provider_as_of(
        &self,
        table: &IvmTable,
        as_of_ms: i64,
    ) -> IvmTableProvider {
        IvmTableProvider::as_of(table.clone(), self.client.clone(), as_of_ms)
    }

    /// A DataFusion provider pinned to the partition versions of an epoch.
    pub fn table_provider_at_versions(
        &self,
        table: &IvmTable,
        versions: Vec<PartitionVersion>,
    ) -> IvmTableProvider {
        IvmTableProvider::at_versions(table.clone(), self.client.clone(), versions)
    }

    /// A DataFusion provider pinned to a committed epoch.
    pub fn table_provider_at_epoch(
        &self,
        table: &IvmTable,
        record: &EpochRecord,
    ) -> IvmTableProvider {
        self.table_provider_at_versions(table, record.mv_versions.clone())
    }

    /// A provider over file groups the caller already resolved from metadata.
    pub(crate) fn table_provider_groups(
        &self,
        table: &IvmTable,
        groups: Vec<PartitionFiles>,
    ) -> IvmTableProvider {
        IvmTableProvider::groups(table.clone(), self.client.clone(), groups)
    }

    /// A provider over explicit delta groups without merge-on-read.
    pub(crate) fn table_provider_unmerged_groups(
        &self,
        table: &IvmTable,
        groups: Vec<PartitionFiles>,
    ) -> IvmTableProvider {
        IvmTableProvider::unmerged_groups(table.clone(), self.client.clone(), groups)
    }

    /// A lazy frame over the current logical state.  Execution streams the
    /// LakeSoul reader; no batch is materialized until the query runs.
    pub(crate) fn current_table_frame(
        &self,
        context: &SessionContext,
        table: &IvmTable,
    ) -> Result<DataFrame> {
        crate::observability::note_full_scan(&table.table_name);
        Ok(context.read_table(Arc::new(self.table_provider(table)))?)
    }

    /// A lazy frame over explicit file groups (merge-on-read).
    pub(crate) fn groups_table_frame(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        groups: Vec<PartitionFiles>,
    ) -> Result<DataFrame> {
        Ok(context.read_table(Arc::new(self.table_provider_groups(table, groups)))?)
    }

    /// A lazy frame over the current state restricted to range partitions.
    pub(crate) async fn current_table_frame_filtered_partitions(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        filters: Vec<Expr>,
        partitions: &[(String, Vec<String>)],
    ) -> Result<DataFrame> {
        let groups = table
            .current_groups_for_partitions(&self.client, partitions)
            .await?;
        apply_filters(self.groups_table_frame(context, table, groups)?, filters)
    }

    /// The range-partition values touched by a source window.
    ///
    /// A refresh only needs the source/state rows of the partitions whose
    /// changelog it is consuming.  The descriptors come from the source, so a
    /// state table whose range columns have the same names and order can reuse
    /// them directly.  Any descriptor that does not parse for `columns`
    /// disables pruning: reading an extra partition is safe, dropping one is
    /// not.
    fn changed_partition_restriction(
        &self,
        window: &SourceWindow,
        columns: &[String],
    ) -> Option<Vec<(String, Vec<String>)>> {
        if columns.is_empty() {
            return None;
        }
        let mut values: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut saw_partition = false;
        for descriptor in window
            .added_files
            .iter()
            .map(|group| group.partition_desc.as_str())
            .chain(window.before_versions.keys().map(String::as_str))
        {
            let parsed = parse_partition_values(descriptor, columns)?;
            saw_partition = true;
            for (column, value) in columns.iter().zip(parsed) {
                values.entry(column.clone()).or_default().insert(value);
            }
        }
        if !saw_partition {
            return None;
        }
        Some(
            columns
                .iter()
                .filter_map(|column| {
                    values
                        .get(column)
                        .map(|values| (column.clone(), values.iter().cloned().collect()))
                })
                .collect(),
        )
    }

    /// A lazy frame over explicit delta groups, keeping delete+insert rows.
    pub(crate) fn unmerged_groups_table_frame(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        groups: Vec<PartitionFiles>,
    ) -> Result<DataFrame> {
        Ok(context
            .read_table(Arc::new(self.table_provider_unmerged_groups(table, groups)))?)
    }

    /// A lazy frame over the state before a refresh window.
    pub(crate) async fn before_window_table_frame(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        before: &std::collections::HashMap<String, Option<i64>>,
    ) -> Result<DataFrame> {
        let groups = table.before_window_groups(&self.client, before).await?;
        self.groups_table_frame(context, table, groups)
    }

    /// A lazy frame over the partition versions of an epoch.
    pub(crate) fn at_versions_table_frame(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        versions: Vec<PartitionVersion>,
    ) -> Result<DataFrame> {
        Ok(context
            .read_table(Arc::new(self.table_provider_at_versions(table, versions)))?)
    }

    /// A lazy frame over the state as of `as_of_ms`.
    pub(crate) fn as_of_table_frame(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        as_of_ms: i64,
    ) -> Result<DataFrame> {
        Ok(context.read_table(Arc::new(self.table_provider_as_of(table, as_of_ms)))?)
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
        let meta = self.source_baseline_meta(source).await?;
        Ok(SourceBaseline {
            groups: meta.groups,
            cursors: meta.cursors,
            to_versions: meta.to_versions,
        })
    }

    /// The source baseline restricted to the partitions a constant filter can
    /// match.  Cursors and versions still cover every partition, so future
    /// windows start from the same boundary.
    async fn source_baseline_for_filter(
        &self,
        source: &IvmTable,
        filter: Option<&str>,
    ) -> Result<SourceBaseline> {
        let mut baseline = self.source_baseline(source).await?;
        if let Some(predicate) = Self::filter_partition_predicate(source, filter) {
            baseline.groups.retain(|group| {
                predicate.matches(&group.partition_desc, &source.range_partition_columns)
                    != Some(false)
            });
        }
        Ok(baseline)
    }

    async fn source_baseline_meta(
        &self,
        source: &IvmTable,
    ) -> Result<SourceBaselineMeta> {
        let mut groups = Vec::new();
        let mut cursors = Vec::new();
        let mut to_versions = Vec::new();
        for partition in self.client.get_all_partition_info(&source.table_id).await? {
            let files = self
                .client
                .get_data_files_of_single_partition(&partition)
                .await?;
            if !files.is_empty() {
                groups.push(PartitionFiles {
                    partition_desc: partition.partition_desc.clone(),
                    files,
                });
            }
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
        Ok(SourceBaselineMeta {
            groups,
            cursors,
            to_versions,
        })
    }

    /// Whether a refresh failed in a way that requires a rebuild: a dropped
    /// source partition, a source window that needs a snapshot, or a cursor
    /// window that no longer aligns with the committed history.
    pub(crate) fn rebuild_required(error: &rootcause::Report) -> bool {
        error.attachments().iter().any(|attachment| {
            attachment.downcast_inner::<PartitionDropped>().is_some()
                || attachment.downcast_inner::<RebuildRequired>().is_some()
        })
    }

    /// Remove the rows of dropped source partitions from a view whose keys
    /// carry the partition columns.
    ///
    /// A dropped source partition removes its rows from every view, and the
    /// file-level window cannot retract them; the caller otherwise rebuilds.
    /// When the view's group keys carry all of the source's range partition
    /// columns, the affected output rows are exactly the MV rows matching the
    /// partition values: they are retracted under their own epoch and the
    /// partition's cursor is dropped, so the partition may be recreated later
    /// without stale state.  Views whose keys do not carry the partition
    /// columns return `None` and are rebuilt.
    pub async fn cleanup_dropped_partitions(
        &self,
        spec: &ViewSpec,
        error: &rootcause::Report,
    ) -> Result<Option<i64>> {
        let Some(dropped) = error
            .attachments()
            .iter()
            .find_map(|attachment| attachment.downcast_inner::<PartitionDropped>())
            .cloned()
        else {
            return Ok(None);
        };
        // A partition local fallback view carries the partition column from
        // its one partitioned source, so the dropped partition's rows are
        // exactly the MV rows of that partition.
        if matches!(spec, ViewSpec::Fallback { .. }) {
            let view = match self.spec_view(spec).await? {
                SpecView::Fallback(view) => view,
                _ => return Ok(None),
            };
            return self
                .cleanup_dropped_fallback_partitions(&view, &dropped)
                .await;
        }
        let Ok(SpecView::SumCount(view)) = self.spec_view(spec).await else {
            return Ok(None);
        };
        let partitions = view.source.range_partition_columns.clone();
        if dropped.source_table_id != view.source.table_id
            || partitions.is_empty()
            || view.group_keys.is_empty()
            || !partitions
                .iter()
                .all(|column| view.group_keys.contains(column))
            || !partitions
                .iter()
                .all(|column| view.mv.schema.field_with_name(column).is_ok())
        {
            return Ok(None);
        }
        let mut predicate_parts = Vec::new();
        let mut identity = Vec::new();
        let mut cursors = Vec::new();
        for (partition, to_version, to_timestamp) in &dropped.partitions {
            let Some(values) = parse_partition_values(partition, &partitions) else {
                return Ok(None);
            };
            let parts = partitions
                .iter()
                .zip(values)
                .map(|(column, value)| {
                    format!("{} = {}", quote_ident(column), sql_literal(&value))
                })
                .collect::<Vec<_>>();
            predicate_parts.push(format!("({})", parts.join(" and ")));
            identity.push((view.source.table_id.clone(), partition.clone(), 0, 0));
            cursors.push(Cursor {
                source_table_id: view.source.table_id.clone(),
                partition_desc: partition.clone(),
                last_version: *to_version,
                last_timestamp: *to_timestamp,
            });
        }
        self.register_view(&view).await?;
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => return Ok(Some(epoch)),
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let context = refresh_session();
        context.register_table("mv", Arc::new(self.table_provider(&view.mv)))?;
        let sql =
            sum_count_cleanup_delete_sql(&view, &predicate_parts.join(" or "), epoch);
        let frame = context.sql(&sql).await?;
        let commit_ids = view
            .mv
            .append_stream(&self.client, frame.execute_stream().await?)
            .await?;
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        // The cursor advances past the deletion: the partition is not reported
        // again, and a partition recreated later starts above it.
        self.advance_cursors(&view.view_id, cursors).await?;
        Ok(Some(epoch))
    }

    /// Validate that a derived outer-join spec can encode its row identity:
    /// both sides' primary keys must be encodable (a struct recurses, a list
    /// or map key is rejected before the step is created).
    pub(crate) async fn validate_identity_keys(&self, spec: &ViewSpec) -> Result<()> {
        let (left_table_id, right_table_id) = match spec {
            ViewSpec::LeftJoin {
                left_table_id,
                right_table_id,
                ..
            }
            | ViewSpec::FullJoin {
                left_table_id,
                right_table_id,
                ..
            } => (left_table_id.clone(), right_table_id.clone()),
            _ => return Ok(()),
        };
        let left = self.open_table_by_id(&left_table_id).await?;
        let right = self.open_table_by_id(&right_table_id).await?;
        self::joins::validate_identity_keys(&left, &right)
    }

    /// Read the changelog window of every partition of a source.
    /// A condition on one range-partition column extracted from a view filter.
    fn filter_partition_predicate(
        source: &IvmTable,
        filter: Option<&str>,
    ) -> Option<PartitionPredicate> {
        let filter = filter?;
        if source.range_partition_columns.is_empty() {
            return None;
        }
        let context = refresh_session();
        let expression = parse_filter(&context, &source.schema, filter).ok()?;
        let mut predicate = PartitionPredicate::default();
        Self::collect_partition_conditions(&expression, source, &mut predicate);
        (!predicate.conditions.is_empty()).then_some(predicate)
    }

    fn collect_partition_conditions(
        expression: &Expr,
        source: &IvmTable,
        predicate: &mut PartitionPredicate,
    ) {
        match expression {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                Self::collect_partition_conditions(&binary.left, source, predicate);
                Self::collect_partition_conditions(&binary.right, source, predicate);
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                if let Some((column, literal)) = Self::partition_column_literal(binary)
                    && source.range_partition_columns.contains(&column.name)
                {
                    predicate.conditions.push(PartitionCondition::In {
                        column: column.name.clone(),
                        values: [lakesoul_io::helpers::format_scalar_value(literal)]
                            .into_iter()
                            .collect(),
                    });
                }
            }
            Expr::InList(list) if !list.negated => {
                let Expr::Column(column) = list.expr.as_ref() else {
                    return;
                };
                if !source.range_partition_columns.contains(&column.name) {
                    return;
                }
                let mut values = std::collections::BTreeSet::new();
                for item in &list.list {
                    let Some(literal) = Self::partition_literal(item) else {
                        // A non-literal item makes a partial IN unsound.
                        return;
                    };
                    values.insert(lakesoul_io::helpers::format_scalar_value(literal));
                }
                if !values.is_empty() {
                    predicate.conditions.push(PartitionCondition::In {
                        column: column.name.clone(),
                        values,
                    });
                }
            }
            Expr::BinaryExpr(binary)
                if matches!(
                    binary.op,
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
                ) =>
            {
                if let Some((column, literal)) = Self::partition_column_literal(binary) {
                    Self::push_partition_comparison(
                        source, predicate, column, literal, binary.op,
                    );
                }
            }
            Expr::Between(between) if !between.negated => {
                let Expr::Column(column) = between.expr.as_ref() else {
                    return;
                };
                let (Some(low), Some(high)) = (
                    Self::partition_literal(&between.low),
                    Self::partition_literal(&between.high),
                ) else {
                    return;
                };
                Self::push_partition_comparison(
                    source,
                    predicate,
                    column,
                    low,
                    Operator::GtEq,
                );
                Self::push_partition_comparison(
                    source,
                    predicate,
                    column,
                    high,
                    Operator::LtEq,
                );
            }
            _ => {}
        }
    }

    fn push_partition_comparison(
        source: &IvmTable,
        predicate: &mut PartitionPredicate,
        column: &Column,
        literal: &ScalarValue,
        op: Operator,
    ) {
        if !source.range_partition_columns.contains(&column.name) {
            return;
        }
        let Ok(field) = source.schema.field_with_name(&column.name) else {
            return;
        };
        let value = lakesoul_io::helpers::format_scalar_value(literal);
        let number = match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => match value.parse::<i128>() {
                Ok(number) => Some(number),
                Err(_) => return,
            },
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _) => None,
            _ => return,
        };
        predicate.conditions.push(PartitionCondition::Cmp {
            column: column.name.clone(),
            op,
            value,
            number,
        });
    }

    fn partition_column_literal(
        binary: &datafusion::logical_expr::BinaryExpr,
    ) -> Option<(&Column, &ScalarValue)> {
        match (&*binary.left, &*binary.right) {
            (Expr::Column(column), other) => {
                Self::partition_literal(other).map(|literal| (column, literal))
            }
            (other, Expr::Column(column)) => {
                Self::partition_literal(other).map(|literal| (column, literal))
            }
            _ => None,
        }
    }

    fn partition_literal(expression: &Expr) -> Option<&ScalarValue> {
        match expression {
            Expr::Literal(literal, _) => Some(literal),
            Expr::Cast(cast) => Self::partition_literal(&cast.expr),
            _ => None,
        }
    }

    /// Collect the source window and drop the changed partitions a constant
    /// filter excludes.  Skipped partitions still advance their cursors; they
    /// cannot affect this view.
    async fn collect_source_window_for_filter(
        &self,
        view_id: &str,
        source: &IvmTable,
        filter: Option<&str>,
    ) -> Result<SourceWindow> {
        let mut window = self.collect_source_window(view_id, source).await?;
        let Some(predicate) = Self::filter_partition_predicate(source, filter) else {
            return Ok(window);
        };
        let columns = &source.range_partition_columns;
        let keep = |desc: &str| predicate.matches(desc, columns) != Some(false);
        window
            .added_files
            .retain(|group| keep(&group.partition_desc));
        window.before_versions.retain(|desc, _| keep(desc));
        window.identity.retain(|(_, desc, _, _)| keep(desc));
        if window.added_files.is_empty() && !window.cursors.is_empty() {
            self.advance_cursors(view_id, window.cursors.clone())
                .await?;
        }
        Ok(window)
    }

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

        let from_versions = cursors
            .iter()
            .map(|(desc, cursor)| (desc.clone(), cursor.last_version))
            .collect::<HashMap<String, i64>>();
        let window = self
            .client
            .get_table_changelog(&source.table_id, &from_versions)
            .await?;

        let mut added_files = Vec::new();
        let mut new_cursors = Vec::new();
        let mut identity = Vec::new();
        let mut before_versions = HashMap::new();
        let mut dropped = Vec::new();
        for partition in window.partitions {
            let last_version = cursors
                .get(&partition.partition_desc)
                .map(|cursor| cursor.last_version)
                .unwrap_or(-1);
            if partition.requires_rebuild {
                return Err(report!(
                    "view {view_id} source partition {} requires a rebuild",
                    partition.partition_desc
                )
                .attach(RebuildRequired));
            }
            // A dropped partition must remove its rows from the view, which
            // the file-level window cannot express: the caller removes them
            // through the partition's keys or rebuilds.
            if partition.partition_deleted {
                dropped.push((
                    partition.partition_desc.clone(),
                    partition.to_version,
                    partition.to_timestamp,
                ));
                continue;
            }
            if partition.to_version <= last_version {
                continue;
            }

            added_files.push(PartitionFiles {
                partition_desc: partition.partition_desc.clone(),
                files: partition
                    .added_files
                    .iter()
                    .map(|file| file.path.clone())
                    .collect(),
            });
            before_versions.insert(
                partition.partition_desc.clone(),
                cursors
                    .get(&partition.partition_desc)
                    .map(|cursor| cursor.last_version),
            );
            identity.push((
                source.table_id.clone(),
                partition.partition_desc.clone(),
                last_version,
                partition.to_version,
            ));
            new_cursors.push(Cursor {
                source_table_id: source.table_id.clone(),
                partition_desc: partition.partition_desc.clone(),
                last_version: partition.to_version,
                last_timestamp: partition.to_timestamp,
            });
        }

        if !dropped.is_empty() {
            return Err(report!(
                "view {view_id} source partitions [{}] of table {} were dropped",
                dropped
                    .iter()
                    .map(|(desc, _, _)| desc.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                source.table_name
            )
            .attach(PartitionDropped {
                source_table_id: source.table_id.clone(),
                partitions: dropped,
            }));
        }

        Ok(SourceWindow {
            added_files,
            cursors: new_cursors,
            identity,
            before_timestamp,
            before_versions,
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
        let state_versions_before = self.state_versions(view_id).await?;

        let record = match self
            .metadata
            .begin_epoch_with_states(
                view_id,
                &window_key,
                &to_versions,
                &mv_versions_before,
                &state_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                return Ok(WindowStart::AlreadyApplied(record.epoch));
            }
            BeginEpoch::Created(record) => record,
            BeginEpoch::Pending(record) => {
                let current = output_partition_versions(&self.client, output).await?;
                if current != record.mv_versions_before {
                    // A previous attempt wrote the MV but did not mark the
                    // epoch.  Check the auxiliary states as well: marking the
                    // epoch while a window/rank/join index is behind would
                    // make later keyed reads silently stale.
                    let current_states = self.state_versions(view_id).await?;
                    let state_incomplete = if !record.state_versions.is_empty() {
                        current_states != record.state_versions
                    } else {
                        // A crash before the checkpoint has no trustworthy
                        // after snapshot.  If this view already has state, do
                        // not guess: a rebuild is safer than accepting a
                        // partially written window index/rank/join state.
                        !record.state_versions_before.is_empty()
                            || !current_states.is_empty()
                    };
                    let mut commit_ids = record.commit_ids.clone();
                    if state_incomplete && !record.state_versions.is_empty() {
                        let (repair_ids, repaired_tables) =
                            self.repair_mirror_states(view_id, output).await?;
                        commit_ids.extend(repair_ids);
                        let repaired = self.state_versions(view_id).await?;
                        let repaired_ok = record.state_versions.iter().all(|expected| {
                            repaired_tables.contains(&expected.table_id)
                                || repaired.iter().any(|current| current == expected)
                        });
                        if !repaired_ok {
                            return Err(report!(
                                "view {view_id} window {window_key} has a committed MV \
                                 but incomplete auxiliary state; a rebuild is required"
                            )
                            .attach(RebuildRequired));
                        }
                    } else if state_incomplete {
                        return Err(report!(
                            "view {view_id} window {window_key} has a committed MV \
                             but incomplete auxiliary state; a rebuild is required"
                        )
                        .attach(RebuildRequired));
                    }
                    self.mark_epoch_committed(&record, &current, &commit_ids)
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
                )
                .attach(RebuildRequired));
            }
        }

        Ok(WindowStart::Apply(Box::new(record)))
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

#[cfg(test)]
mod type_encoding_tests {
    use super::*;

    fn round_trip(data_type: DataType) {
        let encoded = encode_data_type(&data_type).expect("the type encodes");
        let decoded = decode_data_type(&encoded).expect("the type decodes");
        assert_eq!(decoded, data_type, "round trip through {encoded:?}");
    }

    #[test]
    fn scalar_types_round_trip() {
        for data_type in [
            DataType::Boolean,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::BinaryView,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Decimal128(20, 2),
            DataType::Decimal256(40, 4),
        ] {
            round_trip(data_type);
        }
    }

    #[test]
    fn nested_types_round_trip() {
        let list_of_struct = DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, true),
            ])),
            true,
        )));
        round_trip(list_of_struct.clone());
        round_trip(DataType::LargeList(Arc::new(Field::new(
            "items",
            list_of_struct.clone(),
            false,
        ))));
        round_trip(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            3,
        ));
        round_trip(DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("tags", list_of_struct, true),
            Field::new(
                "scores",
                DataType::Struct(Fields::from(vec![Field::new(
                    "score",
                    DataType::Float64,
                    true,
                )])),
                true,
            ),
        ])));
        round_trip(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        ));
    }

    #[test]
    fn nested_names_with_delimiters_round_trip() {
        // Field names may contain the delimiters a compact form would use.
        round_trip(DataType::Struct(Fields::from(vec![Field::new(
            "a:b,c\"d",
            DataType::Int64,
            true,
        )])));
    }

    #[test]
    fn unsupported_types_reject_the_shape() {
        let error = encode_data_type(&DataType::Null).unwrap_err();
        assert!(
            error.to_string().contains("is not supported"),
            "unexpected error: {error}"
        );
        assert!(
            error.attachments().iter().any(|attachment| attachment
                .downcast_inner::<crate::sql::UnsupportedShape>()
                .is_some()),
            "the rejection must carry the unsupported-shape marker"
        );
    }

    #[test]
    fn invalid_encodings_are_rejected() {
        assert!(decode_data_type("no_such_type").is_err());
        assert!(decode_data_type("struct:[{]").is_err());
        assert!(decode_data_type("list:{\"name\":\"i\"}").is_err());
        assert!(decode_data_type("map:1:{}").is_err());
    }

    #[test]
    fn a_computed_group_key_may_shadow_a_source_column() {
        let schema = Schema::new(vec![
            Field::new("spec_name", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]);
        let fields = group_key_fields(
            &schema,
            &["spec_name".to_string()],
            &["CASE WHEN spec_name = 'a' THEN 'b' ELSE spec_name END".to_string()],
        )
        .unwrap();
        assert_eq!(fields[0].name(), "spec_name");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
    }
}
