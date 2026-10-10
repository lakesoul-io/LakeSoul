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

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, ScalarValue};
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::when;
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
use crate::provider::IvmTableProvider;
use crate::table::{
    IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN, IvmTable, IvmTableOptions, PartitionFiles,
    create_ivm_table,
};

mod aggregates;
mod joins;
mod lookup_chain;
mod multi_join;
mod recompute;
mod rows;
mod semi_anti;
mod unions;
mod windows;

pub use self::aggregates::*;
pub use self::joins::*;
pub use self::lookup_chain::*;
pub use self::multi_join::*;
pub use self::recompute::*;
pub use self::rows::*;
pub use self::semi_anti::*;
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
        /// The payload column of the left source.
        left_value: String,
        /// The payload column of the right source.
        right_value: String,
        /// The wide output columns beyond the join keys; empty means the
        /// compact `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
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
        /// The payload column of the left source.
        left_value: String,
        /// The payload column of the right source; NULL without a match.
        right_value: String,
        /// The wide output columns; empty means the compact
        /// `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
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
        /// The wide output columns; empty means the compact
        /// `left_value` / `right_value` shape.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        output_columns: Vec<JoinOutputColumn>,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
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
        /// The left columns materialized; the left primary keys are always
        /// contained.
        output_columns: Vec<String>,
        /// An optional filter the contributing left rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        left_filter: Option<String>,
        /// An optional filter the contributing right rows must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        right_filter: Option<String>,
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
        /// How many rows to keep per group.
        limit: i64,
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
        /// An optional filter applied before windowing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<String>,
    },
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
            | ViewSpec::TopK { view_id, .. } => view_id,
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
            | ViewSpec::MultiAgg {
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
            } => {
                ids.push(source_table_id.as_str());
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
        }
    }
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

/// A portable encoding of the aggregate result types.
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
        DataType::Binary => "binary".to_string(),
        DataType::LargeBinary => "largebinary".to_string(),
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
        DataType::List(field) => {
            format!("list:{}", encode_data_type(field.data_type())?)
        }
        other => {
            return Err(report!(
                "the aggregate result type {other} is not supported"
            ));
        }
    })
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
        "binary" => DataType::Binary,
        "largebinary" => DataType::LargeBinary,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        other => {
            if let Some(rest) = other.strip_prefix("timestamp:") {
                let (unit, timezone) = rest
                    .split_once(':')
                    .ok_or_else(|| report!("invalid aggregate result type {other}"))?;
                let unit = match unit {
                    "s" => TimeUnit::Second,
                    "ms" => TimeUnit::Millisecond,
                    "us" => TimeUnit::Microsecond,
                    "ns" => TimeUnit::Nanosecond,
                    _ => {
                        return Err(report!("invalid aggregate result type {other}"));
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
                    .ok_or_else(|| report!("invalid aggregate result type {other}"))?;
                DataType::Decimal128(
                    precision
                        .parse()
                        .map_err(|_| report!("invalid aggregate result type {other}"))?,
                    scale
                        .parse()
                        .map_err(|_| report!("invalid aggregate result type {other}"))?,
                )
            } else if let Some(rest) = other.strip_prefix("decimal256:") {
                let (precision, scale) = rest
                    .split_once(':')
                    .ok_or_else(|| report!("invalid aggregate result type {other}"))?;
                DataType::Decimal256(
                    precision
                        .parse()
                        .map_err(|_| report!("invalid aggregate result type {other}"))?,
                    scale
                        .parse()
                        .map_err(|_| report!("invalid aggregate result type {other}"))?,
                )
            } else if let Some(inner) = other.strip_prefix("list:") {
                DataType::List(Arc::new(Field::new(
                    "item",
                    decode_data_type(inner)?,
                    true,
                )))
            } else {
                return Err(report!("invalid aggregate result type {other}"));
            }
        }
    })
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
    let context = SessionContext::new();
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
            if let Ok(field) = source_schema.field_with_name(key) {
                // A plain column among computed keys is its own expression.
                if expression == key {
                    return Ok(Arc::new(field.clone()) as arrow_schema::FieldRef);
                }
                return Err(report!(
                    "a computed group key must not reuse the source column {key}"
                ));
            }
            let (data_type, nullable) = expression_type(source_schema, expression)?;
            Ok(Arc::new(Field::new(key, data_type, nullable)) as arrow_schema::FieldRef)
        })
        .collect()
}

/// The IVM runtime: a metadata client plus the `ivm` schema access layer.
pub struct IvmRuntime {
    client: MetaDataClient,
    metadata: IvmMetadata,
}

/// The changelog window of every partition of one source.
struct SourceWindow {
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

/// Build a frame from raw batches and apply the side filter.
fn filtered_frame(
    context: &SessionContext,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    filter: Option<&str>,
) -> Result<DataFrame> {
    apply_side_filter(
        context,
        dataframe(context, batches, schema)?,
        schema,
        filter,
    )
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
        Some(column) => Ok(frame.filter(column_expr(column).not_eq(lit("delete")))?),
        None => Ok(frame),
    }
}

/// Aggregate sum/count batches into `group_key -> (sum, count)`.
fn quote_ident(name: &str) -> String {
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

/// The schema of the projected group keys of a view, computed expressions
/// included.
fn key_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
) -> Result<SchemaRef> {
    Ok(Arc::new(Schema::new(group_key_fields(
        source_schema,
        group_keys,
        group_exprs,
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

/// Project the computed group keys into a batch set, so the aggregate SQL can
/// group by the keys by name.
async fn project_group_keys(
    context: &SessionContext,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    group_keys: &[String],
    group_exprs: &[String],
) -> Result<Vec<RecordBatch>> {
    if group_exprs.is_empty() {
        return Ok(batches);
    }
    let df_schema = DFSchema::try_from(schema.as_ref().clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    let mut frame = dataframe(context, batches, schema)?;
    for (key, expression) in group_keys.iter().zip(group_exprs) {
        let expression = context
            .state()
            .create_logical_expr(expression, &df_schema)
            .map_err(|error| {
                report!("invalid group expression {expression:?}: {error}")
            })?;
        frame = frame.with_column(key.as_str(), expression)?;
    }
    let projected_schema = Arc::new(frame.schema().as_arrow().clone());
    let mut batches = frame
        .collect()
        .await
        .map_err(|error| report!("projecting group keys: {error}"))?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(projected_schema));
    }
    Ok(batches)
}

/// The groups a refresh window can touch: the groups in the delta plus, for a
/// keyed source, the previous groups of the rows that changed.
fn affected_groups_sql(
    source: &IvmTable,
    group_keys: &[String],
    keyed: bool,
    filter: Option<&str>,
) -> String {
    let keys = quoted_list(group_keys);
    let plain_where = filter_where(filter);
    if !keyed {
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

/// Equality/`IN` filters over `keys` of the given rows, used to prune the
/// source / state reads (and the reader's bucket pruning) to the affected
/// partitions or groups.
fn key_filters(
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
fn filter_columns(
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
fn parse_filter(
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
    let context = SessionContext::new();
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
                refresh_interval_ms,
            }),
            ViewSpec::MultiJoin {
                view_id,
                mv_table_id,
                sources,
                keys,
                columns,
                conditions,
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
                left_value,
                right_value,
                output_columns,
                left_filter,
                right_filter,
                pair_filter,
            } => SpecView::Join(JoinView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                pair_filter: pair_filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::LeftJoin {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                join_keys,
                right_keys,
                left_value,
                right_value,
                output_columns,
                left_filter,
                right_filter,
            } => SpecView::LeftJoin(LeftJoinView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::FullJoin {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                join_keys,
                right_keys,
                left_value,
                right_value,
                output_columns,
                left_filter,
                right_filter,
            } => SpecView::FullJoin(FullJoinView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                refresh_interval_ms,
            }),
            ViewSpec::LookupJoin {
                view_id,
                left_table_id,
                right_table_id,
                output_table_id,
                join_keys,
                right_keys,
                left_filter,
                right_filter,
                left_value,
                right_value,
                output_columns,
            } => SpecView::LookupJoin(LookupJoinView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                output: self.open_table_by_id(output_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                output_columns: output_columns.clone(),
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
                filter,
            } => SpecView::Window(WindowView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                partition_keys: partition_keys.clone(),
                order_keys: order_keys.clone(),
                order_by: order_by.clone(),
                columns: columns.clone(),
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
                output_columns,
                left_filter,
                right_filter,
            } => SpecView::LeftAggregate(LeftAggregateView {
                view_id: view_id.clone(),
                left: self.open_table_by_id(left_table_id).await?,
                right: self.open_table_by_id(right_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                join_keys: join_keys.clone(),
                right_keys: right_keys.clone(),
                right_aggregate: right_aggregate.clone(),
                aggregate_column: aggregate_column.clone(),
                output_columns: output_columns.clone(),
                left_filter: left_filter.clone(),
                right_filter: right_filter.clone(),
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
                filter,
            } => SpecView::MultiWindow(MultiWindowView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                windows: windows.clone(),
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
                limit,
                filter,
            } => SpecView::TopK(TopKView {
                view_id: view_id.clone(),
                source: self.open_table_by_id(source_table_id).await?,
                mv: self.open_table_by_id(mv_table_id).await?,
                group_keys: group_keys.clone(),
                order_keys: order_keys.clone(),
                order_by: order_by.clone(),
                output_columns: output_columns.clone(),
                limit: *limit,
                filter: filter.clone(),
                refresh_interval_ms,
            }),
        })
    }

    /// Refresh a view from its persisted [`ViewSpec`], opening every table by
    /// id.  The spec is the only state needed to drive a refresh from any
    /// process; the registered refresh interval is preserved.
    /// Refresh a view and every view it reads, upstream first, so a scheduler
    /// can drive a whole chain with one call.
    ///
    /// Returns `(view_id, epoch)` for every view that was refreshed, in
    /// refresh order (`None` when the view had no changes).  The traversal is
    /// iterative and cycle-safe: a table that is not a registered view is
    /// skipped.
    pub async fn refresh_view_chain(
        &self,
        view_id: &str,
    ) -> Result<Vec<(String, Option<i64>)>> {
        let mut visited = std::collections::HashSet::new();
        let mut specs: HashMap<String, ViewSpec> = HashMap::new();
        let mut refreshed = Vec::new();
        let mut stack = vec![(view_id.to_string(), false)];
        while let Some((id, expanded)) = stack.pop() {
            if !expanded {
                if visited.contains(&id) {
                    continue;
                }
                let Some(value) = self.metadata.get_view_spec(&id).await? else {
                    continue;
                };
                let spec: ViewSpec = serde_json::from_value(value)?;
                visited.insert(id.clone());
                let sources = spec
                    .source_table_ids()
                    .into_iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                specs.insert(id.clone(), spec);
                stack.push((id, true));
                for source in sources.into_iter().rev() {
                    stack.push((source, false));
                }
            } else if let Some(spec) = specs.get(&id) {
                let epoch = self.refresh_spec(spec).await?;
                refreshed.push((id, epoch));
            }
        }
        Ok(refreshed)
    }

    pub async fn refresh_spec(&self, spec: &ViewSpec) -> Result<Option<i64>> {
        let kind = spec.kind();
        let started = std::time::Instant::now();
        let view = self.spec_view(spec).await;
        let result = match view {
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
            Ok(SpecView::LeftJoin(view)) => self.refresh_left_join(&view).await,
            Ok(SpecView::FullJoin(view)) => self.refresh_full_join(&view).await,
            Ok(SpecView::CrossJoin(view)) => self.refresh_cross_join(&view).await,
            Ok(SpecView::MinMax(view)) => self.refresh_min_max(&view).await,
            Ok(SpecView::DistinctAgg(view)) => self.refresh_distinct_agg(&view).await,
            Ok(SpecView::Window(view)) => self.refresh_window(&view).await,
            Ok(SpecView::SemiAnti(view)) => self.refresh_semi_anti(&view).await,
            Ok(SpecView::LeftAggregate(view)) => self.refresh_left_aggregate(&view).await,
            Ok(SpecView::LookupChain(view)) => self.refresh_lookup_chain(&view).await,
            Ok(SpecView::GroupingSets(view)) => self.refresh_grouping_sets(&view).await,
            Ok(SpecView::Row(view)) => self.refresh_row(&view).await,
            Ok(SpecView::UnionAll(view)) => self.refresh_union_all(&view).await,
            Ok(SpecView::UnionDistinct(view)) => self.refresh_union_distinct(&view).await,
            Ok(SpecView::TopK(view)) => self.refresh_top_k(&view).await,
            Ok(SpecView::MultiWindow(view)) => self.refresh_multi_window(&view).await,
            Err(error) => Err(error),
        };
        crate::observability::record_refresh(
            kind,
            matches!(result, Ok(Some(_))),
            started.elapsed(),
        );
        result
    }

    /// Rebuild a view from its persisted [`ViewSpec`]: the full source state is
    /// recomputed, the MV is replaced and the cursors are reset to the current
    /// source versions.
    pub async fn rebuild_spec(&self, spec: &ViewSpec) -> Result<i64> {
        let kind = spec.kind();
        let started = std::time::Instant::now();
        let view = self.spec_view(spec).await;
        let result = match view {
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
            Ok(SpecView::LeftJoin(view)) => self.rebuild_left_join(&view).await,
            Ok(SpecView::FullJoin(view)) => self.rebuild_full_join(&view).await,
            Ok(SpecView::CrossJoin(view)) => self.rebuild_cross_join(&view).await,
            Ok(SpecView::MinMax(view)) => self.rebuild_min_max(&view).await,
            Ok(SpecView::DistinctAgg(view)) => self.rebuild_distinct_agg(&view).await,
            Ok(SpecView::Window(view)) => self.rebuild_window(&view).await,
            Ok(SpecView::SemiAnti(view)) => self.rebuild_semi_anti(&view).await,
            Ok(SpecView::LeftAggregate(view)) => self.rebuild_left_aggregate(&view).await,
            Ok(SpecView::LookupChain(view)) => self.rebuild_lookup_chain(&view).await,
            Ok(SpecView::GroupingSets(view)) => self.rebuild_grouping_sets(&view).await,
            Ok(SpecView::Row(view)) => self.rebuild_row(&view).await,
            Ok(SpecView::UnionAll(view)) => self.rebuild_union_all(&view).await,
            Ok(SpecView::UnionDistinct(view)) => self.rebuild_union_distinct(&view).await,
            Ok(SpecView::TopK(view)) => self.rebuild_top_k(&view).await,
            Ok(SpecView::MultiWindow(view)) => self.rebuild_multi_window(&view).await,
            Err(error) => Err(error),
        };
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
        let mut groups = Vec::new();
        let mut cursors = Vec::new();
        let mut to_versions = Vec::new();
        for partition in self.client.get_all_partition_info(&source.table_id).await? {
            let files = self
                .client
                .get_data_files_of_single_partition(&partition)
                .await?;
            groups.push(PartitionFiles {
                partition_desc: partition.partition_desc.clone(),
                files,
            });
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
        let batches = source.read_partition_files(groups).await?;
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
        for partition in window.partitions {
            let last_version = cursors
                .get(&partition.partition_desc)
                .map(|cursor| cursor.last_version)
                .unwrap_or(-1);
            if partition.requires_rebuild {
                return Err(report!(
                    "view {view_id} source partition {} requires a rebuild",
                    partition.partition_desc
                ));
            }
            // A deleted partition keeps its old cursor; it no longer exists in
            // the table so it will not be scanned again.
            if partition.partition_deleted || partition.to_version <= last_version {
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
                        .mark_epoch_committed(&record, &current, &record.commit_ids)
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
