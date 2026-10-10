// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The incremental aggregate views: `SUM`/`COUNT`/`AVG`, `MIN`/`MAX`,
//! `COUNT(DISTINCT)`/`SUM(DISTINCT)`, their value-count state and the
//! `GROUPING SETS`/`ROLLUP`/`CUBE` sets.

use super::*;

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
    /// The rendered group expressions, parallel to
    /// [`Self::group_keys`]; empty means every key is a plain column.
    pub group_exprs: Vec<String>,
    /// The summed column; `None` counts rows only.
    pub value_column: Option<String>,
    /// The summed expression when the argument is not a plain column
    /// (`SUM(v * 2)`); mutually exclusive with [`Self::value_column`].
    pub value_expr: Option<String>,
    /// The column whose non-NULL values a `COUNT(column)` aggregate counts;
    /// `None` falls back to [`Self::value_column`].
    pub count_column: Option<String>,
    /// The `FILTER (WHERE ...)` predicate of the summed column and the
    /// non-NULL count; `None` when they are unfiltered.  The row count is
    /// never filtered, so a group with no matching row stays with a NULL
    /// value.
    pub aggregate_filter: Option<String>,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized columns.
    pub having: Option<String>,
    /// `true` for an `AVG` view: the MV additionally holds
    /// [`IVM_AVG_COLUMN`].
    pub average: bool,
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
            group_exprs: Vec::new(),
            value_column,
            value_expr: None,
            count_column: None,
            aggregate_filter: None,
            filter: None,
            having: None,
            average: false,
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
            group_exprs: Vec::new(),
            value_column,
            value_expr: None,
            count_column: None,
            aggregate_filter: None,
            filter: None,
            having: None,
            average: false,
            refresh_interval_ms: 0,
        }
    }

    /// Only rows matching `filter` contribute to the view.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Only groups matching `having` stay in the view.
    pub fn with_having(mut self, having: impl Into<String>) -> Self {
        self.having = Some(having.into());
        self
    }

    /// Aggregate over a rendered scalar expression (`SUM(v * 2)`).
    pub fn with_value_expr(mut self, value_expr: impl Into<String>) -> Self {
        self.value_expr = Some(value_expr.into());
        self.value_column = None;
        self
    }

    /// Group by rendered expressions parallel to the group keys
    /// (`SELECT v % 10 AS bucket ... GROUP BY bucket`).
    pub fn with_group_exprs(mut self, group_exprs: Vec<String>) -> Self {
        self.group_exprs = group_exprs;
        self
    }

    /// Materialize the non-NULL count of `count_column` (a `COUNT(column)`
    /// aggregate).
    pub fn with_count_column(mut self, count_column: impl Into<String>) -> Self {
        self.count_column = Some(count_column.into());
        self
    }

    /// Only rows matching `aggregate_filter` contribute to the summed column
    /// and the non-NULL count (an aggregate `FILTER (WHERE ...)`); the row
    /// count stays unfiltered.
    pub fn with_aggregate_filter(mut self, aggregate_filter: impl Into<String>) -> Self {
        self.aggregate_filter = Some(aggregate_filter.into());
        self
    }

    /// Materialize the average as well; the value column must be numeric.
    pub fn with_average(mut self) -> Self {
        self.average = true;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::SumCount {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            count_column: self.count_column.clone(),
            aggregate_filter: self.aggregate_filter.clone(),
            filter: self.filter.clone(),
            having: self.having.clone(),
            average: self.average,
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
    /// The rendered group expressions, parallel to `group_keys`.
    group_exprs: &'a [String],
    value_column: Option<&'a str>,
    value_expr: Option<&'a str>,
    agg: ValueAgg,
    filter: Option<&'a str>,
    having: Option<&'a str>,
}

/// Validate that a grouping-sets view can be maintained.
fn validate_grouping_sets_view(view: &GroupingSetsView) -> Result<()> {
    if view.groupings.is_empty() {
        return Err(report!(
            "grouping sets view {} needs at least one grouping set",
            view.view_id
        ));
    }
    for set in &view.groupings {
        for index in set {
            if *index >= view.group_keys.len() {
                return Err(report!(
                    "grouping sets view {}: key index {index} is out of range",
                    view.view_id
                ));
            }
        }
    }
    if view.group_exprs.is_empty() {
        for key in &view.group_keys {
            view.source.schema.field_with_name(key).map_err(|_| {
                report!(
                    "grouping sets view {}: key column {key} is not in the source",
                    view.view_id
                )
            })?;
        }
    } else {
        group_key_fields(&view.source.schema, &view.group_keys, &view.group_exprs)?;
    }
    if view.source.primary_keys.is_empty() && view.source.cdc_column.is_some() {
        return Err(report!(
            "grouping sets view {}: an append-only changelog source is not maintained",
            view.view_id
        ));
    }
    validate_having(&view.view_id, &view.mv.schema, view.having.as_deref())?;
    if !view.aggregates.is_empty() {
        let mut names = std::collections::HashSet::new();
        for (call, column, _) in &view.aggregates {
            if call.is_empty() || column.is_empty() {
                return Err(report!(
                    "grouping sets view {} has an empty aggregate",
                    view.view_id
                ));
            }
            if !names.insert(column.as_str()) {
                return Err(report!(
                    "grouping sets view {}: column {column} is materialized twice",
                    view.view_id
                ));
            }
        }
        return Ok(());
    }
    if view.value_expr.is_some() && view.value_column.is_some() {
        return Err(report!(
            "grouping sets view {}: a value column and a value expression are \
             mutually exclusive",
            view.view_id
        ));
    }
    if let Some(expression) = &view.value_expr {
        let (data_type, _) = expression_type(&view.source.schema, expression)?;
        if view.average {
            avg_result_type(&data_type)?;
        }
        sum_result_type(&data_type)?;
    } else if let Some(column) = &view.value_column {
        let value_type = field_type(&view.source.schema, column)?;
        if view.average {
            avg_result_type(&value_type)?;
        }
        sum_result_type(&value_type)?;
    }
    if let Some(filter) = &view.filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, filter)?;
    }
    if let Some(filter) = &view.aggregate_filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, filter)?;
    }
    Ok(())
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
    /// The rendered group expressions, parallel to
    /// [`Self::group_keys`]; empty means every key is a plain column.
    pub group_exprs: Vec<String>,
    /// The min/max value column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered min/max value expression.
    pub value_expr: Option<String>,
    /// Whether the minimum or the maximum is maintained.
    pub min_max: MinMaxKind,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized columns.
    pub having: Option<String>,
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
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            min_max,
            filter: None,
            having: None,
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
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            min_max,
            filter: None,
            having: None,
            refresh_interval_ms: 0,
        }
    }

    /// Only rows matching `filter` contribute to the view.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Only groups matching `having` stay in the view.
    pub fn with_having(mut self, having: impl Into<String>) -> Self {
        self.having = Some(having.into());
        self
    }

    /// Aggregate over a rendered scalar expression (`MIN(v * 2)`).
    pub fn with_value_expr(mut self, value_expr: impl Into<String>) -> Self {
        self.value_expr = Some(value_expr.into());
        self.value_column = None;
        self
    }

    /// Group by rendered expressions parallel to the group keys.
    pub fn with_group_exprs(mut self, group_exprs: Vec<String>) -> Self {
        self.group_exprs = group_exprs;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::MinMax {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            state_table_id: self.state.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            min_max: self.min_max,
            filter: self.filter.clone(),
            having: self.having.clone(),
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
    /// The rendered group expressions, parallel to
    /// [`Self::group_keys`]; empty means every key is a plain column.
    pub group_exprs: Vec<String>,
    /// The distinct value column.
    pub value_column: String,
    /// The distinct value columns of a multi-column `COUNT(DISTINCT a, b)`;
    /// empty for the single-column shape, which uses [`Self::value_column`].
    pub value_columns: Vec<String>,
    /// Whether the distinct count or the distinct sum is maintained.
    pub agg: DistinctAggKind,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized columns.
    pub having: Option<String>,
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
            group_exprs: Vec::new(),
            value_column: value_column.into(),
            value_columns: Vec::new(),
            agg,
            filter: None,
            having: None,
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
            group_exprs: Vec::new(),
            value_column: value_column.into(),
            value_columns: Vec::new(),
            agg,
            filter: None,
            having: None,
            refresh_interval_ms: 0,
        }
    }

    /// Maintain a multi-column `COUNT(DISTINCT a, b)` by recomputing the
    /// affected groups; the first column is kept as the primary value column.
    pub fn with_value_columns(mut self, value_columns: Vec<String>) -> Self {
        if let Some(first) = value_columns.first() {
            self.value_column = first.clone();
        }
        self.value_columns = value_columns;
        self
    }

    /// Only rows matching `filter` contribute to the view.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Only groups matching `having` stay in the view.
    pub fn with_having(mut self, having: impl Into<String>) -> Self {
        self.having = Some(having.into());
        self
    }

    /// Group by rendered expressions parallel to the group keys.
    pub fn with_group_exprs(mut self, group_exprs: Vec<String>) -> Self {
        self.group_exprs = group_exprs;
        self
    }

    /// The recompute description of a multi-column distinct count.
    fn recompute_parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "count(distinct {})",
                quoted_list(&self.value_columns)
            ),
            // The distinct-count MV column matches the value-count schema.
            column: IVM_VALUE_COLUMN.to_string(),
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
            distinct_columns: Some(&self.value_columns),
            extra_aggregates: Vec::new(),
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::DistinctAgg {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            state_table_id: self.state.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_columns: self.value_columns.clone(),
            agg: self.agg,
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }
}

/// A `GROUP BY GROUPING SETS`/`ROLLUP`/`CUBE` view over a keyed or plain
/// append-only source with
/// the `SUM`/`COUNT`/`AVG` aggregates.
///
/// Every grouping set is maintained over the same MV: a row carries its
/// grouping index and the flat key columns, and the keys a set does not group
/// by are NULL, so rows of different sets never collide.
#[derive(Debug, Clone)]
pub struct GroupingSetsView {
    /// The view id.
    pub view_id: String,
    /// The source table (keyed).
    pub source: IvmTable,
    /// The materialized view table, created from
    /// [`grouping_sets_mv_schema_for`].
    pub mv: IvmTable,
    /// The flat key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to [`Self::group_keys`].
    pub group_exprs: Vec<String>,
    /// Each grouping set as indices into [`Self::group_keys`].
    pub groupings: Vec<Vec<usize>>,
    /// The summed column.
    pub value_column: Option<String>,
    /// A rendered value expression; mutually exclusive with
    /// [`Self::value_column`].
    pub value_expr: Option<String>,
    /// A `COUNT(column)` column.
    pub count_column: Option<String>,
    /// The shared aggregate `FILTER (WHERE ...)` predicate.
    pub aggregate_filter: Option<String>,
    /// Whether the view also materializes `AVG`.
    pub average: bool,
    /// The materialized `GROUPING(key)` columns.
    pub grouping_columns: Vec<GroupingColumn>,
    /// A general aggregate list (empty keeps the SUM/COUNT/AVG layout).
    pub aggregates: Vec<(String, String, DataType)>,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized columns.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl GroupingSetsView {
    /// A new view over `group_keys` with the given grouping sets.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        groupings: Vec<Vec<usize>>,
        value_column: Option<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            groupings,
            value_column,
            value_expr: None,
            count_column: None,
            aggregate_filter: None,
            average: false,
            grouping_columns: Vec::new(),
            aggregates: Vec::new(),
            filter: None,
            having: None,
            refresh_interval_ms: 0,
        }
    }

    /// Group by rendered expressions parallel to the group keys.
    pub fn with_group_exprs(mut self, group_exprs: Vec<String>) -> Self {
        self.group_exprs = group_exprs;
        self
    }

    /// Sum a rendered expression instead of a column.
    pub fn with_value_expr(mut self, value_expr: impl Into<String>) -> Self {
        self.value_expr = Some(value_expr.into());
        self.value_column = None;
        self
    }

    /// Also maintain `COUNT(column)`.
    pub fn with_count_column(mut self, count_column: impl Into<String>) -> Self {
        self.count_column = Some(count_column.into());
        self
    }

    /// Filter the aggregates with a predicate over the source rows.
    pub fn with_aggregate_filter(mut self, filter: impl Into<String>) -> Self {
        self.aggregate_filter = Some(filter.into());
        self
    }

    /// Also materialize `AVG`.
    pub fn with_average(mut self, average: bool) -> Self {
        self.average = average;
        self
    }

    /// Materialize `GROUPING(key)` columns.
    pub fn with_grouping_columns(
        mut self,
        grouping_columns: Vec<GroupingColumn>,
    ) -> Self {
        self.grouping_columns = grouping_columns;
        self
    }

    /// Recompute a general aggregate list per grouping set instead of the
    /// incremental SUM/COUNT/AVG layout.
    pub fn with_aggregates(
        mut self,
        aggregates: Vec<(String, String, DataType)>,
    ) -> Self {
        self.aggregates = aggregates;
        self
    }

    /// Only rows matching `filter` contribute to the view.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Only groups matching `having` stay in the view.
    pub fn with_having(mut self, having: impl Into<String>) -> Self {
        self.having = Some(having.into());
        self
    }

    fn to_spec(&self) -> Result<ViewSpec> {
        Ok(ViewSpec::GroupingSets {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            groupings: self.groupings.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            count_column: self.count_column.clone(),
            aggregate_filter: self.aggregate_filter.clone(),
            average: self.average,
            grouping_columns: self.grouping_columns.clone(),
            aggregates: self
                .aggregates
                .iter()
                .map(|(call, column, data_type)| {
                    Ok(MultiAggSpec {
                        call: call.clone(),
                        column: column.clone(),
                        result: encode_data_type(data_type)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            filter: self.filter.clone(),
            having: self.having.clone(),
        })
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

/// The schema of a `SUM`/`COUNT` view with optional group and value
/// expressions.
#[allow(clippy::too_many_arguments)]
pub fn sum_count_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
    average: bool,
) -> Result<SchemaRef> {
    let key_fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    let sum_type = match (value_expr, value_column) {
        (Some(value_expr), _) => {
            let (data_type, _) = expression_type(source_schema, value_expr)?;
            if average {
                avg_result_type(&data_type)?;
            }
            sum_result_type(&data_type)?
        }
        (None, Some(column)) => {
            let value_type = field_type(source_schema, column)?;
            if average {
                avg_result_type(&value_type)?;
            }
            sum_result_type(&value_type)?
        }
        (None, None) => DataType::Int64,
    };
    sum_count_schema_for(key_fields, sum_type, average)
}

/// The schema of a [`GroupingSetsView`] materialized view: the grouping index,
/// every flat key (forced nullable, because a set that does not group by a key
/// materializes NULL there) and the SUM/COUNT/AVG columns.
pub fn grouping_sets_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
    average: bool,
    grouping_columns: &[GroupingColumn],
    aggregates: &[(String, DataType)],
) -> Result<SchemaRef> {
    let mut fields: Vec<arrow_schema::FieldRef> = vec![Arc::new(Field::new(
        IVM_GROUPING_COLUMN,
        DataType::Int64,
        false,
    ))];
    if group_exprs.is_empty() {
        for key in group_keys {
            let field = source_schema.field_with_name(key)?;
            fields.push(Arc::new(Field::new(
                key.clone(),
                field.data_type().clone(),
                true,
            )));
        }
    } else {
        if group_exprs.len() != group_keys.len() {
            return Err(report!(
                "grouping sets view: group_keys and group_exprs are not parallel"
            ));
        }
        for (key, expression) in group_keys.iter().zip(group_exprs) {
            let (data_type, _) = expression_type(source_schema, expression)?;
            fields.push(Arc::new(Field::new(key.clone(), data_type, true)));
        }
    }
    if !aggregates.is_empty() {
        let mut fields: Vec<arrow_schema::FieldRef> = vec![Arc::new(Field::new(
            IVM_GROUPING_COLUMN,
            DataType::Int64,
            false,
        ))];
        for (index, key) in group_keys.iter().enumerate() {
            let data_type = if group_exprs.is_empty() {
                field_type(source_schema, key)?
            } else {
                let expression = group_exprs.get(index).ok_or_else(|| {
                    report!(
                        "grouping sets view: group_keys and group_exprs are not parallel"
                    )
                })?;
                expression_type(source_schema, expression)?.0
            };
            fields.push(Arc::new(Field::new(key.clone(), data_type, true)));
        }
        for (column, data_type) in aggregates {
            fields.push(Arc::new(Field::new(
                column.clone(),
                data_type.clone(),
                true,
            )));
        }
        for grouping in grouping_columns {
            fields.push(Arc::new(Field::new(
                grouping.name.clone(),
                DataType::Int32,
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
        return Ok(Arc::new(Schema::new(fields)));
    }
    let sum_type = match (value_expr, value_column) {
        (Some(value_expr), _) => {
            let (data_type, _) = expression_type(source_schema, value_expr)?;
            if average {
                avg_result_type(&data_type)?;
            }
            sum_result_type(&data_type)?
        }
        (None, Some(column)) => {
            let value_type = field_type(source_schema, column)?;
            if average {
                avg_result_type(&value_type)?;
            }
            sum_result_type(&value_type)?
        }
        (None, None) => DataType::Int64,
    };
    let schema = sum_count_schema_for(fields, sum_type, average)?;
    if grouping_columns.is_empty() {
        return Ok(schema);
    }
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    let position = fields.len() - 2;
    for (index, grouping) in grouping_columns.iter().enumerate() {
        fields.insert(
            position + index,
            Arc::new(Field::new(grouping.name.clone(), DataType::Int32, false)),
        );
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

pub fn sum_count_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: Option<&str>,
) -> Result<SchemaRef> {
    let sum_type = match value_column {
        Some(column) => sum_result_type(&field_type(source_schema, column)?)?,
        None => DataType::Int64,
    };
    sum_count_schema_for(key_fields(source_schema, group_keys)?, sum_type, false)
}

/// The schema of a `SUM`/`COUNT` view whose value is a rendered expression
/// (`SUM(v * 2)`).
pub fn sum_expr_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_expr: &str,
    average: bool,
) -> Result<SchemaRef> {
    let (data_type, _) = expression_type(source_schema, value_expr)?;
    let sum_type = sum_result_type(&data_type)?;
    if average {
        avg_result_type(&data_type)?;
    }
    sum_count_schema_for(key_fields(source_schema, group_keys)?, sum_type, average)
}

/// The schema of a `SUM`/`COUNT` (`average` adds the AVG column) view given
/// the summed type.
fn sum_count_schema_for(
    mut fields: Vec<arrow_schema::FieldRef>,
    sum_type: DataType,
    average: bool,
) -> Result<SchemaRef> {
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
    if average {
        fields.push(Arc::new(Field::new(
            IVM_AVG_COLUMN,
            DataType::Float64,
            true,
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

/// The schema of an `AVG` materialized view: the sum/count machinery columns
/// (`sum_v`, `count_v`, the non-NULL count) plus the average
/// ([`IVM_AVG_COLUMN`]).
pub fn avg_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    let value_type = field_type(source_schema, value_column)?;
    avg_result_type(&value_type)?;
    let sum_type = sum_result_type(&value_type)?;
    sum_count_schema_for(key_fields(source_schema, group_keys)?, sum_type, true)
}

/// The schema of a value-count state table, deriving the key and value types
/// from the source schema.
pub fn value_count_state_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    let value_field = source_schema.field_with_name(value_column)?;
    value_count_state_with_value(
        source_schema,
        group_keys,
        &[],
        value_field.data_type().clone(),
        value_field.is_nullable(),
    )
}

/// The value-count state schema of a view with group expressions and an
/// optional value column/expression.
pub fn value_count_groups_state_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
) -> Result<SchemaRef> {
    let (value_type, value_nullable) = match (value_expr, value_column) {
        (Some(expression), _) => expression_type(source_schema, expression)?,
        (None, Some(column)) => {
            let field = source_schema.field_with_name(column)?;
            (field.data_type().clone(), field.is_nullable())
        }
        (None, None) => {
            return Err(report!("the value-count view has no value"));
        }
    };
    value_count_state_with_value(
        source_schema,
        group_keys,
        group_exprs,
        value_type,
        value_nullable,
    )
}

/// The value-count state schema of a view whose value is a rendered
/// expression (`MIN(v * 2)`).
pub fn value_count_state_expr_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_expr: &str,
) -> Result<SchemaRef> {
    let (data_type, nullable) = expression_type(source_schema, value_expr)?;
    value_count_state_with_value(source_schema, group_keys, &[], data_type, nullable)
}

/// The value-count state schema given the value type.
fn value_count_state_with_value(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_type: DataType,
    value_nullable: bool,
) -> Result<SchemaRef> {
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(
        IVM_VALUE_COLUMN,
        value_type,
        value_nullable,
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
    value_count_mv_with_value(source_schema, group_keys, &[], value_type, value_nullable)
}

/// The value-count materialized view schema given the value type and group
/// expressions.
fn value_count_mv_with_value(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_type: DataType,
    value_nullable: bool,
) -> Result<SchemaRef> {
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
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

/// The schema of a [`MinMaxView`] materialized view whose value is a rendered
/// expression.
pub fn min_max_expr_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_expr: &str,
    kind: MinMaxKind,
) -> Result<SchemaRef> {
    let _ = kind;
    let (value_type, _) = expression_type(source_schema, value_expr)?;
    value_count_mv_with_value(source_schema, group_keys, &[], value_type, true)
}

/// The schema of a [`MinMaxView`] materialized view with group expressions and
/// an optional value column/expression.
pub fn min_max_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
    kind: MinMaxKind,
) -> Result<SchemaRef> {
    let (value_type, value_nullable) = match (value_expr, value_column) {
        (Some(expression), _) => {
            let (data_type, _) = expression_type(source_schema, expression)?;
            (data_type, true)
        }
        (None, Some(column)) => {
            let field = source_schema.field_with_name(column)?;
            (field.data_type().clone(), true)
        }
        (None, None) => {
            return Err(report!("the min/max view has no value"));
        }
    };
    let _ = kind;
    value_count_mv_with_value(
        source_schema,
        group_keys,
        group_exprs,
        value_type,
        value_nullable,
    )
}

/// The schema of a [`DistinctAggView`] materialized view with group
/// expressions.
pub fn distinct_agg_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: &str,
    kind: DistinctAggKind,
) -> Result<SchemaRef> {
    // A distinct count is an integer, a distinct sum keeps the numeric type.
    let (value_type, value_nullable) = match kind {
        DistinctAggKind::Count => (DataType::Int64, false),
        DistinctAggKind::Sum => (
            sum_result_type(&field_type(source_schema, value_column)?)?,
            true,
        ),
    };
    value_count_mv_with_value(
        source_schema,
        group_keys,
        group_exprs,
        value_type,
        value_nullable,
    )
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

/// The rendered value of a value-count view: a quoted column or a stored
/// expression.
/// The recompute SQL of a global `MIN`/`MAX`/`DISTINCT` view: one row over
/// the whole current source.
fn value_count_global_sql(view: &ValueCountView<'_>, epoch: i64) -> String {
    let value = value_count_value_sql(view);
    let aggregate = match view.agg {
        ValueAgg::Min => format!("min({value})"),
        ValueAgg::Max => format!("max({value})"),
        ValueAgg::DistinctCount => format!("count(distinct {value})"),
        ValueAgg::DistinctSum => format!("sum(distinct {value})"),
    };
    let src_where = format!(
        "{}{}",
        source_delete_filter("src", change_column(view.source)),
        filter_clause(view.filter),
    );
    let row = format!(
        "select {aggregate} as {}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from src where {src_where}",
        quote_ident(IVM_VALUE_COLUMN),
    );
    match view.having {
        Some(having) => format!("select * from ({row}) t where {having}"),
        None => row,
    }
}

fn value_count_value_sql(view: &ValueCountView<'_>) -> String {
    view.value_expr
        .map(str::to_string)
        .unwrap_or_else(|| quote_ident(view.value_column.unwrap_or_default()))
}

/// The value type of a value-count view: the source column type or the
/// planned expression type.
fn value_count_value_type(view: &ValueCountView<'_>) -> Result<DataType> {
    match (view.value_expr, view.value_column) {
        (Some(expression), _) => Ok(expression_type(&view.source.schema, expression)?.0),
        (None, Some(column)) => field_type(&view.source.schema, column),
        (None, None) => Err(report!("value-count view {} has no value", view.view_id)),
    }
}

/// The rendered SQL of the summed value of a `SUM`/`COUNT` view: a quoted
/// column or a stored expression.
fn sum_count_value_sql(view: &SumCountView) -> Option<String> {
    match (&view.value_expr, &view.value_column) {
        (Some(expression), _) => Some(expression.clone()),
        (None, Some(column)) => Some(quote_ident(column)),
        (None, None) => None,
    }
}

/// Validate a `SUM`/`COUNT` view: the group keys, the value/count columns and
/// the `HAVING` predicate.
fn validate_sum_count_view(view: &SumCountView) -> Result<()> {
    // An empty key list is a global aggregate over the whole source.
    if view.group_exprs.is_empty() && !view.group_keys.is_empty() {
        validate_group_keys(&view.source, &view.group_keys, &view.view_id)?;
    }
    group_key_fields(&view.source.schema, &view.group_keys, &view.group_exprs)?;
    validate_having(&view.view_id, &view.mv.schema, view.having.as_deref())?;
    if let Some(value_expr) = &view.value_expr {
        if view.value_column.is_some() {
            return Err(report!(
                "SUM/COUNT view {}: a value column and a value expression are mutually exclusive",
                view.view_id
            ));
        }
        if view.count_column.is_some() {
            return Err(report!(
                "SUM/COUNT view {}: COUNT(column) cannot be combined with an aggregated expression",
                view.view_id
            ));
        }
        let (data_type, _) = expression_type(&view.source.schema, value_expr)?;
        sum_result_type(&data_type)?;
        if view.average {
            avg_result_type(&data_type)?;
        }
    } else if let Some(value_column) = &view.value_column {
        sum_result_type(&field_type(&view.source.schema, value_column)?)?;
    }
    if let Some(aggregate_filter) = &view.aggregate_filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, aggregate_filter)?;
    }
    if let Some(count_column) = &view.count_column {
        field_type(&view.source.schema, count_column)?;
        if let Some(value_column) = &view.value_column
            && value_column != count_column
        {
            return Err(report!(
                "SUM/COUNT view {}: SUM and COUNT must use the same value column",
                view.view_id
            ));
        }
    }
    if view.average && view.value_expr.is_none() {
        let value_column = view
            .value_column
            .as_deref()
            .ok_or_else(|| report!("AVG view {} needs a value column", view.view_id))?;
        avg_result_type(&field_type(&view.source.schema, value_column)?)?;
    }
    Ok(())
}

/// SQL for one `SUM`/`COUNT` refresh window: a `delete` part (the previous
/// accumulator of changed groups) unioned with an `insert` part (the new
/// accumulator). Keys already written for this epoch are skipped.
fn sum_count_refresh_sql(view: &SumCountView, keyed: bool, epoch: i64) -> String {
    let keys = quoted_list(&view.group_keys);
    let aggregate_filter = view
        .aggregate_filter
        .as_deref()
        .map(|filter| format!(" filter (where {filter})"))
        .unwrap_or_default();
    let value = sum_count_value_sql(view);
    let sum_expr = match &value {
        Some(value) => format!("sum({value}){aggregate_filter}"),
        None => "sum(0)".to_string(),
    };
    let nonnull_expr = match view
        .count_column
        .as_deref()
        .map(quote_ident)
        .or_else(|| value.clone())
    {
        Some(value) => format!("count({value}){aggregate_filter}"),
        None => "count(1)".to_string(),
    };
    if let Some(having) = view.having.as_deref() {
        // The HAVING predicate may have kept groups out of the MV, so the
        // delta alone cannot produce their totals: recompute the affected
        // groups from the (pruned) current source.
        let (group_sum, group_count, group_nonnull) = if keyed {
            (
                sum_expr.clone(),
                "count(1)".to_string(),
                nonnull_expr.clone(),
            )
        } else {
            let (signed_sum, signed_count, signed_nonnull) = signed_delta_exprs(
                "src",
                value.as_deref(),
                view.count_column.as_deref(),
                view.aggregate_filter.as_deref(),
                change_column(&view.source),
            );
            (signed_sum, signed_count, signed_nonnull)
        };
        let src_from = if keyed {
            format!(
                "src where {}{}",
                source_delete_filter("src", change_column(&view.source)),
                filter_clause(view.filter.as_deref()),
            )
        } else {
            // Append-only changelogs keep their markers; the signed
            // aggregation above turns them into retractions.
            format!("src{}", filter_where(view.filter.as_deref()))
        };
        let active_match = key_join_condition_null_safe("a", "s", &view.group_keys);
        let already_match = key_join_condition_null_safe("a", "p", &view.group_keys);
        let avg = quote_ident(IVM_AVG_COLUMN);
        let avg_group = if view.average {
            format!(
                ", case when {group_nonnull} > 0 then cast({group_sum} as double) \
                     / cast({group_nonnull} as double) else null end as {avg}"
            )
        } else {
            String::new()
        };
        let avg_col = if view.average {
            format!(", {avg}")
        } else {
            String::new()
        };
        let sql = format!(
            "with affected as ({affected}), \
             group_now as (select {keys}, {group_sum} as sum_v, {group_count} as count_v, \
                                  {group_nonnull} as {nonnull_c}{avg_group} \
                           from {src_from} group by {keys}), \
             already as (select distinct {keys} from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
             active as (select * from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
             deletes as (select {keys}, sum_v, count_v, {nonnull_c}{avg_col}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                         from active s \
                         where exists (select 1 from affected a where {active_match})), \
             inserts as (select {keys}, sum_v, count_v, {nonnull_c}{avg_col}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                         from group_now p \
                         where count_v <> 0 and ({having}) \
                           and not exists (select 1 from already a where {already_match})) \
             select * from deletes union all select * from inserts \
             order by {keys}, \"rowKinds\"",
            affected = affected_groups_sql(
                &view.source,
                &view.group_keys,
                keyed,
                view.filter.as_deref(),
            ),
            nonnull_c = quote_ident(IVM_NONNULL_COUNT_COLUMN),
        );
        return sql;
    }
    let delta_filter = format!(
        "{}{}",
        source_delete_filter("delta", change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    let old_filter = format!(
        "{}{}",
        source_delete_filter("o", change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    let plain_where = filter_where(view.filter.as_deref());
    let pk_match = key_join_condition("o", "p", &view.source.primary_keys);
    let delta_part = if keyed {
        // The delta is merged per primary key, so only the surviving version
        // contributes; the previous value is retracted through the as-of read.
        format!(
            "new_agg as (select {keys}, {sum_expr} as dsum, count(1) as dcount, \
                            {nonnull_expr} as dnonnull \
                     from delta where {delta_filter} group by {keys})"
        )
    } else {
        // Without a merge key the delta keeps every marker, so retractions
        // (`delete`) subtract their contribution.
        let (signed_sum, signed_count, signed_nonnull) = signed_delta_exprs(
            "delta",
            value.as_deref(),
            view.count_column.as_deref(),
            view.aggregate_filter.as_deref(),
            change_column(&view.source),
        );
        format!(
            "new_agg as (select {keys}, {signed_sum} as dsum, {signed_count} as dcount, \
                            {signed_nonnull} as dnonnull \
                     from delta{plain_where} group by {keys})"
        )
    };
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
    let avg = quote_ident(IVM_AVG_COLUMN);
    let avg_new = if view.average {
        format!(
            ", case when n_nonnull > 0 \
                   then cast(coalesce(sum_v + dsum, sum_v, dsum) as double) \
                        / cast(n_nonnull as double) else null end as {avg}"
        )
    } else {
        String::new()
    };
    let avg_delete = if view.average {
        format!(
            ", case when s_nonnull > 0 then cast(sum_v as double) \
                   / cast(s_nonnull as double) else null end as {avg}"
        )
    } else {
        String::new()
    };
    let avg_col = if view.average {
        format!(", {avg}")
    } else {
        String::new()
    };
    format!(
        "with delta_pks as (select distinct {pks} from delta), \
         {delta_part}, {d2}, \
         already as (select distinct {keys} from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         merged as (select {coalesced_keys}, s.sum_v, s.count_v, s.{nonnull_c} as s_nonnull, \
                           s.\"__ivm_epoch\" as s_epoch, d2.dsum, d2.dcount, d2.dnonnull, \
                           coalesce(s.{nonnull_c} + d2.dnonnull, s.{nonnull_c}, d2.dnonnull) as n_nonnull \
                    from active s full join d2 on {active_match}), \
         new_values as (select {keys}, \
                               case when n_nonnull > 0 then coalesce(sum_v + dsum, sum_v, dsum) else null end as sum_v, \
                               coalesce(count_v + dcount, count_v, dcount) as count_v, \
                               n_nonnull as {nonnull_c}{avg_new} \
                        from merged where dcount is not null), \
         deletes as (select {keys}, sum_v, count_v, s_nonnull as {nonnull_c}{avg_delete}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from merged where s_epoch is not null and dcount is not null), \
         inserts as (select {keys}, sum_v, count_v, {nonnull_c}{avg_col}, \
                            'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from new_values \
                     where count_v <> 0 \
                       and not exists (select 1 from already a where {already_match})) \
         select * from deletes union all select * from inserts \
         order by {keys}, \"rowKinds\"",
        pks = quoted_list(&view.source.primary_keys),
        nonnull_c = quote_ident(IVM_NONNULL_COUNT_COLUMN),
    )
}

/// SQL for a full `SUM`/`COUNT` rebuild.
fn sum_count_rebuild_sql(view: &SumCountView, keyed: bool, epoch: i64) -> String {
    if !keyed {
        // Append-only CDC sources keep every marker in `src`, so the rebuild
        // aggregates them with their sign and keeps the net-positive groups.
        let key_select = key_select(&view.group_keys);
        let group_by = group_by_clause(&view.group_keys);
        let value = sum_count_value_sql(view);
        let (signed_sum, signed_count, signed_nonnull) = signed_delta_exprs(
            "src",
            value.as_deref(),
            view.count_column.as_deref(),
            view.aggregate_filter.as_deref(),
            change_column(&view.source),
        );
        let plain_where = filter_where(view.filter.as_deref());
        let avg = quote_ident(IVM_AVG_COLUMN);
        let avg_rebuild = if view.average {
            format!(
                ", case when nonnull > 0 then cast(dsum as double) \
                       / cast(nonnull as double) else null end as {avg}"
            )
        } else {
            String::new()
        };
        let rebuild = format!(
            "select {key_select}case when nonnull > 0 then dsum else null end as {}, \
                    dcount as {}, nonnull as {}{avg_rebuild}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
             from (select {key_select}{signed_sum} as dsum, {signed_count} as dcount, \
                          {signed_nonnull} as nonnull \
                   from src{plain_where}{group_by}) \
             where dcount > 0",
            quote_ident(IVM_SUM_COLUMN),
            quote_ident(IVM_COUNT_COLUMN),
            quote_ident(IVM_NONNULL_COUNT_COLUMN),
        );
        return match &view.having {
            Some(having) => format!("select * from ({rebuild}) t where {having}"),
            None => rebuild,
        };
    }
    let aggregate_filter = view
        .aggregate_filter
        .as_deref()
        .map(|filter| format!(" filter (where {filter})"))
        .unwrap_or_default();
    let key_select = key_select(&view.group_keys);
    let group_by = group_by_clause(&view.group_keys);
    let value = sum_count_value_sql(view);
    let sum_expr = match &value {
        Some(value) => format!("sum({value}){aggregate_filter}"),
        None => "sum(0)".to_string(),
    };
    let nonnull_expr = match view
        .count_column
        .as_deref()
        .map(quote_ident)
        .or_else(|| value.clone())
    {
        Some(value) => format!("count({value}){aggregate_filter}"),
        None => "count(1)".to_string(),
    };
    let src_where = format!(
        "{}{}",
        source_delete_filter("src", change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    let avg = quote_ident(IVM_AVG_COLUMN);
    let avg_rebuild = if view.average {
        format!(
            ", case when {nonnull_expr} > 0 then cast({sum_expr} as double) \
                   / cast({nonnull_expr} as double) else null end as {avg}"
        )
    } else {
        String::new()
    };
    let rebuild = format!(
        "select {key_select}{sum_expr} as {}, count(1) as {}, {nonnull_expr} as {}{avg_rebuild}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from src where {src_where}{group_by}",
        quote_ident(IVM_SUM_COLUMN),
        quote_ident(IVM_COUNT_COLUMN),
        quote_ident(IVM_NONNULL_COUNT_COLUMN),
    );
    match &view.having {
        Some(having) => format!("select * from ({rebuild}) t where {having}"),
        None => rebuild,
    }
}

/// The CTE chain of one value-count refresh window. It defines `counts`
/// (`(group, value) -> dcount`), `state_del` / `state_ins` (the state rows to
/// write) and `merged` (the post-window state with `n_count`).
fn value_count_refresh_cte(view: &ValueCountView<'_>, keyed: bool, epoch: i64) -> String {
    let keys = quoted_list(view.group_keys);
    let source_value = value_count_value_sql(view);
    let state_value = quote_ident(IVM_VALUE_COLUMN);
    let delta_filter = format!(
        "{}{}",
        source_delete_filter("delta", change_column(view.source)),
        filter_clause(view.filter),
    );
    let old_filter = format!(
        "{}{}",
        source_delete_filter("o", change_column(view.source)),
        filter_clause(view.filter),
    );
    let plain_where = filter_where(view.filter);
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
    // The append-only new_agg must sign its rows instead of filtering the
    // retractions out, so update pairs and deletes move the counts.
    let new_agg = if keyed {
        format!(
            "new_agg as (select {new_select}, count(1) as dcount from delta \
                          where {delta_filter} group by {new_group})"
        )
    } else {
        let retract = source_retract_condition("delta", change_column(view.source));
        format!(
            "new_agg as (select {new_select}, \
                                sum(case when {retract} then -1 else 1 end) as dcount \
                         from delta{plain_where} group by {new_group})"
        )
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
        "with {new_agg}, \
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
                    from agg where not exists (select 1 from already a where {agg_match}){having}) \
         select * from mv_del union all select * from mv_ins order by {keys}, \"rowKinds\"",
        agg = view.agg.sql(IVM_VALUE_COLUMN),
        value_count = quote_ident(IVM_VALUE_COUNT_COLUMN),
        having = view
            .having
            .map(|having| format!(" and ({having})"))
            .unwrap_or_default(),
    )
}

/// The materialized columns of a grouping-sets MV, in schema order.
fn grouping_sets_columns(view: &GroupingSetsView) -> Vec<String> {
    let mut columns = vec![IVM_GROUPING_COLUMN.to_string()];
    columns.extend(view.group_keys.iter().cloned());
    if view.aggregates.is_empty() {
        columns.push(IVM_SUM_COLUMN.to_string());
        columns.push(IVM_COUNT_COLUMN.to_string());
        columns.push(IVM_NONNULL_COUNT_COLUMN.to_string());
        if view.average {
            columns.push(IVM_AVG_COLUMN.to_string());
        }
    } else {
        columns.extend(view.aggregates.iter().map(|(_, column, _)| column.clone()));
    }
    for grouping in &view.grouping_columns {
        columns.push(grouping.name.clone());
    }
    columns
}

/// The rendered `GROUPING(key)` constants of one grouping set.
fn grouping_set_constants(
    view: &GroupingSetsView,
    index: usize,
) -> Vec<(String, String)> {
    let set = &view.groupings[index];
    view.grouping_columns
        .iter()
        .map(|grouping| {
            let value = if set.contains(&grouping.key) { 0 } else { 1 };
            (grouping.name.clone(), value.to_string())
        })
        .collect()
}

/// The rendered aggregate expressions of a grouping-sets view.
fn grouping_sets_aggregates(view: &GroupingSetsView) -> (String, String, String) {
    let value = match (&view.value_expr, &view.value_column) {
        (Some(expression), _) => Some(expression.clone()),
        (None, Some(column)) => Some(quote_ident(column)),
        (None, None) => None,
    };
    let aggregate_filter = view
        .aggregate_filter
        .as_deref()
        .map(|filter| format!(" filter (where {filter})"))
        .unwrap_or_default();
    let sum_expr = match &value {
        Some(value) => format!("sum({value}){aggregate_filter}"),
        None => "sum(0)".to_string(),
    };
    let nonnull_expr = match view
        .count_column
        .as_deref()
        .map(quote_ident)
        .or_else(|| value.clone())
    {
        Some(value) => format!("count({value}){aggregate_filter}"),
        None => "count(1)".to_string(),
    };
    let avg_expr = format!(
        "case when {nonnull_expr} > 0 then cast({sum_expr} as double) \
               / cast({nonnull_expr} as double) else null end"
    );
    (sum_expr, nonnull_expr, avg_expr)
}

/// One grouping set's aggregate rows over the current `src`.
fn grouping_sets_group_now(
    view: &GroupingSetsView,
    index: usize,
    src_where: &str,
) -> String {
    let set = &view.groupings[index];
    let keys = set
        .iter()
        .map(|key| view.group_keys[*key].clone())
        .collect::<Vec<_>>();
    let mut select = vec![format!("{index} as {}", quote_ident(IVM_GROUPING_COLUMN))];
    for key in &view.group_keys {
        if keys.contains(key) {
            select.push(quote_ident(key));
        } else {
            select.push(format!("NULL as {}", quote_ident(key)));
        }
    }
    if view.aggregates.is_empty() {
        let (sum_expr, nonnull_expr, avg_expr) = grouping_sets_aggregates(view);
        select.push(format!("{sum_expr} as {}", quote_ident(IVM_SUM_COLUMN)));
        select.push(format!("count(1) as {}", quote_ident(IVM_COUNT_COLUMN)));
        select.push(format!(
            "{nonnull_expr} as {}",
            quote_ident(IVM_NONNULL_COUNT_COLUMN)
        ));
        if view.average {
            select.push(format!("{avg_expr} as {}", quote_ident(IVM_AVG_COLUMN)));
        }
    } else {
        for (call, column, _) in &view.aggregates {
            select.push(format!("{call} as {}", quote_ident(column)));
        }
    }
    for (name, value) in grouping_set_constants(view, index) {
        select.push(format!("cast({value} as int) as {}", quote_ident(&name)));
    }
    let affected_filter = if keys.is_empty() {
        " and exists (select 1 from affected)".to_string()
    } else {
        format!(
            " and exists (select 1 from affected a where {})",
            key_join_condition_null_safe("a", "src", &keys)
        )
    };
    let group_now = format!(
        "select {} from src where {src_where}{affected_filter}{}",
        select.join(", "),
        group_by_clause(&keys)
    );
    match view.having.as_deref() {
        Some(having) => format!("select * from ({group_now}) t where ({having})"),
        None => group_now,
    }
}

/// One grouping set's current MV rows (matched by its grouped keys).
fn grouping_sets_delete(view: &GroupingSetsView, index: usize, epoch: i64) -> String {
    let set = &view.groupings[index];
    let keys = set
        .iter()
        .map(|key| view.group_keys[*key].clone())
        .collect::<Vec<_>>();
    let affected = if keys.is_empty() {
        "exists (select 1 from affected)".to_string()
    } else {
        format!(
            "exists (select 1 from affected a where {})",
            key_join_condition_null_safe("a", "mv", &keys)
        )
    };
    let columns = quoted_list(&grouping_sets_columns(view));
    format!(
        "select {columns}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from mv where \"rowKinds\" = 'insert' and \"{epoch_column}\" <> {epoch} \
           and {grouping} = {index} and {affected}",
        epoch_column = IVM_EPOCH_COLUMN,
        grouping = quote_ident(IVM_GROUPING_COLUMN),
    )
}

/// One grouping set's recomputed rows, skipping the current epoch's writes.
fn grouping_sets_insert(
    view: &GroupingSetsView,
    index: usize,
    src_where: &str,
    epoch: i64,
) -> String {
    let set = &view.groupings[index];
    let keys = set
        .iter()
        .map(|key| view.group_keys[*key].clone())
        .collect::<Vec<_>>();
    let already = if keys.is_empty() {
        "true".to_string()
    } else {
        key_join_condition_null_safe("m", "recomputed", &keys)
    };
    let group_now = grouping_sets_group_now(view, index, src_where);
    let columns = quoted_list(&grouping_sets_columns(view));
    format!(
        "select {columns}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from ({group_now}) recomputed \
         where not exists (select 1 from mv m where m.\"rowKinds\" = 'insert' \
             and m.\"{epoch_column}\" = {epoch} and m.{grouping} = {index} and ({already}))",
        epoch_column = IVM_EPOCH_COLUMN,
        grouping = quote_ident(IVM_GROUPING_COLUMN),
    )
}

/// The refresh SQL of a grouping-sets view: the affected group tuples drive
/// every set, which recomputes and rewrites its rows.
fn grouping_sets_refresh_sql(view: &GroupingSetsView, epoch: i64) -> String {
    let keyed = !view.source.primary_keys.is_empty();
    let affected = affected_groups_sql(
        &view.source,
        &view.group_keys,
        keyed,
        view.filter.as_deref(),
    );
    let src_where = format!(
        "{}{}",
        source_delete_filter("src", change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    let mut branches = Vec::new();
    for index in 0..view.groupings.len() {
        branches.push(grouping_sets_delete(view, index, epoch));
    }
    for index in 0..view.groupings.len() {
        branches.push(grouping_sets_insert(view, index, &src_where, epoch));
    }
    // Deletes must come before inserts of the same key, so the upsert keeps
    // the recomputed row (like the SUM/COUNT path).
    let mut order_keys = vec![IVM_GROUPING_COLUMN.to_string()];
    order_keys.extend(view.group_keys.iter().cloned());
    let order = quoted_list(&order_keys);
    format!(
        "with affected as ({affected}), combined as ({}) \
         select * from combined order by {order}, \"rowKinds\"",
        branches.join(" union all ")
    )
}

/// The rebuild SQL of a grouping-sets view: every set is recomputed over the
/// full source.
fn grouping_sets_rebuild_sql(view: &GroupingSetsView, epoch: i64) -> String {
    let src_where = format!(
        "{}{}",
        source_delete_filter("src", change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    let columns = quoted_list(&grouping_sets_columns(view));
    let legacy = view.aggregates.is_empty();
    let (sum_expr, nonnull_expr, avg_expr) = grouping_sets_aggregates(view);
    let mut branches = Vec::new();
    for (index, set) in view.groupings.iter().enumerate() {
        let keys = set
            .iter()
            .map(|key| view.group_keys[*key].clone())
            .collect::<Vec<_>>();
        let mut select = vec![format!("{index} as {}", quote_ident(IVM_GROUPING_COLUMN))];
        for key in &view.group_keys {
            if keys.contains(key) {
                select.push(quote_ident(key));
            } else {
                select.push(format!("NULL as {}", quote_ident(key)));
            }
        }
        if legacy {
            select.push(format!("{sum_expr} as {}", quote_ident(IVM_SUM_COLUMN)));
            select.push(format!("count(1) as {}", quote_ident(IVM_COUNT_COLUMN)));
            select.push(format!(
                "{nonnull_expr} as {}",
                quote_ident(IVM_NONNULL_COUNT_COLUMN)
            ));
            if view.average {
                select.push(format!("{avg_expr} as {}", quote_ident(IVM_AVG_COLUMN)));
            }
        } else {
            for (call, column, _) in &view.aggregates {
                select.push(format!("{call} as {}", quote_ident(column)));
            }
        }
        for (name, value) in grouping_set_constants(view, index) {
            select.push(format!("cast({value} as int) as {}", quote_ident(&name)));
        }
        let rows = format!(
            "select {} from src where {src_where}{}",
            select.join(", "),
            group_by_clause(&keys)
        );
        branches.push(match view.having.as_deref() {
            Some(having) => format!("select * from ({rows}) t where ({having})"),
            None => rows,
        });
    }
    format!(
        "with grouped as ({}) \
         select {columns}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" from grouped",
        branches.join(" union all "),
    )
}

impl IvmRuntime {
    /// Persist a sum/count view spec (idempotent).
    pub async fn register_view(&self, view: &SumCountView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
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
        validate_sum_count_view(view)?;

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
        let mut commit_ids = Vec::new();

        if view.group_keys.is_empty() {
            // A global aggregate has no groups to prune and its MV is a
            // single row without a primary key, so the row is recomputed over
            // the current source and the MV is rewritten wholesale.
            let keyed = !view.source.primary_keys.is_empty();
            let context = SessionContext::new();
            register_table(
                &context,
                "src",
                view.source.read_current(&self.client).await?,
                &view.source.schema,
            )?;
            view.mv.truncate(&self.client).await?;
            let sql = sum_count_rebuild_sql(view, keyed, epoch);
            for batch in context.sql(&sql).await?.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
            let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
            self.metadata
                .mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(&view.view_id, window.cursors).await?;
            return Ok(Some(epoch));
        }

        let context = SessionContext::new();
        let delta_batches = project_group_keys(
            &context,
            view.source.read_partition_files(window.added_files).await?,
            &view.source.schema,
            &view.group_keys,
            &view.group_exprs,
        )
        .await?;
        let keyed = !view.source.primary_keys.is_empty();
        // The window only touches the delta groups (plus the previous groups
        // of the changed rows), so the old state and the MV are read pruned to
        // those keys; the reader also prunes their buckets.
        let delta_context = SessionContext::new();
        register_table(
            &delta_context,
            "delta",
            delta_batches.clone(),
            &view.source.schema,
        )?;
        let old_batches = if keyed {
            let pk_filters = key_filters(&view.source.primary_keys, &delta_batches)?;
            let batches = project_group_keys(
                &context,
                view.source
                    .read_before_window_filtered(
                        &self.client,
                        &window.before_versions,
                        pk_filters,
                    )
                    .await?,
                &view.source.schema,
                &view.group_keys,
                &view.group_exprs,
            )
            .await?;
            register_table(&delta_context, "old", batches.clone(), &view.source.schema)?;
            batches
        } else {
            Vec::new()
        };
        let groups = delta_context
            .sql(&affected_groups_sql(
                &view.source,
                &view.group_keys,
                keyed,
                view.filter.as_deref(),
            ))
            .await?
            .collect()
            .await?;
        // A computed group key cannot prune the source reads.
        let filters = if view.group_exprs.is_empty() {
            key_filters(&view.group_keys, &groups)?
        } else {
            Vec::new()
        };

        register_table(&context, "delta", delta_batches, &view.source.schema)?;
        if keyed {
            register_table(&context, "old", old_batches, &view.source.schema)?;
        }
        if view.having.is_some() {
            let batches = project_group_keys(
                &context,
                view.source
                    .read_current_filtered(&self.client, filters.clone())
                    .await?,
                &view.source.schema,
                &view.group_keys,
                &view.group_exprs,
            )
            .await?;
            register_table(&context, "src", batches, &view.source.schema)?;
        }
        register_table(
            &context,
            "mv",
            view.mv.read_current_filtered(&self.client, filters).await?,
            &view.mv.schema,
        )?;
        let sql = sum_count_refresh_sql(view, keyed, epoch);
        for batch in context.sql(&sql).await?.collect().await? {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

    /// Persist a grouping-sets view spec (idempotent).
    pub async fn register_grouping_sets_view(
        &self,
        view: &GroupingSetsView,
    ) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec()?)?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a grouping-sets view: the affected group tuples drive every
    /// grouping set, which recomputes and rewrites its rows.
    pub async fn refresh_grouping_sets(
        &self,
        view: &GroupingSetsView,
    ) -> Result<Option<i64>> {
        self.register_grouping_sets_view(view).await?;
        validate_grouping_sets_view(view)?;

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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        register_table(
            &context,
            "delta",
            project_group_keys(
                &context,
                view.source.read_partition_files(window.added_files).await?,
                &view.source.schema,
                &view.group_keys,
                &view.group_exprs,
            )
            .await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "old",
            project_group_keys(
                &context,
                view.source
                    .read_before_window(&self.client, &window.before_versions)
                    .await?,
                &view.source.schema,
                &view.group_keys,
                &view.group_exprs,
            )
            .await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "src",
            project_group_keys(
                &context,
                view.source.read_current(&self.client).await?,
                &view.source.schema,
                &view.group_keys,
                &view.group_exprs,
            )
            .await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "mv",
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        for batch in context
            .sql(&grouping_sets_refresh_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a grouping-sets view from the full source state.
    pub async fn rebuild_grouping_sets(&self, view: &GroupingSetsView) -> Result<i64> {
        self.register_grouping_sets_view(view).await?;
        validate_grouping_sets_view(view)?;

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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        register_table(
            &context,
            "src",
            project_group_keys(
                &context,
                baseline.batches,
                &view.source.schema,
                &view.group_keys,
                &view.group_exprs,
            )
            .await?,
            &view.source.schema,
        )?;
        for batch in context
            .sql(&grouping_sets_rebuild_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Rebuild a `SUM`/`COUNT` view from the full source state.
    pub async fn rebuild_sum_count(&self, view: &SumCountView) -> Result<i64> {
        self.register_view(view).await?;
        validate_sum_count_view(view)?;

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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let batches = project_group_keys(
            &context,
            baseline.batches,
            &view.source.schema,
            &view.group_keys,
            &view.group_exprs,
        )
        .await?;
        register_table(&context, "src", batches, &view.source.schema)?;
        let rebuild_keyed = !view.source.primary_keys.is_empty();
        for batch in context
            .sql(&sum_count_rebuild_sql(view, rebuild_keyed, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
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
            group_exprs: &view.group_exprs,
            value_column: view.value_column.as_deref(),
            value_expr: view.value_expr.as_deref(),
            filter: view.filter.as_deref(),
            having: view.having.as_deref(),
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
        // A multi-column distinct count has no signed per-value state (a row
        // contributes a tuple, not a value), so the affected groups are
        // recomputed from their current source rows.
        if view.value_columns.len() > 1 {
            let parts = view.recompute_parts();
            validate_recompute_view(&parts)?;
            return self.refresh_recomputed(&parts).await;
        }
        self.refresh_value_count(&ValueCountView {
            view_id: &view.view_id,
            source: &view.source,
            mv: &view.mv,
            state: &view.state,
            group_keys: &view.group_keys,
            group_exprs: &view.group_exprs,
            value_column: Some(&view.value_column),
            value_expr: None,
            filter: view.filter.as_deref(),
            having: view.having.as_deref(),
            agg: ValueAgg::from(view.agg),
        })
        .await
    }

    async fn refresh_value_count(
        &self,
        view: &ValueCountView<'_>,
    ) -> Result<Option<i64>> {
        // An empty key list is a global aggregate over the whole source.
        if view.group_exprs.is_empty() && !view.group_keys.is_empty() {
            validate_group_keys(view.source, view.group_keys, view.view_id)?;
        }
        group_key_fields(&view.source.schema, view.group_keys, view.group_exprs)?;
        validate_having(view.view_id, &view.mv.schema, view.having)?;
        let value_type = value_count_value_type(view)?;
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
        let mut commit_ids = Vec::new();

        if view.group_keys.is_empty() {
            // A global aggregate has no groups to prune and its MV is a
            // single row without a primary key, so the aggregate is
            // recomputed over the current source and the MV is rewritten
            // wholesale.
            let context = SessionContext::new();
            register_table(
                &context,
                "src",
                view.source.read_current(&self.client).await?,
                &view.source.schema,
            )?;
            view.mv.truncate(&self.client).await?;
            let sql = value_count_global_sql(view, epoch);
            for batch in context.sql(&sql).await?.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
            let mv_versions = output_partition_versions(&self.client, view.mv).await?;
            self.metadata
                .mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(view.view_id, window.cursors).await?;
            return Ok(Some(epoch));
        }

        let context = SessionContext::new();
        let delta_batches = project_group_keys(
            &context,
            view.source.read_partition_files(window.added_files).await?,
            &view.source.schema,
            view.group_keys,
            view.group_exprs,
        )
        .await?;
        let keyed = !view.source.primary_keys.is_empty();
        // As in the SUM/COUNT path, only the delta groups (and the previous
        // groups of the changed rows) can change, so the old state, the value
        // state and the MV are read pruned to them (buckets included).
        let delta_context = SessionContext::new();
        register_table(
            &delta_context,
            "delta",
            delta_batches.clone(),
            &view.source.schema,
        )?;
        let old_batches = if keyed {
            let pk_filters = key_filters(&view.source.primary_keys, &delta_batches)?;
            let batches = project_group_keys(
                &context,
                view.source
                    .read_before_window_filtered(
                        &self.client,
                        &window.before_versions,
                        pk_filters,
                    )
                    .await?,
                &view.source.schema,
                view.group_keys,
                view.group_exprs,
            )
            .await?;
            register_table(&delta_context, "old", batches.clone(), &view.source.schema)?;
            batches
        } else {
            Vec::new()
        };
        let groups = delta_context
            .sql(&affected_groups_sql(
                view.source,
                view.group_keys,
                keyed,
                view.filter,
            ))
            .await?
            .collect()
            .await?;
        // A computed group key cannot prune the reads.
        let filters = if view.group_exprs.is_empty() {
            key_filters(view.group_keys, &groups)?
        } else {
            Vec::new()
        };

        register_table(&context, "delta", delta_batches, &view.source.schema)?;
        if keyed {
            register_table(&context, "old", old_batches, &view.source.schema)?;
        }
        register_table(
            &context,
            "state",
            view.state
                .read_current_filtered(&self.client, filters.clone())
                .await?,
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
            &key_schema_for(&view.source.schema, view.group_keys, view.group_exprs)?,
        )?;
        register_table(
            &context,
            "state_now",
            view.state
                .read_current_filtered(&self.client, filters.clone())
                .await?,
            &view.state.schema,
        )?;
        register_table(
            &context,
            "mv",
            view.mv.read_current_filtered(&self.client, filters).await?,
            &view.mv.schema,
        )?;
        for batch in context
            .sql(&value_count_mv_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(view.view_id, window.cursors).await?;
        Ok(Some(epoch))
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
            group_exprs: &view.group_exprs,
            value_column: view.value_column.as_deref(),
            value_expr: view.value_expr.as_deref(),
            filter: view.filter.as_deref(),
            having: view.having.as_deref(),
            agg: ValueAgg::from(view.min_max),
        })
        .await
    }

    /// Rebuild a `COUNT(DISTINCT)`/`SUM(DISTINCT)` view from the full source
    /// state.
    pub async fn rebuild_distinct_agg(&self, view: &DistinctAggView) -> Result<i64> {
        self.register_distinct_agg_view(view).await?;
        if view.value_columns.len() > 1 {
            let parts = view.recompute_parts();
            validate_recompute_view(&parts)?;
            return self.rebuild_recomputed(&parts).await;
        }
        self.rebuild_value_count(&ValueCountView {
            view_id: &view.view_id,
            source: &view.source,
            mv: &view.mv,
            state: &view.state,
            group_keys: &view.group_keys,
            group_exprs: &view.group_exprs,
            value_column: Some(&view.value_column),
            value_expr: None,
            filter: view.filter.as_deref(),
            having: view.having.as_deref(),
            agg: ValueAgg::from(view.agg),
        })
        .await
    }

    /// Rebuild a value-count view.
    ///
    /// Both the value-count state table and the MV are truncated and refilled
    /// from the current source state, published as `rebuild:<generation>`.
    async fn rebuild_value_count(&self, view: &ValueCountView<'_>) -> Result<i64> {
        // An empty key list is a global aggregate over the whole source.
        if view.group_exprs.is_empty() && !view.group_keys.is_empty() {
            validate_group_keys(view.source, view.group_keys, view.view_id)?;
        }
        group_key_fields(&view.source.schema, view.group_keys, view.group_exprs)?;
        validate_having(view.view_id, &view.mv.schema, view.having)?;
        let value_type = value_count_value_type(view)?;
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
        let mut commit_ids = Vec::new();

        if view.group_keys.is_empty() {
            // A global aggregate is recomputed over the baseline into the
            // single MV row (the state table stays empty).
            let context = SessionContext::new();
            register_table(
                &context,
                "src",
                view.source.read_current(&self.client).await?,
                &view.source.schema,
            )?;
            let sql = value_count_global_sql(view, epoch);
            for batch in context.sql(&sql).await?.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
            let mv_versions = output_partition_versions(&self.client, view.mv).await?;
            self.metadata
                .mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(view.view_id, baseline.cursors).await?;
            self.metadata
                .set_view_status(view.view_id, "active")
                .await?;
            return Ok(epoch);
        }

        let context = SessionContext::new();
        let baseline_batches = project_group_keys(
            &context,
            baseline.batches,
            &view.source.schema,
            view.group_keys,
            view.group_exprs,
        )
        .await?;
        register_table(&context, "src", baseline_batches, &view.source.schema)?;
        let state_sql = if view.source.primary_keys.is_empty() {
            let retract = source_retract_condition("src", change_column(view.source));
            let plain_where = filter_where(view.filter);
            format!(
                "select {}, {} as {}, \
                        sum(case when {retract} then -1 else 1 end) as {}, \
                        'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                 from src{plain_where} group by {}, {} \
                 having sum(case when {retract} then -1 else 1 end) > 0",
                quoted_list(view.group_keys),
                value_count_value_sql(view),
                quote_ident(IVM_VALUE_COLUMN),
                quote_ident(IVM_VALUE_COUNT_COLUMN),
                quoted_list(view.group_keys),
                value_count_value_sql(view),
            )
        } else {
            let src_where = format!(
                "{}{}",
                source_delete_filter("src", change_column(view.source)),
                filter_clause(view.filter),
            );
            format!(
                "select {}, {} as {}, count(1) as {}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                 from src where {src_where} group by {}, {}",
                quoted_list(view.group_keys),
                value_count_value_sql(view),
                quote_ident(IVM_VALUE_COLUMN),
                quote_ident(IVM_VALUE_COUNT_COLUMN),
                quoted_list(view.group_keys),
                value_count_value_sql(view),
            )
        };
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
        let rebuild = format!(
            "select {}, {} as {}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
             from state_now where \"rowKinds\" = 'insert' and {} > 0 group by {}",
            quoted_list(view.group_keys),
            view.agg.sql(IVM_VALUE_COLUMN),
            quote_ident(IVM_VALUE_COLUMN),
            quote_ident(IVM_VALUE_COUNT_COLUMN),
            quoted_list(view.group_keys),
        );
        let mv_sql = match view.having {
            Some(having) => format!("select * from ({rebuild}) t where {having}"),
            None => rebuild,
        };
        for batch in context.sql(&mv_sql).await?.collect().await? {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(view.view_id, baseline.cursors).await?;
        self.metadata
            .set_view_status(view.view_id, "active")
            .await?;
        Ok(epoch)
    }
}
