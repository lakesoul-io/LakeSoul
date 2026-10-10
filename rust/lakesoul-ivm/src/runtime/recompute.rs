// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The recomputed aggregate views: variance/median/bool/approx/string/array/
//! computed aggregates and the mixed-kind statements, all rebuilt from the
//! affected groups.

use super::*;

/// The schema of a [`ViewSpec::MultiAgg`] materialized view: the group keys,
/// one nullable column per aggregate, the row kind and the epoch.
pub fn multi_agg_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    columns: &[(String, DataType)],
) -> Result<SchemaRef> {
    if columns.is_empty() {
        return Err(report!(
            "a multi-aggregate view needs at least one aggregate"
        ));
    }
    let mut fields = if group_keys.is_empty() && group_exprs.is_empty() {
        Vec::new()
    } else {
        group_key_fields(source_schema, group_keys, group_exprs)?
    };
    for (name, data_type) in columns {
        fields.push(Arc::new(Field::new(name.clone(), data_type.clone(), true)));
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

/// An aggregate view over one source with any mix of supported aggregate
/// functions in a single statement.
#[derive(Debug, Clone)]
pub struct MultiAggView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to [`Self::group_keys`].
    pub group_exprs: Vec<String>,
    /// `(rendered call, MV column, result type)` per aggregate.
    pub aggregates: Vec<(String, String, DataType)>,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized columns.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl MultiAggView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        aggregates: Vec<(String, String, DataType)>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            aggregates,
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
        Ok(ViewSpec::MultiAgg {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
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

    fn parts(&self) -> Result<RecomputeParts<'_>> {
        let (first_call, first_column, _) = self.aggregates.first().ok_or_else(|| {
            report!("a multi-aggregate view needs at least one aggregate")
        })?;
        Ok(RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: first_call.clone(),
            column: first_column.clone(),
            extra_aggregates: self.aggregates[1..]
                .iter()
                .map(|(call, column, _)| RecomputeAggregate {
                    call: call.clone(),
                    column: column.clone(),
                })
                .collect(),
            distinct_columns: None,
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
        })
    }
}

/// Validate that a computed-aggregate view can be maintained.
fn validate_computed_agg_view(view: &ComputedAggView) -> Result<()> {
    if view.function.is_empty() {
        return Err(report!(
            "computed aggregate view {} has no function",
            view.view_id
        ));
    }
    if view.arguments.is_empty() {
        return Err(report!(
            "computed aggregate view {} has no arguments",
            view.view_id
        ));
    }
    for argument in &view.arguments {
        match (&argument.expr, &argument.column) {
            (Some(expression), _) => {
                expression_type(&view.source.schema, expression)?;
            }
            (None, Some(column)) => {
                field_type(&view.source.schema, column)?;
            }
            (None, None) => {
                return Err(report!(
                    "computed aggregate view {} has an empty argument",
                    view.view_id
                ));
            }
        }
    }
    Ok(())
}

/// A `VAR_*`/`STDDEV_*` view over a source table.
///
/// The statistic is recomputed from the affected groups' current source rows,
/// which keeps the result identical to the native aggregate.
#[derive(Debug, Clone)]
pub struct VarianceView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the statistic.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to
    /// [`Self::group_keys`]; empty means every key is a plain column.
    pub group_exprs: Vec<String>,
    /// The value column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered value expression.
    pub value_expr: Option<String>,
    /// Which statistic is maintained.
    pub statistic: VarianceKind,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized column.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl VarianceView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
        kind: VarianceKind,
    ) -> Self {
        Self::new_with_group_keys(
            view_id,
            source,
            mv,
            vec![group_key.into()],
            value_column,
            kind,
        )
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        value_column: impl Into<String>,
        kind: VarianceKind,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            statistic: kind,
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

    /// Aggregate over a rendered scalar expression (`VAR_SAMP(v * 2)`).
    pub fn with_value_expr(mut self, value_expr: impl Into<String>) -> Self {
        self.value_expr = Some(value_expr.into());
        self.value_column = None;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Variance {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            statistic: self.statistic,
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }
}

/// The schema of a [`VarianceView`] materialized view: the group keys and the
/// statistic (nullable; a sample needs at least two values).
pub fn variance_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
    kind: VarianceKind,
) -> Result<SchemaRef> {
    variance_groups_mv_schema_for(
        source_schema,
        group_keys,
        &[],
        Some(value_column),
        None,
        kind,
    )
}

/// The schema of a [`VarianceView`] materialized view with optional group and
/// value expressions.
pub fn variance_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
    kind: VarianceKind,
) -> Result<SchemaRef> {
    let value_type = aggregate_value_type(source_schema, value_column, value_expr)?;
    variance_result_type(&value_type)?;
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(
        kind.column_name(),
        DataType::Float64,
        true,
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

/// The result type of a variance/stddev aggregate.
fn variance_result_type(value_type: &DataType) -> Result<DataType> {
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
            return Err(report!("variance is not supported for value type {other}"));
        }
    })
}

/// The result type of a `MEDIAN` aggregate: the optimizer coerces integers to
/// Float64; floats keep their width.
fn median_result_type(value_type: &DataType) -> Result<DataType> {
    Ok(match value_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Float64,
        DataType::Float32 | DataType::Float64 => value_type.clone(),
        other => {
            return Err(report!("median is not supported for value type {other}"));
        }
    })
}

/// The derived column of a [`BoolAggView`] materialized view.
pub fn bool_agg_output_column(kind: BoolAggKind, value_column: Option<&str>) -> String {
    match value_column {
        Some(column) => format!("{}_{column}", kind.sql_name()),
        None => format!("{}_value", kind.sql_name()),
    }
}

/// A `BOOL_AND(value)` / `BOOL_OR(value)` view over a source table.
///
/// The aggregate is recomputed from the affected groups' current rows, like
/// the other unmergeable aggregates (NULL inputs are ignored by DataFusion's
/// implementation).
#[derive(Debug, Clone)]
pub struct BoolAggView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the statistic.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to [`Self::group_keys`].
    pub group_exprs: Vec<String>,
    /// The boolean value column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered value expression.
    pub value_expr: Option<String>,
    /// `true` for `BOOL_AND`, `false` for `BOOL_OR`.
    pub kind: BoolAggKind,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized column.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl BoolAggView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
        kind: BoolAggKind,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys: vec![group_key.into()],
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            kind,
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
        group_keys: Vec<String>,
        value_column: impl Into<String>,
        kind: BoolAggKind,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            kind,
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

    /// Aggregate over a rendered scalar expression.
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
        ViewSpec::BoolAgg {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            bool_agg: self.kind,
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }

    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "{}({})",
                self.kind.sql_name(),
                aggregate_value_sql(
                    self.value_column.as_deref(),
                    self.value_expr.as_deref()
                )
            ),
            column: bool_agg_output_column(self.kind, self.value_column.as_deref()),
            distinct_columns: None,
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
            extra_aggregates: Vec::new(),
        }
    }
}

/// The result type of `BOOL_AND`/`BOOL_OR`: only booleans are supported.
fn bool_agg_result_type(value_type: &DataType) -> Result<()> {
    match value_type {
        DataType::Boolean => Ok(()),
        other => Err(report!(
            "BOOL_AND/BOOL_OR is not supported for value type {other}"
        )),
    }
}

/// The schema of a [`BoolAggView`] materialized view with optional group and
/// value expressions.
pub fn bool_agg_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
    kind: BoolAggKind,
) -> Result<SchemaRef> {
    let value_type = aggregate_value_type(source_schema, value_column, value_expr)?;
    bool_agg_result_type(&value_type)?;
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(
        bool_agg_output_column(kind, value_column),
        DataType::Boolean,
        true,
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

/// The schema of a [`BoolAggView`] materialized view deriving the types from
/// the source schema.
pub fn bool_agg_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
    kind: BoolAggKind,
) -> Result<SchemaRef> {
    bool_agg_groups_mv_schema_for(
        source_schema,
        group_keys,
        &[],
        Some(value_column),
        None,
        kind,
    )
}

/// The derived column of an [`ApproxDistinctView`] materialized view.
pub fn approx_distinct_output_column(value_column: Option<&str>) -> String {
    match value_column {
        Some(column) => format!("approx_distinct_{column}"),
        None => "approx_distinct_value".to_string(),
    }
}

/// An `APPROX_DISTINCT(value)` view over a source table.
///
/// The statistic is recomputed from the affected groups' current rows like
/// the other unmergeable aggregates; DataFusion's sketch update is order
/// independent, so the result matches a full rebuild.
#[derive(Debug, Clone)]
pub struct ApproxDistinctView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the statistic.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to [`Self::group_keys`].
    pub group_exprs: Vec<String>,
    /// The value column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered value expression.
    pub value_expr: Option<String>,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized column.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl ApproxDistinctView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
    ) -> Self {
        Self::new_with_group_keys(
            view_id,
            source,
            mv,
            vec![group_key.into()],
            value_column,
        )
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        value_column: impl Into<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
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

    /// Aggregate over a rendered scalar expression.
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
        ViewSpec::ApproxDistinct {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }

    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "approx_distinct({})",
                aggregate_value_sql(
                    self.value_column.as_deref(),
                    self.value_expr.as_deref()
                )
            ),
            column: approx_distinct_output_column(self.value_column.as_deref()),
            extra_aggregates: Vec::new(),
            distinct_columns: None,
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
        }
    }
}

/// The schema of an [`ApproxDistinctView`] materialized view with optional
/// group and value expressions. The approximate count is a `UInt64` (nullable
/// because an all-NULL group has no distinct values).
pub fn approx_distinct_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
) -> Result<SchemaRef> {
    aggregate_value_type(source_schema, value_column, value_expr)?;
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(
        approx_distinct_output_column(value_column),
        DataType::UInt64,
        true,
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

/// The schema of an [`ApproxDistinctView`] materialized view deriving the
/// types from the source schema.
pub fn approx_distinct_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    approx_distinct_groups_mv_schema_for(
        source_schema,
        group_keys,
        &[],
        Some(value_column),
        None,
    )
}

/// The derived column of an [`ApproxPercentileView`] materialized view.
pub fn approx_percentile_output_column(value_column: Option<&str>) -> String {
    match value_column {
        Some(column) => format!("approx_percentile_cont_{column}"),
        None => "approx_percentile_cont_value".to_string(),
    }
}

/// An `APPROX_PERCENTILE_CONT(value, percentile)` view over a source table.
///
/// The statistic is recomputed from the affected groups' current rows like
/// the other unmergeable aggregates; DataFusion's sketch update is order
/// independent, so the result matches a full rebuild.
#[derive(Debug, Clone)]
pub struct ApproxPercentileView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the statistic.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to [`Self::group_keys`].
    pub group_exprs: Vec<String>,
    /// The value column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered value expression.
    pub value_expr: Option<String>,
    /// The percentile (a SQL float literal such as `0.5`).
    pub percentile: String,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized column.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl ApproxPercentileView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
        percentile: impl Into<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys: vec![group_key.into()],
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            percentile: percentile.into(),
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
        group_keys: Vec<String>,
        value_column: impl Into<String>,
        percentile: impl Into<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            percentile: percentile.into(),
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

    /// Aggregate over a rendered scalar expression.
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
        ViewSpec::ApproxPercentile {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            percentile: self.percentile.clone(),
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }

    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "approx_percentile_cont({}, {})",
                aggregate_value_sql(
                    self.value_column.as_deref(),
                    self.value_expr.as_deref()
                ),
                self.percentile
            ),
            column: approx_percentile_output_column(self.value_column.as_deref()),
            extra_aggregates: Vec::new(),
            distinct_columns: None,
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
        }
    }
}

/// The schema of an [`ApproxPercentileView`] materialized view with optional
/// group and value expressions. The approximate percentile is a `Float64`
/// (nullable because an empty group has no percentile).
pub fn approx_percentile_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
) -> Result<SchemaRef> {
    aggregate_value_type(source_schema, value_column, value_expr)?;
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(
        approx_percentile_output_column(value_column),
        DataType::Float64,
        true,
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

/// The schema of an [`ApproxPercentileView`] materialized view deriving the
/// types from the source schema.
pub fn approx_percentile_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    approx_percentile_groups_mv_schema_for(
        source_schema,
        group_keys,
        &[],
        Some(value_column),
        None,
    )
}

/// One argument of a [`ComputedAggView`]: a plain column or a rendered
/// scalar expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputedAggArg {
    /// The plain column, when the argument is a column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// The rendered expression, when the argument is not a plain column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
}

impl ComputedAggArg {
    /// The rendered SQL of the argument.
    pub fn sql(&self) -> String {
        match (&self.expr, &self.column) {
            (Some(expression), _) => expression.clone(),
            (None, Some(column)) => quote_ident(column),
            (None, None) => String::new(),
        }
    }

    /// The name used in the derived MV column.
    pub fn label(&self) -> String {
        match (&self.column, &self.expr) {
            (Some(column), _) => column.clone(),
            (None, Some(_)) => "value".to_string(),
            (None, None) => "value".to_string(),
        }
    }
}

/// The result type of a [`ComputedAggView`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputedAggResult {
    /// The type of the first argument (`BIT_AND`/`BIT_OR`/`BIT_XOR`).
    Value,
    /// `Float64` (correlation, covariance, regression, percentiles).
    Float64,
    /// `UInt64` (`REGR_COUNT`).
    UInt64,
}

/// A deterministic scalar-aggregate view over a source table.
///
/// The statistic is recomputed from the affected groups' current rows like
/// the other unmergeable aggregates.
#[derive(Debug, Clone)]
pub struct ComputedAggView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the statistic.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to [`Self::group_keys`].
    pub group_exprs: Vec<String>,
    /// The SQL aggregate function name.
    pub function: String,
    /// The aggregate arguments, in order.
    pub arguments: Vec<ComputedAggArg>,
    /// The rendered percentile literal, when the aggregate takes one.
    pub percentile: Option<String>,
    /// The MV column holding the statistic.
    pub column: String,
    /// The result type of the aggregate.
    pub result: ComputedAggResult,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized column.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl ComputedAggView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        function: impl Into<String>,
        arguments: Vec<ComputedAggArg>,
        column: impl Into<String>,
        result: ComputedAggResult,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            function: function.into(),
            arguments,
            percentile: None,
            column: column.into(),
            result,
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

    /// The rendered percentile literal of an ordered-set aggregate.
    pub fn with_percentile(mut self, percentile: impl Into<String>) -> Self {
        self.percentile = Some(percentile.into());
        self
    }

    /// Group by rendered expressions parallel to the group keys.
    pub fn with_group_exprs(mut self, group_exprs: Vec<String>) -> Self {
        self.group_exprs = group_exprs;
        self
    }

    /// The rendered aggregate call.
    pub fn aggregate_call(&self) -> String {
        let mut arguments = self
            .arguments
            .iter()
            .map(ComputedAggArg::sql)
            .collect::<Vec<_>>();
        if let Some(percentile) = &self.percentile {
            arguments.push(percentile.clone());
        }
        format!("{}({})", self.function, arguments.join(", "))
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::ComputedAgg {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            function: self.function.clone(),
            arguments: self.arguments.clone(),
            percentile: self.percentile.clone(),
            column: self.column.clone(),
            result: self.result,
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }

    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: self.aggregate_call(),
            column: self.column.clone(),
            extra_aggregates: Vec::new(),
            distinct_columns: None,
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
        }
    }
}

/// The schema of a [`ComputedAggView`] materialized view.
pub fn computed_agg_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    column: &str,
    result: ComputedAggResult,
    first_argument: Option<&ComputedAggArg>,
) -> Result<SchemaRef> {
    let data_type = match result {
        ComputedAggResult::Value => {
            let argument = first_argument
                .ok_or_else(|| report!("the aggregate has no first argument"))?;
            match (&argument.expr, &argument.column) {
                (Some(expression), _) => expression_type(source_schema, expression)?.0,
                (None, Some(name)) => field_type(source_schema, name)?,
                (None, None) => {
                    return Err(report!("the aggregate has no first argument"));
                }
            }
        }
        ComputedAggResult::Float64 => DataType::Float64,
        ComputedAggResult::UInt64 => DataType::UInt64,
    };
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    // Every aggregate is nullable: an empty group aggregates to NULL.
    fields.push(Arc::new(Field::new(column, data_type, true)));
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

#[derive(Debug, Clone)]
pub struct MedianView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the statistic.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to
    /// [`Self::group_keys`]; empty means every key is a plain column.
    pub group_exprs: Vec<String>,
    /// The value column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered value expression.
    pub value_expr: Option<String>,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized column.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl MedianView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
    ) -> Self {
        Self::new_with_group_keys(
            view_id,
            source,
            mv,
            vec![group_key.into()],
            value_column,
        )
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        value_column: impl Into<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
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

    /// Aggregate over a rendered scalar expression (`MEDIAN(v * 2)`).
    pub fn with_value_expr(mut self, value_expr: impl Into<String>) -> Self {
        self.value_expr = Some(value_expr.into());
        self.value_column = None;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Median {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }
}

/// The schema of a [`MedianView`] materialized view: the group keys and the
/// median (nullable when the group has no rows).
pub fn median_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    median_groups_mv_schema_for(source_schema, group_keys, &[], Some(value_column), None)
}

/// The schema of a [`MedianView`] materialized view with optional group and
/// value expressions.
pub fn median_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
) -> Result<SchemaRef> {
    let value_type = aggregate_value_type(source_schema, value_column, value_expr)?;
    let value_type = median_result_type(&value_type)?;
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(IVM_MEDIAN_COLUMN, value_type, true)));
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

/// The source and statistic of a "recomputed aggregate" view: the statistic
/// cannot be merged from signed deltas, so a refresh recomputes the affected
/// groups from their current source rows.
/// One aggregate of a multi-aggregate recompute view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RecomputeAggregate {
    /// The rendered aggregate call over the source columns.
    pub(super) call: String,
    /// The MV column holding the aggregate.
    pub(super) column: String,
}

pub(super) struct RecomputeParts<'a> {
    pub(super) view_id: &'a str,
    pub(super) source: &'a IvmTable,
    pub(super) mv: &'a IvmTable,
    pub(super) group_keys: &'a [String],
    /// The rendered group expressions, parallel to `group_keys`.
    pub(super) group_exprs: &'a [String],
    /// The rendered aggregate call over the source columns (e.g.
    /// `var("v")`, `string_agg("s", ',' order by "k")`).
    pub(super) aggregate_call: String,
    /// The MV column holding the statistic.
    pub(super) column: String,
    /// Additional aggregates materialized alongside the first one (mixed
    /// aggregate kinds in one statement).
    pub(super) extra_aggregates: Vec<RecomputeAggregate>,
    /// The distinct value columns of a multi-column `COUNT(DISTINCT a, b)`:
    /// the aggregate is the count of distinct tuples among the affected
    /// group's rows, computed over a `SELECT DISTINCT` subquery.
    pub(super) distinct_columns: Option<&'a [String]>,
    pub(super) filter: Option<&'a str>,
    pub(super) having: Option<&'a str>,
}

impl RecomputeParts<'_> {
    /// Every aggregate of the view: the first one and the extras.
    fn aggregates(&self) -> Vec<(&str, &str)> {
        std::iter::once((self.aggregate_call.as_str(), self.column.as_str()))
            .chain(
                self.extra_aggregates.iter().map(|aggregate| {
                    (aggregate.call.as_str(), aggregate.column.as_str())
                }),
            )
            .collect()
    }

    /// The rendered `call as "column"` list.
    fn aggregate_select(&self) -> String {
        self.aggregates()
            .iter()
            .map(|(call, column)| format!("{call} as {}", quote_ident(column)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The quoted MV columns of every aggregate, in order.
    fn aggregate_columns(&self) -> Vec<String> {
        self.aggregates()
            .iter()
            .map(|(_, column)| quote_ident(column))
            .collect()
    }
}

impl VarianceView {
    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "{}({})",
                self.statistic.sql_name(),
                aggregate_value_sql(
                    self.value_column.as_deref(),
                    self.value_expr.as_deref()
                )
            ),
            column: self.statistic.column_name().to_string(),
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
            distinct_columns: None,
            extra_aggregates: Vec::new(),
        }
    }
}

impl MedianView {
    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "median({})",
                aggregate_value_sql(
                    self.value_column.as_deref(),
                    self.value_expr.as_deref()
                )
            ),
            column: IVM_MEDIAN_COLUMN.to_string(),
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
            distinct_columns: None,
            extra_aggregates: Vec::new(),
        }
    }
}

/// The derived column of a [`StringAggView`] materialized view.
pub fn string_agg_column(value_column: &str) -> String {
    format!("string_agg_{value_column}")
}

/// The derived column of a `STRING_AGG` view over an expression.
pub fn string_agg_output_column(value_column: Option<&str>) -> String {
    match value_column {
        Some(column) => string_agg_column(column),
        None => "string_agg_value".to_string(),
    }
}

/// The derived column of an [`ArrayAggView`] view over an expression.
pub fn array_agg_output_column(value_column: Option<&str>) -> String {
    match value_column {
        Some(column) => array_agg_column(column),
        None => "array_agg_value".to_string(),
    }
}

/// The result type of `STRING_AGG(value, delimiter)`: the value must be a
/// string column and the concatenation is `LargeUtf8`, like DataFusion's
/// accumulator.
fn string_agg_result_type(value_type: &DataType) -> Result<()> {
    match value_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(()),
        other => Err(report!(
            "STRING_AGG is not supported for value type {other}"
        )),
    }
}

/// A `STRING_AGG(value, delimiter ORDER BY order_keys)` view over a source
/// table.
///
/// The concatenation cannot be merged from signed deltas, so a refresh
/// recomputes the affected groups from their current source rows; the
/// aggregate ordering makes the result deterministic.
#[derive(Debug, Clone)]
pub struct StringAggView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the concatenation.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to
    /// [`Self::group_keys`]; empty means every key is a plain column.
    pub group_exprs: Vec<String>,
    /// The concatenated column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered concatenated expression.
    pub value_expr: Option<String>,
    /// The delimiter, rendered as a SQL string literal (e.g. `','`).
    pub delimiter: String,
    /// The rendered ordering items inside the aggregate.
    pub order_by: Vec<String>,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// An optional `HAVING` predicate over the materialized column.
    pub having: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl StringAggView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
        delimiter: impl Into<String>,
        order_by: Vec<String>,
    ) -> Self {
        Self::new_with_group_keys(
            view_id,
            source,
            mv,
            vec![group_key.into()],
            value_column,
            delimiter,
            order_by,
        )
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        value_column: impl Into<String>,
        delimiter: impl Into<String>,
        order_by: Vec<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            delimiter: delimiter.into(),
            order_by,
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

    /// Aggregate over a rendered scalar expression
    /// (`STRING_AGG(CAST(v AS TEXT), ',')`).
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

    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "string_agg({}, {} order by {})",
                aggregate_value_sql(
                    self.value_column.as_deref(),
                    self.value_expr.as_deref()
                ),
                self.delimiter,
                self.order_by.join(", "),
            ),
            column: string_agg_output_column(self.value_column.as_deref()),
            filter: self.filter.as_deref(),
            having: self.having.as_deref(),
            distinct_columns: None,
            extra_aggregates: Vec::new(),
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::StringAgg {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            delimiter: self.delimiter.clone(),
            order_by: self.order_by.clone(),
            filter: self.filter.clone(),
            having: self.having.clone(),
        }
    }
}

/// The schema of a [`StringAggView`] materialized view: the group keys and the
/// `LargeUtf8` concatenation.
pub fn string_agg_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    string_agg_groups_mv_schema_for(
        source_schema,
        group_keys,
        &[],
        Some(value_column),
        None,
    )
}

/// The schema of a [`StringAggView`] materialized view with optional group and
/// value expressions.
pub fn string_agg_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
) -> Result<SchemaRef> {
    let value_type = aggregate_value_type(source_schema, value_column, value_expr)?;
    string_agg_result_type(&value_type)?;
    let column = if value_expr.is_some() {
        "string_agg_value".to_string()
    } else {
        string_agg_column(value_column.unwrap_or_default())
    };
    string_agg_schema_for(source_schema, group_keys, group_exprs, column)
}

/// The schema of a [`StringAggView`] materialized view whose value is a
/// rendered expression.
pub fn string_agg_expr_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_expr: &str,
) -> Result<SchemaRef> {
    string_agg_groups_mv_schema_for(
        source_schema,
        group_keys,
        &[],
        None,
        Some(value_expr),
    )
}

/// The `STRING_AGG` materialized view schema given the value column name.
fn string_agg_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    column: String,
) -> Result<SchemaRef> {
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(column, DataType::LargeUtf8, true)));
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

/// The derived column of an [`ArrayAggView`] materialized view.
pub fn array_agg_column(value_column: &str) -> String {
    format!("array_agg_{value_column}")
}

/// An `ARRAY_AGG(value ORDER BY order_keys)` view over a source table.
///
/// The collected list cannot be merged from signed deltas, so a refresh
/// recomputes the affected groups from their current source rows; the
/// aggregate ordering makes the result deterministic.
#[derive(Debug, Clone)]
pub struct ArrayAggView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert).
    pub source: IvmTable,
    /// The materialized view table: the group keys and the collected list.
    pub mv: IvmTable,
    /// The group key columns.
    pub group_keys: Vec<String>,
    /// The rendered group expressions, parallel to
    /// [`Self::group_keys`]; empty means every key is a plain column.
    pub group_exprs: Vec<String>,
    /// The collected column; `None` when the argument is an expression.
    pub value_column: Option<String>,
    /// The rendered collected expression.
    pub value_expr: Option<String>,
    /// The rendered ordering items inside the aggregate.
    pub order_by: Vec<String>,
    /// An optional filter the contributing rows must satisfy.
    pub filter: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl ArrayAggView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_key: impl Into<String>,
        value_column: impl Into<String>,
        order_by: Vec<String>,
    ) -> Self {
        Self::new_with_group_keys(
            view_id,
            source,
            mv,
            vec![group_key.into()],
            value_column,
            order_by,
        )
    }

    /// A new view over several group key columns.
    pub fn new_with_group_keys(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        value_column: impl Into<String>,
        order_by: Vec<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            group_exprs: Vec::new(),
            value_column: Some(value_column.into()),
            value_expr: None,
            order_by,
            filter: None,
            refresh_interval_ms: 0,
        }
    }

    /// Only rows matching `filter` contribute to the view.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Aggregate over a rendered scalar expression (`ARRAY_AGG(v * 2 ...)`).
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

    fn parts(&self) -> RecomputeParts<'_> {
        RecomputeParts {
            view_id: &self.view_id,
            source: &self.source,
            mv: &self.mv,
            group_keys: &self.group_keys,
            group_exprs: &self.group_exprs,
            aggregate_call: format!(
                "array_agg({} order by {})",
                aggregate_value_sql(
                    self.value_column.as_deref(),
                    self.value_expr.as_deref()
                ),
                self.order_by.join(", "),
            ),
            column: array_agg_output_column(self.value_column.as_deref()),
            filter: self.filter.as_deref(),
            having: None,
            distinct_columns: None,
            extra_aggregates: Vec::new(),
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::ArrayAgg {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            group_exprs: self.group_exprs.clone(),
            value_column: self.value_column.clone(),
            value_expr: self.value_expr.clone(),
            order_by: self.order_by.clone(),
            filter: self.filter.clone(),
        }
    }
}

/// The schema of an [`ArrayAggView`] materialized view: the group keys and the
/// collected `List` column.
pub fn array_agg_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_column: &str,
) -> Result<SchemaRef> {
    array_agg_groups_mv_schema_for(
        source_schema,
        group_keys,
        &[],
        Some(value_column),
        None,
    )
}

/// The schema of an [`ArrayAggView`] materialized view with optional group and
/// value expressions.
pub fn array_agg_groups_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    value_column: Option<&str>,
    value_expr: Option<&str>,
) -> Result<SchemaRef> {
    let value_type = aggregate_value_type(source_schema, value_column, value_expr)?;
    let column = if value_expr.is_some() {
        "array_agg_value".to_string()
    } else {
        array_agg_column(value_column.unwrap_or_default())
    };
    array_agg_schema_for(source_schema, group_keys, group_exprs, column, value_type)
}

/// The schema of an [`ArrayAggView`] materialized view whose value is a
/// rendered expression.
pub fn array_agg_expr_mv_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    value_expr: &str,
) -> Result<SchemaRef> {
    array_agg_groups_mv_schema_for(source_schema, group_keys, &[], None, Some(value_expr))
}

/// The `ARRAY_AGG` materialized view schema given the value column name and
/// type.
fn array_agg_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
    column: String,
    value_type: DataType,
) -> Result<SchemaRef> {
    let mut fields = group_key_fields(source_schema, group_keys, group_exprs)?;
    fields.push(Arc::new(Field::new(
        column,
        DataType::List(Arc::new(Field::new_list_field(value_type, true))),
        true,
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

/// Validate that a multi-aggregate view can be maintained.
fn validate_multi_agg_view(view: &MultiAggView) -> Result<()> {
    if view.aggregates.is_empty() {
        return Err(report!(
            "multi-aggregate view {} needs at least one aggregate",
            view.view_id
        ));
    }
    let mut names = std::collections::HashSet::new();
    for (call, column, _) in &view.aggregates {
        if call.is_empty() || column.is_empty() {
            return Err(report!(
                "multi-aggregate view {} has an empty aggregate",
                view.view_id
            ));
        }
        if !names.insert(column.as_str()) {
            return Err(report!(
                "multi-aggregate view {}: column {column} is materialized twice",
                view.view_id
            ));
        }
    }
    Ok(())
}

/// The `group_now` SELECT of a recomputed-aggregate refresh/rebuild: the
/// recomputed statistic per group over `src_from`.
///
/// A multi-column distinct count cannot run as DataFusion's
/// `count(DISTINCT a, b)` (the aggregate is not implemented), so it counts the
/// non-NULL tuples of a deduplicated subquery instead.
fn recompute_group_now_sql(
    parts: &RecomputeParts<'_>,
    keys: &str,
    src_from: &str,
) -> String {
    match parts.distinct_columns {
        Some(distinct) => {
            let column = quote_ident(&parts.column);
            let distinct_list = quoted_list(distinct);
            let not_null = distinct
                .iter()
                .map(|column| format!("{} is not null", quote_ident(column)))
                .collect::<Vec<_>>()
                .join(" and ");
            format!(
                "select {keys}, count(case when {not_null} then 1 end) as {column} \
                 from (select distinct {keys}, {distinct_list} from {src_from}) \
                 group by {keys}"
            )
        }
        None => format!(
            "select {keys}, {} from {src_from} group by {keys}",
            parts.aggregate_select()
        ),
    }
}

/// SQL for one recomputed-aggregate refresh window: recompute the affected
/// groups from their current source rows.
fn recompute_refresh_sql(parts: &RecomputeParts<'_>, epoch: i64) -> String {
    let keys = quoted_list(parts.group_keys);
    let columns = parts.aggregate_columns().join(", ");
    let agg = &parts.aggregate_call;
    let keyed = !parts.source.primary_keys.is_empty();
    let src_from = if keyed {
        format!(
            "src where {}{}",
            source_delete_filter("src", change_column(parts.source)),
            filter_clause(parts.filter),
        )
    } else {
        format!("src{}", filter_where(parts.filter))
    };
    let active_match = key_join_condition_null_safe("a", "s", parts.group_keys);
    let already_match = key_join_condition_null_safe("a", "p", parts.group_keys);
    let having = parts
        .having
        .map(|having| format!(" and ({having})"))
        .unwrap_or_default();
    let group_now = recompute_group_now_sql(parts, &keys, &src_from);
    let _ = agg;
    format!(
        "with affected as ({affected}), \
         group_now as ({group_now}), \
         already as (select distinct {keys} from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         deletes as (select {keys}, {columns}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from active s \
                     where exists (select 1 from affected a where {active_match})), \
         inserts as (select {keys}, {columns}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from group_now p \
                     where not exists (select 1 from already a where {already_match}){having}) \
         select * from deletes union all select * from inserts \
         order by {keys}, \"rowKinds\"",
        affected =
            affected_groups_sql(parts.source, parts.group_keys, keyed, parts.filter,),
    )
}

/// The recompute SQL of a global aggregate: one row over the whole current
/// source.
fn recompute_global_sql(parts: &RecomputeParts<'_>, epoch: i64) -> String {
    let src_where = format!(
        "{}{}",
        source_delete_filter("src", change_column(parts.source)),
        filter_clause(parts.filter),
    );
    let row = format!(
        "select {}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from src where {src_where}",
        parts.aggregate_select()
    );
    match parts.having {
        Some(having) => format!("select * from ({row}) t where {having}"),
        None => row,
    }
}

/// SQL for a full recomputed-aggregate rebuild.
fn recompute_rebuild_sql(parts: &RecomputeParts<'_>, epoch: i64) -> String {
    let keys = quoted_list(parts.group_keys);
    let columns = parts.aggregate_columns().join(", ");
    let agg = &parts.aggregate_call;
    let keyed = !parts.source.primary_keys.is_empty();
    let src_from = if keyed {
        format!(
            "src where {}{}",
            source_delete_filter("src", change_column(parts.source)),
            filter_clause(parts.filter),
        )
    } else {
        format!("src{}", filter_where(parts.filter))
    };
    let group_now = recompute_group_now_sql(parts, &keys, &src_from);
    let _ = agg;
    let rebuild = format!(
        "select {keys}, {columns}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from ({group_now}) t"
    );
    match parts.having {
        Some(having) => format!("select * from ({rebuild}) t where {having}"),
        None => rebuild,
    }
}

/// Validate that a recomputed-aggregate view can be maintained.
pub(super) fn validate_recompute_view(parts: &RecomputeParts<'_>) -> Result<()> {
    // An empty key list is a global aggregate over the whole source.
    if parts.group_exprs.is_empty() && !parts.group_keys.is_empty() {
        validate_group_keys(parts.source, parts.group_keys, parts.view_id)?;
    }
    group_key_fields(
        parts.source.schema.as_ref(),
        parts.group_keys,
        parts.group_exprs,
    )?;
    if let Some(filter) = parts.filter {
        let context = SessionContext::new();
        parse_filter(&context, &parts.source.schema, filter)?;
    }
    if let Some(having) = parts.having {
        let context = SessionContext::new();
        parse_filter(&context, &parts.mv.schema, having)?;
    }
    Ok(())
}

impl IvmRuntime {
    /// Persist a variance view spec (idempotent).
    pub async fn register_variance_view(&self, view: &VarianceView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a variance/stddev view.
    ///
    /// Welford's algorithm cannot be merged from signed deltas, so the
    /// affected groups are recomputed from their current source rows (pruned
    /// by the group keys, exactly like the value-count refresh).
    pub async fn refresh_variance(&self, view: &VarianceView) -> Result<Option<i64>> {
        self.register_variance_view(view).await?;
        variance_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Refresh a median view.
    pub async fn refresh_median(&self, view: &MedianView) -> Result<Option<i64>> {
        self.register_median_view(view).await?;
        median_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Refresh a `STRING_AGG` view by recomputing the affected groups from
    /// their current source rows.
    pub async fn refresh_string_agg(&self, view: &StringAggView) -> Result<Option<i64>> {
        self.register_string_agg_view(view).await?;
        string_agg_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        if view.order_by.is_empty() {
            return Err(report!(
                "STRING_AGG view {} needs an aggregate ORDER BY",
                view.view_id
            ));
        }
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Rebuild a `STRING_AGG` view from the full source state.
    pub async fn rebuild_string_agg(&self, view: &StringAggView) -> Result<i64> {
        self.register_string_agg_view(view).await?;
        string_agg_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        if view.order_by.is_empty() {
            return Err(report!(
                "STRING_AGG view {} needs an aggregate ORDER BY",
                view.view_id
            ));
        }
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    /// Refresh an `ARRAY_AGG` view by recomputing the affected groups from
    /// their current source rows.
    pub async fn refresh_array_agg(&self, view: &ArrayAggView) -> Result<Option<i64>> {
        self.register_array_agg_view(view).await?;
        if view.order_by.is_empty() {
            return Err(report!(
                "ARRAY_AGG view {} needs an aggregate ORDER BY",
                view.view_id
            ));
        }
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Rebuild an `ARRAY_AGG` view from the full source state.
    pub async fn rebuild_array_agg(&self, view: &ArrayAggView) -> Result<i64> {
        self.register_array_agg_view(view).await?;
        if view.order_by.is_empty() {
            return Err(report!(
                "ARRAY_AGG view {} needs an aggregate ORDER BY",
                view.view_id
            ));
        }
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    /// Persist an `ARRAY_AGG` view spec (idempotent).
    pub async fn register_array_agg_view(&self, view: &ArrayAggView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a `STRING_AGG` view spec (idempotent).
    pub async fn register_string_agg_view(&self, view: &StringAggView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a recomputed-aggregate view (variance family or median) by
    /// recomputing the affected groups from their current source rows.
    pub(super) async fn refresh_recomputed(
        &self,
        parts: &RecomputeParts<'_>,
    ) -> Result<Option<i64>> {
        let window = self
            .collect_source_window(parts.view_id, parts.source)
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }
        let record = match self
            .begin_window(parts.view_id, &window.identity, parts.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(parts.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        if parts.group_keys.is_empty() {
            // A global aggregate has no groups to prune and its MV is a
            // single row without a primary key, so the aggregate is
            // recomputed over the current source and the MV is rewritten
            // wholesale.
            let context = SessionContext::new();
            register_table(
                &context,
                "src",
                parts.source.read_current(&self.client).await?,
                &parts.source.schema,
            )?;
            parts.mv.truncate(&self.client).await?;
            let sql = recompute_global_sql(parts, epoch);
            for batch in context.sql(&sql).await?.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids.extend(parts.mv.append_batch(&self.client, batch).await?);
                }
            }
            let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
            self.metadata
                .mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(parts.view_id, window.cursors).await?;
            return Ok(Some(epoch));
        }

        let context = SessionContext::new();
        let delta_batches = project_group_keys(
            &context,
            parts
                .source
                .read_partition_files(window.added_files)
                .await?,
            &parts.source.schema,
            parts.group_keys,
            parts.group_exprs,
        )
        .await?;
        let keyed = !parts.source.primary_keys.is_empty();
        let delta_context = SessionContext::new();
        register_table(
            &delta_context,
            "delta",
            delta_batches.clone(),
            &parts.source.schema,
        )?;
        let old_batches = if keyed {
            let pk_filters = key_filters(&parts.source.primary_keys, &delta_batches)?;
            let batches = project_group_keys(
                &context,
                parts
                    .source
                    .read_before_window_filtered(
                        &self.client,
                        &window.before_versions,
                        pk_filters,
                    )
                    .await?,
                &parts.source.schema,
                parts.group_keys,
                parts.group_exprs,
            )
            .await?;
            register_table(&delta_context, "old", batches.clone(), &parts.source.schema)?;
            batches
        } else {
            Vec::new()
        };
        let groups = delta_context
            .sql(&affected_groups_sql(
                parts.source,
                parts.group_keys,
                keyed,
                parts.filter,
            ))
            .await?
            .collect()
            .await?;
        // A computed group key cannot prune the source reads.
        let filters = if parts.group_exprs.is_empty() {
            key_filters(parts.group_keys, &groups)?
        } else {
            Vec::new()
        };

        register_table(&context, "delta", delta_batches, &parts.source.schema)?;
        if keyed {
            register_table(&context, "old", old_batches, &parts.source.schema)?;
        }
        let src_batches = project_group_keys(
            &context,
            parts
                .source
                .read_current_filtered(&self.client, filters.clone())
                .await?,
            &parts.source.schema,
            parts.group_keys,
            parts.group_exprs,
        )
        .await?;
        register_table(&context, "src", src_batches, &parts.source.schema)?;
        register_table(
            &context,
            "mv",
            parts
                .mv
                .read_current_filtered(&self.client, filters)
                .await?,
            &parts.mv.schema,
        )?;
        for batch in context
            .sql(&recompute_refresh_sql(parts, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(parts.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(parts.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a variance view from the full source state.
    pub async fn rebuild_variance(&self, view: &VarianceView) -> Result<i64> {
        self.register_variance_view(view).await?;
        variance_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    /// Rebuild a median view from the full source state.
    pub async fn rebuild_median(&self, view: &MedianView) -> Result<i64> {
        self.register_median_view(view).await?;
        median_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    /// Rebuild a recomputed-aggregate view from the full source state.
    pub(super) async fn rebuild_recomputed(
        &self,
        parts: &RecomputeParts<'_>,
    ) -> Result<i64> {
        self.metadata
            .set_view_status(parts.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(parts.view_id).await?;
        self.metadata.delete_cursors(parts.view_id).await?;
        parts.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(parts.source).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, parts.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                parts.view_id,
                &format!("rebuild:{generation}"),
                &baseline.to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                self.advance_cursors(parts.view_id, baseline.cursors)
                    .await?;
                self.metadata
                    .set_view_status(parts.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        if parts.group_keys.is_empty() {
            // A global aggregate is recomputed over the baseline into the
            // single MV row.
            let context = SessionContext::new();
            register_table(&context, "src", baseline.batches, &parts.source.schema)?;
            let sql = recompute_global_sql(parts, epoch);
            for batch in context.sql(&sql).await?.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids.extend(parts.mv.append_batch(&self.client, batch).await?);
                }
            }
            let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
            self.metadata
                .mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(parts.view_id, baseline.cursors)
                .await?;
            return Ok(epoch);
        }

        let context = SessionContext::new();
        let batches = project_group_keys(
            &context,
            baseline.batches,
            &parts.source.schema,
            parts.group_keys,
            parts.group_exprs,
        )
        .await?;
        register_table(&context, "src", batches, &parts.source.schema)?;
        for batch in context
            .sql(&recompute_rebuild_sql(parts, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(parts.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(parts.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(parts.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Persist a median view spec (idempotent).
    /// Persist a boolean aggregate view spec (idempotent).
    pub async fn register_bool_agg_view(&self, view: &BoolAggView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a `BOOL_AND`/`BOOL_OR` view.
    pub async fn refresh_bool_agg(&self, view: &BoolAggView) -> Result<Option<i64>> {
        self.register_bool_agg_view(view).await?;
        bool_agg_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Rebuild a `BOOL_AND`/`BOOL_OR` view from the full source state.
    pub async fn rebuild_bool_agg(&self, view: &BoolAggView) -> Result<i64> {
        self.register_bool_agg_view(view).await?;
        bool_agg_result_type(&aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    /// Persist an approximate-distinct view spec (idempotent).
    pub async fn register_approx_distinct_view(
        &self,
        view: &ApproxDistinctView,
    ) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh an `APPROX_DISTINCT` view.
    pub async fn refresh_approx_distinct(
        &self,
        view: &ApproxDistinctView,
    ) -> Result<Option<i64>> {
        self.register_approx_distinct_view(view).await?;
        aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Rebuild an `APPROX_DISTINCT` view from the full source state.
    pub async fn rebuild_approx_distinct(
        &self,
        view: &ApproxDistinctView,
    ) -> Result<i64> {
        self.register_approx_distinct_view(view).await?;
        aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    /// Persist an approximate-percentile view spec (idempotent).
    pub async fn register_approx_percentile_view(
        &self,
        view: &ApproxPercentileView,
    ) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh an `APPROX_PERCENTILE_CONT` view.
    pub async fn refresh_approx_percentile(
        &self,
        view: &ApproxPercentileView,
    ) -> Result<Option<i64>> {
        self.register_approx_percentile_view(view).await?;
        aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Rebuild an `APPROX_PERCENTILE_CONT` view from the full source state.
    pub async fn rebuild_approx_percentile(
        &self,
        view: &ApproxPercentileView,
    ) -> Result<i64> {
        self.register_approx_percentile_view(view).await?;
        aggregate_value_type(
            &view.source.schema,
            view.value_column.as_deref(),
            view.value_expr.as_deref(),
        )?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    /// Persist a computed-aggregate view spec (idempotent).
    pub async fn register_computed_agg_view(&self, view: &ComputedAggView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a computed-aggregate view.
    pub async fn refresh_computed_agg(
        &self,
        view: &ComputedAggView,
    ) -> Result<Option<i64>> {
        self.register_computed_agg_view(view).await?;
        validate_computed_agg_view(view)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Rebuild a computed-aggregate view from the full source state.
    pub async fn rebuild_computed_agg(&self, view: &ComputedAggView) -> Result<i64> {
        self.register_computed_agg_view(view).await?;
        validate_computed_agg_view(view)?;
        let parts = view.parts();
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }

    pub async fn register_median_view(&self, view: &MedianView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a multi-aggregate view spec (idempotent).
    pub async fn register_multi_agg_view(&self, view: &MultiAggView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec()?)?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a multi-aggregate view from the affected groups.
    pub async fn refresh_multi_agg(&self, view: &MultiAggView) -> Result<Option<i64>> {
        self.register_multi_agg_view(view).await?;
        validate_multi_agg_view(view)?;
        let parts = view.parts()?;
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    /// Rebuild a multi-aggregate view from the full source state.
    pub async fn rebuild_multi_agg(&self, view: &MultiAggView) -> Result<i64> {
        self.register_multi_agg_view(view).await?;
        validate_multi_agg_view(view)?;
        let parts = view.parts()?;
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }
}
