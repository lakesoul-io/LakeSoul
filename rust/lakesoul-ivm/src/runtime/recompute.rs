// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The recomputed aggregate views: variance/median/bool/approx/string/array/
//! computed aggregates and the mixed-kind statements, all rebuilt from the
//! affected groups.

use super::*;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::common::ScalarValue;
use datafusion::prelude::{col, lit};

use crate::metadata::{PartitionVersion, StateRole};
use datafusion::error::{DataFusionError, Result as DataFusionResult};

/// The internal state schema for a multi-aggregate view: the Arrow row
/// encoding of the group keys as a binary primary key, the group keys as
/// payload, one accumulator per aggregate, the row count, and the bookkeeping
/// columns.
pub fn multi_agg_state_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    columns: &[(String, DataType)],
) -> Result<SchemaRef> {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new(
        IVM_GROUP_ROW_COLUMN,
        DataType::Binary,
        false,
    ))];
    fields.extend(group_key_fields(source_schema, group_keys, &[])?);
    for (name, data_type) in columns {
        fields.push(Arc::new(Field::new(name.clone(), data_type.clone(), true)));
    }
    fields.push(Arc::new(Field::new(
        IVM_ROW_COUNT_COLUMN,
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

/// The schema of the row-level state of a multi-aggregate view.
///
/// It carries the source columns the view reads (group keys, aggregate and
/// filter inputs, and the source primary keys) plus its own row kind and
/// epoch.  The state primary key is the group keys followed by the source
/// primary keys, so a group lookup is a PK prefix lookup and a source-key
/// update is an exact PK upsert.
pub fn multi_agg_rows_state_schema_for(
    source_schema: &Schema,
    group_keys: &[String],
    group_exprs: &[String],
) -> Result<SchemaRef> {
    let key_fields = super::group_key_fields(source_schema, group_keys, group_exprs)?;
    let mut fields = source_schema
        .fields()
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();
    for (key, key_field) in group_keys.iter().zip(&key_fields) {
        match source_schema.field_with_name(key) {
            Ok(existing) => {
                if existing.data_type() != key_field.data_type() {
                    return Err(report!(
                        "computed group key {key} has a different type than the source column"
                    ));
                }
                let computed = group_exprs
                    .iter()
                    .zip(group_keys)
                    .find(|(_, name)| *name == key)
                    .is_some_and(|(expr, _)| !expr.is_empty() && expr != key);
                if computed {
                    return Err(report!(
                        "computed group key {key} would overwrite a source column"
                    ));
                }
            }
            Err(_) => fields.push(Arc::new(Field::new(
                key,
                key_field.data_type().clone(),
                key_field.is_nullable(),
            ))),
        }
    }
    if source_schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_err() {
        fields.push(Arc::new(Field::new(
            IVM_ROW_KINDS_COLUMN,
            DataType::Utf8,
            false,
        )));
    }
    if source_schema.field_with_name(IVM_EPOCH_COLUMN).is_err() {
        fields.push(Arc::new(Field::new(
            IVM_EPOCH_COLUMN,
            DataType::Int64,
            false,
        )));
    }
    Ok(Arc::new(Schema::new(fields)))
}

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
    /// The extra source a `HAVING` subquery reads, when the statement has
    /// one.
    pub having_lookup: Option<HavingLookup>,
    /// Optional keyed state for the plain SUM shape.
    pub state: Option<IvmTable>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

/// A resolved [`crate::runtime::HavingLookupSpec`].
#[derive(Debug, Clone)]
pub struct HavingLookup {
    /// The lookup source table.
    pub table: IvmTable,
    /// The rendered aggregate over the lookup rows.
    pub aggregate: String,
    /// The group keys joined to the lookup keys; empty means a cross join.
    pub join_keys: Vec<String>,
    /// The lookup keys, aligned with `join_keys`.
    pub right_keys: Vec<String>,
    /// The name the joined value carries in the recompute SQL.
    pub column: String,
    /// An optional filter the lookup rows must satisfy.
    pub filter: Option<String>,
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
            having_lookup: None,
            state: None,
            refresh_interval_ms: 0,
        }
    }

    /// Attach the keyed accumulator state.
    pub fn with_state(mut self, state: IvmTable) -> Self {
        self.state = Some(state);
        self
    }

    /// The extra source a `HAVING` subquery reads.
    pub fn with_having_lookup(mut self, lookup: HavingLookup) -> Self {
        self.having_lookup = Some(lookup);
        self
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
            having_lookup: self.having_lookup.as_ref().map(|lookup| {
                crate::runtime::HavingLookupSpec {
                    table_id: lookup.table.table_id.clone(),
                    aggregate: lookup.aggregate.clone(),
                    join_keys: lookup.join_keys.clone(),
                    right_keys: lookup.right_keys.clone(),
                    column: lookup.column.clone(),
                    filter: lookup.filter.clone(),
                }
            }),
            state_table_id: self.state.as_ref().map(|state| state.table_id.clone()),
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
            lookup: self.having_lookup.as_ref().map(|lookup| RecomputeLookup {
                table: &lookup.table,
                aggregate: lookup.aggregate.as_str(),
                join_keys: lookup.join_keys.as_slice(),
                right_keys: lookup.right_keys.as_slice(),
                column: lookup.column.as_str(),
                filter: lookup.filter.as_deref(),
            }),
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
            lookup: None,
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
            lookup: None,
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
            lookup: None,
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
            lookup: None,
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

/// A source joined into the recomputed groups for a `HAVING` subquery.
pub(super) struct RecomputeLookup<'a> {
    /// The lookup source table.
    pub(super) table: &'a IvmTable,
    /// The rendered aggregate over the lookup rows.
    pub(super) aggregate: &'a str,
    /// The group keys joined to the lookup keys; empty means a cross join.
    pub(super) join_keys: &'a [String],
    /// The lookup keys, aligned with `join_keys`.
    pub(super) right_keys: &'a [String],
    /// The name the joined value carries in the recompute SQL.
    pub(super) column: &'a str,
    /// An optional filter the lookup rows must satisfy.
    pub(super) filter: Option<&'a str>,
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
    /// The extra source a `HAVING` subquery reads.
    pub(super) lookup: Option<RecomputeLookup<'a>>,
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
            lookup: None,
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
            lookup: None,
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
            lookup: None,
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
            lookup: None,
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
    if let Some(lookup) = &view.having_lookup {
        if lookup.aggregate.is_empty() || lookup.column.is_empty() {
            return Err(report!(
                "multi-aggregate view {} has an empty HAVING lookup",
                view.view_id
            ));
        }
        if view.mv.schema.field_with_name(&lookup.column).is_ok() {
            return Err(report!(
                "multi-aggregate view {}: the HAVING lookup column {} collides \
                 with a materialized column",
                view.view_id,
                lookup.column
            ));
        }
        let right_keys = if lookup.right_keys.is_empty() {
            &lookup.join_keys
        } else {
            &lookup.right_keys
        };
        if lookup.join_keys.len() != right_keys.len() {
            return Err(report!(
                "multi-aggregate view {}: the HAVING lookup needs one right key \
                 per group key",
                view.view_id
            ));
        }
        for (left, right) in lookup.join_keys.iter().zip(right_keys) {
            let left_type = field_type(&view.mv.schema, left)?;
            let right_type = field_type(&lookup.table.schema, right)?;
            if left_type != right_type {
                return Err(report!(
                    "multi-aggregate view {}: HAVING lookup key {right} does not \
                     match the group key {left}",
                    view.view_id
                ));
            }
        }
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
/// Project the lookup delta onto the joined group key names, so it can take
/// part in the affected-group union.
fn project_lookup_keys(
    frame: DataFrame,
    lookup: &RecomputeLookup<'_>,
) -> Result<Option<DataFrame>> {
    if lookup.join_keys.is_empty() {
        return Ok(None);
    }
    let select = lookup
        .join_keys
        .iter()
        .zip(lookup_right_keys(lookup))
        .map(|(key, right)| col(right.as_str()).alias(key.as_str()))
        .collect::<Vec<_>>();
    Ok(Some(frame.select(select)?))
}

/// The keys the lookup aggregate groups by (falling back to the joined keys).
fn lookup_right_keys<'a>(lookup: &'a RecomputeLookup<'_>) -> &'a [String] {
    if lookup.right_keys.is_empty() {
        lookup.join_keys
    } else {
        lookup.right_keys
    }
}

/// The `from` clause of the lookup aggregate over the registered `lookup`
/// table.
fn lookup_from(parts: &RecomputeParts<'_>) -> String {
    let lookup = parts
        .lookup
        .as_ref()
        .expect("a lookup clause needs the lookup source");
    if !lookup.table.primary_keys.is_empty() {
        format!(
            "lookup where {}{}",
            source_delete_filter("lookup", change_column(lookup.table)),
            filter_clause(lookup.filter),
        )
    } else {
        format!("lookup{}", filter_where(lookup.filter))
    }
}

/// The joined lookup clause of the recompute: a per-key aggregate left join
/// (or a cross join for a global aggregate).  The caller registers the
/// `lookup` table.
fn lookup_join(parts: &RecomputeParts<'_>, left_alias: Option<&str>) -> String {
    let Some(lookup) = &parts.lookup else {
        return String::new();
    };
    let from = lookup_from(parts);
    let column = quote_ident(lookup.column);
    let right_keys = quoted_list(lookup_right_keys(lookup));
    if lookup.join_keys.is_empty() {
        return format!(
            " cross join (select {} as {column} from {from}) l",
            lookup.aggregate
        );
    }
    let frame = format!(
        "select {right_keys}, {} as {column} from {from} group by {right_keys}",
        lookup.aggregate
    );
    let alias = left_alias.unwrap_or("src");
    let on = lookup
        .join_keys
        .iter()
        .zip(lookup_right_keys(lookup))
        .map(|(key, right)| {
            format!("{alias}.{} = l.{}", quote_ident(key), quote_ident(right))
        })
        .collect::<Vec<_>>()
        .join(" and ");
    format!(" left join ({frame}) l on {on}")
}

fn recompute_group_now_sql(
    parts: &RecomputeParts<'_>,
    keys: &str,
    src_from: &str,
) -> String {
    let base = match parts.distinct_columns {
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
    };
    let Some(lookup) = &parts.lookup else {
        return base;
    };
    // The lookup value is functionally dependent on the group keys, so the
    // outer select carries it alongside the aggregate columns.
    let column = quote_ident(lookup.column);
    format!(
        "select t.*, l.{column} from ({base}) t{}",
        lookup_join(parts, Some("t"))
    )
}

/// SQL for one recomputed-aggregate refresh window: recompute the affected
/// groups from their current source rows.
fn recompute_refresh_sql(
    parts: &RecomputeParts<'_>,
    epoch: i64,
    lookup_changed: bool,
) -> String {
    let keys = quoted_list(parts.group_keys);
    let columns = parts.aggregate_columns().join(", ");
    let agg = &parts.aggregate_call;
    let keyed = !parts.source.primary_keys.is_empty();
    let affected_match = key_join_condition_null_safe("src", "a", parts.group_keys);
    let src_from = if keyed {
        format!(
            "src where {}{} and exists (select 1 from affected a where {affected_match})",
            source_delete_filter("src", change_column(parts.source)),
            filter_clause(parts.filter),
        )
    } else {
        let plain_where = filter_where(parts.filter);
        let prefix = if plain_where.is_empty() {
            " where true".to_string()
        } else {
            plain_where
        };
        format!(
            "src{prefix} and exists (select 1 from affected a where {affected_match})"
        )
    };
    let active_match = key_join_condition_null_safe("a", "s", parts.group_keys);
    let already_match = key_join_condition_null_safe("a", "p", parts.group_keys);
    let having = parts
        .having
        .map(|having| format!(" and ({having})"))
        .unwrap_or_default();
    let group_now = recompute_group_now_sql(parts, &keys, &src_from);
    let _ = agg;
    // A changed lookup re-evaluates the groups it can touch: every active
    // group for a global lookup, the changed keys for a per-key one.
    let affected = if lookup_changed && parts.lookup.is_some() {
        if parts
            .lookup
            .as_ref()
            .is_some_and(|lookup| lookup.join_keys.is_empty())
        {
            format!(
                "select distinct {keys} from src \
                 union select distinct {keys} from mv \
                 where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}"
            )
        } else {
            format!(
                "select {keys} from ({affected}) u \
                 union select distinct {keys} from lookup_delta",
                affected = affected_groups_sql(
                    parts.source,
                    parts.group_keys,
                    keyed,
                    parts.filter,
                    false,
                ),
            )
        }
    } else {
        affected_groups_sql(parts.source, parts.group_keys, keyed, parts.filter, false)
    };
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
         order by {order}, \"rowKinds\"",
        order = quoted_list(&refresh_order_columns(parts.mv, parts.group_keys)),
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
    let columns = parts.aggregate_columns().join(", ");
    let project =
        format!("select {columns}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\"");
    match &parts.lookup {
        Some(_) => {
            // The lookup value is computed alongside and projected away.
            let lookup_column =
                quote_ident(parts.lookup.as_ref().expect("checked above").column);
            let row = format!(
                "select {}, l.{lookup_column} as {lookup_column} from src{} where {src_where}",
                parts.aggregate_select(),
                lookup_join(parts, None),
            );
            let having = parts
                .having
                .map(|having| format!(" where {having}"))
                .unwrap_or_default();
            format!("{project} from ({row}) t{having}")
        }
        None => {
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
    // The predicate is applied where the lookup column is visible.
    let having = parts
        .having
        .map(|having| format!(" where {having}"))
        .unwrap_or_default();
    format!(
        "select {keys}, {columns}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from ({group_now}) t{having}"
    )
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
        let context = refresh_session();
        parse_filter(&context, &parts.source.schema, filter)?;
    }
    if let Some(having) = parts.having {
        let context = refresh_session();
        let mut schema = parts.mv.schema.fields().iter().cloned().collect::<Vec<_>>();
        if let Some(lookup) = &parts.lookup {
            // The lookup value is joined into the recompute and referenced by
            // the predicate, but it is not part of the MV.
            let field = parts
                .mv
                .schema
                .fields()
                .iter()
                .find(|field| field.name() == lookup.column);
            if field.is_none() {
                schema.push(Arc::new(Field::new(
                    lookup.column,
                    lookup_type(&context, lookup)?,
                    true,
                )));
            }
        }
        parse_filter(&context, &Arc::new(Schema::new(schema)), having)?;
    }
    Ok(())
}

/// The result type of the lookup aggregate, inferred by planning the call
/// against the lookup schema.
fn lookup_type(
    context: &SessionContext,
    lookup: &RecomputeLookup<'_>,
) -> Result<DataType> {
    let schema = DFSchema::try_from(lookup.table.schema.as_ref().clone())
        .map_err(|error| report!("invalid lookup schema: {error}"))?;
    let expression = context
        .state()
        .create_logical_expr(lookup.aggregate, &schema)
        .map_err(|error| {
            report!("invalid lookup aggregate {:?}: {error}", lookup.aggregate)
        })?;
    expression.get_type(&schema).map_err(|error| {
        report!("invalid lookup aggregate {:?}: {error}", lookup.aggregate)
    })
}

struct StageGuard {
    dir: std::path::PathBuf,
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct KeyedContributions {
    frame: DataFrame,
    _guard: StageGuard,
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
            .collect_source_window_for_filter(parts.view_id, parts.source, parts.filter)
            .await?;
        let lookup_window = match &parts.lookup {
            Some(lookup) => Some(
                self.collect_source_window(parts.view_id, lookup.table)
                    .await?,
            ),
            None => None,
        };
        let lookup_changed = lookup_window
            .as_ref()
            .is_some_and(|window| !window.added_files.is_empty());
        if window.added_files.is_empty() && !lookup_changed {
            return Ok(None);
        }
        let mut identity = window.identity.clone();
        if let Some(lookup_window) = &lookup_window {
            identity.extend(lookup_window.identity.iter().cloned());
        }
        let record = match self
            .begin_window(parts.view_id, &identity, parts.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(parts.view_id, window.cursors).await?;
                if let Some(lookup_window) = lookup_window {
                    self.advance_cursors(parts.view_id, lookup_window.cursors)
                        .await?;
                }
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
            let context = refresh_session();
            context.register_table("src", Arc::new(self.table_provider(parts.source)))?;
            if let Some(lookup) = &parts.lookup {
                context.register_table(
                    "lookup",
                    Arc::new(self.table_provider(lookup.table)),
                )?;
            }
            parts.mv.truncate(&self.client).await?;
            let sql = recompute_global_sql(parts, epoch);
            let frame = context.sql(&sql).await?;
            commit_ids.extend(
                parts
                    .mv
                    .append_stream(&self.client, frame.execute_stream().await?)
                    .await?,
            );
            let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
            self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(parts.view_id, window.cursors).await?;
            if let Some(lookup_window) = lookup_window {
                self.advance_cursors(parts.view_id, lookup_window.cursors)
                    .await?;
            }
            return Ok(Some(epoch));
        }

        let context = refresh_session();
        let delta = project_group_keys_frame(
            &context,
            self.groups_table_frame(&context, parts.source, window.added_files)?,
            &parts.source.schema,
            parts.group_keys,
            parts.group_exprs,
        )?;
        let keyed = !parts.source.primary_keys.is_empty();
        let old_frame = if keyed {
            let old = self
                .before_window_table_frame(
                    &context,
                    parts.source,
                    &window.before_versions,
                )
                .await?;
            project_group_keys_frame(
                &context,
                old,
                &parts.source.schema,
                parts.group_keys,
                parts.group_exprs,
            )?
        } else {
            dataframe(&context, Vec::new(), &parts.source.schema)?
        };
        context.register_table("delta", delta.into_view())?;
        let lookup_delta = match (&parts.lookup, &lookup_window) {
            (Some(lookup), Some(lookup_window))
                if !lookup_window.added_files.is_empty() =>
            {
                let frame = self.groups_table_frame(
                    &context,
                    lookup.table,
                    lookup_window.added_files.clone(),
                )?;
                project_lookup_keys(frame, lookup)?
            }
            _ => None,
        };
        if let Some(lookup) = &parts.lookup {
            context
                .register_table("lookup", Arc::new(self.table_provider(lookup.table)))?;
            if let Some(frame) = lookup_delta {
                context.register_table("lookup_delta", frame.into_view())?;
            }
        }
        if keyed {
            context.register_table("old", old_frame.into_view())?;
        }
        let src = self.current_table_frame(&context, parts.source)?;
        let src = project_group_keys_frame(
            &context,
            src,
            &parts.source.schema,
            parts.group_keys,
            parts.group_exprs,
        )?;
        context.register_table("src", src.into_view())?;
        context.register_table("mv", Arc::new(self.table_provider(parts.mv)))?;
        let frame = context
            .sql(&recompute_refresh_sql(parts, epoch, lookup_changed))
            .await?;
        let stream = frame.execute_stream().await?;
        // The plan and its streaming readers are no longer needed: drop the
        // session before the metadata commit so no reader task/pooled
        // connection is still alive when we ask for a commit connection.
        drop(context);
        commit_ids.extend(parts.mv.append_stream(&self.client, stream).await?);

        let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(parts.view_id, window.cursors).await?;
        if let Some(lookup_window) = lookup_window {
            self.advance_cursors(parts.view_id, lookup_window.cursors)
                .await?;
        }
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

        let baseline = self
            .source_baseline_for_filter(parts.source, parts.filter)
            .await?;
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
            let context = refresh_session();
            context.register_table(
                "src",
                Arc::new(
                    self.table_provider_groups(parts.source, baseline.groups.clone()),
                ),
            )?;
            if let Some(lookup) = &parts.lookup {
                context.register_table(
                    "lookup",
                    Arc::new(self.table_provider(lookup.table)),
                )?;
            }
            let sql = recompute_global_sql(parts, epoch);
            let frame = context.sql(&sql).await?;
            commit_ids.extend(
                parts
                    .mv
                    .append_stream(&self.client, frame.execute_stream().await?)
                    .await?,
            );
            let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
            self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(parts.view_id, baseline.cursors)
                .await?;
            return Ok(epoch);
        }

        let context = refresh_session();
        let frame =
            self.groups_table_frame(&context, parts.source, baseline.groups.clone())?;
        let frame = project_group_keys_frame(
            &context,
            frame,
            &parts.source.schema,
            parts.group_keys,
            parts.group_exprs,
        )?;
        context.register_table("src", frame.into_view())?;
        if let Some(lookup) = &parts.lookup {
            context
                .register_table("lookup", Arc::new(self.table_provider(lookup.table)))?;
        }
        let frame = context.sql(&recompute_rebuild_sql(parts, epoch)).await?;
        commit_ids.extend(
            parts
                .mv
                .append_stream(&self.client, frame.execute_stream().await?)
                .await?,
        );

        let mv_versions = output_partition_versions(&self.client, parts.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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
        let mut tables = vec![(StateRole::Mv, &view.mv)];
        if let Some(state) = &view.state {
            tables.push((StateRole::State, state));
        }
        self.register_state_tables(&view.view_id, &tables).await?;
        let spec = serde_json::to_value(view.to_spec()?)?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Whether a multi-aggregate view can use the keyed accumulator state.
    fn multi_agg_state_eligible(view: &MultiAggView) -> bool {
        view.state.is_some()
            && view.group_exprs.is_empty()
            && view.having.is_none()
            && view.having_lookup.is_none()
            && !view.source.primary_keys.is_empty()
            && view.aggregates.iter().all(|(call, _, _)| {
                let call = call.trim();
                call.get(..4)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SUM("))
                    && call.ends_with(')')
                    && {
                        let expression = &call[4..call.len() - 1];
                        let expression = expression.trim();
                        !expression.is_empty()
                            && !expression.to_ascii_uppercase().contains("DISTINCT")
                    }
            })
    }

    /// Refresh a multi-aggregate view from the affected groups.
    pub async fn refresh_multi_agg(&self, view: &MultiAggView) -> Result<Option<i64>> {
        self.register_multi_agg_view(view).await?;
        validate_multi_agg_view(view)?;
        if let Some(existing) = self
            .metadata
            .get_state(&view.view_id, StateRole::MultiAggRows)
            .await?
        {
            let state = self.open_table_by_id(&existing.table_id).await?;
            return self.refresh_multi_agg_rows(view, &state).await;
        }
        if Self::multi_agg_state_eligible(view)
            && let Some(state) = &view.state
        {
            return self.refresh_multi_agg_state(view, state).await;
        }
        let parts = view.parts()?;
        validate_recompute_view(&parts)?;
        self.refresh_recomputed(&parts).await
    }

    fn bootstrap_state_batch(
        batch: &RecordBatch,
        schema: &SchemaRef,
        group_keys: &[String],
        epoch: i64,
    ) -> DataFusionResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let name = field.name();
            let array: ArrayRef = if name == IVM_GROUP_ROW_COLUMN
                || group_keys.iter().any(|key| key == name)
            {
                batch.column_by_name(name).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!("missing {name}"))
                })?
            } else if name == IVM_ROW_COUNT_COLUMN {
                batch.column_by_name("d_rows").cloned().ok_or_else(|| {
                    DataFusionError::Execution("missing d_rows".to_string())
                })?
            } else if name == IVM_ROW_KINDS_COLUMN {
                Arc::new(StringArray::from(vec!["insert"; num_rows]))
            } else if name == IVM_EPOCH_COLUMN {
                Arc::new(Int64Array::from(vec![epoch; num_rows]))
            } else {
                let source = format!("d_{name}");
                let array = batch.column_by_name(&source).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!("missing {source}"))
                })?;
                arrow::compute::cast(&array, field.data_type())
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
            };
            columns.push(array);
        }
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    fn bootstrap_mv_batch(
        batch: &RecordBatch,
        schema: &SchemaRef,
        group_keys: &[String],
        epoch: i64,
    ) -> DataFusionResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let name = field.name();
            let array: ArrayRef = if group_keys.iter().any(|key| key == name) {
                batch.column_by_name(name).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!("missing {name}"))
                })?
            } else if name == IVM_ROW_KINDS_COLUMN {
                Arc::new(StringArray::from(vec!["insert"; num_rows]))
            } else if name == IVM_EPOCH_COLUMN {
                Arc::new(Int64Array::from(vec![epoch; num_rows]))
            } else {
                let source = format!("d_{name}");
                let array = batch.column_by_name(&source).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!("missing {source}"))
                })?;
                arrow::compute::cast(&array, field.data_type())
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
            };
            columns.push(array);
        }
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    /// Build per-bucket reader filters for primary-key values.
    fn bucket_filters_for_values(
        column: &str,
        values: &[ScalarValue],
        bucket_num: u32,
    ) -> Result<std::collections::HashMap<u32, Vec<Expr>>> {
        let mut buckets: std::collections::HashMap<u32, Vec<ScalarValue>> =
            (0..bucket_num).map(|bucket| (bucket, Vec::new())).collect();
        for value in values {
            let bucket = lakesoul_io::helpers::compute_scalar_hash(value) % bucket_num;
            buckets.entry(bucket).or_default().push(value.clone());
        }
        Ok(buckets
            .into_iter()
            .map(|(bucket, values)| {
                let filter = if values.is_empty() {
                    Vec::new()
                } else {
                    vec![
                        col(column).in_list(values.into_iter().map(lit).collect(), false),
                    ]
                };
                (bucket, filter)
            })
            .collect())
    }

    /// Build a source PK filter from a bounded batch of key values.
    fn source_key_filters(
        primary_key: &str,
        values: &[ScalarValue],
    ) -> DataFusionResult<Vec<Expr>> {
        let list = values.iter().cloned().map(lit).collect::<Vec<_>>();
        if list.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![col(primary_key).in_list(list, false)])
        }
    }

    /// Read source before rows for one bounded key chunk and aggregate them
    /// with the window's delta rows into grouped contributions.
    async fn stage_before_frame(
        register_context: &SessionContext,
        name: &str,
        frame: DataFrame,
        schema: &SchemaRef,
        rows: u64,
    ) -> Result<(Option<StageGuard>, DataFrame)> {
        if rows == 0 {
            register_context
                .register_batch(name, RecordBatch::new_empty(schema.clone()))?;
            return Ok((None, register_context.table(name).await?));
        }
        let dir = std::env::temp_dir().join(format!(
            "lakesoul-ivm-before-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("data.parquet");
        let uri = format!("file://{}", path.display());
        if let Err(error) = frame
            .write_parquet(
                &uri,
                datafusion::dataframe::DataFrameWriteOptions::new(),
                None,
            )
            .await
        {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(error.into());
        }
        register_context
            .register_parquet(
                name,
                &uri,
                datafusion::prelude::ParquetReadOptions {
                    schema: Some(schema.as_ref()),
                    ..Default::default()
                },
            )
            .await?;
        Ok((
            Some(StageGuard { dir }),
            register_context.table(name).await?,
        ))
    }

    async fn source_chunk_contributions(
        &self,
        context: &SessionContext,
        source: &IvmTable,
        before_groups: &[PartitionFiles],
        contrib_sql: &str,
        primary_key: &str,
        keys: &[ScalarValue],
        delta_batches: &[RecordBatch],
        stage_dir: &std::path::Path,
        prefix: &str,
    ) -> Result<Option<SchemaRef>> {
        let filters = Self::source_key_filters(primary_key, keys)?;
        if filters.is_empty() {
            return Ok(None);
        }
        let chunk_started = std::time::Instant::now();
        let before_started = std::time::Instant::now();
        let source_bucket_num = if source.bucket_columns.len() == 1
            && source.bucket_columns[0] == primary_key
        {
            source
                .hash_bucket_num
                .parse::<u32>()
                .ok()
                .filter(|count| *count > 1)
        } else {
            None
        };
        let provider = self.table_provider_groups(source, before_groups.to_vec());
        let provider = if let Some(bucket_num) = source_bucket_num {
            provider.with_bucket_filters(Self::bucket_filters_for_values(
                primary_key,
                keys,
                bucket_num,
            )?)
        } else {
            provider
        };
        let lookup_context = refresh_session();
        lookup_context.register_table("source_before", Arc::new(provider))?;
        let mut before_frame = lookup_context.table("source_before").await?;
        for filter in filters {
            before_frame = before_frame.filter(filter)?;
        }
        let before_elapsed = before_started.elapsed();
        let before_rows = count_frame_rows(before_frame.clone()).await?;
        let (before_stage, _old) = Self::stage_before_frame(
            context,
            "old",
            before_frame,
            &source.schema,
            before_rows as u64,
        )
        .await?;
        let new_frame = context.read_batches(delta_batches.to_vec())?;
        let new_frame = filter_deletes(new_frame, change_column(source))?;
        let _ = context.deregister_table("new")?;
        context.register_table("new", new_frame.into_view())?;
        let _ = before_stage;
        use futures::StreamExt;
        let mut stream = context.sql(contrib_sql).await?.execute_stream().await?;
        let mut output = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                output.push(batch);
            }
        }
        let output_rows = output.iter().map(|batch| batch.num_rows()).sum::<usize>();
        if output.is_empty() {
            return Ok(None);
        }
        let schema = output[0].schema();
        let frame = context.read_batches(output)?;
        let path = stage_dir.join(format!("{prefix}.parquet"));
        let uri = format!("file://{}", path.display());
        frame
            .write_parquet(
                &uri,
                datafusion::dataframe::DataFrameWriteOptions::new(),
                None,
            )
            .await?;
        tracing::info!(
            keys = keys.len(),
            before_rows,
            before_elapsed = ?before_elapsed,
            output_rows,
            elapsed = ?chunk_started.elapsed(),
            "keyed source chunk finished"
        );
        Ok(Some(schema))
    }

    /// Read the source rows that changed in one window through primary-key
    /// lookups and produce the grouped contributions for that window.
    ///
    /// The delta is consumed as a stream.  For each delta batch only its source
    /// rows are read from the before/current versions; the resulting grouped
    /// contributions are bounded by the delta, not by the source table.
    async fn keyed_source_contributions(
        &self,
        caller_context: &SessionContext,
        source: &IvmTable,
        view: &MultiAggView,
        window: &SourceWindow,
        delta: &DataFrame,
        measures: &[(String, String, DataType)],
    ) -> Result<KeyedContributions> {
        if view.source.primary_keys.len() != 1 {
            return Err(report!(
                "keyed source contribution lookup needs exactly one source primary key"
            ));
        }
        let primary_key = view.source.primary_keys[0].as_str();
        let before_groups = source
            .before_window_groups(&self.client, &window.before_versions)
            .await?;
        let stage_dir = std::env::temp_dir().join(format!(
            "lakesoul-ivm-contrib-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&stage_dir)?;
        let stage_guard = StageGuard {
            dir: stage_dir.clone(),
        };
        let context = refresh_session();
        let encoder = Arc::new(GroupKeyRowEncoder::new(
            &view.source.schema,
            &view.group_keys,
        )?);
        let mut input_types = Vec::with_capacity(view.group_keys.len());
        for key in &view.group_keys {
            input_types
                .push(view.source.schema.field_with_name(key)?.data_type().clone());
        }
        let udf_encoder = encoder.clone();
        context.register_udf(datafusion::logical_expr::create_udf(
            "ivm_group_row",
            input_types,
            DataType::Binary,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(move |args: &[datafusion::logical_expr::ColumnarValue]| {
                let arrays =
                    datafusion::logical_expr::ColumnarValue::values_to_arrays(args)?;
                let rows = udf_encoder
                    .encode_columns(&arrays)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                Ok(datafusion::logical_expr::ColumnarValue::Array(Arc::new(
                    rows,
                )))
            }),
        ));
        let keys = quoted_list(&view.group_keys);
        let contributions = measures
            .iter()
            .map(|(inner, column, data_type)| {
                Ok(format!(
                    "cast(coalesce(sum({inner} * w), 0) as {}) as d_{column}",
                    Self::data_type_sql(data_type)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let contrib_sql = format!(
            "select {keys}, ivm_group_row({keys}) as \"{IVM_GROUP_ROW_COLUMN}\", \
                    {contributions}, cast(coalesce(sum(w), 0) as bigint) as d_rows \
             from (select *, -1 as w from old union all select *, 1 as w from new) t{filter} \
             group by {keys}",
            filter = filter_where(view.filter.as_deref())
        );
        use futures::StreamExt;
        const KEY_CHUNK_ROWS: usize = 131_072;
        let key_index = delta
            .schema()
            .index_of_column(&datafusion::common::Column::from_name(primary_key))
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let mut delta_stream = delta.clone().execute_stream().await?;
        let mut pending: Vec<ScalarValue> = Vec::with_capacity(KEY_CHUNK_ROWS);
        let mut pending_delta: Vec<RecordBatch> = Vec::new();
        let mut output_schema: Option<SchemaRef> = None;
        let mut chunk_index = 0usize;
        while let Some(batch) = delta_stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let array = batch.column(key_index);
            let batch_keys = (0..array.len())
                .map(|row| ScalarValue::try_from_array(array, row))
                .collect::<DataFusionResult<Vec<_>>>()?;
            if !pending.is_empty() && pending.len() + batch_keys.len() > KEY_CHUNK_ROWS {
                if let Some(schema) = self
                    .source_chunk_contributions(
                        &context,
                        source,
                        &before_groups,
                        &contrib_sql,
                        primary_key,
                        &pending,
                        &pending_delta,
                        &stage_dir,
                        &format!("chunk_{chunk_index}"),
                    )
                    .await?
                {
                    output_schema = Some(schema);
                    chunk_index += 1;
                }
                pending.clear();
                pending_delta.clear();
            }
            pending.extend(batch_keys);
            pending_delta.push(batch);
        }
        if !pending.is_empty()
            && let Some(schema) = self
                .source_chunk_contributions(
                    &context,
                    source,
                    &before_groups,
                    &contrib_sql,
                    primary_key,
                    &pending,
                    &pending_delta,
                    &stage_dir,
                    &format!("chunk_{chunk_index}"),
                )
                .await?
        {
            output_schema = Some(schema);
            chunk_index += 1;
        }
        let schema = match output_schema {
            Some(schema) => schema,
            None => {
                let mut fields = Vec::new();
                for key in &view.group_keys {
                    let field = view.source.schema.field_with_name(key)?;
                    fields.push(Arc::new(Field::new(
                        key,
                        field.data_type().clone(),
                        true,
                    )));
                }
                fields.push(Arc::new(Field::new(
                    IVM_GROUP_ROW_COLUMN,
                    DataType::Binary,
                    false,
                )));
                for (_, column, data_type) in measures {
                    fields.push(Arc::new(Field::new(
                        format!("d_{column}"),
                        data_type.clone(),
                        true,
                    )));
                }
                fields.push(Arc::new(Field::new("d_rows", DataType::Int64, true)));
                Arc::new(Schema::new(fields))
            }
        };
        let frame = if chunk_index == 0 {
            caller_context.read_batches(vec![RecordBatch::new_empty(schema)])?
        } else {
            let mut frame: Option<DataFrame> = None;
            for index in 0..chunk_index {
                let path = stage_dir.join(format!("chunk_{index}.parquet"));
                let uri = format!("file://{}", path.display());
                let name = format!("contrib_chunk_{index}");
                caller_context
                    .register_parquet(
                        &name,
                        &uri,
                        datafusion::prelude::ParquetReadOptions {
                            schema: Some(schema.as_ref()),
                            ..Default::default()
                        },
                    )
                    .await?;
                let next = caller_context.table(&name).await?;
                frame = Some(match frame {
                    Some(existing) => existing.union(next)?,
                    None => next,
                });
            }
            let frame = frame.ok_or_else(|| report!("keyed contribution is missing"))?;
            if chunk_index > 1 {
                let row = quote_ident(IVM_GROUP_ROW_COLUMN);
                let sums = measures
                    .iter()
                    .map(|(_, column, _)| {
                        let alias = quote_ident(&format!("d_{column}"));
                        format!("sum({alias}) as {alias}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                caller_context.register_table("contrib_chunks", frame.into_view())?;
                caller_context
                    .sql(&format!(
                        "select {keys}, {row}, {sums}, sum(d_rows) as d_rows \
                         from contrib_chunks group by {keys}, {row}"
                    ))
                    .await?
            } else {
                frame
            }
        };
        Ok(KeyedContributions {
            frame,
            _guard: stage_guard,
        })
    }

    /// Project the state merge output onto the user MV schema.
    fn state_to_mv_batch(
        batch: &RecordBatch,
        schema: &SchemaRef,
    ) -> DataFusionResult<RecordBatch> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "state merge is missing {}",
                        field.name()
                    ))
                })
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    fn data_type_sql(data_type: &DataType) -> Result<String> {
        Ok(match data_type {
            DataType::Decimal128(precision, scale) => {
                format!("DECIMAL({precision},{scale})")
            }
            DataType::Int64 => "BIGINT".to_string(),
            other => return Err(report!("unsupported state accumulator type {other}")),
        })
    }

    /// Refresh a multi-aggregate view through a group-key state table.
    ///
    /// The state is keyed by the Arrow row encoding of the group keys, so a
    /// wide key is a single binary primary key: the source/MV are never read
    /// in full, and only the groups touched by the window are written.
    pub async fn refresh_multi_agg_state(
        &self,
        view: &MultiAggView,
        state: &IvmTable,
    ) -> Result<Option<i64>> {
        if !view.group_exprs.is_empty() || view.having.is_some() {
            return Err(report!(
                "multi-agg state refresh needs plain group keys without lookup/having"
            ));
        }
        let mut measures = Vec::new();
        for (call, column, _) in &view.aggregates {
            let call = call.trim();
            let inner = if call
                .get(..4)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SUM("))
                && call.ends_with(')')
            {
                call[4..call.len() - 1].trim()
            } else {
                return Err(report!(
                    "multi-agg state refresh only supports SUM aggregates"
                ));
            };
            let data_type = view.mv.schema.field_with_name(column)?.data_type().clone();
            measures.push((inner.to_string(), column.clone(), data_type));
        }
        self.register_multi_agg_view(view).await?;
        self.register_state_tables(&view.view_id, &[(StateRole::State, state)])
            .await?;
        let window = self
            .collect_source_window_for_filter(
                &view.view_id,
                &view.source,
                view.filter.as_deref(),
            )
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }
        let identity = window.identity.clone();
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let context = refresh_session();
        // The bootstrap is the whole current source; read it through the
        // provider instead of materialising every initial file here.  A new
        // view has no cursors, and `before_versions` still contains the
        // touched partition with a `None` value, so check the values rather
        // than the map's emptiness.
        let bootstrap = window
            .before_versions
            .values()
            .all(|version| version.is_none());
        let delta = if bootstrap {
            None
        } else {
            Some(self.groups_table_frame(
                &context,
                &view.source,
                window.added_files.clone(),
            )?)
        };
        let keyed = !view.source.primary_keys.is_empty();
        let source_partitions = self
            .changed_partition_restriction(&window, &view.source.range_partition_columns);
        let versions: Vec<PartitionVersion> = window
            .before_versions
            .iter()
            .filter_map(|(partition_desc, version)| {
                version.map(|version| PartitionVersion {
                    partition_desc: partition_desc.clone(),
                    version,
                })
            })
            .collect();
        let empty_old = dataframe(&context, Vec::new(), &view.source.schema)?;
        let keyed_contributions =
            if keyed && !versions.is_empty() && view.source.primary_keys.len() == 1 {
                let delta = delta
                    .as_ref()
                    .ok_or_else(|| report!("multi-agg refresh without a delta"))?;
                Some(
                    self.keyed_source_contributions(
                        &context,
                        &view.source,
                        view,
                        &window,
                        delta,
                        &measures,
                    )
                    .await?,
                )
            } else {
                None
            };
        let (old, new, bootstrap_new) = if keyed_contributions.is_some() {
            (empty_old.clone(), empty_old, None)
        } else if keyed && !versions.is_empty() && view.source.primary_keys.len() > 1 {
            // A composite primary key cannot use the single-column chunked
            // contribution path.  Read the before/current rows for exactly
            // the changed tuples through the arrow-Row key lookup instead of
            // scanning the full source.
            let delta = delta
                .as_ref()
                .ok_or_else(|| report!("multi-agg refresh without a delta"))?;
            let key_exprs = view
                .source
                .primary_keys
                .iter()
                .map(|key| col(key.as_str()))
                .collect::<Vec<_>>();
            let delta_keys = delta.clone().select(key_exprs)?.distinct()?;
            let old = self
                .lookup_source_before_by_pks(
                    &context,
                    &view.source,
                    &window,
                    delta_keys.clone(),
                )
                .await?;
            let new = self
                .lookup_source_by_join_keys(
                    &context,
                    &view.source,
                    delta_keys,
                    &view.source.primary_keys,
                    &view.source.primary_keys,
                    true,
                )
                .await?;
            (old, new, None)
        } else if keyed && !versions.is_empty() {
            let delta = delta
                .as_ref()
                .ok_or_else(|| report!("multi-agg refresh without a delta"))?;
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
                .map(|key| col(key.as_str()))
                .collect::<Vec<_>>();
            let delta_keys = delta.clone().select(key_exprs)?.distinct()?;
            let before =
                self.at_versions_table_frame(&context, &view.source, versions)?;
            let old = before.join(
                delta_keys.clone(),
                JoinType::LeftSemi,
                &key_names,
                &key_names,
                None,
            )?;
            let current = if let Some(restriction) = source_partitions.as_deref() {
                self.current_table_frame_filtered_partitions(
                    &context,
                    &view.source,
                    Vec::new(),
                    restriction,
                )
                .await?
            } else {
                self.current_table_frame(&context, &view.source)?
            };
            let new = current.join(
                delta_keys,
                JoinType::LeftSemi,
                &key_names,
                &key_names,
                None,
            )?;
            (old, new, None)
        } else if bootstrap {
            (
                empty_old.clone(),
                empty_old,
                Some(self.table_provider(&view.source)),
            )
        } else {
            let new = filter_deletes(
                delta.ok_or_else(|| report!("multi-agg refresh without a delta"))?,
                change_column(&view.source),
            )?;
            (empty_old, new, None)
        };
        context.register_table("old", old.into_view())?;
        if let Some(provider) = bootstrap_new {
            context.register_table("new", Arc::new(provider))?;
        } else {
            context.register_table("new", new.into_view())?;
        }
        let keys = quoted_list(&view.group_keys);
        let contributions = measures
            .iter()
            .map(|(inner, column, data_type)| {
                Ok(format!(
                    "cast(coalesce(sum({inner} * w), 0) as {}) as d_{column}",
                    Self::data_type_sql(data_type)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let contrib_sql = format!(
            "select {keys}, ivm_group_row({keys}) as \"{IVM_GROUP_ROW_COLUMN}\", \
                    {contributions}, cast(coalesce(sum(w), 0) as bigint) as d_rows \
             from (select *, -1 as w from old union all select *, 1 as w from new) t{filter} \
             group by {keys}",
            filter = filter_where(view.filter.as_deref())
        );
        let encoder = Arc::new(GroupKeyRowEncoder::new(
            &view.source.schema,
            &view.group_keys,
        )?);
        let mut input_types = Vec::with_capacity(view.group_keys.len());
        for key in &view.group_keys {
            input_types
                .push(view.source.schema.field_with_name(key)?.data_type().clone());
        }
        let udf_encoder = encoder.clone();
        let group_row_udf = datafusion::logical_expr::create_udf(
            "ivm_group_row",
            input_types,
            DataType::Binary,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(move |args: &[datafusion::logical_expr::ColumnarValue]| {
                let arrays =
                    datafusion::logical_expr::ColumnarValue::values_to_arrays(args)?;
                let rows = udf_encoder
                    .encode_columns(&arrays)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                Ok(datafusion::logical_expr::ColumnarValue::Array(Arc::new(
                    rows,
                )))
            }),
        );
        context.register_udf(group_row_udf);
        if bootstrap {
            use futures::StreamExt;
            let frame = context.sql(&contrib_sql).await?;
            let mut stream = frame.execute_stream().await?;
            let state_schema = state.schema.clone();
            let mv_schema = view.mv.schema.clone();
            let group_keys = view.group_keys.clone();
            let mut commit_ids: Vec<String> = Vec::new();
            let mut state_chunk: Vec<RecordBatch> = Vec::new();
            let mut mv_chunk: Vec<RecordBatch> = Vec::new();
            let mut chunk_rows = 0usize;
            let mut contrib_rows = 0usize;
            let mut state_writes = 0usize;
            let mut mv_writes = 0usize;
            let mut write_elapsed = std::time::Duration::ZERO;
            let bootstrap_started = std::time::Instant::now();
            let mut first_batch_elapsed = None;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                if first_batch_elapsed.is_none() {
                    first_batch_elapsed = Some(bootstrap_started.elapsed());
                }
                contrib_rows += batch.num_rows();
                let state_batch = Self::bootstrap_state_batch(
                    &batch,
                    &state_schema,
                    &group_keys,
                    epoch,
                )?;
                let mv_batch =
                    Self::bootstrap_mv_batch(&batch, &mv_schema, &group_keys, epoch)?;
                chunk_rows += state_batch.num_rows();
                state_chunk.push(state_batch);
                mv_chunk.push(mv_batch);
                if chunk_rows >= crate::table::MAX_STATE_APPEND_ROWS {
                    let write_started = std::time::Instant::now();
                    let state_batches = std::mem::take(&mut state_chunk);
                    let mv_batches = std::mem::take(&mut mv_chunk);
                    let (state_ids, mv_ids) = futures::future::try_join(
                        state.append_batches(&self.client, state_batches),
                        view.mv.append_batches(&self.client, mv_batches),
                    )
                    .await?;
                    commit_ids.extend(state_ids);
                    commit_ids.extend(mv_ids);
                    write_elapsed += write_started.elapsed();
                    state_writes += 1;
                    mv_writes += 1;
                    chunk_rows = 0;
                }
            }
            if !state_chunk.is_empty() {
                let write_started = std::time::Instant::now();
                let (state_ids, mv_ids) = futures::future::try_join(
                    state.append_batches(&self.client, state_chunk),
                    view.mv.append_batches(&self.client, mv_chunk),
                )
                .await?;
                commit_ids.extend(state_ids);
                commit_ids.extend(mv_ids);
                write_elapsed += write_started.elapsed();
                state_writes += 1;
                mv_writes += 1;
            }
            tracing::info!(
                contrib_rows,
                state_writes,
                mv_writes,
                first_batch_elapsed = ?first_batch_elapsed,
                write_elapsed = ?write_elapsed,
                total_elapsed = ?bootstrap_started.elapsed(),
                "multi-agg bootstrap finished"
            );
            self.checkpoint_epoch_states(&record).await?;
            let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
            self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
                .await?;
            self.advance_cursors(&view.view_id, window.cursors).await?;
            return Ok(Some(epoch));
        }
        use futures::StreamExt;
        let contrib = if let Some(keyed) = &keyed_contributions {
            keyed.frame.clone()
        } else {
            context.sql(&contrib_sql).await?
        };
        let state_partitions =
            self.changed_partition_restriction(&window, &state.range_partition_columns);
        let state_groups = if let Some(restriction) = state_partitions.as_deref() {
            state
                .current_groups_for_partitions(&self.client, restriction)
                .await?
        } else {
            state.current_groups(&self.client).await?
        };
        let state_bucket_num = if state.bucket_columns.len() == 1
            && state.bucket_columns[0] == IVM_GROUP_ROW_COLUMN
        {
            state
                .hash_bucket_num
                .parse::<usize>()
                .ok()
                .filter(|count| *count > 1)
                .unwrap_or(1)
        } else {
            1
        };
        let state_groups_by_bucket =
            super::keyed_plan::groups_by_bucket(state_groups, state_bucket_num);
        let contrib_plan = contrib.create_physical_plan().await?;
        let contrib_plan = if state_bucket_num > 1 {
            super::keyed_plan::bucket_repartition(
                contrib_plan,
                IVM_GROUP_ROW_COLUMN,
                state_bucket_num,
            )?
        } else {
            contrib_plan
        };
        let lookup_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(
            super::keyed_plan::IvmPkLookupExec::new(
                contrib_plan,
                state.clone(),
                state_groups_by_bucket,
                IVM_GROUP_ROW_COLUMN,
                state_bucket_num,
            )?
            // A group with no old state row is a new group: emit the
            // contribution with a null old-state side so the merge plan
            // can insert it.
            .with_multi_match(false),
        );
        let measure_columns = measures
            .iter()
            .map(|(_, column, _)| column.clone())
            .collect::<Vec<_>>();
        let merge_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            Arc::new(super::keyed_plan::IvmStateMergeExec::new(
                lookup_plan,
                state.schema.clone(),
                view.group_keys.clone(),
                measure_columns,
                epoch,
            )?);
        let merge_context = refresh_session();
        let streams = datafusion::physical_plan::execute_stream_partitioned(
            merge_plan,
            merge_context.task_ctx(),
        )?;
        let mv_schema = view.mv.schema.clone();
        let mut state_chunk: Vec<RecordBatch> = Vec::new();
        let mut mv_chunk: Vec<RecordBatch> = Vec::new();
        let mut chunk_rows = 0usize;
        let mut commit_ids: Vec<String> = Vec::new();
        let mut state_writes = 0usize;
        let mut mv_writes = 0usize;
        let mut state_keys = 0usize;
        let write_started = std::time::Instant::now();
        for mut output in streams {
            while let Some(batch) = output.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                state_keys += batch.num_rows();
                let mv_batch = Self::state_to_mv_batch(&batch, &mv_schema)?;
                chunk_rows += batch.num_rows();
                state_chunk.push(batch);
                mv_chunk.push(mv_batch);
                if chunk_rows >= crate::table::MAX_STATE_APPEND_ROWS {
                    let (state_ids, mv_ids) = futures::future::try_join(
                        state.append_batches(
                            &self.client,
                            std::mem::take(&mut state_chunk),
                        ),
                        view.mv
                            .append_batches(&self.client, std::mem::take(&mut mv_chunk)),
                    )
                    .await?;
                    commit_ids.extend(state_ids);
                    commit_ids.extend(mv_ids);
                    state_writes += 1;
                    mv_writes += 1;
                    chunk_rows = 0;
                }
            }
        }
        if !state_chunk.is_empty() {
            let (state_ids, mv_ids) = futures::future::try_join(
                state.append_batches(&self.client, state_chunk),
                view.mv.append_batches(&self.client, mv_chunk),
            )
            .await?;
            commit_ids.extend(state_ids);
            commit_ids.extend(mv_ids);
            state_writes += 1;
            mv_writes += 1;
        }
        tracing::info!(
            state_keys,
            state_writes,
            mv_writes,
            elapsed = ?write_started.elapsed(),
            "multi-agg keyed incremental finished"
        );
        self.checkpoint_epoch_states(&record).await?;
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Refresh a multi-aggregate view from its row-level source state.
    ///
    /// The state stores every current source row keyed by the group keys and
    /// the source primary key.  A window updates only the changed rows, then
    /// reads the state rows of the affected groups by a group-key prefix and
    /// recomputes those groups.  Arbitrary aggregates (COUNT, MAX, scalar
    /// group expressions) therefore never scan the full source.
    async fn refresh_multi_agg_rows(
        &self,
        view: &MultiAggView,
        state: &IvmTable,
    ) -> Result<Option<i64>> {
        self.register_multi_agg_view(view).await?;
        self.register_state_tables(&view.view_id, &[(StateRole::MultiAggRows, state)])
            .await?;
        let window = self
            .collect_source_window_for_filter(
                &view.view_id,
                &view.source,
                view.filter.as_deref(),
            )
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }
        let identity = window.identity.clone();
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let context = refresh_session();
        let delta =
            self.groups_table_frame(&context, &view.source, window.added_files.clone())?;
        // The state is already projected to the columns the view reads.
        // Computed group keys are part of its schema as well.  The epoch is
        // bookkeeping; the source change marker stays because the recompute
        // SQL reads it.
        let state_fields = state
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|column| {
                column != IVM_EPOCH_COLUMN
                    && (view.source.schema.field_with_name(column).is_ok()
                        || view.group_keys.contains(column))
            })
            .collect::<Vec<_>>();
        let pk_exprs = view
            .source
            .primary_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        let bootstrap = window
            .before_versions
            .values()
            .all(|version| version.is_none());
        let before = if bootstrap {
            dataframe(&context, Vec::new(), &view.source.schema)?
        } else {
            let contribution = delta.clone().select(pk_exprs)?.distinct()?;
            let before = self
                .lookup_source_before_by_pks(
                    &context,
                    &view.source,
                    &window,
                    contribution,
                )
                .await?;
            filter_deletes(before, change_column(&view.source))?
        };
        // `before` feeds the old-side state ops, the affected groups and the
        // SQL old CTE.  Keep one materialized copy so the keyed before-image
        // lookup runs once per window.
        let before_batches = before.clone().collect().await?;
        let before = if before_batches.is_empty() {
            before
        } else {
            context.read_batches(before_batches)?
        };
        let delta_clean = filter_deletes(delta.clone(), change_column(&view.source))?;
        let before_keys = project_group_keys_frame(
            &context,
            before.clone(),
            &view.source.schema,
            &view.group_keys,
            &view.group_exprs,
        )?;
        let delta_keys = project_group_keys_frame(
            &context,
            delta.clone(),
            &view.source.schema,
            &view.group_keys,
            &view.group_exprs,
        )?;
        let delta_clean_keys = project_group_keys_frame(
            &context,
            delta_clean,
            &view.source.schema,
            &view.group_keys,
            &view.group_exprs,
        )?;
        let before_data = project_frame(before_keys.clone(), &state_fields)?;
        let delta_data = project_frame(delta_clean_keys, &state_fields)?;
        let before_ops = before_data
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let new_ops = delta_data
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let mut state_ops = before_ops.union(new_ops)?;
        let mut sort_exprs = state
            .primary_keys
            .iter()
            .map(|key| column_expr(key))
            .collect::<Vec<_>>();
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        state_ops = state_ops.sort_by(sort_exprs)?;
        let mut commit_ids = state
            .append_stream(&self.client, state_ops.execute_stream().await?)
            .await?;

        context.register_table("old", before_keys.clone().into_view())?;
        context.register_table("delta", delta_keys.clone().into_view())?;
        let before_groups = project_frame(before_keys, &view.group_keys)?.distinct()?;
        let delta_groups = project_frame(delta_keys, &view.group_keys)?.distinct()?;
        let affected = before_groups.union(delta_groups)?.distinct()?;
        // A state/MV keyed by the group keys can only hold rows of the
        // partitions the affected groups belong to.  The window's changed
        // partitions are exactly those when the range columns are grouped by.
        let state_partitions = if !state.range_partition_columns.is_empty()
            && state
                .range_partition_columns
                .iter()
                .all(|column| view.group_keys.contains(column))
        {
            self.changed_partition_restriction(&window, &state.range_partition_columns)
        } else {
            None
        };
        let state_rows = match &state_partitions {
            Some(restriction) => {
                self.lookup_source_by_join_keys_in_partitions(
                    &context,
                    state,
                    affected.clone(),
                    &view.group_keys,
                    &view.group_keys,
                    true,
                    restriction,
                )
                .await?
            }
            None => {
                self.lookup_source_by_join_keys(
                    &context,
                    state,
                    affected.clone(),
                    &view.group_keys,
                    &view.group_keys,
                    true,
                )
                .await?
            }
        };
        let src = project_frame(state_rows, &state_fields)?;
        context.register_table("src", src.into_view())?;
        let parts = view.parts()?;
        // The row-state refresh already knows the affected groups.  When the
        // MV is keyed by exactly those groups and there is no lookup column,
        // probe only those rows instead of scanning the whole MV for the
        // delete/insert sides.
        if parts.lookup.is_none() && view.mv.primary_keys == view.group_keys {
            let mv_rows = self
                .lookup_source_by_join_keys(
                    &context,
                    &view.mv,
                    affected.clone(),
                    &view.group_keys,
                    &view.group_keys,
                    true,
                )
                .await?;
            context.register_table("mv", mv_rows.into_view())?;
        } else {
            crate::observability::note_full_scan(&view.mv.table_name);
            context.register_table("mv", Arc::new(self.table_provider(&view.mv)))?;
        }
        // A bootstrap writes only inserts into an empty MV: the delete side
        // and its key ordering are unnecessary, and the global ORDER BY would
        // sort the whole result in one plan.  Rebuild SQL streams the same
        // groups in whatever order the aggregate produces them.
        let sql = if bootstrap {
            recompute_rebuild_sql(&parts, epoch)
        } else {
            recompute_refresh_sql(&parts, epoch, false)
        };
        let frame = context.sql(&sql).await?;
        commit_ids.extend(
            view.mv
                .append_stream(&self.client, frame.execute_stream().await?)
                .await?,
        );
        self.checkpoint_epoch_states(&record).await?;
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a multi-aggregate view from the full source state.
    pub async fn rebuild_multi_agg(&self, view: &MultiAggView) -> Result<i64> {
        self.register_multi_agg_view(view).await?;
        validate_multi_agg_view(view)?;
        if let Some(existing) = self
            .metadata
            .get_state(&view.view_id, StateRole::MultiAggRows)
            .await?
        {
            let state = self.open_table_by_id(&existing.table_id).await?;
            self.metadata
                .set_view_status(&view.view_id, "rebuilding")
                .await?;
            let generation = self.metadata.bump_generation(&view.view_id).await?;
            self.metadata.delete_cursors(&view.view_id).await?;
            view.mv.truncate(&self.client).await?;
            state.truncate(&self.client).await?;
            let Some(epoch) = self.refresh_multi_agg_rows(view, &state).await? else {
                return Err(report!(
                    "multi-agg rebuild for {} found no source files (generation {generation})",
                    view.view_id
                ));
            };
            self.metadata
                .set_view_status(&view.view_id, "active")
                .await?;
            return Ok(epoch);
        }
        if Self::multi_agg_state_eligible(view)
            && let Some(state) = &view.state
        {
            self.metadata
                .set_view_status(&view.view_id, "rebuilding")
                .await?;
            let generation = self.metadata.bump_generation(&view.view_id).await?;
            self.metadata.delete_cursors(&view.view_id).await?;
            view.mv.truncate(&self.client).await?;
            state.truncate(&self.client).await?;
            let Some(epoch) = self.refresh_multi_agg_state(view, state).await? else {
                return Err(report!(
                    "multi-agg rebuild for {} found no source files (generation {generation})",
                    view.view_id
                ));
            };
            self.metadata
                .set_view_status(&view.view_id, "active")
                .await?;
            return Ok(epoch);
        }
        let parts = view.parts()?;
        validate_recompute_view(&parts)?;
        self.rebuild_recomputed(&parts).await
    }
}
