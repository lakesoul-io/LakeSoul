// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The pair-keyed join views: inner, lookup `LEFT`, pair-keyed left/right/full
//! and cross joins.

use super::*;

/// Which side of a join an output column comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinSide {
    /// The left (driving) source.
    Left,
    /// The right source.
    Right,
}

/// One payload column of a wide join output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinOutputColumn {
    /// The source side the column comes from.
    pub side: JoinSide,
    /// The source column name.
    pub column: String,
    /// The materialized view column name (the select alias, or the source
    /// column name).
    pub name: String,
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
    /// The right source's key columns when they differ from the left ones;
    /// empty means the keys share their names.
    pub right_keys: Vec<String>,
    /// The payload column of the left source (any type).
    pub left_value: String,
    /// The payload column of the right source (any type).
    pub right_value: String,
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the contributing right rows must satisfy.
    pub right_filter: Option<String>,
    /// An optional filter over the joined pair's payload columns
    /// (`left_value` / `right_value`); non-equality join conditions.
    pub pair_filter: Option<String>,
    /// The wide output columns; empty means the compact payload shape.
    pub output_columns: Vec<JoinOutputColumn>,
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
            right_keys: Vec::new(),
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            left_filter: None,
            right_filter: None,
            pair_filter: None,
            refresh_interval_ms: 0,
        }
    }

    /// Only left rows matching `filter` contribute to the view.
    pub fn with_left_filter(mut self, filter: impl Into<String>) -> Self {
        self.left_filter = Some(filter.into());
        self
    }

    /// Only right rows matching `filter` contribute to the view.
    pub fn with_right_filter(mut self, filter: impl Into<String>) -> Self {
        self.right_filter = Some(filter.into());
        self
    }

    /// The right source's join keys when they differ from the left ones.
    pub fn with_right_keys(mut self, right_keys: Vec<String>) -> Self {
        self.right_keys = right_keys;
        self
    }

    /// Materialize a wide output instead of the single payload per side.
    pub fn with_output_columns(mut self, output_columns: Vec<JoinOutputColumn>) -> Self {
        self.output_columns = output_columns;
        self
    }

    /// Only joined pairs matching `filter` (over `left_value` /
    /// `right_value`) contribute to the view.
    pub fn with_pair_filter(mut self, filter: impl Into<String>) -> Self {
        self.pair_filter = Some(filter.into());
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Join {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            output_table_id: self.output.table_id.clone(),
            join_keys: self.join_keys.clone(),
            right_keys: self.right_keys.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            pair_filter: self.pair_filter.clone(),
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
    keyed_join_schema_for(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        left_value,
        right_value,
        JoinSchema::Inner,
    )
}

/// The schema of a [`LeftJoinView`] output: the join keys keep the left
/// nullability and the right payload and right row identities are nullable.
pub fn left_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    keyed_join_schema_for(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        left_value,
        right_value,
        JoinSchema::Left,
    )
}

#[allow(clippy::too_many_arguments)]
/// The schema of a [`FullJoinView`] output: both payloads and both row
/// identities are nullable.
pub fn full_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    keyed_join_schema_for(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        left_value,
        right_value,
        JoinSchema::Full,
    )
}

/// The schema of a [`CrossJoinView`] materialized view: one payload column
/// from each side, both row identities, the row kind and the epoch.
pub fn cross_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "a cross join view needs primary keys on both sources"
        ));
    }
    let left_field = left_schema.field_with_name(left_value)?;
    let right_field = right_schema.field_with_name(right_value)?;
    let mut fields = Vec::new();
    fields.push(Arc::new(Field::new(
        "left_value",
        left_field.data_type().clone(),
        left_field.is_nullable(),
    )));
    fields.push(Arc::new(Field::new(
        "right_value",
        right_field.data_type().clone(),
        right_field.is_nullable(),
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

/// The fields of a wide join output, in order.
fn wide_output_fields(
    left_schema: &Schema,
    right_schema: &Schema,
    output_columns: &[JoinOutputColumn],
) -> Result<Vec<Arc<Field>>> {
    wide_output_fields_with(left_schema, right_schema, output_columns, false, false)
}

/// The fields of a wide join output, with the nullability of either side
/// forced (outer joins pad the missing side).
fn wide_output_fields_with(
    left_schema: &Schema,
    right_schema: &Schema,
    output_columns: &[JoinOutputColumn],
    left_nullable: bool,
    right_nullable: bool,
) -> Result<Vec<Arc<Field>>> {
    let mut names = std::collections::HashSet::new();
    let mut fields = Vec::with_capacity(output_columns.len());
    for column in output_columns {
        if !names.insert(column.name.as_str()) {
            return Err(report!(
                "join output column {} is materialized twice",
                column.name
            ));
        }
        let (schema, forced) = match column.side {
            JoinSide::Left => (left_schema, left_nullable),
            JoinSide::Right => (right_schema, right_nullable),
        };
        let field = schema.field_with_name(&column.column).map_err(|_| {
            report!(
                "join output column {} is not in the {} source",
                column.column,
                match column.side {
                    JoinSide::Left => "left",
                    JoinSide::Right => "right",
                }
            )
        })?;
        fields.push(Arc::new(Field::new(
            column.name.clone(),
            field.data_type().clone(),
            forced || field.is_nullable(),
        )));
    }
    Ok(fields)
}

/// The schema of a wide outer [`LeftJoinView`] / [`FullJoinView`] output:
/// the fields are nullable because an unmatched row pads one side.
pub fn wide_outer_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "an outer join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for key in join_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            key,
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    fields.extend(wide_output_fields_with(
        left_schema,
        right_schema,
        output_columns,
        true,
        true,
    )?);
    for key in left_primary_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            left_pk_alias(key),
            field.data_type().clone(),
            true,
        )));
    }
    for key in right_primary_keys {
        let field = right_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            right_pk_alias(key),
            field.data_type().clone(),
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

/// The schema of a wide [`LookupJoinView`] output: the left fields keep their
/// nullability and the right fields are nullable (NULL without a match).
pub fn wide_lookup_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    join_keys: &[String],
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() {
        return Err(report!(
            "a lookup join view needs primary keys on the left source"
        ));
    }
    let mut fields = Vec::new();
    for key in join_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            key,
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    fields.extend(wide_output_fields_with(
        left_schema,
        right_schema,
        output_columns,
        false,
        true,
    )?);
    for key in left_primary_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(key, field.data_type().clone(), false)));
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

/// The schema of a wide append-only [`JoinView`] output: the join keys, the
/// wide output columns and the epoch.
pub fn wide_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    join_keys: &[String],
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    for key in join_keys {
        fields.push(Arc::new(Field::new(
            key,
            field_type(left_schema, key)?,
            false,
        )));
    }
    fields.extend(wide_output_fields(
        left_schema,
        right_schema,
        output_columns,
    )?);
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a wide keyed [`JoinView`] or [`CrossJoinView`] output: the
/// join keys, the wide output columns, both row identities, the row kind and
/// the epoch.
pub fn wide_keyed_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    output_columns: &[JoinOutputColumn],
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
    fields.extend(wide_output_fields(
        left_schema,
        right_schema,
        output_columns,
    )?);
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

/// Which nullability an outer pair-keyed join keeps.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinSchema {
    Inner,
    Left,
    Full,
}

fn keyed_join_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    left_value: &str,
    right_value: &str,
    outer: JoinSchema,
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "a keyed join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for key in join_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            key,
            field.data_type().clone(),
            outer != JoinSchema::Inner && field.is_nullable(),
        )));
    }
    fields.push(Arc::new(Field::new(
        "left_value",
        field_type(left_schema, left_value)?,
        outer != JoinSchema::Inner
            || left_schema
                .field_with_name(left_value)
                .map(|field| field.is_nullable())
                .unwrap_or(true),
    )));
    fields.push(Arc::new(Field::new(
        "right_value",
        field_type(right_schema, right_value)?,
        outer != JoinSchema::Inner
            || right_schema
                .field_with_name(right_value)
                .map(|field| field.is_nullable())
                .unwrap_or(true),
    )));
    for key in left_primary_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            left_pk_alias(key),
            field.data_type().clone(),
            outer != JoinSchema::Inner,
        )));
    }
    for key in right_primary_keys {
        let field = right_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            right_pk_alias(key),
            field.data_type().clone(),
            outer != JoinSchema::Inner,
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

/// A `LEFT JOIN` view over two keyed sources: every left row with all of its
/// matching right rows, or a NULL-padded pair when nothing matches.
///
/// The output is keyed by the pair of row identities; a refresh rewrites the
/// affected left rows wholesale (all of their pairs), so a right change only
/// touches the left rows that reference it.
#[derive(Debug, Clone)]
pub struct LeftJoinView {
    /// The view id.
    pub view_id: String,
    /// The left source (keyed/upsert).
    pub left: IvmTable,
    /// The right source (keyed/upsert).
    pub right: IvmTable,
    /// The output table, keyed by both row identities.
    pub output: IvmTable,
    /// The equi-join keys, present in both sources.
    pub join_keys: Vec<String>,
    /// The right source's key columns when they differ from the left ones;
    /// empty means the keys share their names.
    pub right_keys: Vec<String>,
    /// The payload column of the left source.
    pub left_value: String,
    /// The payload column of the right source.
    pub right_value: String,
    /// The wide output columns; empty means the compact payload shape.
    pub output_columns: Vec<JoinOutputColumn>,
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the contributing right rows must satisfy.
    pub right_filter: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl LeftJoinView {
    /// A new view with no refresh interval hint.
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

    /// A new view over several equi-join keys.
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
            right_keys: Vec::new(),
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            left_filter: None,
            right_filter: None,
            refresh_interval_ms: 0,
        }
    }

    /// Only left rows matching `filter` contribute to the view.
    pub fn with_left_filter(mut self, filter: impl Into<String>) -> Self {
        self.left_filter = Some(filter.into());
        self
    }

    /// Only right rows matching `filter` contribute to the view.
    pub fn with_right_filter(mut self, filter: impl Into<String>) -> Self {
        self.right_filter = Some(filter.into());
        self
    }

    /// The right source's join keys when they differ from the left ones.
    pub fn with_right_keys(mut self, right_keys: Vec<String>) -> Self {
        self.right_keys = right_keys;
        self
    }

    /// Materialize a wide output instead of the single payload per side.
    pub fn with_output_columns(mut self, output_columns: Vec<JoinOutputColumn>) -> Self {
        self.output_columns = output_columns;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::LeftJoin {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            output_table_id: self.output.table_id.clone(),
            join_keys: self.join_keys.clone(),
            right_keys: self.right_keys.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
        }
    }
}

/// A `FULL JOIN` view over two keyed sources: the matching pairs plus the
/// unmatched rows of either side (NULL-padded).
#[derive(Debug, Clone)]
pub struct FullJoinView {
    /// The view id.
    pub view_id: String,
    /// The left source (keyed/upsert).
    pub left: IvmTable,
    /// The right source (keyed/upsert).
    pub right: IvmTable,
    /// The output table, keyed by both row identities.
    pub output: IvmTable,
    /// The equi-join keys, present in both sources.
    pub join_keys: Vec<String>,
    /// The right source's key columns when they differ from the left ones;
    /// empty means the keys share their names.
    pub right_keys: Vec<String>,
    /// The payload column of the left source.
    pub left_value: String,
    /// The payload column of the right source.
    pub right_value: String,
    /// The wide output columns; empty means the compact payload shape.
    pub output_columns: Vec<JoinOutputColumn>,
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the contributing right rows must satisfy.
    pub right_filter: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl FullJoinView {
    /// A new view with no refresh interval hint.
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

    /// A new view over several equi-join keys.
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
            right_keys: Vec::new(),
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            left_filter: None,
            right_filter: None,
            refresh_interval_ms: 0,
        }
    }

    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &self.join_keys,
            right_keys: &self.right_keys,
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            pair_filter: None,
        }
    }

    /// Materialize a wide output instead of the single payload per side.
    pub fn with_output_columns(mut self, output_columns: Vec<JoinOutputColumn>) -> Self {
        self.output_columns = output_columns;
        self
    }

    /// The right source's join keys when they differ from the left ones.
    pub fn with_right_keys(mut self, right_keys: Vec<String>) -> Self {
        self.right_keys = right_keys;
        self
    }

    /// Only left rows matching `filter` contribute to the view.
    pub fn with_left_filter(mut self, filter: impl Into<String>) -> Self {
        self.left_filter = Some(filter.into());
        self
    }

    /// Only right rows matching `filter` contribute to the view.
    pub fn with_right_filter(mut self, filter: impl Into<String>) -> Self {
        self.right_filter = Some(filter.into());
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::FullJoin {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            output_table_id: self.output.table_id.clone(),
            join_keys: self.join_keys.clone(),
            right_keys: self.right_keys.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
        }
    }
}

/// A `CROSS JOIN` view over two keyed sources.
///
/// The output is keyed by both row identities; a refresh rewrites the pairs of
/// the identities that changed on either side.
#[derive(Debug, Clone)]
pub struct CrossJoinView {
    /// The view id.
    pub view_id: String,
    /// The left source table (keyed).
    pub left: IvmTable,
    /// The right source table (keyed).
    pub right: IvmTable,
    /// The output table, keyed by both row identities.
    pub output: IvmTable,
    /// The payload column of the left source.
    pub left_value: String,
    /// The payload column of the right source.
    pub right_value: String,
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the contributing right rows must satisfy.
    pub right_filter: Option<String>,
    /// An optional filter over the joined pair's payload columns
    /// (`left_value` / `right_value`); cross-side predicates.
    pub pair_filter: Option<String>,
    /// The wide output columns; empty means the compact payload shape.
    pub output_columns: Vec<JoinOutputColumn>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl CrossJoinView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        left: IvmTable,
        right: IvmTable,
        output: IvmTable,
        left_value: impl Into<String>,
        right_value: impl Into<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            left,
            right,
            output,
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            left_filter: None,
            right_filter: None,
            pair_filter: None,
            refresh_interval_ms: 0,
        }
    }

    /// Only left rows matching `filter` contribute to the view.
    pub fn with_left_filter(mut self, filter: impl Into<String>) -> Self {
        self.left_filter = Some(filter.into());
        self
    }

    /// Only right rows matching `filter` contribute to the view.
    pub fn with_right_filter(mut self, filter: impl Into<String>) -> Self {
        self.right_filter = Some(filter.into());
        self
    }

    /// Materialize a wide output instead of the single payload per side.
    pub fn with_output_columns(mut self, output_columns: Vec<JoinOutputColumn>) -> Self {
        self.output_columns = output_columns;
        self
    }

    /// Only joined pairs matching `filter` (over `left_value` /
    /// `right_value`) contribute to the view.
    pub fn with_pair_filter(mut self, filter: impl Into<String>) -> Self {
        self.pair_filter = Some(filter.into());
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::CrossJoin {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            output_table_id: self.output.table_id.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            pair_filter: self.pair_filter.clone(),
        }
    }

    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &[],
            right_keys: &[],
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            pair_filter: self.pair_filter.as_deref(),
        }
    }
}

/// A `LEFT JOIN` lookup view: every left row with the right row its join keys
/// reference (or NULL).
///
/// The right source must be keyed by the join keys, so a left row has at most
/// one match and the output is keyed by the left primary keys; a right change
/// recomputes the left rows that reference it.
#[derive(Debug, Clone)]
pub struct LookupJoinView {
    /// The view id.
    pub view_id: String,
    /// The left source (keyed/upsert).
    pub left: IvmTable,
    /// The right source, keyed by the join keys.
    pub right: IvmTable,
    /// The output table, keyed by the left primary keys.
    pub output: IvmTable,
    /// The left equi-join keys; the output key columns.
    pub join_keys: Vec<String>,
    /// The right equi-join keys, parallel to [`Self::join_keys`]; empty means
    /// the keys share their names with the left side.
    pub right_keys: Vec<String>,
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the referenced right rows must satisfy; a left row
    /// whose match is filtered out keeps NULLs.
    pub right_filter: Option<String>,
    /// The payload column of the left source.
    pub left_value: String,
    /// The payload column of the right source.
    pub right_value: String,
    /// The wide output columns; empty means the compact payload shape.
    pub output_columns: Vec<JoinOutputColumn>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl LookupJoinView {
    /// A new view with no refresh interval hint.
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

    /// A new view over several equi-join keys (the right primary keys).
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
            right_keys: Vec::new(),
            left_filter: None,
            right_filter: None,
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            refresh_interval_ms: 0,
        }
    }

    /// The right-side join keys: [`Self::right_keys`] or, when empty, the
    /// left key names.
    pub fn right_join_keys(&self) -> &[String] {
        if self.right_keys.is_empty() {
            &self.join_keys
        } else {
            &self.right_keys
        }
    }

    /// Only left rows matching `filter` contribute to the view.
    pub fn with_left_filter(mut self, filter: impl Into<String>) -> Self {
        self.left_filter = Some(filter.into());
        self
    }

    /// Only referenced right rows matching `filter` contribute to the view;
    /// a left row whose match is filtered out keeps NULLs.
    pub fn with_right_filter(mut self, filter: impl Into<String>) -> Self {
        self.right_filter = Some(filter.into());
        self
    }

    /// The right source's join keys when they differ from the left ones.
    pub fn with_right_keys(mut self, right_keys: Vec<String>) -> Self {
        self.right_keys = right_keys;
        self
    }

    /// Materialize a wide output instead of the single payload per side.
    pub fn with_output_columns(mut self, output_columns: Vec<JoinOutputColumn>) -> Self {
        self.output_columns = output_columns;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::LookupJoin {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            output_table_id: self.output.table_id.clone(),
            join_keys: self.join_keys.clone(),
            right_keys: self.right_keys.clone(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
        }
    }
}

/// The schema of a [`LookupJoinView`] output: the join keys, the left payload,
/// the (nullable) right payload, the left primary keys, the row kind and the
/// epoch.
pub fn lookup_join_view_schema_for(
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
            "a lookup join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for key in join_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            key,
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    let left_field = left_schema.field_with_name(left_value)?;
    fields.push(Arc::new(Field::new(
        "left_value",
        left_field.data_type().clone(),
        left_field.is_nullable(),
    )));
    fields.push(Arc::new(Field::new(
        "right_value",
        field_type(right_schema, right_value)?,
        true,
    )));
    for key in left_primary_keys {
        let field = left_schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(key, field.data_type().clone(), false)));
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

/// The `LEFT JOIN` projection of a lookup view: the left rows with their
/// referenced right payload (NULL without a match), plus the left identities.
fn lookup_join_projection(
    left: DataFrame,
    right: DataFrame,
    view: &LookupJoinView,
) -> Result<DataFrame> {
    let wide = !view.output_columns.is_empty();
    let mut left_columns = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    if wide {
        for column in view
            .output_columns
            .iter()
            .filter(|column| column.side == JoinSide::Left)
        {
            left_columns.push(
                col(column.column.as_str())
                    .alias(join_side_alias(JoinSide::Left, &column.name)),
            );
        }
    } else {
        left_columns.push(col(view.left_value.as_str()).alias("left_value"));
    }
    for key in &view.left.primary_keys {
        left_columns.push(col(key.as_str()));
    }
    let left = left.select(left_columns)?;

    let mut right_columns = view
        .right_join_keys()
        .iter()
        .zip(&view.join_keys)
        .map(|(right_key, left_key)| {
            col(right_key.as_str()).alias(format!("__right_{left_key}"))
        })
        .collect::<Vec<_>>();
    if wide {
        for column in view
            .output_columns
            .iter()
            .filter(|column| column.side == JoinSide::Right)
        {
            right_columns.push(
                col(column.column.as_str())
                    .alias(join_side_alias(JoinSide::Right, &column.name)),
            );
        }
    } else {
        right_columns.push(col(view.right_value.as_str()).alias("right_value"));
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
    let joined = left.join(right, JoinType::Left, &key_names, &right_key_refs, None)?;
    if !wide {
        return Ok(joined);
    }
    let mut output = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    for column in &view.output_columns {
        output.push(
            col(join_side_alias(column.side, &column.name).as_str())
                .alias(column.name.as_str()),
        );
    }
    for key in &view.left.primary_keys {
        output.push(col(key.as_str()));
    }
    Ok(joined.select(output)?)
}

/// The output columns of a lookup join, as expressions over the joined frame.
fn lookup_join_output_columns(view: &LookupJoinView) -> Vec<Expr> {
    let mut output = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    if view.output_columns.is_empty() {
        output.push(col("left_value"));
        output.push(col("right_value"));
    } else {
        output.extend(
            view.output_columns
                .iter()
                .map(|column| col(column.name.as_str())),
        );
    }
    output.extend(view.left.primary_keys.iter().map(|key| col(key.as_str())));
    output
}

/// Validate that a full join view can be maintained.
fn validate_full_join_view(view: &FullJoinView) -> Result<()> {
    let left = LeftJoinView {
        view_id: view.view_id.clone(),
        left: view.left.clone(),
        right: view.right.clone(),
        output: view.output.clone(),
        join_keys: view.join_keys.clone(),
        right_keys: view.right_keys.clone(),
        left_value: view.left_value.clone(),
        right_value: view.right_value.clone(),
        output_columns: view.output_columns.clone(),
        left_filter: view.left_filter.clone(),
        right_filter: view.right_filter.clone(),
        refresh_interval_ms: view.refresh_interval_ms,
    };
    validate_left_join_view(&left)
}

/// Validate that a cross join view can be maintained.
fn validate_cross_join_view(view: &CrossJoinView) -> Result<()> {
    for (side, table, value) in [
        ("left", &view.left, &view.left_value),
        ("right", &view.right, &view.right_value),
    ] {
        if table.primary_keys.is_empty() {
            return Err(report!(
                "cross join view {}: the {side} source needs a primary key",
                view.view_id
            ));
        }
        for key in &table.primary_keys {
            let field = table.schema.field_with_name(key).map_err(|_| {
                report!(
                    "cross join view {}: {side} key column {key} is not in the source",
                    view.view_id
                )
            })?;
            if field.is_nullable() {
                return Err(report!(
                    "cross join view {}: {side} key column {key} must be non-nullable",
                    view.view_id
                ));
            }
        }
        table.schema.field_with_name(value).map_err(|_| {
            report!(
                "cross join view {}: {side} value column {value} is not in the source",
                view.view_id
            )
        })?;
    }
    if !view.output_columns.is_empty() {
        wide_output_fields(&view.left.schema, &view.right.schema, &view.output_columns)?;
    }
    if view.left_filter.is_some() || view.right_filter.is_some() {
        let context = SessionContext::new();
        if let Some(filter) = &view.left_filter {
            parse_filter(&context, &view.left.schema, filter)?;
        }
        if let Some(filter) = &view.right_filter {
            parse_filter(&context, &view.right.schema, filter)?;
        }
    }
    if let Some(filter) = &view.pair_filter {
        if view.output_columns.is_empty() {
            validate_pair_filter(
                &view.left,
                &view.left_value,
                &view.right,
                &view.right_value,
                filter,
            )?;
        } else {
            validate_wide_pair_filter(
                &view.left,
                &view.right,
                &view.output_columns,
                filter,
            )?;
        }
    }
    Ok(())
}

/// Validate that a left join view can be maintained.
fn validate_left_join_view(view: &LeftJoinView) -> Result<()> {
    if view.join_keys.is_empty() {
        return Err(report!(
            "left join view {} needs at least one join key",
            view.view_id
        ));
    }
    if view.left.primary_keys.is_empty() || view.right.primary_keys.is_empty() {
        return Err(report!(
            "left join view {} needs primary keys on both sources",
            view.view_id
        ));
    }
    let right_keys = if view.right_keys.is_empty() {
        &view.join_keys
    } else {
        &view.right_keys
    };
    if right_keys.len() != view.join_keys.len() {
        return Err(report!(
            "left join view {}: right_keys must be parallel to join_keys",
            view.view_id
        ));
    }
    for (key, right_key) in view.join_keys.iter().zip(right_keys) {
        let left = view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "left join view {}: join key {key} is not in the left source",
                view.view_id
            )
        })?;
        let right = view.right.schema.field_with_name(right_key).map_err(|_| {
            report!(
                "left join view {}: join key {right_key} is not in the right source",
                view.view_id
            )
        })?;
        if left.data_type() != right.data_type() {
            return Err(report!(
                "left join view {}: join key {key} has different types on the two sources",
                view.view_id
            ));
        }
    }
    for key in view
        .left
        .primary_keys
        .iter()
        .chain(view.right.primary_keys.iter())
    {
        let table = if view.left.primary_keys.contains(key) {
            &view.left
        } else {
            &view.right
        };
        let field = table.schema.field_with_name(key).map_err(|_| {
            report!(
                "left join view {}: key column {key} is not in the source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "left join view {}: key column {key} must be non-nullable",
                view.view_id
            ));
        }
    }
    if view.output_columns.is_empty() {
        field_type(&view.left.schema, &view.left_value)?;
        field_type(&view.right.schema, &view.right_value)?;
    } else {
        wide_output_fields_with(
            &view.left.schema,
            &view.right.schema,
            &view.output_columns,
            true,
            true,
        )?;
    }
    if view.left_filter.is_some() || view.right_filter.is_some() {
        let context = SessionContext::new();
        if let Some(filter) = &view.left_filter {
            parse_filter(&context, &view.left.schema, filter)?;
        }
        if let Some(filter) = &view.right_filter {
            parse_filter(&context, &view.right.schema, filter)?;
        }
    }
    Ok(())
}

/// Validate that a lookup join view can be maintained.
fn validate_lookup_join_view(view: &LookupJoinView) -> Result<()> {
    if view.join_keys.is_empty() {
        return Err(report!(
            "lookup join view {} needs at least one join key",
            view.view_id
        ));
    }
    if let Some(filter) = &view.left_filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.left.schema, filter)?;
    }
    if let Some(filter) = &view.right_filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.right.schema, filter)?;
    }
    if view.left.primary_keys.is_empty() || view.right.primary_keys.is_empty() {
        return Err(report!(
            "lookup join view {} needs primary keys on both sources",
            view.view_id
        ));
    }
    if !view.right_keys.is_empty() && view.right_keys.len() != view.join_keys.len() {
        return Err(report!(
            "lookup join view {}: the right keys must match the join keys",
            view.view_id
        ));
    }
    let mut expected = view.right.primary_keys.clone();
    expected.sort();
    let mut keys = view.right_join_keys().to_vec();
    keys.sort();
    if expected != keys {
        return Err(report!(
            "lookup join view {}: the right source must be keyed by the join keys",
            view.view_id
        ));
    }
    for key in &view.join_keys {
        view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "lookup join view {}: join key {key} is not in the left source",
                view.view_id
            )
        })?;
    }
    for key in view.right_join_keys() {
        view.right.schema.field_with_name(key).map_err(|_| {
            report!(
                "lookup join view {}: join key {key} is not in the right source",
                view.view_id
            )
        })?;
    }
    for key in &view.left.primary_keys {
        let field = view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "lookup join view {}: key column {key} is not in the left source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "lookup join view {}: key column {key} must be non-nullable",
                view.view_id
            ));
        }
    }
    if view.output_columns.is_empty() {
        field_type(&view.left.schema, &view.left_value)?;
        field_type(&view.right.schema, &view.right_value)?;
    } else {
        wide_output_fields_with(
            &view.left.schema,
            &view.right.schema,
            &view.output_columns,
            false,
            true,
        )?;
    }
    Ok(())
}

/// Validate a pair filter against the payload columns of the two sources.
fn validate_pair_filter(
    left: &IvmTable,
    left_value: &str,
    right: &IvmTable,
    right_value: &str,
    filter: &str,
) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("left_value", field_type(&left.schema, left_value)?, true),
        Field::new("right_value", field_type(&right.schema, right_value)?, true),
    ]));
    let context = SessionContext::new();
    parse_filter(&context, &schema, filter)?;
    Ok(())
}

/// Validate a pair filter over a wide join's output columns; the filter
/// references the intermediate `__left_*` / `__right_*` names.
fn validate_wide_pair_filter(
    left: &IvmTable,
    right: &IvmTable,
    output_columns: &[JoinOutputColumn],
    filter: &str,
) -> Result<()> {
    let mut fields = Vec::with_capacity(output_columns.len());
    for column in output_columns {
        let (schema, alias) = match column.side {
            JoinSide::Left => {
                (&left.schema, join_side_alias(JoinSide::Left, &column.name))
            }
            JoinSide::Right => (
                &right.schema,
                join_side_alias(JoinSide::Right, &column.name),
            ),
        };
        fields.push(Field::new(alias, field_type(schema, &column.column)?, true));
    }
    let schema = Arc::new(Schema::new(fields));
    let context = SessionContext::new();
    parse_filter(&context, &schema, filter)?;
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
    if view.left_filter.is_some() || view.right_filter.is_some() {
        let context = SessionContext::new();
        if let Some(filter) = &view.left_filter {
            parse_filter(&context, &view.left.schema, filter)?;
        }
        if let Some(filter) = &view.right_filter {
            parse_filter(&context, &view.right.schema, filter)?;
        }
    }
    let right_keys = if view.right_keys.is_empty() {
        &view.join_keys
    } else {
        &view.right_keys
    };
    if right_keys.len() != view.join_keys.len() {
        return Err(report!(
            "join view {}: right_keys must be parallel to join_keys",
            view.view_id
        ));
    }
    for (key, right_key) in view.join_keys.iter().zip(right_keys) {
        let left = view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "join view {}: join key {key} is not in the left source",
                view.view_id
            )
        })?;
        let right = view.right.schema.field_with_name(right_key).map_err(|_| {
            report!(
                "join view {}: join key {right_key} is not in the right source",
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
    if view.output_columns.is_empty() {
        field_type(&view.left.schema, &view.left_value)?;
        field_type(&view.right.schema, &view.right_value)?;
    } else {
        wide_output_fields(&view.left.schema, &view.right.schema, &view.output_columns)?;
    }

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
        let expected = if view.output_columns.is_empty() {
            keyed_join_view_schema_for(
                &view.left.schema,
                &view.right.schema,
                &view.left.primary_keys,
                &view.right.primary_keys,
                &view.join_keys,
                &view.left_value,
                &view.right_value,
            )?
        } else {
            wide_keyed_join_view_schema_for(
                &view.left.schema,
                &view.right.schema,
                &view.left.primary_keys,
                &view.right.primary_keys,
                &view.join_keys,
                &view.output_columns,
            )?
        };
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
    if let Some(filter) = &view.pair_filter {
        if view.output_columns.is_empty() {
            validate_pair_filter(
                &view.left,
                &view.left_value,
                &view.right,
                &view.right_value,
                filter,
            )?;
        } else {
            validate_wide_pair_filter(
                &view.left,
                &view.right,
                &view.output_columns,
                filter,
            )?;
        }
    }
    Ok(())
}

/// Project an inner equi-join onto `(join keys, left value, right value)`.
///
/// The right side's key columns are aliased before the join because DataFusion
/// rejects duplicate qualified fields for the same name.
/// The right source's join key columns of a pair join (the left names when
/// the keys share them).
fn effective_right_keys<'a>(parts: &'a PairJoin<'_>) -> &'a [String] {
    if parts.right_keys.is_empty() {
        parts.join_keys
    } else {
        parts.right_keys
    }
}

/// The intermediate column name of a wide join output on one side.
pub(crate) fn join_side_alias(side: JoinSide, name: &str) -> String {
    match side {
        JoinSide::Left => format!("__left_{name}"),
        JoinSide::Right => format!("__right_{name}"),
    }
}

/// The intermediate name of a wide output column in a pair filter.
pub(crate) fn wide_pair_alias(column: &JoinOutputColumn) -> String {
    quote_ident(&join_side_alias(column.side, &column.name))
}

/// Apply a pair filter (over `left_value` / `right_value`) to the joined
/// pairs before the output projection.
pub(super) fn apply_pair_filter(
    joined: DataFrame,
    filter: Option<&str>,
) -> Result<DataFrame> {
    match filter {
        Some(filter) => {
            let context = SessionContext::new();
            let expression = context
                .state()
                .create_logical_expr(filter, joined.schema())
                .map_err(|error| report!("invalid pair filter {filter:?}: {error}"))?;
            Ok(joined.filter(expression)?)
        }
        None => Ok(joined),
    }
}

fn join_projection(
    left: DataFrame,
    right: DataFrame,
    view: &JoinView,
) -> Result<DataFrame> {
    let wide = !view.output_columns.is_empty();
    let left_keys = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    let right_keys = if view.right_keys.is_empty() {
        view.join_keys
            .iter()
            .map(|key| col(key.as_str()).alias(format!("__right_{key}")))
            .collect::<Vec<_>>()
    } else {
        view.right_keys
            .iter()
            .zip(view.join_keys.iter())
            .map(|(right, left)| col(right.as_str()).alias(format!("__right_{left}")))
            .collect::<Vec<_>>()
    };
    let mut left_columns = left_keys.clone();
    if wide {
        for column in view
            .output_columns
            .iter()
            .filter(|column| column.side == JoinSide::Left)
        {
            left_columns.push(
                col(column.column.as_str())
                    .alias(join_side_alias(JoinSide::Left, &column.name)),
            );
        }
    } else {
        left_columns.push(col(view.left_value.as_str()).alias("left_value"));
    }
    let left = left.select(left_columns)?;
    let mut right_columns = right_keys;
    if wide {
        for column in view
            .output_columns
            .iter()
            .filter(|column| column.side == JoinSide::Right)
        {
            right_columns.push(
                col(column.column.as_str())
                    .alias(join_side_alias(JoinSide::Right, &column.name)),
            );
        }
    } else {
        right_columns.push(col(view.right_value.as_str()).alias("right_value"));
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
    let joined = apply_pair_filter(joined, view.pair_filter.as_deref())?;

    let mut output = view
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    if wide {
        for column in &view.output_columns {
            output.push(
                col(join_side_alias(column.side, &column.name).as_str())
                    .alias(column.name.as_str()),
            );
        }
    } else {
        output.push(col("left_value"));
        output.push(col("right_value"));
    }
    Ok(joined.select(output)?)
}

/// Project an inner equi-join of two keyed sources onto
/// `(join keys, left value, right value, left pk*, right pk*)`.
fn keyed_join_projection(
    context: &SessionContext,
    left: DataFrame,
    right: DataFrame,
    parts: &PairJoin<'_>,
    join_type: JoinType,
    keys_from_right: bool,
) -> Result<DataFrame> {
    let wide = !parts.output_columns.is_empty();
    let mut left_columns = parts
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    if wide {
        for column in parts
            .output_columns
            .iter()
            .filter(|column| column.side == JoinSide::Left)
        {
            left_columns.push(
                col(column.column.as_str())
                    .alias(join_side_alias(JoinSide::Left, &column.name)),
            );
        }
    } else {
        left_columns.push(col(parts.left_value).alias("left_value"));
    }
    for key in parts.left_primary_keys {
        left_columns.push(col(key.as_str()).alias(left_pk_alias(key)));
    }
    let left = left.select(left_columns)?;

    let mut right_columns = effective_right_keys(parts)
        .iter()
        .zip(parts.join_keys.iter())
        .map(|(right, left)| col(right.as_str()).alias(format!("__right_{left}")))
        .collect::<Vec<_>>();
    if wide {
        for column in parts
            .output_columns
            .iter()
            .filter(|column| column.side == JoinSide::Right)
        {
            right_columns.push(
                col(column.column.as_str())
                    .alias(join_side_alias(JoinSide::Right, &column.name)),
            );
        }
    } else {
        right_columns.push(col(parts.right_value).alias("right_value"));
    }
    for key in parts.right_primary_keys {
        right_columns.push(col(key.as_str()).alias(right_pk_alias(key)));
    }
    let right = right.select(right_columns)?;

    let key_names = parts
        .join_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let right_key_names = parts
        .join_keys
        .iter()
        .map(|key| format!("__right_{key}"))
        .collect::<Vec<_>>();
    let right_key_refs = right_key_names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let joined = if key_names.is_empty() {
        // A cross join has no equality keys.
        let plan = datafusion::logical_expr::LogicalPlanBuilder::new(
            left.logical_plan().clone(),
        )
        .cross_join(right.logical_plan().clone())?
        .build()?;
        DataFrame::new(context.state(), plan)
    } else {
        left.join(right, join_type, &key_names, &right_key_refs, None)?
    };
    let joined = apply_pair_filter(joined, parts.pair_filter)?;

    // An unmatched right row carries its join key on the right side.
    let mut output = if keys_from_right {
        parts
            .join_keys
            .iter()
            .map(|key| col(format!("__right_{key}")).alias(key.as_str()))
            .collect::<Vec<_>>()
    } else {
        parts
            .join_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>()
    };
    if wide {
        for column in parts.output_columns {
            output.push(
                col(join_side_alias(column.side, &column.name).as_str())
                    .alias(column.name.as_str()),
            );
        }
    } else {
        output.push(col("left_value"));
        output.push(col("right_value"));
    }
    for key in parts.left_primary_keys {
        output.push(col(left_pk_alias(key)));
    }
    for key in parts.right_primary_keys {
        output.push(col(right_pk_alias(key)));
    }
    Ok(joined.select(output)?)
}

/// The output columns of a keyed join in schema order.
fn keyed_join_output_columns(
    parts: &PairJoin<'_>,
) -> Vec<datafusion::logical_expr::Expr> {
    let mut output = parts
        .join_keys
        .iter()
        .map(|key| col(key.as_str()))
        .collect::<Vec<_>>();
    if parts.output_columns.is_empty() {
        output.push(col("left_value"));
        output.push(col("right_value"));
    } else {
        for column in parts.output_columns {
            output.push(col(column.name.as_str()));
        }
    }
    for key in parts.left_primary_keys {
        output.push(col(left_pk_alias(key).as_str()));
    }
    for key in parts.right_primary_keys {
        output.push(col(right_pk_alias(key).as_str()));
    }
    output
}

/// The shape of a pair-keyed join used by the projection helpers.
struct PairJoin<'a> {
    join_keys: &'a [String],
    /// The right source's key columns when they differ from `join_keys`.
    right_keys: &'a [String],
    left_value: &'a str,
    right_value: &'a str,
    /// The wide output columns; empty means the compact payload shape.
    output_columns: &'a [JoinOutputColumn],
    left_primary_keys: &'a [String],
    right_primary_keys: &'a [String],
    /// An optional filter over `left_value` / `right_value` the joined pair
    /// must satisfy (a non-equality join condition).
    pair_filter: Option<&'a str>,
}

impl JoinView {
    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &self.join_keys,
            right_keys: &self.right_keys,
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            pair_filter: self.pair_filter.as_deref(),
        }
    }
}

impl LeftJoinView {
    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &self.join_keys,
            right_keys: &self.right_keys,
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            pair_filter: None,
        }
    }
}

/// Whether a join view takes the keyed (upsert) path.
fn join_is_keyed(view: &JoinView) -> bool {
    !view.left.primary_keys.is_empty() || !view.right.primary_keys.is_empty()
}

impl IvmRuntime {
    /// Persist a join view spec (idempotent).
    pub async fn register_join_view(&self, view: &JoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.output)])
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
            // The append-only decomposition reads one as-of before state per
            // side; that only stays consistent for a single partition.
            self.ensure_unpartitioned(&view.left).await?;
            self.ensure_unpartitioned(&view.right).await?;
        }

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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        if keyed {
            let left_now = apply_side_filter(
                &context,
                filter_deletes(
                    dataframe(
                        &context,
                        view.left.read_current(&self.client).await?,
                        &view.left.schema,
                    )?,
                    change_column(&view.left),
                )?,
                &view.left.schema,
                view.left_filter.as_deref(),
            )?;
            let right_now = apply_side_filter(
                &context,
                filter_deletes(
                    dataframe(
                        &context,
                        view.right.read_current(&self.client).await?,
                        &view.right.schema,
                    )?,
                    change_column(&view.right),
                )?,
                &view.right.schema,
                view.right_filter.as_deref(),
            )?;
            let delta_left = dataframe(
                &context,
                view.left
                    .read_partition_files(left_window.added_files.clone())
                    .await?,
                &view.left.schema,
            )?;
            let delta_right = dataframe(
                &context,
                view.right
                    .read_partition_files(right_window.added_files.clone())
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
            let current = keyed_join_projection(
                &context,
                left_rows,
                right_now,
                &view.parts(),
                JoinType::Inner,
                false,
            )?
            .union(keyed_join_projection(
                &context,
                left_now,
                right_rows,
                &view.parts(),
                JoinType::Inner,
                false,
            )?)?
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
                .select(keyed_join_output_columns(&view.parts()))?
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
                    commit_ids
                        .extend(view.output.append_batch(&self.client, batch).await?);
                }
            }
        } else {
            let left_delta = view
                .left
                .read_partition_files(left_window.added_files.clone())
                .await?;
            let right_delta = view
                .right
                .read_partition_files(right_window.added_files.clone())
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
                    filtered_frame(
                        &context,
                        left_delta.clone(),
                        &view.left.schema,
                        view.left_filter.as_deref(),
                    )?,
                    filtered_frame(
                        &context,
                        right_before.clone(),
                        &view.right.schema,
                        view.right_filter.as_deref(),
                    )?,
                    view,
                )?);
            }
            if !left_before.is_empty() && !right_delta.is_empty() {
                terms.push(join_projection(
                    filtered_frame(
                        &context,
                        left_before.clone(),
                        &view.left.schema,
                        view.left_filter.as_deref(),
                    )?,
                    filtered_frame(
                        &context,
                        right_delta.clone(),
                        &view.right.schema,
                        view.right_filter.as_deref(),
                    )?,
                    view,
                )?);
            }
            if !left_delta.is_empty() && !right_delta.is_empty() {
                terms.push(join_projection(
                    filtered_frame(
                        &context,
                        left_delta.clone(),
                        &view.left.schema,
                        view.left_filter.as_deref(),
                    )?,
                    filtered_frame(
                        &context,
                        right_delta.clone(),
                        &view.right.schema,
                        view.right_filter.as_deref(),
                    )?,
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
                        commit_ids
                            .extend(view.output.append_batch(&self.client, batch).await?);
                    }
                }
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Refresh a lookup join view: recompute the affected left rows (changed
    /// left rows plus the left rows whose join key changed on the right) and
    /// rewrite them with the current right payload.
    pub async fn refresh_lookup_join(
        &self,
        view: &LookupJoinView,
    ) -> Result<Option<i64>> {
        self.register_lookup_join_view(view).await?;
        validate_lookup_join_view(view)?;

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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let mut left_now = filter_deletes(
            dataframe(
                &context,
                view.left.read_current(&self.client).await?,
                &view.left.schema,
            )?,
            change_column(&view.left),
        )?;
        if let Some(filter) = view.left_filter.as_deref() {
            left_now =
                left_now.filter(parse_filter(&context, &view.left.schema, filter)?)?;
        }
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(
                    &context,
                    view.right.read_current(&self.client).await?,
                    &view.right.schema,
                )?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let mv = dataframe(
            &context,
            view.output.read_current(&self.client).await?,
            &view.output.schema,
        )?;

        let key_names = view
            .left
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let key_exprs = view
            .left
            .primary_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        let join_key_names = view
            .join_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        // The changed left rows plus the left rows referencing a changed
        // right row.
        let mut affected = left_now.clone().select(key_exprs.clone())?.limit(0, None)?;
        if !left_window.added_files.is_empty() {
            let delta_left = dataframe(
                &context,
                view.left
                    .read_partition_files(left_window.added_files.clone())
                    .await?,
                &view.left.schema,
            )?;
            affected = delta_left
                .select(key_exprs.clone())?
                .distinct()?
                .union(affected)?;
        }
        if !right_window.added_files.is_empty() {
            let delta_right = dataframe(
                &context,
                view.right
                    .read_partition_files(right_window.added_files.clone())
                    .await?,
                &view.right.schema,
            )?;
            let changed_keys = delta_right
                .select(
                    view.right_join_keys()
                        .iter()
                        .map(|key| col(key.as_str()))
                        .collect::<Vec<_>>(),
                )?
                .distinct()?;
            let right_key_names = view
                .right_join_keys()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            affected = affected.union(
                left_now
                    .clone()
                    .join(
                        changed_keys,
                        JoinType::LeftSemi,
                        &join_key_names,
                        &right_key_names,
                        None,
                    )?
                    .select(key_exprs.clone())?,
            )?;
        }
        let affected = affected.distinct()?;

        // Rows written by this epoch make a replay a no-op.
        let already = mv
            .clone()
            .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
            .select(key_exprs.clone())?
            .distinct()?;
        let inserts = lookup_join_projection(left_now, right_now, view)?
            .join(
                affected.clone(),
                JoinType::LeftSemi,
                &key_names,
                &key_names,
                None,
            )?
            .join(already, JoinType::LeftAnti, &key_names, &key_names, None)?
            .select(lookup_join_output_columns(view))?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let deletes = mv
            .join(affected, JoinType::LeftSemi, &key_names, &key_names, None)?
            .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
            .select(lookup_join_output_columns(view))?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

        let mut sort_exprs = key_exprs;
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        for batch in inserts
            .union(deletes)?
            .sort_by(sort_exprs)?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Rebuild a lookup join view from the full source state.
    pub async fn rebuild_lookup_join(&self, view: &LookupJoinView) -> Result<i64> {
        self.register_lookup_join_view(view).await?;
        validate_lookup_join_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.output.truncate(&self.client).await?;

        let left_baseline = self.source_baseline(&view.left).await?;
        let right_baseline = self.source_baseline(&view.right).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.output).await?;
        let mut to_versions = left_baseline.to_versions;
        to_versions.extend(right_baseline.to_versions);
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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let mut left_now = filter_deletes(
            dataframe(&context, left_baseline.batches, &view.left.schema)?,
            change_column(&view.left),
        )?;
        if let Some(filter) = view.left_filter.as_deref() {
            left_now =
                left_now.filter(parse_filter(&context, &view.left.schema, filter)?)?;
        }
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, right_baseline.batches, &view.right.schema)?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        for batch in lookup_join_projection(left_now, right_now, view)?
            .select(lookup_join_output_columns(view))?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

    /// Persist a lookup join view spec (idempotent).
    pub async fn register_lookup_join_view(&self, view: &LookupJoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.output)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a left join view: rewrite every pair of the affected left rows
    /// (the changed left rows plus the left rows that reference a changed
    /// right row).
    pub async fn refresh_left_join(&self, view: &LeftJoinView) -> Result<Option<i64>> {
        self.register_left_join_view(view).await?;
        validate_left_join_view(view)?;

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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(
                    &context,
                    view.left.read_current(&self.client).await?,
                    &view.left.schema,
                )?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(
                    &context,
                    view.right.read_current(&self.client).await?,
                    &view.right.schema,
                )?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let mv = dataframe(
            &context,
            view.output.read_current(&self.client).await?,
            &view.output.schema,
        )?;

        let key_names = view
            .left
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let key_exprs = view
            .left
            .primary_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
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
        let join_key_names = view
            .join_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        let mut affected = left_now.clone().select(key_exprs.clone())?.limit(0, None)?;
        if !left_window.added_files.is_empty() {
            let delta_left = dataframe(
                &context,
                view.left
                    .read_partition_files(left_window.added_files.clone())
                    .await?,
                &view.left.schema,
            )?;
            affected = delta_left
                .select(key_exprs.clone())?
                .distinct()?
                .union(affected)?;
        }
        if !right_window.added_files.is_empty() {
            let delta_right = dataframe(
                &context,
                view.right
                    .read_partition_files(right_window.added_files.clone())
                    .await?,
                &view.right.schema,
            )?;
            let changed_keys = delta_right
                .select(
                    view.join_keys
                        .iter()
                        .map(|key| col(key.as_str()))
                        .collect::<Vec<_>>(),
                )?
                .distinct()?;
            affected = affected.union(
                left_now
                    .clone()
                    .join(
                        changed_keys,
                        JoinType::LeftSemi,
                        &join_key_names,
                        &join_key_names,
                        None,
                    )?
                    .select(key_exprs.clone())?,
            )?;
        }
        let affected = affected.distinct()?;

        // Rewrite all pairs of the affected left rows; a replay deletes and
        // re-inserts the same pairs, so no epoch guard is needed.
        let left_rows = left_now.join(
            affected.clone(),
            JoinType::LeftSemi,
            &key_names,
            &key_names,
            None,
        )?;
        let inserts = keyed_join_projection(
            &context,
            left_rows,
            right_now,
            &view.parts(),
            JoinType::Left,
            false,
        )?
        .select(keyed_join_output_columns(&view.parts()))?
        .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let deletes = mv
            .join(
                affected,
                JoinType::LeftSemi,
                &left_alias_refs,
                &key_names,
                None,
            )?
            .select(keyed_join_output_columns(&view.parts()))?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

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
        let mut sort_exprs = pair_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        for batch in inserts
            .union(deletes)?
            .sort_by(sort_exprs)?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Rebuild a left join view from the full source state.
    pub async fn rebuild_left_join(&self, view: &LeftJoinView) -> Result<i64> {
        self.register_left_join_view(view).await?;
        validate_left_join_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.output.truncate(&self.client).await?;

        let left_baseline = self.source_baseline(&view.left).await?;
        let right_baseline = self.source_baseline(&view.right).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.output).await?;
        let mut to_versions = left_baseline.to_versions;
        to_versions.extend(right_baseline.to_versions);
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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, left_baseline.batches, &view.left.schema)?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, right_baseline.batches, &view.right.schema)?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        for batch in keyed_join_projection(
            &context,
            left_now,
            right_now,
            &view.parts(),
            JoinType::Left,
            false,
        )?
        .select(keyed_join_output_columns(&view.parts()))?
        .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
        .collect()
        .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

    /// Persist a left join view spec (idempotent).
    pub async fn register_left_join_view(&self, view: &LeftJoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.output)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a full join view: rewrite the pairs of the affected left rows
    /// and of the affected right rows (including their NULL-padded rows).
    pub async fn refresh_full_join(&self, view: &FullJoinView) -> Result<Option<i64>> {
        self.register_full_join_view(view).await?;
        validate_full_join_view(view)?;

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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(
                    &context,
                    view.left.read_current(&self.client).await?,
                    &view.left.schema,
                )?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(
                    &context,
                    view.right.read_current(&self.client).await?,
                    &view.right.schema,
                )?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
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
        let left_key_exprs = view
            .left
            .primary_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
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
        let right_key_names = view
            .right
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let right_key_exprs = view
            .right
            .primary_keys
            .iter()
            .map(|key| col(key.as_str()))
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
        let join_key_names = view
            .join_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let join_key_exprs = view
            .join_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        // The right source's key columns when they differ from the left ones.
        let right_join_names = if view.right_keys.is_empty() {
            &view.join_keys
        } else {
            &view.right_keys
        };
        let right_join_key_names = right_join_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let right_join_key_exprs = right_join_names
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();

        let mut affected_left = left_now
            .clone()
            .select(left_key_exprs.clone())?
            .limit(0, None)?;
        let mut affected_right = right_now
            .clone()
            .select(right_key_exprs.clone())?
            .limit(0, None)?;
        if !left_window.added_files.is_empty() {
            let delta_left = apply_side_filter(
                &context,
                dataframe(
                    &context,
                    view.left
                        .read_partition_files(left_window.added_files.clone())
                        .await?,
                    &view.left.schema,
                )?,
                &view.left.schema,
                view.left_filter.as_deref(),
            )?;
            affected_left = affected_left.union(
                delta_left
                    .clone()
                    .select(left_key_exprs.clone())?
                    .distinct()?,
            )?;
            let changed_keys = delta_left.select(join_key_exprs.clone())?.distinct()?;
            affected_right = affected_right.union(
                right_now
                    .clone()
                    .join(
                        changed_keys,
                        JoinType::LeftSemi,
                        &right_join_key_names,
                        &join_key_names,
                        None,
                    )?
                    .select(right_key_exprs.clone())?,
            )?;
        }
        if !right_window.added_files.is_empty() {
            let delta_right = apply_side_filter(
                &context,
                dataframe(
                    &context,
                    view.right
                        .read_partition_files(right_window.added_files.clone())
                        .await?,
                    &view.right.schema,
                )?,
                &view.right.schema,
                view.right_filter.as_deref(),
            )?;
            affected_right = affected_right.union(
                delta_right
                    .clone()
                    .select(right_key_exprs.clone())?
                    .distinct()?,
            )?;
            let changed_keys = delta_right.select(right_join_key_exprs)?.distinct()?;
            affected_left = affected_left.union(
                left_now
                    .clone()
                    .join(
                        changed_keys,
                        JoinType::LeftSemi,
                        &join_key_names,
                        &right_join_key_names,
                        None,
                    )?
                    .select(left_key_exprs.clone())?,
            )?;
        }
        let affected_left = affected_left.distinct()?;
        let affected_right = affected_right.distinct()?;

        // Rewrite all pairs of the affected rows on either side; identical
        // pairs are deduplicated.
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
        let left_pairs = keyed_join_projection(
            &context,
            left_rows,
            right_now,
            &view.parts(),
            JoinType::Left,
            false,
        )?;
        let right_pairs = keyed_join_projection(
            &context,
            left_now,
            right_rows,
            &view.parts(),
            JoinType::Right,
            true,
        )?;
        let inserts = left_pairs
            .union(right_pairs)?
            .distinct()?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let deletes = mv
            .clone()
            .join(
                affected_left,
                JoinType::LeftSemi,
                &left_alias_refs,
                &left_key_names,
                None,
            )?
            .select(keyed_join_output_columns(&view.parts()))?
            .union(
                mv.join(
                    affected_right,
                    JoinType::LeftSemi,
                    &right_alias_refs,
                    &right_key_names,
                    None,
                )?
                .select(keyed_join_output_columns(&view.parts()))?,
            )?
            .distinct()?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

        let pair_keys = left_alias_names
            .into_iter()
            .chain(right_alias_names)
            .collect::<Vec<_>>();
        let mut sort_exprs = pair_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        for batch in inserts
            .union(deletes)?
            .sort_by(sort_exprs)?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Rebuild a full join view from the full source state.
    pub async fn rebuild_full_join(&self, view: &FullJoinView) -> Result<i64> {
        self.register_full_join_view(view).await?;
        validate_full_join_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.output.truncate(&self.client).await?;

        let left_baseline = self.source_baseline(&view.left).await?;
        let right_baseline = self.source_baseline(&view.right).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.output).await?;
        let mut to_versions = left_baseline.to_versions;
        to_versions.extend(right_baseline.to_versions);
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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, left_baseline.batches, &view.left.schema)?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, right_baseline.batches, &view.right.schema)?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let left_pairs = keyed_join_projection(
            &context,
            left_now.clone(),
            right_now.clone(),
            &view.parts(),
            JoinType::Left,
            false,
        )?;
        let right_pairs = keyed_join_projection(
            &context,
            left_now,
            right_now,
            &view.parts(),
            JoinType::Right,
            true,
        )?;
        for batch in left_pairs
            .union(right_pairs)?
            .distinct()?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

    /// Persist a full join view spec (idempotent).
    pub async fn register_full_join_view(&self, view: &FullJoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.output)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a cross join view over two keyed sources.
    ///
    /// The pairs of the identities that changed on either side are rewritten;
    /// a pair whose both sides changed is deduplicated. A replay rewrites the
    /// same pairs, so no epoch guard is needed.
    pub async fn refresh_cross_join(&self, view: &CrossJoinView) -> Result<Option<i64>> {
        self.register_cross_join_view(view).await?;
        validate_cross_join_view(view)?;

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
        let mut commit_ids = Vec::new();
        let context = SessionContext::new();

        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(
                    &context,
                    view.left.read_current(&self.client).await?,
                    &view.left.schema,
                )?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(
                    &context,
                    view.right.read_current(&self.client).await?,
                    &view.right.schema,
                )?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
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
        let left_key_exprs = view
            .left
            .primary_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        let right_key_exprs = view
            .right
            .primary_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
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
        let parts = view.parts();

        let mut inserts: Option<DataFrame> = None;
        let mut deletes: Option<DataFrame> = None;
        if !left_window.added_files.is_empty() {
            let delta_left = dataframe(
                &context,
                view.left
                    .read_partition_files(left_window.added_files.clone())
                    .await?,
                &view.left.schema,
            )?;
            let changed_left = delta_left.select(left_key_exprs)?.distinct()?;
            let left_rows = left_now.clone().join(
                changed_left.clone(),
                JoinType::LeftSemi,
                &left_key_names,
                &left_key_names,
                None,
            )?;
            inserts = Some(keyed_join_projection(
                &context,
                left_rows,
                right_now.clone(),
                &parts,
                JoinType::Inner,
                false,
            )?);
            deletes = Some(mv.clone().join(
                changed_left,
                JoinType::LeftSemi,
                &left_alias_refs,
                &left_key_names,
                None,
            )?);
        }
        if !right_window.added_files.is_empty() {
            let delta_right = dataframe(
                &context,
                view.right
                    .read_partition_files(right_window.added_files.clone())
                    .await?,
                &view.right.schema,
            )?;
            let changed_right = delta_right.select(right_key_exprs)?.distinct()?;
            let right_rows = right_now.clone().join(
                changed_right.clone(),
                JoinType::LeftSemi,
                &right_key_names,
                &right_key_names,
                None,
            )?;
            let term = keyed_join_projection(
                &context,
                left_now,
                right_rows,
                &parts,
                JoinType::Inner,
                false,
            )?;
            inserts = Some(match inserts {
                Some(existing) => existing.union(term)?.distinct()?,
                None => term,
            });
            let term = mv.join(
                changed_right,
                JoinType::LeftSemi,
                &right_alias_refs,
                &right_key_names,
                None,
            )?;
            deletes = Some(match deletes {
                Some(existing) => existing.union(term)?.distinct()?,
                None => term,
            });
        }
        let no_change =
            || report!("cross join view {} refreshed without changes", view.view_id);
        let inserts = inserts
            .ok_or_else(no_change)?
            .select(keyed_join_output_columns(&parts))?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let deletes = deletes
            .ok_or_else(no_change)?
            .select(keyed_join_output_columns(&parts))?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

        let mut pair_keys = left_alias_names;
        pair_keys.extend(right_alias_names);
        let mut sort_exprs = pair_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        for batch in inserts
            .union(deletes)?
            .sort_by(sort_exprs)?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Rebuild a cross join view from the full source states.
    pub async fn rebuild_cross_join(&self, view: &CrossJoinView) -> Result<i64> {
        self.register_cross_join_view(view).await?;
        validate_cross_join_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.output.truncate(&self.client).await?;

        let left_baseline = self.source_baseline(&view.left).await?;
        let right_baseline = self.source_baseline(&view.right).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.output).await?;
        let mut to_versions = left_baseline.to_versions;
        to_versions.extend(right_baseline.to_versions);
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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, left_baseline.batches, &view.left.schema)?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, right_baseline.batches, &view.right.schema)?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let parts = view.parts();
        let inserts = keyed_join_projection(
            &context,
            left_now,
            right_now,
            &parts,
            JoinType::Inner,
            false,
        )?
        .select(keyed_join_output_columns(&parts))?
        .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        for batch in inserts.collect().await? {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.output.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

    /// Persist a cross join view spec (idempotent).
    pub async fn register_cross_join_view(&self, view: &CrossJoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.output)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        if !left_baseline.batches.is_empty() && !right_baseline.batches.is_empty() {
            let joined = if keyed {
                let left = apply_side_filter(
                    &context,
                    filter_deletes(
                        dataframe(&context, left_baseline.batches, &view.left.schema)?,
                        change_column(&view.left),
                    )?,
                    &view.left.schema,
                    view.left_filter.as_deref(),
                )?;
                let right = apply_side_filter(
                    &context,
                    filter_deletes(
                        dataframe(&context, right_baseline.batches, &view.right.schema)?,
                        change_column(&view.right),
                    )?,
                    &view.right.schema,
                    view.right_filter.as_deref(),
                )?;
                keyed_join_projection(
                    &context,
                    left,
                    right,
                    &view.parts(),
                    JoinType::Inner,
                    false,
                )?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            } else {
                join_projection(
                    filtered_frame(
                        &context,
                        left_baseline.batches.clone(),
                        &view.left.schema,
                        view.left_filter.as_deref(),
                    )?,
                    filtered_frame(
                        &context,
                        right_baseline.batches.clone(),
                        &view.right.schema,
                        view.right_filter.as_deref(),
                    )?,
                    view,
                )?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            };
            for batch in joined.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids
                        .extend(view.output.append_batch(&self.client, batch).await?);
                }
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
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
}
