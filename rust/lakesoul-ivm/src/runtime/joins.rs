// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The pair-keyed join views: inner, lookup `LEFT`, pair-keyed left/right/full
//! and cross joins.

use super::*;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions::string::expr_fn::{concat, octet_length};
use datafusion::logical_expr::Cast;

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

/// The hidden MV column a condition-only column is materialized under: the
/// select list does not carry the column, but the pair filter has to compare
/// it, so the analyzer adds it as a wide payload under this name.
pub fn join_condition_column_name(side: JoinSide, column: &str) -> String {
    let side = match side {
        JoinSide::Left => "left",
        JoinSide::Right => "right",
    };
    format!("__ivm_cond_{side}_{column}")
}

/// The hidden MV column of the `index`-th computed join key.
pub fn join_key_expression_name(index: usize) -> String {
    format!("__ivm_key_{index}")
}

/// The rendered join-key expressions of a two-source join, aligned with its
/// `join_keys`; an empty entry means that key is a plain column of that side.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinKeyExprs {
    /// The left expressions, aligned with `join_keys`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub left: Vec<String>,
    /// The right expressions, aligned with `join_keys`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub right: Vec<String>,
}

impl JoinKeyExprs {
    /// True when every key is a plain column of the sources.
    pub fn is_empty(&self) -> bool {
        self.left.iter().all(String::is_empty) && self.right.iter().all(String::is_empty)
    }
}

/// The key expressions of a join without computed keys.
static NO_KEY_EXPRS: JoinKeyExprs = JoinKeyExprs {
    left: Vec::new(),
    right: Vec::new(),
};

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
    /// The rendered key expressions aligned with `join_keys`; empty means
    /// every key is a same-named column of both sources.  A computed key is
    /// evaluated on each side and stored in the MV under its (hidden) key
    /// name.
    pub key_exprs: JoinKeyExprs,
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
    /// Whether a NULL join key matches another NULL join key
    /// (`IS NOT DISTINCT FROM` keys).
    pub null_safe: bool,
    /// The wide output columns; empty means the compact payload shape.
    pub output_columns: Vec<JoinOutputColumn>,
    /// Optional keyed output state table (same schema and keys as the output).
    pub state: Option<IvmTable>,
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
            key_exprs: JoinKeyExprs::default(),
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            left_filter: None,
            right_filter: None,
            pair_filter: None,
            null_safe: false,
            state: None,
            refresh_interval_ms: 0,
        }
    }

    /// Attach the keyed state table that mirrors the join output.
    pub fn with_state(mut self, state: IvmTable) -> Self {
        self.state = Some(state);
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

    /// Match NULL join keys with NULL join keys (`IS NOT DISTINCT FROM`).
    pub fn with_null_safe(mut self, null_safe: bool) -> Self {
        self.null_safe = null_safe;
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
            key_exprs: self.key_exprs.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            payload_names: Vec::new(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            pair_filter: self.pair_filter.clone(),
            null_safe: self.null_safe,
            state_table_id: self.state.as_ref().map(|state| state.table_id.clone()),
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
    join_view_schema_with_keys(
        left_schema,
        right_schema,
        join_keys,
        &NO_KEY_EXPRS,
        left_value,
        right_value,
    )
}

/// The schema of an append-only [`JoinView`] output with computed join keys.
pub fn join_view_schema_with_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    for (index, key) in join_keys.iter().enumerate() {
        fields.push(Arc::new(Field::new(
            key,
            join_key_type(left_schema, join_keys, key_exprs, index)?,
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
/// The MV partitions a keyed join refresh can touch when exactly one side
/// changed.
///
/// The join output carries each side's primary keys under an alias, so map
/// the source column to its output alias.  When both sides changed the other
/// side's matching rows can live in any partition, so no restriction is
/// returned.
fn single_side_join_mv_partitions(
    output: &IvmTable,
    left: &IvmTable,
    left_window: &SourceWindow,
    right: &IvmTable,
    right_window: &SourceWindow,
) -> Option<Vec<(String, Vec<String>)>> {
    let left_mapping = left
        .primary_keys
        .iter()
        .map(|key| (key.clone(), left_pk_alias(key)))
        .collect::<Vec<_>>();
    let right_mapping = right
        .primary_keys
        .iter()
        .map(|key| (key.clone(), right_pk_alias(key)))
        .collect::<Vec<_>>();
    match (
        left_window.added_files.is_empty(),
        right_window.added_files.is_empty(),
    ) {
        (false, true) => super::mv_partitions_for_source_mapped(
            output,
            left,
            left_window,
            &left_mapping,
        ),
        (true, false) => super::mv_partitions_for_source_mapped(
            output,
            right,
            right_window,
            &right_mapping,
        ),
        _ => None,
    }
}

fn left_pk_alias(key: &str) -> String {
    format!("__left_pk_{key}")
}

/// The hidden column holding a right primary key in a keyed join output.
fn right_pk_alias(key: &str) -> String {
    format!("__right_pk_{key}")
}

/// The hidden column a pair projection carries from its left frame: the left
/// primary keys' encoded row identity, computed **below** the join.  A struct
/// field access above a join would be pushed to the leaves by DataFusion's
/// leaf-expression rules, which re-expose both frames' columns under the
/// shared `?table?` placeholder and collide.
const LEFT_IDENTITY_COLUMN: &str = "__left_identity";

/// The right frame's encoded row identity (see [`LEFT_IDENTITY_COLUMN`]).
const RIGHT_IDENTITY_COLUMN: &str = "__right_identity";

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

/// Mark the leading join key columns nullable: a null-safe join matches NULL
/// keys with NULL keys, so its output can carry a NULL join key.
pub fn nullable_join_key_schema(schema: &SchemaRef, key_count: usize) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if index < key_count {
                Arc::new(Field::new(field.name(), field.data_type().clone(), true))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The data type of one join key: the left expression when the key is
/// computed, otherwise the same-named left column.
fn join_key_type(
    left_schema: &Schema,
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    index: usize,
) -> Result<DataType> {
    match key_exprs.left.get(index).filter(|expr| !expr.is_empty()) {
        Some(expr) => Ok(expression_type(left_schema, expr)?.0),
        None => field_type(left_schema, &join_keys[index]),
    }
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
    keyed_join_view_schema_with_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        &NO_KEY_EXPRS,
        left_value,
        right_value,
    )
}

/// The schema of a keyed [`JoinView`] output with computed join keys.
pub fn keyed_join_view_schema_with_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    keyed_join_schema_for(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        key_exprs,
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
    left_join_view_schema_with_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        &NO_KEY_EXPRS,
        left_value,
        right_value,
    )
}

/// The schema of a [`LeftJoinView`] output with computed join keys.
#[allow(clippy::too_many_arguments)]
pub fn left_join_view_schema_with_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    keyed_join_schema_for(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        key_exprs,
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
    full_join_view_schema_with_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        &NO_KEY_EXPRS,
        left_value,
        right_value,
    )
}

/// The schema of a [`FullJoinView`] output with computed join keys.
#[allow(clippy::too_many_arguments)]
pub fn full_join_view_schema_with_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    keyed_join_schema_for(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        key_exprs,
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
    wide_outer_join_view_schema_with_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        &NO_KEY_EXPRS,
        output_columns,
    )
}

/// The schema of a wide outer join output with computed join keys.
#[allow(clippy::too_many_arguments)]
pub fn wide_outer_join_view_schema_with_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "an outer join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for (index, key) in join_keys.iter().enumerate() {
        let computed = key_exprs.left.get(index).filter(|expr| !expr.is_empty());
        let (data_type, nullable) = match computed {
            // A computed key is typed by its expression.
            Some(expr) => expression_type(left_schema, expr)?,
            None => {
                let field = left_schema.field_with_name(key)?;
                (field.data_type().clone(), field.is_nullable())
            }
        };
        fields.push(Arc::new(Field::new(key, data_type, nullable)));
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

/// The schema of a wide `LEFT JOIN` view whose MV also materializes the
/// non-null row identity column ([`IVM_PAIR_IDENTITY_COLUMN`]): a derived
/// step a consumer reads as a keyed source.  A left join keeps every left
/// row, so the left identities stay non-null while the right ones may be
/// NULL.
#[allow(clippy::too_many_arguments)]
pub fn wide_left_join_view_schema_with_identity(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    wide_left_join_view_schema_with_identity_and_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        &NO_KEY_EXPRS,
        output_columns,
    )
}

/// The schema of a wide `LEFT JOIN` view with the row identity column and
/// computed join keys.
#[allow(clippy::too_many_arguments)]
pub fn wide_left_join_view_schema_with_identity_and_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "an outer join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for (index, key) in join_keys.iter().enumerate() {
        let computed = key_exprs.left.get(index).filter(|expr| !expr.is_empty());
        let (data_type, nullable) = match computed {
            // A computed key is typed by its expression.
            Some(expr) => {
                let (data_type, nullable) = expression_type(left_schema, expr)?;
                (data_type, !nullable)
            }
            None => {
                let field = left_schema.field_with_name(key)?;
                (field.data_type().clone(), !field.is_nullable())
            }
        };
        fields.push(Arc::new(Field::new(key, data_type, nullable)));
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
            true,
        )));
    }
    fields.push(Arc::new(Field::new(
        IVM_PAIR_IDENTITY_COLUMN,
        DataType::Utf8,
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

/// The schema of a wide `FULL JOIN` view whose MV also materializes the
/// non-null row identity column ([`IVM_PAIR_IDENTITY_COLUMN`]): a derived
/// step a consumer reads as a keyed source.  Both sides may be unmatched, so
/// both identity sets are nullable and the identity column alone is the key.
pub fn wide_full_join_view_schema_with_identity(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    wide_full_join_view_schema_with_identity_and_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        &NO_KEY_EXPRS,
        output_columns,
    )
}

/// The schema of a wide `FULL JOIN` view with the row identity column and
/// computed join keys.
#[allow(clippy::too_many_arguments)]
pub fn wide_full_join_view_schema_with_identity_and_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    let base = wide_outer_join_view_schema_with_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        key_exprs,
        output_columns,
    )?;
    let mut fields = base.fields().iter().map(Arc::clone).collect::<Vec<_>>();
    // The base ends with the kinds and epoch columns; insert the identity
    // before them.
    let split = fields.len().saturating_sub(2);
    let mut identity =
        vec![
            Arc::new(Field::new(IVM_PAIR_IDENTITY_COLUMN, DataType::Utf8, false))
                as Arc<Field>,
        ];
    identity.append(&mut fields.split_off(split));
    fields.append(&mut identity);
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a wide [`LookupJoinView`] output: the left fields keep their
/// nullability and the right fields are nullable (NULL without a match).
pub fn wide_lookup_join_view_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() {
        return Err(report!(
            "a lookup join view needs primary keys on the left source"
        ));
    }
    let mut fields = Vec::new();
    for (index, key) in join_keys.iter().enumerate() {
        let computed = key_exprs.left.get(index).filter(|expr| !expr.is_empty());
        let (data_type, nullable) = match computed {
            // A computed key is typed by its expression.
            Some(expr) => expression_type(left_schema, expr)?,
            None => {
                let field = left_schema.field_with_name(key)?;
                (field.data_type().clone(), field.is_nullable())
            }
        };
        fields.push(Arc::new(Field::new(key, data_type, nullable)));
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
    wide_join_view_schema_with_keys(
        left_schema,
        right_schema,
        join_keys,
        &NO_KEY_EXPRS,
        output_columns,
    )
}

/// The schema of a wide append-only [`JoinView`] output with computed keys.
pub fn wide_join_view_schema_with_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    for (index, key) in join_keys.iter().enumerate() {
        fields.push(Arc::new(Field::new(
            key,
            join_key_type(left_schema, join_keys, key_exprs, index)?,
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
    wide_keyed_join_view_schema_with_keys(
        left_schema,
        right_schema,
        left_primary_keys,
        right_primary_keys,
        join_keys,
        &NO_KEY_EXPRS,
        output_columns,
    )
}

/// The schema of a wide keyed [`JoinView`] output with computed join keys.
pub fn wide_keyed_join_view_schema_with_keys(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
    output_columns: &[JoinOutputColumn],
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "a keyed join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for (index, key) in join_keys.iter().enumerate() {
        fields.push(Arc::new(Field::new(
            key,
            join_key_type(left_schema, join_keys, key_exprs, index)?,
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

#[allow(clippy::too_many_arguments)]
fn keyed_join_schema_for(
    left_schema: &Schema,
    right_schema: &Schema,
    left_primary_keys: &[String],
    right_primary_keys: &[String],
    join_keys: &[String],
    key_exprs: &JoinKeyExprs,
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
    for (index, key) in join_keys.iter().enumerate() {
        let data_type = join_key_type(left_schema, join_keys, key_exprs, index)?;
        let nullable = match key_exprs.left.get(index).filter(|expr| !expr.is_empty()) {
            Some(expr) => expression_type(left_schema, expr)?.1,
            None => left_schema
                .field_with_name(key)
                .map(|field| field.is_nullable())
                .unwrap_or(true),
        };
        fields.push(Arc::new(Field::new(
            key,
            data_type,
            outer != JoinSchema::Inner && nullable,
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
    /// Whether the MV materializes the non-null row identity column.
    pub identity_column: bool,
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
    /// The rendered key expressions, aligned with `join_keys`; an empty
    /// entry means that key is a plain column of that side.
    pub key_exprs: JoinKeyExprs,
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
    /// Whether a NULL join key matches another NULL join key
    /// (`IS NOT DISTINCT FROM` keys).
    pub null_safe: bool,
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
            identity_column: false,
            view_id: view_id.into(),
            left,
            right,
            output,
            join_keys,
            right_keys: Vec::new(),
            key_exprs: JoinKeyExprs::default(),
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            left_filter: None,
            right_filter: None,
            null_safe: false,
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

    /// Match NULL join keys with NULL join keys (`IS NOT DISTINCT FROM`).
    pub fn with_null_safe(mut self, null_safe: bool) -> Self {
        self.null_safe = null_safe;
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
            key_exprs: self.key_exprs.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            payload_names: Vec::new(),
            identity_column: self.identity_column,
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            null_safe: self.null_safe,
        }
    }
}

/// A `FULL JOIN` view over two keyed sources: the matching pairs plus the
/// unmatched rows of either side (NULL-padded).
#[derive(Debug, Clone)]
pub struct FullJoinView {
    /// Whether the MV materializes the non-null row identity column.
    pub identity_column: bool,
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
    /// The rendered key expressions, aligned with `join_keys`; an empty
    /// entry means that key is a plain column of that side.
    pub key_exprs: JoinKeyExprs,
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
    /// Whether a NULL join key matches another NULL join key
    /// (`IS NOT DISTINCT FROM` keys).
    pub null_safe: bool,
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
            identity_column: false,
            view_id: view_id.into(),
            left,
            right,
            output,
            join_keys,
            right_keys: Vec::new(),
            key_exprs: JoinKeyExprs::default(),
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            left_filter: None,
            right_filter: None,
            null_safe: false,
            refresh_interval_ms: 0,
        }
    }

    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &self.join_keys,
            right_keys: &self.right_keys,
            key_exprs: &self.key_exprs,
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            left_table: &self.left,
            right_table: &self.right,
            pair_filter: None,
            identity_column: self.identity_column,
            null_safe: self.null_safe,
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

    /// Match NULL join keys with NULL join keys (`IS NOT DISTINCT FROM`).
    pub fn with_null_safe(mut self, null_safe: bool) -> Self {
        self.null_safe = null_safe;
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::FullJoin {
            identity_column: self.identity_column,
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            output_table_id: self.output.table_id.clone(),
            join_keys: self.join_keys.clone(),
            right_keys: self.right_keys.clone(),
            key_exprs: self.key_exprs.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            payload_names: Vec::new(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            null_safe: self.null_safe,
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
            payload_names: Vec::new(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            pair_filter: self.pair_filter.clone(),
        }
    }

    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &[],
            right_keys: &[],
            key_exprs: &NO_KEY_EXPRS,
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            left_table: &self.left,
            right_table: &self.right,
            pair_filter: self.pair_filter.as_deref(),
            identity_column: false,
            null_safe: false,
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
    /// The rendered key expressions, aligned with [`Self::join_keys`]; an
    /// empty entry means that key is a plain column of that side.
    pub key_exprs: JoinKeyExprs,
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
    /// Optional keyed output state table (same schema and keys as the output).
    pub state: Option<IvmTable>,
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
            key_exprs: JoinKeyExprs::default(),
            left_filter: None,
            right_filter: None,
            left_value: left_value.into(),
            right_value: right_value.into(),
            output_columns: Vec::new(),
            state: None,
            refresh_interval_ms: 0,
        }
    }

    /// Attach the keyed state table that mirrors the lookup output.
    pub fn with_state(mut self, state: IvmTable) -> Self {
        self.state = Some(state);
        self
    }

    /// The right-side key names the lookup is by: a computed key is
    /// materialized under a hidden name, so a right key that renders to a
    /// plain column of the right source reads through that column.
    pub fn lookup_right_key_names(&self) -> Vec<String> {
        self.right_join_keys()
            .iter()
            .enumerate()
            .map(|(index, key)| {
                match self
                    .key_exprs
                    .right
                    .get(index)
                    .filter(|expr| !expr.is_empty())
                    .filter(|expr| self.right.schema.field_with_name(expr).is_ok())
                {
                    Some(expr) => expr.clone(),
                    None => key.clone(),
                }
            })
            .collect()
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
            key_exprs: self.key_exprs.clone(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            left_value: self.left_value.clone(),
            right_value: self.right_value.clone(),
            output_columns: self.output_columns.clone(),
            payload_names: Vec::new(),
            state_table_id: self.state.as_ref().map(|state| state.table_id.clone()),
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
    key_exprs: &JoinKeyExprs,
    left_value: &str,
    right_value: &str,
) -> Result<SchemaRef> {
    if left_primary_keys.is_empty() || right_primary_keys.is_empty() {
        return Err(report!(
            "a lookup join view needs primary keys on both sources"
        ));
    }
    let mut fields = Vec::new();
    for (index, key) in join_keys.iter().enumerate() {
        let computed = key_exprs.left.get(index).filter(|expr| !expr.is_empty());
        let (data_type, nullable) = match computed {
            // A computed key is typed by its expression.
            Some(expr) => expression_type(left_schema, expr)?,
            None => {
                let field = left_schema.field_with_name(key)?;
                (field.data_type().clone(), field.is_nullable())
            }
        };
        fields.push(Arc::new(Field::new(key, data_type, nullable)));
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
        // A primary key that is already one of the join keys is materialized
        // once, as that key column.
        if join_keys.contains(key) {
            continue;
        }
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
    let context = refresh_session();
    let mut left_columns = join_side_key_columns(
        &context,
        left.schema(),
        &view.join_keys,
        &view.key_exprs.left,
        &[],
        JoinSide::Left,
    )?;
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
        // The key column already carries a primary key that is one of the
        // join keys; projecting it twice is invalid.
        if view.join_keys.contains(key) {
            continue;
        }
        left_columns.push(col(key.as_str()));
    }
    let left = left.select(left_columns)?;

    let mut right_columns = join_side_key_columns(
        &context,
        right.schema(),
        &view.join_keys,
        &view.key_exprs.right,
        view.right_join_keys(),
        JoinSide::Right,
    )?;
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
    // A primary key that is also a join key is materialized once, as the key.
    for key in &view.left.primary_keys {
        if view.join_keys.contains(key) {
            continue;
        }
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
    // A primary key that is also a join key is materialized once, as the key.
    output.extend(
        view.left
            .primary_keys
            .iter()
            .filter(|key| !view.join_keys.contains(key))
            .map(|key| col(key.as_str())),
    );
    output
}

/// Validate that a full join view can be maintained.
fn validate_full_join_view(view: &FullJoinView) -> Result<()> {
    let left = LeftJoinView {
        identity_column: false,
        view_id: view.view_id.clone(),
        left: view.left.clone(),
        right: view.right.clone(),
        output: view.output.clone(),
        join_keys: view.join_keys.clone(),
        right_keys: view.right_keys.clone(),
        key_exprs: view.key_exprs.clone(),
        left_value: view.left_value.clone(),
        right_value: view.right_value.clone(),
        output_columns: view.output_columns.clone(),
        left_filter: view.left_filter.clone(),
        right_filter: view.right_filter.clone(),
        null_safe: view.null_safe,
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
        let context = refresh_session();
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
    for (index, (key, right_key)) in view.join_keys.iter().zip(right_keys).enumerate() {
        // A computed key is materialized by the view, so it needs no source
        // column; a plain key must be one.
        let left_type = match view
            .key_exprs
            .left
            .get(index)
            .filter(|expr| !expr.is_empty())
        {
            Some(expr) => expression_type(&view.left.schema, expr)?.0,
            None => view
                .left
                .schema
                .field_with_name(key)
                .map_err(|_| {
                    report!(
                        "left join view {}: join key {key} is not in the left source",
                        view.view_id
                    )
                })?
                .data_type()
                .clone(),
        };
        let right_type = match view
            .key_exprs
            .right
            .get(index)
            .filter(|expr| !expr.is_empty())
        {
            Some(expr) => expression_type(&view.right.schema, expr)?.0,
            None => view
                .right
                .schema
                .field_with_name(right_key)
                .map_err(|_| {
                    report!(
                        "left join view {}: join key {right_key} is not in the right source",
                        view.view_id
                    )
                })?
                .data_type()
                .clone(),
        };
        if left_type != right_type {
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
        let context = refresh_session();
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
        let context = refresh_session();
        parse_filter(&context, &view.left.schema, filter)?;
    }
    if let Some(filter) = &view.right_filter {
        let context = refresh_session();
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
    let mut keys = view.lookup_right_key_names();
    keys.sort();
    if expected != keys {
        return Err(report!(
            "lookup join view {}: the right source must be keyed by the join keys",
            view.view_id
        ));
    }
    for (index, key) in view.join_keys.iter().enumerate() {
        // A computed key is materialized by the view, so it needs no source
        // column; a plain key must be one.
        let computed = view
            .key_exprs
            .left
            .get(index)
            .filter(|expr| !expr.is_empty());
        match computed {
            Some(expr) => {
                expression_type(&view.left.schema, expr)?;
            }
            None => {
                view.left.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "lookup join view {}: join key {key} is not in the left source",
                        view.view_id
                    )
                })?;
            }
        }
    }
    for (index, key) in view.right_join_keys().iter().enumerate() {
        let computed = view
            .key_exprs
            .right
            .get(index)
            .filter(|expr| !expr.is_empty());
        match computed {
            Some(expr) => {
                expression_type(&view.right.schema, expr)?;
            }
            None => {
                view.right.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "lookup join view {}: join key {key} is not in the right source",
                        view.view_id
                    )
                })?;
            }
        }
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
    let context = refresh_session();
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
    let context = refresh_session();
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
        let context = refresh_session();
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
    if !view.key_exprs.is_empty()
        && (view.key_exprs.left.len() != view.join_keys.len()
            || view.key_exprs.right.len() != view.join_keys.len())
    {
        return Err(report!(
            "join view {}: key_exprs must be parallel to join_keys",
            view.view_id
        ));
    }
    for (index, (key, right_key)) in view.join_keys.iter().zip(right_keys).enumerate() {
        let left_expr = view
            .key_exprs
            .left
            .get(index)
            .filter(|expr| !expr.is_empty());
        let right_expr = view
            .key_exprs
            .right
            .get(index)
            .filter(|expr| !expr.is_empty());
        let left_type = match left_expr {
            Some(expr) => expression_type(&view.left.schema, expr)?.0,
            None => view
                .left
                .schema
                .field_with_name(key)
                .map_err(|_| {
                    report!(
                        "join view {}: join key {key} is not in the left source",
                        view.view_id
                    )
                })?
                .data_type()
                .clone(),
        };
        let right_type = match right_expr {
            Some(expr) => expression_type(&view.right.schema, expr)?.0,
            None => view
                .right
                .schema
                .field_with_name(right_key)
                .map_err(|_| {
                    report!(
                        "join view {}: join key {right_key} is not in the right source",
                        view.view_id
                    )
                })?
                .data_type()
                .clone(),
        };
        if left_type != right_type {
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
            keyed_join_view_schema_with_keys(
                &view.left.schema,
                &view.right.schema,
                &view.left.primary_keys,
                &view.right.primary_keys,
                &view.join_keys,
                &view.key_exprs,
                &view.left_value,
                &view.right_value,
            )?
        } else {
            wide_keyed_join_view_schema_with_keys(
                &view.left.schema,
                &view.right.schema,
                &view.left.primary_keys,
                &view.right.primary_keys,
                &view.join_keys,
                &view.key_exprs,
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

/// The join key columns of one side: a plain column (`name`, or the aligned
/// `right_keys` entry) or the rendered key expression, aliased so the pair
/// join can use it (`__right_<key>` on the right side).
fn join_side_key_columns(
    context: &SessionContext,
    schema: &DFSchema,
    join_keys: &[String],
    exprs: &[String],
    right_keys: &[String],
    side: JoinSide,
) -> Result<Vec<Expr>> {
    let mut columns = Vec::with_capacity(join_keys.len());
    for (index, key) in join_keys.iter().enumerate() {
        let computed = exprs.get(index).filter(|expr| !expr.is_empty());
        let column = match computed {
            Some(expr) => context
                .state()
                .create_logical_expr(expr, schema)
                .map_err(|error| report!("invalid join key {expr:?}: {error}"))?,
            None => {
                let name = match side {
                    JoinSide::Left => key.clone(),
                    JoinSide::Right => right_keys
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| key.clone()),
                };
                col(name.as_str())
            }
        };
        columns.push(match side {
            JoinSide::Left => column.alias(key.as_str()),
            JoinSide::Right => column.alias(format!("__right_{key}")),
        });
    }
    Ok(columns)
}

/// Materialize one side's computed key expressions on a frame, under `names`
/// (a plain key is already a column of the frame).  The affected detection of
/// the outer joins probes by key name, so a computed key has to exist as a
/// column first.
fn with_computed_keys(
    context: &SessionContext,
    frame: DataFrame,
    exprs: &[String],
    names: &[String],
) -> Result<DataFrame> {
    let mut frame = frame;
    for (index, name) in names.iter().enumerate() {
        let computed = exprs.get(index).filter(|expr| !expr.is_empty());
        if let Some(rendered) = computed {
            let expr = context
                .state()
                .create_logical_expr(rendered, frame.schema())?;
            frame = frame.with_column(name, expr)?;
        }
    }
    Ok(frame)
}

/// Project one side's key columns under its own key names: a computed key is
/// selected as its expression, a plain key as its column.
fn key_columns(
    context: &SessionContext,
    schema: &DFSchema,
    exprs: &[String],
    keys: &[String],
) -> Result<Vec<Expr>> {
    keys.iter()
        .enumerate()
        .map(
            |(index, key)| match exprs.get(index).filter(|expr| !expr.is_empty()) {
                Some(rendered) => Ok(context
                    .state()
                    .create_logical_expr(rendered, schema)?
                    .alias(key.as_str())),
                None => Ok(col(key.as_str())),
            },
        )
        .collect()
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
            let context = refresh_session();
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
    context: &SessionContext,
    left: DataFrame,
    right: DataFrame,
    view: &JoinView,
) -> Result<DataFrame> {
    let wide = !view.output_columns.is_empty();
    let right_keys = if view.right_keys.is_empty() {
        view.join_keys.clone()
    } else {
        view.right_keys.clone()
    };
    let mut left_columns = join_side_key_columns(
        context,
        left.schema(),
        &view.join_keys,
        &view.key_exprs.left,
        &[],
        JoinSide::Left,
    )?;
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
    let mut right_columns = join_side_key_columns(
        context,
        right.schema(),
        &view.join_keys,
        &view.key_exprs.right,
        &right_keys,
        JoinSide::Right,
    )?;
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
    let joined = if view.null_safe {
        // A NULL join key matches another NULL join key.
        let plan = LogicalPlanBuilder::new(left.logical_plan().clone())
            .join_detailed(
                right.logical_plan().clone(),
                JoinType::Inner,
                (key_names.clone(), right_key_names.clone()),
                None,
                NullEquality::NullEqualsNull,
            )?
            .build()?;
        DataFrame::new(context.state(), plan)
    } else {
        left.join(right, JoinType::Inner, &key_names, &right_key_refs, None)?
    };
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

/// A semi join on equality keys, matching NULL keys with NULL keys when the
/// view is null-safe.  The key columns of the two sides may be named
/// differently.
fn keyed_semi_join(
    context: &SessionContext,
    left: DataFrame,
    right: DataFrame,
    left_keys: &[&str],
    right_keys: &[&str],
    null_safe: bool,
) -> Result<DataFrame> {
    if !null_safe {
        return Ok(left.join(right, JoinType::LeftSemi, left_keys, right_keys, None)?);
    }
    let plan = LogicalPlanBuilder::new(left.logical_plan().clone())
        .join_detailed(
            right.logical_plan().clone(),
            JoinType::LeftSemi,
            (
                left_keys
                    .iter()
                    .map(|key| key.to_string())
                    .collect::<Vec<_>>(),
                right_keys
                    .iter()
                    .map(|key| key.to_string())
                    .collect::<Vec<_>>(),
            ),
            None,
            NullEquality::NullEqualsNull,
        )?
        .build()?;
    Ok(DataFrame::new(context.state(), plan))
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
    let mut left_columns = join_side_key_columns(
        context,
        left.schema(),
        parts.join_keys,
        &parts.key_exprs.left,
        &[],
        JoinSide::Left,
    )?;
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
    if parts.identity_column {
        left_columns.push(
            identity_side_expr(&parts.left_table.primary_keys, &parts.left_table.schema)?
                .alias(LEFT_IDENTITY_COLUMN),
        );
    }
    let left = left.select(left_columns)?;

    let mut right_columns = join_side_key_columns(
        context,
        right.schema(),
        parts.join_keys,
        &parts.key_exprs.right,
        effective_right_keys(parts),
        JoinSide::Right,
    )?;
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
    if parts.identity_column {
        right_columns.push(
            identity_side_expr(
                &parts.right_table.primary_keys,
                &parts.right_table.schema,
            )?
            .alias(RIGHT_IDENTITY_COLUMN),
        );
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
    } else if parts.null_safe {
        // A NULL join key matches another NULL join key.
        let plan = LogicalPlanBuilder::new(left.logical_plan().clone())
            .join_detailed(
                right.logical_plan().clone(),
                join_type,
                (key_names.clone(), right_key_names.clone()),
                None,
                NullEquality::NullEqualsNull,
            )?
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
    if parts.identity_column {
        output.push(col(LEFT_IDENTITY_COLUMN));
        output.push(col(RIGHT_IDENTITY_COLUMN));
    }
    Ok(joined.select(output)?)
}

/// Where the pair identity of [`keyed_join_output_columns`] comes from.
#[derive(Clone, Copy)]
enum IdentitySource {
    /// The frame already stores the column (a materialized view row).
    Stored,
    /// Compose it from the per-side encodings the pair projection carries.
    EncodedSides,
}

/// The output columns of a keyed join in schema order.
fn keyed_join_output_columns(
    parts: &PairJoin<'_>,
    identity: IdentitySource,
) -> Result<Vec<datafusion::logical_expr::Expr>> {
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
    if parts.identity_column {
        output.push(match identity {
            IdentitySource::Stored => col(IVM_PAIR_IDENTITY_COLUMN),
            IdentitySource::EncodedSides => pair_identity_from_sides()?,
        });
    }
    Ok(output)
}

/// Encode one side's primary keys into a single text column, evaluated on
/// that side's own frame (below the join), so no struct field access ever
/// flows into the join.
fn identity_side_expr(primary_keys: &[String], schema: &Schema) -> Result<Expr> {
    let mut parts = Vec::new();
    for key in primary_keys {
        parts.push(encoded_identity_part(
            col(key.as_str()),
            &field_type(schema, key)?,
        )?);
    }
    Ok(concat(parts))
}

/// The non-null row identity of a derived outer-join step: both sides'
/// primary keys, length prefixed (`v<octets>:<text>`) so concatenating them
/// cannot collide; an unmatched row (a NULL-padded side) encodes as `n`.
///
/// The per-side texts are computed by the pair projection below the join
/// (see [`keyed_join_projection`], [`LEFT_IDENTITY_COLUMN`]); composing them
/// here keeps one collision-free identity for every outer join.
fn pair_identity_from_sides() -> Result<Expr> {
    let mut parts = Vec::new();
    for name in [LEFT_IDENTITY_COLUMN, RIGHT_IDENTITY_COLUMN] {
        let value = col(name);
        parts.push(
            when(value.clone().is_null(), lit("n"))
                .otherwise(value)
                .map_err(|error| report!("the row identity: {error}"))?,
        );
    }
    Ok(concat(parts).alias(IVM_PAIR_IDENTITY_COLUMN))
}

/// One length-prefixed component of a pair identity; NULL encodes as `n`.
///
/// A struct key recurses field by field: every part carries its own length or
/// null marker, so the concatenation stays collision free even when the text
/// forms could otherwise run together.
fn encoded_identity_part(value: Expr, data_type: &DataType) -> Result<Expr> {
    let encoded = match data_type {
        DataType::Struct(fields) => {
            let mut parts = vec![lit("s")];
            for field in fields {
                let field_value = get_field(value.clone(), field.name().as_str());
                parts.push(encoded_identity_part(field_value, field.data_type())?);
            }
            concat(parts)
        }
        _ => {
            let text = Expr::Cast(Cast::new(Box::new(value.clone()), DataType::Utf8));
            let length = Expr::Cast(Cast::new(
                Box::new(octet_length(text.clone())),
                DataType::Utf8,
            ));
            concat(vec![lit("v"), length, lit(":"), text])
        }
    };
    when(value.is_null(), lit("n"))
        .otherwise(encoded)
        .map_err(|error| report!("the row identity for a {data_type} key: {error}"))
}

/// Whether a key type can be encoded into a row identity: a struct recurses
/// through its fields, a list or map primary key cannot.
fn identity_type_supported(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| identity_type_supported(field.data_type())),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Union(_, _)
        | DataType::Null => false,
        _ => true,
    }
}

/// The identity keys of an outer-join spec hold only encodable types.
pub(crate) fn validate_identity_keys(left: &IvmTable, right: &IvmTable) -> Result<()> {
    for (table, side) in [(left, "left"), (right, "right")] {
        for key in &table.primary_keys {
            let data_type = field_type(&table.schema, key)?;
            if !identity_type_supported(&data_type) {
                return Err(report!(
                    "the row identity of a derived outer join cannot encode \
                     the {side} {data_type} primary key {key}"
                )
                .attach(crate::sql::UnsupportedShape));
            }
        }
    }
    Ok(())
}

/// The shape of a pair-keyed join used by the projection helpers.
struct PairJoin<'a> {
    join_keys: &'a [String],
    /// The right source's key columns when they differ from `join_keys`.
    right_keys: &'a [String],
    /// The rendered key expressions aligned with `join_keys`.
    key_exprs: &'a JoinKeyExprs,
    left_value: &'a str,
    right_value: &'a str,
    /// The wide output columns; empty means the compact payload shape.
    output_columns: &'a [JoinOutputColumn],
    left_primary_keys: &'a [String],
    right_primary_keys: &'a [String],
    /// The two sources, for the row identity's key types.
    left_table: &'a IvmTable,
    right_table: &'a IvmTable,
    /// An optional filter over `left_value` / `right_value` the joined pair
    /// must satisfy (a non-equality join condition).
    pair_filter: Option<&'a str>,
    /// Whether the MV materializes the non-null row identity column.
    identity_column: bool,
    /// Whether a NULL join key matches another NULL join key.
    null_safe: bool,
}

impl JoinView {
    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &self.join_keys,
            right_keys: &self.right_keys,
            key_exprs: &self.key_exprs,
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            left_table: &self.left,
            right_table: &self.right,
            pair_filter: self.pair_filter.as_deref(),
            identity_column: false,
            null_safe: self.null_safe,
        }
    }
}

impl LeftJoinView {
    fn parts(&self) -> PairJoin<'_> {
        PairJoin {
            join_keys: &self.join_keys,
            right_keys: &self.right_keys,
            key_exprs: &self.key_exprs,
            left_value: &self.left_value,
            right_value: &self.right_value,
            output_columns: &self.output_columns,
            left_primary_keys: &self.left.primary_keys,
            right_primary_keys: &self.right.primary_keys,
            left_table: &self.left,
            right_table: &self.right,
            pair_filter: None,
            identity_column: self.identity_column,
            null_safe: self.null_safe,
        }
    }
}

async fn latest_table_timestamp(
    runtime: &IvmRuntime,
    table: &IvmTable,
) -> Result<Option<i64>> {
    Ok(runtime
        .client
        .get_all_partition_info(&table.table_id)
        .await?
        .into_iter()
        .map(|partition| partition.timestamp)
        .max())
}

/// Whether a join view takes the keyed (upsert) path.
fn join_is_keyed(view: &JoinView) -> bool {
    !view.left.primary_keys.is_empty() || !view.right.primary_keys.is_empty()
}

/// Add the encoded join-key column to a contribution frame without collecting
/// the delta key batches in the driver.
fn encode_join_key_frame(
    context: &SessionContext,
    frame: DataFrame,
    source: &IvmTable,
    join_keys: &[String],
    encoder: super::join_state::JoinKeyEncoder,
) -> Result<DataFrame> {
    let input_types = join_keys
        .iter()
        .map(|key| Ok(source.schema.field_with_name(key)?.data_type().clone()))
        .collect::<Result<Vec<_>>>()?;
    let encoder = Arc::new(encoder);
    let name = format!("ivm_join_key_probe_{}", uuid::Uuid::new_v4().simple());
    let udf = datafusion::logical_expr::create_udf(
        &name,
        input_types,
        arrow::datatypes::DataType::Binary,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(move |args: &[datafusion::logical_expr::ColumnarValue]| {
            let arrays = datafusion::logical_expr::ColumnarValue::values_to_arrays(args)?;
            Ok(datafusion::logical_expr::ColumnarValue::Array(
                encoder.encode_columns(&arrays)?,
            ))
        }),
    );
    let key = udf.call(
        join_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>(),
    );
    context.register_udf(udf);
    Ok(frame.with_column(super::join_state::IVM_JOIN_KEY_COLUMN, key)?)
}

impl IvmRuntime {
    /// Symmetric lookup of the left rows whose join keys occur in the right
    /// delta.  Used by a right-only keyed join window so the left state does
    /// not have to be scanned in full.
    async fn lookup_join_left_state(
        &self,
        context: &SessionContext,
        view: &JoinView,
        left_state: Option<&IvmTable>,
        delta_right: &DataFrame,
    ) -> Result<Option<DataFrame>> {
        use super::join_state::JoinKeyEncoder;
        use super::keyed_plan::{
            IVM_STATE_COLUMN_PREFIX, IvmLookupTable, groups_by_bucket,
        };
        if view.key_exprs.left.iter().any(|expr| !expr.is_empty())
            || view.key_exprs.right.iter().any(|expr| !expr.is_empty())
        {
            return Ok(None);
        }
        let right_keys = if view.right_keys.is_empty() {
            &view.join_keys
        } else {
            &view.right_keys
        };
        let key_exprs = view
            .join_keys
            .iter()
            .enumerate()
            .map(|(index, key)| col(right_keys[index].as_str()).alias(key.as_str()))
            .collect::<Vec<_>>();
        let contribution = delta_right.clone().select(key_exprs)?.distinct()?;
        if let Some(left_state) = left_state {
            let encoder = JoinKeyEncoder::new(&view.left, &view.join_keys)?;
            let contribution = encode_join_key_frame(
                context,
                contribution,
                &view.left,
                &view.join_keys,
                encoder,
            )?;
            let bucket_count = if left_state.bucket_columns.len() == 1
                && left_state.bucket_columns[0] == super::join_state::IVM_JOIN_KEY_COLUMN
            {
                left_state
                    .hash_bucket_num
                    .parse::<usize>()
                    .ok()
                    .filter(|count| *count > 1)
                    .unwrap_or(1)
            } else {
                1
            };
            let groups = groups_by_bucket(
                left_state.current_groups(&self.client).await?,
                bucket_count,
            );
            let provider = IvmLookupTable::binary(
                contribution,
                left_state.clone(),
                groups,
                super::join_state::IVM_JOIN_KEY_COLUMN.to_string(),
                bucket_count,
                true,
            );
            let frame = context.read_table(Arc::new(provider))?;
            let projections = view
                .left
                .schema
                .fields()
                .iter()
                .map(|field| {
                    column_expr(&format!("{IVM_STATE_COLUMN_PREFIX}{}", field.name()))
                        .alias(field.name())
                })
                .collect::<Vec<_>>();
            let frame = frame.select(projections)?;
            return Ok(Some(filter_deletes(frame, change_column(&view.left))?));
        }
        if !view.left.primary_keys.is_empty() {
            return self
                .lookup_source_by_join_keys(
                    context,
                    &view.left,
                    contribution,
                    &view.join_keys,
                    &view.left.primary_keys,
                    true,
                )
                .await
                .map(Some);
        }
        Ok(None)
    }
}

impl IvmRuntime {
    /// Read all right rows matching the left delta's join keys.  Side state is
    /// used when present; otherwise the right source is the keyed state.
    async fn lookup_right_rows_for_left_keys(
        &self,
        context: &SessionContext,
        view: &JoinView,
        right_state: Option<&IvmTable>,
        right_join_keys: &[String],
        delta_left: &DataFrame,
    ) -> Result<DataFrame> {
        if let Some(right_state) = right_state
            && let Some(frame) = self
                .lookup_join_right_state(context, view, right_state, delta_left)
                .await?
        {
            return Ok(frame);
        }
        if view.right.primary_keys.is_empty()
            || !super::join_state::join_side_needs_state(&view.right, right_join_keys)
        {
            let keys = view
                .join_keys
                .iter()
                .map(|key| col(key.as_str()))
                .collect::<Vec<_>>();
            let contribution = delta_left.clone().select(keys)?.distinct()?;
            return self
                .lookup_source_by_join_keys(
                    context,
                    &view.right,
                    contribution,
                    &view.join_keys,
                    right_join_keys,
                    true,
                )
                .await;
        }
        Err(report!("right join state lookup is unavailable"))
    }

    /// Read all left rows matching the right delta's join keys.
    async fn lookup_left_rows_for_right_keys(
        &self,
        context: &SessionContext,
        view: &JoinView,
        left_state: Option<&IvmTable>,
        delta_right: &DataFrame,
    ) -> Result<DataFrame> {
        if let Some(frame) = self
            .lookup_join_left_state(context, view, left_state, delta_right)
            .await?
        {
            return Ok(frame);
        }
        if view.left.primary_keys.is_empty() {
            return Err(report!("left join state lookup is unavailable"));
        }
        let right_keys = if view.right_keys.is_empty() {
            &view.join_keys
        } else {
            &view.right_keys
        };
        let exprs = view
            .join_keys
            .iter()
            .enumerate()
            .map(|(index, key)| col(right_keys[index].as_str()).alias(key.as_str()))
            .collect::<Vec<_>>();
        let contribution = delta_right.clone().select(exprs)?.distinct()?;
        self.lookup_source_by_join_keys(
            context,
            &view.left,
            contribution,
            &view.join_keys,
            &view.left.primary_keys,
            true,
        )
        .await
    }

    /// Read a source/MV as the keyed state for a join.  The contribution keys
    /// are matched to real table PK columns with `arrow::Row`; bucket routing
    /// uses the table's writer bucket columns when they are part of the key.
    pub(crate) async fn lookup_source_by_join_keys(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        contribution: DataFrame,
        input_columns: &[String],
        table_columns: &[String],
        multi_match: bool,
    ) -> Result<DataFrame> {
        use super::keyed_plan::{
            IVM_STATE_COLUMN_PREFIX, IvmLookupTable, groups_by_bucket,
        };
        let bucket_count = table
            .hash_bucket_num
            .parse::<usize>()
            .ok()
            .filter(|count| *count > 1)
            .unwrap_or(1);
        let routed = contribution;
        let route_columns = if bucket_count > 1
            && !table.bucket_columns.is_empty()
            && table
                .bucket_columns
                .iter()
                .all(|column| table_columns.iter().any(|key| key == column))
        {
            let mut columns = Vec::with_capacity(table.bucket_columns.len());
            for bucket_column in &table.bucket_columns {
                let position = table_columns
                    .iter()
                    .position(|key| key == bucket_column)
                    .ok_or_else(|| {
                        report!("bucket column {bucket_column} is not a lookup key")
                    })?;
                columns.push(input_columns[position].clone());
            }
            Some(columns)
        } else {
            None
        };
        let bucket_count = route_columns.as_ref().map_or(1, |_| bucket_count);
        let groups =
            groups_by_bucket(table.current_groups(&self.client).await?, bucket_count);
        let provider = IvmLookupTable::columns(
            routed,
            table.clone(),
            groups,
            input_columns.to_vec(),
            table_columns.to_vec(),
            bucket_count,
            route_columns,
            multi_match,
        );
        let frame = context.read_table(Arc::new(provider))?;
        let projections = table
            .schema
            .fields()
            .iter()
            .map(|field| {
                column_expr(&format!("{IVM_STATE_COLUMN_PREFIX}{}", field.name()))
                    .alias(field.name())
            })
            .collect::<Vec<_>>();
        let mut frame = frame.select(projections)?;
        // An unmatched lookup contributes a null right row.  The source's
        // change marker is null on that row, so coalesce it to a live marker;
        // otherwise the generic delete filter would drop the null-padded
        // side and turn a LEFT JOIN into an inner join.
        if let Some(kind) = change_column(table) {
            let kind_expr = column_expr(kind);
            let value =
                datafusion::prelude::when(kind_expr.clone().is_null(), lit("insert"))
                    .otherwise(kind_expr)?;
            frame = frame.with_column(kind, value)?;
        }
        filter_deletes(frame, change_column(table))
    }

    /// Read the output pair rows for affected left/right source keys through
    /// the shared PK lookup plan.  This replaces the full MV scan used by the
    /// pair-state delete path.
    async fn lookup_output_pairs(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        view: &JoinView,
        affected_left: &DataFrame,
        affected_right: &DataFrame,
        right_index: Option<&IvmTable>,
    ) -> Result<DataFrame> {
        let left_columns = view
            .left
            .primary_keys
            .iter()
            .map(|key| left_pk_alias(key))
            .collect::<Vec<_>>();
        let right_columns = view
            .right
            .primary_keys
            .iter()
            .map(|key| right_pk_alias(key))
            .collect::<Vec<_>>();
        let left = self
            .lookup_output_side(
                context,
                table,
                affected_left,
                &view.left.primary_keys,
                &left_columns,
            )
            .await?;
        let right = self
            .lookup_output_side(
                context,
                right_index.unwrap_or(table),
                affected_right,
                &view.right.primary_keys,
                &right_columns,
            )
            .await?;
        let pairs = left.union(right)?.distinct()?;
        filter_deletes(pairs, change_column(table))
    }

    async fn lookup_output_side(
        &self,
        context: &SessionContext,
        table: &IvmTable,
        input: &DataFrame,
        input_columns: &[String],
        table_columns: &[String],
    ) -> Result<DataFrame> {
        use super::keyed_plan::{
            IVM_STATE_COLUMN_PREFIX, IvmLookupTable, groups_by_bucket,
        };
        let empty =
            context.read_batches(vec![RecordBatch::new_empty(table.schema.clone())])?;
        if input_columns.is_empty() || table_columns.is_empty() {
            return Ok(empty);
        }
        let bucket_count = table
            .hash_bucket_num
            .parse::<usize>()
            .ok()
            .filter(|count| *count > 1)
            .unwrap_or(1);
        let route_columns = if bucket_count > 1
            && !table.bucket_columns.is_empty()
            && table
                .bucket_columns
                .iter()
                .all(|column| table_columns.iter().any(|key| key == column))
        {
            let mut columns = Vec::with_capacity(table.bucket_columns.len());
            for bucket_column in &table.bucket_columns {
                let position = table_columns
                    .iter()
                    .position(|key| key == bucket_column)
                    .ok_or_else(|| {
                        report!("bucket column {bucket_column} is not an output key")
                    })?;
                columns.push(input_columns[position].clone());
            }
            Some(columns)
        } else {
            None
        };
        let bucket_count = route_columns.as_ref().map_or(1, |_| bucket_count);
        let groups =
            groups_by_bucket(table.current_groups(&self.client).await?, bucket_count);
        let provider = IvmLookupTable::columns(
            input.clone(),
            table.clone(),
            groups,
            input_columns.to_vec(),
            table_columns.to_vec(),
            bucket_count,
            route_columns,
            true,
        );
        let frame = context.read_table(Arc::new(provider))?;
        let projections = table
            .schema
            .fields()
            .iter()
            .map(|field| {
                column_expr(&format!("{IVM_STATE_COLUMN_PREFIX}{}", field.name()))
                    .alias(field.name())
            })
            .collect::<Vec<_>>();
        Ok(frame.select(projections)?)
    }

    /// Read only the right-side state rows whose join keys occur in the left
    /// delta.  The lookup is a physical plan: contribution keys are encoded,
    /// routed to the side state's hash buckets, and matched with multi-match
    /// semantics (one left key may have many right rows).
    async fn lookup_join_right_state(
        &self,
        context: &SessionContext,
        view: &JoinView,
        right_state: &IvmTable,
        delta_left: &DataFrame,
    ) -> Result<Option<DataFrame>> {
        use super::join_state::JoinKeyEncoder;
        use super::keyed_plan::{
            IVM_STATE_COLUMN_PREFIX, IvmLookupTable, groups_by_bucket,
        };
        let right_join_keys = if view.right_keys.is_empty() {
            &view.join_keys
        } else {
            &view.right_keys
        };
        // The contribution is encoded under the left names.  This fast path is
        // valid only when the right state was encoded from the same key names
        // and neither side uses a computed key expression.
        if right_join_keys != &view.join_keys
            || view.key_exprs.left.iter().any(|expr| !expr.is_empty())
            || view.key_exprs.right.iter().any(|expr| !expr.is_empty())
        {
            return Ok(None);
        }
        let encoder = JoinKeyEncoder::new(&view.left, &view.join_keys)?;
        let contribution = delta_left
            .clone()
            .select(
                view.join_keys
                    .iter()
                    .map(|key| col(key.as_str()))
                    .collect::<Vec<_>>(),
            )?
            .distinct()?;
        let contribution = encode_join_key_frame(
            context,
            contribution,
            &view.left,
            &view.join_keys,
            encoder,
        )?;
        let bucket_count = if right_state.bucket_columns.len() == 1
            && right_state.bucket_columns[0] == super::join_state::IVM_JOIN_KEY_COLUMN
        {
            right_state
                .hash_bucket_num
                .parse::<usize>()
                .ok()
                .filter(|count| *count > 1)
                .unwrap_or(1)
        } else {
            1
        };
        let groups = right_state.current_groups(&self.client).await?;
        let groups = groups_by_bucket(groups, bucket_count);
        let provider = IvmLookupTable::binary(
            contribution,
            right_state.clone(),
            groups,
            super::join_state::IVM_JOIN_KEY_COLUMN.to_string(),
            bucket_count,
            true,
        );
        let frame = context.read_table(Arc::new(provider))?;
        let right_columns = view
            .right
            .schema
            .fields()
            .iter()
            .map(|field| {
                column_expr(&format!("{IVM_STATE_COLUMN_PREFIX}{}", field.name()))
                    .alias(field.name())
            })
            .collect::<Vec<_>>();
        let frame = frame.select(right_columns)?;
        Ok(Some(filter_deletes(frame, change_column(&view.right))?))
    }
}

impl IvmRuntime {
    /// Write a join refresh output and, when configured, its keyed output
    /// state table.  The state table has the same schema and primary keys as
    /// the MV, so the pair state can be used as a lookup source by a later
    /// keyed merge plan.
    async fn append_join_rows(
        &self,
        output: &IvmTable,
        state: Option<&IvmTable>,
        pair_index: Option<&IvmTable>,
        frame: DataFrame,
    ) -> Result<Vec<String>> {
        let written = append_dataframe(output, &self.client, frame.clone()).await?;
        let mut commit_ids = written.commit_ids;
        if let Some(state) = state {
            let state_written =
                append_dataframe(state, &self.client, frame.clone()).await?;
            commit_ids.extend(state_written.commit_ids);
        }
        if let Some(index) = pair_index {
            let index_written = append_dataframe(index, &self.client, frame).await?;
            commit_ids.extend(index_written.commit_ids);
        }
        Ok(commit_ids)
    }

    /// Mirror the current logical output into a newly attached state table.
    async fn sync_join_state(
        &self,
        output: &IvmTable,
        state: &IvmTable,
    ) -> Result<Vec<String>> {
        let context = refresh_session();
        let frame = self.current_table_frame(&context, output)?;
        state.truncate(&self.client).await?;
        Ok(append_dataframe(state, &self.client, frame)
            .await?
            .commit_ids)
    }

    async fn ensure_join_state(
        &self,
        output: &IvmTable,
        state: Option<&IvmTable>,
    ) -> Result<Vec<String>> {
        let Some(state) = state else {
            return Ok(Vec::new());
        };
        let output_files = output.current_files(&self.client).await?;
        let state_files = state.current_files(&self.client).await?;
        let state_stale = state_files.is_empty() && !output_files.is_empty();
        // The MV is written before its optional mirror state.  If the state
        // commit failed after the MV commit, the state's latest partition
        // timestamp is behind the MV's; repair it from the MV before applying
        // the next window.  Equal timestamps are left alone (both writes land
        // in the same refresh, and a compaction is allowed to be newer).
        let state_stale = state_stale
            || (!output_files.is_empty()
                && latest_table_timestamp(self, state).await?
                    < latest_table_timestamp(self, output).await?);
        if state_stale {
            self.sync_join_state(output, state).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Create or reuse the right-key pair index for a keyed join output.
    ///
    /// The output MV is bucketed by the left pair-key prefix.  This index
    /// repeats the same output rows with the right source identities first, so
    /// a right-key delete lookup can use its bucket and PK index instead of
    /// scanning every left bucket.
    async fn ensure_join_pair_index(
        &self,
        view: &JoinView,
        output: &IvmTable,
    ) -> Result<(Option<IvmTable>, Vec<String>)> {
        if view.left.primary_keys.is_empty() || view.right.primary_keys.is_empty() {
            return Ok((None, Vec::new()));
        }
        if output
            .schema
            .field_with_name(&right_pk_alias(&view.right.primary_keys[0]))
            .is_err()
            || output
                .schema
                .field_with_name(&left_pk_alias(&view.left.primary_keys[0]))
                .is_err()
        {
            return Ok((None, Vec::new()));
        }
        let mut primary_keys = view
            .right
            .primary_keys
            .iter()
            .map(|key| right_pk_alias(key))
            .collect::<Vec<_>>();
        primary_keys.extend(view.left.primary_keys.iter().map(|key| left_pk_alias(key)));
        let bucket_columns = view
            .right
            .primary_keys
            .iter()
            .map(|key| right_pk_alias(key))
            .collect::<Vec<_>>();
        if let Some(existing) = self
            .metadata()
            .get_state(&view.view_id, StateRole::JoinPairIndex)
            .await?
        {
            let table = self.open_table_by_id(&existing.table_id).await?;
            if table.schema == output.schema && table.primary_keys == primary_keys {
                let output_files = output.current_files(&self.client).await?;
                let index_files = table.current_files(&self.client).await?;
                let stale = index_files.is_empty() && !output_files.is_empty()
                    || (!output_files.is_empty()
                        && latest_table_timestamp(self, &table).await?
                            < latest_table_timestamp(self, output).await?);
                if stale {
                    // The MV is committed before this duplicate index.  A
                    // crash between those two writes leaves a non-empty but
                    // behind index; repair it from the MV instead of using
                    // stale right-key rows for the next delete lookup.
                    let ids = self.sync_join_pair_index(output, &table).await?;
                    return Ok((Some(table), ids));
                }
                return Ok((Some(table), Vec::new()));
            }
            self.metadata
                .unregister_state(&view.view_id, StateRole::JoinPairIndex)
                .await?;
            let _ = self
                .client()
                .drop_table(&table.table_name, &table.namespace)
                .await;
        }
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let mut options = IvmTableOptions::new(
            format!("{}__ivm_right_index_{suffix}", output.table_name),
            format!("{}__ivm_right_index_{suffix}", output.table_path),
            output.schema.clone(),
        )
        .with_namespace(output.namespace.clone())
        .with_primary_keys(primary_keys)
        .with_bucket_columns(bucket_columns)
        .with_file_format(output.file_format);
        if options.schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_ok() {
            options = options.with_cdc_column(IVM_ROW_KINDS_COLUMN);
        }
        options.hash_bucket_num = output.hash_bucket_num.clone();
        let index = self.create_table(options).await?;
        self.metadata()
            .register_state(&view.view_id, StateRole::JoinPairIndex, &index)
            .await?;
        if !output.current_files(&self.client).await?.is_empty() {
            let ids = self.sync_join_pair_index(output, &index).await?;
            return Ok((Some(index), ids));
        }
        Ok((Some(index), Vec::new()))
    }

    async fn sync_join_pair_index(
        &self,
        output: &IvmTable,
        index: &IvmTable,
    ) -> Result<Vec<String>> {
        let context = refresh_session();
        let frame = self.current_table_frame(&context, output)?;
        index.truncate(&self.client).await?;
        Ok(append_dataframe(index, &self.client, frame)
            .await?
            .commit_ids)
    }

    /// Persist a join view spec (idempotent).
    pub async fn register_join_view(&self, view: &JoinView) -> Result<()> {
        let mut tables = vec![(StateRole::Mv, &view.output)];
        if let Some(state) = &view.state {
            tables.push((StateRole::State, state));
        }
        self.register_state_tables(&view.view_id, &tables).await?;
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

        commit_ids.extend(
            self.ensure_join_state(&view.output, view.state.as_ref())
                .await?,
        );
        let (pair_index, pair_index_ids) = if keyed {
            self.ensure_join_pair_index(view, &view.output).await?
        } else {
            (None, Vec::new())
        };
        commit_ids.extend(pair_index_ids);
        let left_side_state = if keyed {
            self.ensure_join_side_state(
                &view.view_id,
                &view.left,
                &view.join_keys,
                StateRole::JoinLeft,
            )
            .await?
        } else {
            None
        };
        let right_side_state = if keyed {
            self.ensure_join_side_state(
                &view.view_id,
                &view.right,
                if view.right_keys.is_empty() {
                    &view.join_keys
                } else {
                    &view.right_keys
                },
                StateRole::JoinRight,
            )
            .await?
        } else {
            None
        };
        if let Some(state) = &left_side_state {
            let ids = if state.current_files(&self.client).await?.is_empty() {
                self.sync_join_side_state(
                    &view.left,
                    state,
                    &view.join_keys,
                    &view.key_exprs.left,
                )
                .await?
            } else {
                self.apply_join_side_delta(
                    &view.left,
                    state,
                    &left_window,
                    &view.join_keys,
                    &view.key_exprs.left,
                )
                .await?
            };
            commit_ids.extend(ids);
        }
        let right_join_keys = if view.right_keys.is_empty() {
            &view.join_keys
        } else {
            &view.right_keys
        };
        if let Some(state) = &right_side_state {
            let ids = if state.current_files(&self.client).await?.is_empty() {
                self.sync_join_side_state(
                    &view.right,
                    state,
                    right_join_keys,
                    &view.key_exprs.right,
                )
                .await?
            } else {
                self.apply_join_side_delta(
                    &view.right,
                    state,
                    &right_window,
                    right_join_keys,
                    &view.key_exprs.right,
                )
                .await?
            };
            commit_ids.extend(ids);
        }
        self.checkpoint_epoch_states(&record).await?;
        let context = refresh_session();
        if keyed {
            let delta_left = self.groups_table_frame(
                &context,
                &view.left,
                left_window.added_files.clone(),
            )?;
            let delta_right = self.groups_table_frame(
                &context,
                &view.right,
                right_window.added_files.clone(),
            )?;
            let left_changed = !left_window.added_files.is_empty();
            let right_changed = !right_window.added_files.is_empty();
            let left_delta = apply_side_filter(
                &context,
                filter_deletes(delta_left.clone(), change_column(&view.left))?,
                &view.left.schema,
                view.left_filter.as_deref(),
            )?;
            let right_delta = apply_side_filter(
                &context,
                filter_deletes(delta_right.clone(), change_column(&view.right))?,
                &view.right.schema,
                view.right_filter.as_deref(),
            )?;

            let full_left = || -> Result<DataFrame> {
                let frame = if let Some(state) = &left_side_state {
                    self.current_table_frame(&context, state)?
                } else {
                    self.current_table_frame(&context, &view.left)?
                };
                apply_side_filter(
                    &context,
                    filter_deletes(frame, change_column(&view.left))?,
                    &view.left.schema,
                    view.left_filter.as_deref(),
                )
            };
            let full_right = || -> Result<DataFrame> {
                let frame = if let Some(state) = &right_side_state {
                    self.current_table_frame(&context, state)?
                } else {
                    self.current_table_frame(&context, &view.right)?
                };
                apply_side_filter(
                    &context,
                    filter_deletes(frame, change_column(&view.right))?,
                    &view.right.schema,
                    view.right_filter.as_deref(),
                )
            };

            // Both sides are keyed and use plain key columns: the changed
            // rows are the deltas, and the opposite side is read by the
            // delta's join keys.  This avoids a full read of either state.
            let fast = !view.null_safe
                && !view.left.primary_keys.is_empty()
                && !view.right.primary_keys.is_empty()
                && (view.right_keys.is_empty() || view.right_keys == view.join_keys)
                && view.key_exprs.left.iter().all(|expr| expr.is_empty())
                && view.key_exprs.right.iter().all(|expr| expr.is_empty());
            let empty_left = context
                .read_batches(vec![RecordBatch::new_empty(view.left.schema.clone())])?;
            let empty_right = context
                .read_batches(vec![RecordBatch::new_empty(view.right.schema.clone())])?;
            let fast_current = if fast {
                let left_changed_rows = if left_changed {
                    left_delta.clone()
                } else {
                    empty_left.clone()
                };
                let right_changed_rows = if right_changed {
                    right_delta.clone()
                } else {
                    empty_right.clone()
                };
                let right_for_left = if left_changed {
                    apply_side_filter(
                        &context,
                        self.lookup_right_rows_for_left_keys(
                            &context,
                            view,
                            right_side_state.as_ref(),
                            right_join_keys,
                            &delta_left,
                        )
                        .await?,
                        &view.right.schema,
                        view.right_filter.as_deref(),
                    )?
                } else {
                    empty_right.clone()
                };
                let left_for_right = if right_changed {
                    apply_side_filter(
                        &context,
                        self.lookup_left_rows_for_right_keys(
                            &context,
                            view,
                            left_side_state.as_ref(),
                            &delta_right,
                        )
                        .await?,
                        &view.left.schema,
                        view.left_filter.as_deref(),
                    )?
                } else {
                    empty_left.clone()
                };
                Some(
                    keyed_join_projection(
                        &context,
                        left_changed_rows,
                        right_for_left,
                        &view.parts(),
                        JoinType::Inner,
                        false,
                    )?
                    .union(keyed_join_projection(
                        &context,
                        left_for_right,
                        right_changed_rows,
                        &view.parts(),
                        JoinType::Inner,
                        false,
                    )?)?
                    .distinct()?,
                )
            } else {
                None
            };

            // One-sided non-fast windows still avoid the unchanged side's
            // full state; both-sided non-fast windows fall back to the full
            // inclusion-exclusion reads.
            let (left_now, right_now) = if fast {
                (empty_left.clone(), empty_right.clone())
            } else {
                let left_now = if left_changed && !right_changed {
                    left_delta.clone()
                } else if right_changed && !left_changed {
                    let mut lookup = self
                        .lookup_join_left_state(
                            &context,
                            view,
                            left_side_state.as_ref(),
                            &delta_right,
                        )
                        .await?;
                    if let Some(frame) = lookup.take() {
                        apply_side_filter(
                            &context,
                            frame,
                            &view.left.schema,
                            view.left_filter.as_deref(),
                        )?
                    } else {
                        full_left()?
                    }
                } else {
                    full_left()?
                };
                let right_now = if right_changed && !left_changed {
                    right_delta.clone()
                } else if left_changed && !right_changed {
                    let mut lookup = None;
                    if let Some(right_state) = &right_side_state {
                        lookup = self
                            .lookup_join_right_state(
                                &context,
                                view,
                                right_state,
                                &delta_left,
                            )
                            .await?;
                    } else if !view.right.primary_keys.is_empty()
                        && !super::join_state::join_side_needs_state(
                            &view.right,
                            right_join_keys,
                        )
                        && view.key_exprs.left.iter().all(|expr| expr.is_empty())
                        && view.key_exprs.right.iter().all(|expr| expr.is_empty())
                    {
                        let keys = view
                            .join_keys
                            .iter()
                            .map(|key| col(key.as_str()))
                            .collect::<Vec<_>>();
                        let contribution = delta_left.clone().select(keys)?.distinct()?;
                        lookup = Some(
                            self.lookup_source_by_join_keys(
                                &context,
                                &view.right,
                                contribution,
                                &view.join_keys,
                                right_join_keys,
                                true,
                            )
                            .await?,
                        );
                    }
                    if let Some(frame) = lookup {
                        apply_side_filter(
                            &context,
                            frame,
                            &view.right.schema,
                            view.right_filter.as_deref(),
                        )?
                    } else {
                        full_right()?
                    }
                } else {
                    full_right()?
                };
                (left_now, right_now)
            };
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
            let pairs = self
                .lookup_output_pairs(
                    &context,
                    view.state.as_ref().unwrap_or(&view.output),
                    view,
                    &affected_left,
                    &affected_right,
                    pair_index.as_ref(),
                )
                .await?;
            let current = if let Some(current) = fast_current {
                current
            } else {
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
                keyed_join_projection(
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
                .distinct()?
            };

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
            let already = pairs
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(pair_exprs.clone())?
                .distinct()?;
            let inserts = current
                .join(already, JoinType::LeftAnti, &pair_refs, &pair_refs, None)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

            // Retract the previous pairs of the affected rows; rows written by
            // this epoch are kept so a replay is idempotent.
            let active = pairs
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?;
            let deletes = active
                .select(keyed_join_output_columns(
                    &view.parts(),
                    IdentitySource::Stored,
                )?)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

            // Delete rows must arrive before the replacement insert of the
            // same pair for merge-on-read.
            let mut sort_exprs = pair_exprs;
            sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
            commit_ids.extend(
                self.append_join_rows(
                    &view.output,
                    view.state.as_ref(),
                    pair_index.as_ref(),
                    inserts.union(deletes)?.sort_by(sort_exprs)?,
                )
                .await?,
            );
        } else {
            let left_delta = self.groups_table_frame(
                &context,
                &view.left,
                left_window.added_files.clone(),
            )?;
            let right_delta = self.groups_table_frame(
                &context,
                &view.right,
                right_window.added_files.clone(),
            )?;
            let left_before = self.as_of_table_frame(
                &context,
                &view.left,
                left_window.before_timestamp,
            )?;
            let right_before = self.as_of_table_frame(
                &context,
                &view.right,
                right_window.before_timestamp,
            )?;

            let left_filtered = |frame: DataFrame| {
                apply_side_filter(
                    &context,
                    frame,
                    &view.left.schema,
                    view.left_filter.as_deref(),
                )
            };
            let right_filtered = |frame: DataFrame| {
                apply_side_filter(
                    &context,
                    frame,
                    &view.right.schema,
                    view.right_filter.as_deref(),
                )
            };
            let terms = [
                join_projection(
                    &context,
                    left_filtered(left_delta.clone())?,
                    right_filtered(right_before)?,
                    view,
                )?,
                join_projection(
                    &context,
                    left_filtered(left_before)?,
                    right_filtered(right_delta.clone())?,
                    view,
                )?,
                join_projection(
                    &context,
                    left_filtered(left_delta)?,
                    right_filtered(right_delta)?,
                    view,
                )?,
            ];
            let mut combined = terms[0].clone();
            for term in &terms[1..] {
                combined = combined.union(term.clone())?;
            }
            commit_ids.extend(
                self.append_join_rows(
                    &view.output,
                    view.state.as_ref(),
                    None,
                    combined.with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
                )
                .await?,
            );
        }

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        commit_ids.extend(
            self.ensure_join_state(&view.output, view.state.as_ref())
                .await?,
        );
        let context = refresh_session();
        let left_changed = !left_window.added_files.is_empty();
        let right_changed = !right_window.added_files.is_empty();
        // The window deltas are the merged current state of the changed keys
        // and are read once: they seed the affected keys and, when the other
        // side did not change, replace that side's source read entirely.
        let delta_left = if left_changed {
            Some(self.groups_table_frame(
                &context,
                &view.left,
                left_window.added_files.clone(),
            )?)
        } else {
            None
        };
        let delta_right = if right_changed {
            Some(self.groups_table_frame(
                &context,
                &view.right,
                right_window.added_files.clone(),
            )?)
        } else {
            None
        };

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

        // The changed right key values, under the left key names: a computed
        // left key is materialized by the probe below, so the probe joins by
        // name.  Delete tombstones are kept: a right row that moved leaves
        // left rows at its old key to retract.
        let changed_keys = match &delta_right {
            Some(delta_right) => {
                let delta_schema = delta_right.schema().clone();
                Some(
                    delta_right
                        .clone()
                        .select(
                            view.join_keys
                                .iter()
                                .enumerate()
                                .map(|(index, key)| {
                                    let expr = match view
                                        .key_exprs
                                        .right
                                        .get(index)
                                        .filter(|expr| !expr.is_empty())
                                    {
                                        Some(rendered) => {
                                            context.state().create_logical_expr(
                                                rendered,
                                                &delta_schema,
                                            )?
                                        }
                                        None => {
                                            col(view.right_join_keys()[index].as_str())
                                        }
                                    };
                                    Ok(expr.alias(key.as_str()))
                                })
                                .collect::<Result<Vec<_>>>()?,
                        )?
                        .distinct()?,
                )
            }
            None => None,
        };

        // A left-only change is just the delta.  For a right-only change the
        // left source is usually keyed by the lookup keys, so probe it with
        // the changed right keys instead of scanning the full left state.
        let left_now = if !right_changed {
            delta_left
                .clone()
                .ok_or_else(|| report!("lookup refresh without a left change"))?
        } else if !left_changed
            && view.left.primary_keys == view.join_keys
            && view.key_exprs.left.iter().all(|expr| expr.is_empty())
            && view.key_exprs.right.iter().all(|expr| expr.is_empty())
        {
            let changed_keys = changed_keys
                .clone()
                .ok_or_else(|| report!("lookup refresh without changed keys"))?;
            self.lookup_source_by_join_keys(
                &context,
                &view.left,
                changed_keys,
                &view.join_keys,
                &view.left.primary_keys,
                true,
            )
            .await?
        } else {
            self.current_table_frame(&context, &view.left)?
        };
        let mut left_now = filter_deletes(left_now, change_column(&view.left))?;
        if let Some(filter) = view.left_filter.as_deref() {
            left_now =
                left_now.filter(parse_filter(&context, &view.left.schema, filter)?)?;
        }

        // Symmetrically, a right-only change is the right delta; a left
        // change needs the current right rows to re-evaluate its projections.
        let right_now = if !left_changed {
            delta_right
                .clone()
                .ok_or_else(|| report!("lookup refresh without a right change"))?
        } else if !right_changed {
            // The right source is keyed by the lookup keys, so a left-only
            // window can read exactly the referenced right rows instead of the
            // full right state.  Unmatched left keys still produce a null right
            // side for the LEFT JOIN.
            if view.key_exprs.left.iter().any(|expr| !expr.is_empty())
                || view.key_exprs.right.iter().any(|expr| !expr.is_empty())
            {
                self.current_table_frame(&context, &view.right)?
            } else {
                let delta_left = delta_left
                    .clone()
                    .ok_or_else(|| report!("lookup refresh without a left change"))?;
                let keys = view
                    .join_keys
                    .iter()
                    .map(|key| col(key.as_str()))
                    .collect::<Vec<_>>();
                let contribution = delta_left.select(keys)?.distinct()?;
                let right_keys = view.right_join_keys();
                self.lookup_source_by_join_keys(
                    &context,
                    &view.right,
                    contribution,
                    &view.join_keys,
                    right_keys,
                    false,
                )
                .await?
            }
        } else {
            self.current_table_frame(&context, &view.right)?
        };
        let right_now = apply_side_filter(
            &context,
            filter_deletes(right_now, change_column(&view.right))?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;

        // The changed left rows plus the left rows referencing a changed
        // right row.
        let mut affected = left_now
            .clone()
            .select(key_exprs.clone())?
            .limit(0, Some(0))?;
        if let Some(delta_left) = &delta_left {
            affected = delta_left
                .clone()
                .select(key_exprs.clone())?
                .distinct()?
                .union(affected)?;
        }
        if let Some(changed_keys) = &changed_keys {
            // The computed key is materialized under its join name.
            let probe = with_computed_keys(
                &context,
                left_now.clone(),
                &view.key_exprs.left,
                &view.join_keys,
            )?;
            affected = affected.union(
                probe
                    .join(
                        changed_keys.clone(),
                        JoinType::LeftSemi,
                        &join_key_names,
                        &join_key_names,
                        None,
                    )?
                    .select(key_exprs.clone())?,
            )?;
        }
        let affected = affected.distinct()?;

        // Read the output state.  A right-only window can probe the output by
        // its left primary key when that is the output identity; otherwise the
        // semi-joins below select the affected rows from the streaming state.
        let state_table = view.state.as_ref().unwrap_or(&view.output);
        let mv = if !left_changed
            && state_table.primary_keys == view.left.primary_keys
            && view.key_exprs.left.iter().all(|expr| expr.is_empty())
        {
            self.lookup_source_by_join_keys(
                &context,
                state_table,
                affected.clone(),
                &view.left.primary_keys,
                &state_table.primary_keys,
                true,
            )
            .await?
        } else {
            self.current_table_frame(&context, state_table)?
        };

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
        commit_ids.extend(
            self.append_join_rows(
                &view.output,
                view.state.as_ref(),
                None,
                inserts.union(deletes)?.sort_by(sort_exprs)?,
            )
            .await?,
        );
        self.checkpoint_epoch_states(&record).await?;

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        let context = refresh_session();
        let mut left_now = filter_deletes(
            self.groups_table_frame(&context, &view.left, left_baseline.groups.clone())?,
            change_column(&view.left),
        )?;
        if let Some(filter) = view.left_filter.as_deref() {
            left_now =
                left_now.filter(parse_filter(&context, &view.left.schema, filter)?)?;
        }
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                self.groups_table_frame(
                    &context,
                    &view.right,
                    right_baseline.groups.clone(),
                )?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        commit_ids.extend(
            append_dataframe(
                &view.output,
                &self.client,
                lookup_join_projection(left_now, right_now, view)?
                    .select(lookup_join_output_columns(view))?
                    .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
            )
            .await?
            .commit_ids,
        );

        if let Some(state) = &view.state {
            commit_ids.extend(self.sync_join_state(&view.output, state).await?);
        }
        self.checkpoint_epoch_states(&record).await?;

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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
        let mut tables = vec![(StateRole::Mv, &view.output)];
        if let Some(state) = &view.state {
            tables.push((StateRole::State, state));
        }
        self.register_state_tables(&view.view_id, &tables).await?;
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

        let context = refresh_session();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                self.current_table_frame(&context, &view.left)?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                self.current_table_frame(&context, &view.right)?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let mv_batches = match single_side_join_mv_partitions(
            &view.output,
            &view.left,
            &left_window,
            &view.right,
            &right_window,
        ) {
            Some(restriction) => {
                self.current_table_frame_filtered_partitions(
                    &context,
                    &view.output,
                    Vec::new(),
                    &restriction,
                )
                .await?
            }
            None => self.current_table_frame(&context, &view.output)?,
        };
        let mv = mv_batches;

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
        let right_join_keys = if view.right_keys.is_empty() {
            &view.join_keys
        } else {
            &view.right_keys
        };

        // The pair join rewrites every pair of an affected row.  A changed
        // join key can invalidate the other side's old match, so the seed
        // starts from the current keys: conservative (the refresh is O(view
        // size), a recorded follow-up) but correct.
        let mut affected = left_now.clone().select(key_exprs.clone())?;
        if !left_window.added_files.is_empty() {
            let delta_left = self.groups_table_frame(
                &context,
                &view.left,
                left_window.added_files.clone(),
            )?;
            affected = delta_left
                .select(key_exprs.clone())?
                .distinct()?
                .union(affected)?;
        }
        if !right_window.added_files.is_empty() {
            let delta_right = self.groups_table_frame(
                &context,
                &view.right,
                right_window.added_files.clone(),
            )?;
            // The changed right key values, under the left key names: a
            // computed left key is materialized below, so the probe joins by
            // name.
            let delta_schema = delta_right.schema().clone();
            let changed_keys = delta_right
                .select(
                    view.join_keys
                        .iter()
                        .enumerate()
                        .map(|(index, key)| {
                            let expr = match view
                                .key_exprs
                                .right
                                .get(index)
                                .filter(|expr| !expr.is_empty())
                            {
                                Some(rendered) => context
                                    .state()
                                    .create_logical_expr(rendered, &delta_schema)?,
                                None => col(right_join_keys[index].as_str()),
                            };
                            Ok(expr.alias(key.as_str()))
                        })
                        .collect::<Result<Vec<_>>>()?,
                )?
                .distinct()?;
            // The computed key is materialized under its join name.
            let probe = with_computed_keys(
                &context,
                left_now.clone(),
                &view.key_exprs.left,
                &view.join_keys,
            )?;
            affected = affected.union(
                keyed_semi_join(
                    &context,
                    probe,
                    changed_keys,
                    &join_key_names,
                    &join_key_names,
                    view.null_safe,
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
        .select(keyed_join_output_columns(
            &view.parts(),
            IdentitySource::EncodedSides,
        )?)?
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
            .select(keyed_join_output_columns(
                &view.parts(),
                IdentitySource::Stored,
            )?)?
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
        commit_ids.extend(
            append_dataframe(
                &view.output,
                &self.client,
                inserts.union(deletes)?.sort_by(sort_exprs)?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        let context = refresh_session();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                self.groups_table_frame(
                    &context,
                    &view.left,
                    left_baseline.groups.clone(),
                )?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                self.groups_table_frame(
                    &context,
                    &view.right,
                    right_baseline.groups.clone(),
                )?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let rows = keyed_join_projection(
            &context,
            left_now,
            right_now,
            &view.parts(),
            JoinType::Left,
            false,
        )?
        .select(keyed_join_output_columns(
            &view.parts(),
            IdentitySource::EncodedSides,
        )?)?
        .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        commit_ids.extend(
            append_dataframe(&view.output, &self.client, rows)
                .await?
                .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        let context = refresh_session();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                self.current_table_frame(&context, &view.left)?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                self.current_table_frame(&context, &view.right)?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let mv_batches = match single_side_join_mv_partitions(
            &view.output,
            &view.left,
            &left_window,
            &view.right,
            &right_window,
        ) {
            Some(restriction) => {
                self.current_table_frame_filtered_partitions(
                    &context,
                    &view.output,
                    Vec::new(),
                    &restriction,
                )
                .await?
            }
            None => self.current_table_frame(&context, &view.output)?,
        };
        let mv = mv_batches;

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
        // The key columns under their own names: a computed key is selected
        // as its expression.
        let join_key_exprs = key_columns(
            &context,
            left_now.schema(),
            &view.key_exprs.left,
            &view.join_keys,
        )?;
        let right_join_key_exprs = key_columns(
            &context,
            right_now.schema(),
            &view.key_exprs.right,
            right_join_names,
        )?;

        // Same conservative seed as the pair left join: both sides' current
        // keys, because a key change on one side can drop the other side's
        // old match (a recorded optimization is to add only the before-match
        // keys from the materialized pairs).
        let mut affected_left = left_now.clone().select(left_key_exprs.clone())?;
        let mut affected_right = right_now.clone().select(right_key_exprs.clone())?;
        if !left_window.added_files.is_empty() {
            let delta_left = apply_side_filter(
                &context,
                self.groups_table_frame(
                    &context,
                    &view.left,
                    left_window.added_files.clone(),
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
            let probe = with_computed_keys(
                &context,
                right_now.clone(),
                &view.key_exprs.right,
                right_join_names,
            )?;
            affected_right = affected_right.union(
                keyed_semi_join(
                    &context,
                    probe,
                    changed_keys,
                    &right_join_key_names,
                    &join_key_names,
                    view.null_safe,
                )?
                .select(right_key_exprs.clone())?,
            )?;
        }
        if !right_window.added_files.is_empty() {
            let delta_right = apply_side_filter(
                &context,
                self.groups_table_frame(
                    &context,
                    &view.right,
                    right_window.added_files.clone(),
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
            let changed_keys = delta_right
                .select(right_join_key_exprs.clone())?
                .distinct()?;
            let probe = with_computed_keys(
                &context,
                left_now.clone(),
                &view.key_exprs.left,
                &view.join_keys,
            )?;
            affected_left = affected_left.union(
                keyed_semi_join(
                    &context,
                    probe,
                    changed_keys,
                    &join_key_names,
                    &right_join_key_names,
                    view.null_safe,
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
        // The pair projection carries each side's encoded identity; compose
        // them here and drop the per-side columns on the way out.
        let inserts = left_pairs.union(right_pairs)?.distinct()?.select(
            keyed_join_output_columns(&view.parts(), IdentitySource::EncodedSides)?,
        )?;
        let inserts = inserts
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
            .select(keyed_join_output_columns(
                &view.parts(),
                IdentitySource::Stored,
            )?)?
            .union(
                mv.join(
                    affected_right,
                    JoinType::LeftSemi,
                    &right_alias_refs,
                    &right_key_names,
                    None,
                )?
                .select(keyed_join_output_columns(
                    &view.parts(),
                    IdentitySource::Stored,
                )?)?,
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
        commit_ids.extend(
            append_dataframe(
                &view.output,
                &self.client,
                inserts.union(deletes)?.sort_by(sort_exprs)?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        let context = refresh_session();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                self.groups_table_frame(
                    &context,
                    &view.left,
                    left_baseline.groups.clone(),
                )?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                self.groups_table_frame(
                    &context,
                    &view.right,
                    right_baseline.groups.clone(),
                )?,
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
        // The pair projection carries each side's encoded identity; compose
        // them here and drop the per-side columns on the way out.
        let inserts = left_pairs.union(right_pairs)?.distinct()?.select(
            keyed_join_output_columns(&view.parts(), IdentitySource::EncodedSides)?,
        )?;
        commit_ids.extend(
            append_dataframe(
                &view.output,
                &self.client,
                inserts
                    .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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
        let context = refresh_session();

        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                self.current_table_frame(&context, &view.left)?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                self.current_table_frame(&context, &view.right)?,
                change_column(&view.right),
            )?,
            &view.right.schema,
            view.right_filter.as_deref(),
        )?;
        let mv_batches = match single_side_join_mv_partitions(
            &view.output,
            &view.left,
            &left_window,
            &view.right,
            &right_window,
        ) {
            Some(restriction) => {
                self.current_table_frame_filtered_partitions(
                    &context,
                    &view.output,
                    Vec::new(),
                    &restriction,
                )
                .await?
            }
            None => self.current_table_frame(&context, &view.output)?,
        };
        let mv = mv_batches;

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
            let delta_left = self.groups_table_frame(
                &context,
                &view.left,
                left_window.added_files.clone(),
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
            let delta_right = self.groups_table_frame(
                &context,
                &view.right,
                right_window.added_files.clone(),
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
            .select(keyed_join_output_columns(
                &parts,
                IdentitySource::EncodedSides,
            )?)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let deletes = deletes
            .ok_or_else(no_change)?
            .select(keyed_join_output_columns(&parts, IdentitySource::Stored)?)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

        let mut pair_keys = left_alias_names;
        pair_keys.extend(right_alias_names);
        let mut sort_exprs = pair_keys
            .iter()
            .map(|key| col(key.as_str()))
            .collect::<Vec<_>>();
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        commit_ids.extend(
            append_dataframe(
                &view.output,
                &self.client,
                inserts.union(deletes)?.sort_by(sort_exprs)?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        let context = refresh_session();
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                self.groups_table_frame(
                    &context,
                    &view.left,
                    left_baseline.groups.clone(),
                )?,
                change_column(&view.left),
            )?,
            &view.left.schema,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                self.groups_table_frame(
                    &context,
                    &view.right,
                    right_baseline.groups.clone(),
                )?,
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
        .select(keyed_join_output_columns(
            &parts,
            IdentitySource::EncodedSides,
        )?)?
        .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        commit_ids.extend(
            append_dataframe(&view.output, &self.client, inserts)
                .await?
                .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        // A rebuild starts a new generation.  Rebuild the side states from the
        // source baseline as well; otherwise rows that disappeared before the
        // rebuild would remain visible to later keyed lookups.
        if keyed {
            let left_state = self
                .ensure_join_side_state(
                    &view.view_id,
                    &view.left,
                    &view.join_keys,
                    StateRole::JoinLeft,
                )
                .await?;
            if let Some(state) = left_state {
                commit_ids.extend(
                    self.sync_join_side_state(
                        &view.left,
                        &state,
                        &view.join_keys,
                        &view.key_exprs.left,
                    )
                    .await?,
                );
            }
            let right_keys = if view.right_keys.is_empty() {
                &view.join_keys
            } else {
                &view.right_keys
            };
            let right_state = self
                .ensure_join_side_state(
                    &view.view_id,
                    &view.right,
                    right_keys,
                    StateRole::JoinRight,
                )
                .await?;
            if let Some(state) = right_state {
                commit_ids.extend(
                    self.sync_join_side_state(
                        &view.right,
                        &state,
                        right_keys,
                        &view.key_exprs.right,
                    )
                    .await?,
                );
            }
        }

        let context = refresh_session();
        let left_empty = left_baseline
            .groups
            .iter()
            .all(|group| group.files.is_empty());
        let right_empty = right_baseline
            .groups
            .iter()
            .all(|group| group.files.is_empty());
        if !left_empty && !right_empty {
            let joined = if keyed {
                let left = apply_side_filter(
                    &context,
                    filter_deletes(
                        self.groups_table_frame(
                            &context,
                            &view.left,
                            left_baseline.groups.clone(),
                        )?,
                        change_column(&view.left),
                    )?,
                    &view.left.schema,
                    view.left_filter.as_deref(),
                )?;
                let right = apply_side_filter(
                    &context,
                    filter_deletes(
                        self.groups_table_frame(
                            &context,
                            &view.right,
                            right_baseline.groups.clone(),
                        )?,
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
                    &context,
                    apply_side_filter(
                        &context,
                        filter_deletes(
                            self.groups_table_frame(
                                &context,
                                &view.left,
                                left_baseline.groups.clone(),
                            )?,
                            change_column(&view.left),
                        )?,
                        &view.left.schema,
                        view.left_filter.as_deref(),
                    )?,
                    apply_side_filter(
                        &context,
                        filter_deletes(
                            self.groups_table_frame(
                                &context,
                                &view.right,
                                right_baseline.groups.clone(),
                            )?,
                            change_column(&view.right),
                        )?,
                        &view.right.schema,
                        view.right_filter.as_deref(),
                    )?,
                    view,
                )?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            };
            commit_ids.extend(
                append_dataframe(&view.output, &self.client, joined)
                    .await?
                    .commit_ids,
            );
        }

        // The right-key pair index and optional output-state mirror are part
        // of the same rebuilt epoch.
        if keyed {
            let (index, ids) = self.ensure_join_pair_index(view, &view.output).await?;
            commit_ids.extend(ids);
            if let Some(index) = index {
                commit_ids.extend(self.sync_join_pair_index(&view.output, &index).await?);
            }
        }
        if let Some(state) = &view.state {
            commit_ids.extend(self.sync_join_state(&view.output, state).await?);
        }
        self.checkpoint_epoch_states(&record).await?;

        let mv_versions = output_partition_versions(&self.client, &view.output).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

#[cfg(test)]
mod identity_encoding_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use datafusion::datasource::memory::MemTable;

    fn struct_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]))
    }

    /// A struct key encodes field by field with length prefixes, so values
    /// whose text forms could run together stay distinct, and NULL is `n`.
    #[test_log::test(tokio::test)]
    async fn struct_keys_encode_field_by_field() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", struct_type(), true),
            Field::new("id", DataType::Int64, false),
        ]));
        let points = StructArray::new(
            Fields::from(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("ab"),
                    Some("a"),
                    None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("bc"), Some("c"), None, None])),
            ],
            Some(NullBuffer::from(vec![true, true, true, false])),
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(points),
                Arc::new(Int64Array::from_iter_values(0..4)),
            ],
        )
        .unwrap();
        let context = refresh_session();
        context
            .register_table(
                "t",
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
            )
            .unwrap();
        let encoded = encoded_identity_part(col("s"), &struct_type())
            .unwrap()
            .alias("identity");
        let batches = context
            .table("t")
            .await
            .unwrap()
            .select(vec![col("id"), encoded])
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            let identities = batch.column(1);
            for row in 0..batch.num_rows() {
                rows.push(
                    arrow::util::display::array_value_to_string(identities, row).unwrap(),
                );
            }
        }
        assert_eq!(rows[3], "n", "a NULL struct encodes as n: {rows:?}");
        assert_ne!(
            rows[0], rows[1],
            "(\"a\", \"bc\") and (\"ab\", \"c\") must not collide: {rows:?}"
        );
        assert_ne!(
            rows[0], rows[2],
            "a NULL field must change the identity: {rows:?}"
        );
        assert_ne!(rows[1], rows[2]);
    }
}
