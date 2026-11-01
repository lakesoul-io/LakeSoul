// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The semi/anti views: `EXISTS`/`NOT EXISTS`/`IN` and the `INTERSECT`/`EXCEPT`
//! subset.

use super::*;
use arrow::array::{Array, BinaryArray};

/// One condition of a [`SemiAntiView`]:
/// `left.{left_column} {op} right.{right_column}`, or
/// `(left.{left_column} {op} right.{right_column}) IS NOT TRUE` for the `ALL`
/// quantifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemiAntiCondition {
    /// The left column.
    pub left_column: String,
    /// The right column.
    pub right_column: String,
    /// The comparison operator.
    pub op: CompareOp,
    /// Whether the condition matches when the comparison is false or unknown
    /// (`IS NOT TRUE`), as the maintained `x op ALL (subquery)` needs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_not_true: bool,
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
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the contributing right rows must satisfy.
    pub right_filter: Option<String>,
    /// Whether a NULL join key matches another NULL join key (set operations
    /// and null-aware predicates).
    pub null_safe: bool,
    /// Whether left rows sharing the join keys compete for the right
    /// multiplicity: `INTERSECT ALL` keeps the first `count_r` copies and
    /// `EXCEPT ALL` the `count_l - count_r` surplus.
    pub count_mode: bool,
    /// An optional aggregate over the right source; when present the right
    /// side contributes one value per join key (a correlated scalar subquery)
    /// instead of a row match.
    pub right_aggregate: Option<String>,
    /// The right group keys, aligned with [`Self::join_keys`]; empty means the
    /// same names as `join_keys`.
    pub right_keys: Vec<String>,
    /// The rendered comparison predicate over the left row and the aggregate
    /// output column ([`IVM_RIGHT_AGG_COLUMN`]).
    pub match_predicate: Option<String>,
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
            left_filter: None,
            right_filter: None,
            null_safe: false,
            count_mode: false,
            right_aggregate: None,
            right_keys: Vec::new(),
            match_predicate: None,
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

    /// Match NULL join keys with NULL join keys, as the set operations and
    /// null-aware predicates do.
    pub fn with_null_safe(mut self, null_safe: bool) -> Self {
        self.null_safe = null_safe;
        self
    }

    /// The right side is a per-join-key aggregate (a correlated scalar
    /// subquery) instead of a row match.
    pub fn with_right_aggregate(
        mut self,
        call: impl Into<String>,
        right_keys: Vec<String>,
        match_predicate: impl Into<String>,
    ) -> Self {
        self.right_aggregate = Some(call.into());
        self.right_keys = right_keys;
        self.match_predicate = Some(match_predicate.into());
        self
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
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            null_safe: self.null_safe,
            count_mode: self.count_mode,
            right_aggregate: self.right_aggregate.clone(),
            right_keys: self.right_keys.clone(),
            match_predicate: self.match_predicate.clone(),
        }
    }
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

/// The probe view used by the shared keyed join helpers for a semi/anti join.
fn semi_anti_probe(view: &SemiAntiView, left: &IvmTable, right: &IvmTable) -> JoinView {
    JoinView {
        view_id: view.view_id.clone(),
        left: left.clone(),
        right: right.clone(),
        output: view.mv.clone(),
        join_keys: view.join_keys.clone(),
        right_keys: view.right_keys.clone(),
        key_exprs: JoinKeyExprs::default(),
        left_value: String::new(),
        right_value: String::new(),
        left_filter: view.left_filter.clone(),
        right_filter: view.right_filter.clone(),
        pair_filter: None,
        null_safe: view.null_safe,
        output_columns: Vec::new(),
        state: None,
        refresh_interval_ms: view.refresh_interval_ms,
    }
}

/// Project both sources to the columns the side states need.  The returned
/// schemas are also the column lists the refresh materializes, so a rebuild
/// creates the same state shape as an incremental refresh.
fn semi_anti_state_sources(
    context: &SessionContext,
    view: &SemiAntiView,
) -> Result<(IvmTable, IvmTable, Vec<String>, Vec<String>)> {
    let mut left_columns = semi_anti_left_columns(view);
    if let Some(filter) = view.left_filter.as_deref() {
        for column in filter_columns(context, &view.left.schema, filter)? {
            if !left_columns.contains(&column) {
                left_columns.push(column);
            }
        }
    }
    if let Some(predicate) = view.match_predicate.as_deref() {
        let aggregate_type = right_aggregate_type(context, view)?;
        let schema = match_schema(&view.left.schema, &aggregate_type)?;
        for column in filter_columns(context, &schema, predicate)? {
            if column != IVM_RIGHT_AGG_COLUMN && !left_columns.contains(&column) {
                left_columns.push(column);
            }
        }
    }
    let mut right_columns = semi_anti_right_columns(view);
    if let Some(call) = view.right_aggregate.as_deref() {
        for column in filter_columns(context, &view.right.schema, call)? {
            if !right_columns.contains(&column) {
                right_columns.push(column);
            }
        }
    }
    if let Some(filter) = view.right_filter.as_deref() {
        for column in filter_columns(context, &view.right.schema, filter)? {
            if !right_columns.contains(&column) {
                right_columns.push(column);
            }
        }
    }
    let left_state_source = view
        .left
        .projected_state_source(&left_columns)
        .map_err(|error| report!("semi/anti left state projection: {error}"))?;
    let right_state_source = view
        .right
        .projected_state_source(&right_columns)
        .map_err(|error| report!("semi/anti right state projection: {error}"))?;
    Ok((
        left_state_source,
        right_state_source,
        left_columns,
        right_columns,
    ))
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
    let right_keys = right_join_keys(view);
    if view.join_keys.len() != right_keys.len() {
        return Err(report!(
            "semi/anti view {}: the join needs one right key per left key",
            view.view_id
        ));
    }
    for (left_key, right_key) in view.join_keys.iter().zip(right_keys) {
        view.left.schema.field_with_name(left_key).map_err(|_| {
            report!(
                "semi/anti view {}: join key {left_key} is not in the left source",
                view.view_id
            )
        })?;
        view.right.schema.field_with_name(right_key).map_err(|_| {
            report!(
                "semi/anti view {}: join key {right_key} is not in the right source",
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
    if view.count_mode {
        if view.right_aggregate.is_some() || view.match_predicate.is_some() {
            return Err(report!(
                "semi/anti view {}: a count mode view has no right aggregate",
                view.view_id
            ));
        }
        if !view.conditions.is_empty() {
            return Err(report!(
                "semi/anti view {}: a count mode view has no extra conditions",
                view.view_id
            ));
        }
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
    match view.right_aggregate.as_deref() {
        Some(call) => {
            if view.anti || !view.conditions.is_empty() || view.null_safe {
                return Err(report!(
                    "semi/anti view {}: an aggregate right side only supports a \
                     semi match",
                    view.view_id
                ));
            }
            let predicate = view.match_predicate.as_deref().ok_or_else(|| {
                report!("semi/anti view {} has no aggregate predicate", view.view_id)
            })?;
            let context = refresh_session();
            let aggregate_type = right_aggregate_type(&context, view)?;
            let schema = match_schema(&view.left.schema, &aggregate_type)?;
            parse_filter(&context, &schema, predicate).map_err(|error| {
                report!(
                    "semi/anti view {}: invalid aggregate {call:?}: {error}",
                    view.view_id
                )
            })?;
        }
        None => {
            if view.match_predicate.is_some() {
                return Err(report!(
                    "semi/anti view {}: an aggregate predicate without an \
                     aggregate",
                    view.view_id
                ));
            }
        }
    }
    Ok(())
}

/// Whether the right side of a plain SEMI/ANTI view can be reduced to a
/// shared existence set.  A condition, an aggregate, count mode or a right
/// filter needs the right row values, not only whether a key exists.
fn semi_anti_shared_existence_applies(view: &SemiAntiView) -> bool {
    view.conditions.is_empty()
        && view.right_aggregate.is_none()
        && !view.count_mode
        && view.right_filter.is_none()
        && !view.null_safe
        && !view.join_keys.is_empty()
        && !view.right.primary_keys.is_empty()
        && right_join_keys(view).len() == view.join_keys.len()
        && view
            .join_keys
            .iter()
            .zip(right_join_keys(view))
            .all(|(left, right)| {
                match (
                    view.left.schema.field_with_name(left),
                    view.right.schema.field_with_name(right),
                ) {
                    (Ok(left), Ok(right)) => left.data_type() == right.data_type(),
                    _ => false,
                }
            })
}

/// A one-column batch of encoded join keys.  The column name differs from
/// the probe's `__ivm_join_key` so the two frames can be joined without an
/// ambiguous reference.
const SHARED_RIGHT_KEY_COLUMN: &str = "__ivm_shared_right_key";

fn encoded_key_frame(
    context: &SessionContext,
    keys: &std::collections::HashSet<Vec<u8>>,
) -> Result<DataFrame> {
    let array = BinaryArray::from_iter_values(keys.iter().map(|key| key.as_slice()));
    let schema = Arc::new(Schema::new(vec![Field::new(
        SHARED_RIGHT_KEY_COLUMN,
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;
    dataframe(context, vec![batch], &schema)
}

/// The encoded join keys of materialized batches.
fn collect_batch_keys(
    batches: &[RecordBatch],
) -> Result<std::collections::HashSet<Vec<u8>>> {
    let mut keys = std::collections::HashSet::new();
    for batch in batches {
        let array = batch
            .column_by_name(super::join_state::IVM_JOIN_KEY_COLUMN)
            .ok_or_else(|| report!("encoded key frame has no join key column"))?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| report!("encoded join key is not binary"))?;
        for row in 0..array.len() {
            if !array.is_null(row) {
                keys.insert(array.value(row).to_vec());
            }
        }
    }
    Ok(keys)
}

/// Drain the encoded key column of a frame into a set.
async fn collect_encoded_keys(
    frame: DataFrame,
) -> Result<std::collections::HashSet<Vec<u8>>> {
    use futures::StreamExt;
    let mut keys = std::collections::HashSet::new();
    let mut stream = frame.execute_stream().await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let array = batch
            .column_by_name(super::join_state::IVM_JOIN_KEY_COLUMN)
            .ok_or_else(|| report!("encoded key frame has no join key column"))?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| report!("encoded join key is not binary"))?;
        for row in 0..array.len() {
            if !array.is_null(row) {
                keys.insert(array.value(row).to_vec());
            }
        }
    }
    Ok(keys)
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
    for column in view
        .join_keys
        .iter()
        .chain(view.left.primary_keys.iter())
        .chain(
            view.conditions
                .iter()
                .map(|condition| &condition.left_column),
        )
    {
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
    for column in right_join_keys(view)
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
    if let Some(change) = view
        .right
        .cdc_column
        .as_deref()
        .filter(|column| view.right.schema.field_with_name(column).is_ok())
        && !columns.iter().any(|column| column == change)
    {
        columns.push(change.to_string());
    }
    columns
}

/// The column the right aggregate output is aliased to; the match predicate
/// refers to it by this name.
pub const IVM_RIGHT_AGG_COLUMN: &str = "__ivm_right_agg";

/// The MV column holding the scalar subquery value when the select list only
/// computes over it (a computed-only left-aggregate projection).
pub const IVM_AGG_VALUE_COLUMN: &str = "__ivm_agg";

/// The name the `HAVING` subquery value carries in the recompute SQL; the
/// rendered predicate references it.
pub const IVM_HAVING_LOOKUP_COLUMN: &str = "__ivm_lookup";

/// The right group keys of a scalar-subquery view, falling back to the shared
/// join keys.
fn right_join_keys(view: &SemiAntiView) -> &[String] {
    if view.right_keys.is_empty() {
        &view.join_keys
    } else {
        &view.right_keys
    }
}

/// The logical type of the right aggregate output.
fn right_aggregate_type(
    context: &SessionContext,
    view: &SemiAntiView,
) -> Result<DataType> {
    let call = view
        .right_aggregate
        .as_deref()
        .ok_or_else(|| report!("semi/anti view {} has no aggregate", view.view_id))?;
    let df_schema = DFSchema::try_from(view.right.schema.as_ref().clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    let expression = context
        .state()
        .create_logical_expr(call, &df_schema)
        .map_err(|error| {
            report!(
                "semi/anti view {}: invalid aggregate {call:?}: {error}",
                view.view_id
            )
        })?;
    expression.get_type(&df_schema).map_err(|error| {
        report!(
            "semi/anti view {}: invalid aggregate {call:?}: {error}",
            view.view_id
        )
    })
}

/// The left schema plus the aggregate output column, for parsing the match
/// predicate.
fn match_schema(left: &SchemaRef, aggregate_type: &DataType) -> Result<SchemaRef> {
    let mut fields = left.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        IVM_RIGHT_AGG_COLUMN,
        aggregate_type.clone(),
        true,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// The left rows whose per-key right aggregate satisfies the match predicate:
/// a correlated scalar subquery maintained as a semi join against one
/// aggregate row per join key.
fn semi_aggregate_match(
    context: &SessionContext,
    left: DataFrame,
    right: DataFrame,
    view: &SemiAntiView,
) -> Result<DataFrame> {
    let call = view
        .right_aggregate
        .as_deref()
        .ok_or_else(|| report!("semi/anti view {} has no aggregate", view.view_id))?;
    let predicate = view.match_predicate.as_deref().ok_or_else(|| {
        report!("semi/anti view {} has no aggregate predicate", view.view_id)
    })?;
    let right_schema = right.schema().clone();
    let aggregate = context
        .state()
        .create_logical_expr(call, &right_schema)
        .map_err(|error| {
            report!(
                "semi/anti view {}: invalid aggregate {call:?}: {error}",
                view.view_id
            )
        })?;
    let group_exprs = right_join_keys(view)
        .iter()
        .map(|key| col(key.as_str()).alias(right_agg_key_alias(key)))
        .collect::<Vec<_>>();
    let aggregated =
        right.aggregate(group_exprs, vec![aggregate.alias(IVM_RIGHT_AGG_COLUMN)])?;
    let aggregate_type = aggregated
        .schema()
        .field_with_name(None, IVM_RIGHT_AGG_COLUMN)
        .map_err(|error| {
            report!(
                "semi/anti view {}: invalid aggregate {call:?}: {error}",
                view.view_id
            )
        })?
        .data_type()
        .clone();
    let schema = match_schema(&view.left.schema, &aggregate_type)?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    let filter = context
        .state()
        .create_logical_expr(predicate, &df_schema)
        .map_err(|error| {
            report!(
                "semi/anti view {}: invalid aggregate predicate {predicate:?}: {error}",
                view.view_id
            )
        })?;
    let left_keys = view
        .join_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    // The right group keys are aliased: a LEFT join keeps both sides'
    // columns, and unaliased same-named keys would collide in the join
    // schema.
    let right_keys = right_join_keys(view)
        .iter()
        .map(|key| right_agg_key_alias(key))
        .collect::<Vec<_>>();
    let right_key_refs = right_keys.iter().map(String::as_str).collect::<Vec<_>>();
    // The predicate is evaluated over a LEFT join: a left key without an
    // aggregate row is padded with a NULL aggregate, which the predicate's
    // empty-set form (`CASE WHEN ... IS NULL THEN 0 ...`, the COUNT family)
    // turns into the aggregate of no rows.  A value aggregate keeps NULL
    // there, so its comparison stays unknown and the row drops, exactly like
    // the semi join this replaces.
    let joined = left.join(
        aggregated,
        JoinType::Left,
        &left_keys,
        &right_key_refs,
        None,
    )?;
    Ok(joined.filter(filter)?)
}

/// The alias of an aggregate group key in the match join of a semi/anti view:
/// the right keys are renamed so a LEFT join cannot collide with the left
/// column names.
fn right_agg_key_alias(key: &str) -> String {
    format!("__ivm_right_agg_key_{key}")
}

/// The left rows that win the per-tuple multiplicity competition:
/// `INTERSECT ALL` keeps the rows with `rank <= count_r` and `EXCEPT ALL` the
/// rows with `rank > count_r` (a missing right tuple counts as zero).  The
/// rank orders the rows of a tuple by the left primary keys, so it is stable
/// across refreshes; NULL join keys partition and join null-safely, exactly
/// like the set operations.
async fn semi_count_qualifying(
    context: &SessionContext,
    left: DataFrame,
    right: DataFrame,
    view: &SemiAntiView,
) -> Result<DataFrame> {
    context.register_table("__ivm_count_left", left.into_view())?;
    context.register_table("__ivm_count_right", right.into_view())?;
    let quote = |columns: &[String]| {
        columns
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let left_keys = quote(&view.join_keys);
    let right_keys = quote(right_join_keys(view));
    let primary_keys = view
        .left
        .primary_keys
        .iter()
        .map(|key| format!("l.{}", quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let order = quote(&view.left.primary_keys);
    let on = view
        .join_keys
        .iter()
        .zip(right_join_keys(view))
        .map(|(left, right)| {
            format!(
                "l.{} IS NOT DISTINCT FROM r.{}",
                quote_ident(left),
                quote_ident(right)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let predicate = if view.anti {
        "r.__ivm_match_count IS NULL OR l.__ivm_rank > r.__ivm_match_count"
    } else {
        "r.__ivm_match_count IS NOT NULL AND l.__ivm_rank <= r.__ivm_match_count"
    };
    let sql = format!(
        "SELECT {primary_keys} FROM \
         (SELECT *, row_number() OVER (PARTITION BY {left_keys} ORDER BY {order}) \
          AS __ivm_rank FROM __ivm_count_left) l \
         LEFT JOIN \
         (SELECT {right_keys}, count(*) AS __ivm_match_count FROM __ivm_count_right \
          GROUP BY {right_keys}) r \
         ON {on} WHERE {predicate}"
    );
    Ok(context.sql(&sql).await?)
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

/// A null-safe semi join on columns shared by both frames (a set operation
/// matches a NULL key with another NULL key).
fn null_safe_semi_join(
    left: DataFrame,
    right: DataFrame,
    keys: &[String],
) -> Result<DataFrame> {
    let plan = LogicalPlanBuilder::from(left.logical_plan().clone())
        .join_detailed(
            right.logical_plan().clone(),
            JoinType::LeftSemi,
            (keys.to_vec(), keys.to_vec()),
            None,
            NullEquality::NullEqualsNull,
        )?
        .build()?;
    Ok(DataFrame::new(refresh_session().state().clone(), plan))
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
        .zip(right_join_keys(view))
        .map(|(left, right)| (left.clone(), semi_anti_right_alias(right)))
        .collect();
    let mut filter: Option<Expr> = None;
    for condition in &view.conditions {
        let alias = semi_anti_right_alias(&condition.right_column);
        if condition.op == CompareOp::Eq && !condition.is_not_true {
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
            let expr = if condition.is_not_true {
                Expr::IsNotTrue(Box::new(expr))
            } else {
                expr
            };
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
    if !view.null_safe {
        return Ok(left.join(right, join_type, &left_on, &right_on, filter)?);
    }
    // A NULL join key matches another NULL join key.
    let plan = LogicalPlanBuilder::from(left.logical_plan().clone())
        .join_detailed(
            right.logical_plan().clone(),
            join_type,
            (
                on_pairs.iter().map(|(left, _)| left.clone()).collect(),
                on_pairs.iter().map(|(_, right)| right.clone()).collect(),
            ),
            filter,
            NullEquality::NullEqualsNull,
        )?
        .build()?;
    Ok(DataFrame::new(refresh_session().state().clone(), plan))
}

impl IvmRuntime {
    /// Persist a semi/anti join view spec (idempotent).
    pub async fn register_semi_anti_view(&self, view: &SemiAntiView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
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

        let left_window = self
            .collect_source_window_for_filter(
                &view.view_id,
                &view.left,
                view.left_filter.as_deref(),
            )
            .await?;
        let right_window = self
            .collect_source_window_for_filter(
                &view.view_id,
                &view.right,
                view.right_filter.as_deref(),
            )
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
        let mut commit_ids = Vec::new();
        let side_keyed = !view.join_keys.is_empty()
            && !view.left.primary_keys.is_empty()
            && !view.right.primary_keys.is_empty();
        // Both side states only need the columns this view reads: the
        // output, the join keys, the primary keys and the change marker.
        // Keeping the whole source would copy every unused payload column
        // into the state on each refresh.
        let context = refresh_session();
        let (left_state_source, right_state_source, left_columns, right_columns) =
            semi_anti_state_sources(&context, view)?;
        let left_state = if side_keyed {
            self.ensure_join_side_state_for_view(
                &view.view_id,
                &left_state_source,
                &view.join_keys,
                StateRole::JoinLeft,
                view.null_safe,
            )
            .await?
        } else {
            None
        };
        let right_state = if side_keyed {
            self.ensure_join_side_state_for_view(
                &view.view_id,
                &right_state_source,
                right_join_keys(view),
                StateRole::JoinRight,
                view.null_safe,
            )
            .await?
        } else {
            None
        };
        if let Some(state) = &left_state {
            commit_ids.extend(if state.current_files(&self.client).await?.is_empty() {
                self.sync_join_side_state(&left_state_source, state, &view.join_keys, &[])
                    .await?
            } else {
                self.apply_join_side_delta(
                    &left_state_source,
                    state,
                    &left_window,
                    &view.join_keys,
                    &[],
                )
                .await?
            });
        }
        if let Some(state) = &right_state {
            commit_ids.extend(if state.current_files(&self.client).await?.is_empty() {
                self.sync_join_side_state(
                    &right_state_source,
                    state,
                    right_join_keys(view),
                    &[],
                )
                .await?
            } else {
                self.apply_join_side_delta(
                    &right_state_source,
                    state,
                    &right_window,
                    right_join_keys(view),
                    &[],
                )
                .await?
            });
        }
        // A plain equality SEMI/ANTI needs only whether a right key exists.
        // When the right source is already keyed by that key, every view over
        // it can share one per-window existence set instead of probing the
        // same keys again.
        let shared_existence = side_keyed
            && right_state.is_none()
            && semi_anti_shared_existence_applies(view);
        self.checkpoint_epoch_states(&record).await?;

        let left_projection = project_schema(&left_state_source.schema, &left_columns)?;
        let right_projection =
            project_schema(&right_state_source.schema, &right_columns)?;

        let mv_partitions =
            super::mv_partitions_for_source(&view.mv, &view.left, &left_window);
        let left_changed = !left_window.added_files.is_empty();
        let right_changed = !right_window.added_files.is_empty();
        let raw_delta_left = self.groups_table_frame(
            &context,
            &view.left,
            left_window.added_files.clone(),
        )?;
        let raw_delta_right = self.groups_table_frame(
            &context,
            &view.right,
            right_window.added_files.clone(),
        )?;
        let left_state_fields = left_state_source
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let raw_delta_left_state =
            project_frame(raw_delta_left.clone(), &left_state_fields)?;
        let delta_left = project_frame(raw_delta_left.clone(), &left_columns)?;
        let delta_right = project_frame(raw_delta_right.clone(), &right_columns)?;
        let mv_partitions = if right_changed { None } else { mv_partitions };
        let probe = semi_anti_probe(view, &left_state_source, &right_state_source);

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
        let left_key_schema = project_schema(&view.left.schema, &view.left.primary_keys)?;

        // With keyed sources on both sides, every row the window needs is read
        // through a keyed lookup: the left rows of the delta and of the changed
        // right join keys, then the right rows matching those left rows.  No
        // side is read in full.
        let (left_now, right_now, affected) = if side_keyed {
            // The current left rows behind each changed join key.  In count
            // mode a left change also shifts the ranks of the whole tuple, so
            // the lookup covers the tuple instead of just the changed row.
            let left_rows_by_key = if left_changed && view.count_mode {
                let changed_tuples = self
                    .join_keys_including_before(
                        &context,
                        &view.left,
                        &left_window,
                        &raw_delta_left_state,
                        &view.join_keys,
                        &[],
                        &view.join_keys,
                    )
                    .await?;
                Some(
                    self.lookup_left_rows_for_keys(
                        &context,
                        &probe,
                        left_state.as_ref(),
                        changed_tuples,
                        false,
                    )
                    .await?,
                )
            } else {
                None
            };
            // The current left rows matching the previous and the current
            // join keys of the changed right rows.
            let right_rows = if right_changed {
                let changed_keys = self
                    .join_keys_including_before(
                        &context,
                        &view.right,
                        &right_window,
                        &raw_delta_right,
                        right_join_keys(view),
                        &[],
                        &view.join_keys,
                    )
                    .await?;
                Some(
                    self.lookup_left_rows_for_keys(
                        &context,
                        &probe,
                        left_state.as_ref(),
                        changed_keys,
                        false,
                    )
                    .await?,
                )
            } else {
                None
            };
            let affected_from_left = if view.count_mode {
                // The deleted keys are absent from the current state, so the
                // delta keys are always part of the affected set.
                let changed = raw_delta_left_state
                    .clone()
                    .select(left_key_exprs.clone())?
                    .distinct()?;
                match &left_rows_by_key {
                    Some(rows) => changed
                        .union(rows.clone().select(left_key_exprs.clone())?.distinct()?)?
                        .distinct()?,
                    None => changed,
                }
            } else {
                match &left_rows_by_key {
                    Some(rows) => {
                        rows.clone().select(left_key_exprs.clone())?.distinct()?
                    }
                    None => raw_delta_left_state
                        .clone()
                        .select(left_key_exprs.clone())?
                        .distinct()?,
                }
            };
            let affected_from_right = match &right_rows {
                Some(rows) => rows.clone().select(left_key_exprs.clone())?.distinct()?,
                None => dataframe(&context, Vec::new(), &left_key_schema)?,
            };
            let affected = match (left_changed, right_changed) {
                (true, true) => {
                    affected_from_left.union(affected_from_right)?.distinct()?
                }
                (true, false) => affected_from_left,
                (false, true) => affected_from_right,
                (false, false) => unreachable!("an empty window returns above"),
            };
            // Reuse the rows already read: the left delta carries the current
            // row of every changed left key, and the right lookups carry the
            // current left rows of the changed right keys, so no extra PK
            // lookup over the left source is needed.
            let left_now = if view.count_mode {
                let rows = match (left_rows_by_key, right_rows) {
                    (Some(left), Some(right)) => left.union(right)?,
                    (Some(rows), None) | (None, Some(rows)) => rows,
                    (None, None) => dataframe(&context, Vec::new(), &left_key_schema)?,
                };
                rows.distinct()?
            } else {
                let rows = match right_rows {
                    Some(right) => raw_delta_left_state.clone().union(right)?,
                    None => raw_delta_left_state.clone(),
                };
                rows.distinct()?
            };
            let right_now = if shared_existence {
                dataframe(&context, Vec::new(), &right_projection)?
            } else {
                self.lookup_right_rows_for_left_keys(
                    &context,
                    &probe,
                    right_state.as_ref(),
                    right_join_keys(view),
                    &left_now,
                )
                .await?
            };
            (
                project_frame(left_now, &left_columns)?,
                project_frame(right_now, &right_columns)?,
                affected,
            )
        } else {
            let left_before = project_frame(
                self.before_window_table_frame(
                    &context,
                    &view.left,
                    &left_window.before_versions,
                )
                .await?,
                &left_columns,
            )?;
            let right_before = if right_changed {
                project_frame(
                    self.before_window_table_frame(
                        &context,
                        &view.right,
                        &right_window.before_versions,
                    )
                    .await?,
                    &right_columns,
                )?
            } else {
                dataframe(&context, Vec::new(), &right_projection)?
            };
            let left_now = project_frame(
                self.current_table_frame(&context, &view.left)?,
                &left_columns,
            )?;
            let right_now = project_frame(
                self.current_table_frame(&context, &view.right)?,
                &right_columns,
            )?;

            // Left rows in the delta are always affected.  In count mode a
            // left insertion or deletion also shifts the ranks of the remaining
            // rows of its tuple, so the whole tuple follows.
            let affected_from_left = if view.count_mode {
                let join_key_exprs = view
                    .join_keys
                    .iter()
                    .map(|key| col(key.as_str()))
                    .collect::<Vec<_>>();
                let changed_tuples = delta_left
                    .clone()
                    .select(join_key_exprs.clone())?
                    .union(
                        left_before
                            .clone()
                            .join(
                                delta_left.clone().select(left_key_exprs.clone())?,
                                JoinType::LeftSemi,
                                &left_key_names,
                                &left_key_names,
                                None,
                            )?
                            .select(join_key_exprs)?,
                    )?
                    .distinct()?;
                let shifted = null_safe_semi_join(
                    left_now.clone(),
                    changed_tuples,
                    &view.join_keys,
                )?
                .select(left_key_exprs.clone())?
                .distinct()?;
                delta_left
                    .clone()
                    .select(left_key_exprs.clone())?
                    .distinct()?
                    .union(shifted)?
                    .distinct()?
            } else {
                delta_left
                    .clone()
                    .select(left_key_exprs.clone())?
                    .distinct()?
            };
            // A right change affects the left rows matching the previous or
            // the current version of the changed right rows: the predicate can
            // flip on an update and a changed equality key can drop an old
            // match.
            let affected = if !right_changed {
                affected_from_left
            } else if view.right_aggregate.is_some() {
                // An aggregate input change re-evaluates the left rows of the
                // changed join keys (their old keys included for a keyed right
                // source).
                let right_key_exprs = right_join_keys(view)
                    .iter()
                    .map(|key| col(key.as_str()))
                    .collect::<Vec<_>>();
                let changed_keys = if view.right.primary_keys.is_empty() {
                    delta_right.clone().select(right_key_exprs.clone())?
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
                    changed_before
                        .union(delta_right)?
                        .distinct()?
                        .select(right_key_exprs)?
                }
                .distinct()?;
                let left_join_keys = view
                    .join_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let right_join_key_names = right_join_keys(view)
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let affected_from_right = left_before
                    .join(
                        changed_keys,
                        JoinType::LeftSemi,
                        &left_join_keys,
                        &right_join_key_names,
                        None,
                    )?
                    .select(left_key_exprs.clone())?
                    .distinct()?;
                affected_from_left.union(affected_from_right)?.distinct()?
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
            (left_now, right_now, affected)
        };
        let left_now = apply_side_filter(
            &context,
            filter_deletes(left_now, change_column(&view.left))?,
            &left_projection,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(right_now, change_column(&view.right))?,
            &right_projection,
            view.right_filter.as_deref(),
        )?;
        let shared_match = if shared_existence {
            Some(
                self.shared_semi_anti_match(
                    &context,
                    view,
                    &left_now,
                    &right_state_source,
                    &right_window,
                )
                .await?,
            )
        } else {
            None
        };

        // The MV only supplies the old rows for the affected left keys.
        // Read those keys through the shared PK lookup instead of scanning
        // the whole MV when its key matches the left key.
        let mv = if view.mv.primary_keys == view.left.primary_keys {
            self.lookup_source_by_join_keys(
                &context,
                &view.mv,
                affected.clone(),
                &view.left.primary_keys,
                &view.mv.primary_keys,
                true,
            )
            .await?
        } else {
            match &mv_partitions {
                Some(restriction) => {
                    self.current_table_frame_filtered_partitions(
                        &context,
                        &view.mv,
                        Vec::new(),
                        restriction,
                    )
                    .await?
                }
                None => self.current_table_frame(&context, &view.mv)?,
            }
        };

        let join_type = if view.anti {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        // Rows that match right now; the ANTI insert takes the difference.
        // The count mode already returns the qualifying rows, so its insert
        // joins semi against them.
        let (matched_now, insert_join_type) = if let Some(rows) = shared_match {
            (rows.select(left_key_exprs.clone())?.distinct()?, join_type)
        } else if view.count_mode {
            (
                semi_count_qualifying(&context, left_now.clone(), right_now, view)
                    .await?,
                JoinType::LeftSemi,
            )
        } else if view.right_aggregate.is_some() {
            (
                semi_aggregate_match(&context, left_now.clone(), right_now, view)?
                    .select(left_key_exprs.clone())?
                    .distinct()?,
                join_type,
            )
        } else {
            (
                semi_anti_join(left_now.clone(), right_now, view, JoinType::LeftSemi)?
                    .select(left_key_exprs.clone())?
                    .distinct()?,
                join_type,
            )
        };

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
                insert_join_type,
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
        commit_ids.extend(
            append_dataframe(
                &view.mv,
                &self.client,
                inserts.union(deletes)?.sort_by(sort_exprs)?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// The left rows of `left` whose right join key exists, using the
    /// per-window key set shared by every plain SEMI/ANTI view over the same
    /// right source.  Only keys that no view has probed yet reach the right
    /// table; a key already answered by another view is served from the set.
    async fn shared_semi_anti_match(
        &self,
        context: &SessionContext,
        view: &SemiAntiView,
        left: &DataFrame,
        right: &IvmTable,
        right_window: &SourceWindow,
    ) -> Result<DataFrame> {
        let join_key = super::join_state::IVM_JOIN_KEY_COLUMN;
        let right_keys = right_join_keys(view);
        let aliases = view
            .join_keys
            .iter()
            .zip(right_keys)
            .map(|(left, right)| col(left.as_str()).alias(right.as_str()))
            .collect::<Vec<_>>();
        // Encode the left rows themselves so the matched frame still carries
        // every column the view materializes.  Eligibility checked that the
        // left and right key types agree, so the bytes are comparable with the
        // right keys encoded below.
        let encoded =
            encode_join_key_frame(context, left.clone(), &view.left, &view.join_keys)?;
        // The left rows of a right-only window come from a keyed lookup;
        // materialize them once so the existence probe and the final match do
        // not repeat that lookup.
        let schema: SchemaRef = Arc::new(encoded.schema().as_arrow().clone());
        let batches = encoded.collect().await?;
        let requested = collect_batch_keys(&batches)?;
        let encoded = if batches.is_empty() {
            dataframe(
                context,
                vec![RecordBatch::new_empty(schema.clone())],
                &schema,
            )?
        } else {
            context.read_batches(batches)?
        };
        let table = format!("{}|{}", right.table_id, right_keys.join(","));
        let window = window_key(&right_window.identity);
        let (mut present, unknown) = {
            let mut cache = self
                .semi_anti_right_key_cache()
                .lock()
                .map_err(|_| report!("semi/anti right key cache is poisoned"))?;
            cache.begin_window(&window);
            cache.lookup(&table, &requested)
        };
        let cached = requested.intersection(&present).count();
        if !unknown.is_empty() {
            let missing = encoded.clone().join(
                encoded_key_frame(context, &unknown)?,
                JoinType::Inner,
                &[join_key],
                &[SHARED_RIGHT_KEY_COLUMN],
                None,
            )?;
            let missing = missing.select(aliases)?;
            // Only the right keys are read: the existence answer never needs
            // the payload of the right table.
            let rows = self
                .lookup_source_by_join_keys(
                    context, right, missing, right_keys, right_keys, true,
                )
                .await?;
            let rows = rows.select(
                right_keys
                    .iter()
                    .map(|key| col(key.as_str()))
                    .collect::<Vec<_>>(),
            )?;
            let encoded_rows = encode_join_key_frame(context, rows, right, right_keys)?;
            let found = collect_encoded_keys(encoded_rows).await?;
            {
                let mut cache = self
                    .semi_anti_right_key_cache()
                    .lock()
                    .map_err(|_| report!("semi/anti right key cache is poisoned"))?;
                cache.record(&table, &unknown, &found);
            }
            present.extend(found);
        }
        // Every requested key is now either known present or known absent.
        present.retain(|key| requested.contains(key));
        tracing::debug!(
            table = %right.table_name,
            requested = requested.len(),
            cached,
            probed = unknown.len(),
            present = present.len(),
            "semi/anti shared right key set"
        );
        let matched = encoded.join(
            encoded_key_frame(context, &present)?,
            JoinType::LeftSemi,
            &[join_key],
            &[SHARED_RIGHT_KEY_COLUMN],
            None,
        )?;
        Ok(matched)
    }

    /// Rebuild a `SEMI`/`ANTI` join from the full state of both sources.
    pub async fn rebuild_semi_anti(&self, view: &SemiAntiView) -> Result<i64> {
        self.register_semi_anti_view(view).await?;
        validate_semi_anti_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let left_baseline = self
            .source_baseline_for_filter(&view.left, view.left_filter.as_deref())
            .await?;
        let right_baseline = self
            .source_baseline_for_filter(&view.right, view.right_filter.as_deref())
            .await?;
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
        let mut commit_ids = Vec::new();
        let context = refresh_session();
        let (left_state_source, right_state_source, _, _) =
            semi_anti_state_sources(&context, view)?;
        if !view.join_keys.is_empty()
            && !view.left.primary_keys.is_empty()
            && !view.right.primary_keys.is_empty()
        {
            let left_state = self
                .ensure_join_side_state_for_view(
                    &view.view_id,
                    &left_state_source,
                    &view.join_keys,
                    StateRole::JoinLeft,
                    view.null_safe,
                )
                .await?;
            if let Some(state) = left_state {
                commit_ids.extend(
                    self.sync_join_side_state(
                        &left_state_source,
                        &state,
                        &view.join_keys,
                        &[],
                    )
                    .await?,
                );
            }
            let right_state = self
                .ensure_join_side_state_for_view(
                    &view.view_id,
                    &right_state_source,
                    right_join_keys(view),
                    StateRole::JoinRight,
                    view.null_safe,
                )
                .await?;
            if let Some(state) = right_state {
                commit_ids.extend(
                    self.sync_join_side_state(
                        &right_state_source,
                        &state,
                        right_join_keys(view),
                        &[],
                    )
                    .await?,
                );
            }
            self.checkpoint_epoch_states(&record).await?;
        }

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
        let join_type = if view.anti {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        let output_columns = semi_anti_output_columns(view)
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();
        let rows = if view.count_mode {
            let qualifying =
                semi_count_qualifying(&context, left.clone(), right, view).await?;
            let key_names = view
                .left
                .primary_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            left.join(qualifying, JoinType::LeftSemi, &key_names, &key_names, None)?
        } else if view.right_aggregate.is_some() {
            semi_aggregate_match(&context, left, right, view)?
        } else {
            semi_anti_join(left, right, view, join_type)?
        }
        .select(output_columns)?
        .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        commit_ids.extend(
            append_dataframe(&view.mv, &self.client, rows)
                .await?
                .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
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

/// A left join against one aggregate row per join key (a select-list
/// correlated scalar subquery).  The MV keeps one row per left key with the
/// aggregate value appended, NULL when the key has no right rows.
#[derive(Debug, Clone)]
pub struct LeftAggregateView {
    /// The view id.
    pub view_id: String,
    /// The left source; it must have a primary key.
    pub left: IvmTable,
    /// The right source the aggregate reads (append-only or keyed).
    pub right: IvmTable,
    /// The materialized view table, created from
    /// [`left_aggregate_mv_schema_for`].
    pub mv: IvmTable,
    /// The left join keys.
    pub join_keys: Vec<String>,
    /// The right group keys, aligned with [`Self::join_keys`]; empty means the
    /// same names as `join_keys`.
    pub right_keys: Vec<String>,
    /// The rendered aggregate call over the right columns.
    pub right_aggregate: String,
    /// The MV column the aggregate value is materialized into.
    pub aggregate_column: String,
    /// Whether the aggregate is a COUNT (see the spec field of the same name).
    pub count_zero: bool,
    /// The left columns materialized; the left primary keys are always
    /// contained.
    pub output_columns: Vec<String>,
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the contributing right rows must satisfy.
    pub right_filter: Option<String>,
    /// The computed select-list expressions over the scalar subquery value:
    /// the rendered expression over [`IVM_RIGHT_AGG_COLUMN`], the MV column
    /// and the result type.
    pub output_exprs: Vec<(String, String, DataType)>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl LeftAggregateView {
    /// A view with no filters and the given output columns.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        view_id: impl Into<String>,
        left: IvmTable,
        right: IvmTable,
        mv: IvmTable,
        join_keys: Vec<String>,
        right_keys: Vec<String>,
        right_aggregate: impl Into<String>,
        aggregate_column: impl Into<String>,
        output_columns: Vec<String>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            left,
            right,
            mv,
            join_keys,
            right_keys,
            right_aggregate: right_aggregate.into(),
            aggregate_column: aggregate_column.into(),
            count_zero: false,
            output_columns,
            left_filter: None,
            right_filter: None,
            output_exprs: Vec::new(),
            refresh_interval_ms: 0,
        }
    }

    /// Only left rows matching `filter` contribute to the view.
    pub fn with_left_filter(mut self, filter: impl Into<String>) -> Self {
        self.left_filter = Some(filter.into());
        self
    }

    /// Only right rows matching `filter` contribute to the aggregate.
    pub fn with_right_filter(mut self, filter: impl Into<String>) -> Self {
        self.right_filter = Some(filter.into());
        self
    }

    /// Append computed select-list expressions over the scalar subquery value.
    pub fn with_output_exprs(
        mut self,
        output_exprs: Vec<(String, String, DataType)>,
    ) -> Self {
        self.output_exprs = output_exprs;
        self
    }

    fn to_spec(&self) -> Result<ViewSpec> {
        Ok(ViewSpec::LeftAggregate {
            view_id: self.view_id.clone(),
            left_table_id: self.left.table_id.clone(),
            right_table_id: self.right.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            join_keys: self.join_keys.clone(),
            right_keys: self.right_keys.clone(),
            right_aggregate: self.right_aggregate.clone(),
            aggregate_column: self.aggregate_column.clone(),
            count_zero: self.count_zero,
            output_columns: self.output_columns.clone(),
            left_filter: self.left_filter.clone(),
            right_filter: self.right_filter.clone(),
            output_exprs: self
                .output_exprs
                .iter()
                .map(|(call, column, data_type)| {
                    Ok(LeftAggregateOutput {
                        call: call.clone(),
                        column: column.clone(),
                        result: encode_data_type(data_type)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

/// The probe view used by the shared keyed join helpers for a left aggregate.
fn left_aggregate_probe(view: &LeftAggregateView) -> JoinView {
    JoinView {
        view_id: view.view_id.clone(),
        left: view.left.clone(),
        right: view.right.clone(),
        output: view.mv.clone(),
        join_keys: view.join_keys.clone(),
        right_keys: view.right_keys.clone(),
        key_exprs: JoinKeyExprs::default(),
        left_value: String::new(),
        right_value: String::new(),
        left_filter: view.left_filter.clone(),
        right_filter: view.right_filter.clone(),
        pair_filter: None,
        null_safe: false,
        output_columns: Vec::new(),
        state: None,
        refresh_interval_ms: view.refresh_interval_ms,
    }
}

/// The join of the affected left rows with one aggregate row per key: a
/// COUNT subquery counts zero for a left row whose key has no right rows,
/// while every other aggregate leaves the unmatched value NULL.
fn left_aggregate_joined(
    view: &LeftAggregateView,
    joined: DataFrame,
) -> Result<DataFrame> {
    if !view.count_zero {
        return Ok(joined);
    }
    let aggregate_type = field_type(&view.mv.schema, &view.aggregate_column)?;
    Ok(joined.with_column(
        IVM_RIGHT_AGG_COLUMN,
        datafusion::functions::core::expr_fn::coalesce(vec![
            col(IVM_RIGHT_AGG_COLUMN),
            zero_literal(&aggregate_type),
        ]),
    )?)
}

/// A typed zero, the default a COUNT subquery materializes for a key with no
/// right rows.
fn zero_literal(data_type: &DataType) -> Expr {
    match data_type {
        DataType::UInt64 => lit(0u64),
        DataType::UInt32 => lit(0u32),
        DataType::Int32 => lit(0i32),
        DataType::Int16 => lit(0i16),
        DataType::Int8 => lit(0i8),
        _ => lit(0i64),
    }
}

/// The projection expressions of one left-aggregate row: the materialized
/// columns for an insert and the plain column list for a delete.  A computed
/// select-list expression is rendered against the raw aggregate column of the
/// joined frame.
fn left_aggregate_row_exprs(
    context: &SessionContext,
    view: &LeftAggregateView,
    joined: &DataFrame,
) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut insert_exprs = view
        .output_columns
        .iter()
        .map(|column| col(column.as_str()))
        .collect::<Vec<_>>();
    insert_exprs.push(col(IVM_RIGHT_AGG_COLUMN).alias(view.aggregate_column.as_str()));
    for (call, column, _) in &view.output_exprs {
        insert_exprs.push(
            context
                .state()
                .create_logical_expr(call, joined.schema())
                .map_err(|error| {
                    report!("invalid scalar subquery output {call:?}: {error}")
                })?
                .alias(column.as_str()),
        );
    }
    let mut output_exprs = view
        .output_columns
        .iter()
        .map(|column| col(column.as_str()))
        .collect::<Vec<_>>();
    output_exprs.push(col(view.aggregate_column.as_str()));
    for (_, column, _) in &view.output_exprs {
        output_exprs.push(col(column.as_str()));
    }
    Ok((insert_exprs, output_exprs))
}

/// The right group keys of a left-aggregate view, falling back to the shared
/// join keys.
fn left_aggregate_right_keys(view: &LeftAggregateView) -> &[String] {
    if view.right_keys.is_empty() {
        &view.join_keys
    } else {
        &view.right_keys
    }
}

/// The schema of a [`LeftAggregateView`] materialized view: the left columns,
/// the aggregate value, the row kind and the epoch.
pub fn left_aggregate_mv_schema_for(
    left_schema: &Schema,
    output_columns: &[String],
    aggregate_column: &str,
    aggregate_type: &DataType,
) -> Result<SchemaRef> {
    left_aggregate_mv_schema_with_outputs(
        left_schema,
        output_columns,
        aggregate_column,
        aggregate_type,
        &[],
    )
}

/// The schema of a [`LeftAggregateView`] with computed outputs: the left
/// columns, the aggregate value, the computed columns, the row kind and the
/// epoch.
pub fn left_aggregate_mv_schema_with_outputs(
    left_schema: &Schema,
    output_columns: &[String],
    aggregate_column: &str,
    aggregate_type: &DataType,
    outputs: &[(String, DataType)],
) -> Result<SchemaRef> {
    if output_columns.is_empty() {
        return Err(report!("a left aggregate view needs its output columns"));
    }
    let mut fields = project_schema(left_schema, output_columns)?
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        aggregate_column,
        aggregate_type.clone(),
        true,
    )));
    for (column, data_type) in outputs {
        fields.push(Arc::new(Field::new(
            column.clone(),
            data_type.clone(),
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

/// Validate that a left-aggregate view can be maintained.
fn validate_left_aggregate_view(view: &LeftAggregateView) -> Result<()> {
    if view.left.primary_keys.is_empty() {
        return Err(report!(
            "left aggregate view {} needs a left source with a primary key",
            view.view_id
        ));
    }
    for key in &view.left.primary_keys {
        let field = view.left.schema.field_with_name(key).map_err(|_| {
            report!(
                "left aggregate view {}: key column {key} is not in the left source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "left aggregate view {}: key column {key} must be non-nullable",
                view.view_id
            ));
        }
        if !view.output_columns.contains(key) {
            return Err(report!(
                "left aggregate view {}: output columns must contain the left key {key}",
                view.view_id
            ));
        }
    }
    for (call, column, _) in &view.output_exprs {
        if call.is_empty() || column.is_empty() {
            return Err(report!(
                "left aggregate view {} has an empty computed output",
                view.view_id
            ));
        }
        if view.output_columns.contains(column) || column == &view.aggregate_column {
            return Err(report!(
                "left aggregate view {}: column {column} is materialized twice",
                view.view_id
            ));
        }
        if view
            .output_exprs
            .iter()
            .filter(|(_, other, _)| other == column)
            .count()
            > 1
        {
            return Err(report!(
                "left aggregate view {}: column {column} is materialized twice",
                view.view_id
            ));
        }
    }
    if view.output_columns.contains(&view.aggregate_column) {
        return Err(report!(
            "left aggregate view {}: the aggregate column {} collides with an output column",
            view.view_id,
            view.aggregate_column
        ));
    }
    let right_keys = left_aggregate_right_keys(view);
    if view.join_keys.len() != right_keys.len() {
        return Err(report!(
            "left aggregate view {}: the join needs one right key per left key",
            view.view_id
        ));
    }
    for (left_key, right_key) in view.join_keys.iter().zip(right_keys) {
        view.left.schema.field_with_name(left_key).map_err(|_| {
            report!(
                "left aggregate view {}: join key {left_key} is not in the left source",
                view.view_id
            )
        })?;
        view.right.schema.field_with_name(right_key).map_err(|_| {
            report!(
                "left aggregate view {}: join key {right_key} is not in the right source",
                view.view_id
            )
        })?;
    }
    let context = refresh_session();
    let df_schema = DFSchema::try_from(view.right.schema.as_ref().clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    context
        .state()
        .create_logical_expr(&view.right_aggregate, &df_schema)
        .map_err(|error| {
            report!(
                "left aggregate view {}: invalid aggregate {:?}: {error}",
                view.view_id,
                view.right_aggregate
            )
        })?;
    if let Some(filter) = &view.left_filter {
        parse_filter(&context, &view.left.schema, filter)?;
    }
    if let Some(filter) = &view.right_filter {
        parse_filter(&context, &view.right.schema, filter)?;
    }
    Ok(())
}

/// The left columns a left-aggregate refresh must read.
fn left_aggregate_left_columns(view: &LeftAggregateView) -> Result<Vec<String>> {
    let mut columns = view.output_columns.clone();
    for key in &view.left.primary_keys {
        if !columns.contains(key) {
            columns.push(key.clone());
        }
    }
    let context = refresh_session();
    if let Some(filter) = &view.left_filter {
        for column in filter_columns(&context, &view.left.schema, filter)? {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
    }
    // A computed output may read the row's own columns as well as the raw
    // aggregate value (its type is the MV's aggregate column).
    if !view.output_exprs.is_empty() {
        let aggregate_type = field_type(&view.mv.schema, &view.aggregate_column)?;
        let schema = match_schema(&view.left.schema, &aggregate_type)?;
        for (call, _, _) in &view.output_exprs {
            for column in filter_columns(&context, &schema, call)? {
                if column != IVM_RIGHT_AGG_COLUMN && !columns.contains(&column) {
                    columns.push(column);
                }
            }
        }
    }
    if let Some(change) = change_column(&view.left)
        && !columns.iter().any(|column| column == change)
    {
        columns.push(change.to_string());
    }
    Ok(columns)
}

/// The right columns a left-aggregate refresh must read: the group keys, the
/// aggregate arguments, the filter columns and the change column.
fn left_aggregate_right_columns(
    view: &LeftAggregateView,
    context: &SessionContext,
) -> Result<Vec<String>> {
    let mut columns = Vec::new();
    for column in left_aggregate_right_keys(view) {
        if !columns.contains(column) {
            columns.push(column.clone());
        }
    }
    // A keyed right source contributes its primary keys: the refresh reads
    // the changed rows' identities from the projected delta.
    for key in &view.right.primary_keys {
        if !columns.contains(key) {
            columns.push(key.clone());
        }
    }
    for column in filter_columns(context, &view.right.schema, &view.right_aggregate)? {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    if let Some(filter) = &view.right_filter {
        for column in filter_columns(context, &view.right.schema, filter)? {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
    }
    if let Some(change) = change_column(&view.right)
        && !columns.iter().any(|column| column == change)
    {
        columns.push(change.to_string());
    }
    Ok(columns)
}

/// One aggregate row per join key, the aggregate aliased to the MV column.
fn left_aggregate_frame(
    context: &SessionContext,
    right: DataFrame,
    view: &LeftAggregateView,
) -> Result<DataFrame> {
    let df_schema = right.schema().clone();
    let aggregate = context
        .state()
        .create_logical_expr(&view.right_aggregate, &df_schema)
        .map_err(|error| {
            report!(
                "left aggregate view {}: invalid aggregate {:?}: {error}",
                view.view_id,
                view.right_aggregate
            )
        })?;
    let group_exprs = left_aggregate_right_keys(view)
        .iter()
        .map(|key| col(key.as_str()).alias(semi_anti_right_alias(key)))
        .collect::<Vec<_>>();
    Ok(right.aggregate(group_exprs, vec![aggregate.alias(IVM_RIGHT_AGG_COLUMN)])?)
}

/// The join keys of the aggregate frame (the aliased group keys).
fn left_aggregate_join_right_keys(view: &LeftAggregateView) -> Vec<String> {
    left_aggregate_right_keys(view)
        .iter()
        .map(|key| semi_anti_right_alias(key))
        .collect()
}

impl IvmRuntime {
    /// Persist a left-aggregate view spec (idempotent).
    pub async fn register_left_aggregate_view(
        &self,
        view: &LeftAggregateView,
    ) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec()?)?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a left-aggregate view: the affected left rows are re-evaluated
    /// against the per-key aggregate.
    pub async fn refresh_left_aggregate(
        &self,
        view: &LeftAggregateView,
    ) -> Result<Option<i64>> {
        self.register_left_aggregate_view(view).await?;
        validate_left_aggregate_view(view)?;

        let left_window = self
            .collect_source_window_for_filter(
                &view.view_id,
                &view.left,
                view.left_filter.as_deref(),
            )
            .await?;
        let right_window = self
            .collect_source_window_for_filter(
                &view.view_id,
                &view.right,
                view.right_filter.as_deref(),
            )
            .await?;
        let right_changed = !right_window.added_files.is_empty();
        if left_window.added_files.is_empty() && !right_changed {
            return Ok(None);
        }
        let mut identity = left_window.identity.clone();
        identity.extend(right_window.identity.iter().cloned());
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
        let mut commit_ids = Vec::new();
        let side_keyed = !view.join_keys.is_empty()
            && !view.left.primary_keys.is_empty()
            && !view.right.primary_keys.is_empty();
        let left_state = if side_keyed {
            self.ensure_join_side_state_for_view(
                &view.view_id,
                &view.left,
                &view.join_keys,
                StateRole::JoinLeft,
                false,
            )
            .await?
        } else {
            None
        };
        let right_state = if side_keyed {
            self.ensure_join_side_state_for_view(
                &view.view_id,
                &view.right,
                left_aggregate_right_keys(view),
                StateRole::JoinRight,
                false,
            )
            .await?
        } else {
            None
        };
        if let Some(state) = &left_state {
            commit_ids.extend(if state.current_files(&self.client).await?.is_empty() {
                self.sync_join_side_state(&view.left, state, &view.join_keys, &[])
                    .await?
            } else {
                self.apply_join_side_delta(
                    &view.left,
                    state,
                    &left_window,
                    &view.join_keys,
                    &[],
                )
                .await?
            });
        }
        if let Some(state) = &right_state {
            commit_ids.extend(if state.current_files(&self.client).await?.is_empty() {
                self.sync_join_side_state(
                    &view.right,
                    state,
                    left_aggregate_right_keys(view),
                    &[],
                )
                .await?
            } else {
                self.apply_join_side_delta(
                    &view.right,
                    state,
                    &right_window,
                    left_aggregate_right_keys(view),
                    &[],
                )
                .await?
            });
        }
        self.checkpoint_epoch_states(&record).await?;

        let context = refresh_session();
        let left_columns = left_aggregate_left_columns(view)?;
        let right_columns = left_aggregate_right_columns(view, &context)?;
        let left_projection = project_schema(&view.left.schema, &left_columns)?;
        let right_projection = project_schema(&view.right.schema, &right_columns)?;
        let left_changed = !left_window.added_files.is_empty();
        let fast_simple = side_keyed && left_changed != right_changed;
        let raw_delta_left = self.groups_table_frame(
            &context,
            &view.left,
            left_window.added_files.clone(),
        )?;
        let raw_delta_right = self.groups_table_frame(
            &context,
            &view.right,
            right_window.added_files.clone(),
        )?;
        let delta_left = project_frame(raw_delta_left.clone(), &left_columns)?;
        let delta_right = project_frame(raw_delta_right.clone(), &right_columns)?;
        let probe = left_aggregate_probe(view);
        let fast_sides = if fast_simple {
            self.keyed_one_sided_join_sides(
                &context,
                &probe,
                left_state.as_ref(),
                right_state.as_ref(),
                if left_changed {
                    Some(&raw_delta_left)
                } else {
                    None
                },
                if right_changed {
                    Some(&raw_delta_right)
                } else {
                    None
                },
                Some(&right_window),
            )
            .await?
        } else {
            None
        };
        let left_before = if fast_sides.is_some() {
            dataframe(&context, Vec::new(), &left_projection)?
        } else {
            filter_deletes(
                project_frame(
                    self.before_window_table_frame(
                        &context,
                        &view.left,
                        &left_window.before_versions,
                    )
                    .await?,
                    &left_columns,
                )?,
                change_column(&view.left),
            )?
        };
        let right_before = if fast_sides.is_some() {
            dataframe(&context, Vec::new(), &right_projection)?
        } else {
            filter_deletes(
                project_frame(
                    self.before_window_table_frame(
                        &context,
                        &view.right,
                        &right_window.before_versions,
                    )
                    .await?,
                    &right_columns,
                )?,
                change_column(&view.right),
            )?
        };
        let fast_keyed = fast_sides.is_some();
        let (left_now, right_now) = match fast_sides {
            Some((left, right)) => (
                project_frame(left, &left_columns)?,
                project_frame(right, &right_columns)?,
            ),
            None => (
                project_frame(
                    self.current_table_frame(&context, &view.left)?,
                    &left_columns,
                )?,
                project_frame(
                    self.current_table_frame(&context, &view.right)?,
                    &right_columns,
                )?,
            ),
        };
        let left_now = apply_side_filter(
            &context,
            filter_deletes(left_now, change_column(&view.left))?,
            &left_projection,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(right_now, change_column(&view.right))?,
            &right_projection,
            view.right_filter.as_deref(),
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
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();
        let right_key_names = view
            .right
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let affected_from_left = delta_left.select(left_key_exprs.clone())?.distinct()?;
        let affected = if fast_keyed {
            if left_changed {
                affected_from_left
            } else {
                left_now
                    .clone()
                    .select(left_key_exprs.clone())?
                    .distinct()?
            }
        } else if !right_changed {
            affected_from_left
        } else {
            let right_key_exprs = left_aggregate_right_keys(view)
                .iter()
                .map(|key| col(key.as_str()))
                .collect::<Vec<_>>();
            let changed_keys = if view.right.primary_keys.is_empty() {
                delta_right.clone().select(right_key_exprs)?
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
                changed_before
                    .union(delta_right)?
                    .distinct()?
                    .select(right_key_exprs)?
            }
            .distinct()?;
            let left_join_keys = view
                .join_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let right_join_key_names = left_aggregate_right_keys(view)
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let affected_from_right = left_before
                .join(
                    changed_keys,
                    JoinType::LeftSemi,
                    &left_join_keys,
                    &right_join_key_names,
                    None,
                )?
                .select(left_key_exprs.clone())?
                .distinct()?;
            affected_from_left.union(affected_from_right)?.distinct()?
        };
        let mv = if view.mv.primary_keys == view.left.primary_keys {
            self.lookup_source_by_join_keys(
                &context,
                &view.mv,
                affected.clone(),
                &view.left.primary_keys,
                &view.mv.primary_keys,
                true,
            )
            .await?
        } else {
            self.current_table_frame(&context, &view.mv)?
        };

        let joined = left_now
            .join(
                affected.clone(),
                JoinType::LeftSemi,
                &left_key_names,
                &left_key_names,
                None,
            )?
            .join(
                left_aggregate_frame(&context, right_now, view)?,
                JoinType::Left,
                &view
                    .join_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                &left_aggregate_join_right_keys(view)
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                None,
            )?;
        let joined = left_aggregate_joined(view, joined)?;
        let (insert_exprs, output_exprs) =
            left_aggregate_row_exprs(&context, view, &joined)?;
        let mv_epoch_keys = mv
            .clone()
            .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
            .select(left_key_exprs.clone())?
            .distinct()?;
        let inserts = joined
            .select(insert_exprs)?
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
            .select(output_exprs)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let mut sort_exprs = view
            .left
            .primary_keys
            .iter()
            .map(|key| column_expr(key))
            .collect::<Vec<_>>();
        sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
        commit_ids.extend(
            append_dataframe(
                &view.mv,
                &self.client,
                inserts.union(deletes)?.sort_by(sort_exprs)?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
    }

    /// Rebuild a left-aggregate view from the current sources.
    pub async fn rebuild_left_aggregate(&self, view: &LeftAggregateView) -> Result<i64> {
        self.register_left_aggregate_view(view).await?;
        validate_left_aggregate_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let left_baseline = self
            .source_baseline_for_filter(&view.left, view.left_filter.as_deref())
            .await?;
        let right_baseline = self
            .source_baseline_for_filter(&view.right, view.right_filter.as_deref())
            .await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let mut to_versions = left_baseline.to_versions.clone();
        to_versions.extend(right_baseline.to_versions.iter().cloned());
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
        if !view.join_keys.is_empty()
            && !view.left.primary_keys.is_empty()
            && !view.right.primary_keys.is_empty()
        {
            commit_ids.extend(
                self.sync_full_join_side_states(
                    &view.view_id,
                    &view.left,
                    &view.right,
                    &view.join_keys,
                    left_aggregate_right_keys(view),
                    &[],
                    &[],
                    false,
                )
                .await?,
            );
            self.checkpoint_epoch_states(&record).await?;
        }

        let context = refresh_session();
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
        let joined = left.join(
            left_aggregate_frame(&context, right, view)?,
            JoinType::Left,
            &view
                .join_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            &left_aggregate_join_right_keys(view)
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            None,
        )?;
        let joined = left_aggregate_joined(view, joined)?;
        let (insert_exprs, _) = left_aggregate_row_exprs(&context, view, &joined)?;
        let rows = joined
            .select(insert_exprs)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        commit_ids.extend(
            append_dataframe(&view.mv, &self.client, rows)
                .await?
                .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
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
