// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The semi/anti views: `EXISTS`/`NOT EXISTS`/`IN` and the `INTERSECT`/`EXCEPT`
//! subset.

use super::*;

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
    /// An optional filter the contributing left rows must satisfy.
    pub left_filter: Option<String>,
    /// An optional filter the contributing right rows must satisfy.
    pub right_filter: Option<String>,
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
        let mut commit_ids = Vec::new();

        // Project both sides to the columns the view actually needs (the
        // side filters included).
        let context = SessionContext::new();
        let mut left_columns = semi_anti_left_columns(view);
        if let Some(filter) = view.left_filter.as_deref() {
            for column in filter_columns(&context, &view.left.schema, filter)? {
                if !left_columns.contains(&column) {
                    left_columns.push(column);
                }
            }
        }
        let mut right_columns = semi_anti_right_columns(view);
        if let Some(filter) = view.right_filter.as_deref() {
            for column in filter_columns(&context, &view.right.schema, filter)? {
                if !right_columns.contains(&column) {
                    right_columns.push(column);
                }
            }
        }
        let left_projection = project_schema(&view.left.schema, &left_columns)?;
        let right_projection = project_schema(&view.right.schema, &right_columns)?;

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
        let left_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, left_now, &left_projection)?,
                change_column(&view.left),
            )?,
            &left_projection,
            view.left_filter.as_deref(),
        )?;
        let right_now = apply_side_filter(
            &context,
            filter_deletes(
                dataframe(&context, right_now, &right_projection)?,
                change_column(&view.right),
            )?,
            &right_projection,
            view.right_filter.as_deref(),
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
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, left_window.cursors)
            .await?;
        self.advance_cursors(&view.view_id, right_window.cursors)
            .await?;
        Ok(Some(epoch))
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
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
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
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
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
