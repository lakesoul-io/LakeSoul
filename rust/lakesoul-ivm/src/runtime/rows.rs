// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The row projection view.

use super::*;

/// One table an uncorrelated scalar subquery reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalarTableSpec {
    /// The name the rendered subquery refers to.
    pub name: String,
    /// The source table id.
    pub table_id: String,
}

/// The scalar subquery inputs of a row view's filter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowScalarSpec {
    /// Every table the subqueries read, deduplicated by table id.
    pub tables: Vec<ScalarTableSpec>,
}

/// The typed scalar subquery inputs of a [`RowView`]: the SQL name and table
/// of every table the subqueries read.
#[derive(Debug, Clone)]
pub struct RowScalarView {
    /// `(SQL name, table)` of every subquery table.
    pub tables: Vec<(String, IvmTable)>,
}

/// A projection (and optional filter) of one source.
///
/// A keyed source is maintained with `delete(old) + insert(current)` per
/// changed primary key; an append-only source simply appends the rows that
/// pass the filter.  A filter may contain uncorrelated scalar subqueries over
/// other tables, which the refresh watches alongside the source.
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
    /// The rendered projection expressions, parallel to
    /// [`Self::output_columns`]; empty means every column is projected
    /// unchanged.
    pub output_exprs: Vec<String>,
    /// An optional filter the source rows must satisfy.
    pub filter: Option<String>,
    /// The scalar subqueries of the filter, when it has any.  Their tables
    /// are registered (and watched) in addition to the source.
    pub scalar: Option<RowScalarView>,
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
            output_exprs: Vec::new(),
            filter: None,
            scalar: None,
            refresh_interval_ms: 0,
        }
    }

    /// Project only `output_columns` (keyed sources must include their keys).
    pub fn with_output_columns(mut self, output_columns: Vec<String>) -> Self {
        self.output_columns = output_columns;
        self
    }

    /// The rendered projection expressions, parallel to the output columns
    /// (`v * 2` for a `v * 2 AS v2` projection).
    pub fn with_output_exprs(mut self, output_exprs: Vec<String>) -> Self {
        self.output_exprs = output_exprs;
        self
    }

    /// Only rows matching `filter` are materialized.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// The scalar subquery inputs of the filter (a keyed source only).
    pub fn with_scalar(mut self, scalar: RowScalarView) -> Self {
        self.scalar = Some(scalar);
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Row {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            output_columns: self.output_columns.clone(),
            output_exprs: self.output_exprs.clone(),
            filter: self.filter.clone(),
            scalar: self.scalar.as_ref().map(|scalar| RowScalarSpec {
                tables: scalar
                    .tables
                    .iter()
                    .map(|(name, table)| ScalarTableSpec {
                        name: name.clone(),
                        table_id: table.table_id.clone(),
                    })
                    .collect(),
            }),
        }
    }
}

/// The schema of a [`RowView`] materialized view: the projected source
/// columns plus the row kind and the epoch.
pub fn row_mv_schema_for(
    source_schema: &Schema,
    output_columns: &[String],
) -> Result<SchemaRef> {
    row_expr_mv_schema_for(source_schema, output_columns, &[])
}

/// The schema of a [`RowView`] materialized view, with one rendered
/// projection expression per output column.  The expression types are derived
/// by planning each expression against the source schema.
pub fn row_expr_mv_schema_for(
    source_schema: &Schema,
    output_columns: &[String],
    output_exprs: &[String],
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
    let mut fields = if output_exprs.is_empty() {
        project_schema(source_schema, &columns)?
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    } else {
        if output_exprs.len() != columns.len() {
            return Err(report!(
                "the projection needs one expression per output column"
            ));
        }
        let mut fields = Vec::with_capacity(columns.len());
        for (column, expression) in columns.iter().zip(output_exprs) {
            let (data_type, nullable) = expression_type(source_schema, expression)?;
            fields.push(Arc::new(Field::new(column, data_type, nullable)));
        }
        fields
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

/// The source columns a [`RowView`] materializes.
/// The aliased projection expressions of a row view, parsed against the
/// source schema.
fn row_projection(view: &RowView, context: &SessionContext) -> Result<Vec<Expr>> {
    let columns = row_output_columns(view);
    if view.output_exprs.is_empty() {
        return Ok(columns.iter().map(|column| col(column.as_str())).collect());
    }
    let df_schema = DFSchema::try_from(view.source.schema.as_ref().clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    columns
        .iter()
        .zip(&view.output_exprs)
        .map(|(column, expression)| {
            let expression = context
                .state()
                .create_logical_expr(expression, &df_schema)
                .map_err(|error| {
                    report!(
                        "row view {}: invalid projection expression {expression:?}: {error}",
                        view.view_id
                    )
                })?;
            Ok(expression.alias(column))
        })
        .collect()
}

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
async fn validate_row_view(view: &RowView) -> Result<()> {
    let columns = row_output_columns(view);
    if view.output_exprs.is_empty() {
        project_schema(&view.source.schema, &columns)?;
    } else {
        if view.output_exprs.len() != columns.len() {
            return Err(report!(
                "row view {}: the projection needs one expression per output column",
                view.view_id
            ));
        }
        let context = SessionContext::new();
        let df_schema = DFSchema::try_from(view.source.schema.as_ref().clone())
            .map_err(|error| report!("invalid source schema: {error}"))?;
        for expression in &view.output_exprs {
            context
                .state()
                .create_logical_expr(expression, &df_schema)
                .map_err(|error| {
                    report!(
                        "row view {}: invalid projection expression {expression:?}: {error}",
                        view.view_id
                    )
                })?;
        }
    }
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
            if !columns.contains(key) {
                return Err(report!(
                    "row view {}: output columns must contain the source key {key}",
                    view.view_id
                ));
            }
            // A key must be projected unchanged, so the MV rows stay keyed by
            // the source key.
            if let Some(position) = columns.iter().position(|column| column == key)
                && let Some(expression) = view.output_exprs.get(position)
            {
                let context = SessionContext::new();
                let df_schema =
                    DFSchema::try_from(view.source.schema.as_ref().clone())
                        .map_err(|error| report!("invalid source schema: {error}"))?;
                let parsed = context
                    .state()
                    .create_logical_expr(expression, &df_schema)
                    .map_err(|error| {
                        report!(
                            "row view {}: invalid projection expression {expression:?}: {error}",
                            view.view_id
                        )
                    })?;
                if !matches!(&parsed, Expr::Column(column) if column.name == *key) {
                    return Err(report!(
                        "row view {}: the key column {key} must be projected unchanged",
                        view.view_id
                    ));
                }
            }
        }
    }
    if view.filter.is_some() {
        // The parser resolves every referenced column against the source schema
        // and every subquery table against the registered ones.
        let context = scalar_context(view)?;
        row_filter_predicate(&context, view).await?;
    }
    if view.scalar.is_some() {
        if view.filter.is_none() {
            return Err(report!(
                "row view {}: scalar subqueries without a filter",
                view.view_id
            ));
        }
        // A scalar subquery is only re-evaluated for every key, which needs a
        // keyed source.
        if view.source.primary_keys.is_empty() {
            return Err(report!(
                "row view {}: a scalar subquery needs a keyed source",
                view.view_id
            ));
        }
    }
    Ok(())
}

/// A session with the view's scalar subquery tables registered as empty
/// relations (enough to parse and plan the filter).
fn scalar_context(view: &RowView) -> Result<SessionContext> {
    let context = SessionContext::new();
    if let Some(scalar) = &view.scalar {
        for (name, table) in &scalar.tables {
            let provider: Arc<dyn datafusion::catalog::TableProvider> =
                Arc::new(datafusion::datasource::memory::MemTable::try_new(
                    table.schema.clone(),
                    vec![vec![]],
                )?);
            context
                .register_table(name.as_str(), provider)
                .map_err(|error| {
                    report!(
                        "row view {}: cannot register scalar table {name}: {error}",
                        view.view_id
                    )
                })?;
        }
    }
    Ok(context)
}

impl IvmRuntime {
    /// Persist a projection/filter view spec (idempotent).
    pub async fn register_row_view(&self, view: &RowView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// A session with the row view's scalar subquery tables registered in
    /// their current state, so the filter's subqueries can resolve and run.
    async fn scalar_context_with_state(&self, view: &RowView) -> Result<SessionContext> {
        let context = SessionContext::new();
        let Some(scalar) = &view.scalar else {
            return Ok(context);
        };
        for (name, table) in &scalar.tables {
            let rows = filter_deletes(
                dataframe(
                    &context,
                    table.read_current(&self.client).await?,
                    &table.schema,
                )?,
                change_column(table),
            )?
            .collect()
            .await?;
            let provider: Arc<dyn datafusion::catalog::TableProvider> =
                Arc::new(datafusion::datasource::memory::MemTable::try_new(
                    table.schema.clone(),
                    vec![rows],
                )?);
            context
                .register_table(name.as_str(), provider)
                .map_err(|error| {
                    report!(
                        "row view {}: cannot register scalar table {name}: {error}",
                        view.view_id
                    )
                })?;
        }
        Ok(context)
    }

    /// Refresh a projection/filter view.
    ///
    /// A keyed source is maintained per changed primary key (`delete(old) +
    /// insert(current)` when the row passes the filter); an append-only source
    /// simply appends the delta rows that pass.
    pub async fn refresh_row(&self, view: &RowView) -> Result<Option<i64>> {
        self.register_row_view(view).await?;
        validate_row_view(view).await?;

        let window = self
            .collect_source_window(&view.view_id, &view.source)
            .await?;
        // The scalar subquery inputs are watched as well: a change to one of
        // them can flip any row, so every known key is re-evaluated.
        let mut scalar_windows = Vec::new();
        if let Some(scalar) = &view.scalar {
            for (_, table) in &scalar.tables {
                scalar_windows
                    .push(self.collect_source_window(&view.view_id, table).await?);
            }
        }
        let scalar_changed = scalar_windows
            .iter()
            .any(|window| !window.added_files.is_empty());
        let outer_changed = !window.added_files.is_empty();
        if !outer_changed && !scalar_changed {
            return Ok(None);
        }
        let mut identity = window.identity.clone();
        for scalar_window in &scalar_windows {
            identity.extend(scalar_window.identity.iter().cloned());
        }
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                let mut cursors = window.cursors;
                for scalar_window in scalar_windows {
                    cursors.extend(scalar_window.cursors);
                }
                self.advance_cursors(&view.view_id, cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();
        let keyed = !view.source.primary_keys.is_empty();

        let context = self.scalar_context_with_state(view).await?;
        let delta = if outer_changed {
            dataframe(
                &context,
                view.source.read_partition_files(window.added_files).await?,
                &view.source.schema,
            )?
        } else {
            dataframe(&context, Vec::new(), &view.source.schema)?
        };
        let output_exprs = row_projection(view, &context)?;
        let predicate = row_filter_predicate(&context, view).await?;

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
            let affected = if scalar_changed {
                // A scalar subquery change re-evaluates every key either the
                // current source or the view knows.
                source_now
                    .clone()
                    .select(key_exprs.clone())?
                    .distinct()?
                    .union(mv.clone().select(key_exprs.clone())?.distinct()?)?
            } else {
                delta.select(key_exprs.clone())?.distinct()?
            };
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
            // The deleted rows come from the MV, so they are selected by
            // their materialized names rather than by the source expressions.
            let delete_exprs = row_output_columns(view)
                .iter()
                .map(|column| col(column.as_str()))
                .collect::<Vec<_>>();
            let deletes = mv
                .join(affected, JoinType::LeftSemi, &key_names, &key_names, None)?
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
                .select(delete_exprs)?
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
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
        } else {
            // A CDC tombstone (`delete`) is not a row: an
            // append-only source cannot retract the matching insert, but the
            // marker itself must not become a live row.
            let mut rows = filter_deletes(delta, change_column(&view.source))?;
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
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        let mut cursors = window.cursors;
        for scalar_window in scalar_windows {
            cursors.extend(scalar_window.cursors);
        }
        self.advance_cursors(&view.view_id, cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a projection/filter view from the full source state.
    pub async fn rebuild_row(&self, view: &RowView) -> Result<i64> {
        self.register_row_view(view).await?;
        validate_row_view(view).await?;

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

        let context = self.scalar_context_with_state(view).await?;
        let mut rows = filter_deletes(
            dataframe(&context, baseline.batches, &view.source.schema)?,
            change_column(&view.source),
        )?;
        if let Some(predicate) = row_filter_predicate(&context, view).await? {
            rows = rows.filter(predicate)?;
        }
        let output_exprs = row_projection(view, &context)?;
        for batch in rows
            .select(output_exprs)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
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
}
