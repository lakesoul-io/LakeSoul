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
    let mut columns = if output_columns.is_empty() {
        source_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    } else {
        output_columns.to_vec()
    };
    let mut exprs = output_exprs.to_vec();
    // The MV schema appends the bookkeeping columns itself; a projection that
    // saw them through `SELECT *` must not materialize them twice.
    if columns.iter().any(|column| {
        column.eq_ignore_ascii_case(IVM_ROW_KINDS_COLUMN)
            || column.eq_ignore_ascii_case(IVM_EPOCH_COLUMN)
    }) {
        let mut kept_columns = Vec::with_capacity(columns.len());
        let mut kept_exprs = Vec::with_capacity(exprs.len());
        for (index, column) in columns.iter().enumerate() {
            if column.eq_ignore_ascii_case(IVM_ROW_KINDS_COLUMN)
                || column.eq_ignore_ascii_case(IVM_EPOCH_COLUMN)
            {
                continue;
            }
            kept_columns.push(column.clone());
            if !exprs.is_empty() {
                kept_exprs.push(exprs[index].clone());
            }
        }
        columns = kept_columns;
        exprs = kept_exprs;
    }
    let mut fields = if exprs.is_empty() {
        project_schema(source_schema, &columns)?
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    } else {
        if exprs.len() != columns.len() {
            return Err(report!(
                "the projection needs one expression per output column"
            ));
        }
        let mut fields = Vec::with_capacity(columns.len());
        for (column, expression) in columns.iter().zip(&exprs) {
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
/// The MV range partitions an affected source row can live in.
///
/// Sound when every MV partition column is a plain source column the view
/// materializes unchanged, the source primary keys carry its partition columns
/// (a key cannot move between partitions), and the MV keys carry the partition
/// columns.  Every row the window can change then lies in one of the delta's
/// partitions, so the other MV partitions can be skipped.
fn row_mv_partitions(
    view: &RowView,
    window: &SourceWindow,
) -> Option<Vec<(String, Vec<String>)>> {
    let mv_partitions = &view.mv.range_partition_columns;
    if mv_partitions.is_empty() {
        return None;
    }
    let source = &view.source;
    if !source
        .range_partition_columns
        .iter()
        .all(|column| source.primary_keys.contains(column))
    {
        return None;
    }
    let outputs = row_output_columns(view);
    for partition in mv_partitions {
        if !source.range_partition_columns.contains(partition)
            || !view.mv.primary_keys.contains(partition)
        {
            return None;
        }
        let index = outputs.iter().position(|column| column == partition)?;
        // A computed projection would not carry the source's partition value.
        if let Some(expression) = view.output_exprs.get(index)
            && expression.trim() != partition
        {
            return None;
        }
    }
    let mut restriction = mv_partitions
        .iter()
        .map(|column| (column.clone(), Vec::new()))
        .collect::<Vec<_>>();
    for files in &window.added_files {
        let values = crate::table::parse_partition_values(
            &files.partition_desc,
            &source.range_partition_columns,
        )?;
        for (column, allowed) in restriction.iter_mut() {
            let index = source
                .range_partition_columns
                .iter()
                .position(|candidate| candidate == column)?;
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

fn row_projection(view: &RowView, context: &SessionContext) -> Result<Vec<Expr>> {
    let columns = row_output_columns(view);
    let mut seen = std::collections::HashSet::new();
    for column in &columns {
        if !seen.insert(column.clone()) {
            return Err(report!(
                "row view {}: duplicate output column {column} over source {}; columns={:?} exprs={:?}",
                view.view_id,
                view.source.table_name,
                columns,
                view.output_exprs
            ));
        }
    }
    if view.output_exprs.is_empty() {
        return Ok(columns.iter().map(|column| col(column.as_str())).collect());
    }
    columns
        .iter()
        .zip(&view.output_exprs)
        .map(|(column, expression)| {
            let expression = parse_row_expression(
                context,
                &view.source.schema,
                expression,
            )
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

/// Parse a rendered projection expression.  A plan may keep an internal
/// column in lower case (`rowkinds`), while the table schema preserves its
/// spelling (`rowKinds`); retry against a lower-cased schema and restore the
/// real field names.
fn parse_row_expression(
    context: &SessionContext,
    schema: &Schema,
    expression: &str,
) -> Result<Expr> {
    let df_schema = DFSchema::try_from(schema.clone())
        .map_err(|error| report!("invalid source schema: {error}"))?;
    if let Ok(expr) = context.state().create_logical_expr(expression, &df_schema) {
        return Ok(expr);
    }
    let lowered = Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                Field::new(
                    field.name().to_lowercase(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>(),
    );
    let lowered_schema = DFSchema::try_from(lowered)
        .map_err(|error| report!("invalid source schema: {error}"))?;
    let expr = context
        .state()
        .create_logical_expr(expression, &lowered_schema)
        .map_err(|error| report!("{error}"))?;
    expr.transform_down(|node| {
        if let Expr::Column(column) = &node
            && schema.field_with_name(&column.name).is_err()
            && let Some(field) = schema
                .fields()
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(&column.name))
        {
            return Ok(Transformed::yes(Expr::Column(Column::new(
                column.relation.clone(),
                field.name().clone(),
            ))));
        }
        Ok(Transformed::no(node))
    })
    .map(|transformed| transformed.data)
    .map_err(|error| report!("{error}"))
}

fn row_output_columns(view: &RowView) -> Vec<String> {
    let resolve = |name: &str| {
        view.source
            .schema
            .field_with_name(name)
            .map(|field| field.name().clone())
            .or_else(|_| {
                view.source
                    .schema
                    .fields()
                    .iter()
                    .find(|field| field.name().eq_ignore_ascii_case(name))
                    .map(|field| field.name().clone())
                    .ok_or(())
            })
            .unwrap_or_else(|_| name.to_string())
    };
    let columns = if view.output_columns.is_empty() {
        view.source
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    } else {
        view.output_columns.clone()
    };
    columns
        .into_iter()
        .filter(|column| {
            !column.eq_ignore_ascii_case(IVM_ROW_KINDS_COLUMN)
                && !column.eq_ignore_ascii_case(IVM_EPOCH_COLUMN)
        })
        .map(|column| resolve(&column))
        .collect()
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
        let context = refresh_session();
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
                let context = refresh_session();
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
                // A cast to the key's own type is an identity the optimizer
                // introduces around a plain column; the value is unchanged.
                let unchanged = match &parsed {
                    Expr::Column(column) => column.name == *key,
                    Expr::Cast(cast) => {
                        cast.field.data_type() == field.data_type()
                            && matches!(
                                cast.expr.as_ref(),
                                Expr::Column(column) if column.name == *key
                            )
                    }
                    _ => false,
                };
                if !unchanged {
                    return Err(report!(
                        "row view {}: the key column {key} must be projected unchanged",
                        view.view_id
                    ));
                }
            }
        }
        // A composite key may have a nullable component (a window partition
        // column, say) as long as at least one component is non-null; the
        // source primary key in the same key list keeps rows distinct.
        if view.source.primary_keys.iter().all(|key| {
            view.source
                .schema
                .field_with_name(key)
                .is_ok_and(|field| field.is_nullable())
        }) {
            return Err(report!(
                "row view {}: source {} needs a non-null key column",
                view.view_id,
                view.source.table_name
            ));
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
    let context = refresh_session();
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
        let context = refresh_session();
        let Some(scalar) = &view.scalar else {
            return Ok(context);
        };
        for (name, table) in &scalar.tables {
            let frame = filter_deletes(
                self.current_table_frame(&context, table)?,
                change_column(table),
            )?;
            context
                .register_table(name.as_str(), frame.into_view())
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
            .collect_source_window_for_filter(
                &view.view_id,
                &view.source,
                view.filter.as_deref(),
            )
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
        // Computed before the delta read moves `window.added_files`.
        let partitions = row_mv_partitions(view, &window);
        let delta = if outer_changed {
            self.groups_table_frame(&context, &view.source, window.added_files)?
        } else {
            dataframe(&context, Vec::new(), &view.source.schema)?
        };
        let output_exprs = row_projection(view, &context)?;
        let predicate = row_filter_predicate(&context, view).await?;

        if keyed {
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
            // The delta is the merged current state of the changed keys.
            // The MV is read as a streaming provider (optionally restricted
            // to the touched range partitions); the semi-joins below select
            // the affected keys, so no key list is collected.
            let (source_now, mv, affected) = if scalar_changed {
                let source_now = filter_deletes(
                    self.current_table_frame(&context, &view.source)?,
                    change_column(&view.source),
                )?;
                let mv = self.current_table_frame(&context, &view.mv)?;
                // A scalar subquery change re-evaluates every key either the
                // current source or the view knows.
                let affected = source_now
                    .clone()
                    .select(key_exprs.clone())?
                    .distinct()?
                    .union(mv.clone().select(key_exprs.clone())?.distinct()?)?;
                (source_now, mv, affected)
            } else {
                // The changed keys include delete tombstones: their state row
                // must be retracted even though no new row survives.
                let affected = delta.clone().select(key_exprs.clone())?.distinct()?;
                let source_now = filter_deletes(delta, change_column(&view.source))?;
                let mv = if view.mv.primary_keys == view.source.primary_keys {
                    self.lookup_source_by_join_keys(
                        &context,
                        &view.mv,
                        affected.clone(),
                        &view.source.primary_keys,
                        &view.mv.primary_keys,
                        true,
                    )
                    .await?
                } else {
                    match &partitions {
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
                (source_now, mv, affected)
            };
            let mut passing = if scalar_changed {
                source_now.join(
                    affected.clone(),
                    JoinType::LeftSemi,
                    &key_names,
                    &key_names,
                    None,
                )?
            } else {
                // Every delta row belongs to an affected key.
                source_now
            };
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
            commit_ids.extend(
                append_dataframe(
                    &view.mv,
                    &self.client,
                    inserts.union(deletes)?.sort_by(sort_exprs)?,
                )
                .await?
                .commit_ids,
            );
        } else {
            // A CDC tombstone (`delete`) is not a row: an
            // append-only source cannot retract the matching insert, but the
            // marker itself must not become a live row.
            let mut rows = filter_deletes(delta, change_column(&view.source))?;
            if let Some(predicate) = predicate {
                rows = rows.filter(predicate)?;
            }
            commit_ids.extend(
                append_dataframe(
                    &view.mv,
                    &self.client,
                    rows.select(output_exprs)?
                        .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
                )
                .await?
                .commit_ids,
            );
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        let baseline = self
            .source_baseline_for_filter(&view.source, view.filter.as_deref())
            .await?;
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
            self.groups_table_frame(&context, &view.source, baseline.groups.clone())?,
            change_column(&view.source),
        )?;
        if let Some(predicate) = row_filter_predicate(&context, view).await? {
            rows = rows.filter(predicate)?;
        }
        let output_exprs = row_projection(view, &context)?;
        commit_ids.extend(
            append_dataframe(
                &view.mv,
                &self.client,
                rows.select(output_exprs.clone())
                    .map_err(|error| {
                        report!(
                            "row view {}: projection {output_exprs:?}: {error}",
                            view.view_id
                        )
                    })?
                    .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }
}
