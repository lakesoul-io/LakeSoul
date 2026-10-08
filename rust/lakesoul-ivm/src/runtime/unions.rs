// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The `UNION ALL` / `UNION` (distinct) views.

use super::*;

/// A persisted source of a [`ViewSpec::UnionAll`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnionSourceSpec {
    /// The source table id.
    pub table_id: String,
    /// An optional filter the source rows must satisfy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// The projected output columns; empty means the branch selects its source
    /// columns in schema order (the CDC change column excluded for `UNION`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// The rendered projection expressions, parallel to `columns`; empty means
    /// every column is a plain source column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exprs: Vec<String>,
}

/// `UNION ALL` of several sources with the same schema.
///
/// With keyed sources the output is keyed by `(__ivm_source, primary keys)`,
/// so updates and deletes are retracted per source; with append-only sources
/// the output is append-only.
#[derive(Debug, Clone)]
pub struct UnionAllView {
    /// The view id.
    pub view_id: String,
    /// The sources, in output order.
    pub sources: Vec<UnionSource>,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

/// One source of a [`UnionAllView`], optionally filtered.
#[derive(Debug, Clone)]
pub struct UnionSource {
    /// The source table.
    pub table: IvmTable,
    /// An optional filter the source rows must satisfy.
    pub filter: Option<String>,
    /// The projected output columns; empty means every source column.
    pub columns: Vec<String>,
    /// The rendered projection expressions, parallel to `columns`; empty means
    /// every column is a plain source column.
    pub exprs: Vec<String>,
}

impl UnionSource {
    /// A source with no filter.
    pub fn new(table: IvmTable) -> Self {
        Self {
            table,
            filter: None,
            columns: Vec::new(),
            exprs: Vec::new(),
        }
    }

    /// Only rows matching `filter` contribute to the view.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Project the branch onto `columns` (`exprs` parallel and empty when
    /// every column is a plain source column).
    pub fn with_projection(mut self, columns: Vec<String>, exprs: Vec<String>) -> Self {
        self.columns = columns;
        self.exprs = exprs;
        self
    }

    fn to_spec(&self) -> UnionSourceSpec {
        UnionSourceSpec {
            table_id: self.table.table_id.clone(),
            filter: self.filter.clone(),
            columns: self.columns.clone(),
            exprs: self.exprs.clone(),
        }
    }
}

impl UnionAllView {
    /// A new union-all view over `sources`.
    pub fn new(
        view_id: impl Into<String>,
        sources: Vec<UnionSource>,
        mv: IvmTable,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            sources,
            mv,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::UnionAll {
            view_id: self.view_id.clone(),
            sources: self.sources.iter().map(UnionSource::to_spec).collect(),
            mv_table_id: self.mv.table_id.clone(),
        }
    }
}

/// A `UNION` (distinct) view over sources with the same schema.
///
/// The materialized view holds one row per distinct value plus its occurrence
/// count ([`IVM_COUNT_COLUMN`]); rows are keyed by the union columns.
#[derive(Debug, Clone)]
pub struct UnionDistinctView {
    /// The view id.
    pub view_id: String,
    /// The sources, in output order.
    pub sources: Vec<UnionSource>,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl UnionDistinctView {
    /// A new union-distinct view over `sources`.
    pub fn new(
        view_id: impl Into<String>,
        sources: Vec<UnionSource>,
        mv: IvmTable,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            sources,
            mv,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::UnionDistinct {
            view_id: self.view_id.clone(),
            sources: self.sources.iter().map(UnionSource::to_spec).collect(),
            mv_table_id: self.mv.table_id.clone(),
        }
    }
}

/// The schema of a [`UnionAllView`] materialized view: the source columns
/// plus the source index, the row kind and the epoch. Keyed sources are keyed
/// by `(__ivm_source, primary keys)`.
pub fn union_all_mv_schema_for(source_schema: &Schema) -> Result<SchemaRef> {
    let mut fields = source_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        IVM_SOURCE_COLUMN,
        DataType::Int32,
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

/// The output schema of one union branch: the projected columns (or every
/// source column when the projection is empty), with the projected types.
pub fn union_output_schema_for(
    source_schema: &Schema,
    columns: &[String],
    exprs: &[String],
) -> Result<SchemaRef> {
    if !exprs.is_empty() && exprs.len() != columns.len() {
        return Err(report!(
            "a union projection needs one expression per column"
        ));
    }
    if columns.is_empty() {
        return Ok(Arc::new(source_schema.clone()));
    }
    if exprs.is_empty() {
        return project_schema(source_schema, columns);
    }
    let mut fields = Vec::with_capacity(columns.len());
    for (column, expression) in columns.iter().zip(exprs) {
        let (data_type, nullable) = expression_type(source_schema, expression)?;
        fields
            .push(Arc::new(Field::new(column, data_type, nullable))
                as arrow_schema::FieldRef);
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a [`UnionDistinctView`] materialized view: the branch output
/// columns plus the occurrence count, the row kind and the epoch.
pub fn union_distinct_mv_schema_for(source_schema: &Schema) -> SchemaRef {
    let mut fields = source_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        IVM_COUNT_COLUMN,
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
    Arc::new(Schema::new(fields))
}

/// Validate that a union-all view can be maintained.
fn validate_union_all_view(view: &UnionAllView) -> Result<()> {
    if view.sources.is_empty() {
        return Err(report!(
            "union all view {} needs at least one source",
            view.view_id
        ));
    }
    let keyed = !view.sources[0].table.primary_keys.is_empty();
    let first = &view.sources[0].table.schema;
    for source in &view.sources {
        if let Some(filter) = &source.filter {
            let context = SessionContext::new();
            parse_filter(&context, &source.table.schema, filter)?;
        }
        let table = &source.table;
        if table.schema.fields().len() != first.fields().len()
            || table
                .schema
                .fields()
                .iter()
                .zip(first.fields())
                .any(|(left, right)| {
                    left.name() != right.name() || left.data_type() != right.data_type()
                })
        {
            return Err(report!(
                "union all view {}: source {} does not have the same schema",
                view.view_id,
                table.table_name
            ));
        }
        if keyed != !table.primary_keys.is_empty() {
            return Err(report!(
                "union all view {}: sources must be all keyed or all append-only",
                view.view_id
            ));
        }
        if keyed {
            for key in &table.primary_keys {
                let field = table.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "union all view {}: key column {key} is not in source {}",
                        view.view_id,
                        table.table_name
                    )
                })?;
                if field.is_nullable() {
                    return Err(report!(
                        "union all view {}: key column {key} must be non-nullable",
                        view.view_id
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The union columns of a union-distinct view.
fn union_distinct_columns(view: &UnionDistinctView) -> Result<Vec<String>> {
    view.sources
        .first()
        .map(|source| union_branch_columns(source, source.table.cdc_column.as_deref()))
        .ok_or_else(|| report!("union view {} has no sources", view.view_id))
}

/// `select <projection>` of one union-distinct branch: the canonical output
/// columns with the branch's rendered expressions (empty means plain columns).
fn union_distinct_projection_sql(columns: &[String], exprs: &[String]) -> String {
    if exprs.is_empty() {
        return quoted_list(columns);
    }
    columns
        .iter()
        .zip(exprs)
        .map(|(column, expression)| format!("{expression} as {}", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The per-source occurrence deltas of a union-distinct refresh.
fn union_distinct_delta_ctes(
    view: &UnionDistinctView,
    columns: &[String],
    keyed: bool,
) -> Result<String> {
    let cols = quoted_list(columns);
    let mut ctes = Vec::with_capacity(view.sources.len());
    for (index, source) in view.sources.iter().enumerate() {
        let table = &source.table;
        let change = change_column(table);
        let filter = source.filter.as_deref();
        let new_table = format!("src{index}");
        let projection = union_distinct_projection_sql(columns, &source.exprs);
        if keyed {
            let new_filter = format!(
                "{}{}",
                source_delete_filter(&new_table, change),
                filter_clause(filter),
            );
            let old_table = format!("old{index}");
            let old_filter = format!(
                "{}{}",
                source_delete_filter(&old_table, change),
                filter_clause(filter),
            );
            let pk_match =
                key_join_condition(&old_table, &new_table, &table.primary_keys);
            ctes.push(format!(
                "d{index} as (select {cols}, count(1) as dcount from \
                 (select {projection} from {new_table} where {new_filter}) p{index} \
                 group by {cols} \
                 union all \
                 select {cols}, -count(1) as dcount from \
                 (select {projection} from {old_table} where {old_filter} \
                    and exists (select 1 from {new_table} where {pk_match})) o{index} \
                 group by {cols})",
            ));
        } else {
            match change {
                Some(_) => {
                    let retract = source_retract_condition(&new_table, change);
                    ctes.push(format!(
                        "d{index} as (select {cols}, sum(dcount) as dcount from \
                         (select {projection}, \
                                 case when {retract} then -1 else 1 end as dcount \
                          from {new_table}{}) p{index} group by {cols})",
                        filter_where(filter),
                    ));
                }
                None => ctes.push(format!(
                    "d{index} as (select {cols}, count(1) as dcount from \
                     (select {projection} from {new_table}{}) p{index} \
                     group by {cols})",
                    filter_where(filter),
                )),
            }
        }
    }
    Ok(ctes.join(", "))
}

/// The SQL of a union-distinct refresh: the signed occurrence delta is added
/// to the active counts, the affected keys are rewritten (delete then insert)
/// and rows whose count drops to zero disappear.
fn union_distinct_refresh_sql(
    view: &UnionDistinctView,
    keyed: bool,
    epoch: i64,
) -> Result<String> {
    let columns = union_distinct_columns(view)?;
    let cols = quoted_list(&columns);
    let count = quote_ident(IVM_COUNT_COLUMN);
    let ctes = union_distinct_delta_ctes(view, &columns, keyed)?;
    let delta = view
        .sources
        .iter()
        .enumerate()
        .map(|(index, _)| format!("select * from d{index}"))
        .collect::<Vec<_>>()
        .join(" union all ");
    let merged_columns = columns
        .iter()
        .map(|column| {
            let quoted = quote_ident(column);
            format!("coalesce(c.{quoted}, a.{quoted}) as {quoted}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mv_columns = columns
        .iter()
        .map(|column| format!("mv.{}", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let counts_to_active = key_join_condition_null_safe("c", "a", &columns);
    let mv_to_counts = key_join_condition_null_safe("mv", "c", &columns);
    let kinds = IVM_ROW_KINDS_COLUMN;
    let epoch_column = IVM_EPOCH_COLUMN;
    Ok(format!(
        "with {ctes}, \
         counts as (select {cols}, sum(dcount) as dcount from ({delta}) delta \
                    group by {cols}), \
         active as (select * from mv where \"{kinds}\" = 'insert' \
                    and \"{epoch_column}\" <> {epoch}), \
         merged as (select {merged_columns}, \
                    coalesce(a.{count}, 0) + c.dcount as {count} \
                    from counts c left join active a on {counts_to_active}), \
         deletes as (select {mv_columns}, mv.{count}, 'delete' as \"{kinds}\", \
                     {epoch} as \"{epoch_column}\" \
                     from mv where \"{kinds}\" = 'insert' \
                       and exists (select 1 from counts c where {mv_to_counts})), \
         inserts as (select {cols}, {count}, 'insert' as \"{kinds}\", \
                     {epoch} as \"{epoch_column}\" \
                     from merged where {count} >= 1) \
         select * from deletes union all select * from inserts \
         order by {cols}, \"{kinds}\"",
    ))
}

/// The SQL of a union-distinct rebuild: the occurrence counts over the full
/// current source states.
fn union_distinct_rebuild_sql(view: &UnionDistinctView, epoch: i64) -> Result<String> {
    let columns = union_distinct_columns(view)?;
    let cols = quoted_list(&columns);
    let count = quote_ident(IVM_COUNT_COLUMN);
    let keyed = !view.sources[0].table.primary_keys.is_empty();
    let mut parts = Vec::with_capacity(view.sources.len());
    for (index, source) in view.sources.iter().enumerate() {
        let table = &source.table;
        let change = change_column(table);
        let table_name = format!("src{index}");
        let filter = source.filter.as_deref();
        let projection = union_distinct_projection_sql(&columns, &source.exprs);
        if keyed {
            // The baseline is merged by key, so the surviving rows count once.
            let where_clause = format!(
                " where {}{}",
                source_delete_filter(&table_name, change),
                filter_clause(filter),
            );
            parts.push(format!(
                "select {cols}, count(1) as dcount from \
                 (select {projection} from {table_name}{where_clause}) p{index} \
                 group by {cols}"
            ));
        } else {
            // An append-only CDC source keeps every marker, so the markers
            // are aggregated with their sign (like the SUM/COUNT rebuild).
            match change {
                Some(_) => {
                    let retract = source_retract_condition(&table_name, change);
                    parts.push(format!(
                        "select {cols}, sum(dcount) as dcount from \
                         (select {projection}, \
                                 case when {retract} then -1 else 1 end as dcount \
                          from {table_name}{}) p{index} group by {cols}",
                        filter_where(filter),
                    ));
                }
                None => parts.push(format!(
                    "select {cols}, count(1) as dcount from \
                     (select {projection} from {table_name}{}) p{index} \
                     group by {cols}",
                    filter_where(filter),
                )),
            }
        }
    }
    let kinds = IVM_ROW_KINDS_COLUMN;
    let epoch_column = IVM_EPOCH_COLUMN;
    Ok(format!(
        "with totals as ({totals}), \
         counts as (select {cols}, sum(dcount) as dcount from totals group by {cols}) \
         select {cols}, dcount as {count}, 'insert' as \"{kinds}\", \
         {epoch} as \"{epoch_column}\" from counts where dcount >= 1",
        totals = parts.join(" union all "),
    ))
}

/// Validate that a union-distinct view can be maintained.
fn validate_union_distinct_view(view: &UnionDistinctView) -> Result<()> {
    if view.sources.len() < 2 {
        return Err(report!(
            "union view {} needs at least two sources",
            view.view_id
        ));
    }
    let keyed = !view.sources[0].table.primary_keys.is_empty();
    let first = &view.sources[0].table.schema;
    for source in &view.sources {
        if let Some(filter) = &source.filter {
            let context = SessionContext::new();
            parse_filter(&context, &source.table.schema, filter)?;
        }
        let table = &source.table;
        if table.schema.fields().len() != first.fields().len()
            || table
                .schema
                .fields()
                .iter()
                .zip(first.fields())
                .any(|(left, right)| {
                    left.name() != right.name() || left.data_type() != right.data_type()
                })
        {
            return Err(report!(
                "union view {}: source {} does not have the same schema",
                view.view_id,
                table.table_name
            ));
        }
        if keyed != !table.primary_keys.is_empty() {
            return Err(report!(
                "union view {}: sources must be all keyed or all append-only",
                view.view_id
            ));
        }
        if keyed {
            for key in &table.primary_keys {
                let field = table.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "union view {}: key column {key} is not in source {}",
                        view.view_id,
                        table.table_name
                    )
                })?;
                if field.is_nullable() {
                    return Err(report!(
                        "union view {}: key column {key} must be non-nullable",
                        view.view_id
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Frames of the projection of a source onto `(columns..., __ivm_source)`.
/// The output columns of a union branch: the projection, or the source's data
/// columns when the branch selects everything (`change_column` is excluded
/// when set).
fn union_branch_columns(
    source: &UnionSource,
    change_column: Option<&str>,
) -> Vec<String> {
    if !source.columns.is_empty() {
        return source.columns.clone();
    }
    source
        .table
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| Some(name.as_str()) != change_column)
        .collect()
}

/// The DataFrame projection of one `UNION ALL` branch: the projected columns
/// plus the source index.
fn union_source_projection(
    context: &SessionContext,
    source: &UnionSource,
    change_column: Option<&str>,
    index: usize,
) -> Result<Vec<Expr>> {
    let columns = union_branch_columns(source, change_column);
    let mut exprs = if source.exprs.is_empty() {
        columns
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>()
    } else {
        let df_schema = DFSchema::try_from(source.table.schema.as_ref().clone())
            .map_err(|error| report!("invalid source schema: {error}"))?;
        columns
            .iter()
            .zip(&source.exprs)
            .map(|(column, expression)| {
                let expression = context
                    .state()
                    .create_logical_expr(expression, &df_schema)
                    .map_err(|error| {
                        report!("invalid union projection {expression:?}: {error}")
                    })?;
                Ok(expression.alias(column))
            })
            .collect::<Result<Vec<_>>>()?
    };
    exprs.push(lit(index as i32).alias(IVM_SOURCE_COLUMN));
    Ok(exprs)
}

/// `UNION ALL` of several frames.
fn union_frames(mut frames: Vec<DataFrame>) -> Result<DataFrame> {
    let first = frames.remove(0);
    let mut combined = first;
    for frame in frames {
        combined = combined.union(frame)?;
    }
    Ok(combined)
}

impl IvmRuntime {
    /// Persist a union-all view spec (idempotent).
    pub async fn register_union_all_view(&self, view: &UnionAllView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a union-distinct view spec (idempotent).
    pub async fn register_union_distinct_view(
        &self,
        view: &UnionDistinctView,
    ) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a `UNION ALL` view over keyed or append-only sources.
    pub async fn refresh_union_all(&self, view: &UnionAllView) -> Result<Option<i64>> {
        self.register_union_all_view(view).await?;
        validate_union_all_view(view)?;
        for source in &view.sources {
            self.ensure_unpartitioned(&source.table).await?;
        }

        let mut windows = Vec::new();
        for source in &view.sources {
            windows.push(
                self.collect_source_window(&view.view_id, &source.table)
                    .await?,
            );
        }
        if windows.iter().all(|window| window.added_files.is_empty()) {
            return Ok(None);
        }
        let identity = windows
            .iter()
            .flat_map(|window| window.identity.iter().cloned())
            .collect::<Vec<_>>();
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                for window in &windows {
                    self.advance_cursors(&view.view_id, window.cursors.clone())
                        .await?;
                }
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();
        let keyed = !view.sources[0].table.primary_keys.is_empty();

        let context = SessionContext::new();
        let mut inserts = Vec::new();
        let mut changed = Vec::new();
        for (index, source) in view.sources.iter().enumerate() {
            let table = &source.table;
            let filter = source
                .filter
                .as_deref()
                .map(|filter| parse_filter(&context, &table.schema, filter))
                .transpose()?;
            let delta = dataframe(
                &context,
                table.read_files(windows[index].added_files.clone()).await?,
                &table.schema,
            )?;
            if keyed {
                let source_now = filter_deletes(
                    dataframe(
                        &context,
                        table.read_current(&self.client).await?,
                        &table.schema,
                    )?,
                    change_column(table),
                )?;
                let key_names = table
                    .primary_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let key_exprs = table
                    .primary_keys
                    .iter()
                    .map(|column| col(column.as_str()))
                    .collect::<Vec<_>>();
                let affected = delta
                    .select(key_exprs)?
                    .distinct()?
                    .with_column(IVM_SOURCE_COLUMN, lit(index as i32))?;
                let rows = source_now.join(
                    affected.clone(),
                    JoinType::LeftSemi,
                    &key_names,
                    &key_names,
                    None,
                )?;
                let rows = match &filter {
                    Some(filter) => rows.filter(filter.clone())?,
                    None => rows,
                };
                inserts.push(
                    rows.select(union_source_projection(&context, source, None, index)?)?,
                );
                changed.push(affected);
            } else {
                let rows = match &filter {
                    Some(filter) => delta.filter(filter.clone())?,
                    None => delta,
                };
                // An append-only CDC source keeps its markers: translate them
                // into the MV row kinds, so logical reads drop the retractions
                // even when the CDC column is not part of the projection.
                let kinds = match change_column(table) {
                    Some(column) => {
                        let retract = col(column)
                            .in_list(vec![lit("delete"), lit("update_before")], false);
                        when(retract, lit("delete")).otherwise(lit("insert"))?
                    }
                    None => lit("insert"),
                };
                let mut projection =
                    union_source_projection(&context, source, None, index)?;
                projection.push(kinds.alias(IVM_ROW_KINDS_COLUMN));
                inserts.push(rows.select(projection)?);
            }
        }

        let output_exprs = union_branch_columns(&view.sources[0], None)
            .iter()
            .map(|column| col(column.as_str()))
            .chain(std::iter::once(col(IVM_SOURCE_COLUMN)))
            .collect::<Vec<_>>();

        if keyed {
            let mv = dataframe(
                &context,
                view.mv.read_current(&self.client).await?,
                &view.mv.schema,
            )?;
            let mut pair_names = vec![IVM_SOURCE_COLUMN.to_string()];
            pair_names.extend(view.sources[0].table.primary_keys.iter().cloned());
            let pair_refs = pair_names.iter().map(String::as_str).collect::<Vec<_>>();
            let pair_exprs = pair_names
                .iter()
                .map(|column| col(column.as_str()))
                .collect::<Vec<_>>();
            let already = mv
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(pair_exprs.clone())?
                .distinct()?;
            let inserts = union_frames(inserts)?
                .join(already, JoinType::LeftAnti, &pair_refs, &pair_refs, None)?
                .select(output_exprs.clone())?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let changed = union_frames(changed)?;
            let deletes = mv
                .join(changed, JoinType::LeftSemi, &pair_refs, &pair_refs, None)?
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
                .select(output_exprs)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let mut sort_exprs = vec![column_expr(IVM_SOURCE_COLUMN)];
            sort_exprs.extend(
                view.sources[0]
                    .table
                    .primary_keys
                    .iter()
                    .map(|key| column_expr(key)),
            );
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
            // The append-only frames already carry the MV column order (the
            // output columns, the source index, the row kind).
            for batch in union_frames(inserts)?
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
        for window in &windows {
            self.advance_cursors(&view.view_id, window.cursors.clone())
                .await?;
        }
        Ok(Some(epoch))
    }

    /// Rebuild a `UNION ALL` view from the full states of its sources.
    pub async fn rebuild_union_all(&self, view: &UnionAllView) -> Result<i64> {
        self.register_union_all_view(view).await?;
        validate_union_all_view(view)?;
        for source in &view.sources {
            self.ensure_unpartitioned(&source.table).await?;
        }

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let mut baselines = Vec::new();
        for source in &view.sources {
            baselines.push(self.source_baseline(&source.table).await?);
        }
        let to_versions = baselines
            .iter()
            .flat_map(|baseline| baseline.to_versions.iter().cloned())
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
                for baseline in &baselines {
                    self.advance_cursors(&view.view_id, baseline.cursors.clone())
                        .await?;
                }
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
        let mut frames = Vec::new();
        for (index, source) in view.sources.iter().enumerate() {
            let table = &source.table;
            let rows = filter_deletes(
                dataframe(&context, baselines[index].batches.clone(), &table.schema)?,
                change_column(table),
            )?;
            let rows = match &source.filter {
                Some(filter) => {
                    rows.filter(parse_filter(&context, &table.schema, filter)?)?
                }
                None => rows,
            };
            frames.push(
                rows.select(union_source_projection(&context, source, None, index)?)?,
            );
        }
        let output_exprs = union_branch_columns(&view.sources[0], None)
            .iter()
            .map(|column| col(column.as_str()))
            .chain(std::iter::once(col(IVM_SOURCE_COLUMN)))
            .collect::<Vec<_>>();
        for batch in union_frames(frames)?
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
        for baseline in &baselines {
            self.advance_cursors(&view.view_id, baseline.cursors.clone())
                .await?;
        }
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Refresh a union-distinct view over keyed or append-only sources.
    ///
    /// A keyed source contributes its new values (`+1`) and the as-of values
    /// of its changed primary keys (`-1`), so updates and deletes retract the
    /// old distinct rows; an append-only source contributes a signed count of
    /// its delta rows (only a CDC column can make it negative).
    pub async fn refresh_union_distinct(
        &self,
        view: &UnionDistinctView,
    ) -> Result<Option<i64>> {
        self.register_union_distinct_view(view).await?;
        validate_union_distinct_view(view)?;
        for source in &view.sources {
            self.ensure_unpartitioned(&source.table).await?;
        }

        let mut windows = Vec::new();
        for source in &view.sources {
            windows.push(
                self.collect_source_window(&view.view_id, &source.table)
                    .await?,
            );
        }
        if windows.iter().all(|window| window.added_files.is_empty()) {
            return Ok(None);
        }
        let identity = windows
            .iter()
            .flat_map(|window| window.identity.iter().cloned())
            .collect::<Vec<_>>();
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                for window in &windows {
                    self.advance_cursors(&view.view_id, window.cursors.clone())
                        .await?;
                }
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();
        let keyed = !view.sources[0].table.primary_keys.is_empty();

        let context = SessionContext::new();
        for (index, source) in view.sources.iter().enumerate() {
            let table = &source.table;
            let delta = table.read_files(windows[index].added_files.clone()).await?;
            if keyed {
                let pk_filters = key_filters(&table.primary_keys, &delta)?;
                let before = table
                    .read_as_of_filtered(
                        &self.client,
                        windows[index].before_timestamp,
                        pk_filters,
                    )
                    .await?;
                register_table(&context, &format!("old{index}"), before, &table.schema)?;
            }
            register_table(&context, &format!("src{index}"), delta, &table.schema)?;
        }
        register_table(
            &context,
            "mv",
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        let sql = union_distinct_refresh_sql(view, keyed, epoch)?;
        for batch in context.sql(&sql).await?.collect().await? {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for window in &windows {
            self.advance_cursors(&view.view_id, window.cursors.clone())
                .await?;
        }
        Ok(Some(epoch))
    }

    /// Rebuild a union-distinct view from the full source states.
    pub async fn rebuild_union_distinct(&self, view: &UnionDistinctView) -> Result<i64> {
        self.register_union_distinct_view(view).await?;
        validate_union_distinct_view(view)?;
        for source in &view.sources {
            self.ensure_unpartitioned(&source.table).await?;
        }

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let mut baselines = Vec::new();
        for source in &view.sources {
            baselines.push(self.source_baseline(&source.table).await?);
        }
        let to_versions = baselines
            .iter()
            .flat_map(|baseline| baseline.to_versions.iter().cloned())
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
                for baseline in &baselines {
                    self.advance_cursors(&view.view_id, baseline.cursors.clone())
                        .await?;
                }
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
        for (index, source) in view.sources.iter().enumerate() {
            register_table(
                &context,
                &format!("src{index}"),
                baselines[index].batches.clone(),
                &source.table.schema,
            )?;
        }
        let sql = union_distinct_rebuild_sql(view, epoch)?;
        for batch in context.sql(&sql).await?.collect().await? {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for baseline in &baselines {
            self.advance_cursors(&view.view_id, baseline.cursors.clone())
                .await?;
        }
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }
}
