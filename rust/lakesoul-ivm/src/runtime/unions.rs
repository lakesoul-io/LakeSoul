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
    /// The branch-index column name.
    pub source_column: String,
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
            source_column: IVM_SOURCE_COLUMN.to_string(),
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::UnionAll {
            view_id: self.view_id.clone(),
            sources: self.sources.iter().map(UnionSource::to_spec).collect(),
            mv_table_id: self.mv.table_id.clone(),
            source_column: self.source_column.clone(),
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
    union_all_mv_schema_with_source(source_schema, IVM_SOURCE_COLUMN)
}

/// The union-all MV schema with an explicit branch-index column name (a
/// nested union uses a per-view name).
pub fn union_all_mv_schema_with_source(
    source_schema: &Schema,
    source_column: &str,
) -> Result<SchemaRef> {
    let mut fields = source_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(source_column, DataType::Int32, false)));
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
    let branch_columns = |source: &UnionSource| {
        union_branch_columns(source, source.table.cdc_column.as_deref())
    };
    let branch_schema = |source: &UnionSource| {
        let columns = branch_columns(source);
        let exprs = if source.columns.is_empty() {
            Vec::new()
        } else {
            source.exprs.clone()
        };
        union_output_schema_for(&source.table.schema, &columns, &exprs)
    };
    let first = branch_schema(&view.sources[0])?;
    for source in &view.sources {
        if let Some(filter) = &source.filter {
            let context = refresh_session();
            parse_filter(&context, &source.table.schema, filter)?;
        }
        let table = &source.table;
        let schema = branch_schema(source)?;
        if first.fields().len() != schema.fields().len()
            || first
                .fields()
                .iter()
                .zip(schema.fields())
                .any(|(left, right)| {
                    left.name() != right.name()
                        || crate::table::normalize_data_type(left.data_type())
                            != crate::table::normalize_data_type(right.data_type())
                })
        {
            return Err(report!(
                "union all view {}: source {} does not have the same output schema",
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
                // A key may be materialized under a canonical output name
                // (`raw AS __ivm_source_inner`); the projection helper maps it.
                let unchanged =
                    crate::sql::union_key_output_name(&source.to_spec(), key).is_some();
                if !unchanged {
                    return Err(report!(
                        "union all view {}: key column {key} is not projected \
                         unchanged from source {}",
                        view.view_id,
                        table.table_name
                    ));
                }
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

/// The distinct values touched by the changed source rows, including the
/// before image of a keyed update/delete.  This is the lookup contribution for
/// the keyed MV read.
fn union_distinct_affected_sql(
    view: &UnionDistinctView,
    keyed: bool,
    changed: &[bool],
) -> Result<String> {
    let columns = union_distinct_columns(view)?;
    let cols = quoted_list(&columns);
    let mut parts = Vec::new();
    for (index, source) in view.sources.iter().enumerate() {
        if source.table.primary_keys.is_empty() || !changed[index] {
            continue;
        }
        let table = &source.table;
        let change = change_column(table);
        let filter = source.filter.as_deref();
        let projection = union_distinct_projection_sql(&columns, &source.exprs);
        let new_table = format!("src{index}");
        parts.push(format!(
            "select {projection} from {new_table} where {}{}",
            source_delete_filter(&new_table, change),
            filter_clause(filter),
        ));
        if keyed {
            let old_table = format!("old{index}");
            parts.push(format!(
                "select {projection} from {old_table} where {}{}",
                source_delete_filter(&old_table, change),
                filter_clause(filter),
            ));
        }
    }
    if parts.is_empty() {
        return Err(report!("union-distinct view has no changed keyed sources"));
    }
    Ok(format!(
        "select distinct {cols} from ({}) affected",
        parts.join(" union all ")
    ))
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
            let context = refresh_session();
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
/// The MV range partitions a union refresh can touch.
///
/// A union row comes from one of the branches, so every branch must satisfy
/// `mv_partitions_for_source` and the restriction is the union of the changed
/// partitions.  Branches without changes contribute nothing.
fn union_mv_partitions(
    mv: &IvmTable,
    sources: &[UnionSource],
    windows: &[SourceWindow],
) -> Option<Vec<(String, Vec<String>)>> {
    let mut merged: Option<Vec<(String, Vec<String>)>> = None;
    for (source, window) in sources.iter().zip(windows) {
        if window.added_files.is_empty() {
            continue;
        }
        let restriction = super::mv_partitions_for_source(mv, &source.table, window)?;
        match &mut merged {
            None => merged = Some(restriction),
            Some(accumulated) => {
                if accumulated.len() != restriction.len() {
                    return None;
                }
                for (index, (column, values)) in restriction.into_iter().enumerate() {
                    if accumulated[index].0 != column {
                        return None;
                    }
                    for value in values {
                        if !accumulated[index].1.contains(&value) {
                            accumulated[index].1.push(value);
                        }
                    }
                }
            }
        }
    }
    merged
}

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
        .filter(|name| {
            Some(name.as_str()) != change_column
                && name != IVM_ROW_KINDS_COLUMN
                && name != IVM_EPOCH_COLUMN
        })
        .collect()
}

/// The DataFrame projection of one `UNION ALL` branch: the projected columns
/// plus the source index.
fn union_source_projection(
    context: &SessionContext,
    source: &UnionSource,
    change_column: Option<&str>,
    index: usize,
    source_column: &str,
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
    exprs.push(lit(index as i32).alias(source_column));
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

        let mut windows = Vec::new();
        for source in &view.sources {
            windows.push(
                self.collect_source_window_for_filter(
                    &view.view_id,
                    &source.table,
                    source.filter.as_deref(),
                )
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

        let context = refresh_session();
        let mut inserts = Vec::new();
        let mut changed = Vec::new();
        for (index, source) in view.sources.iter().enumerate() {
            let table = &source.table;
            let filter = source
                .filter
                .as_deref()
                .map(|filter| parse_filter(&context, &table.schema, filter))
                .transpose()?;
            let delta = self.groups_table_frame(
                &context,
                table,
                windows[index].added_files.clone(),
            )?;
            if keyed {
                // Probe the branch table with its own key names, then rename
                // the changed keys to the union's output names (a nested
                // branch may carry `__ivm_source` under `..._inner`).
                let key_exprs = table
                    .primary_keys
                    .iter()
                    .map(|column| col(column.as_str()))
                    .collect::<Vec<_>>();
                let output_key_exprs = table
                    .primary_keys
                    .iter()
                    .map(|column| {
                        let output =
                            crate::sql::union_key_output_name(&source.to_spec(), column)
                                .unwrap_or_else(|| column.clone());
                        col(column.as_str()).alias(output)
                    })
                    .collect::<Vec<_>>();
                let affected = delta.select(key_exprs)?.distinct()?;
                // Read only the current rows for this source's changed keys.
                let source_now = self
                    .lookup_source_by_join_keys(
                        &context,
                        table,
                        affected.clone(),
                        &table.primary_keys,
                        &table.primary_keys,
                        true,
                    )
                    .await?;
                let rows = match &filter {
                    Some(filter) => source_now.filter(filter.clone())?,
                    None => source_now,
                };
                inserts.push(rows.select(union_source_projection(
                    &context,
                    source,
                    None,
                    index,
                    &view.source_column,
                )?)?);
                changed.push(
                    affected
                        .select(output_key_exprs)?
                        .distinct()?
                        .with_column(view.source_column.as_str(), lit(index as i32))?,
                );
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
                        let retract = col(column).eq(lit("delete"));
                        when(retract, lit("delete")).otherwise(lit("insert"))?
                    }
                    None => lit("insert"),
                };
                let mut projection = union_source_projection(
                    &context,
                    source,
                    None,
                    index,
                    &view.source_column,
                )?;
                projection.push(kinds.alias(IVM_ROW_KINDS_COLUMN));
                inserts.push(rows.select(projection)?);
            }
        }

        let output_exprs = union_branch_columns(&view.sources[0], None)
            .iter()
            .map(|column| col(column.as_str()))
            .chain(std::iter::once(col(view.source_column.as_str())))
            .collect::<Vec<_>>();

        if keyed {
            let mut pair_names = vec![view.source_column.clone()];
            for key in &view.sources[0].table.primary_keys {
                let output =
                    crate::sql::union_key_output_name(&view.sources[0].to_spec(), key)
                        .unwrap_or_else(|| key.clone());
                if !pair_names.contains(&output) {
                    pair_names.push(output);
                }
            }
            let pair_refs = pair_names.iter().map(String::as_str).collect::<Vec<_>>();
            let pair_exprs = pair_names
                .iter()
                .map(|column| col(column.as_str()))
                .collect::<Vec<_>>();
            let changed = union_frames(changed)?;
            let mv = if view.mv.primary_keys == pair_names {
                self.lookup_source_by_join_keys(
                    &context,
                    &view.mv,
                    changed.clone(),
                    &pair_names,
                    &view.mv.primary_keys,
                    true,
                )
                .await?
            } else {
                match union_mv_partitions(&view.mv, &view.sources, &windows) {
                    Some(restriction) => {
                        self.current_table_frame_filtered_partitions(
                            &context,
                            &view.mv,
                            Vec::new(),
                            &restriction,
                        )
                        .await?
                    }
                    None => self.current_table_frame(&context, &view.mv)?,
                }
            };
            let already = mv
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(pair_exprs.clone())?
                .distinct()?;
            let inserts_schema = inserts.first().map(|frame| frame.schema().clone());
            let already_schema = already.schema().clone();
            let inserts = union_frames(inserts)?
                .join(already, JoinType::LeftAnti, &pair_refs, &pair_refs, None)
                .map_err(|error| {
                    report!(
                        "union {}: inserts {:?} already {:?} pair {pair_refs:?}: {error}",
                        view.view_id,
                        inserts_schema,
                        already_schema,
                    )
                })?
                .select(output_exprs.clone())?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let deletes = mv
                .join(changed, JoinType::LeftSemi, &pair_refs, &pair_refs, None)?
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
                .select(output_exprs)?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
            let mut sort_exprs = vec![column_expr(&view.source_column)];
            sort_exprs.extend(pair_names.iter().skip(1).map(|key| column_expr(key)));
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
            // The append-only frames already carry the MV column order (the
            // output columns, the source index, the row kind).
            commit_ids.extend(
                append_dataframe(
                    &view.mv,
                    &self.client,
                    union_frames(inserts)?.with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
                )
                .await?
                .commit_ids,
            );
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let mut baselines = Vec::new();
        for source in &view.sources {
            baselines.push(
                self.source_baseline_for_filter(&source.table, source.filter.as_deref())
                    .await?,
            );
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

        let context = refresh_session();
        let mut frames = Vec::new();
        for (index, source) in view.sources.iter().enumerate() {
            let table = &source.table;
            let rows = filter_deletes(
                self.groups_table_frame(
                    &context,
                    table,
                    baselines[index].groups.clone(),
                )?,
                change_column(table),
            )?;
            let rows = match &source.filter {
                Some(filter) => {
                    rows.filter(parse_filter(&context, &table.schema, filter)?)?
                }
                None => rows,
            };
            frames.push(rows.select(union_source_projection(
                &context,
                source,
                None,
                index,
                &view.source_column,
            )?)?);
        }
        let output_exprs = union_branch_columns(&view.sources[0], None)
            .iter()
            .map(|column| col(column.as_str()))
            .chain(std::iter::once(col(view.source_column.as_str())))
            .collect::<Vec<_>>();
        commit_ids.extend(
            append_dataframe(
                &view.mv,
                &self.client,
                union_frames(frames)?
                    .select(output_exprs)?
                    .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
            )
            .await?
            .commit_ids,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        let mut windows = Vec::new();
        for source in &view.sources {
            windows.push(
                self.collect_source_window_for_filter(
                    &view.view_id,
                    &source.table,
                    source.filter.as_deref(),
                )
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

        let context = refresh_session();
        for (index, source) in view.sources.iter().enumerate() {
            let table = &source.table;
            let delta = self.groups_table_frame(
                &context,
                table,
                windows[index].added_files.clone(),
            )?;
            if keyed {
                let contribution = delta
                    .clone()
                    .select(
                        table
                            .primary_keys
                            .iter()
                            .map(|key| col(key.as_str()))
                            .collect::<Vec<_>>(),
                    )?
                    .distinct()?;
                let before = self
                    .lookup_source_before_by_pks(
                        &context,
                        table,
                        &windows[index],
                        contribution,
                    )
                    .await?;
                context.register_table(format!("old{index}"), before.into_view())?;
            }
            context.register_table(format!("src{index}"), delta.into_view())?;
        }
        let columns = union_distinct_columns(view)?;
        if keyed && !columns.is_empty() && view.mv.primary_keys == columns {
            let changed = windows
                .iter()
                .map(|window| !window.added_files.is_empty())
                .collect::<Vec<_>>();
            let affected = context
                .sql(&union_distinct_affected_sql(view, keyed, &changed)?)
                .await?;
            let mv = self
                .lookup_source_by_join_keys(
                    &context,
                    &view.mv,
                    affected,
                    &columns,
                    &view.mv.primary_keys,
                    true,
                )
                .await?;
            context.register_table("mv", mv.into_view())?;
        } else {
            let mv = match union_mv_partitions(&view.mv, &view.sources, &windows) {
                Some(restriction) => {
                    self.current_table_frame_filtered_partitions(
                        &context,
                        &view.mv,
                        Vec::new(),
                        &restriction,
                    )
                    .await?
                }
                None => self.current_table_frame(&context, &view.mv)?,
            };
            context.register_table("mv", mv.into_view())?;
        }
        let sql = union_distinct_refresh_sql(view, keyed, epoch)?;
        let frame = context.sql(&sql).await?;
        commit_ids.extend(
            view.mv
                .append_stream(&self.client, frame.execute_stream().await?)
                .await?,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let mut baselines = Vec::new();
        for source in &view.sources {
            baselines.push(
                self.source_baseline_for_filter(&source.table, source.filter.as_deref())
                    .await?,
            );
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

        let context = refresh_session();
        for (index, source) in view.sources.iter().enumerate() {
            context.register_table(
                format!("src{index}"),
                Arc::new(self.table_provider_groups(
                    &source.table,
                    baselines[index].groups.clone(),
                )),
            )?;
        }
        let sql = union_distinct_rebuild_sql(view, epoch)?;
        let frame = context.sql(&sql).await?;
        commit_ids.extend(
            view.mv
                .append_stream(&self.client, frame.execute_stream().await?)
                .await?,
        );

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
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
