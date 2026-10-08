// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The multi-way join view: three to eight sources flattened into one chain.

use super::*;

/// One input of a multi-way inner join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinSource {
    /// The source table id.
    pub table_id: String,
    /// The side filter the contributing rows must satisfy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// One equality key pair of a multi-way join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinKey {
    /// The source index of the left column.
    pub left_source: usize,
    /// The left column name.
    pub left_column: String,
    /// The source index of the right column.
    pub right_source: usize,
    /// The right column name.
    pub right_column: String,
}

/// One output column of a multi-way join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinColumn {
    /// The source index the column comes from.
    pub source: usize,
    /// The source column name.
    pub column: String,
    /// The materialized view column name.
    pub name: String,
}

/// One non-equality condition between two sources of a multi-way join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinCondition {
    /// The source index of the left column.
    pub left_source: usize,
    /// The left column name.
    pub left_column: String,
    /// The source index of the right column.
    pub right_source: usize,
    /// The right column name.
    pub right_column: String,
    /// The comparison operator.
    pub op: CompareOp,
}

/// The row identity column of a source in a multi-way join output.
fn multi_join_pk_alias(source: usize, key: &str) -> String {
    format!("__pk{source}_{key}")
}

/// The primary keys of a multi-way join output, in schema order.
pub fn multi_join_primary_keys(primary_keys: &[Vec<String>]) -> Vec<String> {
    primary_keys
        .iter()
        .enumerate()
        .flat_map(|(source, keys)| {
            keys.iter().map(move |key| multi_join_pk_alias(source, key))
        })
        .collect()
}

/// The output fields of a multi-way join.
fn multi_join_output_fields(
    schemas: &[SchemaRef],
    columns: &[MultiJoinColumn],
) -> Result<Vec<Arc<Field>>> {
    let mut names = std::collections::HashSet::new();
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        if !names.insert(column.name.as_str()) {
            return Err(report!(
                "multi join output column {} is materialized twice",
                column.name
            ));
        }
        let schema = schemas.get(column.source).ok_or_else(|| {
            report!(
                "multi join output column {} references source {}",
                column.name,
                column.source
            )
        })?;
        let field = schema.field_with_name(&column.column).map_err(|_| {
            report!(
                "multi join output column {} is not in source {}",
                column.column,
                column.source
            )
        })?;
        fields.push(Arc::new(Field::new(
            column.name.clone(),
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    Ok(fields)
}

/// The schema of a keyed multi-way join materialized view: the output columns,
/// every source's row identity, the row kind and the epoch.
pub fn multi_join_mv_schema_for(
    schemas: &[SchemaRef],
    primary_keys: &[Vec<String>],
    columns: &[MultiJoinColumn],
) -> Result<SchemaRef> {
    if schemas.len() != primary_keys.len() {
        return Err(report!("multi join needs one primary key list per source"));
    }
    let mut fields = multi_join_output_fields(schemas, columns)?;
    for (source, keys) in primary_keys.iter().enumerate() {
        let schema = &schemas[source];
        for key in keys {
            let field = schema.field_with_name(key)?;
            fields.push(Arc::new(Field::new(
                multi_join_pk_alias(source, key),
                field.data_type().clone(),
                false,
            )));
        }
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

/// The schema of an append-only multi-way join materialized view: the output
/// columns and the epoch.
pub fn multi_join_append_schema_for(
    schemas: &[SchemaRef],
    columns: &[MultiJoinColumn],
) -> Result<SchemaRef> {
    let mut fields = multi_join_output_fields(schemas, columns)?;
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// An inner equi-join of three or more sources maintained as one wide MV.
#[derive(Debug, Clone)]
pub struct MultiJoinView {
    /// The view id.
    pub view_id: String,
    /// The joined sources, in chain order.
    pub sources: Vec<IvmTable>,
    /// The side filters, parallel to [`Self::sources`].
    pub filters: Vec<Option<String>>,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The equality key pairs.
    pub keys: Vec<MultiJoinKey>,
    /// The output columns.
    pub columns: Vec<MultiJoinColumn>,
    /// The non-equality pair conditions.
    pub conditions: Vec<MultiJoinCondition>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl MultiJoinView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        sources: Vec<IvmTable>,
        mv: IvmTable,
        keys: Vec<MultiJoinKey>,
        columns: Vec<MultiJoinColumn>,
    ) -> Self {
        let filters = vec![None; sources.len()];
        Self {
            view_id: view_id.into(),
            sources,
            filters,
            mv,
            keys,
            columns,
            conditions: Vec::new(),
            refresh_interval_ms: 0,
        }
    }

    /// The side filters, parallel to the sources.
    pub fn with_filters(mut self, filters: Vec<Option<String>>) -> Self {
        self.filters = filters;
        self
    }

    /// The non-equality pair conditions.
    pub fn with_conditions(mut self, conditions: Vec<MultiJoinCondition>) -> Self {
        self.conditions = conditions;
        self
    }

    /// Whether the sources take the keyed (upsert) path.
    pub fn keyed(&self) -> bool {
        !self.sources[0].primary_keys.is_empty()
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::MultiJoin {
            view_id: self.view_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            sources: self
                .sources
                .iter()
                .zip(self.filters.iter())
                .map(|(source, filter)| MultiJoinSource {
                    table_id: source.table_id.clone(),
                    filter: filter.clone(),
                })
                .collect(),
            keys: self.keys.clone(),
            columns: self.columns.clone(),
            conditions: self.conditions.clone(),
        }
    }
}

/// The intermediate column name of a source column in a multi-way join.
fn multi_join_column_alias(source: usize, column: &str) -> String {
    format!("__c{source}_{column}")
}

/// Join the source frames of a multi-way inner join, apply the pair
/// conditions and project the output (and, when keyed, every source's row
/// identity).
#[allow(clippy::too_many_arguments)]
fn multi_join_frame(
    context: &SessionContext,
    frames: Vec<DataFrame>,
    schemas: &[SchemaRef],
    primary_keys: &[Vec<String>],
    keys: &[MultiJoinKey],
    conditions: &[MultiJoinCondition],
    columns: &[MultiJoinColumn],
    keyed: bool,
) -> Result<DataFrame> {
    let source_count = schemas.len();
    let mut needed: Vec<Vec<String>> = vec![Vec::new(); source_count];
    let mut push = |source: usize, column: &str| {
        if !needed[source].iter().any(|existing| existing == column) {
            needed[source].push(column.to_string());
        }
    };

    for key in keys {
        push(key.left_source, &key.left_column);
        push(key.right_source, &key.right_column);
    }
    for condition in conditions {
        push(condition.left_source, &condition.left_column);
        push(condition.right_source, &condition.right_column);
    }
    for column in columns {
        push(column.source, &column.column);
    }
    if keyed {
        for (source, keys) in primary_keys.iter().enumerate() {
            for key in keys {
                push(source, key);
            }
        }
    }

    let mut projected = Vec::with_capacity(source_count);
    for (source, frame) in frames.into_iter().enumerate() {
        let expressions = needed[source]
            .iter()
            .map(|column| {
                col(column.as_str()).alias(multi_join_column_alias(source, column))
            })
            .collect::<Vec<_>>();
        projected.push(frame.select(expressions)?);
    }

    let mut current = projected
        .drain(..1)
        .next()
        .ok_or_else(|| report!("multi join view has no sources"))?;
    for source in 1..source_count {
        let right = projected
            .drain(..1)
            .next()
            .ok_or_else(|| report!("multi join view has too few sources"))?;
        let mut left_names = Vec::new();
        let mut right_names = Vec::new();
        for key in keys.iter().filter(|key| key.right_source == source) {
            left_names.push(multi_join_column_alias(key.left_source, &key.left_column));
            right_names.push(multi_join_column_alias(source, &key.right_column));
        }
        if left_names.is_empty() {
            // A keyless step is a cross join.
            let plan = datafusion::logical_expr::LogicalPlanBuilder::new(
                current.logical_plan().clone(),
            )
            .cross_join(right.logical_plan().clone())?
            .build()?;
            current = DataFrame::new(context.state(), plan);
            continue;
        }
        let left_refs = left_names.iter().map(String::as_str).collect::<Vec<_>>();
        let right_refs = right_names.iter().map(String::as_str).collect::<Vec<_>>();
        current = current.join(right, JoinType::Inner, &left_refs, &right_refs, None)?;
    }

    if !conditions.is_empty() {
        let clauses = conditions
            .iter()
            .map(|condition| {
                format!(
                    "{} {} {}",
                    quote_ident(&multi_join_column_alias(
                        condition.left_source,
                        &condition.left_column
                    )),
                    crate::sql::compare_op_sql(condition.op),
                    quote_ident(&multi_join_column_alias(
                        condition.right_source,
                        &condition.right_column
                    ))
                )
            })
            .collect::<Vec<_>>();
        current = apply_pair_filter(current, Some(&clauses.join(" AND ")))?;
    }

    let mut output = Vec::with_capacity(columns.len() + primary_keys.len());
    for column in columns {
        output.push(
            col(multi_join_column_alias(column.source, &column.column).as_str())
                .alias(column.name.as_str()),
        );
    }
    if keyed {
        for (source, keys) in primary_keys.iter().enumerate() {
            for key in keys {
                output.push(
                    col(multi_join_column_alias(source, key).as_str())
                        .alias(multi_join_pk_alias(source, key).as_str()),
                );
            }
        }
    }
    Ok(current.select(output)?)
}

/// Validate that a multi-way join view can be maintained.
fn validate_multi_join_view(view: &MultiJoinView) -> Result<()> {
    let source_count = view.sources.len();
    if source_count < 3 {
        return Err(report!(
            "multi join view {} needs at least three sources",
            view.view_id
        ));
    }
    if source_count > 8 {
        return Err(report!(
            "multi join view {} supports at most eight sources",
            view.view_id
        ));
    }
    if view.filters.len() != source_count {
        return Err(report!(
            "multi join view {}: one side filter per source is required",
            view.view_id
        ));
    }
    let context = SessionContext::new();
    let keyed = view.keyed();
    for (index, source) in view.sources.iter().enumerate() {
        if keyed != !source.primary_keys.is_empty() {
            return Err(report!(
                "multi join view {}: the sources must be all keyed or all append-only",
                view.view_id
            ));
        }
        if let Some(filter) = &view.filters[index] {
            parse_filter(&context, &source.schema, filter)?;
        }
        if keyed {
            for key in &source.primary_keys {
                let field = source.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "multi join view {}: key column {key} is not in source {index}",
                        view.view_id
                    )
                })?;
                if field.is_nullable() {
                    return Err(report!(
                        "multi join view {}: key column {key} of source {index} must be non-nullable",
                        view.view_id
                    ));
                }
            }
        }
    }
    // A chain without keys is a cross join of every source.
    for key in &view.keys {
        if key.left_source >= source_count
            || key.right_source >= source_count
            || key.left_source >= key.right_source
        {
            return Err(report!(
                "multi join view {}: join keys must connect an earlier source to a later one",
                view.view_id
            ));
        }
        let left = view.sources[key.left_source]
            .schema
            .field_with_name(&key.left_column)
            .map_err(|_| {
                report!(
                    "multi join view {}: column {} is not in source {}",
                    view.view_id,
                    key.left_column,
                    key.left_source
                )
            })?;
        let right = view.sources[key.right_source]
            .schema
            .field_with_name(&key.right_column)
            .map_err(|_| {
                report!(
                    "multi join view {}: column {} is not in source {}",
                    view.view_id,
                    key.right_column,
                    key.right_source
                )
            })?;
        if left.data_type() != right.data_type() {
            return Err(report!(
                "multi join view {}: join key {} has different types on its sources",
                view.view_id,
                key.left_column
            ));
        }
    }
    // A source without a key pair is cross joined (the chain's `FROM a, b`
    // and mixed keyless steps).
    for condition in &view.conditions {
        for (source, column) in [
            (condition.left_source, &condition.left_column),
            (condition.right_source, &condition.right_column),
        ] {
            if source >= source_count
                || view.sources[source].schema.field_with_name(column).is_err()
            {
                return Err(report!(
                    "multi join view {}: condition column {column} is not in source {source}",
                    view.view_id
                ));
            }
        }
    }
    if view.columns.is_empty() {
        return Err(report!(
            "multi join view {} needs at least one output column",
            view.view_id
        ));
    }
    let mut names = std::collections::HashSet::new();
    for column in &view.columns {
        if column.source >= source_count
            || view.sources[column.source]
                .schema
                .field_with_name(&column.column)
                .is_err()
        {
            return Err(report!(
                "multi join view {}: output column {} is not in source {}",
                view.view_id,
                column.column,
                column.source
            ));
        }
        if !names.insert(column.name.as_str()) {
            return Err(report!(
                "multi join view {}: output column {} is materialized twice",
                view.view_id,
                column.name
            ));
        }
    }
    Ok(())
}

impl IvmRuntime {
    /// Persist a multi-way join view spec (idempotent).
    pub async fn register_multi_join_view(&self, view: &MultiJoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a multi-way inner join view.
    ///
    /// A keyed refresh recomputes the current matches of the rows whose
    /// identities changed on any source and rewrites their tuples; an
    /// append-only refresh runs the inclusion-exclusion decomposition over the
    /// sources that changed in this window (one term per non-empty subset).
    pub async fn refresh_multi_join(&self, view: &MultiJoinView) -> Result<Option<i64>> {
        self.register_multi_join_view(view).await?;
        validate_multi_join_view(view)?;
        let keyed = view.keyed();
        for source in &view.sources {
            self.ensure_unpartitioned(source).await?;
            if !keyed {
                ensure_append_only(source, &view.view_id)?;
            }
        }

        let mut windows = Vec::with_capacity(view.sources.len());
        for source in &view.sources {
            windows.push(self.collect_source_window(&view.view_id, source).await?);
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
                for window in windows {
                    self.advance_cursors(&view.view_id, window.cursors).await?;
                }
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();
        let context = SessionContext::new();
        let schemas = view
            .sources
            .iter()
            .map(|source| source.schema.clone())
            .collect::<Vec<_>>();
        let primary_keys = view
            .sources
            .iter()
            .map(|source| source.primary_keys.clone())
            .collect::<Vec<_>>();

        if keyed {
            let mut current = Vec::with_capacity(view.sources.len());
            let mut changed: Vec<Option<DataFrame>> =
                Vec::with_capacity(view.sources.len());
            let mut affected: Vec<Option<DataFrame>> =
                Vec::with_capacity(view.sources.len());
            for (index, (source, window)) in view.sources.iter().zip(&windows).enumerate()
            {
                let now = apply_side_filter(
                    &context,
                    filter_deletes(
                        dataframe(
                            &context,
                            source.read_current(&self.client).await?,
                            &source.schema,
                        )?,
                        change_column(source),
                    )?,
                    &source.schema,
                    view.filters[index].as_deref(),
                )?;
                if window.added_files.is_empty() {
                    current.push(now);
                    changed.push(None);
                    affected.push(None);
                    continue;
                }
                let delta = dataframe(
                    &context,
                    source.read_files(window.added_files.clone()).await?,
                    &source.schema,
                )?;
                let key_names = source
                    .primary_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let key_exprs = source
                    .primary_keys
                    .iter()
                    .map(|key| col(key.as_str()))
                    .collect::<Vec<_>>();
                let delta_keys = delta.clone().select(key_exprs.clone())?.distinct()?;
                changed.push(Some(now.clone().join(
                    delta_keys,
                    JoinType::LeftSemi,
                    &key_names,
                    &key_names,
                    None,
                )?));
                affected.push(Some(delta.select(key_exprs)?.distinct()?));
                current.push(now);
            }

            // The current matches of the changed rows, one term per changed
            // source and deduplicated across them.
            let mut terms = Vec::new();
            for index in 0..view.sources.len() {
                let Some(changed_rows) = changed[index].clone() else {
                    continue;
                };
                let mut frames = current.clone();
                frames[index] = changed_rows;
                terms.push(multi_join_frame(
                    &context,
                    frames,
                    &schemas,
                    &primary_keys,
                    &view.keys,
                    &view.conditions,
                    &view.columns,
                    true,
                )?);
            }
            let mut current_rows = terms
                .pop()
                .ok_or_else(|| report!("multi join view has no changed sources"))?;
            for term in terms {
                current_rows = current_rows.union(term)?;
            }
            let current_rows = current_rows.distinct()?;

            let identity_names = multi_join_primary_keys(&primary_keys);
            let identity_refs = identity_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let identity_exprs = identity_names
                .iter()
                .map(|name| col(name.as_str()))
                .collect::<Vec<_>>();
            let mv = dataframe(
                &context,
                view.mv.read_current(&self.client).await?,
                &view.mv.schema,
            )?;
            let already = mv
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(identity_exprs.clone())?
                .distinct()?;
            let inserts = current_rows
                .join(
                    already,
                    JoinType::LeftAnti,
                    &identity_refs,
                    &identity_refs,
                    None,
                )?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

            // Retract the previous tuples of every changed row; this epoch's
            // writes are kept so a replay stays a no-op.
            let active = mv.filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?;
            let mut deletes: Option<DataFrame> = None;
            for (index, affected_rows) in affected.into_iter().enumerate() {
                let Some(affected_rows) = affected_rows else {
                    continue;
                };
                let alias_names = primary_keys[index]
                    .iter()
                    .map(|key| multi_join_pk_alias(index, key))
                    .collect::<Vec<_>>();
                let alias_refs =
                    alias_names.iter().map(String::as_str).collect::<Vec<_>>();
                let key_refs = primary_keys[index]
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let matching = active.clone().join(
                    affected_rows,
                    JoinType::LeftSemi,
                    &alias_refs,
                    &key_refs,
                    None,
                )?;
                deletes = Some(match deletes {
                    Some(existing) => existing.union(matching)?,
                    None => matching,
                });
            }
            let mut mv_columns = view
                .columns
                .iter()
                .map(|column| col(column.name.as_str()))
                .collect::<Vec<_>>();
            mv_columns.extend(identity_exprs.clone());
            let mut sort_exprs = identity_exprs;
            sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
            let rows = match deletes {
                Some(deletes) => inserts
                    .union(
                        deletes
                            .distinct()?
                            .select(mv_columns)?
                            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
                    )?
                    .sort_by(sort_exprs)?,
                None => inserts,
            };
            for batch in rows.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
        } else {
            let mut before = Vec::with_capacity(view.sources.len());
            let mut delta = Vec::with_capacity(view.sources.len());
            for (source, window) in view.sources.iter().zip(&windows) {
                before.push(
                    source
                        .read_as_of(&self.client, window.before_timestamp)
                        .await?,
                );
                delta.push(source.read_files(window.added_files.clone()).await?);
            }
            let changed_sources = (0..view.sources.len())
                .filter(|index| !delta[*index].is_empty())
                .collect::<Vec<_>>();
            let mut terms = Vec::new();
            for subset in 1usize..(1usize << changed_sources.len()) {
                let mut selected = vec![false; view.sources.len()];
                for (bit, index) in changed_sources.iter().enumerate() {
                    if subset & (1 << bit) != 0 {
                        selected[*index] = true;
                    }
                }
                let mut frames = Vec::with_capacity(view.sources.len());
                for index in 0..view.sources.len() {
                    let batches = if selected[index] {
                        delta[index].clone()
                    } else {
                        before[index].clone()
                    };
                    frames.push(filtered_frame(
                        &context,
                        batches,
                        &schemas[index],
                        view.filters[index].as_deref(),
                    )?);
                }
                terms.push(multi_join_frame(
                    &context,
                    frames,
                    &schemas,
                    &primary_keys,
                    &view.keys,
                    &view.conditions,
                    &view.columns,
                    false,
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
                            .extend(view.mv.append_batch(&self.client, batch).await?);
                    }
                }
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for window in windows {
            self.advance_cursors(&view.view_id, window.cursors).await?;
        }
        Ok(Some(epoch))
    }

    /// Rebuild a multi-way join view from the full state of its sources.
    pub async fn rebuild_multi_join(&self, view: &MultiJoinView) -> Result<i64> {
        self.register_multi_join_view(view).await?;
        validate_multi_join_view(view)?;
        let keyed = view.keyed();
        for source in &view.sources {
            self.ensure_unpartitioned(source).await?;
            if !keyed {
                ensure_append_only(source, &view.view_id)?;
            }
        }

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let mut baselines = Vec::with_capacity(view.sources.len());
        for source in &view.sources {
            baselines.push(self.source_baseline(source).await?);
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
                for baseline in baselines {
                    self.advance_cursors(&view.view_id, baseline.cursors)
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

        if !baselines.iter().any(|baseline| baseline.batches.is_empty()) {
            let context = SessionContext::new();
            let schemas = view
                .sources
                .iter()
                .map(|source| source.schema.clone())
                .collect::<Vec<_>>();
            let primary_keys = view
                .sources
                .iter()
                .map(|source| source.primary_keys.clone())
                .collect::<Vec<_>>();
            let mut frames = Vec::with_capacity(view.sources.len());
            for (index, (source, baseline)) in
                view.sources.iter().zip(&baselines).enumerate()
            {
                let frame =
                    dataframe(&context, baseline.batches.clone(), &source.schema)?;
                let frame = if keyed {
                    filter_deletes(frame, change_column(source))?
                } else {
                    frame
                };
                frames.push(apply_side_filter(
                    &context,
                    frame,
                    &source.schema,
                    view.filters[index].as_deref(),
                )?);
            }
            let joined = multi_join_frame(
                &context,
                frames,
                &schemas,
                &primary_keys,
                &view.keys,
                &view.conditions,
                &view.columns,
                keyed,
            )?;
            let joined = if keyed {
                joined
                    .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            } else {
                joined.with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            };
            for batch in joined.collect().await? {
                if batch.num_rows() > 0 {
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for baseline in baselines {
            self.advance_cursors(&view.view_id, baseline.cursors)
                .await?;
        }
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }
}
