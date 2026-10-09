// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The left-deep lookup chain view: a base table joined to keyed 1:1 lookups.

use super::*;

/// The typed view of a left-deep chain of keyed 1:1 lookups over a base
/// table.
#[derive(Debug, Clone)]
pub struct LookupChainView {
    /// The view id.
    pub view_id: String,
    /// The chain sources; index 0 is the base, the rest follow the steps.
    pub sources: Vec<(IvmTable, Option<String>)>,
    /// The join steps in chain order; step `k` joins source `k + 1`.
    pub steps: Vec<LookupChainStep>,
    /// The materialized columns.
    pub output_columns: Vec<LookupChainColumn>,
    /// The materialized view table, created from
    /// [`lookup_chain_mv_schema_for`].
    pub mv: IvmTable,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl LookupChainView {
    /// A chain view with no filters.
    pub fn new(
        view_id: impl Into<String>,
        sources: Vec<(IvmTable, Option<String>)>,
        steps: Vec<LookupChainStep>,
        output_columns: Vec<LookupChainColumn>,
        mv: IvmTable,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            sources,
            steps,
            output_columns,
            mv,
            refresh_interval_ms: 0,
        }
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::LookupChain {
            view_id: self.view_id.clone(),
            sources: self
                .sources
                .iter()
                .map(|(table, filter)| LookupChainSource {
                    table_id: table.table_id.clone(),
                    filter: filter.clone(),
                })
                .collect(),
            steps: self.steps.clone(),
            output_columns: self.output_columns.clone(),
            mv_table_id: self.mv.table_id.clone(),
        }
    }
}

/// The schema of a [`LookupChainView`] materialized view: the output columns,
/// the row kind and the epoch.  A column of a step is nullable when any step
/// up to it is a left join.
pub fn lookup_chain_mv_schema_for(
    source_schemas: &[SchemaRef],
    steps: &[LookupChainStep],
    output_columns: &[LookupChainColumn],
) -> Result<SchemaRef> {
    if source_schemas.len() != steps.len() + 1 || steps.is_empty() {
        return Err(report!(
            "a lookup chain needs one base and at least one step"
        ));
    }
    let mut fields: Vec<arrow_schema::FieldRef> = Vec::new();
    let mut names = std::collections::HashSet::new();
    for column in output_columns {
        let schema = source_schemas.get(column.source).ok_or_else(|| {
            report!(
                "lookup chain column {} refers to a missing source",
                column.name
            )
        })?;
        let field = schema.field_with_name(&column.column).map_err(|_| {
            report!("lookup chain column {} is not in its source", column.column)
        })?;
        if !names.insert(column.name.as_str()) {
            return Err(report!(
                "lookup chain column {} is materialized twice",
                column.name
            ));
        }
        let nullable = field.is_nullable()
            || (column.source > 0 && steps[..column.source].iter().any(|step| step.left));
        fields.push(Arc::new(Field::new(
            column.name.clone(),
            field.data_type().clone(),
            nullable,
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

/// The alias of a step column in the replay frame, so the joined sources
/// cannot collide with each other.
fn lookup_chain_alias(source: usize, column: &str) -> String {
    format!("__ivm_chain_{source}_{column}")
}

/// The source a step key comes from (0 is the base).
fn step_key_source(step: &LookupChainStep, index: usize) -> usize {
    step.key_sources.get(index).copied().unwrap_or(0)
}

/// The name of a chain column in the replay frame.
fn chain_column_name(source: usize, column: &str) -> String {
    if source == 0 {
        column.to_string()
    } else {
        lookup_chain_alias(source, column)
    }
}

/// The columns of earlier sources that later steps join on.
fn lookup_chain_referenced(view: &LookupChainView, source: usize) -> Vec<String> {
    let mut columns = Vec::new();
    for step in &view.steps {
        if step.source <= source {
            continue;
        }
        for (index, key) in step.keys.iter().enumerate() {
            if step_key_source(step, index) == source && !columns.contains(key) {
                columns.push(key.clone());
            }
        }
    }
    columns
}

/// Replay the first `steps` chain steps over `frame` (the base rows).
fn lookup_chain_replay(
    view: &LookupChainView,
    frames: &[DataFrame],
    steps: usize,
    mut frame: DataFrame,
) -> Result<DataFrame> {
    for (index, step) in view.steps.iter().enumerate().take(steps) {
        let (_, step_frame) = lookup_chain_step_frame(view, index, &frames[index + 1])?;
        let right_keys = step
            .right_keys
            .iter()
            .map(|key| lookup_chain_alias(index + 1, key))
            .collect::<Vec<_>>();
        let left_keys = step
            .keys
            .iter()
            .enumerate()
            .map(|(key, column)| chain_column_name(step_key_source(step, key), column))
            .collect::<Vec<_>>();
        frame = frame.join(
            step_frame,
            if step.left {
                JoinType::Left
            } else {
                JoinType::Inner
            },
            &left_keys.iter().map(String::as_str).collect::<Vec<_>>(),
            &right_keys.iter().map(String::as_str).collect::<Vec<_>>(),
            None,
        )?;
    }
    Ok(frame)
}

/// The payload columns a source contributes to the view.
fn lookup_chain_payload(view: &LookupChainView, source: usize) -> Vec<String> {
    let mut columns = Vec::new();
    for column in &view.output_columns {
        if column.source == source && !columns.contains(&column.column) {
            columns.push(column.column.clone());
        }
    }
    columns
}

/// Validate that a lookup chain can be maintained.
fn validate_lookup_chain_view(view: &LookupChainView) -> Result<()> {
    if view.steps.is_empty() || view.sources.len() != view.steps.len() + 1 {
        return Err(report!(
            "lookup chain view {} needs a base and one step per source",
            view.view_id
        ));
    }
    let (base, _) = &view.sources[0];
    if base.primary_keys.is_empty() {
        return Err(report!(
            "lookup chain view {} needs a keyed base",
            view.view_id
        ));
    }
    for key in &base.primary_keys {
        let field = base.schema.field_with_name(key).map_err(|_| {
            report!(
                "lookup chain view {}: base key {key} is not in the base",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "lookup chain view {}: base key {key} must be non-nullable",
                view.view_id
            ));
        }
        if !view.output_columns.iter().any(|column| {
            column.source == 0 && &column.column == key && &column.name == key
        }) {
            return Err(report!(
                "lookup chain view {}: output must keep the base key {key}",
                view.view_id
            ));
        }
    }
    for (index, step) in view.steps.iter().enumerate() {
        if step.source != index + 1 {
            return Err(report!(
                "lookup chain view {}: step {} joins source {}",
                view.view_id,
                index,
                step.source
            ));
        }
        let (table, _) = &view.sources[index + 1];
        if table.primary_keys.is_empty() {
            return Err(report!(
                "lookup chain view {}: step source {} needs a primary key",
                view.view_id,
                table.table_name
            ));
        }
        if step.keys.is_empty() || step.keys.len() != step.right_keys.len() {
            return Err(report!(
                "lookup chain view {}: step {} needs one key per column",
                view.view_id,
                index
            ));
        }
        let mut pks = table.primary_keys.clone();
        pks.sort();
        pks.dedup();
        let mut keys = step.right_keys.clone();
        keys.sort();
        keys.dedup();
        if pks != keys {
            return Err(report!(
                "lookup chain view {}: step source {} must be keyed by its join keys",
                view.view_id,
                table.table_name
            ));
        }
        for (key_index, key) in step.keys.iter().enumerate() {
            let source = step_key_source(step, key_index);
            if source > index {
                return Err(report!(
                    "lookup chain view {}: join key {key} must come from an earlier source",
                    view.view_id
                ));
            }
            let (left_table, _) = &view.sources[source];
            let left_field = left_table.schema.field_with_name(key).map_err(|_| {
                report!(
                    "lookup chain view {}: join key {key} is not in {}",
                    view.view_id,
                    left_table.table_name
                )
            })?;
            let right_key = &step.right_keys[key_index];
            let right_field = table.schema.field_with_name(right_key).map_err(|_| {
                report!(
                    "lookup chain view {}: join key {right_key} is not in {}",
                    view.view_id,
                    table.table_name
                )
            })?;
            if left_field.data_type() != right_field.data_type() {
                return Err(report!(
                    "lookup chain view {}: join key {right_key} has different types",
                    view.view_id
                ));
            }
        }
    }
    let context = SessionContext::new();
    for (table, filter) in &view.sources {
        if let Some(filter) = filter {
            parse_filter(&context, &table.schema, filter)?;
        }
    }
    Ok(())
}

/// The columns of one source a refresh must read.
fn lookup_chain_source_columns(
    view: &LookupChainView,
    index: usize,
    context: &SessionContext,
) -> Result<Vec<String>> {
    let (table, filter) = &view.sources[index];
    let mut columns = lookup_chain_payload(view, index);
    for column in lookup_chain_referenced(view, index) {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    if index == 0 {
        for key in &table.primary_keys {
            if !columns.contains(key) {
                columns.push(key.clone());
            }
        }
        for step in &view.steps {
            for key in &step.keys {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    } else {
        for key in &view.steps[index - 1].right_keys {
            if !columns.contains(key) {
                columns.push(key.clone());
            }
        }
    }
    if let Some(filter) = filter {
        for column in filter_columns(context, &table.schema, filter)? {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
    }
    if let Some(change) = change_column(table)
        && !columns.iter().any(|column| column == change)
    {
        columns.push(change.to_string());
    }
    Ok(columns)
}

/// The replay frame of one step: the key and payload columns aliased so the
/// chain joins cannot collide.
fn lookup_chain_step_frame(
    view: &LookupChainView,
    index: usize,
    now: &DataFrame,
) -> Result<(String, DataFrame)> {
    let step = &view.steps[index];
    let mut select = Vec::new();
    for key in &step.right_keys {
        select.push(col(key.as_str()).alias(lookup_chain_alias(index + 1, key)));
    }
    let mut columns = lookup_chain_payload(view, index + 1);
    for column in lookup_chain_referenced(view, index + 1) {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    for column in columns {
        if !step.right_keys.contains(&column) {
            select
                .push(col(column.as_str()).alias(lookup_chain_alias(index + 1, &column)));
        }
    }
    let mut keys = step
        .right_keys
        .iter()
        .map(|key| lookup_chain_alias(index + 1, key))
        .collect::<Vec<_>>();
    keys.sort();
    Ok((keys.join(","), now.clone().select(select)?))
}

impl IvmRuntime {
    /// Persist a lookup chain view spec (idempotent).
    pub async fn register_lookup_chain_view(&self, view: &LookupChainView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a lookup chain: the affected base rows are replayed through
    /// the chain.
    pub async fn refresh_lookup_chain(
        &self,
        view: &LookupChainView,
    ) -> Result<Option<i64>> {
        self.register_lookup_chain_view(view).await?;
        validate_lookup_chain_view(view)?;

        let mut windows = Vec::with_capacity(view.sources.len());
        for (table, _) in &view.sources {
            windows.push(self.collect_source_window(&view.view_id, table).await?);
        }
        let changed = windows
            .iter()
            .map(|window| !window.added_files.is_empty())
            .collect::<Vec<_>>();
        if !changed.iter().any(|changed| *changed) {
            return Ok(None);
        }
        let mut identity = Vec::new();
        for window in &windows {
            identity.extend(window.identity.iter().cloned());
        }
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
        let mut frames = Vec::with_capacity(view.sources.len());
        for (index, (table, filter)) in view.sources.iter().enumerate() {
            let columns = lookup_chain_source_columns(view, index, &context)?;
            let projection = project_schema(&table.schema, &columns)?;
            let delta = dataframe(
                &context,
                table
                    .read_files_projected(
                        windows[index].added_files.clone(),
                        Some(&projection),
                    )
                    .await?,
                &projection,
            )?;
            let before = if changed[index] {
                dataframe(
                    &context,
                    table
                        .read_as_of_projected(
                            &self.client,
                            windows[index].before_timestamp,
                            Some(&projection),
                        )
                        .await?,
                    &projection,
                )?
            } else {
                dataframe(&context, Vec::new(), &projection)?
            };
            let now = apply_side_filter(
                &context,
                filter_deletes(
                    dataframe(
                        &context,
                        table
                            .read_current_projected(&self.client, Some(&projection))
                            .await?,
                        &projection,
                    )?,
                    change_column(table),
                )?,
                &projection,
                filter.as_deref(),
            )?;
            frames.push((delta, before, now));
        }
        let mv = dataframe(
            &context,
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        let (base, _) = &view.sources[0];
        let base_key_names = base
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let base_key_exprs = base
            .primary_keys
            .iter()
            .map(|column| col(column.as_str()))
            .collect::<Vec<_>>();

        let now_frames = frames
            .iter()
            .map(|(_, _, now)| now.clone())
            .collect::<Vec<_>>();

        // The base rows the window can change: the base delta keys plus, for
        // every changed step source, the base rows matching the changed keys.
        let mut affected = frames[0]
            .0
            .clone()
            .select(base_key_exprs.clone())?
            .distinct()?;
        let mut every_base_row = false;
        for (index, step) in view.steps.iter().enumerate() {
            if !changed[index + 1] {
                continue;
            }
            // A change in the base or in an earlier step can move any key, so
            // every base row is re-evaluated.
            if (0..=index).any(|source| changed[source]) {
                every_base_row = true;
                break;
            }
            let (table, _) = &view.sources[index + 1];
            let (delta, before, _) = &frames[index + 1];
            let right_key_exprs = step
                .right_keys
                .iter()
                .map(|key| col(key.as_str()))
                .collect::<Vec<_>>();
            let changed_keys = if table.primary_keys.is_empty() {
                delta.clone().select(right_key_exprs)?
            } else {
                let pk_names = table
                    .primary_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let changed_pks = delta
                    .clone()
                    .select(
                        table
                            .primary_keys
                            .iter()
                            .map(|key| col(key.as_str()))
                            .collect::<Vec<_>>(),
                    )?
                    .distinct()?;
                before
                    .clone()
                    .join(changed_pks, JoinType::LeftSemi, &pk_names, &pk_names, None)?
                    .union(delta.clone())?
                    .distinct()?
                    .select(right_key_exprs)?
            }
            .distinct()?;
            // Match the changed keys against the prefix columns they refer
            // to; the prefix is replayed over the current (unchanged) states.
            let matching = (0..step.keys.len())
                .map(|key| format!("__ivm_changed_{key}"))
                .collect::<Vec<_>>();
            let mut prefix_select = base_key_exprs.clone();
            for (key, name) in step.keys.iter().zip(&matching) {
                let position = step
                    .keys
                    .iter()
                    .position(|column| column == key)
                    .unwrap_or(0);
                let source = step_key_source(step, position);
                prefix_select.push(
                    col(chain_column_name(source, key).as_str()).alias(name.as_str()),
                );
            }
            let prefix =
                lookup_chain_replay(view, &now_frames, index, frames[0].2.clone())?
                    .select(prefix_select)?;
            let changed_renamed = changed_keys.select(
                step.right_keys
                    .iter()
                    .zip(&matching)
                    .map(|(key, name)| col(key.as_str()).alias(name.as_str()))
                    .collect::<Vec<_>>(),
            )?;
            let matched = prefix
                .join(
                    changed_renamed,
                    JoinType::LeftSemi,
                    &matching.iter().map(String::as_str).collect::<Vec<_>>(),
                    &matching.iter().map(String::as_str).collect::<Vec<_>>(),
                    None,
                )?
                .select(base_key_exprs.clone())?
                .distinct()?;
            affected = affected.union(matched)?.distinct()?;
        }
        if every_base_row {
            // Every key the base or the view knows is re-evaluated; the view
            // side keeps the keys the base no longer has.
            let base_now_keys = frames[0]
                .2
                .clone()
                .select(base_key_exprs.clone())?
                .distinct()?;
            let mv_keys = mv.clone().select(base_key_exprs.clone())?.distinct()?;
            affected = affected.union(base_now_keys)?.union(mv_keys)?.distinct()?;
        }

        // Replay the chain over the current states for the affected rows.
        let frame = lookup_chain_replay(
            view,
            &now_frames,
            view.steps.len(),
            frames[0].2.clone().join(
                affected.clone(),
                JoinType::LeftSemi,
                &base_key_names,
                &base_key_names,
                None,
            )?,
        )?;
        let output_exprs = view
            .output_columns
            .iter()
            .map(|column| {
                let expr = if column.source == 0 {
                    col(column.column.as_str())
                } else {
                    col(lookup_chain_alias(column.source, &column.column).as_str())
                };
                expr.alias(column.name.as_str())
            })
            .collect::<Vec<_>>();
        let delete_exprs = view
            .output_columns
            .iter()
            .map(|column| col(column.name.as_str()))
            .collect::<Vec<_>>();
        let mv_epoch_keys = mv
            .clone()
            .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
            .select(base_key_exprs.clone())?
            .distinct()?;
        let inserts = frame
            .select(output_exprs)?
            .join(
                mv_epoch_keys,
                JoinType::LeftAnti,
                &base_key_names,
                &base_key_names,
                None,
            )?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let deletes = mv
            .join(
                affected,
                JoinType::LeftSemi,
                &base_key_names,
                &base_key_names,
                None,
            )?
            .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
            .select(delete_exprs)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let mut sort_exprs = view.sources[0]
            .0
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
        for window in windows {
            self.advance_cursors(&view.view_id, window.cursors).await?;
        }
        Ok(Some(epoch))
    }

    /// Rebuild a lookup chain from the current states of its sources.
    pub async fn rebuild_lookup_chain(&self, view: &LookupChainView) -> Result<i64> {
        self.register_lookup_chain_view(view).await?;
        validate_lookup_chain_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let mut baselines = Vec::with_capacity(view.sources.len());
        for (table, _) in &view.sources {
            baselines.push(self.source_baseline(table).await?);
        }
        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let mut to_versions = Vec::new();
        for baseline in &baselines {
            to_versions.extend(baseline.to_versions.iter().cloned());
        }
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

        let context = SessionContext::new();
        let mut frames = Vec::with_capacity(view.sources.len());
        for (table, filter) in &view.sources {
            let frame = apply_side_filter(
                &context,
                filter_deletes(
                    dataframe(
                        &context,
                        table.read_current(&self.client).await?,
                        &table.schema,
                    )?,
                    change_column(table),
                )?,
                &table.schema,
                filter.as_deref(),
            )?;
            frames.push(frame);
        }
        let frame =
            lookup_chain_replay(view, &frames, view.steps.len(), frames[0].clone())?;
        let output_exprs = view
            .output_columns
            .iter()
            .map(|column| {
                let expr = if column.source == 0 {
                    col(column.column.as_str())
                } else {
                    col(lookup_chain_alias(column.source, &column.column).as_str())
                };
                expr.alias(column.name.as_str())
            })
            .collect::<Vec<_>>();
        let rows = frame
            .select(output_exprs)?
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
