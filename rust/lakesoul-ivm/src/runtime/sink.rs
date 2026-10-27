// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The target projection sink.
//!
//! A sink reads a keyed maintenance MV and writes the user target table by
//! the target primary key.  The maintenance MV already emits a complete
//! `delete(old) + insert(new)` changelog, so the sink only projects and
//! orders that changelog; it does not keep a second copy of the target row.

use super::*;
use datafusion::logical_expr::Cast;

/// A target projection sink.
#[derive(Debug, Clone)]
pub struct SinkView {
    /// The user view id (the target table id).
    pub view_id: String,
    /// The maintenance state MV.
    pub source: IvmTable,
    /// The user target table.
    pub target: IvmTable,
    /// The target output columns, in target schema order.
    pub columns: Vec<SinkColumn>,
    /// The target primary keys.
    pub primary_keys: Vec<String>,
    /// The target range partition columns.
    pub range_partition_columns: Vec<String>,
    /// The definition identity of the source maintenance view.
    pub source_definition: String,
    /// The source view generation last consumed by this sink.
    pub source_generation: i64,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl SinkView {
    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Sink {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.target.table_id.clone(),
            columns: self.columns.clone(),
            primary_keys: self.primary_keys.clone(),
            range_partition_columns: self.range_partition_columns.clone(),
            source_definition: self.source_definition.clone(),
            source_generation: self.source_generation,
        }
    }

    /// The projection from the maintenance delta to the target schema.
    fn projection(&self, epoch: i64, rebuild: bool) -> Result<Vec<Expr>> {
        let mut exprs = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let target = self
                .target
                .schema
                .field_with_name(&column.name)
                .map_err(|_| report!("sink target is missing column {}", column.name))?;
            let source_name = column.source();
            let expr = if column.name == IVM_EPOCH_COLUMN {
                lit(epoch)
            } else if rebuild && column.name == IVM_ROW_KINDS_COLUMN {
                lit("insert")
            } else {
                let source =
                    self.source
                        .schema
                        .field_with_name(source_name)
                        .map_err(|_| {
                            report!(
                                "maintenance state is missing column {source_name} for target column {}",
                                column.name
                            )
                        })?;
                let column_expr = Expr::Column(Column::from_name(source_name));
                if source.data_type() == target.data_type() {
                    column_expr
                } else {
                    Expr::Cast(Cast::new(
                        Box::new(column_expr),
                        target.data_type().clone(),
                    ))
                }
            };
            exprs.push(expr.alias(column.name.clone()));
        }
        Ok(exprs)
    }
}

/// Register the sink's target table as its output state.
pub async fn register_sink_view(runtime: &IvmRuntime, view: &SinkView) -> Result<()> {
    runtime
        .register_state_tables(&view.view_id, &[(StateRole::Mv, &view.target)])
        .await?;
    let spec = serde_json::to_value(view.to_spec())?;
    runtime
        .metadata
        .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
        .await
}

/// Validate that a sink can write its target.
pub fn validate_sink_view(view: &SinkView) -> Result<()> {
    if view.columns.is_empty() {
        return Err(report!("sink view {} has no output columns", view.view_id));
    }
    if view.primary_keys != view.target.primary_keys {
        return Err(report!(
            "sink view {}: target primary key changed; re-run the statement",
            view.view_id
        ));
    }
    if view.range_partition_columns != view.target.range_partition_columns {
        return Err(report!(
            "sink view {}: target range partitions changed; re-run the statement",
            view.view_id
        ));
    }
    if view.columns.len() != view.target.schema.fields().len() {
        return Err(report!(
            "sink view {} has {} columns but target {} has {} fields",
            view.view_id,
            view.columns.len(),
            view.target.table_name,
            view.target.schema.fields().len()
        ));
    }
    for (column, field) in view.columns.iter().zip(view.target.schema.fields()) {
        let data_type = decode_data_type(&column.result)?;
        if column.name != field.name().as_str()
            || data_type != *field.data_type()
            || column.nullable != field.is_nullable()
        {
            return Err(report!(
                "sink view {}: target schema changed at column {}; re-run the statement",
                view.view_id,
                field.name()
            ));
        }
    }
    if view
        .target
        .schema
        .field_with_name(IVM_ROW_KINDS_COLUMN)
        .is_err()
        || view
            .target
            .schema
            .field_with_name(IVM_EPOCH_COLUMN)
            .is_err()
    {
        return Err(report!(
            "sink target {} needs the internal rowKinds/__ivm_epoch columns",
            view.target.table_name
        ));
    }
    for column in &view.columns {
        if column.name == IVM_EPOCH_COLUMN || column.name == IVM_ROW_KINDS_COLUMN {
            continue;
        }
        view.source
            .schema
            .field_with_name(column.source())
            .map_err(|_| {
                report!(
                    "maintenance state {} does not produce target column {}",
                    view.source.table_name,
                    column.name
                )
            })?;
    }
    if view.target.primary_keys.is_empty() && !view.source.primary_keys.is_empty() {
        return Err(report!(
            "sink target {} needs a primary key for a keyed maintenance state",
            view.target.table_name
        ));
    }
    for key in &view.target.primary_keys {
        let field = view.target.schema.field_with_name(key).map_err(|_| {
            report!(
                "sink target {}: primary key {key} is not in the schema",
                view.target.table_name
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "sink target {}: primary key {key} must be non-nullable",
                view.target.table_name
            ));
        }
    }
    Ok(())
}

impl IvmRuntime {
    /// Refresh a target projection sink from its maintenance state delta.
    pub async fn refresh_sink(&self, view: &SinkView) -> Result<Option<i64>> {
        register_sink_view(self, view).await?;
        validate_sink_view(view)?;

        let source_generation = self
            .metadata
            .view_generation(&view.source.table_id)
            .await
            .unwrap_or(0);
        if view.source_generation != source_generation {
            return self.rebuild_sink(view).await.map(Some);
        }

        let window = self
            .collect_source_window(&view.view_id, &view.source)
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }
        let record = match self
            .begin_window(&view.view_id, &window.identity, &view.target)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let context = refresh_session();
        let delta =
            self.unmerged_groups_table_frame(&context, &view.source, window.added_files)?;
        // Collapse an update's delete/insert pair when both target the same
        // partition: the insert alone is an upsert and cannot be ordered
        // incorrectly by a writer that folds one key inside a file.  A
        // partition move keeps the old partition's delete and the new
        // partition's insert.
        let source_name = |name: &str| {
            view.columns
                .iter()
                .find(|column| column.name == name)
                .map(|column| column.source().to_string())
                .unwrap_or_else(|| name.to_string())
        };
        let mut key_columns = view
            .target
            .primary_keys
            .iter()
            .map(|key| source_name(key))
            .collect::<Vec<_>>();
        for column in &view.target.range_partition_columns {
            let column = source_name(column);
            if !key_columns.contains(&column) {
                key_columns.push(column);
            }
        }
        let key_exprs = key_columns
            .iter()
            .map(|key| Expr::Column(Column::from_name(key.as_str())))
            .collect::<Vec<_>>();
        let key_names = key_columns.iter().map(String::as_str).collect::<Vec<_>>();
        let kinds = Expr::Column(Column::from_name(IVM_ROW_KINDS_COLUMN));
        let inserts = delta.clone().filter(kinds.clone().eq(lit("insert")))?;
        let insert_keys = inserts.clone().select(key_exprs)?.distinct()?;
        let deletes = delta.filter(kinds.eq(lit("delete")))?;
        let kept_deletes = deletes.join(
            insert_keys,
            JoinType::LeftAnti,
            &key_names,
            &key_names,
            None,
        )?;
        let delta = inserts.union(kept_deletes)?;

        let mut sort_exprs = view
            .target
            .primary_keys
            .iter()
            .map(|key| {
                Expr::Column(Column::from_name(source_name(key).as_str()))
                    .sort(true, false)
            })
            .collect::<Vec<_>>();
        sort_exprs
            .push(Expr::Column(Column::from_name(IVM_EPOCH_COLUMN)).sort(true, false));
        sort_exprs.push(
            Expr::Column(Column::from_name(IVM_ROW_KINDS_COLUMN)).sort(true, false),
        );
        let frame = delta
            .sort(sort_exprs)?
            .select(view.projection(epoch, false)?)?;
        let stream = frame.execute_stream().await?;
        drop(context);
        commit_ids.extend(
            view.target
                .append_stream_by_key(&self.client, stream)
                .await?,
        );

        let target_versions =
            output_partition_versions(&self.client, &view.target).await?;
        self.mark_epoch_committed(&record, &target_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a target projection sink from the current maintenance state.
    pub async fn rebuild_sink(&self, view: &SinkView) -> Result<i64> {
        register_sink_view(self, view).await?;
        validate_sink_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.target.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
        let target_versions_before =
            output_partition_versions(&self.client, &view.target).await?;
        let record = match self
            .metadata
            .begin_epoch(
                &view.view_id,
                &format!("rebuild:{generation}"),
                &baseline.to_versions,
                &target_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                self.advance_cursors(&view.view_id, baseline.cursors)
                    .await?;
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                let mut updated = view.clone();
                updated.source_generation = self
                    .metadata
                    .view_generation(&view.source.table_id)
                    .await
                    .unwrap_or(0);
                register_sink_view(self, &updated).await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let context = refresh_session();
        let frame = self.current_table_frame(&context, &view.source)?;
        let frame = frame.select(view.projection(epoch, true)?)?;
        let stream = frame.execute_stream().await?;
        drop(context);
        commit_ids.extend(view.target.append_stream(&self.client, stream).await?);

        let target_versions =
            output_partition_versions(&self.client, &view.target).await?;
        self.mark_epoch_committed(&record, &target_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        self.metadata.set_view_error(&view.view_id, None).await?;
        let mut updated = view.clone();
        updated.source_generation = self
            .metadata
            .view_generation(&view.source.table_id)
            .await
            .unwrap_or(0);
        register_sink_view(self, &updated).await?;
        Ok(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesoul_io::file_format::PhysicalFormat;

    #[test]
    fn sink_projection_overrides_bookkeeping_columns() {
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
            Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
        ]));
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
            Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
        ]));
        let view = SinkView {
            view_id: "view".to_string(),
            source: IvmTable {
                table_id: "source".to_string(),
                table_name: "source".to_string(),
                namespace: "default".to_string(),
                table_path: "file:///tmp/source".to_string(),
                schema: source_schema,
                primary_keys: vec!["k".to_string()],
                range_partition_columns: Vec::new(),
                bucket_columns: Vec::new(),
                hash_bucket_num: "1".to_string(),
                cdc_column: Some(IVM_ROW_KINDS_COLUMN.to_string()),
                file_format: PhysicalFormat::Parquet,
                allow_fallback: true,
            },
            target: IvmTable {
                table_id: "target".to_string(),
                table_name: "target".to_string(),
                namespace: "default".to_string(),
                table_path: "file:///tmp/target".to_string(),
                schema: target_schema,
                primary_keys: vec!["k".to_string()],
                range_partition_columns: Vec::new(),
                bucket_columns: Vec::new(),
                hash_bucket_num: "1".to_string(),
                cdc_column: Some(IVM_ROW_KINDS_COLUMN.to_string()),
                file_format: PhysicalFormat::Parquet,
                allow_fallback: true,
            },
            columns: vec![
                SinkColumn {
                    name: "k".to_string(),
                    source: String::new(),
                    result: "int64".to_string(),
                    nullable: false,
                },
                SinkColumn {
                    name: IVM_ROW_KINDS_COLUMN.to_string(),
                    source: String::new(),
                    result: "utf8".to_string(),
                    nullable: false,
                },
                SinkColumn {
                    name: IVM_EPOCH_COLUMN.to_string(),
                    source: String::new(),
                    result: "int64".to_string(),
                    nullable: false,
                },
            ],
            primary_keys: vec!["k".to_string()],
            range_partition_columns: Vec::new(),
            source_definition: "source-hash".to_string(),
            source_generation: 0,
            refresh_interval_ms: 0,
        };
        validate_sink_view(&view).unwrap();
        let exprs = view.projection(7, false).unwrap();
        assert_eq!(
            exprs
                .iter()
                .map(|expr| format!("{expr}"))
                .collect::<Vec<_>>(),
            vec!["k AS k", "rowKinds AS rowKinds", "Int64(7) AS __ivm_epoch",]
        );
    }
}
