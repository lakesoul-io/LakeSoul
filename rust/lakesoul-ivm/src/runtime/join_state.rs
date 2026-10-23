// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Keyed state layout for join sources.
//!
//! A join source can be keyed by a row identity that is different from its
//! join key.  The side state table therefore stores the source row under a
//! canonical `arrow::Row` encoding of the join key plus the source primary
//! keys.  The binary key is the bucket column, so a lookup can address one
//! bucket without scanning the full source.
//!
//! The caller decides whether a side state is needed.  When the source
//! primary key already equals the join key (and the join key is unique), the
//! source itself is the keyed state and no duplicate table is created.
//!
//! The join refresh wiring is the next step; the schema and encoder are kept
//! here so the same layout is used by both inner and lookup joins.
#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};

use super::{GroupKeyRowEncoder, IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN};
use crate::table::IvmTable;

/// Hidden binary key encoding the source join keys with `arrow::Row`.
pub(crate) const IVM_JOIN_KEY_COLUMN: &str = "__ivm_join_key";

/// Build the state schema of one join source.
///
/// The state keeps the source columns and adds the hidden binary join key.
/// The source CDC column (when present) and epoch stay part of the table.
pub(crate) fn join_side_state_schema_for(
    source: &IvmTable,
    join_keys: &[String],
) -> DataFusionResult<SchemaRef> {
    if join_keys.is_empty() {
        return Err(DataFusionError::Execution(
            "join side state needs at least one join key".to_string(),
        ));
    }
    if source.schema.field_with_name(IVM_JOIN_KEY_COLUMN).is_ok() {
        return Err(DataFusionError::Execution(format!(
            "source {} already has a {} column",
            source.table_name, IVM_JOIN_KEY_COLUMN
        )));
    }
    let mut fields = source
        .schema
        .fields()
        .iter()
        .map(|field| Arc::new(field.as_ref().clone()))
        .collect::<Vec<_>>();
    let source_cdc_present = source
        .cdc_column
        .as_ref()
        .is_some_and(|column| source.schema.field_with_name(column).is_ok());
    if !source_cdc_present && source.schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_err()
    {
        fields.push(Arc::new(Field::new(
            IVM_ROW_KINDS_COLUMN,
            DataType::Utf8,
            false,
        )));
    }
    fields.push(Arc::new(Field::new(
        IVM_JOIN_KEY_COLUMN,
        DataType::Binary,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// Primary keys of a side state table: the encoded join key followed by the
/// source row identity.
pub(crate) fn join_side_state_primary_keys(source: &IvmTable) -> Vec<String> {
    let mut keys = vec![IVM_JOIN_KEY_COLUMN.to_string()];
    keys.extend(source.primary_keys.iter().cloned());
    keys
}

/// Whether a side needs an auxiliary state table.
///
/// A unique source keyed exactly by its join key is already a keyed lookup
/// table, so the source itself is the state.
pub(crate) fn join_side_needs_state(source: &IvmTable, join_keys: &[String]) -> bool {
    if source.primary_keys.is_empty() {
        return false;
    }
    let mut source_keys = source.primary_keys.clone();
    let mut join_keys = join_keys.to_vec();
    source_keys.sort();
    join_keys.sort();
    source_keys != join_keys
}

/// Encoder for the hidden join-key column.
pub(crate) struct JoinKeyEncoder {
    keys: Vec<String>,
    kind_column: Option<String>,
    payload_schema: SchemaRef,
    encoder: GroupKeyRowEncoder,
}

impl JoinKeyEncoder {
    pub(crate) fn new(source: &IvmTable, join_keys: &[String]) -> DataFusionResult<Self> {
        Self::new_with_schema(source, &source.schema, join_keys)
    }

    /// Build an encoder over the schema carrying the (possibly computed) key
    /// columns.  The side-state payload still uses the source schema; only the
    /// hidden key is computed from this frame.
    pub(crate) fn new_with_schema(
        source: &IvmTable,
        schema: &Schema,
        join_keys: &[String],
    ) -> DataFusionResult<Self> {
        let encoder = GroupKeyRowEncoder::new(schema, join_keys)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        Ok(Self {
            keys: join_keys.to_vec(),
            kind_column: join_side_state_cdc_column(source),
            payload_schema: source.schema.clone(),
            encoder,
        })
    }

    /// Append the encoded join key to a source batch.
    pub(crate) fn encode_batch(
        &self,
        batch: &RecordBatch,
    ) -> DataFusionResult<RecordBatch> {
        let columns = self
            .keys
            .iter()
            .map(|key| {
                batch.column_by_name(key).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "join key {key} is missing from the batch"
                    ))
                })
            })
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
        let encoded = self
            .encoder
            .encode_columns(&columns)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        // The side-state payload is exactly the source schema.  A computed
        // join-key expression may have been materialized on the frame, so
        // select the payload by source column name instead of forwarding all
        // batch columns.
        let mut fields = self
            .payload_schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone()))
            .collect::<Vec<_>>();
        let mut output = self
            .payload_schema
            .fields()
            .iter()
            .map(|field| {
                batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "join side payload column {} is missing",
                        field.name()
                    ))
                })
            })
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
        if let Some(kind_column) = &self.kind_column
            && self.payload_schema.field_with_name(kind_column).is_err()
        {
            fields.push(Arc::new(Field::new(
                kind_column.clone(),
                DataType::Utf8,
                false,
            )));
            output.push(Arc::new(StringArray::from_iter_values(
                std::iter::repeat_n("insert", batch.num_rows()),
            )));
        }
        fields.push(Arc::new(Field::new(
            IVM_JOIN_KEY_COLUMN,
            DataType::Binary,
            false,
        )));
        output.push(Arc::new(encoded));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), output)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }
}

/// The CDC column of a side state table, if the source has one.
pub(crate) fn join_side_state_cdc_column(source: &IvmTable) -> Option<String> {
    source
        .cdc_column
        .clone()
        .filter(|column| source.schema.field_with_name(column).is_ok())
        .or_else(|| {
            source
                .schema
                .field_with_name(IVM_ROW_KINDS_COLUMN)
                .ok()
                .map(|_| IVM_ROW_KINDS_COLUMN.to_string())
        })
        .or_else(|| Some(IVM_ROW_KINDS_COLUMN.to_string()))
}

/// The epoch column is optional on a source but always present on a join
/// state snapshot.  A source without one is treated as epoch zero.
pub(crate) fn join_side_state_epoch_column(source: &IvmTable) -> Option<String> {
    source
        .schema
        .field_with_name(IVM_EPOCH_COLUMN)
        .ok()
        .map(|_| IVM_EPOCH_COLUMN.to_string())
}

/// Extract the binary join key from a side state or encoded batch.
pub(crate) fn join_key_array(batch: &RecordBatch) -> DataFusionResult<&BinaryArray> {
    batch
        .column_by_name(IVM_JOIN_KEY_COLUMN)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "batch has no {IVM_JOIN_KEY_COLUMN} column"
            ))
        })?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("{IVM_JOIN_KEY_COLUMN} is not binary"))
        })
}

/// Materialize computed join-key expressions under their join-key names.
fn materialize_join_keys(
    context: &datafusion::prelude::SessionContext,
    frame: datafusion::prelude::DataFrame,
    exprs: &[String],
    join_keys: &[String],
) -> crate::error::Result<datafusion::prelude::DataFrame> {
    let mut frame = frame;
    for (index, key) in join_keys.iter().enumerate() {
        if let Some(expr) = exprs.get(index).filter(|expr| !expr.is_empty()) {
            let expression = context.state().create_logical_expr(expr, frame.schema())?;
            frame = frame.with_column(key.as_str(), expression)?;
        }
    }
    Ok(frame)
}

impl super::IvmRuntime {
    /// Create or reuse one side's keyed state table and register it under the
    /// view.  The source itself is used when its primary key already equals
    /// the join key.
    pub(crate) async fn ensure_join_side_state(
        &self,
        view_id: &str,
        source: &IvmTable,
        join_keys: &[String],
        role: crate::metadata::StateRole,
    ) -> crate::error::Result<Option<IvmTable>> {
        if !join_side_needs_state(source, join_keys) {
            return Ok(None);
        }
        if let Some(existing) = self.metadata().get_state(view_id, role).await? {
            return Ok(Some(self.open_table_by_id(&existing.table_id).await?));
        }
        let schema = join_side_state_schema_for(source, join_keys)?;
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let mut options = crate::table::IvmTableOptions::new(
            format!("{}__ivm_join_state_{suffix}", source.table_name),
            format!("{}__ivm_join_state_{suffix}", source.table_path),
            schema,
        )
        .with_namespace(source.namespace.clone())
        .with_primary_keys(join_side_state_primary_keys(source))
        .with_bucket_columns(vec![IVM_JOIN_KEY_COLUMN.to_string()])
        .with_file_format(source.file_format);
        options.hash_bucket_num = source.hash_bucket_num.clone();
        if let Some(cdc) = join_side_state_cdc_column(source) {
            options = options.with_cdc_column(cdc);
        }
        let state = self.create_table(options).await?;
        self.metadata()
            .register_state(view_id, role, &state)
            .await?;
        Ok(Some(state))
    }

    /// Replace a side state with the current logical source rows, encoded
    /// under the hidden `arrow::Row` join key.
    pub(crate) async fn sync_join_side_state(
        &self,
        source: &IvmTable,
        state: &IvmTable,
        join_keys: &[String],
        key_exprs: &[String],
    ) -> crate::error::Result<()> {
        use futures::StreamExt;
        let context = super::refresh_session();
        let frame = materialize_join_keys(
            &context,
            self.current_table_frame(&context, source)?,
            key_exprs,
            join_keys,
        )?;
        let encoder = JoinKeyEncoder::new_with_schema(
            source,
            frame.schema().as_arrow(),
            join_keys,
        )?;
        let mut stream = frame.execute_stream().await?;
        state.truncate(&self.client).await?;
        let mut pending = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = encoder.encode_batch(&batch?)?;
            pending.push(batch);
        }
        if !pending.is_empty() {
            state.append_batches(&self.client, pending).await?;
        }
        Ok(())
    }
}

impl super::IvmRuntime {
    /// Incrementally replace a side state from one source window.
    ///
    /// The source row identity is stable while its join key can change.  The
    /// old side row is therefore found by the source primary keys, marked as a
    /// delete under its old encoded join key, and the window rows are appended
    /// under their new keys.  Keeping both operations in the state table's CDC
    /// log makes the read side a normal merge-on-read keyed table.
    pub(crate) async fn apply_join_side_delta(
        &self,
        source: &IvmTable,
        state: &IvmTable,
        window: &super::SourceWindow,
        join_keys: &[String],
        key_exprs: &[String],
    ) -> crate::error::Result<()> {
        use futures::StreamExt;
        if window.added_files.is_empty() {
            return Ok(());
        }
        let context = super::refresh_session();
        let delta = materialize_join_keys(
            &context,
            self.groups_table_frame(&context, source, window.added_files.clone())?,
            key_exprs,
            join_keys,
        )?;
        let kind = join_side_state_cdc_column(source)
            .ok_or_else(|| rootcause::report!("join side state has no change column"))?;
        let encoder = JoinKeyEncoder::new_with_schema(
            source,
            delta.schema().as_arrow(),
            join_keys,
        )?;

        // Read the before image from the source, whose primary-key index is
        // the source row identity.  Scanning the side state by source PK is
        // not a keyed lookup: the side state is bucketed by join key.
        let before = materialize_join_keys(
            &context,
            self.before_window_table_frame(&context, source, &window.before_versions)
                .await?,
            key_exprs,
            join_keys,
        )?;
        let pk_exprs = source
            .primary_keys
            .iter()
            .map(|key| datafusion::prelude::col(key.as_str()))
            .collect::<Vec<_>>();
        let delta_pks = delta.clone().select(pk_exprs)?.distinct()?;
        let pk_names = source
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let old = if source.primary_keys.len() == 1 {
            // A single source primary key is also the writer's bucket key for
            // the common case, so an IN filter can use the primary-key index.
            // Keep the semi-join fallback for a larger key set and for
            // multi-column identities.
            let pk = source.primary_keys[0].as_str();
            let batches = delta_pks.clone().collect().await?;
            let values = batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).filter_map(move |row| {
                        datafusion::common::ScalarValue::try_from_array(
                            batch.column(0),
                            row,
                        )
                        .ok()
                    })
                })
                .collect::<Vec<_>>();
            if values.is_empty() {
                before.clone().limit(0, Some(0))?
            } else if values.len() <= 8192 {
                let list = values.into_iter().map(datafusion::prelude::lit).collect();
                before.filter(datafusion::prelude::col(pk).in_list(list, false))?
            } else {
                before.join(
                    delta_pks,
                    datafusion::prelude::JoinType::LeftSemi,
                    &pk_names,
                    &pk_names,
                    None,
                )?
            }
        } else {
            before.join(
                delta_pks,
                datafusion::prelude::JoinType::LeftSemi,
                &pk_names,
                &pk_names,
                None,
            )?
        };

        let mut pending = Vec::new();
        let mut old_stream = old.execute_stream().await?;
        while let Some(batch) = old_stream.next().await {
            let batch =
                set_change_kind(&encoder.encode_batch(&batch?)?, &kind, "delete")?;
            pending.push(batch);
        }
        let mut delta_stream = delta.execute_stream().await?;
        while let Some(batch) = delta_stream.next().await {
            pending.push(encoder.encode_batch(&batch?)?);
        }
        if !pending.is_empty() {
            state.append_batches(&self.client, pending).await?;
        }
        Ok(())
    }
}

/// Replace a state change column on a batch (used to turn the source before
/// image into a retraction under its old encoded join key).
fn set_change_kind(
    batch: &RecordBatch,
    column: &str,
    value: &str,
) -> crate::error::Result<RecordBatch> {
    let index = batch.schema().index_of(column)?;
    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        value,
        batch.num_rows(),
    )));
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;

    fn source(schema: SchemaRef) -> IvmTable {
        IvmTable {
            table_id: "t".to_string(),
            table_name: "t".to_string(),
            namespace: "default".to_string(),
            table_path: "file:///tmp/t".to_string(),
            schema,
            primary_keys: vec!["id".to_string()],
            range_partition_columns: Vec::new(),
            bucket_columns: vec!["id".to_string()],
            hash_bucket_num: "4".to_string(),
            cdc_column: None,
            file_format: crate::PhysicalFormat::Vortex,
            allow_fallback: true,
        }
    }

    #[test]
    fn encodes_multi_column_join_keys_with_arrow_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k1", DataType::Utf8, false),
            Field::new("k2", DataType::Int64, false),
        ]));
        let table = source(schema.clone());
        let encoder =
            JoinKeyEncoder::new(&table, &["k1".to_string(), "k2".to_string()]).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![7, 7, 7])),
            ],
        )
        .unwrap();
        let encoded = encoder.encode_batch(&batch).unwrap();
        let keys = join_key_array(&encoded).unwrap();
        assert_eq!(keys.value(0), keys.value(1));
        assert_ne!(keys.value(0), keys.value(2));
        // Source columns + rowKinds + hidden join key.
        assert_eq!(encoded.num_columns(), 5);
    }

    #[test]
    fn skips_side_state_when_pk_is_the_join_key() {
        let table = source(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])));
        assert!(!join_side_needs_state(&table, &["id".to_string()]));
        assert!(join_side_needs_state(&table, &["other".to_string()]));
        assert_eq!(
            join_side_state_primary_keys(&table),
            vec![IVM_JOIN_KEY_COLUMN.to_string(), "id".to_string()]
        );
    }
}
