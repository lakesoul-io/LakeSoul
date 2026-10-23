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
    for key in join_keys {
        source.schema.field_with_name(key).map_err(|_| {
            DataFusionError::Execution(format!(
                "join key {key} is missing from source {}",
                source.table_name
            ))
        })?;
    }
    let mut fields = source
        .schema
        .fields()
        .iter()
        .map(|field| Arc::new(field.as_ref().clone()))
        .collect::<Vec<_>>();
    if source.cdc_column.is_none()
        && source.schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_err()
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
    encoder: GroupKeyRowEncoder,
}

impl JoinKeyEncoder {
    pub(crate) fn new(source: &IvmTable, join_keys: &[String]) -> DataFusionResult<Self> {
        let encoder = GroupKeyRowEncoder::new(&source.schema, join_keys)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        Ok(Self {
            keys: join_keys.to_vec(),
            kind_column: join_side_state_cdc_column(source),
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
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone()))
            .collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            IVM_JOIN_KEY_COLUMN,
            DataType::Binary,
            false,
        )));
        let mut output = batch.columns().to_vec();
        if let Some(kind_column) = &self.kind_column
            && batch.schema().field_with_name(kind_column).is_err()
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
    ) -> crate::error::Result<()> {
        use futures::StreamExt;
        let context = super::refresh_session();
        let frame = self.current_table_frame(&context, source)?;
        let encoder = JoinKeyEncoder::new(source, join_keys)?;
        let mut stream = frame.execute_stream().await?;
        state.truncate(&self.client).await?;
        let mut pending = Vec::new();
        let mut pending_rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = encoder.encode_batch(&batch?)?;
            pending_rows += batch.num_rows();
            pending.push(batch);
            if pending_rows >= crate::table::MAX_STATE_APPEND_ROWS {
                let schema = pending[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &pending)?;
                pending.clear();
                pending_rows = 0;
                state.append_batches(&self.client, vec![batch]).await?;
            }
        }
        if !pending.is_empty() {
            let schema = pending[0].schema();
            let batch = arrow::compute::concat_batches(&schema, &pending)?;
            state.append_batches(&self.client, vec![batch]).await?;
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
    ) -> crate::error::Result<()> {
        use futures::StreamExt;
        if window.added_files.is_empty() {
            return Ok(());
        }
        let context = super::refresh_session();
        let delta =
            self.groups_table_frame(&context, source, window.added_files.clone())?;
        let pk_exprs = source
            .primary_keys
            .iter()
            .map(|key| datafusion::prelude::col(key.as_str()))
            .collect::<Vec<_>>();
        let delta_pks = delta.clone().select(pk_exprs.clone())?.distinct()?;
        let pk_names = source
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let state_frame = self.current_table_frame(&context, state)?;
        let old = state_frame.join(
            delta_pks,
            datafusion::prelude::JoinType::LeftSemi,
            &pk_names,
            &pk_names,
            None,
        )?;
        let kind = join_side_state_cdc_column(source)
            .ok_or_else(|| rootcause::report!("join side state has no change column"))?;
        let old = old.with_column(kind.as_str(), datafusion::prelude::lit("delete"))?;
        super::append_dataframe(state, &self.client, old).await?;
        let encoder = JoinKeyEncoder::new(source, join_keys)?;
        let mut stream = delta.execute_stream().await?;
        let mut pending = Vec::new();
        let mut pending_rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = encoder.encode_batch(&batch?)?;
            pending_rows += batch.num_rows();
            pending.push(batch);
            if pending_rows >= crate::table::MAX_STATE_APPEND_ROWS {
                let schema = pending[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &pending)?;
                pending.clear();
                pending_rows = 0;
                state.append_batches(&self.client, vec![batch]).await?;
            }
        }
        if !pending.is_empty() {
            let schema = pending[0].schema();
            let batch = arrow::compute::concat_batches(&schema, &pending)?;
            state.append_batches(&self.client, vec![batch]).await?;
        }
        Ok(())
    }
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
        assert_eq!(encoded.num_columns(), 4);
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
