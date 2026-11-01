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

    /// Encode the join key from already-projected key arrays.
    pub(crate) fn encode_columns(
        &self,
        columns: &[ArrayRef],
    ) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(self.encoder.encode_columns(columns).map_err(
            |error| DataFusionError::Execution(error.to_string()),
        )?))
    }

    /// Append only the hidden key to a contribution batch.  Unlike the state
    /// payload, a join-key probe does not carry the source columns.
    pub(crate) fn encode_key_only(
        &self,
        batch: &RecordBatch,
    ) -> DataFusionResult<RecordBatch> {
        let columns = self
            .keys
            .iter()
            .map(|key| {
                batch.column_by_name(key).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "join key {key} is missing from the contribution"
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
        output.push(Arc::new(encoded));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), output)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
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
    let actual = |name: &str| {
        source
            .schema
            .fields()
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(name))
            .map(|field| field.name().clone())
    };
    source
        .cdc_column
        .as_deref()
        .and_then(actual)
        .or_else(|| actual(IVM_ROW_KINDS_COLUMN))
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
pub(crate) fn materialize_join_keys(
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
        self.ensure_join_side_state_with_force(view_id, source, join_keys, role, false)
            .await
    }

    /// Like [`Self::ensure_join_side_state`], but also creates a side state
    /// when the source PK already equals the join key.  NULL-safe joins need
    /// this because the direct source lookup deliberately skips NULL keys,
    /// while the binary side-state key preserves them.
    pub(crate) async fn ensure_join_side_state_forced(
        &self,
        view_id: &str,
        source: &IvmTable,
        join_keys: &[String],
        role: crate::metadata::StateRole,
    ) -> crate::error::Result<Option<IvmTable>> {
        self.ensure_join_side_state_with_force(view_id, source, join_keys, role, true)
            .await
    }

    /// Ensure a side state, forcing the auxiliary table for NULL-safe joins.
    pub(crate) async fn ensure_join_side_state_for_view(
        &self,
        view_id: &str,
        source: &IvmTable,
        join_keys: &[String],
        role: crate::metadata::StateRole,
        null_safe: bool,
    ) -> crate::error::Result<Option<IvmTable>> {
        if null_safe {
            self.ensure_join_side_state_forced(view_id, source, join_keys, role)
                .await
        } else {
            self.ensure_join_side_state(view_id, source, join_keys, role)
                .await
        }
    }

    async fn ensure_join_side_state_with_force(
        &self,
        view_id: &str,
        source: &IvmTable,
        join_keys: &[String],
        role: crate::metadata::StateRole,
        force: bool,
    ) -> crate::error::Result<Option<IvmTable>> {
        if !force && !join_side_needs_state(source, join_keys) {
            return Ok(None);
        }
        let schema = join_side_state_schema_for(source, join_keys)?;
        let primary_keys = join_side_state_primary_keys(source);
        if let Some(existing) = self.metadata().get_state(view_id, role).await? {
            let table = self.open_table_by_id(&existing.table_id).await?;
            if table.schema == schema && table.primary_keys == primary_keys {
                return Ok(Some(table));
            }
            // A projected state (or a changed source shape) replaces the old
            // auxiliary table.  The old table is not an MV, so dropping it is
            // safe.
            self.metadata().unregister_state(view_id, role).await?;
            let _ = self
                .client
                .drop_table(&table.table_name, &table.namespace)
                .await;
        }
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let mut options = crate::table::IvmTableOptions::new(
            format!("{}__ivm_join_state_{suffix}", source.table_name),
            format!("{}__ivm_join_state_{suffix}", source.table_path),
            schema,
        )
        .with_namespace(source.namespace.clone())
        .with_primary_keys(primary_keys.clone())
        .with_bucket_columns(vec![IVM_JOIN_KEY_COLUMN.to_string()])
        .with_file_format(crate::table::keyed_state_file_format());
        let range_partitions =
            crate::table::state_range_partition_columns(source, &primary_keys);
        if !range_partitions.is_empty() {
            options = options.with_range_partition_columns(range_partitions);
        }
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
    ) -> crate::error::Result<Vec<String>> {
        use futures::StreamExt;
        let context = super::refresh_session();
        let frame = materialize_join_keys(
            &context,
            self.current_table_frame(&context, source)?,
            key_exprs,
            join_keys,
        )?;
        let encoder = std::sync::Arc::new(JoinKeyEncoder::new_with_schema(
            source,
            frame.schema().as_arrow(),
            join_keys,
        )?);
        state.truncate(&self.client).await?;
        let stream = frame.execute_stream().await?.map(move |batch| {
            let batch = batch?;
            encoder.encode_batch(&batch).map_err(|error| {
                datafusion::error::DataFusionError::Execution(format!("{error:?}"))
            })
        });
        state.append_stream(&self.client, Box::pin(stream)).await
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
    ) -> crate::error::Result<Vec<String>> {
        use futures::StreamExt;
        if window.added_files.is_empty() {
            return Ok(Vec::new());
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

        // Read the before image from the source through the generic PK lookup
        // plan.  The source primary key is the lookup key; the plan handles
        // single/composite keys, bucket routing and the reader's PK index.
        let pk_names = source
            .primary_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        if pk_names.is_empty() {
            return Err(rootcause::report!("join side state needs a keyed source"));
        }
        let pk_exprs = pk_names
            .iter()
            .map(|key| datafusion::prelude::col(*key))
            .collect::<Vec<_>>();
        let contribution = delta.clone().select(pk_exprs)?.distinct()?;
        let bucket_count = source
            .hash_bucket_num
            .parse::<usize>()
            .ok()
            .filter(|count| *count > 1)
            .unwrap_or(1);
        let route_columns = if bucket_count > 1
            && !source.bucket_columns.is_empty()
            && source
                .bucket_columns
                .iter()
                .all(|column| pk_names.contains(&column.as_str()))
        {
            Some(source.bucket_columns.clone())
        } else {
            None
        };
        let bucket_count = route_columns.as_ref().map_or(1, |_| bucket_count);
        let before_groups = source
            .before_window_groups(&self.client, &window.before_versions)
            .await?;
        let groups = super::keyed_plan::groups_by_bucket(before_groups, bucket_count);
        let provider = super::keyed_plan::IvmLookupTable::columns(
            contribution,
            source.clone(),
            groups,
            pk_names.iter().map(|key| key.to_string()).collect(),
            pk_names.iter().map(|key| key.to_string()).collect(),
            bucket_count,
            route_columns,
            true,
        );
        let before = context.read_table(std::sync::Arc::new(provider))?;
        let before_schema = before.schema().clone();
        let projections = source
            .schema
            .fields()
            .iter()
            .map(|field| {
                let prefixed = format!(
                    "{}{}",
                    super::keyed_plan::IVM_STATE_COLUMN_PREFIX,
                    field.name()
                );
                let actual = before_schema
                    .fields()
                    .iter()
                    .find(|candidate| {
                        candidate.name() == &prefixed
                            || candidate.name().eq_ignore_ascii_case(&prefixed)
                    })
                    .map(|candidate| candidate.name().clone())
                    .unwrap_or(prefixed);
                super::column_expr(&actual).alias(field.name())
            })
            .collect::<Vec<_>>();
        let old = materialize_join_keys(
            &context,
            before.select(projections)?,
            key_exprs,
            join_keys,
        )?;

        let encoder = std::sync::Arc::new(encoder);
        let mut streams: Vec<
            std::pin::Pin<
                Box<
                    dyn futures::Stream<Item = datafusion::error::Result<RecordBatch>>
                        + Send,
                >,
            >,
        > = Vec::new();
        let old_encoder = encoder.clone();
        let old_stream = old.execute_stream().await?;
        streams.push(Box::pin(old_stream.map(move |batch| {
            let batch = batch?;
            set_change_kind(&old_encoder.encode_batch(&batch)?, &kind, "delete").map_err(
                |error| {
                    datafusion::error::DataFusionError::Execution(format!("{error:?}"))
                },
            )
        })));
        let delta_encoder = encoder;
        let delta_stream = delta.execute_stream().await?;
        streams.push(Box::pin(delta_stream.map(move |batch| {
            let batch = batch?;
            delta_encoder.encode_batch(&batch).map_err(|error| {
                datafusion::error::DataFusionError::Execution(format!("{error:?}"))
            })
        })));
        let stream = Box::pin(futures::stream::iter(streams).flatten());
        state.append_stream(&self.client, stream).await
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
