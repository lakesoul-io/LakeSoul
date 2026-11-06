// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Target-table validation and row planning.

use std::collections::BTreeSet;

use arrow_schema::{DataType, SchemaRef, TimeUnit};
use datafusion::common::ScalarValue;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata_proto::entity::{PartitionInfo, Uuid};
use pg_walstream::{RelationColumn, RowData};
use rootcause::{bail, report};
use serde_json::Value;

use super::decode::{KeyValue, key_of, null_scalar, parse_value};
use super::source::{PgType, SourceColumn, SourceTable};
use crate::Result;

/// `high` of every commit written by the subscriber. `low` carries the source
/// `commit_lsn`; the target table's latest partition snapshot therefore
/// doubles as a durable watermark without a side table.
pub const SUBSCRIPTION_COMMIT_MAGIC: u64 = 0x4c53_5047_0000_0000;

pub fn commit_id_for(lsn: u64) -> Uuid {
    Uuid {
        high: SUBSCRIPTION_COMMIT_MAGIC,
        low: lsn,
    }
}

pub fn decode_watermark(partitions: &[PartitionInfo]) -> u64 {
    partitions
        .iter()
        .flat_map(|partition| partition.snapshot.iter())
        .filter(|commit| commit.high == SUBSCRIPTION_COMMIT_MAGIC)
        .map(|commit| commit.low)
        .max()
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RowState {
    pub op: Op,
    /// Full target schema, including a placeholder at the CDC column.
    pub values: Vec<ScalarValue>,
}

#[derive(Debug, Clone)]
pub struct ColumnMapping {
    pub target_index: usize,
    pub source_index: usize,
}

#[derive(Debug, Clone)]
pub struct KeyColumn {
    pub target_index: usize,
    pub source: SourceColumn,
}

#[derive(Debug)]
pub struct TablePlan {
    pub source: SourceTable,
    pub target_table: String,
    pub table: LakeSoulTable,
    pub schema: SchemaRef,
    pub cdc_column: String,
    pub cdc_index: usize,
    pub mappings: Vec<ColumnMapping>,
    pub key_columns: Vec<KeyColumn>,
    pub commit_watermark: u64,
    /// Whether the target already has any published commit. A non-subscription
    /// commit with no watermark means the table was loaded by another path.
    pub has_commits: bool,
}

impl TablePlan {
    pub async fn new(
        source: SourceTable,
        target_namespace: &str,
        target_table: &str,
        primary_key: Option<&[String]>,
        meta: MetaDataClientRef,
    ) -> Result<Self> {
        let table = LakeSoulTable::for_namespace_and_name(
            target_namespace,
            target_table,
            Some(meta),
        )
        .await
        .map_err(|error| {
            report!(
                "target table {target_namespace}.{target_table} does not exist or \
                 cannot be opened: {error}"
            )
        })?;
        let schema = table.schema();
        let info = table.table_info();

        let properties: Value =
            serde_json::from_str(&info.properties).map_err(|error| {
                report!("target table properties are not valid JSON: {error}")
            })?;
        let use_cdc = property_string(&properties, "use_cdc");
        if use_cdc.as_deref() != Some("true") {
            bail!(
                "target table {target_namespace}.{target_table} is not a CDC table; \
                 create it WITH (use_cdc = true, lakesoul_cdc_change_column = 'rowKinds')"
            );
        }
        let cdc_column = property_string(&properties, "lakesoul_cdc_change_column")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                report!(
                    "target table {target_namespace}.{target_table} has no \
                     lakesoul_cdc_change_column property"
                )
            })?;
        let cdc_index = schema.index_of(&cdc_column).map_err(|error| {
            report!(
                "target table {target_namespace}.{target_table} has no CDC column \
                 {cdc_column:?}: {error}"
            )
        })?;

        if !table.range_partitions().is_empty() {
            bail!(
                "target table {target_namespace}.{target_table} is range-partitioned by \
                 {:?}; P0 requires an unpartitioned target",
                table.range_partitions()
            );
        }

        let target_pk = table.primary_keys().clone();
        let source_pk: BTreeSet<String> = source
            .primary_key
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        let target_pk_set: BTreeSet<String> = target_pk
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        if source_pk != target_pk_set {
            bail!(
                "target primary key {target_pk:?} does not match source primary key {:?}",
                source.primary_key
            );
        }
        if let Some(primary_key) = primary_key {
            let configured_pk: BTreeSet<String> = primary_key
                .iter()
                .map(|name| name.to_ascii_lowercase())
                .collect();
            if configured_pk != source_pk {
                bail!(
                    "configured primary key {primary_key:?} does not match source \
                     primary key {:?}",
                    source.primary_key
                );
            }
        }

        let mut mappings = Vec::with_capacity(source.columns.len());
        for (target_index, field) in schema.fields().iter().enumerate() {
            if target_index == cdc_index {
                continue;
            }
            let source_index = source
                .columns
                .iter()
                .position(|column| column.name == *field.name())
                .ok_or_else(|| {
                    report!(
                        "target column {} has no source column with the same name",
                        field.name()
                    )
                })?;
            let source_column = &source.columns[source_index];
            check_type(source_column, field.data_type())?;
            if !source_column.not_null && !field.is_nullable() {
                bail!(
                    "target column {} is NOT NULL but source column {} is nullable",
                    field.name(),
                    source_column.name
                );
            }
            mappings.push(ColumnMapping {
                target_index,
                source_index,
            });
        }
        if mappings.len() != source.columns.len() {
            bail!(
                "target table {target_namespace}.{target_table} does not expose every \
                 source column: target has {} user columns, source has {}",
                mappings.len(),
                source.columns.len()
            );
        }

        let mut key_columns = Vec::with_capacity(target_pk.len());
        for key in &target_pk {
            let mapping = mappings
                .iter()
                .find(|mapping| schema.field(mapping.target_index).name() == key)
                .ok_or_else(|| {
                    report!("target primary key column {key} is not a user column")
                })?;
            key_columns.push(KeyColumn {
                target_index: mapping.target_index,
                source: source.columns[mapping.source_index].clone(),
            });
        }

        let partitions = table
            .client()
            .get_all_partition_info_as_of(
                &info.table_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await
            .map_err(|error| {
                report!("read target table watermark for {target_table}: {error}")
            })?;
        let commit_watermark = decode_watermark(&partitions);
        let has_commits = partitions
            .iter()
            .any(|partition| !partition.snapshot.is_empty());

        Ok(Self {
            source,
            target_table: target_table.to_string(),
            table,
            schema,
            cdc_column,
            cdc_index,
            mappings,
            key_columns,
            commit_watermark,
            has_commits,
        })
    }

    pub fn qualified_source_name(&self) -> String {
        self.source.qualified_name()
    }

    /// The RELATION message must describe exactly the schema validated at
    /// startup. P0 has no schema evolution: a mismatch stops the worker.
    pub fn check_relation(
        &self,
        relation_id: u32,
        columns: &[RelationColumn],
    ) -> Result<()> {
        if relation_id != self.source.relation_oid {
            bail!(
                "source relation oid changed from {} to {relation_id} for {}; \
                 the table was dropped and recreated. Resnapshot is required",
                self.source.relation_oid,
                self.qualified_source_name()
            );
        }
        if columns.len() != self.source.columns.len() {
            bail!(
                "source schema of {} changed: RELATION has {} columns, startup had {}; \
                 P0 does not support schema evolution",
                self.qualified_source_name(),
                columns.len(),
                self.source.columns.len()
            );
        }
        for (actual, expected) in columns.iter().zip(&self.source.columns) {
            if actual.name.as_ref() != expected.name
                || actual.type_id != expected.type_id
                || actual.type_modifier != expected.type_modifier
            {
                bail!(
                    "source schema of {} changed at column {:?} (type_id {} -> {}, \
                     typmod {} -> {}); P0 does not support schema evolution",
                    self.qualified_source_name(),
                    expected.name,
                    expected.type_id,
                    actual.type_id,
                    expected.type_modifier,
                    actual.type_modifier
                );
            }
        }
        Ok(())
    }

    /// P0 rejects unchanged-TOAST markers instead of writing NULLs. `RowData`
    /// silently drops `u` columns, so a missing column is the signal.
    pub fn check_full_row(&self, data: &RowData, context: &str) -> Result<()> {
        for column in &self.source.columns {
            if data.get(&column.name).is_none() {
                bail!(
                    "column {} is missing from {context}: pgoutput sent an unchanged \
                     TOAST marker ('u'); P0 does not support TOASTed columns",
                    column.name
                );
            }
        }
        Ok(())
    }

    pub fn parse_row(&self, data: &RowData, op: Op, context: &str) -> Result<RowState> {
        self.check_full_row(data, context)?;
        let mut values = self
            .schema
            .fields()
            .iter()
            .map(|field| null_scalar(field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        for mapping in &self.mappings {
            let source = &self.source.columns[mapping.source_index];
            let target = self.schema.field(mapping.target_index).data_type();
            let value = data.get(&source.name).ok_or_else(|| {
                report!("column {} disappeared while planning a row", source.name)
            })?;
            values[mapping.target_index] = parse_value(source, target, value)
                .map_err(|error| report!("column {}: {error}", source.name))?;
        }
        Ok(RowState { op, values })
    }

    pub fn key_of(&self, values: &[ScalarValue]) -> Result<Vec<KeyValue>> {
        self.key_columns
            .iter()
            .map(|key| {
                key_of(&key.source, &values[key.target_index])
                    .map_err(|error| report!("primary key {}: {error}", key.source.name))
            })
            .collect()
    }

    pub fn parse_insert(&self, data: &RowData) -> Result<(Vec<KeyValue>, RowState)> {
        let state = self.parse_row(data, Op::Insert, "INSERT")?;
        let key = self.key_of(&state.values)?;
        Ok((key, state))
    }

    /// An UPDATE may change the primary key. With REPLICA IDENTITY FULL the
    /// old tuple is the complete non-TOAST row, so the old key gets a full
    /// delete tombstone and the new key gets the new row.
    pub fn parse_update(
        &self,
        old_data: Option<&RowData>,
        new_data: &RowData,
    ) -> Result<Vec<(Vec<KeyValue>, RowState)>> {
        let old_data = old_data.ok_or_else(|| {
            report!(
                "UPDATE on {} carried no old tuple; REPLICA IDENTITY FULL must be set",
                self.qualified_source_name()
            )
        })?;
        let old_state = self.parse_row(old_data, Op::Delete, "UPDATE old tuple")?;
        let new_state = self.parse_row(new_data, Op::Update, "UPDATE new tuple")?;
        let old_key = self.key_of(&old_state.values)?;
        let new_key = self.key_of(&new_state.values)?;
        if old_key == new_key {
            Ok(vec![(new_key, new_state)])
        } else {
            Ok(vec![(old_key, old_state), (new_key, new_state)])
        }
    }

    pub fn parse_delete(&self, old_data: &RowData) -> Result<(Vec<KeyValue>, RowState)> {
        let state = self.parse_row(old_data, Op::Delete, "DELETE")?;
        let key = self.key_of(&state.values)?;
        Ok((key, state))
    }
}

fn property_string(properties: &Value, key: &str) -> Option<String> {
    properties.get(key).and_then(|value| {
        value
            .as_str()
            .map(ToString::to_string)
            .or_else(|| value.as_bool().map(|value| value.to_string()))
    })
}

fn check_type(source: &SourceColumn, target: &DataType) -> Result<()> {
    let compatible = match &source.pg_type {
        PgType::Bool => matches!(target, DataType::Boolean),
        PgType::Int2 => matches!(target, DataType::Int16),
        PgType::Int4 => matches!(target, DataType::Int32),
        PgType::Int8 => matches!(target, DataType::Int64),
        PgType::Float4 => matches!(target, DataType::Float32),
        PgType::Float8 => matches!(target, DataType::Float64),
        PgType::Numeric { precision, scale } => match target {
            DataType::Decimal128(target_precision, target_scale) => {
                let scale_ok = scale.is_none_or(|scale| *target_scale == scale);
                let precision_ok =
                    precision.is_none_or(|precision| *target_precision >= precision);
                scale_ok && precision_ok
            }
            _ => false,
        },
        PgType::Text | PgType::Uuid | PgType::Json | PgType::Jsonb => {
            matches!(target, DataType::Utf8)
        }
        PgType::Bytea => matches!(target, DataType::Binary),
        PgType::Date => matches!(target, DataType::Date32),
        PgType::Time => matches!(target, DataType::Time64(TimeUnit::Microsecond)),
        PgType::Timestamp => {
            matches!(target, DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        PgType::TimestampTz => {
            matches!(target, DataType::Timestamp(TimeUnit::Microsecond, Some(_)))
        }
    };
    if compatible {
        Ok(())
    } else {
        bail!(
            "column {} has source type {} but target type {target}; P0 requires an \
             exact type match",
            source.name,
            source.type_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_id_round_trips_the_lsn() {
        let id = commit_id_for(0x16_B374_D848);
        assert_eq!(id.high, SUBSCRIPTION_COMMIT_MAGIC);
        assert_eq!(id.low, 0x16_B374_D848);
    }

    #[test]
    fn watermark_ignores_foreign_commit_ids() {
        let mut matching = PartitionInfo::default();
        matching.snapshot = vec![
            Uuid {
                high: SUBSCRIPTION_COMMIT_MAGIC,
                low: 100,
            },
            Uuid { high: 0, low: 999 },
        ];
        let mut other = PartitionInfo::default();
        other.snapshot = vec![Uuid {
            high: SUBSCRIPTION_COMMIT_MAGIC,
            low: 42,
        }];
        assert_eq!(decode_watermark(&[matching, other]), 100);
        assert_eq!(decode_watermark(&[]), 0);
    }

    #[test]
    fn op_names_match_lakesoul_cdc() {
        assert_eq!(Op::Insert.as_str(), "insert");
        assert_eq!(Op::Update.as_str(), "update");
        assert_eq!(Op::Delete.as_str(), "delete");
    }
}
