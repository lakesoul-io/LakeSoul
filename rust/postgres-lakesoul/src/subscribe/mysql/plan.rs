// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! MySQL source -> LakeSoul target mapping, creation and safe evolution.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_common::schema_evolution::{ColumnChange, TableLayout, check_evolution};
use lakesoul_common::ser::arrow_java::schema_to_metadata_parts;
use lakesoul_datafusion::catalog::LakeSoulTableProperty;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_datafusion::lakesoul_table::helpers::case_fold_table_name;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata_proto::entity::TableInfo;
use rootcause::{bail, report};

use super::super::SchemaEvolution;
use super::super::plan::decode_mysql_watermark;
use super::super::schema::{TargetOptions, publish_evolved_schema};
use super::catalog::MySqlTable;
use crate::Result;

const DEFAULT_HASH_BUCKET_NUM: &str = "4";
const DEFAULT_FILE_FORMAT: &str = "vortex-compact";

#[derive(Debug, Clone)]
pub struct MySqlColumnMapping {
    pub source_index: usize,
    pub target_index: usize,
}

#[derive(Debug, Clone)]
pub struct MySqlKeyColumn {
    pub name: String,
    pub source_index: usize,
}

#[derive(Debug)]
pub struct MySqlTablePlan {
    pub source: MySqlTable,
    pub target_namespace: String,
    pub target_table: String,
    pub table: LakeSoulTable,
    pub schema: SchemaRef,
    pub cdc_index: usize,
    pub mappings: Vec<MySqlColumnMapping>,
    pub key_columns: Vec<MySqlKeyColumn>,
    /// Latest commit sequence written by this subscriber, or 0.
    pub target_sequence: u64,
    /// Whether the target has any commit at all. A non-subscription commit with
    /// no MySQL watermark means the table was loaded by another path.
    pub has_commits: bool,
}

impl MySqlTablePlan {
    pub async fn new(
        meta: &MetaDataClientRef,
        source: MySqlTable,
        target_namespace: &str,
        target_table: &str,
        options: &TargetOptions,
        allow_create: bool,
        allow_evolve: bool,
    ) -> Result<Self> {
        let table = ensure_target(
            meta,
            &source,
            target_namespace,
            target_table,
            options,
            allow_create,
            allow_evolve,
            super::super::schema::EVOLVE_RETRIES,
        )
        .await?;
        let schema = table.schema();
        let info = table.table_info();
        let cdc_index = schema.index_of(&options.cdc_column).map_err(|error| {
            report!(
                "target table {target_namespace}.{target_table} has no CDC column \
                 {:?}: {error}",
                options.cdc_column
            )
        })?;
        if !matches!(schema.field(cdc_index).data_type(), DataType::Utf8) {
            bail!(
                "target CDC column {:?} must be Utf8, got {}",
                options.cdc_column,
                schema.field(cdc_index).data_type()
            );
        }
        if !table.range_partitions().is_empty() {
            bail!(
                "target table {target_namespace}.{target_table} is range partitioned by \
                 {:?}; P0 requires an unpartitioned target",
                table.range_partitions()
            );
        }

        let target_pk: std::collections::BTreeSet<String> = table
            .primary_keys()
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        let source_pk: std::collections::BTreeSet<String> = source
            .primary_key
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        if target_pk != source_pk {
            bail!(
                "target primary key {:?} does not match MySQL source primary key {:?}",
                table.primary_keys(),
                source.primary_key
            );
        }

        let mut mappings = Vec::with_capacity(source.columns.len());
        for (source_index, column) in source.columns.iter().enumerate() {
            let target_index = schema.index_of(&column.name).map_err(|error| {
                report!(
                    "target {target_namespace}.{target_table} has no source column \
                     {:?}: {error}",
                    column.name
                )
            })?;
            mappings.push(MySqlColumnMapping {
                source_index,
                target_index,
            });
        }
        let mut key_columns = Vec::with_capacity(source.primary_key.len());
        for key in &source.primary_key {
            let source_index = source
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(key))
                .ok_or_else(|| {
                    report!("primary-key column {key:?} is not in the source schema")
                })?;
            key_columns.push(MySqlKeyColumn {
                name: key.clone(),
                source_index,
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
                report!(
                    "read target watermark for {target_namespace}.{target_table}: {error}"
                )
            })?;
        let target_sequence = decode_mysql_watermark(&partitions);
        let has_commits = partitions
            .iter()
            .any(|partition| !partition.snapshot.is_empty());

        Ok(Self {
            source,
            target_namespace: target_namespace.to_string(),
            target_table: target_table.to_string(),
            table,
            schema,
            cdc_index,
            mappings,
            key_columns,
            target_sequence,
            has_commits,
        })
    }

    pub fn source_name(&self) -> String {
        self.source.qualified_name()
    }

    pub fn target_name(&self) -> String {
        format!("{}.{}", self.target_namespace, self.target_table)
    }

    /// Build a full target-schema row from a source row. `values` is indexed by
    /// source column order; the CDC column is left as NULL for `build_batch`.
    pub fn row_values(
        &self,
        source_values: &[datafusion::common::ScalarValue],
    ) -> Result<Vec<datafusion::common::ScalarValue>> {
        if source_values.len() != self.source.columns.len() {
            bail!(
                "source row for {} has {} values, expected {}",
                self.source_name(),
                source_values.len(),
                self.source.columns.len()
            );
        }
        let mut values = self
            .schema
            .fields()
            .iter()
            .map(|field| super::super::decode::null_scalar(field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        for mapping in &self.mappings {
            values[mapping.target_index] = source_values[mapping.source_index].clone();
        }
        values[self.cdc_index] = datafusion::common::ScalarValue::Utf8(None);
        Ok(values)
    }

    pub fn key_value(
        &self,
        source_values: &[datafusion::common::ScalarValue],
    ) -> Result<Vec<crate::subscribe::decode::KeyValue>> {
        self.key_columns
            .iter()
            .map(|column| {
                let value = source_values.get(column.source_index).ok_or_else(|| {
                    report!(
                        "primary-key column {} is missing from source row",
                        column.name
                    )
                })?;
                key_value(column.name.as_str(), value)
            })
            .collect()
    }
}

fn key_value(
    name: &str,
    value: &datafusion::common::ScalarValue,
) -> Result<crate::subscribe::decode::KeyValue> {
    use crate::subscribe::decode::KeyValue;
    use datafusion::common::ScalarValue;
    Ok(match value {
        ScalarValue::Boolean(Some(value)) => KeyValue::Bool(*value),
        ScalarValue::Int8(Some(value)) => KeyValue::Int(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => KeyValue::Int(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => KeyValue::Int(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => KeyValue::Int(*value),
        ScalarValue::UInt8(Some(value)) => KeyValue::UInt(u64::from(*value)),
        ScalarValue::UInt16(Some(value)) => KeyValue::UInt(u64::from(*value)),
        ScalarValue::UInt32(Some(value)) => KeyValue::UInt(u64::from(*value)),
        ScalarValue::UInt64(Some(value)) => KeyValue::UInt(*value),
        ScalarValue::Float32(Some(value)) => KeyValue::Float(f64::from(*value).to_bits()),
        ScalarValue::Float64(Some(value)) => KeyValue::Float(value.to_bits()),
        ScalarValue::Decimal128(Some(value), _, scale) => {
            KeyValue::Decimal(*value, *scale)
        }
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            KeyValue::Text(value.clone())
        }
        ScalarValue::Binary(Some(value)) => KeyValue::Bytes(value.clone()),
        ScalarValue::Date32(Some(value)) => KeyValue::Date(*value),
        ScalarValue::Time64Microsecond(Some(value)) => KeyValue::Time(*value),
        ScalarValue::TimestampMicrosecond(Some(value), tz) => {
            KeyValue::Timestamp(*value, tz.is_some())
        }
        other => bail!("primary-key column {name} has unsupported/NULL value {other:?}"),
    })
}

async fn ensure_target(
    meta: &MetaDataClientRef,
    source: &MySqlTable,
    target_namespace: &str,
    target_table: &str,
    options: &TargetOptions,
    allow_create: bool,
    allow_evolve: bool,
    retries: usize,
) -> Result<LakeSoulTable> {
    let table_name = case_fold_table_name(target_table);
    let info = meta
        .get_table_info_by_table_name(&table_name, target_namespace)
        .await
        .map_err(|error| {
            report!("read target table {target_namespace}.{target_table}: {error}")
        })?;

    if let Some(info) = info {
        let table =
            LakeSoulTable::try_new_with_client_and_table_info(meta.clone(), info.clone())
                .await
                .map_err(|error| {
                    report!(
                        "open target table {target_namespace}.{target_table}: {error}"
                    )
                })?;
        let cdc_column = table_cdc_column(&info)?;
        let layout =
            TableLayout::from_partitions(&info.partitions, Some(cdc_column.clone()));
        let new_schema = evolved_schema(
            source,
            table.schema(),
            &cdc_column,
            &layout,
            options.schema_evolution,
        )?;
        if let Some(new_schema) = new_schema {
            if !allow_evolve {
                return Ok(table);
            }
            tracing::info!(
                target = %format!("{target_namespace}.{target_table}"),
                old_columns = table.schema().fields().len(),
                new_columns = new_schema.fields().len(),
                "evolving MySQL target schema"
            );
            let published = publish_evolved_schema(meta, &info, &new_schema)
                .await
                .map_err(|error| {
                    report!(
                        "update target schema {target_namespace}.{target_table}: {error}"
                    )
                })?;
            if !published {
                if retries == 0 {
                    bail!(
                        "target table {target_namespace}.{target_table} was redefined \
                         concurrently while its schema was being evolved; retry"
                    );
                }
                tracing::info!(
                    target = %format!("{target_namespace}.{target_table}"),
                    "target redefined concurrently; re-reading it and evolving again"
                );
                return Box::pin(ensure_target(
                    meta,
                    source,
                    target_namespace,
                    target_table,
                    options,
                    allow_create,
                    allow_evolve,
                    retries - 1,
                ))
                .await;
            }
            return LakeSoulTable::for_namespace_and_name(
                target_namespace,
                &table_name,
                Some(meta.clone()),
            )
            .await
            .map_err(|error| {
                report!(
                    "reopen target table {target_namespace}.{target_table} after \
                     schema evolution: {error}"
                )
            });
        }
        return Ok(table);
    }

    if !options.auto_create {
        bail!(
            "target table {target_namespace}.{target_table} does not exist; \
             create it first or set auto_create = true"
        );
    }
    if !allow_create {
        bail!(
            "target table {target_namespace}.{target_table} does not exist but the \
             subscription is already registered; dropping the target is not resumable"
        );
    }
    let warehouse = options.warehouse_prefix.as_deref().ok_or_else(|| {
        report!(
            "auto_create needs a warehouse prefix: start postgres-lakesoul \
             with --warehouse-prefix"
        )
    })?;
    validate_target_name(target_namespace, "target namespace")?;
    validate_target_name(&table_name, "target table")?;

    let schema = source_schema(source, &options.cdc_column)?;
    let (table_schema, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
        schema_to_metadata_parts(&schema);
    let table_path = format!(
        "{}/{}/{}",
        warehouse.trim_end_matches('/'),
        target_namespace,
        table_name
    );
    let table_info = TableInfo {
        table_id: format!("table_{}", uuid::Uuid::new_v4()),
        table_namespace: target_namespace.to_string(),
        table_name: table_name.clone(),
        table_schema,
        table_schema_arrow_ipc,
        table_schema_arrow_ipc_json_hash,
        properties: serde_json::to_string(&LakeSoulTableProperty {
            hash_bucket_num: Some(DEFAULT_HASH_BUCKET_NUM.to_string()),
            cdc_change_column: Some(options.cdc_column.clone()),
            use_cdc: Some("true".to_string()),
            file_format: Some(DEFAULT_FILE_FORMAT.to_string()),
            ..Default::default()
        })
        .map_err(|error| report!("serialize target table properties: {error}"))?,
        partitions: format!(";{}", source.primary_key.join(",")),
        table_path,
        domain: "public".to_string(),
    };

    let created = meta
        .create_table_if_not_exists(table_info.clone())
        .await
        .map_err(|error| {
            report!("create target table {target_namespace}.{target_table}: {error}")
        })?;
    if !created {
        return Box::pin(ensure_target(
            meta,
            source,
            target_namespace,
            target_table,
            options,
            false,
            allow_evolve,
            retries,
        ))
        .await;
    }
    tracing::info!(
        target = %format!("{target_namespace}.{target_table}"),
        path = %table_info.table_path,
        "created MySQL target table"
    );
    LakeSoulTable::for_namespace_and_name(
        target_namespace,
        &table_name,
        Some(meta.clone()),
    )
    .await
    .map_err(|error| {
        report!(
            "open newly created target table {target_namespace}.{target_table}: {error}"
        )
    })
}

pub(crate) fn source_schema(source: &MySqlTable, cdc_column: &str) -> Result<SchemaRef> {
    if source
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(cdc_column))
    {
        bail!(
            "source table {} already has a column named {cdc_column:?}; choose \
             another cdc_column for the subscription",
            source.qualified_name()
        );
    }
    let mut fields = Vec::with_capacity(source.columns.len() + 1);
    for column in &source.columns {
        let data_type = column.data_type.arrow_type()?;
        let nullable = column.nullable && !column.primary_key;
        fields.push(Field::new(column.name.clone(), data_type, nullable));
    }
    fields.push(Field::new(cdc_column, DataType::Utf8, true));
    Ok(Arc::new(Schema::new(fields)))
}

/// Compare the MySQL source with the target. A source type must already fit
/// the target column ([`target_holds`]); the target type is never changed.
/// Appending a nullable column and relaxing `NOT NULL` are applied through
/// [`check_evolution`], so the target is held to the same rule as
/// `ALTER TABLE` and the other LakeSoul writers.
fn evolved_schema(
    source: &MySqlTable,
    target: SchemaRef,
    cdc_column: &str,
    layout: &TableLayout,
    mode: SchemaEvolution,
) -> Result<Option<SchemaRef>> {
    if mode == SchemaEvolution::None {
        return Ok(None);
    }
    let apply = |schema: &Schema, change: ColumnChange| {
        check_evolution(schema, &change, layout)
            .map_err(|error| report!("cannot evolve the target schema: {error}"))
    };
    let cdc_index = target.index_of(cdc_column).map_err(|error| {
        report!("target table has no CDC column {cdc_column:?}: {error}")
    })?;
    if !matches!(target.field(cdc_index).data_type(), DataType::Utf8) {
        bail!("target CDC column {cdc_column:?} is not Utf8");
    }

    let mut schema: Schema = target.as_ref().clone();
    let mut changed = false;

    for column in &source.columns {
        let source_type = column.data_type.arrow_type()?;
        let source_nullable = column.nullable && !column.primary_key;
        if let Ok(index) = schema.index_of(&column.name) {
            let target_type = schema.field(index).data_type().clone();
            if target_type != source_type && !target_holds(&target_type, &source_type) {
                bail!(
                    "source column {:?} changed type from {target_type} to {source_type}; \
                     P0 does not narrow or cross-cast target columns",
                    column.name
                );
            }
            if source_nullable && !schema.field(index).is_nullable() {
                schema = apply(&schema, ColumnChange::DropNotNull(column.name.clone()))?;
                changed = true;
            }
        } else {
            if column.primary_key {
                bail!(
                    "source primary-key column {:?} is missing from the target; \
                     primary-key changes require a resnapshot",
                    column.name
                );
            }
            schema = apply(
                &schema,
                ColumnChange::Add(Field::new(column.name.clone(), source_type, true)),
            )?;
            changed = true;
        }
    }

    let source_names: std::collections::BTreeSet<&str> = source
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    for (index, field) in schema.fields().iter().enumerate() {
        if index == cdc_index || source_names.contains(field.name().as_str()) {
            continue;
        }
        bail!(
            "target column {:?} no longer exists in the MySQL source; DROP/RENAME \
             COLUMN requires a resnapshot",
            field.name()
        );
    }

    Ok(changed.then(|| Arc::new(schema)))
}

/// Whether the existing `target` column type already holds every value of
/// the MySQL `source` type (including MySQL's `TINYINT` and unsigned types).
/// Unlike [`lakesoul_common::schema_evolution::can_widen`], which decides
/// whether a target type may be *changed*, this never changes the target.
fn target_holds(target: &DataType, source: &DataType) -> bool {
    if target == source {
        return true;
    }
    matches!(
        (target, source),
        (DataType::Int16, DataType::Int8)
            | (DataType::Int32, DataType::Int8 | DataType::Int16)
            | (
                DataType::Int64,
                DataType::Int8 | DataType::Int16 | DataType::Int32
            )
            | (DataType::UInt16, DataType::UInt8)
            | (DataType::UInt32, DataType::UInt8 | DataType::UInt16)
            | (
                DataType::UInt64,
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32
            )
            | (DataType::Float64, DataType::Float32)
    ) || matches!(
        (target, source),
        (
            DataType::Decimal128(to_precision, to_scale),
            DataType::Decimal128(from_precision, from_scale),
        ) if *to_scale >= *from_scale
            && i16::from(*to_precision) - i16::from(*to_scale)
                >= i16::from(*from_precision) - i16::from(*from_scale)
    )
}

fn table_cdc_column(info: &TableInfo) -> Result<String> {
    let properties: serde_json::Value =
        serde_json::from_str(&info.properties).map_err(|error| {
            report!("target table properties are not valid JSON: {error}")
        })?;
    let use_cdc = property_string(&properties, "use_cdc");
    if use_cdc.as_deref() != Some("true") {
        bail!(
            "target table {} is not a CDC table; create it WITH (use_cdc = true, \
             lakesoul_cdc_change_column = 'rowKinds')",
            info.table_name
        );
    }
    property_string(&properties, "lakesoul_cdc_change_column")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            report!(
                "target table {} has no lakesoul_cdc_change_column property",
                info.table_name
            )
        })
}

fn property_string(properties: &serde_json::Value, key: &str) -> Option<String> {
    properties.get(key).and_then(|value| {
        value
            .as_str()
            .map(str::to_string)
            .or_else(|| value.as_bool().map(|value| value.to_string()))
    })
}

fn validate_target_name(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
    {
        bail!(
            "{label} {value:?} must contain only ASCII letters, digits and underscores"
        );
    }
    Ok(())
}
