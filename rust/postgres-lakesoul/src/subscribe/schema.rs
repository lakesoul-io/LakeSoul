// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Target-table creation and schema evolution.
//!
//! PostgreSQL logical replication does not carry DDL. The subscriber learns
//! about a schema change from the next `Relation` message and from the source
//! catalog it inspects at startup. This module decides which source changes
//! can be projected onto a LakeSoul table:
//!
//! * a missing nullable column is appended;
//! * an integer, float or decimal type may widen;
//! * `NOT NULL` may be relaxed.
//!
//! The target schema never moves backwards: columns are never dropped, types
//! are never narrowed and nullability is never tightened. Historical `Relation`
//! versions can therefore be decoded against the newer target schema by
//! filling newly added columns with `NULL` and casting old types.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use lakesoul_common::schema_evolution::{
    ColumnChange, TableLayout, can_widen, check_evolution, decimal_accepts,
};
use lakesoul_common::ser::arrow_java::schema_to_metadata_parts;
use lakesoul_datafusion::catalog::LakeSoulTableProperty;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_datafusion::lakesoul_table::helpers::case_fold_table_name;
use lakesoul_metadata::{
    LakeSoulMetaDataError, MetaDataClientRef, TableDefinitionUpdate,
};
use lakesoul_metadata_proto::entity::TableInfo;
use pg_walstream::RelationColumn;
use rootcause::{bail, report};

use super::source::{PgType, SourceColumn, SourceTable};
use super::{SchemaEvolution, SubscribeConfig};
use crate::Result;

const DEFAULT_HASH_BUCKET_NUM: &str = "4";
const DEFAULT_FILE_FORMAT: &str = "vortex-compact";

/// Everything target creation and evolution needs from the subscription
/// definition. The SQL path constructs this directly; the CLI path derives it
/// from [`SubscribeConfig`].
#[derive(Debug, Clone)]
pub(crate) struct TargetOptions {
    pub auto_create: bool,
    pub schema_evolution: SchemaEvolution,
    pub warehouse_prefix: Option<String>,
    pub cdc_column: String,
}

impl TargetOptions {
    pub(crate) fn from_config(config: &SubscribeConfig) -> Self {
        Self {
            auto_create: config.auto_create,
            schema_evolution: config.schema_evolution,
            warehouse_prefix: config.warehouse_prefix.clone(),
            cdc_column: config.cdc_column.clone(),
        }
    }
}

/// Create the target when it is missing and evolve it to the current source
/// schema. `allow_create` is false once a subscription is registered: a
/// missing target then means the table was dropped, and recreating it would
/// change its identity instead of resuming the subscription. `allow_evolve`
/// is false for `--dry-run`, which validates without changing target metadata.
pub(crate) async fn ensure_target(
    meta: &MetaDataClientRef,
    source: &SourceTable,
    target_namespace: &str,
    target_table: &str,
    options: &TargetOptions,
    allow_create: bool,
    allow_evolve: bool,
) -> Result<LakeSoulTable> {
    ensure_target_with_retries(
        meta,
        source,
        target_namespace,
        target_table,
        options,
        allow_create,
        allow_evolve,
        EVOLVE_RETRIES,
    )
    .await
}

/// How many times an evolution re-reads the target after a concurrent
/// redefinition before giving up.
pub(crate) const EVOLVE_RETRIES: usize = 1;

#[allow(clippy::too_many_arguments)]
async fn ensure_target_with_retries(
    meta: &MetaDataClientRef,
    source: &SourceTable,
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
        let new_schema = evolved_schema_for_layout(
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
                "evolving target table schema"
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
                return Box::pin(ensure_target_with_retries(
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
             subscription is already registered; dropping the target is not \
             resumable. Recreate the subscription or restore the target"
        );
    }
    let warehouse = options.warehouse_prefix.as_deref().ok_or_else(|| {
        report!(
            "auto_create needs a warehouse prefix: start postgres-lakesoul \
             with --warehouse-prefix, or pass --warehouse-prefix to the CLI"
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
        // Another process created it between the lookup and the insert. Open
        // and evolve the winner instead of failing the subscription.
        return Box::pin(ensure_target_with_retries(
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
        "created target table"
    );
    LakeSoulTable::for_namespace_and_name(
        target_namespace,
        &table_name,
        Some(meta.clone()),
    )
    .await
    .map_err(|error| {
        report!(
            "open newly created target table {target_namespace}.{target_table}: \
             {error}"
        )
    })
}

/// Build the target schema for a newly created table.
pub(crate) fn source_schema(source: &SourceTable, cdc_column: &str) -> Result<SchemaRef> {
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
        let data_type = source_arrow_type(column)?;
        let nullable = !column.not_null && !source.primary_key.contains(&column.name);
        fields.push(Field::new(column.name.clone(), data_type, nullable));
    }
    fields.push(Field::new(cdc_column, DataType::Utf8, true));
    Ok(Arc::new(Schema::new(fields)))
}

/// [`evolved_schema_for_layout`] for a target laid out the way the subscriber
/// creates it: hashed by the source primary key, no range partitions.
#[cfg(test)]
pub(crate) fn evolved_schema(
    source: &SourceTable,
    target: SchemaRef,
    cdc_column: &str,
    mode: SchemaEvolution,
) -> Result<Option<SchemaRef>> {
    let layout = TableLayout {
        primary_keys: source.primary_key.clone(),
        range_partitions: Vec::new(),
        cdc_column: Some(cdc_column.to_string()),
    };
    evolved_schema_for_layout(source, target, cdc_column, &layout, mode)
}

/// Compare the current source schema with the target schema. Returns a new
/// target schema only when an evolution is needed. Historical target columns
/// are never removed.
///
/// Which source changes to follow is decided here (the source is the
/// authority on types and nullability); every change itself is applied
/// through [`check_evolution`], so the target is held to the same rule as
/// `ALTER TABLE` and the other LakeSoul writers, including the refusal to
/// change a column `layout` depends on.
pub(crate) fn evolved_schema_for_layout(
    source: &SourceTable,
    target: SchemaRef,
    cdc_column: &str,
    layout: &TableLayout,
    mode: SchemaEvolution,
) -> Result<Option<SchemaRef>> {
    let apply = |schema: &Schema, change: ColumnChange| {
        check_evolution(schema, &change, layout)
            .map_err(|error| report!("cannot evolve the target schema: {error}"))
    };
    let cdc_index = target.index_of(cdc_column).map_err(|error| {
        report!("target table has no CDC column {cdc_column:?}: {error}")
    })?;
    if !matches!(target.field(cdc_index).data_type(), DataType::Utf8) {
        bail!(
            "target CDC column {cdc_column} is {:?}, not text",
            target.field(cdc_index).data_type()
        );
    }

    let mut schema: Schema = target.as_ref().clone();
    let mut changed = false;
    let mut seen = vec![false; source.columns.len()];

    for (target_index, field) in target.fields().iter().enumerate() {
        if target_index == cdc_index {
            continue;
        }
        let Some(source_index) = source
            .columns
            .iter()
            .position(|column| column.name == *field.name())
        else {
            bail!(
                "source table {} no longer has target column {}; dropping or \
                 renaming a column is not supported by subscription schema \
                 evolution. Recreate the subscription with a new target and \
                 resnapshot",
                source.qualified_name(),
                field.name()
            );
        };
        seen[source_index] = true;
        let source_column = &source.columns[source_index];

        if source.primary_key.contains(&source_column.name) {
            if !pk_type_matches(source_column, field.data_type()) {
                bail!(
                    "source primary key column {} changed type to {}; primary-key \
                     type changes are not supported",
                    source_column.name,
                    source_column.type_name
                );
            }
            if field.is_nullable() {
                bail!("target primary key column {} is nullable", field.name());
            }
            continue;
        }

        if !target_accepts(source_column, field.data_type()) {
            if mode != SchemaEvolution::Auto {
                bail!(
                    "column {} has source type {} but target type {}; schema \
                     evolution is disabled",
                    source_column.name,
                    source_column.type_name,
                    field.data_type()
                );
            }
            let source_type = source_arrow_type(source_column)?;
            if field.data_type() == &source_type
                || !can_widen(field.data_type(), &source_type)
            {
                bail!(
                    "column {} changed from target type {} to source type {}; \
                     this type change is not a safe widening",
                    source_column.name,
                    field.data_type(),
                    source_column.type_name
                );
            }
            schema = apply(
                &schema,
                ColumnChange::Widen {
                    name: field.name().clone(),
                    to: source_type,
                },
            )?;
            changed = true;
        }

        // Columns are only ever appended, so the index still addresses this
        // column in the evolving schema.
        if !source_column.not_null && !schema.field(target_index).is_nullable() {
            if mode != SchemaEvolution::Auto {
                bail!(
                    "source column {} became nullable but target column is NOT NULL; \
                     schema evolution is disabled",
                    source_column.name
                );
            }
            schema = apply(&schema, ColumnChange::DropNotNull(field.name().clone()))?;
            changed = true;
        }
    }

    for (index, source_column) in source.columns.iter().enumerate() {
        if seen[index] {
            continue;
        }
        if mode != SchemaEvolution::Auto {
            bail!(
                "source table {} has column {} that the target does not expose; \
                 schema evolution is disabled",
                source.qualified_name(),
                source_column.name
            );
        }
        if source.primary_key.contains(&source_column.name) {
            bail!(
                "target table does not expose source primary key column {}; \
                 primary-key changes are not supported",
                source_column.name
            );
        }
        if source_column.has_default {
            bail!(
                "source column {} was added with a default; backfilling existing \
                 target rows is not supported. Add the column without a default \
                 or recreate the target",
                source_column.name
            );
        }
        let data_type = source_arrow_type(source_column)?;
        schema = apply(
            &schema,
            ColumnChange::Add(Field::new(source_column.name.clone(), data_type, true)),
        )?;
        changed = true;
    }

    Ok(changed.then(|| Arc::new(schema)))
}

/// Publish an evolved target schema, but only onto the definition `info` the
/// evolution was computed from. Returns `false` when the target was
/// redefined concurrently (nothing was written): the caller re-reads the
/// target and evolves again.
pub(crate) async fn publish_evolved_schema(
    meta: &MetaDataClientRef,
    info: &TableInfo,
    schema: &Schema,
) -> std::result::Result<bool, LakeSoulMetaDataError> {
    match meta
        .alter_table_definition(
            info,
            &TableDefinitionUpdate {
                schema: Some(schema_to_metadata_parts(schema)),
                properties: None,
            },
        )
        .await
    {
        Ok(()) => Ok(true),
        Err(LakeSoulMetaDataError::CommitConflict(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Convert one `Relation` message into the source columns it describes.
/// Metadata that is not in the protocol (`not_null`, `toastable`, default)
/// comes from the current source catalog by name.
pub(crate) fn relation_columns(
    relation_columns: &[RelationColumn],
    source: &SourceTable,
) -> Result<Vec<SourceColumn>> {
    let mut columns = Vec::with_capacity(relation_columns.len());
    for relation in relation_columns {
        let Some(current) = source
            .columns
            .iter()
            .find(|column| column.name == relation.name.as_ref())
        else {
            bail!(
                "source table {} no longer has column {}; dropping or renaming \
                 a column is not supported",
                source.qualified_name(),
                relation.name
            );
        };
        let pg_type = PgType::from_oid(relation.type_id, relation.type_modifier)
            .ok_or_else(|| {
                report!(
                    "column {} has unsupported type oid {} in the Relation message",
                    relation.name,
                    relation.type_id
                )
            })?;
        let mut column = current.clone();
        column.type_id = relation.type_id;
        column.type_modifier = relation.type_modifier;
        column.pg_type = pg_type;
        columns.push(column);
    }
    for key in &source.primary_key {
        if !columns.iter().any(|column| column.name == *key) {
            bail!(
                "primary key column {} is missing from the Relation message for {}",
                key,
                source.qualified_name()
            );
        }
    }
    Ok(columns)
}

pub(crate) fn check_target_type(source: &SourceColumn, target: &DataType) -> Result<()> {
    if target_accepts(source, target) {
        Ok(())
    } else {
        bail!(
            "column {} has source type {} but target type {target}; the target \
             schema is not compatible",
            source.name,
            source.type_name
        )
    }
}

/// Whether `target` can represent every value of `source` without narrowing.
pub(crate) fn target_accepts(source: &SourceColumn, target: &DataType) -> bool {
    match &source.pg_type {
        PgType::Bool => matches!(target, DataType::Boolean),
        PgType::Int2 => {
            matches!(target, DataType::Int16 | DataType::Int32 | DataType::Int64)
        }
        PgType::Int4 => matches!(target, DataType::Int32 | DataType::Int64),
        PgType::Int8 => matches!(target, DataType::Int64),
        PgType::Float4 => matches!(target, DataType::Float32 | DataType::Float64),
        PgType::Float8 => matches!(target, DataType::Float64),
        PgType::Numeric { precision, scale } => match target {
            DataType::Decimal128(target_precision, target_scale) => {
                match (precision, scale) {
                    (Some(precision), Some(scale)) => decimal_accepts(
                        *target_precision,
                        *target_scale,
                        *precision,
                        *scale,
                    ),
                    // The source does not constrain precision or scale. Keep
                    // accepting the explicitly chosen target type, as the
                    // pre-schema-evolution subscriber did.
                    _ => true,
                }
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
    }
}

/// Primary-key changes are rejected; a wider target is not a valid substitute
/// for the source key type either.
pub(crate) fn pk_type_matches(source: &SourceColumn, target: &DataType) -> bool {
    match &source.pg_type {
        PgType::Numeric {
            precision: None,
            scale: None,
        } => {
            matches!(target, DataType::Decimal128(_, _))
        }
        _ => source_arrow_type(source).is_ok_and(|data_type| data_type == *target),
    }
}

/// PG type to Arrow type for a column that is being added to the target.
/// An unconstrained `numeric` has no safe Arrow representation, so creation
/// and column addition require an explicitly typed numeric.
pub(crate) fn source_arrow_type(source: &SourceColumn) -> Result<DataType> {
    Ok(match &source.pg_type {
        PgType::Bool => DataType::Boolean,
        PgType::Int2 => DataType::Int16,
        PgType::Int4 => DataType::Int32,
        PgType::Int8 => DataType::Int64,
        PgType::Float4 => DataType::Float32,
        PgType::Float8 => DataType::Float64,
        PgType::Numeric {
            precision: Some(precision),
            scale: Some(scale),
        } => {
            if *precision == 0
                || *precision > 38
                || *scale < 0
                || *scale as u8 > *precision
            {
                bail!(
                    "column {} has numeric({precision},{scale}), which LakeSoul's \
                     decimal128 cannot represent (precision 1..=38, scale \
                     0..=precision)",
                    source.name
                );
            }
            DataType::Decimal128(*precision, *scale)
        }
        PgType::Numeric { .. } => bail!(
            "column {} has an unconstrained numeric type; create the target \
             with an explicit numeric(precision, scale)",
            source.name
        ),
        PgType::Text | PgType::Uuid | PgType::Json | PgType::Jsonb => DataType::Utf8,
        PgType::Bytea => DataType::Binary,
        PgType::Date => DataType::Date32,
        PgType::Time => DataType::Time64(TimeUnit::Microsecond),
        PgType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        PgType::TimestampTz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        }
    })
}

fn table_cdc_column(info: &TableInfo) -> Result<String> {
    let properties: serde_json::Value =
        serde_json::from_str(&info.properties).map_err(|error| {
            report!("target table properties are not valid JSON: {error}")
        })?;
    let use_cdc = properties.get("use_cdc").and_then(|value| {
        value
            .as_str()
            .map(ToString::to_string)
            .or_else(|| value.as_bool().map(|value| value.to_string()))
    });
    if use_cdc.as_deref() != Some("true") {
        bail!(
            "target table {}.{} is not a CDC table; create it WITH (use_cdc = true, \
             lakesoul_cdc_change_column = 'rowKinds')",
            info.table_namespace,
            info.table_name
        );
    }
    properties
        .get("lakesoul_cdc_change_column")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| {
            report!(
                "target table {}.{} has no lakesoul_cdc_change_column property",
                info.table_namespace,
                info.table_name
            )
        })
}

fn validate_target_name(value: &str, what: &str) -> Result<()> {
    if value.is_empty() || value == "." || value == ".." {
        bail!("{what} {value:?} is not supported");
    }
    if let Some(character) = value.chars().find(|character| {
        matches!(character, '/' | '\\' | '?' | '#' | '%' | ',' | ';')
            || character.is_control()
    }) {
        bail!(
            "{what} {value:?} contains {character:?}, which LakeSoul cannot \
             represent in a table path"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_column(name: &str, pg_type: PgType, type_name: &str) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            type_id: 0,
            type_modifier: -1,
            pg_type,
            type_name: type_name.to_string(),
            not_null: false,
            has_default: false,
            toastable: false,
        }
    }

    fn source_table(columns: Vec<SourceColumn>) -> SourceTable {
        SourceTable {
            schema: "public".to_string(),
            name: "users".to_string(),
            relation_oid: 1,
            columns,
            primary_key: vec!["id".to_string()],
        }
    }

    #[test]
    fn integer_widening_is_allowed() {
        assert!(can_widen(&DataType::Int16, &DataType::Int32));
        assert!(can_widen(&DataType::Int32, &DataType::Int64));
        assert!(!can_widen(&DataType::Int64, &DataType::Int32));
    }

    #[test]
    fn decimal_widening_keeps_integer_digits() {
        assert!(can_widen(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(12, 4)
        ));
        assert!(!can_widen(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(11, 4)
        ));
        assert!(!can_widen(
            &DataType::Decimal128(12, 2),
            &DataType::Decimal128(10, 2)
        ));
    }

    #[test]
    fn nullable_columns_are_appended() {
        let source = source_table(vec![
            SourceColumn {
                not_null: true,
                ..source_column("id", PgType::Int8, "int8")
            },
            source_column("name", PgType::Text, "text"),
        ]);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("rowKinds", DataType::Utf8, true),
        ]));
        let schema = evolved_schema(&source, target, "rowKinds", SchemaEvolution::Auto)
            .unwrap()
            .expect("evolved");
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(schema.field_with_name("name").unwrap().is_nullable());
    }

    #[test]
    fn dropped_columns_are_rejected() {
        let source = source_table(vec![SourceColumn {
            not_null: true,
            ..source_column("id", PgType::Int8, "int8")
        }]);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("rowKinds", DataType::Utf8, true),
        ]));
        let error = evolved_schema(&source, target, "rowKinds", SchemaEvolution::Auto)
            .unwrap_err();
        assert!(format!("{error}").contains("dropping or renaming"));
    }

    #[test]
    fn widening_rewrites_the_existing_target_column() {
        let source = source_table(vec![
            SourceColumn {
                not_null: true,
                ..source_column("id", PgType::Int8, "int8")
            },
            source_column("age", PgType::Int8, "int8"),
        ]);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int32, true),
            Field::new("rowKinds", DataType::Utf8, true),
        ]));
        let schema = evolved_schema(&source, target, "rowKinds", SchemaEvolution::Auto)
            .unwrap()
            .expect("evolved");
        assert_eq!(
            schema.field_with_name("age").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn dropped_not_null_is_relaxed() {
        let source = source_table(vec![
            SourceColumn {
                not_null: true,
                ..source_column("id", PgType::Int8, "int8")
            },
            source_column("name", PgType::Text, "text"),
        ]);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("rowKinds", DataType::Utf8, true),
        ]));
        let schema = evolved_schema(&source, target, "rowKinds", SchemaEvolution::Auto)
            .unwrap()
            .expect("evolved");
        assert!(schema.field_with_name("name").unwrap().is_nullable());
    }

    /// A pre-existing target may be range partitioned. The shared rule
    /// refuses to change a partition column, which the subscriber's own
    /// checks never covered.
    #[test]
    fn range_partition_columns_are_not_evolved() {
        let source = source_table(vec![
            SourceColumn {
                not_null: true,
                ..source_column("id", PgType::Int8, "int8")
            },
            source_column("region", PgType::Int8, "int8"),
        ]);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Int32, false),
            Field::new("rowKinds", DataType::Utf8, true),
        ]));
        let layout =
            TableLayout::from_partitions("region;id", Some("rowKinds".to_string()));
        let error = evolved_schema_for_layout(
            &source,
            target,
            "rowKinds",
            &layout,
            SchemaEvolution::Auto,
        )
        .unwrap_err();
        assert!(format!("{error}").contains("range-partition"), "{error}");
    }

    /// The publication only applies onto the definition the evolution was
    /// computed from; a stale one is reported, not overwritten. Needs the
    /// metadata PostgreSQL configured through `LAKESOUL_PG_*`.
    #[tokio::test]
    async fn publication_refuses_a_stale_definition() {
        let meta: MetaDataClientRef =
            Arc::new(lakesoul_metadata::MetaDataClient::from_env().await.unwrap());
        let base = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("rowKinds", DataType::Utf8, true),
        ]);
        let (table_schema, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
            schema_to_metadata_parts(&base);
        let name = format!("subscribe_publish_{}", uuid::Uuid::new_v4().simple());
        let created = TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: name.clone(),
            table_namespace: "default".to_string(),
            table_path: format!("file:///tmp/{name}"),
            table_schema,
            table_schema_arrow_ipc,
            table_schema_arrow_ipc_json_hash,
            properties: r#"{"use_cdc":"true","lakesoul_cdc_change_column":"rowKinds"}"#
                .to_string(),
            partitions: ";id".to_string(),
            domain: "public".to_string(),
        };
        meta.create_table(created.clone()).await.unwrap();
        let info = meta
            .get_table_info_by_table_id(&created.table_id)
            .await
            .unwrap()
            .unwrap();
        let add = |name: &str| {
            let mut fields = base.fields().to_vec();
            fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
            Schema::new(fields)
        };

        assert!(
            publish_evolved_schema(&meta, &info, &add("first"))
                .await
                .unwrap()
        );
        // `info` is now stale: the second publication must not land.
        assert!(
            !publish_evolved_schema(&meta, &info, &add("second"))
                .await
                .unwrap()
        );
        let stored = meta
            .get_table_info_by_table_id(&info.table_id)
            .await
            .unwrap()
            .unwrap();
        assert!(stored.table_schema.contains("first"));
        assert!(!stored.table_schema.contains("second"));
        meta.delete_table_by_table_info_cascade(&stored)
            .await
            .unwrap();
    }

    #[test]
    fn default_added_columns_are_rejected() {
        let source = source_table(vec![
            SourceColumn {
                not_null: true,
                ..source_column("id", PgType::Int8, "int8")
            },
            SourceColumn {
                has_default: true,
                ..source_column("name", PgType::Text, "text")
            },
        ]);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("rowKinds", DataType::Utf8, true),
        ]));
        let error = evolved_schema(&source, target, "rowKinds", SchemaEvolution::Auto)
            .unwrap_err();
        assert!(format!("{error}").contains("with a default"));
    }
}
