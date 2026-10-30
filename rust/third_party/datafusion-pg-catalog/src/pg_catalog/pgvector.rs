//! pgvector support for the `pg_catalog` schema.
//!
//! DataFusion-backed pgvector clients resolve unknown result-type OIDs through
//! `pg_catalog.pg_type`. To make the pgvector `vector` type resolvable, this
//! module injects a `vector` row into the static `pg_type` table and tags
//! `pg_type.typtype` with the internal Postgres `"char"` wire type.
//!
//! The vector row intentionally lives in `pg_catalog` (namespace OID 11) so the
//! `pg_type`/`pg_namespace` join performed by driver introspection resolves
//! without depending on user-schema OIDs.

use std::sync::Arc;

use arrow_pg::datatypes::PG_VECTOR_TYPE_OID;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;

use super::{ArrowTable, PgCatalogStaticTables};

/// Canonical OID of the `pg_catalog` namespace (`PG_CATALOG_NAMESPACE`).
const PG_CATALOG_NAMESPACE_OID: i32 = 11;

/// Return `tables` with the pgvector pieces needed for driver introspection:
///
/// * a `vector` type row (OID 16385) in `pg_type`, and
/// * the internal `"char"` wire type on `pg_type.typtype`.
pub(crate) fn with_pg_vector_support(
    mut tables: PgCatalogStaticTables,
) -> Result<PgCatalogStaticTables> {
    // 1. Tag pg_type.typtype as a Postgres internal `"char"` column so the wire
    //    layer encodes it correctly for clients decoding `typtype`.
    let pg_type = Arc::new(with_field_metadata(
        &tables.pg_type,
        "typtype",
        "pg.char",
        "char",
    )?);

    // 2. Append the `vector` row to pg_type (all other columns get safe
    //    defaults; the introspection query only reads the ones we set).
    let pg_type = Arc::new(append_row(&pg_type, |field, scalar| {
        match field.name().as_str() {
            "oid" => *scalar = ScalarValue::Int32(Some(PG_VECTOR_TYPE_OID as i32)),
            "typname" => *scalar = ScalarValue::Utf8(Some("vector".to_string())),
            "typtype" => *scalar = ScalarValue::Utf8(Some("b".to_string())),
            "typnamespace" => *scalar = ScalarValue::Int32(Some(PG_CATALOG_NAMESPACE_OID)),
            _ => {}
        }
    })?);

    tables.pg_type = pg_type;
    Ok(tables)
}

/// Rebuild `table` with `key = value` metadata added to the field named `name`.
///
/// Arrow keeps field metadata in the schema; this rebuilds the schema and every
/// record batch so the wire layer (`arrow-pg`) sees the marker when encoding.
fn with_field_metadata(
    table: &ArrowTable,
    name: &str,
    key: &str,
    value: &str,
) -> Result<ArrowTable> {
    let schema = table.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == name {
                let mut metadata = field.metadata().clone();
                metadata.insert(key.to_string(), value.to_string());
                (**field).clone().with_metadata(metadata)
            } else {
                (**field).clone()
            }
        })
        .collect::<Vec<_>>();
    let new_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    let mut batches = Vec::with_capacity(table.data().len());
    for batch in table.data() {
        batches.push(
            RecordBatch::try_new(Arc::clone(&new_schema), batch.columns().to_vec())
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        );
    }
    Ok(ArrowTable {
        schema: new_schema,
        data: batches,
    })
}

/// Return a copy of `table` with one extra row appended. `fill` lets callers
/// override the default scalar produced for each column.
///
/// `fill` is invoked for every field with a fresh default scalar (int 0 /
/// string "" / bool false / ...), matching the field's data type; non-default
/// catalog values are set by the caller.
fn append_row(
    table: &ArrowTable,
    mut fill: impl FnMut(&Field, &mut ScalarValue),
) -> Result<ArrowTable> {
    let schema = table.schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let mut scalar = default_scalar(field.data_type())?;
            fill(field, &mut scalar);
            scalar.to_array_of_size(1)
        })
        .collect::<Result<Vec<_>>>()?;
    let row = RecordBatch::try_new(Arc::clone(&schema), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let mut data = table.data().to_vec();
    data.push(row);
    Ok(ArrowTable {
        schema: Arc::clone(&schema),
        data,
    })
}

/// A single default [`ScalarValue`] for a catalog column of `data_type`.
///
/// Only the data types present in the exported `pg_type` schema are handled;
/// anything else yields a `NotImplemented` error rather than guessing.
fn default_scalar(data_type: &DataType) -> Result<ScalarValue> {
    Ok(match data_type {
        DataType::Null => ScalarValue::Null,
        DataType::Boolean => ScalarValue::Boolean(Some(false)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::Utf8 => ScalarValue::Utf8(Some(String::new())),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(Some(String::new())),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "no default catalog scalar for {other:?}"
            )));
        }
    })
}
