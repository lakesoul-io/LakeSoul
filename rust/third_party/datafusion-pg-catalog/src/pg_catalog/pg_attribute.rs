use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use postgres_types::{Oid, Type};
use tokio::sync::RwLock;

use crate::pg_catalog::catalog_info::CatalogInfo;

use super::oid_field;
use super::{OidCacheKey, resolve_oid};

#[derive(Debug, Clone)]
pub(crate) struct PgAttributeTable<C> {
    schema: SchemaRef,
    catalog_list: C,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
}

impl<C: CatalogInfo> PgAttributeTable<C> {
    pub(crate) fn new(
        catalog_list: C,
        oid_counter: Arc<AtomicU32>,
        oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    ) -> Self {
        // Define the schema for pg_attribute
        // This matches PostgreSQL's pg_attribute table columns
        let schema = Arc::new(Schema::new(vec![
            oid_field::oid_field("attrelid", oid_field::kind::REGCLASS, false), // OID of the relation this column belongs to
            Field::new("attname", DataType::Utf8, false),                       // Column name
            oid_field::oid_field("atttypid", oid_field::kind::REGTYPE, false), // OID of the column data type
            Field::new("attstattarget", DataType::Int32, false),               // Statistics target
            Field::new("attlen", DataType::Int16, false),                      // Length of the type
            Field::new("attnum", DataType::Int16, false), // Column number (positive for regular columns)
            Field::new("attndims", DataType::Int32, false), // Number of dimensions for array types
            Field::new("attcacheoff", DataType::Int32, false), // Cache offset
            Field::new("atttypmod", DataType::Int32, false), // Type-specific modifier
            Field::new("attbyval", DataType::Boolean, false), // True if the type is pass-by-value
            Field::new("attalign", DataType::Utf8, false), // Type alignment
            Field::new("attstorage", DataType::Utf8, false), // Storage type
            Field::new("attcompression", DataType::Utf8, true), // Compression method
            Field::new("attnotnull", DataType::Boolean, false), // True if column cannot be null
            Field::new("atthasdef", DataType::Boolean, false), // True if column has a default value
            Field::new("atthasmissing", DataType::Boolean, false), // True if column has missing values
            Field::new("attidentity", DataType::Utf8, false),      // Identity column type
            Field::new("attgenerated", DataType::Utf8, false),     // Generated column type
            Field::new("attisdropped", DataType::Boolean, false), // True if column has been dropped
            Field::new("attislocal", DataType::Boolean, false), // True if column is local to this relation
            Field::new("attinhcount", DataType::Int32, false), // Number of direct inheritance ancestors
            Field::new("attcollation", DataType::Int32, false), // OID of collation
            Field::new("attacl", DataType::Utf8, true),        // Access privileges
            Field::new("attoptions", DataType::Utf8, true),    // Attribute-level options
            Field::new("attfdwoptions", DataType::Utf8, true), // Foreign data wrapper options
            Field::new("attmissingval", DataType::Utf8, true), // Missing value for added columns
        ]));

        Self {
            schema,
            catalog_list,
            oid_counter,
            oid_cache,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: Self) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut attrelids = Vec::new();
        let mut attnames = Vec::new();
        let mut atttypids = Vec::new();
        let mut attstattargets = Vec::new();
        let mut attlens = Vec::new();
        let mut attnums = Vec::new();
        let mut attndimss = Vec::new();
        let mut attcacheoffs = Vec::new();
        let mut atttymods = Vec::new();
        let mut attbyvals = Vec::new();
        let mut attaligns = Vec::new();
        let mut attstorages = Vec::new();
        let mut attcompressions: Vec<Option<String>> = Vec::new();
        let mut attnotnulls = Vec::new();
        let mut atthasdefs = Vec::new();
        let mut atthasmissings = Vec::new();
        let mut attidentitys = Vec::new();
        let mut attgenerateds = Vec::new();
        let mut attisdroppeds = Vec::new();
        let mut attislocals = Vec::new();
        let mut attinhcounts = Vec::new();
        let mut attcollations = Vec::new();
        let mut attacls: Vec<Option<String>> = Vec::new();
        let mut attoptions: Vec<Option<String>> = Vec::new();
        let mut attfdwoptions: Vec<Option<String>> = Vec::new();
        let mut attmissingvals: Vec<Option<String>> = Vec::new();

        let mut oid_cache = this.oid_cache.write().await;
        // Collect the table OIDs assigned this run into a local map, then
        // reconcile the shared cache below. pg_attribute enumerates only
        // tables, so it must NOT discard `Catalog`/`Schema` entries it didn't
        // visit. The previous `*oid_cache = swap_cache` replaced the whole
        // cache with table-only keys, silently deleting every schema/catalog
        // OID; the next pg_namespace/pg_class query then re-allocated the
        // schema a *different* OID, breaking OID-based joins (e.g. DBeaver's
        // `pg_class WHERE relnamespace = <oid>`). See #389.
        let mut table_oid_cache = HashMap::new();

        for catalog_name in this.catalog_list.catalog_names().await? {
            if let Some(schema_names) = this.catalog_list.schema_names(&catalog_name).await? {
                for schema_name in schema_names {
                    if let Some(table_names) = this
                        .catalog_list
                        .table_names(&catalog_name, &schema_name)
                        .await?
                    {
                        // Process all tables in this schema
                        for table_name in table_names {
                            let cache_key = OidCacheKey::Table(
                                catalog_name.clone(),
                                schema_name.clone(),
                                table_name.clone(),
                            );
                            let table_oid = resolve_oid(&cache_key, &oid_cache, &this.oid_counter);
                            table_oid_cache.insert(cache_key, table_oid);

                            if let Some(table_schema) = this
                                .catalog_list
                                .table_schema(&catalog_name, &schema_name, &table_name)
                                .await?
                            {
                                // Add column entries for this table
                                for (column_idx, field) in table_schema.fields().iter().enumerate()
                                {
                                    let attnum = (column_idx + 1) as i16; // PostgreSQL column numbers start at 1
                                    let (pg_type_oid, type_len, by_val, align, storage) =
                                        Self::datafusion_to_pg_type(field.data_type());

                                    attrelids.push(table_oid as i32);
                                    attnames.push(field.name().clone());
                                    atttypids.push(pg_type_oid as i32);
                                    attstattargets.push(-1); // Default statistics target
                                    attlens.push(type_len);
                                    attnums.push(attnum);
                                    attndimss.push(0); // No array support for now
                                    attcacheoffs.push(-1); // Not cached
                                    atttymods.push(-1); // No type modifiers
                                    attbyvals.push(by_val);
                                    attaligns.push(align.to_string());
                                    attstorages.push(storage.to_string());
                                    attcompressions.push(None); // No compression
                                    attnotnulls.push(!field.is_nullable());
                                    atthasdefs.push(false); // No default values
                                    atthasmissings.push(false); // No missing values
                                    attidentitys.push("".to_string()); // No identity columns
                                    attgenerateds.push("".to_string()); // No generated columns
                                    attisdroppeds.push(false); // Not dropped
                                    attislocals.push(true); // Local to this relation
                                    attinhcounts.push(0); // No inheritance
                                    attcollations.push(0); // Default collation
                                    attacls.push(None); // No ACLs
                                    attoptions.push(None); // No options
                                    attfdwoptions.push(None); // No FDW options
                                    attmissingvals.push(None); // No missing values
                                }
                            }
                        }
                    }
                }
            }
        }

        // Reconcile: drop only stale `Table` entries (tables that no longer
        // exist); leave `Catalog` and `Schema` entries untouched, then refresh
        // the live table OIDs. This mirrors pg_namespace's surgical retain
        // rather than clobbering keys this builder never enumerated.
        oid_cache.retain(|key, _| match key {
            OidCacheKey::Catalog(..) => true,
            OidCacheKey::Schema(..) => true,
            OidCacheKey::Table(..) => table_oid_cache.contains_key(key),
        });
        oid_cache.extend(table_oid_cache);

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(attrelids)),
            Arc::new(StringArray::from(attnames)),
            Arc::new(Int32Array::from(atttypids)),
            Arc::new(Int32Array::from(attstattargets)),
            Arc::new(Int16Array::from(attlens)),
            Arc::new(Int16Array::from(attnums)),
            Arc::new(Int32Array::from(attndimss)),
            Arc::new(Int32Array::from(attcacheoffs)),
            Arc::new(Int32Array::from(atttymods)),
            Arc::new(BooleanArray::from(attbyvals)),
            Arc::new(StringArray::from(attaligns)),
            Arc::new(StringArray::from(attstorages)),
            Arc::new(StringArray::from_iter(attcompressions.into_iter())),
            Arc::new(BooleanArray::from(attnotnulls)),
            Arc::new(BooleanArray::from(atthasdefs)),
            Arc::new(BooleanArray::from(atthasmissings)),
            Arc::new(StringArray::from(attidentitys)),
            Arc::new(StringArray::from(attgenerateds)),
            Arc::new(BooleanArray::from(attisdroppeds)),
            Arc::new(BooleanArray::from(attislocals)),
            Arc::new(Int32Array::from(attinhcounts)),
            Arc::new(Int32Array::from(attcollations)),
            Arc::new(StringArray::from_iter(attacls.into_iter())),
            Arc::new(StringArray::from_iter(attoptions.into_iter())),
            Arc::new(StringArray::from_iter(attfdwoptions.into_iter())),
            Arc::new(StringArray::from_iter(attmissingvals.into_iter())),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;
        Ok(batch)
    }

    /// Map DataFusion data types to PostgreSQL type information
    fn datafusion_to_pg_type(data_type: &DataType) -> (u32, i16, bool, &'static str, &'static str) {
        match arrow_pg::datatypes::into_pg_type(data_type) {
            Ok(t @ Type::BOOL) => (t.oid(), 1, true, "c", "p"),
            Ok(t @ Type::CHAR) => (t.oid(), 1, true, "c", "p"),
            Ok(t @ Type::INT2) => (t.oid(), 2, true, "s", "p"),
            Ok(t @ Type::INT4) => (t.oid(), 4, true, "i", "p"),
            Ok(t @ Type::INT8) => (t.oid(), 8, true, "d", "p"),
            Ok(t @ Type::FLOAT4) => (t.oid(), 4, true, "i", "p"),
            Ok(t @ Type::FLOAT8) => (t.oid(), 8, true, "d", "p"),
            Ok(t @ Type::TEXT) => (t.oid(), -1, false, "i", "x"),
            Ok(t @ Type::BYTEA) => (t.oid(), -1, false, "i", "x"),
            Ok(t @ Type::DATE) => (t.oid(), 4, true, "i", "p"),
            Ok(t @ Type::TIME) => (t.oid(), 8, true, "d", "p"),
            Ok(t @ Type::TIMESTAMP) => (t.oid(), 8, true, "d", "p"),
            Ok(t @ Type::TIMESTAMPTZ) => (t.oid(), 8, true, "d", "p"),
            Ok(t @ Type::NUMERIC) => (t.oid(), -1, false, "i", "m"),
            Ok(t @ Type::INT4_ARRAY) => (t.oid(), -1, false, "i", "x"),
            Ok(t @ Type::TEXT_ARRAY) => (t.oid(), -1, false, "i", "x"),
            _ => (Type::TEXT.oid(), -1, false, "i", "x"), // Default to text for unknown types
        }
    }
}

impl<C: CatalogInfo> PartitionStream for PgAttributeTable<C> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { Self::get_data(this).await }),
        ))
    }
}
