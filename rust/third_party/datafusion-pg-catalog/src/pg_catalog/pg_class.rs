use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use postgres_types::Oid;

use super::oid_field;
use tokio::sync::RwLock;

use crate::pg_catalog::catalog_info::{CatalogInfo, table_type_to_string};

use super::{OidCacheKey, resolve_oid};

#[derive(Debug, Clone)]
pub(crate) struct PgClassTable<C> {
    schema: SchemaRef,
    catalog_list: C,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
}

impl<C: CatalogInfo> PgClassTable<C> {
    pub(crate) fn new(
        catalog_list: C,
        oid_counter: Arc<AtomicU32>,
        oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    ) -> Self {
        // Define the schema for pg_class
        // This matches key columns from PostgreSQL's pg_class
        let schema = Arc::new(Schema::new(vec![
            oid_field::oid_field("oid", oid_field::kind::OID, false), // Object identifier
            Field::new("relname", DataType::Utf8, false), // Name of the table, index, view, etc.
            oid_field::oid_field("relnamespace", oid_field::kind::REGNAMESPACE, false), // OID of the namespace that contains this relation
            oid_field::oid_field("reltype", oid_field::kind::REGTYPE, false), // OID of the data type (composite type) this table describes
            oid_field::oid_field("reloftype", oid_field::kind::REGTYPE, true), // OID of the composite type for typed table, 0 otherwise
            Field::new("relowner", DataType::Int32, false), // Owner of the relation
            Field::new("relam", DataType::Int32, false), // If this is an index, the access method used
            Field::new("relfilenode", DataType::Int32, false), // Name of the on-disk file of this relation
            Field::new("reltablespace", DataType::Int32, false), // Tablespace OID for this relation
            Field::new("relpages", DataType::Int32, false), // Size of the on-disk representation in pages
            Field::new("reltuples", DataType::Float64, false), // Number of tuples
            Field::new("relallvisible", DataType::Int32, false), // Number of all-visible pages
            oid_field::oid_field("reltoastrelid", oid_field::kind::REGCLASS, false), // OID of the TOAST table
            Field::new("relhasindex", DataType::Boolean, false), // True if this is a table and it has (or recently had) any indexes
            Field::new("relisshared", DataType::Boolean, false), // True if this table is shared across all databases
            Field::new("relpersistence", DataType::Utf8, false), // p=permanent table, u=unlogged table, t=temporary table
            Field::new("relkind", DataType::Utf8, false), // r=ordinary table, i=index, S=sequence, v=view, etc.
            Field::new("relnatts", DataType::Int16, false), // Number of user columns
            Field::new("relchecks", DataType::Int16, false), // Number of CHECK constraints
            Field::new("relhasrules", DataType::Boolean, false), // True if table has (or once had) rules
            Field::new("relhastriggers", DataType::Boolean, false), // True if table has (or once had) triggers
            Field::new("relhassubclass", DataType::Boolean, false), // True if table or index has (or once had) any inheritance children
            Field::new("relrowsecurity", DataType::Boolean, false), // True if row security is enabled
            Field::new("relforcerowsecurity", DataType::Boolean, false), // True if row security forced for owners
            Field::new("relispopulated", DataType::Boolean, false), // True if relation is populated (not true for some materialized views)
            Field::new("relreplident", DataType::Utf8, false), // Columns used to form "replica identity" for rows
            Field::new("relispartition", DataType::Boolean, false), // True if table is a partition
            oid_field::oid_field("relrewrite", oid_field::kind::REGCLASS, true), // OID of a rule that rewrites this relation
            Field::new("relfrozenxid", DataType::Int32, false), // All transaction IDs before this have been replaced with a permanent ("frozen") transaction ID
            Field::new("relminmxid", DataType::Int32, false), // All Multixact IDs before this have been replaced with a transaction ID
            Field::new("relpartbound", DataType::Utf8, true),
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
        let mut oids = Vec::new();
        let mut relnames = Vec::new();
        let mut relnamespaces = Vec::new();
        let mut reltypes = Vec::new();
        let mut reloftypes = Vec::new();
        let mut relowners = Vec::new();
        let mut relams = Vec::new();
        let mut relfilenodes = Vec::new();
        let mut reltablespaces = Vec::new();
        let mut relpages = Vec::new();
        let mut reltuples = Vec::new();
        let mut relallvisibles = Vec::new();
        let mut reltoastrelids = Vec::new();
        let mut relhasindexes = Vec::new();
        let mut relisshareds = Vec::new();
        let mut relpersistences = Vec::new();
        let mut relkinds = Vec::new();
        let mut relnattses = Vec::new();
        let mut relcheckses = Vec::new();
        let mut relhasruleses = Vec::new();
        let mut relhastriggersses = Vec::new();
        let mut relhassubclasses = Vec::new();
        let mut relrowsecurities = Vec::new();
        let mut relforcerowsecurities = Vec::new();
        let mut relispopulateds = Vec::new();
        let mut relreplidents = Vec::new();
        let mut relispartitions = Vec::new();
        let mut relrewrites = Vec::new();
        let mut relfrozenxids = Vec::new();
        let mut relminmxids = Vec::new();
        let mut relpartbound = Vec::new();

        let mut oid_cache = this.oid_cache.write().await;
        // Every time when call pg_catalog we generate a new cache and drop the
        // original one in case that schemas or tables were dropped.
        let mut swap_cache = HashMap::new();

        // Iterate through all catalogs and schemas
        for catalog_name in this.catalog_list.catalog_names().await? {
            let cache_key = OidCacheKey::Catalog(catalog_name.clone());
            let catalog_oid = resolve_oid(&cache_key, &oid_cache, &this.oid_counter);
            swap_cache.insert(cache_key, catalog_oid);

            if let Some(schema_names) = this.catalog_list.schema_names(&catalog_name).await? {
                for schema_name in schema_names {
                    let cache_key = OidCacheKey::Schema(catalog_name.clone(), schema_name.clone());
                    let schema_oid = resolve_oid(&cache_key, &oid_cache, &this.oid_counter);
                    swap_cache.insert(cache_key, schema_oid);

                    // Add an entry for the schema itself (as a namespace)
                    // (In a full implementation, this would go in pg_namespace)

                    // Now process all tables in this schema
                    if let Some(table_names) = this
                        .catalog_list
                        .table_names(&catalog_name, &schema_name)
                        .await?
                    {
                        for table_name in table_names {
                            let cache_key = OidCacheKey::Table(
                                catalog_name.clone(),
                                schema_name.clone(),
                                table_name.clone(),
                            );
                            let table_oid = resolve_oid(&cache_key, &oid_cache, &this.oid_counter);
                            swap_cache.insert(cache_key, table_oid);

                            if let Some(table_schema) = this
                                .catalog_list
                                .table_schema(&catalog_name, &schema_name, &table_name)
                                .await?
                            {
                                // Determine the correct table type based on the table provider and context
                                let table_type = this
                                    .catalog_list
                                    .table_type(&catalog_name, &schema_name, &table_name)
                                    .await?
                                    .unwrap_or(TableType::Temporary);

                                // Get column count from schema
                                let column_count = table_schema.fields().len() as i16;

                                // Add table entry
                                oids.push(table_oid as i32);
                                relnames.push(table_name.clone());
                                relnamespaces.push(schema_oid as i32);
                                reltypes.push(0); // Simplified: we're not tracking data types
                                reloftypes.push(None);
                                relowners.push(0); // Simplified: no owner tracking
                                relams.push(0); // Default access method
                                relfilenodes.push(table_oid as i32); // Use OID as filenode
                                reltablespaces.push(0); // Default tablespace
                                relpages.push(1); // Default page count
                                reltuples.push(0.0); // No row count stats
                                relallvisibles.push(0);
                                reltoastrelids.push(0);
                                relhasindexes.push(false);
                                relisshareds.push(false);
                                relpersistences.push("p".to_string()); // Permanent
                                relkinds.push(table_type_to_string(&table_type));
                                relnattses.push(column_count);
                                relcheckses.push(0);
                                relhasruleses.push(false);
                                relhastriggersses.push(false);
                                relhassubclasses.push(false);
                                relrowsecurities.push(false);
                                relforcerowsecurities.push(false);
                                relispopulateds.push(true);
                                relreplidents.push("d".to_string()); // Default
                                relispartitions.push(false);
                                relrewrites.push(None);
                                relfrozenxids.push(0);
                                relminmxids.push(0);
                                relpartbound.push("".to_string());
                            }
                        }
                    }
                }
            }
        }

        *oid_cache = swap_cache;

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(Int32Array::from(relnamespaces)),
            Arc::new(Int32Array::from(reltypes)),
            Arc::new(Int32Array::from_iter(reloftypes.into_iter())),
            Arc::new(Int32Array::from(relowners)),
            Arc::new(Int32Array::from(relams)),
            Arc::new(Int32Array::from(relfilenodes)),
            Arc::new(Int32Array::from(reltablespaces)),
            Arc::new(Int32Array::from(relpages)),
            Arc::new(Float64Array::from_iter(reltuples.into_iter())),
            Arc::new(Int32Array::from(relallvisibles)),
            Arc::new(Int32Array::from(reltoastrelids)),
            Arc::new(BooleanArray::from(relhasindexes)),
            Arc::new(BooleanArray::from(relisshareds)),
            Arc::new(StringArray::from(relpersistences)),
            Arc::new(StringArray::from(relkinds)),
            Arc::new(Int16Array::from(relnattses)),
            Arc::new(Int16Array::from(relcheckses)),
            Arc::new(BooleanArray::from(relhasruleses)),
            Arc::new(BooleanArray::from(relhastriggersses)),
            Arc::new(BooleanArray::from(relhassubclasses)),
            Arc::new(BooleanArray::from(relrowsecurities)),
            Arc::new(BooleanArray::from(relforcerowsecurities)),
            Arc::new(BooleanArray::from(relispopulateds)),
            Arc::new(StringArray::from(relreplidents)),
            Arc::new(BooleanArray::from(relispartitions)),
            Arc::new(Int32Array::from_iter(relrewrites.into_iter())),
            Arc::new(Int32Array::from(relfrozenxids)),
            Arc::new(Int32Array::from(relminmxids)),
            Arc::new(StringArray::from(relpartbound)),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl<C: CatalogInfo> PartitionStream for PgClassTable<C> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { PgClassTable::get_data(this).await }),
        ))
    }
}
