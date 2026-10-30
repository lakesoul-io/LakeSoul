use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use postgres_types::Oid;
use tokio::sync::RwLock;

use crate::pg_catalog::catalog_info::CatalogInfo;

use super::{OidCacheKey, resolve_oid};

#[derive(Debug, Clone)]
pub(crate) struct PgDatabaseTable<C> {
    schema: SchemaRef,
    catalog_list: C,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    include_synthetic_postgres_database: bool,
}

impl<C: CatalogInfo> PgDatabaseTable<C> {
    pub(crate) fn new(
        catalog_list: C,
        oid_counter: Arc<AtomicU32>,
        oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
        include_synthetic_postgres_database: bool,
    ) -> Self {
        // Define the schema for pg_database
        // This matches PostgreSQL's pg_database table columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("datname", DataType::Utf8, false), // Database name
            Field::new("datdba", DataType::Int32, false), // Database owner's user ID
            Field::new("encoding", DataType::Int32, false), // Character encoding
            Field::new("datlocprovider", DataType::Utf8, false),
            Field::new("datcollate", DataType::Utf8, false), // LC_COLLATE for this database
            Field::new("datctype", DataType::Utf8, false),   // LC_CTYPE for this database
            Field::new("datistemplate", DataType::Boolean, false), // If true, database can be used as a template
            Field::new("datallowconn", DataType::Boolean, false), // If false, no one can connect to this database
            Field::new("datconnlimit", DataType::Int32, false), // Max number of concurrent connections (-1=no limit)
            Field::new("datlastsysoid", DataType::Int32, false), // Last system OID in database
            Field::new("datfrozenxid", DataType::Int32, false), // Frozen XID for this database
            Field::new("datminmxid", DataType::Int32, false),   // Minimum multixact ID
            Field::new("dattablespace", DataType::Int32, false), // Default tablespace for this database
            Field::new("daticulocale", DataType::Utf8, true),
            Field::new("daticurules", DataType::Utf8, true),
            Field::new(
                "datacl",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ), // Access privileges
        ]));

        Self {
            schema,
            catalog_list,
            oid_counter,
            oid_cache,
            include_synthetic_postgres_database,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: Self) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut oids = Vec::new();
        let mut datnames = Vec::new();
        let mut datdbas = Vec::new();
        let mut encodings = Vec::new();
        let mut datlocproviders = Vec::new();
        let mut datcollates = Vec::new();
        let mut datctypes = Vec::new();
        let mut datistemplates = Vec::new();
        let mut datallowconns = Vec::new();
        let mut datconnlimits = Vec::new();
        let mut datlastsysoids = Vec::new();
        let mut datfrozenxids = Vec::new();
        let mut datminmxids = Vec::new();
        let mut dattablespaces = Vec::new();
        let mut daticulocales: Vec<Option<String>> = Vec::new();
        let mut daticurules: Vec<Option<String>> = Vec::new();
        let mut datacls: Vec<Option<Vec<Option<i32>>>> = Vec::new();

        // to store all schema-oid mapping temporarily before adding to global oid cache
        let mut catalog_oid_cache = HashMap::new();

        let mut oid_cache = this.oid_cache.write().await;

        // Add a record for each catalog (treating catalogs as "databases")
        for catalog_name in this.catalog_list.catalog_names().await? {
            let cache_key = OidCacheKey::Catalog(catalog_name.clone());
            let catalog_oid = resolve_oid(&cache_key, &oid_cache, &this.oid_counter);
            catalog_oid_cache.insert(cache_key, catalog_oid);

            oids.push(catalog_oid as i32);
            datnames.push(catalog_name.clone());
            datdbas.push(10); // Default owner (assuming 10 = postgres user)
            encodings.push(6); // 6 = UTF8 in PostgreSQL
            datlocproviders.push("libc".to_string());
            datcollates.push("en_US.UTF-8".to_string()); // Default collation
            datctypes.push("en_US.UTF-8".to_string()); // Default ctype
            datistemplates.push(false);
            datallowconns.push(true);
            datconnlimits.push(-1); // No connection limit
            datlastsysoids.push(100000); // Arbitrary last system OID
            datfrozenxids.push(1); // Simplified transaction ID
            datminmxids.push(1); // Simplified multixact ID
            dattablespaces.push(1663); // Default tablespace (1663 = pg_default in PostgreSQL)
            daticulocales.push(None);
            daticurules.push(None);
            datacls.push(None); // No specific ACLs
        }

        // Optionally include a `postgres` database for clients that expect it.
        let default_datname = "postgres".to_string();
        if this.include_synthetic_postgres_database && !datnames.contains(&default_datname) {
            let cache_key = OidCacheKey::Catalog(default_datname.clone());
            let catalog_oid = resolve_oid(&cache_key, &oid_cache, &this.oid_counter);
            catalog_oid_cache.insert(cache_key, catalog_oid);

            oids.push(catalog_oid as i32);
            datnames.push(default_datname);
            datdbas.push(10);
            encodings.push(6);
            datlocproviders.push("libc".to_string());
            datcollates.push("en_US.UTF-8".to_string());
            datctypes.push("en_US.UTF-8".to_string());
            datistemplates.push(false);
            datallowconns.push(true);
            datconnlimits.push(-1);
            datlastsysoids.push(100000);
            datfrozenxids.push(1);
            datminmxids.push(1);
            dattablespaces.push(1663);
            daticulocales.push(None);
            daticurules.push(None);
            datacls.push(None);
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(datnames)),
            Arc::new(Int32Array::from(datdbas)),
            Arc::new(Int32Array::from(encodings)),
            Arc::new(StringArray::from(datlocproviders)),
            Arc::new(StringArray::from(datcollates)),
            Arc::new(StringArray::from(datctypes)),
            Arc::new(BooleanArray::from(datistemplates)),
            Arc::new(BooleanArray::from(datallowconns)),
            Arc::new(Int32Array::from(datconnlimits)),
            Arc::new(Int32Array::from(datlastsysoids)),
            Arc::new(Int32Array::from(datfrozenxids)),
            Arc::new(Int32Array::from(datminmxids)),
            Arc::new(Int32Array::from(dattablespaces)),
            Arc::new(StringArray::from(daticulocales)),
            Arc::new(StringArray::from(daticurules)),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                datacls.into_iter(),
            )),
        ];

        // Create a full record batch
        let full_batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        // update cache
        // remove all schema cache and table of the schema which is no longer exists
        oid_cache.retain(|key, _| match key {
            OidCacheKey::Catalog(..) => false,
            OidCacheKey::Schema(catalog, ..) => {
                catalog_oid_cache.contains_key(&OidCacheKey::Catalog(catalog.clone()))
            }
            OidCacheKey::Table(catalog, ..) => {
                catalog_oid_cache.contains_key(&OidCacheKey::Catalog(catalog.clone()))
            }
        });
        // add new schema cache
        oid_cache.extend(catalog_oid_cache);

        Ok(full_batch)
    }
}

impl<C: CatalogInfo> PartitionStream for PgDatabaseTable<C> {
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
