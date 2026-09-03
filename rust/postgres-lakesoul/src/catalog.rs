use std::{collections::HashMap, sync::Arc};

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use lakesoul_datafusion::catalog::LakeSoulCatalog;
use parking_lot::RwLock;

#[derive(Debug)]
pub struct PgLakeSoulCatalog {
    lakesoul: Arc<LakeSoulCatalog>,
    virtual_schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl PgLakeSoulCatalog {
    pub fn new(lakesoul: Arc<LakeSoulCatalog>) -> Self {
        Self {
            lakesoul,
            virtual_schemas: RwLock::new(HashMap::new()),
        }
    }
}

impl CatalogProvider for PgLakeSoulCatalog {
    fn schema_names(&self) -> Vec<String> {
        let mut names = self.lakesoul.schema_names();
        let virtual_schemas = self.virtual_schemas.read();
        for name in virtual_schemas.keys() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(schema) = self.virtual_schemas.read().get(name) {
            Some(Arc::clone(schema))
        } else {
            self.lakesoul.schema(name)
        }
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn SchemaProvider>>> {
        if name == "pg_catalog" {
            Ok(self.virtual_schemas.write().insert(name.into(), schema))
        } else {
            self.lakesoul.register_schema(name, schema)
        }
    }
}
