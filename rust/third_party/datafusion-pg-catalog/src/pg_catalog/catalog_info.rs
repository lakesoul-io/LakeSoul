use std::fmt::Debug;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, catalog::CatalogProviderList, datasource::TableType,
    error::DataFusionError,
};

/// Define the interface for retrieve catalog data for pg_catalog tables
#[async_trait]
pub trait CatalogInfo: Clone + Send + Sync + Debug + 'static {
    async fn catalog_names(&self) -> Result<Vec<String>, DataFusionError>;

    async fn schema_names(
        &self,
        catalog_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError>;

    async fn table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError>;

    async fn table_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<SchemaRef>, DataFusionError>;

    async fn table_type(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableType>, DataFusionError>;
}

#[async_trait]
impl CatalogInfo for Arc<dyn CatalogProviderList> {
    async fn catalog_names(&self) -> Result<Vec<String>, DataFusionError> {
        Ok(CatalogProviderList::catalog_names(self.as_ref()))
    }

    async fn schema_names(
        &self,
        catalog_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError> {
        Ok(self.catalog(catalog_name).map(|c| c.schema_names()))
    }

    async fn table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError> {
        Ok(self
            .catalog(catalog_name)
            .and_then(|c| c.schema(schema_name))
            .map(|s| s.table_names()))
    }

    async fn table_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<SchemaRef>, DataFusionError> {
        let schema = self
            .catalog(catalog_name)
            .and_then(|c| c.schema(schema_name));
        if let Some(schema) = schema {
            let table_schema = schema.table(table_name).await?.map(|t| t.schema());
            Ok(table_schema)
        } else {
            Ok(None)
        }
    }

    async fn table_type(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableType>, DataFusionError> {
        let schema = self
            .catalog(catalog_name)
            .and_then(|c| c.schema(schema_name));
        if let Some(schema) = schema {
            let table_type = schema.table_type(table_name).await?;
            Ok(table_type)
        } else {
            Ok(None)
        }
    }
}

/// A catalog list that `pg_catalog` reaches without owning it.
///
/// `pg_catalog` is registered as a schema of a catalog in the very list it
/// describes, so a strong reference would make the list keep itself alive.
/// Lookups fail once every owner of the list (the session) is gone.
#[async_trait]
impl CatalogInfo for Weak<dyn CatalogProviderList> {
    async fn catalog_names(&self) -> Result<Vec<String>, DataFusionError> {
        CatalogInfo::catalog_names(&upgrade(self)?).await
    }

    async fn schema_names(
        &self,
        catalog_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError> {
        CatalogInfo::schema_names(&upgrade(self)?, catalog_name).await
    }

    async fn table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Option<Vec<String>>, DataFusionError> {
        CatalogInfo::table_names(&upgrade(self)?, catalog_name, schema_name).await
    }

    async fn table_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<SchemaRef>, DataFusionError> {
        CatalogInfo::table_schema(&upgrade(self)?, catalog_name, schema_name, table_name).await
    }

    async fn table_type(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableType>, DataFusionError> {
        CatalogInfo::table_type(&upgrade(self)?, catalog_name, schema_name, table_name).await
    }
}

fn upgrade(
    list: &Weak<dyn CatalogProviderList>,
) -> Result<Arc<dyn CatalogProviderList>, DataFusionError> {
    list.upgrade().ok_or_else(|| {
        DataFusionError::Execution("pg_catalog: the session's catalog list was dropped".to_string())
    })
}

pub fn table_type_to_string(tt: &TableType) -> String {
    match tt {
        TableType::Base => "r".to_string(),
        TableType::View => "v".to_string(),
        TableType::Temporary => "r".to_string(),
    }
}
