use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;

use crate::pg_catalog::catalog_info::CatalogInfo;

#[derive(Debug, Clone)]
pub(crate) struct PgTablesTable<C> {
    schema: SchemaRef,
    catalog_list: C,
}

impl<C: CatalogInfo> PgTablesTable<C> {
    pub(crate) fn new(catalog_list: C) -> Self {
        // Define the schema for pg_class
        // This matches key columns from PostgreSQL's pg_class
        let schema = Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("tablename", DataType::Utf8, false),
            Field::new("tableowner", DataType::Utf8, false),
            Field::new("tablespace", DataType::Utf8, true),
            Field::new("hasindex", DataType::Boolean, false),
            Field::new("hasrules", DataType::Boolean, false),
            Field::new("hastriggers", DataType::Boolean, false),
            Field::new("rowsecurity", DataType::Boolean, false),
        ]));

        Self {
            schema,
            catalog_list,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: Self) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut schema_names = Vec::new();
        let mut table_names = Vec::new();
        let mut table_owners = Vec::new();
        let mut table_spaces: Vec<Option<String>> = Vec::new();
        let mut has_index = Vec::new();
        let mut has_rules = Vec::new();
        let mut has_triggers = Vec::new();
        let mut row_security = Vec::new();

        // Iterate through all catalogs and schemas
        for catalog_name in this.catalog_list.catalog_names().await? {
            if let Some(catalog_schema_names) =
                this.catalog_list.schema_names(&catalog_name).await?
            {
                for schema_name in catalog_schema_names {
                    if let Some(catalog_table_names) = this
                        .catalog_list
                        .table_names(&catalog_name, &schema_name)
                        .await?
                    {
                        // Now process all tables in this schema
                        for table_name in catalog_table_names {
                            schema_names.push(schema_name.to_string());
                            table_names.push(table_name.to_string());
                            table_owners.push("postgres".to_string());
                            table_spaces.push(None);
                            has_index.push(false);
                            has_rules.push(false);
                            has_triggers.push(false);
                            row_security.push(false);
                        }
                    }
                }
            }
        }

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(schema_names)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(table_owners)),
            Arc::new(StringArray::from(table_spaces)),
            Arc::new(BooleanArray::from(has_index)),
            Arc::new(BooleanArray::from(has_rules)),
            Arc::new(BooleanArray::from(has_triggers)),
            Arc::new(BooleanArray::from(row_security)),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl<C: CatalogInfo> PartitionStream for PgTablesTable<C> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { PgTablesTable::get_data(this).await }),
        ))
    }
}
