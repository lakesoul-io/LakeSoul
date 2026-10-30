use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::error::Result;

#[derive(Debug, Clone)]
pub struct EmptyTable {
    schema: SchemaRef,
}

impl EmptyTable {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn try_into_memtable(&self) -> Result<Arc<MemTable>> {
        MemTable::try_new(self.schema.clone(), vec![vec![]]).map(Arc::new)
    }
}
