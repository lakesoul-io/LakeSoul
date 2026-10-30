use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use std::sync::Arc;

use crate::pg_catalog::BACKEND_PID;

#[derive(Debug, Clone)]
pub(crate) struct PgStatGssApiTable {
    schema: SchemaRef,
}

impl PgStatGssApiTable {
    pub(crate) fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pid", DataType::Int32, true),
            Field::new("gss_authenticated", DataType::Boolean, false),
            Field::new("principal", DataType::Utf8, true),
            Field::new("encrypted", DataType::Boolean, false),
            Field::new("credentials_delegated", DataType::Boolean, false),
        ]));

        Self { schema }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: Self) -> Result<RecordBatch> {
        let pid = vec![BACKEND_PID];
        let gss_authenticated = vec![false];
        let principal: Vec<Option<String>> = vec![None];
        let encrypted = vec![false];
        let credentials_delegated = vec![false];

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(pid)),
            Arc::new(BooleanArray::from(gss_authenticated)),
            Arc::new(StringArray::from(principal)),
            Arc::new(BooleanArray::from(encrypted)),
            Arc::new(BooleanArray::from(credentials_delegated)),
        ];

        // Create a record batch
        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl PartitionStream for PgStatGssApiTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { PgStatGssApiTable::get_data(this).await }),
        ))
    }
}
