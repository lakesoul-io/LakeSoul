use crate::format::FileMeta;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use object_store::ObjectMeta;
use rootcause::Report;

mod multipart_writer;

/// The result of a flush operation with format (partition_desc, file_path, object_meta, file_meta)
// TODO use a TYPE
pub type WriterFlushResult = Vec<(String, String, ObjectMeta, FileMeta)>;

struct FlushOutput;

#[async_trait::async_trait]
pub trait AsyncBatchWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), Report>;
    /// Flush the writer and close it.
    async fn flush_and_close(self: Box<Self>) -> Result<WriterFlushResult, Report>;
    /// Abort the writer and close it when an error occurs.
    async fn abort_and_close(self: Box<Self>) -> Result<(), Report>;
    /// Get the schema of the writer.
    fn schema(&self) -> SchemaRef;
    /// Get the buffered size of rows in the writer.
    fn buffered_size(&self) -> u64 {
        0
    }
}
