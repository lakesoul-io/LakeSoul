use crate::sorted_stream_merger::{BufferedRecordBatchStream, SortKeyRange};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalSortExpr;
use smallvec::SmallVec;

pub enum SortedFileType {
    UniqueSortedPrimaryKeys,
    NonUniqueSortedPrimaryKeys,
}

pub enum MergingType {
    SingledFileMerge,
    MultipleFileWithSameSchemaMerge,
    MultipleFileWithDifferentSchemaMerge,
}

pub enum MergingLogicType {
    UseLast,
    UseAssociativeExpr,
    UseJavaMergeOp,
}

pub trait StreamSortKeyRangesMerger {
    fn new(target_schema: SchemaRef, target_batch_size: usize, stream_schemas: Vec<SchemaRef>) -> Self;

    fn merge(
        &mut self,
        ranges: SmallVec<[SortKeyRange; 4]>,
    ) -> Result<Option<RecordBatch>>;
}

#[async_trait]
pub trait StreamSortKeyRangeFetcher {
    fn new(
        stream_idx: usize,
        stream: BufferedRecordBatchStream,
        expressions: &[PhysicalSortExpr],
        schema: SchemaRef,
    ) -> Result<Self>
    where
        Self: Sized;

    async fn init_batch(&mut self) -> Result<()>;

    fn is_terminated(&self) -> bool;

    async fn fetch_sort_key_range_from_stream(
        &mut self,
        current_range: Option<&SortKeyRange>,
    ) -> Result<Option<SortKeyRange>>;
}

#[async_trait]
pub trait StreamSortKeyRangeCombiner {
    type Fetcher: StreamSortKeyRangeFetcher;

    fn with_fetchers(fetchers: Vec<Self::Fetcher>) -> Self;

    async fn init(&mut self) -> Result<()>;

    async fn next(&mut self) -> Result<Option<SmallVec<[SortKeyRange; 4]>>>;
}
