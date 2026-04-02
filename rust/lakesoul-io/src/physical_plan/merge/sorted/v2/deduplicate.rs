// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! This module provides deduplication functionality for sorted record batch streams.
//! It removes duplicate records based on the sort key while preserving the sorted order.

use crate::Result;
use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::sorted_stream_merger::CursorStream;
use async_stream::try_stream;
use futures::StreamExt;
use futures::stream::Peekable;
use std::pin::Pin;

/// A stream that deduplicates sorted record batches based on cursor values
/// This implementation works on a single CursorStream and removes duplicates within that stream
pub struct DeduplicateStream<C: CursorValues> {
    /// The input cursor stream
    input_stream: Pin<Box<Peekable<CursorStream<C>>>>,
}

impl<C: CursorValues + Send + Sync + 'static> DeduplicateStream<C> {
    /// Create a new deduplicate stream from a single sorted input stream
    ///
    /// # Arguments
    /// * `input_stream` - A single sorted input stream with cursor values
    /// * `schema` - The schema of the output record batches
    pub fn new_from_stream(input_stream: CursorStream<C>) -> Result<Self> {
        Ok(Self {
            input_stream: Box::pin(input_stream.peekable()),
        })
    }

    pub fn create_deduplicated_stream(mut self) -> CursorStream<C> {
        Box::pin(try_stream! {
            while let Some(next) = self.input_stream.next().await {
                let (c, batch) = next?;
                if batch.num_rows() == 0 {
                    continue;
                }
                // find duplicate index
                let mut i = 1;
                let mut last_begin = 0;
                let mut found_duplicate = false;
                while i < batch.num_rows() {
                    if !found_duplicate {
                        if !C::eq(&c, i, &c, i - 1) {
                            i += 1;
                            continue;
                        }
                        // we found duplicate rows starting at i - 1
                        // try produce [last_begin, i - 2]
                        let length = i - last_begin - 1;
                        if length > 0 {
                            yield (c.slice(last_begin, length),
                                          batch.slice(last_begin, length));
                        }
                        // skip to next unequal row
                        found_duplicate = true;
                        last_begin = i;
                        i += 1;
                    } else {
                        if C::eq(&c, i, &c, i - 1) {
                            // skip duplicate rows until last one
                            last_begin = i;
                            i += 1;
                        } else {
                            found_duplicate = false;
                            i += 1;
                        }
                    }
                }
                // now we finished this batch.
                // 1. peek next batch to see if there's duplicate
                // 2. if not, we produce [last_begin, i]
                let mut next_non_empty_exists = false;
                let mut last_equal_next_first = false;
                loop {
                    if let Some(Ok((peek_c, peek_batch))) = self.input_stream.as_mut().peek().await {
                        if peek_batch.num_rows() == 0 {
                            // consume empty batch and continue peeking
                            let _ = self.input_stream.next().await;
                            continue;
                        }
                        next_non_empty_exists = true;
                        last_equal_next_first = C::eq(&c, i - 1, peek_c, 0);
                    }
                    break;
                }

                if next_non_empty_exists {
                    if last_equal_next_first {
                        // try produce [last_begin, i - 2]
                        // just skip last row when it's only one non-dup row
                        if i > last_begin + 1 {
                            let length = i - last_begin - 1;
                            yield (c.slice(last_begin, length), batch.slice(last_begin, length));
                        }
                    } else {
                        // produce [last_begin, num_rows - 1] for no duplicate in next batch
                        let length = i - last_begin;
                        yield (c.slice(last_begin, length), batch.slice(last_begin, length));
                    }
                } else {
                    // produce [last_begin, num_rows - 1] for no next bath
                    let length = i - last_begin;
                    yield (c.slice(last_begin, length), batch.slice(last_begin, length));
                }
            }
        })
    }
}

// Test module with all test cases
#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::InMemGenerator;
    use crate::physical_plan::merge::sorted::cursor::{PrimitiveValues, RowValues};
    use crate::stream::RowCursorStream;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::RecordBatch;
    use arrow_schema::{SchemaRef, SortOptions};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, assert_batches_eq};
    use datafusion_execution::TaskContext;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryReservation,
    };
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_physical_plan::memory::LazyMemoryExec;
    use futures::{StreamExt, TryStreamExt, stream};
    use parking_lot::lock_api::RwLock;
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch(id_values: Vec<i32>, name_values: Vec<&str>) -> RecordBatch {
        let id_array = Int32Array::from(id_values);
        let name_array = StringArray::from(name_values);

        RecordBatch::try_new(
            create_test_schema(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    async fn create_stream(
        batches: Vec<RecordBatch>,
        context: Arc<TaskContext>,
        primary_keys: Vec<String>,
        reservation: MemoryReservation,
    ) -> Result<CursorStream<RowValues>> {
        let schema = batches[0].schema();
        let exec = LazyMemoryExec::try_new(
            schema.clone(),
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(batches).unwrap(),
            ))],
        )?;
        let stream = exec.execute(0, context.clone())?;
        let schema = stream.schema();
        let sort_exprs = primary_keys
            .iter()
            .map(|k| {
                let col_expr = col(k.as_str(), &schema)?;
                Ok(PhysicalSortExpr::new(col_expr, SortOptions::default()))
            })
            .collect::<Result<Vec<_>>>()?;
        let stream = RowCursorStream::try_new(
            schema.as_ref(),
            &LexOrdering::new(sort_exprs)
                .ok_or(DataFusionError::Execution("empty sort expr".into()))?,
            stream,
            reservation.new_empty(),
        )?;
        let stream: CursorStream<RowValues> = Box::pin(stream);
        Ok(stream)
    }

    #[tokio::test]
    async fn test_deduplicate_single_stream() -> crate::Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let primary_keys = vec!["id".to_string()];

        let _schema = create_test_schema();

        // Create a batch with duplicate values
        let batch1 = create_test_batch(
            vec![1, 1, 2, 2, 2, 3, 4, 4, 5],
            vec![
                "1_dup", "1", "2_dup", "2_dup", "2", "3", "4_dup", "4", "5_dup",
            ],
        );

        let batch2 = create_test_batch(
            vec![5, 6, 6, 7, 8, 9, 9],
            vec!["5", "6_dup", "6", "7", "8", "9_dup", "9_dup"],
        );

        let batch3 = create_test_batch(
            vec![9, 10, 11, 12, 13, 14],
            vec!["9", "10", "11", "12", "13", "14"],
        );

        let batch4 = create_test_batch(vec![15, 16], vec!["15", "16"]);

        let stream = create_stream(
            vec![batch1, batch2, batch3, batch4],
            task_ctx.clone(),
            primary_keys.clone(),
            r1.new_empty(),
        )
        .await?;

        // Create deduplicate stream
        let dedup_stream = DeduplicateStream::new_from_stream(stream)?;

        let result = dedup_stream.create_deduplicated_stream();
        let batches = result.try_collect::<Vec<_>>().await?;
        assert_eq!(7, batches.len());
        for i in 0..batches.len() {
            let (c, batch) = &batches[i];
            let rows = batch.num_rows();
            // verify cursor values is unique
            for j in 0..rows {
                if j == 0 && i > 0 {
                    assert!(!RowValues::eq(
                        c,
                        j,
                        &batches[i - 1].0,
                        batches[i - 1].1.num_rows() - 1
                    ));
                } else if j > 0 {
                    assert!(!RowValues::eq(c, j, c, j - 1));
                }
            }
        }
        let batches = batches.into_iter().map(|batch| batch.1).collect::<Vec<_>>();
        assert_batches_eq!(
            &[
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 1  | 1    |",
                "| 2  | 2    |",
                "| 3  | 3    |",
                "| 4  | 4    |",
                "| 5  | 5    |",
                "| 6  | 6    |",
                "| 7  | 7    |",
                "| 8  | 8    |",
                "| 9  | 9    |",
                "| 10 | 10   |",
                "| 11 | 11   |",
                "| 12 | 12   |",
                "| 13 | 13   |",
                "| 14 | 14   |",
                "| 15 | 15   |",
                "| 16 | 16   |",
                "+----+------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deduplicate_empty_stream() -> crate::Result<()> {
        let _schema = create_test_schema();

        // Create empty cursor stream
        let stream: CursorStream<PrimitiveValues<i32>> = Box::pin(stream::empty());

        // Create deduplicate stream
        let dedup_stream = DeduplicateStream::new_from_stream(stream)?;

        // Should return None immediately
        let mut stream = dedup_stream.create_deduplicated_stream();
        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_deduplicate_skip_empty_batches_while_peeking() -> crate::Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let primary_keys = vec!["id".to_string()];

        let _schema = create_test_schema();

        // batch1 ends with 2, then there is an empty batch, and batch3 starts with 3.
        // previous bug: peeking empty batch caused `continue`, dropping tail output of batch1.
        let batch1 = create_test_batch(vec![1, 2], vec!["1", "2"]);
        let empty_batch = create_test_batch(vec![], vec![]);
        let batch3 = create_test_batch(vec![3, 4], vec!["3", "4"]);

        let stream = create_stream(
            vec![batch1, empty_batch, batch3],
            task_ctx,
            primary_keys,
            r1.new_empty(),
        )
        .await?;

        let dedup_stream = DeduplicateStream::new_from_stream(stream)?;
        let result = dedup_stream.create_deduplicated_stream();
        let batches = result.try_collect::<Vec<_>>().await?;
        let batches = batches.into_iter().map(|(_, b)| b).collect::<Vec<_>>();

        assert_batches_eq!(
            &[
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 1  | 1    |",
                "| 2  | 2    |",
                "| 3  | 3    |",
                "| 4  | 4    |",
                "+----+------+",
            ],
            &batches
        );

        Ok(())
    }
}
