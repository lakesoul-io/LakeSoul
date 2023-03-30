/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use arrow_array::RecordBatchOptions;
use futures::Stream;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use arrow_schema::Schema;

use datafusion::physical_plan::RecordBatchStream;

#[derive(Debug)]
pub(crate) struct EmptySchemaStream {
    batch_size: usize,

    remaining_num_rows: usize,

    schema: SchemaRef,
}

impl EmptySchemaStream {
    pub(crate) fn new(batch_size: usize, num_rows: usize) -> Self {
        EmptySchemaStream {
            batch_size,
            remaining_num_rows: num_rows,
            schema: Arc::new(Schema::empty()),
        }
    }
}

impl Stream for EmptySchemaStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining_num_rows > 0 {
            let row_count = if self.batch_size < self.remaining_num_rows {
                self.batch_size
            } else {
                self.remaining_num_rows
            };
            self.remaining_num_rows -= row_count;
            let batch = RecordBatch::try_new_with_options(
                self.schema(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            );
            Poll::Ready(Some(batch))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for EmptySchemaStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
