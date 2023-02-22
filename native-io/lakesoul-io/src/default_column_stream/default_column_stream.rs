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
use std::task::{Context, Poll};
use std::pin::Pin;
use std::fmt::{Debug, Formatter};
use futures::{Stream, StreamExt};


use arrow::datatypes::SchemaRef;
use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use arrow_array::new_null_array;

use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};


pub(crate) struct WrappedSendableRecordBatchStream {
    stream: SendableRecordBatchStream,
}

impl Debug for WrappedSendableRecordBatchStream {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WrappedSendableRecordBatchStream")
    }
}

impl WrappedSendableRecordBatchStream {
    pub(crate) fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

#[derive(Debug)]
pub(crate) struct DefaultColumnStream {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    // streams: MergingStreams,
    inner_stream: WrappedSendableRecordBatchStream,
}

impl DefaultColumnStream{
    pub(crate) fn new_from_stream(
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        DefaultColumnStream{
            schema,
            inner_stream: WrappedSendableRecordBatchStream::new(stream)
        }
    }
}

impl Stream for DefaultColumnStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = &mut self.inner_stream.stream;
        match futures::ready!(stream.poll_next_unpin(cx)) {
            None => return Poll::Ready(None),
            Some(Err(e)) => {
                return Poll::Ready(Some(Err(e)))
            }
            Some(Ok(batch)) => {
                let columns = self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| {
                        match batch.schema().column_with_name(field.name()) {
                            Some((idx, _)) => batch.column(idx).clone(),
                            None => new_null_array(&field.data_type().clone(), batch.num_rows())
                            
                        }
                    })
                    .collect::<Vec<_>>();
                return Poll::Ready(Some(RecordBatch::try_new(self.schema.clone(), columns)))
            }
        }
    }
}

impl RecordBatchStream for DefaultColumnStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}