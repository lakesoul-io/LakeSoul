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
use futures::{Stream, StreamExt};
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};

use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};

use crate::transform::{transform_record_batch, transform_schema};

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

    fill_default_column: bool,
}

impl DefaultColumnStream {
    pub(crate) fn new_from_stream(
        stream: SendableRecordBatchStream,
        target_schema: SchemaRef,
        fill_default_column: bool,
    ) -> Self {
        DefaultColumnStream {
            schema: transform_schema(target_schema.clone(), stream.schema(), fill_default_column),
            inner_stream: WrappedSendableRecordBatchStream::new(stream),
            fill_default_column,
        }
    }
}

impl Stream for DefaultColumnStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = &mut self.inner_stream.stream;
        return match futures::ready!(stream.poll_next_unpin(cx)) {
            None => Poll::Ready(None),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            Some(Ok(batch)) => {
                let batch = transform_record_batch(self.schema(), batch, self.fill_default_column);
                Poll::Ready(Some(Ok(batch)))
            }
        };
    }
}

impl RecordBatchStream for DefaultColumnStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
