use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result as DFResult};
use futures::{Stream, StreamExt};
use rootcause::compat::boxed_error::IntoBoxedError;

use crate::helpers::transform::{transform_record_batch, transform_schema};

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
    merged_schema: SchemaRef,

    /// The sorted input streams to merge together
    // streams: MergingStreams,
    inner_stream: Vec<WrappedSendableRecordBatchStream>,

    cur_stream_idx: usize,

    use_default: bool,

    default_column_value: Arc<HashMap<String, String>>,
}

impl DefaultColumnStream {
    pub(crate) fn new_from_stream(
        stream: SendableRecordBatchStream,
        target_schema: SchemaRef,
    ) -> Self {
        DefaultColumnStream {
            merged_schema: transform_schema(target_schema, stream.schema(), false),
            inner_stream: vec![WrappedSendableRecordBatchStream::new(stream)],
            use_default: false,
            cur_stream_idx: 0,
            default_column_value: Arc::new(Default::default()),
        }
    }

    pub(crate) fn new_from_streams_with_default(
        streams: Vec<SendableRecordBatchStream>,
        merged_schema: SchemaRef,
        default_column_value: Arc<HashMap<String, String>>,
    ) -> Self {
        let use_default = true;
        DefaultColumnStream {
            merged_schema,
            inner_stream: streams
                .into_iter()
                .map(WrappedSendableRecordBatchStream::new)
                .collect::<Vec<_>>(),
            use_default,
            cur_stream_idx: 0,
            default_column_value,
        }
    }
}

impl Stream for DefaultColumnStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.cur_stream_idx >= self.inner_stream.len() {
                return Poll::Ready(None);
            }
            let idx = self.cur_stream_idx;
            let stream = &mut self.inner_stream[idx].stream;
            return match futures::ready!(stream.poll_next_unpin(cx)) {
                None => {
                    self.cur_stream_idx += 1;
                    continue;
                }
                Some(Err(e)) => Poll::Ready(Some(Err(e))),
                Some(Ok(batch)) => {
                    let batch = transform_record_batch(
                        self.schema(),
                        batch,
                        self.use_default,
                        self.default_column_value.clone(),
                    )
                    .map_err(|e| DataFusionError::External(e.into_boxed_error()));
                    Poll::Ready(Some(batch))
                }
            };
        }
    }
}

impl RecordBatchStream for DefaultColumnStream {
    fn schema(&self) -> SchemaRef {
        self.merged_schema.clone()
    }
}
