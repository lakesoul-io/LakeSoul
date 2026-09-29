// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tracing utilities for DataFusion execution streams.

use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Once;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use tracing::Span;

const STREAMS_TOTAL: &str = "lakesoul_execution_streams_total";
const STREAMS_ACTIVE: &str = "lakesoul_execution_streams_active";
const STREAM_DURATION: &str = "lakesoul_execution_stream_duration_seconds";
const OUTPUT_BATCHES_TOTAL: &str = "lakesoul_execution_output_batches_total";
const OUTPUT_ROWS_TOTAL: &str = "lakesoul_execution_output_rows_total";

static DESCRIBE_METRICS: Once = Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            STREAMS_TOTAL,
            "Total number of LakeSoul execution streams by operator and outcome"
        );
        metrics::describe_gauge!(
            STREAMS_ACTIVE,
            "Current number of active LakeSoul execution streams"
        );
        metrics::describe_histogram!(
            STREAM_DURATION,
            metrics::Unit::Seconds,
            "LakeSoul execution stream lifetime from creation to completion or drop"
        );
        metrics::describe_counter!(
            OUTPUT_BATCHES_TOTAL,
            "Total record batches emitted by LakeSoul execution streams"
        );
        metrics::describe_counter!(
            OUTPUT_ROWS_TOTAL,
            "Total rows emitted by LakeSoul execution streams"
        );
    });
}

/// Keeps `span` alive for the lifetime of `stream` and enters it whenever the
/// stream is polled.
///
/// Callers should declare the `outcome`, `output_batches`, `output_rows`, and
/// `error` fields on `span`; this wrapper records them when the stream finishes.
/// Dropping a partially consumed stream records the outcome as `dropped`.
pub fn instrument_record_batch_stream(
    stream: SendableRecordBatchStream,
    span: Span,
    operator: &'static str,
) -> SendableRecordBatchStream {
    describe_metrics();
    metrics::gauge!(STREAMS_ACTIVE, "operator" => operator).increment(1.0);
    Box::pin(TracedRecordBatchStream {
        schema: stream.schema(),
        stream,
        span: Some(span),
        operator,
        started: Instant::now(),
        output_batches: 0,
        output_rows: 0,
        finished: false,
    })
}

struct TracedRecordBatchStream {
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
    span: Option<Span>,
    operator: &'static str,
    started: Instant,
    output_batches: u64,
    output_rows: u64,
    finished: bool,
}

impl TracedRecordBatchStream {
    fn finish(&mut self, outcome: &'static str, error: Option<&str>) {
        if self.finished {
            return;
        }
        self.finished = true;

        metrics::gauge!(STREAMS_ACTIVE, "operator" => self.operator).decrement(1.0);
        metrics::counter!(
            STREAMS_TOTAL,
            "operator" => self.operator,
            "outcome" => outcome
        )
        .increment(1);
        metrics::histogram!(
            STREAM_DURATION,
            "operator" => self.operator,
            "outcome" => outcome
        )
        .record(self.started.elapsed().as_secs_f64());
        metrics::counter!(OUTPUT_BATCHES_TOTAL, "operator" => self.operator)
            .increment(self.output_batches);
        metrics::counter!(OUTPUT_ROWS_TOTAL, "operator" => self.operator)
            .increment(self.output_rows);

        let Some(span) = self.span.take() else {
            return;
        };
        span.record("outcome", outcome);
        span.record("output_batches", self.output_batches);
        span.record("output_rows", self.output_rows);
        if let Some(error) = error {
            span.record("error", error);
            tracing::error!(
                parent: &span,
                error,
                output_batches = self.output_batches,
                output_rows = self.output_rows,
                "record batch stream failed"
            );
        } else {
            tracing::debug!(
                parent: &span,
                outcome,
                output_batches = self.output_batches,
                output_rows = self.output_rows,
                "record batch stream finished"
            );
        }
    }
}

impl Debug for TracedRecordBatchStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TracedRecordBatchStream")
            .field("operator", &self.operator)
            .field("output_batches", &self.output_batches)
            .field("output_rows", &self.output_rows)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl Stream for TracedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let span = self.span.as_ref().cloned().unwrap_or_else(Span::none);
        let poll = {
            let _entered = span.enter();
            self.stream.as_mut().poll_next(cx)
        };

        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                self.output_batches += 1;
                self.output_rows += batch.num_rows() as u64;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                let message = error.to_string();
                self.finish("error", Some(&message));
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.finish("completed", None);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TracedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for TracedRecordBatchStream {
    fn drop(&mut self) {
        self.finish("dropped", None);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::TryStreamExt;
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    #[tokio::test]
    async fn instrumented_stream_preserves_schema_and_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let input = futures::stream::iter(vec![Ok(batch.clone())]);
        let input = Box::pin(RecordBatchStreamAdapter::new(schema.clone(), input))
            as SendableRecordBatchStream;
        let span = tracing::info_span!(
            "test_execution",
            outcome = tracing::field::Empty,
            output_batches = tracing::field::Empty,
            output_rows = tracing::field::Empty,
            error = tracing::field::Empty,
        );

        let stream = instrument_record_batch_stream(input, span, "test");
        assert_eq!(stream.schema(), schema);
        let output: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(output, vec![batch]);
    }

    #[test]
    fn instrumented_stream_records_low_cardinality_metrics() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int32,
                    false,
                )]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
                )
                .unwrap();
                let input = futures::stream::iter(vec![Ok(batch)]);
                let input = Box::pin(RecordBatchStreamAdapter::new(schema, input))
                    as SendableRecordBatchStream;

                let stream = instrument_record_batch_stream(
                    input,
                    tracing::Span::none(),
                    "test_operator",
                );
                let _: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            });
        });

        let rendered = handle.render();
        assert!(rendered.contains(
            "lakesoul_execution_streams_total{operator=\"test_operator\",outcome=\"completed\"} 1"
        ));
        assert!(rendered.contains(
            "lakesoul_execution_output_batches_total{operator=\"test_operator\"} 1"
        ));
        assert!(rendered.contains(
            "lakesoul_execution_output_rows_total{operator=\"test_operator\"} 3"
        ));
        assert!(
            rendered.contains(
                "lakesoul_execution_streams_active{operator=\"test_operator\"} 0"
            )
        );
    }
}
