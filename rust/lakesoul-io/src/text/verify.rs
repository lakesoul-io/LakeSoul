// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Exact verification of text index candidates.
//!
//! The index only provides candidates (primary keys whose *indexed* text may
//! be stale); this stream filters each merged batch down to the rows whose
//! **current** text really matches the query, using the same analyzer as the
//! index.  It runs after merge-on-read and after CDC delete tombstones were
//! dropped, and it removes the text column from the output when the caller
//! did not project it (the reader temporarily adds it to the scan schema so
//! verification can read it).

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_execution::SendableRecordBatchStream;
use futures::Stream;
use lakesoul_common::IndexKind;
use lakesoul_text::{TextIndexConfig, matching_ids};

use crate::Result as IoResult;
use crate::config::LakeSoulIOConfig;
use crate::index::options::SearchRequest;
use crate::text::reader::collect_text_values;

/// What to verify after the merge: the text column, the query and the
/// analyzer configuration recovered from the index commit header.
#[derive(Debug, Clone)]
pub struct TextVerifyRequest {
    pub column: String,
    pub query: String,
    pub config: TextIndexConfig,
}

/// Build the verification request of the reader's text search options, when
/// a text search is configured and its index commit was resolved.
pub fn text_verify_request(io_config: &LakeSoulIOConfig) -> Option<TextVerifyRequest> {
    let request = SearchRequest::from_config(io_config, IndexKind::Text)?;
    let shard = io_config
        .resolved_index_shards_slice()
        .iter()
        .find(|shard| shard.is_kind(IndexKind::Text))?;
    let config: TextIndexConfig = serde_json::from_slice(&shard.header).ok()?;
    if config.column_name != request.column {
        return None;
    }
    Some(TextVerifyRequest {
        column: request.column,
        query: request.query,
        config,
    })
}

/// Filters a merged stream to the rows matching the text query exactly and
/// projects the temporary text column back out.
pub struct TextVerifyStream {
    input: SendableRecordBatchStream,
    request: TextVerifyRequest,
    pk_column: String,
    /// Indices of the caller's output schema inside the verified batch.
    projection: Vec<usize>,
}

impl TextVerifyStream {
    pub fn try_new(
        input: SendableRecordBatchStream,
        request: TextVerifyRequest,
        pk_column: String,
        output_schema: SchemaRef,
    ) -> IoResult<Self> {
        let input_schema = input.schema();
        let mut projection = Vec::with_capacity(output_schema.fields().len());
        for field in output_schema.fields() {
            let index = input_schema.index_of(field.name()).map_err(|error| {
                rootcause::report!(
                    "text verify output column '{}' missing from the scan: {}",
                    field.name(),
                    error
                )
            })?;
            projection.push(index);
        }
        Ok(Self {
            input,
            request,
            pk_column,
            projection,
        })
    }

    fn verify(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let rows = collect_text_values(batch, &self.pk_column, &self.request.column)
            .map_err(|error| {
                DataFusionError::Execution(format!("text verify: {error}"))
            })?;

        // Row-aligned mask: null primary keys never match.
        let mut matched_values: Vec<(u64, Option<String>)> = Vec::new();
        let mut value_of_row: Vec<Option<usize>> = Vec::with_capacity(rows.len());
        for row in rows {
            match row {
                Some((id, text)) => {
                    value_of_row.push(Some(matched_values.len()));
                    matched_values.push((id, text));
                }
                None => value_of_row.push(None),
            }
        }
        let mut mask = vec![false; value_of_row.len()];
        if !matched_values.is_empty() {
            let matched =
                matching_ids(&self.request.config, &matched_values, &self.request.query)
                    .map_err(|error| {
                        DataFusionError::Execution(format!("text verify: {error}"))
                    })?;
            for (row, slot) in value_of_row.iter().enumerate() {
                if let Some(slot) = slot
                    && matched.contains(&matched_values[*slot].0)
                {
                    mask[row] = true;
                }
            }
        }

        let filtered =
            arrow::compute::filter_record_batch(batch, &BooleanArray::from(mask))
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        let projected = filtered
            .project(&self.projection)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        Ok(Some(projected))
    }
}

impl Stream for TextVerifyStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.input).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(batch))) => match this.verify(&batch) {
                    Err(error) => return Poll::Ready(Some(Err(error))),
                    Ok(None) => continue,
                    Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                },
            }
        }
    }
}
