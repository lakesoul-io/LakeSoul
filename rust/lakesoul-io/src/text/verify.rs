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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{BooleanArray, Float32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::DataFusionError;
use datafusion_execution::SendableRecordBatchStream;
use futures::Stream;
use lakesoul_common::IndexKind;
use lakesoul_text::{TextIndexConfig, matching_ids};

use crate::Result as IoResult;
use crate::config::LakeSoulIOConfig;
use crate::index::options::SearchRequest;
use crate::text::reader::collect_text_values;

/// Output column carrying the BM25 score of a verified row when
/// `text_search_scores=true` is set.  The name is reserved.
pub const TEXT_SCORE_FIELD: &str = "__lakesoul_text_score";

/// What to verify after the merge: the text column, the query and the
/// analyzer configuration recovered from the index commit header.
#[derive(Debug, Clone)]
pub struct TextVerifyRequest {
    pub column: String,
    pub query: String,
    pub config: TextIndexConfig,
    /// Expose the BM25 score of each verified row.
    pub with_scores: bool,
}

/// Build the verification request of the reader's text search options, when
/// a text search is configured and its index commit was resolved.
pub fn text_verify_request(io_config: &LakeSoulIOConfig) -> Option<TextVerifyRequest> {
    let with_scores = io_config
        .option(crate::config::OPTION_KEY_TEXT_SEARCH_SCORES)
        .as_deref()
        == Some("true");
    // SQL pushdown keeps an exact `text_match` predicate above the scan and
    // turns this pass off to avoid verifying twice.  Requesting scores needs
    // the pass: stale candidates must not carry a score.
    if !with_scores
        && io_config
            .option(crate::config::OPTION_KEY_TEXT_SEARCH_VERIFY)
            .as_deref()
            == Some("false")
    {
        return None;
    }
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
        with_scores,
    })
}

/// The schema a [`TextVerifyStream`] produces: the caller's schema, with the
/// reserved score field appended when it is not part of the caller's schema
/// and scores are requested.
pub fn text_verify_output_schema(
    request: &TextVerifyRequest,
    original: &SchemaRef,
) -> SchemaRef {
    if !request.with_scores || original.field_with_name(TEXT_SCORE_FIELD).is_ok() {
        return Arc::clone(original);
    }
    let mut fields: Vec<Field> = original
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(TEXT_SCORE_FIELD, DataType::Float32, true));
    Arc::new(Schema::new(fields))
}

/// Drop the reserved score field from a schema.
///
/// Callers may declare the score field up front (so a lazy engine can plan
/// against it); the field is not a physical file column, so the reader
/// removes it before building the scan plan and the verification stream
/// fills it back in at the same position.
pub fn without_text_score_field(schema: &SchemaRef) -> SchemaRef {
    if schema.field_with_name(TEXT_SCORE_FIELD).is_err() {
        return Arc::clone(schema);
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .filter(|field| field.name() != TEXT_SCORE_FIELD)
        .map(|field| field.as_ref().clone())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Filters a merged stream to the rows matching the text query exactly,
/// projects the temporary text column back out and fills in the reserved
/// score column when scores were requested.
pub struct TextVerifyStream {
    input: SendableRecordBatchStream,
    request: TextVerifyRequest,
    pk_column: String,
    /// Indices of the caller's output schema inside the verified batch.
    projection: Vec<usize>,
    /// Position of the reserved score column in the output schema.
    score_position: Option<usize>,
    /// Schema the stream produces (includes the score column if requested).
    output_schema: SchemaRef,
    /// BM25 scores of the index candidates, by primary key.
    candidate_scores: HashMap<u64, f32>,
}

impl TextVerifyStream {
    pub fn try_new(
        input: SendableRecordBatchStream,
        request: TextVerifyRequest,
        pk_column: String,
        output_schema: SchemaRef,
        candidate_scores: HashMap<u64, f32>,
    ) -> IoResult<Self> {
        let input_schema = input.schema();
        let mut projection = Vec::with_capacity(output_schema.fields().len());
        let mut score_position = None;
        for (position, field) in output_schema.fields().iter().enumerate() {
            if request.with_scores && field.name() == TEXT_SCORE_FIELD {
                score_position = Some(position);
                continue;
            }
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
            score_position,
            output_schema,
            candidate_scores,
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

        // Scores stay aligned with the surviving rows: the same mask selects
        // the row-aligned candidate scores.  Computed before the batch is
        // filtered, while the mask is still owned here.
        let scores = self.score_position.map(|_| {
            let scores: Vec<f32> = value_of_row
                .iter()
                .zip(&mask)
                .filter_map(|(slot, matched)| {
                    if !*matched {
                        return None;
                    }
                    let slot = slot.expect("matched rows carry a primary key");
                    let (id, _) = &matched_values[slot];
                    Some(*self.candidate_scores.get(id).unwrap_or(&0.0))
                })
                .collect();
            scores
        });

        let filtered =
            arrow::compute::filter_record_batch(batch, &BooleanArray::from(mask))
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        let projected = filtered
            .project(&self.projection)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let Some(score_position) = self.score_position else {
            return Ok(Some(projected));
        };
        let score_array = Arc::new(Float32Array::from(
            scores.expect("scores were computed when a score field is present"),
        ));
        let mut columns: Vec<arrow_array::ArrayRef> =
            Vec::with_capacity(self.output_schema.fields().len());
        let mut projected_columns = projected.columns().iter();
        for position in 0..self.output_schema.fields().len() {
            if position == score_position {
                columns.push(score_array.clone());
            } else if let Some(column) = projected_columns.next() {
                columns.push(Arc::clone(column));
            }
        }
        if score_position >= columns.len() {
            columns.push(score_array);
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        Ok(Some(batch))
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
