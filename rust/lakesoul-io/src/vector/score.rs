// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Attach index-search distances to the candidate rows (index-only ordering).
//!
//! With `vector_search_order_by` the vector search is ordered by the
//! quantized distance computed inside the index instead of an exact re-rank
//! over the vector column: the vector column is not read at all and the
//! ordering value travels as the reserved [`VECTOR_DISTANCE_FIELD`] column,
//! looked up by each row's primary key.  The SQL executor drops the column
//! again after ordering.
//!
//! For L2 the index score is a squared distance, so it is `sqrt`-ed here to
//! match `array_distance` semantics.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use lakesoul_vector::IndexKey;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::report;

use crate::Result;
use crate::index::key::KeyCodec;

/// Reserved column carrying the index distance of each candidate row.
pub const VECTOR_DISTANCE_FIELD: &str = "__lakesoul_vector_distance";

/// Candidate distances to attach, keyed by the arrow-Row encoded primary key.
pub struct VectorDistances {
    /// Encodes the primary-key columns of every output batch.
    pub codec: KeyCodec,
    /// Index distance of every candidate, newest version of a key wins.
    pub scores: HashMap<IndexKey, f32>,
    /// Apply `sqrt` (L2) so the column matches `array_distance`.
    pub sqrt: bool,
}

impl VectorDistances {
    fn batch_distances(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let keys = self.codec.encode_batch(batch)?;
        let values: Vec<f32> = keys
            .iter()
            .map(|key| {
                let score = self
                    .scores
                    .get(key)
                    .copied()
                    // A row that the index did not return must not exist here
                    // (the scan is narrowed to the candidate keys), but sort
                    // it last instead of failing if it does.
                    .unwrap_or(f32::MAX);
                if self.sqrt {
                    score.max(0.0).sqrt()
                } else {
                    score
                }
            })
            .collect();
        Ok(Arc::new(Float32Array::from(values)))
    }
}

/// Append the reserved distance column to a candidate stream.
pub fn append_vector_distances(
    stream: SendableRecordBatchStream,
    distances: VectorDistances,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        VECTOR_DISTANCE_FIELD,
        DataType::Float32,
        false,
    )));
    let output_schema: SchemaRef =
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    let batch_schema = Arc::clone(&output_schema);
    let mapped = stream.map(move |batch| {
        batch.and_then(|batch| {
            let distance = distances.batch_distances(&batch).map_err(|error| {
                DataFusionError::External(report!("{error}").into_boxed_error())
            })?;
            let mut columns = batch.columns().to_vec();
            columns.push(distance);
            RecordBatch::try_new(Arc::clone(&batch_schema), columns)
                .map_err(DataFusionError::from)
        })
    });
    Box::pin(RecordBatchStreamAdapter::new(output_schema, mapped))
}
