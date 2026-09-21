// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Document write endpoints: `_doc` and `_bulk`.
//!
//! Both the v7 and v8 client shapes are accepted.  The gateway generates the
//! internal u64 primary key and returns it as the string `_id`; the client
//! never relies on `_id` (it reconstructs rows from `_source.chunk_id`).

use std::sync::Arc;
use std::time::Instant;

use arrow::buffer::NullBuffer;
use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Int32Array, RecordBatch,
    StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field};
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::EsError;
use crate::schema::{CDC_COLUMN, CDC_DELETE, CDC_INSERT};
use crate::state::{GatewayState, IndexRuntime};

/// One ES document of the WeKnora index model.
#[derive(Debug, Default, Deserialize)]
pub struct EsDocument {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_type: Option<i32>,
    #[serde(default)]
    pub chunk_id: Option<String>,
    #[serde(default)]
    pub knowledge_id: Option<String>,
    #[serde(default)]
    pub knowledge_base_id: Option<String>,
    #[serde(default)]
    pub tag_id: Option<String>,
    #[serde(default)]
    pub embedding: Option<Vec<f64>>,
    #[serde(default)]
    pub is_enabled: Option<bool>,
    #[serde(default)]
    pub is_recommended: Option<bool>,
}

/// `POST /{index}/_doc` (v8).
pub async fn post_doc(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    write_one(&state, &index, &body).await
}

/// `PUT /{index}/_doc/{id}/_create` (v7).
pub async fn put_doc_create(
    State(state): State<Arc<GatewayState>>,
    Path((index, _id)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    write_one(&state, &index, &body).await
}

/// `PUT /{index}/_doc/{id}`.
pub async fn put_doc(
    State(state): State<Arc<GatewayState>>,
    Path((index, _id)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    write_one(&state, &index, &body).await
}

/// `POST /{index}/_doc/{id}`.
pub async fn post_doc_with_id(
    State(state): State<Arc<GatewayState>>,
    Path((index, _id)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    write_one(&state, &index, &body).await
}

async fn write_one(
    state: &GatewayState,
    index: &str,
    body: &Bytes,
) -> Result<Json<Value>, EsError> {
    let runtime = state.index(index)?;
    let document: EsDocument = serde_json::from_slice(body)
        .map_err(|error| EsError::bad_request(format!("invalid document: {error}")))?;
    let pk = generate_pk();
    write_documents(state, runtime, vec![(pk, document)]).await?;
    Ok(Json(json!({
        "_index": index,
        "_id": pk.to_string(),
        "_version": 1,
        "result": "created",
        "_shards": {"total": 1, "successful": 1, "failed": 0},
        "_seq_no": 0,
        "_primary_term": 1
    })))
}

/// `POST /{index}/_bulk`: NDJSON with `index` (v7, carries `_id`) or
/// `create` (v8, no `_id`) actions.
pub async fn post_bulk(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    let started = Instant::now();
    let runtime = state.index(&index)?;
    let text = std::str::from_utf8(&body).map_err(|error| {
        EsError::bad_request(format!("bulk body is not UTF-8: {error}"))
    })?;

    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let mut pending: Vec<(u64, EsDocument)> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    while let Some(action_line) = lines.next() {
        let action: Value = serde_json::from_str(action_line).map_err(|error| {
            EsError::bad_request(format!("malformed bulk action line: {error}"))
        })?;
        let Some((operation, _meta)) =
            action.as_object().and_then(|object| object.iter().next())
        else {
            return Err(EsError::bad_request(
                "malformed bulk action: expected an action object",
            ));
        };
        match operation.as_str() {
            "index" | "create" => {
                let document_line = lines.next().ok_or_else(|| {
                    EsError::bad_request(format!(
                        "bulk '{operation}' action without a document line"
                    ))
                })?;
                let document: EsDocument =
                    serde_json::from_str(document_line).map_err(|error| {
                        EsError::bad_request(format!("invalid bulk document: {error}"))
                    })?;
                let pk = generate_pk();
                pending.push((pk, document));
                items.push(json!({
                    operation: {
                        "_index": index,
                        "_id": pk.to_string(),
                        "_version": 1,
                        "result": "created",
                        "status": 201,
                        "_shards": {"total": 1, "successful": 1, "failed": 0}
                    }
                }));
            }
            other => {
                return Err(EsError::unsupported(format!(
                    "unsupported bulk action '{other}': only index and create are supported"
                )));
            }
        }
    }

    if !pending.is_empty() {
        write_documents(&state, runtime, pending).await?;
    }
    Ok(Json(json!({
        "took": started.elapsed().as_millis() as u64,
        "errors": false,
        "items": items
    })))
}

/// Write `(primary key, document)` rows through the LakeSoul upsert path.
pub async fn write_documents(
    state: &GatewayState,
    runtime: &IndexRuntime,
    documents: Vec<(u64, EsDocument)>,
) -> Result<(), EsError> {
    let batch = document_batch(runtime, &documents)?;
    state.upsert(runtime, batch).await
}

/// Build the record batch of one write from the fixed document schema.
fn document_batch(
    runtime: &IndexRuntime,
    documents: &[(u64, EsDocument)],
) -> Result<RecordBatch, EsError> {
    let count = documents.len();
    let ids: Vec<u64> = documents.iter().map(|(pk, _)| *pk).collect();
    let content: Vec<Option<&str>> = documents
        .iter()
        .map(|(_, doc)| doc.content.as_deref())
        .collect();
    let source_ids: Vec<Option<&str>> = documents
        .iter()
        .map(|(_, doc)| doc.source_id.as_deref())
        .collect();
    let source_types: Vec<Option<i32>> =
        documents.iter().map(|(_, doc)| doc.source_type).collect();
    let chunk_ids: Vec<Option<&str>> = documents
        .iter()
        .map(|(_, doc)| doc.chunk_id.as_deref())
        .collect();
    let knowledge_ids: Vec<Option<&str>> = documents
        .iter()
        .map(|(_, doc)| doc.knowledge_id.as_deref())
        .collect();
    let knowledge_base_ids: Vec<Option<&str>> = documents
        .iter()
        .map(|(_, doc)| doc.knowledge_base_id.as_deref())
        .collect();
    let tag_ids: Vec<Option<&str>> = documents
        .iter()
        .map(|(_, doc)| doc.tag_id.as_deref())
        .collect();
    let enabled: Vec<Option<bool>> =
        documents.iter().map(|(_, doc)| doc.is_enabled).collect();
    let recommended: Vec<Option<bool>> = documents
        .iter()
        .map(|(_, doc)| doc.is_recommended)
        .collect();

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(ids)),
        Arc::new(StringArray::from(content)),
        Arc::new(StringArray::from(source_ids)),
        Arc::new(Int32Array::from(source_types)),
        Arc::new(StringArray::from(chunk_ids)),
        Arc::new(StringArray::from(knowledge_ids)),
        Arc::new(StringArray::from(knowledge_base_ids)),
        Arc::new(StringArray::from(tag_ids)),
    ];

    if let Some(dim) = runtime.dim {
        let mut values: Vec<f32> = Vec::with_capacity(count * dim);
        let mut validity: Vec<bool> = Vec::with_capacity(count);
        for (_, document) in documents {
            match &document.embedding {
                Some(embedding) => {
                    if embedding.len() != dim {
                        return Err(EsError::bad_request(format!(
                            "embedding dimension {} does not match the index dimension {dim}",
                            embedding.len()
                        )));
                    }
                    values.extend(normalize(embedding));
                    validity.push(true);
                }
                None => {
                    values.extend(std::iter::repeat_n(0.0, dim));
                    validity.push(false);
                }
            }
        }
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let list = FixedSizeListArray::try_new(
            item_field,
            dim as i32,
            Arc::new(Float32Array::from(values)),
            Some(NullBuffer::from(validity)),
        )
        .map_err(|error| EsError::bad_request(format!("invalid embedding: {error}")))?;
        columns.push(Arc::new(list));
    }

    columns.push(Arc::new(BooleanArray::from(enabled)));
    columns.push(Arc::new(BooleanArray::from(recommended)));
    columns.push(Arc::new(StringArray::from(vec![Some(CDC_INSERT); count])));

    RecordBatch::try_new(Arc::clone(&runtime.schema), columns)
        .map_err(|error| EsError::bad_request(format!("document batch: {error}")))
}

/// L2-normalize an embedding so the IP index computes cosine similarity.
fn normalize(embedding: &[f64]) -> Vec<f32> {
    let norm = embedding
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    if norm <= f64::EPSILON {
        return embedding.iter().map(|value| *value as f32).collect();
    }
    embedding
        .iter()
        .map(|value| (value / norm) as f32)
        .collect()
}

/// Random u64 primary key.  `_id` is opaque to the client, and the key space
/// is large enough that collisions are not a practical concern.
fn generate_pk() -> u64 {
    let (high, _low) = uuid::Uuid::new_v4().as_u64_pair();
    if high == 0 { 1 } else { high }
}

/// Replace one column of a full-row batch, keeping the schema.
pub fn replace_column(
    batch: &RecordBatch,
    name: &str,
    values: ArrayRef,
) -> Result<RecordBatch, EsError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| EsError::bad_request(format!("unknown field '{name}'")))?;
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[index] = values;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|error| EsError::internal(format!("replace column '{name}': {error}")))
}

/// Mark every row of a full-row batch as deleted via the CDC column.
pub fn mark_deleted(batch: &RecordBatch) -> Result<RecordBatch, EsError> {
    let values: ArrayRef =
        Arc::new(StringArray::from(vec![Some(CDC_DELETE); batch.num_rows()]));
    replace_column(batch, CDC_COLUMN, values)
}
