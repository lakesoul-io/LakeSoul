// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Index lifecycle endpoints: `HEAD /{index}`, `PUT /{index}` and
//! `GET /{index}/_mapping`.

use std::sync::Arc;

use arrow_schema::DataType;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::error::EsError;
use crate::state::{GatewayState, IndexRuntime};

/// `HEAD /{index}`: declared indexes exist (they are provisioned at startup).
pub async fn head_index(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
) -> StatusCode {
    match state.index(&index) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::NOT_FOUND,
    }
}

/// `PUT /{index}`: validate the declaration.  Settings (`number_of_shards`,
/// `number_of_replicas`) are ignored; the bucket count is fixed by the
/// gateway configuration at provisioning time.
pub async fn put_index(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    let _runtime = state.index(&index)?;
    if !body.is_empty() {
        let value: Value = serde_json::from_slice(&body).map_err(|error| {
            EsError::bad_request(format!("invalid JSON body: {error}"))
        })?;
        if let Some(shards) = value
            .get("settings")
            .and_then(|settings| settings.get("number_of_shards"))
        {
            tracing::warn!(
                index = %index,
                number_of_shards = %shards,
                "ignoring number_of_shards: the bucket count is fixed by the \
                 gateway configuration"
            );
        }
    }
    Ok(Json(json!({
        "acknowledged": true,
        "shards_acknowledged": true,
        "index": index
    })))
}

/// `GET /{index}/_mapping`: the fixed document mapping.
///
/// All filter fields are `keyword` so the WeKnora driver keeps using the
/// bare field names (`chunk_id`, not `chunk_id.keyword`).
pub async fn get_mapping(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
) -> Result<Json<Value>, EsError> {
    let runtime = state.index(&index)?;
    let mut properties = serde_json::Map::new();
    for field in runtime.schema.fields() {
        properties.insert(field.name().clone(), field_mapping(runtime, field.name()));
    }
    Ok(Json(json!({
        index: {
            "mappings": {
                "properties": properties
            }
        }
    })))
}

fn field_mapping(runtime: &IndexRuntime, name: &str) -> Value {
    let data_type = runtime
        .schema
        .field_with_name(name)
        .map(|field| field.data_type())
        .unwrap_or(&DataType::Utf8);
    match data_type {
        DataType::UInt64 | DataType::Int64 => json!({"type": "long"}),
        DataType::Int32 => json!({"type": "integer"}),
        DataType::Boolean => json!({"type": "boolean"}),
        DataType::FixedSizeList(_, dim) => json!({
            "type": "dense_vector",
            "dims": dim,
            "index": true,
            "similarity": "cosine"
        }),
        _ if name == runtime.config.content_column => json!({"type": "text"}),
        _ => json!({"type": "keyword"}),
    }
}
