// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `POST /{index}/_delete_by_query`.
//!
//! Terms filters are resolved to primary keys, then a CDC tombstone row is
//! written for every matching document.  Merge-on-read keeps the tombstone
//! as the newest version, and the reader drops tombstoned rows.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use serde_json::{Value, json};

use crate::document::mark_deleted;
use crate::error::EsError;
use crate::query;
use crate::state::GatewayState;

pub async fn delete_by_query(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    let started = Instant::now();
    let runtime = state.index(&index)?;
    let body: Value = serde_json::from_slice(&body)
        .map_err(|error| EsError::bad_request(format!("invalid JSON body: {error}")))?;
    let query_value = body
        .get("query")
        .ok_or_else(|| EsError::bad_request("missing 'query' in delete body"))?;

    let filters = query::extract_filters(query_value, runtime)?;
    let filter = query::filters_to_expr(&filters, runtime)?;
    let rows = query::resolve_rows(&state, runtime, filter).await?;
    let deleted = rows.as_ref().map(|batch| batch.num_rows()).unwrap_or(0);
    if let Some(batch) = rows {
        let batch = mark_deleted(&batch)?;
        state.upsert(runtime, batch).await?;
    }

    Ok(Json(json!({
        "took": started.elapsed().as_millis() as u64,
        "timed_out": false,
        "total": deleted,
        "deleted": deleted,
        "batches": 1,
        "version_conflicts": 0,
        "noops": 0,
        "retries": {"bulk": 0, "search": 0},
        "throttled_millis": 0,
        "requests_per_second": -1.0,
        "throttled_until_millis": 0,
        "failures": []
    })))
}
