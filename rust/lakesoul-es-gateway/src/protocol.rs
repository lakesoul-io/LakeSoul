// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Protocol-level Elasticsearch compatibility: root info, product header and
//! the refresh no-op.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::state::GatewayState;

/// `GET /`: version and cluster info.
///
/// The reported `version.number` decides which WeKnora ES driver binds to a
/// DB store (7.x → keyword-only, otherwise v8).
pub async fn root(State(state): State<Arc<GatewayState>>) -> impl IntoResponse {
    Json(json!({
        "name": "lakesoul-es-gateway",
        "cluster_name": "lakesoul",
        "cluster_uuid": "lakesoul-es-gateway",
        "version": {
            "number": state.config.server.version,
            "build_flavor": "default",
            "build_type": "lakesoul",
            "build_hash": "lakesoul-es-gateway",
            "build_date": "2026-01-01T00:00:00.000000000Z",
            "build_snapshot": false,
            "lucene_version": "9.0.0",
            "minimum_wire_compatibility_version": "7.17.0",
            "minimum_index_compatibility_version": "7.0.0"
        },
        "tagline": "You Know, for Search"
    }))
}

/// `POST /{index}/_refresh`: writes are searchable synchronously, so this is
/// a no-op for callers that expect the ES refresh API.
pub async fn refresh() -> impl IntoResponse {
    Json(json!({
        "_shards": {"total": 1, "successful": 1, "failed": 0}
    }))
}

/// Add the `X-Elastic-Product: Elasticsearch` header every response needs
/// (the go-elasticsearch v8 client rejects servers without it).
pub async fn product_header(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "X-Elastic-Product",
        HeaderValue::from_static("Elasticsearch"),
    );
    response
}
