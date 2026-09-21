// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Elasticsearch-shaped errors.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// An error returned to the ES client.
///
/// The body follows the Elasticsearch error shape
/// `{"error": {"type", "reason"}, "status": n}`; the v8 typed client decodes
/// it on non-2xx responses, and the v7 client inspects the status code.
#[derive(Debug)]
pub struct EsError {
    pub status: StatusCode,
    pub error_type: String,
    pub reason: String,
}

impl EsError {
    pub fn new(
        status: StatusCode,
        error_type: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            status,
            error_type: error_type.into(),
            reason: reason.into(),
        }
    }

    pub fn bad_request(reason: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            reason,
        )
    }

    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "unsupported_operation_exception",
            reason,
        )
    }

    pub fn index_not_found(index: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!(
                "no such index [{index}]: the index is not declared in the gateway \
                 configuration"
            ),
        )
    }

    pub fn internal(reason: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_server_error",
            reason,
        )
    }
}

impl IntoResponse for EsError {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "error": {
                "type": self.error_type,
                "reason": self.reason,
                "root_cause": [],
            },
            "status": self.status.as_u16(),
        }));
        (self.status, body).into_response()
    }
}

/// Convert any internal error into a 500, logging the full chain.
pub fn internal(error: impl std::fmt::Display) -> EsError {
    tracing::error!("{error}");
    EsError::internal(error.to_string())
}
