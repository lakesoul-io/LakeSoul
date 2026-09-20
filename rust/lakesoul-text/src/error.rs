// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Errors of the text index crate.

/// Error type of the text index crate.
#[derive(Debug, thiserror::Error)]
pub enum TextError {
    /// Tantivy index error.
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    /// Query parsing error.
    #[error("query: {0}")]
    Query(#[from] tantivy::query::QueryParserError),

    /// Local IO error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Object-store error.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// JSON (config, bundle footer) error.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid configuration or corrupted index data.
    #[error("{0}")]
    Invalid(String),
}

/// Result alias of the text index crate.
pub type Result<T> = std::result::Result<T, TextError>;
