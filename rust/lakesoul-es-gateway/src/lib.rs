// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Elasticsearch-compatible API gateway over LakeSoul.
//!
//! The gateway exposes the small Elasticsearch REST surface a RAG client
//! (e.g. WeKnora) uses: index lifecycle, document writes, delete/update by
//! query and search.  Tables are pre-declared in a TOML configuration; the
//! gateway provisions them with a fixed document schema (text index on the
//! content column, optional vector index on the embedding column, an
//! internal CDC column for deletes) and rejects undeclared indexes.

pub mod config;
pub mod delete;
pub mod document;
pub mod error;
pub mod index_api;
pub mod protocol;
pub mod provision;
pub mod query;
pub mod schema;
pub mod state;
pub mod update;

use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use axum::middleware;
use axum::routing::{get, post, put};

pub use config::GatewayConfig;
pub use error::EsError;
pub use state::{GatewayState, IndexRuntime};

/// Build the gateway state: connect to metadata, create sessions and
/// provision every declared index.
pub async fn build_state(config: GatewayConfig) -> anyhow::Result<Arc<GatewayState>> {
    let state = Arc::new(GatewayState::try_new(config).await?);
    if state.config.lakesoul.provision_on_start {
        provision::provision_all(&state)
            .await
            .context("failed to provision declared indexes")?;
    }
    Ok(state)
}

/// Assemble the HTTP router.
pub fn build_router(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/", get(protocol::root))
        .route(
            "/{index}",
            axum::routing::head(index_api::head_index).put(index_api::put_index),
        )
        .route("/{index}/_mapping", get(index_api::get_mapping))
        .route("/{index}/_doc", post(document::post_doc))
        .route(
            "/{index}/_doc/{id}",
            put(document::put_doc).post(document::post_doc_with_id),
        )
        .route("/{index}/_doc/{id}/_create", put(document::put_doc_create))
        .route("/{index}/_bulk", post(document::post_bulk))
        .route("/{index}/_delete_by_query", post(delete::delete_by_query))
        .route("/{index}/_update_by_query", post(update::update_by_query))
        .route("/{index}/_refresh", post(protocol::refresh))
        .layer(middleware::from_fn(protocol::product_header))
        .with_state(state)
}
