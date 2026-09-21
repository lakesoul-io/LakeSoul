// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the gateway integration tests.

#![allow(dead_code)]

use arrow_array::cast::AsArray;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use lakesoul_es_gateway::config::{GatewayConfig, IndexConfig, LakesoulConfig};
use serde_json::Value;
use tower::ServiceExt;

/// Gateway configuration for one test index backed by a local table.
pub fn test_config(
    index_name: &str,
    table: &str,
    dim: Option<usize>,
    hash_bucket_num: usize,
) -> GatewayConfig {
    GatewayConfig {
        server: Default::default(),
        lakesoul: LakesoulConfig {
            namespace: "default".to_string(),
            warehouse_prefix: None,
            endpoint: None,
            s3_bucket: None,
            s3_access_key: None,
            s3_secret_key: None,
            s3_virtual_host_style: false,
            provision_on_start: true,
        },
        defaults: Default::default(),
        indexes: vec![IndexConfig {
            name: index_name.to_string(),
            table: Some(table.to_string()),
            path: Some(format!("file:///tmp/lakesoul_es_gateway_test/{table}")),
            dim,
            hash_bucket_num: Some(hash_bucket_num),
            tokenizer: None,
            with_positions: None,
            nprobe: None,
            content_column: "content".to_string(),
            embedding_column: "embedding".to_string(),
        }],
    }
}

/// Perform a request against the router and decode the JSON body.
pub async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(body) => {
            builder = builder.header("content-type", "application/json");
            builder.body(Body::from(body.to_string())).unwrap()
        }
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, headers, value)
}

/// Run a `count(*)`-style query and return the first value as `u64`.
pub async fn scalar_u64(state: &lakesoul_es_gateway::GatewayState, sql: &str) -> u64 {
    let batches = state
        .session
        .sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let value = batches[0]
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>()
        .value(0);
    value as u64
}

/// Drop a test table and remove its local directory.
pub async fn cleanup(state: &lakesoul_es_gateway::GatewayState, table: &str) {
    let _ = state.client.drop_table(table, "default").await;
    let _ = std::fs::remove_dir_all(format!("/tmp/lakesoul_es_gateway_test/{table}"));
}
