// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul Contributors

//! Usage examples for the LakeSoul vector-query entries.
//!
//! Companion to `rust/lakesoul-vector/向量查询使用说明.md`:
//! * [`single_query`] — index-only SQL query through the DataFusion session;
//! * [`batch_query`] — the snapshot batch entry on the table provider;
//! * [`session_and_rebuild`] — session construction and an explicit index rebuild.
//!
//! Run with `cargo run -p lakesoul-datafusion --example vector_query_usage`
//! after pointing `LAKESOUL_PG_URL`/`LAKESOUL_PG_USERNAME`/`LAKESOUL_PG_PASSWORD`
//! at a metadata database that contains the `vec_glove` demo table (see the guide).
#![allow(dead_code, unused_imports)]

use std::sync::Arc;

use arrow::array::Int64Array;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchConfig;
use lakesoul_datafusion::vector_batch::VectorBatchRequest;
use lakesoul_datafusion::vector_index::rebuild_vector_index;
use lakesoul_datafusion::{
    MetaDataClient, create_lakesoul_session_config,
    create_lakesoul_session_ctx_with_config,
};
use lakesoul_common::misc::SecretMap;

/// 把湖仓内部的 `rootcause::Report` 之类的错误转换成 `Box<dyn Error>`（std 标准写法；
/// 也可以换成 anyhow 等自定义错误类型）。
fn boxed<E: std::fmt::Display>(error: E) -> Box<dyn std::error::Error> {
    error.to_string().into()
}

async fn single_query(ctx: &datafusion::prelude::SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    let query: Vec<f32> = (0..200).map(|i| ((i as f32) * 0.01).sin()).collect();
    let literal = query.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.vec_glove \
         order by array_distance(vec, ARRAY[{literal}]) limit 100"
    );
    let df = ctx.sql(&sql).await?;
    for batch in df.collect().await? {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for id in ids.iter().flatten() {
            println!("{id}");
        }
    }
    Ok(())
}

async fn batch_query(
    ctx: &datafusion::prelude::SessionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = ctx.state();
    let catalog = ctx.catalog("lakesoul").ok_or("lakesoul catalog missing")?;
    let schema = catalog.schema("default").ok_or("default schema missing")?;
    let table = schema.table("vec_glove").await?.ok_or("table missing")?;
    let provider = table
        .downcast_ref::<LakeSoulTableProvider>()
        .ok_or("not a LakeSoul table")?;

    let queries: Vec<Vec<f32>> = (0..200)
        .map(|q| (0..200).map(|i| ((q * 200 + i) as f32 * 0.001).cos()).collect())
        .collect();

    let request = VectorBatchRequest {
        column: "vec".to_string(),
        queries,
        top_k: 100,
        nprobe: 16,
        parallelism: 1,
        batch_size: 0,
    };
    let results: Vec<Vec<i64>> = provider.vector_batch_search(&state, &request).await?;
    for (query_index, ids) in results.iter().enumerate() {
        println!("query {query_index}: top1={:?} len={}", ids.first(), ids.len());
    }
    Ok(())
}

async fn session_and_rebuild() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(MetaDataClient::from_env().await.map_err(boxed)?);
    let mut session_config = create_lakesoul_session_config().map_err(boxed)?;
    let mut vector = LakeSoulVectorSearchConfig::default();
    vector.nprobe = 16;
    vector.rerank = "index".to_string();
    vector.parallelism = 1;
    vector.deferred_payload = true;
    session_config.options_mut().extensions.insert(vector);
    let ctx = create_lakesoul_session_ctx_with_config(
        client.clone(),
        &CoreArgs::from_env(),
        session_config,
    )
    .map_err(boxed)?;
    let _ = single_query(&ctx).await;
    let _ = batch_query(&ctx).await;
    let rebuilt = rebuild_vector_index(
        client.as_ref(),
        "vec_glove",
        "default",
        &["id".to_string()],
        SecretMap::new(),
    )
    .await
    .map_err(boxed)?;
    println!("rebuilt {rebuilt} shards");
    Ok(())
}

fn main() {}
