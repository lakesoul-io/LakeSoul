// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul Contributors

//! Runnable end-to-end example for `rust/lakesoul-vector/向量查询使用说明.md`.
//!
//! It builds the guide's demo table, writes rows, then runs both query entries:
//! * single-query SQL (`order by array_distance(...) limit k`);
//! * the snapshot batch entry (`LakeSoulTableProvider::vector_batch_search`).
//!
//! ```text
//! cargo run --release -p lakesoul-datafusion --example vector_query_usage
//! ```
//!
//! Requires the metadata database from `LAKESOUL_PG_URL` (defaults to the local
//! test database).  The demo table is created under `/tmp/demo_vec`; drop it
//! with `catalog.drop_table("demo_vec", "default")` when done.
//!
//! For the measurable throughput check on a real GloVe table see the companion
//! example `vector_query_qps.rs`.

use std::sync::Arc;

use arrow::array::Int64Array;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchConfig;
use lakesoul_datafusion::vector_batch::VectorBatchRequest;
use lakesoul_datafusion::{
    MetaDataClient, create_lakesoul_session_config,
    create_lakesoul_session_ctx_with_config,
};

/// Converts a lakehouse error (e.g. `rootcause::Report`) into `Box<dyn Error>`;
/// replace with anyhow or your own error type as you like.
fn boxed<E: std::fmt::Display>(error: E) -> Box<dyn std::error::Error> {
    error.to_string().into()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1) 会话：元数据客户端 + 向量检索选项
    let client = Arc::new(MetaDataClient::from_env().await.map_err(boxed)?);
    let mut session_config = create_lakesoul_session_config().map_err(boxed)?;
    let mut vector = LakeSoulVectorSearchConfig::default();
    vector.nprobe = 4; // 小 demo 表用 4 个聚类足够
    vector.rerank = "index".to_string(); // index-only 排序
    session_config.options_mut().extensions.insert(vector);
    let ctx = create_lakesoul_session_ctx_with_config(
        client,
        &CoreArgs::from_env(),
        session_config,
    )
    .map_err(boxed)?;

    // 2) 建表：声明向量索引（dim=4、nlist=4、bits=1、L2、2 个 hash 桶）
    let ddl = r#"CREATE EXTERNAL TABLE IF NOT EXISTS "lakesoul".default.demo_vec (
        id BIGINT NOT NULL PRIMARY KEY,
        vec DOUBLE[] NOT NULL
    ) STORED AS LAKESOUL LOCATION '/tmp/demo_vec'
    OPTIONS ('vector_index_columns'
        '[{"column":"vec","dim":4,"nlist":4,"total_bits":1,"metric":"L2"}]',
        'hash_bucket_num' '2')"#;
    ctx.sql(ddl).await?.collect().await?;
    println!("created (or reused) table lakesoul.default.demo_vec");

    // 3) 写入 100 行（inline 模式下索引随提交自动构建）
    let insert = r#"INSERT OR REPLACE INTO "lakesoul".default.demo_vec
        SELECT CAST(g.value AS BIGINT),
               ARRAY[(g.value % 10) / 10.0, 0.1, 0.2, 0.3]
        FROM generate_series(0, 99) AS g(value)"#;
    ctx.sql(insert).await?.collect().await?;
    println!("inserted 100 rows");

    // 4) 单查询 SQL
    let sql = r#"select id from "lakesoul".default.demo_vec
        order by array_distance(vec, ARRAY[0.1, 0.1, 0.2, 0.3]) limit 10"#;
    let mut single_ids = Vec::new();
    for batch in ctx.sql(sql).await?.collect().await? {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        single_ids.extend(ids.iter().flatten());
    }
    println!("single-query top-10 ids: {single_ids:?}");

    // 5) 批量入口
    let state = ctx.state();
    let catalog = ctx.catalog("lakesoul").expect("lakesoul catalog");
    let schema = catalog.schema("default").expect("default schema");
    let table = schema
        .table("demo_vec")
        .await?
        .expect("demo_vec table is registered");
    let provider = table
        .downcast_ref::<LakeSoulTableProvider>()
        .expect("demo_vec is a LakeSoul table");
    let request = VectorBatchRequest {
        column: "vec".to_string(),
        queries: vec![vec![0.1, 0.1, 0.2, 0.3], vec![0.2, 0.1, 0.2, 0.3]],
        top_k: 10,
        nprobe: 4,
        parallelism: 1,
        batch_size: 0,
        exact: false,
        exact_overfetch: 1,
        metric: "L2".to_string(),
    };
    let results = provider.vector_batch_search(&state, &request).await?;
    for (query, ids) in results.iter().enumerate() {
        println!("batch query {query}: top-10 ids: {ids:?}");
    }

    // 6) 一致性检查：两条路径对同一个 query 返回相同的主键集合
    let batch_first: std::collections::HashSet<i64> =
        results[0].iter().copied().collect();
    let single_set: std::collections::HashSet<i64> = single_ids.iter().copied().collect();
    println!(
        "single-vs-batch first-query overlap: {} of {}",
        single_set.intersection(&batch_first).count(),
        single_ids.len()
    );
    Ok(())
}
