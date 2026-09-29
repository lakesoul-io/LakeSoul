// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Generic primary-key row locator through DataFusion SQL.
//!
//! `pk = value` / `pk IN (...)` filters on a vortex table fetch only the
//! matching rows through the per-file pk -> row map instead of scanning
//! every data file.  These tests use a plain (non-vector) table and check
//! the results across multiple files, updates and misses.

use std::sync::Arc;

use arrow::array::Int64Array;
use datafusion::prelude::SessionContext;
use lakesoul_metadata::MetaDataClient;

use crate::cli::CoreArgs;

fn clean_table_dir(table_name: &str) {
    let path = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let _ = std::fs::remove_dir_all(path);
}

fn default_args() -> CoreArgs {
    CoreArgs {
        warehouse_prefix: None,
        endpoint: None,
        s3_bucket: None,
        s3_access_key: None,
        s3_secret_key: None,
        s3_virtual_host_style: false,
        worker_threads: 2,
    }
}

async fn query_ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.extend(array.values().iter().copied());
    }
    ids.sort_unstable();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_pk_filters_use_the_row_locator() {
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "pk_locator_e2e";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();

    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            value BIGINT NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('file_format' 'vortex')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    // File 1: ids 0..999, value = id.
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name}
         SELECT CAST(g.value AS BIGINT) AS id, CAST(g.value AS BIGINT) AS value
         FROM generate_series(0, 999) AS g(value)"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // File 2: update ids 0..9 (value = 1000 + id).
    let updates = (0..10)
        .map(|id| format!("({id}, {})", 1000 + id))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES {updates}"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // File 3: add ids 1000..1009.
    let additions = (0..10)
        .map(|value| format!("({}, {value})", 1000 + value))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES {additions}"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Point query returns the latest version of the row (merge-on-read).
    assert_eq!(
        query_ids(
            &ctx,
            &format!("select value from \"lakesoul\".default.{table_name} where id = 5")
        )
        .await,
        vec![1005]
    );

    // IN list spanning several files and a missing key.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select id from \"lakesoul\".default.{table_name} where id in (3, 1003, 2000)"
            )
        )
        .await,
        vec![3, 1003]
    );

    // OR of equalities.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select id from \"lakesoul\".default.{table_name} where id = 7 or id = 1007"
            )
        )
        .await,
        vec![7, 1007]
    );

    // Residual (non-primary-key) predicates still apply.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select id from \"lakesoul\".default.{table_name} \
                 where id in (1, 2, 3) and value > 1001"
            )
        )
        .await,
        vec![2, 3]
    );

    // Repeated point queries are served from the pk map cache.
    let (hits_before, _) = lakesoul_io::pk_locator::cache_stats();
    let _ = query_ids(
        &ctx,
        &format!("select value from \"lakesoul\".default.{table_name} where id = 5"),
    )
    .await;
    let _ = query_ids(
        &ctx,
        &format!("select value from \"lakesoul\".default.{table_name} where id = 5"),
    )
    .await;
    let (hits_after, misses_after) = lakesoul_io::pk_locator::cache_stats();
    assert!(
        hits_after > hits_before,
        "repeated point queries should hit the pk map cache"
    );
    assert!(misses_after > 0, "the pk map should have been built");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_string_pk_filters_use_the_row_locator() {
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "pk_locator_string_e2e";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();

    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            k VARCHAR NOT NULL PRIMARY KEY,
            v BIGINT NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('file_format' 'vortex')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    // File 1: keys k000..k099.
    let rows = (0..100)
        .map(|value| format!("('k{value:03}', {value})"))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES {rows}"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // File 2: update k005.
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES ('k005', 1005)"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    assert_eq!(
        query_ids(
            &ctx,
            &format!("select v from \"lakesoul\".default.{table_name} where k = 'k005'")
        )
        .await,
        vec![1005]
    );
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select v from \"lakesoul\".default.{table_name} \
                 where k in ('k001', 'k005', 'missing')"
            )
        )
        .await,
        vec![1, 1005]
    );
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select v from \"lakesoul\".default.{table_name} where k = 'missing'"
            )
        )
        .await,
        Vec::<i64>::new()
    );
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select v from \"lakesoul\".default.{table_name} \
                 where k in ('k005', 'k006') and v > 1000"
            )
        )
        .await,
        vec![1005]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_composite_pk_prefix_uses_the_row_locator() {
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "pk_locator_composite_e2e";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();

    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            g VARCHAR NOT NULL,
            v BIGINT NOT NULL,
            payload BIGINT NOT NULL,
            PRIMARY KEY (g, v)
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('file_format' 'vortex')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    // File 1: groups g0..g2, five versions each.
    let rows = (0..3i64)
        .flat_map(|group| {
            (0..5i64).map(move |version| {
                format!("('g{group}', {version}, {})", group * 100 + version)
            })
        })
        .collect::<Vec<_>>()
        .join(", ");
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES {rows}"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // File 2: update (g1, v2).
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES ('g1', 2, 999)"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Full composite key.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} \
                 where g = 'g1' and v = 2"
            )
        )
        .await,
        vec![999]
    );

    // A prefix of the composite key alone is enough to locate rows.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} where g = 'g1'"
            )
        )
        .await,
        vec![100, 101, 103, 104, 999]
    );

    // Full keys in one IN list.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} \
                 where g in ('g0', 'g2') and v in (1, 3)"
            )
        )
        .await,
        vec![1, 3, 201, 203]
    );

    // Residual predicates on payload columns still apply above the scan.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} \
                 where g = 'g2' and payload > 200"
            )
        )
        .await,
        vec![201, 202, 203, 204]
    );

    // A missing group reads nothing.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} where g = 'g9'"
            )
        )
        .await,
        Vec::<i64>::new()
    );
}
