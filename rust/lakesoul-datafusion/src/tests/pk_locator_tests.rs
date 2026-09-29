// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Primary-key row locator through DataFusion SQL.
//!
//! `pk = value` / `pk IN (...)` filters (and finite prefixes of composite
//! keys) on a vortex table fetch only the matching rows through a per-file
//! arrow-Row index that is mmapped from the shared local disk cache instead
//! of scanning every data file.  These tests use plain (non-vector) tables
//! and check the results across multiple files, updates and misses.  The
//! first file of each table is large enough to clear the index row threshold,
//! while the later upsert files are small, so the mixed-file decision is
//! exercised as well.

use std::sync::{Arc, Once};

use arrow::array::Int64Array;
use datafusion::prelude::SessionContext;
use lakesoul_metadata::MetaDataClient;

use crate::cli::CoreArgs;

/// Rows in the first data file of each table.  Must clear the locator's
/// minimum index size so the mmapped index is actually built.
const LARGE_ROWS: i64 = 60_000;

/// The pk index shares the process-wide disk cache, which is switched on with
/// `LAKESOUL_CACHE`.  Set it once before the first lookup so these tests
/// exercise the mmapped index instead of a full scan.
fn enable_index_cache() {
    static ENABLE: Once = Once::new();
    ENABLE.call_once(|| {
        // SAFETY: the test process sets this before any pk index use.
        unsafe { std::env::set_var("LAKESOUL_CACHE", "1") };
    });
}

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
    enable_index_cache();
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

    // File 1: ids 0..LARGE_ROWS-1, value = id.
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name}
         SELECT CAST(g.value AS BIGINT) AS id, CAST(g.value AS BIGINT) AS value
         FROM generate_series(0, {}) AS g(value)",
        LARGE_ROWS - 1
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

    // File 3: add ids LARGE_ROWS..LARGE_ROWS+9.
    let additions = (0..10)
        .map(|value| format!("({}, {value})", LARGE_ROWS + value))
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
                "select id from \"lakesoul\".default.{table_name} \
                 where id in (3, {}, 999999)",
                LARGE_ROWS + 3
            )
        )
        .await,
        vec![3, LARGE_ROWS + 3]
    );

    // OR of equalities.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select id from \"lakesoul\".default.{table_name} \
                 where id = 7 or id = {}",
                LARGE_ROWS + 7
            )
        )
        .await,
        vec![7, LARGE_ROWS + 7]
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

    // Repeated point queries are served from the local index cache.
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
        "repeated point queries should hit the pk index cache"
    );
    assert!(misses_after > 0, "the pk index should have been built");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_string_pk_filters_use_the_row_locator() {
    enable_index_cache();
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

    // File 1: keys k000000..k059999.
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name}
         SELECT concat('k', lpad(CAST(g.value AS VARCHAR), 6, '0')) AS k,
                CAST(g.value AS BIGINT) AS v
         FROM generate_series(0, {}) AS g(value)",
        LARGE_ROWS - 1
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // File 2: update k000005.
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES ('k000005', 100005)"
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select v from \"lakesoul\".default.{table_name} where k = 'k000005'"
            )
        )
        .await,
        vec![100005]
    );
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select v from \"lakesoul\".default.{table_name} \
                 where k in ('k000001', 'k000005', 'missing')"
            )
        )
        .await,
        vec![1, 100005]
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
                 where k in ('k000005', 'k000006') and v > 100000"
            )
        )
        .await,
        vec![100005]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_composite_pk_prefix_uses_the_row_locator() {
    enable_index_cache();
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

    // File 1: 100 groups x 600 versions, payload = group * 600 + version.
    let groups = LARGE_ROWS / 600;
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name}
         SELECT concat('g', lpad(CAST(CAST(g.value / 600 AS BIGINT) AS VARCHAR), 3, '0')) AS g,
                CAST(g.value % 600 AS BIGINT) AS v,
                CAST(g.value AS BIGINT) AS payload
         FROM generate_series(0, {}) AS g(value)",
        LARGE_ROWS - 1
    ))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert!(groups > 0);

    // File 2: update (g001, v2).
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} VALUES ('g001', 2, 999)"
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
                 where g = 'g001' and v = 2"
            )
        )
        .await,
        vec![999]
    );

    // A prefix of the composite key alone is enough to locate rows.
    let mut group_one: Vec<i64> = (600..1200).filter(|payload| *payload != 602).collect();
    group_one.push(999);
    group_one.sort_unstable();
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} where g = 'g001'"
            )
        )
        .await,
        group_one
    );

    // Full keys in one IN list.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} \
                 where g in ('g000', 'g002') and v in (1, 3)"
            )
        )
        .await,
        vec![1, 3, 1201, 1203]
    );

    // Residual predicates on payload columns still apply above the scan.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} \
                 where g = 'g002' and payload > 1200"
            )
        )
        .await,
        (1201..1800).collect::<Vec<i64>>()
    );

    // A missing group reads nothing.
    assert_eq!(
        query_ids(
            &ctx,
            &format!(
                "select payload from \"lakesoul\".default.{table_name} where g = 'g999'"
            )
        )
        .await,
        Vec::<i64>::new()
    );
}
