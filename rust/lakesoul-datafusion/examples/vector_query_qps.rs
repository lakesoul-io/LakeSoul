// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul Contributors

//! End-to-end QPS check for the code in `python/examples/vector-search/docs/向量查询使用说明.md`.
//!
//! Runs the guide's two query entries against a real LakeSoul table:
//! * single-query SQL — one `SessionContext::sql` statement per query,
//!   exactly the statement shape the guide says the pushdown accepts;
//! * snapshot batch — one `vector_batch_search` call for the whole query set.
//!
//! It also verifies the pushdown (`EXPLAIN` contains `LakeSoulVectorSearchExec`)
//! and computes Recall@k against a ground-truth file, so the measured QPS can be
//! trusted to come from the index path and not from a full scan.
//!
//! Build the table first (see the guide's `--scenario sql --build-only` command),
//! then:
//!
//! ```text
//! cargo run --release -p lakesoul-datafusion --example vector_query_qps -- \
//!   --table vec_glove_qps \
//!   --query-file /dev/shm/lakesoul-vector-query-opt-glove-fixture-20261105/glove-query.fvecs \
//!   --gt-file    /dev/shm/lakesoul-vector-query-opt-glove-fixture-20261105/glove-gt.ivecs \
//!   --iterations 200 --query-limit 10000 --top-k 100 --nprobe 16 \
//!   --rerank index --batch-rounds 5
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchConfig;
use lakesoul_datafusion::vector_batch::VectorBatchRequest;
use lakesoul_datafusion::{
    MetaDataClient, create_lakesoul_session_config,
    create_lakesoul_session_ctx_with_config,
};

fn boxed<E: std::fmt::Display>(error: E) -> Box<dyn std::error::Error> {
    error.to_string().into()
}

struct Args {
    table: String,
    query_file: String,
    gt_file: String,
    top_k: usize,
    nprobe: usize,
    parallelism: usize,
    rerank: String,
    single_rounds: usize,
    batch_rounds: usize,
    batch_size: usize,
    /// How many query vectors to load (single runs `single_rounds` of them,
    /// the batch runs all of them).
    query_limit: usize,
    /// Also measure the whole query set as plain SQL statements ("just use SQL
    /// for a batch") with this many statements in flight (`--sql-batch`).
    sql_batch: bool,
    sql_concurrency: usize,
    /// Print whether a multi-query single-statement SQL pushes down.
    explain_multi: bool,
    /// Pack this many queries into one `UNION ALL` statement (`0` = off).
    union_batch: usize,
}

fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let mut args = Args {
        table: "vec_glove_qps".to_string(),
        query_file: String::new(),
        gt_file: String::new(),
        top_k: 100,
        nprobe: 16,
        parallelism: 1,
        rerank: "index".to_string(),
        single_rounds: 200,
        batch_rounds: 5,
        batch_size: 0,
        query_limit: 10_000,
        sql_batch: false,
        sql_concurrency: 1,
        explain_multi: false,
        union_batch: 0,
    };
    let mut iterator = std::env::args().skip(1);
    while let Some(flag) = iterator.next() {
        let mut value = || {
            iterator
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))
        };
        match flag.as_str() {
            "--table" => args.table = value()?,
            "--query-file" => args.query_file = value()?,
            "--gt-file" => args.gt_file = value()?,
            "--top-k" => args.top_k = value()?.parse()?,
            "--nprobe" => args.nprobe = value()?.parse()?,
            "--parallelism" => args.parallelism = value()?.parse()?,
            "--rerank" => args.rerank = value()?,
            "--iterations" => args.single_rounds = value()?.parse()?,
            "--batch-rounds" => args.batch_rounds = value()?.parse()?,
            "--batch-size" => args.batch_size = value()?.parse()?,
            "--query-limit" => args.query_limit = value()?.parse()?,
            "--sql-batch" => args.sql_batch = true,
            "--sql-concurrency" => {
                args.sql_concurrency = value()?.parse()?;
                args.sql_batch = true;
            }
            "--explain-multi" => args.explain_multi = true,
            "--union-batch" => args.union_batch = value()?.parse()?,
            other => return Err(format!("unknown flag: {other}").into()),
        }
    }
    if args.query_file.is_empty() {
        return Err("--query-file is required".into());
    }
    Ok(args)
}

/// fvecs: per row `dim: i32` followed by `dim` f32 values.
fn read_fvecs(
    path: &str,
    limit: usize,
) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    let mut rows = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= bytes.len() && rows.len() < limit {
        let dim = i32::from_le_bytes(bytes[offset..offset + 4].try_into()?) as usize;
        offset += 4;
        let mut row = Vec::with_capacity(dim);
        for _ in 0..dim {
            row.push(f32::from_le_bytes(bytes[offset..offset + 4].try_into()?));
            offset += 4;
        }
        rows.push(row);
    }
    Ok(rows)
}

/// ivecs: per row `k: i32` followed by `k` i32 values.
fn read_ivecs(path: &str) -> Result<Vec<Vec<i64>>, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    let mut rows = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= bytes.len() {
        let k = i32::from_le_bytes(bytes[offset..offset + 4].try_into()?) as usize;
        offset += 4;
        let mut row = Vec::with_capacity(k);
        for _ in 0..k {
            row.push(i32::from_le_bytes(bytes[offset..offset + 4].try_into()?) as i64);
            offset += 4;
        }
        rows.push(row);
    }
    Ok(rows)
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                let rest = line.strip_prefix("VmRSS:")?;
                let kb: f64 = rest.split_whitespace().next()?.parse().ok()?;
                Some(kb / 1024.0)
            })
        })
        .unwrap_or(0.0)
}

fn percentile(values: &mut [f64], p: f64) -> f64 {
    values.sort_by(|left, right| left.total_cmp(right));
    if values.is_empty() {
        return 0.0;
    }
    values[((values.len() as f64 - 1.0) * p).round() as usize]
}

fn recall_at_k(returned: &[i64], ground_truth: &[i64], k: usize) -> f64 {
    if ground_truth.is_empty() {
        return 0.0;
    }
    let k = k.min(ground_truth.len());
    let truth: std::collections::HashSet<i64> =
        ground_truth.iter().take(k).copied().collect();
    let hits = returned
        .iter()
        .take(k)
        .filter(|id| truth.contains(id))
        .count();
    hits as f64 / k as f64
}

/// One single-query statement: the statement shape the guide documents.
async fn single_query(
    ctx: &datafusion::prelude::SessionContext,
    table: &str,
    query: &[f32],
    top_k: usize,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let literal = query
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table} \
         order by array_distance(vec, ARRAY[{literal}]) limit {top_k}"
    );
    let df = ctx.sql(&sql).await?;
    let mut ids = Vec::with_capacity(top_k);
    for batch in df.collect().await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected an Int64 id column")?;
        ids.extend(column.iter().flatten());
    }
    Ok(ids)
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // ---- session: exactly the guide's setup ----
    let client = Arc::new(MetaDataClient::from_env().await.map_err(boxed)?);
    let mut session_config = create_lakesoul_session_config().map_err(boxed)?;
    let mut vector = LakeSoulVectorSearchConfig::default();
    vector.nprobe = args.nprobe;
    vector.rerank = args.rerank.clone();
    vector.parallelism = args.parallelism;
    session_config.options_mut().extensions.insert(vector);
    let ctx = create_lakesoul_session_ctx_with_config(
        client,
        &CoreArgs::from_env(),
        session_config,
    )
    .map_err(boxed)?;

    let queries = read_fvecs(&args.query_file, args.query_limit.max(1))?;
    let single_queries = args.single_rounds.clamp(1, queries.len());
    let ground_truth = if args.gt_file.is_empty() {
        Vec::new()
    } else {
        read_ivecs(&args.gt_file)?
    };
    if queries.is_empty() {
        return Err("no query vectors read".into());
    }

    // ---- pushdown check: the documented SQL shape must hit the vector exec ----
    let explain_sql = format!(
        "EXPLAIN VERBOSE select id from \"lakesoul\".default.{} \
         order by array_distance(vec, ARRAY[{}]) limit {}",
        args.table,
        queries[0]
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", "),
        args.top_k
    );
    let mut explain_text = String::new();
    for batch in ctx.sql(&explain_sql).await?.collect().await? {
        for column in batch.columns() {
            if let Some(array) =
                column.as_any().downcast_ref::<arrow::array::StringArray>()
            {
                for value in array.iter().flatten() {
                    explain_text.push_str(value);
                    explain_text.push('\n');
                }
            }
        }
    }
    let vector_exec = explain_text.contains("LakeSoulVectorSearchExec");
    let order_by = explain_text.contains("order_by=true");
    println!(
        "pushdown: vector_exec={vector_exec} order_by_true={order_by} \
         (table={} rerank={} nprobe={} top_k={})",
        args.table, args.rerank, args.nprobe, args.top_k
    );

    // ---- 多 query 的单条 SQL（JOIN / UNION ALL）是否能下推？----
    if args.explain_multi {
        let q0 = queries[0]
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let q1 = queries[1.min(queries.len() - 1)]
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let multi_union = format!(
            "(select id from \"lakesoul\".default.{table} \
              order by array_distance(vec, ARRAY[{q0}]) limit {k}) \
             union all \
             (select id from \"lakesoul\".default.{table} \
              order by array_distance(vec, ARRAY[{q1}]) limit {k})",
            table = args.table,
            k = args.top_k
        );
        let multi_join = format!(
            "select id from \"lakesoul\".default.{table} t, \
             (values (ARRAY[{q0}]), (ARRAY[{q1}])) as q(qv) \
             order by array_distance(t.vec, q.qv) limit {k}",
            table = args.table,
            k = args.top_k
        );
        for (name, sql) in [
            ("union_all", multi_union),
            ("cross_join_values", multi_join),
        ] {
            let explain = format!("EXPLAIN VERBOSE {sql}");
            let mut text = String::new();
            for batch in ctx.sql(&explain).await?.collect().await? {
                for column in batch.columns() {
                    if let Some(array) =
                        column.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        for value in array.iter().flatten() {
                            text.push_str(value);
                            text.push('\n');
                        }
                    }
                }
            }
            println!(
                "multi_query_sql[{name}]: vector_exec={} (单条语句里带多个 query 向量时不会下推)",
                text.contains("LakeSoulVectorSearchExec")
            );
        }
    }

    // ---- single-query SQL ----
    let rss_before = rss_mb();
    for index in 0..3.min(single_queries) {
        let _ = single_query(&ctx, &args.table, &queries[index], args.top_k).await?;
    }
    let mut latencies = Vec::with_capacity(single_queries);
    let mut recall_sum = 0.0;
    let started = Instant::now();
    for (index, query) in queries.iter().take(single_queries).enumerate() {
        let t = Instant::now();
        let ids = single_query(&ctx, &args.table, query, args.top_k).await?;
        latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        if let Some(truth) = ground_truth.get(index) {
            recall_sum += recall_at_k(&ids, truth, args.top_k);
        }
    }
    let single_wall = started.elapsed().as_secs_f64();
    let single_qps = single_queries as f64 / single_wall;
    let p50 = percentile(&mut latencies.clone(), 0.50);
    let p99 = percentile(&mut latencies.clone(), 0.99);
    let mean = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let recall = if ground_truth.is_empty() {
        0.0
    } else {
        recall_sum / single_queries.min(ground_truth.len()) as f64
    };
    println!(
        "single_query: n={} qps={single_qps:.1} mean_ms={mean:.3} p50_ms={p50:.3} \
         p99_ms={p99:.3} recall@{k}={recall:.5} rss_mb={:.1}",
        single_queries,
        rss_before.max(rss_mb()),
        k = args.top_k
    );

    // ---- one statement with several queries: `UNION ALL` of per-query SELECTs ----
    if args.union_batch > 0 {
        let per_statement = args.union_batch;
        let started = Instant::now();
        let mut recall_sum = 0.0;
        let mut compared = 0usize;
        let mut statements = 0usize;
        for chunk in queries.chunks(per_statement) {
            let mut branches = Vec::with_capacity(chunk.len());
            for query in chunk {
                let literal = query
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                branches.push(format!(
                    "(select id from \"lakesoul\".default.{table} \
                      order by array_distance(vec, ARRAY[{literal}]) limit {k})",
                    table = args.table,
                    k = args.top_k
                ));
            }
            let sql = branches.join(" union all ");
            let mut ids = Vec::new();
            for batch in ctx.sql(&sql).await?.collect().await? {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("expected an Int64 id column")?;
                ids.extend(column.iter().flatten());
            }
            if statements == 0 {
                println!(
                    "union_batch debug: first statement returned {} rows for {} branches \
                     (top_k={})",
                    ids.len(),
                    chunk.len(),
                    args.top_k
                );
            }
            // 每个分支返回 top_k 行，按分支顺序拼接。
            for (offset, query_index) in chunk
                .iter()
                .enumerate()
                .map(|(offset, _)| (offset, statements * per_statement + offset))
            {
                let begin = offset * args.top_k;
                let end = (begin + args.top_k).min(ids.len());
                if begin >= end {
                    break;
                }
                if let Some(truth) = ground_truth.get(query_index) {
                    recall_sum += recall_at_k(&ids[begin..end], truth, args.top_k);
                    compared += 1;
                }
            }
            statements += 1;
        }
        let wall = started.elapsed().as_secs_f64();
        let n = statements * per_statement;
        println!(
            "union_batch (queries per statement={per_statement}): n={n} \
             statements={statements} qps={:.1} recall@{k}={:.5} rss_mb={:.1}",
            n as f64 / wall,
            if compared == 0 {
                0.0
            } else {
                recall_sum / compared as f64
            },
            rss_mb(),
            k = args.top_k
        );
    }

    // ---- "just use SQL" batch: the whole set as plain SQL statements ----
    if args.sql_batch {
        let concurrency = args.sql_concurrency.max(1);
        let queries = std::sync::Arc::new(queries.clone());
        let ground_truth = std::sync::Arc::new(ground_truth.clone());
        let next = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let latencies = std::sync::Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
        let recall_sum = std::sync::Arc::new(std::sync::Mutex::new(0.0f64));
        let started = Instant::now();
        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let ctx = ctx.clone();
            let queries = std::sync::Arc::clone(&queries);
            let ground_truth = std::sync::Arc::clone(&ground_truth);
            let next = std::sync::Arc::clone(&next);
            let latencies = std::sync::Arc::clone(&latencies);
            let recall_sum = std::sync::Arc::clone(&recall_sum);
            let table = args.table.clone();
            let top_k = args.top_k;
            workers.push(tokio::spawn(async move {
                loop {
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if index >= queries.len() {
                        break;
                    }
                    let t = Instant::now();
                    match single_query(&ctx, &table, &queries[index], top_k).await {
                        Ok(ids) => {
                            latencies
                                .lock()
                                .unwrap()
                                .push(t.elapsed().as_secs_f64() * 1000.0);
                            if let Some(truth) = ground_truth.get(index) {
                                *recall_sum.lock().unwrap() +=
                                    recall_at_k(&ids, truth, top_k);
                            }
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                }
                Ok::<(), String>(())
            }));
        }
        for worker in workers {
            worker
                .await
                .map_err(|error| format!("sql worker panicked: {error}"))??;
        }
        let wall = started.elapsed().as_secs_f64();
        let n = queries.len();
        let values = latencies.lock().unwrap().clone();
        let p50 = percentile(&mut values.clone(), 0.5);
        let recall = if ground_truth.is_empty() {
            0.0
        } else {
            *recall_sum.lock().unwrap() / n.min(ground_truth.len()) as f64
        };
        println!(
            "sql_batch (plain SQL statements): n={n} concurrency={concurrency} \
             qps={:.1} p50_ms={p50:.3} recall@{k}={recall:.5} rss_mb={:.1}",
            n as f64 / wall,
            rss_mb(),
            k = args.top_k
        );
    }

    // ---- snapshot batch ----
    let state = ctx.state();
    let catalog = ctx.catalog("lakesoul").ok_or("lakesoul catalog missing")?;
    let schema = catalog.schema("default").ok_or("default schema missing")?;
    let table = schema
        .table(&args.table)
        .await?
        .ok_or_else(|| format!("table {} not found", args.table))?;
    let provider = table
        .downcast_ref::<LakeSoulTableProvider>()
        .ok_or("not a LakeSoul table")?;
    let request = VectorBatchRequest {
        column: "vec".to_string(),
        queries: queries.clone(),
        top_k: args.top_k,
        nprobe: args.nprobe,
        parallelism: args.parallelism,
        batch_size: args.batch_size,
        exact: args.rerank.eq_ignore_ascii_case("exact"),
        exact_overfetch: std::env::var("LAKESOUL_VECTOR_EXACT_OVERFETCH")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1),
        metric: "L2".to_string(),
    };
    for _ in 0..2 {
        let _ = provider.vector_batch_search(&state, &request).await?;
    }
    let mut batch_qps = Vec::with_capacity(args.batch_rounds.max(1));
    let mut batch_recall = 0.0;
    for round in 0..args.batch_rounds.max(1) {
        let started = Instant::now();
        let results = provider.vector_batch_search(&state, &request).await?;
        let seconds = started.elapsed().as_secs_f64();
        batch_qps.push(queries.len() as f64 / seconds);
        if round == 0 {
            let mut recall_sum = 0.0;
            let mut compared = 0usize;
            for (index, ids) in results.iter().enumerate() {
                if let Some(truth) = ground_truth.get(index) {
                    recall_sum += recall_at_k(ids, truth, args.top_k);
                    compared += 1;
                }
            }
            batch_recall = if compared == 0 {
                0.0
            } else {
                recall_sum / compared as f64
            };
        }
    }
    let mut sorted = batch_qps.clone();
    let batch_p50 = percentile(&mut sorted, 0.5);
    let per_query_ms =
        queries.len() as f64 / batch_p50 * 1000.0 / queries.len() as f64 * 1000.0;
    println!(
        "batch: n_per_call={} calls={} qps_median={batch_p50:.1} qps_all={batch_qps:?} \
         per_query_ms={:.4} recall@{k}={batch_recall:.5} rss_mb={:.1} \
         (batch_size={})",
        queries.len(),
        args.batch_rounds.max(1),
        1000.0 / batch_p50,
        rss_mb(),
        args.batch_size,
        k = args.top_k,
    );
    let _ = per_query_ms;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(
            std::env::var("RAYON_NUM_THREADS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(16),
        )
        .build()?;
    runtime.block_on(run(args))?;
    Ok(())
}
