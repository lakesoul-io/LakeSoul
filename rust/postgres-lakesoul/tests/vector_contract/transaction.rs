// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Read-your-writes and index maintenance for writes published by a
//! transaction block.
//!
//! A statement inside a block stages its files and skips the write path's
//! inline build; `COMMIT` builds the index once over everything it published,
//! and `ROLLBACK` builds nothing. The fixture's tables already carry committed
//! indexes, so the assertions are on their index directories, on index-backed
//! queries, and on `text_match`/`text_score` results.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::support::Harness;
use crate::support::vector::{ANN_TABLE, TEXT_TABLE, corpus_vector};

pub async fn run(h: &Harness) -> Result<()> {
    run_index_after_commit(h).await?;
    run_two_statement_text_index(h).await?;
    run_indexed_read_sees_staged(h).await?;
    run_index_only_mode_keeps_ordering(h).await?;
    run_text_score_in_block(h).await
}

/// A staged write has no index; `COMMIT` builds it over the published files and
/// `ROLLBACK` builds nothing.
async fn run_index_after_commit(h: &Harness) -> Result<()> {
    let table = ANN_TABLE;
    let index_dir = h
        .warehouse_dir
        .join(&h.lake_namespace)
        .join(table)
        .join(lakesoul_common::IndexKind::Vector.directory());
    let lake = h.connect_lake().await?;

    let before = index_files(&index_dir)?;
    assert!(
        !before.is_empty(),
        "{table}: the fixture's inline insert must have committed an index"
    );

    // One statement: a single partition, and the block may write it once (the
    // read-your-writes overlay makes several statements safe too, see below).
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(&insert_sql(table, 900), &[])
        .await
        .context("stage a row into an indexed table")?;
    assert_eq!(
        index_files(&index_dir)?,
        before,
        "{table}: a staged write builds no index before COMMIT"
    );
    lake.batch_execute("COMMIT").await.context("COMMIT")?;
    let after_commit = index_files(&index_dir)?;
    assert_ne!(
        after_commit, before,
        "{table}: COMMIT must build the index of the files it published"
    );

    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(&insert_sql(table, 901), &[])
        .await
        .context("stage one more row")?;
    lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;
    assert_eq!(
        index_files(&index_dir)?,
        after_commit,
        "{table}: ROLLBACK leaves the index exactly as the last COMMIT built it"
    );
    eprintln!(
        "sql-compat case=index-after-commit table={table} \
         shards_before={} shards_after={} outcome=built-on-commit-only",
        before.len(),
        after_commit.len()
    );
    Ok(())
}

/// Two statements of one block may write one partition; `COMMIT` builds the
/// text index over every file they published, not just the last statement's.
async fn run_two_statement_text_index(h: &Harness) -> Result<()> {
    let table = TEXT_TABLE;
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(
        &format!("INSERT INTO public.{table} (id, body) VALUES (5, 'kiwi jam')"),
        &[],
    )
    .await
    .context("the first statement stages")?;
    lake.execute(
        &format!("INSERT INTO public.{table} (id, body) VALUES (6, 'kiwi cake')"),
        &[],
    )
    .await
    .context("the second statement stages into the same partition")?;
    lake.batch_execute("COMMIT").await.context("COMMIT")?;
    let rows = lake
        .query(
            &format!(
                "SELECT id FROM public.{table} WHERE text_match(body, 'kiwi') \
                 ORDER BY id LIMIT 10"
            ),
            &[],
        )
        .await
        .context("text search after COMMIT")?;
    let ids = rows
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![5, 6], "the index covers both statements' files");
    eprintln!(
        "sql-compat case=two-statement-text-index table={table} ids={ids:?} \
         outcome=both-indexed"
    );
    Ok(())
}

/// A kNN query inside a block must see the block's staged row: the index
/// shards describe published files only, so the plan falls back to the exact
/// scan, which includes them.
async fn run_indexed_read_sees_staged(h: &Harness) -> Result<()> {
    let table = ANN_TABLE;
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(&insert_sql(table, 902), &[])
        .await
        .context("stage a row into the indexed table")?;
    let nearest = knn_nearest(&lake, table, 902).await?;
    assert_eq!(
        nearest, 902,
        "the search sees the row the block just staged"
    );
    lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;
    let after: i64 = h
        .lake
        .query_one(
            &format!("SELECT count(*) FROM public.{table} WHERE id = 902"),
            &[],
        )
        .await?
        .get(0);
    assert_eq!(after, 0, "the rolled-back row is not published");
    eprintln!(
        "sql-compat case=indexed-read-sees-staged table={table} nearest={nearest} \
         outcome=exact-scan"
    );
    Ok(())
}

/// Index-only mode drops the distance sort in favour of the index's ordering;
/// in a block the rewrite must stay off, so the exact scan still returns the
/// staged row as the nearest one.
async fn run_index_only_mode_keeps_ordering(h: &Harness) -> Result<()> {
    let table = ANN_TABLE;
    let lake = h.connect_lake().await?;
    lake.batch_execute("SET lakesoul_vector_search.rerank = 'index'")
        .await
        .context("index-only rerank mode")?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(&insert_sql(table, 903), &[])
        .await
        .context("stage a row into the indexed table")?;
    let nearest = knn_nearest(&lake, table, 903).await?;
    assert_eq!(
        nearest, 903,
        "index-only mode still orders by distance over the staged rows"
    );
    lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;
    lake.batch_execute("SET lakesoul_vector_search.rerank = 'exact'")
        .await
        .context("restore exact rerank mode")?;
    eprintln!(
        "sql-compat case=index-only-keeps-ordering table={table} nearest={nearest} \
         outcome=exact-scan-ordered"
    );
    Ok(())
}

/// `text_match` and the projected `text_score` inside a block are served by
/// the exact scan (the index rewrite is off): both staged rows come back.
async fn run_text_score_in_block(h: &Harness) -> Result<()> {
    let table = TEXT_TABLE;
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(
        &format!("INSERT INTO public.{table} (id, body) VALUES (7, 'kiwi jam')"),
        &[],
    )
    .await
    .context("the first statement stages")?;
    lake.execute(
        &format!("INSERT INTO public.{table} (id, body) VALUES (8, 'kiwi cake')"),
        &[],
    )
    .await
    .context("the second statement stages")?;
    // The filter alone is served by the exact scan and sees both rows.
    let rows = lake
        .query(
            &format!(
                "SELECT id FROM public.{table} WHERE text_match(body, 'kiwi') \
                 ORDER BY id LIMIT 10"
            ),
            &[],
        )
        .await
        .context("text_match inside the block")?;
    let mut ids = rows
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![5, 6, 7, 8],
        "the earlier committed rows and both staged rows match"
    );
    // The score is only produced by the index node, which the block's own
    // writes keep off the plan: the projection is refused rather than answered
    // from a stale index.
    let error = lake
        .query(
            &format!(
                "SELECT id FROM public.{table} WHERE text_match(body, 'kiwi') \
                 ORDER BY text_score(body, 'kiwi') DESC LIMIT 10"
            ),
            &[],
        )
        .await
        .err()
        .context("text_score is not served without the index node")?;
    let detail = format!("{error:?}");
    assert!(
        detail.contains("text_score"),
        "the refusal names the function: {detail}"
    );
    lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;
    let published: i64 = h
        .lake
        .query_one(
            &format!("SELECT count(*) FROM public.{table} WHERE id IN (7, 8)"),
            &[],
        )
        .await?
        .get(0);
    assert_eq!(published, 0, "the rolled-back rows are not published");
    eprintln!(
        "sql-compat case=text-score-in-block table={table} ids={ids:?} \
         outcome=exact-scan"
    );
    Ok(())
}

/// The nearest row to `id`'s own corpus vector, queried inside a block.
async fn knn_nearest(
    lake: &tokio_postgres::Client,
    table: &str,
    id: usize,
) -> Result<i64> {
    let query = corpus_vector(id)
        .iter()
        .map(|component| component.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let rows = lake
        .query(
            &format!(
                "SELECT id FROM public.{table} \
                 ORDER BY vec <-> '[{query}]'::vector(8) LIMIT 1"
            ),
            &[],
        )
        .await
        .with_context(|| format!("kNN search for corpus row {id}"))?;
    rows.first()
        .map(|row| row.get::<_, i64>(0))
        .context("the kNN search returned no row")
}

/// One corpus row with the fixture's own vector, so the index builder sees the
/// same shape it was declared for.
fn insert_sql(table: &str, id: usize) -> String {
    let components = corpus_vector(id)
        .iter()
        .map(|component| component.to_string())
        .collect::<Vec<_>>();
    format!(
        "INSERT INTO public.{table} (id, vec) VALUES ({id}, CAST(ARRAY[{}] AS FLOAT[]))",
        components.join(", ")
    )
}

/// Every file under `directory`, sorted, so two listings compare.
fn index_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(next) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}
