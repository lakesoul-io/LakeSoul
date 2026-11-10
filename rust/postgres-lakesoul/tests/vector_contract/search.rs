// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::{Client, Statement, types::Type};

use super::common::{assert_ids, assert_index_path};
use crate::support::{
    Harness, compare,
    vector::{CORPUS_ROWS, DIM, NLIST, corpus_vector},
};

const ZERO: [f32; DIM] = [0.0; DIM];
const Y_AXIS: [f32; DIM] = [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

pub async fn run(h: &Harness) -> Result<()> {
    exact_fallback(h).await?;
    indexed_quality(h).await?;
    probes_execution_and_isolation(h).await
}

fn l2_sql(table: &str, k: usize) -> String {
    format!(
        "SELECT id FROM public.{table} ORDER BY vec <-> '[0,0,0,0,0,0,0,0]'::vector(8) LIMIT {k}"
    )
}

fn l2(a: &[f32; DIM], b: &[f32; DIM]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&a, &b)| (f64::from(a) - f64::from(b)).powi(2))
        .sum::<f64>()
        .sqrt()
}

fn cosine(a: &[f32; DIM], b: &[f32; DIM]) -> f64 {
    let dot = a
        .iter()
        .zip(b)
        .map(|(&a, &b)| f64::from(a) * f64::from(b))
        .sum::<f64>();
    let norm =
        |v: &[f32; DIM]| v.iter().map(|&v| f64::from(v).powi(2)).sum::<f64>().sqrt();
    1.0 - dot / (norm(a) * norm(b))
}

/// Independent scalar math over the exact float32 INPUT corpus. No query result,
/// index candidate list or DataFusion distance implementation supplies the oracle.
fn exact_top_k(
    query: &[f32; DIM],
    k: usize,
    distance: fn(&[f32; DIM], &[f32; DIM]) -> f64,
) -> Result<Vec<i64>> {
    let mut ranked: Vec<_> = (0..CORPUS_ROWS)
        .map(|id| (id as i64, distance(&corpus_vector(id), query)))
        .collect();
    ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
    ensure!(
        ranked.iter().take(k + 1).all(|(_, d)| d.is_finite()),
        "fixture oracle must have finite distances"
    );
    ensure!(
        ranked
            .windows(2)
            .take(k)
            .all(|pair| pair[1].1 - pair[0].1 > 1e-9),
        "fixture oracle must have no ambiguous ordering or top-k boundary ties: {ranked:?}"
    );
    Ok(ranked.into_iter().take(k).map(|(id, _)| id).collect())
}

async fn exact_fallback(h: &Harness) -> Result<()> {
    let sql = l2_sql("vector_plain", 5);
    assert_index_path(&h.lake, "vector-no-index-exact-route", &sql, false).await?;
    assert_ids(
        &h.lake,
        "vector-no-index-exact-top5",
        &sql,
        &exact_top_k(&ZERO, 5, l2)?,
    )
    .await?;

    // An L2 index is not a cosine index. Its presence must not turn a cosine
    // query into an incorrectly scored ANN scan.
    let sql = "SELECT id FROM public.vector_ann ORDER BY vec <=> '[0,1,0,0,0,0,0,0]'::vector(8) LIMIT 5";
    assert_index_path(
        &h.lake,
        "vector-incompatible-metric-exact-route",
        sql,
        false,
    )
    .await?;
    assert_ids(
        &h.lake,
        "vector-incompatible-metric-exact-top5",
        sql,
        &exact_top_k(&Y_AXIS, 5, cosine)?,
    )
    .await
}

async fn indexed_quality(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(&format!("SET ivfflat.probes = {NLIST}"))
        .await?;
    let sql = l2_sql("vector_ann", 5);
    assert_index_path(&h.lake, "vector-ann-physical-index-route", &sql, true).await?;
    let truth = exact_top_k(&ZERO, 5, l2)?;
    assert_ids(&h.lake, "vector-ann-controlled-recall-at5", &sql, &truth).await?;
    eprintln!(
        "PASS case=vector-ann-controlled-recall-at5 recall_at_5=1.0 corpus_rows={CORPUS_ROWS} dim={DIM} nlist={NLIST} nprobe={NLIST} oracle=independent-f64-on-input-f32 guarantee=controlled-fixture-only"
    );
    Ok(())
}

async fn prepared_ids(
    client: &Client,
    statement: &Statement,
    id: &str,
) -> Result<Vec<i64>> {
    ensure!(
        statement.columns().len() == 1
            && statement.columns()[0].name() == "id"
            && *statement.columns()[0].type_() == Type::INT8,
        "case={id}: expected single id/int8 result, actual={:?}",
        statement.columns()
    );
    client
        .query(statement, &[])
        .await
        .with_context(|| {
            format!("case={id} protocol=extended-binary same_prepared_statement=true")
        })?
        .iter()
        .map(|row| row.try_get::<_, i64>(0).map_err(Into::into))
        .collect()
}

async fn assert_prepared_ids(
    client: &Client,
    statement: &Statement,
    id: &str,
    expected: &[i64],
) -> Result<()> {
    let actual = prepared_ids(client, statement, id).await?;
    ensure!(
        actual == expected,
        "case={id}: expected ordered IDs={expected:?}, actual={actual:?}"
    );
    eprintln!(
        "PASS case={id} protocol=extended-binary same_prepared_statement=true ids={actual:?}"
    );
    Ok(())
}

async fn show_probes(client: &Client, id: &str, expected: usize) -> Result<()> {
    let expected = expected.to_string();
    let sql = "SHOW ivfflat.probes";
    let simple = compare::simple(client, sql).await?;
    compare::check_columns(&simple, &[("ivfflat.probes", Type::TEXT)])?;
    compare::compare_rows(&simple.rows, &vec![vec![Some(expected.clone())]], true)?;
    let statement = client.prepare(sql).await?;
    ensure!(
        statement.columns().len() == 1
            && statement.columns()[0].name() == "ivfflat.probes"
            && *statement.columns()[0].type_() == Type::TEXT,
        "case={id}: SHOW Describe mismatch, actual {:?}",
        statement.columns()
    );
    let rows = client.query(&statement, &[]).await?;
    ensure!(
        rows.len() == 1 && rows[0].try_get::<_, String>(0)? == expected,
        "case={id}: SHOW mismatch, expected={expected}, actual={rows:?}"
    );
    eprintln!(
        "PASS case={id} probes={expected} protocols=simple-text,extended-binary evidence=setting-value-and-describe describe_metadata={:?}",
        statement.columns()
    );
    Ok(())
}

async fn probes_execution_and_isolation(h: &Harness) -> Result<()> {
    // Both connections and statements exist before the mutation. A global GUC,
    // prepare-time capture or an ignored SET cannot satisfy these observations.
    let owner = h.connect_lake().await?;
    let peer = h.connect_lake().await?;
    for client in [&owner, &peer] {
        client
            .batch_execute(&format!("SET ivfflat.probes = {NLIST}"))
            .await?;
    }
    let sql = l2_sql("vector_ann", CORPUS_ROWS);
    assert_index_path(&owner, "vector-probes-actual-index-route", &sql, true).await?;
    let owner_statement = owner.prepare(&sql).await?;
    let peer_statement = peer.prepare(&sql).await?;
    let full = exact_top_k(&ZERO, CORPUS_ROWS, l2)?;
    assert_prepared_ids(
        &owner,
        &owner_statement,
        "vector-probes-full-baseline",
        &full,
    )
    .await?;
    assert_prepared_ids(
        &peer,
        &peer_statement,
        "vector-probes-peer-full-baseline",
        &full,
    )
    .await?;

    owner.batch_execute("SET ivfflat.probes = 1").await?;
    let limited =
        prepared_ids(&owner, &owner_statement, "vector-probes-one-list").await?;
    let actual_ids: BTreeSet<_> = limited.iter().copied().collect();
    let all_ids: BTreeSet<_> = full.iter().copied().collect();
    ensure!(
        !actual_ids.is_empty()
            && actual_ids.len() < all_ids.len()
            && actual_ids.is_subset(&all_ids)
            && actual_ids.len() == limited.len()
            && limited.windows(2).all(|pair| pair[0] < pair[1]),
        "one-list query must cover a strict subset of the real corpus, uniquely and exact-distance ordered; full={full:?}, one_list={limited:?}"
    );
    eprintln!(
        "PASS case=vector-probes-one-list actual_index_candidate_coverage={}/{CORPUS_ROWS} ordered_ids={limited:?} nprobe=1 request_k={CORPUS_ROWS}",
        limited.len()
    );
    assert_prepared_ids(
        &peer,
        &peer_statement,
        "vector-probes-owner-mutation-does-not-leak",
        &full,
    )
    .await?;
    show_probes(&owner, "vector-probes-owner-show-one", 1).await?;
    show_probes(&peer, "vector-probes-peer-show-four", NLIST).await?;

    owner
        .batch_execute(&format!("SET ivfflat.probes = {NLIST}"))
        .await?;
    assert_prepared_ids(
        &owner,
        &owner_statement,
        "vector-probes-restored-full-coverage",
        &full,
    )
    .await?;
    peer.batch_execute("SET ivfflat.probes = 1").await?;
    assert_prepared_ids(
        &peer,
        &peer_statement,
        "vector-probes-peer-own-mutation",
        &limited,
    )
    .await?;
    assert_prepared_ids(
        &owner,
        &owner_statement,
        "vector-probes-peer-mutation-does-not-leak",
        &full,
    )
    .await?;

    for sql in [
        "SET ivfflat.probes = 0",
        "SET ivfflat.probes = 32769",
        "SET ivfflat.probes = 'not_an_integer'",
    ] {
        for extended in [false, true] {
            let result = if extended {
                peer.execute(sql, &[]).await.map(drop)
            } else {
                peer.simple_query(sql).await.map(drop)
            };
            let error = match result {
                Ok(()) => bail!("invalid probes accepted: SQL={sql} extended={extended}"),
                Err(error) => error,
            };
            let db = error.as_db_error().with_context(|| format!("expected server rejection, SQL={sql} extended={extended} actual={error:?}"))?;
            ensure!(
                db.code().code() == "22023",
                "SQL={sql} extended={extended} expected SQLSTATE=22023, actual={} message={}",
                db.code().code(),
                db.message()
            );
            // FIRST read after the ErrorResponse must still execute the same
            // ANN statement with the last valid one-list budget.
            assert_prepared_ids(
                &peer,
                &peer_statement,
                "vector-probes-invalid-value-preserves-execution",
                &limited,
            )
            .await?;
            eprintln!(
                "PASS case=vector-probes-invalid-value SQL={sql} extended={extended} SQLSTATE=22023 first_read=same-ann-statement last_valid_nprobe=1"
            );
        }
    }
    assert_prepared_ids(
        &owner,
        &owner_statement,
        "vector-probes-invalid-peer-values-remain-isolated",
        &full,
    )
    .await
}
