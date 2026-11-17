// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Prove the SLT coordinator uses both workers, independently of SQL answers.

use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use sqllogictest::DBOutput;
use tokio_postgres::Client;

use super::{PgDb, Protocol, support::Harness};

pub(super) const SCRIPT: &str = "distributed/remote_write.slt";

pub(super) async fn prove(
    h: &Harness,
    client: &Client,
    protocol: Protocol,
) -> Result<()> {
    let output = PgDb { client, protocol }
        .execute("EXPLAIN ANALYZE SELECT n, SUM(id) FROM slt_remote_source GROUP BY n")
        .await?;
    let DBOutput::Rows { rows, .. } = output else {
        anyhow::bail!("distributed EXPLAIN ANALYZE returned no plan");
    };
    let plan = rows.into_iter().flatten().collect::<Vec<_>>().join("\n");
    ensure!(
        plan.contains("DistributedExec")
            && plan.contains("NetworkShuffleExec")
            && plan.contains("tasks=2")
            && plan.contains("Plan with Metrics")
            && plan.contains("metrics=[output_rows="),
        "SLT must execute a two-task remote scan with runtime metrics:\n{plan}"
    );

    let source = h.reference.query_one(
        "SELECT count(DISTINCT d.commit_id), count(*) \
         FROM public.table_info t JOIN public.data_commit_info d USING (table_id) \
         CROSS JOIN LATERAL unnest(d.file_ops) AS operation \
         WHERE t.table_namespace = $1 AND t.table_name = 'slt_remote_source' AND d.committed",
        &[&h.lake_namespace],
    ).await?;
    let source_commits: i64 = source.get(0);
    let source_files: i64 = source.get(1);
    ensure!(
        source_commits == 4 && source_files >= 4,
        "SLT source requires four independent commits/files: commits={source_commits} files={source_files}"
    );

    // Read by the owned table's identity on the authoritative metadata service,
    // not through the coordinator's catalog. Include uncommitted rows too.
    let files = h
        .reference
        .query(
            "SELECT d.commit_id::text, d.committed, (operation).path \
         FROM public.table_info t JOIN public.data_commit_info d USING (table_id) \
         CROSS JOIN LATERAL unnest(d.file_ops) AS operation \
         WHERE t.table_namespace = $1 AND t.table_name = 'slt_remote_target'",
            &[&h.lake_namespace],
        )
        .await?;
    let mut commits = BTreeSet::new();
    let mut writes = BTreeSet::new();
    let mut tasks = BTreeSet::new();
    let mut committed_paths = BTreeSet::new();
    let warehouse = h.warehouse_dir.canonicalize()?;
    let table_dir = warehouse.join(&h.lake_namespace).join("slt_remote_target");
    for row in &files {
        commits.insert(row.get::<_, String>(0));
        ensure!(
            row.get::<_, bool>(1),
            "SLT writer left an uncommitted data record"
        );
        let path: String = row.get(2);
        let local = if path.starts_with("file:") {
            url::Url::parse(&path)?
                .to_file_path()
                .map_err(|_| anyhow::anyhow!("SLT committed a non-local path: {path}"))?
        } else {
            Path::new(&path).to_path_buf()
        };
        let local = local
            .canonicalize()
            .with_context(|| format!("missing committed SLT file {path}"))?;
        ensure!(
            local.is_file() && local.starts_with(&warehouse),
            "SLT committed file outside its warehouse: {path}"
        );
        committed_paths.insert(local.strip_prefix(&table_dir)?.to_path_buf());
        let stem = local
            .file_stem()
            .and_then(|s| s.to_str())
            .context("SLT file has no UTF-8 stem")?;
        let (write, bucket) = stem
            .strip_prefix("part-")
            .context("SLT file has no write id")?
            .rsplit_once('_')
            .context("SLT file has no bucket suffix")?;
        ensure!(
            !bucket.is_empty() && bucket.bytes().all(|b| b.is_ascii_digit()),
            "invalid SLT bucket: {stem}"
        );
        let (write, task) = write
            .rsplit_once('T')
            .context("SLT file was not stamped by a multi-task worker")?;
        writes.insert(write.to_owned());
        tasks.insert(task.parse::<usize>()?);
    }
    ensure!(
        commits.len() == 1 && writes.len() == 1 && tasks == BTreeSet::from([0, 1]),
        "SLT requires one publication with files from tasks 0/1: commits={commits:?} writes={writes:?} tasks={tasks:?}"
    );
    let actual_paths = h
        .table_file_bytes("slt_remote_target")
        .await?
        .into_keys()
        .map(PathBuf::from)
        .collect::<BTreeSet<_>>();
    ensure!(
        actual_paths == committed_paths,
        "SLT worker failures left extra files or removed committed files: actual={actual_paths:?} committed={committed_paths:?}"
    );
    let publication = h
        .reference
        .query_one(
            "SELECT count(*), count(*) FILTER (WHERE NOT d.committed) \
         FROM public.table_info t JOIN public.data_commit_info d USING (table_id) \
         WHERE t.table_namespace = $1 AND t.table_name = 'slt_remote_target'",
            &[&h.lake_namespace],
        )
        .await?;
    ensure!(
        publication.get::<_, i64>(0) == 1 && publication.get::<_, i64>(1) == 0,
        "SLT target requires one committed row and no uncommitted metadata, including empty file lists"
    );
    eprintln!(
        "PASS SLT distributed-proof protocol={protocol:?} source_commits={source_commits} source_files={source_files} write_tasks={tasks:?} committed_files={} publication_count={}",
        files.len(),
        commits.len()
    );
    Ok(())
}
