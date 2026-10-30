// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use datafusion::arrow::util::display::array_value_to_string;
use tokio_postgres::{Client, Config};

use super::{
    Harness, OPERATION_TIMEOUT, bounded, configure_session, connect_pg, lake_config,
    management_execute,
};

impl Harness {
    /// Driver tasks remain owned by teardown, including on a suite error or panic.
    pub async fn connect_lake(&self) -> Result<Client> {
        self.connect_extra(&lake_config(self.lake_addr, &self.lake_namespace), false)
            .await
    }

    pub async fn connect_reference(&self) -> Result<Client> {
        self.connect_extra(&self.state.reference_config, true).await
    }

    async fn connect_extra(&self, config: &Config, reference: bool) -> Result<Client> {
        let (client, task) = connect_pg(config, "sql_compat_additional").await?;
        self.state.connections.lock().push(task);
        configure_session(&client).await?;
        if reference {
            client
                .batch_execute(&format!(
                    "SET search_path TO {}, pg_catalog",
                    self.reference_schema
                ))
                .await?;
        }
        Ok(client)
    }

    /// No query passes through the read-only service. Include committed and
    /// uncommitted metadata, management-visible values, and exact warehouse
    /// bytes, so a rejected write cannot hide orphan files behind a failed commit.
    pub async fn authoritative_snapshot(&self) -> Result<String> {
        bounded(OPERATION_TIMEOUT, "authoritative no-side-effect snapshot", async {
            let client = self.connect_reference().await?;
            let mut metadata = BTreeMap::new();
            for (table, predicate) in [
                ("namespace", "namespace = $1"),
                ("table_info", "table_namespace = $1"),
                ("table_name_id", "table_namespace = $1"),
                ("table_path_id", "table_namespace = $1"),
                ("partition_info", "table_id IN (SELECT table_id FROM public.table_info WHERE table_namespace = $1)"),
                ("data_commit_info", "table_id IN (SELECT table_id FROM public.table_info WHERE table_namespace = $1)"),
            ] {
                let sql = format!("SELECT row_to_json(t)::text FROM public.{table} t WHERE {predicate} ORDER BY row_to_json(t)::text COLLATE \"C\"");
                let rows = client.query(&sql, &[&self.lake_namespace]).await
                    .map_err(|error| anyhow::anyhow!("authoritative metadata table={table} SQL={sql}: {error:?}"))?;
                metadata.insert(table, rows.iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>());
            }
            let mut data = BTreeMap::new();
            for table in ["items", "empty_items", "pk_items"] {
                let sql = format!("SELECT id, val, label FROM lakesoul.{}.{table} ORDER BY id NULLS LAST, val NULLS LAST, label NULLS LAST", self.lake_namespace);
                let batches = self.state.management.sql(&sql).await?.collect().await?;
                let mut rows = Vec::new();
                for batch in &batches {
                    for row in 0..batch.num_rows() {
                        let values = batch.columns().iter().map(|column| {
                            if column.is_null(row) {
                                Ok(None)
                            } else {
                                array_value_to_string(column, row).map(Some)
                            }
                        }).collect::<std::result::Result<Vec<_>, _>>()?;
                        rows.push(values);
                    }
                }
                data.insert(table, rows);
            }
            let mut files = BTreeMap::new();
            snapshot_files(&self.warehouse_dir, &self.warehouse_dir, &mut files)?;
            Ok::<_, anyhow::Error>(serde_json::to_string(&(metadata, data, files))?)
        }).await
    }

    /// A single management commit followed by a primary PostgreSQL metadata
    /// barrier. Never poll a SELECT on the measured LakeSoul connection.
    pub async fn commit_pk_rows(&self, values: &str) -> Result<()> {
        let authority = self.connect_reference().await?;
        let before = self.pk_publication(&authority).await?;
        management_execute(
            &self.state.management,
            &format!(
                "INSERT OR REPLACE INTO lakesoul.{}.pk_items VALUES {values}",
                self.lake_namespace
            ),
        )
        .await?;
        let after = self.pk_publication(&authority).await?;
        ensure!(
            after.0 == before.0 + 1,
            "commit barrier: expected next partition version after {before:?}, got {after:?}"
        );
        ensure!(
            after.1 == before.1 + 1
                && after.2 == i64::from(after.1)
                && after.3 == before.3 + 1,
            "commit barrier: latest snapshot must include every published commit and exactly one new committed data record: before {before:?}, after {after:?}"
        );
        eprintln!(
            "sql-compat management commit barrier: namespace={} version={} snapshot_commits={} committed_records={}",
            self.lake_namespace, after.0, after.1, after.3
        );
        Ok(())
    }

    async fn pk_publication(&self, authority: &Client) -> Result<(i32, i32, i64, i64)> {
        let sql = "WITH target AS (SELECT table_id FROM public.table_info WHERE table_namespace = $1 AND table_name = 'pk_items'), latest AS (SELECT version, snapshot FROM public.partition_info WHERE table_id = (SELECT table_id FROM target) ORDER BY version DESC LIMIT 1) SELECT version, cardinality(snapshot), (SELECT count(*) FROM public.data_commit_info c WHERE c.table_id = (SELECT table_id FROM target) AND c.committed AND c.commit_id = ANY(latest.snapshot)), (SELECT count(*) FROM public.data_commit_info c WHERE c.table_id = (SELECT table_id FROM target) AND c.committed) FROM latest";
        let row = bounded(
            OPERATION_TIMEOUT,
            "primary metadata publication barrier",
            authority.query_one(sql, &[&self.lake_namespace]),
        )
        .await?;
        Ok((
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            row.try_get(3)?,
        ))
    }
}

fn snapshot_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<PathBuf, Option<Vec<u8>>>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("snapshot warehouse {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_dir() {
            files.insert(path.strip_prefix(root)?.to_path_buf(), None);
            snapshot_files(root, &path, files)?;
        } else {
            ensure!(
                kind.is_file(),
                "unexpected non-regular warehouse entry {}",
                path.display()
            );
            files.insert(
                path.strip_prefix(root)?.to_path_buf(),
                Some(std::fs::read(&path)?),
            );
        }
    }
    Ok(())
}
