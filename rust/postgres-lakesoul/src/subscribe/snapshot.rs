// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Managed initial snapshot and atomic handoff to streaming.
//!
//! The target table stays empty until the whole table has been copied to
//! files; then one `commit_data_commit_infos` call publishes every file with
//! the slot's consistent point as the commit id. A failed snapshot therefore
//! leaves an empty target, not a half-loaded table.
//!
//! The copy is split into `snapshot_parallelism` independent CTID ranges.
//! Every reader imports the *same* exported snapshot and writes its own files;
//! the final commit is still a single metadata operation.

use std::sync::Arc;

use datafusion::common::ScalarValue;
use futures::StreamExt;
use futures::future::try_join_all;
use pg_walstream::{
    ColumnValue, LogicalReplicationStream, RowData, Snapshot, SnapshotOutcome,
};
use rootcause::{bail, report};
use tokio_postgres::{Client, NoTls};

use super::copy_text::{CopyTextDecoder, decode_line};
use super::plan::{Op, RowState, TablePlan};
use super::sink::{build_batch, commit_outputs, write_files};
use super::state::SubscriptionMetadata;
use super::{SubscribeConfig, ordinary_url, runner};
use crate::Result;

pub async fn run_managed_snapshot(
    config: &SubscribeConfig,
    plan: &TablePlan,
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
) -> Result<(LogicalReplicationStream, u64)> {
    let source_url = ordinary_url(&config.source_url);
    drop_slot_if_exists(&source_url, &config.slot).await?;

    let stream = runner::create_stream(config).await?;
    let outcome = stream
        .snapshot()
        .await
        .map_err(|error| report!("create initial snapshot: {error}"))?;
    let mut snapshot = match outcome {
        SnapshotOutcome::Available(snapshot) => snapshot,
        SnapshotOutcome::Unavailable(stream) => {
            // The slot survived a previous attempt but has no exported snapshot.
            // The target is empty (the caller checked), so recreate the slot.
            drop(stream);
            drop_slot_if_exists(&source_url, &config.slot).await?;
            let stream = runner::create_stream(config).await?;
            match stream
                .snapshot()
                .await
                .map_err(|error| report!("create initial snapshot: {error}"))?
            {
                SnapshotOutcome::Available(snapshot) => snapshot,
                SnapshotOutcome::Unavailable(_) => {
                    bail!(
                        "replication slot {} still exists after it was dropped; \
                         another subscriber may be running",
                        config.slot
                    )
                }
            }
        }
    };

    let target = plan.qualified_source_name();
    snapshot.retain_tables(|table| table.qualified_name() == target);
    validate_snapshot_table(&snapshot, plan)?;
    let snapshot_name = snapshot
        .stream()
        .exported_snapshot_name()
        .ok_or_else(|| report!("initial snapshot has no exported snapshot name"))?
        .to_string();
    let consistent_point = snapshot.consistent_point().value();
    metadata
        .set_snapshot_started(
            subscription_id,
            &config.source_schema,
            &config.source_table,
            consistent_point,
        )
        .await?;

    tracing::info!(
        slot = %config.slot,
        lsn = consistent_point,
        table = %target,
        parallelism = config.snapshot_parallelism,
        "starting initial snapshot"
    );

    // Open every reader first and take an ACCESS SHARE lock on the table, so
    // VACUUM FULL/CLUSTER cannot rewrite CTIDs between range planning and the
    // COPYs. The last range is deliberately unbounded: any page allocated after
    // this query is not visible in the exported snapshot, so including it is
    // harmless and avoids a gap if the relation grew concurrently.
    let table = format!(
        "{}.{}",
        quote_ident(&plan.source.schema),
        quote_ident(&plan.source.name)
    );
    let all_clients = open_readers(
        &source_url,
        &snapshot_name,
        config.snapshot_parallelism.max(1),
        &table,
    )
    .await?;
    let pages = relation_pages(&all_clients[0], plan.source.relation_oid).await?;
    let reader_count = reader_count(pages, config.snapshot_parallelism);
    let clients = all_clients
        .into_iter()
        .take(reader_count)
        .collect::<Vec<_>>();
    let ranges = ctid_ranges(pages, reader_count);
    if clients.len() != ranges.len() {
        bail!(
            "snapshot planner produced {} ranges for {} readers",
            ranges.len(),
            clients.len()
        );
    }

    let tasks = clients.into_iter().zip(ranges).enumerate().map(
        |(reader_index, (client, range))| {
            let sql = copy_sql(plan, range);
            async move { copy_range(client, sql, reader_index, config, plan).await }
        },
    );
    let results = try_join_all(tasks)
        .await
        .map_err(|error| report!("read initial snapshot: {error}"))?;

    let mut outputs = Vec::new();
    let mut total_rows = 0usize;
    for (reader_outputs, rows) in results {
        outputs.extend(reader_outputs);
        total_rows += rows;
    }

    let file_count = outputs.len();
    if outputs.is_empty() {
        tracing::info!(
            lsn = consistent_point,
            "snapshot has no rows; skipping the data commit"
        );
    } else {
        commit_outputs(
            &plan.table,
            &plan.target_table,
            outputs,
            consistent_point,
            total_rows,
        )
        .await?;
    }

    // End the exporter's transaction only after every file is durable and the
    // single commit has succeeded. On any earlier failure `Snapshot`'s Drop
    // discards the slot, so the retry gets a fresh export.
    let stream = snapshot
        .abandon()
        .await
        .map_err(|error| report!("finish initial snapshot: {error}"))?;

    metadata
        .set_snapshot_done(
            subscription_id,
            &config.source_schema,
            &config.source_table,
            consistent_point,
        )
        .await?;
    metadata
        .update_table_data_lsn(
            subscription_id,
            &config.source_schema,
            &config.source_table,
            consistent_point,
        )
        .await?;
    metadata
        .update_progress(subscription_id, consistent_point)
        .await?;

    tracing::info!(
        slot = %config.slot,
        lsn = consistent_point,
        table = %target,
        readers = reader_count,
        rows = total_rows,
        files = file_count,
        "initial snapshot committed; switching to streaming"
    );
    Ok((stream, consistent_point))
}

fn validate_snapshot_table(snapshot: &Snapshot, plan: &TablePlan) -> Result<()> {
    let table = snapshot.tables().first().ok_or_else(|| {
        report!(
            "publication does not contain source table {}",
            plan.qualified_source_name()
        )
    })?;
    let relation = table.relation();
    if relation.relation_id != plan.source.relation_oid {
        bail!(
            "source table {} changed relation oid during snapshot setup ({} -> {})",
            plan.qualified_source_name(),
            plan.source.relation_oid,
            relation.relation_id
        );
    }
    if relation.columns.len() != plan.source.columns.len() {
        bail!(
            "source table {} changed schema during snapshot setup: {} columns -> {}",
            plan.qualified_source_name(),
            plan.source.columns.len(),
            relation.columns.len()
        );
    }
    for (actual, expected) in relation.columns.iter().zip(&plan.source.columns) {
        if actual.name.as_ref() != expected.name
            || actual.type_id != expected.type_id
            || actual.type_modifier != expected.type_modifier
        {
            bail!(
                "source table {} changed column {:?} during snapshot setup",
                plan.qualified_source_name(),
                expected.name
            );
        }
    }
    Ok(())
}

/// Number of pages in the relation's main fork, read inside the snapshot
/// transaction after the reader has taken its ACCESS SHARE lock.
async fn relation_pages(client: &Client, oid: u32) -> Result<u64> {
    let row = client
        .query_one(&format!("select pg_relation_size({oid}::oid)"), &[])
        .await
        .map_err(|error| report!("read source relation size: {error}"))?;
    let size: i64 = row.get(0);
    let size = u64::try_from(size)
        .map_err(|_| report!("source relation size {size} is negative"))?;
    Ok(size.div_ceil(8192))
}

fn reader_count(pages: u64, requested: usize) -> usize {
    if pages == 0 {
        0
    } else {
        usize::try_from(pages)
            .unwrap_or(usize::MAX)
            .min(requested.max(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CtidRange {
    /// First heap page in the range.
    start: u64,
    /// Exclusive upper page bound; `None` means "to the end of the relation".
    end: Option<u64>,
}

fn ctid_ranges(pages: u64, reader_count: usize) -> Vec<CtidRange> {
    if pages == 0 || reader_count == 0 {
        return Vec::new();
    }
    let count = usize::try_from(pages)
        .unwrap_or(usize::MAX)
        .min(reader_count);
    let base = pages / count as u64;
    let extra = pages % count as u64;
    let mut ranges = Vec::with_capacity(count);
    for index in 0..count {
        let index = index as u64;
        let start = index * base + index.min(extra);
        let length = base + u64::from(index < extra);
        let end = if index + 1 == count as u64 {
            None
        } else {
            Some(start + length)
        };
        ranges.push(CtidRange { start, end });
    }
    ranges
}

async fn open_readers(
    url: &str,
    snapshot_name: &str,
    count: usize,
    table: &str,
) -> Result<Vec<Client>> {
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|error| report!("connect snapshot reader: {error}"))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "set statement_timeout = 0; \
                 set idle_in_transaction_session_timeout = 0; \
                 set DateStyle = 'ISO, YMD'; \
                 set TimeZone = 'UTC'; \
                 set bytea_output = 'hex'; \
                 begin transaction isolation level repeatable read read only;",
            )
            .await
            .map_err(|error| report!("begin snapshot reader transaction: {error}"))?;
        client
            .batch_execute(&format!(
                "set transaction snapshot {}",
                quote_literal(snapshot_name)
            ))
            .await
            .map_err(|error| {
                report!("import exported snapshot on snapshot reader: {error}")
            })?;
        // Hold ACCESS SHARE for the whole copy. Every reader takes the lock,
        // so a concurrent VACUUM FULL/CLUSTER cannot change CTIDs while the
        // other ranges are still being planned or copied.
        client
            .batch_execute(&format!("lock table {table} in access share mode"))
            .await
            .map_err(|error| report!("lock source table for snapshot: {error}"))?;
        clients.push(client);
    }
    Ok(clients)
}

fn copy_sql(plan: &TablePlan, range: CtidRange) -> String {
    let columns = plan
        .source
        .columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let table = format!(
        "{}.{}",
        quote_ident(&plan.source.schema),
        quote_ident(&plan.source.name)
    );
    let mut predicate = format!("ctid >= '({},0)'::tid", range.start);
    if let Some(end) = range.end {
        predicate.push_str(&format!(" and ctid < '({},0)'::tid", end));
    }
    format!("copy (select {columns} from {table} where {predicate}) to stdout")
}

/// A single COPY row larger than this is rejected instead of growing the
/// decoder buffer without bound. The limit is deliberately independent of the
/// batch budget: one TOASTed value can exceed the whole batch.
const MAX_SNAPSHOT_ROW_BYTES: usize = 64 * 1024 * 1024;

async fn copy_range(
    client: Client,
    sql: String,
    reader_index: usize,
    config: &SubscribeConfig,
    plan: &TablePlan,
) -> Result<(Vec<lakesoul_io::writer::async_writer::FlushOutput>, usize)> {
    let (outputs, rows) = {
        let stream = client.copy_out(&sql).await.map_err(|error| {
            report!("snapshot reader {reader_index} start COPY: {error}")
        })?;
        futures::pin_mut!(stream);
        let mut decoder = CopyTextDecoder::new();
        let mut sink = SnapshotSink::new(config, plan);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                report!("snapshot reader {reader_index} read COPY: {error}")
            })?;
            decoder.push(chunk);
            while let Some(line) = decoder.next_line() {
                let values = decode_line(&line, plan.source.columns.len())?;
                let data = row_data(plan, values);
                sink.push(&data).await?;
            }
            if decoder.buffered_len() > MAX_SNAPSHOT_ROW_BYTES {
                bail!(
                    "snapshot reader {reader_index} buffered more than \
                     {MAX_SNAPSHOT_ROW_BYTES} bytes without a row terminator; \
                     a single source row is too large"
                );
            }
        }
        decoder.finish()?;
        sink.finish().await?;
        sink.into_outputs()
    };
    let _ = client.batch_execute("commit").await;
    tracing::debug!(
        reader = reader_index,
        rows,
        files = outputs.len(),
        "snapshot reader finished"
    );
    Ok((outputs, rows))
}

fn row_data(plan: &TablePlan, values: Vec<ColumnValue>) -> RowData {
    let mut data = RowData::with_capacity(values.len());
    for (column, value) in plan.source.columns.iter().zip(values) {
        data.push(Arc::from(column.name.as_str()), value);
    }
    data
}

struct SnapshotSink<'a> {
    config: &'a SubscribeConfig,
    plan: &'a TablePlan,
    rows: Vec<RowState>,
    outputs: Vec<lakesoul_io::writer::async_writer::FlushOutput>,
    buffered_bytes: usize,
    total_rows: usize,
}

impl<'a> SnapshotSink<'a> {
    fn new(config: &'a SubscribeConfig, plan: &'a TablePlan) -> Self {
        Self {
            config,
            plan,
            rows: Vec::new(),
            outputs: Vec::new(),
            buffered_bytes: 0,
            total_rows: 0,
        }
    }

    async fn push(&mut self, data: &RowData) -> Result<()> {
        let state = self.plan.parse_row(data, Op::Insert, "snapshot")?;
        self.buffered_bytes += state.values.iter().map(scalar_size).sum::<usize>();
        self.rows.push(state);

        if self.rows.len() >= self.config.snapshot_batch_rows
            || self.buffered_bytes >= self.config.snapshot_batch_bytes
        {
            self.flush_files().await?;
        }
        Ok(())
    }

    async fn flush_files(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let states: Vec<&RowState> = self.rows.iter().collect();
        let batch = build_batch(&self.plan.schema, self.plan.cdc_index, &states)?;
        let outputs = write_files(
            &self.plan.table,
            &self.plan.target_table,
            batch,
            &self.config.memory_pool,
        )
        .await?;
        self.outputs.extend(outputs);
        self.total_rows += self.rows.len();
        self.rows.clear();
        self.buffered_bytes = 0;
        tracing::debug!(
            rows = self.total_rows,
            files = self.outputs.len(),
            "snapshot batch written"
        );
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.flush_files().await
    }

    fn into_outputs(
        self,
    ) -> (Vec<lakesoul_io::writer::async_writer::FlushOutput>, usize) {
        (self.outputs, self.total_rows)
    }
}

fn scalar_size(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            value.len()
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            value.len()
        }
        ScalarValue::Utf8View(Some(value)) => value.len(),
        _ => 16,
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn drop_slot_if_exists(url: &str, slot: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|error| report!("connect to source PostgreSQL: {error}"))?;
    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "source PostgreSQL connection closed");
        }
    });
    let result = async {
        let row = client
            .query_opt(
                "select active from pg_replication_slots \
                 where slot_name = $1 and database = current_database()",
                &[&slot],
            )
            .await
            .map_err(|error| report!("read replication slot {slot}: {error}"))?;
        let Some(row) = row else {
            return Ok(());
        };
        let active: bool = row.get(0);
        if active {
            bail!(
                "replication slot {slot} is active; another subscriber is already running"
            );
        }
        client
            .execute("select pg_drop_replication_slot($1)", &[&slot])
            .await
            .map_err(|error| report!("drop replication slot {slot}: {error}"))?;
        tracing::info!(slot, "dropped stale replication slot before snapshot");
        Ok(())
    }
    .await;
    drop(client);
    handle.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_cover_every_page_once() {
        for pages in [0u64, 1, 2, 5, 17, 1000] {
            for requested in [1usize, 2, 4, 8] {
                let ranges = ctid_ranges(pages, reader_count(pages, requested));
                if pages == 0 {
                    assert!(ranges.is_empty());
                    continue;
                }
                assert_eq!(ranges.first().unwrap().start, 0);
                for pair in ranges.windows(2) {
                    assert_eq!(pair[0].end, Some(pair[1].start));
                }
                assert!(ranges.last().unwrap().end.is_none());
                assert_eq!(ranges.len(), reader_count(pages, requested));
            }
        }
    }

    #[test]
    fn quotes_postgres_identifiers_and_literals() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("a'b"), "'a''b'");
    }
}
