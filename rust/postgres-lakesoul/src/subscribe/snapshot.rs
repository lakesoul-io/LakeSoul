// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Managed initial snapshot and atomic handoff to streaming.
//!
//! The target table stays empty until the whole snapshot has been copied to
//! files; then one `commit_data_commit_infos` call publishes every file with
//! the slot's consistent point as the commit id. A failed snapshot therefore
//! leaves an empty target, not a half-loaded table.

use datafusion::common::ScalarValue;
use pg_walstream::{EventType, LogicalReplicationStream, RowData, SnapshotOutcome};
use rootcause::{bail, report};
use tokio_postgres::NoTls;

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
        "starting initial snapshot"
    );

    let mut events = snapshot.events();
    let mut sink = SnapshotSink::new(config, plan);
    while let Some(event) = events
        .next_event()
        .await
        .map_err(|error| report!("read initial snapshot: {error}"))?
    {
        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } if schema.as_ref() == config.source_schema
                && table.as_ref() == config.source_table =>
            {
                sink.push(data).await?;
            }
            EventType::Relation { .. } => {}
            _ => {}
        }
    }
    sink.flush_files().await?;
    let stream = events
        .finish()
        .await
        .map_err(|error| report!("finish initial snapshot: {error}"))?;
    sink.commit(consistent_point).await?;

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
        "initial snapshot committed; switching to streaming"
    );
    Ok((stream, consistent_point))
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
        let outputs =
            write_files(&self.plan.table, &self.plan.target_table, batch).await?;
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

    async fn commit(self, consistent_point: u64) -> Result<()> {
        if self.outputs.is_empty() {
            tracing::info!(
                lsn = consistent_point,
                "snapshot has no rows; skipping the data commit"
            );
            return Ok(());
        }
        commit_outputs(
            &self.plan.table,
            &self.plan.target_table,
            self.outputs,
            consistent_point,
            self.total_rows,
        )
        .await
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
