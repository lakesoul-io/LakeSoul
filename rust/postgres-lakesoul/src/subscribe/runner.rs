// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The P0 replication event loop.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, ReplicationError,
    ReplicationStreamConfig, StreamingMode,
};
use rootcause::{bail, report};

use super::decode::KeyValue;
use super::plan::{RowState, TablePlan};
use super::sink::TableSink;
use super::state::{SubscriptionMetadata, SubscriptionRecord};
use super::{SubscribeConfig, replication_url};
use crate::Result;

pub(crate) async fn create_stream(
    config: &SubscribeConfig,
) -> Result<LogicalReplicationStream> {
    let stream_config =
        ReplicationStreamConfig::builder(&config.slot, &config.publication)
            .with_protocol_version(1)
            .with_streaming_mode(StreamingMode::Off)
            .with_feedback_interval(Duration::from_secs(1));
    LogicalReplicationStream::new(&replication_url(&config.source_url), stream_config)
        .await
        .map_err(|error| report!("create logical replication stream: {error}"))
}

pub async fn run(
    config: SubscribeConfig,
    plan: TablePlan,
    record: SubscriptionRecord,
    metadata: SubscriptionMetadata,
    processed_watermark: u64,
    stream: Option<LogicalReplicationStream>,
) -> Result<()> {
    let source_name = plan.qualified_source_name();
    let target_name = format!("{}.{}", config.target_namespace, config.target_table);
    let subscription_id = record.subscription_id.clone();
    let mut sink = TableSink::new(plan, Arc::clone(&config.memory_pool));

    let mut stream = match stream {
        Some(stream) => stream,
        None => create_stream(&config).await?,
    };
    stream
        .start(None)
        .await
        .map_err(|error| report!("start logical replication stream: {error}"))?;

    let cancellation = CancellationToken::new();
    let mut events = stream.into_stream(cancellation.clone());
    events.update_applied_lsn(processed_watermark);

    let mut ticker = tokio::time::interval(config.commit_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut shutdown = Box::pin(shutdown_signal());

    let mut current_tx: Vec<(Vec<KeyValue>, RowState)> = Vec::new();
    let mut latest_lsn = processed_watermark;
    let mut processed_lsn = processed_watermark;
    let mut persisted_progress = processed_watermark;
    let mut transactions = 0u64;

    tracing::info!(
        source = %source_name,
        target = %target_name,
        watermark = processed_watermark,
        "subscriber streaming"
    );

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received");
                break;
            }
            _ = ticker.tick() => {
                if !sink.is_empty() {
                    flush(
                        &mut sink,
                        &config,
                        &metadata,
                        &subscription_id,
                        latest_lsn,
                        &mut processed_lsn,
                        &mut persisted_progress,
                    ).await?;
                } else if processed_lsn > persisted_progress {
                    metadata.update_progress(&subscription_id, processed_lsn).await?;
                    persisted_progress = processed_lsn;
                }
                events.update_applied_lsn(processed_lsn);
            }
            event = events.next() => {
                let Some(event) = event else {
                    break;
                };
                let event = event.map_err(|error| {
                    if matches!(error, ReplicationError::Cancelled(_)) {
                        report!("logical replication stream cancelled")
                    } else {
                        report!("logical replication stream: {error}")
                    }
                })?;

                match &event.event_type {
                    EventType::Begin { .. } => {
                        if !current_tx.is_empty() {
                            bail!(
                                "received BEGIN while a previous transaction still has \
                                 buffered changes"
                            );
                        }
                    }
                    EventType::Insert { schema, table, data, .. }
                        if is_target(&config, schema, table) =>
                    {
                        let (key, state) = sink.plan.parse_insert(data)?;
                        current_tx.push((key, state));
                    }
                    EventType::Update {
                        schema,
                        table,
                        old_data,
                        new_data,
                        ..
                    } if is_target(&config, schema, table) => {
                        let changes = sink
                            .plan
                            .parse_update(old_data.as_ref(), new_data)?;
                        current_tx.extend(changes);
                    }
                    EventType::Delete {
                        schema,
                        table,
                        old_data,
                        ..
                    } if is_target(&config, schema, table) => {
                        let (key, state) = sink.plan.parse_delete(old_data)?;
                        current_tx.push((key, state));
                    }
                    EventType::Relation {
                        relation_id,
                        namespace,
                        relation_name,
                        columns,
                        ..
                    } if namespace.as_ref() == config.source_schema
                        && relation_name.as_ref() == config.source_table =>
                    {
                        sink.plan.check_relation(*relation_id, columns)?;
                    }
                    EventType::Truncate { tables, .. } => {
                        let qualified = format!(
                            "{}.{}",
                            config.source_schema, config.source_table
                        );
                        if tables.iter().any(|table| table.as_ref() == qualified) {
                            bail!(
                                "source table {qualified} was truncated; P0 does not \
                                 support TRUNCATE"
                            );
                        }
                    }
                    EventType::Commit { commit_lsn, .. } => {
                        let lsn = commit_lsn.value();
                        if lsn > processed_watermark {
                            transactions += 1;
                            for (key, state) in current_tx.drain(..) {
                                sink.apply(key, state);
                            }
                            latest_lsn = latest_lsn.max(lsn);
                        } else {
                            current_tx.clear();
                        }

                        if current_tx.is_empty()
                            && sink.is_empty()
                            && lsn > processed_lsn
                        {
                            processed_lsn = lsn;
                        }

                        if !sink.is_empty() && sink.pending_rows() >= config.commit_rows
                        {
                            flush(
                                &mut sink,
                                &config,
                                &metadata,
                                &subscription_id,
                                latest_lsn,
                                &mut processed_lsn,
                                &mut persisted_progress,
                            ).await?;
                            events.update_applied_lsn(processed_lsn);
                        }

                        if config.max_transactions > 0
                            && transactions >= config.max_transactions
                        {
                            break;
                        }
                    }
                    EventType::Origin { .. }
                    | EventType::Type { .. }
                    | EventType::Message { .. } => {
                        tracing::debug!(?event.event_type, "ignoring pgoutput event");
                    }
                    EventType::StreamStart { .. }
                    | EventType::StreamStop
                    | EventType::StreamCommit { .. }
                    | EventType::StreamAbort { .. }
                    | EventType::BeginPrepare { .. }
                    | EventType::Prepare { .. }
                    | EventType::CommitPrepared { .. }
                    | EventType::RollbackPrepared { .. }
                    | EventType::StreamPrepare { .. } => {
                        bail!(
                            "received streaming/two-phase event {:?} with protocol \
                             version 1; check replication settings",
                            event.event_type
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    if !sink.is_empty() {
        flush(
            &mut sink,
            &config,
            &metadata,
            &subscription_id,
            latest_lsn,
            &mut processed_lsn,
            &mut persisted_progress,
        )
        .await?;
    }
    if processed_lsn > persisted_progress {
        metadata
            .update_progress(&subscription_id, processed_lsn)
            .await?;
    }
    let stop_state = metadata.set_state(&subscription_id, "stopped", None).await;
    events.update_applied_lsn(processed_lsn);
    if let Err(error) = events.shutdown().await {
        tracing::warn!(%error, "final replication feedback failed");
    }
    tracing::info!(
        transactions,
        watermark = processed_lsn,
        "subscriber stopped"
    );
    stop_state?;
    Ok(())
}

fn is_target(config: &SubscribeConfig, schema: &str, table: &str) -> bool {
    schema == config.source_schema && table == config.source_table
}

async fn flush(
    sink: &mut TableSink,
    config: &SubscribeConfig,
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
    latest_lsn: u64,
    processed_lsn: &mut u64,
    persisted_progress: &mut u64,
) -> Result<()> {
    if sink.is_empty() {
        return Ok(());
    }
    let watermark = latest_lsn.max(*processed_lsn);
    if let Some(lsn) = sink.flush(watermark).await? {
        metadata
            .update_table_data_lsn(
                subscription_id,
                &config.source_schema,
                &config.source_table,
                lsn,
            )
            .await?;
        *processed_lsn = (*processed_lsn).max(lsn);
        metadata
            .update_progress(subscription_id, *processed_lsn)
            .await?;
        *persisted_progress = *processed_lsn;
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to install Ctrl-C handler");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
