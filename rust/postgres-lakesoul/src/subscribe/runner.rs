// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The P0 replication event loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use lakesoul_metadata::MetaDataClientRef;
use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, RelationColumn,
    ReplicaIdentity, ReplicationError, ReplicationStreamConfig, StreamingMode,
};
use rootcause::{bail, report};

use super::decode::KeyValue;
use super::plan::{RowState, TablePlan};
use super::schema::{self, TargetOptions};
use super::sink::TableSink;
use super::source;
use super::state::{SubscriptionMetadata, SubscriptionRecord};
use super::{SubscribeConfig, ordinary_url, replication_url};
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
    meta: MetaDataClientRef,
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
                        replica_identity,
                        columns,
                        ..
                    } if namespace.as_ref() == config.source_schema
                        && relation_name.as_ref() == config.source_table =>
                    {
                        handle_relation_single(
                            &config,
                            &meta,
                            &mut sink,
                            &metadata,
                            &subscription_id,
                            *relation_id,
                            *replica_identity,
                            columns,
                            current_tx.is_empty(),
                            latest_lsn,
                            &mut processed_lsn,
                            &mut persisted_progress,
                        )
                        .await?;
                        events.update_applied_lsn(processed_lsn);
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

                        if sink.should_flush(config.commit_rows) {
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

pub async fn run_multi(
    config: SubscribeConfig,
    plans: Vec<TablePlan>,
    record: SubscriptionRecord,
    metadata: SubscriptionMetadata,
    meta: MetaDataClientRef,
    processed_watermark: u64,
    stream: Option<LogicalReplicationStream>,
) -> Result<()> {
    let subscription_id = record.subscription_id.clone();
    let mut source_index = HashMap::new();
    let mut table_names = Vec::new();
    let mut table_watermarks = Vec::with_capacity(plans.len());

    for (index, plan) in plans.iter().enumerate() {
        source_index.insert(
            (plan.source.schema.clone(), plan.source.name.clone()),
            index,
        );
        table_names.push((
            plan.source.schema.clone(),
            plan.source.name.clone(),
            plan.table.table_namespace().to_string(),
            plan.target_table.clone(),
        ));
        let table = record
            .tables
            .iter()
            .find(|table| {
                table.source_schema == plan.source.schema
                    && table.source_table == plan.source.name
            })
            .ok_or_else(|| {
                report!(
                    "subscription state has no mapping for {}.{}",
                    plan.source.schema,
                    plan.source.name
                )
            })?;
        table_watermarks.push(table.data_lsn.max(plan.commit_watermark));
    }

    let mut sinks = plans
        .into_iter()
        .map(|plan| TableSink::new(plan, Arc::clone(&config.memory_pool)))
        .collect::<Vec<_>>();

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

    let mut current_tx: Vec<(usize, Vec<KeyValue>, RowState)> = Vec::new();
    let mut latest_lsn = processed_watermark;
    let mut processed_lsn = processed_watermark;
    let mut persisted_progress = processed_watermark;
    let mut transactions = 0u64;

    tracing::info!(
        subscription = %subscription_id,
        slot = %config.slot,
        publication = %config.publication,
        tables = sinks.len(),
        watermark = processed_watermark,
        "multi-table subscriber streaming"
    );

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received");
                break;
            }
            _ = ticker.tick() => {
                if sinks.iter().any(|sink| !sink.is_empty()) {
                    flush_multi(
                        &mut sinks,
                        &mut table_watermarks,
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
                    EventType::Insert { schema, table, data, .. } => {
                        if let Some(&index) = source_index.get(
                            &(schema.to_string(), table.to_string())
                        ) {
                            let (key, state) = sinks[index].plan.parse_insert(data)?;
                            current_tx.push((index, key, state));
                        }
                    }
                    EventType::Update {
                        schema,
                        table,
                        old_data,
                        new_data,
                        ..
                    } => {
                        if let Some(&index) = source_index.get(
                            &(schema.to_string(), table.to_string())
                        ) {
                            let changes = sinks[index]
                                .plan
                                .parse_update(old_data.as_ref(), new_data)?;
                            current_tx.extend(
                                changes
                                    .into_iter()
                                    .map(|(key, state)| (index, key, state)),
                            );
                        }
                    }
                    EventType::Delete {
                        schema,
                        table,
                        old_data,
                        ..
                    } => {
                        if let Some(&index) = source_index.get(
                            &(schema.to_string(), table.to_string())
                        ) {
                            let (key, state) = sinks[index].plan.parse_delete(old_data)?;
                            current_tx.push((index, key, state));
                        }
                    }
                    EventType::Relation {
                        relation_id,
                        namespace,
                        relation_name,
                        replica_identity,
                        columns,
                        ..
                    } => {
                        if let Some(&index) = source_index.get(
                            &(namespace.to_string(), relation_name.to_string())
                        ) {
                            let current_tx_has_table = current_tx
                                .iter()
                                .any(|(table_index, _, _)| *table_index == index);
                            handle_relation_multi(
                                &config,
                                &meta,
                                &mut sinks,
                                index,
                                &mut table_watermarks,
                                &metadata,
                                &subscription_id,
                                *relation_id,
                                *replica_identity,
                                columns,
                                current_tx_has_table,
                                latest_lsn,
                                &mut processed_lsn,
                                &mut persisted_progress,
                            )
                            .await?;
                            events.update_applied_lsn(processed_lsn);
                        }
                    }
                    EventType::Truncate { tables, .. } => {
                        for table in tables {
                            let Some((schema, name)) = table.as_ref().split_once('.') else {
                                continue;
                            };
                            if source_index.contains_key(
                                &(schema.to_string(), name.to_string())
                            ) {
                                bail!(
                                    "source table {} was truncated; P0 does not \
                                     support TRUNCATE",
                                    table
                                );
                            }
                        }
                    }
                    EventType::Commit { commit_lsn, .. } => {
                        let lsn = commit_lsn.value();
                        if lsn > processed_watermark {
                            transactions += 1;
                            for (index, key, state) in current_tx.drain(..) {
                                if lsn > table_watermarks[index] {
                                    sinks[index].apply(key, state);
                                }
                            }
                            latest_lsn = latest_lsn.max(lsn);
                        } else {
                            current_tx.clear();
                        }

                        if sinks.iter().all(|sink| sink.is_empty())
                            && lsn > processed_lsn
                        {
                            processed_lsn = lsn;
                        }

                        if sinks
                            .iter()
                            .any(|sink| sink.should_flush(config.commit_rows))
                        {
                            flush_multi(
                                &mut sinks,
                                &mut table_watermarks,
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
                }
            }
        }
    }

    if sinks.iter().any(|sink| !sink.is_empty()) {
        flush_multi(
            &mut sinks,
            &mut table_watermarks,
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
        tables = table_names.len(),
        "multi-table subscriber stopped"
    );
    stop_state?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_relation_single(
    config: &SubscribeConfig,
    meta: &MetaDataClientRef,
    sink: &mut TableSink,
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
    relation_id: u32,
    replica_identity: ReplicaIdentity,
    columns: &[RelationColumn],
    current_tx_empty: bool,
    latest_lsn: u64,
    processed_lsn: &mut u64,
    persisted_progress: &mut u64,
) -> Result<()> {
    sink.plan.check_relation_id(relation_id)?;
    if replica_identity != ReplicaIdentity::Full {
        bail!(
            "source table {} changed replica identity to {replica_identity:?}; \
             REPLICA IDENTITY FULL is required",
            sink.plan.qualified_source_name()
        );
    }
    if sink.plan.relation_matches(columns) {
        return Ok(());
    }
    if !current_tx_empty {
        bail!(
            "source schema of {} changed inside a transaction that already has \
             changes for this table; restart the subscriber from the last \
             watermark",
            sink.plan.qualified_source_name()
        );
    }

    if !sink.is_empty() {
        flush(
            sink,
            config,
            metadata,
            subscription_id,
            latest_lsn,
            processed_lsn,
            persisted_progress,
        )
        .await?;
    }

    let source = source::inspect_source(
        &ordinary_url(&config.source_url),
        &config.publication,
        &config.source_schema,
        &config.source_table,
    )
    .await?;
    let target_options = TargetOptions::from_config(config);
    schema::ensure_target(
        meta,
        &source,
        &config.target_namespace,
        &config.target_table,
        &target_options,
        false,
        true,
    )
    .await?;
    let mut plan = TablePlan::new(
        source,
        &config.target_namespace,
        &config.target_table,
        Some(&config.primary_key),
        meta.clone(),
    )
    .await?;
    plan.set_active_relation(columns)?;
    sink.replace_plan(plan);
    tracing::info!(
        table = %config.source_qualified_name(),
        target = %format!("{}.{}", config.target_namespace, config.target_table),
        "target schema updated; continuing the replication stream"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_relation_multi(
    config: &SubscribeConfig,
    meta: &MetaDataClientRef,
    sinks: &mut [TableSink],
    index: usize,
    table_watermarks: &mut [u64],
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
    relation_id: u32,
    replica_identity: ReplicaIdentity,
    columns: &[RelationColumn],
    current_tx_has_table: bool,
    latest_lsn: u64,
    processed_lsn: &mut u64,
    persisted_progress: &mut u64,
) -> Result<()> {
    let mapping = &config.tables[index];
    sinks[index].plan.check_relation_id(relation_id)?;
    if replica_identity != ReplicaIdentity::Full {
        bail!(
            "source table {} changed replica identity to {replica_identity:?}; \
             REPLICA IDENTITY FULL is required",
            sinks[index].plan.qualified_source_name()
        );
    }
    if sinks[index].plan.relation_matches(columns) {
        return Ok(());
    }
    if current_tx_has_table {
        bail!(
            "source schema of {} changed inside a transaction that already has \
             changes for this table; restart the subscriber from the last \
             watermark",
            sinks[index].plan.qualified_source_name()
        );
    }

    flush_multi(
        sinks,
        table_watermarks,
        metadata,
        subscription_id,
        latest_lsn,
        processed_lsn,
        persisted_progress,
    )
    .await?;

    let source = source::inspect_source(
        &ordinary_url(&config.source_url),
        &config.publication,
        &mapping.source_schema,
        &mapping.source_table,
    )
    .await?;
    let target_options = TargetOptions::from_config(config);
    schema::ensure_target(
        meta,
        &source,
        &mapping.target_namespace,
        &mapping.target_table,
        &target_options,
        false,
        true,
    )
    .await?;
    let primary_key = if mapping.primary_key.is_empty() {
        None
    } else {
        Some(mapping.primary_key.as_slice())
    };
    let mut plan = TablePlan::new(
        source,
        &mapping.target_namespace,
        &mapping.target_table,
        primary_key,
        meta.clone(),
    )
    .await?;
    plan.set_active_relation(columns)?;
    sinks[index].replace_plan(plan);
    tracing::info!(
        table = %mapping.source_table,
        target = %format!("{}.{}", mapping.target_namespace, mapping.target_table),
        "target schema updated; continuing the replication stream"
    );
    Ok(())
}

async fn flush_multi(
    sinks: &mut [TableSink],
    table_watermarks: &mut [u64],
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
    latest_lsn: u64,
    processed_lsn: &mut u64,
    persisted_progress: &mut u64,
) -> Result<()> {
    let watermark = latest_lsn.max(*processed_lsn);
    for (index, sink) in sinks.iter_mut().enumerate() {
        if sink.is_empty() {
            continue;
        }
        if let Some(lsn) = sink.flush(watermark).await? {
            metadata
                .update_table_data_lsn(
                    subscription_id,
                    &sink.plan.source.schema,
                    &sink.plan.source.name,
                    lsn,
                )
                .await?;
            table_watermarks[index] = table_watermarks[index].max(lsn);
        }
    }
    if watermark > *processed_lsn {
        *processed_lsn = watermark;
        metadata
            .update_progress(subscription_id, *processed_lsn)
            .await?;
        *persisted_progress = *processed_lsn;
    }
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
