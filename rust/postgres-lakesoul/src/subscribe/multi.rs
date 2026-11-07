// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Multi-table subscription streaming.
//!
//! One publication and one logical replication slot feed one subscriber
//! process. Each mapped table keeps its own `TablePlan`/`TableSink` and its own
//! LakeSoul commit watermark; there is no cross-table atomicity.
//!
//! Managed snapshots copy each table independently from its own temporary
//! logical slot. The persistent slot starts at a common base LSN; streaming
//! begins there and skips each table up to its own snapshot LSN.

use lakesoul_metadata::MetaDataClientRef;
use rootcause::{bail, report};

use super::plan::TablePlan;
use super::state::{
    SubscriptionDefinition, SubscriptionMetadata, SubscriptionRecord, TableDefinition,
};
use super::{SnapshotMode, SubscribeConfig, ordinary_url, snapshot, source};
use crate::Result;

pub(crate) async fn run(config: SubscribeConfig, meta: MetaDataClientRef) -> Result<()> {
    let mut plans = Vec::with_capacity(config.tables.len());
    for mapping in &config.tables {
        let source = source::inspect_source(
            &ordinary_url(&config.source_url),
            &config.publication,
            &mapping.source_schema,
            &mapping.source_table,
        )
        .await?;
        let primary_key = if mapping.primary_key.is_empty() {
            None
        } else {
            Some(mapping.primary_key.as_slice())
        };
        let plan = TablePlan::new(
            source,
            &mapping.target_namespace,
            &mapping.target_table,
            primary_key,
            meta.clone(),
        )
        .await?;
        plans.push(plan);
    }
    if plans.len() != config.tables.len() {
        bail!("multi-table plan count does not match the mapping count");
    }

    if config.dry_run {
        for (mapping, plan) in config.tables.iter().zip(&plans) {
            println!("source: {}.{}", mapping.source_schema, mapping.source_table);
            println!(
                "target: {}.{}",
                mapping.target_namespace, mapping.target_table
            );
            println!("primary key: {}", mapping.primary_key.join(", "));
            println!("cdc column: {}", plan.cdc_column);
            println!("target commit watermark: {}", plan.commit_watermark);
        }
        println!("metadata subscription state is not read or written in --dry-run");
        return Ok(());
    }

    let pool = meta.pooled_client();
    let pool = pool.lock().await.clone();
    let metadata = SubscriptionMetadata::from_pool(pool);
    metadata.init_schema().await?;
    let snapshot_mode = match config.snapshot {
        SnapshotMode::None => "none",
        SnapshotMode::Auto | SnapshotMode::Managed => "managed",
    };
    let definition = definition(&config, &plans, snapshot_mode);
    let record = match metadata.load_by_id(&config.subscription_id).await? {
        Some(record) => {
            record.validate(&definition)?;
            record
        }
        None => {
            if config.require_existing {
                bail!(
                    "subscription {} disappeared before the child process started",
                    config.subscription_id
                );
            }
            metadata.register(&definition).await?;
            let record = metadata
                .load_by_id(&config.subscription_id)
                .await?
                .ok_or_else(|| {
                    report!("subscription {} was not persisted", config.subscription_id)
                })?;
            record.validate(&definition)?;
            record
        }
    };
    if record.tables.len() != plans.len() {
        bail!(
            "subscription {} has {} table mappings but {} plans",
            config.subscription_id,
            record.tables.len(),
            plans.len()
        );
    }

    let record = if config.snapshot == SnapshotMode::None {
        record
    } else {
        let record =
            prepare_managed_snapshots(&config, &plans, record, &metadata).await?;
        for plan in &mut plans {
            plan.refresh_commit_state().await?;
        }
        record
    };

    // `snapshot = none` is the pre-initialized-target mode. Every target must
    // be at least as advanced as its metadata data_lsn.
    let mut table_watermarks = Vec::with_capacity(plans.len());
    let mut any_commits = false;
    for plan in &plans {
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
        if table.data_lsn > plan.commit_watermark {
            bail!(
                "target table {}.{} is behind recorded data_lsn {} (target commit \
                 watermark {}); resnapshot is required",
                table.target_namespace,
                table.target_table,
                table.data_lsn,
                plan.commit_watermark
            );
        }
        any_commits |= plan.has_commits;
        table_watermarks.push(table.data_lsn.max(plan.commit_watermark));
    }
    let data_watermark = table_watermarks.iter().copied().max().unwrap_or(0);
    // Do not use the maximum table watermark as the global replay position:
    // a partially committed batch can leave one table ahead and another behind.
    // `record.processed_lsn` is advanced only after every table's commit.
    let processed_watermark = record.processed_lsn;
    let allow_empty_target_without_slot = record.processed_lsn == 0
        && table_watermarks.iter().all(|watermark| *watermark == 0);

    for (plan, watermark) in plans.iter().zip(&table_watermarks) {
        let table = record
            .tables
            .iter()
            .find(|table| {
                table.source_schema == plan.source.schema
                    && table.source_table == plan.source.name
            })
            .expect("validated above");
        if *watermark > table.data_lsn {
            metadata
                .update_table_data_lsn(
                    &config.subscription_id,
                    &table.source_schema,
                    &table.source_table,
                    *watermark,
                )
                .await?;
        }
    }
    super::check_slot(
        &ordinary_url(&config.source_url),
        &config.slot,
        data_watermark,
        processed_watermark,
        any_commits && !allow_empty_target_without_slot,
    )
    .await?;
    if processed_watermark > record.processed_lsn {
        metadata
            .update_progress(&config.subscription_id, processed_watermark)
            .await?;
    }
    metadata
        .set_state(&config.subscription_id, "streaming", None)
        .await?;
    tracing::info!(
        subscription = %config.subscription_id,
        slot = %config.slot,
        publication = %config.publication,
        tables = plans.len(),
        data_watermark,
        processed_watermark,
        "starting multi-table PostgreSQL subscriber"
    );

    let source_url = config.source_url.clone();
    let subscription_id = record.subscription_id.clone();
    let result = super::runner::run_multi(
        config,
        plans,
        record,
        metadata.clone(),
        processed_watermark,
        None,
    )
    .await;
    if let Err(error) = &result {
        super::record_state_error(&metadata, &subscription_id, &source_url, error).await;
    }
    result
}

async fn prepare_managed_snapshots(
    config: &SubscribeConfig,
    plans: &[TablePlan],
    record: SubscriptionRecord,
    metadata: &SubscriptionMetadata,
) -> Result<SubscriptionRecord> {
    let slot_url = ordinary_url(&config.source_url);
    let slot_exists = snapshot::primary_slot_exists(&slot_url, &config.slot).await?;
    let any_done = record
        .tables
        .iter()
        .any(|table| table.snapshot_state == "done");
    if any_done && !slot_exists {
        bail!(
            "replication slot {} is missing but at least one table snapshot is \
             already done; completed snapshots cannot be resumed safely. \
             Resnapshot the subscription",
            config.slot
        );
    }

    // The persistent slot is the common streaming start point. Each table's
    // temporary slot supplies its own exact snapshot LSN.
    let base_lsn = snapshot::ensure_primary_slot(config).await?;

    for (index, plan) in plans.iter().enumerate() {
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
        snapshot::drop_table_slot_if_exists(config, index).await?;
        match table.snapshot_state.as_str() {
            "done" => {
                if let Some(snapshot_lsn) = table.snapshot_lsn {
                    if base_lsn > snapshot_lsn {
                        bail!(
                            "table {}.{} snapshot LSN {} is older than the slot base \
                             LSN {}; resnapshot is required",
                            table.target_namespace,
                            table.target_table,
                            snapshot_lsn,
                            base_lsn
                        );
                    }
                    if plan.has_commits && plan.commit_watermark < snapshot_lsn {
                        bail!(
                            "target table {}.{} is behind its completed snapshot LSN {} \
                             (target commit watermark {}); resnapshot is required",
                            table.target_namespace,
                            table.target_table,
                            snapshot_lsn,
                            plan.commit_watermark
                        );
                    }
                }
                continue;
            }
            "pending" | "copying" | "failed" => {
                if plan.has_commits {
                    if table.snapshot_lsn == Some(plan.commit_watermark)
                        && plan.commit_watermark > 0
                    {
                        metadata
                            .set_snapshot_done(
                                &config.subscription_id,
                                &table.source_schema,
                                &table.source_table,
                                plan.commit_watermark,
                            )
                            .await?;
                        tracing::info!(
                            lsn = plan.commit_watermark,
                            table = %table.target_table,
                            "table snapshot data was already committed; marking done"
                        );
                        continue;
                    }
                    bail!(
                        "target table {}.{} is not empty but its snapshot is not \
                         complete (snapshot_state={}, snapshot_lsn={:?}, target commit \
                         watermark {}); resnapshot is required",
                        table.target_namespace,
                        table.target_table,
                        table.snapshot_state,
                        table.snapshot_lsn,
                        plan.commit_watermark
                    );
                }
                if let Err(error) = snapshot::run_table_snapshot(
                    config,
                    plan,
                    index,
                    metadata,
                    &config.subscription_id,
                )
                .await
                {
                    let text = super::sanitized_error(&error, &config.source_url);
                    if let Err(state_error) = metadata
                        .set_snapshot_failed(
                            &config.subscription_id,
                            &table.source_schema,
                            &table.source_table,
                            &text,
                        )
                        .await
                    {
                        tracing::warn!(%state_error, "failed to record table snapshot failure");
                    }
                    return Err(error);
                }
            }
            other => {
                bail!(
                    "table {}.{} has unknown snapshot_state {other:?}",
                    table.target_namespace,
                    table.target_table
                );
            }
        }
    }

    let mut record = metadata
        .load_by_id(&config.subscription_id)
        .await?
        .ok_or_else(|| report!("subscription {} disappeared", config.subscription_id))?;
    if base_lsn > record.processed_lsn {
        metadata
            .update_progress(&config.subscription_id, base_lsn)
            .await?;
        record.processed_lsn = base_lsn;
    }
    Ok(record)
}

fn definition(
    config: &SubscribeConfig,
    plans: &[TablePlan],
    snapshot_mode: &str,
) -> SubscriptionDefinition {
    SubscriptionDefinition {
        subscription_id: config.subscription_id.clone(),
        slot: config.slot.clone(),
        publication: config.publication.clone(),
        source_url: config.source_url.clone(),
        snapshot_mode: snapshot_mode.to_string(),
        supervised: config.require_existing,
        commit_rows: config.commit_rows,
        commit_interval_ms: config.commit_interval.as_millis() as u64,
        snapshot_batch_rows: config.snapshot_batch_rows,
        snapshot_batch_bytes: config.snapshot_batch_bytes,
        snapshot_parallelism: config.snapshot_parallelism,
        snapshot_memory_bytes: config.snapshot_memory_bytes,
        tables: plans
            .iter()
            .map(|plan| TableDefinition {
                source_schema: plan.source.schema.clone(),
                source_table: plan.source.name.clone(),
                source_relation_oid: plan.source.relation_oid,
                target_namespace: plan.table.table_namespace().to_string(),
                target_table: plan.target_table.clone(),
                target_table_id: plan.table.table_info().table_id.clone(),
                primary_key: plan.source.primary_key.clone(),
            })
            .collect(),
    }
}
