// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Multi-table subscription streaming.
//!
//! One publication and one logical replication slot feed one subscriber
//! process. Each mapped table keeps its own `TablePlan`/`TableSink` and its own
//! LakeSoul commit watermark; there is no cross-table atomicity.
//!
//! Managed multi-table snapshots are a later phase. Until then a multi-table
//! subscription must use `snapshot = none` with pre-initialized targets.

use lakesoul_metadata::MetaDataClientRef;
use rootcause::{bail, report};

use super::plan::TablePlan;
use super::state::{SubscriptionDefinition, SubscriptionMetadata, TableDefinition};
use super::{SnapshotMode, SubscribeConfig, ordinary_url, source};
use crate::Result;

pub(crate) async fn run(config: SubscribeConfig, meta: MetaDataClientRef) -> Result<()> {
    if config.snapshot != SnapshotMode::None {
        bail!(
            "multi-table managed snapshots are not implemented yet; use \
             snapshot = 'none' with pre-initialized target tables"
        );
    }
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
    let definition = definition(&config, &plans);
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

fn definition(config: &SubscribeConfig, plans: &[TablePlan]) -> SubscriptionDefinition {
    SubscriptionDefinition {
        subscription_id: config.subscription_id.clone(),
        slot: config.slot.clone(),
        publication: config.publication.clone(),
        source_url: config.source_url.clone(),
        snapshot_mode: "none".to_string(),
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
