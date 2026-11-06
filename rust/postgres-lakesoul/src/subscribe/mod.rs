// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Native PostgreSQL logical-replication subscriber (P0).
//!
//! P0 is intentionally narrow: one publication, one table, ordinary `public`
//! tables, `REPLICA IDENTITY FULL`, no TOAST (`u` is an error), no schema
//! evolution, no truncate, no initial snapshot, and no target range
//! partitioning. The goal is to prove the WAL -> Arrow -> LakeSoul CDC batch
//! path and its replay semantics before broadening the surface.
//!
//! The subscription definition and progress live in the metadata PostgreSQL
//! (`subscription` schema); the target table's commit ids remain the
//! authoritative data watermark.

mod copy_text;
mod decode;
mod plan;
mod runner;
mod sink;
mod snapshot;
mod source;
pub(crate) mod sql;
mod state;
pub(crate) mod supervisor;

use std::sync::Arc;
use std::time::Duration;

use clap::{Args, ValueEnum};
use datafusion::execution::memory_pool::{
    FairSpillPool, MemoryPool, UnboundedMemoryPool,
};
use lakesoul_metadata::MetaDataClientRef;
use rootcause::{bail, report};
use tokio_postgres::NoTls;

use crate::Result;
use state::{SubscriptionDefinition, SubscriptionMetadata, SubscriptionRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SnapshotMode {
    Auto,
    Managed,
    None,
}

#[derive(Args, Debug, Clone)]
pub struct SubscribeArgs {
    /// Run the logical-replication subscriber instead of the PG wire server.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE")]
    pub subscribe: bool,

    /// Source PostgreSQL URL (without `replication=database`).
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE_SOURCE_URL")]
    pub source_url: Option<String>,

    /// Source publication name.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE_PUBLICATION")]
    pub publication: Option<String>,

    /// Replication slot name. Created if missing.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE_SLOT")]
    pub slot: Option<String>,

    /// Source table, `public.users` or `users`.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE_SOURCE_TABLE")]
    pub source_table: Option<String>,

    /// Target LakeSoul table name (must already exist).
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE_TARGET_TABLE")]
    pub target_table: Option<String>,

    /// Target LakeSoul namespace (the PG database name).
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE_TARGET_NAMESPACE")]
    pub target_namespace: Option<String>,

    /// Comma-separated source/target primary key columns.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIBE_PRIMARY_KEY")]
    pub primary_key: Option<String>,

    /// Flush a batch once this many distinct keys are pending.
    #[clap(long, default_value_t = 50_000)]
    pub commit_rows: usize,

    /// Flush at most this often while transactions keep arriving.
    #[clap(long, default_value_t = 1_000)]
    pub commit_interval_ms: u64,

    /// Exit after this many source transactions; 0 means unlimited.
    #[clap(long, default_value_t = 0)]
    pub max_transactions: u64,

    /// Initial load strategy: auto (managed when the target is empty),
    /// managed, or none (the target is already initialized).
    #[clap(
        long,
        value_enum,
        default_value = "auto",
        env = "LAKESOUL_PG_SUBSCRIBE_SNAPSHOT"
    )]
    pub snapshot: SnapshotMode,

    /// Snapshot batch size in rows.
    #[clap(long, default_value_t = 50_000)]
    pub snapshot_batch_rows: usize,

    /// Snapshot batch size in bytes.
    #[clap(long, default_value_t = 64 * 1024 * 1024)]
    pub snapshot_batch_bytes: usize,

    /// Number of parallel COPY readers for the initial snapshot. Each reader
    /// also writes its own LakeSoul files.
    #[clap(long, default_value_t = 4)]
    pub snapshot_parallelism: usize,

    /// Process-wide memory budget shared by the snapshot writers. 0 means
    /// unbounded.
    #[clap(long, default_value_t = 1024 * 1024 * 1024)]
    pub snapshot_memory_bytes: usize,

    /// Run a subscription stored in the metadata PostgreSQL. This is used by
    /// the server's subscription supervisor; all other definition flags are
    /// ignored.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIPTION_ID")]
    pub subscription_id: Option<String>,

    /// Validate source/target and print the plan, then exit.
    #[clap(long)]
    pub dry_run: bool,
}

pub struct SubscribeConfig {
    pub subscription_id: String,
    pub source_url: String,
    pub publication: String,
    pub slot: String,
    pub source_schema: String,
    pub source_table: String,
    pub target_namespace: String,
    pub target_table: String,
    pub primary_key: Vec<String>,
    pub commit_rows: usize,
    pub commit_interval: Duration,
    pub max_transactions: u64,
    pub snapshot: SnapshotMode,
    pub snapshot_batch_rows: usize,
    pub snapshot_batch_bytes: usize,
    pub snapshot_parallelism: usize,
    pub snapshot_memory_bytes: usize,
    pub memory_pool: Arc<dyn MemoryPool>,
    pub dry_run: bool,
    /// A supervised child must find its metadata row; it must never recreate
    /// a row concurrently removed by DROP.
    pub require_existing: bool,
}

impl SubscribeConfig {
    fn from_args(args: SubscribeArgs) -> Result<Self> {
        let source_url = required(args.source_url, "--source-url")?;
        let publication = required(args.publication, "--publication")?;
        let slot = required(args.slot, "--slot")?;
        let source_table = required(args.source_table, "--source-table")?;
        let target_namespace = required(args.target_namespace, "--target-namespace")?;
        let target_table = required(args.target_table, "--target-table")?;
        let primary_key = required(args.primary_key, "--primary-key")?
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if primary_key.is_empty() {
            bail!("--primary-key must name at least one column");
        }
        if args.commit_rows == 0 {
            bail!("--commit-rows must be greater than zero");
        }
        if args.commit_interval_ms == 0 {
            bail!("--commit-interval-ms must be greater than zero");
        }
        if args.snapshot_batch_rows == 0 {
            bail!("--snapshot-batch-rows must be greater than zero");
        }
        if args.snapshot_batch_bytes == 0 {
            bail!("--snapshot-batch-bytes must be greater than zero");
        }
        if args.snapshot_parallelism == 0 {
            bail!("--snapshot-parallelism must be greater than zero");
        }

        let (source_schema, source_table) = parse_source_table(&source_table)?;

        Ok(Self {
            subscription_id: slot.clone(),
            source_url,
            publication,
            slot,
            source_schema,
            source_table,
            target_namespace,
            target_table,
            primary_key,
            commit_rows: args.commit_rows,
            commit_interval: Duration::from_millis(args.commit_interval_ms),
            max_transactions: args.max_transactions,
            snapshot: args.snapshot,
            snapshot_batch_rows: args.snapshot_batch_rows,
            snapshot_batch_bytes: args.snapshot_batch_bytes,
            snapshot_parallelism: args.snapshot_parallelism,
            snapshot_memory_bytes: args.snapshot_memory_bytes,
            memory_pool: create_memory_pool(args.snapshot_memory_bytes),
            dry_run: args.dry_run,
            require_existing: false,
        })
    }

    pub fn source_qualified_name(&self) -> String {
        format!("{}.{}", self.source_schema, self.source_table)
    }
}

fn required(value: Option<String>, flag: &str) -> Result<String> {
    value.ok_or_else(|| report!("{flag} is required with --subscribe"))
}

/// One process-wide pool shared by every snapshot writer. A zero budget keeps
/// the old unbounded behavior for small/development deployments.
pub(crate) fn create_memory_pool(bytes: usize) -> Arc<dyn MemoryPool> {
    if bytes == 0 {
        Arc::new(UnboundedMemoryPool::default())
    } else {
        Arc::new(FairSpillPool::new(bytes))
    }
}

pub(crate) fn parse_source_table(source_table: &str) -> Result<(String, String)> {
    let (source_schema, source_table) = match source_table.split_once('.') {
        Some((schema, table)) => {
            if table.contains('.') {
                bail!(
                    "source table {source_table:?} has more than one dot; P0 only \
                     supports schema.table"
                );
            }
            (schema.to_string(), table.to_string())
        }
        None => ("public".to_string(), source_table.to_string()),
    };
    if source_schema != "public" {
        bail!("P0 only supports the public schema, got {source_schema}.{source_table}");
    }
    Ok((source_schema, source_table))
}

/// The validation connection must be an ordinary SQL connection. Strip a
/// `replication` parameter if the caller included it; the streaming URL gets
/// it back in `runner`.
pub(crate) fn ordinary_url(url: &str) -> String {
    let Some((base, query)) = url.split_once('?') else {
        return url.to_string();
    };
    let query = query
        .split('&')
        .filter(|pair| {
            let key = pair.split('=').next().unwrap_or_default();
            key != "replication"
        })
        .collect::<Vec<_>>()
        .join("&");
    if query.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{query}")
    }
}

pub fn replication_url(url: &str) -> String {
    let has_replication = url.split_once('?').is_some_and(|(_, query)| {
        query
            .split('&')
            .any(|pair| pair.split('=').next() == Some("replication"))
    });
    if has_replication {
        url.to_string()
    } else if url.contains('?') {
        format!("{url}&replication=database")
    } else {
        format!("{url}?replication=database")
    }
}

pub async fn run(args: SubscribeArgs, meta: MetaDataClientRef) -> Result<()> {
    if let Some(subscription_id) = args.subscription_id.clone() {
        if args.dry_run {
            bail!("--dry-run cannot be combined with --subscription-id");
        }
        return run_from_metadata(&subscription_id, meta).await;
    }
    let config = SubscribeConfig::from_args(args)?;
    run_config(config, meta).await
}

async fn run_from_metadata(subscription_id: &str, meta: MetaDataClientRef) -> Result<()> {
    let pool = meta.pooled_client();
    let pool = pool.lock().await.clone();
    let metadata = SubscriptionMetadata::from_pool(pool);
    metadata.init_schema().await?;
    let record = metadata
        .load_by_id(subscription_id)
        .await?
        .ok_or_else(|| report!("subscription {subscription_id} does not exist"))?;
    if !record.enabled {
        bail!("subscription {subscription_id} is disabled");
    }
    tracing::debug!(
        subscription = %subscription_id,
        supervisor_pid = ?record.supervisor_pid,
        log_path = ?record.log_path,
        state = %record.state,
        "loaded supervised subscription definition"
    );
    let table = record
        .tables
        .first()
        .ok_or_else(|| report!("subscription {subscription_id} has no table mapping"))?;
    let snapshot = match table.snapshot_mode.as_str() {
        "none" => SnapshotMode::None,
        "managed" => SnapshotMode::Managed,
        other => bail!("subscription has unknown snapshot mode {other:?}"),
    };
    let config = SubscribeConfig {
        subscription_id: record.subscription_id.clone(),
        source_url: record.source_url.clone(),
        publication: record.publication.clone(),
        slot: record.slot.clone(),
        source_schema: table.source_schema.clone(),
        source_table: table.source_table.clone(),
        target_namespace: table.target_namespace.clone(),
        target_table: table.target_table.clone(),
        primary_key: table.primary_key.clone(),
        commit_rows: record.commit_rows,
        commit_interval: Duration::from_millis(record.commit_interval_ms),
        max_transactions: 0,
        snapshot,
        snapshot_batch_rows: record.snapshot_batch_rows,
        snapshot_batch_bytes: record.snapshot_batch_bytes,
        snapshot_parallelism: record.snapshot_parallelism,
        snapshot_memory_bytes: record.snapshot_memory_bytes,
        memory_pool: create_memory_pool(record.snapshot_memory_bytes),
        dry_run: false,
        require_existing: true,
    };
    if config.commit_rows == 0
        || config.commit_interval.is_zero()
        || config.snapshot_batch_rows == 0
        || config.snapshot_batch_bytes == 0
        || config.snapshot_parallelism == 0
    {
        bail!("subscription {subscription_id} has invalid batch settings");
    }
    let source_url = config.source_url.clone();
    let result = run_config(config, meta).await;
    if let Err(error) = &result {
        // `run_config` already records most failures; this also covers the
        // early source/target validation, before it loaded the metadata row.
        record_state_error(&metadata, subscription_id, &source_url, error).await;
    }
    result
}

async fn run_config(config: SubscribeConfig, meta: MetaDataClientRef) -> Result<()> {
    let source = source::inspect_source(
        &ordinary_url(&config.source_url),
        &config.publication,
        &config.source_schema,
        &config.source_table,
    )
    .await?;
    let plan = plan::TablePlan::new(
        source,
        &config.target_namespace,
        &config.target_table,
        Some(&config.primary_key),
        meta.clone(),
    )
    .await?;
    let target_commit_watermark = plan.commit_watermark;

    if config.dry_run {
        print_plan(&config, &plan, target_commit_watermark);
        println!("metadata subscription state is not read or written in --dry-run");
        return Ok(());
    }

    let pool = meta.pooled_client();
    let pool = pool.lock().await.clone();
    let metadata = SubscriptionMetadata::from_pool(pool);
    metadata.init_schema().await?;
    let record = match register_or_validate(&config, &plan, &metadata).await {
        Ok(record) => record,
        Err(error) => {
            record_state_error(
                &metadata,
                &config.subscription_id,
                &config.source_url,
                &error,
            )
            .await;
            return Err(error);
        }
    };
    let subscription_id = record.subscription_id.clone();
    let table = record
        .tables
        .first()
        .ok_or_else(|| report!("subscription {subscription_id} has no table mapping"))?;

    check_snapshot_mode(config.snapshot, &table.snapshot_mode)?;
    tracing::debug!(
        snapshot_state = %table.snapshot_state,
        snapshot_mode = %table.snapshot_mode,
        snapshot_lsn = ?table.snapshot_lsn,
        snapshot_started_at = ?table.snapshot_started_at,
        snapshot_completed_at = ?table.snapshot_completed_at,
        "subscription snapshot state"
    );

    let mode = table.snapshot_mode.as_str();
    let mut needs_snapshot = false;

    if mode != "none" {
        match table.snapshot_state.as_str() {
            "done" => {
                if let Some(snapshot_lsn) = table.snapshot_lsn
                    && plan.has_commits
                    && target_commit_watermark < snapshot_lsn
                {
                    let error = report!(
                        "target table {} is behind the completed snapshot LSN {} \
                         (target commit watermark {}); resnapshot is required",
                        plan.target_table,
                        snapshot_lsn,
                        target_commit_watermark
                    );
                    record_state_error(
                        &metadata,
                        &subscription_id,
                        &config.source_url,
                        &error,
                    )
                    .await;
                    return Err(error);
                }
            }
            "pending" | "copying" | "failed" => {
                if plan.has_commits {
                    if table.snapshot_lsn == Some(target_commit_watermark)
                        && target_commit_watermark > 0
                    {
                        metadata
                            .set_snapshot_done(
                                &subscription_id,
                                &config.source_schema,
                                &config.source_table,
                                target_commit_watermark,
                            )
                            .await?;
                        tracing::info!(
                            lsn = target_commit_watermark,
                            "initial snapshot data was already committed; marking snapshot done"
                        );
                    } else {
                        let error = report!(
                            "target table {} has commits but the initial snapshot is not \
                             complete (snapshot_state={}, snapshot_lsn={:?}, target commit \
                             watermark {}); resnapshot is required",
                            plan.target_table,
                            table.snapshot_state,
                            table.snapshot_lsn,
                            target_commit_watermark
                        );
                        record_state_error(
                            &metadata,
                            &subscription_id,
                            &config.source_url,
                            &error,
                        )
                        .await;
                        return Err(error);
                    }
                } else {
                    needs_snapshot = true;
                }
            }
            other => {
                let error = report!(
                    "target table {} has unknown snapshot_state {other:?}",
                    plan.target_table
                );
                record_state_error(
                    &metadata,
                    &subscription_id,
                    &config.source_url,
                    &error,
                )
                .await;
                return Err(error);
            }
        }
    }

    if needs_snapshot {
        if plan.has_commits {
            let error = report!(
                "target table {} is not empty; initial snapshots require an empty target",
                plan.target_table
            );
            record_state_error(&metadata, &subscription_id, &config.source_url, &error)
                .await;
            return Err(error);
        }
        metadata
            .set_state(&subscription_id, "snapshotting", None)
            .await?;
        tracing::info!(
            source = %config.source_qualified_name(),
            target = %format!("{}.{}", config.target_namespace, config.target_table),
            slot = %config.slot,
            snapshot_batch_rows = config.snapshot_batch_rows,
            snapshot_batch_bytes = config.snapshot_batch_bytes,
            "starting initial snapshot"
        );
        let result =
            snapshot::run_managed_snapshot(&config, &plan, &metadata, &subscription_id)
                .await;
        let (stream, consistent_point) = match result {
            Ok(value) => value,
            Err(error) => {
                let text = sanitized_error(&error, &config.source_url);
                if let Err(state_error) = metadata
                    .set_snapshot_failed(
                        &subscription_id,
                        &config.source_schema,
                        &config.source_table,
                        &text,
                    )
                    .await
                {
                    tracing::warn!(%state_error, "failed to record snapshot failure");
                }
                record_state_error(
                    &metadata,
                    &subscription_id,
                    &config.source_url,
                    &error,
                )
                .await;
                return Err(error);
            }
        };
        metadata
            .set_state(&subscription_id, "streaming", None)
            .await?;
        tracing::info!(
            source = %config.source_qualified_name(),
            target = %format!("{}.{}", config.target_namespace, config.target_table),
            slot = %config.slot,
            publication = %config.publication,
            snapshot_lsn = consistent_point,
            previous_state = %record.state,
            previous_error = ?record.last_error,
            "initial snapshot complete; starting PostgreSQL logical replication subscriber"
        );
        let source_url = config.source_url.clone();
        let result = runner::run(
            config,
            plan,
            record,
            metadata.clone(),
            consistent_point,
            Some(stream),
        )
        .await;
        if let Err(error) = &result {
            record_state_error(&metadata, &subscription_id, &source_url, error).await;
        }
        return result;
    }

    // No snapshot: normal resume. `none` may run over a preloaded target with
    // no subscription commit of its own, so it must not require a slot when
    // there is no subscription watermark yet.
    let allow_empty_target_without_slot = mode == "none"
        && target_commit_watermark == 0
        && table.data_lsn == 0
        && record.processed_lsn == 0;
    if mode != "none"
        && plan.has_commits
        && target_commit_watermark == 0
        && record.processed_lsn == 0
    {
        let error = report!(
            "target table {} has commits but no subscription watermark; use \
             --snapshot none only for a target you initialized yourself",
            plan.target_table
        );
        record_state_error(&metadata, &subscription_id, &config.source_url, &error).await;
        return Err(error);
    }
    let recorded_data_lsn = table.data_lsn;
    if recorded_data_lsn > target_commit_watermark {
        let error = report!(
            "target table {} is behind the recorded data_lsn {} (target commit \
             watermark {}); resnapshot is required",
            plan.target_table,
            recorded_data_lsn,
            target_commit_watermark
        );
        record_state_error(&metadata, &subscription_id, &config.source_url, &error).await;
        return Err(error);
    }

    let data_watermark = recorded_data_lsn.max(target_commit_watermark);
    let processed_watermark = record.processed_lsn.max(data_watermark);

    // Keep the auxiliary table watermark aligned with the authoritative target
    // commit ids, even when the metadata was restored behind them.
    if data_watermark > recorded_data_lsn
        && let Err(error) = metadata
            .update_table_data_lsn(
                &subscription_id,
                &config.source_schema,
                &config.source_table,
                data_watermark,
            )
            .await
    {
        record_state_error(&metadata, &subscription_id, &config.source_url, &error).await;
        return Err(error);
    }

    if let Err(error) = check_slot(
        &ordinary_url(&config.source_url),
        &config.slot,
        data_watermark,
        processed_watermark,
        plan.has_commits && !allow_empty_target_without_slot,
    )
    .await
    {
        record_state_error(&metadata, &subscription_id, &config.source_url, &error).await;
        return Err(error);
    }

    // The metadata row may lag the target commit ids (for example after a
    // metadata restore). Catch it up before streaming.
    if processed_watermark > record.processed_lsn
        && let Err(error) = metadata
            .update_progress(&subscription_id, processed_watermark)
            .await
    {
        record_state_error(&metadata, &subscription_id, &config.source_url, &error).await;
        return Err(error);
    }
    metadata
        .set_state(&subscription_id, "streaming", None)
        .await?;

    tracing::info!(
        source = %config.source_qualified_name(),
        target = %format!("{}.{}", config.target_namespace, config.target_table),
        slot = %config.slot,
        publication = %config.publication,
        data_watermark,
        processed_watermark,
        previous_state = %record.state,
        previous_error = ?record.last_error,
        table_state = %table.state,
        table_error = ?table.last_error,
        "starting PostgreSQL logical replication subscriber"
    );

    let source_url = config.source_url.clone();
    let result = runner::run(
        config,
        plan,
        record,
        metadata.clone(),
        processed_watermark,
        None,
    )
    .await;
    if let Err(error) = &result {
        record_state_error(&metadata, &subscription_id, &source_url, error).await;
    }
    result
}

fn check_snapshot_mode(requested: SnapshotMode, stored: &str) -> Result<()> {
    match (requested, stored) {
        (SnapshotMode::Auto, _) => Ok(()),
        (SnapshotMode::Managed, "managed") => Ok(()),
        (SnapshotMode::None, "none") => Ok(()),
        (SnapshotMode::Managed, _) => bail!(
            "subscription was registered with snapshot_mode={stored}; use \
             --snapshot auto/none or create a new subscription to enable a snapshot"
        ),
        (SnapshotMode::None, _) => bail!(
            "subscription was registered with snapshot_mode={stored}; use \
             --snapshot auto/managed or create a new subscription"
        ),
    }
}

async fn register_or_validate(
    config: &SubscribeConfig,
    plan: &plan::TablePlan,
    metadata: &SubscriptionMetadata,
) -> Result<SubscriptionRecord> {
    let snapshot_mode = match config.snapshot {
        SnapshotMode::None => "none",
        SnapshotMode::Auto | SnapshotMode::Managed => "managed",
    };
    let definition = SubscriptionDefinition {
        subscription_id: config.subscription_id.clone(),
        slot: config.slot.clone(),
        publication: config.publication.clone(),
        source_url: config.source_url.clone(),
        source_schema: config.source_schema.clone(),
        source_table: config.source_table.clone(),
        source_relation_oid: plan.source.relation_oid,
        target_namespace: config.target_namespace.clone(),
        target_table: config.target_table.clone(),
        target_table_id: plan.table.table_info().table_id.clone(),
        primary_key: config.primary_key.clone(),
        snapshot_mode: snapshot_mode.to_string(),
        supervised: config.require_existing,
        commit_rows: config.commit_rows,
        commit_interval_ms: config.commit_interval.as_millis() as u64,
        snapshot_batch_rows: config.snapshot_batch_rows,
        snapshot_batch_bytes: config.snapshot_batch_bytes,
        snapshot_parallelism: config.snapshot_parallelism,
        snapshot_memory_bytes: config.snapshot_memory_bytes,
    };
    match metadata.load_by_id(&config.subscription_id).await? {
        Some(record) => {
            record.validate(&definition)?;
            Ok(record)
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
            Ok(record)
        }
    }
}

async fn record_state_error(
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
    source_url: &str,
    error: &rootcause::Report,
) {
    let text = sanitized_error(error, source_url);
    if let Err(state_error) = metadata
        .set_state(subscription_id, "error", Some(&text))
        .await
    {
        tracing::warn!(%state_error, "failed to record subscription error state");
    }
}

pub(crate) fn sanitized_error(error: &rootcause::Report, source_url: &str) -> String {
    let mut text = error.to_string();
    if !source_url.is_empty() {
        let ordinary = ordinary_url(source_url);
        let replication = replication_url(source_url);
        text = text.replace(&replication, "<source-url>");
        text = text.replace(&ordinary, "<source-url>");
        text = text.replace(source_url, "<source-url>");
    }
    const MAX: usize = 2_000;
    if text.len() > MAX {
        let mut cut = MAX;
        while !text.is_char_boundary(cut) {
            cut -= 1;
        }
        text.truncate(cut);
    }
    text
}

async fn check_slot(
    url: &str,
    slot: &str,
    data_watermark: u64,
    processed_watermark: u64,
    target_has_commits: bool,
) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|error| report!("connect to source PostgreSQL: {error}"))?;
    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "source PostgreSQL connection closed");
        }
    });
    let result = check_slot_inner(
        &client,
        slot,
        data_watermark,
        processed_watermark,
        target_has_commits,
    )
    .await;
    drop(client);
    handle.abort();
    result
}

async fn check_slot_inner(
    client: &tokio_postgres::Client,
    slot: &str,
    data_watermark: u64,
    processed_watermark: u64,
    target_has_commits: bool,
) -> Result<()> {
    let row = client
        .query_opt(
            "select restart_lsn::text, confirmed_flush_lsn::text, active, \
                    slot_type, plugin \
             from pg_replication_slots \
             where slot_name = $1 and database = current_database()",
            &[&slot],
        )
        .await
        .map_err(|error| report!("read replication slot {slot}: {error}"))?;

    let Some(row) = row else {
        if data_watermark > 0 {
            bail!(
                "replication slot {slot} does not exist but the target data watermark is \
                 {data_watermark}; create the slot before writing or resnapshot"
            );
        }
        if target_has_commits {
            bail!(
                "target table has commits but no subscription watermark and slot {slot} \
                 does not exist; P0 does not support initial loads"
            );
        }
        return Ok(());
    };

    let restart: Option<String> = row.get(0);
    let confirmed: Option<String> = row.get(1);
    let active: bool = row.get(2);
    let slot_type: String = row.get(3);
    let plugin: Option<String> = row.get(4);
    if active {
        bail!("replication slot {slot} is active; another subscriber is already running");
    }
    if slot_type != "logical" {
        bail!("replication slot {slot} is {slot_type}, not logical");
    }
    if plugin.as_deref() != Some("pgoutput") {
        bail!(
            "replication slot {slot} uses plugin {:?}, not pgoutput",
            plugin
        );
    }
    let restart = restart
        .as_deref()
        .map(source::parse_lsn)
        .transpose()?
        .unwrap_or(0);
    let confirmed = confirmed
        .as_deref()
        .map(source::parse_lsn)
        .transpose()?
        .unwrap_or(0);

    if restart > processed_watermark {
        bail!(
            "slot {slot} restart_lsn {restart} is ahead of the subscription processed \
             watermark {processed_watermark}; the WAL needed by the target has been \
             removed. Resnapshot"
        );
    }
    if confirmed > processed_watermark {
        bail!(
            "slot {slot} confirmed_flush_lsn {confirmed} is ahead of the subscription \
             processed watermark {processed_watermark}; resnapshot is required"
        );
    }
    Ok(())
}

fn print_plan(config: &SubscribeConfig, plan: &plan::TablePlan, watermark: u64) {
    println!("source: {}", config.source_qualified_name());
    println!(
        "target: {}.{}",
        config.target_namespace, config.target_table
    );
    println!("primary key: {}", config.primary_key.join(", "));
    println!("cdc column: {}", plan.cdc_column);
    println!("target commit watermark: {watermark}");
    println!("columns:");
    for mapping in &plan.mappings {
        let source = &plan.source.columns[mapping.source_index];
        let target = plan.schema.field(mapping.target_index);
        println!(
            "  {} {} -> {} {:?}",
            source.name,
            source.type_name,
            target.name(),
            target.data_type()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_url_drops_replication() {
        assert_eq!(
            ordinary_url("postgresql://u:p@h/db?replication=database&sslmode=disable"),
            "postgresql://u:p@h/db?sslmode=disable"
        );
        assert_eq!(
            ordinary_url("postgresql://u:p@h/db?replication=database"),
            "postgresql://u:p@h/db"
        );
        assert_eq!(
            ordinary_url("postgresql://u:p@h/db"),
            "postgresql://u:p@h/db"
        );
    }

    #[test]
    fn streaming_url_adds_replication() {
        assert_eq!(
            replication_url("postgresql://u:p@h/db"),
            "postgresql://u:p@h/db?replication=database"
        );
        assert_eq!(
            replication_url("postgresql://u:p@h/db?sslmode=disable"),
            "postgresql://u:p@h/db?sslmode=disable&replication=database"
        );
        assert_eq!(
            replication_url("postgresql://u:p@h/db?replication=database"),
            "postgresql://u:p@h/db?replication=database"
        );
    }

    #[test]
    fn error_text_redacts_the_source_url() {
        let error = report!("connect postgresql://user:secret@host/db failed");
        let text = sanitized_error(&error, "postgresql://user:secret@host/db");
        assert!(text.contains("<source-url>"));
        assert!(!text.contains("secret"));
    }
}
