// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! MySQL subscription runner: snapshot -> binlog -> LakeSoul.

use std::collections::HashMap;

use lakesoul_metadata::MetaDataClientRef;
use rootcause::{bail, report};

use super::MySqlSubscribeConfig;
use super::binlog::{MySqlBatch, MySqlBinlogReader, MySqlChange, TableId};
use super::catalog::{
    MySqlOffset, MySqlSource, build_pool, inspect_source, master_status,
};
use super::plan::MySqlTablePlan;
use super::sink::MySqlTableSink;
use super::snapshot::{SnapshotOptions, run_snapshot, run_snapshot_parallel};
use crate::Result;
use crate::subscribe::SnapshotMode;
use crate::subscribe::schema::TargetOptions;
use crate::subscribe::state::{MySqlStateRecord, SubscriptionMetadata};

pub async fn run(
    config: MySqlSubscribeConfig,
    state: MySqlStateRecord,
    meta: MetaDataClientRef,
) -> Result<()> {
    if state.subscription_id != config.subscription_id {
        bail!(
            "MySQL state belongs to subscription {:?}, not {:?}",
            state.subscription_id,
            config.subscription_id
        );
    }
    let metadata = {
        let pool = meta.pooled_client();
        let pool = pool.lock().await.clone();
        let metadata = SubscriptionMetadata::from_pool(pool);
        metadata.init_schema().await?;
        metadata
    };
    let (pool, opts) = build_pool(&config.source_url).await?;
    let table_names = config.source_table_names();
    let source = inspect_source(
        &pool,
        &config.source_database,
        &table_names,
        config.server_id,
        config.gtid_mode,
    )
    .await?;

    let target_options = TargetOptions {
        auto_create: config.auto_create,
        schema_evolution: config.schema_evolution,
        warehouse_prefix: config.warehouse_prefix.clone(),
        cdc_column: config.cdc_column.clone(),
    };
    let mut plans = build_plans(&meta, &source, &config, &target_options).await?;
    let has_commits = plans
        .iter()
        .any(|plan| plan.has_commits || plan.target_sequence > 0);
    let target_sequence = plans
        .iter()
        .map(|plan| plan.target_sequence)
        .max()
        .unwrap_or(0);
    if state.stream_sequence > target_sequence.saturating_add(1) {
        bail!(
            "MySQL subscription metadata is ahead of the LakeSoul target \
             (metadata sequence {}, target sequence {}); refusing to resume to avoid \
             silent data loss",
            state.stream_sequence,
            target_sequence
        );
    }

    let snapshot_state = state.snapshot_state.as_str();
    let mut sequence = state.stream_sequence;
    let mut offset: MySqlOffset;

    if config.snapshot == SnapshotMode::None {
        // A preloaded target has no source watermark. Seed at the current head;
        // rows written between the external load and this point are outside the
        // subscriber's control, so the operator must quiesce writes while
        // preloading. This mirrors the documented snapshot=none contract.
        offset = state
            .stream_offset
            .as_ref()
            .map(MySqlOffset::from_json)
            .transpose()?
            .unwrap_or_else(|| {
                // The caller will immediately replace this after querying the
                // master status below.
                MySqlOffset {
                    gtid: String::new(),
                    binlog_file: String::new(),
                    binlog_pos: 4,
                    sequence,
                }
            });
        if offset.binlog_file.is_empty() {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|error| report!("acquire MySQL connection: {error}"))?;
            let status = master_status(&mut conn).await?;
            offset.binlog_file = status.binlog_file;
            offset.binlog_pos = status.binlog_pos;
            offset.gtid = status.gtid;
        }
        metadata
            .set_mysql_snapshot_state(&config.subscription_id, "done")
            .await?;
        metadata
            .update_mysql_stream_offset(
                &config.subscription_id,
                &offset.to_json()?,
                sequence,
            )
            .await?;
    } else if snapshot_state == "done" {
        if !has_commits && state.snapshot_rows > 0 {
            bail!(
                "MySQL snapshot is marked done but every target table is empty; \
                 resnapshot is required"
            );
        }
        offset = state
            .stream_offset
            .as_ref()
            .map(MySqlOffset::from_json)
            .transpose()?
            .or_else(|| {
                state
                    .snapshot_boundary
                    .as_ref()
                    .map(MySqlOffset::from_json)
                    .transpose()
                    .ok()
                    .flatten()
            })
            .ok_or_else(|| {
                report!(
                    "MySQL snapshot is done but no stream offset or snapshot boundary \
                     was persisted; resnapshot is required"
                )
            })?;
        offset.sequence = sequence;
    } else if snapshot_state == "failed" {
        bail!(
            "MySQL initial snapshot failed previously; drop/recreate the target \
             and resnapshot"
        );
    } else if has_commits {
        bail!(
            "target table has commits but the MySQL initial snapshot is not complete; \
             resnapshot is required"
        );
    } else {
        let snapshot_options = SnapshotOptions {
            locking: config.snapshot_locking,
            batch_rows: config.snapshot_batch_rows,
            commit_rows: config.commit_rows,
            parallelism: config.snapshot_parallelism,
            memory_pool: config.memory_pool.clone(),
        };
        metadata
            .set_mysql_snapshot_state(&config.subscription_id, "copying")
            .await?;
        metadata
            .set_state(&config.subscription_id, "snapshotting", None)
            .await?;
        let snapshot_result = if config.snapshot_parallelism <= 1 {
            run_snapshot(&pool, &opts, &source, plans, sequence, &snapshot_options).await
        } else {
            match run_snapshot_parallel(
                &pool,
                &opts,
                &source,
                &plans,
                sequence,
                &snapshot_options,
                &meta,
                &target_options,
            )
            .await
            {
                Ok(outcome) => build_plans(&meta, &source, &config, &target_options)
                    .await
                    .map(|plans| (plans, outcome)),
                Err(error) => Err(error),
            }
        };
        let (new_plans, outcome) = match snapshot_result {
            Ok(value) => value,
            Err(error) => {
                metadata
                    .set_mysql_snapshot_state(&config.subscription_id, "failed")
                    .await?;
                return Err(error);
            }
        };
        plans = new_plans;
        sequence = outcome.sequence;
        offset = outcome.offset.clone();
        metadata
            .set_mysql_snapshot_boundary(
                &config.subscription_id,
                &outcome.offset.to_json()?,
                outcome.rows,
                "done",
            )
            .await?;
        metadata
            .update_mysql_stream_offset(
                &config.subscription_id,
                &outcome.offset.to_json()?,
                sequence,
            )
            .await?;
        tracing::info!(
            subscription = %config.subscription_id,
            rows = outcome.rows,
            sequence,
            "MySQL initial snapshot complete"
        );
    }

    let mut reader = MySqlBinlogReader::start(&opts, &source, &offset).await?;
    let mut sinks = build_sinks(plans, &config);
    metadata
        .set_state(&config.subscription_id, "streaming", None)
        .await?;
    let mut last_offset = Some(offset);
    let mut shutdown = Box::pin(super::super::runner::shutdown_signal());
    let mut interval = tokio::time::interval(config.commit_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    tracing::info!(
        subscription = %config.subscription_id,
        tables = %source
            .tables
            .iter()
            .map(|table| table.qualified_name())
            .collect::<Vec<_>>()
            .join(", "),
        sequence,
        "MySQL subscriber streaming"
    );

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("MySQL subscriber shutdown signal received");
                break;
            }
            _ = interval.tick() => {
                flush_all(
                    &mut sinks,
                    &mut sequence,
                    &mut last_offset,
                    &metadata,
                    &config.subscription_id,
                ).await?;
            }
            batch = reader.next_batch(config.commit_interval) => {
                let Some(batch) = batch? else {
                    continue;
                };
                apply_batch(&mut sinks, &batch)?;
                last_offset = Some(batch.offset.clone());
                if let Some(ddl) = batch.ddl.as_deref() {
                    if let Some((database, table)) =
                        truncate_target(ddl, &config.source_database)
                        && sinks.contains_key(&(database.clone(), table.clone()))
                    {
                        bail!(
                            "TRUNCATE on mapped table {}.{} is not supported by the \
                             P0 subscriber",
                            database,
                            table
                        );
                    }
                    flush_all(
                        &mut sinks,
                        &mut sequence,
                        &mut last_offset,
                        &metadata,
                        &config.subscription_id,
                    ).await?;
                    handle_ddl(
                        ddl,
                        &mut reader,
                        &mut sinks,
                        &mut sequence,
                        &mut last_offset,
                        &metadata,
                        &config.subscription_id,
                        &pool,
                        &config,
                        &target_options,
                        &meta,
                    ).await?;
                    continue;
                }
                if sinks.values().any(|sink| sink.should_flush(config.commit_rows)) {
                    flush_all(
                        &mut sinks,
                        &mut sequence,
                        &mut last_offset,
                        &metadata,
                        &config.subscription_id,
                    ).await?;
                } else if batch.changes.is_empty() {
                    // A transaction touched only unmapped tables. Its position
                    // is safe to advance because it produced no target rows.
                    flush_all(
                        &mut sinks,
                        &mut sequence,
                        &mut last_offset,
                        &metadata,
                        &config.subscription_id,
                    ).await?;
                }
            }
        }
    }

    flush_all(
        &mut sinks,
        &mut sequence,
        &mut last_offset,
        &metadata,
        &config.subscription_id,
    )
    .await?;
    metadata
        .set_state(&config.subscription_id, "stopped", None)
        .await?;
    Ok(())
}

async fn build_plans(
    meta: &MetaDataClientRef,
    source: &MySqlSource,
    config: &MySqlSubscribeConfig,
    target_options: &TargetOptions,
) -> Result<Vec<MySqlTablePlan>> {
    let mut plans = Vec::with_capacity(config.tables.len());
    for mapping in &config.tables {
        if mapping.source_schema != config.source_database {
            bail!(
                "MySQL subscription source table {}.{} is outside database {:?}",
                mapping.source_schema,
                mapping.source_table,
                config.source_database
            );
        }
        let table = source
            .tables
            .iter()
            .find(|table| table.name == mapping.source_table)
            .cloned()
            .ok_or_else(|| {
                report!(
                    "source table {}.{} was not found",
                    config.source_database,
                    mapping.source_table
                )
            })?;
        if !mapping.primary_key.is_empty() {
            let mut expected = mapping.primary_key.clone();
            let mut actual = table.primary_key.clone();
            expected.sort();
            actual.sort();
            if expected != actual {
                bail!(
                    "configured primary key {:?} does not match MySQL primary key {:?} \
                     for {}.{}",
                    mapping.primary_key,
                    table.primary_key,
                    config.source_database,
                    mapping.source_table
                );
            }
        }
        plans.push(
            MySqlTablePlan::new(
                meta,
                table,
                &mapping.target_namespace,
                &mapping.target_table,
                target_options,
                config.auto_create,
                true,
            )
            .await?,
        );
    }
    Ok(plans)
}

fn build_sinks(
    plans: Vec<MySqlTablePlan>,
    config: &MySqlSubscribeConfig,
) -> HashMap<(String, String), MySqlTableSink> {
    plans
        .into_iter()
        .map(|plan| {
            let key = (plan.source.database.clone(), plan.source.name.clone());
            (key, MySqlTableSink::new(plan, config.memory_pool.clone()))
        })
        .collect()
}

fn apply_batch(
    sinks: &mut HashMap<(String, String), MySqlTableSink>,
    batch: &MySqlBatch,
) -> Result<()> {
    for change in &batch.changes {
        match change {
            MySqlChange::Insert { table, values } => {
                let sink = sink_for(sinks, table)?;
                sink.apply_insert(values.clone())?;
            }
            MySqlChange::Update {
                table,
                before,
                after,
            } => {
                let sink = sink_for(sinks, table)?;
                sink.apply_update(before.clone(), after.clone())?;
            }
            MySqlChange::Delete { table, before } => {
                let sink = sink_for(sinks, table)?;
                sink.apply_delete(before.clone())?;
            }
        }
    }
    Ok(())
}

fn sink_for<'a>(
    sinks: &'a mut HashMap<(String, String), MySqlTableSink>,
    table: &TableId,
) -> Result<&'a mut MySqlTableSink> {
    sinks
        .get_mut(&(table.database.clone(), table.table.clone()))
        .ok_or_else(|| {
            report!("MySQL change for unmapped table {}", table.qualified_name())
        })
}

async fn flush_all(
    sinks: &mut HashMap<(String, String), MySqlTableSink>,
    sequence: &mut u64,
    last_offset: &mut Option<MySqlOffset>,
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
) -> Result<()> {
    for sink in sinks.values_mut() {
        if sink.is_empty() {
            continue;
        }
        let current = *sequence;
        sink.flush(current).await?;
        *sequence = sequence.saturating_add(1);
    }
    if let Some(offset) = last_offset.as_mut() {
        offset.sequence = *sequence;
        metadata
            .update_mysql_stream_offset(subscription_id, &offset.to_json()?, *sequence)
            .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_truncate_targets() {
        assert_eq!(
            truncate_target("TRUNCATE TABLE `app`.`t1`", "app"),
            Some(("app".to_string(), "t1".to_string()))
        );
        assert_eq!(
            truncate_target("truncate t2;", "app"),
            Some(("app".to_string(), "t2".to_string()))
        );
        assert_eq!(truncate_target("UPDATE t1 SET v = 1", "app"), None);
    }
}

/// Parse the target of `TRUNCATE [TABLE] [db.]table`. Returning `None` means
/// the statement is not a truncate; a truncate we cannot parse is rejected
/// conservatively by the caller.
fn truncate_target(statement: &str, default_database: &str) -> Option<(String, String)> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("TRUNCATE") {
        return None;
    }
    let rest = trimmed["TRUNCATE".len()..].trim_start();
    let rest = if rest
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("TABLE"))
        && rest
            .get(5..)
            .and_then(|suffix| suffix.chars().next())
            .is_some_and(char::is_whitespace)
    {
        rest[5..].trim_start()
    } else {
        rest
    };
    let target = rest.split_whitespace().next()?.trim_matches('`');
    if target.is_empty() {
        return Some((default_database.to_string(), String::new()));
    }
    let (database, table) = target
        .rsplit_once('.')
        .map(|(database, table)| (database.trim_matches('`'), table.trim_matches('`')))
        .unwrap_or((default_database, target));
    Some((database.to_string(), table.to_string()))
}

#[allow(clippy::too_many_arguments)]
async fn handle_ddl(
    statement: &str,
    reader: &mut MySqlBinlogReader,
    sinks: &mut HashMap<(String, String), MySqlTableSink>,
    sequence: &mut u64,
    last_offset: &mut Option<MySqlOffset>,
    metadata: &SubscriptionMetadata,
    subscription_id: &str,
    pool: &mysql_async::Pool,
    config: &MySqlSubscribeConfig,
    target_options: &TargetOptions,
    meta: &MetaDataClientRef,
) -> Result<()> {
    // Unsupported DDL is rejected by the caller before any checkpoint moves.
    tracing::info!(statement, "refreshing MySQL schema after DDL");
    // Re-read the catalog and rebuild target plans. This supports CREATE,
    // ADD COLUMN, safe widening and relaxing NOT NULL without a DDL parser.
    let table_names: Vec<String> = sinks
        .values()
        .map(|sink| sink.plan.source.name.clone())
        .collect();
    let refreshed = inspect_source(
        pool,
        &config.source_database,
        &table_names,
        config.server_id,
        config.gtid_mode,
    )
    .await?;
    reader.replace_tables(&refreshed.tables);
    let mut rebuilt = HashMap::new();
    for (key, old_sink) in sinks.drain() {
        let source_table = refreshed
            .tables
            .iter()
            .find(|table| table.name == key.1)
            .cloned()
            .ok_or_else(|| {
                report!(
                    "source table {}.{} disappeared after DDL; DROP/RENAME is not \
                     supported",
                    key.0,
                    key.1
                )
            })?;
        let plan = MySqlTablePlan::new(
            meta,
            source_table,
            &old_sink.plan.target_namespace,
            &old_sink.plan.target_table,
            target_options,
            false,
            true,
        )
        .await?;
        rebuilt.insert(key, MySqlTableSink::new(plan, config.memory_pool.clone()));
    }
    *sinks = rebuilt;
    if let Some(offset) = last_offset.as_mut() {
        offset.sequence = *sequence;
        metadata
            .update_mysql_stream_offset(subscription_id, &offset.to_json()?, *sequence)
            .await?;
    }
    Ok(())
}
