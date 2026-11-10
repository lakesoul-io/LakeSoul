// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Initial consistent snapshot for MySQL subscriptions.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::execution::memory_pool::MemoryPool;
use futures::future::try_join_all;
use lakesoul_io::writer::async_writer::FlushOutput;
use lakesoul_metadata::MetaDataClientRef;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Params, Pool, Row};
use mysql_common::value::Value;
use rootcause::{bail, report};

use super::binlog::snapshot_select;
use super::catalog::{
    MySqlOffset, MySqlSource, MySqlTable, current_gtid_executed, master_status,
    qualified_table, quote_ident,
};
use super::plan::MySqlTablePlan;
use super::sink::{MySqlTableSink, values_from_snapshot_row};
use super::types::MySqlType;
use crate::Result;
use crate::subscribe::plan::commit_id_for_mysql;
use crate::subscribe::schema::TargetOptions;
use crate::subscribe::sink::commit_outputs_with_id;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotLocking {
    /// Record the position, then open a consistent snapshot. Changes between
    /// the two are replayed from the binlog and converge by primary key.
    None,
    /// Hold `FLUSH TABLES WITH READ LOCK` while recording the position and
    /// opening the snapshot transaction. Requires RELOAD.
    Flush,
}

impl SnapshotLocking {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "flush" => Some(Self::Flush),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotOptions {
    pub locking: SnapshotLocking,
    pub batch_rows: usize,
    pub commit_rows: usize,
    pub parallelism: usize,
    pub memory_pool: Arc<dyn MemoryPool>,
}

#[derive(Debug, Clone)]
pub struct SnapshotOutcome {
    pub offset: MySqlOffset,
    pub rows: u64,
    pub sequence: u64,
}

pub async fn run_snapshot(
    pool: &Pool,
    opts: &Opts,
    source: &MySqlSource,
    plans: Vec<MySqlTablePlan>,
    mut sequence: u64,
    options: &SnapshotOptions,
) -> Result<(Vec<MySqlTablePlan>, SnapshotOutcome)> {
    for plan in &plans {
        if plan.has_commits || plan.target_sequence != 0 {
            bail!(
                "target table {} is not empty; an initial snapshot requires an \
                 empty target (use snapshot = 'none' for a preloaded table)",
                plan.target_name()
            );
        }
    }

    let boundary = match options.locking {
        SnapshotLocking::Flush => {
            let mut lock_conn = pool
                .get_conn()
                .await
                .map_err(|error| report!("acquire MySQL lock connection: {error}"))?;
            lock_conn
                .query_drop("SET SESSION lock_wait_timeout = 10")
                .await
                .map_err(|error| report!("set MySQL lock wait timeout: {error}"))?;
            lock_conn
                .query_drop("FLUSH TABLES WITH READ LOCK")
                .await
                .map_err(|error| {
                    report!(
                        "acquire MySQL read lock for snapshot (needs RELOAD): {error}"
                    )
                })?;
            let boundary = snapshot_boundary(&mut lock_conn).await;
            let boundary = match boundary {
                Ok(boundary) => boundary,
                Err(error) => {
                    let _ = lock_conn.query_drop("UNLOCK TABLES").await;
                    return Err(error);
                }
            };
            (boundary, Some(lock_conn))
        }
        SnapshotLocking::None => {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|error| report!("acquire MySQL snapshot connection: {error}"))?;
            let boundary = snapshot_boundary(&mut conn).await?;
            (boundary, None)
        }
    };

    let (boundary, mut lock_conn) = boundary;
    let mut snapshot_conn = Conn::new(opts.clone())
        .await
        .map_err(|error| report!("connect to source MySQL for snapshot: {error}"))?;
    snapshot_conn
        .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .map_err(|error| report!("set snapshot isolation: {error}"))?;
    snapshot_conn
        .query_drop("SET time_zone = '+00:00'")
        .await
        .map_err(|error| report!("set snapshot time zone: {error}"))?;
    snapshot_conn
        .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(|error| report!("start MySQL consistent snapshot: {error}"))?;

    // In flush mode the read view must be pinned while writes are still
    // blocked. A trivial read on each table establishes it in InnoDB.
    if options.locking == SnapshotLocking::Flush {
        for plan in &plans {
            let query = format!(
                "SELECT 1 FROM {} LIMIT 1",
                qualified_table(&source.database, &plan.source.name)
            );
            let _ = snapshot_conn.query_drop(query).await.map_err(|error| {
                report!(
                    "pin MySQL snapshot read view for {}: {error}",
                    plan.source_name()
                )
            });
        }
        if let Some(lock_conn) = lock_conn.as_mut() {
            lock_conn
                .query_drop("UNLOCK TABLES")
                .await
                .map_err(|error| report!("release MySQL snapshot lock: {error}"))?;
        }
    }
    drop(lock_conn);

    let mut total_rows = 0u64;
    let mut completed = Vec::with_capacity(plans.len());
    let snapshot_result: Result<()> = async {
        for plan in plans {
            let start_sequence = sequence;
            let mut sink = MySqlTableSink::new(plan, Arc::clone(&options.memory_pool));
            let mut outputs: Vec<FlushOutput> = Vec::new();
            let rows = snapshot_table(
                &mut snapshot_conn,
                &source.database,
                &mut sink,
                options,
                &mut outputs,
            )
            .await?;
            total_rows = total_rows.saturating_add(rows);
            if !outputs.is_empty() {
                commit_outputs_with_id(
                    &sink.plan.table,
                    &sink.plan.target_table,
                    std::mem::take(&mut outputs),
                    commit_id_for_mysql(sequence),
                    usize::try_from(rows).unwrap_or(usize::MAX),
                    Some(sequence),
                )
                .await?;
                sequence = sequence.saturating_add(1);
            }
            if sequence > start_sequence {
                sink.plan.target_sequence = sequence.saturating_sub(1);
                sink.plan.has_commits = true;
            } else {
                sink.plan.target_sequence = sequence;
                sink.plan.has_commits = false;
            }
            completed.push(sink.plan);
        }
        Ok(())
    }
    .await;

    match snapshot_result {
        Ok(()) => {
            let _ = snapshot_conn.query_drop("COMMIT").await;
            Ok((
                completed,
                SnapshotOutcome {
                    offset: MySqlOffset {
                        gtid: boundary.gtid,
                        binlog_file: boundary.binlog_file,
                        binlog_pos: boundary.binlog_pos,
                        sequence,
                    },
                    rows: total_rows,
                    sequence,
                },
            ))
        }
        Err(error) => {
            let _ = snapshot_conn.query_drop("ROLLBACK").await;
            Err(error)
        }
    }
}

#[derive(Clone, Debug)]
struct NumericRange {
    lower: Value,
    upper: Option<Value>,
}

#[derive(Clone, Debug)]
struct WorkUnit {
    source: MySqlTable,
    target_namespace: String,
    target_table: String,
    range: Option<NumericRange>,
}

/// Parallel initial snapshot. Each worker connection has its own consistent
/// read view; the binlog stream starts at `boundary` (recorded before the
/// views), so any difference between views is replayed and converges by
/// primary key.
#[allow(clippy::too_many_arguments)]
pub async fn run_snapshot_parallel(
    pool: &Pool,
    opts: &Opts,
    source: &MySqlSource,
    plans: &[MySqlTablePlan],
    sequence: u64,
    options: &SnapshotOptions,
    meta: &MetaDataClientRef,
    target_options: &TargetOptions,
) -> Result<SnapshotOutcome> {
    for plan in plans {
        if plan.has_commits || plan.target_sequence != 0 {
            bail!(
                "target table {} is not empty; an initial snapshot requires an \
                 empty target (use snapshot = 'none' for a preloaded table)",
                plan.target_name()
            );
        }
    }
    let parallelism = options.parallelism.max(1);
    let (boundary, connections) =
        begin_snapshot_connections(pool, opts, source, options.locking, parallelism)
            .await?;

    let mut catalog_conn = pool
        .get_conn()
        .await
        .map_err(|error| report!("acquire MySQL snapshot catalog connection: {error}"))?;
    let mut units = Vec::new();
    for plan in plans {
        units.extend(build_work_units(&mut catalog_conn, plan, parallelism).await?);
    }
    drop(catalog_conn);
    if units.is_empty() {
        bail!("MySQL snapshot produced no work units");
    }

    let queue = Arc::new(Mutex::new(VecDeque::from(units)));
    let next_sequence = Arc::new(AtomicU64::new(sequence));
    let total_rows = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::with_capacity(connections.len());
    for mut conn in connections {
        let queue = Arc::clone(&queue);
        let next_sequence = Arc::clone(&next_sequence);
        let total_rows = Arc::clone(&total_rows);
        let meta = meta.clone();
        let options = options.clone();
        let target_options = target_options.clone();
        workers.push(async move {
            loop {
                let unit = {
                    let mut queue = queue
                        .lock()
                        .map_err(|_| report!("MySQL snapshot work queue poisoned"))?;
                    queue.pop_front()
                };
                let Some(unit) = unit else {
                    break;
                };
                let rows = snapshot_work_unit(
                    &mut conn,
                    &unit,
                    &meta,
                    &target_options,
                    &options,
                    &next_sequence,
                )
                .await?;
                total_rows.fetch_add(rows, Ordering::SeqCst);
            }
            let _ = conn.query_drop("COMMIT").await;
            Ok::<(), rootcause::Report>(())
        });
    }
    try_join_all(workers).await?;

    let sequence = next_sequence.load(Ordering::SeqCst);
    Ok(SnapshotOutcome {
        offset: MySqlOffset {
            gtid: boundary.gtid,
            binlog_file: boundary.binlog_file,
            binlog_pos: boundary.binlog_pos,
            sequence,
        },
        rows: total_rows.load(Ordering::SeqCst),
        sequence,
    })
}

async fn begin_snapshot_connections(
    pool: &Pool,
    opts: &Opts,
    source: &MySqlSource,
    locking: SnapshotLocking,
    parallelism: usize,
) -> Result<(MySqlOffset, Vec<Conn>)> {
    let (boundary, mut lock_conn) = match locking {
        SnapshotLocking::Flush => {
            let mut lock_conn = pool
                .get_conn()
                .await
                .map_err(|error| report!("acquire MySQL lock connection: {error}"))?;
            lock_conn
                .query_drop("SET SESSION lock_wait_timeout = 10")
                .await
                .map_err(|error| report!("set MySQL lock wait timeout: {error}"))?;
            lock_conn
                .query_drop("FLUSH TABLES WITH READ LOCK")
                .await
                .map_err(|error| {
                    report!(
                        "acquire MySQL read lock for snapshot (needs RELOAD): {error}"
                    )
                })?;
            let boundary = snapshot_boundary(&mut lock_conn).await;
            let boundary = match boundary {
                Ok(boundary) => boundary,
                Err(error) => {
                    let _ = lock_conn.query_drop("UNLOCK TABLES").await;
                    return Err(error);
                }
            };
            (boundary, Some(lock_conn))
        }
        SnapshotLocking::None => {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|error| report!("acquire MySQL snapshot connection: {error}"))?;
            let boundary = snapshot_boundary(&mut conn).await?;
            (boundary, None)
        }
    };

    let pin_table = source
        .tables
        .first()
        .map(|table| qualified_table(&source.database, &table.name))
        .ok_or_else(|| report!("MySQL snapshot has no source table"))?;
    let mut connections = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let mut conn = Conn::new(opts.clone())
            .await
            .map_err(|error| report!("connect to source MySQL for snapshot: {error}"))?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .map_err(|error| report!("set snapshot isolation: {error}"))?;
        conn.query_drop("SET time_zone = '+00:00'")
            .await
            .map_err(|error| report!("set snapshot time zone: {error}"))?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await
            .map_err(|error| report!("start MySQL consistent snapshot: {error}"))?;
        if locking == SnapshotLocking::Flush {
            conn.query_drop(format!("SELECT 1 FROM {pin_table} LIMIT 1"))
                .await
                .map_err(|error| report!("pin MySQL snapshot read view: {error}"))?;
        }
        connections.push(conn);
    }
    if let Some(lock_conn) = lock_conn.as_mut() {
        lock_conn
            .query_drop("UNLOCK TABLES")
            .await
            .map_err(|error| report!("release MySQL snapshot lock: {error}"))?;
    }
    drop(lock_conn);
    Ok((boundary, connections))
}

#[allow(clippy::too_many_arguments)]
async fn snapshot_work_unit(
    conn: &mut Conn,
    unit: &WorkUnit,
    meta: &MetaDataClientRef,
    target_options: &TargetOptions,
    options: &SnapshotOptions,
    next_sequence: &AtomicU64,
) -> Result<u64> {
    let plan = MySqlTablePlan::new(
        meta,
        unit.source.clone(),
        &unit.target_namespace,
        &unit.target_table,
        target_options,
        false,
        true,
    )
    .await?;
    let mut sink = MySqlTableSink::new(plan, Arc::clone(&options.memory_pool));
    let mut outputs: Vec<FlushOutput> = Vec::new();
    let rows = snapshot_table_range(
        conn,
        &unit.source,
        &mut sink,
        options,
        unit.range.as_ref(),
        &mut outputs,
    )
    .await?;
    if outputs.is_empty() {
        return Ok(0);
    }
    let sequence = next_sequence.fetch_add(1, Ordering::SeqCst);
    commit_outputs_with_id(
        &sink.plan.table,
        &sink.plan.target_table,
        outputs,
        commit_id_for_mysql(sequence),
        usize::try_from(rows).unwrap_or(usize::MAX),
        Some(sequence),
    )
    .await?;
    tracing::info!(
        target = %sink.plan.target_name(),
        rows,
        sequence,
        range = ?unit.range,
        "published MySQL snapshot range"
    );
    Ok(rows)
}

async fn snapshot_table_range(
    conn: &mut Conn,
    table: &MySqlTable,
    sink: &mut MySqlTableSink,
    options: &SnapshotOptions,
    range: Option<&NumericRange>,
    outputs: &mut Vec<FlushOutput>,
) -> Result<u64> {
    let Some(range) = range else {
        return snapshot_table(conn, &table.database, sink, options, outputs).await;
    };
    if table.primary_key.len() != 1 {
        bail!(
            "numeric snapshot ranges require a single primary key on {}",
            table.qualified_name()
        );
    }
    let key = &table.primary_key[0];
    let order = quote_ident(key);
    let base = snapshot_select(&table.database, table);
    let key_index = table
        .columns
        .iter()
        .position(|column| &column.name == key)
        .expect("validated primary key");
    let mut cursor: Option<Value> = None;
    let mut total = 0u64;
    loop {
        let mut conditions = vec![format!("{order} >= ?")];
        let mut params = vec![range.lower.clone()];
        if let Some(upper) = &range.upper {
            conditions.push(format!("{order} < ?"));
            params.push(upper.clone());
        }
        if let Some(cursor) = &cursor {
            conditions.push(format!("{order} > ?"));
            params.push(cursor.clone());
        }
        params.push(Value::Int(
            i64::try_from(options.batch_rows)
                .map_err(|_| report!("snapshot_batch_rows is too large"))?,
        ));
        let query = format!(
            "{base} WHERE {} ORDER BY {order} LIMIT ?",
            conditions.join(" AND ")
        );
        let mut result = conn
            .exec_iter(query, Params::Positional(params))
            .await
            .map_err(|error| {
                report!(
                    "read MySQL snapshot range for {}: {error}",
                    table.qualified_name()
                )
            })?;
        let mut batch_rows = 0usize;
        while let Some(row) = result
            .next()
            .await
            .map_err(|error| report!("stream MySQL snapshot rows: {error}"))?
        {
            let values = values_from_snapshot_row(table, &row)?;
            sink.apply_insert(values)?;
            total = total.saturating_add(1);
            batch_rows += 1;
            cursor = row.as_ref(key_index).cloned().or(Some(Value::NULL));
            if sink.should_flush(options.commit_rows) {
                outputs.extend(sink.flush_files().await?);
            }
        }
        if batch_rows < options.batch_rows {
            break;
        }
    }
    outputs.extend(sink.flush_files().await?);
    Ok(total)
}

async fn build_work_units(
    conn: &mut Conn,
    plan: &MySqlTablePlan,
    parallelism: usize,
) -> Result<Vec<WorkUnit>> {
    let table = &plan.source;
    let make = |range| WorkUnit {
        source: table.clone(),
        target_namespace: plan.target_namespace.clone(),
        target_table: plan.target_table.clone(),
        range,
    };
    if parallelism <= 1 || table.primary_key.len() != 1 {
        return Ok(vec![make(None)]);
    }
    let key = &table.primary_key[0];
    let Some(column) = table.columns.iter().find(|column| &column.name == key) else {
        return Ok(vec![make(None)]);
    };
    let Some(unsigned) = integer_type(&column.data_type) else {
        return Ok(vec![make(None)]);
    };
    let query = format!(
        "SELECT MIN({}), MAX({}) FROM {}",
        quote_ident(key),
        quote_ident(key),
        qualified_table(&table.database, &table.name)
    );
    let row: Option<Row> = conn
        .query_first(query)
        .await
        .map_err(|error| report!("read MySQL snapshot bounds: {error}"))?;
    let Some(row) = row else {
        return Ok(vec![make(None)]);
    };
    let (Some(min), Some(max)) = (
        row.as_ref(0).and_then(value_to_i128),
        row.as_ref(1).and_then(value_to_i128),
    ) else {
        return Ok(vec![make(None)]);
    };
    if max < min {
        return Ok(vec![make(None)]);
    }
    let span = max - min + 1;
    let ranges = parallelism
        .min(usize::try_from(span).unwrap_or(usize::MAX))
        .max(1);
    if ranges <= 1 {
        return Ok(vec![make(None)]);
    }
    let mut units = Vec::with_capacity(ranges);
    for index in 0..ranges {
        let lower = min
            + span * i128::try_from(index).unwrap_or(0)
                / i128::try_from(ranges).unwrap_or(1);
        let upper = if index + 1 == ranges {
            None
        } else {
            Some(i128_to_value(
                min + span * i128::try_from(index + 1).unwrap_or(0)
                    / i128::try_from(ranges).unwrap_or(1),
                unsigned,
            )?)
        };
        units.push(make(Some(NumericRange {
            lower: i128_to_value(lower, unsigned)?,
            upper,
        })));
    }
    Ok(units)
}

fn integer_type(data_type: &MySqlType) -> Option<bool> {
    match data_type {
        MySqlType::TinyInt { unsigned }
        | MySqlType::SmallInt { unsigned }
        | MySqlType::MediumInt { unsigned }
        | MySqlType::Int { unsigned }
        | MySqlType::BigInt { unsigned } => Some(*unsigned),
        _ => None,
    }
}

fn value_to_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Int(value) => Some(i128::from(*value)),
        Value::UInt(value) => Some(i128::from(*value)),
        Value::Bytes(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
        _ => None,
    }
}

fn i128_to_value(value: i128, unsigned: bool) -> Result<Value> {
    if unsigned {
        Ok(Value::UInt(u64::try_from(value).map_err(|_| {
            report!("unsigned snapshot boundary {value} exceeds u64")
        })?))
    } else {
        Ok(Value::Int(i64::try_from(value).map_err(|_| {
            report!("signed snapshot boundary {value} exceeds i64")
        })?))
    }
}

async fn snapshot_boundary(conn: &mut Conn) -> Result<MySqlOffset> {
    let status = master_status(conn).await?;
    let gtid = if status.gtid.trim().is_empty() {
        current_gtid_executed(conn).await?
    } else {
        status.gtid
    };
    Ok(MySqlOffset {
        gtid,
        binlog_file: status.binlog_file,
        binlog_pos: status.binlog_pos,
        sequence: 0,
    })
}

async fn snapshot_table(
    conn: &mut Conn,
    database: &str,
    sink: &mut MySqlTableSink,
    options: &SnapshotOptions,
    outputs: &mut Vec<FlushOutput>,
) -> Result<u64> {
    let table = sink.plan.source.clone();
    let primary_key = table.primary_key.clone();
    if primary_key.is_empty() {
        bail!(
            "MySQL snapshot table {} has no primary key",
            table.qualified_name()
        );
    }
    let base = snapshot_select(database, &table);
    let order = primary_key
        .iter()
        .map(|key| quote_ident(key))
        .collect::<Vec<_>>()
        .join(", ");

    let mut cursor: Vec<Value> = Vec::new();
    let mut total = 0u64;
    loop {
        let (query, mut params) = if cursor.is_empty() {
            (
                format!("{base} ORDER BY {order} LIMIT ?"),
                Vec::with_capacity(1),
            )
        } else if primary_key.len() == 1 {
            (
                format!("{base} WHERE {} > ? ORDER BY {order} LIMIT ?", order),
                Vec::with_capacity(2),
            )
        } else {
            let placeholders = vec!["?"; primary_key.len()].join(", ");
            (
                format!(
                    "{base} WHERE ({order}) > ({placeholders}) ORDER BY {order} LIMIT ?"
                ),
                Vec::with_capacity(primary_key.len() + 1),
            )
        };
        params.extend(cursor.iter().cloned());
        params.push(Value::Int(
            i64::try_from(options.batch_rows)
                .map_err(|_| report!("snapshot_batch_rows is too large"))?,
        ));

        let mut result = conn
            .exec_iter(query, Params::Positional(params))
            .await
            .map_err(|error| {
                report!(
                    "read MySQL snapshot rows for {}: {error}",
                    table.qualified_name()
                )
            })?;
        let mut batch_rows = 0usize;
        while let Some(row) = result
            .next()
            .await
            .map_err(|error| report!("stream MySQL snapshot rows: {error}"))?
        {
            let values = values_from_snapshot_row(&table, &row)?;
            sink.apply_insert(values)?;
            total = total.saturating_add(1);
            batch_rows += 1;
            cursor = primary_key
                .iter()
                .map(|key| {
                    let index = table
                        .columns
                        .iter()
                        .position(|column| column.name == *key)
                        .expect("validated primary key");
                    row.as_ref(index).cloned().unwrap_or(Value::NULL)
                })
                .collect();
            if sink.should_flush(options.commit_rows) {
                outputs.extend(sink.flush_files().await?);
            }
        }
        if batch_rows < options.batch_rows {
            break;
        }
    }
    outputs.extend(sink.flush_files().await?);
    Ok(total)
}
