// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! MySQL binlog reader: transaction grouping and Arrow value decoding.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::time::Duration;

use datafusion::common::ScalarValue;
use futures::StreamExt;
use mysql_async::prelude::*;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, Opts};
use mysql_common::binlog::events::{EventData, RowsEventData};
use mysql_common::binlog::row::BinlogRow;
use mysql_common::packets::Sid;
use rootcause::{bail, report};

use super::catalog::{
    MySqlOffset, MySqlSource, MySqlTable, gtid_purged, qualified_table, quote_ident,
};
use super::types;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId {
    pub database: String,
    pub table: String,
}

impl TableId {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }
}

#[derive(Debug, Clone)]
pub enum MySqlChange {
    Insert {
        table: TableId,
        values: Vec<ScalarValue>,
    },
    Update {
        table: TableId,
        before: Vec<ScalarValue>,
        after: Vec<ScalarValue>,
    },
    Delete {
        table: TableId,
        before: Vec<ScalarValue>,
    },
}

#[derive(Debug, Clone)]
pub struct MySqlBatch {
    pub changes: Vec<MySqlChange>,
    pub offset: MySqlOffset,
    /// DDL that must be applied after `changes`. DDL implicitly commits, so it
    /// is always a batch boundary.
    pub ddl: Option<String>,
}

pub struct MySqlBinlogReader {
    stream: BinlogStream,
    tables: HashMap<(String, String), MySqlTable>,
    binlog_file: String,
    binlog_pos: u32,
    gtid_set: GtidSet,
    active_gtid: Option<String>,
    tx: Vec<MySqlChange>,
    tx_open: bool,
}

impl MySqlBinlogReader {
    pub async fn start(
        opts: &Opts,
        source: &MySqlSource,
        offset: &MySqlOffset,
    ) -> Result<Self> {
        let mut conn = Conn::new(opts.clone())
            .await
            .map_err(|error| report!("connect to source MySQL for binlog: {error}"))?;

        if source.gtid_enabled && !offset.gtid.trim().is_empty() {
            let purged = gtid_purged(&mut conn).await?;
            if !purged.is_empty() {
                let subset: Option<i64> = conn
                    .exec_first("SELECT GTID_SUBSET(?, ?)", (&purged, &offset.gtid))
                    .await
                    .map_err(|error| {
                        report!("verify MySQL GTID resume position: {error}")
                    })?;
                if subset != Some(1) {
                    let missing: Option<String> = conn
                        .exec_first("SELECT GTID_SUBTRACT(?, ?)", (&purged, &offset.gtid))
                        .await
                        .ok()
                        .flatten();
                    bail!(
                        "MySQL binlog no longer contains the subscription position; \
                         purged GTIDs missing from the checkpoint: {}. Resnapshot is \
                         required.",
                        missing.as_deref().unwrap_or("<unknown>")
                    );
                }
            }
        } else {
            let logs: Vec<mysql_async::Row> = conn
                .query("SHOW BINARY LOGS")
                .await
                .map_err(|error| report!("list MySQL binary logs: {error}"))?;
            let files: Vec<String> = logs
                .iter()
                .map(|row| row.get::<String, _>(0).unwrap_or_default())
                .collect();
            if !files.iter().any(|file| file == &offset.binlog_file) {
                bail!(
                    "MySQL binlog file {:?} is no longer available; retained files: {:?}. \
                     Resnapshot is required.",
                    offset.binlog_file,
                    files
                );
            }
        }

        conn.query_drop("SET @master_heartbeat_period = 1000000000")
            .await
            .map_err(|error| report!("set MySQL heartbeat period: {error}"))?;

        let mut request = BinlogStreamRequest::new(source.server_id);
        if source.gtid_enabled && !offset.gtid.trim().is_empty() {
            let sids = parse_gtid_sids(&offset.gtid)?;
            request = request.with_gtid().with_gtid_set(sids);
        } else {
            request = request
                .with_filename(offset.binlog_file.as_bytes())
                .with_pos(u64::from(offset.binlog_pos));
        }
        let stream = conn.get_binlog_stream(request).await.map_err(|error| {
            report!(
                "start MySQL binlog stream at {}:{}: {error}",
                offset.binlog_file,
                offset.binlog_pos
            )
        })?;

        let mut tables = HashMap::new();
        for table in &source.tables {
            tables.insert((table.database.clone(), table.name.clone()), table.clone());
        }

        Ok(Self {
            stream,
            tables,
            binlog_file: offset.binlog_file.clone(),
            binlog_pos: offset.binlog_pos,
            gtid_set: GtidSet::parse(&offset.gtid)?,
            active_gtid: None,
            tx: Vec::new(),
            tx_open: false,
        })
    }

    /// Refresh the table catalog after a DDL. The binlog reader uses the
    /// catalog to decode row images; a stale seven-column table against an
    /// eight-column row is a hard error, not something to paper over.
    pub fn replace_tables(&mut self, tables: &[MySqlTable]) {
        self.tables = tables
            .iter()
            .map(|table| ((table.database.clone(), table.name.clone()), table.clone()))
            .collect();
    }

    pub fn current_offset(&self, sequence: u64) -> MySqlOffset {
        MySqlOffset {
            gtid: self.gtid_set.to_string(),
            binlog_file: self.binlog_file.clone(),
            binlog_pos: self.binlog_pos,
            sequence,
        }
    }

    /// Read until one transaction boundary or DDL. Returns `None` when the
    /// timeout expires with no complete batch.
    pub async fn next_batch(&mut self, timeout: Duration) -> Result<Option<MySqlBatch>> {
        loop {
            let next = tokio::time::timeout(timeout, self.stream.next()).await;
            let event = match next {
                Err(_) => return Ok(None),
                Ok(None) => bail!("MySQL binlog stream closed unexpectedly"),
                Ok(Some(event)) => {
                    event.map_err(|error| report!("read MySQL binlog event: {error}"))?
                }
            };

            let header = event.header();
            if header.log_pos() > 0 {
                self.binlog_pos = header.log_pos();
            }
            let timestamp_ms = u64::from(header.timestamp()) * 1_000;
            let Some(data) = event
                .read_data()
                .map_err(|error| report!("decode MySQL binlog event: {error}"))?
            else {
                continue;
            };

            if let Some(batch) = self.handle_event(data, timestamp_ms)? {
                return Ok(Some(batch));
            }
        }
    }

    fn handle_event(
        &mut self,
        data: EventData<'_>,
        timestamp_ms: u64,
    ) -> Result<Option<MySqlBatch>> {
        match data {
            EventData::GtidEvent(gtid) => {
                if gtid.is_tagged() {
                    bail!(
                        "MySQL 8.4 tagged GTIDs are not supported by the P0 \
                         subscriber; disable tagged GTIDs on the source"
                    );
                }
                let uuid = uuid::Uuid::from_bytes(gtid.sid()).to_string();
                self.active_gtid = Some(format!("{uuid}:{}", gtid.gno()));
            }
            EventData::AnonymousGtidEvent(_) => {
                self.active_gtid = None;
            }
            EventData::QueryEvent(query) => {
                let statement = query.query().to_string();
                let trimmed = statement.trim();
                if trimmed.eq_ignore_ascii_case("BEGIN") {
                    if !self.tx.is_empty() {
                        bail!("MySQL BEGIN while a transaction is already buffered");
                    }
                    self.tx_open = true;
                } else if trimmed.eq_ignore_ascii_case("COMMIT") {
                    return Ok(Some(self.finish_transaction(None)));
                } else if is_ddl(trimmed) {
                    // DDL implicitly commits. Deliver buffered row changes first,
                    // then the DDL in the same batch.
                    let mut batch = self.finish_transaction(Some(trimmed.to_string()));
                    batch.ddl = Some(trimmed.to_string());
                    return Ok(Some(batch));
                }
            }
            EventData::RowsEvent(rows) => {
                let changes = decode_rows(&self.stream, &self.tables, &rows)?;
                if !self.tx_open {
                    self.tx_open = true;
                }
                self.tx.extend(changes);
            }
            EventData::XidEvent(_) => {
                return Ok(Some(self.finish_transaction(None)));
            }
            EventData::RotateEvent(rotate) => {
                self.binlog_file = sanitize_binlog_file_name(&rotate.name())?;
                self.binlog_pos = u32::try_from(rotate.position())
                    .map_err(|_| report!("MySQL rotate position exceeds u32"))?;
            }
            EventData::IncidentEvent(_) => {
                bail!(
                    "MySQL binlog contains an incident event; events may have been \
                     lost. Resnapshot is required."
                );
            }
            EventData::HeartbeatEvent => {}
            EventData::PreviousGtidsEvent(_) => {}
            _ => {}
        }
        let _ = timestamp_ms;
        Ok(None)
    }

    fn finish_transaction(&mut self, ddl: Option<String>) -> MySqlBatch {
        let changes = std::mem::take(&mut self.tx);
        self.tx_open = false;
        if let Some(gtid) = self.active_gtid.take() {
            self.gtid_set.add_gtid(&gtid);
        }
        let mut offset = self.current_offset(0);
        // DDL is committed with the transaction's GTID; the caller advances the
        // sequence after the target commit.
        offset.binlog_file = self.binlog_file.clone();
        offset.binlog_pos = self.binlog_pos;
        MySqlBatch {
            changes,
            offset,
            ddl,
        }
    }
}

fn is_ddl(statement: &str) -> bool {
    let upper = statement.trim_start().to_ascii_uppercase();
    upper.starts_with("CREATE ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("DROP ")
        || upper.starts_with("TRUNCATE ")
        || upper.starts_with("RENAME ")
}

fn decode_rows(
    stream: &BinlogStream,
    tables: &HashMap<(String, String), MySqlTable>,
    rows: &RowsEventData<'_>,
) -> Result<Vec<MySqlChange>> {
    let table_map = stream.get_tme(rows.table_id()).ok_or_else(|| {
        report!(
            "MySQL rows event has no table map for table_id {}",
            rows.table_id()
        )
    })?;
    let database = table_map.database_name().to_string();
    let table_name = table_map.table_name().to_string();
    // The binlog carries every table in the server/database. A subscription is
    // usually a subset, so unmapped tables are skipped rather than treated as
    // an error. Their transactions still advance the position.
    let Some(table) = tables.get(&(database.clone(), table_name.clone())) else {
        return Ok(Vec::new());
    };

    let is_insert = matches!(
        rows,
        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_)
    );
    let is_update = matches!(
        rows,
        RowsEventData::UpdateRowsEventV1(_)
            | RowsEventData::UpdateRowsEvent(_)
            | RowsEventData::PartialUpdateRowsEvent(_)
    );
    let is_delete = matches!(
        rows,
        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_)
    );
    if matches!(rows, RowsEventData::PartialUpdateRowsEvent(_)) {
        bail!(
            "MySQL partial JSON updates are not supported; clear \
             binlog_row_value_options on the source"
        );
    }

    let table_id = TableId::new(database, table_name);
    let mut changes = Vec::new();
    for row in rows.rows(table_map) {
        let (before, after) = row.map_err(|error| {
            report!(
                "decode MySQL row for {}: {error}",
                table_id.qualified_name()
            )
        })?;
        if is_insert {
            let Some(after) = after else {
                bail!("MySQL INSERT row has no after image");
            };
            changes.push(MySqlChange::Insert {
                table: table_id.clone(),
                values: row_values(table, &after)?,
            });
        } else if is_update {
            let (Some(before), Some(after)) = (before, after) else {
                bail!("MySQL UPDATE row is missing a before or after image");
            };
            changes.push(MySqlChange::Update {
                table: table_id.clone(),
                before: row_values(table, &before)?,
                after: row_values(table, &after)?,
            });
        } else if is_delete {
            let Some(before) = before else {
                bail!("MySQL DELETE row has no before image");
            };
            changes.push(MySqlChange::Delete {
                table: table_id.clone(),
                before: row_values(table, &before)?,
            });
        } else {
            bail!("unsupported MySQL rows event type");
        }
    }
    Ok(changes)
}

fn row_values(table: &MySqlTable, row: &BinlogRow) -> Result<Vec<ScalarValue>> {
    let row_columns = row.columns_ref();
    let mut values = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let index = row_columns
            .iter()
            .position(|candidate| {
                candidate
                    .name_str()
                    .eq_ignore_ascii_case(column.name.as_str())
            })
            .ok_or_else(|| {
                report!(
                    "binlog row for {} is missing column {:?}; set \
                     binlog_row_metadata=FULL and binlog_row_image=FULL",
                    table.qualified_name(),
                    column.name
                )
            })?;
        let value = row.as_ref(index).ok_or_else(|| {
            report!(
                "binlog row for {} has a missing value for column {:?}",
                table.qualified_name(),
                column.name
            )
        })?;
        let target_type = column.data_type.arrow_type()?;
        values.push(types::binlog_value_to_scalar(
            value.clone().into_owned(),
            &column.data_type,
            column.charset.as_deref(),
            &target_type,
        )?);
    }
    Ok(values)
}

fn sanitize_binlog_file_name(raw: &str) -> Result<String> {
    let (base, suffix) = raw.rsplit_once('.').ok_or_else(|| {
        report!("MySQL rotate event carried an unusable binlog file name {raw:?}")
    })?;
    let digits: String = suffix.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        bail!("MySQL rotate event carried an unusable binlog file name {raw:?}");
    }
    Ok(format!("{base}.{digits}"))
}

fn parse_gtid_sids(gtid: &str) -> Result<Vec<Sid<'static>>> {
    gtid.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            Sid::from_str(part)
                .map_err(|error| report!("invalid MySQL GTID {part:?}: {error}"))
        })
        .collect()
}

/// A small, order-independent GTID set used only for checkpointing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct GtidSet {
    /// UUID -> sorted half-open intervals `[start, end)`.
    intervals: BTreeMap<String, Vec<(u64, u64)>>,
}

impl GtidSet {
    fn parse(value: &str) -> Result<Self> {
        let mut set = Self::default();
        for item in value.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let mut parts = item.split(':');
            let uuid = parts
                .next()
                .ok_or_else(|| report!("invalid GTID {item:?}"))?
                .to_ascii_lowercase();
            if uuid.is_empty() {
                bail!("invalid GTID {item:?}: empty UUID");
            }
            for interval in parts {
                let (start, end) = if let Some((start, end)) = interval.split_once('-') {
                    let start: u64 = start
                        .parse()
                        .map_err(|error| report!("invalid GTID interval: {error}"))?;
                    let end: u64 = end
                        .parse()
                        .map_err(|error| report!("invalid GTID interval: {error}"))?;
                    if start > end {
                        bail!("invalid GTID interval {interval:?}");
                    }
                    (start, end + 1)
                } else {
                    let value: u64 = interval
                        .parse()
                        .map_err(|error| report!("invalid GTID interval: {error}"))?;
                    (value, value + 1)
                };
                set.add_interval(&uuid, start, end);
            }
        }
        Ok(set)
    }

    fn add_gtid(&mut self, gtid: &str) {
        if let Some((uuid, gno)) = gtid.rsplit_once(':')
            && let Ok(gno) = gno.parse::<u64>()
        {
            self.add_interval(&uuid.to_ascii_lowercase(), gno, gno + 1);
        }
    }

    fn add_interval(&mut self, uuid: &str, start: u64, end: u64) {
        let intervals = self.intervals.entry(uuid.to_string()).or_default();
        intervals.push((start, end));
        intervals.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
        for (start, end) in intervals.drain(..) {
            if let Some(last) = merged.last_mut()
                && start <= last.1
            {
                last.1 = last.1.max(end);
            } else {
                merged.push((start, end));
            }
        }
        *intervals = merged;
    }
}

impl std::fmt::Display for GtidSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = self
            .intervals
            .iter()
            .map(|(uuid, intervals)| {
                let ranges = intervals
                    .iter()
                    .map(|(start, end)| {
                        if *end == *start + 1 {
                            start.to_string()
                        } else {
                            format!("{start}-{}", end - 1)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(":");
                format!("{uuid}:{ranges}")
            })
            .collect::<Vec<_>>()
            .join(",");
        f.write_str(&text)
    }
}

/// Build the `SELECT` projection used by snapshot readers.
pub fn snapshot_select(database: &str, table: &MySqlTable) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {columns} FROM {}",
        qualified_table(database, &table.name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gtid_set_roundtrip_and_merge() {
        let mut set = GtidSet::parse("uuid-a:1-5:10,uuid-b:1").unwrap();
        set.add_gtid("uuid-a:6");
        set.add_gtid("uuid-a:11");
        assert_eq!(set.to_string(), "uuid-a:1-6:10-11,uuid-b:1");
        let parsed = GtidSet::parse(&set.to_string()).unwrap();
        assert_eq!(parsed, set);
    }

    #[test]
    fn ddl_detection() {
        assert!(is_ddl("CREATE TABLE t (id int)"));
        assert!(is_ddl("  alter table t add column x int"));
        assert!(!is_ddl("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn rotate_name_strips_trailing_checksum() {
        assert_eq!(
            sanitize_binlog_file_name("mysql-bin.000002\u{57}\u{07}\u{03}").unwrap(),
            "mysql-bin.000002"
        );
    }
}
