// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! MySQL source catalog and startup validation.

use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Pool, Row};
use rootcause::{bail, report};
use serde::{Deserialize, Serialize};

use super::types::MySqlType;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlOffset {
    #[serde(default)]
    pub gtid: String,
    pub binlog_file: String,
    pub binlog_pos: u32,
    #[serde(default)]
    pub sequence: u64,
}

impl MySqlOffset {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|error| report!("decode MySQL offset: {error}"))
    }

    pub fn to_json(&self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| report!("encode MySQL offset: {error}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlColumn {
    pub name: String,
    pub data_type: MySqlType,
    pub nullable: bool,
    pub primary_key: bool,
    pub charset: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlTable {
    pub database: String,
    pub name: String,
    pub columns: Vec<MySqlColumn>,
    pub primary_key: Vec<String>,
}

impl MySqlTable {
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.database, self.name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GtidMode {
    Auto,
    On,
    Off,
}

impl GtidMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "on" | "true" | "1" => Some(Self::On),
            "off" | "false" | "0" => Some(Self::Off),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MySqlSource {
    pub database: String,
    pub tables: Vec<MySqlTable>,
    pub gtid_enabled: bool,
    pub server_id: u32,
}

#[derive(Debug, Clone)]
pub struct MySqlServerStatus {
    pub binlog_file: String,
    pub binlog_pos: u32,
    pub gtid: String,
}

pub async fn build_pool(url: &str) -> Result<(Pool, Opts)> {
    let opts = Opts::from_url(url)
        .map_err(|error| report!("invalid MySQL connection URL: {error}"))?;
    Ok((Pool::new(opts.clone()), opts))
}

pub async fn inspect_source(
    pool: &Pool,
    database: &str,
    tables: &[String],
    server_id: u32,
    gtid_mode: GtidMode,
) -> Result<MySqlSource> {
    if server_id == 0 {
        bail!("MySQL subscriptions require a non-zero server_id");
    }
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|error| report!("connect to source MySQL: {error}"))?;

    validate_binlog_settings(&mut conn, server_id).await?;
    let gtid_enabled = validate_gtid_mode(&mut conn, gtid_mode).await?;

    let mut inspected = Vec::with_capacity(tables.len());
    for table in tables {
        inspected.push(inspect_table(&mut conn, database, table).await?);
    }

    Ok(MySqlSource {
        database: database.to_string(),
        tables: inspected,
        gtid_enabled,
        server_id,
    })
}

async fn validate_binlog_settings(conn: &mut Conn, server_id: u32) -> Result<()> {
    let version: Option<String> = conn
        .query_first("SELECT VERSION()")
        .await
        .map_err(|error| report!("read MySQL version: {error}"))?;
    let version = version.unwrap_or_default();

    let binlog_format: Option<String> = conn
        .query_first("SELECT @@GLOBAL.binlog_format")
        .await
        .map_err(|error| report!("read binlog_format: {error}"))?;
    if !binlog_format
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("ROW"))
    {
        bail!(
            "source MySQL {version} has binlog_format={binlog_format:?}; \
             set binlog_format=ROW"
        );
    }

    let row_image: Option<String> = conn
        .query_first("SELECT @@GLOBAL.binlog_row_image")
        .await
        .map_err(|error| report!("read binlog_row_image: {error}"))?;
    if !row_image
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("FULL"))
    {
        bail!(
            "source MySQL has binlog_row_image={row_image:?}; \
             set binlog_row_image=FULL so UPDATE/DELETE carry complete rows"
        );
    }

    let row_metadata: Option<String> = conn
        .query_first("SELECT @@GLOBAL.binlog_row_metadata")
        .await
        .map_err(|error| {
            report!("read binlog_row_metadata (MySQL 8.0+ is required): {error}")
        })?;
    if !row_metadata
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("FULL"))
    {
        bail!(
            "source MySQL has binlog_row_metadata={row_metadata:?}; \
             set binlog_row_metadata=FULL so binlog events carry column names \
             and primary-key flags"
        );
    }

    let row_value_options: Option<String> = conn
        .query_first("SELECT @@GLOBAL.binlog_row_value_options")
        .await
        .map_err(|error| report!("read binlog_row_value_options: {error}"))?;
    if row_value_options
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        bail!(
            "source MySQL has binlog_row_value_options={row_value_options:?}; \
             clear it so JSON values are complete"
        );
    }

    let log_bin: Option<u8> = conn
        .query_first("SELECT @@GLOBAL.log_bin")
        .await
        .map_err(|error| report!("read log_bin: {error}"))?;
    if log_bin != Some(1) {
        bail!("source MySQL binary logging is disabled (log_bin=OFF)");
    }

    let read_only: Option<u8> = conn
        .query_first("SELECT @@GLOBAL.read_only")
        .await
        .map_err(|error| report!("read read_only: {error}"))?;
    let super_read_only: Option<u8> = conn
        .query_first("SELECT @@GLOBAL.super_read_only")
        .await
        .ok()
        .flatten();
    if read_only == Some(1) || super_read_only == Some(1) {
        bail!("source MySQL is read-only; subscribe from the primary");
    }

    let source_server_id: Option<u32> = conn
        .query_first("SELECT @@GLOBAL.server_id")
        .await
        .map_err(|error| report!("read server_id: {error}"))?;
    if source_server_id == Some(server_id) {
        bail!(
            "subscription server_id {server_id} must differ from the source \
             server's own server_id"
        );
    }

    // This query needs REPLICATION CLIENT and also proves the account can read
    // the binlog coordinate. `SHOW MASTER STATUS` is removed in MySQL 8.4.
    let _ = master_status(conn).await?;
    Ok(())
}

async fn validate_gtid_mode(conn: &mut Conn, requested: GtidMode) -> Result<bool> {
    let gtid_mode: Option<String> = conn
        .query_first("SELECT @@GLOBAL.gtid_mode")
        .await
        .ok()
        .flatten();
    let enabled = gtid_mode
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("ON"));
    match requested {
        GtidMode::On if !enabled => bail!(
            "gtid_mode=on was requested, but source MySQL has gtid_mode={gtid_mode:?}"
        ),
        _ => Ok(enabled),
    }
}

pub async fn master_status(conn: &mut Conn) -> Result<MySqlServerStatus> {
    let row = match conn.query_first::<Row, _>("SHOW BINARY LOG STATUS").await {
        Ok(row) => row,
        Err(primary_error) => conn
            .query_first::<Row, _>("SHOW MASTER STATUS")
            .await
            .map_err(|fallback_error| {
                report!(
                    "read MySQL binary log status (SHOW BINARY LOG STATUS: \
                     {primary_error}; SHOW MASTER STATUS: {fallback_error})"
                )
            })?,
    }
    .ok_or_else(|| {
        report!("MySQL binary log status returned no row; is log_bin enabled?")
    })?;

    let binlog_file: String = row.get(0).unwrap_or_default();
    let binlog_pos: u64 = row.get(1).unwrap_or(4);
    let gtid: String = row
        .get::<Option<String>, _>(4)
        .flatten()
        .unwrap_or_default();
    if binlog_file.is_empty() {
        bail!("MySQL binary log status has an empty file name");
    }
    Ok(MySqlServerStatus {
        binlog_file,
        binlog_pos: u32::try_from(binlog_pos)
            .map_err(|_| report!("MySQL binlog position {binlog_pos} exceeds u32"))?,
        gtid,
    })
}

pub async fn current_gtid_executed(conn: &mut Conn) -> Result<String> {
    Ok(conn
        .query_first::<String, _>("SELECT @@GLOBAL.gtid_executed")
        .await
        .ok()
        .flatten()
        .unwrap_or_default())
}

pub async fn gtid_purged(conn: &mut Conn) -> Result<String> {
    Ok(conn
        .query_first::<String, _>("SELECT @@GLOBAL.gtid_purged")
        .await
        .ok()
        .flatten()
        .unwrap_or_default())
}

async fn inspect_table(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<MySqlTable> {
    let rows: Vec<Row> = conn
        .exec(
            "SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, DATA_TYPE, \
                    IS_NULLABLE, CHARACTER_SET_NAME, COLUMN_KEY, \
                    NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .map_err(|error| report!("read MySQL columns for {database}.{table}: {error}"))?;
    if rows.is_empty() {
        bail!("source table {database}.{table} does not exist");
    }

    let pk_rows: Vec<Row> = conn
        .exec(
            "SELECT COLUMN_NAME \
             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
             ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .map_err(|error| {
            report!("read MySQL primary key for {database}.{table}: {error}")
        })?;
    let primary_key: Vec<String> = pk_rows
        .iter()
        .map(|row| row.get::<String, _>(0).unwrap_or_default())
        .collect();
    if primary_key.is_empty() {
        bail!("source table {database}.{table} has no primary key; CDC requires one");
    }

    let mut columns = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let name: String = row.get(0).unwrap_or_default();
        let _ordinal: u32 = row.get(1).unwrap_or((index + 1) as u32);
        let column_type: String = row.get(2).unwrap_or_default();
        let data_type: String = row.get(3).unwrap_or_default();
        let is_nullable: String = row.get(4).unwrap_or_else(|| "NO".to_string());
        let charset: Option<String> = row.get::<Option<String>, _>(5).flatten();
        let column_key: String = row
            .get::<Option<String>, _>(6)
            .flatten()
            .unwrap_or_default();
        let numeric_precision: Option<u64> = row.get::<Option<u64>, _>(7).flatten();
        let numeric_scale: Option<u64> = row.get::<Option<u64>, _>(8).flatten();
        let datetime_precision: Option<u64> = row.get::<Option<u64>, _>(9).flatten();
        if name.is_empty() {
            bail!("source table {database}.{table} has an empty column name");
        }
        let data_type = MySqlType::from_information_schema(
            &data_type,
            &column_type,
            numeric_precision,
            numeric_scale,
            datetime_precision,
        );
        let primary_key = primary_key.iter().any(|key| key == &name)
            || column_key.eq_ignore_ascii_case("PRI");
        columns.push(MySqlColumn {
            name,
            data_type,
            nullable: is_nullable.eq_ignore_ascii_case("YES"),
            primary_key,
            charset,
        });
    }

    Ok(MySqlTable {
        database: database.to_string(),
        name: table.to_string(),
        columns,
        primary_key,
    })
}

/// Quote a MySQL identifier for generated snapshot SQL.
pub fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

pub fn qualified_table(database: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(database), quote_ident(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_json_roundtrip() {
        let offset = MySqlOffset {
            gtid: "uuid:1-5".to_string(),
            binlog_file: "mysql-bin.000001".to_string(),
            binlog_pos: 42,
            sequence: 7,
        };
        let json = offset.to_json().unwrap();
        assert_eq!(MySqlOffset::from_json(&json).unwrap(), offset);
    }

    #[test]
    fn quote_escapes_backticks() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }
}
