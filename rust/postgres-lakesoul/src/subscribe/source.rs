// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Source-side catalog inspection for the P0 subscriber.
//!
//! P0 deliberately only supports ordinary tables in the `public` schema with
//! `REPLICA IDENTITY FULL`, a primary key, and publications that publish
//! insert/update/delete but not truncate. Everything else fails before the
//! first WAL byte is read, with an actionable message.

use rootcause::{bail, report};
use tokio_postgres::NoTls;

use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric {
        precision: Option<u8>,
        scale: Option<i8>,
    },
    Text,
    Bytea,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Uuid,
    Json,
    Jsonb,
}

impl PgType {
    pub fn from_oid(oid: u32, typmod: i32) -> Option<Self> {
        Some(match oid {
            16 => Self::Bool,
            21 => Self::Int2,
            23 => Self::Int4,
            20 => Self::Int8,
            700 => Self::Float4,
            701 => Self::Float8,
            1700 => {
                let (precision, scale) = numeric_typmod(typmod);
                Self::Numeric { precision, scale }
            }
            25 | 1043 | 1042 | 19 => Self::Text,
            17 => Self::Bytea,
            1082 => Self::Date,
            1083 => Self::Time,
            1114 => Self::Timestamp,
            1184 => Self::TimestampTz,
            2950 => Self::Uuid,
            114 => Self::Json,
            3802 => Self::Jsonb,
            _ => return None,
        })
    }
}

/// PostgreSQL packs `numeric(precision, scale)` into `atttypmod`; `-1` means
/// an unconstrained numeric. The scale field is 11 bits and may be negative
/// on newer servers, so sign-extend it.
fn numeric_typmod(typmod: i32) -> (Option<u8>, Option<i8>) {
    if typmod < 4 {
        return (None, None);
    }
    let packed = typmod - 4;
    let precision = ((packed >> 16) & 0xffff) as u8;
    let mut scale = (packed & 0x7ff) as i16;
    if scale & 0x400 != 0 {
        scale -= 0x800;
    }
    let precision = (precision > 0).then_some(precision);
    (precision, Some(scale as i8))
}

#[derive(Debug, Clone)]
pub struct SourceColumn {
    pub name: String,
    pub type_id: u32,
    pub type_modifier: i32,
    pub pg_type: PgType,
    pub type_name: String,
    pub not_null: bool,
    /// Whether PostgreSQL may store this column out of line in a TOAST table.
    /// `pgoutput` sends `u` for an unchanged TOASTable value in an UPDATE's
    /// new tuple; with REPLICA IDENTITY FULL the old tuple carries the value
    /// that the subscriber merges back in.
    pub toastable: bool,
}

#[derive(Debug, Clone)]
pub struct SourceTable {
    pub schema: String,
    pub name: String,
    pub relation_oid: u32,
    pub columns: Vec<SourceColumn>,
    pub primary_key: Vec<String>,
}

impl SourceTable {
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// Validate the publication/table and read the column and primary-key
/// metadata. The caller still owns the replication connection.
pub async fn inspect_source(
    url: &str,
    publication: &str,
    schema: &str,
    table: &str,
) -> Result<SourceTable> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|error| report!("connect to source PostgreSQL: {error}"))?;
    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "source PostgreSQL connection closed");
        }
    });

    let result = inspect(&client, publication, schema, table).await;
    drop(client);
    handle.abort();
    result
}

async fn inspect(
    client: &tokio_postgres::Client,
    publication: &str,
    schema: &str,
    table: &str,
) -> Result<SourceTable> {
    if schema != "public" {
        bail!("P0 only supports the public schema, got source table {schema}.{table}");
    }

    let wal_level: String = client
        .query_one("show wal_level", &[])
        .await
        .map_err(|error| report!("read wal_level: {error}"))?
        .get(0);
    if wal_level != "logical" {
        bail!(
            "source wal_level is {wal_level}; set wal_level=logical and restart PostgreSQL"
        );
    }

    let in_recovery: bool = client
        .query_one("select pg_is_in_recovery()", &[])
        .await
        .map_err(|error| report!("read pg_is_in_recovery: {error}"))?
        .get(0);
    if in_recovery {
        bail!("source is a standby; P0 subscribes from a primary");
    }

    let publication_row = client
        .query_opt(
            "select pubinsert, pubupdate, pubdelete, pubtruncate \
             from pg_publication where pubname = $1",
            &[&publication],
        )
        .await
        .map_err(|error| report!("read publication {publication}: {error}"))?
        .ok_or_else(|| report!("publication {publication} does not exist"))?;
    let pubinsert: bool = publication_row.get(0);
    let pubupdate: bool = publication_row.get(1);
    let pubdelete: bool = publication_row.get(2);
    let pubtruncate: bool = publication_row.get(3);
    if !(pubinsert && pubupdate && pubdelete) {
        bail!(
            "publication {publication} must publish insert, update and delete \
             (pubinsert={pubinsert}, pubupdate={pubupdate}, pubdelete={pubdelete})"
        );
    }
    if pubtruncate {
        bail!(
            "publication {publication} publishes truncate; P0 does not support it. \
             Recreate it with WITH (publish = 'insert,update,delete')"
        );
    }

    let published = client
        .query_opt(
            "select 1 from pg_publication_tables \
             where pubname = $1 and schemaname = $2 and tablename = $3",
            &[&publication, &schema, &table],
        )
        .await
        .map_err(|error| report!("read pg_publication_tables: {error}"))?;
    if published.is_none() {
        bail!("table {schema}.{table} is not in publication {publication}");
    }

    let relation = client
        .query_opt(
            "select c.oid::int8, c.relkind::text, c.relreplident::text \
             from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname = $2",
            &[&schema, &table],
        )
        .await
        .map_err(|error| report!("read pg_class for {schema}.{table}: {error}"))?
        .ok_or_else(|| report!("table {schema}.{table} does not exist"))?;
    let relation_oid: i64 = relation.get(0);
    let relation_oid = u32::try_from(relation_oid).map_err(|_| {
        report!("table {schema}.{table} has an out-of-range relation oid {relation_oid}")
    })?;
    let relkind: String = relation.get(1);
    let replident: String = relation.get(2);
    if relkind != "r" {
        bail!(
            "table {schema}.{table} has relkind={relkind}; P0 only supports ordinary \
             tables (not partitioned, foreign, views or matviews)"
        );
    }
    if replident != "f" {
        bail!(
            "table {schema}.{table} does not use REPLICA IDENTITY FULL; run \
             ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL"
        );
    }

    let rows = client
        .query(
            "select a.attname, a.atttypid::int8, a.atttypmod, a.attnotnull, \
                    a.attgenerated::text, t.typname, a.attstorage::text, a.attlen \
             from pg_attribute a \
             join pg_class c on c.oid = a.attrelid \
             join pg_namespace n on n.oid = c.relnamespace \
             join pg_type t on t.oid = a.atttypid \
             where n.nspname = $1 and c.relname = $2 \
               and a.attnum > 0 and not a.attisdropped \
             order by a.attnum",
            &[&schema, &table],
        )
        .await
        .map_err(|error| report!("read columns of {schema}.{table}: {error}"))?;

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let oid: i64 = row.get(1);
        let typmod: i32 = row.get(2);
        let not_null: bool = row.get(3);
        let generated: String = row.get(4);
        let type_name: String = row.get(5);
        let storage: String = row.get(6);
        let attlen: i16 = row.get(7);
        if !generated.is_empty() {
            bail!(
                "column {name} of {schema}.{table} is generated; P0 does not support \
                 generated columns"
            );
        }
        let oid = u32::try_from(oid)
            .map_err(|_| report!("column {name} has an out-of-range type oid {oid}"))?;
        let pg_type = PgType::from_oid(oid, typmod).ok_or_else(|| {
            report!(
                "column {name} of {schema}.{table} has unsupported type {type_name} \
                 (oid {oid}); P0 supports bool, int2/4/8, float4/8, numeric, text, \
                 varchar, bpchar, name, bytea, date, time, timestamp, timestamptz, \
                 uuid, json and jsonb"
            )
        })?;
        columns.push(SourceColumn {
            name,
            type_id: oid,
            type_modifier: typmod,
            pg_type,
            type_name,
            not_null,
            toastable: attlen == -1 && matches!(storage.as_str(), "e" | "m" | "x"),
        });
    }
    if columns.is_empty() {
        bail!("table {schema}.{table} has no columns");
    }

    let key_rows = client
        .query(
            "select a.attname \
             from pg_index i \
             join pg_class c on c.oid = i.indrelid \
             join pg_namespace n on n.oid = c.relnamespace \
             join unnest(i.indkey) with ordinality as k(attnum, ord) on true \
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum \
             where n.nspname = $1 and c.relname = $2 and i.indisprimary \
             order by k.ord",
            &[&schema, &table],
        )
        .await
        .map_err(|error| report!("read primary key of {schema}.{table}: {error}"))?;
    let primary_key: Vec<String> =
        key_rows.iter().map(|row| row.get::<_, String>(0)).collect();
    if primary_key.is_empty() {
        bail!(
            "table {schema}.{table} has no primary key; P0 uses the primary key as \
             the LakeSoul merge key"
        );
    }

    Ok(SourceTable {
        schema: schema.to_string(),
        name: table.to_string(),
        relation_oid,
        columns,
        primary_key,
    })
}

/// Parse PostgreSQL's `pg_lsn` text form (`16/B374D848`).
pub fn parse_lsn(text: &str) -> Result<u64> {
    let (high, low) = text
        .split_once('/')
        .ok_or_else(|| report!("invalid pg_lsn {text:?}"))?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|error| report!("invalid pg_lsn high part {high:?}: {error}"))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|error| report!("invalid pg_lsn low part {low:?}: {error}"))?;
    Ok((high << 32) | low)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_lsn_parses_both_halves() {
        assert_eq!(parse_lsn("0/0").unwrap(), 0);
        assert_eq!(
            parse_lsn("16/B374D848").unwrap(),
            (0x16_u64 << 32) | 0xB374D848_u64
        );
        assert!(parse_lsn("nope").is_err());
    }

    #[test]
    fn numeric_typmod_is_decoded() {
        let typmod = (((20_i32 << 16) | 4) + 4) as i32;
        assert_eq!(
            PgType::from_oid(1700, typmod),
            Some(PgType::Numeric {
                precision: Some(20),
                scale: Some(4)
            })
        );
        assert_eq!(
            PgType::from_oid(1700, -1),
            Some(PgType::Numeric {
                precision: None,
                scale: None
            })
        );
        assert_eq!(PgType::from_oid(99999, -1), None);
    }
}
