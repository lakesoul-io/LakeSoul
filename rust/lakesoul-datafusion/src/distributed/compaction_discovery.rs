// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Read-only compaction discovery against the three independently replicated metadata tables.

use std::cmp::Reverse;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion::common::{
    DataFusionError, Result, ScalarValue, TableReference, scalar::ScalarStructBuilder,
};
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::*;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata_proto::entity::{self, CommitOp, PartitionInfo, TableInfo};
use serde::Deserialize;
use uuid::Uuid;

use super::compaction::CompactionFile;
use super::compaction_coordinator::{CompactionCoordinator, LoadedSnapshot};

pub const DEFAULT_MIRROR_NAMESPACE: &str = "lakesoul_meta";

pub struct DiscoveryConfig {
    /// Namespace (PostgreSQL database in the destination endpoint) holding the
    /// three replicated metadata tables.
    pub mirror_namespace: String,
    pub excluded_namespaces: BTreeSet<String>,
    pub excluded_tables: BTreeSet<(String, String)>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            mirror_namespace: DEFAULT_MIRROR_NAMESPACE.to_string(),
            excluded_namespaces: BTreeSet::new(),
            excluded_tables: BTreeSet::new(),
        }
    }
}

#[derive(Debug)]
pub enum SnapshotLoad {
    Ready(Box<LoadedSnapshot>),
    NotReady {
        table_info_missing: bool,
        missing_commits: Vec<Uuid>,
        uncommitted_commits: Vec<Uuid>,
    },
}

fn invalid(msg: impl ToString) -> DataFusionError {
    DataFusionError::Execution(msg.to_string())
}

/// The namespace is interpolated into the discovery SQL, so keep it to a
/// simple, unquoted identifier shape.
fn valid_mirror_namespace(namespace: &str) -> bool {
    let mut characters = namespace.chars();
    matches!(characters.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && characters.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

#[derive(Deserialize)]
struct FileOp {
    path: String,
    file_op: String,
    size: Option<i64>,
    file_exist_cols: Option<String>,
}

fn op_fields() -> Vec<Field> {
    use DataType::*;
    [
        ("op_pos", Int64),
        ("path", Utf8),
        ("file_op", Utf8),
        ("size", Int64),
        ("file_exist_cols", Utf8),
    ]
    .into_iter()
    .map(|(name, ty)| Field::new(name, ty, true))
    .collect()
}

fn decode_ops(json: &str) -> Result<Vec<ScalarValue>> {
    let ops: Vec<FileOp> = serde_json::from_str(json).map_err(invalid)?;
    let fields = op_fields();
    ops.into_iter()
        .enumerate()
        .map(|(pos, op)| {
            if !matches!(op.file_op.as_str(), "add" | "del") || op.path.is_empty() {
                return Err(invalid(format!(
                    "invalid mirrored file operation at position {pos}"
                )));
            }
            if op.file_op == "add" && op.size.is_none_or(|size| size < 0) {
                return Err(invalid(format!("invalid add size for {}", op.path)));
            }
            let values = [
                ScalarValue::Int64(Some(pos as i64)),
                op.path.into(),
                op.file_op.into(),
                ScalarValue::Int64(op.size),
                ScalarValue::Utf8(op.file_exist_cols),
            ];
            fields
                .iter()
                .zip(values)
                .fold(ScalarStructBuilder::new(), |builder, (field, value)| {
                    builder.with_scalar(field.clone(), value)
                })
                .build()
        })
        .collect()
}

fn decode_ids(json: &str) -> Result<Vec<ScalarValue>> {
    let ids: Vec<String> = serde_json::from_str(json).map_err(invalid)?;
    ids.into_iter()
        .map(|id| {
            Uuid::parse_str(&id)
                .map(|id| id.to_string().into())
                .map_err(invalid)
        })
        .collect()
}

pub(super) fn register_mirror_functions(ctx: &SessionContext) {
    type Decode = fn(&str) -> Result<Vec<ScalarValue>>;
    for (name, item, decode) in [
        ("mirror_snapshot", DataType::Utf8, decode_ids as Decode),
        (
            "mirror_file_ops",
            DataType::Struct(op_fields().into()),
            decode_ops as Decode,
        ),
    ] {
        let ty = DataType::new_list(item.clone(), true);
        ctx.register_udf(create_udf(
            name,
            vec![DataType::Utf8],
            ty.clone(),
            Volatility::Immutable,
            Arc::new(move |args| {
                let input = args[0].clone().into_array_of_size(match &args[0] {
                    ColumnarValue::Array(a) => a.len(),
                    ColumnarValue::Scalar(_) => 1,
                })?;
                let strings = input
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("expected mirror JSON text"))?;
                let mut lists = Vec::with_capacity(strings.len());
                for json in strings.iter() {
                    let values =
                        decode(json.ok_or_else(|| invalid("NULL mirror JSON array"))?)?;
                    lists.push(ScalarValue::List(ScalarValue::new_list_nullable(
                        &values, &item,
                    )));
                }
                match &args[0] {
                    ColumnarValue::Scalar(_) => {
                        Ok(ColumnarValue::Scalar(lists.remove(0)))
                    }
                    _ if lists.is_empty() => {
                        Ok(ColumnarValue::Array(arrow::array::new_empty_array(&ty)))
                    }
                    _ => Ok(ColumnarValue::Array(ScalarValue::iter_to_array(lists)?)),
                }
            }),
        ));
    }
}

struct Row<'a>(&'a RecordBatch, usize);
impl Row<'_> {
    fn value(&self, name: &str) -> Result<ScalarValue> {
        let col = self
            .0
            .column_by_name(name)
            .ok_or_else(|| invalid(format!("missing mirror column {name}")))?;
        ScalarValue::try_from_array(col, self.1)
    }
    fn text(&self, name: &str) -> Result<String> {
        match self.value(name)? {
            ScalarValue::Utf8(s)
            | ScalarValue::Utf8View(s)
            | ScalarValue::LargeUtf8(s) => Ok(s.unwrap_or_default()),
            other => Err(invalid(format!("expected text for {name}, got {other:?}"))),
        }
    }
    fn i64(&self, name: &str) -> Result<i64> {
        i64::try_from(self.value(name)?)
    }
}

/// Only the directory immediately under the table root identifies a compaction level.
fn file_level(root: &str, path: &str) -> Result<u32> {
    let relative = path
        .strip_prefix(root.trim_end_matches('/'))
        .and_then(|tail| tail.strip_prefix('/'))
        .filter(|tail| !tail.is_empty() && !tail.starts_with('/'))
        .ok_or_else(|| invalid(format!("file {path} outside table root {root}")))?;
    let Some((directory, _)) = relative.split_once('/') else {
        return Ok(0);
    };
    match directory.strip_prefix("compactdir") {
        Some(digits)
            if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) =>
        {
            digits.parse().map_err(invalid)
        }
        _ => Ok(0),
    }
}

fn bucket_count(table: &TableInfo) -> Result<Option<usize>> {
    let properties: serde_json::Value =
        serde_json::from_str(&table.properties).map_err(invalid)?;
    let count = match properties.get("hashBucketNum") {
        Some(serde_json::Value::String(value)) => {
            value.parse::<i64>().map_err(invalid)?
        }
        Some(value) => value
            .as_i64()
            .ok_or_else(|| invalid("invalid hashBucketNum"))?,
        None => return Ok(None),
    };
    match count {
        -1 => Ok(None),
        n if n > 0 => Ok(Some(usize::try_from(n).map_err(invalid)?)),
        _ => Err(invalid(format!("invalid hashBucketNum {count}"))),
    }
}

impl CompactionCoordinator {
    fn mirror_namespace(&self) -> Result<&str> {
        let namespace = self.config.mirror_namespace.as_str();
        if !valid_mirror_namespace(namespace) {
            return Err(invalid(format!("invalid mirror namespace {namespace:?}")));
        }
        Ok(namespace)
    }

    fn mirror_table(&self, table: &str) -> Result<TableReference> {
        Ok(TableReference::partial(self.mirror_namespace()?, table))
    }

    pub async fn scan_candidates(&self) -> Result<Vec<PartitionInfo>> {
        let mirror_namespace = self.mirror_namespace()?.to_owned();
        let mirror_namespace_sql = format!("\"{mirror_namespace}\"");
        let mut df = self.metadata_session.sql(&format!(
            "SELECT h.*, t.table_namespace, t.table_name FROM
             (SELECT p.*, row_number() OVER (PARTITION BY table_id, partition_desc ORDER BY version DESC) AS r
              FROM {mirror_namespace_sql}.partition_info p) h
             LEFT JOIN {mirror_namespace_sql}.table_info t ON t.table_id=h.table_id
             WHERE h.r=1"
        )).await?;
        let ns = col("table_namespace");
        df = df.filter(
            ns.clone()
                .is_null()
                .or(ns.clone().not_eq(lit(mirror_namespace))),
        )?;
        let name = col("table_name");
        for excluded in &self.config.excluded_namespaces {
            df = df.filter(
                ns.clone()
                    .is_null()
                    .or(ns.clone().not_eq(lit(excluded.clone()))),
            )?;
        }
        for (schema, table) in &self.config.excluded_tables {
            df = df.filter(
                ns.clone()
                    .is_null()
                    .or(name.clone().is_null())
                    .or(ns.clone().not_eq(lit(schema.clone())))
                    .or(name.clone().not_eq(lit(table.clone()))),
            )?;
        }
        let df = df.with_column(
            "ids",
            datafusion::logical_expr::registry::FunctionRegistry::udf(
                self.metadata_session.as_ref(),
                "mirror_snapshot",
            )?
            .call(vec![col("snapshot")]),
        )?;
        let mut partitions = Vec::new();
        for batch in df.collect().await? {
            for i in 0..batch.num_rows() {
                let row = Row(&batch, i);
                let ScalarValue::List(ids) = row.value("ids")? else {
                    return Err(invalid("expected UUID list"));
                };
                let values = ids.value(0);
                let strings = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("expected UUID strings"))?;
                let snapshot = strings
                    .iter()
                    .map(|s| {
                        let (high, low) = Uuid::parse_str(
                            s.ok_or_else(|| invalid("NULL snapshot ID"))?,
                        )
                        .map_err(invalid)?
                        .as_u64_pair();
                        Ok(entity::Uuid { high, low })
                    })
                    .collect::<Result<_>>()?;
                partitions.push(PartitionInfo {
                    table_id: row.text("table_id")?,
                    partition_desc: row.text("partition_desc")?,
                    version: i32::try_from(row.value("version")?)?,
                    snapshot,
                    commit_op: CommitOp::from_str_name(&row.text("commit_op")?)
                        .ok_or_else(|| invalid("invalid commit_op"))?
                        as i32,
                    timestamp: row.i64("timestamp")?,
                    expression: row.text("expression")?,
                    domain: row.text("domain")?,
                    pinned: bool::try_from(row.value("pinned")?)?,
                });
            }
        }
        Ok(partitions)
    }

    pub async fn load_snapshot(&self, partition: &PartitionInfo) -> Result<SnapshotLoad> {
        let ctx = &self.metadata_session;
        let mut table = None;
        for batch in ctx
            .table(self.mirror_table("table_info")?)
            .await?
            .filter(col("table_id").eq(lit(partition.table_id.clone())))?
            .collect()
            .await?
        {
            for i in 0..batch.num_rows() {
                if table.is_some() {
                    return Err(invalid("duplicate mirrored table_id"));
                }
                let r = Row(&batch, i);
                table = Some(TableInfo {
                    table_id: r.text("table_id")?,
                    table_namespace: r.text("table_namespace")?,
                    table_name: r.text("table_name")?,
                    table_path: r.text("table_path")?,
                    table_schema: r.text("table_schema")?,
                    table_schema_arrow_ipc: match r.value("table_schema_arrow_ipc")? {
                        ScalarValue::Binary(v)
                        | ScalarValue::BinaryView(v)
                        | ScalarValue::LargeBinary(v) => v.unwrap_or_default(),
                        _ => return Err(invalid("expected binary Arrow schema")),
                    },
                    table_schema_arrow_ipc_json_hash: r
                        .text("table_schema_arrow_ipc_json_hash")?,
                    properties: r.text("properties")?,
                    partitions: r.text("partitions")?,
                    domain: r.text("domain")?,
                    creator: r.text("creator")?,
                });
            }
        }
        let mut seen = HashSet::with_capacity(partition.snapshot.len());
        let mut ids = Vec::with_capacity(partition.snapshot.len());
        for entry in &partition.snapshot {
            let id = Uuid::from_u64_pair(entry.high, entry.low);
            if !seen.insert(id) {
                return Err(invalid(format!(
                    "duplicate snapshot commit {id} in {} partition {} version {}",
                    partition.table_id, partition.partition_desc, partition.version
                )));
            }
            ids.push(id.to_string());
        }
        // Expose the referenced IDs as a scan predicate as well as a join key.
        // The ordered left side below still accounts for missing mirror rows.
        let referenced = if ids.is_empty() {
            lit(false)
        } else {
            col("commit_id").in_list(ids.iter().cloned().map(lit).collect(), false)
        };
        let refs = ctx.read_batch(RecordBatch::try_from_iter([
            ("ref_id", Arc::new(StringArray::from(ids)) as ArrayRef),
            (
                "commit_pos",
                Arc::new(Int64Array::from_iter_values(
                    0..partition.snapshot.len() as i64,
                )) as ArrayRef,
            ),
        ])?)?;
        let commits =
            ctx.table(self.mirror_table("data_commit_info")?)
                .await?
                .filter(col("table_id").eq(lit(partition.table_id.clone())).and(
                    col("partition_desc").eq(lit(partition.partition_desc.clone())),
                ))?
                .filter(referenced)?
                .select_columns(&["commit_id", "committed", "timestamp", "file_ops"])?;
        let joined =
            refs.join(commits, JoinType::Left, &["ref_id"], &["commit_id"], None)?;
        let schema = Arc::new(joined.schema().as_arrow().clone());
        let batches = joined.collect().await?;
        let mut missing = Vec::new();
        let mut uncommitted = Vec::new();
        let mut positions = BTreeSet::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                let r = Row(batch, i);
                if !positions.insert(r.i64("commit_pos")?) {
                    return Err(invalid("duplicate mirrored commit row"));
                }
                if r.value("commit_id")?.is_null() {
                    missing.push(Uuid::parse_str(&r.text("ref_id")?).map_err(invalid)?);
                } else if r.value("committed")? != ScalarValue::Boolean(Some(true)) {
                    uncommitted
                        .push(Uuid::parse_str(&r.text("ref_id")?).map_err(invalid)?);
                }
            }
        }
        let table_info_missing = table.is_none();
        if table_info_missing || !missing.is_empty() || !uncommitted.is_empty() {
            return Ok(SnapshotLoad::NotReady {
                table_info_missing,
                missing_commits: missing,
                uncommitted_commits: uncommitted,
            });
        }
        let table = table.ok_or_else(|| invalid("missing validated table_info"))?;
        let buckets = bucket_count(&table)?;
        // Bind exactly the rows validated above. Concurrent loads have separate catalogs.
        let replay = SessionContext::new();
        register_mirror_functions(&replay);
        replay.register_table(
            "snapshot_commits",
            Arc::new(datafusion::datasource::MemTable::try_new(
                schema,
                vec![batches],
            )?),
        )?;
        let df=replay.sql("WITH expanded AS (
            SELECT commit_pos, timestamp, unnest(mirror_file_ops(file_ops)) AS op FROM snapshot_commits
        ), ops AS (
            SELECT commit_pos, timestamp, op.op_pos AS op_pos, op.path AS path,
                   op.file_op AS file_op, op.size AS size, op.file_exist_cols AS file_exist_cols FROM expanded
        ), replayed AS (
            SELECT *, sum(CASE WHEN file_op='del' THEN 1 ELSE 0 END) OVER (PARTITION BY path ORDER BY commit_pos, op_pos
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS later_deletes FROM ops
        ) SELECT * FROM replayed WHERE file_op='add' AND later_deletes=0 ORDER BY commit_pos, op_pos").await?;
        let mut active_files = Vec::new();
        for batch in df.collect().await? {
            for i in 0..batch.num_rows() {
                let r = Row(&batch, i);
                let path = r.text("path")?;
                let level = file_level(&table.table_path, &path)?;
                let filename = path.rsplit('/').next().unwrap_or_default();
                let bucket_id =
                    DataFileInfo::parse_bucket_id(filename).filter(|id| *id >= 0);
                if let Some(count) = buckets
                    && bucket_id.is_none_or(|id| id as usize >= count)
                {
                    return Err(invalid(format!(
                        "invalid compaction bucket for {path} (table {})",
                        table.table_id
                    )));
                }
                active_files.push(CompactionFile {
                    level,
                    file: DataFileInfo {
                        bucket_id,
                        path,
                        file_op: "add".into(),
                        size: r.i64("size")?,
                        file_exist_cols: r.text("file_exist_cols")?,
                        modification_time: r.i64("timestamp")?,
                        partition_desc: partition.partition_desc.clone(),
                    },
                });
            }
        }
        // Preserve every surviving add, including repeated paths, as in Spark replay.
        active_files.sort_by_key(|file| Reverse(file.level));
        Ok(SnapshotLoad::Ready(Box::new(
            LoadedSnapshot::from_discovery(table, partition.clone(), active_files),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The commit UUIDs sort in the opposite order to the snapshot.
    const FIRST: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";
    const SECOND: &str = "00000000-0000-0000-0000-000000000002";

    #[tokio::test]
    async fn null_snapshot_is_invalid_while_empty_json_array_is_valid() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        ctx.sql("CREATE SCHEMA lakesoul_meta")
            .await?
            .collect()
            .await?;
        ctx.sql(
            "CREATE TABLE lakesoul_meta.table_info AS SELECT 't' AS table_id,
                 'sales' AS table_namespace, 'orders' AS table_name",
        )
        .await?
        .collect()
        .await?;
        ctx.sql(
            "CREATE TABLE lakesoul_meta.partition_info (
                 table_id VARCHAR NOT NULL, partition_desc VARCHAR NOT NULL,
                 version INT NOT NULL, commit_op VARCHAR NOT NULL, timestamp BIGINT NOT NULL,
                 snapshot VARCHAR, expression VARCHAR NOT NULL, domain VARCHAR NOT NULL,
                 pinned BOOLEAN NOT NULL
             )",
        )
        .await?
        .collect()
        .await?;
        ctx.sql(
            "INSERT INTO lakesoul_meta.partition_info VALUES
                 ('t','-5',1,'UpdateCommit',10,'[]','','public',false)",
        )
        .await?
        .collect()
        .await?;
        let coordinator =
            CompactionCoordinator::new(ctx.clone(), DiscoveryConfig::default());
        let partitions = coordinator.scan_candidates().await?;
        let [head] = partitions.as_slice() else {
            panic!("expected one partition")
        };
        assert!(head.snapshot.is_empty());

        ctx.sql(
            "INSERT INTO lakesoul_meta.partition_info VALUES
                 ('t','-5',2,'UpdateCommit',20,NULL,'','public',false)",
        )
        .await?
        .collect()
        .await?;
        let error = coordinator.scan_candidates().await.unwrap_err().to_string();
        assert!(error.contains("NULL mirror JSON array"), "{error}");
        Ok(())
    }

    #[test]
    fn level_directory_precedes_partition_directories() -> Result<()> {
        assert_eq!(
            file_level("path", "path/compactdir2/d=1/part_0000.parquet")?,
            2
        );
        // Spark's classifier searches the full path. Discovery recognizes the
        // writer layout only: a nested directory of this name does not set the level.
        assert_eq!(
            file_level("path", "path/d=1/compactdir2/part_0000.parquet")?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn replay_preserves_repeated_adds_from_visible_commits() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        ctx.sql("CREATE SCHEMA lakesoul_meta")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE lakesoul_meta.table_info AS SELECT 't' AS table_id,
            'sales' AS table_namespace, 'orders' AS table_name, 'path' AS table_path,
            '{}' AS table_schema, CAST(NULL AS BYTEA) AS table_schema_arrow_ipc,
            '' AS table_schema_arrow_ipc_json_hash, '{}' AS properties, ';id' AS partitions,
            'public' AS domain, 'owner' AS creator").await?.collect().await?;
        ctx.sql(&format!(r#"CREATE TABLE lakesoul_meta.partition_info AS SELECT
            't' AS table_id, '-5' AS partition_desc, CAST(2 AS INT) AS version,
            'UpdateCommit' AS commit_op, CAST(20 AS BIGINT) AS timestamp,
            '["{SECOND}"]' AS snapshot, '' AS expression, 'public' AS domain, false AS pinned"#))
            .await?.collect().await?;
        let a = "path/a_0000.parquet";
        let b = "path/b_0000.parquet";
        let c = "path/c_0000.parquet";
        let add = |path: &str| serde_json::json!({"path":path,"file_op":"add","size":10,"file_exist_cols":"id"});
        let first = serde_json::json!([add(a), add(b)]);
        let second = serde_json::json!([add(a), {"path":b,"file_op":"del"}, add(c)]);
        ctx.sql(&format!(
            "CREATE TABLE lakesoul_meta.data_commit_info AS SELECT
            't' AS table_id, '-5' AS partition_desc, id AS commit_id, true AS committed,
            CAST(ts AS BIGINT) AS timestamp, file_ops FROM (VALUES
            ('{FIRST}',10,'{first}'),('{SECOND}',20,'{second}')) v(id,ts,file_ops)"
        ))
        .await?
        .collect()
        .await?;
        let coordinator =
            CompactionCoordinator::new(ctx.clone(), DiscoveryConfig::default());
        let candidates = coordinator.scan_candidates().await?;
        let [replacement] = candidates.as_slice() else {
            panic!("expected one partition")
        };
        let SnapshotLoad::Ready(loaded) = coordinator.load_snapshot(replacement).await?
        else {
            panic!("replacement snapshot should be complete");
        };
        let files: Vec<_> = loaded
            .active_files()
            .iter()
            .map(|f| (f.file.path.as_str(), f.file.modification_time))
            .collect();
        assert_eq!(files, vec![(a, 20), (c, 20)]);

        // Keep the commit rows unchanged; only the snapshot's visibility changes.
        ctx.sql(&format!(
            r#"INSERT INTO lakesoul_meta.partition_info VALUES
            ('t','-5',3,'UpdateCommit',30,'["{FIRST}","{SECOND}"]','','public',false)"#
        ))
        .await?
        .collect()
        .await?;
        let candidates = coordinator.scan_candidates().await?;
        let [overlap] = candidates.as_slice() else {
            panic!("expected one partition")
        };
        assert_eq!(overlap.version, 3);
        let SnapshotLoad::Ready(loaded) = coordinator.load_snapshot(overlap).await?
        else {
            panic!("expected complete snapshot with both commits visible");
        };
        let files: Vec<_> = loaded
            .active_files()
            .iter()
            .map(|f| (f.file.path.as_str(), f.file.modification_time))
            .collect();
        assert_eq!(files, vec![(a, 10), (a, 20), (c, 20)]);
        Ok(())
    }

    #[tokio::test]
    async fn discovery_loads_unbucketed_files_for_both_table_property_encodings()
    -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        ctx.sql("CREATE SCHEMA lakesoul_meta")
            .await?
            .collect()
            .await?;
        ctx.sql(r#"CREATE TABLE lakesoul_meta.table_info AS SELECT
            table_id, 'sales' AS table_namespace, table_id AS table_name,
            'path' AS table_path, '{}' AS table_schema, CAST(NULL AS BYTEA) AS table_schema_arrow_ipc,
            '' AS table_schema_arrow_ipc_json_hash, properties, ';' AS partitions,
            'public' AS domain, 'owner' AS creator
            FROM (VALUES ('unspecified', '{}'), ('disabled', '{"hashBucketNum":"-1"}'))
            v(table_id, properties)"#).await?.collect().await?;
        let path = "path/part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb-c000.parquet";
        let file_ops = serde_json::json!([{"path":path,"file_op":"add","size":10,"file_exist_cols":"id"}]);
        ctx.sql(&format!(
            "CREATE TABLE lakesoul_meta.data_commit_info AS SELECT
            table_id, '-5' AS partition_desc, '{FIRST}' AS commit_id, true AS committed,
            CAST(1 AS BIGINT) AS timestamp, '{file_ops}' AS file_ops
            FROM lakesoul_meta.table_info"
        ))
        .await?
        .collect()
        .await?;

        let coordinator = CompactionCoordinator::new(ctx, DiscoveryConfig::default());
        let (high, low) = Uuid::parse_str(FIRST).unwrap().as_u64_pair();
        for table_id in ["unspecified", "disabled"] {
            let partition = PartitionInfo {
                table_id: table_id.into(),
                partition_desc: "-5".into(),
                version: 1,
                snapshot: vec![entity::Uuid { high, low }],
                ..Default::default()
            };
            let SnapshotLoad::Ready(loaded) =
                coordinator.load_snapshot(&partition).await?
            else {
                panic!("expected complete snapshot for {table_id}");
            };
            let [active] = loaded.active_files() else {
                panic!("expected one active file for {table_id}");
            };
            assert_eq!(active.file.path, path);
            assert_eq!(active.file.bucket_id, None, "{table_id}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn discovery_uses_the_configured_mirror_namespace() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        ctx.sql("CREATE SCHEMA mirror_meta")
            .await?
            .collect()
            .await?;
        ctx.sql(
            "CREATE TABLE mirror_meta.table_info AS SELECT 't' AS table_id,
                 'sales' AS table_namespace, 'orders' AS table_name, 'path' AS table_path,
                 '{}' AS table_schema, CAST(NULL AS BYTEA) AS table_schema_arrow_ipc,
                 '' AS table_schema_arrow_ipc_json_hash, '{\"hashBucketNum\":\"4\"}' AS properties,
                 ';id' AS partitions, 'public' AS domain, 'owner' AS creator",
        )
        .await?
        .collect()
        .await?;
        ctx.sql(&format!(
            "CREATE TABLE mirror_meta.partition_info AS SELECT
             't' AS table_id, '-5' AS partition_desc, CAST(1 AS INT) AS version,
             'AppendCommit' AS commit_op, CAST(1 AS BIGINT) AS timestamp,
             '[\"{FIRST}\"]' AS snapshot, '' AS expression, 'public' AS domain, false AS pinned"
        ))
        .await?
        .collect()
        .await?;
        let path = "path/a_0000.parquet";
        let file_ops = serde_json::json!([{"path":path,"file_op":"add","size":10,"file_exist_cols":"id"}]);
        ctx.sql(&format!(
            "CREATE TABLE mirror_meta.data_commit_info AS SELECT
             't' AS table_id, '-5' AS partition_desc, '{FIRST}' AS commit_id,
             true AS committed, CAST(1 AS BIGINT) AS timestamp, '{file_ops}' AS file_ops"
        ))
        .await?
        .collect()
        .await?;

        let config = DiscoveryConfig {
            mirror_namespace: "mirror_meta".to_string(),
            ..Default::default()
        };
        let coordinator = CompactionCoordinator::new(ctx, config);
        let candidates = coordinator.scan_candidates().await?;
        let [partition] = candidates.as_slice() else {
            panic!("expected one partition")
        };
        let SnapshotLoad::Ready(loaded) = coordinator.load_snapshot(partition).await?
        else {
            panic!("expected complete snapshot")
        };
        let [active] = loaded.active_files() else {
            panic!("expected one active file")
        };
        assert_eq!(active.file.path, path);
        assert_eq!(active.file.bucket_id, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn discovery_uses_the_selected_version_and_source_replay_order() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        ctx.sql("CREATE SCHEMA lakesoul_meta")
            .await?
            .collect()
            .await?;
        ctx.sql("CREATE TABLE lakesoul_meta.table_info AS SELECT 't' AS table_id,
            'sales' AS table_namespace, 'orders' AS table_name, 'path' AS table_path,
            '{}' AS table_schema, CAST(NULL AS BYTEA) AS table_schema_arrow_ipc,
            '' AS table_schema_arrow_ipc_json_hash, '{\"hashBucketNum\":\"4\"}' AS properties, ';id' AS partitions,
            'public' AS domain, 'owner' AS creator").await?.collect().await?;
        ctx.sql(&format!(
            "CREATE TABLE lakesoul_meta.partition_info AS SELECT
            't' AS table_id, '-5' AS partition_desc, CAST(version AS INT) AS version,
            'AppendCommit' AS commit_op, CAST(10 AS BIGINT) AS timestamp, snapshot,
            '' AS expression, 'public' AS domain, false AS pinned FROM
            (VALUES (1,'[]'),(2,'[\"{FIRST}\",\"{SECOND}\"]')) v(version,snapshot)"
        ))
        .await?
        .collect()
        .await?;
        let coordinator =
            CompactionCoordinator::new(ctx.clone(), DiscoveryConfig::default());
        let candidates = coordinator.scan_candidates().await?;
        assert_eq!(candidates.len(), 1);
        let p = &candidates[0];
        assert_eq!(p.version, 2);
        let add = |path: &str| serde_json::json!({"path":path,"file_op":"add","size":10,"file_exist_cols":"id"});
        let a = "path/compactdir1/part_0001.parquet";
        let b = "path/part_0002.parquet";
        let c = "path/compactdir2/part_0000.parquet";
        let first = serde_json::json!([add(a), add(b), add(c)]);
        let second = serde_json::json!([{"path":a,"file_op":"del"},add(a)]);
        ctx.sql(&format!("CREATE TABLE lakesoul_meta.data_commit_info AS SELECT 't' AS table_id,
            '-5' AS partition_desc, commit_id, committed, CAST(1 AS BIGINT) AS timestamp, file_ops FROM
            (VALUES ('{SECOND}',true,'{second}'),('{FIRST}',true,'{first}')) v(commit_id,committed,file_ops)"))
            .await?.collect().await?;
        let SnapshotLoad::Ready(snapshot) = coordinator.load_snapshot(p).await? else {
            panic!("complete mirror")
        };
        let actual: Vec<_> = snapshot
            .active_files()
            .iter()
            .map(|f| (f.file.path.as_str(), f.level, f.file.bucket_id))
            .collect();
        assert_eq!(
            actual,
            vec![(c, 2, Some(0)), (a, 1, Some(1)), (b, 0, Some(2))]
        );

        let mut empty = p.clone();
        empty.snapshot.clear();
        assert!(
            matches!(coordinator.load_snapshot(&empty).await?, SnapshotLoad::Ready(s) if s.active_files().is_empty())
        );
        empty.table_id = "missing".into();
        assert!(matches!(
            coordinator.load_snapshot(&empty).await?,
            SnapshotLoad::NotReady {
                table_info_missing: true,
                ..
            }
        ));

        let mut config = DiscoveryConfig::default();
        config
            .excluded_tables
            .insert(("sales".into(), "orders".into()));
        assert!(
            CompactionCoordinator::new(ctx.clone(), config)
                .scan_candidates()
                .await?
                .is_empty()
        );

        for (ops, expected_error) in [
            (Some(serde_json::json!([{"path":a,"file_op":"del"}])), None),
            (
                Some(serde_json::json!([add("path/missing_bucket.parquet")])),
                Some("invalid compaction bucket"),
            ),
            (None, Some("NULL mirror JSON array")),
        ] {
            ctx.deregister_table("lakesoul_meta.data_commit_info")?;
            let json = ops
                .map(|v| format!("'{v}'"))
                .unwrap_or_else(|| "CAST(NULL AS VARCHAR)".into());
            ctx.sql(&format!(
                "CREATE TABLE lakesoul_meta.data_commit_info AS SELECT 't' AS table_id,
                '-5' AS partition_desc, '{FIRST}' AS commit_id, true AS committed,
                CAST(1 AS BIGINT) AS timestamp, {json} AS file_ops"
            ))
            .await?
            .collect()
            .await?;
            let mut one = p.clone();
            one.snapshot.truncate(1);
            let result = coordinator.load_snapshot(&one).await;
            match expected_error {
                Some(message) => {
                    assert!(result.unwrap_err().to_string().contains(message))
                }
                None => assert!(
                    matches!(result?, SnapshotLoad::Ready(s) if s.active_files().is_empty())
                ),
            }
        }

        ctx.deregister_table("lakesoul_meta.data_commit_info")?;
        ctx.sql(&format!(
            "CREATE TABLE lakesoul_meta.data_commit_info AS SELECT 't' AS table_id,
            '-5' AS partition_desc, '{SECOND}' AS commit_id, false AS committed,
            CAST(1 AS BIGINT) AS timestamp, 'invalid until committed' AS file_ops"
        ))
        .await?
        .collect()
        .await?;
        let SnapshotLoad::NotReady {
            missing_commits,
            uncommitted_commits,
            table_info_missing,
        } = coordinator.load_snapshot(p).await?
        else {
            panic!("incomplete mirror")
        };
        assert!(!table_info_missing);
        assert_eq!(missing_commits, vec![Uuid::parse_str(FIRST).unwrap()]);
        assert_eq!(uncommitted_commits, vec![Uuid::parse_str(SECOND).unwrap()]);

        ctx.deregister_table("lakesoul_meta.data_commit_info")?;
        ctx.sql(&format!(
            "CREATE TABLE lakesoul_meta.data_commit_info AS SELECT 't' AS table_id,
            '-5' AS partition_desc, '{FIRST}' AS commit_id, true AS committed,
            CAST(1 AS BIGINT) AS timestamp, '[]' AS file_ops"
        ))
        .await?
        .collect()
        .await?;
        let mut repeated = p.clone();
        repeated.snapshot = vec![p.snapshot[0].clone(), p.snapshot[0].clone()];
        let error = coordinator
            .load_snapshot(&repeated)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!(
                "duplicate snapshot commit {FIRST} in t partition -5 version 2"
            )),
            "{error}"
        );

        // A unique input reference can still match two corrupted mirror rows.
        repeated.snapshot.truncate(1);
        ctx.sql(&format!(
            "INSERT INTO lakesoul_meta.data_commit_info VALUES
            ('t','-5','{FIRST}',true,2,'[]')"
        ))
        .await?
        .collect()
        .await?;
        let error = coordinator
            .load_snapshot(&repeated)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicate mirrored commit row"), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn datafusion_replays_only_the_selected_snapshot_with_exact_file_metadata()
    -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        let delete_commit = "00000000-0000-0000-0000-000000000004";
        let root = "s3://warehouse/orders";
        let part = "day=2026-11-23";
        ctx.sql("CREATE SCHEMA lakesoul_meta")
            .await?
            .collect()
            .await?;
        ctx.sql(&format!(r#"CREATE TABLE lakesoul_meta.table_info AS SELECT
            'orders' AS table_id, 'sales' AS table_namespace, 'orders' AS table_name,
            '{root}' AS table_path, '{{}}' AS table_schema, CAST(NULL AS BYTEA) AS table_schema_arrow_ipc,
            '' AS table_schema_arrow_ipc_json_hash, '{{"hashBucketNum":"4"}}' AS properties,
            'day;id' AS partitions, 'public' AS domain, 'test' AS creator"#)).await?.collect().await?;
        ctx.sql(&format!(r#"CREATE TABLE lakesoul_meta.partition_info AS SELECT
            'orders' AS table_id, '{part}' AS partition_desc, CAST(7 AS INT) AS version,
            'AppendCommit' AS commit_op, CAST(300 AS BIGINT) AS timestamp,
            '["{FIRST}","{delete_commit}","{SECOND}"]' AS snapshot, '' AS expression, 'public' AS domain, false AS pinned"#))
            .await?.collect().await?;
        let coordinator =
            CompactionCoordinator::new(ctx.clone(), DiscoveryConfig::default());
        let candidates = coordinator.scan_candidates().await?;
        assert_eq!(candidates.len(), 1);
        let selected = &candidates[0];

        // The mirror advances after discovery. Loading version 7 must keep its original references.
        let later = "00000000-0000-0000-0000-000000000003";
        ctx.sql(&format!(
            r#"INSERT INTO lakesoul_meta.partition_info VALUES
            ('orders','{part}',8,'AppendCommit',400,'["{later}"]','','public',false)"#
        ))
        .await?
        .collect()
        .await?;
        let a = format!("{root}/compactdir1/{part}/a_0001.parquet");
        let removed = format!("{root}/{part}/removed_0003.parquet");
        let z = format!("{root}/{part}/z_0002.parquet");
        let b = format!("{root}/{part}/b_0000.parquet");
        let high = format!("{root}/compactdir2/{part}/h_0003.parquet");
        let add = |path: &str, size: i64, cols: &str| serde_json::json!({"file_op":"add","path":path,"size":size,"file_exist_cols":cols});
        let del = |path: &str| serde_json::json!({"file_op":"del","path":path});
        let first = serde_json::json!([
            add(&a, 1, "old"),
            add(&removed, 2, "old"),
            add(&z, 17, "id,z")
        ]);
        let deletes = serde_json::json!([del(&removed), del(&a)]);
        let second = serde_json::json!([
            add(&b, 23, "id,b"),
            add(&a, 31, "id,updated"),
            add(&high, 41, "id,h")
        ]);
        let unrelated = serde_json::json!([del(&z)]);
        // Insert in reverse snapshot order and reverse timestamp order. Other tables,
        // partitions and unreferenced versions must never supply an operation.
        ctx.sql(&format!("CREATE TABLE lakesoul_meta.data_commit_info AS SELECT
            table_id, partition_desc, commit_id, true AS committed, CAST(ts AS BIGINT) AS timestamp, file_ops
            FROM (VALUES
              ('orders','{part}','{SECOND}',100,'{second}'),
              ('orders','{part}','{delete_commit}',150,'{deletes}'),
              ('orders','{part}','{FIRST}',200,'{first}'),
              ('orders','{part}','{later}',400,'malformed unreferenced file_ops'),
              ('other','{part}','{FIRST}',500,'{unrelated}'),
              ('orders','day=other','{SECOND}',500,'{unrelated}')
            ) v(table_id,partition_desc,commit_id,ts,file_ops)")).await?.collect().await?;

        let SnapshotLoad::Ready(snapshot) = coordinator.load_snapshot(selected).await?
        else {
            panic!("all commit rows for the selected version are present")
        };
        assert_eq!(snapshot.partition(), selected);
        assert_eq!(snapshot.partition().version, 7);
        let actual: Vec<_> = snapshot
            .active_files()
            .iter()
            .map(|f| (f.level, f.file.clone()))
            .collect();
        let expected = |path: String, bucket, size, cols: &str, timestamp| DataFileInfo {
            partition_desc: part.into(),
            path,
            file_op: "add".into(),
            size,
            bucket_id: Some(bucket),
            modification_time: timestamp,
            file_exist_cols: cols.into(),
        };
        // Hand-written oracle: newest surviving add for a follows a delete;
        // z and b share L0 and must stay in replay order despite their names/timestamps.
        assert_eq!(
            actual,
            vec![
                (2, expected(high, 3, 41, "id,h", 100)),
                (1, expected(a, 1, 31, "id,updated", 100)),
                (0, expected(z, 2, 17, "id,z", 200)),
                (0, expected(b, 0, 23, "id,b", 100)),
            ]
        );
        Ok(())
    }
}
