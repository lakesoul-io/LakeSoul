// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Owned, real Parquet fixtures: an unindexed corpus, the same corpus with a
//! committed IVF+RaBitQ index, nullable fixed vectors for protocol tests, and
//! a controlled text corpus with a committed `text_match` index.
//! Before startup, decode the Parquet inputs and committed segment payloads;
//! require multiple occupied IVF clusters so probes can change coverage.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result, anyhow, ensure};
use datafusion::arrow::array::{Array, Float32Array, Int64Array, ListArray};
use datafusion::arrow::record_batch::RecordBatch;
use lakesoul_common::IndexKind;
use lakesoul_datafusion::index::store_for_files;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_vector::rabitq::segment::read_segment_full;
use lakesoul_vector::{IndexHeader, IndexStore, Metric};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{
    OPERATION_TIMEOUT, StartupState, bounded, ensure_eq, management_execute,
    management_row_count,
};

/// Vector dimension of every fixture column and of the declared index.
pub const DIM: usize = 8;
/// Number of corpus rows; the corpus ids are exactly `0..CORPUS_ROWS`.
pub const CORPUS_ROWS: usize = 64;
/// IVF clusters declared for [`ANN_TABLE`]; the committed index must carry
/// one base segment per cluster.
pub const NLIST: usize = 4;
/// Corpus rows per separated group (four groups of [`DIM`]-wide vectors).
const GROUP_SIZE: usize = 16;
/// RaBitQ width declared for [`ANN_TABLE`]; 1, 3 and 7 are the widths with a
/// kernel.
const TOTAL_BITS: usize = 7;
/// Fixed rotation seed: the corpus and the index must not depend on entropy.
const SEED: u64 = 42;

/// Corpus table without an index: the exact search oracle.
pub const PLAIN_TABLE: &str = "vector_plain";
/// Corpus table with one auto-built, committed IVF+RaBitQ index.
pub const ANN_TABLE: &str = "vector_ann";
/// Fixed-value table for codec, distance and NULL cases (no index).
pub const VALUES_TABLE: &str = "vector_values";
/// Corpus with one committed text (`text_match`) index over `body`:
/// `(1, 'apple pie'), (2, 'banana bread'), (3, 'cherry tart'),
/// (4, 'apple juice')`.
pub const TEXT_TABLE: &str = "vector_text_ann";
/// `text_index_columns` declaration for [`TEXT_TABLE`]; the remaining fields
/// are the documented defaults.
const TEXT_INDEX_CONFIG: &str = r#"[{"column":"body","tokenizer":"jieba"}]"#;

/// Exact float32 input shared by the indexed and unindexed tables. Four
/// separated groups; L2 distance from zero strictly increases with id.
pub fn corpus_vector(id: usize) -> [f32; DIM] {
    let group = (id / GROUP_SIZE) as f32;
    let within = (id % GROUP_SIZE) as f32;
    [
        group * 100.0 + within * 0.25 + 1.0,
        within * 0.125,
        0.5,
        -0.5,
        1.0,
        -1.0,
        0.25,
        -0.25,
    ]
}

impl StartupState {
    /// Create, fill and prove the three vector fixtures.
    ///
    /// Every table is registered for teardown immediately after its
    /// `CREATE EXTERNAL TABLE` succeeds, before any insert or index build, so
    /// a failure below still removes the table's metadata.
    pub(super) async fn create_vector_fixtures(&mut self) -> Result<()> {
        self.create_corpus_table(PLAIN_TABLE, false).await?;
        self.create_corpus_table(ANN_TABLE, true).await?;
        self.create_values_table().await?;
        self.create_text_table().await
    }

    /// Table location inside this suite's disposable warehouse.
    fn vector_location(&self, table: &str) -> String {
        format!(
            "{}/{}/{table}",
            self.warehouse_url.trim_end_matches('/'),
            self.namespace
        )
    }

    async fn create_vector_table(
        &mut self,
        table: &str,
        columns: &str,
        extra_options: &str,
    ) -> Result<()> {
        let sql = format!(
            "CREATE EXTERNAL TABLE lakesoul.{}.{table} ({columns}) \
             STORED AS LAKESOUL LOCATION '{}' \
             OPTIONS ('file_format' 'parquet', 'hashBucketNum' '1'{extra_options})",
            self.namespace,
            self.vector_location(table),
        );
        management_execute(&self.management, &sql)
            .await
            .with_context(|| format!("create vector fixture table={table}"))?;
        // Registered before writing: a failing insert or index build must
        // still drop this table through the normal teardown.
        self.created_tables.push(table.to_owned());
        Ok(())
    }

    async fn create_corpus_table(&mut self, table: &str, indexed: bool) -> Result<()> {
        let options = if indexed {
            format!(", 'vector_index_columns' '{}'", index_config_json())
        } else {
            String::new()
        };
        self.create_vector_table(
            table,
            "id BIGINT NOT NULL PRIMARY KEY, vec FLOAT[] NOT NULL",
            &options,
        )
        .await?;
        let values = corpus_values();
        management_execute(
            &self.management,
            &format!(
                "INSERT INTO lakesoul.{}.{table} (id, vec) VALUES {values}",
                self.namespace
            ),
        )
        .await
        .with_context(|| {
            format!("insert {CORPUS_ROWS}-row corpus into vector fixture table={table}")
        })?;

        ensure_eq(
            &format!("lake {table}"),
            management_row_count(&self.management, &self.namespace, table).await?,
            CORPUS_ROWS,
        )?;
        let expected = corpus_rows();
        let files = self.verify_persisted(table, &expected).await?;
        // The insert above commits the data and, for the indexed table, the
        // auto-built index: both must be complete before this returns.
        if indexed {
            self.verify_committed_index(table, &files).await?;
        } else {
            self.verify_no_index(table).await?;
        }
        Ok(())
    }

    async fn create_values_table(&mut self) -> Result<()> {
        self.create_vector_table(
            VALUES_TABLE,
            "id BIGINT NOT NULL PRIMARY KEY, vec FLOAT[]",
            "",
        )
        .await?;
        management_execute(
            &self.management,
            &format!(
                "INSERT INTO lakesoul.{}.{VALUES_TABLE} (id, vec) VALUES {}",
                self.namespace,
                values_rows_sql()
            ),
        )
        .await
        .context("insert fixed values into vector fixture table=vector_values")?;

        ensure_eq(
            "lake vector_values",
            management_row_count(&self.management, &self.namespace, VALUES_TABLE).await?,
            3,
        )?;
        self.verify_persisted(VALUES_TABLE, &values_rows()).await?;
        self.verify_no_index(VALUES_TABLE).await
    }

    /// A controlled text corpus with one committed `text_match` index. The
    /// UPDATE contract moves row 1 from `apple pie` to `elderberry pie`, so
    /// `apple` loses that row and `elderberry` gains it.
    async fn create_text_table(&mut self) -> Result<()> {
        self.create_vector_table(
            TEXT_TABLE,
            "id BIGINT NOT NULL PRIMARY KEY, body TEXT",
            &format!(", 'text_index_columns' '{TEXT_INDEX_CONFIG}'"),
        )
        .await?;
        management_execute(
            &self.management,
            &format!(
                "INSERT INTO lakesoul.{}.{TEXT_TABLE} (id, body) VALUES \
                 (1, 'apple pie'), (2, 'banana bread'), \
                 (3, 'cherry tart'), (4, 'apple juice')",
                self.namespace
            ),
        )
        .await
        .with_context(|| {
            format!("insert text corpus into vector fixture table={TEXT_TABLE}")
        })?;
        ensure_eq(
            &format!("lake {TEXT_TABLE}"),
            management_row_count(&self.management, &self.namespace, TEXT_TABLE).await?,
            4,
        )?;
        // The insert above commits the data and, inline, the text index.
        self.verify_text_index(TEXT_TABLE).await?;
        Ok(())
    }

    /// The text fixture must have written committed index artifacts before the
    /// server starts, and a `text_match` query on the fixture session must be
    /// served by the text search node; the wire contract then questions the
    /// same index through `UPDATE`.
    async fn verify_text_index(&self, table: &str) -> Result<()> {
        let directory = self.warehouse_dir.join(&self.namespace).join(table);
        let artifacts = index_artifact_files(&directory, IndexKind::Text.directory())?;
        ensure!(
            artifacts > 0,
            "{table}: no committed {} artifact under {}",
            IndexKind::Text.directory(),
            directory.display()
        );

        let sql = format!(
            "SELECT id FROM lakesoul.{}.{table} WHERE text_match(body, 'apple') \
             ORDER BY id LIMIT 10",
            self.namespace
        );
        let batches = bounded(OPERATION_TIMEOUT, "fixture text_match query", async {
            let frame = self
                .management
                .sql(&sql)
                .await
                .map_err(|error| format!("{error:?}"))?;
            frame.collect().await.map_err(|error| format!("{error:?}"))
        })
        .await
        .with_context(|| format!("{table}: fixture text_match query"))?;
        let mut ids = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("{table}: text_match id column is not Int64"))?;
            for row in 0..batch.num_rows() {
                ids.push(column.value(row));
            }
        }
        ensure!(
            ids == vec![1i64, 4],
            "{table}: fixture text_match(apple) returned {ids:?}, expected [1, 4]"
        );

        let explain = bounded(OPERATION_TIMEOUT, "fixture text_match EXPLAIN", async {
            let frame = self
                .management
                .sql(&format!("EXPLAIN VERBOSE {sql}"))
                .await
                .map_err(|error| format!("{error:?}"))?;
            frame.collect().await.map_err(|error| format!("{error:?}"))
        })
        .await
        .with_context(|| format!("{table}: fixture text_match EXPLAIN"))?;
        let mut plan = String::new();
        for batch in &explain {
            for row in 0..batch.num_rows() {
                plan.push_str(&datafusion::arrow::util::display::array_value_to_string(
                    batch.column(1),
                    row,
                )?);
                plan.push('\n');
            }
        }
        ensure!(
            plan.contains("LakeSoulTextSearchExec"),
            "{table}: fixture text_match query did not use the text index:\n{plan}"
        );

        eprintln!(
            "vector fixture proof: table={table} text_index_artifacts={artifacts} \
             committed=true route=LakeSoulTextSearchExec"
        );
        Ok(())
    }

    async fn vector_table_files(&self, table: &str) -> Result<Vec<String>> {
        let files = bounded(OPERATION_TIMEOUT, "read vector fixture data files", async {
            self.meta
                .get_data_files_by_table_name(table, &self.namespace)
                .await
                .map_err(|error| format!("{error:?}"))
        })
        .await?;
        ensure!(
            !files.is_empty(),
            "{table}: committed metadata lists no active data file"
        );
        Ok(files)
    }

    /// Decode every persisted Parquet file of `table` and compare it with the
    /// exact input rows, then return the committed file list.
    async fn verify_persisted(
        &self,
        table: &str,
        expected: &BTreeMap<i64, Option<Vec<f32>>>,
    ) -> Result<Vec<String>> {
        let files = self.vector_table_files(table).await?;
        let table_dir = self.warehouse_dir.join(&self.namespace).join(table);
        let mut actual: BTreeMap<i64, Option<Vec<f32>>> = BTreeMap::new();
        let mut stored_rows = 0usize;
        for file in &files {
            let path = local_file(file)?;
            ensure!(
                path.starts_with(&table_dir),
                "{table}: active data file {file} is outside {}",
                table_dir.display()
            );
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
                .with_context(|| format!("{table}: read persisted Parquet {file}"))?;
            for batch in builder.build()? {
                let batch = batch?;
                stored_rows += batch.num_rows();
                read_rows(&batch, table, &mut actual)?;
            }
        }
        ensure!(
            stored_rows == expected.len(),
            "{table}: persisted {stored_rows} raw row(s), expected {}",
            expected.len()
        );
        for (id, want) in expected {
            let got = actual
                .get(id)
                .ok_or_else(|| anyhow!("{table}: persisted rows are missing id {id}"))?;
            ensure!(
                got == want,
                "{table}: persisted id {id} is {got:?}, expected {want:?}"
            );
        }
        eprintln!(
            "vector fixture proof: table={table} files={} rows={stored_rows} \
             exact_raw_parquet=true",
            files.len()
        );
        Ok(files)
    }

    /// Prove the indexed table really has a committed index for `vec`:
    /// the declared configuration, the control-plane commit view, and every
    /// segment file it references.
    async fn verify_committed_index(&self, table: &str, files: &[String]) -> Result<()> {
        let prefix = shard_index_prefix(files, IndexKind::Vector, "vec");
        let table_dir = self.warehouse_dir.join(&self.namespace).join(table);
        ensure!(
            Path::new(&prefix).starts_with(&table_dir),
            "{table}: index shard prefix {prefix} is outside {}",
            table_dir.display()
        );
        let catalog = self.meta.vector_index_catalog();
        let view = bounded(OPERATION_TIMEOUT, "resolve committed vector index", async {
            catalog
                .resolve(&prefix)
                .await
                .map_err(|error| format!("{error:?}"))
        })
        .await?;
        let view = view.ok_or_else(|| {
            anyhow!("{table}: the insert committed no vector index shard at {prefix}")
        })?;

        let header = IndexHeader::deserialize(&view.header).map_err(|error| {
            anyhow!(
                "{table}: committed index header at {prefix} is unreadable: {error:?}"
            )
        })?;
        ensure!(
            header.dim == DIM,
            "{table}: committed index dim is {}, expected {DIM}",
            header.dim
        );
        ensure!(
            header.metric == Metric::L2,
            "{table}: committed index metric is {:?}, expected L2",
            header.metric
        );
        ensure!(
            header.total_bits == TOTAL_BITS,
            "{table}: committed index total_bits is {}, expected {TOTAL_BITS}",
            header.total_bits
        );

        // The same object store and segment reader the search path uses, so
        // cluster occupancy is proven from the committed segment payloads.
        let store_options: HashMap<String, String> = HashMap::new();
        let store = store_for_files(&files[0], &store_options).map_err(|error| {
            anyhow!("{table}: open the fixture object store: {error:?}")
        })?;
        let istore = IndexStore::new(store, prefix.clone());

        let active: BTreeSet<&str> = files.iter().map(String::as_str).collect();
        let mut covered: BTreeSet<&str> = BTreeSet::new();
        // Counts recorded by the commit and counts read from its payloads: the
        // two must agree.
        let mut stored_by_cluster: BTreeMap<u32, usize> = BTreeMap::new();
        let mut vectors = 0usize;
        for segment in &view.segments {
            let path = Path::new(&prefix).join(&segment.filename);
            let size = std::fs::metadata(&path)
                .with_context(|| {
                    format!(
                        "{table}: committed segment {} does not exist",
                        path.display()
                    )
                })?
                .len();
            ensure!(
                size == segment.file_size,
                "{table}: segment {} is {size} bytes, commit records {}",
                path.display(),
                segment.file_size
            );
            for covered_file in &segment.data_files {
                ensure!(
                    active.contains(covered_file.as_str()),
                    "{table}: segment {} claims unrelated data file {covered_file}",
                    segment.filename
                );
                covered.insert(covered_file.as_str());
            }
            let stored =
                bounded(OPERATION_TIMEOUT, "read committed index segment", async {
                    read_segment_full(&istore, &segment.filename)
                        .await
                        .map_err(|error| format!("{error:?}"))
                })
                .await?;
            ensure!(
                stored.cluster_id == segment.cluster_id
                    && stored.version == segment.segment_version,
                "{table}: segment {} holds cluster {} version {}, commit records {} version {}",
                segment.filename,
                stored.cluster_id,
                stored.version,
                segment.cluster_id,
                segment.segment_version
            );
            ensure!(
                stored.ids.len() == segment.num_vectors as usize,
                "{table}: segment {} holds {} id(s), commit records {}",
                segment.filename,
                stored.ids.len(),
                segment.num_vectors
            );
            *stored_by_cluster.entry(stored.cluster_id).or_default() += stored.ids.len();
            vectors += stored.ids.len();
        }
        ensure!(
            vectors == CORPUS_ROWS,
            "{table}: the committed index holds {vectors} vector(s), expected {CORPUS_ROWS}"
        );
        ensure!(
            covered.len() == active.len()
                && covered.iter().all(|file| active.contains(*file)),
            "{table}: committed index covers {covered:?}, active files are {active:?}"
        );
        ensure!(
            stored_by_cluster.len() == NLIST,
            "{table}: expected {NLIST} committed clusters, actual={stored_by_cluster:?}"
        );
        // The probes cases need a real partition of the candidates: with fewer
        // than two non-empty clusters `ivfflat.probes = 1` could not differ
        // from probing everything.
        let nonempty = stored_by_cluster
            .values()
            .filter(|count| **count > 0)
            .count();
        ensure!(
            nonempty > 1,
            "{table}: only {nonempty} non-empty IVF cluster(s) in {stored_by_cluster:?}; \
             probes cannot change the candidate set"
        );

        eprintln!(
            "vector fixture proof: table={table} index_prefix={prefix} commit={} \
             generation={} version={} dim={} metric={:?} total_bits={} \
             vectors={vectors} stored_vectors_by_cluster={stored_by_cluster:?} \
             nonempty_clusters={nonempty} segments={} segment_payloads_read=true",
            view.commit_id,
            view.generation,
            view.version,
            header.dim,
            header.metric,
            header.total_bits,
            view.segments.len(),
        );
        Ok(())
    }

    /// Prove no usable index exists in either the control plane or storage.
    async fn verify_no_index(&self, table: &str) -> Result<()> {
        let catalog = self.meta.vector_index_catalog();
        let directory = self.vector_location(table);
        let shards = bounded(OPERATION_TIMEOUT, "list vector index shards", async {
            catalog
                .list_shards_under(&directory)
                .await
                .map_err(|error| format!("{error:?}"))
        })
        .await?;
        ensure!(
            shards.is_empty(),
            "{table} must have no vector index, but the control plane holds {shards:?}"
        );

        let index_dir = self
            .warehouse_dir
            .join(&self.namespace)
            .join(table)
            .join(IndexKind::Vector.directory());
        ensure!(
            !index_dir.exists(),
            "{table} must have no vector index, but {} exists",
            index_dir.display()
        );
        eprintln!(
            "vector fixture proof: table={table} index_shards=0 index_dir_absent=true"
        );
        Ok(())
    }
}

/// The `vector_index_columns` value for [`ANN_TABLE`]; every field is
/// explicit so the built index never depends on a default.
fn index_config_json() -> String {
    format!(
        r#"[{{"column":"vec","dim":{DIM},"nlist":{NLIST},"total_bits":{TOTAL_BITS},"metric":"L2","seed":{SEED}}}]"#
    )
}

/// SQL `VALUES` body for the whole corpus, ids `0..CORPUS_ROWS`.
fn corpus_values() -> String {
    (0..CORPUS_ROWS)
        .map(|id| {
            let components = corpus_vector(id)
                .iter()
                .map(|component| literal(*component))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({id}, ARRAY[{components}])")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// SQL `VALUES` body for [`VALUES_TABLE`]; row 3 stores a NULL vector.
fn values_rows_sql() -> String {
    values_rows()
        .iter()
        .map(|(id, vector)| match vector {
            None => format!("({id}, NULL)"),
            Some(components) => format!(
                "({id}, ARRAY[{}])",
                components
                    .iter()
                    .map(|component| literal(*component))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Expected raw rows of the corpus tables.
fn corpus_rows() -> BTreeMap<i64, Option<Vec<f32>>> {
    (0..CORPUS_ROWS)
        .map(|id| (id as i64, Some(corpus_vector(id).to_vec())))
        .collect()
}

/// Expected raw rows of [`VALUES_TABLE`]: independent fixed values, so a
/// corpus change cannot silently move the codec cases.
fn values_rows() -> BTreeMap<i64, Option<Vec<f32>>> {
    BTreeMap::from([
        (1, Some(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])),
        (2, Some(vec![-2.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
        (3, None),
    ])
}

/// Exact decimal text for one fixture component.
///
/// Every component is a multiple of `0.125` within ±804, so the shortest
/// round-tripping text is exact and never uses an exponent; a trailing `.0`
/// keeps each element a float literal inside `ARRAY[...]`.
fn literal(component: f32) -> String {
    let text = component.to_string();
    assert!(
        !text.contains(['e', 'E']),
        "fixture component {component} must not need an exponent"
    );
    if text.contains('.') {
        text
    } else {
        format!("{text}.0")
    }
}

/// Collect `(id, vec)` pairs of one decoded Parquet batch.
fn read_rows(
    batch: &RecordBatch,
    table: &str,
    rows: &mut BTreeMap<i64, Option<Vec<f32>>>,
) -> Result<()> {
    let schema = batch.schema();
    let ids = batch
        .column(schema.index_of("id")?)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("{table}: stored `id` is not INT64"))?;
    let vectors = batch
        .column(schema.index_of("vec")?)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("{table}: stored `vec` is not a list"))?;
    for row in 0..batch.num_rows() {
        ensure!(!ids.is_null(row), "{table}: stored a NULL primary key");
        let vector = if vectors.is_null(row) {
            None
        } else {
            let elements = vectors.value(row);
            let elements = elements
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    anyhow!(
                        "{table}: stored `vec` elements are {}, expected Float32",
                        elements.data_type()
                    )
                })?;
            ensure!(
                elements.len() == DIM,
                "{table}: stored a {}-dimension vector, expected {DIM}",
                elements.len()
            );
            Some(elements.values().to_vec())
        };
        ensure!(
            rows.insert(ids.value(row), vector).is_none(),
            "{table}: id {} is stored twice",
            ids.value(row)
        );
    }
    Ok(())
}

/// Existing absolute local file behind a `file://` metadata path.
fn local_file(file: &str) -> Result<&Path> {
    let path = Path::new(file.strip_prefix("file://").unwrap_or(file));
    ensure!(
        path.is_absolute() && path.is_file(),
        "not an existing absolute local file: {file}"
    );
    Ok(path)
}

/// Count the regular files under the `directory` (for example `_text_index`)
/// of one table directory.
///
/// A committed index writes at least one artifact; the contract starts the
/// server only after that file exists.
fn index_artifact_files(table_dir: &Path, directory: &str) -> Result<usize> {
    if !table_dir.exists() {
        return Ok(0);
    }
    let mut files = 0;
    for entry in std::fs::read_dir(table_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some(directory) {
            files += regular_files(&path)?;
        } else {
            files += index_artifact_files(&path, directory)?;
        }
    }
    Ok(files)
}

/// Regular files below `directory`, recursively.
fn regular_files(directory: &Path) -> Result<usize> {
    let mut files = 0;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            files += regular_files(&entry.path())?;
        } else if kind.is_file() {
            files += 1;
        }
    }
    Ok(files)
}
