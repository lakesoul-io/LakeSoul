// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Text index benchmark scenario runner.
//!
//! Mirrors `vector_rebuild_bench.rs`: it writes real corpus documents as
//! LakeSoul data files, maintains the Tantivy text index through the
//! production build/compaction paths, searches with the production reader
//! helpers and evaluates against an exact single-index BM25 baseline and the
//! dataset's qrels.
//!
//! Results and methodology: website/docs/03-Usage Docs/19-text-index-benchmark.md.
//!
//! Scenarios:
//!   * `build`   — fresh build scaling + first quality/latency measurement
//!   * `search`  — reuse an existing work dir; sweep per-shard candidate counts
//!   * `stream`  — incremental update rounds under a rebuild policy
//!   * `trigger` — incremental rounds with no rebuild, recording drift signals
//!   * `sql`     — end-to-end DataFusion SQL (write + index-backed search)
//!
//! Usage:
//!   cargo bench -p lakesoul-datafusion --bench text_index_bench -- \
//!       --scenario build --corpus docs.jsonl --queries queries.jsonl \
//!       --qrels qrels.tsv --limit 100000 --work-dir /tmp/text-bench/msmarco

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use lakesoul_common::IndexKind;
use lakesoul_datafusion::index::IndexManagementConfig;
use lakesoul_datafusion::text_index::{
    TextIndexParams, TextIndexTableConfig, auto_build_text_index,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::index::Candidate;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_io::text::search::search_resolved_shard;
use lakesoul_io::writer::create_writer_with_io_config;
use lakesoul_metadata::index_catalog::{IndexCatalog, IndexCommitView};
use lakesoul_text::tantivy::schema::TantivyDocument;
use lakesoul_text::tantivy::{Index, IndexWriter};
use lakesoul_text::{
    TextIndexConfig, TextSchema, TextSplitEntry, matching_ids, register_tokenizers,
    search_index,
};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use serde_json::{Value, json};

const PK_COLUMN: &str = "id";
const DOC_COLUMN: &str = "content";
const WRITE_BATCH_SIZE: usize = 8192;
/// A ratio small enough that any non-empty delta triggers a rebuild.
const FORCE_REBUILD_RATIO: f32 = 1e-9;
/// Quality metrics are computed from the top 100 results.
const QUALITY_K: usize = 100;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Build,
    Search,
    Stream,
    Trigger,
    Sql,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    None,
    Auto,
    Periodic,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Drift {
    /// Append unseen pool documents (uniform growth).
    Append,
    /// Rewrite live documents with pool texts (stale index entries).
    Rewrite,
    /// Delete live documents (tombstone semantics).
    Delete,
    /// Append pool documents from a disjoint id range (distribution shift).
    TopicShift,
}

struct Args {
    scenario: Scenario,
    corpus: PathBuf,
    queries: PathBuf,
    qrels: Option<PathBuf>,
    limit: usize,
    query_limit: usize,
    work_dir: PathBuf,
    out: Option<PathBuf>,
    tokenizer: String,
    with_positions: bool,
    stored: bool,
    shards: usize,
    top_k: usize,
    candidates: usize,
    candidate_sweep: Vec<usize>,
    verify: bool,
    clear_split_cache: bool,
    reuse: bool,
    rounds: usize,
    per_round: usize,
    checkpoint_every: usize,
    policy: Policy,
    max_delta_ratio: f32,
    period: usize,
    drift: Drift,
    table: String,
    sql_hash_buckets: usize,
    sql_format: String,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            scenario: Scenario::Build,
            corpus: PathBuf::new(),
            queries: PathBuf::new(),
            qrels: None,
            limit: 100_000,
            query_limit: 200,
            work_dir: PathBuf::from("/tmp/lakesoul_test/text_bench"),
            out: None,
            tokenizer: "jieba".to_string(),
            with_positions: true,
            stored: false,
            shards: 1,
            top_k: 10,
            candidates: 100,
            candidate_sweep: vec![10, 20, 40, 100, 200],
            verify: true,
            clear_split_cache: false,
            reuse: false,
            rounds: 10,
            per_round: 10_000,
            checkpoint_every: 1,
            policy: Policy::Auto,
            max_delta_ratio: 1.0,
            period: 3,
            drift: Drift::Append,
            table: "text_bench_sql".to_string(),
            sql_hash_buckets: 1,
            sql_format: "parquet".to_string(),
        }
    }
}

fn usage() -> String {
    r#"text_index_bench — text index benchmark scenario runner

  --scenario <build|search|stream|trigger|sql>  Scenario to run (default: build)
  --corpus <path>                           Corpus JSONL ({"id","text"})
  --queries <path>                          Queries JSONL ({"id","text"})
  --qrels <path>                            Qrels TSV (qid, 0, docid, grade)
  --limit <n>                               Documents to load as the base corpus
  --query-limit <n>                         Maximum queries to use
  --work-dir <dir>                          Table/work directory
  --out <path>                              Write the JSON summary here
  --tokenizer <name>                        jieba|default|en_stem|whitespace|raw
  --no-positions                            Disable term positions
  --stored                                  Store the original text in the index
  --shards <n>                              Hash bucket (index shard) count
  --top-k <k>                               Result size for latency measurements
  --candidates <n>                          Candidates fetched per shard
  --candidate-sweep <a,b,c>                 Candidate counts for the search scenario
  --no-verify                               Skip the exact verification pass
  --clear-split-cache                       Remove the local split cache first
  --reuse                                   Reuse an existing work dir (search)
  --rounds <n>                              Update rounds (stream/trigger/sql)
  --per-round <n>                           Documents per update round
  --checkpoint-every <n>                    Measure quality every n rounds
  --policy <none|auto|periodic|always>      Rebuild policy
  --max-delta-ratio <f>                     Auto rebuild trigger (default 1.0)
  --period <n>                              Periodic rebuild interval
  --drift <append|rewrite|delete|topic-shift> Update mode
  --table <name>                            SQL scenario table name
  --sql-hash-buckets <n>                    SQL scenario hash buckets
  --sql-format <parquet|vortex>             SQL scenario file format
"#
    .to_string()
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = || -> Result<String, String> {
            iter.next()
                .ok_or_else(|| format!("missing value for {flag}"))
        };
        match flag.as_str() {
            "--scenario" => {
                args.scenario = match value()?.as_str() {
                    "build" => Scenario::Build,
                    "search" => Scenario::Search,
                    "stream" => Scenario::Stream,
                    "trigger" => Scenario::Trigger,
                    "sql" => Scenario::Sql,
                    other => return Err(format!("unknown scenario: {other}")),
                }
            }
            "--corpus" => args.corpus = value()?.into(),
            "--queries" => args.queries = value()?.into(),
            "--qrels" => args.qrels = Some(value()?.into()),
            "--limit" => args.limit = parse_num(&value()?)?,
            "--query-limit" => args.query_limit = parse_num(&value()?)?,
            "--work-dir" => args.work_dir = value()?.into(),
            "--out" => args.out = Some(value()?.into()),
            "--tokenizer" => args.tokenizer = value()?,
            "--no-positions" => args.with_positions = false,
            "--stored" => args.stored = true,
            "--shards" => args.shards = parse_num(&value()?)?,
            "--top-k" => args.top_k = parse_num(&value()?)?,
            "--candidates" => args.candidates = parse_num(&value()?)?,
            "--candidate-sweep" => {
                args.candidate_sweep = value()?
                    .split(',')
                    .map(|part| parse_num(part.trim()))
                    .collect::<Result<_, _>>()?
            }
            "--no-verify" => args.verify = false,
            "--clear-split-cache" => args.clear_split_cache = true,
            "--reuse" => args.reuse = true,
            "--rounds" => args.rounds = parse_num(&value()?)?,
            "--per-round" => args.per_round = parse_num(&value()?)?,
            "--checkpoint-every" => args.checkpoint_every = parse_num(&value()?)?.max(1),
            "--policy" => {
                args.policy = match value()?.as_str() {
                    "none" => Policy::None,
                    "auto" => Policy::Auto,
                    "periodic" => Policy::Periodic,
                    "always" => Policy::Always,
                    other => return Err(format!("unknown policy: {other}")),
                }
            }
            "--max-delta-ratio" => {
                args.max_delta_ratio = value()?
                    .parse()
                    .map_err(|e| format!("invalid max-delta-ratio: {e}"))?
            }
            "--period" => args.period = parse_num(&value()?)?.max(1),
            "--drift" => {
                args.drift = match value()?.as_str() {
                    "append" | "uniform" => Drift::Append,
                    "rewrite" => Drift::Rewrite,
                    "delete" => Drift::Delete,
                    "topic-shift" | "shift" => Drift::TopicShift,
                    other => return Err(format!("unknown drift: {other}")),
                }
            }
            "--table" => args.table = value()?,
            "--sql-hash-buckets" => args.sql_hash_buckets = parse_num(&value()?)?,
            "--sql-format" => args.sql_format = value()?,
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            // Passed by `cargo bench` to harness=false binaries.
            "--bench" | "--test" => {}
            other => return Err(format!("unknown argument: {other}\n\n{}", usage())),
        }
    }
    if args.corpus.as_os_str().is_empty() {
        return Err("--corpus is required".to_string());
    }
    if args.queries.as_os_str().is_empty() {
        return Err("--queries is required".to_string());
    }
    if !matches!(args.sql_format.as_str(), "parquet" | "vortex") {
        return Err(format!("unknown sql format: {}", args.sql_format));
    }
    Ok(args)
}

fn parse_num(value: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|e| format!("invalid number '{value}': {e}"))
}

/// Natural-language queries may contain characters Tantivy's query parser
/// treats as syntax (`+`, `-`, `(`, `:` ...).  A serving layer must escape
/// them; the benchmark normalizes each query to words before it reaches the
/// parser, so every query is comparable across configurations.
fn sanitize_query(query: &str) -> String {
    let cleaned: String = query
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c.is_whitespace() {
                c
            } else {
                ' '
            }
        })
        .collect();
    cleaned.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

struct Doc {
    key: u64,
    text: String,
}

struct Query {
    id: String,
    text: String,
}

struct Dataset {
    docs: Vec<Doc>,
    queries: Vec<Query>,
    /// qid -> doc key -> grade, restricted to the loaded corpus.
    qrels: HashMap<String, HashMap<u64, u32>>,
}

impl Dataset {
    fn load(args: &Args) -> Result<Self, String> {
        let mut docs = Vec::new();
        let mut key_by_id: HashMap<String, u64> = HashMap::new();
        for line in reader(&args.corpus)?.lines() {
            let line = line.map_err(|e| format!("read line: {e}"))?;
            if line.trim().is_empty() {
                continue;
            }
            if docs.len() >= args.limit {
                break;
            }
            let value: Value =
                serde_json::from_str(&line).map_err(|e| format!("corpus line: {e}"))?;
            let id = value
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| "corpus entry without an id".to_string())?
                .to_string();
            let text = value
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let key = docs.len() as u64;
            key_by_id.insert(id, key);
            docs.push(Doc { key, text });
        }

        let mut queries: Vec<Query> = Vec::new();
        for line in reader(&args.queries)?.lines() {
            let line = line.map_err(|e| format!("read line: {e}"))?;
            if line.trim().is_empty() {
                continue;
            }
            let value: Value =
                serde_json::from_str(&line).map_err(|e| format!("query line: {e}"))?;
            let id = value
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| "query entry without an id".to_string())?
                .to_string();
            let text = value
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            queries.push(Query {
                id,
                text: sanitize_query(&text),
            });
        }

        let mut qrels: HashMap<String, HashMap<u64, u32>> = HashMap::new();
        if let Some(path) = &args.qrels {
            for line in reader(path)?.lines() {
                let line = line.map_err(|e| format!("read line: {e}"))?;
                if line.trim().is_empty() {
                    continue;
                }
                let fields: Vec<&str> = line.split('\t').collect();
                if fields.len() < 4 {
                    continue;
                }
                let (qid, docid, grade) = (fields[0], fields[2], fields[3]);
                let Some(key) = key_by_id.get(docid) else {
                    continue;
                };
                let grade: u32 = grade.trim().parse().unwrap_or(1);
                qrels
                    .entry(qid.to_string())
                    .or_default()
                    .insert(*key, grade);
            }
            // Keep only queries that have at least one judged document in the
            // loaded corpus, and drop judged documents outside it.
            queries.retain(|query| {
                qrels
                    .get(&query.id)
                    .is_some_and(|judged| !judged.is_empty())
            });
        } else {
            // Without qrels, quality is only measured against the exact BM25
            // baseline.
            qrels.clear();
        }
        if queries.len() > args.query_limit {
            queries.truncate(args.query_limit);
        }
        if args.qrels.is_some() && queries.is_empty() {
            return Err("no query has a judged document in the loaded corpus".to_string());
        }
        Ok(Self {
            docs,
            queries,
            qrels,
        })
    }

    /// Documents used as the update pool: everything after the base limit.
    /// The loader stops at `limit`, so the pool is re-read lazily.
    fn load_pool(args: &Args) -> Result<Vec<Doc>, String> {
        // Held-out documents from the tail of the corpus (a disjoint id range
        // from the base prefix), so appends simulate a distribution shift.
        let capacity = args.rounds * args.per_round + 1024;
        let mut tail: std::collections::VecDeque<Doc> =
            std::collections::VecDeque::with_capacity(capacity);
        let mut seen = 0usize;
        for line in reader(&args.corpus)?.lines() {
            let line = line.map_err(|e| format!("read line: {e}"))?;
            if line.trim().is_empty() {
                continue;
            }
            let key = seen;
            seen += 1;
            if key < args.limit {
                continue;
            }
            let value: Value =
                serde_json::from_str(&line).map_err(|e| format!("corpus line: {e}"))?;
            let text = value
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if tail.len() == capacity {
                tail.pop_front();
            }
            tail.push_back(Doc {
                key: key as u64,
                text,
            });
        }
        Ok(tail.into_iter().collect())
    }
}

fn reader(path: &Path) -> Result<Box<dyn BufRead>, String> {
    let file =
        fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    Ok(Box::new(BufReader::new(file)))
}

// ---------------------------------------------------------------------------
// Work dir / IO helpers
// ---------------------------------------------------------------------------

fn prepare_work_dir(path: &Path) -> Result<PathBuf, String> {
    fs::create_dir_all(path).map_err(|e| format!("create work dir: {e}"))?;
    Ok(path.to_path_buf())
}

fn clean_work_dir(path: &Path) -> Result<(), String> {
    let index_dir = path.join("_text_index");
    if index_dir.exists() {
        fs::remove_dir_all(&index_dir).map_err(|e| format!("clean index: {e}"))?;
    }
    for file in find_data_files(path) {
        let _ = fs::remove_file(file);
    }
    let gt = path.join("_gt_index");
    if gt.exists() {
        let _ = fs::remove_dir_all(gt);
    }
    Ok(())
}

fn find_data_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "_text_index")
                    || path.file_name().is_some_and(|name| name == "_gt_index")
                {
                    continue;
                }
                stack.push(path);
            } else if path
                .extension()
                .is_some_and(|ext| ext == "parquet" || ext == "vortex")
            {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn index_size_bytes(root: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = path.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

fn rss_mb() -> f64 {
    status_kb("VmRSS").unwrap_or(0) as f64 / 1024.0
}

fn peak_rss_mb() -> f64 {
    status_kb("VmHWM").unwrap_or(0) as f64 / 1024.0
}

fn status_kb(key: &str) -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            let value = rest.trim_start_matches(':').trim();
            return value
                .split_whitespace()
                .next()
                .and_then(|value| value.parse().ok());
        }
    }
    None
}

fn print_summary(value: &Value) {
    println!("{}", serde_json::to_string_pretty(value).unwrap());
}

fn write_summary(args: &Args, value: &Value) -> Result<(), String> {
    if let Some(path) = &args.out {
        fs::write(path, serde_json::to_vec_pretty(value).unwrap())
            .map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    Ok(())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(PK_COLUMN, DataType::UInt64, false),
        Field::new(DOC_COLUMN, DataType::Utf8, true),
    ]))
}

fn doc_batch_sql(docs: &[(u64, String)]) -> Result<RecordBatch, String> {
    // The SQL table declares `id BIGINT`, so the batch uses Int64 ids.
    let schema = Arc::new(Schema::new(vec![
        Field::new(PK_COLUMN, DataType::Int64, false),
        Field::new(DOC_COLUMN, DataType::Utf8, true),
    ]));
    let ids: Vec<i64> = docs.iter().map(|(key, _)| *key as i64).collect();
    let texts: Vec<String> = docs.iter().map(|(_, text)| text.clone()).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(texts)) as ArrayRef,
        ],
    )
    .map_err(|e| format!("document batch: {e}"))
}

fn doc_batch(docs: &[(u64, String)]) -> Result<RecordBatch, String> {
    let ids: Vec<u64> = docs.iter().map(|(key, _)| *key).collect();
    let texts: Vec<String> = docs.iter().map(|(_, text)| text.clone()).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(texts)) as ArrayRef,
        ],
    )
    .map_err(|e| format!("document batch: {e}"))
}

/// Write documents as LakeSoul data files under `work_dir`, hash-distributed
/// over `shards` buckets (the production write path supplies the bucket ids
/// in the file names).
async fn write_docs(
    work_dir: &Path,
    shards: usize,
    format: PhysicalFormat,
    docs: &[(u64, String)],
) -> Result<Vec<String>, String> {
    // The io writer runs with `target_partitions = 1`, so a caller owns one
    // bucket (production distributed writers do the same).  Distribute the
    // rows by `key % shards` and name each file with its bucket id.
    let mut files = Vec::new();
    if shards <= 1 {
        files.extend(write_group(work_dir, format, docs).await?);
        return Ok(files);
    }
    let mut groups: Vec<Vec<(u64, String)>> = vec![Vec::new(); shards];
    for (key, text) in docs {
        groups[*key as usize % shards].push((*key, text.clone()));
    }
    for (bucket, group) in groups.iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        for file in write_group(work_dir, format, group).await? {
            files.push(rename_bucket(&file, bucket)?);
        }
    }
    Ok(files)
}

/// Write one group of documents with a single-bucket writer.
async fn write_group(
    work_dir: &Path,
    format: PhysicalFormat,
    docs: &[(u64, String)],
) -> Result<Vec<String>, String> {
    let config = LakeSoulIOConfigBuilder::new()
        .with_prefix(work_dir.to_string_lossy().to_string())
        .with_schema(schema())
        .with_primary_keys(vec![PK_COLUMN.to_string()])
        .with_hash_bucket_num("1")
        .with_physical_format(format)
        .with_batch_size(WRITE_BATCH_SIZE)
        .build();
    let mut writer = create_writer_with_io_config(config)
        .await
        .map_err(|e| format!("create writer: {e}"))?;
    for chunk in docs.chunks(WRITE_BATCH_SIZE) {
        let batch = doc_batch(chunk)?;
        writer
            .write_record_batch(batch)
            .await
            .map_err(|e| format!("write batch: {e}"))?;
    }
    let outputs = writer
        .flush_and_close()
        .await
        .map_err(|e| format!("flush: {e}"))?;
    Ok(outputs.into_iter().map(|output| output.file_path).collect())
}

/// Rename a written file's `_0000` suffix to the actual bucket id.
fn rename_bucket(file: &str, bucket: usize) -> Result<String, String> {
    let (prefix, local) = match file.strip_prefix("file://") {
        Some(rest) => ("file://", rest),
        None => ("", file),
    };
    let path = Path::new(local);
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| format!("unexpected data file name: {file}"))?;
    let base = stem
        .rsplit_once('_')
        .map(|(base, _)| base)
        .ok_or_else(|| format!("unexpected data file name: {file}"))?;
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("parquet");
    let renamed = path.with_file_name(format!("{base}_{bucket:04}.{extension}"));
    fs::rename(path, &renamed).map_err(|e| format!("rename {file}: {e}"))?;
    Ok(format!("{prefix}{}", renamed.display()))
}

// ---------------------------------------------------------------------------
// Text index configuration
// ---------------------------------------------------------------------------

fn text_config(args: &Args) -> TextIndexConfig {
    let _ = &args.tokenizer;
    TextIndexConfig {
        column_name: DOC_COLUMN.to_string(),
        tokenizer: args.tokenizer.clone(),
        with_positions: args.with_positions,
        stored: args.stored,
    }
}

fn table_config(args: &Args, rebuild_mode: &str, ratio: f32) -> TextIndexTableConfig {
    TextIndexTableConfig {
        column: DOC_COLUMN.to_string(),
        params: TextIndexParams {
            tokenizer: args.tokenizer.clone(),
            with_positions: args.with_positions,
            stored: args.stored,
        },
        management: IndexManagementConfig {
            rebuild_mode: rebuild_mode.to_string(),
            max_delta_ratio: ratio,
            // Production defaults: superseded generations are collected
            // after the grace period, so index size includes files still
            // protected for in-flight readers.
            gc_enabled: true,
            gc_grace_seconds: 3600,
            gc_keep_generations: 1,
        },
    }
}

fn bench_store() -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

async fn bench_catalog() -> Result<IndexCatalog<TextSplitEntry>, String> {
    lakesoul_metadata::MetaDataClient::from_env()
        .await
        .map(|client| client.index_catalog(IndexKind::Text))
        .map_err(|e| format!("metadata client: {e}"))
}

fn prefixes_of(files: &[String], column: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    let mut seen = HashSet::new();
    for file in files {
        let prefix =
            shard_index_prefix(std::slice::from_ref(file), IndexKind::Text, column);
        if seen.insert(prefix.clone()) {
            prefixes.push(prefix);
        }
    }
    prefixes
}

fn resolved_of(prefix: &str, view: &IndexCommitView<TextSplitEntry>) -> ResolvedIndex {
    ResolvedIndex {
        kind: IndexKind::Text,
        index_prefix: prefix.to_string(),
        commit_id: view.commit_id,
        generation: view.generation,
        version: view.version,
        header: view.header.clone(),
        segments: serde_json::to_value(&view.segments).unwrap_or(Value::Array(vec![])),
    }
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

struct SearchOutcome {
    /// Merged candidate ids, best score first.
    ids: Vec<u64>,
    candidates: usize,
    dropped_stale: usize,
    verify_ms: f64,
}

/// Search every shard and merge the per-shard candidates like the SQL exec:
/// per-shard top-`per_shard_k`, then a global score-ordered truncation.
async fn search_shards(
    store: &Arc<dyn ObjectStore>,
    catalog: &IndexCatalog<TextSplitEntry>,
    prefixes: &[String],
    config: &TextIndexConfig,
    query: String,
    per_shard_k: usize,
    global_k: usize,
    verify: bool,
    live: &HashMap<u64, String>,
) -> Result<SearchOutcome, String> {
    let mut best: HashMap<u64, f32> = HashMap::new();
    for prefix in prefixes {
        let Some(view) = catalog
            .resolve_cached(prefix)
            .await
            .map_err(|e| format!("resolve {prefix}: {e}"))?
        else {
            continue;
        };
        let resolved = resolved_of(prefix, &view);
        let hits = search_resolved_shard(store, &resolved, &query, per_shard_k)
            .await
            .map_err(|e| format!("search {prefix}: {e}"))?;
        for candidate in hits {
            if let Some(score) = candidate.score {
                let entry = best.entry(candidate.id).or_insert(f32::NEG_INFINITY);
                if score > *entry {
                    *entry = score;
                }
            }
        }
    }
    let mut merged: Vec<Candidate> = best
        .into_iter()
        .map(|(id, score)| Candidate::scored(id, score))
        .collect();
    merged.sort_by(|left, right| {
        right
            .score
            .unwrap_or(f32::NEG_INFINITY)
            .total_cmp(&left.score.unwrap_or(f32::NEG_INFINITY))
            .then_with(|| left.id.cmp(&right.id))
    });
    let candidates = merged.len();

    let mut dropped_stale = 0usize;
    let verify_start = Instant::now();
    let ids: Vec<u64> = if verify {
        let rows: Vec<(u64, Option<String>)> = merged
            .iter()
            .filter_map(|hit| live.get(&hit.id).map(|text| (hit.id, Some(text.clone()))))
            .collect();
        let matched =
            matching_ids(config, &rows, &query).map_err(|e| format!("verify: {e}"))?;
        dropped_stale = merged.len() - matched.len();
        merged
            .into_iter()
            .map(|hit| hit.id)
            .filter(|id| matched.contains(id))
            .take(global_k)
            .collect()
    } else {
        merged
            .into_iter()
            .map(|hit| hit.id)
            .take(global_k)
            .collect()
    };
    let verify_ms = verify_start.elapsed().as_secs_f64() * 1000.0;
    Ok(SearchOutcome {
        ids,
        candidates,
        dropped_stale,
        verify_ms,
    })
}

// ---------------------------------------------------------------------------
// Exact BM25 baseline (single Tantivy index over the live documents)
// ---------------------------------------------------------------------------

struct GtIndex {
    index: Index,
    dir: PathBuf,
}

impl GtIndex {
    fn build(
        work_dir: &Path,
        config: &TextIndexConfig,
        live: &HashMap<u64, String>,
    ) -> Result<Self, String> {
        let dir = work_dir.join("_gt_index");
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| format!("clean gt: {e}"))?;
        }
        fs::create_dir_all(&dir).map_err(|e| format!("create gt: {e}"))?;
        let schema = TextSchema::build(config);
        let index = Index::create_in_dir(&dir, schema.schema.clone())
            .map_err(|e| e.to_string())?;
        register_tokenizers(&index);
        let mut writer: IndexWriter = index
            .writer_with_num_threads(1, 128 * 1024 * 1024)
            .map_err(|e| e.to_string())?;
        for (key, text) in live {
            let mut document = TantivyDocument::new();
            document.add_u64(schema.pk_field, *key);
            document.add_text(schema.text_field, text.as_str());
            writer.add_document(document).map_err(|e| e.to_string())?;
        }
        writer.commit().map_err(|e| e.to_string())?;
        Ok(Self { index, dir })
    }

    fn top_k(&self, query: &str, k: usize) -> Result<Vec<u64>, String> {
        // The production search helper parses the query and reads the primary
        // keys exactly like the index path does.
        Ok(search_index(&self.index, query, k)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|hit| hit.id)
            .collect())
    }
}

impl Drop for GtIndex {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

struct Timing {
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    qps: f64,
}

async fn measure<F, Fut>(
    queries: &[Query],
    mut run: F,
) -> Result<(Vec<SearchOutcome>, Timing), String>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<SearchOutcome, String>>,
{
    let mut latencies = Vec::with_capacity(queries.len());
    let mut outcomes = Vec::with_capacity(queries.len());
    let started = Instant::now();
    for query in queries {
        let t = Instant::now();
        let outcome = run(query.text.clone()).await?;
        latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        outcomes.push(outcome);
    }
    let total = started.elapsed().as_secs_f64();
    latencies.sort_by(f64::total_cmp);
    let mean = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;
    let pct = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        let idx = ((latencies.len() - 1) as f64 * p).round() as usize;
        latencies[idx]
    };
    Ok((
        outcomes,
        Timing {
            mean_ms: mean,
            p50_ms: pct(0.50),
            p95_ms: pct(0.95),
            p99_ms: pct(0.99),
            qps: queries.len() as f64 / total.max(1e-9),
        },
    ))
}

fn dcg(grades: &[u32]) -> f64 {
    grades
        .iter()
        .enumerate()
        .map(|(index, grade)| {
            (2f64.powi(*grade as i32) - 1.0) / (index as f64 + 2.0).log2()
        })
        .sum()
}

fn eval_quality(
    result_ids: &[u64],
    gt_ids: &[u64],
    judged: Option<&HashMap<u64, u32>>,
    k: usize,
) -> (f64, f64, f64, f64) {
    // (recall@k vs exact BM25, ndcg@10, recall@100, mrr@10)
    let overlap = result_ids
        .iter()
        .take(k)
        .filter(|id| gt_ids.contains(id))
        .count();
    let recall_at_k = overlap as f64 / k.max(1) as f64;

    let Some(judged) = judged else {
        return (recall_at_k, 0.0, 0.0, 0.0);
    };
    let relevant: Vec<u64> = judged
        .iter()
        .filter(|(_, grade)| **grade > 0)
        .map(|(id, _)| *id)
        .collect();
    let ndcg = {
        let grades: Vec<u32> = result_ids
            .iter()
            .take(10)
            .map(|id| judged.get(id).copied().unwrap_or(0))
            .collect();
        let mut ideal: Vec<u32> = judged.values().copied().collect();
        ideal.sort_unstable_by(|left, right| right.cmp(left));
        let idcg = dcg(&ideal);
        if idcg == 0.0 {
            0.0
        } else {
            dcg(&grades) / idcg
        }
    };
    let recall100 = if relevant.is_empty() {
        0.0
    } else {
        result_ids
            .iter()
            .take(100)
            .filter(|id| relevant.contains(id))
            .count() as f64
            / relevant.len() as f64
    };
    let mrr = result_ids
        .iter()
        .take(10)
        .position(|id| judged.get(id).is_some_and(|grade| *grade > 0))
        .map(|position| 1.0 / (position as f64 + 1.0))
        .unwrap_or(0.0);
    (recall_at_k, ndcg, recall100, mrr)
}

struct QualitySummary {
    recall_at_k: f64,
    ndcg_at_10: f64,
    recall_at_100: f64,
    mrr_at_10: f64,
}

fn summarize_quality(
    outcomes: &[SearchOutcome],
    gt: &[Vec<u64>],
    queries: &[Query],
    qrels: &HashMap<String, HashMap<u64, u32>>,
    k: usize,
) -> QualitySummary {
    let count = outcomes.len().max(1) as f64;
    let mut recall_at_k = 0.0;
    let mut ndcg = 0.0;
    let mut recall100 = 0.0;
    let mut mrr = 0.0;
    for (index, outcome) in outcomes.iter().enumerate() {
        let judged = qrels.get(&queries[index].id);
        let (r, n, r100, m) = eval_quality(&outcome.ids, &gt[index], judged, k);
        recall_at_k += r;
        ndcg += n;
        recall100 += r100;
        mrr += m;
    }
    QualitySummary {
        recall_at_k: recall_at_k / count,
        ndcg_at_10: ndcg / count,
        recall_at_100: recall100 / count,
        mrr_at_10: mrr / count,
    }
}

fn quality_json(
    quality: &QualitySummary,
    timing: &Timing,
    outcomes: &[SearchOutcome],
) -> Value {
    let candidates =
        outcomes.iter().map(|o| o.candidates).sum::<usize>() / outcomes.len().max(1);
    let dropped =
        outcomes.iter().map(|o| o.dropped_stale).sum::<usize>() / outcomes.len().max(1);
    let verify_ms =
        outcomes.iter().map(|o| o.verify_ms).sum::<f64>() / outcomes.len().max(1) as f64;
    json!({
        "recall_at_k_vs_exact_bm25": quality.recall_at_k,
        "ndcg_at_10": quality.ndcg_at_10,
        "recall_at_100": quality.recall_at_100,
        "mrr_at_10": quality.mrr_at_10,
        "mean_ms": timing.mean_ms,
        "p50_ms": timing.p50_ms,
        "p95_ms": timing.p95_ms,
        "p99_ms": timing.p99_ms,
        "qps": timing.qps,
        "candidates_avg": candidates,
        "dropped_stale_avg": dropped,
        "verify_ms_avg": verify_ms,
    })
}

// ---------------------------------------------------------------------------
// Index statistics
// ---------------------------------------------------------------------------

struct SplitStats {
    generation: u64,
    splits: usize,
    indexed_docs: u64,
    max_delta_ratio: f32,
}

async fn collect_stats(
    catalog: &IndexCatalog<TextSplitEntry>,
    prefixes: &[String],
) -> Result<SplitStats, String> {
    let mut generation = 0u64;
    let mut splits = 0usize;
    let mut indexed_docs = 0u64;
    let mut max_ratio = 0f32;
    for prefix in prefixes {
        let Some(view) = catalog
            .resolve_cached(prefix)
            .await
            .map_err(|e| format!("resolve {prefix}: {e}"))?
        else {
            continue;
        };
        generation = generation.max(view.generation);
        splits += view.segments.len();
        let total: u64 = view.segments.iter().map(|split| split.num_docs).sum();
        indexed_docs += total;
        let base = view
            .segments
            .first()
            .map(|split| split.num_docs)
            .unwrap_or(0)
            .max(1);
        let delta = total.saturating_sub(base);
        max_ratio = max_ratio.max(delta as f32 / base as f32);
    }
    Ok(SplitStats {
        generation,
        splits,
        indexed_docs,
        max_delta_ratio: max_ratio,
    })
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

fn table_config_for(args: &Args, rebuild_mode: &str, ratio: f32) -> TextIndexTableConfig {
    table_config(args, rebuild_mode, ratio)
}

fn policies_for(args: &Args, round: usize) -> (&'static str, f32) {
    match args.policy {
        Policy::None => ("none", args.max_delta_ratio),
        Policy::Auto => ("auto", args.max_delta_ratio),
        Policy::Always => ("auto", FORCE_REBUILD_RATIO),
        Policy::Periodic => {
            if round.is_multiple_of(args.period) {
                ("auto", FORCE_REBUILD_RATIO)
            } else {
                ("none", args.max_delta_ratio)
            }
        }
    }
}

async fn update_index(
    catalog: &IndexCatalog<TextSplitEntry>,
    args: &Args,
    round: usize,
    new_files: &[String],
    all_files: &[String],
    trigger_only: bool,
) -> Result<(), String> {
    let (mode, ratio) = if trigger_only {
        ("none", args.max_delta_ratio)
    } else {
        policies_for(args, round)
    };
    let mut partition_files: HashMap<String, (Vec<String>, u64)> = HashMap::new();
    partition_files.insert(
        "-5".to_string(),
        (new_files.to_vec(), new_files.len() as u64),
    );
    auto_build_text_index(
        &[table_config_for(args, mode, ratio)],
        &[PK_COLUMN.to_string()],
        &HashMap::new(),
        &partition_files,
        Some(all_files),
        catalog,
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("index update failed: {e}"))
}

fn clear_split_cache() {
    let cache = lakesoul_text::SplitCache::from_env();
    let _ = fs::remove_dir_all(cache.root());
}

async fn measure_search(
    args: &Args,
    store: &Arc<dyn ObjectStore>,
    catalog: &IndexCatalog<TextSplitEntry>,
    prefixes: &[String],
    dataset: &Dataset,
    live: &HashMap<u64, String>,
    per_shard_k: usize,
) -> Result<(Vec<SearchOutcome>, Timing, Option<(GtIndex, Vec<Vec<u64>>)>), String> {
    let gt = if !dataset.queries.is_empty() {
        let index = GtIndex::build(&args.work_dir, &text_config(args), live)?;
        let gt_ids: Vec<Vec<u64>> = dataset
            .queries
            .iter()
            .map(|query| index.top_k(&query.text, QUALITY_K))
            .collect::<Result<_, _>>()?;
        Some((index, gt_ids))
    } else {
        None
    };
    let config = text_config(args);
    let (outcomes, timing) = measure(&dataset.queries, |query| {
        search_shards(
            store,
            catalog,
            prefixes,
            &config,
            query,
            per_shard_k,
            QUALITY_K,
            args.verify,
            live,
        )
    })
    .await?;
    Ok((outcomes, timing, gt))
}

async fn run_build(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    if args.clear_split_cache {
        clear_split_cache();
    }
    let work_dir = prepare_work_dir(&args.work_dir)?;
    if !args.reuse {
        clean_work_dir(&work_dir)?;
    }

    let rows: Vec<(u64, String)> = dataset
        .docs
        .iter()
        .map(|doc| (doc.key, doc.text.clone()))
        .collect();
    let rss_before = rss_mb();
    let t = Instant::now();
    let files =
        write_docs(&work_dir, args.shards, PhysicalFormat::Parquet, &rows).await?;
    let data_write_ms = t.elapsed().as_secs_f64() * 1000.0;

    let catalog = bench_catalog().await?;
    let mut partition_files: HashMap<String, (Vec<String>, u64)> = HashMap::new();
    partition_files.insert("-5".to_string(), (files.clone(), rows.len() as u64));
    let t = Instant::now();
    auto_build_text_index(
        &[table_config_for(args, "auto", FORCE_REBUILD_RATIO)],
        &[PK_COLUMN.to_string()],
        &HashMap::new(),
        &partition_files,
        Some(files.as_slice()),
        &catalog,
    )
    .await
    .map_err(|e| format!("index build failed: {e}"))?;
    let build_ms = t.elapsed().as_secs_f64() * 1000.0;
    let rss_after = rss_mb();

    let prefixes = prefixes_of(&files, DOC_COLUMN);
    let stats = collect_stats(&catalog, &prefixes).await?;
    let live: HashMap<u64, String> = rows.iter().cloned().collect();
    let store = bench_store();

    let (quality, search) = if dataset.queries.is_empty() {
        (Value::Null, Value::Null)
    } else {
        let (outcomes, timing, gt) = measure_search(
            args,
            &store,
            &catalog,
            &prefixes,
            dataset,
            &live,
            args.candidates,
        )
        .await?;
        let quality = summarize_quality(
            &outcomes,
            &gt.as_ref().map(|(_, ids)| ids.clone()).unwrap_or_default(),
            &dataset.queries,
            &dataset.qrels,
            args.top_k,
        );
        (
            json!({
                "recall_at_k_vs_exact_bm25": quality.recall_at_k,
                "ndcg_at_10": quality.ndcg_at_10,
                "recall_at_100": quality.recall_at_100,
                "mrr_at_10": quality.mrr_at_10,
            }),
            quality_json(&quality, &timing, &outcomes),
        )
    };

    let summary = json!({
        "scenario": "build",
        "tokenizer": args.tokenizer,
        "with_positions": args.with_positions,
        "stored": args.stored,
        "shards": args.shards,
        "n_docs": rows.len(),
        "n_queries": dataset.queries.len(),
        "data_write_ms": data_write_ms,
        "build_ms": build_ms,
        "docs_per_sec": rows.len() as f64 / (build_ms / 1000.0).max(1e-9),
        "rss_before_mb": rss_before,
        "rss_after_mb": rss_after,
        "peak_rss_mb": peak_rss_mb(),
        "index_size_bytes": index_size_bytes(&work_dir),
        "splits": stats.splits,
        "generation": stats.generation,
        "indexed_docs": stats.indexed_docs,
        "quality": quality,
        "search": search,
        "data_files": files,
    });
    write_summary(args, &summary)?;
    Ok(summary)
}

async fn run_search(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    if args.clear_split_cache {
        clear_split_cache();
    }
    let work_dir = prepare_work_dir(&args.work_dir)?;
    let files: Vec<String> = find_data_files(&work_dir)
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect();
    if files.is_empty() {
        return Err(format!(
            "no data files under {} — run --scenario build first",
            work_dir.display()
        ));
    }
    let catalog = bench_catalog().await?;
    let prefixes = prefixes_of(&files, DOC_COLUMN);
    let store = bench_store();

    // Live documents come from the work dir files (the merged current rows
    // are exactly what was written; the benchmark write path never keeps a
    // stale version).
    let live = read_live_docs(&work_dir).await?;
    let gt = if dataset.queries.is_empty() {
        None
    } else {
        let index = GtIndex::build(&work_dir, &text_config(args), &live)?;
        let ids: Vec<Vec<u64>> = dataset
            .queries
            .iter()
            .map(|query| index.top_k(&query.text, QUALITY_K))
            .collect::<Result<_, _>>()?;
        Some((index, ids))
    };

    let mut sweep = Vec::new();
    let config = text_config(args);
    for &candidates in &args.candidate_sweep {
        let (outcomes, timing) = measure(&dataset.queries, |query| {
            search_shards(
                &store,
                &catalog,
                &prefixes,
                &config,
                query,
                candidates,
                QUALITY_K,
                args.verify,
                &live,
            )
        })
        .await?;
        let quality = summarize_quality(
            &outcomes,
            &gt.as_ref().map(|(_, ids)| ids.clone()).unwrap_or_default(),
            &dataset.queries,
            &dataset.qrels,
            args.top_k,
        );
        println!(
            "candidates={:<5} recall@{}={:.4} ndcg@10={:.4} p50={:.3}ms p99={:.3}ms qps={:.1}",
            candidates,
            args.top_k,
            quality.recall_at_k,
            quality.ndcg_at_10,
            timing.p50_ms,
            timing.p99_ms,
            timing.qps
        );
        sweep.push(json!({
            "candidates_per_shard": candidates,
            "recall_at_k_vs_exact_bm25": quality.recall_at_k,
            "ndcg_at_10": quality.ndcg_at_10,
            "recall_at_100": quality.recall_at_100,
            "mrr_at_10": quality.mrr_at_10,
            "mean_ms": timing.mean_ms,
            "p50_ms": timing.p50_ms,
            "p95_ms": timing.p95_ms,
            "p99_ms": timing.p99_ms,
            "qps": timing.qps,
            "candidates_avg": outcomes.iter().map(|o| o.candidates).sum::<usize>() / outcomes.len().max(1),
            "dropped_stale_avg": outcomes.iter().map(|o| o.dropped_stale).sum::<usize>() / outcomes.len().max(1),
            "verify_ms_avg": outcomes.iter().map(|o| o.verify_ms).sum::<f64>() / outcomes.len().max(1) as f64,
        }));
    }

    let stats = collect_stats(&catalog, &prefixes).await?;
    let summary = json!({
        "scenario": "search",
        "tokenizer": args.tokenizer,
        "with_positions": args.with_positions,
        "stored": args.stored,
        "shards": args.shards,
        "n_docs": live.len(),
        "n_queries": dataset.queries.len(),
        "index_size_bytes": index_size_bytes(&work_dir),
        "splits": stats.splits,
        "generation": stats.generation,
        "indexed_docs": stats.indexed_docs,
        "sweep": sweep,
    });
    write_summary(args, &summary)?;
    Ok(summary)
}

/// Read the current documents back from the work dir data files (id, text).
async fn read_live_docs(work_dir: &Path) -> Result<HashMap<u64, String>, String> {
    let files: Vec<String> = find_data_files(work_dir)
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect();
    if files.is_empty() {
        return Ok(HashMap::new());
    }
    let config = LakeSoulIOConfigBuilder::new()
        .with_prefix(work_dir.to_string_lossy().to_string())
        .with_files(files)
        .with_schema(schema())
        .build();
    let mut reader =
        lakesoul_io::reader::LakeSoulReader::new(config).map_err(|e| e.to_string())?;
    reader.start().await.map_err(|e| e.to_string())?;
    let mut live = HashMap::new();
    while let Some(batch) = reader.next_rb().await {
        let batch = batch.map_err(|e| e.to_string())?;
        let ids = batch
            .column_by_name(PK_COLUMN)
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| "primary key column missing".to_string())?;
        let texts = batch
            .column_by_name(DOC_COLUMN)
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| "content column missing".to_string())?;
        for row in 0..batch.num_rows() {
            live.insert(ids.value(row), texts.value(row).to_string());
        }
    }
    Ok(live)
}

async fn run_stream_inner(
    args: &Args,
    dataset: &Dataset,
    trigger_only: bool,
) -> Result<Value, String> {
    if args.reuse {
        return Err("--reuse is not supported for stream/trigger".to_string());
    }
    if args.clear_split_cache {
        clear_split_cache();
    }
    let work_dir = prepare_work_dir(&args.work_dir)?;
    clean_work_dir(&work_dir)?;

    // Base documents + fresh index through the production policy path.
    let rows: Vec<(u64, String)> = dataset
        .docs
        .iter()
        .map(|doc| (doc.key, doc.text.clone()))
        .collect();
    let mut live: HashMap<u64, String> = rows.iter().cloned().collect();
    let t = Instant::now();
    let mut all_files =
        write_docs(&work_dir, args.shards, PhysicalFormat::Parquet, &rows).await?;
    let base_write_ms = t.elapsed().as_secs_f64() * 1000.0;
    let catalog = bench_catalog().await?;
    let t = Instant::now();
    update_index(&catalog, args, 0, &all_files, &all_files, false).await?;
    let base_build_ms = t.elapsed().as_secs_f64() * 1000.0;
    let prefixes = prefixes_of(&all_files, DOC_COLUMN);
    let store = bench_store();

    // Update pool: documents beyond the base limit.
    let mut pool = Dataset::load_pool(args)?;
    // For deletes/rewrites prefer documents that are not judged relevant, so
    // the qrels stay meaningful.
    let judged: HashSet<u64> = dataset
        .qrels
        .values()
        .flat_map(|map| map.keys().copied())
        .collect();

    let mut rounds_json: Vec<Value> = Vec::new();
    let mut series: Vec<(usize, f32, Option<f64>)> = Vec::new();
    let mut rebuilds = 0usize;
    let mut total_data_ms = 0.0;
    let mut total_index_ms = 0.0;
    let mut min_quality = f64::INFINITY;
    let mut last_quality = Value::Null;
    let mut next_round_key = args.limit as u64;

    for round in 1..=args.rounds {
        // Build this round's update rows.
        let mut updates: Vec<(u64, String)> = Vec::new();
        match args.drift {
            Drift::Append | Drift::TopicShift => {
                for _ in 0..args.per_round {
                    let doc = if args.drift == Drift::TopicShift {
                        pool.pop().unwrap_or_else(|| Doc {
                            key: next_round_key,
                            text: "lakesoul benchmark generated document".to_string(),
                        })
                    } else {
                        pool.pop().unwrap_or_else(|| Doc {
                            key: next_round_key,
                            text: "lakesoul benchmark generated document".to_string(),
                        })
                    };
                    let key = next_round_key;
                    next_round_key += 1;
                    live.insert(key, doc.text.clone());
                    updates.push((key, doc.text));
                }
            }
            Drift::Rewrite => {
                let mut targets: Vec<u64> = live
                    .keys()
                    .copied()
                    .filter(|key| !judged.contains(key))
                    .collect();
                targets.sort_unstable();
                for index in 0..args.per_round.min(targets.len()) {
                    let key = targets[index * 7 % targets.len()];
                    let text = pool.pop().map(|doc| doc.text).unwrap_or_else(|| {
                        "lakesoul benchmark rewritten document".to_string()
                    });
                    live.insert(key, text.clone());
                    updates.push((key, text));
                }
                // Add fresh keys when there are not enough rewrite targets.
                while updates.len() < args.per_round {
                    let key = next_round_key;
                    next_round_key += 1;
                    let text = pool.pop().map(|doc| doc.text).unwrap_or_else(|| {
                        "lakesoul benchmark generated document".to_string()
                    });
                    live.insert(key, text.clone());
                    updates.push((key, text));
                }
            }
            Drift::Delete => {
                let mut targets: Vec<u64> = live
                    .keys()
                    .copied()
                    .filter(|key| !judged.contains(key))
                    .collect();
                targets.sort_unstable();
                for index in 0..args.per_round.min(targets.len()) {
                    let key = targets[index * 13 % targets.len()];
                    live.remove(&key);
                    // Write an empty tombstone-like row so a rebuild does not
                    // resurrect the document.
                    updates.push((key, String::new()));
                }
            }
        }

        let t = Instant::now();
        let new_files =
            write_docs(&work_dir, args.shards, PhysicalFormat::Parquet, &updates).await?;
        let data_write_ms = t.elapsed().as_secs_f64() * 1000.0;
        all_files.extend(new_files.iter().cloned());

        let generation_before = collect_stats(&catalog, &prefixes).await?.generation;
        let t = Instant::now();
        update_index(&catalog, args, round, &new_files, &all_files, trigger_only).await?;
        let index_update_ms = t.elapsed().as_secs_f64() * 1000.0;
        total_data_ms += data_write_ms;
        total_index_ms += index_update_ms;

        let stats = collect_stats(&catalog, &prefixes).await?;
        let rebuilt = stats.generation > generation_before;
        if rebuilt {
            rebuilds += 1;
        }

        let mut search = Value::Null;
        let mut recall_here = None;
        if dataset.queries.is_empty() {
            // no quality measurement without queries
        } else if round % args.checkpoint_every == 0 || round == args.rounds {
            let (outcomes, timing, gt) = measure_search(
                args,
                &store,
                &catalog,
                &prefixes,
                dataset,
                &live,
                args.candidates,
            )
            .await?;
            let quality = summarize_quality(
                &outcomes,
                &gt.as_ref().map(|(_, ids)| ids.clone()).unwrap_or_default(),
                &dataset.queries,
                &dataset.qrels,
                args.top_k,
            );
            recall_here = Some(quality.recall_at_k);
            min_quality = min_quality.min(quality.recall_at_k);
            last_quality = quality_json(&quality, &timing, &outcomes);
            search = last_quality.clone();
        }
        series.push((round, stats.max_delta_ratio, recall_here));

        println!(
            "round {round:>2} live={:<7} indexed={:<7} splits={:<3} index={:<7.1}ms rebuild={rebuilt} delta/base={:.3} recall={}",
            live.len(),
            stats.indexed_docs,
            stats.splits,
            index_update_ms,
            stats.max_delta_ratio,
            recall_here
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "-".to_string()),
        );
        rounds_json.push(json!({
            "round": round,
            "updates": updates.len(),
            "live_docs": live.len(),
            "data_write_ms": data_write_ms,
            "index_update_ms": index_update_ms,
            "rebuilt": rebuilt,
            "splits": stats.splits,
            "indexed_docs": stats.indexed_docs,
            "max_delta_ratio": stats.max_delta_ratio,
            "index_size_bytes": index_size_bytes(&work_dir),
            "search": search,
        }));
    }

    let total_docs = live.len() as f64 + args.rounds as f64 * args.per_round as f64;
    let amortized = total_docs / ((total_data_ms + total_index_ms) / 1000.0).max(1e-9);
    let mut summary = json!({
        "scenario": if trigger_only { "trigger" } else { "stream" },
        "policy": if trigger_only { "none".to_string() } else { format!("{:?}", args.policy).to_lowercase() },
        "drift": format!("{:?}", args.drift).to_lowercase(),
        "max_delta_ratio": args.max_delta_ratio,
        "tokenizer": args.tokenizer,
        "with_positions": args.with_positions,
        "stored": args.stored,
        "shards": args.shards,
        "rounds": args.rounds,
        "per_round": args.per_round,
        "n_base": dataset.docs.len(),
        "n_queries": dataset.queries.len(),
        "base_write_ms": base_write_ms,
        "base_build_ms": base_build_ms,
        "rounds_data": rounds_json,
        "summary": {
            "rebuilds": rebuilds,
            "total_data_write_ms": total_data_ms,
            "total_index_update_ms": total_index_ms,
            "amortized_docs_per_sec": amortized,
            "min_recall_at_k": if min_quality.is_finite() { json!(min_quality) } else { Value::Null },
            "final_quality": last_quality,
            "final_index_size_bytes": index_size_bytes(&work_dir),
            "peak_rss_mb": peak_rss_mb(),
        },
    });

    if trigger_only {
        let mut analysis = Vec::new();
        for threshold in [0.25f32, 0.5, 1.0, 2.0] {
            let firing = series
                .iter()
                .find(|(_, ratio, _)| *ratio > threshold)
                .map(|(round, ratio, recall)| {
                    json!({"round": round, "ratio": ratio, "recall_at_k": recall})
                });
            analysis.push(json!({"threshold": threshold, "first_firing": firing}));
        }
        summary["trigger_analysis"] = json!(analysis);
    }
    write_summary(args, &summary)?;
    Ok(summary)
}

// ---------------------------------------------------------------------------
// SQL scenario
// ---------------------------------------------------------------------------

async fn run_sql(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    use datafusion::prelude::SessionContext;
    use lakesoul_datafusion::cli::CoreArgs;

    let client: lakesoul_datafusion::MetaDataClientRef = Arc::new(
        lakesoul_metadata::MetaDataClient::from_env()
            .await
            .map_err(|e| format!("metadata client: {e}"))?,
    );
    let ctx: Arc<SessionContext> = lakesoul_datafusion::create_lakesoul_session_ctx(
        client.clone(),
        &CoreArgs {
            warehouse_prefix: None,
            endpoint: None,
            s3_bucket: None,
            s3_access_key: None,
            s3_secret_key: None,
            s3_virtual_host_style: false,
            worker_threads: 2,
        },
    )
    .map_err(|e| format!("session: {e}"))?;

    let table = args.table.clone();
    let _ = client.drop_table(&table, "default").await;
    let work_dir = prepare_work_dir(&args.work_dir)?;
    clean_work_dir(&work_dir)?;

    let text_option = serde_json::json!([{
        "column": DOC_COLUMN,
        "tokenizer": args.tokenizer,
        "with_positions": args.with_positions,
        "stored": args.stored,
    }])
    .to_string()
    .replace('\'', "''");
    let location = work_dir.display().to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table} (
            id BIGINT NOT NULL PRIMARY KEY,
            {DOC_COLUMN} STRING
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('text_index_columns' '{text_option}', 'hashBucketNum' '{}', 'file_format' '{}')",
        args.sql_hash_buckets, args.sql_format
    );
    ctx.sql(&create_sql)
        .await
        .map_err(|e| format!("create table: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("create table: {e}"))?;

    let table_handle =
        lakesoul_datafusion::lakesoul_table::LakeSoulTable::for_name(&table)
            .await
            .map_err(|e| format!("open table: {e}"))?;

    // Base writes + incremental rounds through the production upsert path.
    let rows: Vec<(u64, String)> = dataset
        .docs
        .iter()
        .map(|doc| (doc.key, doc.text.clone()))
        .collect();
    let mut live: HashMap<u64, String> = rows.iter().cloned().collect();
    let t = Instant::now();
    table_handle
        .execute_upsert(doc_batch_sql(&rows)?)
        .await
        .map_err(|e| format!("base insert: {e}"))?;
    let base_insert_ms = t.elapsed().as_secs_f64() * 1000.0;

    let mut pool = Dataset::load_pool(args)?;
    let mut round_ms = Vec::new();
    let mut next_key = args.limit as u64;
    for _ in 0..args.rounds {
        let mut updates: Vec<(u64, String)> = Vec::new();
        for _ in 0..args.per_round {
            let text = pool
                .pop()
                .map(|doc| doc.text)
                .unwrap_or_else(|| "lakesoul benchmark generated document".to_string());
            let key = next_key;
            next_key += 1;
            live.insert(key, text.clone());
            updates.push((key, text));
        }
        let t = Instant::now();
        table_handle
            .execute_upsert(doc_batch_sql(&updates)?)
            .await
            .map_err(|e| format!("round insert: {e}"))?;
        round_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    // Search through SQL: candidate scan + residual exact predicate.
    let gt = GtIndex::build(&work_dir, &text_config(args), &live)?;
    let gt_ids: Vec<Vec<u64>> = dataset
        .queries
        .iter()
        .map(|query| gt.top_k(&query.text, QUALITY_K))
        .collect::<Result<_, _>>()?;

    let explain_sql = format!(
        "EXPLAIN VERBOSE SELECT id FROM \"lakesoul\".default.{table} \
         WHERE text_match({DOC_COLUMN}, 'benchmark') \
         ORDER BY text_score({DOC_COLUMN}, 'benchmark') DESC LIMIT 10"
    );
    let explain = ctx
        .sql(&explain_sql)
        .await
        .map_err(|e| format!("explain: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("explain: {e}"))?;
    let explain_text = format!("{explain:?}");
    let uses_index = explain_text.contains("LakeSoulTextSearchExec");

    let (outcomes, timing) = measure(&dataset.queries, |query| {
        let sql = format!(
            "SELECT id FROM \"lakesoul\".default.{table} \
             WHERE text_match({DOC_COLUMN}, '{}') \
             ORDER BY text_score({DOC_COLUMN}, '{}') DESC LIMIT {}",
            query.replace('\'', "''"),
            query.replace('\'', "''"),
            QUALITY_K
        );
        let ctx = Arc::clone(&ctx);
        async move {
            let batches = ctx
                .sql(&sql)
                .await
                .map_err(|e| format!("sql: {e}"))?
                .collect()
                .await
                .map_err(|e| format!("collect: {e}"))?;
            let mut ids = Vec::new();
            for batch in batches {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| "id column is not Int64".to_string())?;
                for row in 0..batch.num_rows() {
                    ids.push(values.value(row) as u64);
                }
            }
            Ok(SearchOutcome {
                ids,
                candidates: 0,
                dropped_stale: 0,
                verify_ms: 0.0,
            })
        }
    })
    .await?;
    let quality = summarize_quality(
        &outcomes,
        &gt_ids,
        &dataset.queries,
        &dataset.qrels,
        args.top_k,
    );

    let summary = json!({
        "scenario": "sql",
        "table": table,
        "tokenizer": args.tokenizer,
        "with_positions": args.with_positions,
        "shards": args.sql_hash_buckets,
        "format": args.sql_format,
        "n_base": rows.len(),
        "n_rounds": args.rounds,
        "n_written": rows.len() + args.rounds * args.per_round,
        "base_insert_ms": base_insert_ms,
        "round_insert_ms": round_ms,
        "uses_index": uses_index,
        "quality": {
            "recall_at_k_vs_exact_bm25": quality.recall_at_k,
            "ndcg_at_10": quality.ndcg_at_10,
            "recall_at_100": quality.recall_at_100,
            "mrr_at_10": quality.mrr_at_10,
        },
        "search": quality_json(&quality, &timing, &outcomes),
        "index_size_bytes": index_size_bytes(&work_dir),
    });

    let _ = client.drop_table(&table, "default").await;
    write_summary(args, &summary)?;
    Ok(summary)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<(), String> {
    let args = parse_args()?;
    let dataset = Dataset::load(&args)?;
    println!(
        "dataset: {} docs, {} queries ({} with qrels)",
        dataset.docs.len(),
        dataset.queries.len(),
        dataset
            .queries
            .iter()
            .filter(|query| dataset.qrels.contains_key(&query.id))
            .count()
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    let summary = runtime.block_on(async {
        match args.scenario {
            Scenario::Build => run_build(&args, &dataset).await,
            Scenario::Search => run_search(&args, &dataset).await,
            Scenario::Stream => run_stream_inner(&args, &dataset, false).await,
            Scenario::Trigger => run_stream_inner(&args, &dataset, true).await,
            Scenario::Sql => run_sql(&args, &dataset).await,
        }
    })?;
    print_summary(&summary);
    Ok(())
}
