# lakesoul-text: LakeSoul Text Index Module

This crate provides Tantivy-based full-text indexing for LakeSoul: it builds a
self-contained index **split** from `(primary key, text)` rows, persists it as a
single object on the table's object store, and searches the splits with BM25.

The crate deliberately has **no LakeSoul IO or metadata dependency**: it receives
rows, produces an object, and returns catalog entries. The `lakesoul-io` layer
provides the shard builder and the reader integration; `lakesoul-datafusion`
owns table properties, auto-build and catalog commits.

## Module Structure

```
lakesoul-text/src/
├── lib.rs          # crate exports
├── config.rs       # TextIndexConfig — column, tokenizer, with_positions, stored
├── tokenizer.rs    # jieba analyzer registration + supported tokenizer names
├── schema.rs       # TextSchema — __lakesoul_pk (u64) + fixed "text" field
├── split.rs        # split bundle encode/decode, write_split, SplitCache
├── search.rs       # search_index / search_splits / merge_hits, TextHit
├── verify.rs       # matching_ids / matching_scores — exact re-verification
└── error.rs        # TextError / Result

lakesoul-io/src/text/
├── builder.rs      # TextShardIndexBuilder — read shard files → one split
├── reader.rs       # collect_text_documents — (pk, text) extraction
├── search.rs       # resolve catalog commits → search splits → merged hits
└── verify.rs       # TextVerifyStream — exact pass after merge-on-read

lakesoul-datafusion/src/
├── text_index.rs   # `text_index_columns` property, auto-build, compaction, rebuild
├── udf/text_search_marker.rs  # `text_match` UDF + pushdown marker
└── planner/text_search_rule.rs # Limit/Filter/TableScan → index candidate scan
```

## Object Layout

```
{table_path}/_text_index/{column}/{partition}/{bucket}/{split_id}.split
```

The shard identity is `(partition_desc, hash_bucket_id)`; files from different
range partitions never share a shard. Because a split directory is derived from
the data-file paths, a reader can locate the index without extra catalog lookups.

## Split Bundle Format

A split is one immutable object containing a complete Tantivy index:

```
LSTX | format_version | tantivy files... | footer JSON | footer_len (u32 LE)
```

- `LSTX` magic + format version, followed by every Tantivy file;
- the JSON footer stores `num_docs` (live documents, see compaction) and, per
  file, `{name, offset, len, crc32}`;
- readers verify the magic, the format version and every CRC, then materialize
  the files into the local [`SplitCache`] (`$LAKESOUL_TEXT_SPLIT_CACHE_DIR` or
  the system temp directory) and open them read-only. Splits are immutable, so a
  materialized directory is reused across queries.

`write_split()` force-merges the Tantivy segments into one before packaging, so
opening a split touches a single segment.

## Schema and Tokenizers

Inside a split the schema is fixed:

- `__lakesoul_pk`: `u64`, `INDEXED | FAST` — maps hits back to table rows and
  backs the upsert/compaction deletes;
- `text`: the analyzed content field. The Arrow column name is only used when
  reading the data files; using a fixed internal name keeps Arrow columns that
  contain characters Tantivy rejects in field names usable.

Tantivy pre-registers `default`, `raw`, `en_stem` and `whitespace`. This crate
adds `jieba` (jieba word segmentation + `LowerCaser`, so mixed Chinese/English
text matches case-insensitively). The tokenizer is **not** persisted in the
index metadata, so the same registration runs when a split is created and when
it is opened for search.

## Search

```text
search_index(index, query, k)        → top-k TextHit { id, score } of one split
search_splits(indexes, query, ...)   → per-split top-k → merge_hits
merge_hits(hits, k)                  → best score per primary key, score desc
```

Scoring is Tantivy's BM25 over the split's own statistics; scores of different
splits are therefore not strictly comparable, which is why the reader treats
the result as a candidate set (see below).

## Exact Verification

An index search returns candidates from the *indexed* text, which may be a
stale version of a row that was updated (or deleted) afterwards. The vector
index tolerates this because candidates are re-ranked by exact distance to the
raw vector; a text query instead needs an exact boolean pass over the current
rows:

```text
merge-on-read (current row versions, CDC tombstones dropped)
    → verify::matching_scores(config, rows, query)
        builds a tiny in-memory Tantivy index over (pk, current text)
        evaluates the same query with the same tokenizer
        returns the current BM25 scores of the rows that really match
```

The reader uses this to drop stale candidates and to keep per-candidate scores
available for the planned ranking work.

## Configuration

`TextIndexConfig` (the `text_index_columns` payload):

| Field | Description | Default |
|-------|-------------|---------|
| `column` | Text column name | required |
| `tokenizer` | One of `jieba`, `default`, `en_stem`, `whitespace`, `raw` | `jieba` |
| `with_positions` | Index term positions (phrase queries) | `true` |
| `stored` | Store the original text in the split | `false` |

`TextIndexConfig::parse_json` accepts a single JSON object, an array, or a JSON
string containing either, and is the single source of truth for both the Rust
and Python SDKs.

The management knobs (`rebuild_mode`, `max_delta_ratio`, `gc_enabled`,
`gc_grace_seconds`, `gc_keep_generations`) are shared with the other index
kinds and live in `lakesoul-datafusion`'s generic index configuration.

## Compaction

Splits are written with Tantivy `delete_term`: every document is preceded by a
delete of its primary key, so the last occurrence wins. A rebuild therefore
keeps only the newest version of each row even when it reads several overlapping
data files.

Delta builds append one split per shard. `drift_exceeds_threshold(splits,
max_delta_ratio)` compares the accumulated delta documents against the base
split published by the last rebuild; engines that build on write rebuild the
shard from **all** of its active data files once the threshold is crossed,
which folds the history back into one split. The exact verification pass makes
stale splits harmless in the meantime.

## Integration

```text
LakeSoul write commit
  └─ auto-build (lakesoul-datafusion)
       ├─ group new files by (partition, bucket)
       ├─ TextShardIndexBuilder::build()
       │    ├─ read shard files (lakesoul-io) → collect_text_documents
       │    ├─ write_split() → upload {split_id}.split
       │    └─ TextBuildOutcome { header, new_splits }
       ├─ IndexCatalog::<TextSplitEntry>::commit(Delta | Rebuild)
       └─ gc_shard_now() for superseded generations (best effort)

LakeSoulReader::start()
  ├─ caller-resolved text shards + leases (PyO3 / DataFusion)
  ├─ search committed splits → merged TextHit candidates
  ├─ inject `pk IN (...)` candidate filter
  └─ TextVerifyStream — exact pass over merged current rows, scores kept
```

## Tests

```bash
cargo -q test -p lakesoul-text
```

covers the config parser, tokenizers, split bundle round-trips/CRC checks,
BM25 search and merge ordering, duplicate-primary-key upserts, and the
build → upload → materialize → search end-to-end path (including jieba).
