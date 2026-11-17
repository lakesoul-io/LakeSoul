---
sidebar_position: 3
---

# Vector search configuration

Two groups of settings control vector search: the **per-column index build
parameters** stored in the table property `vector_index_columns`, and the
**process/session runtime settings** that control caching, probing and batch
execution.

## Index build parameters

Every entry of the `vector_index_columns` JSON array accepts the following
fields. Only `column` and `dim` are required.

| Field | Default | Meaning |
| --- | --- | --- |
| `column` | required | Vector column name |
| `dim` | required | Vector dimension; must match the column's list size |
| `nlist` | `256` | Number of IVF clusters. Larger is more precise but builds slower and needs a higher `nprobe` |
| `total_bits` | `7` | RaBitQ bits per dimension; only `1`, `3` and `7` are supported |
| `metric` | `L2` | `L2` (Euclidean) or `IP` (inner product) |
| `rotator_type` | `FhtKac` | Random rotation: `FhtKac` (fast, default) or `Matrix` |
| `seed` | `42` | Random seed for the rotation and k-means |
| `use_faster_config` | `true` | Faster quantization mode with < 1% accuracy loss |
| `rebuild_mode` | `auto` | `auto` rebuilds a shard when a cluster drifts past `max_delta_ratio`; `none` only appends deltas |
| `max_delta_ratio` | `1.0` | Per-cluster rebuild trigger: `delta_vectors / base_vectors > max_delta_ratio` |
| `gc_enabled` | `true` | Delete superseded index files after commits (best effort) |
| `gc_grace_seconds` | `3600` | Superseded files younger than this are kept for readers of old commits |
| `gc_keep_generations` | `1` | Generations to keep, including the current one |

```python
table = catalog.create_table(
    "documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    vector_index=[{
        "column": "embedding",
        "dim": 768,
        "nlist": 256,
        "total_bits": 7,
        "metric": "L2",
        "rotator_type": "FhtKac",
        "seed": 42,
        "use_faster_config": True,
        "rebuild_mode": "auto",
        "max_delta_ratio": 1.0,
    }],
)
```

The same JSON is accepted in SQL:

```sql
OPTIONS (
  'vector_index_columns'
    '[{"column":"embedding","dim":768,"nlist":256,"total_bits":7,"metric":"L2"}]'
)
```

`rebuild_mode: "auto"` is enforced by the writers that run automatic index
maintenance: the native Rust/DataFusion write path and the connectors rebuild a
shard when any of its clusters crosses the threshold. Writes made through the
Python SDK append delta segments only; call `table.rebuild_vector_index()`
explicitly to re-cluster from Python.

Related table-level properties:

| Property | Default | Meaning |
| --- | --- | --- |
| `hash_bucket_num` (create-table argument) | `4` | Hash buckets per partition; each bucket is a separate index shard |
| `index_maintenance` | unset (inline) | Set to `deferred` to commit data only and build pending shards out of band (`table.build_vector_index()`) |

`vector_index_columns` itself can be read or set as a raw JSON table property;
`create_table(vector_index=...)` is the validated shorthand.

## Cache tiers

The vector caches are **per process**, in three tiers:

```text
L1  process memory
    parsed index / lazy clusters / query snapshot / decoded vector blocks
L2  local SSD
    generic DiskCache (data pages, primary-key index, decoded blocks)
    optional index segment cache (whole _vector_index segments)
L3  object store
    data files + _vector_index/...   (source of truth)
```

Only latency depends on L1/L2; the object store is always authoritative. A
miss re-reads from L3 and refills the upper tiers. Cache entries are keyed by
the object store's allocation identity as well as the path, so two buckets or
endpoints that serve the same relative path never share entries.

### Lifetime and process boundaries

- **Same process: reuse works.** Rebuilding a cache instance inside a running
  process keeps hitting the files already written to L2, because the
  object-store allocation (and its token in the cache key) stays stable.
- **Process restart: L2 is rebuilt, not reused.** Neither the generic
  `DiskCache` nor the index segment cache hits entries written by a previous
  process — this includes the `IndexSsdCache`. Contents left in the cache
  directories are lazily scanned, adopted or evicted by modification time and
  the current budget, and entries are rewritten on demand. Plan for a cold
  first query after every restart unless you warm the cache.
- **Multiple processes do not share capacity.** Capacity accounting is per
  process, not a global quota for a shared directory. Give each process its own
  cache directory, or accept that entries are duplicated on disk.
- **Vector search does not take the generic page cache by storm.**
  `_vector_index` segments are read through the index segment cache, which has
  its own opt-in budget and does not consume the shared L2 capacity. Exact
  candidate vectors are decoded block by block on demand instead of reading
  whole columns or going through the generic page cache; the decoded blocks are
  admitted to the shared DiskCache, so size L2 for the exact working set.

The L1 budget is split internally between the cluster cache (about 60%), the
decoded-block cache (about 30%, when exact re-rank is active) and snapshot and
query buffers. Index-segment caching is a separate, explicitly sized layer
(`LAKESOUL_INDEX_SSD_BYTES`); it is not carved out of the L2 capacity.

Environment variables are read once per process, when the cache is first
initialized. Set them before importing the Python package or starting the
server; changing them later has no effect.

## Runtime configuration

### Cache and IO

| Variable | Default | Meaning |
| --- | --- | --- |
| `LAKESOUL_VECTOR_CACHE` | on | Master switch for the vector caches; `0`/`false` disables L1 and L2 (queries still work, but cold; exact falls back to the reader path) |
| `LAKESOUL_VECTOR_MEMORY_BYTES` | `512MiB` | L1 total budget (parsed index, lazy clusters, snapshot, decoded blocks, query buffers) |
| `LAKESOUL_CACHE_PATH` | `lakesoul_cache_dir` | L2 root; holds the generic DiskCache (and the index cache unless overridden) |
| `LAKESOUL_CACHE_SIZE` | `1GiB` when L2 is enabled | L2 capacity for the generic DiskCache; `0` disables L2. Setting the path or size (together with the unified switch) enables L2 |
| `LAKESOUL_INDEX_SSD_BYTES` | `0` (disabled) | Optional dedicated budget for the index segment cache. Enabling it does not reduce `LAKESOUL_CACHE_SIZE` |
| `LAKESOUL_INDEX_SSD_DIR` | `<LAKESOUL_CACHE_PATH>/index-cache` | Directory of the index segment cache |
| `LAKESOUL_VECTOR_LAZY_INDEX` | `auto` | `auto`: lazy cluster loading for shards with at least 64 clusters; `1` always lazy; `0` always eager |

Block tunables (expert overrides; the defaults are derived from the L1 budget
and rarely need changing):

| Variable | Default | Meaning |
| --- | --- | --- |
| `LAKESOUL_VECTOR_BLOCK_BYTES` | `256KiB` | Target size of one decoded exact-re-rank block |
| `LAKESOUL_VECTOR_BLOCK_ROWS` | derived (32–1024 per block) | Fixed number of rows per decoded block, overriding `BLOCK_BYTES` |
| `LAKESOUL_VECTOR_BLOCK_CACHE_BYTES` | ~30% of L1 (256 MiB in legacy mode) | L1 budget for decoded blocks |
| `LAKESOUL_VECTOR_BLOCK_SOURCE_CACHE` | on | Set to `0` to disable caching of opened block sources |

Example for a search service with a 8 GiB memory budget and a 100 GiB SSD
cache, plus a dedicated 20 GiB index cache:

```bash
export LAKESOUL_VECTOR_CACHE=1
export LAKESOUL_VECTOR_MEMORY_BYTES=8GiB
export LAKESOUL_CACHE_PATH=/data/lakesoul/cache
export LAKESOUL_CACHE_SIZE=100GiB
export LAKESOUL_INDEX_SSD_BYTES=20GiB
export LAKESOUL_INDEX_SSD_DIR=/data/lakesoul/index-cache
export LAKESOUL_VECTOR_LAZY_INDEX=auto
```

### Metadata freshness, maintenance and leases

| Variable | Default | Meaning |
| --- | --- | --- |
| `LAKESOUL_METADATA_TTL_MS` | `0` (disabled) | Reuse table/partition metadata for up to N ms per statement. `0` keeps every statement on the current metadata; enabling it makes concurrent commits and DDL visible within N ms |
| `LAKESOUL_INDEX_GC_EVERY` | `16` | Amortize write-path index GC over every N writes (unless the grace period is 0) |
| `LAKESOUL_VECTOR_INDEX_LEASE_TTL_SECS` | `300` | Time-to-live of reader index leases |
| `LAKESOUL_VECTOR_EXACT_OVERFETCH` | unset | Per-process override for `exact_overfetch`; when set it takes precedence over the session setting |
| `LAKESOUL_ALLOW_INDEX_DDL` | unset | Set to `1` to allow destructive `lakesoul_drop_vector_index(...)` calls from SQL; without it only `dry_run` works |

Object-store credentials for index builds are read from the
`LAKESOUL_OBJECT_STORE_*` variables when no explicit `store_config` /
`object_store_options` is passed. See the
[configuration reference](../../reference/configuration.md) for the general
metadata and object-storage variables.

### Session settings

These are per connection (DataFusion session or pgwire connection) and can be
set with SQL:

```sql
SET lakesoul_vector_search.nprobe = 64;
SET lakesoul_vector_search.rerank = 'index';
SET lakesoul_vector_search.parallelism = 4;
SET lakesoul_vector_search.deferred_payload = true;
SET lakesoul_vector_search.exact_overfetch = 10;
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `nprobe` | `64` | IVF clusters probed per bucket |
| `rerank` | `index` | `index` orders by the quantized index distance; `exact` re-ranks candidates against the stored vectors |
| `parallelism` | `1` | Clusters probed concurrently per search; the result set is unchanged |
| `deferred_payload` | `true` | Read payload columns only for the rows the limit can return |
| `exact_overfetch` | `10` | Candidates per bucket in exact mode (`top_k × exact_overfetch`) |

The pgwire endpoint accepts the pgvector spelling as an alias:
`SET ivfflat.probes = 64`.

Native batch execution has its own bounded controls:

```sql
SET lakesoul_vector_batch.query_window = 256;
SET lakesoul_vector_batch.parallelism = 0;
SET lakesoul_vector_batch.io_concurrency = 8;
SET lakesoul_vector_batch.memory_bytes = 134217728;
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `lakesoul_vector_batch.query_window` | `256` | Queries per window (1–256) |
| `lakesoul_vector_batch.parallelism` | `0` | CPU workers per window; `0` uses the process default |
| `lakesoul_vector_batch.io_concurrency` | `8` | Concurrent object/vector-block reads (1–64) |
| `lakesoul_vector_batch.memory_bytes` | `134217728` | Transient batch memory budget (at most 128 MiB) |

## Next

- [API reference](api.md) — the complete Python, SQL and pgwire surface.
- [Benchmarks](benchmarks.md) — measured recall and throughput.
