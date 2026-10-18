# Refresh benchmark (B0)

`examples/ivm_bench.rs` measures whether incremental maintenance pays off
against a full recompute of the **same statement**.  Every round appends the
same change batch to the source and times two independent views:

| View | Statement | Expected action |
|---|---|---|
| incremental | `INSERT INTO <mv> SELECT ...` | `incremental` |
| full baseline | `INSERT OVERWRITE <mv> SELECT ...` | `overwrite` |

Both views share the plan, the source, the file format and the write path, so
the difference is the maintenance strategy itself.  The source write is timed
separately and is not part of the refresh numbers.

Shapes: `row` (projection + filter), `sum_count` (grouped aggregate),
`lookup` (a left join to a keyed dimension), `multi_join` (a three-way inner
chain), `top_k` (a partitioned `row_number`), `chain` (a lookup view feeding a
row view, both refreshed end to end), `partitioned` (a range-partitioned
source and view whose changes stay in one partition) and `skew` (a
`sum_count` whose hot group holds most of the rows).  `both` runs row and
sum_count, `all` runs every shape, each with its own tables.  `--mix` sets the
insert/update/delete weights and `--secondary-change` the fraction of
mutations aimed at the dimension/non-fact side.

The IO counters (`B1`) attribute files read, rows scanned/written and bytes
written per view and round; a fixed report and the remaining shapes (derived
chains, partitions, skew) follow in `B2b`/`B3`.

## Setup

The local development services (`devenv up`) provide PostgreSQL and RustFS
(S3) at `127.0.0.1:5432` and `127.0.0.1:9000`.  Create the bucket once:

```sh
uv run --quiet --no-project --with boto3 python - <<'PY'
import boto3
client = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="rustfsadmin",
    aws_secret_access_key="rustfsadmin",
    region_name="us-east-1",
)
client.create_bucket(Bucket="lakesoul-test-bucket")
PY
```

Environment:

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_REGION=us-east-1
```

## Run

Build in release mode for timing (`--release`); `--profile test-fast` is fine
for a functional smoke test.

```sh
# Smoke test on the local filesystem.
cargo run --release -p lakesoul-ivm --example ivm_bench -- \
    --storage local --rows 20000 --groups 200 --change-rows 200 --rounds 5

# Default S3 run: both shapes, 100k rows, 1% change per round.
cargo run --release -p lakesoul-ivm --example ivm_bench -- \
    --shape both --rows 100000 --change-ratio 0.01 --rounds 15

# Scaling run: same change size on a larger table.
cargo run --release -p lakesoul-ivm --example ivm_bench -- \
    --rows 1000000 --groups 10000 --change-rows 1000 --rounds 15 \
    --verify-every 5 --out /tmp/ivm-bench-1m.csv
```

`--help` lists every flag.  The main ones:

| Flag | Meaning |
|---|---|
| `--shape <row\|sum_count\|both>` | views to measure |
| `--rows <N>` | source rows to seed |
| `--change-ratio <F>` / `--change-rows <N>` | changes per round (fraction or absolute) |
| `--rounds <N>` / `--warmup <N>` | measured and unmeasured rounds |
| `--groups <N>` | distinct groups (aggregate cost knob) |
| `--mix <I,U,D>` | insert/update/delete weights (default `1,1,1`) |
| `--dim-rows <N>` | dimension rows for the lookup shape (default `rows/2`) |
| `--secondary-change <F>` | mutations aimed at the non-fact side (default `0.2`) |
| `--partitions <N>` | range partitions for the partitioned shape (default `10`) |
| `--hot-partition <N>` | the partition all partitioned changes hit (default `0`) |
| `--skew <F>` | hot-group share for the skew shape (default `0.8`) |
| `--storage <local\|s3>` | table storage |
| `--bucket` / `--prefix` | S3 location |
| `--out <FILE>` | write the CSV there instead of stdout |
| `--verify-every <N>` | compare the two views every N rounds (`0`: at the end) |
| `--keep` | keep the tables after the run |

Each run creates uniquely named tables under
`s3://<bucket>/<prefix>/<run-id>/` (or a temporary directory), verifies that
the actions are `incremental` and `overwrite`, optionally compares the two
views row by row, and drops the tables at the end.  S3 objects are not
deleted; use a prefix and clean it up out of band.

## Output

Per round, one CSV row per shape:

```
shape,round,change_rows,source_write_ms,incr_ms,full_ms,speedup,
incr_files,incr_rows_scanned,incr_rows_written,incr_mv_bytes,incr_io_bytes,
full_files,full_rows_scanned,full_rows_written,full_mv_bytes,full_io_bytes,
incr_action,full_action
row,3,1000,31.706,173.670,968.256,5.58,54,40278,267,8131,1233650,20,20134,19879,145126,706938,incremental,overwrite
```

The IO columns come from the refresh-scoped counters
(`lakesoul_ivm_refresh_files_read`, `_rows_scanned`, `lakesoul_ivm_mv_rows_written`,
`lakesoul_ivm_mv_bytes_written`) and from `lakesoul_object_store_bytes_total`:
`*_io_bytes` is 0 on local storage (plain filesystem reads are not object-store
requests) and covers reads plus writes on S3.

The trailing table summarizes p50/p95 for both paths:

```
shape      rows      groups    change    incr_p50   full_p50   speedup  incr_p95   full_p95
row        20000     200       200       144.0ms    746.8ms    5.2x     173.7ms    968.3ms
sum_count  20000     200       200       311.7ms    1103.7ms   3.5x     335.2ms    1151.5ms
```

Five-shape smoke on RustFS (20k fact rows, 200 changes per round,
`--profile test-fast`, so indicative only):

| shape | incr p50 | full p50 | speedup | incr rows scanned | incr rows written |
|---|---|---|---|---|---|
| row | 176–229 ms | 654–828 ms | 3.6–3.7x | ~40k (pre-P1) | 260–267 |
| sum_count | 229–257 ms | 1060–1132 ms | 4.4–4.6x | ~460 | 254–274 |
| lookup | 210–248 ms | 716–841 ms | 3.4x | ~50k (pre-P1b) | 284–300 |
| multi_join | 294 ms | 817–951 ms | 2.8–3.2x | ~81k | 265–270 |
| top_k | 284–549 ms | 1258–2590 ms | 4.4–4.7x | ~21k | 1270–1390 |
| chain | 456–513 ms | 2222–2297 ms | 4.3–5.0x | ~101k | 564–592 |
| partitioned | 409–453 ms | 473–509 ms | 1.1–1.2x | ~320 | 8–10 |
| skew | 307–325 ms | 1358–3246 ms | 4.4–10.0x | ~367 | 54–76 |

How to read it:

* the `row` scanned count in the smoke table predates P1 (§10.179); the keyed
  row refresh now reads only the changed keys (a few dozen rows at this size);
* **speedup** = `full_p50 / incr_p50`.  The incremental path should win at
  small change ratios and on large tables;
* the incremental latency should stay **flat as the table grows** for a fixed
  change size — run the same `--change-rows` at `--rows 1e5` and `--rows 1e7`
  and compare `incr_p50`;
* the **break-even** change ratio is where `incr_ms` approaches `full_ms`;
  sweep `--change-ratio` on a fixed table;
* `sum_count` scales with the number of **affected groups**, not just the
  changed rows: with `--groups 200` and 200 changes almost every group is
  touched, which is the expected worst case;
* the IO columns explain a speedup: at a small table the incremental `row`
  refresh can read **more** files and bytes than the full recompute (it walks
  its materialized state to retract the changed tuples) while writing far
  fewer rows and finishing faster; the balance shifts as the table grows.
  Always read the latency and the IO columns together;
* `partitioned` shows the pruning (`~320` rows scanned against a 20k-row
  source) but its latency is still dominated by fixed per-refresh overhead at
  this size; the gap should widen with the table;
* `skew` is a `sum_count`, whose mergeable state makes the hot group cheap
  (`~367` rows scanned); a MIN/MAX or `COUNT(DISTINCT)` skew variant would
  recompute the whole hot group and is a recorded follow-up;
* the join shapes currently read the **full current source state** on every
  refresh (the affected-key computation seeds from the current left rows and
  the materialized view), so their latency gain comes from writing far fewer
  rows; key-filtered reads are a recorded follow-up.  A benchmark run of a
  lookup view is also how the `limit(0, None)` affected-set bug was found
  (DataFusion treats `None` as no fetch limit, so the lookup view was
  rewritten in full every round — fixed to an empty seed, see PLAN §10.176).
  The pair left/full joins keep a **conservative all-keys seed** because a
  changed join key can drop the other side's old match; making them
  key-filtered needs a before-match expansion (recorded follow-up).

## Results (release build, local RustFS)

Environment: one Linux host, PostgreSQL 14 and RustFS at `127.0.0.1:9000`, a
`--release` build, Parquet, four hash buckets, `--groups 1000`, 2 warmup +
8 measured rounds (a few retried groups used 1 + 6), changes mixed
insert/update/delete 1:1:1 with 20% aimed at the non-fact side for the
two-source shapes.  Each cell is `incremental p50 -> full overwrite p50
(speedup)` in ms.  Raw CSVs are in [`bench-results/`](bench-results/).

The `row` cells were measured again after the P1 key-filtered read change
(PLAN §10.179): its `incr_rows_scanned` fell from ~2x the table to 16 / 165 /
1662 rows at 0.01% / 0.1% / 1%, and the 1M-row speedup rose from 4.7x / 4.1x /
3.4x to 7.4x / 7.2x / 6.4x.  The 1M/10% cell (100k changed keys, above the
filter threshold) was not re-measured.  The `lookup` 0.01% cells were measured
again after the P1b key-filtered lookup read (PLAN §10.180): at 1M it scans
322 rows instead of ~2.5M and p50 falls from 1632ms to 1505ms (3.4x -> 3.6x),
while at 1e5 the latency is flat within run noise; its file count is unchanged,
so the fixed floor still dominates.  The other lookup cells (¹) and the
remaining shapes still read their full sources and are from the B3 run.

1e5 fact rows:

| shape | 0.01% changes | 1% changes |
|---|---|---|
| row | 222 -> 1227 (5.5x) | 254 -> 1218 (4.8x) |
| sum_count | 339 -> 1245 (3.7x) | 396 -> 2573 (6.5x) |
| lookup | 379 -> 1224 (3.2x) | 469 -> 1392 (3.0x) |
| multi_join | 583 -> 1336 (2.3x) | 680 -> 2531 (3.7x) |
| top_k | 349 -> 1236 (3.5x) | 495 -> 3100 (6.3x) |
| chain | 723 -> 2567 (3.6x) | 910 -> 3224 (3.5x) |
| partitioned | 702 -> 774 (1.1x) | 826 -> 885 (1.1x) |
| skew | 354 -> 1808 (5.1x) | 390 -> 2676 (6.9x) |

1e6 fact rows:

| shape | 0.01% | 0.1% | 1% | 10% |
|---|---|---|---|---|
| row | 645 -> 4740 (7.4x) | 674 -> 4856 (7.2x) | 1055 -> 6776 (6.4x) | 4410 -> 5206 (1.2x) |
| sum_count | 781 -> 1659 (2.1x) | 860 -> 2350 (2.7x) | 1041 -> 1827 (1.8x) | 11511 -> 2110 (**0.2x**) |
| lookup | 1505 -> 5430 (3.6x) | 1568 -> 5111 (3.3x)¹ | 3613 -> 5994 (1.7x)¹ | not measured |
| multi_join | 2749 -> 6357 (2.3x) | not measured | 6280 -> 6443 (1.0x) | not measured |
| top_k | 910 -> 2327 (2.6x) | not measured | 993 -> 1703 (1.7x) | not measured |
| chain | 3213 -> 11136 (3.5x) | not measured | 7489 -> 11396 (1.5x) | not measured |
| partitioned | not measured | — | not measured | — |
| skew | 805 -> 1689 (2.1x) | not measured | 1952 -> 1762 (0.9x) | not measured |

`partitioned` at 1M and `lookup` at 10% could not be measured: the local
RustFS returned intermittent 500 / connection-reset errors while serving the
1M-row partitions under large deltas (a service limitation, not a view
behaviour).  The 100% change points accumulate one full-table update file
per round without compaction; they are pathological and were stopped.  The
`partitioned` 1e5 cells already show the pruning (18–1627 rows scanned
against 100k), with the latency floor masking it at this size.

## What the numbers say

* **Reads are the dominant cost.**  The incremental refresh of `top_k`,
  `multi_join` and `chain` reads the full current sources and the whole
  materialized view every round regardless of the change size (multi_join
  ~4x the table, chain ~5x; `incr rows scanned` in the CSVs).  `lookup` is now
  key-filtered too (PLAN §10.180): it scans a few hundred rows instead of
  ~2.5x the table, but still opens every file, so its latency gain is small
  until the fixed file/metadata floor is addressed.  `row` now reads the delta plus the affected keys of its state
  (`read_current_filtered` with a primary-key `IN` list, up to 4096 keys;
  PLAN §10.179), so it scans 16 rows instead of ~1.9M at 1M/0.01% and its
  speedup roughly doubles.  The speedups therefore come from writing far fewer
  rows (129–592 vs 1,000,000 at 0.01%), and they decay as the delta grows
  (multi_join 2.3x at 0.01% but 1.0x at 1%).
* **Latency tracks the file count, not the rows.**  At 1M rows `lookup` reads
  1334 files in 1514 ms while `row` reads 518 in 667 ms: the 816-file gap
  accounts for 847 ms, about **1.0 ms per file**; `chain` reads 2634 files in
  3219 ms and its 5M scanned rows cost only ~0.25 s more.  The marginal cost
  is roughly **0.3 us per scanned row** and **4 us per written row**, so one
  file costs about as much as 3000–4000 scanned rows.
* **The state table is written as many tiny files**: a 100k-row `--keep` run
  wrote **107 files** for the incremental MV and **208** for the full-recompute
  MV (5–8 KB each), plus 56 files for the source; at 1M the MV is ~488 files
  by extrapolation (8192-row batches, four buckets).  The bench seed
  (`--batch 8192`) fragments every source the same way, so a join reads 2–5
  fragmented sources plus the MV (1300–2700 files at 1M).  The next lever is
  therefore file-count reduction (state-file coalescing first, then source
  compaction), not more row filtering; see `ROADMAP.md` §G.
* **A fixed floor of ~0.1–0.3 s per refresh** remains after the file term:
  `partitioned` scans only 18–1627 rows yet takes 0.70–1.9 s (568–702 files);
  planning, metadata round trips, S3 LIST/HEAD and the epoch commit dominate
  there.  The floor is about 2x higher on S3 than on local storage.
* **High change ratios erase the advantage**: `sum_count` at 10% changes is
  0.2x (11.5 s vs 2.1 s), `skew` at 1% is 0.9x and `multi_join` at 1% is 1.0x —
  the incremental delta/merge work grows with the batch while the full
  recompute stays a single sequential scan and write.
* **Partition pruning works** (18–1627 rows scanned for the partitioned
  shape) but needs a larger table to outweigh the fixed floor.
* **Mergeable aggregates are skew-insensitive**: the `skew` shape scans only
  ~18k rows at 1M/1% because `sum_count` keeps a group state; MIN/MAX or
  `COUNT(DISTINCT)` variants would rescan the hot group (recorded follow-up).

## Methodology notes

* Timing wraps `IvmSqlExecutor::execute`, so it includes planning, metadata,
  the source scan, the merge and the commit; source writes are separate.
* The full baseline is `INSERT OVERWRITE` on the same statement: it truncates
  and rewrites the whole MV, the cheapest fair "recompute everything".
* Warmup rounds are excluded from the summary; results are deterministic for
  a given `--seed`, and the CSV keeps every measured round so paired
  comparisons are possible.
* File format is Parquet; the initial seed uses `--batch` rows per write, so
  the table starts with `rows / batch` files per bucket.  File counts matter
  on S3; keep `--batch` consistent across runs you compare.
* B0 does not yet report bytes scanned or rows written (B1 adds the counters);
  for now compare wall-clock, actions and the change/group knobs.
* Pin the environment (same machine, same PostgreSQL, same RustFS) when
  comparing runs; object-store latency and OS caches dominate small-change
  rounds.
