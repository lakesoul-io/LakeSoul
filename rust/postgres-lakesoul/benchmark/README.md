# Kubernetes-native SQL service benchmark

Harness for benchmarking the LakeSoul PG-wire SQL service (coordinator
`postgres-lakesoul` plus `lakesoul-worker`) on a real cluster. Results are
TPC-H-derived (tpchgen-cli 3.0.0 data and answers); they are not audited
TPC-H results.

| Path | Purpose |
|---|---|
| `driver/` | `lsbench`, a stdlib-only async PG-wire driver (see `driver/README.md`) |
| `workloads/tpch/` | q01–q22 with `{{SF}}` placeholders, answers for sf0.1/sf1/sf10 |
| `workloads/mixes/` | load mixes: `short`, `cpu`, `mem`, `mixed` (paths relative to the mix file) |
| `datagen/tpch.sh` | tpchgen → `lakesoul-console` loader (`LAYOUT=pk|pkmonth|nopk`, `SCHEMA`, `BUCKETS`, `CHUNKS`) |
| `k8s/base` | patches over `../k8s` overlay: Recreate strategy, bench image |
| `k8s/profiles/{small,medium,throttled}` | coordinator/worker requests and limits |
| `k8s/profiles/pgwire` | 4 cores / 8 GiB Guaranteed coordinator and workers with 40Gi scratch PVCs, one per `Type=compute` node, apart from bench and observability Pods (single node vs PG-wire comparison) |
| `k8s/profiles/pgwire-single` | `pgwire` with discovery disabled and the conflicting Kubernetes service/namespace settings removed; use with `--workers 0`. Set `DATAFUSION_EXECUTION_TARGET_PARTITIONS=4` to match the distributed sessions' partition count. |
| `k8s/profiles/{large,xl}` | definitions only (no cluster capacity) |
| `k8s/components/sampler` | sidecar sampling cgroup v1 and `/proc` once per second |
| `k8s/jobs` | driver, datagen and console Jobs |
| `k8s/blocked/` | slow storage/worker (netem), node pressure, Chaos Mesh: definitions only, not authorized on the shared cluster |
| `scripts/bench.py` | deploy, datagen, drive, console, collect |
| `scripts/report.py` | summary, compare, memory tables |
| `scripts/*.sh` | scenario suites: memory matrix, backpressure, worker buffer budget, concurrency, faults |
| `reports/` | inventory and final reports |
| `results/` | raw results, one directory per run |

## Requirements

- `kubectl` context `external`, namespace `lakesoul`, with `lakesoul-secrets`
  and `lakesoul-config` created out of band (see `../k8s/README.md`).
- Observability requires the `lakesoul-grafana` Secret with an `admin-password` key; create it using the command in `../k8s/README.md`. `deploy` waits for Grafana as well as the query service, Prometheus and Tempo.
- Service and bench images pushed under new tags (never overwrite `:dev`):

  ```sh
  P=$(nix build .#bench-image --no-link --print-out-paths | tail -1); $P | docker load
  R=swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo
  docker tag lakesoul-bench:dev $R/lakesoul-bench:<commit12>
  docker push --quiet $R/lakesoul-bench:<commit12>
  ```

  Record every tag and digest in `results/images.json`.

## Workflow

```sh
cd rust/postgres-lakesoul/benchmark
# 1. Load data (Job in the cluster; pk layout = PRIMARY KEY + 16 hash buckets)
python3 scripts/bench.py datagen --run-dir results/d01-datagen --bench-tag <tag> --sf 1 \
  --location s3://<bucket>/bench/tpch_sf1 [--layout pkmonth|nopk] [--buckets 16] [--chunks 8]
# 2. Fresh (cold) deployment; waits for rollout and worker discovery
python3 scripts/bench.py deploy --run-dir results/<run> --profile medium \
  --service-tag <service-tag> --bench-tag <tag> --workers 2 \
  [--env all:LAKESOUL_MEMORY_LIMIT=2Gi --env all:LAKESOUL_MEMORY_POOL=fair]
# 3. Run a driver Job and collect its evidence
python3 scripts/bench.py drive --run-dir results/<run> --bench-tag <tag> --name tpch-sf1 \
  -- tpch --host lakesoul-coordinator --database tpch_sf1 --sf 1 --iterations 5 \
     --answers /bench/workloads/tpch/answers/sf1
# 4. Summaries
python3 scripts/report.py summary results/<run> --name tpch-sf1
python3 scripts/report.py compare results/<base> results/<cand> --name tpch-sf1
python3 scripts/report.py memory results/m2-*
```

`datagen`, `drive` and `console` take `--node-selector K=V` (repeatable) for
clusters that dedicate labelled nodes to workloads, e.g.
`--node-selector Type=compute`; `console --node` adds
`kubernetes.io/hostname` to the same selector.

Layouts (`datagen/tpch.sh` documents them in full):

| `--layout` | Schema | Distributed execution |
|---|---|---|
| `pk` | `tpch_sf<N>` | one work unit per (range partition, hash bucket) pair, read from the committed file names; a table without range columns is one implicit partition, so it still yields one unit per populated bucket |
| `pkmonth` | `tpch_sf<N>_pkmonth` | orders/lineitem range-partitioned by month (80/84 partitions) on top of the hash buckets, so their units are (month, bucket) pairs; the six dimension tables stay unpartitioned and split by bucket alone |
| `nopk` | `tpch_sf<N>_nopk` | append-only: scans split by bytes across worker tasks |

Each merge owns whole work units, and the units are packed over the
distributed tasks. Both `pk` and `pkmonth` therefore distribute. For a
like-for-like comparison, pass `--buckets 16` for both layouts; other runs
can choose a different bucket count.

`SCHEMA=<name> datagen/tpch.sh ...` overrides the per-layout schema name.
Choose an unused name: tables in the selected schema are dropped and
recreated. Set `LAKESOUL_WAREHOUSE` to a fresh object-store prefix as well;
the console derives table locations from that warehouse and the schema,
overriding SQL `LOCATION`. `bench.py datagen` does not forward `SCHEMA`,
so use a run-specific Job environment for isolated loads.

`pkmonth` writes a file per month × bucket × chunk, so lower `--buckets` at
small SF. The TPC-H queries do not filter the month columns, so they measure
full-scan merge throughput, not partition pruning.

For regular Parquet/Vortex scans the coordinator plans from PostgreSQL table
metadata and the selected snapshot's ordered file descriptors; planning issues no
data-file HEAD, GET, range or list request. It dispatches `LakeSoulScanExec`
descriptors instead of preparing per-file readers. MOR keeps each
partition/bucket unit atomic across format changes; append-only scans distribute
files and legal Parquet byte ranges, while Vortex files stay whole.

Those work units — not whole range partitions — are packed round-robin over the
tasks the scan stage may get, so `pkmonth`'s (month, bucket) units spread over
every worker instead of stopping at the month count. A unit is never divided, and
a node holds at most `target_partitions` open units at a time. Monthly fan-out
still increases unit construction and dispatch, so balanced worker assignments do
not by themselves establish good end-to-end scaling.

Readers initialize on the first poll of an executing unit, including
file-schema/statistics inference and primary-key candidate retrieval.
Execution-time metadata reads share a process-wide admission budget across
tables and sessions, for both Parquet and Vortex. The budget reads
`DATAFUSION_EXECUTION_META_FETCH_CONCURRENCY` on first use; set it before starting
the process. A session can lower its own concurrency but cannot raise the process
budget. Admission precedes metadata reads and per-file configuration allocation,
and covers schema/statistics inference and configuration construction. Completed
files immediately free a slot; returned plans retain input group/file order for
merge version semantics. This is a metadata concurrency budget, not a query
memory or general execution-I/O limit.

For scan-planning diagnosis, coordinator INFO logs include `scan planning table
summary` (per-table `wall_ms` split into `classify_ms`, `key_constraint_ms`,
`list_ms`, `work_units_ms`, `scan_execs_ms` and `finish_ms`, plus `unit_count`
and `scan_node_count`), `scan initialization flatten summary` (eager flattening
only: file/group counts and cumulative schema, statistics and local
configuration-building time), and physical/distributed planner summaries. Match
them by `query_id` and `table_name`; timestamps and elapsed times locate
overlapping table scans. `list_ms` is the metadata replay, not an object-store
listing; `work_units_ms` builds the descriptors and `scan_execs_ms` packs nodes
and constructs the scan executors. Per-file `schema_sum_ms` and `stats_sum_ms`
include async waiting, not just CPU or remote I/O, and must not be added as
sequential wall time. Waiting for admission contributes to flatten wall time, not
to the cumulative schema, statistics or configuration-building times. The
summaries cover successful regular scans, not empty/index early returns or failed
operations.

`scan planning schema summary` isolates reads issued by `infer_schema`, including
Vortex's spawned inference and I/O tasks. A schema-only store wrapper carries the
counter handle through requests and streamed payload consumption; stats and
execution continue using the original store. `files_with_reads` counts files
that issued non-HEAD reads, not page-cache misses. `page_hits`/`page_misses`,
`remote_bytes`, and `requested_bytes` distinguish cached bytes from remote page
fills. The flatten summary's `physical_format` identifies the actual format;
an `infer_stats` span by itself does not establish Parquet use. Metadata-only
scans emit no such summary per unit: their planning performs no per-file
preparation, and their execution-time reads are counted by
`lakesoul_object_store_requests_total` instead.

All schema timing fields are cumulative, not sequential query wall time:
`store_read_sum_ms` covers requests through stream completion;
`range_sum_ms` nests inside it; `remote_sum_ms`, `lookup_read_sum_ms`, and
`cache_write_sum_ms` nest inside cached range reads. `range_head_sum_ms` is also
inside range time, while `get_opts_head_sum_ms` precedes payload consumption.
Cache-write time includes cache-entry contention, blocking-task scheduling and
disk writes, not just device service time. Schema time outside store reads
includes task/runtime scheduling, metadata decoding, schema conversion and
merging; it is not a pure decoder measurement. Independent PG connections build
new runtime metadata caches, while the process-wide disk page cache may stay
warm. Do not infer metadata-cache hits from a faster second query.

`scan planning cache put summary` splits successful schema-read cache fills by
table and `outcome` (`write` or existing-entry `nop`). It emits no event when that
outcome has no calls. `count` counts operations; `bytes` counts their input
payloads, so only the `write` outcome represents bytes actually written.
`total_sum_ms`/`total_max_ms` measure inside `DiskCache::put`, before recording
the aggregate counters; the caller's `cache_write_sum_ms` also includes that
recording and the call boundary.

The `stages` array names the matching `stage_sum_ns` and `stage_max_ns` positions:
preparation, entry admission, path preparation, payload copy, blocking queue,
open, write, resume, commit, and bookkeeping. Entry admission runs until the
Moka compute closure begins and is not a pure lock measurement. Copy includes
the existing `to_vec` before blocking-task submission. Blocking queue runs from
that submission boundary until the blocking closure starts. Open and write
measure `OpenOptions::open` and `write_all` separately; no `fsync` is added.
Resume includes the blocking closure's epilogue, join delivery and async
rescheduling after the write completes. Commit includes result construction and
Moka completion after async resumption; bookkeeping covers size/key registration.
For a `nop`, copy and blocking/file stages remain zero.

Each successful call checks its stage sum against its own total. Unclassified
checkpoint gaps accumulate in `residual_sum_ns`/`residual_max_ns`;
`overlap_count` must be zero. Sum the stage totals plus residual to reconcile
`total_sum_ms` (allowing float-format rounding), but never add independent
per-stage maxima. These are cumulative elapsed durations, not sequential query
wall time or device service time. A dominant blocking-queue stage locates the
wait before closure execution; it does not by itself identify the runtime's
pool size, other pool users, or the underlying storage/CPU cause.

Keep the metadata concurrency budget fixed when comparing planning-stage
measurements.
Label the first query after a coordinator rollout as coordinator-cold only:
worker and remote-store caches have not necessarily been reset.

To check planning I/O on a cluster, snapshot per-pod
`lakesoul_object_store_requests_total` and `lakesoul_object_store_bytes_total`
around plain `EXPLAIN` on otherwise idle applications: require zero read deltas
on coordinator and workers, then execute a distributed read as a positive
control. Record pod UIDs and restart counts to exclude counter resets.
`EXPLAIN ANALYZE` executes the query and is not a planning-only check; small
queries may execute locally even with worker discovery enabled.

For a native-console smoke check, use `INSERT OR REPLACE` to write a newer
version of an existing primary key; plain `INSERT` rejects that conflict.
Read after both commits and verify that non-key predicates cannot resurrect
superseded rows, in both local and distributed execution.

Lazy scan descriptors use distributed protocol `/3`, adding a physical codec
beyond `/2`. Coordinators and workers must advertise the updated generation
together; do not roll a lazy-scan coordinator out against `/2` workers.

`deploy` always recreates the pods, so the first measured iteration is cold.
It returns only after the coordinator log reports
`EndpointSlice refresh ok ... eligible=<workers>`; queries issued earlier fail
with `found no eligible worker` because the resolver error bypasses
`LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL`.

`drive` writes, per `--name`:

| File | Content |
|---|---|
| `NAME.jsonl` | driver samples, ticks, summaries, recovery records |
| `NAME.driver.log`, `NAME.driver.json` | driver Job log and arguments |
| `NAME.sampler.<role>.<pod>.jsonl` | 1 Hz cgroup/proc samples per pod |
| `NAME.app.<role>.<pod>.log` | service logs for the window |
| `NAME.prom.json` | Prometheus range queries for the window, including each process's RSS/RssAnon, jemalloc allocated/active/resident and pool reserved bytes |
| `NAME.pods.json` | pod status, restarts, last termination reason |

`collect --run-dir --name --since --until` re-collects the last four for a
window, for runs not started by `drive`.

Crash-loop guards: `tpch --max-outages N` (default 3) stops after N dropped
queries, and `load` ends the run at the first dropped connection
(`--stop-on-outage`, default on) and writes an `aborted` record, so an
OOM-killed coordinator is not driven into CrashLoopBackOff.
`scripts/faults.sh` passes `--no-stop-on-outage` because its pod kills are
intentional.

## lakesoul-console engine runs

`bench.py console` measures the DataFusion engine without the PG-wire
coordinator: `lsbench console` runs `lakesoul-console` in a Job Pod and
parses its per-statement `Elapsed` times into `NAME.jsonl` (same schema as
`tpch`, `scenario: "console"`). Requests equal limits for every container, so
the Pod is Guaranteed QoS with a fixed CPU quota and memory limit; the
console gets one tokio thread per CPU unless `--worker-threads` is passed.
Pod anti-affinity keeps it off the coordinator, worker and observability
nodes and off any node already running another bench Pod: two console Pods
sharing a node made queries 2.1–2.6× slower at equal CPU time ([INFERENCE]
shared disk and page-cache IO) (`results/20261027-pool-vs-limit-feb90f43/`).
Its `/var/scratch` (spill, disk cache, tracing log) is a 40Gi per-Pod PVC from
the default StorageClass, like a worker's. Bench Job Pods carry
`cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`; without it the
cluster autoscaler evicted a running console when it scaled down a node.

```sh
# Single node: the console plans and executes everything in its Pod
python3 scripts/bench.py console --run-dir results/<run> --bench-tag <tag> --name console-sf1 \
  --cpu 4 --mem 8Gi [--node <node>] [--env LAKESOUL_MEMORY_LIMIT=6Gi] [--env LAKESOUL_CACHE=1] \
  -- --database tpch_sf1_nopk --sf 1 --iterations 3 --warmup 1 --target-partitions 4 \
     --answers /bench/workloads/tpch/answers/sf1
# Distributed: every ready lakesoul-worker Pod becomes a --worker
python3 scripts/bench.py console --run-dir results/<run> --bench-tag <tag> --name console-sf1-dist \
  --cpu 2 --mem 4Gi --distributed \
  -- --database tpch_sf1_nopk --sf 1 --iterations 3 --bytes-per-partition 16777216
```

Without workers, `--target-partitions` becomes `SET
datafusion.execution.target_partitions` (the console's single-node default
is 1). The worker Pods keep the resources of the last `deploy`. `--answers`
compares values as in `tpch` (`lakesoul-console --format json` prints typed
rows).
The console reads the coordinator's execution-memory and local-cache
variables (`LAKESOUL_MEMORY_LIMIT`, `LAKESOUL_MEMORY_POOL`,
`LAKESOUL_VORTEX_DECODE_BUDGET`, `LAKESOUL_CACHE`, `LAKESOUL_CACHE_SIZE`,
`LAKESOUL_CACHE_PATH`; see `../k8s/README.md`), so `--env` bounds its
memory and caches warehouse reads on the Pod's `/var/scratch` volume.
The console runs on jemalloc (see `../k8s/README.md`). It does not read
`lakesoul-config`, so `bench.py console` passes the ConfigMap's
`MALLOC_CONF=background_thread:true` itself; `--env MALLOC_CONF=...`
overrides it. With `--env RUST_LOG=lakesoul_console=info`, the console writes
a `memory allocator` line naming the allocator and its settings to its
tracing log under `--log-dir` (default `/var/scratch` in the Pod).

`console` writes `NAME.jsonl`, `NAME.console.log` (console output without
result rows), `NAME.console.json` (node, QoS class, resources, allowed
CPUs, worker URLs, termination state), the console's
`NAME.sampler.console.<pod>.jsonl`, plus the service evidence of `collect`.
The samples are read from the container log after the Pod completes, so an
OOM-killed console still leaves the finished queries and an `aborted` record.

Every console query record also carries `memory`: the console's in-process
samples taken while the query ran (`lsbench console
--memory-sample-interval-ms`, default 100), as `{"samples": n, "peak": {...},
"at_rss_anon_peak": {...}}` over `rss_bytes`, `rss_anon_bytes`,
`jemalloc_allocated_bytes`, `jemalloc_active_bytes`, `jemalloc_resident_bytes`
and `pool_reserved_bytes` (`null` when there were no samples). `peak` holds
each view's own maximum; `at_rss_anon_peak` is the one sample with the highest
RssAnon, where the views compare at one instant. `report.py summary` prints
them per query; `report.py memory` prints the coordinator's views at its
Prometheus RssAnon peak (15 s scrapes, so between-scrape peaks are missed).

## Scenario suites

| Script | Covers |
|---|---|
| `scripts/memory-matrix.sh PREFIX SERVICE_TAG BENCH_TAG PROFILE:BUDGET:POOL...` | per cell: fresh deploy, serial SF10 heavy queries, then a closed-loop burst (`BURST_CONCURRENCY`, `BURST_SECONDS`) |
| `scripts/backpressure.sh RUN_DIR BENCH_TAG [CASE...]` | slow client at max/10MiB/1MiB/100KiB/s and stall, simple vs extended protocol, pk vs nopk |
| `scripts/concurrency.sh RUN_DIR BENCH_TAG closed|open|isolation` | closed loop 1..128, open loop 0.25–8 qps, short-query isolation against CPU/memory-heavy background |
| `scripts/faults.sh RUN_DIR BENCH_TAG SCENARIO...` | cancel, statement timeout, FIN/RST disconnect, connection flood, extreme `max_rows`, coordinator and worker pod kill under load |

Environment variables at the top of each script set defaults (queries,
timeouts, levels, rates).

## Teardown safety

Do **not** run `kubectl delete -f <run>/k8s/rendered.yaml`: the rendered
resource set includes the shared `lakesoul` Namespace. Deleting that Namespace
also deletes unrelated PostgreSQL workloads, Secrets and PVCs; a disk with
reclaim policy `Delete` can then be destroyed. Delete only explicitly named
benchmark Deployments, Services, Jobs and benchmark PVCs. Preserve the
Namespace, `postgres-0`, `data-postgres-0` and the out-of-band Secrets.

## Results policy

Raw results are kept. `results/.gitignore` excludes `*.app.*.log` and
`*.sampler.*.jsonl`, which exceed the 1 MiB jj snapshot limit; they remain on
the machine that ran the benchmark. Invalid attempts are kept under a
separate directory with a `NOTE.md` explaining why (for example
`results/m1-attempt/`). Environment metadata is in `results/env/`.

Tables are created under the warehouse configured in `lakesoul-secrets`; the
console ignores `LOCATION`. The current data set was loaded with:

| Schema | Flags | Objects |
|---|---|---|
| `tpch_sf1` | `--sf 1 --buckets 16` | 114 |
| `tpch_sf1_pkmonth` | `--sf 1 --layout pkmonth --buckets 4` | 679 |
| `tpch_sf1_nopk` | `--sf 1 --layout nopk` | 50 |
| `tpch_sf10` | `--sf 10 --buckets 16 --chunks 8 --mem-req 3Gi` | 798 |
| `tpch_sf10_pkmonth` | `--sf 10 --layout pkmonth --buckets 4 --chunks 8 --mem-req 3Gi` | 5400 |
| `tpch_sf10_nopk` | `--sf 10 --layout nopk --mem-req 3Gi`, lineitem with `--chunks 4` | 482 |

Every `nopk` table has one file per tpchgen part (8 at SF1, 80 at SF10;
nation and region one each): `nopk` chunks are key ranges of whole parts, so
SF10 lineitem in 4 chunks has the same 80 files of ~36 MiB as one statement.

At SF10 the `pkmonth` and `nopk` loads were OOM-killed at the 6Gi default
limit while writing lineitem, the last table. The `nopk` sink runs one writer
per part concurrently, so a single statement holds all 80 lineitem parts in
flight. Rerunning only lineitem in a new Job (`--tables lineitem --mem-lim
8Gi`, plus `--chunks 4` for `nopk`) succeeded; the script drops and recreates
the table. A dropped table keeps its files, so delete
`warehouse/<schema>/lineitem/` before the rerun.

On the second cluster (warehouse `s3://dmetasoul-bucket/lakesoul-k8s/warehouse`,
workloads on `Type=compute` nodes) the three SF10 schemas were loaded in one
pass each, with the same layout flags and the same object counts as above:
`tpch_sf10` with `--mem-req 3Gi` (246 s), `tpch_sf10_pkmonth` and
`tpch_sf10_nopk` (`--chunks 4` for every table) with `--mem-req 10Gi
--mem-lim 10Gi` (580 s, 284 s). Run evidence is in
`results/20261027-datagen-sf10-{pk,pkmonth,nopk}/`. SF1 is not loaded there.
