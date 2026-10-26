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
| `datagen/tpch.sh` | tpchgen → `lakesoul-console` loader (`LAYOUT=pk|pkmonth|nopk`, `BUCKETS`, `CHUNKS`) |
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
| `pk` | `tpch_sf<N>` | none: an unpartitioned PK table is one merge pinned to one task, so the whole query stays on the coordinator |
| `pkmonth` | `tpch_sf<N>_pkmonth` | orders/lineitem range-partitioned by month (80/84 merges): scans spread over up to one task per month; the six dimension tables stay single-task |
| `nopk` | `tpch_sf<N>_nopk` | append-only: scans split by bytes across worker tasks |

`pkmonth` writes a file per month × bucket × chunk, so lower `--buckets` at
small SF. The TPC-H queries do not filter the month columns, so they measure
full-scan merge throughput, not partition pruning.

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
