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
| `k8s/profiles/{large,xl}` | definitions only (no cluster capacity) |
| `k8s/components/sampler` | sidecar sampling cgroup v1 and `/proc` once per second |
| `k8s/jobs` | driver and datagen Jobs |
| `k8s/blocked/` | slow storage/worker (netem), node pressure, Chaos Mesh: definitions only, not authorized on the shared cluster |
| `scripts/bench.py` | deploy, datagen, drive, collect |
| `scripts/report.py` | summary, compare, memory tables |
| `scripts/*.sh` | scenario suites: memory matrix, backpressure, worker buffer budget, concurrency, faults |
| `reports/` | inventory and final reports |
| `results/` | raw results, one directory per run |

## Requirements

- `kubectl` context `external`, namespace `lakesoul`, with `lakesoul-secrets`
  and `lakesoul-config` created out of band (see `../k8s/README.md`).
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
| `NAME.prom.json` | Prometheus range queries for the window |
| `NAME.pods.json` | pod status, restarts, last termination reason |

`collect --run-dir --name --since --until` re-collects the last four for a
window, for runs not started by `drive`.

Crash-loop guards: `tpch --max-outages N` (default 3) stops after N dropped
queries, and `load` ends the run at the first dropped connection
(`--stop-on-outage`, default on) and writes an `aborted` record, so an
OOM-killed coordinator is not driven into CrashLoopBackOff.
`scripts/faults.sh` passes `--no-stop-on-outage` because its pod kills are
intentional.

## Scenario suites

| Script | Covers |
|---|---|
| `scripts/memory-matrix.sh PREFIX SERVICE_TAG BENCH_TAG PROFILE:BUDGET:POOL...` | per cell: fresh deploy, serial SF10 heavy queries, then a closed-loop burst (`BURST_CONCURRENCY`, `BURST_SECONDS`) |
| `scripts/backpressure.sh RUN_DIR BENCH_TAG [CASE...]` | slow client at max/10MiB/1MiB/100KiB/s and stall, simple vs extended protocol, pk vs nopk |
| `scripts/concurrency.sh RUN_DIR BENCH_TAG closed|open|isolation` | closed loop 1..128, open loop 0.25–8 qps, short-query isolation against CPU/memory-heavy background |
| `scripts/faults.sh RUN_DIR BENCH_TAG SCENARIO...` | cancel, statement timeout, FIN/RST disconnect, connection flood, extreme `max_rows`, coordinator and worker pod kill under load |

Environment variables at the top of each script set defaults (queries,
timeouts, levels, rates).

## Results policy

Raw results are kept. `results/.gitignore` excludes `*.app.*.log` and
`*.sampler.*.jsonl`, which exceed the 1 MiB jj snapshot limit; they remain on
the machine that ran the benchmark. Invalid attempts are kept under a
separate directory with a `NOTE.md` explaining why (for example
`results/m1-attempt/`). Environment metadata is in `results/env/`.

Tables are created under the warehouse configured in `lakesoul-secrets`
(`tpch_sf1`, `tpch_sf10`, `tpch_sf1_nopk`); the console ignores `LOCATION`.
