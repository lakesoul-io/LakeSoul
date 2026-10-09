# LakeSoul observability stack (Grafana + Prometheus + Tempo + Loki)

Local development stack for LakeSoul's OpenTelemetry traces, Prometheus metrics
and stdout logs. It uses host networking (Linux), so the LakeSoul processes run
on the host and everything talks over `127.0.0.1`.

Host networking removes Docker's port isolation, so every listener here is
pinned to loopback: Grafana's `GF_SERVER_HTTP_ADDR`, Prometheus'
`--web.listen-address`, Tempo's server plus OTLP receiver addresses, Loki's
HTTP/gRPC listeners, and Alloy's UI. A default `0.0.0.0` bind would put the
anonymous Grafana Editor session, Prometheus' unauthenticated remote-write
receiver, Tempo's OTLP ingest (an unauthenticated trace-write and
disk-consumption path) and Loki's unauthenticated push/query API on the host's
LAN, VPN or cloud interfaces.

```
LakeSoul processes ──OTLP/gRPC :4317──> Tempo ──span metrics──┐
        │                  │                                  │ remote_write
        │ /metrics         │ trace storage                    ▼
        │ :19090/19091/19000                         Prometheus :9090
        │                                                     │
        │ stdout ──tee──> LAKESOUL_LOG_DIR ──Alloy──> Loki :3100
        │                                                     │
        └─────────────────────────────────────────────────────┴─> Grafana :3000
```

| Service | URL | Purpose |
|---|---|---|
| Grafana | http://localhost:3000 (loopback only — `GF_SERVER_HTTP_ADDR=127.0.0.1`, since anonymous **Editor** + admin/admin must never be network-reachable) | Provisioned datasources + "LakeSoul Overview" dashboard (anonymous **Editor**, so Explore works; admin/admin). Pinned to 12.2.5: Traces Drilldown needs >= 11.6.11, and 11.6.x cannot parse Tempo 2.7's TraceQL metrics response (`promLabels`) |
| Prometheus | http://localhost:9090 | Scrapes LakeSoul `/metrics` and receives Tempo's span metrics |
| Tempo | http://localhost:3200 | OTLP ingest on 4317 (gRPC) / 4318 (HTTP), trace storage, span metrics |
| Loki | http://localhost:3100 | Log storage; receives the processes' stdout from Alloy, queried by Grafana |
| Alloy | http://localhost:12345 (UI, loopback only) | Tails `LAKESOUL_LOG_DIR` and pushes it to Loki |

## Start the stack

```sh
cd docker/observability
# The directory the justfile recipes tee into; create it before the stack so
# Docker does not create it root-owned.
mkdir -p "${LAKESOUL_LOG_DIR:-/tmp/lakesoul/logs}"
docker compose up -d
```

## Run LakeSoul against it

Every process that calls `lakesoul_observability::init_tracing` exports to the
endpoint from `OTEL_EXPORTER_OTLP_ENDPOINT`:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_SERVICE_NAME=postgres-lakesoul   # metrics `service` label + trace resource
                                             # (defaults to the binary role)
export RUST_LOG=info                         # filtered spans are not exported

# e.g. the PostgreSQL wire server (see rust/justfile for the full recipe)
cargo run -p postgres-lakesoul -- -p 6543 \
  --warehouse-prefix "s3://lakesoul-test-bucket" \
  --endpoint "http://localhost:9000" \
  --s3-bucket "lakesoul-test-bucket" \
  --s3-access-key "rustfsadmin" \
  --s3-secret-key "rustfsadmin"
```

The metrics endpoint listens on `--metrics-addr` (defaults: `19090` for
postgres-lakesoul, `19091` for lakesoul-worker, `19000` for lakesoul-flight) and
is already wired into Prometheus' scrape config. A launcher that overrides
`--metrics-addr` needs a matching entry in `prometheus/prometheus.yml`: the
`role` label there is what the dashboard and the `build_info` table join on.

### Build identity

`lakesoul-worker` and `postgres-lakesoul` report the commit they were built from
on all three telemetry paths — a stale replica is the usual explanation for
"works on worker-1, fails on worker-2":

| Where | What |
|---|---|
| Log | the startup line carries `version`/`commit`/`target`/`profile`; `--version` prints the same string |
| Prometheus | `build_info{version, commit, target, profile} 1` on every `/metrics` endpoint, joinable to the scrape labels `service`/`role`/`worker`; dashboard panel *Build identity (version / commit) by replica* |
| Tempo | `build.commit` resource attribute on every span |

### Logs into Loki

The processes run on the host and log to their terminal, which Alloy cannot read
from its container. The `worker-s3-1`, `worker-s3-2` and `pg` recipes in
`rust/justfile` therefore tee stdout into `LAKESOUL_LOG_DIR` (default
`/tmp/lakesoul/logs`), which is mounted into Alloy read-only; the
`rust/postgres-lakesoul/run-lakesoul-worker.sh` and `run-lakesoul-pg-demo.sh`
launchers redirect into the same directory under the same names. Start the
processes through one of those; a process started bare shows up in Prometheus
and Tempo but not in Loki.

The file name carries the identity, `<service>.<instance>.log`, and Alloy derives
the `service`/`worker` stream labels from it (Loki and Alloy add further labels
of their own, such as `filename` and `detected_level`):

| File | Stream |
|---|---|
| `lakesoul-worker.worker-1.log` | `{job="lakesoul", service="lakesoul-worker", worker="worker-1"}` |
| `lakesoul-worker.worker-2.log` | `{job="lakesoul", service="lakesoul-worker", worker="worker-2"}` |
| `postgres-lakesoul.pg.log` | `{job="lakesoul", service="postgres-lakesoul", worker="pg"}` |

`service` stays the role, never the replica: that is what makes one Grafana
query work across all three datasources.

### Two distributed workers with explicit identity

The distributed protocol has no worker id: the coordinator addresses workers by
their URL (`--distributed-workers`), and a worker cannot learn its own ordinal
from a task. Each replica therefore declares its identity in its own
deployment ("explicit injection") — see the `worker-s3-1` / `worker-s3-2`
recipes in `rust/justfile`:

| | worker-1 | worker-2 |
|---|---|---|
| gRPC | `127.0.0.1:50051` | `127.0.0.1:50052` |
| metrics | `127.0.0.1:19091` | `127.0.0.1:19092` |
| `OTEL_SERVICE_NAME` | `lakesoul-worker` | `lakesoul-worker` |
| `OTEL_RESOURCE_ATTRIBUTES` | `service.instance.id=worker-1` | `service.instance.id=worker-2` |
| Prometheus labels | `worker="worker-1"` | `worker="worker-2"` |

```sh
just worker-s3-1     # terminal 1
just worker-s3-2     # terminal 2

# coordinator: send stage tasks to both
cargo run -p postgres-lakesoul -- -p 6543 \
  --distributed-workers http://127.0.0.1:50051,http://127.0.0.1:50052 \
  --warehouse-prefix "s3://lakesoul-test-bucket" --endpoint "http://localhost:9000" \
  --s3-bucket "lakesoul-test-bucket" --s3-access-key "rustfsadmin" --s3-secret-key "rustfsadmin"
```

- **Tempo**: one service `lakesoul-worker`; per-replica filter
  `{resource.service.instance.id="worker-1"}`
- **Prometheus**: `sum by (worker) (rate(lakesoul_object_store_requests_total[5m]))`
- Keep `service.name` as the role, not the replica name: Tempo's service list,
  the dashboard's `service` variable and the span metrics stay stable while the
  replica labels vary.

## What to look at in Grafana

- **Drilldown → Traces** (`http://localhost:3000/a/grafana-exploretraces-app/`,
  installed through `GF_INSTALL_PLUGINS` on first start): RED metrics per
  service/span from the Tempo span metrics, click through to the matching
  traces.
- **Explore → Tempo**: `{resource.service.name="postgres-lakesoul"}` or the
  service dropdown; a trace shows `statement → physical_plan → table_scan →
  list_files_for_scan → metadata_query` and the merge stream. A merge work unit
  reads whatever physical format its table is stored in, so the worker's
  `merge_parquet_execute` spans carry `file_format` and one `file_scan` child
  per input file (`file`, `file_format`, `file_count`) — that is where a vortex
  scan is distinguishable from a parquet one. Below the scans, object store
  requests are spans named after their metric operation (`object_store_get`,
  `object_store_put`, `object_store_head`, `object_store_list`,
  `object_store_get_ranges`, `object_store_delete`, ...) and carrying the
  object `path`, the returned `bytes` and the `outcome`; the spans nest under
  the operation that issued them, so a slow `list_files_for_scan` reads as the
  per-file `object_store_head` calls below it. They come in the two levels the
  operation's span count justifies: `object_store_head`, `object_store_get` and
  `object_store_get_ranges` are `debug`, because one scan emits one per input
  file and per read range, while `put`/`list`/`delete`/parts/completion/abort
  are `info`, because they fire once per statement or commit — a failing upload
  belongs in the default trace, the per-chunk reads do not. Enable the scan
  spans with `RUST_LOG=info,lakesoul_io::object_store_metrics=debug`, or through
  the `LAKESOUL_LOG_FILTER_FILE` + `SIGHUP` route below. A request is never a
  root span: without a current span it is not traced at all — that is what
  keeps one-span traces out of Tempo. Vortex polls its reads on its own IO
  runtime, below a `trace`-level `vortex_io::spawn_io` span, so those reads join
  the trace only when it is enabled: `RUST_LOG=info,vortex_io::spawn_io=trace`.
  The request metrics — and therefore the dashboard's object store panels, which
  read `lakesoul_object_store_*` rather than span metrics — are unaffected by
  any of this. With the scan spans on, a trace is as long as its scan is wide
  (one head per input file, one ranged read per parquet chunk), which can
  outgrow Tempo's per-trace size limit (`max_bytes_per_trace`, 5 MiB by
  default); narrowing the filter back to `info`, or turning the target off
  (`…,lakesoul_io::object_store_metrics=off`), keeps the metrics and drops the
  spans.
- **Explore → Prometheus**: `lakesoul_query_duration_seconds_bucket`,
  `lakesoul_object_store_bytes_total`, `traces_spanmetrics_latency_bucket`,
  `build_info`, ...
- **Explore → Loki**: `{service="lakesoul-worker"}` for both replicas, or
  `{service="lakesoul-worker", worker="worker-1"}` for one. Add `| logfmt` to
  turn the `key=value` tail of the log lines into columns, so the startup
  banner's `commit` becomes a filterable field:
  `{service=~"lakesoul-.+"} | logfmt | commit="4465df735f5d"`.
- **LakeSoul Overview dashboard** (folder *LakeSoul*): query latency / rate,
  metadata DAO latency, object store throughput, cache hit ratio, execution
  stream p95, Tempo-generated span metrics, the per-replica build identity
  table, and the Loki log panel.
- **Trace → Metrics**: open a trace in Tempo, the *Metrics* tab queries the
  span metrics through the Prometheus datasource.
- **Trace → Logs**: the trace view's *Logs* link opens the Loki stream of the
  span's service. LakeSoul log lines carry no trace id, so the link matches on
  `service` plus the span's time range rather than on a trace id.
- **Metric → Trace**: panels with exemplars (span metrics) show a small dot;
  clicking it opens the trace in Tempo.

## Configuration

| Env var | Used by | Meaning |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | LakeSoul | OTLP endpoint; unset disables trace export |
| `OTEL_SERVICE_NAME` | LakeSoul | `service.name` resource attribute and the metrics `service` label (Loki's `service` label comes from the log file name instead) |
| `OTEL_TRACES_SAMPLER_ARG` | LakeSoul | Head sampling ratio in `[0,1]`, default `1.0` (parent-based). Any other value — `0,01` included — fails tracing initialization instead of silently sampling everything |
| `OTEL_TRACES_EXPORTER=none` / `OTEL_SDK_DISABLED=true` | LakeSoul | Disable trace export |
| `LAKESOUL_LOG_FILTER_FILE` | LakeSoul | Filter file re-read on `SIGHUP`; falls back to `RUST_LOG` |
| `LAKESOUL_LOG_DIR` | `rust/justfile`, Alloy | Directory the recipes tee stdout into and Alloy tails; default `/tmp/lakesoul/logs` |

## Runtime log level (SIGHUP)

Every process started through `lakesoul_observability::init_tracing` can reload
its log filter without a restart:

```sh
export LAKESOUL_LOG_FILTER_FILE=/tmp/lakesoul-log-filter
echo 'info' > "$LAKESOUL_LOG_FILTER_FILE"
# ... start postgres-lakesoul / lakesoul-worker / lakesoul-flight ...

# turn SQL logging on, then off again
echo 'info,lakesoul_sql=debug' > "$LAKESOUL_LOG_FILTER_FILE"
kill -HUP "$(pgrep -f postgres-lakesoul | head -1)"
# 17:11:09 INFO reloaded log filter after SIGHUP filter=info,lakesoul_sql=debug
# 17:11:10 DEBUG executing SQL (simple query) query_id=2 sql=select 2;

echo 'info' > "$LAKESOUL_LOG_FILTER_FILE"
kill -HUP "$(pgrep -f postgres-lakesoul | head -1)"
```

- Source order: `LAKESOUL_LOG_FILTER_FILE`, then `RUST_LOG`; an empty or
  unreadable source is refused and the current filter is kept.
- SQL logging uses the dedicated `lakesoul_sql` target
  (`executing SQL (simple query)` / `planning SQL (extended query)`) with
  `query_id`, so it can be toggled without enabling debug for every target.
- The switch only affects new events: spans already filtered out are not
  recovered, and metrics are unaffected.

## Stop

```sh
docker compose down          # keep data volumes
docker compose down -v       # drop traces, metrics, logs and dashboards state
```
