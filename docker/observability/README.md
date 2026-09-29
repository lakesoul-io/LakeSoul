# LakeSoul observability stack (Grafana + Prometheus + Tempo)

Local development stack for LakeSoul's OpenTelemetry traces and Prometheus
metrics. It uses host networking (Linux), so the LakeSoul processes run on the
host and everything talks over `127.0.0.1`.

Host networking removes Docker's port isolation, so every listener here is
pinned to loopback: Grafana's `GF_SERVER_HTTP_ADDR`, Prometheus'
`--web.listen-address`, and Tempo's server plus OTLP receiver addresses. A
default `0.0.0.0` bind would put the anonymous Grafana Editor session,
Prometheus' unauthenticated remote-write receiver and Tempo's OTLP ingest (an
unauthenticated trace-write and disk-consumption path) on the host's LAN, VPN
or cloud interfaces.

```
LakeSoul processes ──OTLP/gRPC :4317──> Tempo ──span metrics──┐
        │                  │                                  │ remote_write
        │ /metrics         │ trace storage                    ▼
        │ :19090/19091/19000                         Prometheus :9090
        └──────────────────────────────────────────────┴─────> Grafana :3000
```

| Service | URL | Purpose |
|---|---|---|
| Grafana | http://localhost:3000 (loopback only — `GF_SERVER_HTTP_ADDR=127.0.0.1`, since anonymous **Editor** + admin/admin must never be network-reachable) | Provisioned datasources + "LakeSoul Overview" dashboard (anonymous **Editor**, so Explore works; admin/admin). Pinned to 12.2.5: Traces Drilldown needs >= 11.6.11, and 11.6.x cannot parse Tempo 2.7's TraceQL metrics response (`promLabels`) |
| Prometheus | http://localhost:9090 | Scrapes LakeSoul `/metrics` and receives Tempo's span metrics |
| Tempo | http://localhost:3200 | OTLP ingest on 4317 (gRPC) / 4318 (HTTP), trace storage, span metrics |

## Start the stack

```sh
cd docker/observability
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
is already wired into Prometheus' scrape config.

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
  list_files_for_scan → metadata_query` and the merge stream.
- **Explore → Prometheus**: `lakesoul_query_duration_seconds_bucket`,
  `lakesoul_object_store_bytes_total`, `traces_spanmetrics_latency_bucket`, ...
- **LakeSoul Overview dashboard** (folder *LakeSoul*): query latency / rate,
  metadata DAO latency, object store throughput, cache hit ratio, execution
  stream p95, and Tempo-generated span metrics.
- **Trace → Metrics**: open a trace in Tempo, the *Metrics* tab queries the
  span metrics through the Prometheus datasource.
- **Metric → Trace**: panels with exemplars (span metrics) show a small dot;
  clicking it opens the trace in Tempo.

## Configuration

| Env var | Used by | Meaning |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | LakeSoul | OTLP endpoint; unset disables trace export |
| `OTEL_SERVICE_NAME` | LakeSoul | `service.name` resource attribute and the metrics `service` label |
| `OTEL_TRACES_SAMPLER_ARG` | LakeSoul | Head sampling ratio in `[0,1]`, default `1.0` (parent-based). Any other value — `0,01` included — fails tracing initialization instead of silently sampling everything |
| `OTEL_TRACES_EXPORTER=none` / `OTEL_SDK_DISABLED=true` | LakeSoul | Disable trace export |
| `LAKESOUL_LOG_FILTER_FILE` | LakeSoul | Filter file re-read on `SIGHUP`; falls back to `RUST_LOG` |

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
docker compose down -v       # drop traces, metrics and dashboards state
```
