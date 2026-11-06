---
sidebar_position: 6
---

# Operations

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Server flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--bind` | `127.0.0.1` | Bind address (`LAKESOUL_PG_BIND`) |
| `-p`, `--port` | `5432` | Listen port (`LAKESOUL_PG_PORT`) |
| `--warehouse-prefix` | — | Base location for new table data (`LAKESOUL_WAREHOUSE`) |
| `--endpoint` | — | S3-compatible endpoint (`AWS_ENDPOINT`) |
| `--s3-bucket` | — | Default bucket (`AWS_BUCKET`) |
| `--s3-access-key` | — | Access key (`AWS_ACCESS_KEY_ID`) |
| `--s3-secret-key` | — | Secret key (`AWS_SECRET_ACCESS_KEY`) |
| `--s3-virtual-host-style` | `false` | Use virtual-hosted S3 requests |
| `--worker-threads` | `2` | Tokio worker threads |
| `--metrics-addr` | `127.0.0.1:19090` | Prometheus metrics listener |
| `--no-metrics` | off | Do not start the Prometheus endpoint |
| `--subscription-log-dir` | `LAKESOUL_LOG_DIR` or a temp directory | Logs for supervised subscription processes |
| `--max-connections` | `0` (unlimited) | Concurrent connections |
| `--max-queries-per-user` | `0` | Concurrent statements per user |
| `--max-result-rows` | `0` | Row limit per statement |
| `--max-result-bytes` | `0` | Encoded result size limit per statement |
| `--ivm` | off | Enable incremental SQL for `INSERT ... SELECT` |
| `--distributed-workers` | — | Comma-separated worker endpoints |
| `--distributed-fallback-to-local` | — | Run a query locally when distributed planning fails |

`--ivm` and distributed execution are mutually exclusive in v1.

## Runtime limits

Limits are enforced per connection and per statement. A result that exceeds
`--max-result-rows` or `--max-result-bytes` fails the statement instead of
streaming an unbounded result. `--max-queries-per-user` limits concurrent
statements for one user; `--max-connections` limits the process.

## Metrics

The server exposes Prometheus metrics on `--metrics-addr`:

- `lakesoul_query_requests_total`, `lakesoul_query_rows_total`,
  `lakesoul_query_duration_seconds` — query outcomes and latency, labelled by
  outcome.
- `lakesoul_memory_reservation_failures_total` — memory admission failures.
- `lakesoul_worker_discovery_snapshot_age_seconds`,
  `lakesoul_worker_discovery_eligible_workers`,
  `lakesoul_worker_discovery_refresh_failures_total` — distributed discovery.

## Cancellation

PostgreSQL cancel requests are honoured: cancelling a query stops the execution
and releases its workers. A cancelled statement does not leave the connection
unusable.

## Logs and tracing

`RUST_LOG` controls the log filter. `LAKESOUL_LOG_DIR` tees logs to a directory
for collection, and `SIGHUP` reloads the filter from
`LAKESOUL_LOG_FILTER_FILE`. OTLP tracing is enabled when the tracing
configuration provides an endpoint.

## Distributed execution

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix s3://bucket/warehouse \
  --distributed-workers http://127.0.0.1:50051,http://127.0.0.1:50052
```

The coordinator plans across the workers and falls back to local execution only
when `--distributed-fallback-to-local` is set. Kubernetes discovery is available
with `--discovery kubernetes --k8s-worker-service ... --k8s-namespace ...`.

## Next

- [Compatibility](compatibility.md)
- [Metadata store](../../operations/metadata-postgres.md)
- [Workspace and RBAC](../../operations/workspace-and-rbac.md)
