---
sidebar_position: 3
sidebar_label: 配置
---

# Configuration

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Metadata connection

| Variable | Meaning |
| --- | --- |
| `LAKESOUL_PG_URL` | JDBC-style PostgreSQL URL, e.g. `jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified` |
| `LAKESOUL_PG_SECONDARY_URL` | Optional read-only standby for metadata queries |
| `LAKESOUL_PG_USERNAME` | Metadata user |
| `LAKESOUL_PG_PASSWORD` | Metadata password |
| `LAKESOUL_PG_POOL_SIZE` | Metadata connection pool size |
| `LAKESOUL_PG_POOL_TIMEOUT_SECS` | Pool checkout timeout |
| `LAKESOUL_PG_POOL_IDLE_SECS` | Idle connection timeout |

The endpoint and the Python SDK read the same variables.

## Object storage

| Variable / flag | Meaning |
| --- | --- |
| `LAKESOUL_WAREHOUSE` / `--warehouse-prefix` | Base location for new table data |
| `AWS_ENDPOINT` / `--endpoint` | S3-compatible endpoint |
| `AWS_BUCKET` / `--s3-bucket` | Default bucket |
| `AWS_ACCESS_KEY_ID` / `--s3-access-key` | Access key |
| `AWS_SECRET_ACCESS_KEY` / `--s3-secret-key` | Secret key |
| `AWS_S3_VIRTUAL_HOST_STYLE` / `--s3-virtual-host-style` | Virtual-hosted requests |

The Python SDK reads `LAKESOUL_OBJECT_STORE_*` for index builds; see
[Vector index management](../engine/vector-search/index-management.md).

## Endpoint

| Variable | Meaning |
| --- | --- |
| `LAKESOUL_PG_BIND` | Bind address (default `127.0.0.1`) |
| `LAKESOUL_PG_PORT` | Listen port (default `5432`) |
| `LAKESOUL_PG_METRICS_ADDR` | Prometheus listener (default `127.0.0.1:19090`) |
| `LAKESOUL_PG_MAX_CONNECTIONS` | Connection limit |
| `LAKESOUL_PG_MAX_QUERIES_PER_USER` | Concurrent statements per user |
| `LAKESOUL_PG_MAX_RESULT_ROWS` | Row limit per statement |
| `LAKESOUL_PG_MAX_RESULT_BYTES` | Result size limit per statement |
| `LAKESOUL_PG_IVM` | Enable incremental SQL on `INSERT ... SELECT` |
| `LAKESOUL_WORKER_THREADS` | Tokio worker threads |

The full flag table is in [PostgreSQL operations](../engine/postgres/operations.md).

## Distributed execution

| Variable | Meaning |
| --- | --- |
| `LAKESOUL_DISTRIBUTED_DISCOVERY` | `none`, `static` or `kubernetes` |
| `LAKESOUL_DISTRIBUTED_WORKERS` | Comma-separated worker endpoints |
| `LAKESOUL_K8S_WORKER_SERVICE` | Worker Service name for EndpointSlice discovery |
| `LAKESOUL_K8S_NAMESPACE` | Namespace holding the Service |
| `LAKESOUL_DISCOVERY_POLL_INTERVAL_SECONDS` | Discovery refresh interval |
| `LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS` | Staleness limit for a discovery snapshot |
| `LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL` | Fall back to local execution when distributed planning fails |

## Logging

| Variable | Meaning |
| --- | --- |
| `RUST_LOG` | Log filter, e.g. `info` or `lakesoul_io=debug` |
| `LAKESOUL_LOG_DIR` | Directory that receives a copy of the logs |
| `LAKESOUL_LOG_FILTER_FILE` | File read on `SIGHUP` to reload the filter |

## Example

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix s3://my-bucket/warehouse \
  --endpoint http://127.0.0.1:9000 \
  --s3-bucket my-bucket \
  --s3-access-key rustfsadmin \
  --s3-secret-key rustfsadmin
```

## Next

- [PostgreSQL operations](../engine/postgres/operations.md)
- [Metadata store](../operations/metadata-postgres.md)
