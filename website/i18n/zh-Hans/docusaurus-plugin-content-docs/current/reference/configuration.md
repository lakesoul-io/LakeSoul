---
sidebar_position: 3
sidebar_label: 配置
---

# 配置

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 元数据连接

| 变量 | 含义 |
| --- | --- |
| `LAKESOUL_PG_URL` | JDBC 风格 PostgreSQL URL，例如 `jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified` |
| `LAKESOUL_PG_SECONDARY_URL` | 可选的只读 standby，用于元数据查询 |
| `LAKESOUL_PG_USERNAME` | 元数据用户 |
| `LAKESOUL_PG_PASSWORD` | 元数据密码 |
| `LAKESOUL_PG_POOL_SIZE` | 元数据连接池大小 |
| `LAKESOUL_PG_POOL_TIMEOUT_SECS` | 连接池获取超时 |
| `LAKESOUL_PG_POOL_IDLE_SECS` | 空闲连接超时 |

端点和 Python SDK 读取同一组变量。

## 对象存储

| 变量 / 参数 | 含义 |
| --- | --- |
| `LAKESOUL_WAREHOUSE` / `--warehouse-prefix` | 新表数据的基础位置 |
| `AWS_ENDPOINT` / `--endpoint` | S3 兼容端点 |
| `AWS_BUCKET` / `--s3-bucket` | 默认 bucket |
| `AWS_ACCESS_KEY_ID` / `--s3-access-key` | Access key |
| `AWS_SECRET_ACCESS_KEY` / `--s3-secret-key` | Secret key |
| `AWS_S3_VIRTUAL_HOST_STYLE` / `--s3-virtual-host-style` | Virtual-hosted 请求 |

Python SDK 构建索引时读取 `LAKESOUL_OBJECT_STORE_*`；见[向量索引管理](../engine/vector-search/index-management.md)。

## 端点

| 变量 | 含义 |
| --- | --- |
| `LAKESOUL_PG_BIND` | 绑定地址（默认 `127.0.0.1`） |
| `LAKESOUL_PG_PORT` | 监听端口（默认 `5432`） |
| `LAKESOUL_PG_METRICS_ADDR` | Prometheus 监听地址（默认 `127.0.0.1:19090`） |
| `LAKESOUL_PG_MAX_CONNECTIONS` | 连接数上限 |
| `LAKESOUL_PG_MAX_QUERIES_PER_USER` | 每个用户的并发语句数 |
| `LAKESOUL_PG_MAX_RESULT_ROWS` | 每条语句的行数上限 |
| `LAKESOUL_PG_MAX_RESULT_BYTES` | 每条语句的结果大小上限 |
| `LAKESOUL_PG_IVM` | 为 `INSERT ... SELECT` 开启增量 SQL |
| `LAKESOUL_WORKER_THREADS` | Tokio worker 线程数 |

完整参数表见 [PostgreSQL 运维](../engine/postgres/operations.md)。

## 分布式执行

| 变量 | 含义 |
| --- | --- |
| `LAKESOUL_DISTRIBUTED_DISCOVERY` | `none`、`static` 或 `kubernetes` |
| `LAKESOUL_DISTRIBUTED_WORKERS` | 逗号分隔的 worker 端点 |
| `LAKESOUL_K8S_WORKER_SERVICE` | 用于 EndpointSlice 发现的 worker Service 名 |
| `LAKESOUL_K8S_NAMESPACE` | Service 所在 namespace |
| `LAKESOUL_DISCOVERY_POLL_INTERVAL_SECONDS` | 发现刷新间隔 |
| `LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS` | 发现快照的过期上限 |
| `LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL` | 分布式计划失败时回退本地执行 |

## 日志

| 变量 | 含义 |
| --- | --- |
| `RUST_LOG` | 日志过滤器，例如 `info` 或 `lakesoul_io=debug` |
| `LAKESOUL_LOG_DIR` | 接收日志副本的目录 |
| `LAKESOUL_LOG_FILTER_FILE` | `SIGHUP` 时重新加载过滤器的文件 |

## 示例

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

## 下一步

- [PostgreSQL 运维](../engine/postgres/operations.md)
- [元数据存储](../operations/metadata-postgres.md)
