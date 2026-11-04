---
sidebar_position: 6
sidebar_label: 运维
---

# 运维

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 服务参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--bind` | `127.0.0.1` | 绑定地址（`LAKESOUL_PG_BIND`） |
| `-p`、`--port` | `5432` | 监听端口（`LAKESOUL_PG_PORT`） |
| `--warehouse-prefix` | — | 新表数据的基础位置（`LAKESOUL_WAREHOUSE`） |
| `--endpoint` | — | S3 兼容端点（`AWS_ENDPOINT`） |
| `--s3-bucket` | — | 默认 bucket（`AWS_BUCKET`） |
| `--s3-access-key` | — | Access key（`AWS_ACCESS_KEY_ID`） |
| `--s3-secret-key` | — | Secret key（`AWS_SECRET_ACCESS_KEY`） |
| `--s3-virtual-host-style` | `false` | 使用 virtual-hosted S3 请求 |
| `--worker-threads` | `2` | Tokio worker 线程数 |
| `--metrics-addr` | `127.0.0.1:19090` | Prometheus 指标监听地址 |
| `--max-connections` | `0`（不限） | 并发连接数 |
| `--max-queries-per-user` | `0` | 每个用户的并发语句数 |
| `--max-result-rows` | `0` | 每条语句的行数上限 |
| `--max-result-bytes` | `0` | 每条语句的编码结果大小上限 |
| `--ivm` | 关闭 | 为 `INSERT ... SELECT` 开启增量 SQL |
| `--distributed-workers` | — | 逗号分隔的 worker 端点 |
| `--distributed-fallback-to-local` | — | 分布式计划失败时本地执行 |

v1 中 `--ivm` 与分布式执行互斥。

## 运行时限制

限制按连接和语句生效。结果超过 `--max-result-rows` 或 `--max-result-bytes` 时语句失败，而不是流式返回无界结果。`--max-queries-per-user` 限制单个用户的并发语句；`--max-connections` 限制进程连接数。

## 指标

服务在 `--metrics-addr` 上暴露 Prometheus 指标：

- `lakesoul_query_requests_total`、`lakesoul_query_rows_total`、`lakesoul_query_duration_seconds` —— 查询结果和延迟，按 outcome 标注。
- `lakesoul_memory_reservation_failures_total` —— 内存准入失败。
- `lakesoul_worker_discovery_snapshot_age_seconds`、`lakesoul_worker_discovery_eligible_workers`、`lakesoul_worker_discovery_refresh_failures_total` —— 分布式发现。

## 取消

PostgreSQL cancel 请求会被处理：取消查询会停止执行并释放 worker。被取消的语句不会让连接不可用。

## 日志与 tracing

`RUST_LOG` 控制日志过滤器。`LAKESOUL_LOG_DIR` 把日志复制到目录供采集，`SIGHUP` 会从 `LAKESOUL_LOG_FILTER_FILE` 重新加载过滤器。tracing 配置提供 endpoint 时会启用 OTLP。

## 分布式执行

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix s3://bucket/warehouse \
  --distributed-workers http://127.0.0.1:50051,http://127.0.0.1:50052
```

coordinator 会跨 worker 计划；只有在设置 `--distributed-fallback-to-local` 时才会回退到本地执行。Kubernetes 发现使用 `--discovery kubernetes --k8s-worker-service ... --k8s-namespace ...`。

## 下一步

- [兼容性](compatibility.md)
- [元数据存储](../../operations/metadata-postgres.md)
- [工作空间与 RBAC](../../operations/workspace-and-rbac.md)
