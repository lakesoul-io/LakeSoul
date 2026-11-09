# index-only / exact batch 查询端到端 QPS 与 RSS（MinIO + GloVe-200）

> 环境：MinIO 本地 `127.0.0.1:9000`，bucket `lakesoul-test`；LakeSoul 元数据为本地
> PostgreSQL。表 `vec_glove_batch`：GloVe base **1,183,514 行 × 200 维 f32**，4 个 hash
> bucket，Vortex，`nlist=1024`、`total_bits=1`、L2。查询为 200 条真实 GloVe query，
> `top_k=100`、`nprobe=16`、`exact_overfetch=1`，index-only 与 exact 分别测量。
>
> 所有 QPS 都是**预热后**的批量吞吐；每个进程独立启动。exact 首次查询会构建 903 MB
> 本地 payload（约 2 s），不计入下表 QPS。

## 1. 测试入口

| 入口 | 说明 |
|---|---|
| **DataFusion SQL loop** | `LakeSoulTable.search` 每 query 一条 SQL，200 条顺序执行，同一 session |
| **DataFusion native batch** | `LakeSoulSession.batch_search`，200 query 一次调用，`batch_size=200` |
| **Python batch API** | `LakeSoulTable.search_batch`，200 query 一次调用，内部走 native snapshot batch |
| **pgvector SQL loop** | `postgres-lakesoul` PG wire，psycopg 单连接顺序执行 200 条 `<->` SQL |

公共缓存配置（exact 需要）：

```bash
LAKESOUL_CACHE=1
LAKESOUL_CACHE_PATH=/tmp/lakesoul-batch-pk
LAKESOUL_CACHE_SIZE=8GiB
LAKESOUL_VECTOR_RERANK_CACHE=1
LAKESOUL_VECTOR_RERANK_CACHE_DIR=/tmp/lakesoul-batch-payload
LAKESOUL_VECTOR_RERANK_CACHE_BYTES=17179869184
```

pgvector 服务端额外使用 `--worker-threads 8`，并开启同样的 payload/key-index 缓存。

## 2. 结果

### index-only（`rerank='index'`）

| 入口 | QPS | 进程 RSS | 备注 |
|---|---:|---:|---|
| DataFusion SQL loop | **412.0** | 303.5 MB | 每 query 重新 planning |
| DataFusion native batch | **5,934.7** | 293.9 MB | 200 query 一次调用 |
| Python `search_batch` | **6,522.4** | 286.9 MB | 与 native batch 同路径，Python 包装 |
| pgvector SQL loop | **634.2** | 258.3 MB（服务端） | 客户端 RSS 44.5 MB；PG wire 顺序 |

### exact（`rerank='exact'`）

| 入口 | QPS | 进程 RSS | 备注 |
|---|---:|---:|---|
| DataFusion SQL loop | **315.2** | 350.3 MB | 每 query 重新 planning |
| DataFusion native batch | **6,492.2** | 338.1 MB | 预热后；首查约 563 ms（payload 已存在） |
| Python `search_batch` | **9,801–11,075**（约 10.3k） | 331.7–332.5 MB | 三次重复测量区间 |
| pgvector SQL loop | **447.3** | 301.8 MB（服务端） | 客户端 RSS 44.3 MB；首查 101 ms |

### 对照：单 query / SQL 路径的历史数据

- DataFusion SQL 单 query：index-only ~693 QPS、exact ~535 QPS（R57/R58 记录）；
- pgvector 单 query：index-only 781–791 QPS、exact 591–598 QPS（R58 记录，c=1、psql）；
- 本表 pgvector 数值是 **200 条顺序 batch workload + psycopg**，包含每条 SQL 的
  planning/协议开销，所以低于单 query 最优值，但仍在同一量级。

## 2.1 补充：exact block cache vs eager payload（R84）

同一 GloVe 表、200 query 一次 native batch、warm 3 轮：

| 模式 | 首 batch | warm QPS | RSS | 本地缓存 |
|---|---:|---:|---:|---:|
| **index-only（无 payload）** | 1219 ms | **11,424.7** | 292.6 MB | — |
| **exact block 256，L2=1GiB** | 2131 ms | **386.6** | 858.7 MB | block 917.6 MB |
| **exact eager payload（16GiB）** | 1441 ms | **6,561.4** | 455.9 MB | payload 903.0 MB |

说明：block 首 batch 包含候选块解码/写入，warm 3 轮后 386.6 QPS；eager 首 batch
包含 903 MB payload 构建，之后 6,561 QPS。index-only batch 不读向量列，最高。
按需 block 适合“逐查询/热点”路径，batch exact 的高吞吐仍建议 eager payload 或
index-only；block 不是 batch 的吞吐最优选择。

## 3. 观察

1. **batch 入口是数量级提升**：DataFusion 的 native batch 把 200 个 query 的 snapshot
   解析、shard 解析和候选合并做一次，index-only 从 SQL loop 的 412 QPS 提升到约 5.9k，
   exact 从 315 QPS 提升到约 6.5k（Python 包装约 10k）。
2. **pgvector 的收益主要受 PG wire / 每语句 planning 限制**：单连接顺序 batch 只有
   634（index）/447（exact）QPS；要提升需要多连接或客户端并行，而不是期望单连接
   batch 接近 native batch。
3. **RSS**：DataFusion batch 在 287–338 MB；SQL loop 因为 session/plan/cache 状态在
   303–350 MB；pgvector 服务端在 258–302 MB，客户端约 44 MB。exact 比 index-only
   多约 44–92 MB，主要是 payload/index 缓存与候选读取缓冲。
4. **exact 批量并不一定比 index-only 慢**：exact batch 每个 query 只读 100 个候选
   payload 行（80 KB），候选合并/并行后吞吐与 index-only 同量级；但 exact 首查需要
   构建 payload（约 2 s），RSS 也更高。实际选择应看 recall/延迟要求。
5. **DataFusion native batch 与 Python `search_batch` 的差异**：两者都走 native
   snapshot batch，实测 Python 包装的 QPS 更高（尤其 exact 约 10k vs 6.5k），
   属于同路径下的调用/实现差异，建议以同一入口做回归，不要跨入口混用数字。
6. 本机 MinIO 与查询进程同机，网络不是瓶颈；真实远程 S3 下，本测试的首查/缓存行为
   会不同，但 batch 稳态 QPS 主要受 CPU/内存限制。

## 4. 测试与清理

- 表：`vec_glove_batch`（GloVe-200，4 桶，1024 lists，bits=1）；
- 每个入口/模式和缓存配置独立进程；
- 测试后：
  - `drop_table("vec_glove_batch")`，PG `table_info=0`；
  - 删除 MinIO `s3://lakesoul-test/vec_glove_batch/`；
  - 删除 `/tmp/lakesoul-batch-pk`、`/tmp/lakesoul-batch-payload`；
  - 停止 `postgres-lakesoul` 与 MinIO，删除测试数据目录。
