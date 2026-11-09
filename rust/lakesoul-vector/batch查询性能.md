# batch 查询性能：DataFusion / Python / pgvector

> 测试时间：R84（整文件 payload 已移除；exact 只走按需 decoded-block cache）。
> GloVe-200：1,183,514 × 200d，4 hash bucket，nlist=1024，bits=1；
> top_k=100、nprobe=16、exact_overfetch=1；200 条真实 query；MinIO 本地。

## 1. 入口与配置

| 入口 | 说明 |
|---|---|
| DataFusion SQL loop | `LakeSoulTable.search` 每 query 一条 SQL，200 条顺序执行，同一 session |
| DataFusion native batch | `LakeSoulSession.batch_search`，200 query 一次调用，`batch_size=200` |
| Python batch API | `LakeSoulTable.search_batch`，200 query 一次调用，内部走 native snapshot batch |
| pgvector SQL loop | `postgres-lakesoul` PG wire，psycopg 单连接顺序执行 `<->` SQL |

exact 的公共缓存配置：

```bash
LAKESOUL_CACHE=1                 # 必需：S3-SSD 数据页 + 按需 decoded-block 缓存
LAKESOUL_CACHE_PATH=/tmp/lakesoul-batch-cache
LAKESOUL_CACHE_SIZE=8GiB
LAKESOUL_VECTOR_BLOCK_ROWS=256
LAKESOUL_VECTOR_BLOCK_CACHE_BYTES=268435456
```

没有整文件 payload 配置；`LAKESOUL_VECTOR_RERANK_CACHE*` 已删除。
pgvector 服务端额外使用 `--worker-threads 8`。

## 2. index-only（`rerank='index'`）

不读向量列，只按索引距离排序：

| 入口 | QPS | 进程 RSS | 备注 |
|---|---:|---:|---|
| DataFusion SQL loop | **412.0** | 303.5 MB | 每 query 重新 planning |
| DataFusion native batch | **5,934.7** | 293.9 MB | 200 query 一次调用 |
| Python `search_batch` | **6,522.4** | 286.9 MB | 与 native batch 同路径 |
| pgvector SQL loop | **634.2** | 258.3 MB（服务端） | 客户端 44.5 MB；PG wire 顺序 |

补充 R84 native batch（无缓存预热、同一 GloVe 表）：

| 入口 | QPS | RSS |
|---|---:|---:|
| `Session.batch_search(exact=False)` | **11,424.7** | 292.6 MB |

不同轮次差异来自缓存/进程状态，量级一致；index-only 始终是最高吞吐。

## 3. exact（`rerank='exact'`，按需 block cache）

native batch、一次 200 query、warm 3 轮：

| 模式 | 首 batch | warm QPS | RSS | 本地 block |
|---|---:|---:|---:|---:|
| **block 256，L2=1GiB** | 2131 ms | **386.6** | 858.7 MB | 917.6 MB |
| block 256，L2 仅 256MiB | 8153 ms | 9.4 | 973.1 MB | 258.9 MB（抖动） |

单查询 exact 参考（200 query 单条循环，block 256、L2=1GiB）：
首查 791.9 ms、warm 70.0 QPS、热重复 83.7 / 107.3 / 107.9 QPS、RSS 873.4 MB。

说明：

- exact 首 batch 包含候选块解码/写入；warm QPS 仍低于 index-only；
- batch exact 会同时触达大量候选块，block cache 的按需优势主要体现在
  首次不预写整列、逐查询/热点路径；
- 更高 batch exact 吞吐需要更细的块布局或预取优化，当前不提供整文件 payload
  作为快速路径。

## 4. 观察

1. **DataFusion SQL loop** 每 query 重新 planning，明显低于 native batch；
2. **Python `search_batch` 与 native batch** 基本同量级，Python 包装开销很小；
3. **pgvector SQL loop** 受 PG wire + 文本协议影响，单连接吞吐低于 native batch；
4. exact 的代价主要在向量读取/解码：index-only 不读向量列，QPS 高一个数量级；
5. exact block 的 L2 预算必须覆盖工作集；预算过小会频繁淘汰，吞吐骤降；
6. 需要高 exact batch 吞吐时应优先考虑扩大 L2、调整 `nprobe`/`overfetch`，
   或使用 index-only；不再提供整文件 payload 模式。

## 5. 清理

```bash
rm -rf "$LAKESOUL_CACHE_PATH"
```

PK index、原始数据页、decoded block 全在 `LAKESOUL_CACHE_PATH` 下，无独立
payload 目录。
