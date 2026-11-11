# 四数据集小缓存基准（2026-11-11）

## 1. 目的

逐个数据集测试 SIFT、GloVe、GIST、Deep-Image：

- 构建时间、构建 RSS、写表时间；
- index-only / exact；
- 单查询 / batch；
- Python native search、DataFusion SQL、pgvector；
- 两种缓存配置：**small = L1 512 MiB + L2 512 MiB**，**medium = L1 1 GiB + L2 1 GiB**；
- QPS、first latency、recall@100、RSS。

**每个数据集完成全部测试后立即：**

```text
drop table
删除本地 table/index 目录
删除本地 cache 目录
停止 pgvector 服务
```

然后才进入下一个数据集。

## 2. 环境和方法

- 数据：本地 HDF5 → 本地 LakeSoul 表（file store），避免 MinIO 网络噪声；绝对 QPS 会比 S3 高。
- 索引：nlist=1024，total_bits=1，hash_bucket_num=2。
- 查询：top_k=100，nprobe=16，exact_overfetch=1。
- SIFT/GloVe NQ=10；GIST/Deep NQ=5。
- batch QPS 为同一批查询 2 轮的平均吞吐；single 为逐条查询。
- recall 对 HDF5 ground truth 前 100 计算。
- Python native 用 `table.search` / `table.search_batch`；
- DataFusion 用 `session.sql` 执行 `ORDER BY <distance> LIMIT`；
- pgvector 用 psycopg 连接 `postgres-lakesoul` 6543；`rss_mb` 是**客户端进程** RSS，不是服务端 RSS。

## 3. 构建

| 数据集 | 行数 | dim | 写表 s | 写表 RSS MB | 构建 s | 构建 RSS MB |
|---|---:|---:|---:|---:|---:|---:|

| sift | 1,000,000 | 128 | 3.0 | 777 | 7.0 | 421 |
| glove | 1,183,514 | 200 | 7.8 | 1895 | 14.3 | 723 |
| gist | 1,000,000 | 960 | 29.8 | 5967 | 52.5 | 722 |
| deep | 9,990,000 | 96 | 32.6 | 7599 | 176.9 | 1306 |

构建 RSS 为单独 build 进程的 `VmRSS`，不包含写表阶段的峰值。

## 4. 查询结果

每格格式：**QPS / mean ms / RSS MB / recall**。


### sift

**cache=small**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 146.4 / 6.8 / 199 / 0.554 | 2070.1 / 0.5 / 202 / 0.554 | 11.1 / 89.8 / 965 / 0.864 | 197.9 / 5.1 / 1130 / 0.734 |
| datafusion | 162.4 / 6.2 / 196 / 0.554 | 565.2 / 1.8 / 200 / 0.554 | 27.2 / 36.8 / 495 / 0.734 | 163.9 / 6.1 / 521 / 0.734 |
| pgvector | 64.8 / 15.4 / 64 / 0.573 | 381.4 / 2.6 / 65 / 0.573 | 11.5 / 86.7 / 65 / 0.978 | 16.5 / 60.7 / 65 / 0.978 |

**cache=medium**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 154.0 / 6.5 / 198 / 0.554 | 2144.8 / 0.5 / 203 / 0.554 | 18.4 / 54.4 / 873 / 0.864 | 501.1 / 2.0 / 947 / 0.734 |
| datafusion | 168.6 / 5.9 / 196 / 0.554 | 558.2 / 1.8 / 204 / 0.554 | 20.8 / 48.0 / 545 / 0.734 | 436.7 / 2.3 / 547 / 0.734 |
| pgvector | 64.0 / 15.6 / 64 / 0.573 | 353.5 / 2.8 / 64 / 0.573 | 17.6 / 56.7 / 64 / 0.978 | 30.9 / 32.3 / 64 / 0.978 |

### glove

**cache=small**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 139.9 / 7.1 / 215 / 0.319 | 1606.3 / 0.6 / 224 / 0.319 | 3.3 / 305.6 / 787 / 0.563 | 82.4 / 12.1 / 970 / 0.395 |
| datafusion | 140.5 / 7.1 / 217 / 0.319 | 394.5 / 2.5 / 228 / 0.319 | 10.5 / 95.4 / 542 / 0.395 | 111.6 / 9.0 / 622 / 0.395 |
| pgvector | 51.6 / 19.4 / 69 / 0.358 | 247.8 / 4.0 / 69 / 0.358 | 3.3 / 302.9 / 69 / 0.762 | 3.1 / 319.5 / 69 / 0.762 |

**cache=medium**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 127.3 / 7.9 / 216 / 0.319 | 1504.2 / 0.7 / 226 / 0.319 | 7.0 / 142.1 / 1274 / 0.563 | 201.6 / 5.0 / 1448 / 0.395 |
| datafusion | 136.1 / 7.3 / 219 / 0.319 | 391.6 / 2.6 / 231 / 0.319 | 16.4 / 60.9 / 632 / 0.395 | 174.6 / 5.7 / 706 / 0.395 |
| pgvector | 54.4 / 18.4 / 68 / 0.358 | 263.8 / 3.8 / 69 / 0.358 | 7.4 / 135.4 / 69 / 0.762 | 14.7 / 68.2 / 69 / 0.762 |

### gist

**cache=small**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 81.4 / 12.3 / 223 / 0.712 | 695.3 / 1.4 / 229 / 0.712 | 2.2 / 460.0 / 752 / 0.854 | 53.2 / 18.8 / 898 / 0.850 |
| datafusion | 64.2 / 15.6 / 213 / 0.712 | 195.9 / 5.1 / 243 / 0.712 | 12.9 / 77.2 / 495 / 0.850 | 97.7 / 10.2 / 571 / 0.850 |
| pgvector | 30.7 / 32.6 / 60 / 0.762 | 139.3 / 7.2 / 60 / 0.762 | 2.2 / 458.7 / 60 / 0.986 | 1.8 / 549.4 / 60 / 0.986 |

**cache=medium**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 75.8 / 13.2 / 220 / 0.712 | 674.7 / 1.5 / 226 / 0.712 | 2.4 / 415.9 / 1110 / 0.854 | 88.1 / 11.3 / 1258 / 0.850 |
| datafusion | 70.3 / 14.2 / 217 / 0.712 | 185.0 / 5.4 / 241 / 0.712 | 4.9 / 202.5 / 529 / 0.850 | 167.5 / 6.0 / 553 / 0.850 |
| pgvector | 27.2 / 36.7 / 60 / 0.762 | 158.0 / 6.3 / 60 / 0.762 | 2.5 / 405.8 / 60 / 0.986 | 2.5 / 401.0 / 60 / 0.986 |

### deep

**cache=small**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 30.9 / 32.4 / 304 / 0.572 | 242.3 / 4.1 / 308 / 0.572 | 1.8 / 567.5 / 977 / 0.950 | 65.7 / 15.2 / 1098 / 0.756 |
| datafusion | 32.2 / 31.1 / 311 / 0.572 | 127.6 / 7.8 / 378 / 0.572 | 12.7 / 78.6 / 814 / 0.756 | 76.0 / 13.2 / 892 / 0.756 |
| pgvector | 6.3 / 157.6 / 63 / 0.590 | 43.1 / 23.2 / 63 / 0.590 | 0.9 / 1127.4 / 63 / 0.996 | 1.6 / 627.1 / 63 / 0.996 |

**cache=medium**

| 接口 | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| python | 45.4 / 22.0 / 290 / 0.572 | 239.1 / 4.2 / 307 / 0.572 | 2.1 / 480.7 / 1243 / 0.950 | 83.4 / 12.0 / 1329 / 0.756 |
| datafusion | 44.6 / 22.4 / 309 / 0.572 | 135.1 / 7.4 / 359 / 0.572 | 11.6 / 86.1 / 822 / 0.756 | 116.4 / 8.6 / 853 / 0.756 |
| pgvector | 13.3 / 75.4 / 63 / 0.590 | 42.2 / 23.7 / 63 / 0.590 | 2.1 / 470.0 / 63 / 0.996 | 1.9 / 514.7 / 63 / 0.996 |

## 5. 观察

1. **构建**：Deep 10M 构建 177 s、RSS 1.3 GB；GIST 52.5 s、RSS 0.72 GB；SIFT/GloVe 分别约 7 s/14 s。流式构建 RSS 已明显低于早期 3–5 GB。
2. **小缓存对 index-only 单查影响有限**：SIFT 约 146–162 QPS，GIST 64–83 QPS，Deep 31–45 QPS；medium 相对 small 有提升，但不是数量级。
3. **exact 对缓存最敏感**：SIFT small exact 单查 11–27 QPS，medium 18–28 QPS；GIST exact 单查约 2–13 QPS；Deep exact 单查约 0.9–12.7 QPS。
4. **batch 通常明显更高**：SIFT index batch 约 0.6k–2.1k QPS；GIST 约 0.14k–0.70k；Deep 约 0.13k–0.24k。
5. **pgvector**：QPS 低于 Python native/DataFusion SQL；exact batch 在 GIST/Deep 上尤其慢（约 1.6–2.5 QPS）。pgvector 的 `rss_mb` 是客户端进程，不是服务端。
6. **recall**：bits=1 + nprobe=16 的 index-only recall 约 0.32–0.71；exact 提升但 batch exact 的 recall 可能低于 single exact（batch native exact 使用 `exact_overfetch=1`，候选更窄），不要把 batch exact 当成更高精度路径。
7. 所有数据集完成后已清理 table/index/cache；每轮结束都执行 drop。

## 6. 局限

- 使用本地 file store，不是 MinIO/S3；绝对 QPS 不能直接和之前 S3 结果比较。
- pgvector 只测到客户端 RSS；服务端 RSS 需要单独采集 `/proc/<pid>/status`。
- exact single/batch 的 recall 差异来自候选 overfetch 路径不同，不是结果不一致。
- 只测了 nlist=1024,bits=1,nprobe=16；nlist/nbits 组合见之前的完整参数基准。
