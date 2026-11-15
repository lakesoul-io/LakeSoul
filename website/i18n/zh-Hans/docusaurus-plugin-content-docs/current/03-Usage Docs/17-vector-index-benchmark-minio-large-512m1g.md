# 向量索引大规模基准（L1 512MiB / L2 1GiB）

> **补测完成（2026-11-14）：30/30组结果已验证。** 全部专属索引、表对象及缓存已按来源清理；20组跨接口配对的164,000个query比较完全同序，测量后8项原有湖仓兼容性回归通过。原始数据与核验证据见文末。

本页使用原生批查询重测 Python、DataFusion SQL 和 LakeSoul pgwire。单查固定 **200** 条；GIST批查 **1,000** 条；SIFT/GloVe/Deep-Image批查 **10,000** 条真实query。DataFusion/pgwire批查均为一次SQL，不是客户端逐条SQL循环。

## 1. 方法与历史校正

| 项目 | 本轮设定 |
|---|---|
| L1 | `LAKESOUL_VECTOR_MEMORY_BYTES=512MiB` |
| L2 | `LAKESOUL_CACHE_SIZE=1GiB`，每接口/模式独立cache |
| 存储 | MinIO `s3://lakesoul-bench2`，独立owner前缀 |
| 环境 | KVM x86_64，16 vCPU（8核×2线程），Intel family6/model173，AVX2/AVX-512，约60GiB RAM，无swap；非独占宿主机 |
| 索引 | IVF + RaBitQ，7bits，4hash buckets，top-k=100 |
| 角度数据 | GloVe、Deep的base/query先归一化，按IP检索 |
| exact | ANN候选原始向量重排，`exact_overfetch=1`，不等价于全库精确kNN保证 |
| 单查 | 每配置新进程/新应用cache，顺序执行200条真实查询，包含第一条；P50/P90/P99为实测单条延迟 |
| 缓存顺序 | full batch在该配置的200条单查之后执行，已有应用预热；不清空操作系统或MinIO缓存，不宣称全系统冷启动 |
| 原生batch | 完整调用仅计时一次，读取全部返回结果；QPS=完成query数/该次wall秒数 |
| 输入准备 | Python batch向量已在内存；SQL full batch读取query表。构造列表/inline SQL在计时外，SQL执行、query源扫描和全部结果消费在计时内 |
| 返回负载 | Python原生batch返回每query的ID列表；SQL/pgwire返回query_id、rank、id、distance四列，包含更多编码与传输成本 |
| batch百分位 | 另跑100-query chunk归一化采样，**不计入full batch分母**，不是每query服务延迟 |
| 并行 | native query并行度使用auto（进程共享CPU池上限16）；SQL window上限256；pgwire Tokio worker线程4，与native CPU并行度不是同一参数 |
| 构建 | Rust release，`-C target-cpu=native`；仅本机性能构建，不作为跨CPU通用产物 |
| 清理 | 同时只测一个数据集；每个run/接口组完成后停止专属进程，drop专属表并删除其prefix/cache，再继续；Deep中断恢复的两组各自建表并清理 |

旧DataFusion/pgwire的batch列是SQL loop，不能作为native batch性能。后续旧补测脚本还出现过full batch与百分位采样混入同一计时分母、结果重复导致recall越界。本页用新runner的真实测量替换，不将旧QPS乘2推算。原2026-11-11 raw保留在[旧记录](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/sift-512m1g.jsonl)。第一轮新runner还触发cache初始化删除owner标记，已在修正owner目录后整轮重跑；旧失败run不混入本表。

本次有效记录来自2026年11月13日的20组完整测量和11月14日的10组Deep补测。13日的Deep运行收到外部SIGTERM；它的未完成exact测量未纳入本表，完成的20组保留。补测使用相同二进制SHA256、参数和query/truth指纹，未重跑已完成组。各来源的中断记录、恢复清理及选择关系保留在manifest和raw的source_run字段。不同日期的非独占宿主机负载可能影响耗时，不作微小差距的稳定性承诺。

两个SQL入口共享原生引擎：`lakesoul_vector_search_batch`、`lakesoul_vector_search_batch_from_table`。本轮full batch用query表入口，chunk采样用inline JSON。SQL算子会跨小Arrow输入batch填充有界query window，避免GIST每batch35行导致过小窗口；1,000query回归实际验证为4个窗口。pgwire是LakeSoul的PostgreSQL/pgvector兼容前端，不是上游pgvector扩展的batch能力。输入/输出、LUT/heap和候选使用受限window及工作内存；缓存容量不等于进程RSS，RSS包含运行时、映射和瞬时工作。

### 写表与构建

| 数据集 | base rows | dim | nlist | bits | 写表s | 构建s | 构建RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | 1,000,000 | 128 | 256 | 7 | 8.19 | 23.51 | 492.6 |
| GloVe-200 | 1,183,514 | 200 | 1024 | 7 | 19.23 | 57.99 | 802.8 |
| GIST-960 | 1,000,000 | 960 | 256 | 7 | 78.94 | 55.53 | 1077.0 |
| Deep-Image-96 | 9,990,000 | 96 | 1024 | 7 | 78.19 | 148.41 | 1419.2 |

## 2. Python native

### 单查200条

| 数据集 | rerank | nprobe | 单查QPS | 单查总s | P50 ms | P90 ms | P99 ms | recall@100 | 客户端/进程RSS MiB | 服务端RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 206.4 | 0.969 | 3.279 | 3.449 | 5.109 | 0.9747 | 405.7 | — |
| SIFT-128 | exact | 64 | 58.4 | 3.423 | 9.989 | 18.048 | 119.200 | 0.9998 | 790.6 | — |
| GloVe-200 | index | 512 | 56.0 | 3.570 | 7.106 | 8.456 | 148.237 | 0.9712 | 676.3 | — |
| GloVe-200 | exact | 512 | 21.4 | 9.362 | 16.724 | 137.957 | 272.140 | 0.9913 | 1046.6 | — |
| GIST-960 | index | 64 | 9.9 | 20.214 | 8.218 | 255.352 | 343.184 | 0.9802 | 1346.1 | — |
| GIST-960 | exact | 64 | 2.4 | 81.890 | 403.199 | 440.433 | 1060.365 | 0.9988 | 1703.9 | — |
| Deep-Image-96 | index | 32 | 17.5 | 11.423 | 57.550 | 71.962 | 81.376 | 0.9503 | 753.0 | — |
| Deep-Image-96 | exact | 32 | 4.6 | 43.955 | 210.399 | 226.884 | 304.364 | 0.9674 | 1445.2 | — |
| Deep-Image-96 | index | 64 | 9.2 | 21.795 | 112.548 | 135.810 | 150.129 | 0.9687 | 829.9 | — |
| Deep-Image-96 | exact | 64 | 3.6 | 55.107 | 266.659 | 299.951 | 474.919 | 0.9894 | 1589.9 | — |

### 一次完整native batch

| 数据集 | rerank | nprobe | batch_nq | batch QPS | 实际总s | P50 ms/query | P90 ms/query | P99 ms/query | recall@100 | 客户端/进程RSS MiB | 服务端RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 10000 | 4460.0 | 2.242 | 0.499 | 0.546 | 0.572 | 0.9747 | 688.2 | — |
| SIFT-128 | exact | 64 | 10000 | 1416.8 | 7.058 | 1.193 | 1.241 | 1.279 | 0.9997 | 1290.9 | — |
| GloVe-200 | index | 512 | 10000 | 1204.3 | 8.304 | 3.019 | 3.228 | 3.407 | 0.9718 | 871.6 | — |
| GloVe-200 | exact | 512 | 10000 | 660.3 | 15.144 | 3.757 | 4.511 | 4.755 | 0.9918 | 1661.0 | — |
| GIST-960 | index | 64 | 1000 | 1004.7 | 0.995 | 4.843 | 5.024 | 5.448 | 0.9789 | 1841.6 | — |
| GIST-960 | exact | 64 | 1000 | 35.1 | 28.485 | 68.380 | 70.261 | 70.608 | 0.9980 | 2385.4 | — |
| Deep-Image-96 | index | 32 | 10000 | 109.5 | 91.353 | 20.497 | 21.338 | 21.833 | 0.9533 | 1720.5 | — |
| Deep-Image-96 | exact | 32 | 10000 | 25.5 | 392.644 | 88.591 | 90.291 | 114.010 | 0.9707 | 2420.1 | — |
| Deep-Image-96 | index | 64 | 10000 | 58.0 | 172.364 | 32.743 | 34.599 | 35.308 | 0.9691 | 1719.0 | — |
| Deep-Image-96 | exact | 64 | 10000 | 20.8 | 480.200 | 101.420 | 104.460 | 127.619 | 0.9900 | 2600.8 | — |

这里的batch P50/P90/P99来自单独的100-query chunk采样，不是full batch中的单query实际延迟；full batch耗时与QPS仅对应一次完整调用。

## 3. DataFusion 原生批量SQL

### 单查200条

| 数据集 | rerank | nprobe | 单查QPS | 单查总s | P50 ms | P90 ms | P99 ms | recall@100 | 客户端/进程RSS MiB | 服务端RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 233.6 | 0.856 | 2.742 | 2.981 | 3.778 | 0.9747 | 415.6 | — |
| SIFT-128 | exact | 64 | 60.5 | 3.305 | 9.521 | 18.755 | 134.812 | 0.9999 | 758.6 | — |
| GloVe-200 | index | 512 | 56.6 | 3.531 | 6.656 | 8.276 | 161.164 | 0.9712 | 685.7 | — |
| GloVe-200 | exact | 512 | 21.2 | 9.434 | 16.699 | 140.218 | 262.915 | 0.9913 | 1138.0 | — |
| GIST-960 | index | 64 | 10.6 | 18.861 | 7.288 | 251.251 | 261.279 | 0.9802 | 1357.1 | — |
| GIST-960 | exact | 64 | 2.4 | 82.036 | 398.034 | 429.128 | 1067.058 | 0.9988 | 1651.8 | — |
| Deep-Image-96 | index | 32 | 17.6 | 11.390 | 57.779 | 71.404 | 82.606 | 0.9503 | 744.2 | — |
| Deep-Image-96 | exact | 32 | 4.5 | 44.392 | 211.287 | 232.846 | 481.945 | 0.9674 | 1462.7 | — |
| Deep-Image-96 | index | 64 | 9.2 | 21.780 | 111.943 | 134.007 | 151.299 | 0.9687 | 846.0 | — |
| Deep-Image-96 | exact | 64 | 3.7 | 54.158 | 264.822 | 289.692 | 395.987 | 0.9894 | 1566.8 | — |

### 一次完整native batch

| 数据集 | rerank | nprobe | batch_nq | batch QPS | 实际总s | P50 ms/query | P90 ms/query | P99 ms/query | recall@100 | 客户端/进程RSS MiB | 服务端RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 10000 | 4758.5 | 2.101 | 0.527 | 0.561 | 0.575 | 0.9747 | 578.6 | — |
| SIFT-128 | exact | 64 | 10000 | 1479.2 | 6.760 | 1.228 | 1.284 | 9.680 | 0.9997 | 1195.8 | — |
| GloVe-200 | index | 512 | 10000 | 1227.1 | 8.150 | 3.060 | 3.238 | 3.413 | 0.9718 | 877.2 | — |
| GloVe-200 | exact | 512 | 10000 | 682.0 | 14.662 | 4.168 | 4.576 | 10.740 | 0.9918 | 1637.7 | — |
| GIST-960 | index | 64 | 1000 | 1111.2 | 0.900 | 4.620 | 5.049 | 5.668 | 0.9789 | 1837.8 | — |
| GIST-960 | exact | 64 | 1000 | 35.1 | 28.509 | 68.022 | 69.244 | 69.683 | 0.9980 | 2524.6 | — |
| Deep-Image-96 | index | 32 | 10000 | 108.0 | 92.634 | 20.579 | 21.597 | 22.143 | 0.9533 | 1560.9 | — |
| Deep-Image-96 | exact | 32 | 10000 | 25.3 | 396.032 | 88.985 | 90.406 | 93.169 | 0.9707 | 2422.3 | — |
| Deep-Image-96 | index | 64 | 10000 | 57.3 | 174.516 | 32.783 | 34.641 | 35.700 | 0.9691 | 1748.7 | — |
| Deep-Image-96 | exact | 64 | 10000 | 20.6 | 484.484 | 101.487 | 106.455 | 116.153 | 0.9900 | 2640.5 | — |

这里的batch P50/P90/P99来自单独的100-query chunk采样，不是full batch中的单query实际延迟；full batch耗时与QPS仅对应一次完整调用。

## 4. LakeSoul pgwire 原生批量SQL

### 单查200条

| 数据集 | rerank | nprobe | 单查QPS | 单查总s | P50 ms | P90 ms | P99 ms | recall@100 | 客户端/进程RSS MiB | 服务端RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 242.4 | 0.825 | 2.508 | 2.859 | 3.396 | 0.9747 | 131.4 | 323.5 |
| SIFT-128 | exact | 64 | 60.0 | 3.331 | 9.533 | 17.514 | 135.729 | 0.9999 | 130.4 | 692.6 |
| GloVe-200 | index | 512 | 56.8 | 3.518 | 5.844 | 8.070 | 168.977 | 0.9712 | 139.0 | 892.5 |
| GloVe-200 | exact | 512 | 19.6 | 10.194 | 16.158 | 138.905 | 393.607 | 0.9913 | 139.2 | 1669.1 |
| GIST-960 | index | 64 | 13.0 | 15.430 | 5.232 | 249.863 | 397.782 | 0.9802 | 125.3 | 3972.6 |
| GIST-960 | exact | 64 | 2.5 | 81.189 | 395.061 | 414.029 | 948.812 | 0.9989 | 124.2 | 4021.2 |
| Deep-Image-96 | index | 32 | 17.6 | 11.342 | 56.187 | 70.712 | 80.917 | 0.9503 | 131.5 | 691.4 |
| Deep-Image-96 | exact | 32 | 4.5 | 44.061 | 210.249 | 232.446 | 321.126 | 0.9674 | 131.4 | 1443.2 |
| Deep-Image-96 | index | 64 | 9.0 | 22.171 | 114.532 | 137.043 | 151.093 | 0.9687 | 131.6 | 784.0 |
| Deep-Image-96 | exact | 64 | 3.6 | 54.989 | 267.159 | 292.363 | 476.412 | 0.9894 | 131.1 | 1498.4 |

### 一次完整native batch

| 数据集 | rerank | nprobe | batch_nq | batch QPS | 实际总s | P50 ms/query | P90 ms/query | P99 ms/query | recall@100 | 客户端/进程RSS MiB | 服务端RSS MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 10000 | 3617.2 | 2.765 | 0.551 | 0.615 | 0.644 | 0.9747 | 480.2 | 341.4 |
| SIFT-128 | exact | 64 | 10000 | 1336.3 | 7.483 | 1.268 | 1.324 | 1.355 | 0.9997 | 479.2 | 908.9 |
| GloVe-200 | index | 512 | 10000 | 1173.2 | 8.524 | 3.026 | 3.209 | 3.405 | 0.9718 | 483.2 | 905.9 |
| GloVe-200 | exact | 512 | 10000 | 678.8 | 14.732 | 4.122 | 4.449 | 8.257 | 0.9918 | 482.5 | 1922.5 |
| GIST-960 | index | 64 | 1000 | 1178.8 | 0.848 | 5.168 | 5.300 | 5.320 | 0.9789 | 160.7 | 3955.3 |
| GIST-960 | exact | 64 | 1000 | 35.3 | 28.366 | 67.398 | 68.347 | 69.179 | 0.9980 | 159.9 | 4209.7 |
| Deep-Image-96 | index | 32 | 10000 | 107.2 | 93.327 | 20.929 | 21.704 | 22.384 | 0.9533 | 479.6 | 1281.4 |
| Deep-Image-96 | exact | 32 | 10000 | 25.2 | 396.131 | 88.467 | 90.055 | 98.236 | 0.9707 | 478.5 | 2252.8 |
| Deep-Image-96 | index | 64 | 10000 | 57.3 | 174.453 | 33.076 | 34.991 | 35.387 | 0.9691 | 479.6 | 1439.0 |
| Deep-Image-96 | exact | 64 | 10000 | 21.1 | 474.825 | 101.927 | 107.514 | 122.544 | 0.9900 | 479.2 | 2496.3 |

这里的batch P50/P90/P99来自单独的100-query chunk采样，不是full batch中的单query实际延迟；full batch耗时与QPS仅对应一次完整调用。

## 5. 解释与可复现性

- 单条SQL仍按一个query执行；native batch一次传入整张query表，内部按有界window/CPU tile并行，共享快照、cluster和FastScan block。不能将SQL loop的QPS当作native batch。
- index和exact的差距同时受候选计算、原始向量块读取、L1/L2命中和结果传输影响；仅凭QPS不能把慢全部归因于MinIO。
- 新SQL入口目前要求Int64主键id、完整索引覆盖、无CDC/mutation chain；不能证明可见性时明确拒绝。不同range partition的同id是不同的行，不按全表ID误去重。
- 每组保留实际query数、单查逐条延迟、batch wall时间、分离的百分位采样时间、recall、RSS、数据query/truth指纹和binary SHA256。每个来源run均已存在成功cleanup记录，并已独立复核其专属资源释放情况。

## 6. 本轮原始记录

- [SIFT-128 raw](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/sift.v2.jsonl)
- [GloVe-200 raw](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/glove.v2.jsonl)
- [GIST-960 raw](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/gist.v2.jsonl)
- [Deep-Image-96 raw](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/deep.v2.jsonl)
- [协议、参数与二进制指纹](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/manifest.v2.json)
- [全量跨接口ID与排名核验：20组配对、164,000个query比较](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/api-parity-final.json)
- [最终清理复核：3个来源、6个专属前缀为空](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/cleanup-proof-final.json)
- [测量后原有湖仓兼容性：8项通过、release哈希不变](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/core-compatibility-results.json)
- [真实长请求读租约：150秒跨TTL续期证明](/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/live-read-lease-renewal-proof.json)

本地完整ranked结果与stderr/stdout保留在以下来源run目录；原始压缩结果的路径及SHA256见raw的result_artifact字段。写表/构建表展示每个数据集首个来源run的成本；Deep恢复run重新构建的成本也完整保留在raw中。

- `/tmp/lakesoul-native-batch-results/native-batch-v2-20261113T060505Z-321091b5f07d48abbed8e03552ef0cb4`
- `/tmp/lakesoul-native-batch-results/native-batch-v2-20261114T041849Z-6aad3284328a4464b32562cb6819c6eb`
- `/tmp/lakesoul-native-batch-results/native-batch-v2-20261114T051135Z-32b55b0d8ce64403b816cb2160eee722`
