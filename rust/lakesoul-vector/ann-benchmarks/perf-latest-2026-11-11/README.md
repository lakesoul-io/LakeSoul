# 最新代码向量检索性能（2026-11-11）

## 1. 发现并修复的 QPS 回归

上一轮参数扫描中 SIFT 单查只有约 200 QPS，而历史记录是 400–500 QPS。

根因：`LAKESOUL_VECTOR_LAZY_INDEX=auto` 只看 cluster 数：

```text
cluster_count >= 64  -> lazy
```

SIFT/GloVe/GIST 的 nlist=1024 全部被强制走 lazy，即使索引很小、可以完整放进 L1。
lazy 路径每查询都要走 cluster 选择/加载，虽然 cluster cache 命中，仍明显慢于
eager 的整 shard index cache。

修复（`rust/lakesoul-io/src/vector/search.rs`）：

```text
Auto:
  cluster_count >= 64
  且 (index_bytes >= 1 GiB 或 vectors >= 2_000_000)
    -> lazy
  否则 -> eager
Never / Always 仍可显式强制
```

验证：

| SIFT, nprobe=16, cache 8 GiB | 修复前 auto | 强制 lazy=0 | 修复后 auto |
|---|---:|---:|---:|
| index 单查 QPS | 183 | 493 | **490** |
| index batch QPS | 4011 | 11138 | **10940** |
| exact 单查 QPS | 99 | 89 | **97** |
| exact batch QPS | 2586 | 3849 | **3937** |

小/中索引恢复 eager，Deep-Image 10M 仍走 lazy。

## 2. 最新代码 primary 配置结果

配置：

```text
nlist=1024, total_bits=1, nprobe=16
L1=8GiB, L2=16GiB
hash_bucket_num=4
top_k=100, exact_overfetch=1
NQ: SIFT/GloVe/GIST 50, Deep 10
```

| 数据集 | index 单查 QPS | index batch QPS | exact 单查 QPS | exact batch QPS | index recall | exact recall |
|---|---:|---:|---:|---:|---:|---:|
| SIFT | 490 | 10,940 | 97 | 3,937 | 0.569 | 0.835 |
| GloVe | 396 | 8,218 | 50 | 3,275 | 0.420 | 0.632 |
| GIST | 209 | 3,400 | 14.8 | 1,403 | 0.700 | 0.822 |
| Deep | 52.4 | 627 | 8.0 | 543 | 0.527 | 0.858 |

构建（最新流式构建）：

| 数据集 | build s | build RSS MB |
|---|---:|---:|
| SIFT | 6.2 | 333 |
| GloVe | 15.7 | 698 |
| GIST | 52.2 | 548 |
| Deep | 95.7 | 1,206 |

## 3. SIFT 多参数扫描（最新代码，小缓存）

小缓存：L1=512MiB，L2=512MiB。每格为 **QPS / recall**。

| nlist | bits | nprobe | index 单查 | index batch | exact 单查 | exact batch |
|---:|---:|---:|---|---|---|---|
| 256 | 1 | 8 | 382.6 / .538 | 5515 / .538 | 28.3 / .825 | 161.6 / .825 |
| 256 | 1 | 16 | 390.2 / .552 | 4807 / .552 | 29.0 / .876 | 151.1 / .876 |
| 256 | 1 | 32 | 381.3 / .542 | 4432 / .542 | 29.4 / .886 | 167.0 / .886 |
| 256 | 7 | 8 | 378.9 / .875 | 4449 / .875 | 29.0 / .883 | 146.2 / .883 |
| 256 | 7 | 16 | 349.7 / .952 | 3982 / .952 | 28.1 / .970 | 175.5 / .970 |
| 256 | 7 | 32 | 327.0 / .973 | 3370 / .973 | 24.6 / .997 | 151.5 / .997 |
| 1024 | 1 | 8 | 342.2 / .520 | 4707 / .520 | 29.6 / .682 | 148.8 / .682 |
| 1024 | 1 | 16 | 339.6 / .550 | 4558 / .550 | 29.9 / .804 | 181.8 / .804 |
| 1024 | 1 | 32 | 340.1 / .585 | 4418 / .585 | 30.1 / .893 | 157.1 / .893 |
| 1024 | 7 | 8 | 301.9 / .682 | 4123 / .682 | 29.0 / .686 | 146.5 / .686 |
| 1024 | 7 | 16 | 311.1 / .816 | 3861 / .816 | 29.3 / .821 | 149.6 / .821 |
| 1024 | 7 | 32 | 298.9 / .928 | 3778 / .928 | 31.0 / .940 | 195.7 / .940 |

观察：

- nlist=256 的 index 单查略快（327–390）于 nlist=1024（299–342）；
- nprobe 增大主要是 recall 提升，index QPS 变化不大；
- bits=7 的 recall 明显高（nlist256,nprobe32：0.973），QPS 低约 10–20%；
- 小缓存下 exact 单查只有约 25–31 QPS，batch 约 146–196 QPS；exact 对 cache 容量比 index 敏感得多。

## 4. 结论

1. QPS 回归不是流式构建本身，而是 **auto lazy 过度触发**；
2. 小/中索引恢复 eager 后，SIFT index 单查 490 QPS、batch 10.9k，符合历史 400–500 / 数 k–10k 的水平；
3. exact 依赖 block cache；小缓存（512MiB）下 exact QPS 明显低，8GiB 配置更合理；
4. Deep-Image 10M 仍走 lazy，单查 52 QPS、batch 627 QPS，符合大索引设计目标；
5. GloVe/GIST 也恢复 eager，分别约 396/209 QPS 单查。
