# index-only 三级缓存：S3 → SSD → Memory

> 目标：让 **index-only**（`rerank='index'`）也具备完整的三级缓存：
> 已解析索引在内存（L1）、索引文件在本地 SSD（L2）、索引文件原始对象在 S3（L3）。
> S3→SSD 复用 LakeSoul 的 `DiskCache`/`PageCache` 设计模式；SSD→Memory 扩展现有的
> moka 加权索引缓存。payload 缓存与本文无关，只服务 `rerank='exact'`。

## 1. 背景与缺口

之前 index-only 只有两层：

| 层 | 之前 | 缺口 |
|---|---|---|
| Memory | `lakesoul-io/src/index/cache.rs` 的进程级索引缓存（moka 加权 LRU，默认 512 MiB） | 有，但缺少淘汰/容量指标；进程重启即清空 |
| SSD | 只有可选、通用的 `LAKESOUL_CACHE` read-through（默认关、启动清空、不分 index/payload） | 没有专门的持久索引 SSD 缓存 |
| S3 | 索引文件 source of truth | 正常 |

因此进程重启、新节点扩容、或内存缓存淘汰后，索引文件仍需从 S3 重新读取并解析。
本改造新增一个 **独立的 index SSD cache class**，并把它接入 shard 加载路径。

## 2. 设计

```
query node
┌──────────────────────────────────────────────────────────────┐
│ L1 Memory: index cache (moka, 已解析 IvfRabitqIndex)         │
│    key = (kind, index_prefix, commit_id)                     │
│    默认 512 MiB；commit 变化自动失效                         │
├──────────────────────────────────────────────────────────────┤
│ L2 SSD: IndexSsdCache (persistent, whole-file entries)       │
│    key = index 对象路径哈希（segment / mutation 文件）        │
│    预算来自 LAKESOUL_CACHE_SIZE 的 index 份额；moka 加权 LRU；跨重启保留 │
├──────────────────────────────────────────────────────────────┤
│ L3 S3: _vector_index/... 的 segment / mutation 文件          │
│    仅在 L2 miss 时读取；用 IndexCachedStore 包装             │
└──────────────────────────────────────────────────────────────┘
```

### 2.1 S3 → SSD：复用 LakeSoul 的缓存设计

- 新增 **`IndexSsdCache`**（`rust/lakesoul-io/src/index/ssd_cache.rs`）：
  - 复用 `DiskCache` 的模式：moka 加权 LRU、容量字节预算、淘汰监听器删除本地文件；
  - 但缓存的是 **整个索引文件**（`get`/`get_range` 都按整文件落盘），不是 16 KiB 页；
  - **持久化**：目录不在启动时清空；残留 `.tmp` 清理后，已有文件在查询命中时惰性注册；
  - 原子写入：下载到 `.tmp` → `sync_all` → `rename`；不会读到半成品；
  - 预算：`LAKESOUL_CACHE_SIZE` 的 index 份额；总容量为 0 时关闭；
  - 指标：`IndexSsdCache::stats() = (hits, misses, fetched_bytes)`，后续可接 Prometheus。
- 新增 **`IndexCachedStore`**（实现 `object_store::ObjectStore`）：
  - 只重写 `get_opts`（HEAD/条件 GET 直接委托，避免为 HEAD 下载整文件）；
  - `get`/`get_range` 从 L2 本地文件服务；miss 时调用内层 S3 下载并写入 L2；
  - 写/列举/删除全部委托内层 store，索引构建路径完全不受影响。
- `rust/lakesoul-io/src/vector/search.rs` 的 `load_shard` 中：

  ```rust
  let index_store = crate::index::ssd_cache::wrap(store);
  let istore = IndexStore::new(index_store, prefix);
  ```

  只有**索引加载**经过缓存；数据文件读取仍走原 session object store，互不污染。

### 2.2 SSD → Memory：借鉴 LakeSoul 的 moka 缓存设计

- 现有 `index/cache.rs` 已是 moka 加权 LRU；本次增强：
  - L1 预算来自 **`LAKESOUL_VECTOR_MEMORY_BYTES`**（内部自动分配）；
  - 增加 `lakesoul_index_cache_{hits,misses,evictions}_total` 指标；
  - 保留 commit 校验、singleflight `try_get_with`、类型擦除、超预算 entry 直接再次加载等行为。
- 消费路径：
  1. L1 命中 → 直接 ANN；
  2. L1 miss → `IndexSsdCache` 读本地索引文件 → 解析成 `IvfRabitqIndex` → 插入 L1；
  3. L1/L2 都 miss → S3 下载到 L2 → 解析 → 插入 L1。
- 索引文件不可变，commit 变化会生成新文件/新 key，因此不需要“更新缓存内容”，只需
  按 commit 失效 L1、由 LRU 淘汰 L2 旧文件。

## 3. 配置

### 3.1 公共变量（与 exact 共用同一套）

| 变量 | 所属层 | 默认 | 含义 |
|---|---|---|---|
| `LAKESOUL_VECTOR_CACHE` | 总开关 | 开 | `0` 关闭向量缓存；index-only 变慢但结果不变 |
| `LAKESOUL_VECTOR_MEMORY_BYTES` | L1 Memory | 512 MiB | 进程内总预算；cluster/directory/block 自动分配 |
| `LAKESOUL_CACHE_PATH` | L2 SSD | `lakesoul_cache_dir` | index segment 文件与 exact block 共用根目录 |
| `LAKESOUL_CACHE_SIZE` | L2 SSD | 1 GiB（配置 L2 时） | L2 总容量；`0` 关闭 L2 |
| `LAKESOUL_VECTOR_LAZY_INDEX` | index 读取 | auto | cluster 数 ≥64 时只加载 probe 的 cluster |

index-only 与 exact 不再各自配置容量。内部：

```text
LAKESOUL_VECTOR_MEMORY_BYTES
  ├─ directory/snapshot
  ├─ cluster cache（index-only 主用）
  └─ block cache（只有 exact 活跃时占用）

LAKESOUL_CACHE_SIZE
  ├─ index segment 文件
  └─ exact decoded block / PK key index
```

### 3.2 兼容/专家变量

以下旧变量仍可用，但不属于公共配置，后续会移除：

```text
LAKESOUL_INDEX_MEMORY_BYTES / LAKESOUL_INDEX_CACHE_BYTES
LAKESOUL_INDEX_SSD_BYTES / _DIR / _KIND
LAKESOUL_VECTOR_CLUSTER_CACHE_BYTES
LAKESOUL_VECTOR_SNAPSHOT_CACHE_BYTES
LAKESOUL_VECTOR_BLOCK_CACHE_BYTES / _ROWS / _BYTES
LAKESOUL_VECTOR_BLOCK_SOURCE_CACHE
```

### 3.3 示例

```bash
export LAKESOUL_VECTOR_CACHE=1
export LAKESOUL_VECTOR_MEMORY_BYTES=4GiB
export LAKESOUL_CACHE_PATH=/data/lakesoul/cache
export LAKESOUL_CACHE_SIZE=100GiB
export LAKESOUL_VECTOR_LAZY_INDEX=auto
```

index-only 可以只开 L1（`LAKESOUL_CACHE_SIZE=0`），但重启/淘汰后要从 S3 重新读
index 文件；配置 L2 后 index segment 会落本地 SSD。exact 需要 L2；没有 L2 时
exact 自动回退 reader，结果正确但慢。

## 4. 正确性

- 索引文件不可变；L2 文件只按对象路径哈希寻址，且是 S3 对象的完整副本；
- L1 key 含 `commit_id`，commit 变化不会命中旧索引；
- 本地文件下载使用临时文件 + 原子 rename；并发同 key 由 moka `try_get_with` 合并；
- L2 只影响速度；缺失/损坏时按现有逻辑回退 S3；再失败则按原有“无索引扫描”路径处理；
- 不改变 index-only 查询语义，结果与无缓存基线一致（e2e 中 top-1 一致、后续单测/回归见日志）。

## 5. MinIO 端到端验证

### 环境

- MinIO `RELEASE.2025-09-07T16-13-09Z`，本地 `127.0.0.1:9000`，bucket `lakesoul-test`；
- LakeSoul metadata：本地 PostgreSQL `lakesoul_test`；
- 表：`idx_cache_e2e`，**200,000 行 × 128 维 f32**，4 个 hash bucket，
  `nlist=256`、`total_bits=1`、L2、top_k=10、nprobe=16；
- 查询：200 条真实 query（base 前 200 行），Python `LakeSoulTable.search(rerank="index")`；
- 索引文件：1024 个 segment 文件，SSD 缓存占用约 **16 MiB**；
- 每次 QPS 测量使用全新 Python 进程（L1 为空），先测首查延迟，再 warmup 20 条、顺序测 200 条；
- RSS 为进程 `/proc/self/status` 的 `VmRSS`，HWM 为 `VmHWM`。

### 结果

| 场景 | 首查延迟 | QPS（200 query） | RSS | HWM |
|---|---:|---:|---:|---:|
| **基线：SSD 缓存关闭**（索引从 MinIO 读） | **103.8 ms** | **562.9** | 278.0 MB | 387.9 MB |
| SSD 首次填充（从 MinIO 下载 1024 文件） | 504.5 ms | 557.8 | 280.9 MB | 387.7 MB |
| **SSD 命中，且 S3 上的 `_vector_index/` 对象已删除** | **174.6 ms** | **533.4** | 278.6 MB | 387.6 MB |

第三行是“SSD 缓存确实在服务”的强证据：MinIO 仍运行、数据文件可 HEAD，但
**S3 上的索引对象已经不存在**，index-only 查询仍成功返回（top-1 一致）。如果 L2 未命中，
加载会因 S3 GET 404 失败。

说明：

- 本机 MinIO 与查询进程同机，S3 延迟远低于真实远程对象存储，所以基线的首查 104 ms 已经很快；
  在真实 S3 上，SSD 命中对首查/tail 的收益会更大。
- SSD 首查 174.6 ms 包含 1024 个本地文件打开 + 解析 + 首次 L1 插入；后续 warm QPS 与基线差异
  约 5%，主要是 Python/缓存包装的测量噪声，RSS 基本持平。
- 本测试的索引只有 16 MiB；对大表（GB 级索引），L2 的作用会更明显。

### 清理

测试后已执行：

- `LakeSoulCatalog.drop_table("idx_cache_e2e")`；PG `table_info=0`；
- 删除 MinIO `s3://lakesoul-test/idx_cache_e2e/` 全部对象；
- 删除本地 `/tmp/lakesoul-index-ssd` 缓存目录；
- 停止 MinIO、删除其测试数据目录（对象与索引均已清理）。

## 6. 已知限制 / 后续

1. **L2 由 `LAKESOUL_CACHE_SIZE` 控制**：未配置时只有 L1/S3；配置后 index 份额自动启用。
2. **启动惰性注册**：进程启动不扫描/打开所有缓存文件；首次访问才注册进 moka，
   因此重启后的首个查询仍有一次全量文件打开/解析成本（本测试 ~175 ms）。
3. **无内容校验**：依赖索引文件不可变 + 索引 segment 自带 CRC；若本地缓存文件被截断，
   解析失败后当前不会自动删除重试，后续可加“失败删除 + 单次重拉”。
4. **每文件一次 moka 查询/文件打开**：1024 个小 segment 时冷启动开销不可忽略；后续可：
   - 按 shard 合并索引文件减少文件数；
   - 启动时批量注册（用 manifest），或用 mmap 延迟打开；
   - 预热 API：建索引后把新 segment 文件预取到 L2。
5. **指标**：当前 `IndexSsdCache::stats()` 只在进程内计数；可接入 Prometheus
   （`lakesoul_index_ssd_*`、`lakesoul_index_cache_*`）。
6. **多进程共享目录**：文件不可变、原子 rename，安全；容量统计是每进程近似，不做跨进程硬配额。

## 7. 代码位置

| 模块 | 位置 |
|---|---|
| SSD 缓存 + cached object store | `rust/lakesoul-io/src/index/ssd_cache.rs` |
| L1 内存索引缓存增强 | `rust/lakesoul-io/src/index/cache.rs` |
| 接入 shard 加载 | `rust/lakesoul-io/src/vector/search.rs`（`load_shard`） |
| 模块导出 | `rust/lakesoul-io/src/index/mod.rs` |
| 单测 | `index::ssd_cache::tests`（字节/range、跨实例持久化、容量淘汰） |
