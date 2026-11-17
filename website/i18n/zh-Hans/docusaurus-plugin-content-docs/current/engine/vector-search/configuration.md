---
sidebar_position: 3
sidebar_label: 配置
---

# 向量检索配置

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

本文列出向量索引的**构建参数**（写入 `vector_index_columns` 表属性）与**运行时配置**（环境变量、会话设置、缓存层级）。快速上手见[快速开始](quickstart.md)，接口语义见 [API 参考](api.md)。

## 索引构建参数

`vector_index=` / `vector_index_columns` 的每个条目支持以下字段：

| 字段 | 默认值 | 说明 |
|---|---|---|
| `column` | 必填 | 向量列名 |
| `dim` | 必填 | 向量维度，必须与 `FixedSizeList` 长度一致 |
| `nlist` | `256` | IVF cluster 数；实际会被 clamp 到 `min(nlist, N)` |
| `total_bits` | `7` | RaBitQ 总位数，只支持 `1` / `3` / `7` |
| `metric` | `L2` | 距离度量：`L2` 或 `IP`（内积） |
| `rotator_type` | `FhtKac` | 旋转方式：`FhtKac` 或 `Matrix` |
| `seed` | `42` | 随机种子 |
| `use_faster_config` | `true` | 快速量化模式（精度损失约 &lt;1%） |
| `rebuild_mode` | `auto` | 重建策略：`auto` 在 shard 内任一 cluster 的 `delta/base` 超过 `max_delta_ratio` 时全量重建；`none` 只追加 delta segment |
| `max_delta_ratio` | `1.0` | cluster 级重建触发阈值（`delta_vectors / base_vectors`；无 base 但已有 delta 的 cluster 视为完全漂移） |
| `gc_enabled` | `true` | 写入后是否执行索引文件 GC |
| `gc_grace_seconds` | `3600` | generation 被取代后的回收宽限期 |
| `gc_keep_generations` | `1` | 保留的 generation 数 |

参数调优经验：

- `nlist`：每 bucket 约 `sqrt(rows)`；10 万行约 256–512，100 万行约 1024–4096。越大 cluster 越细、构建越慢，需要更大的 `nprobe` 才能覆盖。
- `total_bits`：`7`（默认）召回最好、索引最大；`3` / `1` 更省空间/带宽，召回下降。
- `metric`：必须与查询一致；cosine 需要写入时归一化后使用 IP。
- `hash_bucket_num`（建表选项）：大表 4–16 并行构建，小表 1–2；不是索引 JSON 的字段。
- `rebuild_mode=auto` 配合 `max_delta_ratio` 适合数据分布会漂移的表；`none` 适合只追加、分布稳定的表。

> Python SDK 的写入路径始终只做增量更新（即使 `rebuild_mode=auto`）；需要自动重建时使用 Rust/DataFusion 引擎写入，或显式调用 `rebuild_vector_index()`。

## 缓存层级

向量缓存是**进程内**的，分三层：

```text
L1 Memory（进程内）
  ├─ 已解析索引（eager index cache）
  ├─ lazy cluster（只加载 probe 的 cluster）
  ├─ snapshot（DataFusion 查询快照）
  └─ decoded block（exact 候选块）

L2 Local SSD
  ├─ 通用 DiskCache（数据页 / PK key index / decoded block）
  └─ IndexSsdCache（索引 segment / mutation 文件，可选）

L3 Object Storage（source of truth）
  └─ 数据文件 + _vector_index/...
```

L1 / L2 只影响速度，对象存储是唯一事实来源。缓存键包含实际 `ObjectStore` 的身份（display 形式 + 存储选项）：不同 bucket / endpoint 的同名路径不会复用 PK locator、decoded block、parsed vector shard 或 index SSD 内容，避免把另一个 store 的数据当成同一份索引。

### 生命周期

- **同一进程内**重建缓存实例（`IndexSsdCache` / decoded block / PK index）仍能命中已写入的 L2 文件：同一个 `ObjectStore` 分配期间缓存身份（含进程 nonce）保持稳定；
- **进程重启后两类 L2 都从零重建**（包括 `IndexSsdCache`）：旧目录内容只在首次访问时被惰性扫描、按 mtime 与当前预算采用/淘汰，随后按需重建；跨进程不盲目复用旧后端的同路径缓存；
- 实际收益：同进程内热查询命中 L1/L2；冷启动首查仍需从对象存储加载索引段或候选 block。

### 与通用 page cache 的关系

`_vector_index/` 下的索引段是不可变对象、每个冷 shard 只读一次，因此索引段读取**绕过通用 read-through page cache**（直接走 inner store），避免"一次 HEAD + 一次本地页写"的额外开销，也避免挤占共享的 L2 预算；需要本地持久缓存时显式开启 `LAKESOUL_INDEX_SSD_BYTES` 的 `IndexSsdCache`。

exact 路径的候选行读取同样不经过 read-through page cache：候选行通过 raw store alias 发出大量一次性小 range 读（避免每次读的页缓存记账与重复缓存），解码后的候选 block 进入 decoded-block 缓存——L1 是独立的 block cache（`LAKESOUL_VECTOR_BLOCK_CACHE_BYTES`，默认约占 L1 的 30%），L2 与数据页共享 `LAKESOUL_CACHE_SIZE` 的 DiskCache（使用 `vector-blocks/...` 独立 key 空间，设置 `LAKESOUL_CACHE` 后启用）。因此 exact 工作集大时要为 block 预留足够的 L2 容量，否则会与数据页互相挤出。

## 运行时配置

### 公共环境变量

必须在 `import lakesoul` **之前**设置（配置在进程启动时一次性解析，之后修改不生效）：

| 变量 | 默认 | 作用 |
|---|---|---|
| `LAKESOUL_VECTOR_CACHE` | 开 | 向量缓存总开关；`0` / `false` 关闭 L1/L2（index-only 变慢，exact 回退 reader） |
| `LAKESOUL_VECTOR_MEMORY_BYTES` | `512MiB` | L1 总预算，内部按 cluster / snapshot / block 分配 |
| `LAKESOUL_CACHE` | 未设置 | 本地盘缓存（read-through page cache、decoded block L2、PK locator 缓存）的启用开关；设置任意值即启用，容量由 `LAKESOUL_CACHE_SIZE` 决定。exact 路径的 decoded-block L2 与原生 batch exact 快速路径依赖它 |
| `LAKESOUL_CACHE_PATH` | `lakesoul_cache_dir` | L2 根目录：数据页、PK index、decoded block 共用 |
| `LAKESOUL_CACHE_SIZE` | `1GiB`（L2 启用时） | 通用 DiskCache 容量；`0` 关闭 L2 |
| `LAKESOUL_INDEX_SSD_BYTES` | `0`（关闭） | 索引文件专用缓存（`IndexSsdCache`）容量；显式设置后启用，预算独立于通用 L2 |
| `LAKESOUL_INDEX_SSD_DIR` | `<LAKESOUL_CACHE_PATH>/index-cache` | `IndexSsdCache` 目录 |
| `LAKESOUL_VECTOR_LAZY_INDEX` | `auto` | cluster-lazy 模式：`auto` 在 shard cluster 数 ≥64 时只加载 probe 的 cluster；`0` 强制 eager；`1` 总是 lazy |

L2 默认不启用：设置 `LAKESOUL_CACHE`（如 `LAKESOUL_CACHE=1`）打开本地盘缓存，容量由 `LAKESOUL_CACHE_SIZE` 控制（默认 1 GiB），目录由 `LAKESOUL_CACHE_PATH` 指定；三者配合使用。

```bash
export LAKESOUL_VECTOR_CACHE=1
export LAKESOUL_VECTOR_MEMORY_BYTES=8GiB
export LAKESOUL_CACHE=1
export LAKESOUL_CACHE_PATH=/data/lakesoul/cache
export LAKESOUL_CACHE_SIZE=100GiB
export LAKESOUL_VECTOR_LAZY_INDEX=auto

# 可选：索引文件独立持久缓存
export LAKESOUL_INDEX_SSD_BYTES=20GiB
export LAKESOUL_INDEX_SSD_DIR=/data/lakesoul/index-cache
```

### L1 内部预算

当使用统一的 `LAKESOUL_VECTOR_CACHE` / `LAKESOUL_VECTOR_MEMORY_BYTES` 配置面时，L1 预算大致按以下比例切分：

```text
LAKESOUL_VECTOR_MEMORY_BYTES
  ├─ cluster cache          ~60%
  ├─ decoded block cache    ~30%（exact 活跃时）
  ├─ snapshot               ~10%（clamp 到 64MiB–512MiB）
  └─ query / Arrow          剩余
```

旧部署（只设置旧变量、未启用统一配置面）保持历史默认：512 MiB 已解析索引、512 MiB snapshot、256 MiB block cache。

### 向量 block 调优（专家参数）

exact 路径按 block 解码候选行（不是整文件 payload）。以下变量在需要精细控制时使用：

| 变量 | 默认 | 说明 |
|---|---|---|
| `LAKESOUL_VECTOR_BLOCK_BYTES` | `256KiB` | 目标 block 字节数；按行宽换算 block 行数 |
| `LAKESOUL_VECTOR_BLOCK_ROWS` | 派生 | 直接指定每个 block 的行数（覆盖按字节换算的结果） |
| `LAKESOUL_VECTOR_BLOCK_CACHE_BYTES` | L1 预算的 ~30% | L1 decoded-block 缓存容量 |
| `LAKESOUL_VECTOR_BLOCK_SOURCE_CACHE` | 开 | 是否复用已打开的 `VectorBlockSource`；`0` 关闭 |
| `LAKESOUL_VECTOR_CLUSTER_CACHE_BYTES` | L1 预算的 ~60% | L1 cluster 缓存容量 |
| `LAKESOUL_VECTOR_SNAPSHOT_CACHE_BYTES` | L1 预算的 ~10% | L1 snapshot 缓存容量 |

### 元数据新鲜度

| 变量 | 默认 | 说明 |
|---|---|---|
| `LAKESOUL_METADATA_TTL_MS` | 未设置 / `0`（关闭） | 一条语句可以复用多少毫秒前取到的表信息/分区列表；设置后减少每查询的 PG 往返，代价是并发 DDL/commit 的跨节点可见性最多延迟该窗口 |

多节点部署时，drop / 重建 / 新 epoch 的可见性受该窗口和本地 snapshot cache 影响；需要强一致时不要开启或设置很小。

### 其它进程级变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `LAKESOUL_VECTOR_INDEX_LEASE_TTL_SECS` | `300` | 交互式单查 reader lease 的 TTL（秒）；原生 batch 的读租约固定为 120 秒 TTL、30 秒续租 |
| `LAKESOUL_VECTOR_EXACT_OVERFETCH` | 取会话设置（10） | 进程级覆盖 exact 模式每 bucket 的候选倍数 |
| `LAKESOUL_INDEX_GC_EVERY` | `16` | 每 N 次写入摊销执行一次索引文件 GC |
| `LAKESOUL_VECTOR_SEARCH_PROFILE` | 关闭 | 设置后打印每个 bucket 的耗时（排障） |
| `LAKESOUL_READER_PLAN` | 关闭 | 设置后打印 reader 物理计划（排障） |
| `LAKESOUL_PK_PROFILE` | 关闭 | 设置后打印 pk locator 耗时（排障） |

### 会话设置

DataFusion / pgwire 会话内可直接 `SET`（详见 [API 参考](api.md#25-会话设置)）：

```sql
SET lakesoul_vector_search.nprobe = 64;          -- 默认 64
SET lakesoul_vector_search.rerank = 'index';     -- 默认 index
SET lakesoul_vector_search.exact_overfetch = 10;
SET lakesoul_vector_search.parallelism = 1;
SET lakesoul_vector_search.deferred_payload = true;

SET ivfflat.probes = 64;                          -- pgwire，等价于 nprobe
```

| Key | 默认 | 说明 |
|---|---|---|
| `lakesoul_vector_search.nprobe` / `ivfflat.probes` | 64 | 每个 shard 探测的 cluster 数；越大召回越高、延迟越高 |
| `lakesoul_vector_search.rerank` | `index` | `index`：按索引量化距离排序、不读向量列；`exact`：候选读原始向量精确重排 |
| `lakesoul_vector_search.exact_overfetch` | 10 | exact 模式每 bucket 候选倍数 |
| `lakesoul_vector_search.parallelism` | 1 | 并行探测组数；高并发场景保持 1 |
| `lakesoul_vector_search.deferred_payload` | true | 两阶段读 payload |

pgwire 的 `ivfflat.probes` 是连接本地设置，默认 64，`0` 会被拒绝。

## 缓存清理与常见问题

```bash
rm -rf "$LAKESOUL_CACHE_PATH"      # 通用 DiskCache、decoded block、PK index
rm -rf "$LAKESOUL_INDEX_SSD_DIR"   # 索引文件专用缓存
```

没有独立的 payload 目录（exact 路径不再生成整文件 payload）。

| 现象 | 原因 | 处理 |
|---|---|---|
| 首次查询慢 | 索引 / block 未缓存 | 预热（先跑一次检索）、配置 `LAKESOUL_INDEX_SSD_BYTES` |
| 重启后 index-only 慢 | 索引不在 L2 | 开启 `LAKESOUL_INDEX_SSD_BYTES` |
| exact 反复慢 | L2 太小或未配置 | 增大 `LAKESOUL_CACHE_SIZE` |
| exact 回退普通 reader | 没有 L2 或缓存关闭 | 开启 `LAKESOUL_CACHE_SIZE` |
| 缓存配置不生效 | 在 `import lakesoul` 之后才设置 | 重启进程，在 import 前设置 |
| `IndexSsd` 不生效 | 只设置了 `LAKESOUL_CACHE_SIZE` | 显式设置 `LAKESOUL_INDEX_SSD_BYTES` |
| 召回低 | `nprobe` / `total_bits` 小 | 调大参数或重建索引 |
| 写入慢 | 每个小批都触发索引构建 | `auto_build_vector_index=False`，批量后统一补建 |
