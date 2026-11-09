# exact 三级缓存：S3 原始数据 → SSD payload → Memory 描述符/热点行

> 目标：exact（`rerank='exact'`）不把 payload 上传到 S3，S3 只保留原始数据文件；
> 本地 SSD 保存 row-addressable payload；内存保存 payload 描述符、key index/payload
> source 和可选的热点候选行。查询期只按候选行读取，不做大块/整列读取。

## 1. 为什么 exact 需要单独的缓存

exact 的输入是 ANN 候选 key，必须回到数据文件读取这些 key 的**原始 f32 向量**：

- 无 payload 时，Vortex/Parquet 列式文件必须解码整列；GloVe 1.18M×200d、4 桶，
  每查询约 **944 MB** 解码，实测 only **1.65 QPS**（本节实测值）；
- payload 把同一向量列按行序写成独立文件，exact 用 `pread` 只读候选行
  （100 候选 × 800 B = 80 KB），QPS 提升到 300+；
- payload 是**派生数据**，可以由原始数据随时重建，因此不需要 S3 副本。

## 2. 三级结构

```
L3 S3：原始数据文件（Vortex/Parquet，含 f32 向量列）
   │ 只用于构建 payload / 读取 payload 不覆盖的列
   ▼
L2 SSD：本地 payload 文件（row-addressable，持久、LRU/GC）
   │ 以及现有 LAKESOUL_CACHE 中的 PK key index（exact direct 依赖）
   ▼
L1 Memory：
   ├─ PayloadView / VectorPayloadSource 缓存（描述符、row count、dim、offset、key index）
   ├─ row cache（可选，默认 0=关闭；按候选行缓存原始字节）
   └─ 索引/快照缓存（既有机制，不在本文重复）
```

| 层 | 介质 | 内容 | 是否进 S3 | 生命周期 |
|---|---|---|---|---|
| L1 | DRAM | 描述符、source、热点行 | 否 | 进程内 LRU |
| L2 | 本地 NVMe | payload 文件、PK key index | 否 | 持久 / 预算淘汰 |
| L3 | S3 | 原始数据文件、索引文件 | 是 | 表数据生命周期 |

**明确约束：不把 payload 上传 S3。** 多节点各自在本地惰性构建；这是无 S3 payload
副本方案的代价，也是本设计的边界。

## 3. 实现

### 3.1 payload 文件（既有格式，本次增加管理）

- 每个 `(数据文件, 向量列)` 一个本地 payload：header + 行序 f32 向量；
- 行 i 偏移 = `header_len + i * row_stride`，exact 直接 `pread` 候选行；
- header 保存 `source_hash`/`kind_hash`/`rows`/`total_len`，不匹配则丢弃重建；
- 本次新增 **L2 目录预算淘汰**：当新 payload 超出
  `LAKESOUL_VECTOR_RERANK_CACHE_BYTES` 时，按 mtime 删除最旧 payload 文件直到能放下；
  单个 payload 大于整个预算时才回退 reader。

### 3.2 L1：描述符/source 缓存

- `VIEWS`：`PayloadView`（文件句柄、header、rows、dims、offsets），容量 256；
- `PAYLOAD_SOURCES`：`VectorPayloadSource`（key index + payload view），容量 16；
- 两者都按数据文件路径/布局寻址；数据文件不可变，compaction 后新路径自然 miss；
- 本次保留既有实现并补充指标，不改变缓存语义。

### 3.3 L1 可选：热点行缓存

- env `LAKESOUL_PAYLOAD_ROW_CACHE_BYTES`（默认 0）；
- key=`(source_hash, kind_hash, column_pos, row_id)`，value=原始 row bytes；
- moka sync 加权 LRU，按字节计重；
- **随机 exact 查询下实测是负收益**（见 §5），因此默认关闭；只有重复查询/热点候选
  明显的负载才建议开启。

### 3.4 读放大控制

| 方案 | 每查询读取 | 结论 |
|---|---:|---|
| reader 无 payload 解码整列 | ~944 MB | 最差，已实测 1.65 QPS |
| payload 按行 pread | 100×800 B ≈ 80 KB | 采用 |
| 256 KiB 块缓存随机候选 | 最多 25 MB | 反而放大，禁用 |
| L1 row cache | 命中省一次 pread；未命中多一次 cache 查 | 可选，默认关 |

所以 exact 的 L1 不是“大块 payload 缓存”，而是**描述符/source + 可选行级缓存**；
L2 也不按大块读，只按候选行偏移读。

## 4. 配置

```bash
# L2 payload（本地，不上 S3）
LAKESOUL_VECTOR_RERANK_CACHE=1
LAKESOUL_VECTOR_RERANK_CACHE_DIR=/data/lakesoul/rerank-payload
LAKESOUL_VECTOR_RERANK_CACHE_BYTES=17179869184   # 16 GiB，超出按 mtime 淘汰

# exact direct 还依赖 PK key index 的本地 cache（现有机制）
LAKESOUL_CACHE=1
LAKESOUL_CACHE_PATH=/data/lakesoul/cache
LAKESOUL_CACHE_SIZE=8GiB

# L1 热点行缓存（默认关闭）
LAKESOUL_PAYLOAD_ROW_CACHE_BYTES=0
```

注意：`VectorPayloadSource::open` 需要 `LAKESOUL_CACHE` 提供的 PK key index
（`get_or_load_file`）；只开 payload 而不开 `LAKESOUL_CACHE` 时，exact fast path
会回退到 reader。这是既有实现依赖，本次未改变。

## 5. GloVe-200 + MinIO 端到端测试

### 环境

- MinIO `RELEASE.2025-09-07T16-13-09Z`，`127.0.0.1:9000`，bucket `lakesoul-test`；
- 表 `vec_glove_exact_e2e`：GloVe base **1,183,514 行 × 200 维 f32**，
  4 个 hash bucket，`nlist=1024`、`total_bits=1`、L2、top_k=100、nprobe=16、
  exact_overfetch=1；
- 200 条真实 GloVe query；每次测量独立 Python 进程（L1 为空）；
- payload 只写本地 `/tmp/lakesoul-glove-payload`，MinIO 上无 payload 对象；
- exact direct 使用 `LAKESOUL_CACHE` 的 PK key index。

### 结果

| 场景 | 首查 | QPS（200 query） | RSS | HWM |
|---|---:|---:|---:|---:|
| **基线：无 payload（reader 解码整列）** | 892.0 ms | **1.65** | 486.7 MB | 664.1 MB |
| **payload 首次构建（903 MB 本地）** | 1460 ms | — | — | — |
| **payload 命中 + row cache** | 543.2 ms | **279.9** | 442.3 MB | 442.3 MB |
| **payload 命中，row cache 关闭（推荐）** | 546.4 ms | **313.4** | 349.6 MB | 349.6 MB |

结论：

- 无 payload 的 GloVe exact 基本不可用（1.65 QPS）；
- 本地 payload 命中后 **313.4 QPS**，约 **190×** 基线，RSS 从 486.7 MB 降到 349.6 MB；
- 首查 538 ms 包含 exact probe/coverage/打开 payload/PK key index；payload 已持久，
  不再从 S3 重建；真正冷构建一次约 1.46 s（903 MB 写本地）；
- **row cache 反而慢 10.7%、RSS 多 92.7 MB**：GloVe candidate 随机、200 条 query 内重复低，
  每次 miss 都要 cache lookup/插入，收益不足。因此默认关闭，保留为可选能力；
- MinIO 对象列表只有 4 个数据文件 + `_vector_index/`，**没有 payload 对象**，满足“不在 S3 存 payload”。

### 清理

测试后执行：

- `drop_table("vec_glove_exact_e2e")`；PG `table_info=0`；
- 删除 MinIO `s3://lakesoul-test/vec_glove_exact_e2e/` 全部对象；
- 删除本地 `/tmp/lakesoul-glove-payload`、`/tmp/lakesoul-glove-pk-cache`；
- 停止 MinIO，删除测试数据目录。

## 6. 测试

- `cargo test -p lakesoul-io --lib pk_locator::payload::tests`：2 项通过
  （payload round-trip、目录预算淘汰）；
- `cargo test -p lakesoul-io --lib index::ssd_cache::tests`：3 项通过（既有索引缓存回归）；
- GloVe-200 MinIO e2e：上述 QPS/RSS；结果 top-1 与基线一致。

## 7. 已知限制

1. **多节点不共享 payload**：无 S3 payload，各节点首次 exact 各自构建 903 MB；
   若节点多/弹性强，需要共享本地卷（RWX/NFS）或接受每节点冷构建。
2. **依赖 `LAKESOUL_CACHE`**：exact direct 的 PK key index 通过现有通用 cache 提供；
   只开 payload 不开它时会回退 reader。
3. **row cache 默认关闭**：随机查询负收益；热点/重复查询需按负载评估后开启。
4. **L2 淘汰按 mtime 近似 LRU**；payload 是派生数据，删除只导致重建，不影响正确性。
5. **payload 构建读整列**：首次每文件一次全列扫描（GloVe 903 MB）；这是无 S3 副本
   的固有成本，写入时/索引构建时预生成可作为后续优化，但需要本地磁盘在查询节点。

## 8. 代码位置

| 模块 | 位置 |
|---|---|
| payload 读取/构建/row cache/L2 淘汰 | `rust/lakesoul-io/src/pk_locator/payload.rs` |
| `VectorPayloadSource` / PK key index 缓存 | `rust/lakesoul-io/src/pk_locator.rs` |
| exact fast path | `rust/lakesoul-datafusion/src/datasource/file_format/vector_search_exec.rs` |
| 本文 | `rust/lakesoul-vector/exact三级缓存.md` |
