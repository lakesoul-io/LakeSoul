# exact 按需 decoded-block 缓存：S3 → SSD → Memory

> 状态：exact 的**整文件 payload 已彻底移除**。当前只有按需 decoded-block
> 缓存：候选行按 block 解码，块写入 LakeSoul 现有 `LAKESOUL_CACHE` 的
> `DiskCache`，不再生成整列 payload 文件，也不支持 `LAKESOUL_VECTOR_RERANK_CACHE*`。

## 1. 目标与约束

- exact（`rerank='exact'`）需要候选 key 对应的原始 f32/f64 向量；
- 数据文件是列式块（Vortex/Parquet），随机取候选行会解码整列，读放大严重；
- 不把 payload 上传到 S3，也不在本地物化整列；
- 只缓存查询真正读到的候选块，复用 LakeSoul 的 S3→SSD 缓存；
- 候选 key 唯一映射到一行；多版本/多行由 merge 路径处理，exact direct 返回
  `None` 回退 reader。

## 2. 结构

```text
exact 查询
  │
  ├─ ANN 候选 key（每 bucket top_k*overfetch）
  │
  ▼
VectorBlockSource（每 data file / 向量列一个）
  │
  ├─ L1：moka sync 加权 LRU（block bytes，进程内）
  │
  ├─ L2：DiskCache（复用 LAKESOUL_CACHE；S3→SSD）
  │       key = synthetic path(source_hash, field_hash) + block_id
  │
  └─ miss：按 block 行范围做行选择读取（Vortex take / Parquet row selection）
            → 解码成 raw little-endian f32/f64
            → 写入 L2 + L1
```

没有 `PayloadView`、没有 payload 文件、没有 payload header/描述符/整列构建；
`payload.rs` 只保留 block 解码/Arrow 组装/精确打分所需的小工具。

## 3. block 定义

- 行数：`LAKESOUL_VECTOR_BLOCK_ROWS`，默认 **256**；
- 200d f32 时 block 约 `256 × 200 × 4 = 200 KiB`；
- Key：`(source_hash, field_hash, block_id)`；
- block 内容：该 block 内所有向量行，row-major little-endian；
- 行地址：`block_id = row / block_rows`，行内偏移
  `(row % block_rows) * dim * elem_size`；
- 最后一块可以不足 `block_rows` 行；
- 只支持固定长度向量列（`FixedSizeList`）；变长列表回退 reader。

## 4. 读写路径

### 4.1 L1

- `moka::sync::Cache`，按 1 KiB 权重计；
- 容量 `LAKESOUL_VECTOR_BLOCK_CACHE_BYTES`，默认 256 MiB；0 表示只用 L2。

### 4.2 L2 / S3-SSD

- 直接复用 `crate::cache::DiskCache::put/get`（`LAKESOUL_CACHE` 的同一实例）；
- 与 PK key index、原始数据页共享 `LAKESOUL_CACHE_PATH` / `LAKESOUL_CACHE_SIZE`
  和容量淘汰；
- block 写入后由 `ReadThroughCache` 的 S3-SSD 数据页缓存继续为源读取服务；
- block 缓存进程内按 synthetic path 寻址，进程重启按需重建；
- **不写 S3**，也不写独立的 payload 目录。

### 4.3 miss 时只读一个 block

- 先查 L1；再查 `DiskCache`；都没有才回到源数据文件；
- 源读取只取该 block 的行范围：
  - Vortex：按行索引 `take`；
  - Parquet：`with_row_selection`，跳过不需要的页；
- 解码后立即写 L2/L1，后续候选命中不再解码；
- 不存在“首次查询构建整列”的步骤。

## 5. 生命周期

| 对象 | 生命周期 |
|---|---|
| L1 block | 进程内 moka LRU；进程退出释放 |
| L2 block | `LAKESOUL_CACHE` 的 DiskCache；容量 LRU 淘汰；重启按需重建 |
| S3 payload | **不存在** |
| 整文件 payload | **已移除** |
| block 目录 | 无独立目录；位于 `LAKESOUL_CACHE_PATH` 下 |

## 6. 配置

```bash
# 必需：按需 block cache 复用 LAKESOUL_CACHE 的 S3-SSD DiskCache
export LAKESOUL_CACHE=1
export LAKESOUL_CACHE_PATH=/data/lakesoul/cache
export LAKESOUL_CACHE_SIZE=8GiB

# block 配置
export LAKESOUL_VECTOR_BLOCK_ROWS=256
export LAKESOUL_VECTOR_BLOCK_CACHE_BYTES=268435456   # L1，默认 256 MiB
# export LAKESOUL_VECTOR_BLOCK_CACHE=0                # 关闭 block cache

# 可选：block source 缓存（避免每 query 重开 key index；默认开）
# export LAKESOUL_VECTOR_BLOCK_SOURCE_CACHE=0
```

- 没有 `LAKESOUL_CACHE` 时 exact direct 不启用，回退 reader（正确但慢）；
- 没有 `LAKESOUL_VECTOR_RERANK_CACHE*` 任何开关；
- `LAKESOUL_VECTOR_BLOCK_CACHE=0` 关闭 block 路径；
- L1 预算为 0 时仍可用 L2。

## 7. 与批查询的关系

- 单查询/热点查询：block cache 只读候选块，首查不需要整列物化；
- batch exact：一次提交很多 query，会同时触达大量候选块；warm 后 QPS 仍低于
  以前的整文件 payload 模式，但 batch 也不再支持 payload；
- **exact native batch 支持每桶多个数据文件**：每个文件一个 `VectorBlockSource`，
  候选 key 按 segment `birth_version` rank 解析到最新 `(file, row)`，再按文件分组读
  block；单文件 bucket 保留原 fast path；
- 仍要求覆盖性证明通过（索引覆盖全部文件、无 mutation chain、无 CDC），否则回退
  逐 query SQL exact；增量写后需要用 `rebuild_vector_index()` 形成覆盖全部文件的
  generation；
- rank 并列、key 在文件内重复、缺少 rank 或 source 打开失败时回退；不返回猜测结果；
- `deferred_payload` 与 batch 无关：native batch 只返回主键，没有额外投影列；
  该选项只影响单查询额外列的二次读取。

## 8. GloVe-200 / MinIO 实测

1,183,514 × 200d、4 bucket、nlist=1024、bits=1、top_k=100、nprobe=16、of=1；
200 query，MinIO 本地：

| 模式（已无 eager 选项） | 首查 | QPS | RSS | 本地 block |
|---|---:|---:|---:|---:|
| block 256，L2=1GiB | 791.9 ms | 70.0 | 873.4 MB | ~917.6 MB |
| block 256，L2=1GiB，重复热查询 | — | 83.7 / 107.3 / 107.9 | — | — |
| block 256，L2 仅 256MiB | 1592.9 ms | 9.4 | 973.1 MB | 258.9 MB（抖动） |

native batch exact（200 query 一次调用，warm 3 轮）：

| 模式 | 首 batch | warm QPS | RSS |
|---|---:|---:|---:|
| index-only | 1219 ms | 11,424.7 | 292.6 MB |
| block 256，L2=1GiB | 2131 ms | 386.6 | 858.7 MB |

结论：

- 不再有 903 MB 整列预写；首查只构建候选块；
- 随机 200 query 最终仍会触达大部分 block（~918 MB），block 缓存适合
  按访问逐步填充；
- 重复/热点 query 可到 84–108 QPS；
- batch 高吞吐不是 block 的强项，index-only 更好；
- L2 预算必须覆盖工作集，否则频繁淘汰（256 MiB → 9.4 QPS）。

## 9. 正确性

- 本地 6 行 × 8 维表、`LAKESOUL_CACHE=1`、`rerank='exact'`：
  - exact ids `[1,2,6]` 正确；
  - 缓存目录只有 PK index + 小 block 文件；
  - 无 payload 目录、无 payload 文件；
- 数据文件行序 = index 行号；多版本 key 返回 `None` 回退 merge reader；
- 固定长度向量列之外的类型回退 reader；
- `cargo test -p lakesoul-io --lib`：195 passed, 1 ignored。

## 10. 清理

```bash
rm -rf "$LAKESOUL_CACHE_PATH"
```

block 缓存、PK index、原始数据页都在 `LAKESOUL_CACHE_PATH` 下，统一删除即可；
不再有独立 payload 目录需要清理。
