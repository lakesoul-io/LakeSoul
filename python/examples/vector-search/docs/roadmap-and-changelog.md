# Roadmap & Changelog

> Roadmap 是方向性说明，不是交付承诺；以仓库当前代码和 issue/PR 为准。

## 当前已实现

- IVF + RaBitQ 向量索引（L2 / InnerProduct）；
- 建表时通过 `vector_index_columns` 声明索引；
- 写入提交后自动增量构建索引；
- `build_vector_index()` 给历史数据补建；
- `rebuild_vector_index()` 从全部 active 文件重建；
- Upsert/Delete 的索引可见性（mutation mask + birth version）；
- `table.search` 的 exact / index-only 两种模式；
- DataFusion SQL 下推：`ORDER BY array_distance(...) LIMIT k`；
- 多列向量查询和加权融合；
- L1/L2 缓存、索引 SSD 缓存、lazy cluster；
- `drop_vector_index`：删除索引控制面、可选删除对象、移除声明、缓存失效；
- pgvector 兼容服务（只读）。

## Roadmap / 候选方向

| 方向 | 说明 | 状态 |
|---|---|---|
| SQL DDL 删除索引 | `ALTER TABLE ... DROP VECTOR INDEX`，需要 parser/planner 扩展 | 规划 |
| 完整的 drop tombstone/epoch | 跨节点阻止旧写者重建；grace 期物理清理 | 部分实现/规划 |
| 跨节点缓存失效 | DataFusion worker 的 snapshot/index 缓存失效广播 | 规划 |
| Warm cache hint / pinning | 类似托管服务的显式预热 API | 规划 |
| 文本 + 向量混合检索 | BM25 与 ANN 的统一融合查询 | 规划 |
| cosine 下推 | 归一化或在索引层支持 cosine | 规划 |
| recall / index stats API | 线上召回率与索引健康度 | 规划 |
| 更强的 filtered ANN | 属性索引与 cluster 层级协同 | 规划 |

## Changelog（按主题）

### 向量索引基础

- 独立 `lakesoul-vector` crate：IVF+RaBitQ、segment、manifest、FastScan；
- shard 构建器接入 LakeSoul 表，按 partition + hash bucket 构建；
- catalog 控制面：`index_shard` / `index_commit` / `index_lease` / GC。

### 增量与 Upsert

- 增量写入追加 delta segment；
- 全局 revision + `birth_version` + mutation sidecar；
- 查询加载时构建 32-lane invalid mask；
- 修复跨 cluster 更新时旧副本得分获胜的问题。

### 查询与下推

- DataFusion `VectorSearchPushdownRule`；
- `LakeSoulVectorSearchExec`；
- exact / index-only、deferred payload、uncovered files 精确扫描；
- 批量查询一次加载 shard。

### 缓存

- L1 已解析索引缓存、lazy cluster、snapshot、decoded block；
- L2 通用 DiskCache + 可选 IndexSsdCache；
- 统一的 `LAKESOUL_VECTOR_*` 配置。

### 删除索引

- Python `LakeSoulTable.drop_vector_index`；
- Rust `lakesoul_datafusion::vector_index::drop_vector_index`；
- 删除 `vector_index_columns` 声明、控制面、可选对象文件；
- 本进程 L1/lazy/snapshot 缓存失效；
- tombstone 表阻止 drop 期间并发 build 重建。

## 兼容性说明

- 向量索引是派生数据，删除/重建不影响数据文件和表快照；
- 旧二进制不认识新表属性时，可能仍按旧配置 auto-build；
- 跨版本混跑时建议先升级所有写者/查询节点，再做索引删除/重建；
- 详细 API 变更以 [drop-index.md](drop-index.md) 和代码为准。
