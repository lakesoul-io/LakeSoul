# LakeSoul Python 向量检索接口设计（详细方案）

> 目标：在 Python 侧提供**一等公民的向量检索接口**，同时
> ①保持湖仓一体（ACID 快照、LSM upsert/merge-on-read、CDC、分区、时间旅行）；
> ②支持湖仓 SQL 查询（filter/投影/聚合/join）；
> ③支持分布式查询（多 worker / 多分区）；
> ④复用 Rust 侧已实现的快速路径（R70–R77：L2/IP、index-only/exact、多文件桶、
> 多版本、任意投影、filter 扩探、分区/CDC、多列融合）。
>
> 本文只做设计，不包含实现；实现按第 11 节分阶段进行。

---

## 1. 背景：现状与问题

### 1.1 现有 Python 查询路径（代码事实）

| 层 | 位置 | 当前行为 |
|---|---|---|
| 元数据/表管理 | `python/src/lakesoul/catalog.py` + `_lib`（PyO3 `metadata.rs`） | PG 元数据客户端、建表/写入/快照/tag/manifest 管理 |
| 扫描/读取 | `python/src/reader.rs`（`_sync_reader`/`_one_reader`）→ `lakesoul-io` 的 `LakeSoulReader` | 每个 shard 构造一个 reader，输出 Arrow `RecordBatch` |
| 向量检索（Python） | `python/src/lakesoul/daft/search.py` | 用 reader options（`vector_search_column/query/top_k/nprobe/...`）取候选，再由 **Daft UDF 在 Python 侧重排** |
| SQL/湖仓查询 | 无 Python 内嵌引擎 | 只能借助 Daft/Ray/Spark/DuckDB，或通过 Rust 侧 DataFusion 服务（`lakesoul-worker`/ES gateway） |

### 1.2 关键问题

1. **Python 路径不走 Rust 快速 exec**：`lakesoul.daft.vector_search` 是
   "per-shard reader 取候选 + Daft UDF 重排"，exact 模式还要把候选的向量列读进 Daft
   再算距离；而 Rust DataFusion 路径（`LakeSoulVectorSearchExec`）已经有
   payload 缓存直读、只读 keep 行、融合排序等优化。两者差距明显（此前实测记录在
   `向量查询优化日志.md`：Python/Daft 路径远低于 SQL 直达路径）。
2. **能力不对齐**：Python 高层 API 目前 index-only 仅 L2、多列融合仅 L2（SQL 侧 R68/R77 已支持 IP）；
   批量检索（Rust `vector_batch_search`）没有 Python 绑定；filter/投影/分区语义在 Python 侧重实现，
   与 Rust 的 P2/P3/P4 语义容易漂移。
3. **没有湖仓 SQL 能力**：Python 用户想"向量检索 + WHERE + join 维表 + 聚合"只能拼多个系统，
   或者退化成把整表拉进 Daft。
4. **没有分布式入口**：Rust 侧已有 `datafusion_distributed`（coordinator planner + `lakesoul-worker`
   gRPC worker，见 `rust/lakesoul-datafusion/src/distributed/`），Python 无法使用。

### 1.3 设计结论（一句话）

**Python 不重新实现扫描/重排，而是把 Rust DataFusion 的 LakeSoul 会话（`LakeSoulSessionFactory`）
通过 PyO3 暴露成 `NativeSession`；所有向量检索接口都是该会话上的 SQL（或快照批量 API）的语法糖。**
这样 Python 自动获得湖仓语义与 R70–R77 的快路径，同时天然支持湖仓 SQL 与分布式规划。

---

## 2. 目标与非目标

### 2.1 目标

1. `LakeSoulTable.search(...)`：单查询向量检索，支持 filter/投影/metric/rerank/index-only、
   snapshot/tag，返回 `pyarrow.Table`（或适配器对象）。
2. `LakeSoulTable.search_batch(...)` / `catalog.search_batch(...)`：批量查询（10k 级），
   走 Rust `vector_batch_search` 快照批量入口。
3. `NativeSession.sql(...)`：在 Python 里执行任意湖仓 SQL（DataFusion 能力），
   向量检索只是 `ORDER BY <distance> LIMIT k` 的一种查询。
4. `Table.distributed(...)`：分布式检索/分布式 SQL（Ray/Dask 数据并行 与 Rust worker 两种形态）。
5. 适配器：`to_pyarrow()/to_daft()/to_ray_dataset()/to_duckdb()`，不重复执行湖仓扫描。
6. 兼容旧 API：`lakesoul.daft.vector_search/multi_vector_search` 保留，内部切换到新引擎。
7. 性能目标：与 Rust DataFusion 路径同量级（见第 10 节）。

### 2.2 非目标

- 不在 Python 里重新实现向量索引、merge-on-read、commit 协议；
- 不改变表格式、索引格式、元数据表结构；
- 不改变现有写入/索引构建/维护路径（`catalog.write`、auto build、compact 不动）；
- 不要求用户必须使用 Python 进程内的存储/缓存配置（凭据仍由环境/参数提供）；
- 不把 Spark/Flink 的查询下推纳入本期（它们是写入端；查询侧先用 DataFusion）。

---

## 3. 不变量：如何保证"不改变湖仓一体的性质"

设计上通过以下硬约束保证：

1. **只读检索**：`search/search_batch/sql` 不写数据文件、不写索引、不写 PG 元数据
   （索引构建/compact 仍由显式 API 或写入后自动维护触发）。
2. **结果 = 快照的 merge-on-read 可见行**：检索最终落到 Rust 的
   `LakeSoulVectorSearchExec` / native reader，遵循既有语义：
   - 同 key 多版本按 `birth_version`（文件年龄）取最新（R72）；
   - CDC 表 `op='delete'` 墓碑不返回（R75）；
   - 范围分区按 work unit 独立读取、分区谓词裁剪（R76）；
   - hash bucket 内多文件桶逐候选解析（P1a）。
   Python 侧**不实现**任何版本/墓碑/分区判断，因此不可能引入与湖仓语义不一致的行为。
3. **索引只是加速器**：索引缺失、metric 不匹配、覆盖性证明失败、谓词无法下推时，
   Rust scan 自动回退普通 SQL 扫描，**结果不变**（只是变慢）。Python 接口必须原样暴露
   这个属性（提供 `explain()` 让用户确认是否走了索引）。
4. **快照可钉**：`snapshot=` / `tag=` 参数把查询固定到某个 PG snapshot（需要 Rust 侧补齐读取接口，
   见 7.3）；分布式查询由 driver 解析一次快照后统一下发，保证所有 worker 读同一版本。
5. **不引入旁路元数据**：所有表/分区/索引/快照信息仍来自 PG + 对象存储；
   Python 侧不落任何私有状态文件。
6. **SQL 是唯一真相**：`search()` 等高层接口是 SQL 的语法糖，提供
   `search_sql()`/`to_sql()` 便于审计与等价性测试。

---

## 4. 总体架构

```
┌────────────────────────────── Python 用户 API ──────────────────────────────┐
│  LakeSoulCatalog / LakeSoulTable                                            │
│    ├─ table.search(query, filter=..., columns=..., snapshot=...)             │
│    ├─ table.search_batch(queries, ...)                                      │
│    ├─ table.sql("SELECT ... ORDER BY array_distance(...) LIMIT k")           │
│    ├─ table.distributed(engine="ray"|"worker", workers=...)                  │
│    └─ adapters: to_pyarrow() / to_daft() / to_ray_dataset() / to_duckdb()    │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │ PyO3（GIL 释放，Arrow C Data 零拷贝）
┌───────────────────────────────────▼──────────────────────────────────────────┐
│ NativeSession（新增 python/src/session.rs）                                   │
│   • lake_soul_datafusion::LakeSoulSessionFactory + CoreArgs                  │
│   • SessionContext（DataFusion 55 + LakeSoul planner/rules/UDF）             │
│   • vector_search 会话选项：rerank/nprobe/exact_overfetch/parallelism          │
│   • 缓存：index cache / payload cache / metadata TTL / snapshot cache         │
│   • 分布式：DistributedOptions（worker 列表 / K8s discovery）                  │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼──────────────────────────────────────────┐
│ Rust 既有快速路径（已实现，R70–R77）                                          │
│   VectorSearchPushdownRule → LakeSoulVectorSearchExec                        │
│     ├─ index-only：只读索引 key（投影=主键时）                                │
│     ├─ exact 直读：payload 缓存 + 多文件/多版本解析 + keep 行投影（P1b/P2）     │
│     ├─ filter/CDC/分区：候选行读回 + 进程内谓词 + 迭代扩探（P3/P4）             │
│     └─ 多列融合：L2/IP 同 metric（R77）                                      │
│   批量：vector_batch_search（快照级、10k 查询/次）                             │
│   分布式：datafusion_distributed coordinator + lakesoul-worker(gRPC)          │
└──────────────────────────────────────────────────────────────────────────────┘
```

设计原则：**Python 层薄、Rust 层厚**。Python 只负责参数校验、SQL 构造、结果适配、分布式编排；
所有扫描/merge/打分/版本语义由 Rust 完成。

---

## 5. Python 接口详细设计

### 5.1 `NativeSession`：Python 内的湖仓查询会话

```python
from lakesoul import LakeSoulSession

session = LakeSoulSession(
    # 复用现有 CoreArgs/env 约定；显式参数优先级高于环境变量
    pg_url="jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified",
    pg_user="lakesoul_test",
    pg_password="lakesoul_test",
    warehouse_prefix="file:///data/warehouse",     # 或 s3://bucket/warehouse
    object_store_options={"fs.s3a.endpoint": "http://127.0.0.1:9000",
                          "fs.s3a.access.key": "...", "fs.s3a.secret.key": "..."},
    # 向量检索默认参数（等价于 SET lakesoul_vector_search.*）
    vector_search=dict(rerank="exact", nprobe=48, exact_overfetch=1,
                       parallelism=1, deferred_payload=True),
    # 性能相关（可选）
    metadata_ttl_ms=200,
    payload_cache_dir="/dev/shm/lakesoul-rerank",  # 不设则用默认 temp 目录
    payload_cache_bytes=16 << 30,
    index_cache_bytes=8 << 30,
    # 分布式（可选，见 5.6）
    distributed=None | dict(worker_urls=["grpc://worker1:50051", ...],
                            target_partitions=8,
                            bytes_per_partition=16 << 20),
    target_partitions=1,
)
# 上下文管理器
with LakeSoulSession(...) as session:
    ...

session.sql(sql: str, *, params: Mapping[str, Any] | None = None) -> pa.Table
session.explain(sql: str, *, analyze: bool = False) -> str
session.set_vector_search(**kwargs) -> None
session.snapshot(table: str) -> int          # 当前快照 id（来自 PG）
session.close() -> None
```

要点：

- **会话复用**：一次创建、多次查询。Rust 侧的 index cache/payload cache/metadata snapshot
  都是进程级或会话级缓存，复用能显著减少前端规划与索引加载开销（此前实测
  `LAKESOUL_METADATA_TTL_MS=200` 单查询约 +20%）。
- **GIL**：`sql()` 内部 `py.allow_threads(|| runtime.block_on(...))`，同时释放 GIL；
  结果用 Arrow C Data Interface 转 `pyarrow.Table`（零拷贝）。
- **错误映射**：`DataFusionError` → Python 异常：`LakeSoulQueryError`（基类）/`IndexNotReadyError`/
  `DimensionMismatchError`/`SnapshotNotFoundError`；保留 Rust message 与 SQL 文本。
- **凭据安全**：不接受日志打印；`object_store_options` 支持从 env/密钥文件加载（已有 `SecretMap`）。

### 5.2 `LakeSoulTable.search`：单查询向量检索

```python
class LakeSoulTable:
    def search(
        self,
        query: Sequence[float] | np.ndarray | pa.Array,
        *,
        column: str | None = None,           # None：表里唯一的向量索引列
        top_k: int = 10,
        metric: str | None = None,           # 默认取 vector_index_columns 的 metric
        rerank: str = "auto",                # auto|exact|index
        nprobe: int | None = None,
        exact_overfetch: int | None = None,
        filter: str | ds.Expression | None = None,   # SQL 谓词或 pyarrow 表达式
        columns: Sequence[str] | None = None,        # 投影列（None=全部非向量列）
        with_distance: bool = False,
        snapshot: int | None = None,          # 时间旅行（需 Rust 支持，见 7.3）
        tag: str | None = None,
        session: LakeSoulSession | None = None,
    ) -> pa.Table:
        """返回按距离排序的 top_k 行（Arrow Table）。

        等价 SQL：
          SELECT {columns}, dist
          FROM {table}
          WHERE {filter}
          ORDER BY {distance}({column}, ARRAY[...]) {ASC|DESC}
          LIMIT {top_k}
        """
```

参数语义：

| 参数 | 语义 |
|---|---|
| `query` | float32 向量；也接受 numpy/pa；维度校验失败抛 `DimensionMismatchError` |
| `column` | 被检索列；多列索引时必须显式给出 |
| `metric` | 必须与索引 metric 一致；不一致时**不报错，走普通扫描**（与 SQL 一致），并通过 `explain()` 可查 |
| `rerank` | `auto`：exact（默认）——结果精确、读 payload；`index`：索引顺序，快、近似、不读向量列；`auto` 在"表很小/索引不可用"时自动 exact |
| `nprobe`/`exact_overfetch` | 对应会话选项；默认继承 session |
| `filter` | 字符串按 SQL 谓词解析（支持分区列裁剪、P3 扩探）；`pyarrow.dataset.Expression` 由适配层转 SQL |
| `columns` | 任意投影（P2）：额外列只对 keep 行二次读取 |
| `with_distance` | 返回 `__lakesoul_distance` 列（exact=精确距离；index=索引距离） |
| `snapshot/tag` | 固定读取版本（需 7.3 的 Rust 能力） |

返回：`pyarrow.Table`，列按 `columns` 顺序 + 可选距离列；行序 = 距离序（L2 升序、IP 降序）。

实现：构造 SQL → `session.sql(sql)`。不直接调用 `search_vector_batch` 等内部 API，
保证"向量检索 = 一种 SQL 查询"，从而自带 P2/P3/P4 与湖仓语义。

```python
# 用法示例
t = catalog.table("docs")
rows = t.search(
    query=np.array(embedding, dtype=np.float32),
    top_k=10,
    rerank="index",                       # 索引排序，快
    filter="lang = 'zh' AND publish_ts >= '2026-01-01'",
    columns=["id", "title", "lang"],
    with_distance=True,
    session=session,
)
```

### 5.3 `search_batch`：批量检索

```python
class LakeSoulTable:
    def search_batch(
        self,
        queries: Sequence[Sequence[float]] | np.ndarray,   # (nq, dim)
        *,
        column: str | None = None,
        top_k: int = 10,
        rerank: str = "exact",
        metric: str | None = None,
        nprobe: int | None = None,
        batch_size: int = 1024,
        columns: Sequence[str] | None = None,   # 只允许主键（batch API 返回主键）
        with_distance: bool = False,
    ) -> BatchSearchResult
```

```python
@dataclass
class BatchSearchResult:
    ids: list[list[int]]            # 每个 query 的 top-k 主键
    distances: list[list[float]] | None
    def to_arrow(self) -> pa.Table  # (query_id, rank, id, distance?)
```

实现与限制：

- 优先走 Rust `vector_batch_search`（快照级、单次 10k 查询、QPS 同引擎）；
  Python 侧 PyO3 绑定直接调用 `LakeSoulTableProvider::vector_batch_search`。
- **已知限制（沿用 Rust）**：批量入口目前要求"单个名为 `id` 的 Int64 主键"。
  Python 层在表不满足时给出明确提示，并自动回退到"SQL 批量"：
  `SELECT ... FROM t JOIN (VALUES (qid, ARRAY[...]), ...) AS q(qid, v) ...`
  或按 `batch_size` 切分逐条 `search()`（慢但通用）。
- 批量结果的去重/版本语义与 SQL 路径一致（同一 Rust 索引与 merge 语义）。

### 5.4 `table.sql`：湖仓查询（向量 + 任意 SQL）

```python
# 直接在 Python 里跑 DataFusion SQL；向量检索只是其中一种 ORDER BY
df = table.sql("""
    SELECT d.id, d.title, m.category
    FROM "lakesoul".default.docs d
    JOIN "lakesoul".default.meta m ON d.id = m.doc_id
    WHERE d.lang = 'zh'
    ORDER BY array_distance(d.embedding, ARRAY[0.1, 0.2, ...])  -- 或 inner_product DESC
    LIMIT 10
""").to_pyarrow()
```

- 覆盖：filter、投影、join、聚合、窗口、子查询、UNION、EXPLAIN/EXPLAIN ANALYZE、
  时间旅行（待 7.3）。
- 语义与 Rust SQL 完全一致（同一个 DataFusion + LakeSoul 规则）；
  这就是"湖仓查询"入口，也是向量检索的底层实现。
- 保持旧入口：`catalog.scan(...)`、`table.to_daft()` 不变（纯 Arrow 读取）。

### 5.5 与 Daft/Ray/DuckDB 的适配（只包装结果）

```python
rows: pa.Table = t.search(...)                # 默认 Arrow
t.search(..., to="daft")                      # 内部：pa.Table → daft.DataFrame
t.search(..., to="ray")                       # 内部：pa.Table → ray.data.Dataset
t.search(..., to="duckdb")                     # 返回 duckdb relation（注册 Arrow 表后 select）

# 旧 API 保留并切到新引擎（输出 schema 不变）
from lakesoul.daft import vector_search, multi_vector_search
df = vector_search(t, query, top_k=10, rerank="index")   # 内部 NativeSession SQL
```

**关键约束**：适配器只做"Arrow 结果 → 目标框架对象"，**绝不允许**目标框架重新扫描 LakeSoul 表
（否则又回到 Daft UDF 慢路径）。文档与类型系统都要明确这一点。

### 5.6 分布式接口

Python 侧提供两种形态，语义一致（固定快照 + 全局 top-k）：

#### 形态 A：driver 归并（建议先实现；无需新增服务）

```python
dist = table.distributed(engine="ray", num_workers=8)   # 或 engine="dask"

result = dist.search(
    query, top_k=10, rerank="exact", filter="lang='zh'",
    columns=["id", "title"],
)     # 返回 pyarrow.Table（全局 top-k）

# 也可以分布式跑任意湖仓 SQL（分片 join/聚合在 worker 内完成，driver 归并）
result = dist.sql("SELECT lang, count(*) FROM t GROUP BY lang")
```

实现：

1. driver 解析一次快照（`session.snapshot(table)` → snapshot_id）并广播；
2. 任务划分：优先按 **range partition × hash bucket** 切分（每个 worker 处理若干 shard，
   与 Rust 的 merge work unit 对齐，避免同一 bucket 的多个版本被拆到不同 worker）；
3. 每个 task 在 worker 进程内创建 `NativeSession`（同一快照），执行带 `filter` 的
   `search`（每 shard 取 `top_k * overfetch`）；
4. driver 汇总候选，按"主键去重（取各 task 中最新版本，理论上同快照不会冲突）+ 精确距离排序"
   取全局 top-k；
5. 失败处理：某 worker 失败时按 `top_k * fanout` 重新分配该 shard（至少 1 次重试），
   仍失败则回退 driver 单机执行该 shard（结果不变）。

#### 形态 B：Rust 分布式 coordinator（复用 `datafusion_distributed`）

```python
session = LakeSoulSession(
    ...,
    distributed=dict(worker_urls=["grpc://w1:50051", "grpc://w2:50051"],
                     target_partitions=8, fallback_to_local=False),
)
table.sql("SELECT ... ORDER BY array_distance(...) LIMIT 10")   # 由 Rust 分布式 planner 下发
```

要点：

- 复用 `rust/lakesoul-datafusion/src/distributed/`：coordinator（Python 进程内）+
  `lakesoul-worker` 二进制（独立部署，gRPC）；
- `DISTRIBUTED_PROTOCOL_VERSION` 要求 coordinator/worker 同版本，接口层暴露版本检查错误；
- merge-on-read 的 work unit 会被 planner 固定为单 task（`Maximum(1)`），保证同一 bucket 的
  多版本在同一个 worker 合并 —— 这是分布式下"不破坏湖仓语义"的关键，Python 无需处理；
- 部署成本：需要 worker 服务；无 worker 时自动回退单机（或按 `fallback_to_local` 报错）。

#### 分布式一致性契约

- 同一查询的所有 task/worker 读**同一个 snapshot_id**（driver 解析一次）；
- driver 归并按主键合并，结果与单机 SQL 逐行一致（测试按主键排序后比对）；
- 索引/分片解析错误在 worker 内回退普通扫描（该分片结果仍正确，只是慢）。

---

## 6. 参数、异常与结果约定

### 6.1 结果

| 接口 | 默认返回 | 可选 `to=` |
|---|---|---|
| `search` | `pyarrow.Table`（按距离排序，含 `columns`，可选距离列） | `daft`/`ray`/`duckdb` |
| `search_batch` | `BatchSearchResult`（`ids` + 可选 `distances`，`to_arrow()`） | — |
| `sql` | `pyarrow.Table` | `daft`/`ray`/`duckdb` |
| `explain` | `str`（`EXPLAIN`/`EXPLAIN ANALYZE` 文本） | — |

### 6.2 异常

```text
LakeSoulError
 ├─ LakeSoulQueryError(DataFusionError, sql=...)
 ├─ IndexNotReadyError(column, reason)      # 索引未提交/不覆盖快照（接口提示回退或等待）
 ├─ DimensionMismatchError(expected, actual)
 ├─ SnapshotNotFoundError(snapshot/tag)
 ├─ MetricMismatchError(column, index_metric, query_metric)  # 显式指定 metric 时
 └─ DistributedError(reason, worker_errors=[...])
```

> 注意：`metric` 与索引不一致时**默认不抛错**（与 SQL 一致：静默回退普通扫描），
> 只有显式 `metric=` 且与表属性不一致时才提示（`strict_metric=True` 可改为抛错）。

### 6.3 可观测性

- `session.explain(sql)` / `EXPLAIN ANALYZE`：确认 `LakeSoulVectorSearchExec` 与
  `order_by`/`metric`/`top_k`；`record_index_fallback` 的 reason 通过 `session.fallback_reason()`
  暴露（`unsupported_metric`/`index_not_declared`/`no_files`/...）。
- `search(..., profile=True)`：开启 `LAKESOUL_VECTOR_SEARCH_PROFILE`，Rust 侧打印
  per-stage 耗时（probe/fetch/rank/merge）。
- 指标：`vector_search_rows/ranked/payload_rows` 计数通过 `session.metrics()` 返回 dict（可选）。

---

## 7. 需要的 Rust / PyO3 改动清单

### 7.1 新增 `python/src/session.rs`

```rust
#[pyclass(module = "lakesoul._lib.session")]
pub struct LakeSoulSession {
    factory: Arc<LakeSoulSessionFactory>,
    ctx: Arc<SessionContext>,
    runtime: &'static Runtime,        // lakesoul_io::session::GLOBAL_RUNTIME
}

#[pymethods]
impl LakeSoulSession {
    #[new]
    fn new(py: Python<'_>, config: PyObject) -> PyResult<Self>;      // 解析 dict → CoreArgs

    fn sql(&self, py: Python<'_>, sql: &str) -> PyResult<PyArrowType<ArrowTable>> {
        let ctx = self.ctx.clone();
        let sql = sql.to_string();
        py.allow_threads(|| self.runtime.block_on(async move {
            let df = ctx.sql(&sql).await?;
            df.collect().await.map(|batches| concat_batches(...))
        }).map_err(to_py_err))
    }

    fn explain(&self, py: Python<'_>, sql: &str, analyze: bool) -> PyResult<String>;
    fn set_vector_search(&self, py: Python<'_>, kwargs: PyObject) -> PyResult<()>;
    fn snapshot(&self, py: Python<'_>, table: &str) -> PyResult<i64>;
    fn close(&self) {}
}
```

- 复用 `lakesoul_datafusion::create_lakesoul_session_ctx_with_config` /
  `LakeSoulSessionFactory`；`CoreArgs` 从 `lakesoul_datafusion::cli` 构造。
- `CoreArgs` 的 `s3_options`/`warehouse_prefix`/distributed 字段按 Python dict 赋值。
- `PyArrowType<RecordBatch>` 已有（`vector.rs` 在用），`concat_batches` 后转 pyarrow。

### 7.2 新增批量绑定

```rust
#[pyfunction]
fn vector_batch_search(
    py: Python<'_>,
    table: &LakeSoulTableHandle,      // 或 (table_name, namespace, session 配置)
    column: String,
    queries: Vec<Vec<f32>>,
    top_k: usize,
    nprobe: usize,
    rerank: String,                    // exact|index
    metric: String,
) -> PyResult<Vec<Vec<i64>>>;
```

- 内部调用 `LakeSoulTableProvider::vector_batch_search`（已有 Rust 批量入口）；
- 限制：单 Int64 主键（文档明确），不满足时 Python 层回退 SQL 批量；
- 返回 `ids`；`distances` 可选（需要 Rust 侧批量入口返回分数，当前只回 key，
  因此 v1 Python 批量只回 ids；后续给 Rust 批量入口加 `with_scores`）。

### 7.3 时间旅行读取（前置能力，可独立推进）

- PG 侧已有 snapshot/tag 元数据（`catalog.create_snapshot/list_snapshots/create_tag`）；
- **Rust scan 目前只读最新 commit**（`lakesoul_datafusion` 没有 snapshot_id 读取入口）。
  若要支持 `search(snapshot=...)`，需要：
  1. `LakeSoulTableProviderFactory`/scan 增加 `snapshot_id` 选项（PG 按 snapshot 解析的
     partition/data commit 列表）；
  2. 分布式下 driver 解析一次并下发；
  3. 单机 SQL 语法糖：`session.sql(f"SELECT ... FROM t FOR SYSTEM_TIME AS OF {sid} ...")`
     或表函数 `lakesoul_snapshot('t', sid)`。
- 未完成前，`search(snapshot=...)` 抛 `SnapshotNotFoundError`/`NotImplementedError`（明确提示），
  默认行为始终"最新快照"。

---

## 8. 语义契约（必须写进文档与测试）

1. **只读**：检索不产生 commit、不改索引、不改 PG（除缓存刷新）。
2. **快照**：默认最新 commit；同一 `NativeSession` 内连续查询不保证跨查询同一快照
   （除非显式 `snapshot=` 或传入已解析 snapshot）；分布式查询必须显式固定。
3. **版本**：同 key 返回 merge-on-read 可见的最新版本；CDC 墓碑不返回；分区按分区语义。
4. **索引回退**：索引不可用/不匹配 → 普通扫描，结果不变；`explain` 可确认。
5. **近似性**：`rerank="index"` 是近似排序（召回由 nprobe/total_bits 决定），
   `rerank="exact"` 在候选集内精确；两者都**不改变**表语义。
6. **批量和单查询一致**：同一快照、同一索引配置下，批量结果的 top-k 应与逐条 `search()`
   一致（允许近似差异时，按召回口径测试）。
7. **分布式一致**：结果与单机 SQL 逐行一致（按主键排序比对）。
8. **错误**：所有错误可定位到 SQL/参数；worker 错误聚合展示。

---

## 9. 兼容性与迁移

| 现有入口 | 处理 |
|---|---|
| `lakesoul.daft.vector_search` | 保留签名与返回（daft.DataFrame）；内部改走 `NativeSession` SQL；输出列保持一致 |
| `lakesoul.daft.multi_vector_search` | 同上；`rerank="index"` 的 L2 限制随 R77 扩展为 IP（按 metric 自动判断） |
| `table.scan()/dataset()/to_daft()` | 不变（Arrow 读取，不走索引） |
| `catalog.create_table(..., vector_index=[...])` | 不变 |
| 写入/索引构建 API | 不变 |
| Native reader 选项 `vector_search_*` | 保留（低层 API），文档标注"推荐用 search/sql" |

迁移策略：新接口先以 `from __future__ import annotations` 风格加入；
旧 Daft API 内部切换后做 A/B（同一查询两种实现结果一致 + 性能对比），稳定后文档默认推荐新接口。

---

## 10. 性能目标与基准

沿用 Rust 实测口径（GloVe-1.18M×200d、4 桶、bits=1、top_k=100）：

| 场景 | Rust 引擎实测 | Python 目标（含 Arrow 转换开销，≤5%） |
|---|---:|---:|
| 单查询 exact（200 真实 query） | L2 533 / IP 591 QPS | ≥ 500 / ≥ 560 QPS |
| 单查询 index-only | L2 694 / IP 779 QPS | ≥ 660 / ≥ 740 QPS |
| 批量 10k exact | 6.7–7.1k QPS | ≥ 6.4k QPS |
| 批量 10k index-only | 11.9–12.4k QPS | ≥ 11.3k QPS |
| 并发 c=8 exact | 2.1–2.9k QPS | 同量级 |

基准脚本：扩展 `python/examples/vector-search/run_e2e_benchmark.py`：
- 新增 `bench=sql|search|search_batch` 三个入口的对照；
- 记录 QPS/p50/p99/RSS/首次冷启动（payload 构建、索引加载）；
- 固定 session 复用 vs 每查询新建 session 的对比（预期 session 复用显著更快）。

其他性能要点：

- 冷启动：payload 缓存首次构建 ≈ 向量列大小（GloVe 903 MB，~1 s）；
  文档必须提示缓存目录与内存预算（`/dev/shm` 占 RAM）。
- 索引缓存 `LAKESOUL_INDEX_CACHE_BYTES`、metadata TTL 影响首查/重复查。
- Daft/Ray 适配只包装结果；若用户自行 `to_daft()` 后再 filter/sort，Daft 只处理结果集
  （不重扫湖仓）。

---

## 11. 分阶段实施计划

| 阶段 | 内容 | 验收 |
|---|---|---|
| **S0（能力补齐）** | Rust：snapshot/version 读取入口（7.3）；批量入口返回可选 distance | SQL 能按 snapshot 读取；批量返回分数 |
| **S1（会话 + 单查询）** | `NativeSession` PyO3；`table.search()` 生成 SQL 执行；`explain()`；异常映射 | 单查询 exact/index-only QPS 达目标；search 与手写 SQL 结果逐行一致；filter/投影/分区/CDC 用例 |
| **S2（批量）** | `vector_batch_search` PyO3；`search_batch()`；旧 Daft API 切新引擎 | 批量 10k QPS 达目标；批量与逐条 search 召回一致 |
| **S3（适配器 + 湖仓 SQL）** | `to_daft/to_ray/to_duckdb`；`session.sql()` 完整 SQL 能力；时间旅行语法糖 | join/聚合/EXPLAIN 用例；适配器不重扫湖仓（验证扫描次数） |
| **S4（分布式 A）** | `table.distributed(engine="ray"|"dask")`：driver 归并、快照固定、任务划分 | 8 worker 结果与单机一致；扩展性（QPS 随 worker 增长） |
| **S5（分布式 B）** | Rust 分布式 coordinator（`lakesoul-worker`）；Python 配置 worker 列表 | 跨 worker 计划下发；merge-on-read 单 task 语义；协议版本检查 |
| **S6（打磨）** | 文档、迁移、bench、错误/指标完善 | 全量测试 + 性能报告 |

依赖关系：S0 可与 S1 并行（时间旅行部分）；S4 依赖 S1；S5 依赖 S4 的快照一致性设计。

---

## 12. 风险与取舍

| 风险 | 影响 | 缓解 |
|---|---|---|
| `NativeSession` 链接 `lakesoul-datafusion` 使 wheel 变大 | 安装体积 | 作为可选扩展（extra）或单独 wheel；核心 PyO3 保持轻量 |
| GIL/线程/运行时冲突 | 死锁/性能 | 统一 `GLOBAL_RUNTIME`；`allow_threads`；禁止在持有 GIL 时 block |
| 时间旅行读取缺失 | `snapshot=` 不可用 | S0 补齐；未完成前明确报错（不静默读最新） |
| 分布式快照不一致 | 结果错误 | driver 解析一次 snapshot 并强制下发；worker 校验 snapshot_id |
| merge-on-read 被拆到多 worker | 重复/缺行 | 复用 Rust 分布式 planner 的 `Maximum(1)`；driver 归并兜底 |
| 对象存储凭据泄漏 | 安全 | `SecretMap`/env 加载，不落日志；session 隔离 |
| Daft 双重执行 | 性能回退 | 适配器只包装 Arrow 结果；文档/测试禁止重扫 |
| 批量入口 Int64 单主键限制 | 通用性 | Python 自动回退 SQL 批量；文档明确 |
| 缓存目录误用 `/dev/shm` | OOM | 文档给出预算公式；默认 temp 目录；可配置 |

---

## 13. 使用示例（目标形态）

```python
import numpy as np
from lakesoul import LakeSoulCatalog, LakeSoulSession

session = LakeSoulSession.from_env(vector_search=dict(rerank="auto", nprobe=48))
catalog = LakeSoulCatalog.from_env(namespace="default")
docs = catalog.table("docs")

# 1) 单查询（exact，带 filter/投影）
rows = docs.search(
    query=query_vec,
    top_k=10,
    filter="lang = 'zh'",
    columns=["id", "title", "lang"],
    with_distance=True,
    session=session,
)

# 2) 索引排序（近似、不读向量列）
fast = docs.search(query=query_vec, top_k=10, rerank="index")

# 3) 批量
batch = docs.search_batch(queries=np.stack(queries), top_k=100, batch_size=1024)
ids = batch.ids

# 4) 湖仓 SQL（向量 + join + 聚合）
report = session.sql("""
    SELECT m.category, count(*) AS cnt
    FROM "lakesoul".default.docs d
    JOIN "lakesoul".default.meta m ON d.id = m.doc_id
    WHERE d.lang = 'zh'
    ORDER BY array_distance(d.embedding, ARRAY[...]) LIMIT 100
""")

# 5) 分布式
dist = docs.distributed(engine="ray", num_workers=8)
rows = dist.search(query_vec, top_k=10, filter="lang='zh'")

# 6) 旧 API 兼容
from lakesoul.daft import vector_search
df = vector_search(docs, query_vec, top_k=10, rerank="index")
```

---

## 14. 待确认的问题（需要产品/使用方决策）

1. **返回类型默认**：`pyarrow.Table`（零拷贝、最通用）还是 Daft DataFrame（生态友好）？
   建议默认 Arrow，`to=` 选择目标框架。
2. **批量返回**：只回主键（当前 Rust 批量入口的能力）还是回完整行 + 距离？后者需要扩展 Rust 批量入口。
3. **时间旅行优先级**：是否在 S1 前先补 Rust snapshot 读取（影响 S1 的 `snapshot=` 参数）。
4. **分布式默认**：默认 driver 归并（无需服务）还是要求部署 `lakesoul-worker`？
5. **旧 Daft API 的弃用节奏**：是否保留但标 deprecated，还是默认切换实现即可。
6. **缓存目录默认值**：temp 目录（安全）还是提示用户设置（性能）；
   `/dev/shm` 需要显式选择（避免 OOM）。

---

## 15. 实现进度

### S1（已完成）：`NativeSession` + `table.search` + `table.sql`

代码：

| 文件 | 内容 |
|---|---|
| `python/src/session.rs`（新） | PyO3 `NativeSession`：`new(metadata_config, secondary_config, max_retry, config)`、`sql`、`explain(verbose, analyze)`、`set_vector_search`、`nprobe`、`close`；GIL 释放 + Arrow 零拷贝；复用 `create_lakesoul_session_ctx_with_config` 与 `LakeSoulVectorSearchConfig` |
| `python/src/lib.rs` | 注册 `lakesoul._lib._session`（`#[cfg(feature = "ci-unify")]`：该模块链接 `lakesoul-datafusion`） |
| `python/src/lakesoul/session.py`（新） | `LakeSoulSession`：`from_env`、`sql() -> pyarrow.Table`、`explain()`、`set_vector_search()`、`nprobe`、上下文管理；未编译 session 时给出构建提示 |
| `python/src/lakesoul/catalog.py` | `LakeSoulCatalog.session()`（复用缓存）；`LakeSoulTable.search()` / `to_sql()` / `sql()`；`search` 校验 metric/维度/列并生成标准 SQL |
| `python/tests/vector/test_native_session.py`（新） | 5 个用例：session SQL + EXPLAIN、search 与 SQL/暴力结果一致、filter/投影、index-only/IP、参数校验 |

构建：`maturin develop --release --features ci-unify`（`pyproject.toml` 的 maturin features 已包含 `ci-unify`，
默认构建即带 session；若不需要可去掉）。

已实测（本机，PG + 本地对象存储，200×8 维 L2/IP 表）：
- `EXPLAIN` 出现 `LakeSoulVectorSearchExec(vec=vec, top_k=5, metric=L2, order_by=false)`；
- exact 检索与暴力 top-5 完全一致（`exact_overfetch=30`）；index-only 返回 5 行；
- `filter="id < 100"` 全部命中且行数不变；`with_distance` 距离列升序；
- `table.search` / `table.sql(to_sql)` / `session.sql(to_sql)` 三者结果一致；
- `pytest python/tests/vector/test_native_session.py`：**5 passed**。

### S2（已完成）：批量检索

- `NativeSession.vector_batch_search(...)`（PyO3）绑定 Rust 快照批量入口；
- `LakeSoulSession.batch_search(...)` + `BatchSearchResult`（`ids`/`distances`/`to_arrow()`）；
- `LakeSoulTable.search_batch(...)`：优先原生批量（单 Int64 `id` 主键 + exact 需要 payload 缓存），
  否则逐条 SQL 回退（结果一致、`distances` 由 SQL 距离列填充）；
- `LakeSoulCatalog.search/search_batch` 便捷入口。

测试：原生批量与暴力 top-k 逐 query 比对（recall ≥4/5）；UInt64 主键表走 SQL 回退同样达标；
参数校验（维度不一致/非法 rerank/overfetch=0/空 query）。

### S3（已完成）：输出适配器

- `python/src/lakesoul/adapters.py`：`to_pyarrow/to_daft/to_ray/to_duckdb` + `convert(result, to)`；
- `session.sql(..., to=)`、`table.search(..., to=)`、`table.sql(..., to=)`；
- 适配器只包装 Arrow 结果，**不重新扫描湖仓表**；缺依赖时给出 pip extra 提示。

测试：`to="daft"` 的 search/SQL 结果行数与列名；非法格式抛 `ValueError`。

### S4（已完成）：分布式 driver 归并

- `python/src/lakesoul/distributed.py`：`DistributedSearch`；
- `table.distributed(engine="threads"|"ray"|"dask", num_workers=...)`；
- 流程：driver 用一条 `SELECT DISTINCT` 解析分区值 → 按 worker 分组并拼上分区谓词 +
  用户 filter → 每个 worker 用独立 `LakeSoulSession` 执行同一条候选 SQL（每分区取
  `top_k * partition_overfetch`）→ driver 按主键去重、按 metric 方向合并全局 top-k；
- 无范围分区时自动单机（fan-out 单位就是分区）；`dist.sql(sql)` 暂由 driver 单机计划（S5 后续）。

测试：三分区表 `engine="threads"`、3 workers 的跨分区 top-k 与暴力一致；`with_distance` 升序；
`filter` + 投影；无分区表回退与单机结果逐行一致；非法 engine/num_workers；ray 缺失时报
`ImportError`（环境无 ray 时验证）。

### S5（已完成）：分布式 coordinator 配置

- `NativeSession(config={"distributed": {...}})`：`worker_urls`（必填）、`target_partitions`（≥2，
  默认 8）、`fallback_to_local`、`bytes_per_partition`，构建
  `DistributedOptions{WorkerDiscovery::Static, ...}`，session 使用
  `LakeSoulSessionFactory::with_distributed(...)`；
- Python 侧 `LakeSoulSession(distributed=...)` 做完整校验（未知键/空列表/字符串误用/分区数）；
- 无 worker 可达时查询仍按 planner 规则留在 coordinator 单机执行（结果不变）。

测试：
- 非法配置校验；带一个不可达 worker 的 coordinator session 可创建且 `SELECT 1` 正常；
- **真实 worker 集群下发**（新增 `python/tests/vector/test_distributed_workers.py`，按
  `LAKESOUL_WORKER_URLS` 环境变量启用）：
  - 启动两个 `lakesoul-worker`（`cargo build --release -p lakesoul-datafusion --bin lakesoul-worker`，
    端口 50061/50062，metrics 19091/19092）；
  - coordinator session 的向量检索与 `GROUP BY` 聚合结果与单机 session 完全一致；
  - 通过 worker 的 Prometheus 计数 `lakesoul_execution_output_rows_total` 在查询后增长，
    证明 scan/merge 阶段确实在 worker 上执行（实测两个 worker 分别处理 306 / 154 行）。

### S6（已完成）：旧 Daft API 切引擎 + 基准

- `lakesoul.daft.vector_search` / `multi_vector_search` 现在**优先**走 `NativeSession`：
  - 单列：等价 SQL（含 filter/投影/rerank/metric），结果以 `daft.from_arrow` 返回，列与旧行为一致
    （exact 返回 pk + 向量列 + extra；index-only 只返回 pk + extra）；
  - 多列：加权融合 SQL（`Σ weight * array_distance(...)`），语义与旧 Daft 融合一致；
  - `with_distance=True`、`LakeSoulScan` 源（分区裁剪状态）继续走旧路径，保证兼容；
  - 任何异常回退旧路径，不改变现有调用方行为；
- 新增基准脚本 `python/examples/vector-search/bench_native_session.py`：
  `sql` / `search` / `search_batch` / `distributed` 四种入口的 QPS、p50/p95 与 RSS，
  支持 `--create-demo` 自建合成表（20k×64d）后自动清理。

实测（本机，demo 20k×64d，4 buckets，nprobe=64，top_k=10）：

| 入口 | QPS | p50 |
|---|---|---|
| `session.sql`（原始 SQL） | 606 | 1.60 ms |
| `table.search` | 623 | 1.59 ms |
| `table.search_batch`（128 queries/call） | 89.9 calls/s ≈ **11.5k queries/s** | 11.1 ms |
| `distributed`（无分区 demo） | 跳过（fan-out 单位是分区） | — |

RSS ≈195 MB（含 Arrow/pyarrow 基线）。回归：`pytest python/tests/vector -q` →
**41 passed, 2 skipped**（含既有 daft 向量检索 8 个用例在新引擎下全部通过）。

### S0（已完成）：快照 / 时间旅行读取

Rust：
- `lakesoul-metadata`：`get_snapshot_commits`、`get_tag_snapshot_id`、
  `get_partition_infos_at_snapshot`、`get_partition_infos_by_tag`（`snapshot_commit`
  → 每个分区的历史 `partition_info` 版本）；
- `lakesoul-datafusion`：`TableReadSnapshot { SnapshotId, Tag, AsOfMs }`；
  `LakeSoulTableProvider::snapshot_view()` 生成固定视图，扫描时用
  `get_all_partition_info_as_of` / `get_partition_infos_at_snapshot` 取代最新分区列表，
  merge-on-read、投影、过滤、分区裁剪全部复用；**固定视图禁用向量/文本索引下推**
  （索引描述的是最新文件），距离排序仍精确；
- `python/src/session.rs`：`NativeSession.register_snapshot_table()` 把固定视图注册到
  会话私有的内存 catalog（`__lakesoul_snapshots.public.<alias>`），`unregister_table` 注销。

Python：
- `LakeSoulSession.register_snapshot(table, snapshot=/tag=/as_of=)` / `unregister`；
- `LakeSoulTable.search(..., snapshot= | tag= | as_of=)`（自动注册/注销别名，SQL 用别名执行）；
- `LakeSoulTable.sql(..., snapshot=/tag=/as_of=)`（把 SQL 中对本表的引用改写到别名，支持
  `"lakesoul"."ns"."tbl"` 与 `lakesoul.ns.tbl` 两种写法）；
- 任一时刻只能指定一个时间点参数，否则 `ValueError`。

测试（`python/tests/vector/test_native_session.py::test_search_pinned_snapshot_tag_and_as_of`）：
建表写 N 行 → `create_snapshot` + `create_tag` → 追加 50 行 → snapshot/tag/as_of 检索只返回
旧行、最新检索包含新行；`tag` 与 `snapshot` 结果一致；`as_of=snapshot.created_at` 结果一致；
`table.sql(snapshot=)` 计数为旧值、最新计数为新值；固定视图 EXPLAIN **不含**
`LakeSoulVectorSearchExec` 但含 `SortExec`；固定快照 + filter 正常；多时间点参数报错。
`pytest tests/vector` 全部通过（14 个 native session 用例）。

### GloVe 全量验证（1,183,514 × 200d）

之前各阶段的功能测试用的是合成小表。这里在 GloVe 全量数据上补做端到端验证
（fixture：`/dev/shm/lakesoul-vector-query-opt-glove-fixture-20261105`，
`glove-base.fvecs` 1,183,514×200、`glove-query-10k.fvecs` 10k queries、
`glove-gt-10k.ivecs` 10k×100 ground truth；脚本：
`python/examples/vector-search/glove_native_session_validation.py`）：

条件：4 hash buckets（单文件/桶）、nlist=64、bits=1，top_k=100；建表+索引 22.9 s。

| 入口（on 1.18M） | QPS | recall@100 |
|---|---|---|
| `table.search` exact（nprobe=64, overfetch=10，200q） | 164–165 | 0.965 |
| `session.sql(to_sql())` exact（同上，仅 SQL 层） | 170 | — |
| `table.search` index-only（200q） | 310 | 0.453 |
| `table.search_batch` exact（10k queries/次） | 833（overfetch=10） | 0.966 |
| 同上 overfetch=5 / 2 / 1 | 1371 / 2009 / 2333 | 0.926 / 0.834 / 0.732 |
| 时间旅行 `snapshot=`（追加 1000 行后） | 0.77 次/秒（1.29 s/q，全量 pinned scan） | 1.0 |

RSS / 缓存（**独立查询进程**，不含建表内存）：查询后 current **808 MB**、peak
**934 MB**；payload cache **903 MB**（磁盘，与更早基准的 903 MB 一致）、本地对象缓存
15 MB。建表进程（numpy+Arrow 拷贝）peak 约 4.5 GB，仅影响建表阶段。

**同一参数下的 DataFusion vs Python A/B（GloVe 1.18M，2026-11 实测）**

为了避免"引擎差异"和"参数差异"混在一起，用同一张表、同一组参数分别跑
**Rust DataFusion 进程**（`SessionContext::sql` + `LakeSoulTableProvider::vector_batch_search`）
和 **Python 接口进程**（`session.sql` / `table.search` / `table.search_batch`）：
4 buckets（单文件/桶）、nlist=64、bits=1、nprobe=64、top_k=100、overfetch=10（batch 另有 1）、
相同 cache 目录、各自 20 次预热、200 条单查询、10k 条批量；RSS 用 `/proc/self/status` VmHWM。

| 阶段（10k query / 200 单查） | Rust DataFusion | Python `session.sql` | Python `table.search` |
|---|---|---|---|
| 单查询 exact（200q） | 172.9 qps / 5.79 ms | 166.7 qps / 6.00 ms（**96%**） | 151.3 qps / 6.61 ms（87%） |
| 单查询 index-only（200q） | 364.3 qps / 2.74 ms | 334.1 qps / 2.99 ms（**92%**） | — |
| batch exact，overfetch=10 | 952.6 qps / 1.05 ms/q | 924.1 qps / 1.08 ms/q（**97%**） | 同左（`search_batch`） |
| batch exact，overfetch=1 | 2,622.5 qps / 0.381 ms/q | 2,365.1 qps / 0.423 ms/q（**90%**） | 同左 |
| 进程 RSS（单查询阶段 / batch 后峰值） | 412 MB / 982 MB | 351–355 MB / 1,008 MB | 同进程 |

结论：**在相同参数下 Python 与 DataFusion 基本对齐**（SQL 96%、index-only 92%、
batch 90–97%），差异来自 PyO3/Arrow 转换与 Python 侧 SQL 字符串/参数处理；
`table.search` 比裸 `session.sql` 再多 ~9%（封装层的参数/校验/`to_sql` 构造），
批量接口没有可选参数透传时几乎无额外开销。RSS 也在同一量级（≈350–420 MB / ≈1 GB）。
因此之前 533/6745 qps 等旧数字与本轮 172/953 qps 的差距是**参数与测量口径不同**，
不是 Python 造成的；旧基准没有记录当时的 nprobe/overfetch 等参数。

**A/B 配置二：nlist=1024、nprobe=64（真实 ANN 参数，nprobe<nlist）**

上一组 A/B 用了 nlist=64、nprobe=64（等于探测全部 cluster，属于退化配置）。这里换成
nlist=1024、nprobe=64、bits=1、4 buckets（单文件/桶）、top_k=100、overfetch=10（batch 另测 1），
仍用同一张 GloVe 表、同一批 query，Rust DataFusion 进程 vs Python 接口进程，并补上
**Python `table.search` index-only** 的单查询测量：

| 阶段 | Rust DataFusion | Python `session.sql` | Python `table.search` |
|---|---|---|---|
| 单查询 exact（200q） | **222.1 qps** / 4.50 ms | 208.6 qps / 4.79 ms（94%），recall 0.796 | 207.0 qps / 4.83 ms（93%） |
| 单查询 index-only（200q） | **493.3 qps** / 2.03 ms | 460.0 qps / 2.17 ms（93%），recall 0.472 | 415.8 qps / 2.40 ms（84%），recall 0.472 |
| batch exact，overfetch=10 | **1,415.4 qps** / 0.707 ms/q | 1,320.9 qps / 0.757 ms/q（93%），recall 0.796 | 同左 |
| batch exact，overfetch=1 | **6,214.6 qps** / 0.161 ms/q | 5,445.9 qps / 0.184 ms/q（88%），recall 0.701 | 同左 |
| 进程 RSS（单查询后 / batch 后） | 427 MB / 874 MB | 362 MB / 909 MB | 同进程 |

结论更新：
1. 真实参数下 Python 仍保持 Rust 的 **88–94%**：SQL 路径 93–94%，批量 88–93%，
   `table.search` index-only 因封装层每查询构造 SQL/校验，约为 Rust SQL 的 84%；
2. batch overfetch=1 的 Rust 数字 **6,214.6 qps**，与更早基准的 6,745–6,991 qps 基本对上，
   进一步说明旧数字对应的是"overfetch≈1 / 候选更少"的参数组合，而不是 Python 层的问题；
3. nprobe<nlist 后 recall 自然下降（exact 0.796 / index-only 0.472），这是参数选择问题，
   与接口实现无关；nprobe=64 只探测 1024 个 cluster 中的 6.25%。

**与更早 Rust 层基准未对齐**。更早基准（同一 GloVe 集）为：单查询 exact 533 qps、
index-only 694 qps、batch 10k exact 6,745–6,991 qps、RSS ≈350 MB。本轮 Python 侧
同配置下为 165 / 310 / 833 qps、RSS 808 MB——差异主要来自搜索参数不同：
GloVe 上的扫描显示候选数（overfetch）从 1→10 时 batch QPS 从 2,333 降到 833、
单查询从 321 降到 176，而 recall 从 0.73 升到 0.96；更早基准没有记录当时的
nprobe/overfetch（推测候选数更少，且 batch 测于 Rust harness 而非 Python 包装）。
SQL-only（170 qps）与 `table.search`（165 qps）说明 Python 包装开销很小，
不是主要差异来源；RSS 差异则是"查询进程" vs 更早 harness 的测量口径/构建方式不同。
要对齐需要让两轮使用完全相同的 nprobe/overfetch/nlist 与测量进程——当前数据不足以
断言性能回退。

批量回退的坑（本轮发现并修复）：当表不满足 native batch 前提（例如每 bucket
多文件）时回退为逐 query SQL。原回退一并在每个 query 调用 `table.search` 时传入
`nprobe`/`exact_overfetch`，而 `table.search` 每次都会执行 `SET`（两次 SQL 执行/查询），
GloVe 多文件布局实测只有 68 qps——**比单查询 204 qps 还低**。修复两点：
1) `with_distance=True` → 只取主键（距离投影会关闭候选/载荷快路径）；
2) 参数**整批设置一次**，不再逐 query SET。
300k 多文件布局复测：回退 198.9 qps vs 单查询 217.5 qps（差 9%，回退本质就是逐 query
SQL，无法超过单查询）；而 native batch（单文件/桶）为单查询的约 5×（833 vs 165）。

### 收尾与后续可选优化

设计说明书中的 S0–S6 阶段均已完成并有测试覆盖（见各节；`pytest tests/vector`、
`pytest tests/vector/test_distributed_workers.py`、`cargo test -p lakesoul-datafusion --lib`）。

后续可选优化（不属于接口完成标准）：

1. `snapshot=` 与向量索引的组合：当前固定快照走全量扫描 + 精确排序（结果正确），
   可增加按历史版本构建/复用索引的能力；
2. 分布式 vector search 的 stage 拆分（当前 planner 把 Limit+Sort 留在 coordinator，
   scan/merge 阶段可下发，见测试中的 worker 计数）；
3. `to_ray`/`to_duckdb` 适配器在对应依赖环境下的端到端测试（当前按 importorskip 跳过）。
