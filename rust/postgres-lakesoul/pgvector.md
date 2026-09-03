## 结论

建议把目标拆成两层：

1. **先支持 pgvector 查询语法**：`<->`、`<#>`、`vector(n)`、`'[1,2,3]'`，没有可用索引时执行精确计算。
2. **再把 `ORDER BY distance LIMIT k` 优化为 LakeSoul IVF+RaBitQ 检索**。

不要直接在 `postgres-lakesoul` 做字符串替换，也不要把 pgvector 参数塞进共享 `SessionContext`。语法和计划优化放在 `lakesoul-datafusion`；`postgres-lakesoul` 只处理 PG wire、`CREATE EXTENSION`、`SET ivfflat.probes` 和 catalog 兼容。

---

# 一、当前代码的关键限制

## 1. PG Server 目前还没接上 LakeSoul

`rust/postgres-lakesoul/src/main.rs:75-78` 当前创建的是普通 DataFusion：

```rust
let conf = SessionConfig::new().with_information_schema(true);
let ctx = SessionContext::new_with_config(conf);
setup_pg_catalog(&ctx, "datafusion", am)?;
```

它没有调用：

```rust
create_lakesoul_session_ctx(...)
```

所以第一步不是向量语法，而是先让它使用：

```text
LakeSoulCatalog
LakeSoulTableProvider
LakeSoulQueryPlanner
LakeSoul RuntimeEnv/ObjectStore
```

`setup_pg_catalog` 的 catalog name 也应改成刚统一的小写：

```rust
setup_pg_catalog(&ctx, "lakesoul", am)?;
```

---

## 2. SQL parser 对 pgvector 的支持情况

我用仓库当前的 `sqlparser 0.61` 实际解析了以下 SQL：

| 语法 | 解析结果 | AST operator |
|---|---|---|
| `embedding <-> '[1,2,3]'` | 成功 | `LtDashGt` |
| `embedding <#> '[1,2,3]'` | 成功 | `Custom("<#>")` |
| `embedding <=> '[1,2,3]'` | 成功，但有冲突 | `Spaceship` |
| `embedding <+> '[1,2,3]'` | **解析失败** | 无 |
| `embedding vector(3)` | 成功 | `DataType::Custom("vector", ["3"])` |
| `CREATE EXTENSION vector` | 成功 | `CreateExtension` |
| `CREATE INDEX ... USING ivfflat` | 成功 | `CreateIndex` |

这里最大的坑是 `<=>`：

DataFusion 当前把 `BinaryOperator::Spaceship` 映射为：

```rust
Operator::IsNotDistinctFrom
```

即 null-safe equality，而 pgvector 把它定义为 cosine distance。因此必须在默认 DataFusion operator 转换前拦截，否则查询可以执行，但结果完全错误。

`<+>` 必须在 token/parser 层补支持，普通 `ExprPlanner` 来不及处理。

---

## 3. 现有向量搜索没有进入 DataFusion SQL 路径

现有路径是：

```text
LakeSoulReader::start()
  → inject_vector_search_filter()
  → search_matching_shards()
  → candidate IDs
  → PK filter
```

位置：

- `rust/lakesoul-io/src/reader.rs:148-340`
- `rust/lakesoul-io/src/vector/search.rs`

但 SQL 查询走的是：

```text
LakeSoulTableProvider::scan()
  → list_files_for_scan()
  → DataSourceExec
  → MergeParquetExec
```

位置：

- `rust/lakesoul-datafusion/src/datasource/table_provider.rs:701-918`

它不会创建 `LakeSoulReader`，所以即使 SQL 层设置了 `vector_search_*` option，也不会触发现有向量索引检索。

因此不能只做：

```sql
embedding <-> '[1,2,3]'
```

到 `vector_search_query` option 的简单映射。需要把索引检索接入 `LakeSoulTableProvider::scan` 或增加专用 logical/physical node。

---

## 4. 现有搜索 API 还不足以实现 SQL Top-K

`search_matching_shards()` 当前：

- 只使用第一个 index prefix；
- 只返回 `Vec<u64>`，丢弃 score；
- 只检索单 bucket，跨 bucket 合并由调用方负责；
- 传入的 `_metric` 被忽略，真正 metric 来自索引；
- index 不存在时返回空 IDs；
- 没有验证索引是否覆盖当前 LakeSoul snapshot。

直接拿它实现 SQL 会产生几个严重问题：

### 缺索引时错误返回空结果

当前逻辑：

```text
missing index
  → empty IDs
  → Boolean(false) filter
  → zero rows
```

对于 SQL，这是错误语义。缺索引或索引不完整时必须回退到精确扫描，而不是返回空集。

### L2 score 不是 pgvector 展示语义

RaBitQ 当前 L2 score 是平方距离：

```rust
l2_distance_sqr(...)
```

pgvector 的 `<->` 返回欧氏距离，即需要开平方。排序顺序相同，但不能直接把索引 score 作为 SQL 的 `distance` 输出。

### Inner Product 符号不同

当前索引 `Metric::InnerProduct` 返回正的 inner product；pgvector `<#>` 返回**负 inner product**，因为 PostgreSQL 索引扫描主要按 ASC 工作。

因此索引只应用来选候选集。最终 SQL 仍应在候选行上精确计算 pgvector distance、排序并执行 LIMIT。

---

# 二、推荐架构

## 层 1：DataFusion 表达式兼容

在 `rust/lakesoul-datafusion/src/vector/` 增加：

```text
vector/
├── expr_planner.rs
├── type_planner.rs
├── functions.rs
└── optimizer.rs
```

### `PgVectorExprPlanner`

实现 DataFusion 54 的：

```rust
ExprPlanner::plan_binary_op
```

映射：

| pgvector operator | DataFusion expression | 第一版 |
|---|---|---|
| `<->` | `pgvector_l2_distance(left, right)` | 精确 + ANN |
| `<#>` | `pgvector_negative_inner_product(left, right)` | 精确 + ANN |
| `<=>` | `pgvector_cosine_distance(left, right)` | 先精确 |
| `<+>` | `pgvector_l1_distance(left, right)` | parser 修复后精确 |
| `<~>` | Hamming | 暂不支持 |
| `<%>` | Jaccard | 暂不支持 |

使用独立的 `pgvector_*` UDF 名，而不是直接改写为 `array_distance`。这样后续 optimizer 能可靠识别“这是向量 Top-K 查询”，不会误优化用户普通数组计算。

可以在 UDF 内复用 DataFusion 已有的：

- `array_distance`
- `inner_product`
- `cosine_distance`

它们会把 `FixedSizeList` coercion 成普通 `List`。L1 需要补一个小型 Arrow kernel。

### 向量输入

支持两种输入：

```sql
embedding <-> '[1,2,3]'
embedding <-> ARRAY[1,2,3]
```

内部类型统一为：

```rust
FixedSizeList<Float32, dimension>
```

文本向量要求严格解析：

- 必须有 `[...]`；
- 维度必须与列一致；
- 元素必须是有限 `f32`；
- 拒绝空元素、NaN、Infinity 和溢出；
- literal 在 planning 阶段只解析一次，不能每行解析一次；
- prepared parameter 保持 scalar，不要扩成每行重复数组。

---

## 层 2：`vector(n)` 类型

增加 `PgVectorTypePlanner`：

```text
vector(768)
  → Arrow FixedSizeList<Float32, 768>
```

这和现有索引构建要求一致：

```rust
DataType::FixedSizeList(_, dim)
```

见 `rust/lakesoul-vector/src/reader.rs:88-120`。

建议规则：

- 表字段定义必须使用 `vector(n)`，`n > 0`；
- `vector` 无维度只允许临时表达式，不允许声明可索引 LakeSoul 字段；
- `vector(n)` 只表示存储类型，**不自动创建索引**；
- 索引仍由 `vector_index_columns` 或后续 `CREATE INDEX` 管理。

### TypePlanner 组合问题

`setup_pg_catalog()` 会安装自己的 `PgOidTypePlanner`。DataFusion 一个 SessionState 只能配置一个 `TypePlanner`，所以不能简单连续调用两个 `with_type_planner()`。

需要组合：

```rust
struct LakeSoulPgTypePlanner {
    vector: PgVectorTypePlanner,
    postgres: PgOidTypePlanner,
}
```

执行顺序：

```text
vector / pg_catalog.vector
  → PgVectorTypePlanner

其他类型
  → PgOidTypePlanner
```

然后在 `setup_pg_catalog()` 之后安装 composite planner，否则 PG catalog 初始化会覆盖你的 vector planner。

---

## 层 3：识别 Top-K 并使用索引

增加 logical optimizer rule，匹配这个计划形状：

```text
Limit(k)
  └─ Sort(
       pgvector_l2_distance(embedding, constant_query) ASC
     )
       └─ Filter(optional)
          └─ TableScan(LakeSoulTableProvider)
```

转换成携带 query-local 配置的扫描：

```rust
struct VectorSearchSpec {
    column: String,
    query: Arc<[f32]>,
    metric: Metric,
    limit: usize,
    candidate_k: usize,
    nprobe: usize,
}
```

不要写进：

- 全局环境变量；
- 共享 `SessionContext`;
- 持久的 `LakeSoulTableProvider.io_config`;
- table metadata。

同一个 PG server 会并发执行不同查询。修改共享 provider/session 会造成查询向量串线。

### 只在这些条件下优化

必须同时满足：

1. 单个 LakeSoul table；
2. `ORDER BY` 是支持的向量 distance；
3. ASC；
4. 有有限的 `LIMIT`；
5. 左侧是该表声明的向量索引列；
6. query vector 是 literal 或已绑定 prepared parameter；
7. operator metric 与 index manifest metric 一致；
8. index 覆盖当前查询 snapshot。

其他情况继续执行精确全表扫描。

例如以下查询不能直接 ANN pushdown：

```sql
ORDER BY embedding <-> query_table.embedding
```

右侧不是查询常量，需要 join/exact execution。

---

## 层 4：候选集扫描和精确 rerank

推荐执行流：

```text
识别 range partition filter
        │
        ▼
列出当前 LakeSoul snapshot 的文件
        │
        ▼
按 partition_desc + bucket_id 分组
        │
        ▼
并发搜索每个 shard index
        │
        ▼
合并 candidate PK，去重
        │
        ▼
PK IN (...) 注入 LakeSoul merge-on-read scan
        │
        ▼
读取当前版本的完整记录
        │
        ▼
精确 distance UDF
        │
        ▼
全局 Sort + Limit
```

原始 `Sort + Limit` 不要移除。它承担：

- 精确 rerank；
- 跨 shard 全局 Top-K；
- L2 开平方；
- `<#>` 符号转换；
- 对更新后记录使用当前向量，而不是索引里的旧向量。

候选过滤使用 `Expr::InList`，不要构造几千层：

```text
id = 1 OR id = 2 OR id = 3 ...
```

现有 `LakeSoulReader::inject_vector_search_filter()` 就在构建 OR 链；SQL 路径不应沿用这个实现。

---

# 三、索引 freshness 是上线前的 P0

现有索引路径只有：

```text
table/_vector_index/column/partition/bucket/LATEST
```

但 SQL 查询需要知道：

```text
这个 manifest 覆盖了哪些 LakeSoul data commit/file？
```

建议 manifest 增加：

```rust
struct LakeSoulVectorSnapshot {
    table_id: String,
    partition_desc: String,
    bucket_id: u32,
    vector_column: String,
    covered_file_ops: Vec<FileIdentity>,
    metric: Metric,
    dimension: usize,
}
```

查询时固定一次 LakeSoul snapshot，再比较 index coverage：

| 状态 | 行为 |
|---|---|
| 索引完整覆盖 snapshot | ANN |
| 索引缺失 | 精确扫描 |
| 索引落后，能识别新增文件 | 索引候选 + 新文件精确扫描 |
| manifest 损坏/metric 不匹配 | 明确报错或配置化 fallback |
| 索引引用已删除文件 | 视为 stale，不得返回空结果 |

否则 compaction、update 或自动索引构建延迟期间会静默漏行。

---

# 四、PG wire 层要做什么

放在 `rust/postgres-lakesoul`，不要放进 RaBitQ core。

## `CREATE EXTENSION vector`

第一版可以由 `PgVectorQueryHook` 拦截：

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

因为能力已经编译进 server，可以返回成功，但必须只接受 `vector`，其他 extension 继续报 unsupported。

完整兼容还需要向这些虚拟系统表暴露记录：

```text
pg_extension
pg_type
pg_proc
pg_operator
pg_opclass
```

否则很多 pgvector 客户端会执行：

```sql
SELECT typname, oid FROM pg_type WHERE typname = 'vector';
```

然后认为 pgvector 没安装。

## `SET ivfflat.probes`

可以映射为：

```text
ivfflat.probes → nprobe
```

但必须保存在 connection-local 状态，不能修改共享 SessionContext。

当前 `datafusion-postgres::SetShowHook` 只识别有限配置项。自定义 vector hook 要排在它前面：

```text
PgVectorHook
CursorStatementHook
SetShowHook
TransactionStatementHook
```

另外，当前 `serve()` 共享一个 `Arc<SessionContext>`。在实现 connection-local nprobe 前，应先完成 `plan.md` 中的 per-connection Session factory，或者第一版固定使用 server 级默认 nprobe。

## PG 自定义类型

当前 `arrow-pg` 会把：

```rust
FixedSizeList<Float32>
```

编码成：

```text
float4[]
```

而不是 pgvector 的 `vector` OID。

所以需要明确区分：

### 语法兼容 MVP

- 输入：文本 `'[1,2,3]'` 或 `float4[]`；
- 输出：`float4[]`；
- SQL operator 可用；
- 普通 psql/JDBC 可用；
- pgvector 专用 driver 不保证可直接注册。

### 完整 pgvector driver 兼容

还需要：

- 固定或动态分配 `vector` type OID；
- `pg_type` 暴露该 OID；
- pgwire text/binary vector decoder；
- Arrow `FixedSizeList<Float32>` 到 vector text/binary encoder；
- prepared statement 参数类型推断；
- 支持 `$1` 由 pgvector driver 以 custom OID 发送。

这是独立的一层工作，不能只靠 SQL parser 完成。

---

# 五、`CREATE INDEX USING ivfflat` 怎么处理

官方语法是：

```sql
CREATE INDEX ON items
USING ivfflat (embedding vector_l2_ops)
WITH (lists = 100);
```

当前 parser 能解析，但 DataFusion 转成 LogicalPlan 时丢掉了重要字段：

- operator class，例如 `vector_l2_ops`；
- `WITH (lists = 100)`。

因此必须在 `QueryHook` 中读取原始 `Statement::CreateIndex`，不能依赖 DataFusion 的 `CreateIndex` LogicalPlan。

后续可映射：

| pgvector DDL | LakeSoul |
|---|---|
| `USING ivfflat` | IVF+RaBitQ index |
| `vector_l2_ops` | `Metric::L2` |
| `vector_ip_ops` | `Metric::InnerProduct` |
| `WITH (lists=N)` | `nlist=N` |
| `SET ivfflat.probes=N` | `nprobe=N` |
| `USING hnsw` | 明确拒绝 |
| `vector_cosine_ops` | 当前拒绝 |
| `vector_l1_ops` | 当前拒绝 |

注意：LakeSoul 实际使用的是 **IVF+RaBitQ**，不是 pgvector 原生 IVFFlat。接受 `USING ivfflat` 可以作为 SQL 兼容 alias，但文档和 `pg_indexes` 中必须说明真实实现，不能假装算法完全相同。

另外，当前索引构建编排主要在 Python 层。PG server 要执行 `CREATE INDEX`，需要补一套 Rust coordinator：

```text
更新 vector_index_columns
  → 枚举 partition/bucket
  → VectorShardIndexBuilder
  → 发布 manifests
  → 成功后提交索引状态
```

因此建议第一版仍保持只读，先不开放 `CREATE INDEX`。

---

# 六、推荐实现顺序

## PR 1：精确查询语法

支持：

```sql
SELECT id,
       embedding <-> '[3,1,2]' AS distance
FROM items
ORDER BY embedding <-> '[3,1,2]'
LIMIT 10;
```

内容：

- PG Server 改用 `create_lakesoul_session_ctx`;
- `PgVectorExprPlanner`;
- `<->`、`<#>`；
- vector text literal；
- 精确 distance UDF；
- dimension/null/finite 校验；
- 不使用索引。

这是最小闭环，而且即使索引缺失也正确。

## PR 2：ANN query optimization

- `VectorSearchSpec`;
- Top-K logical rule；
- 多 shard 并发检索；
- candidate PK `IN` filter；
- 精确 rerank；
- metric 校验；
- missing index fallback。

## PR 3：索引 snapshot 正确性

- manifest 记录 LakeSoul snapshot；
- stale index 检测；
- 新文件 hybrid scan；
- update/delete/compaction 一致性。

## PR 4：PG 会话语法

- `CREATE EXTENSION vector`;
- `SET/SHOW ivfflat.probes`;
- connection-local state；
- `pg_extension` 和基础 pgvector catalog 行。

## PR 5：DDL 与 wire type

- `vector(n)` DDL；
- `CREATE INDEX USING ivfflat`;
- vector OID；
- binary prepared parameters；
- pgvector-python/JDBC adapter 兼容。

`<=>`、`<+>`、HNSW、halfvec、sparsevec、bit vector 最后做；当前索引层没有这些对应能力。

---

# 七、验证矩阵

至少覆盖：

### 解析与精确语义

```sql
embedding <-> '[...]'
embedding <#> '[...]'
embedding <=> '[...]'
embedding <+> '[...]'
```

检查：

- 正常结果；
- NULL；
- 维度不一致；
- NaN/Infinity；
- literal 和 prepared parameter；
- `<#>` 返回负 inner product；
- `<->` 返回欧氏距离而非平方距离。

### ANN 正确性

- 单 shard；
- 多 bucket；
- 多 range partition；
- 全局 Top-K；
- WHERE + Top-K；
- duplicate PK/version merge；
- 更新向量；
- 删除记录；
- 新数据已 commit 但索引未更新；
- manifest 缺失/损坏；
- L2 operator 查询 IP index；
- ANN 结果经过精确 rerank。

### PG 协议

- simple query；
- extended query；
- `$1` 参数；
- psql；
- JDBC；
- psycopg；
- pgvector-python `register_vector()`；
- text/binary result format。

---

# 八、主要风险排序

| 优先级 | 风险 |
|---|---|
| P0 | 缺失或 stale index 被解释为空结果，静默漏数据 |
| P0 | SQL scan 不走 `LakeSoulReader`，现有 vector options 实际无效 |
| P0 | 多 shard 没做全局 candidate 合并 |
| P0 | query vector/nprobe 写进共享 SessionContext，造成并发串线 |
| P0 | `<=>` 被 DataFusion 当作 null-safe equality |
| P1 | L2 平方距离被直接暴露为 pgvector `<->` 结果 |
| P1 | IP score 没转成 pgvector 的负 inner product |
| P1 | Arrow wire 输出是 `float4[]`，pgvector driver 注册失败 |
| P1 | 当前只支持首个 UInt64/Int64 PK，复合/字符串 PK 无法建索引 |
| P2 | `CREATE INDEX` 的 opclass/`WITH` 参数在 DataFusion plan 中丢失 |
| P2 | `<+>` 在当前 sqlparser 中直接解析失败 |

---

## 最终数据流

```text
PG SQL
  │
  ▼
PostgresCompatibilityParser
  │
  ├─ <-> → LtDashGt
  ├─ <#> → Custom("<#>")
  └─ <=> → Spaceship
  │
  ▼
PgVectorExprPlanner
  │
  ▼
pgvector_* distance UDF
  │
  ▼
LogicalPlan
  │
  ▼
VectorTopKOptimizerRule
  │
  ├─ 无可用/完整索引 ──────────────┐
  │                                │
  └─ 有完整索引                    │
       │                           │
       ▼                           │
  按 partition/bucket 并发 ANN      │
       │                           │
       ▼                           │
  candidate PK set                 │
       │                           │
       └──────────────┬────────────┘
                      ▼
             LakeSoulTableProvider
                      │
                      ▼
          DataSourceExec + MergeParquetExec
                      │
                      ▼
               精确 distance
                      │
                      ▼
               global Sort + Limit
                      │
                      ▼
              Arrow → PG wire
```

推荐下一步直接做 **PR 1：`<->`/`<#>` 精确查询闭环**。它能先确定 parser、类型、UDF、prepared parameter 和 PG wire 的边界，再接 ANN；反过来先接索引，会把语法问题和 snapshot 正确性问题混在一起。

参考：[pgvector 官方语法与操作符](https://github.com/pgvector/pgvector#querying)。