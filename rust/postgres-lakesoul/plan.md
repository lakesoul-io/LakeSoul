## 结论

建议按 **“单机 PG 兼容 → 分布式只读 → LakeSoul merge-on-read 分布式正确性 → 生产化 → 写入”** 的顺序推进。

不要一开始同时解决完整 PostgreSQL 语义、分布式执行和分布式写入。第一版目标应明确为：

> 通过 PostgreSQL wire protocol 暴露 LakeSoul 表，支持 psql/JDBC/BI 工具进行分布式分析查询；DDL/DML 暂不开放。

最关键的技术约束：

1. 当前 LakeSoul 使用 **DataFusion 54 / Arrow 58**：`../Cargo.toml:46-75`。
2. `datafusion-postgres 0.18.x` 正好是 **DataFusion 54 / Arrow 58**。
3. `datafusion-distributed v3.0.0` 也是 **DataFusion 54 / Arrow 58**。
4. `datafusion-distributed v4.0.0` 已升级到 **DataFusion 55 / Arrow 59**，目前不能直接接入。
5. LakeSoul 的 `MergeParquetExec` 是自定义物理节点，不能仅打开 distributed planner 就认为 merge-on-read 可以正确分布式执行。

因此第一版依赖应固定为：

```toml
datafusion-postgres = "=0.18.0"
datafusion-distributed = "=3.0.0"
```

不要跟随两个仓库的 `main/master` 分支。

---

# 一、目标架构

```text
 psql / JDBC / DBeaver / Metabase / Grafana
                    |
              PostgreSQL TLS
                 :5432
                    |
          TCP Load Balancer
                    |
      ┌─────────────▼─────────────┐
      │ LakeSoul PG Coordinator   │
      │                           │
      │ - pgwire                  │
      │ - 每连接 SessionContext   │
      │ - PostgreSQL parser       │
      │ - pg_catalog              │
      │ - LakeSoul RBAC           │
      │ - LakeSoulCatalog         │
      │ - LakeSoulQueryPlanner    │
      │ - Distributed Planner     │
      └───────┬───────────┬───────┘
              │           │
       Metadata read      │ Distributed physical plan
              │           │ Arrow Flight/gRPC + mTLS
              ▼           ▼
   LakeSoul Metadata PG   Worker Resolver
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
          Worker 1    Worker 2    Worker N
          :8000       :8000       :8000
              │           │           │
              ├───────────┼───────────┤
              │ S3 / HDFS / local storage
              │
     Parquet/Vortex scan + LakeSoul merge-on-read
              │
              └──── Arrow RecordBatch ────► Coordinator
                                              │
                                         final aggregate
                                              │
                                     arrow-pg row encoding
                                              │
                                         PG result stream
```

建议：

- PG Coordinator 和 Worker 使用独立 Deployment。
- 不把 pgwire 直接塞进 `lakesoul-flight`。
- 将公共 Session/RuntimeEnv 构造逻辑放到 `lakesoul-datafusion`。
- 可以新增一个 `rust/lakesoul-query-server` crate，包含两个二进制：
  - `lakesoul-pg-server`
  - `lakesoul-query-worker`

这样 PG 协议生命周期、Worker 扩缩容、Flight SQL 服务互不耦合。

---

# 二、对外对象映射

建议固定一种映射，不提供多套别名：

| PostgreSQL 概念 | LakeSoul 概念 |
|---|---|
| database | 固定为 `lakesoul` |
| schema | LakeSoul namespace |
| table | LakeSoul table |
| `pg_catalog` | `datafusion-pg-catalog` |
| `information_schema` | DataFusion information schema |
| 默认 `search_path` | `default, pg_catalog` |

不要同时把 LakeSoul `default` namespace 映射成 `public`，否则 SQL、权限和元数据查询会形成两套命名体系。

当前 `create_lakesoul_session_ctx` 使用大写 `LAKESOUL` catalog，而 PostgreSQL 非引号标识符默认小写。接入前应统一成小写 `lakesoul`，并迁移现有调用和测试。

需要覆盖：

- `table`
- `schema.table`
- `lakesoul.schema.table`
- 大小写和 quoted identifier
- `current_database()`
- `current_schema()`
- JDBC `DatabaseMetaData`

---

# 三、端到端数据流

## 查询入口

```text
PG SQL
  → datafusion-postgres PostgreSQL compatibility parser
  → DataFusion LogicalPlan
  → LakeSoulCatalog / LakeSoulNamespace
  → LakeSoulTableProvider::scan
  → 查询 LakeSoul metadata，固定本次查询的文件快照
  → DataSourceExec + MergeParquetExec
  → LakeSoulQueryPlanner
  → DistributedQueryPlanner
  → stages/tasks/network boundaries
  → worker 执行
  → Arrow batches
  → arrow-pg 编码为 PostgreSQL DataRow
  → 客户端
```

相关现有入口：

- Session 构造：`lakesoul-datafusion/src/lib.rs:52`
- Catalog：`lakesoul-datafusion/src/catalog/lakesoul_catalog.rs`
- Namespace：`lakesoul-datafusion/src/catalog/lakesoul_namespace.rs`
- 表扫描：`lakesoul-datafusion/src/datasource/table_provider.rs:701`
- LakeSoul planner：`lakesoul-datafusion/src/planner/physical_planner.rs`
- merge-on-read：`lakesoul-io/src/physical_plan/merge/mod.rs`

## 状态变化

只读版本中，持久状态不应发生变化：

- LakeSoul metadata：只读。
- LakeSoul 数据文件：只读。
- Coordinator：
  - 连接级 Session 状态；
  - prepared statement/portal；
  - query/cancellation registry；
  - WorkerResolver 快照。
- Worker：
  - query-local memory；
  - spill 文件；
  - object-store cache；
  - task metrics。

## 并发模型

- 每个 PG 连接一个 Tokio task。
- 同一连接的语句按 PostgreSQL 协议顺序处理。
- 不同连接并发规划和执行。
- 一个查询拆成多个 stage/task 并发下发到 Worker。
- PG 客户端读取慢时，背压必须一路传到 Arrow stream，不能在 Coordinator 全量 collect。

## 错误流

建议统一映射 SQLSTATE：

| 错误 | SQLSTATE |
|---|---|
| 表不存在 | `42P01` |
| 无权限 | `42501` |
| 查询取消/超时 | `57014` |
| 不支持的 SQL/DML | `0A000` |
| 资源不足 | `53200` |
| Worker 网络失败 | `08006` 或服务端内部错误 |
| 未分类内部错误 | `XX000` |

---

# 四、实施阶段

## 阶段 0：固定产品边界和依赖

### 工作项

1. 固定 DataFusion 54 依赖组合：
   - `datafusion-postgres = 0.18.0`
   - `datafusion-distributed = 3.0.0`
2. 定义第一版为只读：
   - 支持 `SELECT`、`EXPLAIN`、prepared statement；
   - 支持 BI 工具必要的 `SHOW`、`SET`、`BEGIN/COMMIT`；
   - 拒绝 `INSERT/UPDATE/DELETE/CREATE/DROP/COPY`。
3. `BEGIN/COMMIT` 只提供 READ COMMITTED 风格的只读会话行为：
   - 每条语句重新取得 LakeSoul 快照；
   - `REPEATABLE READ`、`SERIALIZABLE` 明确拒绝。
4. 建立 SQL、类型、客户端兼容矩阵。

### 验收

- Cargo 依赖树中只有一套 DataFusion/Arrow 主版本。
- 不依赖 Git `main/master`。
- 不支持的写操作返回明确 SQLSTATE，不允许静默成功。

---

## 阶段 1：单机 pgwire 基线

先证明 LakeSoul Catalog 能通过 PG 协议稳定工作，不引入分布式变量。

### 工作项

1. 提取公共 Session 构造器，例如：

```text
LakeSoulSessionFactory
  ├── build_base_state()
  ├── build_pg_session(identity)
  ├── build_coordinator_state(worker_resolver)
  └── build_worker_state(headers)
```

2. 重构 `create_lakesoul_session_ctx`：
   - 不再通过 `env::set_var` 修改进程全局配置；
   - ObjectStore 使用显式配置；
   - `target_partitions` 参数化，不再固定为 `1`；
   - catalog 名统一为 `lakesoul`。

3. 注册：
   - LakeSoul catalog；
   - `pg_catalog`；
   - PostgreSQL compatibility functions；
   - `information_schema`。

4. 实现每连接独立的 `SessionContext`。

这是生产化的必要条件。上游 `datafusion-postgres` 当前共享一个 `SessionContext`，其 `SET TIME ZONE` 会修改共享 Session 配置，可能造成跨连接污染。建议：

- 给上游贡献 `SessionContextFactory`；或
- 在 LakeSoul 内维护一层很薄的自定义 handler。

不要接受“所有连接共享一个 SessionContext”。

5. 修复 Catalog 并发风险：
   - `LakeSoulCatalog::schema_names` 当前同步阻塞异步 metadata；
   - `LakeSoulNamespace::table_names` 存在 `block_on`、`spawn`、`expect`；
   - BI 工具会高频并发查询元数据。

建议维护异步刷新的 namespace/table catalog snapshot，CatalogProvider 的同步方法只读快照。

6. 保持 Arrow 流式返回，禁止全量结果 materialize 后再编码 PG 行。

### 验收场景

```sql
\dn
\dt default.*
\d default.some_table

SELECT * FROM default.some_table LIMIT 10;
SELECT count(*) FROM default.some_table;
SELECT * FROM default.some_table WHERE id = $1;
```

客户端：

- psql
- PostgreSQL JDBC
- psycopg
- DBeaver
- Metabase metadata sync

上游当前声明支持 psql、DBeaver、Metabase、Grafana；DataGrip 和 PowerBI 尚未声明支持，不应在第一版承诺。

---

## 阶段 2：分布式 append-only POC

先接通 distributed planner、WorkerResolver 和 Worker，不立即处理复杂 merge 语义。

### 工作项

1. Coordinator 的 builder 顺序必须正确：

```rust
SessionStateBuilder::new()
    .with_config(config)
    .with_runtime_env(runtime)
    .with_default_features()
    .with_query_planner(LakeSoulQueryPlanner::new_ref())
    .with_distributed_worker_resolver(resolver)
    .with_distributed_planner()
    .build()
```

`with_distributed_planner()` 会包装已有 QueryPlanner；如果顺序反过来，LakeSoul planner 会覆盖 distributed planner。

2. 启动独立 Worker：

```text
Worker::from_session_builder(...)
  → 注册 RuntimeEnv
  → 注册 S3/HDFS ObjectStore
  → 注册 LakeSoul codec
  → 注册配置扩展
```

3. WorkerResolver：
   - 本地开发：静态 URL 列表；
   - Kubernetes：EndpointSlice/watch；
   - 只返回 Ready 且协议版本匹配的 Worker；
   - `get_urls()` 只读取内存快照，不能同步请求 Kubernetes API。

4. Coordinator 和 Worker 使用相同：
   - DataFusion/Arrow 版本；
   - LakeSoul 物理计划 codec 版本；
   - ObjectStore 配置；
   - UDF/配置扩展。

5. 开启分布式 metrics，确认 `EXPLAIN ANALYZE` 能看到多个 task 和网络传输。

### 验收

- 3 个 Worker 执行 append-only LakeSoul 表的 scan/filter/aggregate/join。
- 结果与单机逐行或 checksum 一致。
- 物理计划明确出现分布式 stage，不能仅凭性能推测。
- 无 Worker 时生产模式快速失败，不静默回退到 Coordinator。
- 开发模式可配置单机回退。

---

## 阶段 3：LakeSoul merge-on-read 分布式正确性

这是整个方案最重要、风险最高的一步。

当前 `LakeSoulTableProvider::scan` 会：

1. 按 `partition_desc` 组织文件；
2. 为每组文件创建多个 `DataSourceExec`；
3. 用 `MergeParquetExec` 合并相同 LakeSoul partition/bucket 的文件；
4. 用 `UnionExec` 合并不同 partition。

### 核心不变量

> 同一个 LakeSoul range/hash partition 下，参与 merge-on-read 的所有版本文件必须在同一个 merge task 中执行，不能被 distributed planner 任意拆到多个 Worker。

否则会发生：

- 同一主键产生重复行；
- 新旧版本在不同 Worker，`UseLast` 失效；
- CDC delete 不能正确删除旧记录；
- schema evolution/default column 行为不一致。

### 工作项

1. 实现 `PhysicalExtensionCodec`：

```text
LakeSoulPhysicalExtensionCodec
  ├── MergeParquetExec
  ├── 后续需要时：DefaultColumnExec
  ├── SelfIncrementalIndexColumnExec
  └── 写入阶段再支持 Sink/Repartition 节点
```

2. 定义版本化 wire proto，只序列化执行必需字段：

```text
LakeSoulMergeExecProto
  - codec_version
  - schema
  - primary_keys
  - default_column_values
  - merge_operators
  - file/compaction flags
  - non-secret IO behavior
  - table_id / partition_desc / snapshot id
```

3. 绝对不能把以下内容序列化进物理计划：

- AWS secret/access key；
- metadata 数据库密码；
- JWT secret；
- 任意 ObjectStore credentials。

Worker 应通过 workload identity、IRSA、环境或 Secret 挂载自行获得存储凭据。

4. 注册 codec：
   - Coordinator 编码；
   - Worker 解码；
   - rolling upgrade 时 codec version 必须兼容或拒绝调度。

5. 自定义 task count/scale handler：
   - 一个 merge work unit = 同一 `partition_desc` 的完整文件集合；
   - task 只能在 work unit 边界拆分；
   - 禁止在单个 work unit 内按文件拆分。

6. 先验证现有 `UnionExec` 是否已经让每个 merge group 成为独立 stage。若不能稳定保证，新增显式的 `LakeSoulDistributedScanExec`，持有 `Vec<MergeWorkUnit>`，由 LakeSoul handler 分配 work unit。不要依赖 distributed planner 的内部偶然行为。

### 验收矩阵

必须同时比较单机和分布式结果：

- append-only 表；
- 主键表，多次 upsert；
- 同一主键跨多个文件；
- CDC delete；
- range partition；
- hash bucket；
- schema evolution；
- default column；
- compacted + uncompacted 文件混合；
- filter/projection pushdown；
- `ORDER BY/LIMIT`；
- 查询期间并发 upsert/compaction。

必须证明：

- 每个 merge work unit 恰好执行一次；
- 没有重复、丢失或旧版本回流；
- Coordinator 规划出来的文件快照在整个查询中不变化；
- GC 不会删除仍被运行中查询引用的文件。

---

## 阶段 4：认证、授权和多租户

### 建议认证方式

LakeSoul 已有 JWT/Claims，但没有现成的 PostgreSQL 密码体系。第一版可采用：

```text
PG username = Claims.sub
PG password = JWT token
```

要求：

- 外部 PG 端口强制 TLS；
- 自定义 `StartupHandler` 验证 JWT；
- Claims 绑定到连接生命周期；
- 不使用上游默认的 `NoopStartupHandler`。

长期如需原生密码体验，再接入 SCRAM-SHA-256/企业 IdP。

### RBAC

复用：

- `lakesoul_metadata::rbac::verify_permission_by_table_name`
- 现有 Claims：`sub`、`group`

但权限检查不能只在最终执行前做一次：

1. Schema/table 枚举就要过滤，避免 BI 元数据浏览泄露表名。
2. LogicalPlan 中每个 `TableScan` 都要授权。
3. Worker 收到任务时应验证来自可信 Coordinator。
4. 计划中携带 `table_id/domain`，使 Worker 可以审计任务身份。
5. Coordinator → Worker 使用 mTLS。
6. Claims/query_id/tenant 可以通过 distributed passthrough headers 传播；只允许白名单 header。

不要仅靠 SQL 文本正则提取表名。应在 LogicalPlan 生成后遍历真实 `TableScan`。

### 验收

- 未认证连接被拒绝；
- 无权限表既不能查询，也不能从 `pg_catalog`/JDBC metadata 中看到；
- 多个用户并发连接，`search_path/timezone/statement_timeout` 不串；
- Worker 不能接受伪造的外部请求；
- plan、日志和 metrics 中不出现密钥。

---

## 阶段 5：生产化

### 取消和超时

上游 `datafusion-postgres` 当前的 `statement_timeout` 主要包围 DataFrame 创建，未完整覆盖结果流消费。需要扩展为：

```text
PG cancel / disconnect / timeout
  → query CancellationToken
  → Coordinator physical execution
  → distributed stages
  → Worker tasks
  → ObjectStore reads
```

客户端停止读取时，也应取消远端 task。

### 资源治理

- 最大连接数；
- 用户级并发查询数；
- Coordinator/Worker memory pool；
- spill 目录及容量；
- task 数和 scan bytes；
- 最大结果行数/字节数；
- 慢客户端背压；
- Worker readiness 和 draining。

### 可观测性

统一 `query_id`，记录：

- PG 解析/规划耗时；
- metadata 耗时；
- Worker 选择；
- 每 stage/task 的 rows/bytes；
- object-store bytes；
- shuffle bytes；
- spill bytes；
- PG row encoding 耗时；
- 客户端等待时间；
- cancel/failure 原因。

`datafusion-distributed` 支持把 Worker metrics 汇总回 Coordinator，可直接用于跨节点 `EXPLAIN ANALYZE`。

### 故障测试

- Worker 在 scan 中退出；
- Worker 在 shuffle 中退出；
- Coordinator 断开；
- S3 超时；
- metadata 暂时不可用；
- PG 客户端取消；
- PG 客户端长时间不读取；
- Worker 版本不一致；
- 扩缩容期间运行查询。

上游文档没有提供完整调度器或明确的自动重试保证，因此当前应按“Worker 失败导致查询失败”设计和测试；不要默认任务会自动容错。

---

# 五、写入能力放到后续独立阶段

LakeSoul 已有 DataFusion INSERT/sink，但不能因此直接宣称具备 PostgreSQL 事务语义。

建议顺序：

1. 单语句、autocommit `INSERT`。
2. Worker 写 staging 文件。
3. Worker 返回 commit manifest。
4. Coordinator 汇总并执行唯一一次 metadata commit。
5. `query_id + attempt_id` 保证重试幂等。
6. commit 失败时异步清理 orphan 文件。
7. 再考虑 upsert、COPY、DDL。
8. 多语句事务必须等真正的 transaction coordinator/snapshot pinning 完成后再开放。

必须避免：

- 每个 Worker 独立提交 metadata；
- `BEGIN/COMMIT` 只返回成功标签但实际每条语句已经提交；
- Worker 重试导致重复文件或重复 commit。

如果主要目标是 BI 和分析生态，这一阶段可以长期不做。

---

# 六、推荐 PR 拆分

## PR 1：依赖和 Session 重构

- 固定兼容依赖版本；
- `LakeSoulSessionFactory`；
- 移除查询路径中的全局 `env::set_var`；
- catalog 小写化；
- `target_partitions` 参数化；
- Catalog metadata snapshot/cache。

## PR 2：单机 PG Server

- pgwire/TLS；
- `pg_catalog`；
- per-connection Session；
- 只读 SQL gate；
- psql/JDBC/DBeaver 集成测试。

## PR 3：分布式基础设施

- Worker binary；
- static WorkerResolver；
- distributed planner 组合；
- RuntimeEnv/ObjectStore 一致性；
- distributed metrics。

## PR 4：LakeSoul 物理计划 codec

- versioned proto；
- `MergeParquetExec` codec；
- 无敏感信息序列化测试；
- Coordinator/Worker codec compatibility。

## PR 5：merge work-unit 调度

- partition/bucket 原子调度；
- 单机/分布式结果对照；
- CDC/schema evolution/compaction 并发测试。

## PR 6：认证与生产化

- JWT-as-password；
- RBAC catalog filtering；
- mTLS；
- cancel/timeout propagation；
- metrics、limits、chaos tests。

---

# 七、主要风险排序

| 优先级 | 风险 | 处理 |
|---|---|---|
| P0 | DataFusion/Arrow 双版本 | 固定 PG 0.18 + distributed 3.0 |
| P0 | 同一主键版本被拆到不同 Worker | 按 merge work unit 原子调度 |
| P0 | 共享 SessionContext 导致用户会话串扰 | 每连接 Session |
| P0 | 凭据被序列化进物理计划 | 专用无密钥 wire proto |
| P0 | pgwire 默认无认证 | 自定义 StartupHandler + TLS |
| P1 | `target_partitions=1` 使分布式失效 | 配置化并验证物理计划 task 数 |
| P1 | Catalog 中 block/expect 在并发 metadata 探测下失败 | 异步刷新只读快照 |
| P1 | PG cancel 不传播到 Worker | query cancellation token 全链路 |
| P1 | compaction/GC 删除运行中查询文件 | snapshot lease 或延迟 GC |
| P2 | PostgreSQL 客户端依赖未实现的 pg_catalog 函数 | 客户端兼容矩阵驱动补齐 |
| P2 | Worker 扩缩容/版本不一致 | readiness + version-aware resolver |
| P2 | 慢 PG 客户端拖住 Coordinator 内存 | 流式编码和背压 |

---

# 八、明确假设与不确定点

- **假设**：第一目标是 PostgreSQL 分析生态兼容，而非完整 OLTP PostgreSQL。
- **假设**：第一版允许只读，不要求分布式 DML。
- **不确定**：现有 `UnionExec<MergeParquetExec>` 经 distributed planner 改写后，是否天然保持每个 merge group 的原子性。必须通过物理计划和主键正确性测试确认；不能从 API 文档推断。
- **不确定**：目标客户端是否包含 DataGrip/PowerBI。上游当前未声明支持，应单独验证。
- **不确定**：LakeSoul 当前文件 GC 是否有运行中查询 lease。若没有，分布式查询持续时间变长后风险更明显。

参考资料：

- [datafusion-postgres](https://github.com/datafusion-contrib/datafusion-postgres)
- [datafusion-postgres Cargo.toml](https://github.com/datafusion-contrib/datafusion-postgres/blob/master/Cargo.toml)
- [datafusion-distributed Quick Start](https://datafusion-contrib.github.io/datafusion-distributed/user-guide/01-quick-start.html)
- [datafusion-distributed v3.0.0 Cargo.toml](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/Cargo.toml)
- [分发自定义 ExecutionPlan](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/user-guide/04-distribute-custom-plan.md)
- [Worker 定制](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/user-guide/03-worker.md)
- [Passthrough headers](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/docs/source/advanced/01-passthrough-headers.md)
- [Distributed metrics](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/docs/source/user-guide/05-metrics.md)

