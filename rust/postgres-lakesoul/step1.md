## 问题

当前 `postgres-lakesoul` 在启动时只创建了一个全局 `SessionContext`，随后直接调用：

```rust
serve(ctx, &server_opts).await
```

`datafusion-postgres` 的 `HandlerFactory` 也把这个 `SessionContext` 固定在单个 `DfSessionService` 中。结果：

- `SET TIME ZONE` 会修改共享 `SessionState.config_mut()`，跨连接污染。
- `search_path`、`statement_timeout` 虽然部分写在 pgwire client metadata，但没有统一的连接级 Session 状态模型。
- prepared statement / portal 本身由 pgwire 的 `MemPortalStore` 按连接保存，但其中的 logical plan 来自共享 `SessionContext`。
- 默认 cancel handler 只按 `(pid, secret_key)` 找连接并触发当前 `ConnectionHandle`；上游 `DfSessionService` 没有把取消继续传播到 DataFusion 流执行和 Worker。

`cargo tree` 确认当前使用 `datafusion-postgres 0.18.0` 与 `pgwire 0.40.7`。

---

## 决策

不要在全局 `SessionContext` 上加锁或执行前“临时改回配置”。必须改为：

```text
server-global immutable resources
  ├── MetaDataClient pool
  ├── LakeSoulSessionFactory
  ├── RuntimeEnv / ObjectStore configuration template
  ├── Auth / connection manager
  └── QueryRegistry
           │
           └─ 每个 PG connection
                ├── Arc<SessionContext>
                ├── SessionSettings
                ├── pgwire prepared statement / portal store
                ├── transaction status
                └── current QueryLease
```

核心原则：

1. **每个连接创建一个 `SessionContext`，连接关闭即释放。**
2. **会话变量只能写连接自己的状态。**
3. **prepared statement、portal、事务状态只存在 pgwire 的单连接 client 内。**
4. **取消精确定位到 `connection_id + query_id`，而不是取消共享上下文或同用户的其他查询。**

---

## 1. Session 工厂：每个连接独立构建 `SessionContext`

在 `lakesoul-datafusion` 增加可复用工厂，不让 `postgres-lakesoul` 自己复制 Session 初始化逻辑：

```rust
pub struct LakeSoulSessionFactory {
    meta_client: MetaDataClientRef,
    session_template: SessionConfig,
    object_store_config: ObjectStoreConfig,
    warehouse: WarehouseConfig,
}

impl LakeSoulSessionFactory {
    pub fn new(meta_client: MetaDataClientRef, args: &CoreArgs) -> Result<Self>;

    pub fn create_pg_session(
        &self,
        identity: SessionIdentity,
        settings: &SessionSettings,
    ) -> Result<Arc<SessionContext>>;
}
```

### 工厂职责

每次 `create_pg_session`：

1. 从不可变模板 clone `SessionConfig`。
2. 应用该连接的初始值：
   - `default_catalog = "lakesoul"`
   - 初始 schema：`search_path` 的第一个可用 schema，默认 `default`
   - `execution.time_zone`
   - 不从环境变量读取或写入会话状态。
3. 创建新的 `RuntimeEnv`。
4. 通过显式配置注册 S3/HDFS/local ObjectStore。
5. 创建新的 `SessionState`，注册：
   - `LakeSoulQueryPlanner`
   - LakeSoul table factory
   - `PgLakeSoulCatalog`
   - `pg_catalog`
   - `information_schema`
6. 返回新的 `Arc<SessionContext>`。

### 必须顺便修复的全局状态

`create_lakesoul_session_ctx_with_catalog_decorator` 当前会执行：

```rust
env::set_var("LAKESOUL_WAREHOUSE_PREFIX", ...);
env::set_var("AWS_SECRET_ACCESS_KEY", ...);
env::set_var("AWS_ACCESS_KEY_ID", ...);
env::set_var("AWS_ENDPOINT", ...);
```

这不能进入 PG server 查询路径。环境变量是进程全局的，多个连接或多租户配置时必然串扰。

改为只构造并传递：

```rust
LakeSoulIOConfigBuilder::new_with_object_store_options(args.s3_options()).build()
```

ObjectStore 注册在每个连接自己的 `RuntimeEnv` 内；凭据不通过 `env::set_var` 传播。

> 若 ObjectStore client 支持安全共享，可在工厂内部共享不可变 client/template；但 `RuntimeEnv` 和 `SessionContext` 仍必须独立。

---

## 2. 连接状态：显式 `PgSession`

`pgwire` 的 `DefaultClient` 已经天然按 socket 保存：

- `metadata`
- `MemPortalStore`
- `SessionExtensions`
- `TransactionStatus`

但它没有 LakeSoul 的连接上下文。启动认证成功后，向 `SessionExtensions` 插入：

```rust
pub struct PgSession {
    pub connection_id: ConnectionId,
    pub identity: SessionIdentity,
    pub context: Arc<SessionContext>,
    pub settings: RwLock<SessionSettings>,
    pub active_query: Mutex<Option<Arc<QueryLease>>>,
}
```

```rust
pub struct SessionSettings {
    pub search_path: Vec<SearchPathEntry>,
    pub time_zone: String,
    pub statement_timeout: Option<Duration>,
}
```

`PgSession` 生命周期严格等同于 TCP 连接生命周期。不要放入全局 `HashMap<user, SessionContext>`，也不要按用户复用。

### Server handler 改造

不能继续使用：

```rust
datafusion_postgres::serve(ctx, &server_opts)
```

因为它的 `HandlerFactory::new(ctx)` 在 server 启动时就绑定了一个共享 `SessionContext`。

改为：

```rust
let factory = Arc::new(LakeSoulHandlerFactory::new(
    session_factory,
    auth_manager,
    query_registry,
));

serve_with_handlers(factory, &server_opts).await?;
```

自定义 `LakeSoulHandlerFactory` 实现：

```rust
impl PgWireServerHandlers for LakeSoulHandlerFactory {
    fn startup_handler(&self) -> Arc<impl StartupHandler>;
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler>;
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler>;
    fn cancel_handler(&self) -> Arc<impl CancelHandler>;
}
```

`StartupHandler` 的职责：

```text
Startup / TLS / authentication
  → 得到 identity
  → SessionFactory.create_pg_session(identity, defaults)
  → 创建 PgSession
  → 插入 client.session_extensions()
  → 注册 pgwire ConnectionHandle / ConnectionGuard
  → ReadyForQuery
```

---

## 3. `search_path`、`TimeZone`、`statement_timeout` 都只修改本连接

### `TimeZone`

当前上游 `SetShowHook` 虽然将 timezone 写入 client metadata，但也会执行：

```rust
session_context
    .state()
    .config_mut()
    .options_mut()
    .execution
    .time_zone = Some(tz.to_string());
```

在共享 Context 下这是串扰根源。

改为从 client extension 获取 `PgSession`：

```rust
let session = pg_session(client)?;
session.settings.write().time_zone = tz.to_string();

session.context.state()
    .config_mut()
    .options_mut()
    .execution
    .time_zone = Some(tz.to_string());
```

因为 `session.context` 是该连接私有，所以该 mutation 是 session-local。

支持：

```sql
SET TIME ZONE 'Asia/Shanghai';
SHOW TIME ZONE;
RESET TIME ZONE;
```

`RESET` 回到服务端配置的默认值，例如 `UTC`，不能依赖前一个连接的值。

### `statement_timeout`

当前上游实现把 timeout 放到 client metadata，且仅包裹：

- `SessionContext::sql`
- `SessionContext::execute_logical_plan`

没有覆盖：

- 优化后开始的 physical execution；
- Arrow RecordBatch stream 消费；
- PG row 编码；
- 客户端慢读取；
- 分布式 Worker task。

应改为 `PgSession.settings.statement_timeout` 是唯一事实源，每条 execute 创建一个 deadline：

```rust
pub struct QueryLease {
    pub query_id: QueryId,
    pub connection_id: ConnectionId,
    pub cancellation: CancellationToken,
    pub deadline: Option<Instant>,
    pub worker_queries: DashMap<WorkerId, WorkerQueryId>,
}
```

执行入口：

```text
statement begins
  → snapshot SessionSettings.statement_timeout
  → create QueryLease
  → install as PgSession.active_query
  → register in QueryRegistry
  → plan / optimize / execute / stream all observe cancellation
  → completion, error, disconnect: remove active QueryLease
```

超时触发后：

```text
deadline elapsed
  → QueryLease.cancel(StatementTimeout)
  → stop coordinator stream
  → cancel worker tasks
  → PostgreSQL SQLSTATE 57014
```

必须覆盖从 logical plan 创建到最后一个 DataRow 写完的完整语句生命周期。

### `search_path`

当前上游只把 `search_path` 写入 client metadata；它不会让 DataFusion 按 PostgreSQL 多 schema 搜索表。

实施上分两层：

1. `SessionSettings.search_path` 保存完整连接级值，例如：

   ```text
   ["tenant_a", "default", "pg_catalog"]
   ```

2. 在 SQL AST → LogicalPlan 前，使用连接的 `SearchPathResolver` 解析未限定表名：
   - 显式 `catalog.schema.table`：不改写；
   - 显式 `schema.table`：不改写；
   - 未限定表名：按 `search_path` 顺序寻找第一个存在且已授权的 schema/table；
   - CTE、derived table、table alias 不能被错误改写；
   - `pg_catalog` 内置对象保留 PostgreSQL 兼容路径；
   - 解析结果统一为 `lakesoul.schema.table`，再交给 DataFusion 生成 logical plan。

不要只把 `SessionConfig.default_schema` 设为 `search_path[0]`。这只能支持单 schema，不能实现 PostgreSQL 的多项搜索顺序。

支持：

```sql
SET search_path TO tenant_a, default, pg_catalog;
SHOW search_path;
RESET search_path;
```

所有 catalog metadata 查询也必须使用该 session 的权限和 search path；否则 JDBC/DBeaver 能看到但不能查询、或能查询但 metadata 不可见。

---

## 4. prepared statement、portal、transaction state 不跨连接

### prepared statement / portal

pgwire 当前的 `MemPortalStore` 已经在 `DefaultClient` 内，是每个 socket 独立的。保持该设计，不要引入全局 prepared statement cache。

但必须替换上游的全局 `Parser`：

```rust
pub struct Parser {
    session_context: Arc<SessionContext>, // 当前上游：错误的共享绑定
}
```

改为无状态 parser facade：

```rust
pub struct LakeSoulQueryParser;

impl QueryParser for LakeSoulQueryParser {
    async fn parse_sql<C>(&self, client: &C, sql: &str, ...) -> PgWireResult<Statement>
}
```

在 `parse_sql` 中：

```text
client.session_extensions()
  → PgSession
  → PgSession.context
  → PgSession.settings.search_path
  → AST-aware name resolution
  → SessionState::statement_to_plan(...)
  → prepared statement 存回当前 client.portal_store
```

这样同名 prepared statement 可以在不同连接中有不同的：

- `search_path`
- `TimeZone`
- identity/RBAC
- logical plan
- 参数类型推断
- catalog snapshot

### transaction state

pgwire 的 `TransactionStatus` 同样已经在每个 `DefaultClient` 中。只读第一版建议语义明确为：

```text
BEGIN / COMMIT / ROLLBACK
  → 只维护 PG 协议 transaction status
  → 每个 SELECT 独立取得 LakeSoul metadata snapshot
  → 不承诺多语句 snapshot pinning
```

因此：

- 支持 `BEGIN`、`COMMIT`、`ROLLBACK` 供 JDBC/BI 客户端工作；
- 拒绝 `REPEATABLE READ`、`SERIALIZABLE`；
- 拒绝写入事务；
- 不伪装成真正的跨语句 LakeSoul 事务。

若后续要支持 repeatable read，需要在 `PgSession` 中增加 transaction snapshot lease，直到 `COMMIT/ROLLBACK` 才释放。

---

## 5. cancel 精确关联到当前 query/session

`pgwire 0.40.7` 已提供正确的连接寻址基础：

```text
CancelRequest(pid, secret_key)
  → ConnectionManager
  → ConnectionHandle
  → 当前连接的 oneshot cancel receiver
```

但上游 `DfSessionService` 只让 pgwire 在 handler future 运行时 `select!`。一旦 handler 返回 `QueryResponse`，DataFrame 的 Arrow stream 仍在被编码/发送，取消不一定覆盖结果流。

### 设计

`QueryRegistry`：

```rust
pub struct QueryRegistry {
    by_query: DashMap<QueryId, Arc<QueryLease>>,
    by_connection: DashMap<ConnectionId, QueryId>,
}
```

`QueryLease`：

```rust
pub struct QueryLease {
    pub query_id: QueryId,
    pub connection_id: ConnectionId,
    pub cancel: CancellationToken,
    pub reason: Mutex<Option<CancelReason>>,
    pub worker_queries: DashMap<WorkerId, WorkerQueryId>,
}
```

其中 `CancelReason` 至少包括：

```rust
enum CancelReason {
    ClientRequest,
    StatementTimeout,
    ClientDisconnected,
    WorkerFailure,
    ServerShutdown,
}
```

### 取消路径

```text
客户端发送 CancelRequest(pid, secret_key)
  → pgwire ConnectionManager 找到唯一 connection
  → LakeSoulCancelHandler 取得该连接的 PgSession.active_query
  → QueryLease.cancel(ClientRequest)
  → DataFusion coordinator execution future / batch stream 终止
  → 发送 CancelQuery(query_id) 到此 query 的每个 Worker
  → Worker QueryRegistry 找到相同 worker_query_id
  → worker CancellationToken.cancel()
  → Drop execution stream / abort task
  → client 收到 SQLSTATE 57014
```

断连和超时走同一条路径：

```text
socket write/read failure          → ClientDisconnected
statement deadline elapsed         → StatementTimeout
worker RPC / Worker task failure   → WorkerFailure
```

### 必须保证的并发不变量

1. 一个连接同一时刻最多一个 `active_query`。
2. `CancelRequest` 只能取消该 `(pid, secret_key)` 对应连接当前的 `QueryLease`。
3. 新 query 注册前，旧 query 的 lease 必须已移除或标记完成。
4. `QueryLease` 通过 RAII 清理；正常完成、错误、取消、断连都必须从 registry 移除。
5. Worker cancel RPC 带 `query_id`、`attempt_id`、Coordinator 身份；不能按 SQL 文本、用户名或 table id 广播取消。
6. 取消后丢弃 coordinator 与 worker 的 `RecordBatch` stream，避免后台继续占用 ObjectStore / memory / shuffle 资源。
7. `57014` 仅用于 client cancel / timeout；Worker 网络故障应返回连接或内部执行错误，不伪装为用户取消。

---

## 6. 推荐代码拆分

```text
rust/postgres-lakesoul/src/
  main.rs
  server.rs                 # accept loop + serve_with_handlers
  session.rs                # PgSession, SessionSettings, SessionIdentity
  session_factory.rs        # LakeSoulSessionFactory 的 PG 装配
  startup.rs                # auth + connection Session 创建
  handlers.rs               # simple / extended query handlers
  parser.rs                 # client-scoped parser + SearchPathResolver
  set_show.rs               # session-local SET / SHOW / RESET
  query_registry.rs         # QueryLease / QueryRegistry
  cancel.rs                 # PG cancel → QueryLease → worker cancel
  stream.rs                 # cancellation-aware Arrow → PG response stream
  catalog.rs
```

公共 DataFusion 初始化能力放到：

```text
rust/lakesoul-datafusion/src/
  session_factory.rs
```

不要把 pgwire 依赖倒灌到 `lakesoul-datafusion`；后者只暴露 LakeSoul `SessionContext` 构造能力。

---

## 7. 测试验收

### 连接隔离

两个独立 PostgreSQL client 并发：

```sql
-- connection A
SET search_path TO schema_a;
SET TIME ZONE 'Asia/Shanghai';
SET statement_timeout TO '50ms';

-- connection B
SET search_path TO schema_b;
SET TIME ZONE 'UTC';
SET statement_timeout TO 0;
```

验证：

- A/B 的 `SHOW search_path`、`SHOW TIME ZONE`、`SHOW statement_timeout` 分别正确；
- A 的未限定表名落到 `schema_a`，B 落到 `schema_b`；
- A 的 timeout 不影响 B；
- A 的 timezone 不改变 B 的 timestamp 显示和计算。

### prepared statement / portal

两个连接使用同名 prepared statement：

```sql
PREPARE q(int) AS SELECT ... WHERE id = $1;
```

验证：

- 两条连接的 statement 和 portal 互不可见；
- 两连接在不同 `search_path` 下的同名 SQL 绑定到各自表；
- 关闭 A 后 B 的 prepared statement/portal 仍可执行；
- B 不能 `EXECUTE` A 的 statement 名称。

### transaction state

```sql
-- A
BEGIN;
SELECT invalid_sql; -- transaction enters Error
SELECT 1;           -- rejected until rollback
ROLLBACK;

-- B
SELECT 1;           -- 仍然正常
```

验证 transaction error state 只影响 A。

### cancellation

1. 在连接 A 执行可观测长查询。
2. 使用 A 的 PostgreSQL cancel token 发送 cancel request。
3. 验证：
   - A 返回 SQLSTATE `57014`；
   - A 对应 `query_id` 变为 cancelled；
   - Worker 收到相同 `query_id` 的 cancel；
   - Worker task / batch stream 停止；
   - 连接 B 的长查询不被影响。
4. 对 `statement_timeout`、客户端断连、Worker cancel RPC 分别重复验证。
5. 对慢客户端读取场景验证：停止读取或断连后远端任务停止，不允许 coordinator 累积全量结果。

---

## 实施顺序

1. **Session factory 重构**
   - 去除 `env::set_var`；
   - 独立 `RuntimeEnv` / `SessionContext`；
   - 保持现有单机 SQL 行为。

2. **替换全局 `serve(ctx)`**
   - 自定义 startup handler；
   - 每连接创建、保存、释放 `PgSession`；
   - 自定义 client-scoped parser。

3. **会话变量**
   - `SET/SHOW/RESET TimeZone`；
   - `statement_timeout` 完整 query 生命周期；
   - AST-aware `search_path` resolver。

4. **QueryLease 与 cancel**
   - PG cancel、timeout、disconnect；
   - 流式 Arrow 编码阶段取消；
   - SQLSTATE 映射。

5. **分布式传播**
   - query/attempt id；
   - coordinator → worker cancel RPC；
   - worker registry 与 task cancellation；
   - chaos / slow-reader 验证。

上游 `datafusion-postgres` 可以贡献 `SessionContextFactory` 和 stream-aware cancellation；在当前版本直接依赖其 `HandlerFactory` 不满足这四项隔离要求，因此 LakeSoul 侧应先实现薄的自定义 handler。