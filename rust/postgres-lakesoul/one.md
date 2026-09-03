1. 让 LakeSoulCatalog 支持 pg_catalog virtual overlay

 文件： rust/lakesoul-datafusion/src/catalog/lakesoul_catalog.rs

 当前 register_schema() 的问题是：它把任何注册都解释为“创建 LakeSoul metadata
 namespace”。

 需要保留 LakeSoul 业务 schema 的当前逻辑，但对系统 schema 保存真正传入的 provider。

 结构可以是：

 ```rust
pub struct LakeSoulCatalog {
    metadata_client: MetaDataClientRef,
    context: Arc<SessionContext>,

    // 只放 DataFusion/PG 的虚拟 schema；不写 LakeSoul metadata。
    system_schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}
 ```

 register_schema() 的关键分流：

 ```rust
fn register_schema(
    &self,
    name: &str,
    schema: Arc<dyn SchemaProvider>,
) -> Result<Option<Arc<dyn SchemaProvider>>> {
    if name == "pg_catalog" {
        return Ok(self.system_schemas.write().insert(name.into(), schema));
    }

    // 保留现有逻辑：
    // 创建 LakeSoul Namespace，并返回已有的 LakeSoulNamespace（若存在）。
    ...
}
 ```

 schema() 先查 virtual overlay：

 ```rust
fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
    if let Some(schema) = self.system_schemas.read().get(name) {
        return Some(Arc::clone(schema));
    }

    // 现有 LakeSoul metadata namespace lookup
    ...
}
 ```

 schema_names() 也必须合并 pg_catalog：

 ```text
LakeSoul metadata namespaces: default, sales, analytics
virtual schemas:             pg_catalog
result:                      default, sales, analytics, pg_catalog
 ```

 否则 pg_namespace / pg_class 的动态生成结果不完整，部分 psql/JDBC metadata join
 会出问题。

 不要把 pg_catalog 写入 LakeSoul metadata。它不是业务 namespace，不能被用户 DDL、ACL
 或 namespace 清理流程当作普通 schema 处理。

 ────────────────────────────────────────────────────────────────────────────────

 2. 安装 catalog 的顺序保持现在这样

 文件： rust/postgres-lakesoul/src/main.rs

 现有正确顺序是：

 ```rust
let ctx =
    create_lakesoul_session_ctx(Arc::clone(&meta_client), &CoreArgs::from_env())?;

setup_pg_catalog(&ctx, "lakesoul", am)?;
 ```

 原因：

 ```text
create_lakesoul_session_ctx()
  → 注册 LakeSoulCatalog 到 catalog list，名称为 lakesoul

setup_pg_catalog(ctx, "lakesoul", ...)
  → 查找 lakesoul catalog
  → register_schema("pg_catalog", PgCatalogSchemaProvider)
 ```

 不要改成 "datafusion"。当前 session 明确禁用默认 datafusion.public，并且只注册了
 lakesoul catalog。

 安装完成后的可验证状态：

 ```text
ctx.catalog("lakesoul").unwrap().schema("pg_catalog")
  → Some(PgCatalogSchemaProvider)

ctx.catalog("lakesoul").unwrap()
   .schema("pg_catalog").unwrap()
   .table("pg_database")
  → Some(...)
 ```

 这正是修复 lakesoul.pg_catalog.pg_database not found 的最小断言。

 ────────────────────────────────────────────────────────────────────────────────

 3. 启动阶段严格校验 database

 现状： datafusion-postgres 的 SimpleStartupHandler 仅实现
 NoopStartupHandler。pgwire 会把 StartupMessage 参数写到：

 ```text
client.metadata()["database"]
 ```

 但 handler 不读取它，因此任何 database 名称都能连上。

 ### 不建议

 ```rust
impl NoopStartupHandler for LakeSoulStartupHandler {
    async fn post_startup(...) { ... }
}
 ```

 NoopStartupHandler 已在 post_startup() 前发送 AuthenticationOk。此时才拒绝
 database，协议行为不干净。

 ### 正确做法

 新增本地 handler，例如：

 ```text
rust/postgres-lakesoul/src/handler.rs
 ```

 实现 pgwire::api::auth::StartupHandler，在发送认证成功前验证：

 ```rust
const DATABASE: &str = "lakesoul";

async fn on_startup<C>(
    &self,
    client: &mut C,
    message: PgWireFrontendMessage,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
{
    let PgWireFrontendMessage::Startup(startup) = &message else {
        return Ok(());
    };

    protocol_negotiation(client, startup).await?;
    save_startup_parameters_to_metadata(client, startup);

    let database = client
        .metadata()
        .get(METADATA_DATABASE)
        .map(String::as_str)
        .unwrap_or_default();

    if database != DATABASE {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".into(),
            "3D000".into(), // invalid_catalog_name / database does not exist
            format!("database \"{database}\" does not exist"),
        ))));
    }

    // register cancellation handle
    // emit AuthenticationOk + ParameterStatus + BackendKeyData + ReadyForQuery
    ...
}
 ```

 同时 ParameterStatus 应与实际 session 一致，而不是 pgwire 默认的：

 ```text
search_path = public
 ```

 至少返回：

 ```text
search_path = default, pg_catalog
session_authorization = <authenticated principal>
default_transaction_read_only = on   // 如果第一版只读
 ```

 ────────────────────────────────────────────────────────────────────────────────

 4. 用本地 PgWireServerHandlers 替代 serve()

 原因： datafusion_postgres::serve() 固定创建自己的 HandlerFactory，其中 startup
 handler 是不可配置的 SimpleStartupHandler。

 但 crate 提供：

 ```rust
serve_with_handlers(...)
 ```

 实现本地 handler factory：

 ```rust
struct LakeSoulHandlers {
    session_service: Arc<DfSessionService>,
    startup_handler: Arc<LakeSoulStartupHandler>,
    cancel_handler: Arc<DefaultCancelHandler>,
    error_handler: Arc<LakeSoulErrorHandler>,
}
 ```

 实现：

 ```rust
impl PgWireServerHandlers for LakeSoulHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.session_service.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.session_service.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup_handler.clone()
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.cancel_handler.clone()
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        self.error_handler.clone()
    }
}
 ```

 main.rs 从：

 ```rust
serve(ctx, &server_opts).await?;
 ```

 变为：

 ```rust
let handlers = Arc::new(LakeSoulHandlers::new(ctx)?);
serve_with_handlers(handlers, &server_opts).await?;
 ```

 第一版仍可复用单个 DfSessionService，因为只支持一个逻辑 database；但 SET
 search_path、TimeZone、prepared statement、事务状态还不是连接隔离的。下一阶段再将
 SessionContext 下沉到 connection-local session。

 ────────────────────────────────────────────────────────────────────────────────

 5. 修正 pg_database 的内容

 这里有一个必须先处理的矛盾。

 datafusion-pg-catalog 当前的 PgDatabaseTable：

 1. 将每个 DataFusion catalog 视为一个 database；
 2. 如果没有 postgres，额外伪造一个 postgres row。

 所以即使只注册 lakesoul，\l 仍可能显示：

 ```text
lakesoul
postgres
 ```

 但按上面的 startup policy，\c postgres 会拒绝。这是不一致的。

 选择其一：

 ### 推荐：严格单 database

 本地 patch/fork datafusion-pg-catalog：

 ```text
pg_database
  → 仅返回已注册且允许连接的 LakeSoul logical database
  → 第一版仅 lakesoul
 ```

 删除自动补的：

 ```rust
// Always include a "postgres" database entry ...
 ```

 然后将 datafusion-postgres 与直接使用的 datafusion-pg-catalog 一起指向同一
 patch，避免出现两个不同版本的 PgCatalogSchemaProvider。