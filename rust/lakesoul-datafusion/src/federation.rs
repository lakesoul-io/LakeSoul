// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Distributed execution of ADBC foreign-table scans.
//!
//! A foreign scan is intentionally one partition. A plan whose stages are all
//! single-task runs on the coordinator; when the scan borders a multi-task
//! stage (a join or aggregation over a distributed LakeSoul scan), its stage is
//! assigned to one worker while the rest of the plan stays distributed. The
//! coordinator and workers resolve a server key to a cached ADBC pool; the plan
//! payload carries only that key, SQL and schema, never credentials.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDriver;
use datafusion_table_providers_adbc::pool::ADBCPool;
use lakesoul_metadata::{MetaDataClientRef, QueryType};
use url::Url;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result as DFResult, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::unparser::dialect::{
    BigQueryDialect, DefaultDialect, Dialect, DuckDBDialect, MySqlDialect,
    PostgreSqlDialect, SnowflakeDialect, SqliteDialect,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::sql_provider_datafusion::{
    SqlExec, SqlTable,
};
use lakesoul_common::ser::arrow_java::{
    schema_from_metadata_str, schema_to_metadata_str,
};
use parking_lot::RwLock;
use r2d2_adbc::AdbcConnectionManager;
use serde::{Deserialize, Serialize};

type AdbcConnection =
    r2d2::PooledConnection<AdbcConnectionManager<adbc_driver_manager::ManagedDatabase>>;

/// A pool usable by an ADBC table provider. The concrete pool is
/// `ADBCPool<ManagedDatabase>`; the trait object is what `SqlTable` stores.
pub type FederationPool =
    Arc<dyn DbConnectionPool<AdbcConnection, RecordBatch> + Send + Sync>;

/// The SQL dialect used to turn a DataFusion scan into remote SQL.
///
/// `SqlTable` pushes projections, filters and limits down by unparsing them,
/// so the generated SQL must match the source database. The dialect is fixed
/// when a foreign server is created and stored with the server definition;
/// queries then use it without guessing from the driver name again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectKind {
    Postgres,
    MySql,
    Sqlite,
    DuckDb,
    BigQuery,
    Snowflake,
    Generic,
}

impl DialectKind {
    /// The dialect a known ADBC driver speaks. Unknown drivers must set the
    /// `dialect` server option explicitly instead of being guessed.
    pub fn from_driver(driver: &str) -> Option<Self> {
        match driver.trim().to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" => Some(Self::Postgres),
            "mysql" => Some(Self::MySql),
            "sqlite" => Some(Self::Sqlite),
            "duckdb" => Some(Self::DuckDb),
            "bigquery" => Some(Self::BigQuery),
            "snowflake" => Some(Self::Snowflake),
            "trino" => Some(Self::Generic),
            _ => None,
        }
    }

    /// Parse a `dialect` server option. Accepts aliases such as
    /// `postgresql` and `default`.
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Some(Self::Postgres),
            "mysql" => Some(Self::MySql),
            "sqlite" => Some(Self::Sqlite),
            "duckdb" => Some(Self::DuckDb),
            "bigquery" => Some(Self::BigQuery),
            "snowflake" => Some(Self::Snowflake),
            "generic" | "default" => Some(Self::Generic),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::Sqlite => "sqlite",
            Self::DuckDb => "duckdb",
            Self::BigQuery => "bigquery",
            Self::Snowflake => "snowflake",
            Self::Generic => "generic",
        }
    }

    pub fn supported_names() -> &'static str {
        "postgres, mysql, sqlite, duckdb, bigquery, snowflake, generic"
    }

    pub fn dialect(self) -> Arc<dyn Dialect + Send + Sync> {
        match self {
            Self::Postgres => Arc::new(PostgreSqlDialect {}),
            Self::MySql => Arc::new(MySqlDialect {}),
            Self::Sqlite => Arc::new(SqliteDialect {}),
            Self::DuckDb => Arc::new(DuckDBDialect::default()),
            Self::BigQuery => Arc::new(BigQueryDialect::default()),
            Self::Snowflake => Arc::new(SnowflakeDialect::new()),
            Self::Generic => Arc::new(DefaultDialect {}),
        }
    }
}

impl std::fmt::Display for DialectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

static POOLS: OnceLock<RwLock<BTreeMap<String, FederationPool>>> = OnceLock::new();

fn pools() -> &'static RwLock<BTreeMap<String, FederationPool>> {
    POOLS.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Register the pool a worker resolves when it decodes a federation plan.
/// The key is opaque to the plan; the endpoint uses `server:mapping-user`,
/// where the mapping user is the query user or `PUBLIC` for a shared mapping.
pub fn register_pool(server_key: &str, pool: FederationPool) {
    pools().write().insert(server_key.to_string(), pool);
}

pub fn clear_pools() {
    pools().write().clear();
}

fn lookup_pool(server_key: &str) -> Option<FederationPool> {
    pools().read().get(server_key).cloned()
}

/// A `SqlTable` that emits a wire-encodable `FederationExec` on scan.
#[derive(Debug)]
pub struct FederationTableProvider {
    server_key: String,
    table: SqlTable<AdbcConnection, RecordBatch>,
}

impl FederationTableProvider {
    pub fn new(server_key: String, table: SqlTable<AdbcConnection, RecordBatch>) -> Self {
        Self { server_key, table }
    }
}

#[async_trait]
impl TableProvider for FederationTableProvider {
    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        self.table.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.table.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan = self.table.scan(state, projection, filters, limit).await?;
        let inner = plan
            .as_ref()
            .downcast_ref::<SqlExec<AdbcConnection, RecordBatch>>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "ADBC table scan did not produce a SqlExec".to_string(),
                )
            })?
            .clone();
        Ok(Arc::new(FederationExec {
            server_key: self.server_key.clone(),
            inner,
        }))
    }
}

/// One-partition ADBC scan that can cross the distributed wire boundary.
#[derive(Clone)]
pub struct FederationExec {
    server_key: String,
    inner: SqlExec<AdbcConnection, RecordBatch>,
}

impl std::fmt::Debug for FederationExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederationExec")
            .field("server_key", &self.server_key)
            .field("sql", &self.inner.sql().ok())
            .finish()
    }
}

impl DisplayAs for FederationExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "FederationExec server={} sql={}",
            self.server_key,
            self.inner.sql().unwrap_or_default()
        )
    }
}

impl ExecutionPlan for FederationExec {
    fn name(&self) -> &str {
        "FederationExec"
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )
            -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "FederationExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        tracing::debug!(
            server = %self.server_key,
            partition,
            sql = %self.inner.sql().unwrap_or_default(),
            "executing federation scan"
        );
        self.inner.execute(partition, context)
    }
}

/// Wire representation of a federation scan. It deliberately contains no
/// credentials: a worker resolves `server_key` in its federation registry.
#[derive(Debug, Serialize, Deserialize)]
struct FederationPlan {
    server_key: String,
    sql: String,
    schema: String,
}

#[derive(Debug, Clone, Default)]
pub struct FederationPlanCodec;

impl PhysicalExtensionCodec for FederationPlanCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan: FederationPlan = serde_json::from_slice(buf).map_err(|error| {
            DataFusionError::Internal(format!("decode federation plan payload: {error}"))
        })?;
        let schema = schema_from_metadata_str(&plan.schema).map_err(|error| {
            DataFusionError::Internal(format!("decode federation plan schema: {error}"))
        })?;
        let pool = lookup_pool(&plan.server_key).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "federation server key {:?} is not registered on this worker",
                plan.server_key
            ))
        })?;
        // The SQL text was already generated with the source dialect by the
        // coordinator. A decoded stage only executes it, so no target-specific
        // unparsing happens here and the generic dialect is enough.
        let inner = SqlExec::new(
            None,
            &Arc::new(schema),
            pool,
            plan.sql,
            Arc::new(DefaultDialect {}),
        )?;
        Ok(Arc::new(FederationExec {
            server_key: plan.server_key,
            inner,
        }))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        let Some(exec) = node.downcast_ref::<FederationExec>() else {
            return Err(DataFusionError::NotImplemented(
                "FederationPlanCodec only encodes FederationExec".to_string(),
            ));
        };
        let plan = FederationPlan {
            server_key: exec.server_key.clone(),
            sql: exec.inner.sql().map_err(|error| {
                DataFusionError::Internal(format!("read federation SQL: {error}"))
            })?,
            schema: schema_to_metadata_str(exec.inner.schema().as_ref()),
        };
        serde_json::to_writer(buf, &plan).map_err(|error| {
            DataFusionError::Internal(format!("encode federation plan: {error}"))
        })
    }
}

/// A convenience constructor used by the endpoint's catalog.
pub fn table_provider(
    server_key: String,
    pool: FederationPool,
    dialect: DialectKind,
    schema: SchemaRef,
    remote_table: TableReference,
) -> Arc<dyn TableProvider> {
    let table = SqlTable::new_with_schema("federation", &pool, schema, remote_table)
        .with_dialect(dialect.dialect());
    Arc::new(FederationTableProvider::new(server_key, table))
}

/// Open an ADBC pool from a foreign-server definition.
///
/// The endpoint and every distributed worker use this one function, so a
/// server's driver, URI and driver-specific options are resolved identically
/// on both sides. `mapping_options` are the options of the user mapping that
/// applies to the query user; they override the server options.
///
/// Reserved options are removed before the rest are forwarded to the ADBC
/// driver: `driver` and `dialect` are server metadata, `uri` becomes
/// `OptionDatabase::Uri`, `host`/`port`/`dbname`/`user`/`password` build the
/// PostgreSQL URI, and user-mapping credentials are folded into an explicit
/// URI when it does not carry them. Other drivers should normally provide
/// `uri`; any remaining option is passed to the driver as a native ADBC
/// database option.
pub fn open_pool(
    server: &str,
    driver_name: &str,
    server_options: &BTreeMap<String, String>,
    mapping_options: Option<&BTreeMap<String, String>>,
) -> crate::Result<FederationPool> {
    if driver_name.contains('/') || driver_name.contains('\\') {
        return Err(rootcause::report!(
            "federation server {server:?} has an unsafe driver name \
             {driver_name:?}; configure the ADBC driver by name, not by path"
        ));
    }

    let mut options = server_options.clone();
    if let Some(mapping) = mapping_options {
        options.extend(mapping.clone());
    }

    let uri = options.remove("uri");
    let host = options.remove("host");
    let port = options.remove("port");
    let dbname = options.remove("dbname");
    let user = options.remove("user");
    let password = options.remove("password");
    // These are FDW metadata, not ADBC database options. Older rows may still
    // carry `driver` in the options JSON.
    options.remove("driver");
    options.remove("dialect");

    let mut database_options: Vec<(OptionDatabase, OptionValue)> = Vec::new();
    if let Some(uri) = uri {
        database_options.push((
            OptionDatabase::Uri,
            uri_with_credentials(&uri, user.as_deref(), password.as_deref()).into(),
        ));
    } else if is_postgres_driver(driver_name) {
        database_options.push((
            OptionDatabase::Uri,
            postgres_uri(
                server,
                host.as_deref(),
                port.as_deref(),
                dbname.as_deref(),
                user.as_deref(),
                password.as_deref(),
            )?
            .into(),
        ));
    } else {
        for (key, value) in [("host", host), ("port", port), ("dbname", dbname)] {
            if let Some(value) = value {
                database_options
                    .push((OptionDatabase::Other(key.to_string()), value.into()));
            }
        }
        // Drivers without a URI use their own option names. The ADBC
        // PostgreSQL driver, for example, rejects the canonical `username`
        // option and expects credentials in the URI.
        if let Some(user) = user {
            database_options
                .push((OptionDatabase::Other("user".to_string()), user.into()));
        }
        if let Some(password) = password {
            database_options.push((
                OptionDatabase::Other("password".to_string()),
                password.into(),
            ));
        }
    }
    for (key, value) in options {
        database_options.push((OptionDatabase::Other(key), value.into()));
    }

    let mut driver = ManagedDriver::load_from_name(
        driver_name,
        None,
        AdbcVersion::default(),
        LOAD_FLAG_DEFAULT,
        None,
    )
    .map_err(|error| {
        rootcause::report!(
            "load ADBC driver {driver_name} for federation server {server}: {error}"
        )
    })?;
    let database = driver
        .new_database_with_opts(database_options)
        .map_err(|error| {
            rootcause::report!(
                "open ADBC database for federation server {server}: {error}"
            )
        })?;
    let pool = ADBCPool::new(database, None).map_err(|error| {
        rootcause::report!(
            "create ADBC connection pool for federation server {server}: {error}"
        )
    })?;
    Ok(Arc::new(pool))
}

fn is_postgres_driver(driver: &str) -> bool {
    driver.eq_ignore_ascii_case("postgresql") || driver.eq_ignore_ascii_case("postgres")
}

/// Put user-mapping credentials into a hierarchical URI when the URI does not
/// carry them already. Driver URIs such as `postgresql://host/db` and
/// `mysql://host/db` use this shape; `file:` URIs (SQLite) have no credentials
/// and are returned unchanged.
fn uri_with_credentials(uri: &str, user: Option<&str>, password: Option<&str>) -> String {
    if user.is_none() && password.is_none() {
        return uri.to_string();
    }
    let Ok(mut url) = Url::parse(uri) else {
        return uri.to_string();
    };
    if url.scheme() == "file" {
        return uri.to_string();
    }
    if let Some(user) = user
        && url.username().is_empty()
    {
        let _ = url.set_username(user);
    }
    if let Some(password) = password
        && url.password().is_none()
        && !url.username().is_empty()
    {
        let _ = url.set_password(Some(password));
    }
    url.to_string()
}

fn postgres_uri(
    server: &str,
    host: Option<&str>,
    port: Option<&str>,
    dbname: Option<&str>,
    user: Option<&str>,
    password: Option<&str>,
) -> crate::Result<String> {
    let host = host.ok_or_else(|| {
        rootcause::report!("federation server {server} has no host option")
    })?;
    let dbname = dbname.ok_or_else(|| {
        rootcause::report!("federation server {server} has no dbname option")
    })?;
    let user = user.ok_or_else(|| {
        rootcause::report!("federation server {server} has no user option")
    })?;
    let port: u16 = port.unwrap_or("5432").parse().map_err(|_| {
        rootcause::report!("federation server {server} has an invalid port")
    })?;
    let mut url = Url::parse("postgresql://localhost")
        .map_err(|error| rootcause::report!("build federation URI: {error}"))?;
    url.set_host(Some(host)).map_err(|error| {
        rootcause::report!("federation server {server} host: {error}")
    })?;
    url.set_port(Some(port)).map_err(|_| {
        rootcause::report!("federation server {server} has an invalid port")
    })?;
    url.set_path(&format!("/{dbname}"));
    url.set_username(user)
        .map_err(|_| rootcause::report!("federation user mapping has an invalid user"))?;
    if let Some(password) = password {
        url.set_password(Some(password)).map_err(|_| {
            rootcause::report!("federation user mapping has an invalid password")
        })?;
    }
    Ok(url.to_string())
}

/// Register all user-mapping pools from the federation metadata.
///
/// The coordinator registers pools lazily while planning; a standalone
/// distributed worker has no catalog session, so it calls this at startup when
/// `LAKESOUL_PG_URL` is configured. Plans reference the same server and
/// user-mapping keys, including `PUBLIC` for a shared mapping.
pub async fn load_pools_from_metadata(meta: &MetaDataClientRef) -> crate::Result<()> {
    let pool = meta.pooled_client();
    let client = pool.lock().await.clone();
    let servers = client
        .query(
            "select server_name, driver, options from federation.servers",
            QueryType::RW,
            &[],
        )
        .await
        .map_err(|error| rootcause::report!("read federation servers: {error}"))?;
    let mappings = client
        .query(
            "select server_name, pg_user, options from federation.user_mappings",
            QueryType::RW,
            &[],
        )
        .await
        .map_err(|error| rootcause::report!("read federation user mappings: {error}"))?;
    let mut definitions = BTreeMap::new();
    for row in servers {
        let name: String = row.get(0);
        let driver: String = row.get(1);
        let options: String = row.get(2);
        let options: BTreeMap<String, String> =
            serde_json::from_str(&options).map_err(|error| {
                rootcause::report!(
                    "federation server {name} options are invalid JSON: {error}"
                )
            })?;
        definitions.insert(name, (driver, options));
    }

    clear_pools();
    for row in mappings {
        let server: String = row.get(0);
        let user: String = row.get(1);
        let mapping_options: String = row.get(2);
        let mapping_options: BTreeMap<String, String> =
            serde_json::from_str(&mapping_options).map_err(|error| {
                rootcause::report!(
                    "federation mapping {server}:{user} options are invalid JSON: \
                     {error}"
                )
            })?;
        let Some((driver, options)) = definitions.get(&server) else {
            continue;
        };
        let pool = open_pool(&server, driver, options, Some(&mapping_options))?;
        register_pool(&format!("{server}:{user}"), pool);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;
    use datafusion_table_providers_common::sql::db_connection_pool::JoinPushDown;
    use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::DbConnection;

    use super::*;

    const SQL: &str = "SELECT id, name FROM public.remote_users";

    /// A pool that exists only so a `SqlExec` can be built: the codec never
    /// opens a connection.
    struct TestPool;

    #[async_trait]
    impl DbConnectionPool<AdbcConnection, RecordBatch> for TestPool {
        async fn connect(
            &self,
        ) -> std::result::Result<
            Box<dyn DbConnection<AdbcConnection, RecordBatch>>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            unreachable!("the codec tests never open a connection")
        }

        fn join_push_down(&self) -> JoinPushDown {
            JoinPushDown::Disallow
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn federation_exec(server_key: &str) -> (Arc<dyn ExecutionPlan>, FederationPool) {
        let pool: FederationPool = Arc::new(TestPool);
        let inner = SqlExec::new(
            None,
            &schema(),
            Arc::clone(&pool),
            SQL.to_string(),
            Arc::new(DefaultDialect {}),
        )
        .expect("build SqlExec");
        let exec: Arc<dyn ExecutionPlan> = Arc::new(FederationExec {
            server_key: server_key.to_string(),
            inner,
        });
        (exec, pool)
    }

    #[test]
    fn codec_roundtrips_a_federation_scan() {
        let key = "codec-roundtrip:reader";
        let (exec, pool) = federation_exec(key);
        register_pool(key, pool);

        let codec = FederationPlanCodec;
        let mut buf = Vec::new();
        codec
            .try_encode(
                Arc::clone(&exec),
                &mut buf,
                &DefaultPhysicalProtoConverter {},
            )
            .expect("encode");
        let decoded = codec
            .try_decode(
                &buf,
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .expect("decode");

        let decoded = decoded
            .downcast_ref::<FederationExec>()
            .expect("decoded node is a FederationExec");
        assert_eq!(decoded.server_key, key);
        assert_eq!(decoded.inner.sql().expect("sql"), SQL);
        assert_eq!(decoded.schema(), schema());
    }

    #[test]
    fn codec_reports_an_unregistered_server_key() {
        let plan = FederationPlan {
            server_key: "unregistered:reader".to_string(),
            sql: "SELECT 1".to_string(),
            schema: schema_to_metadata_str(&schema()),
        };
        let buf = serde_json::to_vec(&plan).expect("serialize plan");
        let error = FederationPlanCodec
            .try_decode(
                &buf,
                &[],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .expect_err("decode must fail without a registered pool");
        assert!(matches!(error, DataFusionError::Execution(_)));
        assert!(error.to_string().contains("not registered"));
    }

    #[test]
    fn codec_refuses_a_node_it_does_not_own() {
        let other: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        let mut buf = Vec::new();
        let error = FederationPlanCodec
            .try_encode(other, &mut buf, &DefaultPhysicalProtoConverter {})
            .expect_err("encode must refuse an unknown node");
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
    }

    #[test]
    fn dialect_kind_maps_known_drivers() {
        assert_eq!(
            DialectKind::from_driver("postgresql"),
            Some(DialectKind::Postgres)
        );
        assert_eq!(
            DialectKind::from_driver("POSTGRES"),
            Some(DialectKind::Postgres)
        );
        assert_eq!(DialectKind::from_driver("mysql"), Some(DialectKind::MySql));
        assert_eq!(
            DialectKind::from_driver("sqlite"),
            Some(DialectKind::Sqlite)
        );
        assert_eq!(
            DialectKind::from_driver("duckdb"),
            Some(DialectKind::DuckDb)
        );
        assert_eq!(
            DialectKind::from_driver("bigquery"),
            Some(DialectKind::BigQuery)
        );
        assert_eq!(
            DialectKind::from_driver("snowflake"),
            Some(DialectKind::Snowflake)
        );
        assert_eq!(
            DialectKind::from_driver("trino"),
            Some(DialectKind::Generic)
        );
        assert_eq!(DialectKind::from_driver("unknown"), None);
    }

    #[test]
    fn dialect_kind_parses_names_and_aliases() {
        assert_eq!(
            DialectKind::parse("postgresql"),
            Some(DialectKind::Postgres)
        );
        assert_eq!(DialectKind::parse("pg"), Some(DialectKind::Postgres));
        assert_eq!(DialectKind::parse("mysql"), Some(DialectKind::MySql));
        assert_eq!(DialectKind::parse("default"), Some(DialectKind::Generic));
        assert_eq!(DialectKind::parse("sqlserver"), None);
        for kind in [
            DialectKind::Postgres,
            DialectKind::MySql,
            DialectKind::Sqlite,
            DialectKind::DuckDb,
            DialectKind::BigQuery,
            DialectKind::Snowflake,
            DialectKind::Generic,
        ] {
            assert_eq!(DialectKind::parse(kind.name()), Some(kind));
        }
    }

    #[test]
    fn uri_credentials_are_added_only_when_missing() {
        assert_eq!(
            uri_with_credentials(
                "mysql://db.example/app",
                Some("reader"),
                Some("secret")
            ),
            "mysql://reader:secret@db.example/app"
        );
        assert_eq!(
            uri_with_credentials(
                "postgresql://existing:old@db.example/app",
                Some("reader"),
                Some("secret")
            ),
            "postgresql://existing:old@db.example/app"
        );
        assert_eq!(
            uri_with_credentials("file:/tmp/source.db", Some("reader"), Some("secret")),
            "file:/tmp/source.db"
        );
    }

    #[test]
    fn postgres_uri_encodes_mapping_credentials() {
        let uri = postgres_uri(
            "fed_pg",
            Some("db.example"),
            Some("5433"),
            Some("app"),
            Some("reader"),
            Some("secret"),
        )
        .expect("build URI");
        assert_eq!(uri, "postgresql://reader:secret@db.example:5433/app");
    }
}
