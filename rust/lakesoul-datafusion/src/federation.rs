// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Distributed execution of ADBC and external-file foreign-table scans.
//!
//! An ADBC foreign scan is intentionally one partition; an external-file scan is
//! a self-contained distributed leaf carrying its object-store configuration. A plan whose stages are all
//! single-task runs on the coordinator; when the scan borders a multi-task
//! stage (a join or aggregation over a distributed LakeSoul scan), its stage is
//! assigned to one worker while the rest of the plan stays distributed. The
//! coordinator and workers resolve a server key to a cached ADBC pool; the plan
//! payload carries only that key, SQL and schema, never credentials.

pub mod external_file;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Driver, LOAD_FLAG_DEFAULT, Statement};
use adbc_driver_manager::ManagedDriver;
use datafusion_distributed::DistributedGetterExt;
use datafusion_table_providers_adbc::ADBC;
use datafusion_table_providers_adbc::pool::ADBCPool;
use lakesoul_metadata::{MetaDataClientRef, QueryType};
use url::Url;

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Result as DFResult, ScalarValue, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Distribution, Partitioning, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{
    BigQueryDialect, DefaultDialect, Dialect, DuckDBDialect, MySqlDialect,
    PostgreSqlDialect, SnowflakeDialect, SqliteDialect,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::sql_provider_datafusion::{
    SqlExec, SqlTable,
};
use futures::StreamExt;
use lakesoul_common::ser::arrow_java::{
    schema_from_metadata_str, schema_to_metadata_str,
};
use parking_lot::RwLock;
use r2d2_adbc::AdbcConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

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

/// What an inserted row does when the target already has the same key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FederationConflictMode {
    /// Plain append. Retrying the same rows inserts them again.
    Append,
    /// `ON CONFLICT DO NOTHING`: retries keep the first version.
    Nothing,
    /// `ON CONFLICT DO UPDATE`: retries overwrite with the latest version.
    Update,
}

impl FederationConflictMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "append" | "none" | "off" => Some(Self::Append),
            "nothing" | "ignore" | "do_nothing" => Some(Self::Nothing),
            "update" | "upsert" | "do_update" => Some(Self::Update),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Nothing => "nothing",
            Self::Update => "update",
        }
    }
}

/// Write options read from `CREATE FOREIGN TABLE ... OPTIONS (...)`.
///
/// `primary_key` (or `conflict_columns`) selects the conflict target;
/// `on_conflict` is `append`, `nothing` or `update`; `write_batch_size`
/// bounds one generated INSERT statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederationWriteConfig {
    pub conflict: FederationConflictMode,
    pub primary_key: Vec<String>,
    pub batch_size: usize,
}

impl Default for FederationWriteConfig {
    fn default() -> Self {
        Self {
            conflict: FederationConflictMode::Append,
            primary_key: Vec::new(),
            batch_size: 1024,
        }
    }
}

impl FederationWriteConfig {
    pub fn from_options(options: &BTreeMap<String, String>) -> DFResult<Self> {
        let conflict = match options.get("on_conflict") {
            Some(value) => FederationConflictMode::parse(value).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "unknown on_conflict value {value:?}; use append, nothing or update"
                ))
            })?,
            None => FederationConflictMode::Append,
        };
        let primary_key = options
            .get("primary_key")
            .or_else(|| options.get("conflict_columns"))
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|column| !column.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let batch_size = match options.get("write_batch_size") {
            Some(value) => value.parse::<usize>().map_err(|_| {
                DataFusionError::Plan(format!(
                    "invalid write_batch_size {value:?}; expected a positive integer"
                ))
            })?,
            None => 1024,
        };
        if batch_size == 0 {
            return Err(DataFusionError::Plan(
                "write_batch_size must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            conflict,
            primary_key,
            batch_size,
        })
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

/// A `SqlTable` that emits a wire-encodable `FederationExec` on scan and
/// writes through a worker-side `FederationWriteExec` on insert.
#[derive(Debug)]
pub struct FederationTableProvider {
    server_key: String,
    table: SqlTable<AdbcConnection, RecordBatch>,
    remote_table: TableReference,
    dialect: DialectKind,
    options: BTreeMap<String, String>,
}

impl FederationTableProvider {
    pub fn new(
        server_key: String,
        table: SqlTable<AdbcConnection, RecordBatch>,
        remote_table: TableReference,
        dialect: DialectKind,
        options: BTreeMap<String, String>,
    ) -> Self {
        Self {
            server_key,
            table,
            remote_table,
            dialect,
            options,
        }
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

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(
                "foreign tables only support INSERT (append); \
                 INSERT OVERWRITE and upsert are not implemented"
                    .to_string(),
            ));
        }
        let config = FederationWriteConfig::from_options(&self.options)?;
        if config.conflict != FederationConflictMode::Append {
            if config.primary_key.is_empty() {
                return Err(DataFusionError::Plan(
                    "idempotent foreign writes require OPTIONS (primary_key '...') \
                     on the foreign table"
                        .to_string(),
                ));
            }
            let fields = self.table.schema();
            for column in &config.primary_key {
                if fields.field_with_name(column).is_err() {
                    return Err(DataFusionError::Plan(format!(
                        "foreign table conflict column {column:?} is not in the schema"
                    )));
                }
            }
        }

        // A write stage with N tasks is the only way datafusion-distributed
        // keeps it on workers: a one-task producer/consumer boundary is
        // elided and would run the write on the coordinator. Use one task per
        // registered worker. A single-worker deployment therefore runs the
        // write locally, matching the coordinator fallback.
        let workers = state
            .config()
            .get_distributed_worker_resolver()
            .ok()
            .and_then(|resolver| resolver.get_urls().ok())
            .map(|urls| urls.len())
            .unwrap_or(0);
        let max_tasks = datafusion_distributed::DistributedConfig::from_session_config(
            state.config(),
        )
        .map(|config| config.max_tasks_per_stage)
        .unwrap_or(0);
        // The session factory raises max_tasks_per_stage for foreign-write
        // sessions; this is also how one worker can own two write tasks.
        let parallelism = if workers == 0 {
            1
        } else if max_tasks > 0 {
            max_tasks
        } else {
            workers
        };
        let first_field =
            self.table
                .schema()
                .fields()
                .first()
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "cannot write a foreign table without columns".to_string(),
                    )
                })?;
        let key_expr: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new(first_field.name(), 0));
        // A hash repartition is a stage boundary in datafusion-distributed.
        // The writer is its consumer, so the stage runs on workers while the
        // coalesce above it returns the row count to the coordinator. A
        // round-robin repartition is local and would be optimized away.
        let input = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![key_expr], parallelism),
        )?);
        let write = Arc::new(FederationWriteExec::new(
            input,
            self.server_key.clone(),
            self.table.clone_pool(),
            self.remote_table.clone(),
            self.table.schema(),
            self.dialect,
            config,
            parallelism,
        )?);
        Ok(Arc::new(CoalescePartitionsExec::new(write)))
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

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
    RecordBatch::try_new(
        count_schema(),
        vec![Arc::new(UInt64Array::from(vec![count]))],
    )
    .map_err(|error| {
        DataFusionError::Execution(format!("build write count batch: {error}"))
    })
}

/// Worker-side half of an idempotent foreign INSERT.
///
/// Every task consumes one input partition, batches the rows into SQL
/// statements and executes them against the target through ADBC. Rows are
/// committed per statement, so a failed task can leave previously written
/// batches in the target. With `ON CONFLICT` the whole statement is safe to
/// retry: already-written keys are ignored or updated instead of duplicated.
#[derive(Clone)]
pub struct FederationWriteExec {
    input: Arc<dyn ExecutionPlan>,
    server_key: String,
    pool: FederationPool,
    table: TableReference,
    target_schema: SchemaRef,
    dialect: DialectKind,
    config: FederationWriteConfig,
    parallelism: usize,
    count_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FederationWriteExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        server_key: String,
        pool: FederationPool,
        table: TableReference,
        target_schema: SchemaRef,
        dialect: DialectKind,
        config: FederationWriteConfig,
        parallelism: usize,
    ) -> DFResult<Self> {
        if parallelism == 0 {
            return Err(DataFusionError::Plan(
                "foreign write parallelism must be greater than zero".to_string(),
            ));
        }
        let count_schema = count_schema();
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(
                &count_schema,
            )),
            Partitioning::UnknownPartitioning(parallelism),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            server_key,
            pool,
            table,
            target_schema,
            dialect,
            config,
            parallelism,
            count_schema,
            properties,
        })
    }

    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    pub fn config(&self) -> &FederationWriteConfig {
        &self.config
    }
}

impl std::fmt::Debug for FederationWriteExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederationWriteExec")
            .field("server_key", &self.server_key)
            .field("table", &self.table.to_string())
            .field("parallelism", &self.parallelism)
            .field("conflict", &self.config.conflict)
            .finish()
    }
}

impl DisplayAs for FederationWriteExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "FederationWriteExec server={} table={} tasks={} conflict={}",
            self.server_key,
            self.table,
            self.parallelism,
            self.config.conflict.name()
        )
    }
}

impl ExecutionPlan for FederationWriteExec {
    fn name(&self) -> &str {
        "FederationWriteExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.count_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let Some(field) = self.target_schema.fields().first() else {
            return vec![Distribution::UnspecifiedDistribution];
        };
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), 0));
        vec![Distribution::KeyPartitioned(vec![expr])]
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "FederationWriteExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.server_key.clone(),
            Arc::clone(&self.pool),
            self.table.clone(),
            Arc::clone(&self.target_schema),
            self.dialect,
            self.config.clone(),
            self.parallelism,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.parallelism {
            return Err(DataFusionError::Internal(format!(
                "FederationWriteExec partition {partition} is outside {} tasks",
                self.parallelism
            )));
        }
        tracing::debug!(
            partition,
            parallelism = self.parallelism,
            "executing federation write task"
        );
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let pool = Arc::clone(&self.pool);
        let table = self.table.clone();
        let schema = Arc::clone(&self.target_schema);
        let dialect = self.dialect;
        let config = self.config.clone();
        let count_schema = Arc::clone(&self.count_schema);
        let stream = futures::stream::once(async move {
            let count = write_stream(pool, table, schema, dialect, config, input).await?;
            make_count_batch(count)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }
}

async fn write_stream(
    pool: FederationPool,
    table: TableReference,
    schema: SchemaRef,
    dialect: DialectKind,
    config: FederationWriteConfig,
    mut data: SendableRecordBatchStream,
) -> DFResult<u64> {
    let conn = pool.connect().await.map_err(|error| {
        DataFusionError::Execution(format!(
            "connect to federation target {}: {error}",
            table
        ))
    })?;
    let (batch_tx, batch_rx) = mpsc::channel::<RecordBatch>(8);
    let writer = tokio::task::spawn_blocking(move || {
        write_batches(conn, batch_rx, table, schema, dialect, config)
    });

    let mut input_error = None;
    while let Some(batch) = data.next().await {
        match batch {
            Ok(batch) => {
                if batch_tx.send(batch).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                input_error = Some(error);
                break;
            }
        }
    }
    drop(batch_tx);

    let write_result = match writer.await {
        Ok(result) => result,
        Err(error) => Err(DataFusionError::Execution(format!(
            "foreign write task join error: {error}"
        ))),
    };
    match input_error {
        Some(error) => Err(error),
        None => write_result,
    }
}

fn write_batches(
    mut conn: Box<dyn datafusion_table_providers_common::sql::db_connection_pool::dbconnection::DbConnection<AdbcConnection, RecordBatch>>,
    mut batch_rx: mpsc::Receiver<RecordBatch>,
    table: TableReference,
    schema: SchemaRef,
    dialect: DialectKind,
    config: FederationWriteConfig,
) -> DFResult<u64> {
    let adbc_conn = ADBC::<adbc_driver_manager::ManagedDatabase>::adbc_conn(&mut conn)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "downcast federation write connection: {error}"
            ))
        })?;
    let conn_mx = adbc_conn.conn.lock().map_err(|error| {
        DataFusionError::Execution(format!("lock federation write connection: {error}"))
    })?;
    let mut conn = conn_mx.borrow_mut();
    let mut total = 0u64;
    while let Some(batch) = batch_rx.blocking_recv() {
        let mut offset = 0usize;
        while offset < batch.num_rows() {
            let length = (batch.num_rows() - offset).min(config.batch_size);
            let slice = batch.slice(offset, length);
            let sql = build_insert_sql(&table, &schema, dialect, &config, &slice)?;
            tracing::debug!(
                table = %table,
                rows = length,
                "executing federation insert"
            );
            let mut statement = conn.new_statement().map_err(|error| {
                DataFusionError::Execution(format!(
                    "create ADBC insert statement for {table}: {error}"
                ))
            })?;
            statement.set_sql_query(&sql).map_err(|error| {
                DataFusionError::Execution(format!(
                    "set federation insert SQL for {table}: {error}"
                ))
            })?;
            let count = statement.execute_update().map_err(|error| {
                DataFusionError::Execution(format!(
                    "execute federation insert into {table}: {error}"
                ))
            })?;
            total = total.saturating_add(
                count
                    .and_then(|count| u64::try_from(count).ok())
                    .unwrap_or(0),
            );
            offset += length;
        }
    }
    Ok(total)
}

fn quote_ident(dialect: &dyn Dialect, ident: &str) -> String {
    match dialect.identifier_quote_style(ident) {
        Some(quote) => format!(
            "{quote}{}{quote}",
            ident.replace(quote, &format!("{quote}{quote}"))
        ),
        None => ident.to_string(),
    }
}

fn quote_table(dialect: &dyn Dialect, table: &TableReference) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = table.catalog() {
        parts.push(quote_ident(dialect, catalog));
    }
    if let Some(schema) = table.schema() {
        parts.push(quote_ident(dialect, schema));
    }
    parts.push(quote_ident(dialect, table.table()));
    parts.join(".")
}

fn build_insert_sql(
    table: &TableReference,
    schema: &SchemaRef,
    dialect: DialectKind,
    config: &FederationWriteConfig,
    batch: &RecordBatch,
) -> DFResult<String> {
    if batch.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "foreign write batch has {} columns but the foreign table has {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let dialect_impl = dialect.dialect();
    let unparser = Unparser::new(dialect_impl.as_ref());
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| quote_ident(dialect_impl.as_ref(), field.name()))
        .collect();

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(columns.len());
        for column in 0..columns.len() {
            let value = ScalarValue::try_from_array(batch.column(column), row).map_err(
                |error| {
                    DataFusionError::Execution(format!(
                        "read foreign write value at row {row}, column {column}: {error}"
                    ))
                },
            )?;
            let sql = unparser
                .expr_to_sql(&Expr::Literal(value, None))
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "format foreign write value at row {row}, column {column}: {error}"
                    ))
                })?;
            values.push(sql.to_string());
        }
        rows.push(format!("({})", values.join(", ")));
    }

    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_table(dialect_impl.as_ref(), table),
        columns.join(", "),
        rows.join(", ")
    );
    sql.push_str(&conflict_clause(dialect, config, schema)?);
    Ok(sql)
}

fn conflict_clause(
    dialect: DialectKind,
    config: &FederationWriteConfig,
    schema: &SchemaRef,
) -> DFResult<String> {
    if config.conflict == FederationConflictMode::Append {
        return Ok(String::new());
    }
    if config.primary_key.is_empty() {
        return Err(DataFusionError::Plan(
            "idempotent foreign writes require a primary_key option".to_string(),
        ));
    }
    let dialect_impl = dialect.dialect();
    let keys: Vec<String> = config
        .primary_key
        .iter()
        .map(|column| {
            schema
                .field_with_name(column)
                .map(|_| quote_ident(dialect_impl.as_ref(), column))
                .map_err(|_| {
                    DataFusionError::Plan(format!(
                        "foreign table conflict column {column:?} is not in the schema"
                    ))
                })
        })
        .collect::<DFResult<Vec<_>>>()?;

    match dialect {
        DialectKind::Postgres | DialectKind::Sqlite | DialectKind::DuckDb => {
            match config.conflict {
                FederationConflictMode::Append => Ok(String::new()),
                FederationConflictMode::Nothing => {
                    Ok(format!(" ON CONFLICT ({}) DO NOTHING", keys.join(", ")))
                }
                FederationConflictMode::Update => {
                    let updates: Vec<String> = schema
                        .fields()
                        .iter()
                        .filter(|field| {
                            !config.primary_key.iter().any(|key| key == field.name())
                        })
                        .map(|field| {
                            let column = quote_ident(dialect_impl.as_ref(), field.name());
                            format!("{column} = EXCLUDED.{column}")
                        })
                        .collect();
                    if updates.is_empty() {
                        Ok(format!(" ON CONFLICT ({}) DO NOTHING", keys.join(", ")))
                    } else {
                        Ok(format!(
                            " ON CONFLICT ({}) DO UPDATE SET {}",
                            keys.join(", "),
                            updates.join(", ")
                        ))
                    }
                }
            }
        }
        DialectKind::MySql => match config.conflict {
            FederationConflictMode::Append => Ok(String::new()),
            FederationConflictMode::Nothing => {
                let key = &keys[0];
                Ok(format!(" ON DUPLICATE KEY UPDATE {key} = {key}"))
            }
            FederationConflictMode::Update => {
                let updates: Vec<String> = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        let column = quote_ident(dialect_impl.as_ref(), field.name());
                        format!("{column} = VALUES({column})")
                    })
                    .collect();
                if updates.is_empty() {
                    let key = &keys[0];
                    Ok(format!(" ON DUPLICATE KEY UPDATE {key} = {key}"))
                } else {
                    Ok(format!(" ON DUPLICATE KEY UPDATE {}", updates.join(", ")))
                }
            }
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "idempotent foreign writes are not implemented for dialect {}",
            dialect.name()
        ))),
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

/// Wire representation of a federation write stage. It carries the server
/// key, target table, Arrow schema, dialect and conflict policy, never
/// credentials. `FederationWriteExec` resolves the pool on the worker.
#[derive(Debug, Serialize, Deserialize)]
struct FederationWritePlan {
    server_key: String,
    table: String,
    schema: String,
    dialect: String,
    conflict: String,
    primary_key: Vec<String>,
    batch_size: usize,
    parallelism: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum FederationWirePlan {
    Scan(FederationPlan),
    Write(FederationWritePlan),
}

#[derive(Debug, Clone, Default)]
pub struct FederationPlanCodec;

impl PhysicalExtensionCodec for FederationPlanCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan: FederationWirePlan = serde_json::from_slice(buf).map_err(|error| {
            DataFusionError::Internal(format!("decode federation plan payload: {error}"))
        })?;
        match plan {
            FederationWirePlan::Scan(plan) => {
                let schema = schema_from_metadata_str(&plan.schema).map_err(|error| {
                    DataFusionError::Internal(format!(
                        "decode federation plan schema: {error}"
                    ))
                })?;
                let pool = lookup_pool(&plan.server_key).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "federation server key {:?} is not registered on this worker",
                        plan.server_key
                    ))
                })?;
                // The SQL text was already generated with the source dialect
                // by the coordinator. A decoded stage only executes it, so no
                // target-specific unparsing happens here.
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
            FederationWirePlan::Write(plan) => {
                let schema = schema_from_metadata_str(&plan.schema).map_err(|error| {
                    DataFusionError::Internal(format!(
                        "decode federation write schema: {error}"
                    ))
                })?;
                let pool = lookup_pool(&plan.server_key).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "federation server key {:?} is not registered on this worker",
                        plan.server_key
                    ))
                })?;
                let dialect = DialectKind::parse(&plan.dialect).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "decode federation write dialect {:?}",
                        plan.dialect
                    ))
                })?;
                let conflict =
                    FederationConflictMode::parse(&plan.conflict).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "decode federation conflict mode {:?}",
                            plan.conflict
                        ))
                    })?;
                let input = inputs.first().ok_or_else(|| {
                    DataFusionError::Internal(
                        "federation write stage has no input plan".to_string(),
                    )
                })?;
                let config = FederationWriteConfig {
                    conflict,
                    primary_key: plan.primary_key,
                    batch_size: plan.batch_size,
                };
                Ok(Arc::new(FederationWriteExec::new(
                    Arc::clone(input),
                    plan.server_key,
                    pool,
                    TableReference::parse_str(&plan.table),
                    Arc::new(schema),
                    dialect,
                    config,
                    plan.parallelism,
                )?))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        if let Some(exec) = node.downcast_ref::<FederationExec>() {
            let plan = FederationPlan {
                server_key: exec.server_key.clone(),
                sql: exec.inner.sql().map_err(|error| {
                    DataFusionError::Internal(format!("read federation SQL: {error}"))
                })?,
                schema: schema_to_metadata_str(exec.inner.schema().as_ref()),
            };
            return serde_json::to_writer(buf, &plan).map_err(|error| {
                DataFusionError::Internal(format!("encode federation plan: {error}"))
            });
        }
        if let Some(exec) = node.downcast_ref::<FederationWriteExec>() {
            let plan = FederationWritePlan {
                server_key: exec.server_key.clone(),
                table: exec.table.to_string(),
                schema: schema_to_metadata_str(exec.target_schema.as_ref()),
                dialect: exec.dialect.name().to_string(),
                conflict: exec.config.conflict.name().to_string(),
                primary_key: exec.config.primary_key.clone(),
                batch_size: exec.config.batch_size,
                parallelism: exec.parallelism,
            };
            return serde_json::to_writer(buf, &plan).map_err(|error| {
                DataFusionError::Internal(format!(
                    "encode federation write plan: {error}"
                ))
            });
        }
        Err(DataFusionError::NotImplemented(
            "FederationPlanCodec only encodes FederationExec and FederationWriteExec"
                .to_string(),
        ))
    }
}

/// A convenience constructor used by the endpoint's catalog.
pub fn table_provider(
    server_key: String,
    pool: FederationPool,
    dialect: DialectKind,
    schema: SchemaRef,
    remote_table: TableReference,
    options: BTreeMap<String, String>,
) -> Arc<dyn TableProvider> {
    let table =
        SqlTable::new_with_schema("federation", &pool, schema, remote_table.clone())
            .with_dialect(dialect.dialect());
    Arc::new(FederationTableProvider::new(
        server_key,
        table,
        remote_table,
        dialect,
        options,
    ))
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
            "select server_name, backend, driver, options from federation.servers",
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
        let backend: String = row.get(1);
        let driver: String = row.get(2);
        let options: String = row.get(3);
        if !backend.eq_ignore_ascii_case("adbc") {
            // External-file servers carry their store configuration in each
            // scan plan; they do not have an ADBC pool to load.
            continue;
        }
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

#[cfg(test)]
mod write_tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_proto::physical_plan::DefaultPhysicalProtoConverter;
    use datafusion_table_providers_common::sql::db_connection_pool::JoinPushDown;
    use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::DbConnection;

    struct TestPool;

    #[async_trait]
    impl DbConnectionPool<AdbcConnection, RecordBatch> for TestPool {
        async fn connect(
            &self,
        ) -> std::result::Result<
            Box<dyn DbConnection<AdbcConnection, RecordBatch>>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            unreachable!("the write codec test never opens a connection")
        }

        fn join_push_down(&self) -> JoinPushDown {
            JoinPushDown::Disallow
        }
    }

    fn write_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn write_batch() -> RecordBatch {
        RecordBatch::try_new(
            write_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("alice")])),
            ],
        )
        .expect("batch")
    }

    fn write_config(conflict: FederationConflictMode) -> FederationWriteConfig {
        FederationWriteConfig {
            conflict,
            primary_key: vec!["id".to_string()],
            batch_size: 1024,
        }
    }

    #[test]
    fn write_config_parses_options() {
        let mut options = BTreeMap::new();
        options.insert("on_conflict".to_string(), "update".to_string());
        options.insert("primary_key".to_string(), "id, tenant".to_string());
        options.insert("write_batch_size".to_string(), "128".to_string());
        let config = FederationWriteConfig::from_options(&options).expect("config");
        assert_eq!(config.conflict, FederationConflictMode::Update);
        assert_eq!(config.primary_key, vec!["id", "tenant"]);
        assert_eq!(config.batch_size, 128);

        let mut bad = BTreeMap::new();
        bad.insert("on_conflict".to_string(), "bogus".to_string());
        assert!(FederationWriteConfig::from_options(&bad).is_err());
    }

    #[test]
    fn builds_append_sql_without_a_conflict_clause() {
        let sql = build_insert_sql(
            &TableReference::parse_str("public.remote_users"),
            &write_schema(),
            DialectKind::Postgres,
            &write_config(FederationConflictMode::Append),
            &write_batch(),
        )
        .expect("sql");
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"remote_users\" (\"id\", \"name\") VALUES (1, 'alice')"
        );
    }

    #[test]
    fn builds_postgres_upsert_sql() {
        let sql = build_insert_sql(
            &TableReference::parse_str("public.remote_users"),
            &write_schema(),
            DialectKind::Postgres,
            &write_config(FederationConflictMode::Update),
            &write_batch(),
        )
        .expect("sql");
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"remote_users\" (\"id\", \"name\") VALUES (1, 'alice') ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
        );
    }

    #[test]
    fn builds_mysql_upsert_sql() {
        let sql = build_insert_sql(
            &TableReference::parse_str("public.remote_users"),
            &write_schema(),
            DialectKind::MySql,
            &write_config(FederationConflictMode::Update),
            &write_batch(),
        )
        .expect("sql");
        assert_eq!(
            sql,
            "INSERT INTO `public`.`remote_users` (`id`, `name`) VALUES (1, 'alice') ON DUPLICATE KEY UPDATE `id` = VALUES(`id`), `name` = VALUES(`name`)"
        );
    }

    #[test]
    fn builds_sqlite_ignore_sql() {
        let sql = build_insert_sql(
            &TableReference::parse_str("public.remote_users"),
            &write_schema(),
            DialectKind::Sqlite,
            &write_config(FederationConflictMode::Nothing),
            &write_batch(),
        )
        .expect("sql");
        assert_eq!(
            sql,
            "INSERT INTO `public`.`remote_users` (`id`, `name`) VALUES (1, 'alice') ON CONFLICT (`id`) DO NOTHING"
        );
    }

    #[test]
    fn write_codec_roundtrips() {
        let key = "write-codec:reader";
        let pool: FederationPool = Arc::new(TestPool);
        register_pool(key, Arc::clone(&pool));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(write_schema()));
        let write: Arc<dyn ExecutionPlan> = Arc::new(
            FederationWriteExec::new(
                Arc::clone(&input),
                key.to_string(),
                pool,
                TableReference::parse_str("public.remote_users"),
                write_schema(),
                DialectKind::Postgres,
                write_config(FederationConflictMode::Update),
                2,
            )
            .expect("write exec"),
        );
        let codec = FederationPlanCodec;
        let mut buf = Vec::new();
        codec
            .try_encode(
                Arc::clone(&write),
                &mut buf,
                &DefaultPhysicalProtoConverter {},
            )
            .expect("encode");
        let decoded = codec
            .try_decode(
                &buf,
                &[input],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .expect("decode");
        let decoded = decoded
            .downcast_ref::<FederationWriteExec>()
            .expect("write exec");
        assert_eq!(decoded.parallelism(), 2);
        assert_eq!(decoded.config().conflict, FederationConflictMode::Update);
        assert_eq!(decoded.config().primary_key, vec!["id"]);
    }
}
