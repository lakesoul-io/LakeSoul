// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Table functions for detached-query status and results.
//!
//! `lakesoul_query_status`, `lakesoul_queries` and `lakesoul_query_result` are
//! ordinary DataFusion table functions. The status providers read the
//! in-memory job view at `scan` time, so a prepared statement can be executed
//! repeatedly and still see the latest state; the result provider captures the
//! finished job's schema and scans its Parquet spool with an offset/limit.

use std::fmt;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use uuid::Uuid;

use crate::jobs::{JobService, JobSnapshot, spool_scan};
use crate::session::PgIdentityConfig;

/// The columns every status row carries.
pub fn status_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("job_key", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("attempt", DataType::Int64, false),
        Field::new("result_sets", DataType::Int64, false),
        Field::new("sql", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "started_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "finished_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("rows_produced", DataType::Int64, false),
        Field::new("bytes_produced", DataType::Int64, false),
        Field::new("command_tag", DataType::Utf8, true),
        Field::new("error_sqlstate", DataType::Utf8, true),
        Field::new("error_message", DataType::Utf8, true),
        Field::new("result_kind", DataType::Utf8, true),
        Field::new("result_rows", DataType::Int64, true),
        Field::new("result_bytes", DataType::Int64, true),
        Field::new(
            "result_expires_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]))
}

fn status_batch(snapshots: &[JobSnapshot]) -> DFResult<RecordBatch> {
    let query_ids =
        StringArray::from_iter(snapshots.iter().map(|job| Some(job.id.to_string())));
    let job_keys =
        StringArray::from_iter(snapshots.iter().map(|job| job.job_key.clone()));
    let users =
        StringArray::from_iter(snapshots.iter().map(|job| Some(job.user.clone())));
    let databases =
        StringArray::from_iter(snapshots.iter().map(|job| Some(job.database.clone())));
    let states = StringArray::from_iter(
        snapshots
            .iter()
            .map(|job| Some(job.state.as_str().to_string())),
    );
    let attempts =
        Int64Array::from_iter(snapshots.iter().map(|job| Some(job.attempt as i64)));
    let result_set_counts = Int64Array::from_iter(
        snapshots
            .iter()
            .map(|job| Some(job.result_set_count as i64)),
    );
    let sql = StringArray::from_iter(
        snapshots.iter().map(|job| Some(truncate(&job.sql, 4096))),
    );
    let created_at = TimestampMillisecondArray::from_iter(
        snapshots.iter().map(|job| Some(job.created_at)),
    );
    let started_at =
        TimestampMillisecondArray::from_iter(snapshots.iter().map(|job| job.started_at));
    let finished_at =
        TimestampMillisecondArray::from_iter(snapshots.iter().map(|job| job.finished_at));
    let rows = Int64Array::from_iter(snapshots.iter().map(|job| Some(job.rows as i64)));
    let bytes = Int64Array::from_iter(snapshots.iter().map(|job| Some(job.bytes as i64)));
    let command_tag =
        StringArray::from_iter(snapshots.iter().map(|job| job.command_tag.clone()));
    let error_sqlstate =
        StringArray::from_iter(snapshots.iter().map(|job| job.error_sqlstate.clone()));
    let error_message =
        StringArray::from_iter(snapshots.iter().map(|job| job.error_message.clone()));
    let result_kind = StringArray::from_iter(
        snapshots
            .iter()
            .map(|job| job.result_kind.map(|kind| kind.as_str().to_string())),
    );
    let result_rows = Int64Array::from_iter(
        snapshots
            .iter()
            .map(|job| job.result_rows.map(|rows| rows as i64)),
    );
    let result_bytes = Int64Array::from_iter(
        snapshots
            .iter()
            .map(|job| job.result_bytes.map(|bytes| bytes as i64)),
    );
    let result_expires_at = TimestampMillisecondArray::from_iter(
        snapshots.iter().map(|job| job.result_expires_at),
    );

    let columns: Vec<ArrayRef> = vec![
        Arc::new(query_ids),
        Arc::new(job_keys),
        Arc::new(users),
        Arc::new(databases),
        Arc::new(states),
        Arc::new(attempts),
        Arc::new(result_set_counts),
        Arc::new(sql),
        Arc::new(created_at),
        Arc::new(started_at),
        Arc::new(finished_at),
        Arc::new(rows),
        Arc::new(bytes),
        Arc::new(command_tag),
        Arc::new(error_sqlstate),
        Arc::new(error_message),
        Arc::new(result_kind),
        Arc::new(result_rows),
        Arc::new(result_bytes),
        Arc::new(result_expires_at),
    ];
    RecordBatch::try_new(status_schema(), columns).map_err(Into::into)
}

fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        return value.to_string();
    }
    let mut end = max;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

/// A provider over the current in-memory job view. Reading in `scan` keeps a
/// prepared `SELECT * FROM lakesoul_query_status($1)` fresh on every Execute.
struct JobStatusTable {
    jobs: Arc<JobService>,
    user: String,
    database: String,
    query_id: Option<Uuid>,
}

impl fmt::Debug for JobStatusTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobStatusTable")
            .field("user", &self.user)
            .field("database", &self.database)
            .field("query_id", &self.query_id)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for JobStatusTable {
    fn schema(&self) -> SchemaRef {
        status_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let snapshots = match self.query_id {
            Some(query_id) => self
                .jobs
                .snapshot(query_id, &self.user, &self.database)
                .into_iter()
                .collect(),
            None => self.jobs.snapshots(&self.user, &self.database),
        };
        let batch = status_batch(&snapshots)?;
        let table = MemTable::try_new(status_schema(), vec![vec![batch]])?;
        table.scan(state, projection, filters, limit).await
    }
}

fn identity(session: &dyn Session) -> DFResult<(String, String)> {
    let options = session.config().options();
    let identity = options
        .extensions
        .get::<PgIdentityConfig>()
        .ok_or_else(|| {
            DataFusionError::Plan(
                "this session has no PostgreSQL identity for the query job tables"
                    .to_string(),
            )
        })?;
    if identity.user.is_empty() {
        return Err(DataFusionError::Plan(
            "this session has no PostgreSQL user for the query job tables".to_string(),
        ));
    }
    Ok((identity.user.clone(), identity.database.clone()))
}

fn string_argument(exprs: &[Expr], index: usize, name: &str) -> DFResult<String> {
    let Some(expr) = exprs.get(index) else {
        return Err(DataFusionError::Plan(format!("{name} is required")));
    };
    literal_string(expr).ok_or_else(|| {
        DataFusionError::Plan(format!("{name} must be a string literal, got {expr}"))
    })
}

fn literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Some(value.clone()),
        Expr::Cast(cast) => literal_string(&cast.expr),
        _ => None,
    }
}

fn integer_argument(
    exprs: &[Expr],
    index: usize,
    name: &str,
    default: i64,
) -> DFResult<i64> {
    let Some(expr) = exprs.get(index) else {
        return Ok(default);
    };
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(*value),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Ok(*value as i64),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => Ok(*value as i64),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Ok(*value as i64),
        Expr::Literal(ScalarValue::Null, _) => Ok(default),
        other => Err(DataFusionError::Plan(format!(
            "{name} must be an integer literal, got {other}"
        ))),
    }
}

fn parse_query_id(raw: &str) -> DFResult<Uuid> {
    Uuid::parse_str(raw.trim())
        .map_err(|_| DataFusionError::Plan(format!("invalid query id: {raw}")))
}

/// `SELECT * FROM lakesoul_query_status('<query_id>')`.
pub struct QueryStatusFunction {
    jobs: Arc<JobService>,
}

impl QueryStatusFunction {
    pub fn new(jobs: Arc<JobService>) -> Self {
        Self { jobs }
    }
}

impl fmt::Debug for QueryStatusFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QueryStatusFunction")
    }
}

impl TableFunctionImpl for QueryStatusFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let (user, database) = identity(args.session())?;
        let query_id = parse_query_id(&string_argument(args.exprs(), 0, "query_id")?)?;
        Ok(Arc::new(JobStatusTable {
            jobs: Arc::clone(&self.jobs),
            user,
            database,
            query_id: Some(query_id),
        }))
    }
}

/// `SELECT * FROM lakesoul_queries`.
pub struct QueriesFunction {
    jobs: Arc<JobService>,
}

impl QueriesFunction {
    pub fn new(jobs: Arc<JobService>) -> Self {
        Self { jobs }
    }
}

impl fmt::Debug for QueriesFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QueriesFunction")
    }
}

impl TableFunctionImpl for QueriesFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let (user, database) = identity(args.session())?;
        Ok(Arc::new(JobStatusTable {
            jobs: Arc::clone(&self.jobs),
            user,
            database,
            query_id: None,
        }))
    }
}

/// `SELECT * FROM lakesoul_query_result('<query_id>', offset, limit)`.
pub struct QueryResultFunction {
    jobs: Arc<JobService>,
}

impl QueryResultFunction {
    pub fn new(jobs: Arc<JobService>) -> Self {
        Self { jobs }
    }
}

impl fmt::Debug for QueryResultFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QueryResultFunction")
    }
}

impl TableFunctionImpl for QueryResultFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let (user, database) = identity(args.session())?;
        let raw = string_argument(args.exprs(), 0, "query_id")?;
        let query_id = parse_query_id(&raw)?;
        let offset = integer_argument(args.exprs(), 1, "offset", 0)?.max(0) as usize;
        let limit =
            integer_argument(args.exprs(), 2, "limit", 1_000_000)?.max(0) as usize;
        let result = self
            .jobs
            .result(query_id, &user, &database)
            .map_err(|error| DataFusionError::Plan(error.message))?;
        let Some(last) = result.sets.last() else {
            return Err(DataFusionError::Plan(format!(
                "query {query_id} did not produce a row result; its result is {}",
                result.kind.as_str()
            )));
        };
        Ok(Arc::new(JobResultTable {
            schema: last.schema.clone(),
            file: last.file.clone(),
            offset,
            limit,
            expires_at: result.expires_at,
        }))
    }
}

/// `SELECT * FROM lakesoul_query_result_set(qid, set_index, offset, limit)`.
pub struct QueryResultSetFunction {
    jobs: Arc<JobService>,
}

impl QueryResultSetFunction {
    pub fn new(jobs: Arc<JobService>) -> Self {
        Self { jobs }
    }
}

impl fmt::Debug for QueryResultSetFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QueryResultSetFunction")
    }
}

impl TableFunctionImpl for QueryResultSetFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let (user, database) = identity(args.session())?;
        let query_id = parse_query_id(&string_argument(args.exprs(), 0, "query_id")?)?;
        let set_index =
            integer_argument(args.exprs(), 1, "set_index", 0)?.max(0) as usize;
        let offset = integer_argument(args.exprs(), 2, "offset", 0)?.max(0) as usize;
        let limit =
            integer_argument(args.exprs(), 3, "limit", 1_000_000)?.max(0) as usize;
        let result = self
            .jobs
            .result(query_id, &user, &database)
            .map_err(|error| DataFusionError::Plan(error.message))?;
        let Some(set) = result.sets.get(set_index) else {
            return Err(DataFusionError::Plan(format!(
                "query {query_id} has no result set {set_index}"
            )));
        };
        Ok(Arc::new(JobResultTable {
            schema: set.schema.clone(),
            file: set.file.clone(),
            offset,
            limit,
            expires_at: result.expires_at,
        }))
    }
}

/// `SELECT * FROM lakesoul_query_result_sets(qid)`.
pub struct QueryResultSetsFunction {
    jobs: Arc<JobService>,
}

impl QueryResultSetsFunction {
    pub fn new(jobs: Arc<JobService>) -> Self {
        Self { jobs }
    }
}

impl fmt::Debug for QueryResultSetsFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QueryResultSetsFunction")
    }
}

fn result_sets_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("set_index", DataType::Int64, false),
        Field::new("statement_index", DataType::Int64, false),
        Field::new("rows", DataType::Int64, false),
        Field::new("bytes", DataType::Int64, false),
        Field::new(
            "expires_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]))
}

impl TableFunctionImpl for QueryResultSetsFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let (user, database) = identity(args.session())?;
        let query_id = parse_query_id(&string_argument(args.exprs(), 0, "query_id")?)?;
        let result = self
            .jobs
            .result(query_id, &user, &database)
            .map_err(|error| DataFusionError::Plan(error.message))?;
        let schema = result_sets_schema();
        let set_index = Int64Array::from_iter(
            result.sets.iter().map(|set| Some(set.set_index as i64)),
        );
        let statement_index = Int64Array::from_iter(
            result
                .sets
                .iter()
                .map(|set| Some(set.statement_index as i64)),
        );
        let rows =
            Int64Array::from_iter(result.sets.iter().map(|set| Some(set.rows as i64)));
        let bytes =
            Int64Array::from_iter(result.sets.iter().map(|set| Some(set.bytes as i64)));
        let expires_at = TimestampMillisecondArray::from_iter(
            result.sets.iter().map(|_| result.expires_at),
        );
        let columns: Vec<ArrayRef> = vec![
            Arc::new(set_index),
            Arc::new(statement_index),
            Arc::new(rows),
            Arc::new(bytes),
            Arc::new(expires_at),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(table))
    }
}

struct JobResultTable {
    schema: SchemaRef,
    file: crate::jobs::JobFile,
    offset: usize,
    limit: usize,
    expires_at: Option<i64>,
}

impl fmt::Debug for JobResultTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobResultTable")
            .field("offset", &self.offset)
            .field("limit", &self.limit)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for JobResultTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if self
            .expires_at
            .is_some_and(|expires_at| crate::jobs::now_ms() >= expires_at)
        {
            return Err(DataFusionError::Plan(
                "the query result has expired".to_string(),
            ));
        }
        spool_scan(
            self.schema.clone(),
            &self.file.url,
            self.file.size,
            self.offset,
            self.limit,
            projection,
        )
    }
}
