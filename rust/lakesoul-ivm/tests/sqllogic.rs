// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! sqllogictest harness for the incremental SQL entry.
//!
//! Statements run through the dedicated executor when they start with
//! `INSERT`, everything else runs on a DataFusion session where the source and
//! MV tables are registered.  Source mutations use a small directive because
//! the LakeSoul providers are read-only:
//!
//! ```text
//! statement ok
//! /*ivm-append src: k=1,g=a,v=10,op=insert; k=2,g=a,v=20,op=insert*/
//! ```
//!
//! The row fields are parsed by column name against the table schema.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, MinMaxKind, PhysicalFormat,
    min_max_mv_schema_for, sum_count_mv_schema_for,
};
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

#[derive(Debug)]
struct SltError(String);

impl fmt::Display for SltError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SltError {}

impl From<rootcause::Report> for SltError {
    fn from(report: rootcause::Report) -> Self {
        SltError(format!("{report:?}"))
    }
}

impl From<datafusion::error::DataFusionError> for SltError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        SltError(error.to_string())
    }
}

impl From<arrow::error::ArrowError> for SltError {
    fn from(error: arrow::error::ArrowError) -> Self {
        SltError(error.to_string())
    }
}

struct SltDb {
    executor: IvmSqlExecutor,
    ctx: SessionContext,
    tables: HashMap<String, IvmTable>,
    /// A private runtime: the sqllogictest `AsyncDB` future must be `Send`,
    /// while LakeSoul readers are only `Send` (not `Sync`).  The SQL work runs
    /// through `block_on`, so no non-`Send` value is held across an await, and
    /// everything (metadata pool included) lives on this runtime.
    rt: tokio::runtime::Runtime,
}

impl SltDb {
    /// Open `(registration name, table name)` pairs on a private runtime and
    /// register them in a fresh session.
    fn new(registrations: Vec<(String, String)>) -> Result<Self, SltError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| SltError(error.to_string()))?;
        let (executor, ctx, tables) = rt.block_on(async move {
            let runtime = IvmRuntime::from_env().await.map_err(SltError::from)?;
            let ctx = SessionContext::new();
            let mut tables = HashMap::new();
            for (register_as, table_name) in &registrations {
                let table = runtime
                    .open_table(table_name, "default")
                    .await
                    .map_err(SltError::from)?;
                ctx.register_table(
                    register_as.as_str(),
                    Arc::new(runtime.table_provider(&table)),
                )
                .map_err(|error| SltError(error.to_string()))?;
                tables.insert(table_name.clone(), table);
            }
            let executor = IvmSqlExecutor::new(runtime).with_session(ctx.clone());
            Ok::<_, SltError>((executor, ctx, tables))
        })?;
        Ok(Self {
            executor,
            ctx,
            tables,
            rt,
        })
    }
}

#[async_trait::async_trait]
impl AsyncDB for SltDb {
    type Error = SltError;
    type ColumnType = DefaultColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> Result<DBOutput<DefaultColumnType>, Self::Error> {
        let SltDb {
            rt,
            executor,
            ctx,
            tables,
        } = self;
        rt.block_on(run_statement(executor, ctx, tables, sql))
    }

    async fn shutdown(&mut self) {}
}

async fn run_statement(
    executor: &mut IvmSqlExecutor,
    ctx: &SessionContext,
    tables: &mut HashMap<String, IvmTable>,
    sql: &str,
) -> Result<DBOutput<DefaultColumnType>, SltError> {
    let sql = sql.trim();
    if let Some(directive) = sql
        .strip_prefix("/*")
        .and_then(|rest| rest.strip_suffix("*/"))
    {
        append_rows(executor, tables, directive.trim()).await?;
        return Ok(DBOutput::StatementComplete(0));
    }
    if sql.to_ascii_lowercase().starts_with("insert") {
        executor.execute(sql).await.map_err(SltError::from)?;
        return Ok(DBOutput::StatementComplete(0));
    }

    let frame = ctx.sql(sql).await.map_err(SltError::from)?;
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.map_err(SltError::from)?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|column| format_value(batch.column(column).as_ref(), row))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    let types = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DefaultColumnType::Integer,
            DataType::Float32 | DataType::Float64 => DefaultColumnType::FloatingPoint,
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Boolean => DefaultColumnType::Text,
            _ => DefaultColumnType::Any,
        })
        .collect();
    Ok(DBOutput::Rows { types, rows })
}

/// `/*ivm-append <table>: k=1,g=a,v=10,op=insert; ...*/`
async fn append_rows(
    executor: &IvmSqlExecutor,
    tables: &mut HashMap<String, IvmTable>,
    directive: &str,
) -> Result<(), SltError> {
    let rest = directive
        .strip_prefix("ivm-append")
        .ok_or_else(|| SltError(format!("unknown directive {directive:?}")))?
        .trim();
    let (table_name, rows) = rest
        .split_once(':')
        .ok_or_else(|| SltError("ivm-append needs `<table>: <rows>`".to_string()))?;
    let table = tables
        .get(table_name.trim())
        .ok_or_else(|| SltError(format!("table {table_name} is not known")))?
        .clone();

    let mut columns: HashMap<String, Vec<String>> = HashMap::new();
    let mut count = 0usize;
    for row in rows.split(';').map(str::trim).filter(|row| !row.is_empty()) {
        count += 1;
        for field in row.split(',').map(str::trim) {
            let (name, value) = field
                .split_once('=')
                .ok_or_else(|| SltError(format!("bad row field {field:?}")))?;
            columns
                .entry(name.trim().to_string())
                .or_default()
                .push(value.trim().to_string());
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(table.schema.fields().len());
    for field in table.schema.fields() {
        let values = columns.get(field.name()).ok_or_else(|| {
            SltError(format!("row values miss column {}", field.name()))
        })?;
        if values.len() != count {
            return Err(SltError(format!(
                "column {} has {} values, expected {count}",
                field.name(),
                values.len()
            )));
        }
        arrays.push(match field.data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(
                values
                    .iter()
                    .map(|value| value.parse::<i64>())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| SltError(error.to_string()))?,
            )) as ArrayRef,
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .iter()
                    .map(|value| value.parse::<i32>())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| SltError(error.to_string()))?,
            )) as ArrayRef,
            DataType::Utf8 => Arc::new(StringArray::from_iter_values(
                values.iter().map(String::as_str),
            )),
            other => {
                return Err(SltError(format!(
                    "ivm-append does not support column type {other}"
                )));
            }
        });
    }
    let batch = RecordBatch::try_new(table.schema.clone(), arrays)
        .map_err(|error| SltError(error.to_string()))?;
    table
        .append_batch(executor.runtime().client(), batch)
        .await
        .map_err(SltError::from)?;
    Ok(())
}

fn format_value(array: &dyn Array, row: usize) -> Result<String, SltError> {
    if array.is_null(row) {
        return Ok("NULL".to_string());
    }
    array_value_to_string(array, row).map_err(SltError::from)
}

fn source_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("v", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn group_keys(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|key| (*key).to_string()).collect()
}

/// Create a source table (keyed on `k`, CDC column `op`) plus an MV with the
/// given schema and run one script against them.
fn run_script_for_mv(
    tag: &str,
    script: &str,
    mv_schema: arrow::datatypes::SchemaRef,
    mv_primary_keys: Vec<String>,
) {
    let setup = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source_name = format!("slt_{tag}_src_{suffix}");
    let mv_name = format!("slt_{tag}_mv_{suffix}");
    let schema = source_schema();
    setup.block_on(async {
        let runtime = IvmRuntime::from_env().await.unwrap();
        runtime.init_schema().await.unwrap();
        runtime
            .create_table(
                IvmTableOptions::new(
                    source_name.clone(),
                    format!("file://{}", dir.path().join("src").display()),
                    schema.clone(),
                )
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN)
                .with_file_format(PhysicalFormat::Vortex),
            )
            .await
            .unwrap();
        runtime
            .create_table(
                IvmTableOptions::new(
                    mv_name.clone(),
                    format!("file://{}", dir.path().join("mv").display()),
                    mv_schema,
                )
                .with_primary_keys(mv_primary_keys)
                .with_file_format(PhysicalFormat::Vortex),
            )
            .await
            .unwrap();
    });
    drop(setup);

    let script = script
        .replace("__SRC__", &source_name)
        .replace("__MV__", &mv_name);
    let registrations = vec![
        (source_name.clone(), source_name.clone()),
        (mv_name.clone(), mv_name.clone()),
    ];
    // Every run gets its own session and private runtime; the tables live in
    // LakeSoul metadata, so they are reopened by name.
    let make_connection = move || {
        let registrations = registrations.clone();
        async move { SltDb::new(registrations) }
    };
    let mut runner = Runner::new(make_connection);
    runner
        .run_script(&script)
        .unwrap_or_else(|error| panic!("{tag}: {error}"));
}

#[test]
fn sqllogic_sum_count() {
    run_script_for_mv(
        "sum",
        include_str!("slt/sum_count.slt"),
        sum_count_mv_schema_for(&source_schema(), &group_keys(&["g"]), Some("v"))
            .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_min_max() {
    run_script_for_mv(
        "minmax",
        include_str!("slt/min_max.slt"),
        min_max_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            "v",
            MinMaxKind::Min,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_errors() {
    run_script_for_mv(
        "errors",
        include_str!("slt/errors.slt"),
        sum_count_mv_schema_for(&source_schema(), &group_keys(&["g"]), Some("v"))
            .unwrap(),
        group_keys(&["g"]),
    );
}
