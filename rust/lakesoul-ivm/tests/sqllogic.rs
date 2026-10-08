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

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    BoolAggKind, ComputedAggArg, ComputedAggResult, DistinctAggKind, GroupingColumn,
    IVM_SOURCE_COLUMN, IvmExecutionAction, IvmRuntime, IvmSqlExecutor, IvmTable,
    IvmTableOptions, JoinOutputColumn, JoinSide, MinMaxKind, MultiJoinColumn,
    PhysicalFormat, VarianceKind, WindowColumn, WindowFunction, WindowGroupSpec,
    approx_distinct_mv_schema_for, approx_percentile_mv_schema_for,
    array_agg_mv_schema_for, avg_mv_schema_for, bool_agg_mv_schema_for,
    computed_agg_mv_schema_for, cross_join_view_schema_for,
    distinct_agg_groups_mv_schema_for, distinct_agg_mv_schema_for,
    full_join_view_schema_for, grouping_sets_mv_schema_for,
    keyed_join_output_primary_keys, keyed_join_view_schema_for,
    left_join_view_schema_for, lookup_join_view_schema_for, median_mv_schema_for,
    min_max_expr_mv_schema_for, min_max_groups_mv_schema_for, min_max_mv_schema_for,
    multi_join_append_schema_for, multi_join_mv_schema_for, multi_join_primary_keys,
    multi_window_mv_schema_for, row_expr_mv_schema_for, row_mv_schema_for,
    semi_anti_mv_schema_for, string_agg_expr_mv_schema_for, string_agg_mv_schema_for,
    sum_count_groups_mv_schema_for, sum_count_mv_schema_for, sum_expr_mv_schema_for,
    top_k_mv_schema_for, union_all_mv_schema_for, union_distinct_mv_schema_for,
    union_output_schema_for, variance_groups_mv_schema_for, variance_mv_schema_for,
    wide_keyed_join_view_schema_for, wide_lookup_join_view_schema_for,
    wide_outer_join_view_schema_for, window_aggregate_mv_schema_for,
    window_columns_mv_schema_for, window_ranking_mv_schema_for,
    window_value_mv_schema_for,
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
    /// The action of the last `INSERT` the executor ran, for
    /// `/*ivm-expect-action*/` assertions.
    last_action: Option<IvmExecutionAction>,
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
            last_action: None,
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
            last_action,
        } = self;
        rt.block_on(run_statement(executor, ctx, tables, last_action, sql))
    }

    async fn shutdown(&mut self) {}
}

async fn run_statement(
    executor: &mut IvmSqlExecutor,
    ctx: &SessionContext,
    tables: &mut HashMap<String, IvmTable>,
    last_action: &mut Option<IvmExecutionAction>,
    sql: &str,
) -> Result<DBOutput<DefaultColumnType>, SltError> {
    let sql = sql.trim();
    if let Some(directive) = sql
        .strip_prefix("/*")
        .and_then(|rest| rest.strip_suffix("*/"))
    {
        run_directive(executor, tables, last_action, directive.trim()).await?;
        return Ok(DBOutput::StatementComplete(0));
    }
    if sql.to_ascii_lowercase().starts_with("insert") {
        let execution = executor.execute(sql).await.map_err(SltError::from)?;
        *last_action = Some(execution.action);
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

/// `/*ivm-append ...*/` appends source rows, `/*ivm-expect-action ...*/`
/// asserts the action of the last `INSERT`.
async fn run_directive(
    executor: &IvmSqlExecutor,
    tables: &mut HashMap<String, IvmTable>,
    last_action: &Option<IvmExecutionAction>,
    directive: &str,
) -> Result<(), SltError> {
    if let Some(expected) = directive.strip_prefix("ivm-expect-action") {
        let expected = expected.trim();
        let Some(last_action) = last_action else {
            return Err(SltError(
                "ivm-expect-action: no INSERT has run yet".to_string(),
            ));
        };
        if last_action.to_string() != expected {
            return Err(SltError(format!(
                "expected action {expected}, got {last_action}"
            )));
        }
        return Ok(());
    }
    append_rows(executor, tables, directive).await
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
        let column = columns.get(field.name()).ok_or_else(|| {
            SltError(format!("row values miss column {}", field.name()))
        })?;
        if column.len() != count {
            return Err(SltError(format!(
                "column {} has {} values, expected {count}",
                field.name(),
                column.len()
            )));
        }
        // `null` (case-insensitive) maps to a NULL of any type.
        let values = column
            .iter()
            .map(|value| {
                if value.eq_ignore_ascii_case("null") {
                    None
                } else {
                    Some(value.as_str())
                }
            })
            .collect::<Vec<_>>();
        arrays.push(match field.data_type() {
            DataType::Int64 => {
                Arc::new(Int64Array::from(parse_i64(&values)?)) as ArrayRef
            }
            DataType::Int32 => {
                Arc::new(Int32Array::from(parse_i32(&values)?)) as ArrayRef
            }
            DataType::Float64 => {
                Arc::new(Float64Array::from(parse_f64(&values)?)) as ArrayRef
            }
            DataType::Boolean => {
                Arc::new(BooleanArray::from(parse_bool(&values)?)) as ArrayRef
            }
            DataType::Utf8 => {
                Arc::new(StringArray::from_iter(values.clone())) as ArrayRef
            }
            DataType::Date32 => {
                Arc::new(Date32Array::from(parse_i32(&values)?)) as ArrayRef
            }
            DataType::Timestamp(unit, _) => {
                let parsed = parse_i64(&values)?;
                let array: ArrayRef = match unit {
                    TimeUnit::Second => Arc::new(TimestampSecondArray::from(parsed)),
                    TimeUnit::Millisecond => {
                        Arc::new(TimestampMillisecondArray::from(parsed))
                    }
                    TimeUnit::Microsecond => {
                        Arc::new(TimestampMicrosecondArray::from(parsed))
                    }
                    TimeUnit::Nanosecond => {
                        Arc::new(TimestampNanosecondArray::from(parsed))
                    }
                };
                array
            }
            DataType::Decimal128(precision, scale) => {
                let parsed = values
                    .iter()
                    .map(|value| {
                        value.map(|value| parse_decimal(value, *scale)).transpose()
                    })
                    .collect::<Result<Vec<Option<i128>>, _>>()?;
                Arc::new(
                    Decimal128Array::from(parsed)
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(SltError::from)?,
                ) as ArrayRef
            }
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

fn parse_i64(values: &[Option<&str>]) -> Result<Vec<Option<i64>>, SltError> {
    values
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    value
                        .parse::<i64>()
                        .map_err(|error| SltError(error.to_string()))
                })
                .transpose()
        })
        .collect()
}

fn parse_i32(values: &[Option<&str>]) -> Result<Vec<Option<i32>>, SltError> {
    values
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    value
                        .parse::<i32>()
                        .map_err(|error| SltError(error.to_string()))
                })
                .transpose()
        })
        .collect()
}

fn parse_f64(values: &[Option<&str>]) -> Result<Vec<Option<f64>>, SltError> {
    values
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    value
                        .parse::<f64>()
                        .map_err(|error| SltError(error.to_string()))
                })
                .transpose()
        })
        .collect()
}

fn parse_bool(values: &[Option<&str>]) -> Result<Vec<Option<bool>>, SltError> {
    values
        .iter()
        .map(|value| {
            value
                .map(|value| match value.to_ascii_lowercase().as_str() {
                    "true" => Ok(true),
                    "false" => Ok(false),
                    other => Err(SltError(format!("bad boolean {other:?}"))),
                })
                .transpose()
        })
        .collect()
}

/// A decimal string like `12.34` with scale `2` becomes `1234`.
fn parse_decimal(value: &str, scale: i8) -> Result<i128, SltError> {
    let (sign, rest) = match value.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, value),
    };
    let (integer, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    if !integer.is_ascii() || !fraction.is_ascii() {
        return Err(SltError(format!("bad decimal {value:?}")));
    }
    let scale = usize::try_from(scale)
        .map_err(|_| SltError(format!("bad decimal scale {scale}")))?;
    let mut fraction = fraction.to_string();
    if fraction.len() > scale {
        if fraction[scale..].chars().any(|digit| digit != '0') {
            return Err(SltError(format!(
                "decimal {value} does not fit scale {scale}"
            )));
        }
        fraction.truncate(scale);
    } else {
        fraction.push_str(&"0".repeat(scale - fraction.len()));
    }
    let integer = if integer.is_empty() { "0" } else { integer };
    let digits = format!("{integer}{fraction}");
    let magnitude = digits
        .parse::<i128>()
        .map_err(|error| SltError(error.to_string()))?;
    Ok(sign * magnitude)
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

/// The default fixture with a nullable value column.
fn nullable_source_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("v", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

/// A source table with a composite primary key.
fn multi_key_source_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k1", DataType::Int64, false),
        arrow::datatypes::Field::new("k2", DataType::Utf8, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("v", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

/// A source with nullable float/decimal columns and a boolean flag.
fn typed_source_schema() -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("v", DataType::Float64, true),
        arrow::datatypes::Field::new("flag", DataType::Boolean, false),
        arrow::datatypes::Field::new("d", DataType::Decimal128(10, 2), true),
        arrow::datatypes::Field::new("day", DataType::Date32, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

/// One source table of an SLT fixture.
struct SltSource {
    /// The placeholder in the script, e.g. `__SRC__`.
    placeholder: &'static str,
    schema: arrow::datatypes::SchemaRef,
    primary_keys: Vec<String>,
    cdc_column: bool,
}

impl SltSource {
    /// A keyed source with CDC column `op`.
    fn keyed(
        placeholder: &'static str,
        schema: arrow::datatypes::SchemaRef,
        primary_keys: Vec<String>,
    ) -> Self {
        Self {
            placeholder,
            schema,
            primary_keys,
            cdc_column: true,
        }
    }

    /// An append-only source without CDC semantics.
    fn append_only(
        placeholder: &'static str,
        schema: arrow::datatypes::SchemaRef,
    ) -> Self {
        Self {
            placeholder,
            schema,
            primary_keys: Vec::new(),
            cdc_column: false,
        }
    }

    /// An append-only changelog with CDC markers but no merge key.
    fn append_only_cdc(
        placeholder: &'static str,
        schema: arrow::datatypes::SchemaRef,
    ) -> Self {
        Self {
            placeholder,
            schema,
            primary_keys: Vec::new(),
            cdc_column: true,
        }
    }
}

/// Create the given source tables plus an MV and run one script against them.
fn run_script_for_sources(
    tag: &str,
    script: &str,
    sources: Vec<SltSource>,
    mv_schema: arrow::datatypes::SchemaRef,
    mv_primary_keys: Vec<String>,
) {
    let setup = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source_names = (0..sources.len())
        .map(|index| format!("slt_{tag}_src{index}_{suffix}"))
        .collect::<Vec<_>>();
    let mv_name = format!("slt_{tag}_mv_{suffix}");
    setup.block_on(async {
        let runtime = IvmRuntime::from_env().await.unwrap();
        runtime.init_schema().await.unwrap();
        for (index, source) in sources.iter().enumerate() {
            let mut options = IvmTableOptions::new(
                source_names[index].clone(),
                format!(
                    "file://{}",
                    dir.path().join(format!("src{index}")).display()
                ),
                source.schema.clone(),
            )
            .with_file_format(PhysicalFormat::Vortex);
            if !source.primary_keys.is_empty() {
                options = options.with_primary_keys(source.primary_keys.clone());
            }
            if source.cdc_column {
                options = options.with_cdc_column(CHANGE_COLUMN);
            }
            runtime.create_table(options).await.unwrap();
        }
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

    let mut script = script.to_string();
    for (source, name) in sources.iter().zip(&source_names) {
        script = script.replace(source.placeholder, name);
    }
    let script = script.replace("__MV__", &mv_name);
    let mut registrations = source_names
        .iter()
        .map(|name| (name.clone(), name.clone()))
        .collect::<Vec<_>>();
    registrations.push((mv_name.clone(), mv_name.clone()));
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

/// Create one source table plus an MV and run one script against them.
fn run_script_for_source(
    tag: &str,
    script: &str,
    source_schema: arrow::datatypes::SchemaRef,
    source_primary_keys: Vec<String>,
    mv_schema: arrow::datatypes::SchemaRef,
    mv_primary_keys: Vec<String>,
) {
    run_script_for_sources(
        tag,
        script,
        vec![SltSource::keyed(
            "__SRC__",
            source_schema,
            source_primary_keys,
        )],
        mv_schema,
        mv_primary_keys,
    );
}

/// The default fixture: keyed on `k`, CDC column `op`.
fn run_script_for_mv(
    tag: &str,
    script: &str,
    mv_schema: arrow::datatypes::SchemaRef,
    mv_primary_keys: Vec<String>,
) {
    run_script_for_source(
        tag,
        script,
        source_schema(),
        vec!["k".to_string()],
        mv_schema,
        mv_primary_keys,
    );
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
fn sqllogic_aggregate_filter() {
    run_script_for_mv(
        "aggfilter",
        include_str!("slt/aggregate_filter.slt"),
        avg_mv_schema_for(&source_schema(), &group_keys(&["g"]), "v").unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_group_expression() {
    run_script_for_mv(
        "groupexpr",
        include_str!("slt/group_expr.slt"),
        sum_count_groups_mv_schema_for(
            &source_schema(),
            &group_keys(&["bucket"]),
            &group_keys(&["v % 10"]),
            Some("v"),
            None,
            false,
        )
        .unwrap(),
        group_keys(&["bucket"]),
    );
}

#[test]
fn sqllogic_sum_expression() {
    run_script_for_mv(
        "sumexpr",
        include_str!("slt/sum_expr.slt"),
        sum_expr_mv_schema_for(&source_schema(), &group_keys(&["g"]), "v * 2", true)
            .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_sum_expression_append_only() {
    let source = source_schema();
    run_script_for_sources(
        "sumexprappend",
        include_str!("slt/sum_expr_append.slt"),
        vec![SltSource::append_only_cdc("__SRC__", source.clone())],
        sum_expr_mv_schema_for(&source, &group_keys(&["g"]), "v * 2", false).unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_count_column() {
    let schema = nullable_source_schema();
    run_script_for_source(
        "countcolumn",
        include_str!("slt/count_column.slt"),
        schema.clone(),
        group_keys(&["k"]),
        sum_count_mv_schema_for(&schema, &group_keys(&["g"]), None).unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_count_column_append_only() {
    let schema = nullable_source_schema();
    run_script_for_sources(
        "countcolumnappend",
        include_str!("slt/count_column_append.slt"),
        vec![SltSource::append_only_cdc("__SRC__", schema.clone())],
        sum_count_mv_schema_for(&schema, &group_keys(&["g"]), None).unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_min_expression() {
    run_script_for_mv(
        "minexpr",
        include_str!("slt/min_expr.slt"),
        min_max_expr_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            "v * 2",
            MinMaxKind::Min,
        )
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
fn sqllogic_sum_count_where() {
    run_script_for_mv(
        "where",
        include_str!("slt/sum_count_where.slt"),
        sum_count_mv_schema_for(&source_schema(), &group_keys(&["g"]), Some("v"))
            .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_min_max_where() {
    run_script_for_mv(
        "minmaxwhere",
        include_str!("slt/min_max_where.slt"),
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
fn sqllogic_sum_count_multi_key() {
    let schema = multi_key_source_schema();
    run_script_for_source(
        "multikey",
        include_str!("slt/sum_count_multi_key.slt"),
        schema.clone(),
        group_keys(&["k1", "k2"]),
        sum_count_mv_schema_for(&schema, &group_keys(&["g"]), Some("v")).unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_row_where() {
    run_script_for_mv(
        "rowwhere",
        include_str!("slt/row_where.slt"),
        row_mv_schema_for(&source_schema(), &group_keys(&["k", "v"])).unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_row_expressions() {
    let source = source_schema();
    let columns = group_keys(&["k", "v2", "bucket"]);
    let exprs = vec![
        "k".to_string(),
        "v * 2".to_string(),
        "CASE WHEN v > 5 THEN 'big' ELSE 'small' END".to_string(),
    ];
    run_script_for_mv(
        "rowexprs",
        include_str!("slt/row_exprs.slt"),
        row_expr_mv_schema_for(&source, &columns, &exprs).unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_row_typed_append() {
    let schema = typed_source_schema();
    let output_columns = group_keys(&["k", "v", "flag", "d", "day"]);
    run_script_for_source(
        "rowtypes",
        include_str!("slt/row_types.slt"),
        schema.clone(),
        group_keys(&["k"]),
        row_mv_schema_for(&schema, &output_columns).unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_typed_sum_avg() {
    let schema = typed_source_schema();
    run_script_for_source(
        "typedsumavg",
        include_str!("slt/typed_sum_avg.slt"),
        schema.clone(),
        group_keys(&["k"]),
        avg_mv_schema_for(&schema, &group_keys(&["flag"]), "v").unwrap(),
        group_keys(&["flag"]),
    );
}

#[test]
fn sqllogic_typed_min_max() {
    let schema = typed_source_schema();
    run_script_for_source(
        "typedminmax",
        include_str!("slt/typed_min_max.slt"),
        schema.clone(),
        group_keys(&["k"]),
        min_max_mv_schema_for(&schema, &group_keys(&["day"]), "d", MinMaxKind::Min)
            .unwrap(),
        group_keys(&["day"]),
    );
}

#[test]
fn sqllogic_typed_distinct() {
    let schema = typed_source_schema();
    run_script_for_source(
        "typeddistinct",
        include_str!("slt/typed_distinct.slt"),
        schema.clone(),
        group_keys(&["k"]),
        distinct_agg_mv_schema_for(
            &schema,
            &group_keys(&["flag"]),
            "d",
            DistinctAggKind::Count,
        )
        .unwrap(),
        group_keys(&["flag"]),
    );
}

#[test]
fn sqllogic_typed_variance() {
    let schema = typed_source_schema();
    run_script_for_source(
        "typedvariance",
        include_str!("slt/typed_variance.slt"),
        schema.clone(),
        group_keys(&["k"]),
        variance_mv_schema_for(
            &schema,
            &group_keys(&["flag"]),
            "v",
            VarianceKind::VarSamp,
        )
        .unwrap(),
        group_keys(&["flag"]),
    );
}

#[test]
fn sqllogic_typed_window() {
    let schema = typed_source_schema();
    run_script_for_source(
        "typedwindow",
        include_str!("slt/typed_window.slt"),
        schema.clone(),
        group_keys(&["k"]),
        window_columns_mv_schema_for(
            &schema,
            &group_keys(&["flag"]),
            &group_keys(&["k"]),
            &[WindowColumn::new(WindowFunction::RowNumber)],
        )
        .unwrap(),
        group_keys(&["flag", "k"]),
    );
}

#[test]
fn sqllogic_array_agg() {
    run_script_for_mv(
        "arrayagg",
        include_str!("slt/array_agg.slt"),
        array_agg_mv_schema_for(&source_schema(), &group_keys(&["g"]), "v").unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_string_agg_expression() {
    run_script_for_mv(
        "stringaggexpr",
        include_str!("slt/string_agg_expr.slt"),
        string_agg_expr_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            "CAST(v AS VARCHAR)",
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_string_agg() {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("s", DataType::Utf8, true),
        arrow::datatypes::Field::new("v", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_source(
        "stringagg",
        include_str!("slt/string_agg.slt"),
        schema.clone(),
        group_keys(&["k"]),
        string_agg_mv_schema_for(&schema, &group_keys(&["g"]), "s").unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_window_order_expression() {
    run_script_for_mv(
        "windoworderexpr",
        include_str!("slt/window_order_expr.slt"),
        window_columns_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            &[WindowColumn::new(WindowFunction::RowNumber)],
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_desc() {
    let schema = nullable_source_schema();
    run_script_for_source(
        "windowdesc",
        include_str!("slt/window_desc.slt"),
        schema.clone(),
        group_keys(&["k"]),
        window_ranking_mv_schema_for(
            &schema,
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::RowNumber,
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_top_k_desc() {
    run_script_for_mv(
        "topkdesc",
        include_str!("slt/top_k_desc.slt"),
        top_k_mv_schema_for(&source_schema(), &group_keys(&["k", "g", "v"])).unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_where() {
    run_script_for_mv(
        "windowwhere",
        include_str!("slt/window_where.slt"),
        window_ranking_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::RowNumber,
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_multi() {
    let columns = vec![
        WindowColumn::new(WindowFunction::Sum)
            .with_value("v")
            .with_column("total"),
        WindowColumn::new(WindowFunction::Count).with_column("n"),
        WindowColumn::new(WindowFunction::RowNumber).with_column("rn"),
    ];
    run_script_for_mv(
        "windowmulti",
        include_str!("slt/window_multi.slt"),
        window_columns_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            &columns,
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_select_distinct() {
    run_script_for_mv(
        "selectdistinct",
        include_str!("slt/select_distinct.slt"),
        sum_count_mv_schema_for(&source_schema(), &group_keys(&["g"]), None).unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_select_distinct_rows() {
    run_script_for_mv(
        "selectdistinctrows",
        include_str!("slt/select_distinct_rows.slt"),
        sum_count_mv_schema_for(&source_schema(), &group_keys(&["k", "g"]), None)
            .unwrap(),
        group_keys(&["k", "g"]),
    );
}

#[test]
fn sqllogic_multi_distinct() {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, true),
        arrow::datatypes::Field::new("a", DataType::Int64, true),
        arrow::datatypes::Field::new("b", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "multidistinct",
        include_str!("slt/multi_distinct.slt"),
        vec![SltSource::keyed(
            "__SRC__",
            schema.clone(),
            group_keys(&["id"]),
        )],
        distinct_agg_mv_schema_for(
            &schema,
            &group_keys(&["g"]),
            "a",
            DistinctAggKind::Count,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_distinct_agg() {
    run_script_for_mv(
        "distinct",
        include_str!("slt/distinct_agg.slt"),
        distinct_agg_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            "v",
            DistinctAggKind::Count,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_distinct_sum() {
    run_script_for_mv(
        "distinctsum",
        include_str!("slt/distinct_sum.slt"),
        distinct_agg_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            "v",
            DistinctAggKind::Sum,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_median() {
    run_script_for_mv(
        "median",
        include_str!("slt/median.slt"),
        median_mv_schema_for(&source_schema(), &group_keys(&["g"]), "v").unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_variance() {
    run_script_for_mv(
        "variance",
        include_str!("slt/variance.slt"),
        variance_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            "v",
            VarianceKind::VarSamp,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_variance_group_expression() {
    run_script_for_mv(
        "variancegroupexpr",
        include_str!("slt/variance_group_expr.slt"),
        variance_groups_mv_schema_for(
            &source_schema(),
            &group_keys(&["bucket"]),
            &group_keys(&["v % 10"]),
            Some("v"),
            None,
            VarianceKind::VarSamp,
        )
        .unwrap(),
        group_keys(&["bucket"]),
    );
}

#[test]
fn sqllogic_variance_expression() {
    run_script_for_mv(
        "varianceexpr",
        include_str!("slt/variance_expr.slt"),
        variance_groups_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &[],
            None,
            Some("v * 2"),
            VarianceKind::VarPop,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_stddev() {
    run_script_for_mv(
        "stddev",
        include_str!("slt/stddev.slt"),
        variance_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            "v",
            VarianceKind::StddevSamp,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_avg() {
    run_script_for_mv(
        "avg",
        include_str!("slt/avg.slt"),
        avg_mv_schema_for(&source_schema(), &group_keys(&["g"]), "v").unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_avg_null() {
    let schema = nullable_source_schema();
    run_script_for_source(
        "avgnull",
        include_str!("slt/avg_null.slt"),
        schema.clone(),
        group_keys(&["k"]),
        avg_mv_schema_for(&schema, &group_keys(&["g"]), "v").unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_having() {
    run_script_for_mv(
        "having",
        include_str!("slt/having.slt"),
        sum_count_mv_schema_for(&source_schema(), &group_keys(&["g"]), Some("v"))
            .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_having_append_only() {
    let source = source_schema();
    run_script_for_sources(
        "havingappend",
        include_str!("slt/having_append.slt"),
        vec![SltSource::append_only_cdc("__SRC__", source.clone())],
        sum_count_mv_schema_for(&source, &group_keys(&["g"]), Some("v")).unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_min_max_having() {
    run_script_for_mv(
        "minmaxhaving",
        include_str!("slt/min_max_having.slt"),
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
fn sqllogic_window_lag() {
    run_script_for_mv(
        "windowlag",
        include_str!("slt/window_lag.slt"),
        window_value_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::Lag,
            "v",
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_ignore_nulls() {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("v", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_source(
        "ignorenulls",
        include_str!("slt/window_ignore_nulls.slt"),
        schema.clone(),
        group_keys(&["k"]),
        window_value_mv_schema_for(
            &schema,
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::Lag,
            "v",
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_lead() {
    run_script_for_mv(
        "windowlead",
        include_str!("slt/window_lead.slt"),
        window_value_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::Lead,
            "v",
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_ntile() {
    run_script_for_mv(
        "windowntile",
        include_str!("slt/window_ntile.slt"),
        window_ranking_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::Ntile,
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_percent_rank() {
    run_script_for_mv(
        "windowpercent",
        include_str!("slt/window_percent_rank.slt"),
        window_ranking_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::PercentRank,
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_cume_dist() {
    run_script_for_mv(
        "windowcume",
        include_str!("slt/window_cume_dist.slt"),
        window_ranking_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::CumeDist,
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_global() {
    run_script_for_mv(
        "windowglobal",
        include_str!("slt/window_global.slt"),
        window_ranking_mv_schema_for(
            &source_schema(),
            &[],
            &group_keys(&["k"]),
            WindowFunction::RowNumber,
        )
        .unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_window_global_sum() {
    run_script_for_mv(
        "windowglobalsum",
        include_str!("slt/window_global_sum.slt"),
        window_aggregate_mv_schema_for(
            &source_schema(),
            &[],
            &group_keys(&["k"]),
            WindowFunction::Sum,
            Some("v"),
        )
        .unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_window_filter() {
    run_script_for_mv(
        "windowfilter",
        include_str!("slt/window_filter.slt"),
        window_aggregate_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::Sum,
            Some("v"),
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_first_value() {
    run_script_for_mv(
        "windowfirst",
        include_str!("slt/window_first_value.slt"),
        window_value_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::FirstValue,
            "v",
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_last_value() {
    run_script_for_mv(
        "windowlast",
        include_str!("slt/window_last_value.slt"),
        window_value_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::LastValue,
            "v",
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_nth_value() {
    run_script_for_mv(
        "windownth",
        include_str!("slt/window_nth_value.slt"),
        window_value_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::NthValue,
            "v",
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_sum_frame() {
    run_script_for_mv(
        "windowsumframe",
        include_str!("slt/window_sum_frame.slt"),
        window_aggregate_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::Sum,
            Some("v"),
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_window_aggregate_where() {
    run_script_for_mv(
        "windowaggwhere",
        include_str!("slt/window_aggregate_where.slt"),
        window_aggregate_mv_schema_for(
            &source_schema(),
            &group_keys(&["g"]),
            &group_keys(&["k"]),
            WindowFunction::Sum,
            Some("v"),
        )
        .unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_top_k_where() {
    run_script_for_mv(
        "topkwhere",
        include_str!("slt/top_k_where.slt"),
        top_k_mv_schema_for(&source_schema(), &group_keys(&["k", "g", "v"])).unwrap(),
        group_keys(&["g", "k"]),
    );
}

#[test]
fn sqllogic_join_filters() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "joinfilters",
        include_str!("slt/join_filters.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rid"])),
        ],
        keyed_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_left_lookup_join() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("jk", DataType::Utf8, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "leftjoin",
        include_str!("slt/left_join.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["jk"])),
        ],
        lookup_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["jk"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        group_keys(&["id"]),
    );
}

#[test]
fn sqllogic_lookup_join_filter() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("jk", DataType::Utf8, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "lookupfilter",
        include_str!("slt/lookup_join_filter.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["jk"])),
        ],
        lookup_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["jk"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        group_keys(&["id"]),
    );
}

#[test]
fn sqllogic_lookup_join_names() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rk", DataType::Utf8, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "lookupnames",
        include_str!("slt/lookup_join_names.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rk"])),
        ],
        lookup_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rk"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        group_keys(&["id"]),
    );
}

#[test]
fn sqllogic_left_join_multi() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "leftjoinmulti",
        include_str!("slt/left_join_multi.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rid"])),
        ],
        left_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_full_join() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "fulljoin",
        include_str!("slt/full_join.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rid"])),
        ],
        full_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_right_join() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("jk", DataType::Utf8, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "rightjoin",
        include_str!("slt/right_join.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["jk"])),
        ],
        left_join_view_schema_for(
            &dim,
            &fact,
            &group_keys(&["jk"]),
            &group_keys(&["id"]),
            &group_keys(&["jk"]),
            "rv",
            "lv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["jk"]), &group_keys(&["id"])),
    );
}

#[test]
fn sqllogic_union_types() {
    let schema = typed_source_schema();
    let output =
        union_output_schema_for(&schema, &group_keys(&["k", "v", "d", "day"]), &[])
            .unwrap();
    run_script_for_sources(
        "uniontypes",
        include_str!("slt/union_types.slt"),
        vec![
            SltSource::keyed("__SRC1__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", schema.clone(), group_keys(&["k"])),
        ],
        union_all_mv_schema_for(&output).unwrap(),
        group_keys(&[IVM_SOURCE_COLUMN, "k"]),
    );
}

#[test]
fn sqllogic_union_projection() {
    let schema = source_schema();
    let output = union_output_schema_for(
        &schema,
        &group_keys(&["k", "g", "amount"]),
        &group_keys(&["k", "g", "(v * 2)"]),
    )
    .unwrap();
    run_script_for_sources(
        "unionprojection",
        include_str!("slt/union_projection.slt"),
        vec![
            SltSource::keyed("__SRC1__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", schema.clone(), group_keys(&["k"])),
        ],
        union_all_mv_schema_for(&output).unwrap(),
        group_keys(&[IVM_SOURCE_COLUMN, "k"]),
    );
}

#[test]
fn sqllogic_union_distinct() {
    let schema = source_schema();
    run_script_for_sources(
        "uniondistinct",
        include_str!("slt/union_distinct.slt"),
        vec![
            SltSource::keyed("__SRC1__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", schema.clone(), group_keys(&["k"])),
        ],
        union_distinct_mv_schema_for(
            &union_output_schema_for(&schema, &group_keys(&["k", "g", "v"]), &[])
                .unwrap(),
        ),
        group_keys(&["k", "g", "v"]),
    );
}

#[test]
fn sqllogic_union_distinct_append_only() {
    let schema = source_schema();
    run_script_for_sources(
        "uniondistinctappend",
        include_str!("slt/union_distinct_append.slt"),
        vec![
            SltSource::append_only_cdc("__SRC1__", schema.clone()),
            SltSource::append_only_cdc("__SRC2__", schema.clone()),
        ],
        union_distinct_mv_schema_for(
            &union_output_schema_for(&schema, &group_keys(&["k", "g", "v"]), &[])
                .unwrap(),
        ),
        group_keys(&["k", "g", "v"]),
    );
}

#[test]
fn sqllogic_union_where() {
    let source = source_schema();
    run_script_for_sources(
        "unionwhere",
        include_str!("slt/union_where.slt"),
        vec![
            SltSource::keyed("__SRC1__", source.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", source.clone(), group_keys(&["k"])),
        ],
        union_all_mv_schema_for(&source).unwrap(),
        group_keys(&[IVM_SOURCE_COLUMN, "k"]),
    );
}

#[test]
fn sqllogic_union_append_where() {
    let source = source_schema();
    run_script_for_sources(
        "unionappend",
        include_str!("slt/union_append_where.slt"),
        vec![
            SltSource::append_only("__SRC1__", source.clone()),
            SltSource::append_only("__SRC2__", source.clone()),
        ],
        union_all_mv_schema_for(&source).unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_min_max_group_expression() {
    run_script_for_mv(
        "minmaxgroupexpr",
        include_str!("slt/min_max_group_expr.slt"),
        min_max_groups_mv_schema_for(
            &source_schema(),
            &group_keys(&["bucket"]),
            &group_keys(&["(v % 10)"]),
            Some("v"),
            None,
            MinMaxKind::Min,
        )
        .unwrap(),
        group_keys(&["bucket"]),
    );
}

#[test]
fn sqllogic_distinct_group_expression() {
    run_script_for_mv(
        "distinctgroupexpr",
        include_str!("slt/distinct_group_expr.slt"),
        distinct_agg_groups_mv_schema_for(
            &source_schema(),
            &group_keys(&["bucket"]),
            &group_keys(&["(v % 10)"]),
            "g",
            DistinctAggKind::Count,
        )
        .unwrap(),
        group_keys(&["bucket"]),
    );
}

#[test]
fn sqllogic_multi_window() {
    let groups = vec![
        WindowGroupSpec {
            partition_keys: group_keys(&["v"]),
            order_keys: Vec::new(),
            order_by: Vec::new(),
            columns: vec![WindowColumn {
                function: WindowFunction::Sum,
                value_column: Some("v".to_string()),
                window_args: None,
                window_filter: None,
                ignore_nulls: false,
                window_frame: None,
                column: "cnt".to_string(),
            }],
        },
        WindowGroupSpec {
            partition_keys: group_keys(&["g"]),
            order_keys: group_keys(&["v"]),
            order_by: Vec::new(),
            columns: vec![WindowColumn {
                function: WindowFunction::RowNumber,
                value_column: None,
                window_args: None,
                window_filter: None,
                ignore_nulls: false,
                window_frame: None,
                column: "rn".to_string(),
            }],
        },
    ];
    run_script_for_mv(
        "multiwindow",
        include_str!("slt/multi_window.slt"),
        multi_window_mv_schema_for(&source_schema(), &group_keys(&["k"]), &groups)
            .unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_semi_anti() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Int64, false),
        arrow::datatypes::Field::new("x", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rk", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "semianti",
        include_str!("slt/semi_anti.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rk"])),
        ],
        semi_anti_mv_schema_for(&left, &group_keys(&["k", "g", "x"])).unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_semi_anti_filter() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Int64, false),
        arrow::datatypes::Field::new("x", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rk", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Int64, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "semiantifilter",
        include_str!("slt/semi_anti_filter.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rk"])),
        ],
        semi_anti_mv_schema_for(&left, &group_keys(&["k", "g", "x"])).unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_cross_join() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "crossjoin",
        include_str!("slt/cross_join.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rid"])),
        ],
        cross_join_view_schema_for(
            &left,
            &right,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_wide_lookup_join() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new("ln", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("jk", DataType::Utf8, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new("rn", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "widelookup",
        include_str!("slt/wide_lookup_join.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["jk"])),
        ],
        wide_lookup_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["jk"]),
            &[
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "lv".to_string(),
                    name: "lv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "ln".to_string(),
                    name: "ln".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rv".to_string(),
                    name: "rv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rn".to_string(),
                    name: "rn".to_string(),
                },
            ],
        )
        .unwrap(),
        group_keys(&["id"]),
    );
}

#[test]
fn sqllogic_multi_cross_join() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let middle = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("mid", DataType::Int64, false),
        arrow::datatypes::Field::new("mv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "multicross",
        include_str!("slt/multi_cross_join.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", middle.clone(), group_keys(&["mid"])),
            SltSource::keyed("__SRC3__", right.clone(), group_keys(&["rid"])),
        ],
        multi_join_mv_schema_for(
            &[left, middle, right],
            &[
                group_keys(&["id"]),
                group_keys(&["mid"]),
                group_keys(&["rid"]),
            ],
            &[
                MultiJoinColumn {
                    source: 0,
                    column: "lv".to_string(),
                    name: "lv".to_string(),
                },
                MultiJoinColumn {
                    source: 1,
                    column: "mv".to_string(),
                    name: "mv".to_string(),
                },
                MultiJoinColumn {
                    source: 2,
                    column: "rv".to_string(),
                    name: "rv".to_string(),
                },
            ],
        )
        .unwrap(),
        multi_join_primary_keys(&[
            group_keys(&["id"]),
            group_keys(&["mid"]),
            group_keys(&["rid"]),
        ]),
    );
}

#[test]
fn sqllogic_lookup_join_right_filter() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("jk", DataType::Utf8, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "lookupjoinrightfilter",
        include_str!("slt/lookup_join_right_filter.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["jk"])),
        ],
        lookup_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["jk"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        group_keys(&["id"]),
    );
}

#[test]
fn sqllogic_full_join_names_filter() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Int64, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rk", DataType::Int64, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "fulljoinnamesfilter",
        include_str!("slt/full_join_names_filter.slt"),
        vec![
            SltSource::keyed("__SRC1__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rid"])),
        ],
        full_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_wide_full_join() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Int64, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new("ln", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Int64, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new("rn", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "widefull",
        include_str!("slt/wide_full_join.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rid"])),
        ],
        wide_outer_join_view_schema_for(
            &left,
            &right,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            &[
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "lv".to_string(),
                    name: "lv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "ln".to_string(),
                    name: "ln".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rv".to_string(),
                    name: "rv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rn".to_string(),
                    name: "rn".to_string(),
                },
            ],
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_multi_join() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("lk", DataType::Int64, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let middle = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("mid", DataType::Int64, false),
        arrow::datatypes::Field::new("mk", DataType::Int64, true),
        arrow::datatypes::Field::new("mv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dimension = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rk", DataType::Int64, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "multijoin",
        include_str!("slt/multi_join.slt"),
        vec![
            SltSource::keyed("__SRC1__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", middle.clone(), group_keys(&["mid"])),
            SltSource::keyed("__SRC3__", dimension.clone(), group_keys(&["rid"])),
        ],
        multi_join_mv_schema_for(
            &[fact, middle, dimension],
            &[
                group_keys(&["id"]),
                group_keys(&["mid"]),
                group_keys(&["rid"]),
            ],
            &[
                MultiJoinColumn {
                    source: 0,
                    column: "lk".to_string(),
                    name: "lk".to_string(),
                },
                MultiJoinColumn {
                    source: 0,
                    column: "lv".to_string(),
                    name: "lv".to_string(),
                },
                MultiJoinColumn {
                    source: 1,
                    column: "mv".to_string(),
                    name: "mv".to_string(),
                },
                MultiJoinColumn {
                    source: 2,
                    column: "rv".to_string(),
                    name: "rv".to_string(),
                },
            ],
        )
        .unwrap(),
        multi_join_primary_keys(&[
            group_keys(&["id"]),
            group_keys(&["mid"]),
            group_keys(&["rid"]),
        ]),
    );
}

#[test]
fn sqllogic_multi_join_append_only() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("lk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
    ]));
    let middle = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("mk", DataType::Utf8, true),
        arrow::datatypes::Field::new("mv", DataType::Int64, true),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rk", DataType::Utf8, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
    ]));
    run_script_for_sources(
        "multijoinappend",
        include_str!("slt/multi_join_append.slt"),
        vec![
            SltSource::append_only("__SRC1__", left.clone()),
            SltSource::append_only("__SRC2__", middle.clone()),
            SltSource::append_only("__SRC3__", right.clone()),
        ],
        multi_join_append_schema_for(
            &[left, middle, right],
            &[
                MultiJoinColumn {
                    source: 0,
                    column: "lk".to_string(),
                    name: "lk".to_string(),
                },
                MultiJoinColumn {
                    source: 0,
                    column: "lv".to_string(),
                    name: "lv".to_string(),
                },
                MultiJoinColumn {
                    source: 1,
                    column: "mv".to_string(),
                    name: "mv".to_string(),
                },
                MultiJoinColumn {
                    source: 2,
                    column: "rv".to_string(),
                    name: "rv".to_string(),
                },
            ],
        )
        .unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_cross_join_wide() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new("ln", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new("rn", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "crossjoinwide",
        include_str!("slt/cross_join_wide.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rid"])),
        ],
        wide_keyed_join_view_schema_for(
            &left,
            &right,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &[],
            &[
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "lv".to_string(),
                    name: "lv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "ln".to_string(),
                    name: "ln".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rv".to_string(),
                    name: "rv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rn".to_string(),
                    name: "rn".to_string(),
                },
            ],
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_global_min_max() {
    run_script_for_mv(
        "globalminmax",
        include_str!("slt/global_min_max.slt"),
        min_max_mv_schema_for(&source_schema(), &[], "v", MinMaxKind::Min).unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_global_distinct() {
    run_script_for_mv(
        "globaldistinct",
        include_str!("slt/global_distinct.slt"),
        distinct_agg_mv_schema_for(&source_schema(), &[], "v", DistinctAggKind::Count)
            .unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_approx_percentile() {
    run_script_for_mv(
        "approxpercentile",
        include_str!("slt/approx_percentile.slt"),
        approx_percentile_mv_schema_for(&source_schema(), &group_keys(&["g"]), "v")
            .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_approx_distinct() {
    run_script_for_mv(
        "approxdistinct",
        include_str!("slt/approx_distinct.slt"),
        approx_distinct_mv_schema_for(&source_schema(), &group_keys(&["g"]), "v")
            .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_bit_agg() {
    let schema = source_schema();
    run_script_for_mv(
        "bitagg",
        include_str!("slt/bit_agg.slt"),
        computed_agg_mv_schema_for(
            &schema,
            &group_keys(&["g"]),
            &[],
            "bit_and_v",
            ComputedAggResult::Value,
            Some(&ComputedAggArg {
                column: Some("v".to_string()),
                expr: None,
            }),
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_grouping_sets_grouping() {
    let schema = source_schema();
    run_script_for_mv(
        "groupingsetsgrouping",
        include_str!("slt/grouping_sets_grouping.slt"),
        grouping_sets_mv_schema_for(
            &schema,
            &group_keys(&["g"]),
            &[],
            Some("v"),
            None,
            false,
            &[GroupingColumn {
                key: 0,
                name: "is_total".to_string(),
            }],
        )
        .unwrap(),
        group_keys(&["__ivm_grouping", "g"]),
    );
}

#[test]
fn sqllogic_bool_agg() {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("flag", DataType::Boolean, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_source(
        "boolagg",
        include_str!("slt/bool_agg.slt"),
        schema.clone(),
        group_keys(&["k"]),
        bool_agg_mv_schema_for(
            &schema,
            &group_keys(&["g"]),
            "flag",
            BoolAggKind::BoolAnd,
        )
        .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_bool_or() {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("flag", DataType::Boolean, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_source(
        "boolor",
        include_str!("slt/bool_or.slt"),
        schema.clone(),
        group_keys(&["k"]),
        bool_agg_mv_schema_for(&schema, &group_keys(&["g"]), "flag", BoolAggKind::BoolOr)
            .unwrap(),
        group_keys(&["g"]),
    );
}

#[test]
fn sqllogic_grouping_sets() {
    let schema = source_schema();
    run_script_for_mv(
        "groupingsets",
        include_str!("slt/grouping_sets.slt"),
        grouping_sets_mv_schema_for(
            &schema,
            &group_keys(&["g"]),
            &[],
            Some("v"),
            None,
            false,
            &[],
        )
        .unwrap(),
        group_keys(&["__ivm_grouping", "g"]),
    );
}

#[test]
fn sqllogic_grouping_sets_multi() {
    let schema = source_schema();
    run_script_for_mv(
        "groupingsetsmulti",
        include_str!("slt/grouping_sets_multi.slt"),
        grouping_sets_mv_schema_for(
            &schema,
            &group_keys(&["g", "v"]),
            &[],
            Some("v"),
            None,
            false,
            &[],
        )
        .unwrap(),
        group_keys(&["__ivm_grouping", "g", "v"]),
    );
}

#[test]
fn sqllogic_union_all_branches() {
    let schema = source_schema();
    let output =
        union_output_schema_for(&schema, &group_keys(&["k", "g", "v"]), &[]).unwrap();
    run_script_for_sources(
        "unionallbranches",
        include_str!("slt/union_all_branches.slt"),
        vec![
            SltSource::keyed("__SRC1__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC3__", schema.clone(), group_keys(&["k"])),
        ],
        union_all_mv_schema_for(&output).unwrap(),
        group_keys(&[IVM_SOURCE_COLUMN, "k"]),
    );
}

#[test]
fn sqllogic_union_distinct_branches() {
    let schema = source_schema();
    run_script_for_sources(
        "uniondistinctbranches",
        include_str!("slt/union_distinct_branches.slt"),
        vec![
            SltSource::keyed("__SRC1__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC3__", schema.clone(), group_keys(&["k"])),
        ],
        union_distinct_mv_schema_for(
            &union_output_schema_for(&schema, &group_keys(&["k", "g", "v"]), &[])
                .unwrap(),
        ),
        group_keys(&["k", "g", "v"]),
    );
}

#[test]
fn sqllogic_global_variance() {
    run_script_for_mv(
        "globalvariance",
        include_str!("slt/global_variance.slt"),
        variance_mv_schema_for(&source_schema(), &[], "v", VarianceKind::VarSamp)
            .unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_global_median() {
    run_script_for_mv(
        "globalmedian",
        include_str!("slt/global_median.slt"),
        median_mv_schema_for(&source_schema(), &[], "v").unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_global_string_agg() {
    run_script_for_mv(
        "globalstringagg",
        include_str!("slt/global_string_agg.slt"),
        string_agg_mv_schema_for(&source_schema(), &[], "g").unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_global_array_agg() {
    run_script_for_mv(
        "globalarrayagg",
        include_str!("slt/global_array_agg.slt"),
        array_agg_mv_schema_for(&source_schema(), &[], "v").unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_global_distinct_sum() {
    run_script_for_mv(
        "globaldistinctsum",
        include_str!("slt/global_distinct_sum.slt"),
        distinct_agg_mv_schema_for(&source_schema(), &[], "v", DistinctAggKind::Sum)
            .unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_global_sum() {
    run_script_for_mv(
        "globalsum",
        include_str!("slt/global_sum.slt"),
        avg_mv_schema_for(&source_schema(), &[], "v").unwrap(),
        Vec::new(),
    );
}

#[test]
fn sqllogic_set_ops() {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", DataType::Int64, false),
        arrow::datatypes::Field::new("g", DataType::Utf8, false),
        arrow::datatypes::Field::new("v", DataType::Int64, false),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "setops",
        include_str!("slt/set_ops.slt"),
        vec![
            SltSource::keyed("__SRC__", schema.clone(), group_keys(&["k"])),
            SltSource::keyed("__SRC2__", schema.clone(), group_keys(&["k"])),
        ],
        semi_anti_mv_schema_for(&schema, &group_keys(&["k", "v"])).unwrap(),
        group_keys(&["k"]),
    );
}

#[test]
fn sqllogic_wide_join() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Int64, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rk", DataType::Int64, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "widejoin",
        include_str!("slt/wide_join.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rid"])),
        ],
        wide_keyed_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            &[
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "lv".to_string(),
                    name: "lv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Left,
                    column: "id".to_string(),
                    name: "id".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rv".to_string(),
                    name: "rv".to_string(),
                },
                JoinOutputColumn {
                    side: JoinSide::Right,
                    column: "rid".to_string(),
                    name: "rid".to_string(),
                },
            ],
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_inner_join_names() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Int64, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rk", DataType::Int64, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "innerjoinnames",
        include_str!("slt/inner_join_names.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rid"])),
        ],
        keyed_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_theta_join() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "thetajoin",
        include_str!("slt/theta_join.slt"),
        vec![
            SltSource::keyed("__SRC__", left.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rid"])),
        ],
        keyed_join_view_schema_for(
            &left,
            &right,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_cross_theta_join() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("lv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "crosstheta",
        include_str!("slt/cross_theta_join.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rid"])),
        ],
        cross_join_view_schema_for(
            &left,
            &right,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_cross_join_filter() {
    let left = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "crossjoinfilter",
        include_str!("slt/cross_join_filter.slt"),
        vec![
            SltSource::keyed("__SRC1__", left.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", right.clone(), group_keys(&["rid"])),
        ],
        cross_join_view_schema_for(
            &left,
            &right,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_left_join_filter() {
    let fact = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("lv", DataType::Utf8, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("rid", DataType::Int64, false),
        arrow::datatypes::Field::new("jk", DataType::Utf8, true),
        arrow::datatypes::Field::new("rv", DataType::Int64, true),
        arrow::datatypes::Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    run_script_for_sources(
        "leftjoinfilter",
        include_str!("slt/left_join_filter.slt"),
        vec![
            SltSource::keyed("__SRC__", fact.clone(), group_keys(&["id"])),
            SltSource::keyed("__SRC2__", dim.clone(), group_keys(&["rid"])),
        ],
        left_join_view_schema_for(
            &fact,
            &dim,
            &group_keys(&["id"]),
            &group_keys(&["rid"]),
            &group_keys(&["jk"]),
            "lv",
            "rv",
        )
        .unwrap(),
        keyed_join_output_primary_keys(&group_keys(&["id"]), &group_keys(&["rid"])),
    );
}

#[test]
fn sqllogic_cte() {
    run_script_for_mv(
        "cte",
        include_str!("slt/cte.slt"),
        sum_count_mv_schema_for(&source_schema(), &group_keys(&["g"]), Some("v"))
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
