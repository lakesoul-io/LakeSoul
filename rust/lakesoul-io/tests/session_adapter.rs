// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The IO session adapter executes with the consumer's registries.
//!
//! A scan is planned and executed by a consumer session that owns the
//! function registries. The adapter this crate hands to its helpers must
//! therefore carry those registries, and any [`TaskContext`] derived from it
//! must carry them too, or a helper that resolves a function would see an
//! empty registry. These tests drive real parquet files and a real UDF
//! predicate, and check the paths the writer and merge code use to rebuild a
//! task context.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Column, Result as DFResult, ToDFSchema, exec_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::{
    LakeSoulScanConfig, LakeSoulScanExec, ScanFile, ScanWorkUnit,
};
use lakesoul_io::session::LakeSoulIOSession;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// A user-defined scalar function that only the consumer registers.
// ---------------------------------------------------------------------------

static IS_EVEN_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::exact(vec![DataType::Int64], Volatility::Immutable));

#[derive(Debug, PartialEq, Eq, Hash)]
struct IsEven;

impl ScalarUDFImpl for IsEven {
    fn name(&self) -> &str {
        "is_even"
    }

    fn signature(&self) -> &Signature {
        &IS_EVEN_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let values = args.args[0].clone().into_array(rows)?;
        let values = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| exec_datafusion_err!("is_even expects an int64 column"))?;
        let flags = values
            .iter()
            .map(|value| value.map(|value| value % 2 == 0))
            .collect::<arrow_array::BooleanArray>();
        Ok(ColumnarValue::Array(Arc::new(flags)))
    }
}

/// The registering consumer: a task context that owns `is_even`.
fn consumer_context() -> Arc<TaskContext> {
    let is_even: Arc<ScalarUDF> = Arc::new(ScalarUDF::new_from_impl(IsEven));
    Arc::new(TaskContext::new(
        None,
        "consumer".to_string(),
        SessionConfig::default(),
        HashMap::from([("is_even".to_string(), is_even)]),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(RuntimeEnv::default()),
    ))
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]))
}

fn int_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .expect("batch")
}

fn write_parquet(
    dir: &Path,
    name: &str,
    batch: &RecordBatch,
    row_group_size: usize,
) -> (String, u64) {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet");
    let properties = WriterProperties::builder()
        .set_max_row_group_size(row_group_size)
        .build();
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), Some(properties)).expect("writer");
    writer.write(batch).expect("write");
    writer.close().expect("close");
    (
        format!("file://{}", path.display()),
        std::fs::metadata(&path).expect("metadata").len(),
    )
}

fn io_config(table_schema: &SchemaRef) -> LakeSoulIOConfig {
    LakeSoulIOConfig::builder()
        .with_schema(Arc::clone(table_schema))
        .build()
}

fn scan_config(table_schema: &SchemaRef) -> LakeSoulScanConfig {
    LakeSoulScanConfig {
        table_schema: Arc::clone(table_schema),
        output_schema: Arc::clone(table_schema),
        partition_schema: Arc::new(Schema::empty()),
        io_config: io_config(table_schema),
        predicate: None,
        key_constraint: None,
        parquet_force_view_types: false,
        read_options: HashMap::new(),
        limit: None,
    }
}

async fn collect(node: &LakeSoulScanExec, context: Arc<TaskContext>) -> Vec<RecordBatch> {
    let mut stream = node.execute(0, context).expect("execute the scan");
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.expect("batch"));
    }
    batches
}

fn int64_values(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("i")
                .expect("the scan emits its output column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column")
                .values()
                .to_vec()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// A UDF predicate resolved from the adapter's registry filters real rows.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_udf_predicate_resolved_from_the_session_filters_rows() {
    let dir = TempDir::new().expect("temp dir");
    let table_schema = id_schema();
    let values: Vec<i64> = (0..40).collect();
    let (url, size) = write_parquet(
        dir.path(),
        "part-0.parquet",
        &int_batch(&table_schema, &values),
        5,
    );
    let consumer = consumer_context();
    let session = LakeSoulIOSession::from_plain_config_and_context(
        io_config(&table_schema),
        consumer,
    );

    // Resolve the function the way a planner does: through the session's
    // registry, not through a handle held by the test.
    let function = session
        .scalar_functions()
        .get("is_even")
        .cloned()
        .expect("the adapter keeps the consumer's function registry");
    let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
        function,
        vec![Expr::Column(Column::from_name("i"))],
    ));
    let df_schema = table_schema
        .as_ref()
        .clone()
        .to_dfschema()
        .expect("table schema");
    // The adapter builds physical expressions for predicate adaptation; this
    // one carries the consumer's UDF.
    let predicate = session
        .create_physical_expr(expr, &df_schema)
        .expect("physical predicate");

    let mut config = scan_config(&table_schema);
    config.predicate = Some(predicate);
    // The reader applies predicates row by row only when asked to.
    config.read_options = HashMap::from([(
        "datafusion.execution.parquet.pushdown_filters".to_string(),
        "true".to_string(),
    )]);
    let unit = ScanWorkUnit {
        files: vec![ScanFile {
            path: url,
            physical_format: PhysicalFormat::Parquet,
            size: Some(size),
            range: None,
        }],
        default_column_value: HashMap::new(),
        lane: 0,
    };
    let node =
        LakeSoulScanExec::try_new(config, vec![unit], 1).expect("valid descriptor");

    // Execute on the task context derived from the adapter, which is the
    // context the writer and merge paths rebuild the same way.
    let batches = collect(&node, session.task_ctx()).await;
    assert_eq!(
        int64_values(&batches),
        values
            .into_iter()
            .filter(|value| value % 2 == 0)
            .collect::<Vec<_>>(),
        "the UDF predicate filters the rows the scan returns"
    );
}
