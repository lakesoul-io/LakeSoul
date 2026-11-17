// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! SQL table functions for native, snapshot-pinned vector batch search.
//!
//! The synchronous hook validates literals only. Table resolution uses the
//! SessionState supplied to `scan`, including the PostgreSQL frontend catalog.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{ScalarValue, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::SessionContext;

use crate::datasource::file_format::vector_batch_exec::{
    VectorBatchSearchExec, output_schema, validate_query_schema,
};
use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::vector_batch::native::{
    MAX_BATCH_INPUT_BYTES, MAX_BATCH_QUERIES, NativeBatchOptions,
};

pub const VECTOR_SEARCH_BATCH: &str = "lakesoul_vector_search_batch";
pub const VECTOR_SEARCH_BATCH_FROM_TABLE: &str =
    "lakesoul_vector_search_batch_from_table";

datafusion::common::extensions_options! {
    /// Bounded execution controls for native vector batch SQL functions.
    pub struct VectorBatchConfig {
        /// Maximum query vectors per native window, from 1 to 256.
        pub query_window: usize, default = 256
        /// CPU workers per window; zero selects the process CPU limit.
        pub parallelism: usize, default = 0
        /// Concurrent object/vector block reads, from 1 to 64.
        pub io_concurrency: usize, default = 8
        /// Total transient batch bytes, including query and output buffers.
        pub memory_bytes: usize, default = 134_217_728
    }
}
impl datafusion::common::config::ConfigExtension for VectorBatchConfig {
    const PREFIX: &'static str = "lakesoul_vector_batch";
}
impl VectorBatchConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.query_window == 0 || self.query_window > 256 {
            return Err(plan_error(
                "lakesoul_vector_batch.query_window must be between 1 and 256",
            ));
        }
        if self.parallelism > 256 {
            return Err(plan_error(
                "lakesoul_vector_batch.parallelism must be between 0 and 256",
            ));
        }
        if self.io_concurrency == 0 || self.io_concurrency > 64 {
            return Err(plan_error(
                "lakesoul_vector_batch.io_concurrency must be between 1 and 64",
            ));
        }
        if self.memory_bytes == 0
            || self.memory_bytes > crate::vector_batch::native::DEFAULT_WORK_MEMORY
        {
            return Err(plan_error(
                "lakesoul_vector_batch.memory_bytes must be between 1 and 134217728",
            ));
        }
        Ok(())
    }
}

/// Register both functions on the context that will actually plan SQL.
/// No closure retains a stale context or bypasses its catalog.
pub fn register(context: &SessionContext) {
    {
        let state_ref = context.state_ref();
        let mut state = state_ref.write();
        if state
            .config_options()
            .extensions
            .get::<VectorBatchConfig>()
            .is_none()
        {
            state
                .config_mut()
                .options_mut()
                .extensions
                .insert(VectorBatchConfig::default());
        }
    }
    context.register_udtf(VECTOR_SEARCH_BATCH, Arc::new(VectorBatchFunction(false)));
    context.register_udtf(
        VECTOR_SEARCH_BATCH_FROM_TABLE,
        Arc::new(VectorBatchFunction(true)),
    );
}

pub(crate) fn is_vector_batch_provider(provider: &dyn TableProvider) -> bool {
    provider.is::<VectorBatchProvider>()
}

#[derive(Debug)]
struct VectorBatchFunction(bool);

#[derive(Clone, Debug)]
enum QueryInput {
    Inline(Arc<Vec<Vec<f32>>>),
    Table {
        table: String,
        qid: String,
        vector: String,
    },
}

#[derive(Debug)]
struct VectorBatchProvider {
    target: String,
    column: String,
    input: QueryInput,
    options: NativeBatchOptions,
    metric: String,
}

impl TableFunctionImpl for VectorBatchFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let required = if self.0 { 6 } else { 4 };
        if !(required..=required + 4).contains(&args.len()) {
            return Err(plan_error(format!(
                "{} expects {} to {} literal arguments",
                if self.0 {
                    VECTOR_SEARCH_BATCH_FROM_TABLE
                } else {
                    VECTOR_SEARCH_BATCH
                },
                required,
                required + 4,
            )));
        }
        for (index, argument) in args.iter().enumerate() {
            if !matches!(argument, Expr::Literal(_, _)) {
                return Err(plan_error(format!(
                    "vector batch argument {} must be a literal; placeholders and dynamic arguments are not supported",
                    index + 1,
                )));
            }
        }
        let target = string_arg(args, 0, "target")?;
        let column = string_arg(args, 1, "column")?;
        let input = if self.0 {
            QueryInput::Table {
                table: string_arg(args, 2, "query_table")?,
                qid: string_arg(args, 3, "qid_col")?,
                vector: string_arg(args, 4, "qvec_col")?,
            }
        } else {
            let json = string_arg(args, 2, "queries_json")?;
            if json.len() > MAX_BATCH_INPUT_BYTES {
                return Err(plan_error(
                    "queries_json exceeds 64 MiB; use lakesoul_vector_search_batch_from_table",
                ));
            }
            let queries: Vec<Vec<f32>> = serde_json::from_str(&json)
                .map_err(|error| plan_error(format!("invalid queries_json: {error}")))?;
            if queries.len() > MAX_BATCH_QUERIES {
                return Err(plan_error(format!(
                    "batch supports at most {MAX_BATCH_QUERIES} queries"
                )));
            }
            let input_bytes = queries
                .iter()
                .try_fold(0usize, |sum, query| {
                    query
                        .len()
                        .checked_mul(4)
                        .and_then(|bytes| sum.checked_add(bytes + 8))
                })
                .ok_or_else(|| plan_error("batch input size overflow"))?;
            if input_bytes > MAX_BATCH_INPUT_BYTES {
                return Err(plan_error("batch query input exceeds 64 MiB"));
            }
            let dim = queries.first().map(Vec::len).unwrap_or(0);
            if !queries.is_empty() && dim == 0 {
                return Err(plan_error("query vectors must not be empty"));
            }
            for (index, query) in queries.iter().enumerate() {
                if query.len() != dim || query.iter().any(|value| !value.is_finite()) {
                    return Err(plan_error(format!(
                        "query {index} must have {dim} finite float32 values"
                    )));
                }
            }
            QueryInput::Inline(Arc::new(queries))
        };
        let top_k = positive_arg(args, required - 1, "k")?;
        if top_k > i32::MAX as usize {
            return Err(plan_error("k exceeds the int32 rank limit"));
        }
        let nprobe = if args.len() > required {
            positive_arg(args, required, "nprobe")?
        } else {
            64
        };
        let metric = if args.len() > required + 1 {
            match string_arg(args, required + 1, "metric")?
                .to_ascii_uppercase()
                .as_str()
            {
                "AUTO" => "auto".to_string(),
                "L2" => "L2".to_string(),
                "IP" => "IP".to_string(),
                _ => return Err(plan_error("metric must be auto, L2 or IP")),
            }
        } else {
            "auto".to_string()
        };
        let exact = if args.len() > required + 2 {
            match string_arg(args, required + 2, "rerank")?
                .to_ascii_lowercase()
                .as_str()
            {
                "index" => false,
                "exact" => true,
                _ => return Err(plan_error("rerank must be index or exact")),
            }
        } else {
            false
        };
        let exact_overfetch = if args.len() > required + 3 {
            positive_arg(args, required + 3, "exact_overfetch")?
        } else {
            10
        };
        let options = NativeBatchOptions {
            top_k,
            nprobe,
            exact,
            exact_overfetch,
            ..Default::default()
        };
        options.validate()?;
        Ok(Arc::new(VectorBatchProvider {
            target,
            column,
            input,
            options,
            metric,
        }))
    }
}

fn plan_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

fn string_arg(args: &[Expr], index: usize, name: &str) -> Result<String> {
    match &args[index] {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _)
            if !value.is_empty() =>
        {
            Ok(value.clone())
        }
        _ => Err(plan_error(format!(
            "{name} must be a non-empty string literal"
        ))),
    }
}

fn positive_arg(args: &[Expr], index: usize, name: &str) -> Result<usize> {
    let value = match &args[index] {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value > 0 => *value as u64,
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) if *value > 0 => *value,
        _ => {
            return Err(plan_error(format!(
                "{name} must be a positive integer literal"
            )));
        }
    };
    usize::try_from(value).map_err(|_| plan_error(format!("{name} is too large")))
}

async fn resolve_table(
    state: &SessionState,
    name: &str,
) -> Result<Arc<dyn TableProvider>> {
    let reference = TableReference::parse_str(name);
    state
        .schema_for_ref(reference.clone())?
        .table(reference.table())
        .await?
        .ok_or_else(|| plan_error(format!("table '{name}' not found")))
}

#[async_trait]
impl TableProvider for VectorBatchProvider {
    fn schema(&self) -> SchemaRef {
        output_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = session
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                plan_error("vector batch requires a DataFusion SessionState")
            })?;
        let target = resolve_table(state, &self.target).await?;
        if target
            .as_ref()
            .downcast_ref::<LakeSoulTableProvider>()
            .is_none()
        {
            return Err(plan_error("vector batch target must be a LakeSoul table"));
        }
        let child = match &self.input {
            QueryInput::Inline(queries) => Arc::new(
                crate::datasource::file_format::vector_batch_exec::InlineVectorQueriesExec::new(queries.clone()),
            ) as Arc<dyn ExecutionPlan>,
            QueryInput::Table { table, qid, vector } => {
                let source = resolve_table(state, table).await?;
                let schema = source.schema();
                let qid_index = schema.index_of(qid)?;
                let vector_index = schema.index_of(vector)?;
                if qid_index == vector_index {
                    return Err(plan_error(
                        "qid_col and qvec_col must be different columns",
                    ));
                }
                validate_query_schema(&Schema::new(vec![
                    schema.field(qid_index).clone(),
                    schema.field(vector_index).clone(),
                ]))?;
                source
                    .scan(state, Some(&vec![qid_index, vector_index]), &[], None)
                    .await?
            }
        };
        let mut exec: Arc<dyn ExecutionPlan> = Arc::new(VectorBatchSearchExec::try_new(
            target,
            Arc::new(state.clone()),
            self.column.clone(),
            self.metric.clone(),
            self.options.clone(),
            child,
        )?);
        if let Some(projection) = projection {
            let schema = exec.schema();
            let expressions = projection
                .iter()
                .map(|&index| {
                    let name = schema.field(index).name();
                    (Arc::new(Column::new(name, index)) as _, name.clone())
                })
                .collect::<Vec<_>>();
            exec = Arc::new(ProjectionExec::try_new(expressions, exec)?);
        }
        Ok(exec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::expr::Placeholder;
    use datafusion::prelude::lit;

    fn inline_args(json: &str) -> Vec<Expr> {
        vec![
            lit("never_resolve_during_call"),
            lit("vec"),
            lit(json),
            lit(3i64),
        ]
    }

    #[test]
    fn vector_batch_literals_require_no_catalog_io_and_have_fixed_schema() {
        let provider = VectorBatchFunction(false)
            .call(&inline_args("[[1,2],[3,4]]"))
            .unwrap();
        assert_eq!(provider.schema(), output_schema());
        assert!(VectorBatchFunction(false).call(&inline_args("[]")).is_ok());
        let table_args = vec![
            lit("target"),
            lit("vec"),
            lit("source"),
            lit("qid"),
            lit("qvec"),
            lit(5i64),
        ];
        assert!(VectorBatchFunction(true).call(&table_args).is_ok());
    }

    #[test]
    fn vector_batch_rejects_malformed_inputs_and_dynamic_arguments() {
        for json in [
            "null",
            "[null]",
            "[[1,null]]",
            "[[]]",
            "[[1],[1,2]]",
            "[[1e100]]",
            "{}",
        ] {
            assert!(
                VectorBatchFunction(false).call(&inline_args(json)).is_err(),
                "{json}"
            );
        }
        let mut args = inline_args("[[1,2]]");
        args[2] = Expr::Placeholder(Placeholder::new("$1".to_string(), None));
        let error = VectorBatchFunction(false)
            .call(&args)
            .unwrap_err()
            .to_string();
        assert!(error.contains("placeholders"), "{error}");
        args = inline_args("[[1,2]]");
        args[3] = lit(0i64);
        assert!(VectorBatchFunction(false).call(&args).is_err());
        args[3] = lit(i32::MAX as i64 + 1);
        assert!(VectorBatchFunction(false).call(&args).is_err());
        args = inline_args("[[1,2]]");
        args.extend([lit(0i64), lit("L2"), lit("index")]);
        assert!(VectorBatchFunction(false).call(&args).is_err());
        args[4] = lit(64i64);
        args[5] = lit("cosine");
        assert!(VectorBatchFunction(false).call(&args).is_err());
        args[5] = lit("IP");
        args[6] = lit("invalid");
        assert!(VectorBatchFunction(false).call(&args).is_err());
        args[6] = lit("exact");
        assert!(VectorBatchFunction(false).call(&args).is_ok());
        args.push(lit(1i64));
        assert!(VectorBatchFunction(false).call(&args).is_ok());
        args[7] = lit(0i64);
        assert!(VectorBatchFunction(false).call(&args).is_err());
        let too_many = format!("[{}]", vec!["[1]"; MAX_BATCH_QUERIES + 1].join(","));
        assert!(
            VectorBatchFunction(false)
                .call(&inline_args(&too_many))
                .unwrap_err()
                .to_string()
                .contains("at most")
        );
    }

    #[test]
    fn vector_batch_config_has_bounded_memory_and_concurrency() {
        let mut config = VectorBatchConfig::default();
        config.validate().unwrap();
        config.query_window = 257;
        assert!(config.validate().is_err());
        config.query_window = 1;
        config.parallelism = 257;
        assert!(config.validate().is_err());
        config.parallelism = 0;
        config.memory_bytes = 0;
        assert!(config.validate().is_err());
        config.memory_bytes = crate::vector_batch::native::DEFAULT_WORK_MEMORY + 1;
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn vector_batch_registration_does_not_capture_the_original_session() {
        let context = SessionContext::new();
        register(&context);
        let state = context.state();
        drop(context);
        let final_context = SessionContext::new_with_state(state);
        final_context
            .sql("SET lakesoul_vector_batch.query_window = '32'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        final_context
            .sql("SET lakesoul_vector_batch.parallelism = '2'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            final_context
                .state()
                .config_options()
                .extensions
                .get::<VectorBatchConfig>()
                .unwrap()
                .query_window,
            32
        );
        let plan = final_context.sql("SELECT * FROM lakesoul_vector_search_batch('missing', 'vec', '[[1,2]]', 2)").await.unwrap();
        assert_eq!(plan.schema().as_arrow(), output_schema().as_ref());
        let error = plan.collect().await.unwrap_err().to_string();
        assert!(error.contains("missing"), "{error}");
    }
}
