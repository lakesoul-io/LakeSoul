// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! SQL entry point for dropping a vector index.
//!
//! This is a scalar DDL function (not a table function) so it can run through
//! the normal expression evaluator:
//!
//! ```sql
//! SELECT lakesoul_drop_vector_index(
//!     'docs', 'default', 'embedding',
//!     false,  -- keep_declaration
//!     true,   -- purge_files
//!     0.0,    -- grace seconds
//!     true,   -- wait_for_cleanup
//!     false   -- dry_run
//! );
//! ```
//!
//! It returns a JSON report.  `LAKESOUL_ALLOW_INDEX_DDL=1` is required unless
//! `dry_run` is true, so a read-only SQL deployment does not expose a
//! destructive side effect by default.

use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use lakesoul_metadata::MetaDataClient;

use crate::vector_index::{DropVectorIndexOptions, drop_vector_index};

pub const DROP_VECTOR_INDEX_FUNCTION: &str = "lakesoul_drop_vector_index";

#[derive(Debug, PartialEq, Eq, Hash)]
struct DropVectorIndexUdf {
    signature: Signature,
    object_store_options: Vec<(String, String)>,
}

impl DropVectorIndexUdf {
    fn new(object_store_options: Vec<(String, String)>) -> Self {
        Self {
            object_store_options,
            signature: Signature::exact(
                vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Boolean,
                    DataType::Boolean,
                    DataType::Float64,
                    DataType::Boolean,
                    DataType::Boolean,
                ],
                Volatility::Volatile,
            ),
        }
    }
}

fn scalar_string(args: &ScalarFunctionArgs, index: usize) -> DFResult<Option<String>> {
    match args.args.get(index) {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(value))) => Ok(value.clone()),
        Some(ColumnarValue::Scalar(ScalarValue::LargeUtf8(value))) => Ok(value.clone()),
        _ => Err(DataFusionError::Execution(format!(
            "{DROP_VECTOR_INDEX_FUNCTION} argument {} must be a string literal",
            index + 1
        ))),
    }
}

fn scalar_bool(args: &ScalarFunctionArgs, index: usize) -> DFResult<bool> {
    match args.args.get(index) {
        Some(ColumnarValue::Scalar(ScalarValue::Boolean(value))) => {
            value.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{DROP_VECTOR_INDEX_FUNCTION} argument {} must not be NULL",
                    index + 1
                ))
            })
        }
        _ => Err(DataFusionError::Execution(format!(
            "{DROP_VECTOR_INDEX_FUNCTION} argument {} must be a boolean literal",
            index + 1
        ))),
    }
}

fn scalar_f64(args: &ScalarFunctionArgs, index: usize) -> DFResult<f64> {
    match args.args.get(index) {
        Some(ColumnarValue::Scalar(ScalarValue::Float64(value))) => {
            value.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{DROP_VECTOR_INDEX_FUNCTION} argument {} must not be NULL",
                    index + 1
                ))
            })
        }
        _ => Err(DataFusionError::Execution(format!(
            "{DROP_VECTOR_INDEX_FUNCTION} argument {} must be a float literal",
            index + 1
        ))),
    }
}

impl ScalarUDFImpl for DropVectorIndexUdf {
    fn name(&self) -> &str {
        DROP_VECTOR_INDEX_FUNCTION
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        let table = scalar_string(&args, 0)?.ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{DROP_VECTOR_INDEX_FUNCTION} table name must not be NULL"
            ))
        })?;
        let namespace = scalar_string(&args, 1)?.unwrap_or_else(|| "default".to_string());
        let column = scalar_string(&args, 2)?;
        let keep_declaration = scalar_bool(&args, 3)?;
        let purge_files = scalar_bool(&args, 4)?;
        let grace = scalar_f64(&args, 5)?;
        let wait_for_cleanup = scalar_bool(&args, 6)?;
        let dry_run = scalar_bool(&args, 7)?;
        if !grace.is_finite() || grace < 0.0 {
            return Err(DataFusionError::Execution(
                "grace seconds must be a finite non-negative number".to_string(),
            ));
        }
        if !dry_run {
            let allowed = std::env::var("LAKESOUL_ALLOW_INDEX_DDL")
                .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE"));
            if !allowed {
                return Err(DataFusionError::Execution(
                    "index DDL is disabled; set LAKESOUL_ALLOW_INDEX_DDL=1 \
                     to allow drop from SQL"
                        .to_string(),
                ));
            }
        }
        let object_store_options: std::collections::HashMap<String, String> =
            self.object_store_options.iter().cloned().collect();
        let report = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            runtime.block_on(async move {
                let client = MetaDataClient::from_env()
                    .await
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                drop_vector_index(
                    &client,
                    &table,
                    &namespace,
                    &object_store_options,
                    DropVectorIndexOptions {
                        columns: column.map(|column| vec![column]),
                        keep_declaration,
                        purge_files,
                        dry_run,
                        grace: std::time::Duration::from_secs_f64(grace),
                        wait_for_cleanup,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| DataFusionError::Execution(error.to_string()))
            })
        })
        .join()
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "drop vector index thread panicked: {error:?}"
            ))
        })??;
        let json = serde_json::to_string(&report)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let array = StringArray::from(vec![json; args.number_rows]);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

pub fn udf() -> ScalarUDF {
    udf_with_options(&lakesoul_common::misc::SecretMap::new())
}

/// UDF carrying the warehouse's object-store credentials so SQL DDL works on
/// S3/HDFS tables, not only local paths.
pub fn udf_with_options(options: &lakesoul_common::misc::SecretMap) -> ScalarUDF {
    let mut pairs: Vec<(String, String)> = options
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    pairs.sort();
    ScalarUDF::from(DropVectorIndexUdf::new(pairs))
}
