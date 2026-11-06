// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Internal distance function that carries its query vector by handle.
//!
//! `ORDER BY array_distance(vec, ARRAY[…]) LIMIT k` reaches the analyzer with
//! the query vector spelled out as a list literal.  Even after the literal is
//! folded into one node, the optimizer's work before the pushdown rewrite
//! scales with the size of that literal (measured: ~0.16 ms of the ~1.9 ms a
//! single-query statement spends planning with a 200-dimension vector against
//! 0.03 ms for a two-element one).  The analyzer therefore rewrites the call
//! into [`VECTOR_DISTANCE_HANDLE`], which carries an 8-byte handle instead; the
//! pushdown rule resolves the handle when it builds the index search request.
//!
//! The handle function still evaluates, by delegating to the built-in
//! `array_distance` / `inner_product`, so a statement whose shape is not
//! rewritten into an index search keeps exactly the same results.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// Function name of the internal handle-carrying distance call.
pub const VECTOR_DISTANCE_HANDLE: &str = "__lakesoul_vector_distance";

/// Prefix of a query-vector handle.
pub const QUERY_HANDLE_PREFIX: &str = "@q";

/// How many query vectors stay addressable.
///
/// A handle only has to outlive the planning and execution of the statement
/// that created it, so the bound is far larger than the number of statements in
/// flight; it only keeps the map from growing without limit.
const QUERY_HANDLES_CAPACITY: usize = 4096;

struct QueryHandles {
    next: u64,
    entries: HashMap<u64, Arc<Vec<f32>>>,
    order: std::collections::VecDeque<u64>,
}

static QUERY_HANDLES: LazyLock<Mutex<QueryHandles>> = LazyLock::new(|| {
    Mutex::new(QueryHandles {
        next: 0,
        entries: HashMap::new(),
        order: std::collections::VecDeque::new(),
    })
});

/// Store one query vector and return the handle that stands for it.
pub fn register_query_vector(query: &[f32]) -> String {
    let mut handles = QUERY_HANDLES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    handles.next += 1;
    let id = handles.next;
    while handles.order.len() >= QUERY_HANDLES_CAPACITY {
        let Some(evicted) = handles.order.pop_front() else {
            break;
        };
        handles.entries.remove(&evicted);
    }
    handles.order.push_back(id);
    handles.entries.insert(id, Arc::new(query.to_vec()));
    format!("{QUERY_HANDLE_PREFIX}{id:016x}")
}

/// Resolve a handle back into the query vector.
pub fn resolve_query_vector(handle: &str) -> Option<Arc<Vec<f32>>> {
    let id = u64::from_str_radix(handle.strip_prefix(QUERY_HANDLE_PREFIX)?, 16).ok()?;
    let handles = QUERY_HANDLES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    handles.entries.get(&id).cloned()
}

/// Return true when `name` is the handle-carrying distance function.
pub fn is_handle_distance_name(name: &str) -> bool {
    name == VECTOR_DISTANCE_HANDLE
}

/// The metric stored in a handle call (`"L2"` / `"IP"`).
pub fn handle_metric(call: &ScalarFunction) -> Option<&'static str> {
    if !is_handle_distance_name(call.func.name()) || call.args.len() != 3 {
        return None;
    }
    match &call.args[2] {
        Expr::Literal(ScalarValue::Utf8(Some(metric)), _) => match metric.as_str() {
            "L2" => Some("L2"),
            "IP" => Some("IP"),
            _ => None,
        },
        _ => None,
    }
}

/// The query-vector handle stored in a handle call.
pub fn handle_query(call: &ScalarFunction) -> Option<&str> {
    if !is_handle_distance_name(call.func.name()) || call.args.len() != 3 {
        return None;
    }
    match &call.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(handle)), _) => Some(handle.as_str()),
        _ => None,
    }
}

/// Build the handle-carrying distance call.
pub fn distance_handle_expr(column: &Expr, handle: &str, metric: &str) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        distance_handle_udf(),
        vec![
            column.clone(),
            Expr::Literal(ScalarValue::Utf8(Some(handle.to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some(metric.to_string())), None),
        ],
    ))
}

/// The internal distance UDF.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorDistanceHandleUdf {
    signature: Signature,
}

impl Default for VectorDistanceHandleUdf {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VectorDistanceHandleUdf {
    fn name(&self) -> &str {
        VECTOR_DISTANCE_HANDLE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The vector column is coerced exactly like the built-in distance
    /// functions coerce their first argument, so the value the delegated call
    /// sees is the one an unrewritten plan would have seen.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() != 3 {
            return Err(DataFusionError::Execution(format!(
                "{VECTOR_DISTANCE_HANDLE} expects 3 arguments, got {}",
                arg_types.len()
            )));
        }
        let mut coerced = builtin_distance_udf("L2")
            .coerce_types(&[arg_types[0].clone(), arg_types[0].clone()])?;
        let vector_type = coerced.remove(0);
        for (index, data_type) in arg_types.iter().enumerate().skip(1) {
            if !matches!(data_type, DataType::Utf8) {
                return Err(DataFusionError::Execution(format!(
                    "{VECTOR_DISTANCE_HANDLE} unsupported argument {index}: {data_type}"
                )));
            }
        }
        Ok(vec![vector_type, DataType::Utf8, DataType::Utf8])
    }

    /// The built-in functions return `Float64`.
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let handles = args
            .args
            .get(1)
            .and_then(|value| match value {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(handle))) => {
                    Some(handle.clone())
                }
                _ => None,
            })
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{VECTOR_DISTANCE_HANDLE} expects a scalar query handle"
                ))
            })?;
        let metric = match args.args.get(2) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(metric)))) => {
                metric.clone()
            }
            _ => {
                return exec_err!("{VECTOR_DISTANCE_HANDLE} expects a scalar metric");
            }
        };
        let query = resolve_query_vector(&handles).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{VECTOR_DISTANCE_HANDLE} no longer knows query handle {handles}; \
                 re-plan the statement"
            ))
        })?;
        // The built-in functions see a plain query list, exactly as an
        // unrewritten plan would have passed it.
        let element_type = match args.arg_fields.first().map(|field| field.data_type()) {
            Some(DataType::List(item)) | Some(DataType::LargeList(item)) => {
                item.data_type().clone()
            }
            Some(DataType::FixedSizeList(item, _)) => item.data_type().clone(),
            _ => DataType::Float64,
        };
        let values: Vec<ScalarValue> = if matches!(element_type, DataType::Float32) {
            query
                .iter()
                .map(|value| ScalarValue::Float32(Some(*value)))
                .collect()
        } else {
            query
                .iter()
                .map(|value| ScalarValue::Float64(Some(*value as f64)))
                .collect()
        };
        let list = ScalarValue::List(ScalarValue::new_list(&values, &element_type, true));
        let list_type = list.data_type();
        let delegated = ScalarFunctionArgs {
            args: vec![args.args[0].clone(), ColumnarValue::Scalar(list)],
            arg_fields: vec![
                args.arg_fields[0].clone(),
                Arc::new(Field::new("query", list_type, true)) as FieldRef,
            ],
            number_rows: args.number_rows,
            return_field: args.return_field.clone(),
            config_options: args.config_options.clone(),
        };
        builtin_distance_udf(if metric == "IP" { "IP" } else { "L2" })
            .invoke_with_args(delegated)
    }
}

/// The built-in distance UDF for a metric.
fn builtin_distance_udf(metric: &str) -> Arc<ScalarUDF> {
    if metric == "IP" {
        datafusion::functions_nested::inner_product::inner_product_udf()
    } else {
        datafusion::functions_nested::distance::array_distance_udf()
    }
}

/// Return the handle-carrying distance UDF.
pub fn distance_handle_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(VectorDistanceHandleUdf::default()))
}

/// The `Float64Array`-shaped query list used by the delegated call, for tests.
#[cfg(test)]
fn query_list(query: &[f32]) -> ScalarValue {
    let values = query
        .iter()
        .map(|value| ScalarValue::Float64(Some(*value as f64)))
        .collect::<Vec<_>>();
    ScalarValue::List(ScalarValue::new_list(&values, &DataType::Float64, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::ColumnarValue;

    #[test]
    fn handle_round_trips() {
        let handle = register_query_vector(&[0.5, -0.25, 1.0]);
        let query = resolve_query_vector(&handle).expect("handle resolves");
        assert_eq!(query.as_ref(), &[0.5, -0.25, 1.0]);
    }

    #[test]
    fn evaluation_matches_the_builtin() {
        let query = [0.5f32, -0.25, 1.0];
        let handle = register_query_vector(&query);
        let row = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Float32(Some(1.0)),
                ScalarValue::Float32(Some(2.0)),
                ScalarValue::Float32(Some(3.0)),
            ],
            &DataType::Float32,
            true,
        ));
        let column = ColumnarValue::Array(row.to_array_of_size(1).expect("column"));
        let field: FieldRef = Arc::new(Field::new("vec", column.data_type(), true));
        let udf = distance_handle_udf();
        let ours = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    column.clone(),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(handle))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("L2".to_string()))),
                ],
                arg_fields: vec![field.clone(), field.clone(), field.clone()],
                number_rows: 1,
                return_field: Arc::new(Field::new("d", DataType::Float64, true)),
                config_options: Arc::new(Default::default()),
            })
            .expect("handle call evaluates");
        let builtin = builtin_distance_udf("L2")
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![column, ColumnarValue::Scalar(query_list(&query))],
                arg_fields: vec![field.clone(), field],
                number_rows: 1,
                return_field: Arc::new(Field::new("d", DataType::Float64, true)),
                config_options: Arc::new(Default::default()),
            })
            .expect("builtin evaluates");
        let ours = ours.into_array(1).expect("array");
        let builtin = builtin.into_array(1).expect("array");
        let ours = ours
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .expect("f64");
        let builtin = builtin
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .expect("f64");
        assert!((ours.value(0) - builtin.value(0)).abs() < 1e-12);
    }
}
