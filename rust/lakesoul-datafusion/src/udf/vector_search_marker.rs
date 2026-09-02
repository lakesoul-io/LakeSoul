// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Internal marker UDF and session options for the vector-search pushdown.
//!
//! The [`VectorSearchPushdownRule`](crate::planner::vector_search_rule::VectorSearchPushdownRule)
//! rewrites `ORDER BY <distance>(vec, q) LIMIT k` into a LakeSoul
//! vector-index search by appending a marker call to the `TableScan`
//! filters.  The marker is never evaluated: [`LakeSoulTableProvider::scan`]
//! recognizes it, extracts the search parameters, and strips it before the
//! standard filter pipeline.  Its return value (always true) only matters
//! if some planner stage left it behind.

use std::sync::Arc;

use arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Function name of the internal vector-search marker.
pub const VECTOR_SEARCH_MARKER: &str = "__lakesoul_vector_search";

/// Parsed marker arguments: (vector column, query, per-bucket candidate
/// count, distance metric).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchRequest {
    /// Name of the vector column to search.
    pub vec_column: String,
    /// Query vector serialized as a comma-separated list of `f32` values.
    pub query_csv: String,
    /// Number of candidates requested per bucket (the SQL `LIMIT` value).
    pub top_k: usize,
    /// Distance metric: `"L2"` or `"IP"`.
    pub metric: String,
}

/// Build the marker expression appended to a `TableScan.filters` list.
pub fn marker_expr(vec_column: &str, query: &[f32], top_k: usize, metric: &str) -> Expr {
    let query_csv = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    Expr::ScalarFunction(ScalarFunction::new_udf(
        marker_udf(),
        vec![
            Expr::Column(datafusion::common::Column::new_unqualified(
                vec_column.to_string(),
            )),
            Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(query_csv)), None),
            Expr::Literal(
                datafusion::common::ScalarValue::Int64(Some(top_k as i64)),
                None,
            ),
            Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some(metric.to_string())),
                None,
            ),
        ],
    ))
}

/// Return true if `expr` is the internal vector-search marker call.
pub fn is_marker_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call) if call.func.name() == VECTOR_SEARCH_MARKER)
}

/// Parse a vector-search request from `filters`; returns None when no
/// marker is present.
pub fn parse_vector_search_request(filters: &[Expr]) -> Option<VectorSearchRequest> {
    filters
        .iter()
        .find(|f| is_marker_expr(f))
        .and_then(|marker| {
            let Expr::ScalarFunction(call) = marker else {
                return None;
            };
            if call.args.len() != 4 {
                return None;
            }
            let vec_column = match &call.args[0] {
                Expr::Column(col) => col.name.clone(),
                _ => return None,
            };
            let query_csv = match &call.args[1] {
                Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(s)), _) => {
                    s.clone()
                }
                _ => return None,
            };
            let top_k = match &call.args[2] {
                Expr::Literal(datafusion::common::ScalarValue::Int64(Some(k)), _) => {
                    (*k).max(1) as usize
                }
                _ => return None,
            };
            let metric = match &call.args[3] {
                Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(s)), _) => {
                    s.clone()
                }
                _ => return None,
            };
            Some(VectorSearchRequest {
                vec_column,
                query_csv,
                top_k,
                metric,
            })
        })
}

/// The internal marker UDF.  Never evaluated as a physical function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorSearchMarkerUDF {
    signature: Signature,
}

impl Default for VectorSearchMarkerUDF {
    fn default() -> Self {
        Self {
            // Volatile so the optimizer's simplify_expressions pass never
            // constant-folds the marker call into a literal `true`.
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for VectorSearchMarkerUDF {
    fn name(&self) -> &str {
        VECTOR_SEARCH_MARKER
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
            true; args.number_rows
        ]))))
    }
}

/// Return the marker UDF (a new Arc is cheap; it is only used by name).
pub fn marker_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(VectorSearchMarkerUDF::default()))
}

/// Session-level options for LakeSoul vector search.
///
/// Set via `session_config.set_extension(Arc::new(LakeSoulVectorSearchOptions { nprobe: 8 }))`
/// and read in the scan path with
/// `session_state.config().get_extension::<LakeSoulVectorSearchOptions>()`.
#[derive(Debug, Clone)]
pub struct LakeSoulVectorSearchOptions {
    /// Number of IVF clusters to probe in each bucket index.
    pub nprobe: usize,
}

impl Default for LakeSoulVectorSearchOptions {
    fn default() -> Self {
        Self { nprobe: 64 }
    }
}
