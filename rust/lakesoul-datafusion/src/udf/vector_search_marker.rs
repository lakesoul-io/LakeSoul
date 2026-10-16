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
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Function name of the internal vector-search marker.
pub const VECTOR_SEARCH_MARKER: &str = "__lakesoul_vector_search";

/// Function name of the internal multi-column vector-search marker.
pub const VECTOR_SEARCH_MULTI_MARKER: &str = "__lakesoul_vector_search_multi";

/// One weighted vector column of a search request.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VectorColumnQuery {
    /// Name of the vector column to search.
    pub column: String,
    /// Query vector serialized as a comma-separated list of `f32` values.
    pub query_csv: String,
    /// Distance metric of this column: `"L2"` or `"IP"`.
    pub metric: String,
    /// Fusion weight used when several columns are combined.
    #[serde(default = "default_weight")]
    pub weight: f32,
}

fn default_weight() -> f32 {
    1.0
}

/// Parsed marker arguments: the weighted columns, the per-bucket candidate
/// count and the ordering flags.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchRequest {
    /// One entry per searched column; a single-column search has one entry.
    pub columns: Vec<VectorColumnQuery>,
    /// Number of candidates requested per bucket (the SQL `LIMIT` value).
    pub top_k: usize,
    /// Order the candidates by the index distance instead of the exact
    /// re-rank over the vector column (index-only ordering).
    pub order_by: bool,
    /// Per-bucket candidate multiplier, chosen by the pushdown rule from the
    /// plan shape (index-only ordering needs far fewer candidates than the
    /// exact re-rank).
    pub overfetch: usize,
    /// True when the plan has **no filter** between the scan and the sort, so
    /// the rows this scan keeps are exactly the rows the plan above ranks.
    /// The scan can then read the payload columns for those rows only
    /// (deferred payload read).
    pub narrow: bool,
}

impl VectorSearchRequest {
    /// The number of candidates requested per bucket.
    pub fn candidates_per_bucket(&self) -> usize {
        if self.order_by {
            self.top_k.saturating_mul(self.overfetch.max(1))
        } else {
            self.top_k.saturating_mul(10).max(100)
        }
    }

    /// True when the request searches a single column.
    pub fn is_single(&self) -> bool {
        self.columns.len() == 1
    }
}

/// Build the marker expression appended to a `TableScan.filters` list.
pub fn marker_expr(
    vec_column: &str,
    query: &[f32],
    top_k: usize,
    metric: &str,
    order_by: bool,
    overfetch: usize,
    narrow: bool,
) -> Expr {
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
            Expr::Literal(
                datafusion::common::ScalarValue::Boolean(Some(order_by)),
                None,
            ),
            Expr::Literal(
                datafusion::common::ScalarValue::Int64(Some(overfetch as i64)),
                None,
            ),
            Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(narrow)), None),
        ],
    ))
}

/// Build the multi-column marker expression appended to a
/// `TableScan.filters` list.  The columns travel as a JSON payload.
pub fn multi_marker_expr(
    columns: &[VectorColumnQuery],
    top_k: usize,
    order_by: bool,
    overfetch: usize,
    narrow: bool,
) -> Expr {
    let payload = serde_json::to_string(columns).unwrap_or_else(|_| "[]".to_string());
    Expr::ScalarFunction(ScalarFunction::new_udf(
        multi_marker_udf(),
        vec![
            Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(payload)), None),
            Expr::Literal(
                datafusion::common::ScalarValue::Int64(Some(top_k as i64)),
                None,
            ),
            Expr::Literal(
                datafusion::common::ScalarValue::Boolean(Some(order_by)),
                None,
            ),
            Expr::Literal(
                datafusion::common::ScalarValue::Int64(Some(overfetch as i64)),
                None,
            ),
            Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(narrow)), None),
        ],
    ))
}

/// Return true if `expr` is one of the internal vector-search markers.
pub fn is_marker_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(call)
            if call.func.name() == VECTOR_SEARCH_MARKER
                || call.func.name() == VECTOR_SEARCH_MULTI_MARKER
    )
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
            if call.func.name() == VECTOR_SEARCH_MULTI_MARKER {
                return parse_multi_marker(call);
            }
            if call.args.len() != 7 {
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
            let order_by = match &call.args[4] {
                Expr::Literal(
                    datafusion::common::ScalarValue::Boolean(Some(value)),
                    _,
                ) => *value,
                _ => return None,
            };
            let overfetch = match &call.args[5] {
                Expr::Literal(datafusion::common::ScalarValue::Int64(Some(value)), _) => {
                    (*value).max(1) as usize
                }
                _ => return None,
            };
            let narrow = match &call.args[6] {
                Expr::Literal(
                    datafusion::common::ScalarValue::Boolean(Some(value)),
                    _,
                ) => *value,
                _ => return None,
            };
            Some(VectorSearchRequest {
                columns: vec![VectorColumnQuery {
                    column: vec_column,
                    query_csv,
                    metric,
                    weight: 1.0,
                }],
                top_k,
                order_by,
                overfetch,
                narrow,
            })
        })
}

/// Parse the JSON payload of the multi-column marker.
fn parse_multi_marker(call: &ScalarFunction) -> Option<VectorSearchRequest> {
    if call.args.len() != 5 {
        return None;
    }
    let payload = match &call.args[0] {
        Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(s)), _) => s,
        _ => return None,
    };
    let columns: Vec<VectorColumnQuery> = serde_json::from_str(payload).ok()?;
    if columns.is_empty() {
        return None;
    }
    let top_k = match &call.args[1] {
        Expr::Literal(datafusion::common::ScalarValue::Int64(Some(k)), _) => {
            (*k).max(1) as usize
        }
        _ => return None,
    };
    let order_by = match &call.args[2] {
        Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(value)), _) => *value,
        _ => return None,
    };
    let overfetch = match &call.args[3] {
        Expr::Literal(datafusion::common::ScalarValue::Int64(Some(value)), _) => {
            (*value).max(1) as usize
        }
        _ => return None,
    };
    let narrow = match &call.args[4] {
        Expr::Literal(datafusion::common::ScalarValue::Boolean(Some(value)), _) => *value,
        _ => return None,
    };
    Some(VectorSearchRequest {
        columns,
        top_k,
        order_by,
        overfetch,
        narrow,
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

    /// `__lakesoul_vector_search` accepts `(vec, query, top_k, metric,
    /// order_by, overfetch, narrow)`; the vector column keeps whatever type it
    /// has (any `FixedSizeList`/`List` of floats) and the rest are the
    /// literals the pushdown rule produces.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() != 7 {
            return Err(DataFusionError::Execution(format!(
                "__lakesoul_vector_search expects 7 arguments, got {}",
                arg_types.len()
            )));
        }
        for (index, data_type) in arg_types.iter().enumerate() {
            let accepted = match index {
                0 => true,
                1 | 3 => matches!(data_type, DataType::Utf8),
                2 | 5 => matches!(data_type, DataType::Int64),
                4 | 6 => matches!(data_type, DataType::Boolean),
                _ => false,
            };
            if !accepted {
                return Err(DataFusionError::Execution(format!(
                    "__lakesoul_vector_search unsupported argument {index}: {data_type}"
                )));
            }
        }
        Ok(arg_types.to_vec())
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

/// The internal multi-column marker UDF.  Never evaluated as a physical
/// function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorSearchMultiMarkerUDF {
    signature: Signature,
}

impl Default for VectorSearchMultiMarkerUDF {
    fn default() -> Self {
        Self {
            // Volatile so the optimizer's simplify_expressions pass never
            // constant-folds the marker call into a literal `true`.
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for VectorSearchMultiMarkerUDF {
    fn name(&self) -> &str {
        VECTOR_SEARCH_MULTI_MARKER
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// `__lakesoul_vector_search_multi` accepts `(requests_json, top_k,
    /// order_by, overfetch, narrow)`.
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() != 5 {
            return Err(DataFusionError::Execution(format!(
                "__lakesoul_vector_search_multi expects 5 arguments, got {}",
                arg_types.len()
            )));
        }
        for (index, data_type) in arg_types.iter().enumerate() {
            let accepted = match index {
                0 => matches!(data_type, DataType::Utf8),
                1 | 3 => matches!(data_type, DataType::Int64),
                2 | 4 => matches!(data_type, DataType::Boolean),
                _ => false,
            };
            if !accepted {
                return Err(DataFusionError::Execution(format!(
                    "__lakesoul_vector_search_multi unsupported argument {index}: {data_type}"
                )));
            }
        }
        Ok(arg_types.to_vec())
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

/// Return the multi-column marker UDF.
pub fn multi_marker_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(
        VectorSearchMultiMarkerUDF::default(),
    ))
}

/// How the vector search orders its candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RerankMode {
    /// Exact re-rank over the stored vector column (default).
    #[default]
    Exact,
    /// Order by the quantized distance computed inside the index; the vector
    /// column is not read at all.
    Index,
}

datafusion::common::extensions_options! {
    /// Session-level LakeSoul vector-search options.
    ///
    /// Registered on the session `ConfigOptions` so both the logical
    /// pushdown rule and the scan path can read them, and so they can be set
    /// from SQL (`SET lakesoul_vector_search.rerank = 'index'`):
    /// `session_config.options_mut().extensions.insert(LakeSoulVectorSearchConfig::default())`.
    pub struct LakeSoulVectorSearchConfig {
        /// Number of IVF clusters to probe in each bucket index.
        pub nprobe: usize, default = 64
        /// Candidate ordering: `"exact"` re-ranks against the stored vector
        /// column; `"index"` orders by the index distance and never reads it.
        pub rerank: String, default = String::new()
        /// Clusters probed concurrently per search (1 = sequential).  The
        /// probed clusters are partitioned into that many groups and merged,
        /// so the result set is unchanged; it trades per-query latency for
        /// throughput because the groups share the thread pool.
        pub parallelism: usize, default = 1
        /// Read the payload columns only for the rows the plan above keeps
        /// (`LIMIT`): the scan ranks the candidates by the vector columns,
        /// keeps the top ones and fetches their payload in a second pass.
        /// Disable to trade IO for a single read.
        pub deferred_payload: bool, default = true
    }
}

impl datafusion::common::config::ConfigExtension for LakeSoulVectorSearchConfig {
    const PREFIX: &'static str = "lakesoul_vector_search";
}

impl LakeSoulVectorSearchConfig {
    /// The parsed candidate ordering mode (`"exact"` unless `"index"`).
    pub fn rerank_mode(&self) -> RerankMode {
        if self.rerank.eq_ignore_ascii_case("index") {
            RerankMode::Index
        } else {
            RerankMode::Exact
        }
    }
}
