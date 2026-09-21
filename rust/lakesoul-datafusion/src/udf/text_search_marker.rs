// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Scalar UDFs for the text-search pushdown.
//!
//! * `text_match(column, query)` is the user-facing exact predicate.  The
//!   pushdown rule rewrites it to `text_match(column, query, tokenizer,
//!   with_positions)` using the table's `text_index_columns` config, so the
//!   residual filter above the index scan uses the same analyzer as the
//!   index.
//! * `__lakesoul_text_search(column, query, top_k)` is an internal marker
//!   appended to `TableScan.filters`; the table provider recognizes it and
//!   switches the scan to the text-index candidate path.  It is never
//!   evaluated.
//!
//! Both `text_match` forms evaluate exactly per batch by building a tiny
//! in-memory Tantivy index over the batch's current texts, which is what
//! makes results exact even when the index returned stale candidates.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, LargeStringArray, StringArray, StringViewArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::common::{DataFusionError, ScalarValue, TableReference};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, LogicalPlan, Operator, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use lakesoul_text::{TextIndexConfig, matching_ids};

/// Function name of the internal text-search marker.
pub const TEXT_SEARCH_MARKER: &str = "__lakesoul_text_search";

/// Function name of the user-facing (and rewritten exact) text predicate.
pub const TEXT_MATCH_FUNCTION: &str = "text_match";

/// Function name of the relevance-ordering expression.
pub const TEXT_SCORE_FUNCTION: &str = "text_score";

/// Parsed marker arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearchRequest {
    /// Text column to search.
    pub column: String,
    /// Raw query string, parsed by the text index.
    pub query: String,
    /// Number of candidates requested (the SQL `LIMIT` value).
    pub top_k: usize,
    /// `ORDER BY text_score(column, query) DESC` was present: the scan must
    /// return the global top-`top_k` ordered by BM25 score.
    pub order_by: bool,
}

/// Build the marker expression appended to a `TableScan.filters` list.
pub fn marker_expr(column: &str, query: &str, top_k: usize, order_by: bool) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        marker_udf(),
        vec![
            Expr::Column(datafusion::common::Column::new_unqualified(
                column.to_string(),
            )),
            Expr::Literal(ScalarValue::Utf8(Some(query.to_string())), None),
            Expr::Literal(ScalarValue::Int64(Some(top_k as i64)), None),
            Expr::Literal(ScalarValue::Boolean(Some(order_by)), None),
        ],
    ))
}

/// Return true if `expr` is the internal text-search marker call.
pub fn is_marker_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call) if call.func.name() == TEXT_SEARCH_MARKER)
}

/// Parse a text-search request from `filters`.
pub fn parse_text_search_request(filters: &[Expr]) -> Option<TextSearchRequest> {
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
            let column = match &call.args[0] {
                Expr::Column(col) => col.name.clone(),
                _ => return None,
            };
            let query = match &call.args[1] {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
                _ => return None,
            };
            let top_k = match &call.args[2] {
                Expr::Literal(ScalarValue::Int64(Some(k)), _) => (*k).max(1) as usize,
                _ => return None,
            };
            let order_by = match &call.args[3] {
                Expr::Literal(ScalarValue::Boolean(Some(order_by)), _) => *order_by,
                _ => return None,
            };
            Some(TextSearchRequest {
                column,
                query,
                top_k,
                order_by,
            })
        })
}

/// The internal marker UDF.  Never evaluated as a physical function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TextSearchMarkerUDF {
    signature: Signature,
}

impl Default for TextSearchMarkerUDF {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for TextSearchMarkerUDF {
    fn name(&self) -> &str {
        TEXT_SEARCH_MARKER
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

/// Return the marker UDF.
pub fn marker_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TextSearchMarkerUDF::default()))
}

/// Extract `(column, query)` from a `text_match(column, query[, ...])` call.
pub fn parse_text_match_call(expr: &Expr) -> Option<(String, String)> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    if call.func.name() != TEXT_MATCH_FUNCTION || call.args.len() < 2 {
        return None;
    }
    let column = match &call.args[0] {
        Expr::Column(col) => col.name.clone(),
        _ => return None,
    };
    let query = match &call.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(q)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(q)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(q)), _) => q.clone(),
        _ => return None,
    };
    Some((column, query))
}

/// Find the first user (two-argument, not yet configured) `text_match` term
/// in a conjunction chain.
pub fn find_user_text_match(expr: &Expr) -> Option<(String, String)> {
    if is_configured_text_match(expr) {
        return None;
    }
    if let Some(pair) = parse_text_match_call(expr) {
        return Some(pair);
    }
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            find_user_text_match(&binary.left)
                .or_else(|| find_user_text_match(&binary.right))
        }
        _ => None,
    }
}

/// Rewrite the `text_match` terms of `predicate` to the analyzer-configured
/// form, resolving the table's `text_index_columns` through `session`.
///
/// Called by the physical planner (the logical optimizer cannot resolve the
/// table provider).  When the table or column has no text index config the
/// predicate is returned unchanged.
pub async fn configure_predicate(
    predicate: &Expr,
    input: &LogicalPlan,
    session: &dyn Session,
) -> DFResult<Expr> {
    if find_user_text_match(predicate).is_none() {
        return Ok(predicate.clone());
    }
    let Some(configs) = table_text_index_configs(input, session).await else {
        return Ok(predicate.clone());
    };
    Ok(rewrite_with_configs(predicate, &configs))
}

async fn table_text_index_configs(
    input: &LogicalPlan,
    session: &dyn Session,
) -> Option<Vec<crate::text_index::TextIndexTableConfig>> {
    use downcast_rs::Downcast;

    let table_ref = table_ref_of(input)?;
    let catalog_list = session.catalog_list();
    let catalog_name = table_ref
        .catalog()
        .unwrap_or(crate::session::DEFAULT_CATALOG);
    let catalog = catalog_list.catalog(catalog_name)?;
    let schema_name = table_ref.schema().unwrap_or(crate::session::DEFAULT_SCHEMA);
    let schema = catalog.schema(schema_name)?;
    let provider = schema.table(table_ref.table()).await.ok()??;
    let provider = provider
        .as_any()
        .downcast_ref::<crate::datasource::table_provider::LakeSoulTableProvider>(
    )?;
    Some(provider.text_index_configs.clone())
}

fn table_ref_of(input: &LogicalPlan) -> Option<TableReference> {
    match input {
        LogicalPlan::TableScan(scan) => Some(scan.table_name.clone()),
        LogicalPlan::Projection(projection) => table_ref_of(&projection.input),
        LogicalPlan::Filter(filter) => table_ref_of(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => table_ref_of(&alias.input),
        _ => None,
    }
}

fn rewrite_with_configs(
    expr: &Expr,
    configs: &[crate::text_index::TextIndexTableConfig],
) -> Expr {
    if !is_configured_text_match(expr)
        && let Some((column, query)) = parse_text_match_call(expr)
        && let Some(config) = configs.iter().find(|config| config.column == column)
    {
        return configured_text_match_expr(
            &column,
            &query,
            &config.to_text_index_config(),
        );
    }
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::And
    {
        return Expr::BinaryExpr(BinaryExpr::new(
            Box::new(rewrite_with_configs(&binary.left, configs)),
            Operator::And,
            Box::new(rewrite_with_configs(&binary.right, configs)),
        ));
    }
    expr.clone()
}

/// Whether `expr` is a `text_match` call already carrying the analyzer
/// configuration (four arguments).
pub fn is_configured_text_match(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call)
        if call.func.name() == TEXT_MATCH_FUNCTION && call.args.len() == 4)
}

/// The rewritten call carrying the table's analyzer configuration.
pub fn configured_text_match_expr(
    column: &str,
    query: &str,
    config: &TextIndexConfig,
) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        text_match_udf(),
        vec![
            Expr::Column(datafusion::common::Column::new_unqualified(
                column.to_string(),
            )),
            Expr::Literal(ScalarValue::Utf8(Some(query.to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some(config.tokenizer.clone())), None),
            Expr::Literal(ScalarValue::Boolean(Some(config.with_positions)), None),
        ],
    ))
}

/// The exact `text_match` UDF (2 or 4 arguments).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TextMatchUDF {
    signature: Signature,
}

impl Default for TextMatchUDF {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TextMatchUDF {
    fn name(&self) -> &str {
        TEXT_MATCH_FUNCTION
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// `text_match` accepts `(text, query)` and the rewritten
    /// `(text, query, tokenizer, with_positions)`; argument types are
    /// accepted as-is (the text column may be Utf8, LargeUtf8 or Utf8View).
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        for (index, data_type) in arg_types.iter().enumerate() {
            let accepted = match index {
                0 | 1 => matches!(
                    data_type,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ),
                2 => matches!(data_type, DataType::Utf8),
                3 => matches!(data_type, DataType::Boolean),
                _ => false,
            };
            if !accepted {
                return Err(DataFusionError::Execution(format!(
                    "text_match unsupported argument {index}: {data_type}"
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
        if args.args.len() < 2 {
            return Err(DataFusionError::Execution(
                "text_match requires (text, query[, tokenizer, with_positions])"
                    .to_string(),
            ));
        }
        let text_array = args.args[0].clone().into_array(args.number_rows)?;
        let query = literal_string(&args.args[1]).ok_or_else(|| {
            DataFusionError::Execution(
                "text_match query must be a constant string".to_string(),
            )
        })?;
        let tokenizer = args
            .args
            .get(2)
            .and_then(literal_string)
            .unwrap_or_else(|| lakesoul_text::config::DEFAULT_TOKENIZER.to_string());
        let with_positions = args.args.get(3).and_then(literal_bool).unwrap_or(true);

        let config = TextIndexConfig {
            column_name: "text".to_string(),
            tokenizer,
            with_positions,
            stored: false,
        };
        let rows = row_texts(&text_array)?;
        let matched = matching_ids(&config, &rows, &query).map_err(|error| {
            DataFusionError::Execution(format!("text_match failed: {error}"))
        })?;
        let mask: Vec<bool> = (0..rows.len())
            .map(|row| matched.contains(&(row as u64)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(mask))))
    }
}

/// Return the `text_match` UDF.
pub fn text_match_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TextMatchUDF::default()))
}

/// Extract `(column, query)` from a `text_score(column, query)` call.
pub fn parse_text_score_call(expr: &Expr) -> Option<(String, String)> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    if call.func.name() != TEXT_SCORE_FUNCTION || call.args.len() != 2 {
        return None;
    }
    let column = match &call.args[0] {
        Expr::Column(col) => col.name.clone(),
        _ => return None,
    };
    let query = match &call.args[1] {
        Expr::Literal(ScalarValue::Utf8(Some(q)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(q)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(q)), _) => q.clone(),
        _ => return None,
    };
    Some((column, query))
}

/// The relevance expression.  It is only meaningful when the pushdown rule
/// removes the `Sort` node and the index scan provides the scores, so its
/// physical implementation is never expected to run.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TextScoreUDF {
    signature: Signature,
}

impl Default for TextScoreUDF {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TextScoreUDF {
    fn name(&self) -> &str {
        TEXT_SCORE_FUNCTION
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() != 2
            || !matches!(
                arg_types[0],
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
            || !matches!(arg_types[1], DataType::Utf8)
        {
            return Err(DataFusionError::Execution(format!(
                "text_score expects (text, query), got {arg_types:?}"
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float32)
    }

    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        Err(DataFusionError::Execution(format!(
            "{TEXT_SCORE_FUNCTION}() is only supported as `ORDER BY \
             {TEXT_SCORE_FUNCTION}(column, query) DESC` together with a \
             `{TEXT_MATCH_FUNCTION}(column, query)` filter and a LIMIT; \
             read scores from the scan API instead (text_search_scores)"
        )))
    }
}

/// Return the `text_score` UDF.
pub fn text_score_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TextScoreUDF::default()))
}

fn literal_string(value: &ColumnarValue) -> Option<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(value)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(value))) => {
            Some(value.clone())
        }
        _ => None,
    }
}

fn literal_bool(value: &ColumnarValue) -> Option<bool> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))) => Some(*value),
        _ => None,
    }
}

/// Row-index-keyed texts of one batch.
fn row_texts(array: &arrow::array::ArrayRef) -> DFResult<Vec<(u64, Option<String>)>> {
    let values: Vec<Option<String>> = match array.data_type() {
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<StringArray>().unwrap();
            (0..values.len())
                .map(|i| (!values.is_null(i)).then(|| values.value(i).to_string()))
                .collect()
        }
        DataType::LargeUtf8 => {
            let values = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            (0..values.len())
                .map(|i| (!values.is_null(i)).then(|| values.value(i).to_string()))
                .collect()
        }
        DataType::Utf8View => {
            let values = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            (0..values.len())
                .map(|i| (!values.is_null(i)).then(|| values.value(i).to_string()))
                .collect()
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "text_match requires a Utf8 column, got {other}"
            )));
        }
    };
    Ok(values
        .into_iter()
        .enumerate()
        .map(|(row, text)| (row as u64, text))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_roundtrips_through_filters() {
        let expr = marker_expr("body", "hello world", 7, true);
        assert!(is_marker_expr(&expr));
        let request = parse_text_search_request(&[expr]).unwrap();
        assert_eq!(request.column, "body");
        assert_eq!(request.query, "hello world");
        assert_eq!(request.top_k, 7);
        assert!(request.order_by);
    }

    #[test]
    fn parses_text_score_calls() {
        let score = Expr::ScalarFunction(ScalarFunction::new_udf(
            text_score_udf(),
            vec![
                Expr::Column(datafusion::common::Column::new_unqualified("body")),
                Expr::Literal(ScalarValue::Utf8(Some("hi".to_string())), None),
            ],
        ));
        assert_eq!(
            parse_text_score_call(&score),
            Some(("body".to_string(), "hi".to_string()))
        );
        assert_eq!(
            parse_text_score_call(&Expr::Literal(ScalarValue::Int64(Some(1)), None,)),
            None
        );
    }

    #[test]
    fn parses_user_and_configured_match_calls() {
        let user = Expr::ScalarFunction(ScalarFunction::new_udf(
            text_match_udf(),
            vec![
                Expr::Column(datafusion::common::Column::new_unqualified("body")),
                Expr::Literal(ScalarValue::Utf8(Some("hi".to_string())), None),
            ],
        ));
        assert_eq!(
            parse_text_match_call(&user),
            Some(("body".to_string(), "hi".to_string()))
        );
        assert!(!is_configured_text_match(&user));

        let config = TextIndexConfig {
            column_name: "body".to_string(),
            tokenizer: "en_stem".to_string(),
            with_positions: true,
            stored: false,
        };
        let configured = configured_text_match_expr("body", "hi", &config);
        assert!(is_configured_text_match(&configured));
    }
}
