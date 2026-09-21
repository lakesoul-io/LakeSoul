// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Translation of the small ES query subset used by delete/update into
//! DataFusion filters, and primary-key resolution.

use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, col, lit};
use serde_json::Value;

use crate::error::EsError;
use crate::schema::PK_COLUMN;
use crate::state::{GatewayState, IndexRuntime};

/// Extract `(field, values)` terms constraints from a query.
///
/// Supports `terms`, `term` and `bool.filter`/`bool.must` conjunctions; the
/// `match_all` query is rejected because a delete/update must select rows.
pub fn extract_filters(
    query: &Value,
    runtime: &IndexRuntime,
) -> Result<Vec<(String, Vec<Value>)>, EsError> {
    let mut out = Vec::new();
    walk(query, runtime, &mut out)?;
    if out.is_empty() {
        return Err(EsError::bad_request(
            "delete/update requires a terms or term filter",
        ));
    }
    Ok(out)
}

fn walk(
    node: &Value,
    runtime: &IndexRuntime,
    out: &mut Vec<(String, Vec<Value>)>,
) -> Result<(), EsError> {
    let map = node
        .as_object()
        .ok_or_else(|| EsError::bad_request("query must be a JSON object"))?;
    if let Some(terms) = map.get("terms") {
        let terms = terms
            .as_object()
            .ok_or_else(|| EsError::bad_request("'terms' must be an object"))?;
        for (field, values) in terms {
            let values = values.as_array().ok_or_else(|| {
                EsError::bad_request(format!(
                    "'terms' values of '{field}' must be an array"
                ))
            })?;
            out.push((normalize_field(field, runtime)?, values.clone()));
        }
        return Ok(());
    }
    if let Some(term) = map.get("term") {
        let term = term
            .as_object()
            .ok_or_else(|| EsError::bad_request("'term' must be an object"))?;
        for (field, value) in term {
            let value = match value {
                Value::Object(inner) => {
                    inner.get("value").cloned().unwrap_or(Value::Null)
                }
                other => other.clone(),
            };
            out.push((normalize_field(field, runtime)?, vec![value]));
        }
        return Ok(());
    }
    if let Some(bool_query) = map.get("bool") {
        let bool_query = bool_query
            .as_object()
            .ok_or_else(|| EsError::bad_request("'bool' must be an object"))?;
        for key in ["filter", "must"] {
            if let Some(clauses) = bool_query.get(key) {
                let clauses = clauses.as_array().ok_or_else(|| {
                    EsError::bad_request(format!("'bool.{key}' must be an array"))
                })?;
                for clause in clauses {
                    walk(clause, runtime, out)?;
                }
            }
        }
        if bool_query.contains_key("must_not") || bool_query.contains_key("should") {
            return Err(EsError::unsupported(
                "only bool.filter and bool.must are supported for delete/update",
            ));
        }
        return Ok(());
    }
    Err(EsError::unsupported(
        "unsupported query: expected terms, term or bool.filter/bool.must",
    ))
}

/// Normalize a filter field (`chunk_id.keyword` → `chunk_id`) and check it
/// exists in the table schema.
fn normalize_field(field: &str, runtime: &IndexRuntime) -> Result<String, EsError> {
    let field = field.strip_suffix(".keyword").unwrap_or(field);
    if field == PK_COLUMN {
        return Err(EsError::unsupported(
            "delete/update by the internal primary key is not supported",
        ));
    }
    if runtime.schema.field_with_name(field).is_err() {
        return Err(EsError::bad_request(format!("unknown field '{field}'")));
    }
    Ok(field.to_string())
}

/// Combine the extracted constraints into one DataFusion filter.
pub fn filters_to_expr(
    filters: &[(String, Vec<Value>)],
    runtime: &IndexRuntime,
) -> Result<Expr, EsError> {
    let mut combined: Option<Expr> = None;
    for (field, values) in filters {
        let data_type = runtime
            .schema
            .field_with_name(field)
            .expect("field validated by extract_filters")
            .data_type()
            .clone();
        let mut literals = Vec::with_capacity(values.len());
        for value in values {
            literals.push(Expr::Literal(scalar_value(&data_type, value, field)?, None));
        }
        let expr = col(field).in_list(literals, false);
        combined = Some(match combined {
            Some(previous) => previous.and(expr),
            None => expr,
        });
    }
    Ok(combined.unwrap_or_else(|| lit(true)))
}

pub(crate) fn scalar_value(
    data_type: &DataType,
    value: &Value,
    field: &str,
) -> Result<ScalarValue, EsError> {
    let invalid = || {
        EsError::bad_request(format!("field '{field}' expects {data_type}, got {value}"))
    };
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => value
            .as_str()
            .map(|value| ScalarValue::Utf8(Some(value.to_string())))
            .ok_or_else(invalid),
        DataType::Int32 => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(|value| ScalarValue::Int32(Some(value)))
            .ok_or_else(invalid),
        DataType::Int64 => value
            .as_i64()
            .map(|value| ScalarValue::Int64(Some(value)))
            .ok_or_else(invalid),
        DataType::UInt64 => value
            .as_u64()
            .map(|value| ScalarValue::UInt64(Some(value)))
            .ok_or_else(invalid),
        DataType::Boolean => value
            .as_bool()
            .map(|value| ScalarValue::Boolean(Some(value)))
            .ok_or_else(invalid),
        other => Err(EsError::unsupported(format!(
            "field '{field}' has unsupported type {other}"
        ))),
    }
}

/// Resolve the current (merge-on-read) rows matching a filter.
///
/// Updates and deletes rewrite **full rows**: partial-column writes would
/// leave the text/embedding columns out of the new data files and break the
/// index auto-build, while a full-row rewrite keeps the unchanged columns
/// and re-indexes them harmlessly.
pub async fn resolve_rows(
    state: &GatewayState,
    runtime: &IndexRuntime,
    filter: Expr,
) -> Result<Option<arrow_array::RecordBatch>, EsError> {
    let dataframe = state
        .session
        .table(runtime.table_ref())
        .await
        .map_err(crate::error::internal)?;
    let dataframe = dataframe.filter(filter).map_err(crate::error::internal)?;
    let columns: Vec<&str> = runtime
        .schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let dataframe = dataframe
        .select_columns(&columns)
        .map_err(crate::error::internal)?;
    let batches = dataframe.collect().await.map_err(crate::error::internal)?;
    let batches: Vec<_> = batches
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect();
    if batches.is_empty() {
        return Ok(None);
    }
    let batch = arrow::compute::concat_batches(&runtime.schema, &batches)
        .map_err(crate::error::internal)?;
    Ok(Some(batch))
}
