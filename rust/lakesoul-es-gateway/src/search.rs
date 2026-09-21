// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `POST /{index}/_search`: keyword (BM25) and filter-only queries.
//!
//! The keyword path searches the Tantivy text index of every shard, merges
//! the candidate scores globally, fetches the current rows, verifies the
//! candidates against the current content (dropping stale ones) and returns
//! `hits` with a numeric `_score` — the contract of an Elasticsearch
//! `_search` response.
//!
//! Filter-only queries (the copy-pagination path) run as a plain filtered
//! scan with `from`/`size` and a zero score.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Int32Array,
    Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray, UInt64Array,
};
use arrow_schema::DataType;
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::logical_expr::{Expr, Operator, binary_expr, col, lit};
use lakesoul_common::IndexKind;
use lakesoul_io::index::IndexLease;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_io::text::reader::collect_text_values;
use lakesoul_io::text::search::search_resolved_shard;
use lakesoul_text::{TextIndexConfig, TextSplitEntry, matching_ids};
use serde_json::{Map, Value, json};

use crate::error::EsError;
use crate::query::scalar_value;
use crate::schema::{CDC_COLUMN, PK_COLUMN};
use crate::state::{GatewayState, IndexRuntime};

/// One scored hit before rendering.
struct Hit {
    id: u64,
    score: f32,
    source: Map<String, Value>,
}

/// `_source` projection of a search request.
#[derive(Debug, Default, Clone)]
struct SourceFilter {
    disabled: bool,
    includes: Option<Vec<String>>,
    excludes: Vec<String>,
}

/// Parsed `_search` body.
struct SearchBody {
    query: Value,
    from: usize,
    size: usize,
    source: SourceFilter,
}

impl SearchBody {
    fn parse(body: &Bytes) -> Result<Self, EsError> {
        let value: Value = if body.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(body).map_err(|error| {
                EsError::bad_request(format!("invalid search body: {error}"))
            })?
        };
        let map = value
            .as_object()
            .ok_or_else(|| EsError::bad_request("search body must be a JSON object"))?;
        Ok(Self {
            query: map.get("query").cloned().unwrap_or_else(|| json!({})),
            from: map.get("from").and_then(Value::as_u64).unwrap_or(0) as usize,
            // The v7 keyword search sends no size; Elasticsearch's default
            // of 10 applies.
            size: map.get("size").and_then(Value::as_u64).unwrap_or(10) as usize,
            source: parse_source(map.get("_source"))?,
        })
    }
}

fn parse_source(value: Option<&Value>) -> Result<SourceFilter, EsError> {
    match value {
        None => Ok(SourceFilter::default()),
        Some(Value::Bool(false)) => Ok(SourceFilter {
            disabled: true,
            ..Default::default()
        }),
        Some(Value::Bool(true)) => Ok(SourceFilter::default()),
        Some(Value::Array(fields)) => Ok(SourceFilter {
            includes: Some(string_list(fields)?),
            ..Default::default()
        }),
        Some(Value::Object(map)) => {
            let includes = map
                .get("includes")
                .or_else(|| map.get("include"))
                .map(|value| {
                    value
                        .as_array()
                        .ok_or_else(|| {
                            EsError::bad_request("_source.includes must be an array")
                        })
                        .and_then(|fields| string_list(fields))
                })
                .transpose()?;
            let excludes = map
                .get("excludes")
                .or_else(|| map.get("exclude"))
                .map(|value| {
                    value
                        .as_array()
                        .ok_or_else(|| {
                            EsError::bad_request("_source.excludes must be an array")
                        })
                        .and_then(|fields| string_list(fields))
                })
                .transpose()?
                .unwrap_or_default();
            Ok(SourceFilter {
                disabled: false,
                includes,
                excludes,
            })
        }
        Some(_) => Err(EsError::bad_request("invalid _source")),
    }
}

fn string_list(fields: &[Value]) -> Result<Vec<String>, EsError> {
    fields
        .iter()
        .map(|field| {
            field
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| EsError::bad_request("_source fields must be strings"))
        })
        .collect()
}

/// The parsed query: an optional scoring `match` plus non-scoring equality
/// constraints.
#[derive(Debug, Default)]
struct ParsedQuery {
    match_query: Option<(String, String)>,
    positive: Vec<(String, Vec<Value>)>,
    negative: Vec<(String, Vec<Value>)>,
}

impl ParsedQuery {
    fn parse(query: &Value, runtime: &IndexRuntime) -> Result<Self, EsError> {
        let mut parsed = Self::default();
        parsed.walk(query, runtime, false)?;
        Ok(parsed)
    }

    fn walk(
        &mut self,
        node: &Value,
        runtime: &IndexRuntime,
        negated: bool,
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
                self.push(field, values.clone(), runtime, negated)?;
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
                self.push(field, vec![value], runtime, negated)?;
            }
            return Ok(());
        }
        if let Some(match_query) = map.get("match") {
            if negated {
                return Err(EsError::unsupported(
                    "match is not supported inside bool.must_not",
                ));
            }
            let match_query = match_query
                .as_object()
                .ok_or_else(|| EsError::bad_request("'match' must be an object"))?;
            let Some((field, value)) = match_query.iter().next() else {
                return Err(EsError::bad_request("'match' must name a field"));
            };
            let query = match value {
                Value::String(query) => query.clone(),
                Value::Object(inner) => inner
                    .get("query")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        EsError::bad_request("'match' query must be a string")
                    })?
                    .to_string(),
                _ => {
                    return Err(EsError::bad_request("'match' query must be a string"));
                }
            };
            self.match_query = Some((field.clone(), query));
            return Ok(());
        }
        if map.contains_key("match_all") {
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
                        self.walk(clause, runtime, negated)?;
                    }
                }
            }
            if let Some(clauses) = bool_query.get("must_not") {
                let clauses = clauses.as_array().ok_or_else(|| {
                    EsError::bad_request("'bool.must_not' must be an array")
                })?;
                for clause in clauses {
                    self.walk(clause, runtime, true)?;
                }
            }
            if bool_query.contains_key("should") {
                return Err(EsError::unsupported(
                    "bool.should is not supported for text search",
                ));
            }
            return Ok(());
        }
        Err(EsError::unsupported(format!("unsupported query: {node}")))
    }

    fn push(
        &mut self,
        field: &str,
        values: Vec<Value>,
        runtime: &IndexRuntime,
        negated: bool,
    ) -> Result<(), EsError> {
        let field = normalize_field(field, runtime)?;
        if negated {
            self.negative.push((field, values));
        } else {
            self.positive.push((field, values));
        }
        Ok(())
    }
}

fn normalize_field(field: &str, runtime: &IndexRuntime) -> Result<String, EsError> {
    let field = field.strip_suffix(".keyword").unwrap_or(field);
    if runtime.schema.field_with_name(field).is_err() {
        return Err(EsError::bad_request(format!("unknown field '{field}'")));
    }
    Ok(field.to_string())
}

/// Combine the non-scoring constraints into a DataFusion filter.
///
/// `must_not` uses `IS DISTINCT FROM` so that rows with a missing value pass
/// (missing `is_enabled` counts as enabled, matching the client's data).
fn filter_expr(parsed: &ParsedQuery, runtime: &IndexRuntime) -> Result<Expr, EsError> {
    let mut combined: Option<Expr> = None;
    for (field, values) in &parsed.positive {
        let data_type = field_type(runtime, field);
        let mut literals = Vec::with_capacity(values.len());
        for value in values {
            literals.push(Expr::Literal(scalar_value(&data_type, value, field)?, None));
        }
        let expr = col(field).in_list(literals, false);
        combined = Some(and(combined, expr));
    }
    for (field, values) in &parsed.negative {
        let data_type = field_type(runtime, field);
        for value in values {
            let literal = Expr::Literal(scalar_value(&data_type, value, field)?, None);
            let expr = binary_expr(col(field), Operator::IsDistinctFrom, literal);
            combined = Some(and(combined, expr));
        }
    }
    Ok(combined.unwrap_or_else(|| lit(true)))
}

fn field_type(runtime: &IndexRuntime, field: &str) -> DataType {
    runtime
        .schema
        .field_with_name(field)
        .expect("field validated by parse")
        .data_type()
        .clone()
}

fn and(combined: Option<Expr>, expr: Expr) -> Expr {
    match combined {
        Some(previous) => datafusion::logical_expr::Expr::BinaryExpr(
            datafusion::logical_expr::BinaryExpr::new(
                Box::new(previous),
                Operator::And,
                Box::new(expr),
            ),
        ),
        None => expr,
    }
}

/// `POST /{index}/_search`.
pub async fn search(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    let started = Instant::now();
    let runtime = state.index(&index)?;
    let body = SearchBody::parse(&body)?;
    let parsed = ParsedQuery::parse(&body.query, runtime)?;

    let (mut hits, shards) = match &parsed.match_query {
        Some((field, query)) => {
            keyword_hits(&state, runtime, field, query, &parsed, &body).await?
        }
        None => filter_only_hits(&state, runtime, &parsed, &body).await?,
    };

    // Global relevance order, then pagination.
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
    let max_score = hits.first().map(|hit| hit.score);
    let page: Vec<Value> = hits
        .into_iter()
        .skip(body.from)
        .take(body.size)
        .map(|hit| {
            json!({
                "_index": index,
                "_id": hit.id.to_string(),
                "_score": hit.score,
                "_source": hit.source
            })
        })
        .collect();

    Ok(Json(json!({
        "took": started.elapsed().as_millis() as u64,
        "timed_out": false,
        "_shards": {
            "total": shards,
            "successful": shards,
            "skipped": 0,
            "failed": 0
        },
        "hits": {
            "total": {"value": page.len(), "relation": "eq"},
            "max_score": max_score,
            "hits": page
        }
    })))
}

/// Keyword search: text-index candidates + exact verification.
async fn keyword_hits(
    state: &GatewayState,
    runtime: &IndexRuntime,
    field: &str,
    query: &str,
    parsed: &ParsedQuery,
    body: &SearchBody,
) -> Result<(Vec<Hit>, usize), EsError> {
    if field != runtime.config.content_column {
        return Err(EsError::unsupported(format!(
            "match on '{field}' is not supported: only the indexed content \
             column '{}' can be searched",
            runtime.config.content_column
        )));
    }
    // Fetch extra candidates per shard so verification and the final size
    // keep margin against stale index entries.
    let candidate_k = body.size.saturating_mul(10).max(100);
    let (candidates, config, shards) =
        collect_candidates(state, runtime, field, query, candidate_k).await?;
    if candidates.is_empty() {
        return Ok((Vec::new(), shards));
    }
    let Some(config) = config else {
        return Ok((Vec::new(), shards));
    };
    if config.column_name != field {
        return Err(EsError::unsupported(format!(
            "text index column '{}' does not match the searched field '{field}'",
            config.column_name
        )));
    }

    let scores: HashMap<u64, f32> = candidates
        .iter()
        .filter_map(|candidate| candidate.score.map(|score| (candidate.id, score)))
        .collect();
    let ids: Vec<Expr> = candidates
        .iter()
        .map(|candidate| Expr::Literal(ScalarValue::UInt64(Some(candidate.id)), None))
        .collect();
    let candidate_filter = col(PK_COLUMN).in_list(ids, false);
    let filter = and(Some(filter_expr(parsed, runtime)?), candidate_filter);
    let batches = fetch_rows(state, runtime, filter).await?;

    let mut hits = Vec::new();
    for batch in &batches {
        let rows = collect_text_values(batch, PK_COLUMN, field)
            .map_err(|error| EsError::internal(format!("text verify: {error}")))?;
        let compact: Vec<(u64, Option<String>)> =
            rows.iter().flatten().cloned().collect();
        let matched: HashSet<u64> = matching_ids(&config, &compact, query)
            .map_err(|error| EsError::internal(format!("text verify: {error}")))?;
        for (row, value) in rows.iter().enumerate() {
            let Some((id, _)) = value else { continue };
            let Some(score) = scores.get(id) else {
                continue;
            };
            if !matched.contains(id) {
                continue;
            }
            hits.push(Hit {
                id: *id,
                score: *score,
                source: source_for_row(batch, runtime, row, &body.source)?,
            });
        }
    }
    Ok((hits, shards))
}

/// Filter-only search (copy pagination): no scoring, no verification.
async fn filter_only_hits(
    state: &GatewayState,
    runtime: &IndexRuntime,
    parsed: &ParsedQuery,
    body: &SearchBody,
) -> Result<(Vec<Hit>, usize), EsError> {
    let filter = filter_expr(parsed, runtime)?;
    let batches = fetch_rows(state, runtime, filter).await?;
    let mut hits = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name(PK_COLUMN)
            .ok_or_else(|| EsError::internal("primary key column missing"))?
            .as_primitive::<arrow_array::types::UInt64Type>();
        for row in 0..batch.num_rows() {
            hits.push(Hit {
                id: ids.value(row),
                score: 0.0,
                source: source_for_row(batch, runtime, row, &body.source)?,
            });
        }
    }
    Ok((hits, batches.len()))
}

/// Search every shard's text index and merge the candidate scores.
async fn collect_candidates(
    state: &GatewayState,
    runtime: &IndexRuntime,
    column: &str,
    query: &str,
    top_k: usize,
) -> Result<
    (
        Vec<lakesoul_io::index::Candidate>,
        Option<TextIndexConfig>,
        usize,
    ),
    EsError,
> {
    let files = state
        .client
        .get_data_files_by_table_name(&runtime.table, &runtime.namespace)
        .await
        .map_err(crate::error::internal)?;
    if files.is_empty() {
        return Ok((Vec::new(), None, 0));
    }
    let table = state
        .lake_soul_table(runtime)
        .await
        .map_err(crate::error::internal)?;
    let table_path = table.table_info().table_path.clone();
    let table_url =
        ListingTableUrl::parse(&table_path).map_err(crate::error::internal)?;
    let store = state
        .session
        .runtime_env()
        .object_store(table_url.object_store())
        .map_err(crate::error::internal)?;

    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    for file in &files {
        let prefix =
            shard_index_prefix(std::slice::from_ref(file), IndexKind::Text, column);
        groups.entry(prefix).or_default().push(file.clone());
    }

    let catalog = state
        .client
        .index_catalog::<TextSplitEntry>(IndexKind::Text);
    let mut candidates = Vec::new();
    let mut config = None;
    let mut leases = Vec::new();
    let shards = groups.len();
    for (prefix, _group) in groups {
        let Some(view) = catalog
            .resolve_cached(&prefix)
            .await
            .map_err(crate::error::internal)?
        else {
            tracing::warn!(index = %prefix, "text index shard has no commit; skipping");
            continue;
        };
        if config.is_none() {
            config = serde_json::from_slice::<TextIndexConfig>(&view.header).ok();
        }
        match catalog
            .acquire_lease(&prefix, lease_ttl(), &lease_owner())
            .await
        {
            Ok(Some(handle)) => leases.push(IndexLease::new(handle)),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(index = %prefix, "failed to lease text index: {error}");
            }
        }
        let resolved = ResolvedIndex {
            kind: IndexKind::Text,
            index_prefix: prefix.clone(),
            commit_id: view.commit_id,
            generation: view.generation,
            version: view.version,
            header: view.header.clone(),
            segments: serde_json::to_value(&view.segments)
                .map_err(crate::error::internal)?,
        };
        let hits = search_resolved_shard(&store, &resolved, query, top_k)
            .await
            .map_err(crate::error::internal)?;
        candidates.extend(hits);
    }
    // The request is done reading the splits; the GC grace period protects
    // them from here on.
    drop(leases);

    let mut best: HashMap<u64, f32> = HashMap::new();
    for candidate in candidates {
        if let Some(score) = candidate.score {
            let entry = best.entry(candidate.id).or_insert(f32::NEG_INFINITY);
            if score > *entry {
                *entry = score;
            }
        }
    }
    let mut merged: Vec<lakesoul_io::index::Candidate> = best
        .into_iter()
        .map(|(id, score)| lakesoul_io::index::Candidate::scored(id, score))
        .collect();
    merged.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
    merged.truncate(top_k);
    Ok((merged, config, shards))
}

async fn fetch_rows(
    state: &GatewayState,
    runtime: &IndexRuntime,
    filter: Expr,
) -> Result<Vec<RecordBatch>, EsError> {
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
    dataframe.collect().await.map_err(crate::error::internal)
}

fn source_for_row(
    batch: &RecordBatch,
    runtime: &IndexRuntime,
    row: usize,
    source: &SourceFilter,
) -> Result<Map<String, Value>, EsError> {
    let mut map = Map::new();
    if source.disabled {
        return Ok(map);
    }
    for field in runtime.schema.fields() {
        let name = field.name();
        if name == PK_COLUMN || name == CDC_COLUMN {
            continue;
        }
        if let Some(includes) = &source.includes
            && !includes.iter().any(|include| include == name)
        {
            continue;
        }
        if source.excludes.iter().any(|exclude| exclude == name) {
            continue;
        }
        let index = batch
            .schema()
            .index_of(name)
            .map_err(crate::error::internal)?;
        map.insert(name.clone(), value_at(batch.column(index), row));
    }
    Ok(map)
}

fn value_at(array: &ArrayRef, row: usize) -> Value {
    match array.data_type() {
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<StringArray>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                Value::String(values.value(row).to_string())
            }
        }
        DataType::LargeUtf8 => {
            let values = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                Value::String(values.value(row).to_string())
            }
        }
        DataType::Utf8View => {
            let values = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                Value::String(values.value(row).to_string())
            }
        }
        DataType::Int32 => {
            let values = array.as_any().downcast_ref::<Int32Array>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                json!(values.value(row))
            }
        }
        DataType::Int64 => {
            let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                json!(values.value(row))
            }
        }
        DataType::UInt64 => {
            let values = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                json!(values.value(row))
            }
        }
        DataType::Boolean => {
            let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                json!(values.value(row))
            }
        }
        DataType::FixedSizeList(_, _) => {
            let values = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            if values.is_null(row) {
                Value::Null
            } else {
                let row_values = values.value(row);
                let floats = row_values.as_any().downcast_ref::<Float32Array>();
                match floats {
                    Some(floats) => {
                        Value::Array(floats.values().iter().map(|v| json!(v)).collect())
                    }
                    None => Value::Null,
                }
            }
        }
        _ => Value::Null,
    }
}

fn lease_ttl() -> Duration {
    let seconds = std::env::var("LAKESOUL_INDEX_LEASE_TTL_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(300);
    Duration::from_secs(seconds)
}

fn lease_owner() -> String {
    format!(
        "{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
        std::process::id()
    )
}
