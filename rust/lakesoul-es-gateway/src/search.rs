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
use lakesoul_io::index::Candidate;
use lakesoul_io::index::IndexLease;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_io::text::reader::collect_text_values;
use lakesoul_io::text::search::search_resolved_shard;
use lakesoul_io::vector::search::search_resolved_shard as search_vector_shard;
use lakesoul_metadata::index_catalog::{IndexCommitView, VectorSegmentEntry};
use lakesoul_text::{TextIndexConfig, TextSplitEntry, matching_ids};
use lakesoul_vector::SegmentEntry;
use object_store::ObjectStore;
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
#[derive(Debug, Clone)]
struct SearchBody {
    query: Value,
    from: usize,
    size: usize,
    source: SourceFilter,
}

impl SearchBody {
    fn parse(body: &[u8]) -> Result<Self, EsError> {
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

/// A parsed `script_score` vector query.
#[derive(Debug, Clone)]
struct VectorQuery {
    field: String,
    vector: Vec<f32>,
    min_score: Option<f32>,
}

/// The parsed query: an optional scoring clause (`match` or `script_score`)
/// plus non-scoring equality constraints.
#[derive(Debug, Default, Clone)]
struct ParsedQuery {
    match_query: Option<(String, String)>,
    vector_query: Option<VectorQuery>,
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
        if let Some(script_score) = map.get("script_score") {
            if negated {
                return Err(EsError::unsupported(
                    "script_score is not supported inside bool.must_not",
                ));
            }
            let script = script_score.get("script").ok_or_else(|| {
                EsError::bad_request("script_score requires a 'script' object")
            })?;
            let source =
                script
                    .get("source")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        EsError::bad_request("script_score requires a 'source' string")
                    })?;
            let empty = Map::new();
            let params = script
                .get("params")
                .and_then(Value::as_object)
                .unwrap_or(&empty);
            let (field, vector) = parse_cosine_script(source, params)?;
            let min_score = script_score
                .get("min_score")
                .and_then(Value::as_f64)
                .map(|score| score as f32);
            if let Some(inner) = script_score.get("query") {
                self.walk(inner, runtime, false)?;
            }
            self.vector_query = Some(VectorQuery {
                field,
                vector,
                min_score,
            });
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

/// Parse the fixed `cosineSimilarity` scoring script WeKnora sends: the
/// clamped v7/v8 spelling and the bare spelling older gateway images send.
/// They differ only in whitespace and in the `Math.max(..., 0.0)` wrapper;
/// the gateway clamps its cosine to `[0, 1]` either way.
fn parse_cosine_script(
    source: &str,
    params: &Map<String, Value>,
) -> Result<(String, Vec<f32>), EsError> {
    let compact: String = source.chars().filter(|c| !c.is_whitespace()).collect();
    let field = compact
        .strip_prefix("Math.max(cosineSimilarity(params.query_vector,")
        .and_then(|rest| rest.strip_suffix("),0.0)"))
        .or_else(|| {
            compact
                .strip_prefix("cosineSimilarity(params.query_vector,")
                .and_then(|rest| rest.strip_suffix(")"))
        })
        .map(|field| field.trim_matches(|c| c == '\'' || c == '"').to_string())
        .filter(|field| !field.is_empty())
        .ok_or_else(|| {
            EsError::unsupported(format!("unsupported script_score: {source}"))
        })?;
    let values = params
        .get("query_vector")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            EsError::bad_request("script_score params must contain a query_vector array")
        })?;
    let vector: Vec<f32> = values
        .iter()
        .map(|value| {
            value.as_f64().map(|value| value as f32).ok_or_else(|| {
                EsError::bad_request("query_vector must be an array of numbers")
            })
        })
        .collect::<Result<_, _>>()?;
    Ok((field, vector))
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
///
/// The search runs on a blocking task (the io/DataFusion work is blocking):
/// driving the future inside `block_on` also keeps the handler's `Send`
/// requirement on the owned inputs instead of the future's borrows.
pub async fn search(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    let value = tokio::task::spawn_blocking(move || {
        lakesoul_io::session::GLOBAL_RUNTIME.block_on(search_impl(
            state,
            index,
            body.to_vec(),
        ))
    })
    .await
    .map_err(crate::error::internal)??;
    Ok(Json(value))
}

async fn search_impl(
    state: Arc<GatewayState>,
    index: String,
    body: Vec<u8>,
) -> Result<Value, EsError> {
    let started = Instant::now();
    let runtime = state.index(&index)?;
    let body = SearchBody::parse(&body)?;
    let parsed = ParsedQuery::parse(&body.query, runtime)?;
    if parsed.vector_query.is_some() && parsed.match_query.is_some() {
        return Err(EsError::bad_request(
            "a query cannot combine match and script_score",
        ));
    }

    let (mut hits, shards) = if let Some(vector) = &parsed.vector_query {
        vector_hits(
            Arc::clone(&state),
            runtime.clone(),
            vector.clone(),
            parsed.clone(),
            body.clone(),
        )
        .await?
    } else if let Some((field, query)) = &parsed.match_query {
        keyword_hits(
            Arc::clone(&state),
            runtime.clone(),
            field.clone(),
            query.clone(),
            parsed.clone(),
            body.clone(),
        )
        .await?
    } else {
        filter_only_hits(
            Arc::clone(&state),
            runtime.clone(),
            parsed.clone(),
            body.clone(),
        )
        .await?
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

    Ok(json!({
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
    }))
}

/// Vector search: `script_score` cosine candidates + exact rerank.
async fn vector_hits(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    vector: VectorQuery,
    parsed: ParsedQuery,
    body: SearchBody,
) -> Result<(Vec<Hit>, usize), EsError> {
    let Some(dim) = runtime.dim else {
        return Err(EsError::unsupported(
            "the index is keyword-only: no embedding dimension is configured",
        ));
    };
    if vector.field != runtime.config.embedding_column {
        return Err(EsError::unsupported(format!(
            "script_score on '{}' is not supported: only the indexed \
             embedding column '{}' can be searched",
            vector.field, runtime.config.embedding_column
        )));
    }
    if vector.vector.len() != dim {
        return Err(EsError::bad_request(format!(
            "query_vector dimension {} does not match the index dimension {dim}",
            vector.vector.len()
        )));
    }

    let query = normalize_vector(&vector.vector);
    let candidate_k = body.size.saturating_mul(10).max(100);
    let (candidates, shards) = collect_vector_candidates(
        Arc::clone(&state),
        runtime.clone(),
        query.clone(),
        candidate_k,
    )
    .await?;
    if candidates.is_empty() {
        return Ok((Vec::new(), shards));
    }

    let ids: Vec<Expr> = candidates
        .iter()
        .map(|candidate| Expr::Literal(ScalarValue::UInt64(Some(candidate.id)), None))
        .collect();
    let candidate_filter = col(PK_COLUMN).in_list(ids, false);
    let filter = and(Some(filter_expr(&parsed, &runtime)?), candidate_filter);
    let batches = fetch_rows(Arc::clone(&state), runtime.clone(), filter).await?;

    let mut hits = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name(PK_COLUMN)
            .ok_or_else(|| EsError::internal("primary key column missing"))?
            .as_primitive::<arrow_array::types::UInt64Type>();
        let embedding_index = batch
            .schema()
            .index_of(&runtime.config.embedding_column)
            .map_err(crate::error::internal)?;
        let embeddings = batch.column(embedding_index);
        for row in 0..batch.num_rows() {
            let Some(stored) = embedding_at(embeddings, row, dim) else {
                continue;
            };
            // The script clamps the similarity at zero; min_score compares
            // against the clamped score, like Elasticsearch does.
            let score = cosine(&query, &stored);
            if let Some(min_score) = vector.min_score
                && score < min_score
            {
                continue;
            }
            hits.push(Hit {
                id: ids.value(row),
                score,
                source: source_for_row(batch, &runtime, row, &body.source)?,
            });
        }
    }
    Ok((hits, shards))
}

/// Keyword search: text-index candidates + exact verification.
async fn keyword_hits(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    field: String,
    query: String,
    parsed: ParsedQuery,
    body: SearchBody,
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
    let (candidates, config, shards) = collect_candidates(
        Arc::clone(&state),
        runtime.clone(),
        field.clone(),
        query.clone(),
        candidate_k,
    )
    .await?;
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
    let filter = and(Some(filter_expr(&parsed, &runtime)?), candidate_filter);
    let batches = fetch_rows(Arc::clone(&state), runtime.clone(), filter).await?;

    let mut hits = Vec::new();
    for batch in &batches {
        let rows = collect_text_values(batch, PK_COLUMN, &field)
            .map_err(|error| EsError::internal(format!("text verify: {error}")))?;
        let compact: Vec<(u64, Option<String>)> =
            rows.iter().flatten().cloned().collect();
        let matched: HashSet<u64> = matching_ids(&config, &compact, &query)
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
                source: source_for_row(batch, &runtime, row, &body.source)?,
            });
        }
    }
    Ok((hits, shards))
}

/// Filter-only search (copy pagination): no scoring, no verification.
async fn filter_only_hits(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    parsed: ParsedQuery,
    body: SearchBody,
) -> Result<(Vec<Hit>, usize), EsError> {
    let filter = filter_expr(&parsed, &runtime)?;
    let batches = fetch_rows(Arc::clone(&state), runtime.clone(), filter).await?;
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
                source: source_for_row(batch, &runtime, row, &body.source)?,
            });
        }
    }
    Ok((hits, batches.len()))
}

/// The object store of a table path.
async fn index_store(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
) -> Result<Arc<dyn ObjectStore>, EsError> {
    let table = state
        .lake_soul_table(&runtime)
        .await
        .map_err(crate::error::internal)?;
    let table_path = table.table_info().table_path.clone();
    let table_url =
        ListingTableUrl::parse(&table_path).map_err(crate::error::internal)?;
    state
        .session
        .runtime_env()
        .object_store(table_url.object_store())
        .map_err(crate::error::internal)
}

/// Distinct index shard prefixes behind a table's active files.
fn shard_prefixes(files: &[String], kind: IndexKind, column: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    let mut seen = HashSet::new();
    for file in files {
        let prefix = shard_index_prefix(std::slice::from_ref(file), kind, column);
        if seen.insert(prefix.clone()) {
            prefixes.push(prefix);
        }
    }
    prefixes
}

/// Search every shard's text index and merge the candidate scores.
async fn collect_candidates(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    column: String,
    query: String,
    top_k: usize,
) -> Result<(Vec<Candidate>, Option<TextIndexConfig>, usize), EsError> {
    let files = state
        .client
        .get_data_files_by_table_name(&runtime.table, &runtime.namespace)
        .await
        .map_err(crate::error::internal)?;
    if files.is_empty() {
        return Ok((Vec::new(), None, 0));
    }
    let store = index_store(Arc::clone(&state), runtime.clone()).await?;
    let prefixes = shard_prefixes(&files, IndexKind::Text, &column);

    let catalog = state
        .client
        .index_catalog::<TextSplitEntry>(IndexKind::Text);
    let mut candidates = Vec::new();
    let mut config = None;
    let mut leases = Vec::new();
    let shards = prefixes.len();
    for prefix in prefixes {
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
        let hits = search_resolved_shard(&store, &resolved, &query, top_k)
            .await
            .map_err(crate::error::internal)?;
        candidates.extend(hits);
    }
    // The request is done reading the splits; the GC grace period protects
    // them from here on.
    drop(leases);

    let merged = merge_candidates(candidates, top_k);
    Ok((merged, config, shards))
}

/// Search every shard's vector index and merge the candidate distances.
async fn collect_vector_candidates(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    query: Vec<f32>,
    top_k: usize,
) -> Result<(Vec<Candidate>, usize), EsError> {
    let column = runtime.config.embedding_column.clone();
    let files = state
        .client
        .get_data_files_by_table_name(&runtime.table, &runtime.namespace)
        .await
        .map_err(crate::error::internal)?;
    if files.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let store = index_store(Arc::clone(&state), runtime.clone()).await?;
    let prefixes = shard_prefixes(&files, IndexKind::Vector, &column);

    let catalog = state.client.vector_index_catalog();
    let mut candidates = Vec::new();
    let mut leases = Vec::new();
    let shards = prefixes.len();
    let nprobe = runtime.nprobe(&state.config.defaults);
    for prefix in prefixes {
        let Some(view) = catalog
            .resolve_cached(&prefix)
            .await
            .map_err(crate::error::internal)?
        else {
            tracing::warn!(index = %prefix, "vector index shard has no commit; skipping");
            continue;
        };
        match catalog
            .acquire_lease(&prefix, lease_ttl(), &lease_owner())
            .await
        {
            Ok(Some(handle)) => leases.push(IndexLease::new(handle)),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(index = %prefix, "failed to lease vector index: {error}");
            }
        }
        let resolved = vector_resolved(&prefix, &view)?;
        let hits = search_vector_shard(&store, &resolved, &query, top_k, nprobe)
            .await
            .map_err(crate::error::internal)?;
        candidates.extend(hits);
    }
    drop(leases);

    // The provisioned gateway indexes use the IP metric, where a higher
    // score is closer.
    Ok((merge_candidates(candidates, top_k), shards))
}

/// Keep the best score per primary key, ordered by score descending.
fn merge_candidates(candidates: Vec<Candidate>, top_k: usize) -> Vec<Candidate> {
    let mut best: HashMap<u64, f32> = HashMap::new();
    for candidate in candidates {
        if let Some(score) = candidate.score {
            let entry = best.entry(candidate.id).or_insert(f32::NEG_INFINITY);
            if score > *entry {
                *entry = score;
            }
        }
    }
    let mut merged: Vec<Candidate> = best
        .into_iter()
        .map(|(id, score)| Candidate::scored(id, score))
        .collect();
    merged.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });
    merged.truncate(top_k);
    merged
}

/// Convert a catalog vector commit into the io transport payload (the
/// catalog names the artifact `filename`, the io segment expects
/// `segment_filename`).
fn vector_resolved(
    prefix: &str,
    view: &IndexCommitView<VectorSegmentEntry>,
) -> Result<ResolvedIndex, EsError> {
    let segments: Vec<SegmentEntry> = view
        .segments
        .iter()
        .map(|segment| SegmentEntry {
            cluster_id: segment.cluster_id,
            segment_version: segment.segment_version,
            segment_filename: segment.filename.clone(),
            num_vectors: segment.num_vectors,
            file_size: segment.file_size,
        })
        .collect();
    Ok(ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: prefix.to_string(),
        commit_id: view.commit_id,
        generation: view.generation,
        version: view.version,
        header: view.header.clone(),
        segments: serde_json::to_value(&segments).map_err(crate::error::internal)?,
    })
}

/// L2-normalize a query vector (stored vectors are normalized on write, so
/// the exact rerank's cosine is a plain dot product).
fn normalize_vector(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm <= f32::EPSILON {
        return vector.to_vec();
    }
    vector.iter().map(|value| value / norm).collect()
}

/// Cosine similarity clamped to `[0, 1]`, mirroring the client's script.
fn cosine(query: &[f32], stored: &[f32]) -> f32 {
    let dot: f32 = query
        .iter()
        .zip(stored)
        .map(|(left, right)| left * right)
        .sum();
    let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
    let stored_norm = stored.iter().map(|value| value * value).sum::<f32>().sqrt();
    if query_norm <= f32::EPSILON || stored_norm <= f32::EPSILON {
        return 0.0;
    }
    (dot / (query_norm * stored_norm)).clamp(0.0, 1.0)
}

fn embedding_at(array: &ArrayRef, row: usize, dim: usize) -> Option<Vec<f32>> {
    let values = array.as_any().downcast_ref::<FixedSizeListArray>()?;
    if values.is_null(row) {
        return None;
    }
    let row_values = values.value(row);
    let floats = row_values.as_any().downcast_ref::<Float32Array>()?;
    (floats.len() == dim).then(|| floats.values().to_vec())
}

async fn fetch_rows(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
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
