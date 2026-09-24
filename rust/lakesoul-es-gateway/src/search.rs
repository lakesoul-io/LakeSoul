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
use futures::StreamExt;
use lakesoul_common::IndexKind;
use lakesoul_common::index::pending_shard_files;
use lakesoul_io::index::Candidate;
use lakesoul_io::index::IndexLease;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_io::index::reader::read_shard_batches;
use lakesoul_io::text::reader::collect_text_values;
use lakesoul_io::text::search::{search_resolved_shard_with, shard_stats};
use lakesoul_io::vector::reader::extract_vector_batch;
use lakesoul_io::vector::search::search_resolved_shard as search_vector_shard;
use lakesoul_metadata::index_catalog::{IndexCommitView, VectorSegmentEntry};
use lakesoul_text::{
    CorpusStats, TextIndexConfig, TextSplitEntry, bm25_scores_with_terms, is_plain_query,
    matching_ids_with, matching_scores_with, query_terms, stats_for_rows, token_spans,
};
use lakesoul_vector::SegmentEntry;
use object_store::ObjectStore;
use serde_json::{Map, Value, json};

use crate::error::EsError;
use crate::query::scalar_value;
use crate::schema::{CDC_COLUMN, PK_COLUMN};
use crate::state::{GatewayState, IndexRuntime};
use crate::timing::timing_enabled;

/// One scored hit before rendering.
struct Hit {
    id: u64,
    score: f32,
    source: Map<String, Value>,
    /// ES `highlight` result, keyed by field.
    highlight: Option<Value>,
    /// Values of the columns referenced by `sort` and `aggs`, keyed by field.
    /// Only populated when the request asks for them.
    computed: Map<String, Value>,
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
    highlight: Option<Highlight>,
    /// Raw `sort` / `aggs` values; resolved against the index schema by
    /// [`SearchBody::resolve`] once the runtime is known.
    sort: Option<Value>,
    aggregations: Option<Value>,
    sort_fields: Vec<SortField>,
    aggs: Vec<Aggregation>,
    /// Union of the columns referenced by `sort` and `aggs`, collected once
    /// so the hit loop only materializes what the request needs.
    computed_fields: Vec<String>,
}

/// Parsed ES `highlight` request.
#[derive(Debug, Clone)]
struct Highlight {
    /// Requested fields; `*` (the default) matches the content column.
    fields: Vec<String>,
    pre_tags: Vec<String>,
    post_tags: Vec<String>,
    fragment_size: usize,
    number_of_fragments: usize,
}

impl Default for Highlight {
    fn default() -> Self {
        Self {
            fields: Vec::new(),
            pre_tags: vec!["<em>".to_string()],
            post_tags: vec!["</em>".to_string()],
            fragment_size: 150,
            number_of_fragments: 1,
        }
    }
}

impl Highlight {
    fn parse(value: &Value) -> Result<Option<Self>, EsError> {
        let Some(map) = value.as_object() else {
            return Err(EsError::bad_request("'highlight' must be an object"));
        };
        let mut highlight = Highlight::default();
        if let Some(fields) = map.get("fields") {
            let fields = fields.as_object().ok_or_else(|| {
                EsError::bad_request("'highlight.fields' must be an object")
            })?;
            highlight.fields = fields.keys().cloned().collect();
        }
        if let Some(tags) = map.get("pre_tags") {
            highlight.pre_tags = string_list(tags.as_array().ok_or_else(|| {
                EsError::bad_request("'highlight.pre_tags' must be an array")
            })?)?;
        }
        if let Some(tags) = map.get("post_tags") {
            highlight.post_tags = string_list(tags.as_array().ok_or_else(|| {
                EsError::bad_request("'highlight.post_tags' must be an array")
            })?)?;
        }
        if let Some(size) = map.get("fragment_size") {
            highlight.fragment_size =
                size.as_u64().unwrap_or(150).clamp(1, 10_000) as usize;
        }
        if let Some(count) = map.get("number_of_fragments") {
            highlight.number_of_fragments =
                count.as_u64().unwrap_or(1).clamp(1, 100) as usize;
        }
        if highlight.pre_tags.is_empty() {
            highlight.pre_tags.push("<em>".to_string());
        }
        if highlight.post_tags.is_empty() {
            highlight.post_tags.push("</em>".to_string());
        }
        Ok(Some(highlight))
    }

    /// Whether `field` should be highlighted.
    fn applies_to(&self, field: &str) -> bool {
        self.fields.is_empty()
            || self
                .fields
                .iter()
                .any(|requested| requested == field || requested == "*")
    }

    fn pre_tag(&self) -> &str {
        self.pre_tags.first().map(String::as_str).unwrap_or("<em>")
    }

    fn post_tag(&self) -> &str {
        self.post_tags
            .first()
            .map(String::as_str)
            .unwrap_or("</em>")
    }
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
            highlight: map
                .get("highlight")
                .map(Highlight::parse)
                .transpose()?
                .flatten(),
            sort: map.get("sort").cloned(),
            aggregations: map.get("aggs").or_else(|| map.get("aggregations")).cloned(),
            sort_fields: Vec::new(),
            aggs: Vec::new(),
            computed_fields: Vec::new(),
        })
    }

    /// Resolve `sort` and `aggs` against the index schema and collect the
    /// columns their evaluation needs.
    fn resolve(&mut self, runtime: &IndexRuntime) -> Result<(), EsError> {
        self.sort_fields = SortField::parse_list(self.sort.as_ref(), runtime)?;
        self.aggs = parse_aggregations(self.aggregations.as_ref(), runtime)?;
        let mut fields: Vec<String> = Vec::new();
        for sort in &self.sort_fields {
            if let SortKey::Field(field) = &sort.key
                && !fields.contains(field)
            {
                fields.push(field.clone());
            }
        }
        collect_aggregation_fields(&self.aggs, &mut fields);
        self.computed_fields = fields;
        Ok(())
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

/// Sort order of one `sort` key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    fn parse(value: &Value) -> Result<Self, EsError> {
        match value.as_str().map(str::to_ascii_lowercase).as_deref() {
            Some("asc") => Ok(Self::Asc),
            Some("desc") => Ok(Self::Desc),
            _ => Err(EsError::bad_request("sort order must be 'asc' or 'desc'")),
        }
    }
}

/// What a `sort` key refers to.
#[derive(Debug, Clone)]
enum SortKey {
    Score,
    Id,
    Field(String),
}

/// One parsed `sort` entry.
#[derive(Debug, Clone)]
struct SortField {
    key: SortKey,
    order: SortOrder,
}

impl SortField {
    /// Parse the ES `sort` value: a field name, `{field: order}`,
    /// `{field: {"order": order}}` or an array of those.
    fn parse_list(
        value: Option<&Value>,
        runtime: &IndexRuntime,
    ) -> Result<Vec<Self>, EsError> {
        let Some(value) = value else {
            return Ok(Vec::new());
        };
        match value {
            Value::Array(items) => {
                let mut fields = Vec::with_capacity(items.len());
                for item in items {
                    fields.extend(Self::parse(item, runtime)?);
                }
                Ok(fields)
            }
            other => Self::parse(other, runtime),
        }
    }

    fn parse(value: &Value, runtime: &IndexRuntime) -> Result<Vec<Self>, EsError> {
        match value {
            Value::String(field) => Ok(vec![Self::from_field(field, None, runtime)?]),
            Value::Object(map) if !map.is_empty() => map
                .iter()
                .map(|(field, spec)| {
                    let order = match spec {
                        Value::String(_) => Some(SortOrder::parse(spec)?),
                        Value::Object(options) => {
                            options.get("order").map(SortOrder::parse).transpose()?
                        }
                        _ => {
                            return Err(EsError::bad_request(format!(
                                "invalid sort options for '{field}'"
                            )));
                        }
                    };
                    Self::from_field(field, order, runtime)
                })
                .collect(),
            _ => Err(EsError::bad_request("invalid sort")),
        }
    }

    fn from_field(
        field: &str,
        order: Option<SortOrder>,
        runtime: &IndexRuntime,
    ) -> Result<Self, EsError> {
        // `_score` defaults to descending; every other key to ascending.
        let (key, default_order) = match field {
            "_score" => (SortKey::Score, SortOrder::Desc),
            "_id" | "_doc" | "id" => (SortKey::Id, SortOrder::Asc),
            _ => (
                SortKey::Field(normalize_field(field, runtime)?),
                SortOrder::Asc,
            ),
        };
        Ok(Self {
            key,
            order: order.unwrap_or(default_order),
        })
    }
}

/// Total order across the JSON values the schema produces.  Cross-type
/// comparisons only order by type, which a single column cannot mix.
fn compare_json(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        _ => json_type_rank(left).cmp(&json_type_rank(right)),
    }
}

fn json_type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

fn is_missing(value: Option<&Value>) -> bool {
    matches!(value, None | Some(Value::Null))
}

/// Order two hits by the explicit `sort` keys.  Missing values sort last in
/// either direction (Elasticsearch's default `missing: _last`), and ties fall
/// back to the primary key so the order stays deterministic.
fn compare_hits(left: &Hit, right: &Hit, sort: &[SortField]) -> Ordering {
    for field in sort {
        let ordering = match &field.key {
            SortKey::Score => left
                .score
                .partial_cmp(&right.score)
                .unwrap_or(Ordering::Equal),
            SortKey::Id => left.id.cmp(&right.id),
            SortKey::Field(name) => {
                let left_value = left.computed.get(name);
                let right_value = right.computed.get(name);
                match (is_missing(left_value), is_missing(right_value)) {
                    (true, true) => Ordering::Equal,
                    (true, false) => return Ordering::Greater,
                    (false, true) => return Ordering::Less,
                    (false, false) => compare_json(
                        left_value.expect("checked above"),
                        right_value.expect("checked above"),
                    ),
                }
            }
        };
        let ordering = match field.order {
            SortOrder::Asc => ordering,
            SortOrder::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.id.cmp(&right.id)
}

/// A named aggregation in the request order.
#[derive(Debug, Clone)]
struct Aggregation {
    name: String,
    agg: Agg,
}

#[derive(Debug, Clone)]
enum Agg {
    Terms(TermsAgg),
    Metric(MetricAgg),
}

#[derive(Debug, Clone)]
struct TermsAgg {
    field: String,
    size: usize,
    order: TermsOrder,
    min_doc_count: u64,
    sub: Vec<Aggregation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TermsOrderKey {
    Count,
    Key,
}

#[derive(Debug, Clone, Copy)]
struct TermsOrder {
    key: TermsOrderKey,
    order: SortOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Avg,
    Sum,
    Min,
    Max,
    ValueCount,
    Cardinality,
    Stats,
}

#[derive(Debug, Clone)]
struct MetricAgg {
    kind: MetricKind,
    field: String,
}

/// Elasticsearch caps `terms.size` well below this; the gateway rejects
/// nothing and simply clamps.
const MAX_TERMS_SIZE: u64 = 10_000;

fn parse_aggregations(
    value: Option<&Value>,
    runtime: &IndexRuntime,
) -> Result<Vec<Aggregation>, EsError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let map = value
        .as_object()
        .ok_or_else(|| EsError::bad_request("'aggs' must be an object"))?;
    map.iter()
        .map(|(name, spec)| parse_aggregation(name, spec, runtime))
        .collect()
}

fn parse_aggregation(
    name: &str,
    value: &Value,
    runtime: &IndexRuntime,
) -> Result<Aggregation, EsError> {
    let map = value.as_object().ok_or_else(|| {
        EsError::bad_request(format!("aggregation '{name}' must be an object"))
    })?;
    let sub =
        parse_aggregations(map.get("aggs").or_else(|| map.get("aggregations")), runtime)?;
    if let Some(terms) = map.get("terms") {
        let terms = terms.as_object().ok_or_else(|| {
            EsError::bad_request(format!(
                "'terms' aggregation '{name}' must be an object"
            ))
        })?;
        let field = aggregation_field(name, terms.get("field"), runtime)?;
        let size = terms
            .get("size")
            .and_then(Value::as_u64)
            .unwrap_or(10)
            .clamp(1, MAX_TERMS_SIZE) as usize;
        let min_doc_count = terms
            .get("min_doc_count")
            .and_then(Value::as_u64)
            .unwrap_or(1);
        let order = parse_terms_order(terms.get("order"))?;
        return Ok(Aggregation {
            name: name.to_string(),
            agg: Agg::Terms(TermsAgg {
                field,
                size,
                order,
                min_doc_count,
                sub,
            }),
        });
    }
    for (key, kind) in [
        ("avg", MetricKind::Avg),
        ("sum", MetricKind::Sum),
        ("min", MetricKind::Min),
        ("max", MetricKind::Max),
        ("value_count", MetricKind::ValueCount),
        ("cardinality", MetricKind::Cardinality),
        ("stats", MetricKind::Stats),
    ] {
        if let Some(spec) = map.get(key) {
            let spec = spec.as_object().ok_or_else(|| {
                EsError::bad_request(format!(
                    "'{key}' aggregation '{name}' must be an object"
                ))
            })?;
            let field = aggregation_field(name, spec.get("field"), runtime)?;
            return Ok(Aggregation {
                name: name.to_string(),
                agg: Agg::Metric(MetricAgg { kind, field }),
            });
        }
    }
    Err(EsError::unsupported(format!(
        "unsupported aggregation: {value}"
    )))
}

fn aggregation_field(
    name: &str,
    field: Option<&Value>,
    runtime: &IndexRuntime,
) -> Result<String, EsError> {
    let field = field.and_then(Value::as_str).ok_or_else(|| {
        EsError::bad_request(format!("aggregation '{name}' requires a 'field'"))
    })?;
    normalize_field(field, runtime)
}

fn parse_terms_order(value: Option<&Value>) -> Result<TermsOrder, EsError> {
    let Some(value) = value else {
        // Elasticsearch defaults to `_count` descending.
        return Ok(TermsOrder {
            key: TermsOrderKey::Count,
            order: SortOrder::Desc,
        });
    };
    let (key, order) = match value {
        Value::Object(options) => {
            let Some((key, order)) = options.iter().next() else {
                return Err(EsError::bad_request("terms order must not be empty"));
            };
            (key.clone(), SortOrder::parse(order)?)
        }
        Value::String(key) => (key.clone(), SortOrder::Desc),
        _ => return Err(EsError::bad_request("invalid terms order")),
    };
    let key = match key.as_str() {
        "_count" => TermsOrderKey::Count,
        "_key" => TermsOrderKey::Key,
        _ => {
            return Err(EsError::unsupported(format!(
                "terms order '{key}' is not supported: use _count or _key"
            )));
        }
    };
    Ok(TermsOrder { key, order })
}

fn collect_aggregation_fields(aggs: &[Aggregation], fields: &mut Vec<String>) {
    for agg in aggs {
        match &agg.agg {
            Agg::Terms(terms) => {
                if !fields.contains(&terms.field) {
                    fields.push(terms.field.clone());
                }
                collect_aggregation_fields(&terms.sub, fields);
            }
            Agg::Metric(metric) => {
                if !fields.contains(&metric.field) {
                    fields.push(metric.field.clone());
                }
            }
        }
    }
}

/// Evaluate aggregations over the verified hits (the page is cut after, so
/// aggregations always cover the full match set the request fetched).
fn eval_aggregations(aggs: &[Aggregation], hits: &[&Hit]) -> Value {
    let mut result = Map::new();
    for agg in aggs {
        result.insert(agg.name.clone(), eval_aggregation(&agg.agg, hits));
    }
    Value::Object(result)
}

fn eval_aggregation(agg: &Agg, hits: &[&Hit]) -> Value {
    match agg {
        Agg::Metric(metric) => eval_metric(metric, hits),
        Agg::Terms(terms) => eval_terms(terms, hits),
    }
}

fn eval_metric(metric: &MetricAgg, hits: &[&Hit]) -> Value {
    let numbers: Vec<f64> = hits
        .iter()
        .filter_map(|hit| hit.computed.get(&metric.field))
        .filter_map(Value::as_f64)
        .collect();
    let value_count = || {
        hits.iter()
            .filter_map(|hit| hit.computed.get(&metric.field))
            .filter(|value| !value.is_null())
            .count() as u64
    };
    match metric.kind {
        MetricKind::Avg => json!({"value": average(&numbers)}),
        MetricKind::Sum => json!({"value": numbers.iter().sum::<f64>()}),
        MetricKind::Min => json!({"value": numbers.iter().copied().reduce(f64::min)}),
        MetricKind::Max => json!({"value": numbers.iter().copied().reduce(f64::max)}),
        MetricKind::ValueCount => json!({"value": value_count()}),
        MetricKind::Cardinality => {
            let distinct: HashSet<String> = hits
                .iter()
                .filter_map(|hit| hit.computed.get(&metric.field))
                .filter(|value| !value.is_null())
                .map(value_key)
                .collect();
            json!({"value": distinct.len() as u64})
        }
        MetricKind::Stats => json!({
            "count": value_count(),
            "min": numbers.iter().copied().reduce(f64::min),
            "max": numbers.iter().copied().reduce(f64::max),
            "avg": average(&numbers),
            "sum": numbers.iter().sum::<f64>(),
        }),
    }
}

struct TermsBucket<'a> {
    key: Value,
    count: u64,
    hits: Vec<&'a Hit>,
}

fn eval_terms(terms: &TermsAgg, hits: &[&Hit]) -> Value {
    let mut buckets: HashMap<String, TermsBucket> = HashMap::new();
    for hit in hits {
        let Some(value) = hit.computed.get(&terms.field) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let bucket = buckets
            .entry(value_key(value))
            .or_insert_with(|| TermsBucket {
                key: value.clone(),
                count: 0,
                hits: Vec::new(),
            });
        bucket.count += 1;
        bucket.hits.push(hit);
    }
    let mut buckets: Vec<TermsBucket> = buckets
        .into_values()
        .filter(|bucket| bucket.count >= terms.min_doc_count)
        .collect();
    buckets.sort_by(|left, right| match terms.order.key {
        TermsOrderKey::Count => match terms.order.order {
            // `_count` keeps the key ascending as its tie-break, so the
            // primary order alone is reversed.
            SortOrder::Desc => right
                .count
                .cmp(&left.count)
                .then_with(|| compare_json(&left.key, &right.key)),
            SortOrder::Asc => left
                .count
                .cmp(&right.count)
                .then_with(|| compare_json(&left.key, &right.key)),
        },
        TermsOrderKey::Key => match terms.order.order {
            SortOrder::Asc => compare_json(&left.key, &right.key),
            SortOrder::Desc => compare_json(&right.key, &left.key),
        },
    });
    let sum_other_doc_count: u64 = buckets
        .iter()
        .skip(terms.size)
        .map(|bucket| bucket.count)
        .sum();
    buckets.truncate(terms.size);

    let buckets: Vec<Value> = buckets
        .iter()
        .map(|bucket| {
            let mut object = Map::new();
            let (key, key_as_string) = term_key(&bucket.key);
            object.insert("key".to_string(), key);
            if let Some(key_as_string) = key_as_string {
                object.insert("key_as_string".to_string(), key_as_string);
            }
            object.insert("doc_count".to_string(), json!(bucket.count));
            if !terms.sub.is_empty()
                && let Value::Object(sub) = eval_aggregations(&terms.sub, &bucket.hits)
            {
                for (name, value) in sub {
                    object.insert(name, value);
                }
            }
            Value::Object(object)
        })
        .collect();
    json!({
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": sum_other_doc_count,
        "buckets": buckets,
    })
}

/// Elasticsearch renders boolean term keys as 1/0 with a string form.
fn term_key(value: &Value) -> (Value, Option<Value>) {
    match value {
        Value::Bool(value) => (
            json!(u8::from(*value)),
            Some(Value::String(value.to_string())),
        ),
        other => (other.clone(), None),
    }
}

/// Canonical key for the exact `cardinality` and terms grouping.
fn value_key(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => format!("bool:{value}"),
        Value::Number(value) => format!("number:{value}"),
        Value::String(value) => format!("string:{value}"),
        other => format!("json:{other}"),
    }
}

fn average(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

/// A parsed `script_score` vector query.
#[derive(Debug, Clone)]
struct VectorQuery {
    field: String,
    vector: Vec<f32>,
    min_score: Option<f32>,
}

/// A parsed `match` clause.
#[derive(Debug, Clone)]
struct MatchQuery {
    field: String,
    query: String,
    /// Query-time analyzer override (already validated and dropped when it
    /// equals the index-time analyzer).
    analyzer: Option<String>,
}

/// The parsed query: an optional scoring clause (`match` or `script_score`)
/// plus non-scoring equality constraints.
#[derive(Debug, Default, Clone)]
struct ParsedQuery {
    match_query: Option<MatchQuery>,
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
            let (query, analyzer) = match value {
                Value::String(query) => (query.clone(), None),
                Value::Object(inner) => {
                    let query = inner
                        .get("query")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            EsError::bad_request("'match' query must be a string")
                        })?
                        .to_string();
                    let analyzer = inner
                        .get("analyzer")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    (query, analyzer)
                }
                _ => {
                    return Err(EsError::bad_request("'match' query must be a string"));
                }
            };
            let analyzer = match analyzer {
                None => None,
                Some(analyzer) => {
                    if !lakesoul_text::is_supported(&analyzer) {
                        return Err(EsError::bad_request(format!(
                            "unsupported match analyzer '{analyzer}': expected one \
                             of {:?}",
                            lakesoul_text::SUPPORTED_TOKENIZERS
                        )));
                    }
                    if !lakesoul_text::is_plain_query(&query) {
                        return Err(EsError::unsupported(
                            "a match analyzer override only supports plain query \
                             text, not query syntax",
                        ));
                    }
                    Some(analyzer)
                }
            };
            self.match_query = Some(MatchQuery {
                field: field.clone(),
                query,
                analyzer,
            });
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
    let mut body = SearchBody::parse(&body)?;
    body.resolve(runtime)?;
    let parse_ms = started.elapsed().as_secs_f64() * 1000.0;
    let parsed = ParsedQuery::parse(&body.query, runtime)?;
    if parsed.vector_query.is_some() && parsed.match_query.is_some() {
        return Err(EsError::bad_request(
            "a query cannot combine match and script_score",
        ));
    }

    let computed = &body.computed_fields;
    let (mut hits, shards) = if let Some(vector) = &parsed.vector_query {
        vector_hits(
            Arc::clone(&state),
            runtime.clone(),
            vector.clone(),
            parsed.clone(),
            body.clone(),
            computed,
        )
        .await?
    } else if let Some(match_query) = &parsed.match_query {
        keyword_hits(
            Arc::clone(&state),
            runtime.clone(),
            match_query.clone(),
            parsed.clone(),
            body.clone(),
            computed,
        )
        .await?
    } else {
        filter_only_hits(
            Arc::clone(&state),
            runtime.clone(),
            parsed.clone(),
            body.clone(),
            computed,
        )
        .await?
    };

    // Global order, then pagination.  Without an explicit `sort` the
    // relevance order (score descending, primary key ascending) still
    // applies; an explicit `sort` replaces it and ties fall back to the
    // primary key.
    if body.sort_fields.is_empty() {
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
    } else {
        hits.sort_by(|left, right| compare_hits(left, right, &body.sort_fields));
    }
    let max_score = hits.iter().map(|hit| hit.score).reduce(f32::max);
    let aggregations = if body.aggs.is_empty() {
        None
    } else {
        let docs: Vec<&Hit> = hits.iter().collect();
        Some(eval_aggregations(&body.aggs, &docs))
    };
    let page: Vec<Value> = hits
        .into_iter()
        .skip(body.from)
        .take(body.size)
        .map(|hit| {
            let mut object = json!({
                "_index": index,
                "_id": hit.id.to_string(),
                "_score": hit.score,
                "_source": hit.source
            });
            if let Some(highlight) = hit.highlight
                && let Some(object) = object.as_object_mut()
            {
                object.insert("highlight".to_string(), highlight);
            }
            object
        })
        .collect();

    if timing_enabled() {
        let kind = if parsed.vector_query.is_some() {
            "vector"
        } else if parsed.match_query.is_some() {
            "keyword"
        } else {
            "filter"
        };
        tracing::info!(
            target: "lakesoul_es_gateway::timing",
            "search.total kind={} parse_ms={:.1} total_ms={:.1} hits={}",
            kind,
            parse_ms,
            started.elapsed().as_secs_f64() * 1000.0,
            page.len()
        );
    }
    let mut response = json!({
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
    });
    if let Some(aggregations) = aggregations
        && let Some(object) = response.as_object_mut()
    {
        object.insert("aggregations".to_string(), aggregations);
    }
    Ok(response)
}

/// Vector search: `script_score` cosine candidates + exact rerank.
async fn vector_hits(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    vector: VectorQuery,
    parsed: ParsedQuery,
    body: SearchBody,
    computed_fields: &[String],
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
    // A full candidate budget can be the binding constraint when stale
    // candidates lose the exact rerank; widen it before under-filling.
    let mut candidate_k = body.size.saturating_mul(10).max(100);
    let mut retries = 0;
    loop {
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
        let fetch_started = Instant::now();
        let batches = fetch_rows(Arc::clone(&state), runtime.clone(), filter).await?;
        let fetch_ms = fetch_started.elapsed().as_secs_f64() * 1000.0;

        let rerank_started = Instant::now();
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
                // The script clamps the similarity at zero; min_score
                // compares against the clamped score, like Elasticsearch.
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
                    highlight: None,
                    computed: computed_for_row(batch, row, computed_fields)?,
                });
            }
        }
        if timing_enabled() {
            tracing::info!(
                target: "lakesoul_es_gateway::timing",
                "search.vector_rerank fetch_ms={:.1} rerank_ms={:.1} batches={} \
                 hits={}",
                fetch_ms,
                rerank_started.elapsed().as_secs_f64() * 1000.0,
                batches.len(),
                hits.len()
            );
        }
        let exhausted = candidates.len() >= candidate_k;
        if hits.len() >= body.size || !exhausted || retries >= 2 {
            return Ok((hits, shards));
        }
        retries += 1;
        candidate_k = candidate_k.saturating_mul(4);
    }
}

/// Keyword search: text-index candidates + exact verification.
async fn keyword_hits(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    match_query: MatchQuery,
    parsed: ParsedQuery,
    body: SearchBody,
    computed_fields: &[String],
) -> Result<(Vec<Hit>, usize), EsError> {
    let field = match_query.field;
    let query = match_query.query;
    // An override equal to the index-time analyzer changes nothing.
    let analyzer = match_query
        .analyzer
        .filter(|analyzer| analyzer != runtime.tokenizer(&state.config.defaults));
    let analyzer = analyzer.as_deref();
    if field != runtime.config.content_column {
        return Err(EsError::unsupported(format!(
            "match on '{field}' is not supported: only the indexed content \
             column '{}' can be searched",
            runtime.config.content_column
        )));
    }
    // Fetch extra candidates per shard so verification and the final size
    // keep margin against stale index entries.  Verification can under-fill
    // the result when stale candidates consume the budget, so retry with a
    // larger budget before returning fewer hits than asked for.
    let mut candidate_k = body.size.saturating_mul(10).max(100);
    let mut retries = 0;
    loop {
        let (candidates, config, shards, stats) = collect_candidates(
            Arc::clone(&state),
            runtime.clone(),
            field.clone(),
            query.clone(),
            candidate_k,
            analyzer,
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

        let mut scores: HashMap<u64, f32> = candidates
            .iter()
            .filter_map(|candidate| candidate.score.map(|score| (candidate.id, score)))
            .collect();
        let ids: Vec<Expr> = candidates
            .iter()
            .map(|candidate| Expr::Literal(ScalarValue::UInt64(Some(candidate.id)), None))
            .collect();
        let candidate_filter = col(PK_COLUMN).in_list(ids, false);
        let filter = and(Some(filter_expr(&parsed, &runtime)?), candidate_filter);
        let fetch_started = Instant::now();
        let batches = fetch_rows(Arc::clone(&state), runtime.clone(), filter).await?;
        let fetch_ms = fetch_started.elapsed().as_secs_f64() * 1000.0;

        let verify_started = Instant::now();
        // Verify all batches with one in-memory index instead of rebuilding
        // it per batch.
        let mut batch_rows = Vec::with_capacity(batches.len());
        let mut all_rows: Vec<(u64, Option<String>)> = Vec::new();
        for batch in &batches {
            let rows = collect_text_values(batch, PK_COLUMN, &field)
                .map_err(|error| EsError::internal(format!("text verify: {error}")))?;
            all_rows.extend(rows.iter().flatten().map(|(id, text)| (*id, text.clone())));
            batch_rows.push(rows);
        }
        let matched: HashSet<u64> =
            matching_ids_with(&config, &all_rows, &query, analyzer)
                .map_err(|error| EsError::internal(format!("text verify: {error}")))?;

        // One corpus-wide BM25 score per matching row, so the merged order
        // no longer depends on which shard returned a candidate (the
        // `dfs_query_then_fetch` semantics of a distributed search engine).
        if let Some(stats) = &stats {
            let query_config = match analyzer {
                Some(analyzer) => TextIndexConfig {
                    tokenizer: analyzer.to_string(),
                    ..config.clone()
                },
                None => config.clone(),
            };
            let terms = query_terms(&query_config, &query).map_err(|error| {
                EsError::internal(format!("global text terms: {error}"))
            })?;
            let global = bm25_scores_with_terms(&config, &all_rows, &terms, stats)
                .map_err(|error| {
                    EsError::internal(format!("global text score: {error}"))
                })?;
            if !global.is_empty() {
                scores = global;
            }
        }

        let highlight_terms = body
            .highlight
            .as_ref()
            .and_then(|_| query_terms(&config, &query).ok());
        let mut hits = Vec::new();
        for (batch_index, batch) in batches.iter().enumerate() {
            for (row, value) in batch_rows[batch_index].iter().enumerate() {
                let Some((id, text)) = value else { continue };
                let Some(score) = scores.get(id) else {
                    continue;
                };
                if !matched.contains(id) {
                    continue;
                }
                let mut hit = Hit {
                    id: *id,
                    score: *score,
                    source: source_for_row(batch, &runtime, row, &body.source)?,
                    highlight: None,
                    computed: computed_for_row(batch, row, computed_fields)?,
                };
                if let (Some(highlight), Some(terms), Some(text)) =
                    (&body.highlight, &highlight_terms, text)
                    && highlight.applies_to(&field)
                {
                    let fragments = highlight_fragments(&config, text, terms, highlight);
                    if !fragments.is_empty() {
                        hit.highlight = Some(json!({ &field: fragments }));
                    }
                }
                hits.push(hit);
            }
        }
        if timing_enabled() {
            tracing::info!(
                target: "lakesoul_es_gateway::timing",
                "search.keyword_verify fetch_ms={:.1} verify_ms={:.1} batches={} \
                 candidates={} hits={} global={}",
                fetch_ms,
                verify_started.elapsed().as_secs_f64() * 1000.0,
                batches.len(),
                candidates.len(),
                hits.len(),
                stats.is_some()
            );
        }
        // A full candidate budget means the shard top-k may be the binding
        // constraint; widen it when verification dropped too many rows.
        let exhausted = candidates.len() >= candidate_k;
        if hits.len() >= body.size || !exhausted || retries >= 2 {
            return Ok((hits, shards));
        }
        retries += 1;
        candidate_k = candidate_k.saturating_mul(4);
    }
}

/// Filter-only search (copy pagination): no scoring, no verification.
async fn filter_only_hits(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    parsed: ParsedQuery,
    body: SearchBody,
    computed_fields: &[String],
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
                highlight: None,
                computed: computed_for_row(batch, row, computed_fields)?,
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

/// Active data files grouped by index shard prefix, in listing order.
fn shard_file_groups(
    files: &[String],
    kind: IndexKind,
    column: &str,
) -> Vec<(String, Vec<String>)> {
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    let mut positions: HashMap<String, usize> = HashMap::new();
    for file in files {
        let prefix = shard_index_prefix(std::slice::from_ref(file), kind, column);
        match positions.get(&prefix) {
            Some(position) => groups[*position].1.push(file.clone()),
            None => {
                positions.insert(prefix.clone(), groups.len());
                groups.push((prefix, vec![file.clone()]));
            }
        }
    }
    groups
}

/// Exact candidates from data files that no index commit covers yet.
///
/// This is the read-your-writes fallback of deferred index maintenance: the
/// pending tail is read with merge-on-read semantics and scored exactly, so
/// a document is searchable as soon as its write commits.
async fn text_tail_candidates(
    files: &[String],
    config: &TextIndexConfig,
    query: &str,
    analyzer: Option<&str>,
) -> Result<(Vec<Candidate>, CorpusStats), EsError> {
    let projection = vec![config.column_name.clone()];
    let batches =
        read_shard_batches(files, PK_COLUMN, &projection, &HashMap::new(), None)
            .await
            .map_err(crate::error::internal)?;
    let mut rows: Vec<(u64, Option<String>)> = Vec::new();
    for batch in &batches {
        let values = collect_text_values(batch, PK_COLUMN, &config.column_name)
            .map_err(|error| EsError::internal(format!("text tail read: {error}")))?;
        rows.extend(values.into_iter().flatten());
    }
    let scores = matching_scores_with(config, &rows, query, analyzer)
        .map_err(|error| EsError::internal(format!("text tail score: {error}")))?;
    // The tail documents are not part of any index commit, so they must
    // still count towards the corpus statistics.
    let stats = stats_for_rows(config, &rows)
        .map_err(|error| EsError::internal(format!("text tail stats: {error}")))?;
    Ok((
        scores
            .into_iter()
            .map(|(id, score)| Candidate::scored(id, score))
            .collect(),
        stats,
    ))
}

/// Exact cosine candidates from the not-yet-indexed data files.
async fn vector_tail_candidates(
    files: &[String],
    column: &str,
    query: &[f32],
    dim: usize,
) -> Result<Vec<Candidate>, EsError> {
    let projection = vec![column.to_string()];
    let batches =
        read_shard_batches(files, PK_COLUMN, &projection, &HashMap::new(), None)
            .await
            .map_err(crate::error::internal)?;
    let mut hits = Vec::new();
    for batch in &batches {
        let extracted = extract_vector_batch(batch, PK_COLUMN, column, dim)
            .map_err(|error| EsError::internal(format!("vector tail read: {error}")))?;
        for (index, id) in extracted.ids.iter().enumerate() {
            let stored = &extracted.vectors[index * dim..(index + 1) * dim];
            hits.push(Candidate::scored(*id, cosine(query, stored)));
        }
    }
    Ok(hits)
}

/// Per-shard slow-path timings, summed over the shards of one request.
#[derive(Debug, Default, Clone, Copy)]
struct ShardTimings {
    resolve_ms: f64,
    lease_ms: f64,
    search_ms: f64,
    tail_ms: f64,
}

impl ShardTimings {
    fn merge(&mut self, other: Self) {
        self.resolve_ms += other.resolve_ms;
        self.lease_ms += other.lease_ms;
        self.search_ms += other.search_ms;
        self.tail_ms += other.tail_ms;
    }
}

struct TextShardOutcome {
    hits: Vec<Candidate>,
    config: TextIndexConfig,
    stats: CorpusStats,
    lease: Option<IndexLease>,
    timings: ShardTimings,
}

struct VectorShardOutcome {
    hits: Vec<Candidate>,
    lease: Option<IndexLease>,
    timings: ShardTimings,
}

/// Shards searched in parallel per request.  The count is small (one per
/// hash bucket per partition); the bound keeps a wide table from opening
/// dozens of index files at once.
const SHARD_CONCURRENCY: usize = 8;

/// Search every shard's text index and merge the candidate scores.
///
/// Shards whose data files are not fully covered by the index commit also
/// contribute exact candidates from those files (deferred maintenance).
async fn collect_candidates(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    column: String,
    query: String,
    top_k: usize,
    analyzer: Option<&str>,
) -> Result<
    (
        Vec<Candidate>,
        Option<TextIndexConfig>,
        usize,
        Option<CorpusStats>,
    ),
    EsError,
> {
    let started = Instant::now();
    let files = state
        .client
        .get_data_files_by_table_name(&runtime.table, &runtime.namespace)
        .await
        .map_err(crate::error::internal)?;
    if files.is_empty() {
        return Ok((Vec::new(), None, 0, None));
    }
    let store = index_store(Arc::clone(&state), runtime.clone()).await?;
    let groups = shard_file_groups(&files, IndexKind::Text, &column);
    let files_ms = started.elapsed().as_secs_f64() * 1000.0;

    let catalog = state
        .client
        .index_catalog::<TextSplitEntry>(IndexKind::Text);
    let fallback_config = TextIndexConfig {
        column_name: column.clone(),
        tokenizer: runtime.tokenizer(&state.config.defaults).to_string(),
        with_positions: runtime.with_positions(&state.config.defaults),
        stored: false,
    };
    // Plain queries can be scored against one corpus-wide ranking; the
    // per-shard statistics are collected while the shards are searched.
    let terms_config = match analyzer {
        Some(analyzer) => TextIndexConfig {
            tokenizer: analyzer.to_string(),
            ..fallback_config.clone()
        },
        None => fallback_config.clone(),
    };
    let terms = if is_plain_query(&query) {
        query_terms(&terms_config, &query).unwrap_or_default()
    } else {
        Vec::new()
    };
    let shards = groups.len();
    let concurrency = shards.clamp(1, SHARD_CONCURRENCY);
    let outcomes: Vec<Result<TextShardOutcome, EsError>> = futures::stream::iter(groups)
        .map(|(prefix, shard_files)| {
            let store = Arc::clone(&store);
            let catalog = &catalog;
            let query = query.as_str();
            let fallback_config = fallback_config.clone();
            let terms = terms.clone();
            async move {
                let resolve_started = Instant::now();
                let view = catalog
                    .resolve_cached(&prefix)
                    .await
                    .map_err(crate::error::internal)?;
                let resolve_ms = resolve_started.elapsed().as_secs_f64() * 1000.0;
                let config = view
                    .as_ref()
                    .and_then(|view| {
                        serde_json::from_slice::<TextIndexConfig>(&view.header).ok()
                    })
                    .unwrap_or_else(|| fallback_config.clone());
                let lease_started = Instant::now();
                let mut lease = None;
                let mut hits = Vec::new();
                let mut search_ms = 0.0;
                let mut stats = CorpusStats::default();
                if let Some(view) = &view {
                    if !lakesoul_io::index::cache::is_loaded(
                        &store,
                        IndexKind::Text,
                        &prefix,
                        view.commit_id,
                    )
                    .await
                    {
                        match catalog
                            .acquire_lease(&prefix, lease_ttl(), &lease_owner())
                            .await
                        {
                            Ok(Some(handle)) => lease = Some(IndexLease::new(handle)),
                            Ok(None) => {}
                            Err(error) => {
                                tracing::warn!(
                                    index = %prefix,
                                    "failed to lease text index: {error}"
                                );
                            }
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
                    let search_started = Instant::now();
                    hits = search_resolved_shard_with(
                        &store, &resolved, query, top_k, analyzer,
                    )
                    .await
                    .map_err(crate::error::internal)?;
                    search_ms = search_started.elapsed().as_secs_f64() * 1000.0;
                    if !terms.is_empty() {
                        match shard_stats(&store, &resolved, &terms).await {
                            Ok(shard) => stats.merge(shard),
                            Err(error) => tracing::warn!(
                                index = %prefix,
                                "failed to collect text statistics: {error}"
                            ),
                        }
                    }
                }
                let lease_ms = lease_started.elapsed().as_secs_f64() * 1000.0;

                // Files no segment covers: an untouched shard contributes
                // all of them, a legacy commit (no coverage recorded) is
                // skipped by the helper.
                let pending = match &view {
                    Some(view) => pending_shard_files(&shard_files, &view.segments),
                    None => shard_files.clone(),
                };
                let tail_started = Instant::now();
                let mut tail_hits = Vec::new();
                if !pending.is_empty() {
                    let (tail, tail_stats) =
                        text_tail_candidates(&pending, &config, query, analyzer).await?;
                    tail_hits = tail;
                    stats.merge(tail_stats);
                }
                let tail_ms = tail_started.elapsed().as_secs_f64() * 1000.0;
                hits.extend(tail_hits);

                Ok(TextShardOutcome {
                    hits,
                    config,
                    stats,
                    lease,
                    timings: ShardTimings {
                        resolve_ms,
                        lease_ms,
                        search_ms,
                        tail_ms,
                    },
                })
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut candidates = Vec::new();
    let mut config = None;
    let mut corpus_stats = CorpusStats::default();
    let mut leases = Vec::new();
    let mut timings = ShardTimings::default();
    for outcome in outcomes {
        let outcome = outcome?;
        if config.is_none() {
            config = Some(outcome.config);
        }
        corpus_stats.merge(outcome.stats);
        if let Some(lease) = outcome.lease {
            leases.push(lease);
        }
        candidates.extend(outcome.hits);
        timings.merge(outcome.timings);
    }
    // The request is done reading the splits; the GC grace period protects
    // them from here on.
    drop(leases);

    if timing_enabled() {
        tracing::info!(
            target: "lakesoul_es_gateway::timing",
            "search.text files_ms={:.1} resolve_ms={:.1} lease_ms={:.1} \
             shard_search_ms={:.1} tail_ms={:.1} shards={}",
            files_ms,
            timings.resolve_ms,
            timings.lease_ms,
            timings.search_ms,
            timings.tail_ms,
            shards
        );
    }

    let merged = merge_candidates(candidates, top_k);
    let stats = (!terms.is_empty()).then_some(corpus_stats);
    Ok((merged, config, shards, stats))
}

/// Search every shard's vector index and merge the candidate distances.
///
/// Shards whose data files are not fully covered by the index commit also
/// contribute exact cosine candidates from those files.
async fn collect_vector_candidates(
    state: Arc<GatewayState>,
    runtime: IndexRuntime,
    query: Vec<f32>,
    top_k: usize,
) -> Result<(Vec<Candidate>, usize), EsError> {
    let started = Instant::now();
    let column = runtime.config.embedding_column.clone();
    let Some(dim) = runtime.dim else {
        return Err(EsError::unsupported(
            "the index is keyword-only: no embedding dimension is configured",
        ));
    };
    let files = state
        .client
        .get_data_files_by_table_name(&runtime.table, &runtime.namespace)
        .await
        .map_err(crate::error::internal)?;
    if files.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let store = index_store(Arc::clone(&state), runtime.clone()).await?;
    let groups = shard_file_groups(&files, IndexKind::Vector, &column);
    let files_ms = started.elapsed().as_secs_f64() * 1000.0;

    let catalog = state.client.vector_index_catalog();
    let shards = groups.len();
    let nprobe = runtime.nprobe(&state.config.defaults);
    let concurrency = shards.clamp(1, SHARD_CONCURRENCY);
    let outcomes: Vec<Result<VectorShardOutcome, EsError>> =
        futures::stream::iter(groups)
            .map(|(prefix, shard_files)| {
                let store = Arc::clone(&store);
                let catalog = &catalog;
                let query = query.as_slice();
                let column = column.clone();
                async move {
                    let resolve_started = Instant::now();
                    let view = catalog
                        .resolve_cached(&prefix)
                        .await
                        .map_err(crate::error::internal)?;
                    let resolve_ms = resolve_started.elapsed().as_secs_f64() * 1000.0;
                    let lease_started = Instant::now();
                    let mut lease = None;
                    let mut hits = Vec::new();
                    let mut search_ms = 0.0;
                    if let Some(view) = &view {
                        if !lakesoul_io::index::cache::is_loaded(
                            &store,
                            IndexKind::Vector,
                            &prefix,
                            view.commit_id,
                        )
                        .await
                        {
                            match catalog
                                .acquire_lease(&prefix, lease_ttl(), &lease_owner())
                                .await
                            {
                                Ok(Some(handle)) => lease = Some(IndexLease::new(handle)),
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::warn!(
                                        index = %prefix,
                                        "failed to lease vector index: {error}"
                                    );
                                }
                            }
                        }
                        let resolved = vector_resolved(&prefix, view)?;
                        let search_started = Instant::now();
                        hits =
                            search_vector_shard(&store, &resolved, query, top_k, nprobe)
                                .await
                                .map_err(crate::error::internal)?;
                        search_ms = search_started.elapsed().as_secs_f64() * 1000.0;
                    }
                    let lease_ms = lease_started.elapsed().as_secs_f64() * 1000.0;

                    let pending = match &view {
                        Some(view) => pending_shard_files(&shard_files, &view.segments),
                        None => shard_files.clone(),
                    };
                    let tail_started = Instant::now();
                    let mut tail_hits = Vec::new();
                    if !pending.is_empty() {
                        tail_hits =
                            vector_tail_candidates(&pending, &column, query, dim).await?;
                    }
                    let tail_ms = tail_started.elapsed().as_secs_f64() * 1000.0;
                    hits.extend(tail_hits);

                    Ok(VectorShardOutcome {
                        hits,
                        lease,
                        timings: ShardTimings {
                            resolve_ms,
                            lease_ms,
                            search_ms,
                            tail_ms,
                        },
                    })
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

    let mut candidates = Vec::new();
    let mut leases = Vec::new();
    let mut timings = ShardTimings::default();
    for outcome in outcomes {
        let outcome = outcome?;
        if let Some(lease) = outcome.lease {
            leases.push(lease);
        }
        candidates.extend(outcome.hits);
        timings.merge(outcome.timings);
    }
    drop(leases);

    if timing_enabled() {
        tracing::info!(
            target: "lakesoul_es_gateway::timing",
            "search.vector files_ms={:.1} resolve_ms={:.1} lease_ms={:.1} \
             shard_search_ms={:.1} tail_ms={:.1} shards={}",
            files_ms,
            timings.resolve_ms,
            timings.lease_ms,
            timings.search_ms,
            timings.tail_ms,
            shards
        );
    }

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

/// Materialize the columns referenced by `sort`/`aggs` for one row.  Kept
/// separate from `_source`: sorting and aggregating must not depend on the
/// `_source` projection.
fn computed_for_row(
    batch: &RecordBatch,
    row: usize,
    fields: &[String],
) -> Result<Map<String, Value>, EsError> {
    if fields.is_empty() {
        return Ok(Map::new());
    }
    let mut values = Map::new();
    for field in fields {
        let index = batch
            .schema()
            .index_of(field)
            .map_err(crate::error::internal)?;
        values.insert(field.clone(), value_at(batch.column(index), row));
    }
    Ok(values)
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

/// Fragments of `text` around occurrences of the analyzed query `terms`.
///
/// Offsets come from the same analyzer that indexed the text, so the
/// highlighted spans are exactly the tokens the search matched.  HTML in the
/// source text is passed through unchanged (Elasticsearch only escapes it
/// with an explicit `encoder`).
fn highlight_fragments(
    config: &TextIndexConfig,
    text: &str,
    terms: &[String],
    highlight: &Highlight,
) -> Vec<String> {
    if text.is_empty() || terms.is_empty() {
        return Vec::new();
    }
    let Ok(spans) = token_spans(config, text) else {
        return Vec::new();
    };
    let mut matched: Vec<(usize, usize)> = spans
        .iter()
        .filter(|span| terms.iter().any(|term| term == &span.text))
        .map(|span| (span.start, span.end))
        .collect();
    if matched.is_empty() {
        return Vec::new();
    }
    matched.sort_unstable();
    // Merge touching or overlapping tokens into one highlighted span.
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in matched {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }

    let (pre, post) = (highlight.pre_tag(), highlight.post_tag());
    let half = highlight.fragment_size / 2;
    let mut fragments = Vec::new();
    let mut cursor = 0usize;
    for (start, end) in &merged {
        if fragments.len() >= highlight.number_of_fragments {
            break;
        }
        if *start < cursor {
            continue;
        }
        let before = char_window_start(text, *start, half);
        let after = char_window_end(text, *end, highlight.fragment_size - half);
        let mut fragment = String::new();
        let mut position = before;
        for (match_start, match_end) in &merged {
            if *match_start < before || *match_end > after || *match_start < position {
                continue;
            }
            fragment.push_str(&text[position..*match_start]);
            fragment.push_str(pre);
            fragment.push_str(&text[*match_start..*match_end]);
            fragment.push_str(post);
            position = *match_end;
        }
        fragment.push_str(&text[position..after]);
        fragments.push(fragment);
        cursor = after;
    }
    fragments
}

/// Byte offset of the char `max_chars` before `offset` (or the start).
fn char_window_start(text: &str, offset: usize, max_chars: usize) -> usize {
    let mut start = offset;
    for (index, _) in text[..offset].char_indices().rev().take(max_chars) {
        start = index;
    }
    start
}

/// Byte offset after up to `max_chars` chars starting at `offset`.
fn char_window_end(text: &str, offset: usize, max_chars: usize) -> usize {
    let mut end = offset;
    for (index, character) in text[offset..].char_indices().take(max_chars) {
        end = offset + index + character.len_utf8();
    }
    end
}
