// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `POST /{index}/_update_by_query`.
//!
//! WeKnora uses four fixed Painless scripts for enable/disable, tag updates
//! and moving a knowledge base.  They are recognized syntactically and
//! translated into partial-column upserts: LakeSoul merge-on-read keeps the
//! columns that are not part of the update.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, BooleanArray, StringArray};
use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::document::replace_column;
use crate::error::EsError;
use crate::query;
use crate::state::GatewayState;

#[derive(Debug, Deserialize)]
struct UpdateBody {
    query: Value,
    script: Script,
}

#[derive(Debug, Deserialize)]
struct Script {
    source: String,
    #[serde(default)]
    params: serde_json::Map<String, Value>,
}

/// The supported fixed scripts.
#[derive(Debug, Clone, PartialEq)]
enum ScriptAction {
    SetEnabled(bool),
    SetTag(String),
    MoveKnowledgeBase(String),
}

pub async fn update_by_query(
    State(state): State<Arc<GatewayState>>,
    Path(index): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, EsError> {
    let started = Instant::now();
    let runtime = state.index(&index)?;
    let body: UpdateBody = serde_json::from_slice(&body)
        .map_err(|error| EsError::bad_request(format!("invalid update body: {error}")))?;
    let action = parse_script(&body.script)?;

    let filters = query::extract_filters(&body.query, runtime)?;
    let filter = query::filters_to_expr(&filters, runtime)?;
    let rows = query::resolve_rows(&state, runtime, filter).await?;
    let total = rows.as_ref().map(|batch| batch.num_rows()).unwrap_or(0);

    if let Some(batch) = rows {
        let batch = match &action {
            ScriptAction::SetEnabled(enabled) => replace_column(
                &batch,
                "is_enabled",
                Arc::new(BooleanArray::from(vec![Some(*enabled); total])) as ArrayRef,
            )?,
            ScriptAction::SetTag(tag) => replace_column(
                &batch,
                "tag_id",
                Arc::new(StringArray::from(vec![Some(tag.as_str()); total])) as ArrayRef,
            )?,
            ScriptAction::MoveKnowledgeBase(target) => {
                let batch = replace_column(
                    &batch,
                    "knowledge_base_id",
                    Arc::new(StringArray::from(vec![Some(target.as_str()); total]))
                        as ArrayRef,
                )?;
                replace_column(
                    &batch,
                    "tag_id",
                    Arc::new(StringArray::from(vec![Some(""); total])) as ArrayRef,
                )?
            }
        };
        state.upsert(runtime, batch).await?;
    }

    Ok(Json(json!({
        "took": started.elapsed().as_millis() as u64,
        "timed_out": false,
        "total": total,
        "updated": total,
        "deleted": 0,
        "batches": 1,
        "version_conflicts": 0,
        "noops": 0,
        "retries": {"bulk": 0, "search": 0},
        "throttled_millis": 0,
        "requests_per_second": -1.0,
        "throttled_until_millis": 0,
        "failures": []
    })))
}

/// Recognize the four scripts WeKnora sends; whitespace is normalized away
/// so minor formatting differences do not matter.
fn parse_script(script: &Script) -> Result<ScriptAction, EsError> {
    let compact: String = script
        .source
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    match compact.as_str() {
        "ctx._source.is_enabled=true" => Ok(ScriptAction::SetEnabled(true)),
        "ctx._source.is_enabled=false" => Ok(ScriptAction::SetEnabled(false)),
        "ctx._source.tag_id=params.tag_id" => {
            let tag = script
                .params
                .get("tag_id")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    EsError::bad_request("script params must contain a string 'tag_id'")
                })?;
            Ok(ScriptAction::SetTag(tag.to_string()))
        }
        "ctx._source.knowledge_base_id=params.target;ctx._source.tag_id=''" => {
            let target = script
                .params
                .get("target")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    EsError::bad_request("script params must contain a string 'target'")
                })?;
            Ok(ScriptAction::MoveKnowledgeBase(target.to_string()))
        }
        other => Err(EsError::unsupported(format!(
            "unsupported update script: {other}"
        ))),
    }
}
