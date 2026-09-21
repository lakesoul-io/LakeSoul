// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PyO3 bindings for the lakesoul-text crate.
//!
//! Exposes text-index configuration parsing and `build_shard_text_index()` to
//! Python — builds a Tantivy split for a single shard (partition + hash
//! bucket), persists it to object storage and commits it to the catalog.

use std::collections::HashMap;

use lakesoul_common::IndexKind;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::text::builder::TextShardIndexBuilder;
use lakesoul_metadata::index_catalog::{CommitMode, IndexCatalog, IndexCommitView};
use lakesoul_text::{TextIndexConfig, TextSplitEntry};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use tokio::runtime::Runtime;

use crate::vector::create_object_store;

/// Parse a table's ``text_index_columns`` property into a list of config dicts.
///
/// The Rust ``TextIndexConfig`` parser is the single source of truth for the
/// text parameters; the management knobs (``rebuild_mode``,
/// ``max_delta_ratio``) are parsed alongside. Each returned dict has keys:
/// ``column``, ``tokenizer``, ``with_positions``, ``stored``,
/// ``rebuild_mode``, ``max_delta_ratio``.
///
/// Args:
///     value: JSON (single object or array) from the ``text_index_columns``
///         table property.
///
/// Returns:
///     List of config dicts. Empty list for empty input.
#[pyfunction]
fn parse_text_index_configs(py: Python<'_>, value: String) -> PyResult<Vec<Py<PyAny>>> {
    use pyo3::types::PyDict;

    let configs = TextIndexConfig::parse_json(&value).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "invalid text_index_columns property: {}",
            e
        ))
    })?;
    let management = parse_management_entries(&value)?;

    let mut result = Vec::with_capacity(configs.len());
    for (index, c) in configs.into_iter().enumerate() {
        let (rebuild_mode, max_delta_ratio) = management
            .get(index)
            .cloned()
            .unwrap_or_else(|| (default_rebuild_mode(), default_max_delta_ratio()));
        let d = PyDict::new(py);
        d.set_item("column", c.column_name)?;
        d.set_item("tokenizer", c.tokenizer)?;
        d.set_item("with_positions", c.with_positions)?;
        d.set_item("stored", c.stored)?;
        d.set_item("rebuild_mode", rebuild_mode)?;
        d.set_item("max_delta_ratio", max_delta_ratio)?;
        result.push(d.into_any().unbind());
    }
    Ok(result)
}

fn default_rebuild_mode() -> String {
    "auto".to_string()
}

fn default_max_delta_ratio() -> f64 {
    1.0
}

/// The management knobs of one entry, parsed leniently (unknown keys are
/// ignored so the kind parameters stay the text parser's business).
#[derive(serde::Deserialize)]
struct TextManagementEntry {
    #[serde(default = "default_rebuild_mode")]
    rebuild_mode: String,
    #[serde(default = "default_max_delta_ratio")]
    max_delta_ratio: f64,
}

/// Parse the management knobs of every entry of the property value.
fn parse_management_entries(value: &str) -> PyResult<Vec<(String, f64)>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let json: serde_json::Value = serde_json::from_str(trimmed).map_err(invalid_property)?;
    let json = match json {
        serde_json::Value::String(inner) => {
            let inner = inner.trim();
            if inner.is_empty() {
                return Ok(Vec::new());
            }
            serde_json::from_str(inner).map_err(invalid_property)?
        }
        other => other,
    };
    let entries: Vec<TextManagementEntry> = match json {
        serde_json::Value::Array(_) => serde_json::from_value(json).map_err(invalid_property)?,
        serde_json::Value::Object(_) => {
            vec![serde_json::from_value(json).map_err(invalid_property)?]
        }
        _ => return Ok(Vec::new()),
    };
    Ok(entries
        .into_iter()
        .map(|entry| (entry.rebuild_mode, entry.max_delta_ratio))
        .collect())
}

fn invalid_property(error: serde_json::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
        "invalid text_index_columns property: {}",
        error
    ))
}

/// Current compaction statistics of the text index shard containing
/// ``file_paths``.
///
/// Args:
///     file_paths: any data files of the shard (the first one derives the
///         index prefix).
///     text_column: the indexed text column.
///
/// Returns:
///     ``None`` when the shard has no index commit yet, otherwise a tuple
///     ``(generation, splits, base_docs, total_docs)``.  ``base_docs`` is
///     the document count of the compacted base split (the first one) and
///     ``total_docs`` the sum over every split, so ``total_docs - base_docs``
///     is the (upper-bounded) stale part.
#[pyfunction]
fn text_index_stats(
    file_paths: Vec<String>,
    text_column: String,
) -> PyResult<Option<(u64, u64, u64, u64)>> {
    let Some(first) = file_paths.first() else {
        return Ok(None);
    };
    let prefix = lakesoul_io::index::prefix::shard_index_prefix(
        std::slice::from_ref(first),
        IndexKind::Text,
        &text_column,
    );
    // Compaction checks run per shard on every write; keep the runtime and
    // the catalog process-wide instead of paying for a pool per call.
    static RUNTIME: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();
    static CATALOG: tokio::sync::OnceCell<Option<IndexCatalog<TextSplitEntry>>> =
        tokio::sync::OnceCell::const_new();
    let runtime = match RUNTIME.get() {
        Some(runtime) => runtime,
        None => {
            let runtime = Runtime::new().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "failed to create tokio runtime: {}",
                    e
                ))
            })?;
            let _ = RUNTIME.set(runtime);
            RUNTIME.get().expect("runtime was just set")
        }
    };
    runtime.block_on(async move {
        let catalog = CATALOG
            .get_or_init(|| async {
                match lakesoul_metadata::MetaDataClient::from_env().await {
                    Ok(client) => Some(client.index_catalog(IndexKind::Text)),
                    Err(error) => {
                        log::warn!("text index catalog unavailable: {error}");
                        None
                    }
                }
            })
            .await
            .clone();
        let Some(catalog) = catalog else {
            return Ok(None);
        };
        let view = catalog.resolve(&prefix).await.map_err(|error| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to resolve text index at '{prefix}': {error}"
            ))
        })?;
        let Some(view) = view else {
            return Ok(None);
        };
        let base = view
            .segments
            .first()
            .map(|split| split.num_docs)
            .unwrap_or(0);
        let total: u64 = view.segments.iter().map(|split| split.num_docs).sum();
        Ok(Some((
            view.generation,
            view.segments.len() as u64,
            base,
            total,
        )))
    })
}

/// Tokenizer names accepted by the text index.
#[pyfunction]
fn text_supported_tokenizers() -> Vec<String> {
    lakesoul_text::SUPPORTED_TOKENIZERS
        .iter()
        .map(|name| (*name).to_string())
        .collect()
}

/// Build a text index shard (one partition + one hash bucket).
///
/// Reads parquet files, indexes the text column with Tantivy, writes one
/// split bundle to object storage and commits it to the catalog.  When the
/// shard already has a commit the split is appended as a delta, so the reader
/// sees both the base and the newly written rows.
///
/// Args:
///     store_config: dict with S3/local storage credentials (same shape as
///         the vector index builder).
///     file_paths: list of parquet file paths for this shard.
///     pk_column: name of the u64 primary key column.
///     text_column: name of the text column (Utf8/LargeUtf8/Utf8View).
///     tokenizer: tokenizer name (default "jieba").
///     with_positions: index term positions for phrase queries (default True).
///     stored: also store the original text in the index (default False).
///
/// Returns:
///     "ok" on success, raises RuntimeError on failure.
#[pyfunction]
#[pyo3(signature = (
    store_config,
    file_paths,
    pk_column,
    text_column,
    tokenizer = String::from("jieba"),
    with_positions = true,
    stored = false,
))]
fn build_shard_text_index(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    pk_column: String,
    text_column: String,
    tokenizer: String,
    with_positions: bool,
    stored: bool,
) -> PyResult<String> {
    run_shard_text_index(
        store_config,
        file_paths,
        pk_column,
        text_column,
        tokenizer,
        with_positions,
        stored,
        false,
    )
}

/// Rebuild a text index shard from scratch.
///
/// *file_paths* must contain **all** active data files of the shard (base +
/// earlier deltas + the latest batch).  The new split replaces every
/// previous split of the shard as a new index generation.
#[pyfunction]
#[pyo3(signature = (
    store_config,
    file_paths,
    pk_column,
    text_column,
    tokenizer = String::from("jieba"),
    with_positions = true,
    stored = false,
))]
fn rebuild_shard_text_index(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    pk_column: String,
    text_column: String,
    tokenizer: String,
    with_positions: bool,
    stored: bool,
) -> PyResult<String> {
    run_shard_text_index(
        store_config,
        file_paths,
        pk_column,
        text_column,
        tokenizer,
        with_positions,
        stored,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_shard_text_index(
    store_config: HashMap<String, String>,
    file_paths: Vec<String>,
    pk_column: String,
    text_column: String,
    tokenizer: String,
    with_positions: bool,
    stored: bool,
    force_rebuild: bool,
) -> PyResult<String> {
    if !lakesoul_text::is_supported(&tokenizer) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "unknown tokenizer '{}', expected one of: {}",
            tokenizer,
            lakesoul_text::SUPPORTED_TOKENIZERS.join(", ")
        )));
    }
    let store = create_object_store(&store_config)?;

    let config = TextIndexConfig {
        column_name: text_column,
        tokenizer,
        with_positions,
        stored,
    };

    let object_store_options = store_config.clone();
    let default_fs = store_config
        .get("default_fs")
        .cloned()
        .or_else(|| store_config.get("bucket").map(|b| format!("s3://{}", b)));

    let index_prefix = lakesoul_io::index::prefix::shard_index_prefix(
        &file_paths,
        IndexKind::Text,
        &config.column_name,
    );
    let builder = TextShardIndexBuilder::new(
        store,
        config,
        file_paths,
        pk_column,
        object_store_options,
        default_fs,
    );

    let runtime = Runtime::new().map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "failed to create tokio runtime: {}",
            e
        ))
    })?;

    runtime.block_on(async move {
        let client = lakesoul_metadata::MetaDataClient::from_env()
            .await
            .map_err(|error| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "text index build requires metadata access: {error}"
                ))
            })?;
        let catalog: IndexCatalog<TextSplitEntry> = client.index_catalog(IndexKind::Text);
        let resolved = catalog.resolve(&index_prefix).await.map_err(|error| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "failed to resolve text index at '{index_prefix}': {error}"
            ))
        })?;

        let (mode, builder) = match &resolved {
            Some(view) if !force_rebuild => {
                let resolved = to_resolved_shard(&index_prefix, view);
                (CommitMode::Delta, builder.with_base(resolved))
            }
            _ => (CommitMode::Rebuild, builder),
        };
        let outcome = builder.build().await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "text index build failed: {:?}",
                e
            ))
        })?;

        if !outcome.new_splits.is_empty() {
            catalog
                .commit(
                    &outcome.index_prefix,
                    &outcome.header,
                    &outcome.new_splits,
                    mode,
                )
                .await
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "text index commit failed: {e}"
                    ))
                })?;
        }
        Ok("ok".to_string())
    })
}

fn to_resolved_shard(prefix: &str, view: &IndexCommitView<TextSplitEntry>) -> ResolvedIndex {
    // The builder only reads `index_prefix` from the base; empty segments are
    // still kept so the transport payload stays well-formed.
    let segments = serde_json::to_value(&view.segments).unwrap_or(serde_json::json!([]));
    ResolvedIndex {
        kind: IndexKind::Text,
        index_prefix: prefix.to_string(),
        commit_id: view.commit_id,
        generation: view.generation,
        version: view.version,
        header: view.header.clone(),
        segments,
    }
}

/// Register the text submodule.
pub fn init(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    let submodule = PyModule::new(m.py(), "text")?;
    submodule.add_function(wrap_pyfunction!(build_shard_text_index, &submodule)?)?;
    submodule.add_function(wrap_pyfunction!(rebuild_shard_text_index, &submodule)?)?;
    submodule.add_function(wrap_pyfunction!(parse_text_index_configs, &submodule)?)?;
    submodule.add_function(wrap_pyfunction!(text_supported_tokenizers, &submodule)?)?;
    submodule.add_function(wrap_pyfunction!(text_index_stats, &submodule)?)?;
    m.add_submodule(&submodule)?;
    let full_name = format!("{}.text", m.name()?);
    crate::install_module(&full_name, &submodule)?;
    Ok(())
}
