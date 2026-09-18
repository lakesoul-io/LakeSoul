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
/// The Rust side is the single source of truth for parsing
/// (``TextIndexConfig::parse_json``). Each returned dict has keys:
/// ``column``, ``tokenizer``, ``with_positions``, ``stored``.
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

    let mut result = Vec::with_capacity(configs.len());
    for c in configs {
        let d = PyDict::new(py);
        d.set_item("column", c.column_name)?;
        d.set_item("tokenizer", c.tokenizer)?;
        d.set_item("with_positions", c.with_positions)?;
        d.set_item("stored", c.stored)?;
        result.push(d.into_any().unbind());
    }
    Ok(result)
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
    m.add_submodule(&submodule)?;
    let full_name = format!("{}.text", m.name()?);
    crate::install_module(&full_name, &submodule)?;
    Ok(())
}
