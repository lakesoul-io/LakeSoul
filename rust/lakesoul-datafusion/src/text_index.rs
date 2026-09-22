// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Text index configuration (table property `text_index_columns`).
//!
//! Mirrors the vector index module: the shared [`IndexTableConfig`] wrapper
//! carries the column and the management knobs, the kind parameters
//! (tokenizer/positions/stored) are flattened next to them.

use lakesoul_common::IndexKind;
use lakesoul_metadata::index_catalog::{CommitMode, IndexCatalog, IndexCommitView};
use lakesoul_text::TextSplitEntry;
use rootcause::{bail, report};
use std::collections::HashMap;
use std::time::Duration;
use tracing::warn;

use crate::Result;
use crate::index::build::{
    commit_if_non_empty, group_files_by_shard, group_shard_files, plan_shard_build,
};
use crate::index::config::{
    IndexTableConfig, index_columns_to_json, parse_index_columns,
    parse_index_from_table_properties,
};
use crate::index::gc::{gc_shard_now, should_run_write_gc};
use crate::index::store_for_files;
use lakesoul_io::index::commit::ResolvedIndex;

/// Property key holding the text index configurations (JSON).
pub const TEXT_INDEX_COLUMNS_KEY: &str = IndexKind::Text.property_key();

fn default_tokenizer() -> String {
    lakesoul_text::config::DEFAULT_TOKENIZER.to_string()
}

fn default_with_positions() -> bool {
    true
}

/// Text-specific parameters of one `text_index_columns` entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TextIndexParams {
    /// Tokenizer registered on the index (`jieba`, `default`, `en_stem`, ...).
    #[serde(default = "default_tokenizer")]
    pub tokenizer: String,
    /// Whether to index term positions (phrase queries).
    #[serde(default = "default_with_positions")]
    pub with_positions: bool,
    /// Whether to store the original text in the index.
    #[serde(default)]
    pub stored: bool,
}

impl Default for TextIndexParams {
    fn default() -> Self {
        Self {
            tokenizer: default_tokenizer(),
            with_positions: default_with_positions(),
            stored: false,
        }
    }
}

/// One entry of the `text_index_columns` table property.
pub type TextIndexTableConfig = IndexTableConfig<TextIndexParams>;

impl TextIndexTableConfig {
    /// Convert into the native text index configuration.
    pub fn to_text_index_config(&self) -> lakesoul_text::TextIndexConfig {
        lakesoul_text::TextIndexConfig {
            column_name: self.column.clone(),
            tokenizer: self.params.tokenizer.clone(),
            with_positions: self.params.with_positions,
            stored: self.params.stored,
        }
    }
}

/// Serialize configurations into the `text_index_columns` property value.
pub fn text_index_columns_to_json(configs: &[TextIndexTableConfig]) -> String {
    index_columns_to_json(configs)
}

/// Parse a `text_index_columns` property value.
pub fn parse_text_index_columns(raw: Option<&str>) -> Result<Vec<TextIndexTableConfig>> {
    parse_index_columns(raw)
}

/// Extract the text index configurations from a table's raw properties JSON.
pub fn parse_text_index_from_table_properties(
    properties_json: &str,
) -> Result<Vec<TextIndexTableConfig>> {
    parse_index_from_table_properties(properties_json, TEXT_INDEX_COLUMNS_KEY)
}

/// Validate that a table schema can support the configured text indexes.
pub fn validate_text_index_configs(
    configs: &[TextIndexTableConfig],
    schema: &arrow::datatypes::Schema,
    primary_keys: &[String],
) -> Result<()> {
    use arrow::datatypes::DataType;
    if configs.is_empty() {
        return Ok(());
    }
    let Some(pk_column) = primary_keys.first() else {
        bail!(
            "a text index requires an id column: pass primary_keys=[...] \
             when creating a table with a text index (the index maps hits \
             to primary key values)"
        );
    };
    let Some(pk_index) = schema.index_of(pk_column).ok() else {
        bail!("text index primary key '{pk_column}' not found in table schema");
    };
    match schema.field(pk_index).data_type() {
        DataType::UInt64 | DataType::Int64 => {}
        other => bail!(
            "text index primary key '{pk_column}' must be UInt64 or Int64, got {other}"
        ),
    }
    for config in configs {
        let column = &config.column;
        config.management.validate(IndexKind::Text, column)?;
        if !lakesoul_text::is_supported(&config.params.tokenizer) {
            bail!(
                "text index column '{column}' uses unsupported tokenizer '{}'; \
                 supported: {}",
                config.params.tokenizer,
                lakesoul_text::SUPPORTED_TOKENIZERS.join(", ")
            );
        }
        let Some(column_index) = schema.index_of(column).ok() else {
            bail!("text index column '{column}' not found in table schema");
        };
        match schema.field(column_index).data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
            other => bail!(
                "text index column '{column}' must be Utf8, LargeUtf8 or Utf8View, got {other}"
            ),
        }
    }
    Ok(())
}

/// One text index split artifact, converted for the catalog's opaque
/// transport type.
fn to_resolved_shard(
    prefix: &str,
    view: &IndexCommitView<TextSplitEntry>,
) -> Result<ResolvedIndex> {
    let segments = serde_json::to_value(&view.segments)
        .map_err(|error| report!("failed to serialize text index splits: {}", error))?;
    Ok(ResolvedIndex {
        kind: IndexKind::Text,
        index_prefix: prefix.to_string(),
        commit_id: view.commit_id,
        generation: view.generation,
        version: view.version,
        header: view.header.clone(),
        segments,
    })
}

/// Whether an object file in a text shard directory belongs to the index.
fn is_text_split_file(name: &str) -> bool {
    name.ends_with(".split")
}

/// Build (or incrementally update) the text index for newly committed files
/// of a write, then optionally garbage collect.
///
/// Files are grouped by `(partition_desc, hash_bucket_id)`; each group is one
/// index shard.  An existing commit receives a delta split built from the new
/// files; a fresh shard (or a rebuilt one) reads all of its active files.
/// Stale splits are harmless for correctness (the reader verifies candidates
/// against the current rows exactly) and are compacted away once the shard's
/// delta history outweighs its compacted base (`max_delta_ratio`).
pub async fn auto_build_text_index(
    configs: &[TextIndexTableConfig],
    primary_keys: &[String],
    object_store_options: &HashMap<String, String>,
    partition_files: &HashMap<String, (Vec<String>, u64)>,
    all_active_files: Option<&[String]>,
    catalog: &IndexCatalog<TextSplitEntry>,
) -> Result<usize> {
    if configs.is_empty() || partition_files.is_empty() {
        return Ok(0);
    }
    let Some(pk_column) = primary_keys.first() else {
        bail!("a text index requires a table with a primary key");
    };
    let Some(first_file) = partition_files
        .values()
        .find_map(|(files, _)| files.first())
    else {
        return Ok(0);
    };
    let store = store_for_files(first_file, object_store_options)?;

    let mut built = 0usize;
    for config in configs {
        let management = &config.management;
        let auto_rebuild = management.rebuild_mode.eq_ignore_ascii_case("auto");
        // Files of each shard, for a full rebuild once its drift fires.
        let shard_all_files: HashMap<String, Vec<String>> = if auto_rebuild {
            match all_active_files {
                Some(all) => group_files_by_shard(all, IndexKind::Text, &config.column),
                None => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
        let mut failures: Vec<String> = Vec::new();
        for shard in group_shard_files(partition_files) {
            let (partition_desc, bucket, bucket_files) =
                (shard.partition_desc, shard.bucket, shard.files);
            let prefix = lakesoul_io::index::prefix::shard_index_prefix(
                &bucket_files,
                IndexKind::Text,
                &config.column,
            );
            let resolve_started = std::time::Instant::now();
            let resolved = match catalog.resolve(&prefix).await {
                Ok(view) => view,
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: failed to resolve index: {error}"
                    ));
                    continue;
                }
            };
            let resolve_ms = resolve_started.elapsed().as_secs_f64() * 1000.0;
            let full_shard_files = shard_all_files.get(&prefix).cloned();
            // Compaction: once the accumulated delta history of a shard
            // outweighs its compacted base, rebuild it from all active files
            // into a single fresh split.
            // The drift ratio comes from the commit already resolved above,
            // so the rebuild decision costs no extra catalog round trip.
            let should_rebuild = auto_rebuild
                && full_shard_files
                    .as_ref()
                    .is_some_and(|files| !files.is_empty())
                && resolved.as_ref().is_some_and(|view| {
                    lakesoul_text::drift_exceeds_threshold(
                        &view.segments,
                        management.max_delta_ratio,
                    )
                });
            // The heal-from-scratch fallback below must read the whole shard,
            // not just the new files of a delta build.
            let rebuild_files = full_shard_files
                .clone()
                .unwrap_or_else(|| bucket_files.clone());
            let plan = plan_shard_build(
                resolved.as_ref(),
                should_rebuild,
                full_shard_files,
                bucket_files,
            );
            let config_for_build = config.to_text_index_config();
            let mut builder = lakesoul_io::text::builder::TextShardIndexBuilder::new(
                store.clone(),
                config_for_build.clone(),
                plan.files.clone(),
                pk_column.clone(),
                object_store_options.clone(),
                None,
            );
            if let Some(view) = &plan.base {
                builder = builder.with_base(to_resolved_shard(&prefix, view)?);
            }
            let mut commit_mode = plan.mode;
            let build_started = std::time::Instant::now();
            let outcome = match builder.build().await {
                Ok(outcome) => outcome,
                Err(error) if commit_mode == CommitMode::Delta => {
                    // The resolved commit points at files that no longer
                    // exist; heal by rebuilding the shard from all of its
                    // active data files instead of failing the write.
                    warn!(
                        "incremental text index build for '{prefix}' failed ({error}); \
                         rebuilding the shard from scratch"
                    );
                    commit_mode = CommitMode::Rebuild;
                    match lakesoul_io::text::builder::TextShardIndexBuilder::new(
                        store.clone(),
                        config_for_build,
                        rebuild_files,
                        pk_column.clone(),
                        object_store_options.clone(),
                        None,
                    )
                    .build()
                    .await
                    {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            failures.push(format!(
                                "partition {partition_desc:?} bucket {bucket}: {error}"
                            ));
                            continue;
                        }
                    }
                }
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: {error}"
                    ));
                    continue;
                }
            };
            let build_ms = build_started.elapsed().as_secs_f64() * 1000.0;
            let commit_started = std::time::Instant::now();
            match commit_if_non_empty(
                catalog,
                &prefix,
                &outcome.header,
                &outcome.new_splits,
                commit_mode,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: {error}"
                    ));
                    continue;
                }
            }
            let commit_ms = commit_started.elapsed().as_secs_f64() * 1000.0;
            built += 1;
            let mut gc_ms = 0.0;
            if management.gc_enabled && should_run_write_gc(management.gc_grace_seconds) {
                let gc_started = std::time::Instant::now();
                if let Err(error) = gc_shard_now(
                    &store,
                    catalog,
                    &prefix,
                    Duration::from_secs(management.gc_grace_seconds),
                    management.gc_keep_generations,
                    &is_text_split_file,
                )
                .await
                {
                    warn!("text index gc failed for '{prefix}': {error}");
                }
                gc_ms = gc_started.elapsed().as_secs_f64() * 1000.0;
            }
            debug!(
                prefix = %prefix,
                rebuilt = should_rebuild,
                resolve_ms,
                build_ms,
                commit_ms,
                gc_ms,
                "text index shard updated"
            );
        }
        if !failures.is_empty() {
            return Err(report!(
                "text index build failed for column '{}': {}",
                config.column,
                failures.join("; ")
            ));
        }
    }
    Ok(built)
}

/// Rebuild every text index shard of a table from scratch (all active data
/// files re-read into fresh splits), regardless of the configured
/// `rebuild_mode`/`max_delta_ratio`.
///
/// This is the manual compaction entry point: a shard that accumulated many
/// delta splits collapses into one, dropping superseded and deleted
/// documents.  Returns the number of rebuilt shards; fails loudly when any
/// shard of a configured column fails.
pub async fn rebuild_text_index(
    client: &lakesoul_metadata::MetaDataClient,
    table_name: &str,
    namespace: &str,
    primary_keys: &[String],
    object_store_options: HashMap<String, String>,
) -> Result<usize> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    let configs = parse_text_index_from_table_properties(&table_info.properties)?;
    if configs.is_empty() {
        return Ok(0);
    }
    let Some(pk_column) = primary_keys.first() else {
        bail!("a text index requires a table with a primary key");
    };
    let all_active_files = client
        .get_data_files_by_table_name(table_name, namespace)
        .await?;
    let Some(first_file) = all_active_files.first() else {
        return Ok(0);
    };
    let store = store_for_files(first_file, &object_store_options)?;
    let catalog = client.index_catalog::<TextSplitEntry>(IndexKind::Text);

    let mut rebuilt = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for config in &configs {
        let mut shards: Vec<(String, Vec<String>)> =
            group_files_by_shard(&all_active_files, IndexKind::Text, &config.column)
                .into_iter()
                .collect();
        shards.sort_by(|a, b| a.0.cmp(&b.0));
        for (prefix, files) in shards {
            let result = lakesoul_io::text::builder::TextShardIndexBuilder::new(
                store.clone(),
                config.to_text_index_config(),
                files,
                pk_column.clone(),
                object_store_options.clone(),
                None,
            )
            .build()
            .await;
            match result {
                Ok(outcome) => {
                    match commit_if_non_empty(
                        &catalog,
                        &prefix,
                        &outcome.header,
                        &outcome.new_splits,
                        CommitMode::Rebuild,
                    )
                    .await
                    {
                        Ok(true) => rebuilt += 1,
                        Ok(false) => {}
                        Err(error) => failures.push(format!("{prefix}: {error}")),
                    }
                }
                Err(error) => failures.push(format!("{prefix}: {error}")),
            }
        }
    }
    if !failures.is_empty() {
        return Err(report!(
            "text index rebuild failed for column(s): {}",
            failures.join("; ")
        ));
    }
    Ok(rebuilt)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field};
        arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("body", DataType::Utf8, true),
        ])
    }

    #[test]
    fn parses_default_tokenizer() {
        let configs = parse_text_index_columns(Some(r#"[{"column":"body"}]"#)).unwrap();
        assert_eq!(configs[0].params.tokenizer, "jieba");
        assert!(configs[0].params.with_positions);
        validate_text_index_configs(&configs, &schema(), &["id".to_string()]).unwrap();
    }

    #[test]
    fn rejects_unknown_tokenizer_and_bad_pk() {
        let configs = parse_text_index_columns(Some(
            r#"[{"column":"body","tokenizer":"does-not-exist"}]"#,
        ))
        .unwrap();
        let error = validate_text_index_configs(&configs, &schema(), &["id".to_string()])
            .unwrap_err();
        assert!(error.to_string().contains("tokenizer"), "{error}");

        let configs = parse_text_index_columns(Some(r#"[{"column":"body"}]"#)).unwrap();
        assert!(
            validate_text_index_configs(&configs, &schema(), &[]).is_err(),
            "a text index requires a primary key"
        );
    }
}
