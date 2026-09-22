// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Out-of-band (deferred) secondary-index maintenance.
//!
//! When [`crate::set_defer_index_maintenance`] is enabled, writes only commit
//! their data files.  This module finds the shards whose active data files
//! are not yet covered by the index commit (coverage is recorded on every
//! artifact, see [`lakesoul_common::index::pending_shard_files`]) and builds
//! their delta or rebuild indexes, using exactly the same builders and policy
//! as the inline write path.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use lakesoul_common::IndexKind;
use lakesoul_common::index::pending_shard_files;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_text::TextSplitEntry;
use rootcause::compat::boxed_error::IntoBoxedError;
use tracing::{debug, warn};

use crate::catalog::create_io_config_builder;
use crate::index::build::group_files_by_shard;

/// Build every index shard of `table_name` whose active data files are not
/// yet covered by its current commit.  Returns the number of shards built.
///
/// The function is a no-op for tables without declared index columns, for
/// shards whose commits carry no coverage information (legacy), and while
/// another writer/index build holds the shard.
pub async fn build_pending_indices(
    client: MetaDataClientRef,
    table_name: &str,
    namespace: &str,
) -> Result<usize> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await
        .map_err(|error| DataFusionError::External(error.into()))?
    else {
        return Ok(0);
    };
    let text_configs =
        crate::text_index::parse_text_index_from_table_properties(&table_info.properties)
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
    let vector_configs = crate::vector_index::parse_vector_index_from_table_properties(
        &table_info.properties,
    )
    .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
    if text_configs.is_empty() && vector_configs.is_empty() {
        return Ok(0);
    }

    let builder = create_io_config_builder(
        client.clone(),
        Some(table_name),
        false,
        namespace,
        Default::default(),
        Default::default(),
    )
    .await
    .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
    let io_config = builder.build();
    let Some(pk_column) = io_config.primary_keys_slice().first().cloned() else {
        debug!(table_name, "deferred index maintenance: no primary key");
        return Ok(0);
    };
    let object_store_options = io_config.object_store_options().clone();

    let all_files = client
        .get_data_files_by_table_name(table_name, namespace)
        .await
        .map_err(|error| DataFusionError::External(error.into()))?;
    if all_files.is_empty() {
        debug!(table_name, "deferred index maintenance: no active files");
        return Ok(0);
    }
    debug!(
        table_name,
        files = all_files.len(),
        text_configs = text_configs.len(),
        vector_configs = vector_configs.len(),
        pk = %pk_column,
        "deferred index maintenance scan"
    );

    let mut built = 0usize;
    for config in &text_configs {
        let catalog = client.index_catalog::<TextSplitEntry>(IndexKind::Text);
        let pending =
            pending_for_column(&catalog, &all_files, IndexKind::Text, &config.column)
                .await;
        if pending.is_empty() {
            debug!(column = %config.column, "text index fully covered");
            continue;
        }
        debug!(
            column = %config.column,
            shards = pending.len(),
            files = pending.values().map(|(files, _)| files.len()).sum::<usize>(),
            "building pending text index shards"
        );
        built += crate::text_index::auto_build_text_index(
            std::slice::from_ref(config),
            std::slice::from_ref(&pk_column),
            &object_store_options,
            &pending,
            Some(&all_files),
            &catalog,
        )
        .await
        .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
    }

    for config in &vector_configs {
        let catalog = client.vector_index_catalog();
        let pending =
            pending_for_column(&catalog, &all_files, IndexKind::Vector, &config.column)
                .await;
        if pending.is_empty() {
            debug!(column = %config.column, "vector index fully covered");
            continue;
        }
        debug!(
            column = %config.column,
            shards = pending.len(),
            files = pending.values().map(|(files, _)| files.len()).sum::<usize>(),
            "building pending vector index shards"
        );
        built += crate::vector_index::auto_build_vector_index(
            std::slice::from_ref(config),
            std::slice::from_ref(&pk_column),
            &object_store_options,
            &pending,
            Some(&all_files),
            &catalog,
        )
        .await
        .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
    }

    Ok(built)
}

/// Group the active files by shard and keep the shards with uncovered files.
async fn pending_for_column<S: lakesoul_common::CatalogSegment>(
    catalog: &lakesoul_metadata::index_catalog::IndexCatalog<S>,
    all_files: &[String],
    kind: IndexKind,
    column: &str,
) -> HashMap<String, (Vec<String>, u64)> {
    let mut pending: HashMap<String, (Vec<String>, u64)> = HashMap::new();
    for (prefix, files) in group_files_by_shard(all_files, kind, column) {
        let missing = match catalog.resolve(&prefix).await {
            // A shard without a commit has no indexed files at all.
            Ok(None) => files.clone(),
            // A commit without recorded coverage is "unknown": leave it
            // alone instead of indexing the shard again.
            Ok(Some(view)) => pending_shard_files(&files, &view.segments),
            Err(error) => {
                warn!(index = %prefix, "failed to resolve {kind} index: {error}");
                continue;
            }
        };
        if !missing.is_empty() {
            pending.insert(prefix, (missing, 0));
        }
    }
    pending
}
