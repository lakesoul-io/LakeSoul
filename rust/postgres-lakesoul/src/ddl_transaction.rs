// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Deferred table DDL for PostgreSQL transaction blocks.
//!
//! A block does not write table metadata when the statement runs. It records
//! the change here, keeps a session-local catalog overlay so later statements
//! see the pending definition, and publishes everything with the block's data
//! commits in one metadata transaction at `COMMIT`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use lakesoul_metadata::{
    MetadataChangeSet, TableAlteration, TableDefinitionUpdate, TransactionTableCommit,
};
use lakesoul_metadata_proto::entity::TableInfo;

/// The session-local view of unpublished DDL.
///
/// `PgPublicSchema` consults this before the published metadata view; the
/// transaction hook keeps it in step with [`DdlChanges`].
#[derive(Debug, Default, Clone)]
pub(crate) struct PendingCatalog {
    /// Tables whose provider differs from the published one (`CREATE` or
    /// `ALTER`).
    pub(crate) tables: BTreeMap<String, Arc<dyn TableProvider>>,
    /// Tables hidden because the block dropped them.
    pub(crate) hidden: HashSet<String>,
}

impl PendingCatalog {
    pub(crate) fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).cloned()
    }

    pub(crate) fn is_hidden(&self, name: &str) -> bool {
        self.hidden.contains(name)
    }

    pub(crate) fn register(&mut self, name: String, provider: Arc<dyn TableProvider>) {
        self.hidden.remove(&name);
        self.tables.insert(name, provider);
    }

    pub(crate) fn hide(&mut self, name: String) {
        self.tables.remove(&name);
        self.hidden.insert(name);
    }

    pub(crate) fn remove(&mut self, name: &str) {
        self.tables.remove(name);
        self.hidden.remove(name);
    }

    /// Merge the overlay into a published table-name listing.
    pub(crate) fn table_names(&self, mut published: Vec<String>) -> Vec<String> {
        published.retain(|name| !self.hidden.contains(name));
        for name in self.tables.keys() {
            if !published.contains(name) {
                published.push(name.clone());
            }
        }
        published
    }
}

/// A table created in the block.
#[derive(Debug, Clone)]
pub(crate) struct PendingCreate {
    pub(crate) table_info: TableInfo,
}

/// A table whose definition changed in the block.
#[derive(Debug, Clone)]
pub(crate) struct PendingAlter {
    /// The definition the block started from.
    pub(crate) expected: TableInfo,
    /// Everything the block changed, folded into one update.
    pub(crate) update: TableDefinitionUpdate,
    /// The effective definition later statements use.
    pub(crate) final_info: TableInfo,
}

/// A normalized set of table DDL staged by one transaction block.
#[derive(Debug, Default, Clone)]
pub(crate) struct DdlChanges {
    /// Net creates (`CREATE` not cancelled by a later `DROP`).
    pub(crate) creates: BTreeMap<String, PendingCreate>,
    /// Net alters; a create that is later altered stays in `creates`.
    pub(crate) alters: BTreeMap<String, PendingAlter>,
    /// Net drops.
    pub(crate) drops: BTreeMap<String, TableInfo>,
    /// Table ids whose staged data must never be published (dropped tables,
    /// or tables created and dropped again).
    pub(crate) discarded: HashSet<String>,
}

impl DdlChanges {
    pub(crate) fn metadata_changeset(
        &self,
        data: Vec<TransactionTableCommit>,
    ) -> MetadataChangeSet {
        MetadataChangeSet {
            creates: self
                .creates
                .values()
                .map(|create| create.table_info.clone())
                .collect(),
            alters: self
                .alters
                .values()
                .map(|alter| TableAlteration {
                    expected: alter.expected.clone(),
                    update: alter.update.clone(),
                })
                .collect(),
            drops: self.drops.values().cloned().collect(),
            data,
        }
    }

    /// The effective definition of a table visible in this block, if the
    /// block created or altered it.
    pub(crate) fn table_info(&self, name: &str) -> Option<TableInfo> {
        self.creates
            .get(name)
            .map(|create| create.table_info.clone())
            .or_else(|| self.alters.get(name).map(|alter| alter.final_info.clone()))
    }

    /// Whether the block staged no table DDL.
    pub(crate) fn is_empty(&self) -> bool {
        self.creates.is_empty() && self.alters.is_empty() && self.drops.is_empty()
    }

    pub(crate) fn is_dropped_id(&self, table_id: &str) -> bool {
        self.discarded.contains(table_id)
            || self.drops.values().any(|table| table.table_id == table_id)
    }
}

/// Apply a planned definition update to a table-info clone.
pub(crate) fn apply_update(
    table_info: &TableInfo,
    update: &TableDefinitionUpdate,
) -> TableInfo {
    let mut updated = table_info.clone();
    if let Some((schema, ipc, hash)) = &update.schema {
        updated.table_schema = schema.clone();
        updated.table_schema_arrow_ipc = ipc.clone();
        updated.table_schema_arrow_ipc_json_hash = hash.clone();
    }
    if let Some(properties) = &update.properties {
        updated.properties = properties.to_string();
    }
    updated
}

/// Fold a later definition update into the first one of the same block.
pub(crate) fn merge_update(
    first: &mut TableDefinitionUpdate,
    later: TableDefinitionUpdate,
) {
    if later.schema.is_some() {
        first.schema = later.schema;
    }
    if later.properties.is_some() {
        first.properties = later.properties;
    }
}

/// Permissions a block grants on tables it creates but has not published yet.
#[derive(Debug, Default, Clone)]
pub(crate) struct PendingAcl {
    /// Names created in this block; the connection may use them as its own.
    pub(crate) created: HashSet<String>,
}

impl PendingAcl {
    pub(crate) fn can_access(&self, name: &str) -> bool {
        self.created.contains(name)
    }
}
