// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A read-only footprint of a view chain.
//!
//! The report walks the registered views from a root (a target table or any
//! view id) to its base tables, and lists every materialized table and every
//! `ivm.states` entry with its file bytes, file count and optional logical row
//! count.  Bytes and files come from LakeSoul metadata only; `include_rows`
//! additionally runs `COUNT(*)` over each table.
//!
//! This is the diagnostic for state amplification: a derived step, a
//! row-level multi-aggregate state, a rank state and a split-target
//! maintenance MV all appear as ordinary tables in the chain, so their sizes
//! can be compared before changing a view definition.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::Int64Array;
use datafusion::prelude::SessionContext;
use rootcause::report;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::metadata::ViewInfo;
use crate::runtime::{IvmRuntime, ViewSpec};
use crate::table::IvmTable;

/// Options for [`IvmRuntime::state_inventory_with_options`].
#[derive(Debug, Clone)]
pub struct InventoryOptions {
    /// Run `COUNT(*)` over every materialized table and state table.
    ///
    /// The count is exact and reads the tables.  Leave it off for a
    /// metadata-only report on a large view chain.
    pub include_rows: bool,
    /// How many tables the text report lists in its "top by bytes" section.
    pub top_tables: usize,
}

impl Default for InventoryOptions {
    fn default() -> Self {
        Self {
            include_rows: true,
            top_tables: 10,
        }
    }
}

/// One materialized table or internal state table of a view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableFootprint {
    /// The LakeSoul table id.
    pub table_id: String,
    /// The LakeSoul table name.
    pub table_name: String,
    /// The namespace of the table.
    pub namespace: String,
    /// The roles the table plays for its view (`mv`, `state`, `join_left`,
    /// `rank_state`, ...).  A physical table registered under several roles
    /// appears once.
    pub roles: Vec<String>,
    /// Logical rows, when `include_rows` was set and the count succeeded.
    pub rows: Option<u64>,
    /// Active file bytes recorded in metadata.
    pub bytes: u64,
    /// Active data files.
    pub files: u64,
    /// Active files whose recorded size was unknown (not included in `bytes`).
    pub unknown_size_files: u64,
    /// Storage partitions, including empty ones.
    pub partitions: u64,
    /// Whether some file metadata could not be read; `bytes` and `files` are
    /// then a lower bound.
    pub incomplete: bool,
    /// The number of schema fields.
    pub columns: usize,
    /// The merge key of the table.
    pub primary_keys: Vec<String>,
    /// The bucket columns of the table.
    pub bucket_columns: Vec<String>,
    /// The range partition columns of the table.
    pub range_partition_columns: Vec<String>,
    /// The configured hash bucket number.
    pub hash_bucket_num: String,
    /// The physical file format (`parquet`, `vortex`, ...).
    pub file_format: String,
}

impl TableFootprint {
    /// Bytes per logical row, when both values are known.
    pub fn bytes_per_row(&self) -> Option<f64> {
        self.rows
            .filter(|rows| *rows > 0)
            .map(|rows| self.bytes as f64 / rows as f64)
    }
}

/// One source of a view: another registered view or a base table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRef {
    /// The table id of the source.
    pub table_id: String,
    /// The table name of the source.
    pub table_name: String,
    /// Whether the source is itself a registered view in this chain.
    pub is_view: bool,
}

/// One view in the chain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewInventory {
    /// The view id.
    pub view_id: String,
    /// The table id of the materialized output.
    pub table_id: String,
    /// The table name of the materialized output.
    pub table_name: String,
    /// The namespace of the output table.
    pub namespace: String,
    /// The stable view kind (`join`, `sum_count`, `multi_agg`, ...).
    pub kind: String,
    /// Whether the output is an internal derived step (`__ivm_stepN`).
    pub derived: bool,
    /// Whether the view is a Tier-2 fallback recompute.
    pub fallback: bool,
    /// The lifecycle status (`active` / `rebuilding` / `dirty`).
    pub status: String,
    /// The last refresh action, when one was recorded.
    pub last_action: Option<String>,
    /// The last committed epoch.
    pub last_epoch: i64,
    /// The sources this view reads, in definition order.
    pub sources: Vec<SourceRef>,
    /// The materialized output followed by every internal state table.
    pub tables: Vec<TableFootprint>,
    /// Logical rows summed over `tables`, when every count succeeded.
    pub rows: Option<u64>,
    /// Bytes summed over `tables`.
    pub bytes: u64,
    /// Files summed over `tables`.
    pub files: u64,
}

/// Chain-wide totals.  A table shared by several views is counted once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryTotals {
    /// Registered views in the chain.
    pub views: usize,
    /// Unique materialized and state tables.
    pub tables: usize,
    /// Logical rows over the unique tables, when every count succeeded.
    pub rows: Option<u64>,
    /// File bytes over the unique tables.
    pub bytes: u64,
    /// Files over the unique tables.
    pub files: u64,
}

/// Totals for one primary table role.  A table registered under several
/// roles is counted under its first role, so the role totals add up to the
/// chain totals without double counting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleTotal {
    /// The primary role (`mv`, `join_left`, `rank_state`, ...).
    pub role: String,
    /// Unique tables with this primary role.
    pub tables: usize,
    /// Logical rows, when every count succeeded.
    pub rows: Option<u64>,
    /// File bytes.
    pub bytes: u64,
    /// Files.
    pub files: u64,
}

/// A read-only state inventory of one view chain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateInventory {
    /// The view the report started from.
    pub root_view_id: String,
    /// The materialized table id of the root view.
    pub root_table_id: String,
    /// The materialized table name of the root view.
    pub root_table_name: String,
    /// The namespace of the root output table.
    pub root_namespace: String,
    /// Wall-clock generation time (unix milliseconds).
    pub generated_at_ms: i64,
    /// Whether row counts were requested.
    pub include_rows: bool,
    /// How many tables the text report lists in "top by bytes".
    pub top_tables: usize,
    /// The reachable views, sources first.
    pub views: Vec<ViewInventory>,
    /// Base tables read by the chain, sorted by name.  They are not part of
    /// the state totals.
    pub base_sources: Vec<SourceRef>,
    /// Totals over the unique materialized and state tables.
    pub totals: InventoryTotals,
    /// Totals per primary table role, largest first.
    pub role_totals: Vec<RoleTotal>,
    /// Problems that did not stop the report (missing state tables, unknown
    /// file sizes, failed counts, ...).
    pub warnings: Vec<String>,
}

impl StateInventory {
    /// Render the report as a human-readable table.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "IVM state inventory");
        let _ = writeln!(
            out,
            "root: {} ({})",
            self.root_table_name, self.root_view_id
        );
        let _ = writeln!(out, "generated_at_ms: {}", self.generated_at_ms);
        let _ = writeln!(
            out,
            "rows included: {}",
            if self.include_rows {
                "yes"
            } else {
                "no (--no-rows)"
            }
        );
        let _ = writeln!(
            out,
            "views={} tables={} rows={} bytes={} files={}",
            self.totals.views,
            self.totals.tables,
            format_rows(self.totals.rows),
            format_bytes(self.totals.bytes),
            self.totals.files
        );
        if !self.role_totals.is_empty() {
            let _ = writeln!(out, "BY PRIMARY ROLE");
            for role in &self.role_totals {
                let _ = writeln!(
                    out,
                    "  {:<20} tables={:>3} bytes={:>10} files={:>6} rows={}",
                    role.role,
                    role.tables,
                    format_bytes(role.bytes),
                    role.files,
                    format_rows(role.rows)
                );
            }
        }
        let _ = writeln!(out, "{}", "=".repeat(100));

        for (index, view) in self.views.iter().enumerate() {
            let mut flags = String::new();
            if view.derived {
                flags.push_str(" [derived]");
            }
            if view.fallback {
                flags.push_str(" [fallback]");
            }
            let _ = writeln!(
                out,
                "[{}/{}] {}{} kind={} status={} epoch={} last={}",
                index + 1,
                self.views.len(),
                view.table_name,
                flags,
                view.kind,
                view.status,
                view.last_epoch,
                view.last_action.as_deref().unwrap_or("-")
            );
            if !view.sources.is_empty() {
                let sources = view
                    .sources
                    .iter()
                    .map(|source| {
                        if source.is_view {
                            format!("{}[view]", source.table_name)
                        } else {
                            source.table_name.clone()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = writeln!(out, "      sources: {sources}");
            }
            for table in &view.tables {
                let _ = writeln!(
                    out,
                    "      {:<52} rows={:>12} bytes={:>10} files={:>6} parts={:>5} cols={:>3} roles={}{}",
                    table.table_name,
                    format_rows(table.rows),
                    format_bytes(table.bytes),
                    table.files,
                    table.partitions,
                    table.columns,
                    table.roles.join(","),
                    if table.incomplete { " INCOMPLETE" } else { "" }
                );
            }
        }

        let mut seen = HashSet::new();
        let mut tables = Vec::new();
        for view in &self.views {
            for table in &view.tables {
                if seen.insert(table.table_id.as_str()) {
                    tables.push(table);
                }
            }
        }
        tables.sort_by(|left, right| {
            right
                .bytes
                .cmp(&left.bytes)
                .then_with(|| left.table_name.cmp(&right.table_name))
        });
        if !tables.is_empty() {
            let _ = writeln!(out);
            let _ = writeln!(out, "TOP TABLES BY BYTES");
            for (index, table) in tables.iter().take(self.top_tables).enumerate() {
                let ratio = table
                    .bytes_per_row()
                    .map(|ratio| format!("{ratio:.1}"))
                    .unwrap_or_else(|| "-".to_string());
                let _ = writeln!(
                    out,
                    "  {}. {:<52} bytes={:>10} rows={:>12} files={:>6} bytes/row={:>8} roles={}",
                    index + 1,
                    table.table_name,
                    format_bytes(table.bytes),
                    format_rows(table.rows),
                    table.files,
                    ratio,
                    table.roles.join(",")
                );
            }
        }

        if !self.base_sources.is_empty() {
            let _ = writeln!(out);
            let _ = writeln!(
                out,
                "BASE SOURCES (not counted): {}",
                self.base_sources
                    .iter()
                    .map(|source| source.table_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if !self.warnings.is_empty() {
            let _ = writeln!(out);
            let _ = writeln!(out, "WARNINGS");
            for warning in &self.warnings {
                let _ = writeln!(out, "  - {warning}");
            }
        }
        out
    }

    /// Serialize the report as pretty JSON.
    pub fn to_json_pretty(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl IvmRuntime {
    /// Build a state inventory for `view_id` and its upstream chain.
    ///
    /// The root may be a target table id or any registered view id.  The
    /// report is read-only and does not refresh the chain.  Row counts are
    /// enabled; use [`Self::state_inventory_with_options`] to skip them.
    pub async fn state_inventory(&self, view_id: &str) -> Result<StateInventory> {
        self.state_inventory_with_options(view_id, &InventoryOptions::default())
            .await
    }

    /// Build a state inventory for a table by namespace and name.
    pub async fn state_inventory_for_table(
        &self,
        namespace: &str,
        table_name: &str,
    ) -> Result<StateInventory> {
        self.state_inventory_for_table_with_options(
            namespace,
            table_name,
            &InventoryOptions::default(),
        )
        .await
    }

    /// Build a state inventory for a table by namespace and name, with
    /// explicit options.
    pub async fn state_inventory_for_table_with_options(
        &self,
        namespace: &str,
        table_name: &str,
        options: &InventoryOptions,
    ) -> Result<StateInventory> {
        let table = self.open_table(table_name, namespace).await?;
        let view_id = if self
            .metadata()
            .get_view_spec(&table.table_id)
            .await?
            .is_some()
        {
            table.table_id.clone()
        } else {
            // A Rust API view may use a view id that is not its output table
            // id.  Resolve the registered view that materializes into this
            // table.
            let mut resolved = None;
            for info in self.metadata().list_views().await? {
                let Some(spec) = self.load_inventory_view_spec(&info.view_id).await?
                else {
                    continue;
                };
                if spec.mv_table_id() == table.table_id {
                    resolved = Some(info.view_id);
                    break;
                }
            }
            resolved.ok_or_else(|| {
                report!(
                    "table {}.{} is not an IVM view; nothing to inventory",
                    table.namespace,
                    table.table_name
                )
            })?
        };
        self.state_inventory_with_options(&view_id, options).await
    }

    /// Build a state inventory for `view_id` and its upstream chain.
    pub async fn state_inventory_with_options(
        &self,
        view_id: &str,
        options: &InventoryOptions,
    ) -> Result<StateInventory> {
        let root_spec =
            self.load_inventory_view_spec(view_id)
                .await?
                .ok_or_else(|| {
                    report!("table {view_id} is not an IVM view; nothing to inventory")
                })?;
        let root_table = self.open_table_by_id(root_spec.mv_table_id()).await?;

        let infos = self
            .metadata()
            .list_views()
            .await?
            .into_iter()
            .map(|info| (info.view_id.clone(), info))
            .collect::<HashMap<_, _>>();
        let (specs, edges, base_source_ids) = self.load_inventory_chain(view_id).await?;
        let order = inventory_topological_order(&specs, &edges)?;

        let mut warnings = Vec::new();
        let mut views = Vec::with_capacity(order.len());
        for id in &order {
            let spec = specs
                .get(id)
                .ok_or_else(|| report!("view {id} disappeared from the chain"))?;
            let view = self
                .inventory_one_view(
                    id,
                    spec,
                    infos.get(id),
                    options,
                    &specs,
                    &mut warnings,
                )
                .await?;
            views.push(view);
        }

        let mut base_sources = Vec::with_capacity(base_source_ids.len());
        for id in base_source_ids {
            match self.open_table_by_id(&id).await {
                Ok(table) => base_sources.push(SourceRef {
                    table_id: id,
                    table_name: table.table_name,
                    is_view: false,
                }),
                Err(error) => {
                    warnings.push(format!("base source {id} is missing: {error}"));
                    base_sources.push(SourceRef {
                        table_id: id.clone(),
                        table_name: id,
                        is_view: false,
                    });
                }
            }
        }
        base_sources.sort_by(|left, right| left.table_name.cmp(&right.table_name));

        let totals = inventory_totals(&views, &mut warnings);
        let role_totals = inventory_role_totals(&views);
        Ok(StateInventory {
            root_view_id: view_id.to_string(),
            root_table_id: root_table.table_id,
            root_table_name: root_table.table_name,
            root_namespace: root_table.namespace,
            generated_at_ms: crate::now_ms(),
            include_rows: options.include_rows,
            top_tables: options.top_tables,
            views,
            base_sources,
            totals,
            role_totals,
            warnings,
        })
    }

    async fn load_inventory_view_spec(&self, view_id: &str) -> Result<Option<ViewSpec>> {
        let Some(value) = self.metadata().get_view_spec(view_id).await? else {
            return Ok(None);
        };
        serde_json::from_value(value)
            .map(Some)
            .map_err(|error| report!("view {view_id} has an invalid spec: {error}"))
    }

    /// Load every reachable view spec, the view-to-view edges and the base
    /// source ids.
    async fn load_inventory_chain(
        &self,
        root: &str,
    ) -> Result<(
        HashMap<String, ViewSpec>,
        HashMap<String, Vec<String>>,
        Vec<String>,
    )> {
        let mut specs = HashMap::new();
        let mut edges = HashMap::new();
        let mut base_sources = HashSet::new();
        let mut queued = HashSet::new();
        let mut queue = vec![root.to_string()];

        while let Some(id) = queue.pop() {
            if !queued.insert(id.clone()) {
                continue;
            }
            let Some(spec) = self.load_inventory_view_spec(&id).await? else {
                return Err(report!(
                    "view {id} is a source of view {root} but is not registered"
                ));
            };
            let mut view_sources = Vec::new();
            for source in spec.source_table_ids() {
                match self.load_inventory_view_spec(source).await? {
                    Some(_) => {
                        view_sources.push(source.to_string());
                        queue.push(source.to_string());
                    }
                    None => {
                        base_sources.insert(source.to_string());
                    }
                }
            }
            edges.insert(id.clone(), view_sources);
            specs.insert(id, spec);
        }

        let mut base_sources = base_sources.into_iter().collect::<Vec<_>>();
        base_sources.sort();
        Ok((specs, edges, base_sources))
    }

    async fn inventory_one_view(
        &self,
        view_id: &str,
        spec: &ViewSpec,
        info: Option<&ViewInfo>,
        options: &InventoryOptions,
        specs: &HashMap<String, ViewSpec>,
        warnings: &mut Vec<String>,
    ) -> Result<ViewInventory> {
        let mv_table = self.open_table_by_id(spec.mv_table_id()).await?;
        let mut tables = Vec::new();
        let mut table_index = HashMap::new();

        let mut mv_footprint = self
            .inventory_table_footprint(&mv_table, options, warnings)
            .await;
        mv_footprint.roles.push("mv".to_string());
        table_index.insert(mv_table.table_id.clone(), tables.len());
        tables.push(mv_footprint);

        for state in self.metadata().list_states(view_id).await? {
            let role = state.role.role_name().to_string();
            if let Some(index) = table_index.get(&state.table_id).copied() {
                if !tables[index].roles.contains(&role) {
                    tables[index].roles.push(role);
                }
                continue;
            }
            match self.open_table_by_id(&state.table_id).await {
                Ok(table) => {
                    let mut footprint = self
                        .inventory_table_footprint(&table, options, warnings)
                        .await;
                    footprint.roles.push(role);
                    table_index.insert(table.table_id.clone(), tables.len());
                    tables.push(footprint);
                }
                Err(error) => warnings.push(format!(
                    "view {view_id}: state {role} table {} is missing: {error}",
                    state.table_id
                )),
            }
        }

        let mut sources = Vec::with_capacity(spec.source_table_ids().len());
        for source_id in spec.source_table_ids() {
            let is_view = specs.contains_key(source_id);
            let table_name = if let Some(source_spec) = specs.get(source_id) {
                match self.open_table_by_id(source_spec.mv_table_id()).await {
                    Ok(table) => table.table_name,
                    Err(error) => {
                        warnings.push(format!(
                            "view {view_id}: source view {source_id} is missing: {error}"
                        ));
                        source_id.to_string()
                    }
                }
            } else {
                match self.open_table_by_id(source_id).await {
                    Ok(table) => table.table_name,
                    Err(error) => {
                        warnings.push(format!(
                            "view {view_id}: base source {source_id} is missing: {error}"
                        ));
                        source_id.to_string()
                    }
                }
            };
            sources.push(SourceRef {
                table_id: source_id.to_string(),
                table_name,
                is_view,
            });
        }

        let rows = if options.include_rows {
            tables
                .iter()
                .map(|table| table.rows)
                .try_fold(0u64, |total, rows| rows.map(|rows| total + rows))
        } else {
            None
        };
        let bytes = tables.iter().map(|table| table.bytes).sum();
        let files = tables.iter().map(|table| table.files).sum();
        let (status, last_action, last_epoch, fallback) = match info {
            Some(info) => (
                info.status.clone(),
                info.last_action.clone(),
                info.last_epoch,
                info.fallback,
            ),
            None => ("unknown".to_string(), None, 0, false),
        };

        Ok(ViewInventory {
            view_id: view_id.to_string(),
            table_id: mv_table.table_id.clone(),
            table_name: mv_table.table_name.clone(),
            namespace: mv_table.namespace.clone(),
            kind: spec.kind().to_string(),
            derived: mv_table.table_name.contains("__ivm_step"),
            fallback,
            status,
            last_action,
            last_epoch,
            sources,
            tables,
            rows,
            bytes,
            files,
        })
    }

    async fn inventory_table_footprint(
        &self,
        table: &IvmTable,
        options: &InventoryOptions,
        warnings: &mut Vec<String>,
    ) -> TableFootprint {
        let mut bytes = 0u64;
        let mut files = 0u64;
        let mut unknown_size_files = 0u64;
        let mut incomplete = false;
        let partitions = match self.client().get_all_partition_info(&table.table_id).await
        {
            Ok(partitions) => partitions,
            Err(error) => {
                incomplete = true;
                warnings.push(format!(
                    "table {}: list partitions failed: {error}",
                    table.table_name
                ));
                Vec::new()
            }
        };
        for partition in &partitions {
            match self
                .client()
                .get_data_file_infos_of_single_partition(partition)
                .await
            {
                Ok(infos) => {
                    for file in infos {
                        files += 1;
                        if file.size > 0 {
                            bytes += file.size as u64;
                        } else {
                            unknown_size_files += 1;
                        }
                    }
                }
                Err(error) => {
                    incomplete = true;
                    warnings.push(format!(
                        "table {} partition {}: list files failed: {error}",
                        table.table_name, partition.partition_desc
                    ));
                }
            }
        }
        if unknown_size_files > 0 {
            warnings.push(format!(
                "table {}: {unknown_size_files} file(s) have no recorded size",
                table.table_name
            ));
        }

        let rows = if options.include_rows {
            match self.inventory_row_count(table).await {
                Ok(rows) => Some(rows),
                Err(error) => {
                    warnings.push(format!(
                        "table {}: count(*) failed: {error}",
                        table.table_name
                    ));
                    None
                }
            }
        } else {
            None
        };

        TableFootprint {
            table_id: table.table_id.clone(),
            table_name: table.table_name.clone(),
            namespace: table.namespace.clone(),
            roles: Vec::new(),
            rows,
            bytes,
            files,
            unknown_size_files,
            partitions: partitions.len() as u64,
            incomplete,
            columns: table.schema.fields().len(),
            primary_keys: table.primary_keys.clone(),
            bucket_columns: table.bucket_columns.clone(),
            range_partition_columns: table.range_partition_columns.clone(),
            hash_bucket_num: table.hash_bucket_num.clone(),
            file_format: table.file_format.name().to_string(),
        }
    }

    async fn inventory_row_count(&self, table: &IvmTable) -> Result<u64> {
        let context = SessionContext::new();
        context
            .register_table("inventory_table", Arc::new(self.table_provider(table)))?;
        let batches = context
            .sql("select count(*) as n from inventory_table")
            .await?
            .collect()
            .await?;
        let Some(batch) = batches.first() else {
            return Ok(0);
        };
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                report!(
                    "count(*) over {} did not return an Int64 column",
                    table.table_name
                )
            })?;
        Ok(count.value(0).max(0) as u64)
    }
}

fn inventory_topological_order(
    specs: &HashMap<String, ViewSpec>,
    edges: &HashMap<String, Vec<String>>,
) -> Result<Vec<String>> {
    let mut indegree = specs
        .keys()
        .map(|id| (id.as_str(), 0usize))
        .collect::<HashMap<_, _>>();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for (view, sources) in edges {
        for source in sources {
            if !specs.contains_key(source) {
                continue;
            }
            if let Some(degree) = indegree.get_mut(view.as_str()) {
                *degree += 1;
            }
            dependents
                .entry(source.as_str())
                .or_default()
                .push(view.as_str());
        }
    }
    for deps in dependents.values_mut() {
        deps.sort_unstable();
    }

    let mut ready = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    ready.sort_unstable();
    let mut order = Vec::with_capacity(specs.len());
    let mut next = 0;
    while next < ready.len() {
        let id = ready[next];
        next += 1;
        order.push(id.to_string());
        if let Some(deps) = dependents.get(id) {
            for dep in deps {
                if let Some(degree) = indegree.get_mut(dep) {
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push(dep);
                    }
                }
            }
        }
    }

    if order.len() != specs.len() {
        let mut remaining = indegree
            .iter()
            .filter(|(_, degree)| **degree > 0)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        remaining.sort_unstable();
        return Err(report!(
            "view chain has a cycle among: {}",
            remaining.join(", ")
        ));
    }
    Ok(order)
}

fn inventory_totals(
    views: &[ViewInventory],
    warnings: &mut Vec<String>,
) -> InventoryTotals {
    let mut seen = HashMap::new();
    let mut tables = 0usize;
    let mut bytes = 0u64;
    let mut files = 0u64;
    let mut rows = Some(0u64);
    for view in views {
        for table in &view.tables {
            if let Some(previous) =
                seen.insert(table.table_id.as_str(), view.view_id.as_str())
            {
                warnings.push(format!(
                    "table {} is shared by views {} and {}; counted once",
                    table.table_name, previous, view.view_id
                ));
                continue;
            }
            tables += 1;
            bytes += table.bytes;
            files += table.files;
            rows = match (rows, table.rows) {
                (Some(total), Some(count)) => Some(total + count),
                _ => None,
            };
        }
    }
    InventoryTotals {
        views: views.len(),
        tables,
        rows,
        bytes,
        files,
    }
}

fn inventory_role_totals(views: &[ViewInventory]) -> Vec<RoleTotal> {
    let mut seen = HashSet::new();
    let mut by_role: HashMap<&str, RoleTotal> = HashMap::new();
    for view in views {
        for table in &view.tables {
            if !seen.insert(table.table_id.as_str()) {
                continue;
            }
            let role = table.roles.first().map(String::as_str).unwrap_or("unknown");
            let entry = by_role.entry(role).or_insert_with(|| RoleTotal {
                role: role.to_string(),
                tables: 0,
                rows: Some(0),
                bytes: 0,
                files: 0,
            });
            entry.tables += 1;
            entry.bytes += table.bytes;
            entry.files += table.files;
            entry.rows = match (entry.rows, table.rows) {
                (Some(total), Some(count)) => Some(total + count),
                _ => None,
            };
        }
    }
    let mut roles = by_role.into_values().collect::<Vec<_>>();
    roles.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then_with(|| left.role.cmp(&right.role))
    });
    roles
}

fn format_rows(rows: Option<u64>) -> String {
    match rows {
        Some(rows) => rows.to_string(),
        None => "n/a".to_string(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_are_rendered_in_binary_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
    }

    #[test]
    fn topological_order_puts_sources_first() {
        let specs = HashMap::from([
            (
                "root".to_string(),
                serde_json::from_value(serde_json::json!({
                    "kind": "sink",
                    "view_id": "root",
                    "source_table_id": "step",
                    "mv_table_id": "root",
                    "columns": [],
                    "primary_keys": ["k"],
                    "range_partition_columns": [],
                    "source_definition": "x",
                    "source_generation": 0,
                }))
                .unwrap(),
            ),
            (
                "step".to_string(),
                serde_json::from_value(serde_json::json!({
                    "kind": "sum_count",
                    "view_id": "step",
                    "source_table_id": "base",
                    "mv_table_id": "step",
                    "group_keys": ["g"],
                    "group_exprs": [],
                    "value_column": "v",
                    "count_column": null,
                    "aggregate_filter": null,
                    "filter": null,
                    "having": null,
                    "average": false,
                }))
                .unwrap(),
            ),
        ]);
        let edges = HashMap::from([
            ("root".to_string(), vec!["step".to_string()]),
            ("step".to_string(), Vec::new()),
        ]);
        assert_eq!(
            inventory_topological_order(&specs, &edges).unwrap(),
            vec!["step".to_string(), "root".to_string()]
        );
    }
}
