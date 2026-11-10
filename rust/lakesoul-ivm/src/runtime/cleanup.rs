// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Explicit removal of a view's IVM metadata and internal state.
//!
//! The target materialized view is a normal LakeSoul table: it may be read
//! and written by other engines, and it is not owned by the IVM runtime.  A
//! cleanup therefore never drops the target.  It removes the view definition,
//! cursors, epochs, consumer watermarks and every internal state table, so the
//! target can keep living as a plain table or be maintained by IVM again
//! later.

use std::collections::HashSet;

use super::*;

/// What a [`IvmRuntime::drop_view`] call removes.
#[derive(Debug, Clone)]
pub struct DropViewOptions {
    /// Drop the internal state tables (the default).  The target MV itself is
    /// never dropped, even when this is true.
    pub drop_states: bool,
    /// Drop the internal derived step tables (`<target>__ivm_step*`).
    pub drop_derived: bool,
    /// Also drop the IVM metadata of views that read this view's target.
    /// Their targets are still left in place.
    pub cascade: bool,
    /// Only report what would be removed; change nothing.
    pub dry_run: bool,
}

impl Default for DropViewOptions {
    fn default() -> Self {
        Self {
            drop_states: true,
            drop_derived: true,
            cascade: false,
            dry_run: false,
        }
    }
}

/// The result of dropping one view's IVM artifacts.
#[derive(Debug, Clone, Default)]
pub struct DropViewReport {
    /// The view id.
    pub view_id: String,
    /// The target table id.
    pub target_table_id: String,
    /// The target table name, when it could still be opened.
    pub target_table_name: Option<String>,
    /// Internal state tables removed (or that would be removed).
    pub states: Vec<String>,
    /// Internal derived step tables removed (or that would be removed).
    pub derived_views: Vec<String>,
    /// Views that read this view's target, when `cascade` was false.
    pub dependents: Vec<String>,
    /// Whether the metadata was actually removed.
    pub dropped: bool,
}

impl DropViewReport {
    fn missing(view_id: &str) -> Self {
        Self {
            view_id: view_id.to_string(),
            ..Self::default()
        }
    }
}

impl IvmRuntime {
    /// Drop one IVM view by id, leaving its target table untouched.
    ///
    /// The per-view lease is taken for the duration.  When other views read
    /// this view's target, the call fails unless `options.cascade` is true;
    /// cascading removes the dependent view definitions and state too, but
    /// never their target tables.
    pub async fn drop_view(
        &self,
        view_id: &str,
        options: &DropViewOptions,
    ) -> Result<DropViewReport> {
        let mut visited = HashSet::new();
        self.drop_view_inner(view_id, options, true, &mut visited)
            .await
    }

    /// Drop every IVM view whose target is `table_name` in `namespace`.
    ///
    /// The caller names the target table, not the internal view id.  The
    /// target is resolved to its table id and every registered spec that
    /// materializes into it is removed; the table itself is never dropped.
    pub async fn drop_views_for_target(
        &self,
        namespace: &str,
        table_name: &str,
        options: &DropViewOptions,
    ) -> Result<Vec<DropViewReport>> {
        let target = self.open_table(table_name, namespace).await?;
        let view_ids = self.view_ids_for_target(&target.table_id).await?;
        let mut visited = HashSet::new();
        let mut reports = Vec::with_capacity(view_ids.len());
        for view_id in view_ids {
            reports.push(
                self.drop_view_inner(&view_id, options, true, &mut visited)
                    .await?,
            );
        }
        Ok(reports)
    }

    /// Every registered view that materializes into `target_table_id`.
    ///
    /// SQL views use the target table id as their view id; the Rust API lets a
    /// caller choose a different view id, so the persisted spec is the source
    /// of truth.
    async fn view_ids_for_target(&self, target_table_id: &str) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        for info in self.metadata().list_views().await? {
            if info.view_id == target_table_id {
                ids.push(info.view_id);
                continue;
            }
            let Some(value) = self.metadata().get_view_spec(&info.view_id).await? else {
                continue;
            };
            // Read the target id from the raw spec: a metadata row written by
            // an older revision may not deserialize into today's `ViewSpec`,
            // but it can still be matched and cleaned by its target.
            let target = value
                .get("mv_table_id")
                .or_else(|| value.get("output_table_id"))
                .and_then(serde_json::Value::as_str);
            if target == Some(target_table_id) {
                ids.push(info.view_id);
            }
        }
        Ok(ids)
    }

    async fn drop_view_inner(
        &self,
        view_id: &str,
        options: &DropViewOptions,
        keep_target: bool,
        visited: &mut HashSet<String>,
    ) -> Result<DropViewReport> {
        if !visited.insert(view_id.to_string()) {
            return Ok(DropViewReport::missing(view_id));
        }
        let Some(value) = self.metadata().get_view_spec(view_id).await? else {
            return Ok(DropViewReport::missing(view_id));
        };
        let spec: ViewSpec = serde_json::from_value(value)?;
        let target_table_id = spec.mv_table_id().to_string();
        let target = self.open_table_by_id(&target_table_id).await.ok();
        let mut report = DropViewReport {
            view_id: view_id.to_string(),
            target_table_id: target_table_id.clone(),
            target_table_name: target.as_ref().map(|table| table.table_name.clone()),
            ..DropViewReport::default()
        };

        let dependents = self
            .dependent_views(&target_table_id, view_id, target.as_ref())
            .await?;
        if !dependents.is_empty() && !options.cascade {
            report.dependents = dependents;
            return Err(report!(
                "view {view_id} is read by other IVM views: {}; \
                 pass cascade to drop their definitions too (their targets are kept)",
                report.dependents.join(", ")
            ));
        }
        if options.cascade {
            for dependent in dependents {
                Box::pin(self.drop_view_inner(&dependent, options, true, visited))
                    .await?;
            }
        }

        let owner = uuid::Uuid::new_v4().to_string();
        let ttl_ms = crate::config::options().refresh_lock_ttl_ms.max(1000) as i64;
        let locked = !options.dry_run;
        if locked
            && !self
                .metadata()
                .try_lock_view(view_id, &owner, ttl_ms)
                .await?
        {
            return Err(report!(
                "view {view_id} is being refreshed by another statement"
            ));
        }
        let result = self
            .drop_view_artifacts(
                view_id,
                &target_table_id,
                target.as_ref(),
                keep_target,
                options,
                &mut report,
            )
            .await;
        if locked {
            self.metadata().unlock_view(view_id, &owner).await?;
        }
        let report = result?;
        Ok(report)
    }

    async fn drop_view_artifacts(
        &self,
        view_id: &str,
        target_table_id: &str,
        target: Option<&IvmTable>,
        keep_target: bool,
        options: &DropViewOptions,
        report: &mut DropViewReport,
    ) -> Result<DropViewReport> {
        self.drop_states_for_view(view_id, target_table_id, keep_target, options, report)
            .await?;

        if options.drop_derived
            && let Some(target) = target
        {
            for index in 0..=crate::config::options().max_derived_steps.max(1) {
                let name = format!("{}__ivm_step{index}", target.table_name);
                let Ok(derived) = self.open_table(&name, &target.namespace).await else {
                    continue;
                };
                report.derived_views.push(derived.table_name.clone());
                if options.dry_run {
                    continue;
                }
                // The derived MV is an internal table, so unlike the user
                // target it is removed together with its state.
                let mut derived_report = DropViewReport {
                    view_id: derived.table_id.clone(),
                    target_table_id: derived.table_id.clone(),
                    target_table_name: Some(derived.table_name.clone()),
                    ..DropViewReport::default()
                };
                self.drop_states_for_view(
                    &derived.table_id,
                    &derived.table_id,
                    false,
                    options,
                    &mut derived_report,
                )
                .await?;
                self.metadata().delete_view(&derived.table_id).await?;
                let _ = self
                    .client()
                    .drop_table(&derived.table_name, &derived.namespace)
                    .await;
            }
        }

        if !options.dry_run {
            self.metadata().delete_view(view_id).await?;
            report.dropped = true;
        }
        Ok(std::mem::take(report))
    }

    async fn drop_states_for_view(
        &self,
        view_id: &str,
        target_table_id: &str,
        keep_target: bool,
        options: &DropViewOptions,
        report: &mut DropViewReport,
    ) -> Result<()> {
        if !options.drop_states {
            return Ok(());
        }
        for state in self.metadata().list_states(view_id).await? {
            if keep_target
                && state.role == StateRole::Mv
                && state.table_id == target_table_id
            {
                // The user-visible target is never an IVM artifact.
                continue;
            }
            report.states.push(state.table_name.clone());
            if !options.dry_run {
                self.metadata()
                    .unregister_state(view_id, state.role)
                    .await?;
                let _ = self
                    .client()
                    .drop_table(&state.table_name, &state.namespace)
                    .await;
            }
        }
        Ok(())
    }

    /// Other registered views whose sources include `target_table_id`.
    ///
    /// Internal derived steps are excluded: they belong to the view being
    /// dropped and are handled by `drop_derived`.
    async fn dependent_views(
        &self,
        target_table_id: &str,
        owner_view_id: &str,
        owner_target: Option<&IvmTable>,
    ) -> Result<Vec<String>> {
        let owner_is_derived =
            owner_target.is_some_and(|table| table.table_name.contains("__ivm_step"));
        let mut dependents = Vec::new();
        for info in self.metadata().list_views().await? {
            if info.view_id == owner_view_id {
                continue;
            }
            let Some(value) = self.metadata().get_view_spec(&info.view_id).await? else {
                continue;
            };
            let Ok(spec) = serde_json::from_value::<ViewSpec>(value) else {
                // A stale/incompatible definition cannot participate in the
                // dependency graph; it is still removable by its own id.
                continue;
            };
            if !spec.source_table_ids().contains(&target_table_id) {
                continue;
            }
            let target_is_derived = match self.open_table_by_id(spec.mv_table_id()).await
            {
                Ok(table) => table.table_name.contains("__ivm_step"),
                Err(_) => false,
            };
            if target_is_derived && !owner_is_derived {
                // The derived step of this same view: not a downstream user
                // view, and `drop_derived` decides its fate.
                continue;
            }
            dependents.push(info.view_id);
        }
        Ok(dependents)
    }
}
