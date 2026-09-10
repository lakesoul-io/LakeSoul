// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Asynchronously refreshed snapshot of LakeSoul catalog metadata.
//!
//! [`CatalogProvider`](datafusion::catalog::CatalogProvider) and
//! [`SchemaProvider`](datafusion::catalog::SchemaProvider) expose synchronous
//! listing methods (`schema_names`, `table_names`, `table_exist`), while
//! LakeSoul metadata lives behind an async client. Answering those methods by
//! blocking on metadata (the previous `block_on` + `Handle::current()` +
//! `expect` pattern) parked runtime worker threads and panicked when no tokio
//! runtime was current — while PostgreSQL/BI clients query metadata
//! concurrently and often.
//!
//! [`CatalogSnapshot`] therefore keeps an in-memory view of the namespaces and
//! their table names, refreshed by one background task per snapshot:
//!
//! - synchronous catalog lookups only read that view — after the one-off
//!   initial load they never block, never panic, and never issue a metadata
//!   query themselves;
//! - a lookup miss starts one throttled background refresh, so a namespace or
//!   table created out of band becomes visible without a metadata query per
//!   call;
//! - [`CatalogSnapshot::load`] awaits the initial load for callers that want a
//!   warm view deterministically (tests, server start-up);
//! - metadata writes refresh the view inline, so a committed write is visible
//!   to the next listing; a failed refresh is logged instead of failing the
//!   write, which already happened.
//!
//! Because the view is eventually consistent, catalog *writes* never use it as
//! an authoritative existence check: see `LakeSoulCatalog::register_schema`.
//! The async [`SchemaProvider::table`](datafusion::catalog::SchemaProvider::table)
//! lookup reads live metadata, so a stale listing can never hide a table from
//! a query.
//!
//! One snapshot is shared by every session of a [`LakeSoulSessionFactory`]:
//! per-session snapshots would multiply the periodic metadata load by the
//! number of connections, which is the starvation this module exists to avoid.
//!
//! [`LakeSoulSessionFactory`]: crate::session::LakeSoulSessionFactory

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use datafusion::error::DataFusionError;
use parking_lot::{Mutex, RwLock};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::Mutex as AsyncMutex;

use crate::MetaDataClientRef;

/// How often a catalog snapshot refreshes itself by default.
pub const DEFAULT_CATALOG_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Minimum spacing between two miss-triggered refreshes.
const MISS_REFRESH_GAP: Duration = Duration::from_secs(1);

/// Namespaces and table names as of the last successful refresh.
#[derive(Debug, Default, Clone)]
pub struct CatalogView {
    namespaces: Vec<String>,
    tables: HashMap<String, Vec<String>>,
    refreshed_at: Option<Instant>,
}

impl CatalogView {
    /// Namespace names, in metadata order.
    pub fn namespaces(&self) -> &[String] {
        &self.namespaces
    }

    /// Names of the tables of `namespace`, or `None` when the namespace is not
    /// part of this view.
    pub fn tables(&self, namespace: &str) -> Option<&[String]> {
        self.tables.get(namespace).map(Vec::as_slice)
    }

    /// When this view was last fully refreshed, `None` while it has never been
    /// loaded.
    pub fn refreshed_at(&self) -> Option<Instant> {
        self.refreshed_at
    }
}

/// In-memory view of the LakeSoul catalog, refreshed in the background.
pub struct CatalogSnapshot {
    client: MetaDataClientRef,
    interval: Duration,
    view: Arc<RwLock<CatalogView>>,
    /// Serializes fetch-and-publish: a refresh that fetched earlier can never
    /// publish after one that fetched later, so the view never moves backwards.
    refresh_lock: Arc<AsyncMutex<()>>,
    /// A streaming refresh is in flight (cheap dedup in front of the lock).
    refreshing: Arc<AtomicBool>,
    /// Whether the periodic refresher task is running.
    refresher_running: Arc<AtomicBool>,
    /// When a lookup last triggered a refresh, to keep misses from turning
    /// into one metadata query per call.
    last_attempt: Mutex<Option<Instant>>,
}

impl CatalogSnapshot {
    /// Builds a snapshot and starts its periodic refresher when called from a
    /// tokio runtime.
    ///
    /// Outside a runtime there is nothing to refresh on; the refresher is then
    /// started by the first lookup that runs with a runtime, and
    /// [`Self::refresh`]/[`Self::load`] fill the view from any async context.
    pub fn new(client: MetaDataClientRef, interval: Duration) -> Arc<Self> {
        let snapshot = Arc::new(Self {
            client,
            interval,
            view: Arc::new(RwLock::new(CatalogView::default())),
            refresh_lock: Arc::new(AsyncMutex::new(())),
            refreshing: Arc::new(AtomicBool::new(false)),
            refresher_running: Arc::new(AtomicBool::new(false)),
            last_attempt: Mutex::new(None),
        });
        snapshot.ensure_refresher();
        snapshot
    }

    /// The refresh interval of the background task.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// The current view. Point-in-time only: it is replaced wholesale by a
    /// refresh, and updated in place by the `note_*` methods.
    pub fn view(&self) -> CatalogView {
        self.view.read().clone()
    }

    /// Namespace names as of the last refresh.
    pub fn namespaces(&self) -> Vec<String> {
        self.view.read().namespaces.clone()
    }

    /// Table names of `namespace` as of the last refresh; empty when the
    /// namespace is unknown to the view.
    pub fn tables(&self, namespace: &str) -> Vec<String> {
        self.view
            .read()
            .tables(namespace)
            .unwrap_or_default()
            .to_vec()
    }

    /// Whether `namespace` exists in the view.
    pub fn has_namespace(&self, namespace: &str) -> bool {
        self.view.read().tables.contains_key(namespace)
    }

    /// Whether `table` exists in `namespace` in the view (ASCII
    /// case-insensitive, like the metadata lookup it replaces).
    pub fn table_exists(&self, namespace: &str, table: &str) -> bool {
        self.view.read().tables(namespace).is_some_and(|names| {
            names.iter().any(|name| name.eq_ignore_ascii_case(table))
        })
    }

    /// Whether the view has never been fully loaded.
    pub fn never_refreshed(&self) -> bool {
        self.view.read().refreshed_at.is_none()
    }

    /// Reloads the view from LakeSoul metadata: the namespace list plus the
    /// table names of every namespace.
    ///
    /// Refreshes are serialized and the view is replaced only after every
    /// query succeeded, so a failed refresh keeps the last known-good view and
    /// a slow refresh cannot overwrite a newer one.
    pub async fn refresh(&self) -> Result<(), DataFusionError> {
        let _publish = self.refresh_lock.lock().await;
        self.fetch_and_publish().await
    }

    /// Loads the view if it has never been loaded.
    ///
    /// Concurrent callers coalesce into a single metadata scan: the first one
    /// loads while the others wait for the lock and then return immediately.
    pub async fn load(&self) -> Result<(), DataFusionError> {
        if !self.never_refreshed() {
            return Ok(());
        }
        let _publish = self.refresh_lock.lock().await;
        if !self.never_refreshed() {
            return Ok(());
        }
        self.fetch_and_publish().await
    }

    async fn fetch_and_publish(&self) -> Result<(), DataFusionError> {
        let namespaces = self
            .client
            .get_all_namespace()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let mut tables = HashMap::with_capacity(namespaces.len());
        for namespace in &namespaces {
            let names = self
                .client
                .get_all_table_name_id_by_namespace(&namespace.namespace)
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?;
            tables.insert(
                namespace.namespace.clone(),
                names.into_iter().map(|id| id.table_name).collect(),
            );
        }
        *self.view.write() = CatalogView {
            namespaces: namespaces.into_iter().map(|np| np.namespace).collect(),
            tables,
            refreshed_at: Some(Instant::now()),
        };
        Ok(())
    }

    /// Refreshes the view after a committed metadata write, so the write is
    /// visible to the next listing.
    ///
    /// Best effort by design: the write is already committed, so a listing
    /// failure is logged (the periodic refresher retries) rather than reported
    /// to the caller as a failed write. Refreshes are serialized, so this
    /// cannot be overtaken by a background refresh that fetched earlier.
    pub fn refresh_after_write(self: &Arc<Self>) {
        let snapshot = Arc::clone(self);
        if let Err(err) = wait_on_runtime(async move { snapshot.refresh().await }) {
            warn!("catalog snapshot refresh after a metadata write failed: {err}");
        }
    }

    /// Loads the view when it has never been loaded.
    ///
    /// Synchronous lookups call this before answering, so a freshly created
    /// session never resolves against a cold view. The wait is safe
    /// (`wait_on_runtime` yields the worker with `block_in_place`) and bounded
    /// to one load per snapshot — one per factory, not one per session, and
    /// concurrent callers coalesce into that single load. On a current-thread
    /// runtime, or outside a runtime, the load is left to the background
    /// refresher and the caller may see an empty view once.
    pub fn ensure_loaded(self: &Arc<Self>) {
        self.ensure_refresher();
        if !self.never_refreshed() {
            return;
        }
        let snapshot = Arc::clone(self);
        if let Err(err) = wait_on_runtime(async move { snapshot.load().await }) {
            debug!("catalog snapshot initial load deferred: {err}");
        }
    }

    /// Starts the periodic refresher when a runtime is available. Idempotent,
    /// and called again by every lookup, so a refresher that could not start
    /// earlier (no runtime) or that stopped (runtime shutdown) is restarted.
    pub fn ensure_refresher(self: &Arc<Self>) {
        let Ok(handle) = Handle::try_current() else {
            debug!("catalog snapshot has no tokio runtime yet: refresher deferred");
            return;
        };
        if self.refresher_running.swap(true, Ordering::AcqRel) {
            return;
        }
        // Built before spawning and moved into the task: the flag is reset when
        // the task ends, panics, is aborted, or is dropped before its first
        // poll, so a later lookup can always restart the refresher.
        let running = FlagGuard(Arc::clone(&self.refresher_running));
        let weak = Arc::downgrade(self);
        let interval = self.interval;
        handle.spawn(async move {
            let _running = running;
            loop {
                let Some(snapshot) = weak.upgrade() else {
                    // Every snapshot user is gone: stop refreshing.
                    break;
                };
                if let Err(err) = snapshot.refresh().await {
                    warn!("catalog snapshot refresh failed: {err}");
                }
                drop(snapshot);
                tokio::time::sleep(interval).await;
            }
        });
    }

    /// Starts a miss-triggered refresh when the last attempt is older than
    /// [`MISS_REFRESH_GAP`].
    ///
    /// Called by lookups that miss: a namespace created since the last refresh
    /// converges without blocking the caller, and a miss for a namespace that
    /// does not exist cannot turn into a metadata query per call.
    pub fn spawn_refresh_if_stale(self: &Arc<Self>) {
        self.ensure_refresher();
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        {
            let mut last_attempt = self.last_attempt.lock();
            if last_attempt.is_some_and(|at| at.elapsed() < MISS_REFRESH_GAP) {
                return;
            }
            *last_attempt = Some(Instant::now());
        }
        if self.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        // Built before spawning and moved into the task, so the flag is reset
        // even when the task is dropped before its first poll.
        let in_flight = FlagGuard(Arc::clone(&self.refreshing));
        let snapshot = Arc::clone(self);
        handle.spawn(async move {
            let _in_flight = in_flight;
            if let Err(err) = snapshot.refresh().await {
                warn!("catalog snapshot refresh failed: {err}");
            }
        });
    }
}

/// Clears a flag when the task that owns it ends, panics, is aborted, or is
/// dropped before its first poll.
struct FlagGuard(Arc<AtomicBool>);

impl Drop for FlagGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Waits for `future` on the current tokio runtime from a synchronous catalog
/// method.
///
/// Only metadata *writes* use this: unlike the read path they must not act on
/// a stale view, and they are rare enough that waiting is acceptable. Reads
/// never call it. Returns a configuration error when no runtime is current
/// (the previous `Handle::current()` implementation panicked there) and on a
/// current-thread runtime, where waiting for a spawned task would deadlock
/// (the previous `block_in_place` implementation aborted there).
pub(crate) fn wait_on_runtime<T, F>(future: F) -> Result<T, DataFusionError>
where
    F: Future<Output = Result<T, DataFusionError>> + Send + 'static,
    T: Send + 'static,
{
    let handle = Handle::try_current().map_err(|_| {
        DataFusionError::Configuration(
            "LakeSoul metadata writes require a tokio runtime".to_string(),
        )
    })?;
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        return Err(DataFusionError::Configuration(
            "LakeSoul metadata writes require a multi-thread tokio runtime".to_string(),
        ));
    }
    // `block_in_place` keeps this from blocking the runtime's workers: the
    // runtime can poll the spawned metadata task on another worker.
    tokio::task::block_in_place(|| futures::executor::block_on(handle.spawn(future)))
        .map_err(|err| {
            DataFusionError::Internal(format!("metadata task failed: {err}"))
        })?
}
