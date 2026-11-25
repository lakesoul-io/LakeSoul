// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-connection view of table/namespace ACLs.
//!
//! The metadata database is authoritative; the pg-wire server keeps one
//! refreshable [`AclEntry`] per `(user, domain)` and consults it for catalog
//! visibility and write checks. Direct metadata connections (Spark/Flink/
//! Python) are enforced by the SQL RLS functions in `meta_rbac_init.sql`, not
//! by this module.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::prelude::SessionContext;
use datafusion_postgres::QueryHook;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::acl::{AclView, TableAccess};
use parking_lot::{Mutex, RwLock};
use pgwire::api::ClientInfo;
use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tokio::sync::Mutex as AsyncMutex;

use crate::ddl::user_error;
use crate::server::ConnectionSession;

pub(crate) const UNDEFINED_TABLE: &str = "42P01";
pub(crate) const INSUFFICIENT_PRIVILEGE: &str = "42501";

/// How often an idle session refreshes its ACL view when it runs a statement.
const ACL_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// One user's ACL view, shared by every connection of that user/domain pair.
pub(crate) struct AclEntry {
    client: MetaDataClientRef,
    user: String,
    domains: Vec<String>,
    interval: Duration,
    view: RwLock<Arc<AclView>>,
    /// Fingerprint of [`Self::view`], used to tell the connection whether a
    /// refresh changed visibility and prepared plans must be replanned.
    fingerprint: Mutex<Option<u64>>,
    /// Monotonic version of the published view. Unlike the local `changed`
    /// flag of a refresh, this is visible to every connection sharing the
    /// entry, so a prepared plan can detect a revoke performed by a peer.
    version: AtomicU64,
    refreshed_at: Mutex<Option<Instant>>,
    refresh_lock: AsyncMutex<()>,
}

impl std::fmt::Debug for AclEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AclEntry")
            .field("user", &self.user)
            .field("domains", &self.domains)
            .finish_non_exhaustive()
    }
}

impl AclEntry {
    fn new(client: MetaDataClientRef, user: String, domains: Vec<String>) -> Self {
        Self {
            client,
            user,
            domains,
            interval: ACL_REFRESH_INTERVAL,
            view: RwLock::new(Arc::new(AclView::default())),
            fingerprint: Mutex::new(None),
            version: AtomicU64::new(0),
            refreshed_at: Mutex::new(None),
            refresh_lock: AsyncMutex::new(()),
        }
    }

    pub(crate) fn view(&self) -> Arc<AclView> {
        Arc::clone(&self.view.read())
    }

    /// Version of the currently published view. Prepared statements record
    /// this at plan time and compare it again at execute time.
    pub(crate) fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    pub(crate) async fn live_table_access(
        &self,
        namespace: &str,
        table: &str,
    ) -> lakesoul_metadata::Result<Option<TableAccess>> {
        AclView::load_table(&self.client, &self.user, &self.domains, namespace, table)
            .await
    }

    fn is_fresh(&self) -> bool {
        self.refreshed_at
            .lock()
            .is_some_and(|at| at.elapsed() < self.interval)
    }

    /// Refresh when the view is older than [`ACL_REFRESH_INTERVAL`]. Returns
    /// whether the visible privileges changed.
    pub(crate) async fn refresh_if_stale(&self) -> lakesoul_metadata::Result<bool> {
        if self.is_fresh() {
            return Ok(false);
        }
        self.refresh().await
    }

    /// Reload the view. Returns whether the visible privileges changed.
    pub(crate) async fn refresh(&self) -> lakesoul_metadata::Result<bool> {
        let _guard = self.refresh_lock.lock().await;
        let view = AclView::load(&self.client, &self.user, &self.domains).await?;
        let fingerprint = view.fingerprint();
        let changed = {
            let mut stored = self.fingerprint.lock();
            let changed = *stored != Some(fingerprint);
            // Publish the view before the new fingerprint: a concurrent reader
            // must never pair a new fingerprint with the old visibility.
            *self.view.write() = Arc::new(view);
            *stored = Some(fingerprint);
            changed
        };
        if changed {
            self.version.fetch_add(1, Ordering::SeqCst);
        }
        *self.refreshed_at.lock() = Some(Instant::now());
        Ok(changed)
    }
}

/// Shared cache keyed by user and domain set.
#[derive(Debug)]
pub(crate) struct AclCache {
    client: MetaDataClientRef,
    entries: Mutex<HashMap<String, Arc<AclEntry>>>,
}

impl AclCache {
    pub(crate) fn new(client: MetaDataClientRef) -> Self {
        Self {
            client,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn get_or_load(
        &self,
        user: &str,
        domains: &[String],
    ) -> lakesoul_metadata::Result<Arc<AclEntry>> {
        let key = format!("{user}\u{1f}{}", domains.join("\u{1f}"));
        let existing = self.entries.lock().get(&key).cloned();
        if let Some(entry) = existing {
            // A new connection must observe grants made since the cached entry
            // was built; existing sessions still use the short TTL refresh.
            let _ = entry.refresh().await?;
            return Ok(entry);
        }
        let entry = Arc::new(AclEntry::new(
            Arc::clone(&self.client),
            user.to_string(),
            domains.to_vec(),
        ));
        let _ = entry.refresh().await?;
        self.entries.lock().insert(key, Arc::clone(&entry));
        Ok(entry)
    }
}

/// The ACL extension installed on every pg-wire session.
#[derive(Debug, Clone)]
pub(crate) struct PgAclSession {
    pub acl: Arc<AclEntry>,
    /// The domain new tables are created in and the only domain a client may
    /// write to. `None` keeps the pre-cluster-domain behaviour (the token's
    /// domain).
    pub cluster_domain: Option<String>,
    /// Pending DDL of the connection's transaction block, for tables that are
    /// not visible to the published ACL view yet.
    pub transaction: crate::transaction::SharedTransaction,
}

pub(crate) fn pg_acl(session: &SessionContext) -> Option<Arc<PgAclSession>> {
    session.state().config().get_extension::<PgAclSession>()
}

/// Require write access to a federation foreign table. Foreign tables are not
/// in `table_info`, so their domain/creator live in the federation metadata and
/// use the same default rule as LakeSoul tables.
pub(crate) async fn require_foreign_table_write(
    session: &SessionContext,
    database: &str,
    table: &str,
) -> PgWireResult<()> {
    let Some(federation) = session
        .state()
        .config()
        .get_extension::<crate::federation::PgFederationSession>()
    else {
        // Non-RBAC sessions keep the legacy federation behaviour.
        return Ok(());
    };
    let Some(acl) = pg_acl(session) else {
        return Ok(());
    };
    match federation.runtime.foreign_table_privilege(
        database,
        table,
        &federation.user,
        federation.domain.as_deref(),
        Some(&acl.acl.view()),
    ) {
        lakesoul_metadata::acl::Privilege::Write => Ok(()),
        lakesoul_metadata::acl::Privilege::Read => Err(user_error(
            INSUFFICIENT_PRIVILEGE,
            format!("permission denied for foreign table {table}"),
        )),
        lakesoul_metadata::acl::Privilege::None => Err(user_error(
            UNDEFINED_TABLE,
            format!("relation \"{table}\" does not exist"),
        )),
    }
}

fn insufficient(table: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        INSUFFICIENT_PRIVILEGE.to_string(),
        format!("permission denied for table {table}"),
    )))
}

fn invisible(table: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        UNDEFINED_TABLE.to_string(),
        format!("relation \"{table}\" does not exist"),
    )))
}

fn write_domain_allows(acl: &PgAclSession, access: &TableAccess) -> bool {
    if access.is_public() {
        return true;
    }
    match acl.cluster_domain.as_deref() {
        Some(cluster) => access.domain == cluster,
        None => true,
    }
}

/// Require write (DML) access to a table. Missing tables stay `42P01`;
/// visible read-only tables answer `42501`.
/// Whether the open block created `table` and the connection may use it as its
/// own before the metadata commit publishes it.
fn pending_table_visible(session: &SessionContext, table: &str) -> bool {
    let Some(acl) = pg_acl(session) else {
        return false;
    };
    let block = acl.transaction.lock();
    block
        .as_ref()
        .is_some_and(|state| state.pending_acl.can_access(table))
}

pub(crate) async fn require_table_write(
    session: &SessionContext,
    namespace: &str,
    table: &str,
) -> PgWireResult<()> {
    if pending_table_visible(session, table) {
        return Ok(());
    }
    let Some(acl) = pg_acl(session) else {
        return Ok(());
    };
    let view = acl.acl.view();
    let access = match view.effective_table_access(namespace, table) {
        Some(access) if access.privilege.can_read() => access,
        _ => {
            // A table created by another engine (or a public table created
            // after the last refresh) may not be in the cached view yet.
            let Some(mut live) = acl
                .acl
                .live_table_access(namespace, table)
                .await
                .map_err(refresh_error)?
            else {
                return Err(invisible(table));
            };
            live.privilege = view.namespace_privilege(namespace).min(live.privilege);
            live
        }
    };
    if !access.privilege.can_read() {
        return Err(invisible(table));
    }
    if access.privilege.can_write() && write_domain_allows(&acl, &access) {
        Ok(())
    } else {
        Err(insufficient(table))
    }
}

/// Require that the user may create a table in `namespace`.
pub(crate) fn require_namespace_write(
    session: &SessionContext,
    namespace: &str,
) -> PgWireResult<()> {
    let Some(acl) = pg_acl(session) else {
        return Ok(());
    };
    let view = acl.acl.view();
    let Some(access) = view.namespace_access(namespace) else {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "3D000".to_string(),
            format!("database \"{namespace}\" does not exist"),
        ))));
    };
    let same_write_domain = access.is_public()
        || acl
            .cluster_domain
            .as_deref()
            .is_none_or(|cluster| access.domain == cluster);
    if !access.privilege.can_read() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "3D000".to_string(),
            format!("database \"{namespace}\" does not exist"),
        ))));
    }
    if access.privilege.can_write() && same_write_domain {
        Ok(())
    } else {
        Err(insufficient(namespace))
    }
}

/// Require ownership/admin rights for DDL.
pub(crate) fn require_table_ddl(
    session: &SessionContext,
    namespace: &str,
    table: &str,
) -> PgWireResult<()> {
    if pending_table_visible(session, table) {
        return Ok(());
    }
    let Some(acl) = pg_acl(session) else {
        return Ok(());
    };
    let view = acl.acl.view();
    let Some(access) = view.effective_table_access(namespace, table) else {
        return Err(invisible(table));
    };
    if !access.privilege.can_read() {
        return Err(invisible(table));
    }
    if access.can_ddl() && write_domain_allows(&acl, &access) {
        Ok(())
    } else {
        Err(insufficient(table))
    }
}

pub(crate) fn refresh_error(
    error: lakesoul_metadata::LakeSoulMetaDataError,
) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        format!("refreshing table ACLs failed: {error}"),
    )))
}

async fn refresh_for(client: &(impl ClientInfo + ?Sized)) -> PgWireResult<()> {
    let Some(session) = client.session_extensions().get::<ConnectionSession>() else {
        return Ok(());
    };
    let Some(acl) = session.session.acl() else {
        return Ok(());
    };
    if acl.refresh_if_stale().await.map_err(refresh_error)? {
        session
            .session
            .replace_visible_databases(acl.view().visible_namespaces());
        // A plan prepared under the old visibility must not keep reading a
        // table after a revoke (or keep missing one after a grant).
        session.settings.bump();
    }
    Ok(())
}

/// Refreshes the connection ACL view before every statement.
pub(crate) struct AclRefreshHook;

impl QueryHook for AclRefreshHook {
    fn handle_simple_query<'a>(
        &'a self,
        _statement: &'a datafusion::sql::sqlparser::ast::Statement,
        _session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            match refresh_for(client).await {
                Ok(()) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }

    fn handle_extended_parse_query<'a>(
        &'a self,
        _statement: &'a datafusion::sql::sqlparser::ast::Statement,
        _session_context: &'a SessionContext,
        client: &'a (dyn ClientInfo + Send + Sync),
    ) -> futures::future::BoxFuture<
        'a,
        Option<PgWireResult<datafusion::logical_expr::LogicalPlan>>,
    > {
        Box::pin(async move {
            match refresh_for(client).await {
                Ok(()) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }

    fn handle_extended_query<'a>(
        &'a self,
        _statement: &'a datafusion::sql::sqlparser::ast::Statement,
        _logical_plan: &'a datafusion::logical_expr::LogicalPlan,
        _params: &'a datafusion::common::ParamValues,
        _session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            match refresh_for(client).await {
                Ok(()) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }
}
