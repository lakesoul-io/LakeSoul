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

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::acl::{AclView, TableAccess};
use parking_lot::{Mutex, RwLock};
use pgwire::api::ClientInfo;
use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tokio::sync::Mutex as AsyncMutex;

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
            refreshed_at: Mutex::new(None),
            refresh_lock: AsyncMutex::new(()),
        }
    }

    pub(crate) fn view(&self) -> Arc<AclView> {
        Arc::clone(&self.view.read())
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
}

pub(crate) fn pg_acl(session: &SessionContext) -> Option<Arc<PgAclSession>> {
    session.state().config().get_extension::<PgAclSession>()
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
pub(crate) async fn require_table_write(
    session: &SessionContext,
    namespace: &str,
    table: &str,
) -> PgWireResult<()> {
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

#[async_trait]
impl QueryHook for AclRefreshHook {
    async fn handle_simple_query(
        &self,
        _statement: &datafusion::sql::sqlparser::ast::Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match refresh_for(client).await {
            Ok(()) => None,
            Err(error) => Some(Err(error)),
        }
    }

    async fn handle_extended_parse_query(
        &self,
        _statement: &datafusion::sql::sqlparser::ast::Statement,
        _session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<datafusion::logical_expr::LogicalPlan>> {
        match refresh_for(client).await {
            Ok(()) => None,
            Err(error) => Some(Err(error)),
        }
    }

    async fn handle_extended_query(
        &self,
        _statement: &datafusion::sql::sqlparser::ast::Statement,
        _logical_plan: &datafusion::logical_expr::LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match refresh_for(client).await {
            Ok(()) => None,
            Err(error) => Some(Err(error)),
        }
    }
}
