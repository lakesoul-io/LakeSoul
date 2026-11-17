// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::prelude::SessionContext;
use lakesoul_datafusion::distributed::DistributedOptions;
use parking_lot::RwLock;
use rootcause::{bail, report};

use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::datafusion_pg_catalog::{
    PgCatalogOptions, setup_pg_catalog_with_options,
};
use lakesoul_common::misc::{SecretMap, TimeZone};
use lakesoul_datafusion::catalog::{
    CatalogSnapshot, LakeSoulProviderOptions, LakeSoulVectorFieldMetadata,
};
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::distributed::headers::{
    SessionActor, StatementIdGuard, set_identity_headers,
};
use lakesoul_datafusion::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchConfig;
use lakesoul_metadata::MetaDataClientRef;
use rootcause::Report;
use tracing::{info, instrument};

use crate::acl::{AclCache, AclEntry};
use crate::catalog::{
    PUBLIC_SCHEMA, PgDatabaseCatalogList, pg_lakesoul_catalog_with_federation,
};
use crate::pg_settings::PgParameters;

/// The `pg_catalog` role provider for this connection.
///
/// The pinned catalog's blanket `impl PgCatalogContextProvider for Arc<T>`
/// calls `self.roles()` inside the `Arc` implementation, which recurses until
/// the stack overflows (`SELECT * FROM pg_roles` aborts the process). This
/// newtype delegates to `AuthManager` directly and keeps that blanket impl
/// out of the path.
#[derive(Clone, Debug)]
struct AuthRoles(Arc<AuthManager>);

#[async_trait::async_trait]
impl datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::PgCatalogContextProvider
    for AuthRoles
{
    async fn roles(&self) -> Vec<String> {
        self.0.list_roles().await
    }

    async fn role(
        &self,
        name: &str,
    ) -> Option<datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::Role>
    {
        self.0.get_role(name).await
    }
}

/// Identity resolved for one PostgreSQL connection during startup.
#[derive(Debug, Clone, Default)]
pub struct SessionIdentity {
    pub user: String,
    pub database: String,
    /// The domain the connection is confined to; `None` without `--rbac`.
    pub access: Option<crate::auth::DomainAccess>,
}

datafusion::common::extensions_options! {
    /// The PostgreSQL identity a session's table functions use for ownership
    /// checks. It lives in the DataFusion session config because a table
    /// function only receives a `&dyn Session`, not the pgwire client.
    pub struct PgIdentityConfig {
        pub user: String, default = String::new()
        pub database: String, default = String::new()
        /// The domain under `--rbac`; part of a job's owner.
        pub domain: Option<String>, default = None
    }
}

impl datafusion::common::config::ConfigExtension for PgIdentityConfig {
    const PREFIX: &'static str = "lakesoul_pg_identity";
}

/// PostgreSQL connection-local settings.
#[derive(Debug, Clone)]
pub struct SessionSettings {
    /// PostgreSQL search path, most significant schema first.
    pub search_path: Vec<String>,
    /// Session time zone applied to DataFusion execution.
    pub time_zone: TimeZone,
    pub statement_timeout: Option<Duration>,
}

impl Default for SessionSettings {
    fn default() -> Self {
        // default is utc
        let tz = TimeZone::try_system().unwrap_or(TimeZone::UTC);
        Self {
            search_path: vec![PUBLIC_SCHEMA.to_string(), "pg_catalog".to_string()],
            time_zone: tz,
            statement_timeout: None,
        }
    }
}

impl SessionSettings {
    /// Initial DataFusion schema derived from the PostgreSQL search path.
    /// Full multi-schema resolution is handled by the PG query layer.
    pub fn initial_schema(&self) -> &str {
        self.search_path
            .iter()
            .map(String::as_str)
            .find(|schema| *schema != "pg_catalog")
            .unwrap_or(PUBLIC_SCHEMA)
    }
}

/// State owned by one PostgreSQL connection.
pub struct PgSession {
    pub identity: SessionIdentity,
    pub context: Arc<SessionContext>,
    pub settings: RwLock<SessionSettings>,
    /// The PostgreSQL session parameters `SHOW`, `current_setting` and
    /// `pg_settings` report.
    pub parameters: Arc<PgParameters>,
    pub actor: SessionActor,
    pub ivm: Option<Arc<crate::ivm::IvmService>>,
    distributed: bool,
    /// Metadata view this connection lists from: the factory's shared
    /// snapshot, i.e. one refresher per process rather than one per
    /// connection.
    catalog_snapshot: Arc<CatalogSnapshot>,
    /// The connection's ACL view; `None` when `--rbac` is off.
    acl: Option<Arc<AclEntry>>,
    /// The connection-level database list, updated when the ACL view refreshes.
    catalog_list: Arc<PgDatabaseCatalogList>,
    /// The domain new tables are created in under a shared cluster.
    cluster_domain: Option<String>,
    /// Pending DDL of the connection's open transaction block, shared with
    /// the catalog overlay and the write sink.
    pub(crate) transaction: crate::transaction::SharedTransaction,
}

impl PgSession {
    pub fn new(
        identity: SessionIdentity,
        context: Arc<SessionContext>,
        settings: SessionSettings,
        parameters: Arc<PgParameters>,
        catalog_snapshot: Arc<CatalogSnapshot>,
        acl: Option<Arc<AclEntry>>,
        catalog_list: Arc<PgDatabaseCatalogList>,
        cluster_domain: Option<String>,
        distributed: bool,
        transaction: crate::transaction::SharedTransaction,
    ) -> Result<Self> {
        let actor =
            SessionActor::new(identity.user.clone(), "", identity.database.clone());
        if distributed {
            set_identity_headers(&context, &actor).map_err(|error| {
                report!("invalid connection identity for distributed headers: {error}")
            })?;
        }
        Ok(Self {
            identity,
            context,
            settings: RwLock::new(settings),
            parameters,
            actor,
            ivm: None,
            distributed,
            catalog_snapshot,
            acl,
            catalog_list,
            cluster_domain,
            transaction,
        })
    }

    pub(crate) fn acl(&self) -> Option<&Arc<AclEntry>> {
        self.acl.as_ref()
    }

    pub(crate) fn cluster_domain(&self) -> Option<&str> {
        self.cluster_domain.as_deref()
    }

    /// Replace the databases visible in `pg_database` after an ACL refresh.
    pub(crate) fn replace_visible_databases(
        &self,
        namespaces: std::collections::BTreeSet<String>,
    ) {
        self.catalog_list.replace_visible_databases(namespaces);
    }

    /// Refresh the ACL view immediately after a DDL statement changed the
    /// table set, so the same connection can write the new table.
    ///
    /// Returns whether the visible privileges changed.
    pub(crate) async fn refresh_acl_now(&self) -> Result<bool> {
        let Some(acl) = &self.acl else {
            return Ok(false);
        };
        let changed = acl.refresh().await?;
        self.replace_visible_databases(acl.view().visible_namespaces());
        Ok(changed)
    }

    /// Refresh the ACL view when its TTL expired. Returns whether the visible
    /// privileges changed.
    pub(crate) async fn refresh_acl_if_stale(&self) -> Result<bool> {
        let Some(acl) = &self.acl else {
            return Ok(false);
        };
        let changed = acl.refresh_if_stale().await?;
        // A peer connection may have refreshed the shared entry already. Keep
        // this connection's catalog snapshot in step with the published view
        // even when the local `changed` flag is false.
        self.replace_visible_databases(acl.view().visible_namespaces());
        Ok(changed)
    }

    /// Refresh the views a completed table DDL publication changed.
    ///
    /// Runs after `COMMIT` (including a publication detached from a cancelled
    /// statement) and after `ROLLBACK`, so a pending definition never leaks
    /// into a later statement.
    pub(crate) async fn refresh_after_ddl(
        &self,
        ddl: &crate::ddl_transaction::DdlChanges,
    ) {
        for name in ddl
            .creates
            .keys()
            .chain(ddl.alters.keys())
            .chain(ddl.drops.keys())
        {
            lakesoul_datafusion::catalog::invalidate_table(&self.identity.database, name);
        }
        self.catalog_snapshot.refresh_after_write();
        if let Err(error) = self.refresh_acl_now().await {
            tracing::warn!(%error, "refreshing ACLs after table DDL failed");
        }
    }

    /// The metadata view backing this connection's catalog listings.
    pub fn catalog_snapshot(&self) -> &Arc<CatalogSnapshot> {
        &self.catalog_snapshot
    }
    pub fn label_statement(
        &self,
        query_id: u64,
        span: &tracing::Span,
    ) -> Option<StatementIdGuard> {
        self.distributed.then(|| {
            // Entering the statement span makes its OTel context current while
            // the distributed headers are built, so `StatementIdGuard` can
            // forward `traceparent` to the workers.
            let _entered = span.enter();
            StatementIdGuard::set(&self.context, &self.actor, query_id)
        })
    }
}

type Result<T, E = Report> = std::result::Result<T, E>;

/// PostgreSQL-specific adapter around the generic LakeSoul session factory.
///
/// It maps connection-local PG settings to DataFusion session options and
/// installs the PostgreSQL database semantics:
///
/// - the PG database parameter names the LakeSoul namespace the connection
///   attaches to; the session's default catalog and schema are that
///   database and its `public` schema;
/// - a connection-level [`PgDatabaseCatalogList`] lists every visible
///   namespace as a database (empty marker catalogs outside the current
///   one), so `pg_catalog.pg_database` reports them without exposing
///   cross-database objects;
/// - `pg_catalog` plus the PostgreSQL compatibility functions are installed
///   only into the current database's catalog.
pub struct PgSessionFactory {
    meta_client: MetaDataClientRef,
    base: LakeSoulSessionFactory,
    auth_manager: Arc<AuthManager>,
    catalog_options: PgCatalogOptions,
    distributed: bool,
    ivm: Option<Arc<crate::ivm::IvmService>>,
    /// Value of `lakesoul.ivm.enabled` for new connections. The server keeps
    /// the upstream default (off) unless the deployment asks for IVM by
    /// default.
    ivm_default: bool,
    /// Object-store prefix new tables are created under; `CREATE TABLE`
    /// refuses to run without one.
    warehouse: Option<String>,
    /// `fs.s3a.*` options from the server's CLI/environment.  Index DDL and
    /// the background runner build stores outside a connection's
    /// `SessionContext`, so they need these explicitly.
    object_store_options: SecretMap,
    /// Supervises SQL-created subscription processes. `None` in tests and in
    /// embeddings that do not enable subscriptions.
    subscriptions: Option<Arc<crate::subscribe::supervisor::SubscriptionSupervisor>>,
    /// Shared foreign-server registry. `None` disables federation.
    federation: Option<Arc<crate::federation::FederationRuntime>>,
    /// Detached-query service. `None` in tests and in embeddings that do not
    /// install the control statements.
    jobs: Option<Arc<crate::jobs::JobService>>,
    /// How connections authenticate; the default authenticates nobody.
    auth: crate::auth::AuthSettings,
    /// Per-user ACL views under `--rbac`.
    acl_cache: Arc<AclCache>,
    /// The domain this cluster creates tables in and permits writes to.
    /// `None` falls back to each token's domain.
    cluster_domain: Option<String>,
}

impl PgSessionFactory {
    #[cfg(test)]
    pub fn new(
        meta_client: MetaDataClientRef,
        args: &CoreArgs,
        auth_manager: Arc<AuthManager>,
    ) -> Result<Self> {
        Self::new_with_memory_pool(
            meta_client,
            args,
            auth_manager,
            lakesoul_datafusion::session::ExecutionMemoryArgs::default().create_pool(),
        )
    }

    /// Build a factory whose sessions share `memory_pool`, created once by the
    /// server process.
    pub fn new_with_memory_pool(
        meta_client: MetaDataClientRef,
        args: &CoreArgs,
        auth_manager: Arc<AuthManager>,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let base = LakeSoulSessionFactory::new_with_memory_pool(
            Arc::clone(&meta_client),
            args,
            memory_pool,
        )?;
        let acl_cache = Arc::new(AclCache::new(Arc::clone(&meta_client)));
        Ok(Self {
            meta_client,
            base,
            auth_manager,
            distributed: false,
            ivm: None,
            ivm_default: false,
            catalog_options: PgCatalogOptions {
                include_synthetic_postgres_database: false,
            },
            warehouse: args.warehouse_prefix.clone(),
            object_store_options: SecretMap::from_map(args.s3_options()),
            subscriptions: None,
            federation: None,
            jobs: None,
            auth: crate::auth::AuthSettings::default(),
            acl_cache,
            cluster_domain: None,
        })
    }

    /// Sets how connections authenticate (see [`crate::auth`]).
    pub fn with_auth(mut self, auth: crate::auth::AuthSettings) -> Self {
        self.auth = auth;
        self
    }

    pub fn auth(&self) -> &crate::auth::AuthSettings {
        &self.auth
    }

    /// Fix the domain this cluster writes in. Requires `--rbac`; when unset,
    /// each connection writes in its token's domain (pre-cluster behaviour).
    pub fn with_cluster_domain(mut self, domain: Option<String>) -> Self {
        self.cluster_domain = domain;
        self
    }

    /// Installs the foreign-server runtime; `None` (`--no-federation`) leaves
    /// foreign tables unresolved and foreign-data DDL refused.
    pub fn with_federation(
        mut self,
        federation: Option<Arc<crate::federation::FederationRuntime>>,
    ) -> Self {
        self.federation = federation;
        self
    }

    pub fn federation(&self) -> Option<&Arc<crate::federation::FederationRuntime>> {
        self.federation.as_ref()
    }

    /// Installs the detached-query service used by the SQL control statements
    /// and the status/result table functions.
    pub fn with_jobs(mut self, jobs: Arc<crate::jobs::JobService>) -> Self {
        self.jobs = Some(jobs);
        self
    }

    pub fn jobs(&self) -> Option<&Arc<crate::jobs::JobService>> {
        self.jobs.as_ref()
    }

    /// Installs the subscription supervisor; `None` (`--no-subscriptions`)
    /// leaves subscription DDL refused.
    pub fn with_subscriptions(
        mut self,
        subscriptions: Option<Arc<crate::subscribe::supervisor::SubscriptionSupervisor>>,
    ) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    pub fn subscriptions(
        &self,
    ) -> Option<&Arc<crate::subscribe::supervisor::SubscriptionSupervisor>> {
        self.subscriptions.as_ref()
    }

    /// Plans every session against distributed workers.
    ///
    /// Installed by the server entry point when workers are configured: the
    /// coordinator then serves the queries the distributed planner cannot
    /// plan by itself — or, in production mode, fails them instead of falling
    /// back to single-node execution.
    pub fn with_distributed(mut self, options: DistributedOptions) -> Self {
        self.base = self.base.with_distributed(options);
        self.distributed = true;
        self
    }

    /// Install the process-wide IVM service used by `INSERT ... SELECT`
    /// when the connection enables `lakesoul.ivm.enabled`.
    pub fn with_ivm(mut self, ivm: Arc<crate::ivm::IvmService>) -> Self {
        self.ivm = Some(ivm);
        self
    }

    /// Start new connections with `lakesoul.ivm.enabled` set to `enabled`.
    /// A connection can still override it with `SET`.
    pub fn with_ivm_default(mut self, enabled: bool) -> Self {
        self.ivm_default = enabled;
        self
    }

    /// A session with the factory's memory pool, catalog and planner, used to
    /// build the IVM runtime configuration at startup.
    pub(crate) fn create_template_session(&self) -> Result<Arc<SessionContext>> {
        self.base.create_session(&LakeSoulSessionOptions::default())
    }

    /// A cloneable factory for isolated IVM refresh sessions.
    pub fn refresh_session_factory(&self) -> LakeSoulSessionFactory {
        self.base.clone()
    }

    /// The metadata client `CREATE TABLE`/`DROP TABLE` write through.
    pub fn meta_client(&self) -> &MetaDataClientRef {
        &self.meta_client
    }

    /// The object-store prefix new tables are created under.
    pub fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    /// `fs.s3a.*` options from the server's CLI/environment.
    pub fn object_store_options(&self) -> &SecretMap {
        &self.object_store_options
    }

    /// The metadata view shared by every connection of this factory.
    ///
    /// Call [`CatalogSnapshot::load`] on it once at start-up to warm it, so the
    /// first connection does not pay for the initial load.
    pub fn catalog_snapshot(&self) -> &Arc<CatalogSnapshot> {
        self.base.catalog_snapshot()
    }

    /// One span per connection: the namespace-visibility query below is
    /// instrumented on its own, and without this parent it opens a root trace
    /// per connection.
    #[instrument(
        name = "create_session",
        level = "info",
        skip_all,
        fields(user = %identity.user, database = %identity.database)
    )]
    pub async fn create_session(
        &self,
        identity: SessionIdentity,
        settings: &SessionSettings,
    ) -> Result<Arc<PgSession>> {
        // Fail closed: under domain enforcement every session - interactive
        // or a detached job's - must say which domain it acts in.
        if self.auth.rbac && identity.access.is_none() {
            bail!(
                "domain enforcement is on, but the session for \"{}\" carries no domain",
                identity.user
            );
        }
        let token_domain = identity
            .access
            .as_ref()
            .map(|access| access.domain().to_string());
        let cluster_domain = if self.auth.rbac {
            Some(
                self.cluster_domain
                    .clone()
                    .or_else(|| token_domain.clone())
                    .expect("--rbac sessions carry a domain"),
            )
        } else {
            None
        };
        let acl = if self.auth.rbac {
            let domains: Vec<String> = token_domain.clone().into_iter().collect();
            Some(self.acl_cache.get_or_load(&identity.user, &domains).await?)
        } else {
            None
        };
        // Namespace visibility is snapshotted once per connection; the sync
        // catalog interfaces later read this snapshot without blocking. Under
        // --rbac the ACL view decides visible namespaces (including
        // cross-domain grants); otherwise only the token's domain applies.
        let visible_databases: BTreeSet<String> = if let Some(entry) = &acl {
            entry.view().visible_namespaces()
        } else {
            let namespaces = self.meta_client.get_all_namespace().await?;
            namespaces
                .into_iter()
                .filter(|namespace| {
                    identity
                        .access
                        .as_ref()
                        .is_none_or(|access| access.allows(&namespace.domain))
                })
                .map(|namespace| namespace.namespace)
                .collect()
        };
        if !visible_databases.contains(&identity.database) {
            bail!("database \"{}\" does not exist", identity.database);
        }

        let context = self.base.create_session(&LakeSoulSessionOptions {
            default_schema: settings.initial_schema().to_string(),
            time_zone: settings.time_zone.iana_name().map(str::to_owned),
            federation_writes: true,
        })?;

        let state = context.state();

        // Report the table's vector columns as the pgvector `vector` type on
        // the wire.  The provider tags the declared vector columns with this
        // marker, the result schemas inherit it, and the encoder picks the
        // vector format from the reported type — so drivers (psycopg with
        // `register_vector`) decode results as vectors.  It goes on the config
        // before any provider options are derived from it, so the catalog and
        // every later provider agree.
        let mut config = state
            .config()
            .clone()
            .with_default_catalog_and_schema(
                identity.database.clone(),
                PUBLIC_SCHEMA.to_string(),
            )
            .with_create_default_catalog_and_schema(false)
            .with_extension(Arc::new(LakeSoulVectorFieldMetadata {
                fields: vec![(
                    datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_KEY.to_string(),
                    "vector".to_string(),
                )],
            }));
        // The vector-search options exist on every connection config: the
        // scan reads them, `SET lakesoul_vector_search.*` writes them and
        // `SET ivfflat.probes` maps onto `nprobe`.
        config
            .options_mut()
            .extensions
            .insert(LakeSoulVectorSearchConfig::default());
        config
            .options_mut()
            .extensions
            .insert(crate::ivm::IvmSessionConfig {
                enabled: self.ivm_default,
            });
        config.options_mut().extensions.insert(PgIdentityConfig {
            user: identity.user.clone(),
            database: identity.database.clone(),
            domain: identity
                .access
                .as_ref()
                .map(|access| access.domain().to_string()),
        });
        let transaction = crate::transaction::shared();
        if let Some(acl) = &acl {
            config.set_extension(Arc::new(crate::acl::PgAclSession {
                acl: Arc::clone(acl),
                cluster_domain: cluster_domain.clone(),
                transaction: Arc::clone(&transaction),
            }));
        }
        if let Some(federation) = &self.federation {
            config.set_extension(Arc::new(crate::federation::PgFederationSession {
                runtime: Arc::clone(federation),
                user: identity.user.clone(),
                domain: identity
                    .access
                    .as_ref()
                    .map(|access| access.domain().to_string()),
            }));
        }
        // How the next statement treats a key that is already present. The
        // INSERT hook sets it before every write; the sink reads it while
        // planning, so `ON CONFLICT DO NOTHING` drops rows instead of failing.
        config.set_extension(Arc::new(
            lakesoul_datafusion::write_mode::LakeSoulWriteMode::new(),
        ));
        // The read-modify-write snapshot of the UPDATE being executed. A new
        // handle is created for each connection (never cloned from another
        // session), so two sessions cannot share a mutable snapshot; the
        // statement hook sets it at Execute and its guard clears it again.
        config.set_extension(Arc::new(
            lakesoul_datafusion::write_mode::LakeSoulFixedSnapshot::new(),
        ));
        // Where an open transaction block collects the writes it will publish.
        // The block hook installs a sink on `BEGIN` and clears it when the block
        // ends; the sink writes through the connection's metadata client.
        config.set_extension(Arc::new(
            lakesoul_datafusion::write_mode::LakeSoulDeferredCommit::new(),
        ));
        let state = SessionStateBuilder::new_from_existing(state)
            .with_config(config)
            .build();
        // Created before the catalog: the catalog pins local table scans to
        // `lakesoul.read.time_travel`, which lives in this map.
        let parameters = Arc::new(PgParameters::new(&identity, settings));
        let catalog = pg_lakesoul_catalog_with_federation(
            Arc::clone(&self.meta_client),
            LakeSoulProviderOptions::from_session(&state),
            &identity.database,
            Arc::clone(self.base.catalog_snapshot()),
            self.federation.clone(),
            &identity.user,
            identity.access.clone(),
            acl.clone(),
            Arc::clone(&parameters),
            Arc::clone(&transaction),
        );
        let catalog_list: Arc<PgDatabaseCatalogList> =
            Arc::new(PgDatabaseCatalogList::new(
                identity.database.clone(),
                visible_databases,
                catalog,
            ));
        let catalog_list_for_context: Arc<dyn datafusion::catalog::CatalogProviderList> =
            catalog_list.clone();
        let context = Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new_from_existing(state)
                .with_catalog_list(catalog_list_for_context)
                .build(),
        ));

        setup_pg_catalog_with_options(
            &context,
            &identity.database,
            AuthRoles(Arc::clone(&self.auth_manager)),
            self.catalog_options,
        )?;
        crate::pg_types::install(&context)
            .map_err(|error| report!("PostgreSQL type planner: {error}"))?;
        // After the upstream setup: the registry is keyed by name, so the shims
        // replace the narrower upstream signatures.
        crate::pg_compat::register_pg_catalog_shims(
            &context,
            &parameters,
            &identity.user,
        );
        // pgvector syntax (`<->`, `<#>`, `<=>` and `'[...]'` literals).  The
        // install rebuilds the state from the existing one, so the LakeSoul
        // catalog, optimizer rules and config extensions stay in place; it
        // must run after `setup_pg_catalog_with_options`, whose own planners
        // it appends to.  The parameter planner goes first: planners are
        // consulted in order and it only claims the operators whose
        // parameters it annotates.
        crate::pg_vector::install(&context)
            .map_err(|error| report!("pgvector parameter planner: {error}"))?;
        datafusion_postgres::pgvector::install(&context)
            .map_err(|error| report!("pgvector install: {error}"))?;
        if let Some(jobs) = &self.jobs {
            crate::jobs::register_control_stubs(&context);
            context.register_udtf(
                "lakesoul_query_status",
                Arc::new(crate::jobs::QueryStatusFunction::new(Arc::clone(jobs))),
            );
            context.register_udtf(
                "lakesoul_queries",
                Arc::new(crate::jobs::QueriesFunction::new(Arc::clone(jobs))),
            );
            context.register_udtf(
                "lakesoul_query_result",
                Arc::new(crate::jobs::QueryResultFunction::new(Arc::clone(jobs))),
            );
            context.register_udtf(
                "lakesoul_query_result_set",
                Arc::new(crate::jobs::QueryResultSetFunction::new(Arc::clone(jobs))),
            );
            context.register_udtf(
                "lakesoul_query_result_sets",
                Arc::new(crate::jobs::QueryResultSetsFunction::new(Arc::clone(jobs))),
            );
        }
        // Index rebuild is a per-session function: it captures the
        // connection's database and the server's object-store options, which
        // a shared UDF in the base session cannot know.
        context.register_udf(crate::index_ddl::rebuild_udf(
            Arc::clone(&self.meta_client),
            identity.database.clone(),
            self.object_store_options.clone(),
        ));
        // Register on the final frontend context, after catalog/default-schema
        // and expression-planner replacement. UDTFs resolve through this state.
        // The pinned-read function resolves tables through this context's
        // catalog, so ACL/RBAC visibility is the same as for a plain scan.
        context.register_udtf(
            crate::time_travel::FUNCTION_NAME,
            Arc::new(crate::time_travel::TableScanFunction::new(&context)),
        );
        context.register_udtf(
            crate::snapshot::TAGS_FUNCTION,
            Arc::new(crate::snapshot::TagsFunction::new(
                &context,
                Arc::clone(self.meta_client()),
                identity.database.clone(),
            )),
        );
        lakesoul_datafusion::udf::vector_batch::register(&context);
        let mut session = PgSession::new(
            identity,
            context,
            settings.clone(),
            parameters,
            Arc::clone(self.base.catalog_snapshot()),
            acl,
            Arc::clone(&catalog_list),
            cluster_domain,
            self.distributed,
            Arc::clone(&transaction),
        )?;
        session.ivm = self.ivm.clone();
        let session = Arc::new(session);
        {
            let settings = session.settings.read();
            info!(
                user = %session.identity.user,
                database = %session.identity.database,
                time_zone = ?settings.time_zone.iana_name().unwrap_or("None"),
                statement_timeout = ?settings.statement_timeout,
                catalog_view_age_ms = ?session
                    .catalog_snapshot()
                    .view()
                    .refreshed_at()
                    .map(|at| at.elapsed().as_millis()),
                "created PostgreSQL session"
            );
        }
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_schema_skips_pg_catalog() {
        let settings = SessionSettings {
            search_path: vec!["pg_catalog".to_string(), "tenant_a".to_string()],
            ..Default::default()
        };
        assert_eq!(settings.initial_schema(), "tenant_a");
    }

    #[test]
    fn initial_schema_falls_back_to_public() {
        let settings = SessionSettings {
            search_path: vec!["pg_catalog".to_string()],
            ..Default::default()
        };
        assert_eq!(settings.initial_schema(), PUBLIC_SCHEMA);
    }

    /// The PG factory replaces the original catalog/default schema and installs
    /// additional planners. Both functions must resolve through the final state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_batch_functions_work_in_final_postgres_session() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::physical_plan::displayable;

        let metadata =
            Arc::new(lakesoul_metadata::MetaDataClient::from_env().await.unwrap());
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("pg_native_batch_{}_{}", std::process::id(), unique);
        let location = std::env::temp_dir().join(&name);
        let base = lakesoul_datafusion::create_lakesoul_session_ctx(
            metadata.clone(),
            &CoreArgs::from_env(),
        )
        .unwrap();
        let config =
            r#"[{"column":"vec","dim":8,"nlist":4,"total_bits":7,"metric":"L2"}]"#;
        base.sql(&format!("CREATE EXTERNAL TABLE lakesoul.default.{name} (id BIGINT NOT NULL PRIMARY KEY, vec FLOAT[] NOT NULL) STORED AS LAKESOUL LOCATION '{}' OPTIONS ('vector_index_columns' '{config}', 'hashBucketNum' '4')", location.display()))
            .await.unwrap().collect().await.unwrap();
        base.sql(&format!("INSERT INTO lakesoul.default.{name} SELECT CAST(g.value AS BIGINT), ARRAY[g.value * 0.1, sin(g.value), cos(g.value), 0.5, -0.5, 1.0, -1.0, g.value * 0.01] FROM generate_series(0, 49) AS g(value)"))
            .await.unwrap().collect().await.unwrap();
        let query_name = format!("{name}_queries");
        let query_location = std::env::temp_dir().join(&query_name);
        base.sql(&format!("CREATE EXTERNAL TABLE lakesoul.default.{query_name} (qid BIGINT NOT NULL PRIMARY KEY, qvec FLOAT[] NOT NULL) STORED AS LAKESOUL LOCATION '{}' OPTIONS ('hashBucketNum' '1')", query_location.display()))
            .await.unwrap().collect().await.unwrap();
        base.sql(&format!("INSERT INTO lakesoul.default.{query_name} SELECT CAST(g.value + 1000 AS BIGINT), ARRAY[0.73, sin(7), cos(7), 0.5, -0.5, 1.0, -1.0, 0.07] FROM generate_series(0, 999) AS g(value)"))
            .await.unwrap().collect().await.unwrap();
        let factory = PgSessionFactory::new(
            metadata.clone(),
            &CoreArgs::from_env(),
            Arc::new(AuthManager::new()),
        )
        .unwrap();
        let session = factory
            .create_session(
                SessionIdentity {
                    user: "lakesoul_test".into(),
                    database: "default".into(),
                    access: None,
                },
                &SessionSettings::default(),
            )
            .await
            .unwrap();
        let context = &session.context;
        assert_eq!(
            context.state().config_options().catalog.default_catalog,
            "default"
        );
        assert_eq!(
            context.state().config_options().catalog.default_schema,
            "public"
        );
        let query = vec![
            0.73f32,
            (7.0f32).sin(),
            (7.0f32).cos(),
            0.5,
            -0.5,
            1.0,
            -1.0,
            0.07,
        ];
        let queries_json =
            serde_json::to_string(&vec![query.clone(), query.clone()]).unwrap();
        // Keep errors until after fixture cleanup, including all index objects.
        let check: datafusion::error::Result<()> = async {
            let sql = format!("SELECT * FROM lakesoul_vector_search_batch('{name}', 'vec', '{queries_json}', 3, 4, 'auto', 'exact')");
            let plan = context.sql(&sql).await?.create_physical_plan().await?;
            assert!(displayable(plan.as_ref()).indent(true).to_string().contains("VectorBatchSearchExec"));
            let analyze = context.sql(&format!("EXPLAIN ANALYZE {sql}")).await?.collect().await?;
            let analyze = datafusion::arrow::util::pretty::pretty_format_batches(&analyze)?.to_string();
            for metric in ["batch_windows", "batch_output_batches", "batch_queries", "batch_shard_probes", "batch_cluster_loads", "batch_rerank_blocks", "batch_shared_block_reuses", "batch_peak_workers"] {
                assert!(analyze.contains(metric), "missing {metric} in EXPLAIN ANALYZE:\n{analyze}");
            }
            let rows = context.sql(&sql).await?.collect().await?;
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
            let batch = &rows[0];
            assert_eq!(batch.schema().fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["query_id", "rank", "id", "distance"]);
            let ids = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            let literal = query.iter().map(f32::to_string).collect::<Vec<_>>().join(",");
            let expected = context.sql(&format!("SELECT id FROM {name} ORDER BY array_distance(vec, ARRAY[{literal}]) LIMIT 3")).await?.collect().await?;
            let expected: Vec<i64> = expected.iter().flat_map(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().iter().copied()).collect();
            assert_eq!(&ids.values()[..3], expected.as_slice());
            assert_eq!(&ids.values()[3..], expected.as_slice());
            // An existing query table is sufficient; the read-only PG server
            // needs no CREATE TEMP TABLE or statement-policy changes.
            let sql = format!("SELECT * FROM lakesoul_vector_search_batch_from_table('{name}', 'vec', '{query_name}', 'qid', 'qvec', 1, 4, 'L2', 'exact', 1)");
            let explain = context.sql(&format!("EXPLAIN ANALYZE {sql}")).await?.collect().await?;
            let explain = datafusion::arrow::util::pretty::pretty_format_batches(&explain)?.to_string();
            assert!(explain.contains("batch_windows=4"), "expected four 256-query windows:\n{explain}");
            let rows = context.sql(&sql).await?.collect().await?;
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1000);
            let mut qids: Vec<i64> = rows.iter().flat_map(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().iter().copied()).collect();
            qids.sort_unstable();
            assert_eq!(qids, (1000..2000).collect::<Vec<_>>());
            let parameter_sql = format!("SELECT * FROM lakesoul_vector_search_batch('{name}', 'vec', $1, 3)");
            let error = context.sql(&parameter_sql).await.unwrap_err().to_string();
            assert!(error.contains("placeholders"), "{error}");
            let hidden = context.sql("SELECT * FROM lakesoul_vector_search_batch('another_database.public.private_vectors', 'vec', '[[1,2]]', 1)").await?;
            assert!(hidden.collect().await.is_err(), "batch may not bypass the visible catalog");
            Ok(())
        }.await;
        metadata.drop_table(&name, "default").await.unwrap();
        metadata.drop_table(&query_name, "default").await.unwrap();
        assert_eq!(location.parent(), Some(std::env::temp_dir().as_path()));
        assert_eq!(location.file_name().unwrap().to_str(), Some(name.as_str()));
        assert_eq!(
            query_location.parent(),
            Some(std::env::temp_dir().as_path())
        );
        assert_eq!(
            query_location.file_name().unwrap().to_str(),
            Some(query_name.as_str())
        );
        std::fs::remove_dir_all(&location).unwrap();
        std::fs::remove_dir_all(&query_location).unwrap();
        check.unwrap();
    }
}
