// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Reusable LakeSoul DataFusion session construction.
//!
//! [`LakeSoulSessionFactory`] owns immutable resources shared by all sessions
//! (metadata client, session config template, object-store configuration and
//! warehouse location) and builds an independent [`SessionContext`] on each
//! call. PostgreSQL connection identity and settings are intentionally kept in
//! the `postgres-lakesoul` crate.

use datafusion::catalog::{CatalogProvider, TableProviderFactory};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt, WorkerResolver};
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::object_store::{register_hdfs_object_store, register_s3_object_store};
use object_store::local::LocalFileSystem;
use rootcause::{bail, report};
use std::sync::Arc;
use url::Url;

use crate::Result;
use crate::catalog::{
    CatalogSnapshot, DEFAULT_CATALOG_REFRESH_INTERVAL, LakeSoulCatalog,
    LakeSoulProviderOptions,
};
use crate::cli::CoreArgs;
use crate::datasource::table_factory::LakeSoulTableProviderFactory;
use crate::distributed::DistributedOptions;
use crate::distributed::codec::LakeSoulCodec;
use crate::distributed::planner::LakeSoulDistributedQueryPlanner;
use crate::distributed::resolver::{
    KubernetesWorkerResolver, StaticWorkerResolver, WorkerDiscovery,
};
use crate::planner::LakeSoulQueryPlanner;

pub const DEFAULT_CATALOG: &str = "lakesoul";
pub const DEFAULT_SCHEMA: &str = "default";

/// Configuration used to create object-store clients.
pub type ObjectStoreConfig = LakeSoulIOConfig;

/// Generic options applied to one newly-created LakeSoul session.
#[derive(Debug, Clone)]
pub struct LakeSoulSessionOptions {
    /// Schema used to resolve unqualified table names.
    pub default_schema: String,
    /// Optional DataFusion execution time zone. When omitted, the value from
    /// the factory's [`SessionConfig`] template is preserved.
    pub time_zone: Option<String>,
}

impl Default for LakeSoulSessionOptions {
    fn default() -> Self {
        Self {
            default_schema: DEFAULT_SCHEMA.to_string(),
            time_zone: None,
        }
    }
}

/// Parsed warehouse prefix used to register an object store in each session's
/// [`RuntimeEnv`].
#[derive(Debug, Clone)]
pub enum WarehouseConfig {
    /// `s3://`/`s3a://` prefix, trimmed to scheme + authority.
    S3(Url),
    /// `hdfs://` prefix; the authority is used as the HDFS default FS.
    Hdfs(Url),
    /// `file://` prefix or no prefix at all.
    Local,
}

type CatalogDecorator =
    dyn Fn(Arc<LakeSoulCatalog>) -> Arc<dyn CatalogProvider> + Send + Sync;

/// Factory for independent LakeSoul [`SessionContext`]s.
///
/// Every [`create_session`](Self::create_session) call clones the session
/// config template, builds a fresh [`RuntimeEnv`] and wires the LakeSoul
/// planner, table factory and catalog into a fresh context.
pub struct LakeSoulSessionFactory {
    meta_client: crate::MetaDataClientRef,
    session_template: SessionConfig,
    object_store_config: ObjectStoreConfig,
    warehouse: WarehouseConfig,
    planner: Arc<dyn QueryPlanner + Send + Sync>,
    table_factory: Arc<LakeSoulTableProviderFactory>,
    catalog_decorator: Option<Arc<CatalogDecorator>>,
    /// Distributed planning options; `None` keeps sessions single-node.
    distributed: Option<DistributedOptions>,
    /// `target_partitions` of single-node sessions; distributed sessions use
    /// [`DistributedOptions::target_partitions`] instead.
    target_partitions: Option<usize>,
    /// Metadata view shared by every session of this factory.
    ///
    /// One snapshot (and therefore one periodic refresher) per factory instead
    /// of one per session: per-session snapshots would multiply the periodic
    /// metadata load by the number of connections.
    catalog_snapshot: Arc<CatalogSnapshot>,
}

impl LakeSoulSessionFactory {
    pub fn new(meta_client: crate::MetaDataClientRef, args: &CoreArgs) -> Result<Self> {
        let session_template = Self::session_template()?;
        let object_store_config =
            LakeSoulIOConfigBuilder::new_with_object_store_options(args.s3_options())
                .build();
        let warehouse = parse_warehouse(args.warehouse_prefix.as_deref())?;
        let table_factory = Arc::new(LakeSoulTableProviderFactory::new(
            Arc::clone(&meta_client),
            args.warehouse_prefix.clone(),
        ));
        let catalog_snapshot = CatalogSnapshot::new(
            Arc::clone(&meta_client),
            DEFAULT_CATALOG_REFRESH_INTERVAL,
        );
        Ok(Self {
            meta_client,
            session_template,
            object_store_config,
            warehouse,
            planner: LakeSoulQueryPlanner::new_ref(),
            table_factory,
            catalog_decorator: None,
            distributed: None,
            target_partitions: None,
            catalog_snapshot,
        })
    }

    /// Install a decorator applied to the `lakesoul` catalog of every session
    /// created by this factory.
    pub fn with_catalog_decorator<F>(mut self, decorate_catalog: F) -> Self
    where
        F: Fn(Arc<LakeSoulCatalog>) -> Arc<dyn CatalogProvider> + Send + Sync + 'static,
    {
        self.catalog_decorator = Some(Arc::new(decorate_catalog));
        self
    }

    /// Replace the session config template shared by every session.
    ///
    /// Used by [`crate::create_lakesoul_session_ctx_with_config`] to attach
    /// caller-supplied extensions (e.g. vector-search options).
    pub fn with_session_template(mut self, session_template: SessionConfig) -> Self {
        self.session_template = session_template;
        self
    }

    /// Build a fresh [`SessionContext`].
    pub fn create_session(
        &self,
        options: &LakeSoulSessionOptions,
    ) -> Result<Arc<SessionContext>> {
        let mut session_config = self
            .session_template
            .clone()
            .with_default_catalog_and_schema(
                DEFAULT_CATALOG.to_string(),
                options.default_schema.clone(),
            );
        if let Some(time_zone) = &options.time_zone {
            session_config.options_mut().execution.time_zone = Some(time_zone.clone());
        }

        let distributed = self.distributed.clone();
        // `target_partitions` is parameterized rather than fixed at 1:
        // distributed planning relies on hash `RepartitionExec` stage
        // boundaries, which the physical planner only emits when
        // `target_partitions > 1`. A session template supplied by the caller
        // keeps its own value unless the factory overrides it.
        let target_partitions = match &distributed {
            Some(distributed) => distributed.target_partitions.max(2),
            None => self
                .target_partitions
                .unwrap_or(session_config.options().execution.target_partitions)
                .max(1),
        };
        session_config.options_mut().execution.target_partitions = target_partitions;

        let runtime = Arc::new(RuntimeEnv::default());
        register_warehouse_object_store(
            &self.warehouse,
            &self.object_store_config,
            &runtime,
        )?;

        let resolver = distributed
            .as_ref()
            .map(|dist| build_worker_resolver(&dist.discovery))
            .transpose()?;

        // Distributed sessions must install the LakeSoul planner *before*
        // `with_distributed_planner()`: the latter wraps the planner already
        // present on the builder, and the reverse order would let the
        // LakeSoul planner overwrite the distributed one.
        let mut builder = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::clone(&self.planner))
            .with_optimizer_rule(Arc::new(
                crate::planner::vector_search_rule::VectorSearchPushdownRule,
            ));
        if let (Some(dist), Some(resolver)) = (&distributed, &resolver) {
            builder = builder
                .with_distributed_worker_resolver(Arc::clone(resolver))
                .with_distributed_user_codec(LakeSoulCodec)
                .with_distributed_metrics_collection(true)?;
            if let Some(bytes_per_partition) = dist.bytes_per_partition {
                builder = builder.with_distributed_file_scan_config_bytes_per_partition(
                    bytes_per_partition,
                )?;
            }
            builder = builder.with_distributed_planner();

            // Availability policy gate in front of the distributed planner:
            // fast-fail in production, optional single-node fallback in
            // development.
            let distributed_planner = std::mem::take(builder.query_planner())
                .expect("with_distributed_planner installed a query planner");
            builder = builder.with_query_planner(Arc::new(
                LakeSoulDistributedQueryPlanner::new(
                    distributed_planner,
                    Arc::clone(resolver),
                    dist.fallback_to_local,
                ),
            ));
        }

        let mut state = builder.build();
        state.table_factories_mut().insert(
            "LAKESOUL".to_string(),
            Arc::clone(&self.table_factory) as Arc<dyn TableProviderFactory>,
        );
        let provider_options = LakeSoulProviderOptions::from_session(&state);
        let ctx = Arc::new(SessionContext::new_with_state(state));
        ctx.register_udf((*crate::udf::vector_search_marker::marker_udf()).clone());

        let lakesoul_catalog = Arc::new(LakeSoulCatalog::with_snapshot(
            Arc::clone(&self.meta_client),
            provider_options,
            Arc::clone(&self.catalog_snapshot),
        ));
        let catalog = match &self.catalog_decorator {
            Some(decorate) => decorate(lakesoul_catalog),
            None => lakesoul_catalog,
        };
        ctx.state()
            .catalog_list()
            .register_catalog(DEFAULT_CATALOG.to_string(), catalog);

        info!(
            "created LakeSoul session, catalogs: {:?}",
            ctx.catalog_names()
        );
        Ok(ctx)
    }

    /// Sets `target_partitions` for sessions created by this factory.
    ///
    /// Unset by default, so the session template's value is kept (1 for the
    /// default template, and whatever a caller supplied through
    /// [`Self::with_session_template`]). Distributed sessions ignore this and
    /// use [`DistributedOptions::target_partitions`] instead, which is forced
    /// to at least 2.
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = Some(target_partitions);
        self
    }

    /// The metadata view shared by this factory's sessions.
    ///
    /// Call [`CatalogSnapshot::load`] on it once at start-up to warm the view
    /// before serving queries.
    pub fn catalog_snapshot(&self) -> &Arc<CatalogSnapshot> {
        &self.catalog_snapshot
    }

    /// Enable distributed planning for sessions created by this factory.
    pub fn with_distributed(mut self, options: DistributedOptions) -> Self {
        self.distributed = Some(options);
        self
    }

    /// Constant parts of the session config, shared by every session.
    /// Single source of truth: [`crate::create_lakesoul_session_config`].
    fn session_template() -> Result<SessionConfig> {
        crate::create_lakesoul_session_config()
    }
}

pub(crate) fn parse_warehouse(prefix: Option<&str>) -> Result<WarehouseConfig> {
    let Some(prefix) = prefix else {
        return Ok(WarehouseConfig::Local);
    };
    let url = Url::parse(prefix).map_err(|_| report!("Invalid warehouse prefix"))?;
    match url.scheme() {
        "s3" | "s3a" => {
            // Register under scheme + authority; paths resolve through the same store.
            let url = Url::parse(&url[..url::Position::BeforePath])
                .map_err(|_| report!("Invalid warehouse prefix"))?;
            Ok(WarehouseConfig::S3(url))
        }
        "hdfs" => Ok(WarehouseConfig::Hdfs(url)),
        "file" => Ok(WarehouseConfig::Local),
        _ => bail!("Invalid scheme of warehouse prefix"),
    }
}

/// Register the warehouse object store on a session-private runtime.
fn register_warehouse_object_store(
    warehouse: &WarehouseConfig,
    object_store_config: &ObjectStoreConfig,
    runtime: &RuntimeEnv,
) -> Result<()> {
    match warehouse {
        WarehouseConfig::S3(url) => {
            register_s3_object_store(url, object_store_config, runtime)?
        }
        WarehouseConfig::Hdfs(url) => {
            if !url.has_host() {
                bail!("HDFS warehouse prefix without host is not supported");
            }
            register_hdfs_object_store(
                url,
                &url[url::Position::BeforeHost..url::Position::BeforePath],
                object_store_config,
                runtime,
            )?;
        }
        WarehouseConfig::Local => {
            runtime.register_object_store(
                &Url::parse("file://").expect("file:// is a valid URL"),
                Arc::new(LocalFileSystem::new()),
            );
        }
    }
    Ok(())
}

/// Build a [`RuntimeEnv`] carrying the warehouse object store registered the
/// same way a coordinator session does it.
///
/// Workers and coordinators must observe identical object-store configuration
/// for encoded plans to resolve their stores on either side.
pub fn build_worker_runtime_env(args: &CoreArgs) -> Result<Arc<RuntimeEnv>> {
    let warehouse = parse_warehouse(args.warehouse_prefix.as_deref())?;
    let object_store_config =
        LakeSoulIOConfigBuilder::new_with_object_store_options(args.s3_options()).build();
    let runtime = Arc::new(RuntimeEnv::default());
    register_warehouse_object_store(&warehouse, &object_store_config, &runtime)?;
    Ok(runtime)
}

/// Instantiates the worker resolver for the configured discovery backend.
pub(crate) fn build_worker_resolver(
    discovery: &WorkerDiscovery,
) -> Result<Arc<dyn WorkerResolver>> {
    match discovery {
        WorkerDiscovery::Static(urls) => {
            Ok(Arc::new(StaticWorkerResolver::new(urls.clone())?))
        }
        WorkerDiscovery::Kubernetes(config) => {
            Ok(Arc::new(KubernetesWorkerResolver::start(config)?))
        }
    }
}
