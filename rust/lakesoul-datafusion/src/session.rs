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

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, TableProviderFactory};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::object_store::{register_hdfs_object_store, register_s3_object_store};
use object_store::local::LocalFileSystem;
use rootcause::{bail, report};
use url::Url;

use crate::Result;
use crate::catalog::{LakeSoulCatalog, LakeSoulProviderOptions};
use crate::cli::CoreArgs;
use crate::datasource::table_factory::LakeSoulTableProviderFactory;
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
}

impl LakeSoulSessionFactory {
    pub fn new(meta_client: crate::MetaDataClientRef, args: &CoreArgs) -> Result<Self> {
        let session_template = Self::session_template()?;
        let object_store_config =
            LakeSoulIOConfigBuilder::new_with_object_store_options(args.s3_options())
                .build();
        let warehouse = Self::parse_warehouse(args.warehouse_prefix.as_deref())?;
        let table_factory = Arc::new(LakeSoulTableProviderFactory::new(
            Arc::clone(&meta_client),
            args.warehouse_prefix.clone(),
        ));
        Ok(Self {
            meta_client,
            session_template,
            object_store_config,
            warehouse,
            planner: LakeSoulQueryPlanner::new_ref(),
            table_factory,
            catalog_decorator: None,
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

        let runtime = Arc::new(RuntimeEnv::default());
        self.register_warehouse_object_store(&runtime)?;

        let mut state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(Arc::clone(&self.planner))
            .with_optimizer_rule(Arc::new(
                crate::planner::vector_search_rule::VectorSearchPushdownRule,
            ))
            .build();
        state.table_factories_mut().insert(
            "LAKESOUL".to_string(),
            Arc::clone(&self.table_factory) as Arc<dyn TableProviderFactory>,
        );
        let provider_options = LakeSoulProviderOptions::from_session(&state);
        let ctx = Arc::new(SessionContext::new_with_state(state));
        ctx.register_udf((*crate::udf::vector_search_marker::marker_udf()).clone());

        let lakesoul_catalog = Arc::new(LakeSoulCatalog::new(
            Arc::clone(&self.meta_client),
            provider_options,
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

    /// Constant parts of the session config, shared by every session.
    /// Single source of truth: [`crate::create_lakesoul_session_config`].
    fn session_template() -> Result<SessionConfig> {
        crate::create_lakesoul_session_config()
    }

    fn parse_warehouse(prefix: Option<&str>) -> Result<WarehouseConfig> {
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
    fn register_warehouse_object_store(&self, runtime: &RuntimeEnv) -> Result<()> {
        match &self.warehouse {
            WarehouseConfig::S3(url) => {
                register_s3_object_store(url, &self.object_store_config, runtime)?
            }
            WarehouseConfig::Hdfs(url) => {
                if !url.has_host() {
                    bail!("HDFS warehouse prefix without host is not supported");
                }
                register_hdfs_object_store(
                    url,
                    &url[url::Position::BeforeHost..url::Position::BeforePath],
                    &self.object_store_config,
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
}
