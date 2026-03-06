// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The LakeSoul DataFusion module.

#[macro_use]
extern crate tracing;

use std::{env, sync::Arc};

use catalog::LakeSoulCatalog;
use datafusion::{
    config::Dialect,
    execution::{
        SessionStateBuilder, object_store::ObjectStoreUrl, runtime_env::RuntimeEnv,
    },
    prelude::{SessionConfig, SessionContext},
};
use datasource::table_factory::LakeSoulTableProviderFactory;
use lakesoul_io::{
    config::LakeSoulIOConfigBuilder,
    object_store::{register_hdfs_object_store, register_s3_object_store},
};
use object_store::local::LocalFileSystem;
use rootcause::{Report, bail};
use url::Url;

use crate::planner::LakeSoulQueryPlanner;

// re export
pub use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

pub mod catalog;
pub mod cli;
pub mod datasource;
pub mod lakesoul_table;
pub mod planner;
pub mod serialize;
pub mod tpch;

#[cfg(feature = "adbc")]
#[expect(dead_code)]
mod adbc;

#[cfg(test)]
mod tests;

type Result<T, E = Report> = std::result::Result<T, E>;

pub fn create_lakesoul_session_ctx(
    meta_client: MetaDataClientRef,
    args: &cli::CoreArgs,
) -> Result<Arc<SessionContext>> {
    let mut session_config = SessionConfig::from_env()?
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(false)
        .with_batch_size(8192)
        .with_default_catalog_and_schema("LAKESOUL".to_string(), "default".to_string());
    session_config.options_mut().sql_parser.dialect = Dialect::PostgreSQL;
    session_config
        .options_mut()
        .sql_parser
        .map_string_types_to_utf8view = false;
    session_config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
    session_config.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
    session_config
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = true;
    session_config.options_mut().execution.target_partitions = 1;
    session_config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    session_config
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = true;
    // TODO use this
    session_config
        .options_mut()
        .execution
        .listing_table_factory_infer_partitions = false;

    let planner = LakeSoulQueryPlanner::new_ref();

    let mut state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(Arc::new(RuntimeEnv::default()))
        .with_default_features()
        .with_query_planner(planner)
        .build();
    state.table_factories_mut().insert(
        "LAKESOUL".to_string(),
        Arc::new(LakeSoulTableProviderFactory::new(
            meta_client.clone(),
            args.warehouse_prefix.clone(),
        )),
    );
    let ctx = Arc::new(SessionContext::new_with_state(state));

    let catalog = Arc::new(LakeSoulCatalog::new(meta_client.clone(), ctx.clone()));

    if let Some(warehouse_prefix) = &args.warehouse_prefix {
        debug!("warehouse_prefix: {:?}", warehouse_prefix);
        // FIXME: s3 related args will ignore local file system
        unsafe {
            env::set_var("LAKESOUL_WAREHOUSE_PREFIX", warehouse_prefix);
        }
        let url = Url::parse(warehouse_prefix);
        match url {
            Ok(url) => match url.scheme() {
                "s3" | "s3a" => {
                    if let Some(s3_secret_key) = &args.s3_secret_key {
                        unsafe {
                            env::set_var("AWS_SECRET_ACCESS_KEY", s3_secret_key);
                        }
                    }
                    if let Some(s3_access_key) = &args.s3_access_key {
                        unsafe {
                            env::set_var("AWS_ACCESS_KEY_ID", s3_access_key);
                        }
                    }
                    if let Some(endpoint) = &args.endpoint {
                        unsafe {
                            env::set_var("AWS_ENDPOINT", endpoint);
                        }
                    }

                    if ctx
                        .runtime_env()
                        .object_store(ObjectStoreUrl::parse(
                            &url[..url::Position::BeforePath],
                        )?)
                        .is_ok()
                    {
                        bail!("Object store already registered")
                    }

                    let config = LakeSoulIOConfigBuilder::new_with_object_store_options(
                        args.s3_options(),
                    )
                    .build();
                    // need cache?
                    register_s3_object_store(&url, &config, &ctx.runtime_env())?;
                }
                "hdfs" => {
                    if url.has_host() {
                        if ctx
                            .runtime_env()
                            .object_store(ObjectStoreUrl::parse(
                                &url[..url::Position::BeforePath],
                            )?)
                            .is_ok()
                        {
                            bail!("Object store already registered")
                        }
                        let config =
                            LakeSoulIOConfigBuilder::new_with_object_store_options(
                                args.s3_options(),
                            )
                            .build();
                        register_hdfs_object_store(
                            &url,
                            &url[url::Position::BeforeHost..url::Position::BeforePath],
                            &config,
                            &ctx.runtime_env(),
                        )?;
                    } else {
                        // defaultFS should have been registered with hdfs,
                        // and we convert hdfs://user/hadoop/file to
                        // hdfs://defaultFS/user/hadoop/file
                        todo!()
                    }
                }
                "file" => {
                    ctx.runtime_env()
                        .register_object_store(&url, Arc::new(LocalFileSystem::new()));
                }
                _ => {
                    bail!("Invalid scheme of warehouse prefix")
                }
            },
            Err(_) => {
                bail!("Invalid warehouse prefix")
            }
        }
    } else {
        ctx.runtime_env().register_object_store(
            &Url::parse("file://").unwrap(),
            Arc::new(LocalFileSystem::new()),
        );
    }

    ctx.state()
        .catalog_list()
        .register_catalog("LAKESOUL".to_string(), catalog.clone());

    info!("catalogs: {:?}", ctx.catalog_names());

    Ok(ctx)
}
