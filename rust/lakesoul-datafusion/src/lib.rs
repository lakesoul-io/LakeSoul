// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The LakeSoul DataFusion module.

#[macro_use]
extern crate tracing;

use std::sync::Arc;

use datafusion::config::Dialect;
use datafusion::prelude::{SessionConfig, SessionContext};
use rootcause::Report;

use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};

pub mod catalog;
pub mod cli;
pub mod datasource;
pub mod lakesoul_table;
pub mod planner;
pub mod session;
pub mod tpch;
pub mod udf;
pub mod vector_index;

// re export
pub use datafusion::*;
pub use lakesoul_common::ser;
pub use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

pub fn create_lakesoul_session_ctx(
    meta_client: MetaDataClientRef,
    args: &cli::CoreArgs,
) -> Result<Arc<SessionContext>> {
    create_lakesoul_session_ctx_with_config(
        meta_client,
        args,
        create_lakesoul_session_config()?,
    )
}

/// Build the default [`SessionConfig`] used by LakeSoul sessions.
pub fn create_lakesoul_session_config() -> Result<SessionConfig> {
    let mut session_config = SessionConfig::from_env()?
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(false)
        .with_batch_size(8192)
        .with_default_catalog_and_schema(
            session::DEFAULT_CATALOG.to_string(),
            session::DEFAULT_SCHEMA.to_string(),
        );
    session_config.options_mut().sql_parser.dialect = Dialect::PostgreSQL;
    session_config
        .options_mut()
        .sql_parser
        .map_string_types_to_utf8view = false; // TODO(jiax): check this
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
    // TODO use this
    session_config
        .options_mut()
        .execution
        .listing_table_factory_infer_partitions = false;
    Ok(session_config)
}

/// Create a LakeSoul session context with a caller-supplied session
/// configuration (e.g. with vector-search extension options attached).
pub fn create_lakesoul_session_ctx_with_config(
    meta_client: MetaDataClientRef,
    args: &cli::CoreArgs,
    session_config: SessionConfig,
) -> Result<Arc<SessionContext>> {
    LakeSoulSessionFactory::new(meta_client, args)?
        .with_session_template(session_config)
        .create_session(&LakeSoulSessionOptions::default())
}

pub fn create_lakesoul_session_ctx_with_catalog_decorator<F>(
    meta_client: MetaDataClientRef,
    args: &cli::CoreArgs,
    decorate_catalog: F,
) -> Result<Arc<SessionContext>>
where
    F: Fn(Arc<catalog::LakeSoulCatalog>) -> Arc<dyn datafusion::catalog::CatalogProvider>
        + Send
        + Sync
        + 'static,
{
    LakeSoulSessionFactory::new(meta_client, args)?
        .with_catalog_decorator(decorate_catalog)
        .create_session(&LakeSoulSessionOptions::default())
}

type Result<T, E = Report> = std::result::Result<T, E>;

#[cfg(feature = "adbc")]
#[expect(dead_code)]
mod adbc;

#[cfg(test)]
mod tests;
