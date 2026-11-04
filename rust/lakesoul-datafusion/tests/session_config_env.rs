// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Where a LakeSoul session's batch size comes from.
//!
//! The process environment is global, so this lives in its own test binary
//! with a single test: nothing else here reads the environment while the test
//! changes it.

use std::sync::Arc;

use lakesoul_datafusion::MetaDataClient;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::create_lakesoul_session_config;
use lakesoul_datafusion::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};

const BATCH_SIZE: &str = "DATAFUSION_EXECUTION_BATCH_SIZE";
const PREFER_HASH_JOIN: &str = "DATAFUSION_OPTIMIZER_PREFER_HASH_JOIN";

fn set_env(key: &str, value: Option<&str>) {
    // SAFETY: the only test in this binary, and no runtime is alive while the
    // environment changes, so no other thread reads it concurrently.
    unsafe {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

/// Batch size of the session template, and of a session from the factory the
/// console and the PG coordinator build their sessions with, after running
/// `statement` on it.
fn batch_sizes(statement: Option<&str>) -> (usize, usize) {
    let template = create_lakesoul_session_config().unwrap().batch_size();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let session = runtime.block_on(async {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let ctx = LakeSoulSessionFactory::new(client, &CoreArgs::default())
            .unwrap()
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();
        if let Some(statement) = statement {
            ctx.sql(statement).await.unwrap().collect().await.unwrap();
        }
        ctx.state().config().batch_size()
    });
    (template, session)
}

#[test]
fn batch_size_follows_datafusion_default_environment_and_set() {
    set_env(BATCH_SIZE, None);
    set_env(PREFER_HASH_JOIN, None);
    assert_eq!(batch_sizes(None), (8192, 8192), "DataFusion's default");

    set_env(BATCH_SIZE, Some("2048"));
    assert_eq!(batch_sizes(None), (2048, 2048), "environment override");
    assert_eq!(
        batch_sizes(Some("SET datafusion.execution.batch_size = 1024")).1,
        1024,
        "a session SET wins over the environment"
    );

    // The environment does not override the options LakeSoul sets itself.
    set_env(PREFER_HASH_JOIN, Some("true"));
    let config = create_lakesoul_session_config().unwrap();
    assert!(!config.options().optimizer.prefer_hash_join);
    assert_eq!(config.batch_size(), 2048);

    set_env(BATCH_SIZE, None);
    set_env(PREFER_HASH_JOIN, None);
}
