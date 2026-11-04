// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Where a LakeSoul session's batch size and target partitions come from.
//!
//! The process environment is global, so this lives in its own test binary
//! with a single test: nothing else here reads the environment while the test
//! changes it.

use std::sync::Arc;

use lakesoul_datafusion::MetaDataClient;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use lakesoul_datafusion::{TARGET_PARTITIONS_ENV, create_lakesoul_session_config};

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

/// `(batch_size, target_partitions)` of the session template, and of a
/// single-node session from the factory the console and the PG coordinator
/// build their sessions with, after running `statement` on it.
fn settings(statement: Option<&str>) -> ((usize, usize), (usize, usize)) {
    let template = create_lakesoul_session_config().unwrap();
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
        let config = ctx.state().config().clone();
        (config.batch_size(), config.target_partitions())
    });
    (
        (template.batch_size(), template.target_partitions()),
        session,
    )
}

#[test]
fn session_settings_follow_defaults_environment_and_set() {
    set_env(BATCH_SIZE, None);
    set_env(PREFER_HASH_JOIN, None);
    set_env(TARGET_PARTITIONS_ENV, None);
    assert_eq!(
        settings(None),
        ((8192, 1), (8192, 1)),
        "DataFusion's batch size, LakeSoul's single partition"
    );

    set_env(BATCH_SIZE, Some("2048"));
    set_env(TARGET_PARTITIONS_ENV, Some("4"));
    assert_eq!(
        settings(None),
        ((2048, 4), (2048, 4)),
        "environment override"
    );
    assert_eq!(
        settings(Some("SET datafusion.execution.batch_size = 1024"))
            .1
            .0,
        1024,
        "a session SET wins over the environment"
    );
    assert_eq!(
        settings(Some("SET datafusion.execution.target_partitions = 2"))
            .1
            .1,
        2,
        "a session SET wins over the environment"
    );

    // The environment does not override the options LakeSoul sets itself.
    set_env(PREFER_HASH_JOIN, Some("true"));
    let config = create_lakesoul_session_config().unwrap();
    assert!(!config.options().optimizer.prefer_hash_join);
    assert_eq!(config.batch_size(), 2048);

    set_env(BATCH_SIZE, None);
    set_env(PREFER_HASH_JOIN, None);
    set_env(TARGET_PARTITIONS_ENV, None);
}
