// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A network partition on the PostgreSQL metadata link.
//!
//! The whole runtime talks to PostgreSQL through a local TCP proxy.  The test
//! pauses a refresh after the target commit (but before the epoch mark), cuts
//! the proxy, and verifies that a retry fails while the metadata is
//! unreachable and, once the link is restored, resolves the pending epoch
//! from the persisted commit plan without publishing the row twice.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lakesoul_ivm::{
    IvmRuntime, IvmRuntimeConfig, IvmTableOptions, RowView, ViewSpec, row_mv_schema_for,
};
use tempfile::tempdir;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;

/// A TCP proxy in front of PostgreSQL that can be cut and restored.
#[derive(Clone)]
struct PartitionProxy {
    shutdown: watch::Sender<bool>,
}

impl PartitionProxy {
    fn partition(&self) {
        let _ = self.shutdown.send(true);
    }

    fn heal(&self) {
        let _ = self.shutdown.send(false);
    }
}

async fn spawn_proxy(target: String) -> (String, PartitionProxy, JoinSet<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        run_proxy(listener, target, receiver).await;
    });
    (format!("{addr}"), PartitionProxy { shutdown }, tasks)
}

async fn run_proxy(
    listener: TcpListener,
    target: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    loop {
        if *shutdown.borrow() {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            if shutdown.changed().await.is_err() {
                return;
            }
            continue;
        }
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((mut client, _)) = accepted else { continue };
                let target = target.clone();
                let mut shutdown = shutdown.clone();
                connections.spawn(async move {
                    let Ok(mut upstream) = TcpStream::connect(&target).await else {
                        return;
                    };
                    tokio::select! {
                        _ = copy_bidirectional(&mut client, &mut upstream) => {}
                        _ = shutdown.changed() => {}
                    }
                });
            }
            _ = shutdown.changed() => {}
        }
        while connections.try_join_next().is_some() {}
    }
}

/// Parse the JDBC test URL into the PostgreSQL host:port and database name.
fn split_pg_url(url: &str) -> (String, String) {
    let url = url.strip_prefix("jdbc:").unwrap_or(url);
    let url = url
        .strip_prefix("postgresql://")
        .expect("the test URL must use the PostgreSQL scheme");
    let (url, _) = url.split_once('?').unwrap_or((url, ""));
    let (authority, database) =
        url.split_once('/').expect("the test URL needs a database");
    let authority = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:5432")
    };
    (authority, database.to_string())
}

async fn rows(runtime: &IvmRuntime, table: &lakesoul_ivm::IvmTable) -> usize {
    table
        .read_current(runtime.client())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

#[test_log::test(tokio::test)]
async fn a_metadata_partition_leaves_a_recoverable_pending_epoch() {
    let real_url =
        std::env::var("LAKESOUL_PG_URL").expect("LAKESOUL_PG_URL must be set for tests");
    let (target, database) = split_pg_url(&real_url);
    let (proxy_addr, proxy, mut proxy_tasks) = spawn_proxy(target).await;
    let proxied_url =
        format!("jdbc:postgresql://{proxy_addr}/{database}?stringtype=unspecified");
    // SAFETY: this integration test binary has a single test, so no other
    // runtime reads the variable while it is overridden.
    unsafe { std::env::set_var("LAKESOUL_PG_URL", &proxied_url) };
    let result = async {
        let runtime = Arc::new(
            IvmRuntime::from_env_with_config(IvmRuntimeConfig::default())
                .await
                .unwrap(),
        );
        runtime.init_schema().await.unwrap();
        let dir = tempdir().unwrap();
        let suffix = uuid::Uuid::new_v4().simple();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
            Field::new("op", DataType::Utf8, false),
        ]));
        let source = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_meta_src_{suffix}"),
                    format!("file://{}", dir.path().join("src").display()),
                    schema.clone(),
                )
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column("op"),
            )
            .await
            .unwrap();
        let mv = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_meta_mv_{suffix}"),
                    format!("file://{}", dir.path().join("mv").display()),
                    row_mv_schema_for(&source.schema, &[]).unwrap(),
                )
                .with_primary_keys(vec!["k".to_string()]),
            )
            .await
            .unwrap();
        let view =
            RowView::new(format!("ivm_meta_row_{suffix}"), source.clone(), mv.clone());
        runtime.register_row_view(&view).await.unwrap();
        let spec: ViewSpec = serde_json::from_value(
            runtime
                .metadata()
                .get_view_spec(&view.view_id)
                .await
                .unwrap()
                .expect("the row view spec must be persisted"),
        )
        .unwrap();
        source
            .append_batch(
                runtime.client(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![1, 2])),
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(StringArray::from(vec!["insert", "insert"])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        runtime.refresh_spec(&spec).await.unwrap().unwrap();
        assert_eq!(rows(runtime.as_ref(), &mv).await, 2);

        source
            .append_batch(
                runtime.client(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![3])),
                        Arc::new(StringArray::from(vec!["c"])),
                        Arc::new(StringArray::from(vec!["insert"])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let marker = dir.path().join("target-committed");
        // SAFETY: the process-wide switches are only read by this test.
        unsafe {
            std::env::set_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER", &marker);
            std::env::set_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT", "1");
        }
        let handle = {
            let runtime = Arc::clone(&runtime);
            let spec = spec.clone();
            tokio::spawn(async move { runtime.refresh_spec(&spec).await })
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while !marker.exists() {
            assert!(
                !handle.is_finished(),
                "the refresh finished before the target-commit hook"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "the refresh never reached the target-commit hook"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Drop the paused coordinator, leaving the target committed but the
        // epoch unmarked.
        handle.abort();
        let _ = handle.await;
        unsafe {
            std::env::remove_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT");
            std::env::remove_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER");
        }
        assert_eq!(
            rows(runtime.as_ref(), &mv).await,
            3,
            "the target commit of the paused attempt must be visible"
        );
        // Cut the metadata link before the retry.
        proxy.partition();

        // While the metadata link is down the retry must fail rather than
        // mark the epoch from stale state.
        let failed = tokio::time::timeout(
            std::time::Duration::from_secs(40),
            runtime.refresh_spec(&spec),
        )
        .await
        .expect("the partitioned retry must not hang");
        assert!(
            failed.is_err(),
            "a retry without metadata must fail: {failed:?}"
        );

        proxy.heal();
        let replayed = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            runtime.refresh_spec(&spec),
        )
        .await
        .expect("the healed retry must not hang")
        .expect("the healed retry must succeed");
        assert!(
            replayed.is_some(),
            "the pending epoch must be resolved after the link returns"
        );
        assert_eq!(
            rows(runtime.as_ref(), &mv).await,
            3,
            "the recovered epoch must not publish the row twice"
        );
        assert!(runtime.refresh_spec(&spec).await.unwrap().is_none());
    }
    .await;
    proxy_tasks.shutdown().await;
    // SAFETY: restoring the value the other test binaries expect.
    unsafe { std::env::set_var("LAKESOUL_PG_URL", real_url) };
    result
}
