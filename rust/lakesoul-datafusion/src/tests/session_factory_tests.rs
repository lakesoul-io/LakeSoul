// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use tokio::runtime::Runtime;

use crate::cli::CoreArgs;
use crate::distributed::{DistributedOptions, WorkerDiscovery};
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};

fn new_factory(client: crate::MetaDataClientRef) -> LakeSoulSessionFactory {
    LakeSoulSessionFactory::new(client, &CoreArgs::default()).unwrap()
}

/// Sessions built by one factory have separate runtimes and configs.
#[test]
fn test_sessions_are_independent() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let client = Arc::new(crate::MetaDataClient::from_env().await.unwrap());
        let factory = new_factory(client);

        let ctx_a = factory
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();
        let ctx_b = factory
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();

        // Separate RuntimeEnv per session; both register the local object store.
        let runtime_a = ctx_a.runtime_env();
        let runtime_b = ctx_b.runtime_env();
        assert!(!Arc::ptr_eq(&runtime_a, &runtime_b));
        for runtime in [&runtime_a, &runtime_b] {
            runtime
                .object_store(ObjectStoreUrl::parse("file://").unwrap())
                .unwrap();
        }

        let ctx_b_time_zone = ctx_b.state().config_options().execution.time_zone.clone();
        let state_a = ctx_a.state_ref();
        state_a
            .write()
            .config_mut()
            .options_mut()
            .execution
            .time_zone = Some("UTC".to_string());

        assert_eq!(
            ctx_a.state().config_options().execution.time_zone,
            Some("UTC".to_string())
        );
        assert_eq!(
            ctx_b.state().config_options().execution.time_zone,
            ctx_b_time_zone // NONE
        );
    });
}

#[test]
fn test_session_is_released_when_last_owner_is_dropped() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let client = Arc::new(crate::MetaDataClient::from_env().await.unwrap());
        let factory = new_factory(client);
        let ctx = factory
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();
        let weak_ctx = Arc::downgrade(&ctx);

        drop(ctx);

        assert!(
            weak_ctx.upgrade().is_none(),
            "catalog must not retain its owning SessionContext"
        );
    });
}

/// Generic initial schema and time-zone options are applied while the catalog
/// and information schema are wired into the new context.
#[test]
fn test_session_initial_values() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let client = Arc::new(crate::MetaDataClient::from_env().await.unwrap());
        let factory = new_factory(client);

        let options = LakeSoulSessionOptions {
            default_schema: "tenant_a".to_string(),
            time_zone: Some("UTC".to_string()),
        };
        let ctx = factory.create_session(&options).unwrap();

        let state = ctx.state();
        let options = state.config_options();
        assert_eq!(options.catalog.default_catalog, "lakesoul");
        assert_eq!(options.catalog.default_schema, "tenant_a");
        assert_eq!(options.execution.time_zone, Some("UTC".to_string()));

        assert_eq!(ctx.catalog_names(), vec!["lakesoul"]);
        let catalog = ctx.state().catalog_list().catalog("lakesoul").unwrap();
        assert!(catalog.schema("default").is_some());

        // information_schema is resolved at plan time from the session config.
        let df = ctx
            .sql("SELECT count(*) FROM information_schema.tables")
            .await
            .unwrap();
        df.collect().await.unwrap();
    });
}

/// `target_partitions` is parameterized: a caller-supplied session template
/// keeps its value, an explicit factory value wins, and distributed sessions
/// are forced above one partition (otherwise the physical planner emits no
/// hash `RepartitionExec` stage boundary and nothing is distributed).
#[test]
fn test_target_partitions_is_parameterized() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let client = Arc::new(crate::MetaDataClient::from_env().await.unwrap());

        let template = crate::create_lakesoul_session_config()
            .unwrap()
            .with_target_partitions(7);
        let factory = new_factory(client.clone()).with_session_template(template);
        let ctx = factory
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();
        assert_eq!(ctx.state().config_options().execution.target_partitions, 7);

        let factory = new_factory(client.clone()).with_target_partitions(3);
        let ctx = factory
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();
        assert_eq!(ctx.state().config_options().execution.target_partitions, 3);

        let factory = new_factory(client).with_distributed(DistributedOptions {
            discovery: WorkerDiscovery::Static(Vec::new()),
            fallback_to_local: true,
            target_partitions: 1,
            bytes_per_partition: None,
        });
        let ctx = factory
            .create_session(&LakeSoulSessionOptions::default())
            .unwrap();
        assert_eq!(ctx.state().config_options().execution.target_partitions, 2);
    });
}

#[test]
fn test_memory_limit_parser() {
    use crate::session::parse_memory_limit;

    assert_eq!(parse_memory_limit("0").unwrap(), 0);
    assert_eq!(parse_memory_limit("512Mi").unwrap(), 512 * 1024 * 1024);
    assert_eq!(parse_memory_limit("1G").unwrap(), 1_000_000_000);
    for (suffix, multiplier) in [
        ("K", 1000),
        ("m", 1_000_000),
        ("g", 1_000_000_000),
        ("kI", 1024),
        ("MI", 1024 * 1024),
        ("gi", 1024 * 1024 * 1024),
    ] {
        assert_eq!(
            parse_memory_limit(&format!("1{suffix}")).unwrap(),
            multiplier
        );
    }
    assert_eq!(
        parse_memory_limit(&usize::MAX.to_string()).unwrap(),
        usize::MAX
    );
    for invalid in [
        "garbage",
        "",
        "-1",
        "1.5Gi",
        "1GB",
        "1 Mi",
        "18446744073709551616",
        &format!("{}Gi", usize::MAX),
    ] {
        assert!(parse_memory_limit(invalid).is_err(), "{invalid}");
    }
}

/// Exercise the real factory/planner/execution path, not just reservations.
/// The budget is deliberately smaller than one generated batch, forcing the
/// sort to exhaust resources even when its spill implementation is enabled.
#[test]
fn test_session_execution_memory_budget() {
    use datafusion::common::DataFusionError;
    use datafusion::execution::memory_pool::MemoryConsumer;
    use metrics_exporter_prometheus::PrometheusBuilder;

    use crate::session::{ExecutionMemoryArgs, ExecutionMemoryPool};

    fn metric(text: &str, name: &str) -> f64 {
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .unwrap_or_else(|| panic!("missing metric {name} in {text}"))
            .parse()
            .unwrap()
    }

    let rt = Runtime::new().unwrap();
    for kind in [ExecutionMemoryPool::Fair, ExecutionMemoryPool::Greedy] {
        for limit in [0, 1024] {
            let recorder = PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::with_local_recorder(&recorder, || {
                rt.block_on(async {
                    let client =
                        Arc::new(crate::MetaDataClient::from_env().await.unwrap());
                    let pool = ExecutionMemoryArgs {
                        memory_limit: limit,
                        memory_pool: kind,
                        vortex_decode_budget: None,
                    }
                    .create_pool();
                    let factory = LakeSoulSessionFactory::new_with_memory_pool(
                        client,
                        &CoreArgs::default(),
                        Arc::clone(&pool),
                    )
                    .unwrap();
                    let ctx = factory
                        .create_session(&LakeSoulSessionOptions::default())
                        .unwrap();
                    let other = factory
                        .create_session(&LakeSoulSessionOptions::default())
                        .unwrap();
                    assert!(Arc::ptr_eq(
                        &ctx.runtime_env().memory_pool,
                        &other.runtime_env().memory_pool
                    ));
                    // A second connection cannot obtain a separate copy of the budget.
                    if limit != 0 {
                        let first = MemoryConsumer::new("connection-a")
                            .register(&ctx.runtime_env().memory_pool);
                        let second = MemoryConsumer::new("connection-b")
                            .register(&other.runtime_env().memory_pool);
                        first.try_grow(600).unwrap();
                        assert_eq!(
                            metric(
                                &handle.render(),
                                "lakesoul_memory_pool_reserved_bytes"
                            ),
                            600.0
                        );
                        assert!(second.try_grow(600).is_err());
                    }
                    let result = ctx
                        .sql(
                            "SELECT value FROM generate_series(1, 10000) AS g(value) \
                             ORDER BY value DESC",
                        )
                        .await
                        .unwrap()
                        .collect()
                        .await;
                    if limit == 0 {
                        let rows: usize =
                            result.unwrap().iter().map(|batch| batch.num_rows()).sum();
                        assert_eq!(rows, 10000);
                    } else {
                        let error = result.expect_err(
                            "a batch cannot fit in the 1 KiB execution budget",
                        );
                        assert!(
                            matches!(
                                error.find_root(),
                                DataFusionError::ResourcesExhausted(_)
                            ),
                            "{error}"
                        );
                    }
                    assert_eq!(pool.reserved(), 0, "query must release all reservations");
                });
            });
            let text = handle.render();
            assert_eq!(
                metric(&text, "lakesoul_memory_pool_limit_bytes"),
                limit as f64
            );
            assert_eq!(metric(&text, "lakesoul_memory_pool_reserved_bytes"), 0.0);
            if limit != 0 {
                assert!(
                    metric(&text, "lakesoul_memory_reservation_failures_total") > 0.0
                );
            }
        }
    }
}
