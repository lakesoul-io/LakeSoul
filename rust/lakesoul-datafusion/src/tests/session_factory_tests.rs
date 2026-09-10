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
