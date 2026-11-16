// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `MetaDataClient::alter_table_definition`.
//!
//! The publication must only apply to the definition it was validated
//! against:
//!
//! - a matching definition is replaced (schema with its IPC mirror and hash,
//!   and/or properties), and fields the update leaves out are kept;
//! - a stale expectation (a concurrent alteration landed first) is a
//!   `CommitConflict` and writes nothing;
//! - two alterations racing from the same expectation have exactly one
//!   winner, the other gets `CommitConflict`;
//! - a definition change makes a read-modify-write publication planned
//!   against the old definition fail its own identity check;
//! - an absent table is `NotFound`.
//!
//! The tests require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and use unique table names.

use std::sync::Arc;

use lakesoul_metadata::error::Result;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClient, TableDefinitionUpdate};
use lakesoul_metadata_proto::entity::TableInfo;
use tokio::sync::Barrier;

const NAMESPACE: &str = "default";

async fn create_test_table(client: &MetaDataClient, tag: &str) -> Result<TableInfo> {
    let name = format!("alter_definition_{tag}_{}", uuid::Uuid::new_v4().simple());
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.clone(),
            table_namespace: NAMESPACE.to_string(),
            table_path: format!("file:///tmp/{name}"),
            table_schema: r#"{"fields":[]}"#.to_string(),
            properties: r#"{"hashBucketNum":"4"}"#.to_string(),
            partitions: ";id".to_string(),
            domain: "public".to_string(),
            ..Default::default()
        })
        .await?;
    stored(client, &table_id).await
}

async fn stored(client: &MetaDataClient, table_id: &str) -> Result<TableInfo> {
    client
        .get_table_info_by_table_id(table_id)
        .await?
        .ok_or_else(|| LakeSoulMetaDataError::NotFound(table_id.to_string()))
}

fn schema_update(tag: &str) -> TableDefinitionUpdate {
    TableDefinitionUpdate {
        schema: Some((
            format!(r#"{{"fields":[{{"name":"{tag}"}}]}}"#),
            tag.as_bytes().to_vec(),
            format!("hash-{tag}"),
        )),
        properties: None,
    }
}

#[tokio::test]
async fn matching_definition_is_replaced_and_the_rest_kept() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let before = create_test_table(&client, "replace").await?;

    client
        .alter_table_definition(&before, &schema_update("a"))
        .await?;
    let after_schema = stored(&client, &before.table_id).await?;
    assert_eq!(after_schema.table_schema, r#"{"fields":[{"name":"a"}]}"#);
    assert_eq!(after_schema.table_schema_arrow_ipc, b"a".to_vec());
    assert_eq!(after_schema.table_schema_arrow_ipc_json_hash, "hash-a");
    assert_eq!(after_schema.properties, before.properties);
    assert_eq!(after_schema.partitions, before.partitions);
    assert_eq!(after_schema.domain, before.domain);

    let properties =
        serde_json::json!({"hashBucketNum": "4", "vortex_row_block_size": "4096"});
    client
        .alter_table_definition(
            &after_schema,
            &TableDefinitionUpdate {
                schema: None,
                properties: Some(properties.clone()),
            },
        )
        .await?;
    let after_properties = stored(&client, &before.table_id).await?;
    assert_eq!(after_properties.table_schema, after_schema.table_schema);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&after_properties.properties).unwrap(),
        properties
    );

    client.delete_table_by_table_info_cascade(&before).await?;
    Ok(())
}

#[tokio::test]
async fn stale_expectation_conflicts_and_writes_nothing() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let before = create_test_table(&client, "stale").await?;
    client
        .alter_table_definition(&before, &schema_update("first"))
        .await?;
    let current = stored(&client, &before.table_id).await?;

    let error = client
        .alter_table_definition(&before, &schema_update("second"))
        .await
        .expect_err("a stale expectation must conflict");
    assert!(
        matches!(error, LakeSoulMetaDataError::CommitConflict(_)),
        "{error:?}"
    );
    assert_eq!(stored(&client, &before.table_id).await?, current);

    client.delete_table_by_table_info_cascade(&current).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn racing_alterations_have_one_winner() -> Result<()> {
    let setup = MetaDataClient::from_env().await?;
    let before = Arc::new(create_test_table(&setup, "race").await?);
    let barrier = Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for tag in ["left", "right"] {
        let client = MetaDataClient::from_env().await?;
        let before = Arc::clone(&before);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .alter_table_definition(&before, &schema_update(tag))
                .await
        }));
    }
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.expect("alteration task panicked"));
    }
    let winners = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    assert_eq!(winners, 1, "{outcomes:?}");
    assert!(
        outcomes.iter().any(|outcome| matches!(
            outcome,
            Err(LakeSoulMetaDataError::CommitConflict(_))
        )),
        "{outcomes:?}"
    );
    setup.delete_table_by_table_info_cascade(&before).await?;
    Ok(())
}

#[tokio::test]
async fn a_publication_planned_against_the_old_definition_fails() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let before = create_test_table(&client, "publication").await?;
    client
        .alter_table_definition(&before, &schema_update("new"))
        .await?;
    let error = client
        .commit_update(
            &before,
            &[],
            vec![DataFileInfo {
                partition_desc: "-5".to_string(),
                path: "/tmp/alter_definition/file.parquet".to_string(),
                file_op: "add".to_string(),
                size: 1,
                file_exist_cols: "id".to_string(),
                ..Default::default()
            }],
        )
        .await
        .expect_err("a write planned against the old definition must not publish");
    assert!(
        matches!(error, LakeSoulMetaDataError::CommitConflict(_)),
        "{error:?}"
    );
    let current = stored(&client, &before.table_id).await?;
    client.delete_table_by_table_info_cascade(&current).await?;
    Ok(())
}

#[tokio::test]
async fn absent_table_is_not_found() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let before = create_test_table(&client, "absent").await?;
    client.delete_table_by_table_info_cascade(&before).await?;
    let error = client
        .alter_table_definition(&before, &schema_update("late"))
        .await
        .expect_err("an absent table cannot be altered");
    assert!(
        matches!(error, LakeSoulMetaDataError::NotFound(_)),
        "{error:?}"
    );
    Ok(())
}
