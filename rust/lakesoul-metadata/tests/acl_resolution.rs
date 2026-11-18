// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! ACL resolution against the real metadata database.
//!
//! The pg-wire application layer and the S3 proxy call `AclView`; these cases
//! pin the same rules the SQL RLS functions implement.

use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::acl::Privilege;
use lakesoul_metadata_proto::entity::{Namespace, TableInfo};

const NAMESPACE: &str = "default";

fn unique(tag: &str) -> String {
    format!("acl_{tag}_{}", uuid::Uuid::new_v4().simple())
}

async fn create_table(
    client: &MetaDataClient,
    name: &str,
    domain: &str,
    creator: &str,
) -> String {
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.to_string(),
            table_namespace: NAMESPACE.to_string(),
            table_path: format!("file:///tmp/{name}"),
            table_schema: "[]".to_string(),
            table_schema_arrow_ipc: Vec::new(),
            table_schema_arrow_ipc_json_hash: String::new(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            domain: domain.to_string(),
            creator: creator.to_string(),
        })
        .await
        .unwrap();
    table_id
}

#[tokio::test]
async fn owner_domain_default_cross_domain_and_public() {
    let client = MetaDataClient::from_env().await.unwrap();
    let name = unique("matrix");
    let table_id = create_table(&client, &name, "team_a", "alice").await;
    let domains_a = vec!["team_a".to_string()];
    let domains_b = vec!["team_b".to_string()];

    let alice = client.load_acl_view("alice", &domains_a).await.unwrap();
    assert_eq!(
        alice.effective_table_privilege(NAMESPACE, &name),
        Privilege::Write
    );
    assert!(alice.table_access(NAMESPACE, &name).unwrap().can_ddl());

    let bob = client.load_acl_view("bob", &domains_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Read
    );
    assert!(!bob.table_access(NAMESPACE, &name).unwrap().can_ddl());

    let carol = client.load_acl_view("carol", &domains_b).await.unwrap();
    assert_eq!(
        carol.effective_table_privilege(NAMESPACE, &name),
        Privilege::None
    );

    // Cross-domain read is visible, write is clamped to read.
    client
        .grant_table_privilege(&table_id, "team_b", "carol", Privilege::Read, "alice")
        .await
        .unwrap();
    let carol = client.load_acl_view("carol", &domains_b).await.unwrap();
    assert_eq!(
        carol.effective_table_privilege(NAMESPACE, &name),
        Privilege::Read
    );
    let denied = client
        .grant_table_privilege(&table_id, "team_b", "carol", Privilege::Write, "alice")
        .await;
    assert!(denied.is_err(), "cross-domain rw must be refused");

    // An explicit none hides a same-domain user.
    client
        .grant_table_privilege(&table_id, "team_a", "bob", Privilege::None, "alice")
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &domains_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::None
    );

    client.drop_table(&name, NAMESPACE).await.unwrap();

    // public tables are open for every domain, including a cross-domain user.
    let public_name = unique("public");
    create_table(&client, &public_name, "public", "alice").await;
    let carol = client.load_acl_view("carol", &domains_b).await.unwrap();
    assert_eq!(
        carol.effective_table_privilege(NAMESPACE, &public_name),
        Privilege::Write
    );
    client.drop_table(&public_name, NAMESPACE).await.unwrap();
}

#[tokio::test]
async fn namespace_visibility_follows_table_grants() {
    let client = MetaDataClient::from_env().await.unwrap();
    let namespace = unique("ns");
    client
        .create_namespace(Namespace {
            namespace: namespace.clone(),
            properties: "{}".to_string(),
            comment: "acl test".to_string(),
            domain: "team_a".to_string(),
            creator: "alice".to_string(),
        })
        .await
        .unwrap();
    let name = unique("ns_table");
    let table_id = create_table_in(&client, &namespace, &name, "team_a", "alice").await;

    let alice = client
        .load_acl_view("alice", &["team_a".to_string()])
        .await
        .unwrap();
    assert_eq!(alice.namespace_privilege(&namespace), Privilege::Write);

    let bob = client
        .load_acl_view("bob", &["team_a".to_string()])
        .await
        .unwrap();
    assert_eq!(bob.namespace_privilege(&namespace), Privilege::Write);
    assert_eq!(
        bob.effective_table_privilege(&namespace, &name),
        Privilege::Read
    );

    let carol = client
        .load_acl_view("carol", &["team_b".to_string()])
        .await
        .unwrap();
    assert_eq!(carol.namespace_privilege(&namespace), Privilege::None);
    client
        .grant_table_privilege(&table_id, "team_b", "carol", Privilege::Read, "alice")
        .await
        .unwrap();
    let carol = client
        .load_acl_view("carol", &["team_b".to_string()])
        .await
        .unwrap();
    assert_eq!(carol.namespace_privilege(&namespace), Privilege::Read);
    assert!(
        carol.visible_namespaces().contains(&namespace),
        "a table grant must make the namespace visible read-only"
    );

    client.drop_table(&name, &namespace).await.unwrap();
    client
        .delete_namespace_by_namespace(&namespace)
        .await
        .unwrap();
}

async fn create_table_in(
    client: &MetaDataClient,
    namespace: &str,
    name: &str,
    domain: &str,
    creator: &str,
) -> String {
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.to_string(),
            table_namespace: namespace.to_string(),
            table_path: format!("file:///tmp/{namespace}/{name}"),
            table_schema: "[]".to_string(),
            table_schema_arrow_ipc: Vec::new(),
            table_schema_arrow_ipc_json_hash: String::new(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            domain: domain.to_string(),
            creator: creator.to_string(),
        })
        .await
        .unwrap();
    table_id
}

#[tokio::test]
async fn path_resolution_matches_table_resolution() {
    let client = MetaDataClient::from_env().await.unwrap();
    let name = unique("path");
    let path = format!("file:///tmp/{name}");
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.clone(),
            table_namespace: NAMESPACE.to_string(),
            table_path: path.clone(),
            table_schema: "[]".to_string(),
            table_schema_arrow_ipc: Vec::new(),
            table_schema_arrow_ipc_json_hash: String::new(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            domain: "team_a".to_string(),
            creator: "alice".to_string(),
        })
        .await
        .unwrap();
    client
        .grant_table_privilege(&table_id, "team_b", "carol", Privilege::Read, "alice")
        .await
        .unwrap();

    assert_eq!(
        client
            .table_privilege_by_path(&path, "carol", "team_b")
            .await
            .unwrap(),
        Some(Privilege::Read)
    );
    assert_eq!(
        client
            .table_privilege_by_path(&path, "bob", "team_a")
            .await
            .unwrap(),
        Some(Privilege::Read)
    );
    assert_eq!(
        client
            .table_privilege_by_path(&path, "alice", "team_a")
            .await
            .unwrap(),
        Some(Privilege::Write)
    );
    assert_eq!(
        client
            .table_privilege_by_path("file:///tmp/not-a-table", "alice", "team_a")
            .await
            .unwrap(),
        None
    );
    client.drop_table(&name, NAMESPACE).await.unwrap();
}
