// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Atomic table ACL mutations used by the pg-wire GRANT/REVOKE statements.

use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::acl::{AclChange, AclGranteeKind, AclRevokeScope, Privilege};
use lakesoul_metadata_proto::entity::TableInfo;

const NAMESPACE: &str = "default";

fn unique(tag: &str) -> String {
    format!("acl_change_{tag}_{}", uuid::Uuid::new_v4().simple())
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

fn grant(
    table_id: &str,
    grantee_domain: &str,
    grantee_kind: AclGranteeKind,
    grantee: &str,
    privilege: Privilege,
) -> AclChange {
    AclChange::GrantTable {
        table_id: table_id.to_string(),
        grantee_domain: grantee_domain.to_string(),
        grantee_kind,
        grantee: grantee.to_string(),
        privilege,
        granted_by: "alice".to_string(),
    }
}

fn revoke(
    table_id: &str,
    grantee_domain: &str,
    grantee_kind: AclGranteeKind,
    grantee: &str,
    scope: AclRevokeScope,
) -> AclChange {
    AclChange::RevokeTable {
        table_id: table_id.to_string(),
        grantee_domain: grantee_domain.to_string(),
        grantee_kind,
        grantee: grantee.to_string(),
        scope,
        granted_by: "alice".to_string(),
    }
}

#[tokio::test]
async fn user_grants_are_additive_and_revoke_collapses() {
    let client = MetaDataClient::from_env().await.unwrap();
    let name = unique("user");
    let table_id = create_table(&client, &name, "team_a", "alice").await;
    let team_a = vec!["team_a".to_string()];

    client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_a",
            AclGranteeKind::User,
            "bob",
            Privilege::Read,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Read
    );

    client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_a",
            AclGranteeKind::User,
            "bob",
            Privilege::Write,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Write
    );

    // GRANT SELECT must not downgrade an existing write grant.
    client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_a",
            AclGranteeKind::User,
            "bob",
            Privilege::Read,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Write
    );

    // Revoking write leaves read.
    client
        .apply_acl_changes(&[revoke(
            &table_id,
            "team_a",
            AclGranteeKind::User,
            "bob",
            AclRevokeScope::Write,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Read
    );

    // Revoking read hides the table from a same-domain user.
    client
        .apply_acl_changes(&[revoke(
            &table_id,
            "team_a",
            AclGranteeKind::User,
            "bob",
            AclRevokeScope::Read,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::None
    );

    // GRANT restores access after an explicit none.
    client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_a",
            AclGranteeKind::User,
            "bob",
            Privilege::Write,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Write
    );

    client.drop_table(&name, NAMESPACE).await.unwrap();
}

#[tokio::test]
async fn domain_grants_and_cross_domain_grants() {
    let client = MetaDataClient::from_env().await.unwrap();
    let name = unique("domain");
    let table_id = create_table(&client, &name, "team_a", "alice").await;
    let team_a = vec!["team_a".to_string()];
    let team_b = vec!["team_b".to_string()];

    // A domain grant gives every user of the table's domain write.
    client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_a",
            AclGranteeKind::Domain,
            "*",
            Privilege::Write,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Write
    );

    // Revoking write from the domain leaves the same-domain read default.
    client
        .apply_acl_changes(&[revoke(
            &table_id,
            "team_a",
            AclGranteeKind::Domain,
            "*",
            AclRevokeScope::Write,
        )])
        .await
        .unwrap();
    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Read
    );

    // Cross-domain grants are read-only.
    let denied = client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_b",
            AclGranteeKind::User,
            "carol",
            Privilege::Write,
        )])
        .await;
    assert!(
        denied.is_err(),
        "a cross-domain write grant must be refused"
    );
    client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_b",
            AclGranteeKind::User,
            "carol",
            Privilege::Read,
        )])
        .await
        .unwrap();
    let carol = client.load_acl_view("carol", &team_b).await.unwrap();
    assert_eq!(
        carol.effective_table_privilege(NAMESPACE, &name),
        Privilege::Read
    );

    // A domain grant cannot target a different domain.
    let denied = client
        .apply_acl_changes(&[grant(
            &table_id,
            "team_b",
            AclGranteeKind::Domain,
            "*",
            Privilege::Read,
        )])
        .await;
    assert!(
        denied.is_err(),
        "a domain grant must belong to the table's domain"
    );

    client.drop_table(&name, NAMESPACE).await.unwrap();
}

#[tokio::test]
async fn a_failed_change_rolls_back_the_whole_statement() {
    let client = MetaDataClient::from_env().await.unwrap();
    let name = unique("atomic");
    let table_id = create_table(&client, &name, "team_a", "alice").await;
    let team_a = vec!["team_a".to_string()];

    let result = client
        .apply_acl_changes(&[
            grant(
                &table_id,
                "team_a",
                AclGranteeKind::User,
                "bob",
                Privilege::Write,
            ),
            grant(
                &table_id,
                "team_b",
                AclGranteeKind::User,
                "carol",
                Privilege::Write,
            ),
        ])
        .await;
    assert!(result.is_err(), "the invalid second change must fail");

    let bob = client.load_acl_view("bob", &team_a).await.unwrap();
    assert_eq!(
        bob.effective_table_privilege(NAMESPACE, &name),
        Privilege::Read,
        "the first change must not survive the failed statement"
    );

    client.drop_table(&name, NAMESPACE).await.unwrap();
}

#[tokio::test]
async fn public_tables_take_no_acl() {
    let client = MetaDataClient::from_env().await.unwrap();
    let name = unique("public");
    let table_id = create_table(&client, &name, "public", "alice").await;

    let result = client
        .apply_acl_changes(&[grant(
            &table_id,
            "public",
            AclGranteeKind::User,
            "bob",
            Privilege::Read,
        )])
        .await;
    assert!(result.is_err(), "public tables must not accept ACL rows");

    client.drop_table(&name, NAMESPACE).await.unwrap();
}
