// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! ACL resolution against the real metadata database.
//!
//! The pg-wire application layer and the S3 proxy call `AclView`; these cases
//! pin the same rules the SQL RLS functions implement.

use lakesoul_metadata::acl::Privilege;
use lakesoul_metadata::{MetaDataClient, QueryType};
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

async fn set_domain_admin(
    client: &MetaDataClient,
    domain: &str,
    user: &str,
    admin: bool,
) {
    let pooled = client.pooled_client();
    let locked = pooled.lock().await;
    let connection = locked.get(QueryType::RW).await.unwrap();
    if admin {
        connection
            .execute(
                "insert into domain_admin(domain, user_name) values ($1, $2)
                 on conflict do nothing",
                &[&domain, &user],
            )
            .await
            .unwrap();
    } else {
        connection
            .execute(
                "delete from domain_admin where domain = $1 and user_name = $2",
                &[&domain, &user],
            )
            .await
            .unwrap();
    }
}

async fn set_namespace_acl(
    client: &MetaDataClient,
    namespace: &str,
    grantee_domain: &str,
    grantee: &str,
    privilege: Privilege,
) {
    let pooled = client.pooled_client();
    let locked = pooled.lock().await;
    let connection = locked.get(QueryType::RW).await.unwrap();
    connection
        .execute(
            "insert into namespace_acl
               (namespace, grantee_domain, grantee_kind, grantee, privilege, granted_by)
             values ($1, $2, 'user', $3, $4, 'alice')
             on conflict (namespace, grantee_domain, grantee_kind, grantee)
             do update set privilege = excluded.privilege, updated_at = now()",
            &[&namespace, &grantee_domain, &grantee, &privilege.as_str()],
        )
        .await
        .unwrap();
}

async fn clear_namespace_acl(client: &MetaDataClient, namespace: &str) {
    let pooled = client.pooled_client();
    let locked = pooled.lock().await;
    let connection = locked.get(QueryType::RW).await.unwrap();
    connection
        .execute(
            "delete from namespace_acl where namespace = $1",
            &[&namespace],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn effective_table_privilege_is_capped_by_namespace_acl() {
    let client = MetaDataClient::from_env().await.unwrap();
    let namespace = unique("ns_cap");
    client
        .create_namespace(Namespace {
            namespace: namespace.clone(),
            properties: "{}".to_string(),
            comment: "acl namespace cap test".to_string(),
            domain: "team_a".to_string(),
            creator: "alice".to_string(),
        })
        .await
        .unwrap();
    let name = unique("ns_cap_table");
    let table_id = create_table_in(&client, &namespace, &name, "team_a", "alice").await;
    client
        .grant_table_privilege(&table_id, "team_a", "bob", Privilege::Write, "alice")
        .await
        .unwrap();

    let domains_a = vec!["team_a".to_string()];
    let bob = || client.load_acl_view("bob", &domains_a);
    assert_eq!(
        bob()
            .await
            .unwrap()
            .effective_table_privilege(&namespace, &name),
        Privilege::Write
    );

    // A same-domain namespace `none` hides the table even with a table rw grant.
    set_namespace_acl(&client, &namespace, "team_a", "bob", Privilege::None).await;
    assert_eq!(
        bob()
            .await
            .unwrap()
            .effective_table_privilege(&namespace, &name),
        Privilege::None
    );

    // A namespace `ro` clamps table rw down to read.
    set_namespace_acl(&client, &namespace, "team_a", "bob", Privilege::Read).await;
    assert_eq!(
        bob()
            .await
            .unwrap()
            .effective_table_privilege(&namespace, &name),
        Privilege::Read
    );

    // A cross-domain table grant still makes the namespace visible read-only.
    client
        .grant_table_privilege(&table_id, "team_b", "carol", Privilege::Read, "alice")
        .await
        .unwrap();
    let carol = client
        .load_acl_view("carol", &["team_b".to_string()])
        .await
        .unwrap();
    assert_eq!(
        carol.effective_table_privilege(&namespace, &name),
        Privilege::Read
    );
    assert_eq!(carol.namespace_privilege(&namespace), Privilege::Read);

    clear_namespace_acl(&client, &namespace).await;
    client.drop_table(&name, &namespace).await.unwrap();
    client
        .delete_namespace_by_namespace(&namespace)
        .await
        .unwrap();
}

#[tokio::test]
async fn current_domain_guc_only_narrows_role_memberships() {
    let client = MetaDataClient::from_env().await.unwrap();
    let role = format!("acl_guc_{}", uuid::Uuid::new_v4().simple());
    let pool = client.pooled_client();
    {
        let locked = pool.lock().await;
        let conn = locked.get(QueryType::RW).await.unwrap();
        // `meta_init_for_local_test.sh` installs meta_rbac_init.sql, so the
        // helper is always present in CI and local test databases.
        conn.execute(&format!("create role \"{role}\""), &[])
            .await
            .unwrap();
        conn.batch_execute(
            "do $$ begin
                if not exists (select 1 from pg_roles where rolname = 'team_a') then
                    create role team_a;
                end if;
             end $$;",
        )
        .await
        .unwrap();
        conn.execute(&format!("grant team_a to \"{role}\""), &[])
            .await
            .unwrap();

        conn.execute(
            "select set_config('lakesoul.current_domain', 'team_a', false)",
            &[],
        )
        .await
        .unwrap();
        let domains: Vec<String> = conn
            .query_one("select lakesoul_current_domains($1)", &[&role])
            .await
            .unwrap()
            .get(0);
        assert_eq!(domains, vec!["team_a".to_string()]);

        conn.execute(
            "select set_config('lakesoul.current_domain', 'team_b', false)",
            &[],
        )
        .await
        .unwrap();
        let domains: Vec<String> = conn
            .query_one("select lakesoul_current_domains($1)", &[&role])
            .await
            .unwrap()
            .get(0);
        assert!(
            domains.is_empty(),
            "an unselected domain must fail closed, got {domains:?}"
        );

        conn.batch_execute("reset lakesoul.current_domain")
            .await
            .unwrap();
        conn.execute(&format!("revoke team_a from \"{role}\""), &[])
            .await
            .unwrap();
        conn.execute(&format!("drop role \"{role}\""), &[])
            .await
            .unwrap();
    }
}

/// Ownership and space-admin rights are identities in the object's domain.
/// A token for another domain must not inherit them; an explicit cross-domain
/// read grant still applies and keeps the namespace read-only.
#[tokio::test]
async fn owner_and_admin_do_not_escape_the_session_domain() {
    let client = MetaDataClient::from_env().await.unwrap();
    let namespace = unique("session_ns");
    client
        .create_namespace(Namespace {
            namespace: namespace.clone(),
            properties: "{}".to_string(),
            comment: "acl session-domain test".to_string(),
            domain: "team_a".to_string(),
            creator: "alice".to_string(),
        })
        .await
        .unwrap();
    let name = unique("session_table");
    let path = format!("file:///tmp/{namespace}/{name}");
    let table_id = create_table_in(&client, &namespace, &name, "team_a", "alice").await;

    // The owner (alice) connected with a team_b token is not in team_a.
    let alice_b = client
        .load_acl_view("alice", &["team_b".to_string()])
        .await
        .unwrap();
    assert_eq!(alice_b.namespace_privilege(&namespace), Privilege::None);
    assert_eq!(
        alice_b.effective_table_privilege(&namespace, &name),
        Privilege::None
    );
    assert!(!alice_b.visible_namespaces().contains(&namespace));

    // The same holds for a space administrator of team_a.
    set_domain_admin(&client, "team_a", "dave", true).await;
    let dave_b = client
        .load_acl_view("dave", &["team_b".to_string()])
        .await
        .unwrap();
    assert_eq!(dave_b.namespace_privilege(&namespace), Privilege::None);
    assert_eq!(
        dave_b.effective_table_privilege(&namespace, &name),
        Privilege::None
    );
    assert!(!dave_b.visible_namespaces().contains(&namespace));

    let dave_a = client
        .load_acl_view("dave", &["team_a".to_string()])
        .await
        .unwrap();
    assert_eq!(dave_a.namespace_privilege(&namespace), Privilege::Write);
    assert_eq!(
        dave_a.effective_table_privilege(&namespace, &name),
        Privilege::Write
    );

    // An explicit cross-domain read grant applies to the owner and to the
    // administrator, and makes the namespace visible read-only.
    client
        .grant_table_privilege(&table_id, "team_b", "alice", Privilege::Read, "alice")
        .await
        .unwrap();
    client
        .grant_table_privilege(&table_id, "team_b", "dave", Privilege::Read, "alice")
        .await
        .unwrap();
    for user in ["alice", "dave"] {
        let view = client
            .load_acl_view(user, &["team_b".to_string()])
            .await
            .unwrap();
        assert_eq!(
            view.effective_table_privilege(&namespace, &name),
            Privilege::Read,
            "{user} must get the explicit cross-domain read grant"
        );
        assert_eq!(
            view.namespace_privilege(&namespace),
            Privilege::Read,
            "{user} must see the namespace read-only through the table grant"
        );
        assert_eq!(
            client
                .table_privilege_by_path(&path, user, "team_b")
                .await
                .unwrap(),
            Some(Privilege::Read),
            "{user} must get the same result from the path resolver"
        );
    }
    assert_eq!(
        client
            .table_privilege_by_path(&path, "dave", "team_a")
            .await
            .unwrap(),
        Some(Privilege::Write),
        "the administrator keeps write in the domain they administer"
    );

    set_domain_admin(&client, "team_a", "dave", false).await;
    client.drop_table(&name, &namespace).await.unwrap();
    client
        .delete_namespace_by_namespace(&namespace)
        .await
        .unwrap();
}
