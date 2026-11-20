// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Table-level ACL contracts against the real pg-wire server.
//!
//! The same rules are executed by the SQL RLS functions for direct metadata
//! connections; `lakesoul-metadata/tests/acl_resolution.rs` pins the Rust
//! resolver used by the server and the S3 proxy.

pub mod support;

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::Client;
use tokio_postgres::error::SqlState;

use support::{Harness, RBAC_TEAM_A, RBAC_TEAM_B};

fn unique(tag: &str) -> String {
    format!("acl_{tag}_{}", uuid::Uuid::new_v4().simple())
}

async fn connect_as(
    h: &Harness,
    label: &str,
    database: &str,
    user: &str,
    token: &str,
) -> Result<Client> {
    h.ensure_token_user(token).await?;
    let config = h.lake_config_as(database, user, Some(token));
    h.connect_attempt(&config)
        .await?
        .with_context(|| format!("{label}: connect as {user}"))
}

async fn sqlstate(client: &Client, sql: &str) -> Result<String> {
    match client.simple_query(sql).await {
        Ok(_) => bail!("{sql}: expected a server error"),
        Err(error) => Ok(error
            .as_db_error()
            .map(|db| db.code().code().to_string())
            .unwrap_or_else(|| format!("client-side: {error}"))),
    }
}

async fn listed_in_catalog(client: &Client, table: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = $1",
            &[&table],
        )
        .await?;
    Ok(row.get::<_, i64>(0) > 0)
}

async fn wait_listed(client: &Client, table: &str, expected: bool) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if listed_in_catalog(client, table).await? == expected {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "pg_class listing of {table} never became {expected}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_acl_contract() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let bob_token = h.token("bob", RBAC_TEAM_A)?;
            let carol_token = h.token("carol", RBAC_TEAM_B)?;
            let dave_token = h.token("dave", RBAC_TEAM_A)?;
            let namespace = h.lake_namespace.clone();
            let table = unique("owned");
            let alice = connect_as(h, "acl-alice", &namespace, "alice", &alice_token)
                .await?;

            alice
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
                ))
                .await
                .context("acl-create: alice creates the table")?;
            alice
                .batch_execute(&format!("INSERT INTO {table} VALUES (1, 10), (2, 20)"))
                .await
                .context("acl-create: alice fills the table")?;

            // creator is the connecting user, not the cluster's metadata user.
            let info = h
                .meta()
                .get_table_info_by_table_name(&table, &namespace)
                .await?
                .context("acl-create: table metadata missing")?;
            ensure!(
                info.creator == "alice",
                "creator is {:?}, expected alice",
                info.creator
            );
            ensure!(
                info.domain == RBAC_TEAM_A,
                "table domain is {:?}, expected {RBAC_TEAM_A}",
                info.domain
            );

            // Same domain, no grant: the other user is read-only by default.
            let bob = connect_as(h, "acl-bob", &namespace, "bob", &bob_token).await?;
            let count: i64 = bob
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await
                .context("acl-default-read: same-domain SELECT")?
                .get(0);
            ensure!(count == 2, "bob saw {count} rows, expected 2");
            ensure!(
                sqlstate(&bob, &format!("INSERT INTO {table} VALUES (3, 30)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "same-domain default must be read-only"
            );
            ensure!(
                listed_in_catalog(&bob, &table).await?,
                "read-only table must be listed"
            );

            // Another domain: invisible without a grant.
            let carol = connect_as(h, "acl-carol", &namespace, "carol", &carol_token).await?;
            ensure!(
                sqlstate(&carol, &format!("INSERT INTO {table} VALUES (9, 90)")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "cross-domain write without a grant must look missing"
            );
            ensure!(
                carol.simple_query(&format!("SELECT * FROM {table}")).await.is_err(),
                "cross-domain SELECT without a grant must fail"
            );
            ensure!(
                !listed_in_catalog(&carol, &table).await?,
                "cross-domain table without a grant must not be listed"
            );

            // Cross-domain ro: visible, readable, never writable.
            h.meta()
                .grant_table_privilege(
                    &info.table_id,
                    RBAC_TEAM_B,
                    "carol",
                    lakesoul_metadata::acl::Privilege::Read,
                    "alice",
                )
                .await?;
            let carol = connect_as(h, "acl-carol-granted", &namespace, "carol", &carol_token).await?;
            let count: i64 = carol
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await
                .context("acl-cross-read: granted SELECT")?
                .get(0);
            ensure!(count == 2, "carol saw {count} rows, expected 2");
            wait_listed(&carol, &table, true).await?;
            ensure!(
                sqlstate(&carol, &format!("INSERT INTO {table} VALUES (4, 40)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "cross-domain read grant must not permit writes"
            );
            ensure!(
                h.meta()
                    .grant_table_privilege(
                        &info.table_id,
                        RBAC_TEAM_B,
                        "carol",
                        lakesoul_metadata::acl::Privilege::Write,
                        "alice",
                    )
                    .await
                    .is_err(),
                "cross-domain rw must be rejected by the management API"
            );

            // Same-domain rw grant permits writes.
            h.meta()
                .grant_table_privilege(
                    &info.table_id,
                    RBAC_TEAM_A,
                    "bob",
                    lakesoul_metadata::acl::Privilege::Write,
                    "alice",
                )
                .await?;
            let bob = connect_as(h, "acl-bob-rw", &namespace, "bob", &bob_token).await?;
            bob.batch_execute(&format!("INSERT INTO {table} VALUES (3, 30)"))
                .await
                .context("acl-rw: granted INSERT")?;

            // An explicit none hides the table from a same-domain user.
            h.meta()
                .grant_table_privilege(
                    &info.table_id,
                    RBAC_TEAM_A,
                    "bob",
                    lakesoul_metadata::acl::Privilege::None,
                    "alice",
                )
                .await?;
            let bob = connect_as(h, "acl-bob-none", &namespace, "bob", &bob_token).await?;
            ensure!(
                sqlstate(&bob, &format!("INSERT INTO {table} VALUES (9, 90)")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "an explicit none must look missing for writes"
            );
            wait_listed(&bob, &table, false).await?;

            // A domain admin sees and may write every table in its domain.
            let admin = h.connect_reference().await?;
            admin
                .execute(
                    "INSERT INTO public.domain_admin(domain, user_name) VALUES ($1, $2)
                     ON CONFLICT DO NOTHING",
                    &[&RBAC_TEAM_A, &"dave"],
                )
                .await
                .context("acl-admin: insert domain_admin")?;
            let dave = connect_as(h, "acl-dave", &namespace, "dave", &dave_token).await?;
            dave.batch_execute(&format!("INSERT INTO {table} VALUES (4, 40)"))
                .await
                .context("acl-admin: INSERT as space admin")?;
            dave.batch_execute(&format!("ALTER TABLE {table} ADD COLUMN note TEXT"))
                .await
                .context("acl-admin: ALTER as space admin")?;

            // public tables are open regardless of domain.
            let public_table = unique("public");
            h.create_table_out_of_band(&public_table, "public", "alice")
                .await?;
            let count: i64 = carol
                .query_one(&format!("SELECT count(*) FROM {public_table}"), &[])
                .await
                .context("acl-public: cross-domain SELECT on a public table")?
                .get(0);
            ensure!(count == 0, "public table count {count}, expected 0");
            carol
                .batch_execute(&format!(
                    "INSERT INTO {public_table} (id, val, label) VALUES (99, 99, 'public')"
                ))
                .await
                .context("acl-public: cross-domain INSERT on a public table")?;

            // The owner may drop the private table; the test table is dropped
            // here rather than by teardown so the public table stays for the
            // harness cleanup.
            let alice = connect_as(h, "acl-alice-drop", &namespace, "alice", &alice_token).await?;
            alice
                .batch_execute(&format!("DROP TABLE {table}"))
                .await
                .context("acl-owner-drop")?;
            admin
                .execute(
                    "DELETE FROM public.domain_admin
                     WHERE domain = $1 AND user_name = 'dave'",
                    &[&RBAC_TEAM_A],
                )
                .await
                .context("acl-admin: remove domain_admin fixture")?;
            Ok(())
        })
    })
    .await
}

/// A cross-domain table grant makes the otherwise invisible private namespace
/// reachable read-only, and only the granted table is listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_namespace_is_visible_after_a_table_grant() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let carol_token = h.token("carol", RBAC_TEAM_B)?;
            let namespace = h.namespace_of(RBAC_TEAM_A)?.to_string();
            let table = unique("private");

            let alice =
                connect_as(h, "acl-private-alice", &namespace, "alice", &alice_token)
                    .await?;
            alice
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
                ))
                .await?;
            alice
                .batch_execute(&format!("INSERT INTO {table} VALUES (1, 10)"))
                .await?;
            let info = h
                .meta()
                .get_table_info_by_table_name(&table, &namespace)
                .await?
                .context("private table metadata missing")?;

            let carol_base = connect_as(
                h,
                "acl-private-carol-base",
                &h.lake_namespace,
                "carol",
                &carol_token,
            )
            .await?;
            let listed = carol_base
                .query_one(
                    "SELECT count(*) FROM pg_catalog.pg_database WHERE datname = $1",
                    &[&namespace],
                )
                .await?
                .get::<_, i64>(0);
            ensure!(listed == 0, "ungranted private namespace is listed");

            h.meta()
                .grant_table_privilege(
                    &info.table_id,
                    RBAC_TEAM_B,
                    "carol",
                    lakesoul_metadata::acl::Privilege::Read,
                    "alice",
                )
                .await?;

            let carol_base = connect_as(
                h,
                "acl-private-carol-granted",
                &h.lake_namespace,
                "carol",
                &carol_token,
            )
            .await?;
            let listed = carol_base
                .query_one(
                    "SELECT count(*) FROM pg_catalog.pg_database WHERE datname = $1",
                    &[&namespace],
                )
                .await?
                .get::<_, i64>(0);
            ensure!(
                listed == 1,
                "a granted private namespace must be listed read-only"
            );

            let carol =
                connect_as(h, "acl-private-carol", &namespace, "carol", &carol_token)
                    .await?;
            let count: i64 = carol
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await?
                .get(0);
            ensure!(count == 1, "carol saw {count} rows, expected 1");
            ensure!(
                sqlstate(&carol, &format!("INSERT INTO {table} VALUES (2, 20)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "a private-namespace grant must be read-only"
            );

            alice.batch_execute(&format!("DROP TABLE {table}")).await?;
            Ok(())
        })
    })
    .await
}

/// Owner and space-admin rights belong to the object's domain. A token for
/// another domain must not inherit them, even though writes would also be
/// stopped by the cluster write-domain check; reads and catalog visibility
/// must be hidden as well. An explicit cross-domain read grant still applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_domain_owner_and_admin_are_invisible() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_a_token = h.token("alice", RBAC_TEAM_A)?;
            let alice_b_token = h.token("alice", RBAC_TEAM_B)?;
            let admin = unique("cross_admin");
            let admin_b_token = h.token(&admin, RBAC_TEAM_B)?;
            let namespace = h.lake_namespace.clone();
            let table = unique("cross_owned");

            let alice_a =
                connect_as(h, "cross-owner-a", &namespace, "alice", &alice_a_token)
                    .await?;
            alice_a
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY)"
                ))
                .await
                .context("cross-domain: alice creates a team-A table")?;
            let info = h
                .meta()
                .get_table_info_by_table_name(&table, &namespace)
                .await?
                .context("cross-domain: table metadata missing")?;
            ensure!(
                info.domain == RBAC_TEAM_A,
                "table domain is {:?}, expected {RBAC_TEAM_A}",
                info.domain
            );

            // The owner, connected with a team-B token, cannot see the table.
            let alice_b =
                connect_as(h, "cross-owner-b", &namespace, "alice", &alice_b_token)
                    .await?;
            ensure!(
                !listed_in_catalog(&alice_b, &table).await?,
                "a cross-domain owner must not list the table"
            );
            ensure!(
                sqlstate(&alice_b, &format!("SELECT * FROM {table}")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "a cross-domain owner must see the table as missing"
            );

            // A team-A space administrator with a team-B token is equally
            // outside the table's domain.
            let reference = h.connect_reference().await?;
            reference
                .execute(
                    "INSERT INTO public.domain_admin(domain, user_name)
                     VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&RBAC_TEAM_A, &admin],
                )
                .await
                .context("cross-domain: make the fixture a team-A admin")?;
            let admin_b =
                connect_as(h, "cross-admin-b", &namespace, &admin, &admin_b_token)
                    .await?;
            ensure!(
                !listed_in_catalog(&admin_b, &table).await?,
                "a cross-domain administrator must not list the table"
            );
            ensure!(
                sqlstate(&admin_b, &format!("SELECT * FROM {table}")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "a cross-domain administrator must see the table as missing"
            );

            // An explicit cross-domain read grant applies, but only as read.
            for (user, token, label) in [
                ("alice", &alice_b_token, "cross-owner-granted"),
                (admin.as_str(), &admin_b_token, "cross-admin-granted"),
            ] {
                h.meta()
                    .grant_table_privilege(
                        &info.table_id,
                        RBAC_TEAM_B,
                        user,
                        lakesoul_metadata::acl::Privilege::Read,
                        "alice",
                    )
                    .await?;
                let client = connect_as(h, label, &namespace, user, token).await?;
                let count: i64 = client
                    .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                    .await?
                    .get(0);
                ensure!(count == 0, "{user} saw {count} rows, expected 0");
                ensure!(
                    sqlstate(&client, &format!("INSERT INTO {table} VALUES (1)")).await?
                        == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                    "{user} must keep a cross-domain grant read-only"
                );
            }

            reference
                .execute(
                    "DELETE FROM public.domain_admin
                     WHERE domain = $1 AND user_name = $2",
                    &[&RBAC_TEAM_A, &admin],
                )
                .await?;
            alice_a
                .batch_execute(&format!("DROP TABLE {table}"))
                .await?;
            Ok(())
        })
    })
    .await
}

/// SQL GRANT/REVOKE maps PostgreSQL privileges onto the table ACL model,
/// including the collapsed REVOKE semantics and the domain.user extension.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grant_revoke_contract() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let bob_token = h.token("bob", RBAC_TEAM_A)?;
            let carol_token = h.token("carol", RBAC_TEAM_B)?;
            let frank_token = h.token("frank", RBAC_TEAM_A)?;
            let namespace = h.lake_namespace.clone();
            let table = unique("grant");
            let public_table = unique("grant_public");

            let alice =
                connect_as(h, "grant-alice", &namespace, "alice", &alice_token).await?;
            alice
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
                ))
                .await
                .context("grant: create table")?;
            alice
                .batch_execute(&format!("INSERT INTO {table} VALUES (1, 10), (2, 20)"))
                .await
                .context("grant: insert rows")?;

            // Same-domain SELECT makes the table read-only.
            alice
                .batch_execute(&format!("GRANT SELECT ON {table} TO bob"))
                .await
                .context("grant: SELECT to bob")?;
            let bob =
                connect_as(h, "grant-bob-read", &namespace, "bob", &bob_token).await?;
            let count: i64 = bob
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await?
                .get(0);
            ensure!(count == 2, "bob saw {count} rows, expected 2");
            ensure!(
                sqlstate(&bob, &format!("INSERT INTO {table} VALUES (3, 30)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "SELECT only must not permit writes"
            );

            // Granting a write privilege upgrades to rw.
            alice
                .batch_execute(&format!("GRANT INSERT, UPDATE ON {table} TO bob"))
                .await
                .context("grant: INSERT, UPDATE to bob")?;
            let bob =
                connect_as(h, "grant-bob-write", &namespace, "bob", &bob_token).await?;
            bob.batch_execute(&format!("INSERT INTO {table} VALUES (3, 30)"))
                .await
                .context("grant: bob inserts")?;
            ensure!(
                alice
                    .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                    .await?
                    .get::<_, i64>(0)
                    == 3,
                "the granted insert must be visible"
            );

            // Revoking a write privilege downgrades to ro, not none.
            alice
                .batch_execute(&format!("REVOKE INSERT ON {table} FROM bob"))
                .await
                .context("grant: revoke INSERT")?;
            let bob =
                connect_as(h, "grant-bob-revoked-write", &namespace, "bob", &bob_token)
                    .await?;
            ensure!(
                sqlstate(&bob, &format!("INSERT INTO {table} VALUES (4, 40)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "revoking INSERT must leave the table read-only"
            );
            let count: i64 = bob
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await?
                .get(0);
            ensure!(count == 3, "bob still reads after the write revoke");

            // Revoking read hides the table from a same-domain user.
            alice
                .batch_execute(&format!("REVOKE ALL ON {table} FROM bob"))
                .await
                .context("grant: revoke all from bob")?;
            let bob =
                connect_as(h, "grant-bob-none", &namespace, "bob", &bob_token).await?;
            ensure!(
                sqlstate(&bob, &format!("INSERT INTO {table} VALUES (4, 40)")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "revoke all must hide a same-domain table"
            );
            ensure!(
                bob.simple_query(&format!("SELECT * FROM {table}"))
                    .await
                    .is_err(),
                "revoke all must hide a same-domain table from SELECT"
            );

            // GRANT restores access after an explicit none.
            alice
                .batch_execute(&format!("GRANT ALL ON {table} TO bob"))
                .await
                .context("grant: ALL to bob")?;
            let bob =
                connect_as(h, "grant-bob-all", &namespace, "bob", &bob_token).await?;
            bob.batch_execute(&format!("INSERT INTO {table} VALUES (5, 50)"))
                .await
                .context("grant: bob inserts after ALL")?;

            // A user with rw but not ownership cannot grant.
            ensure!(
                sqlstate(&bob, &format!("GRANT SELECT ON {table} TO carol")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "a non-owner must not grant"
            );

            // Cross-domain grants are read-only. The grantee is written as
            // `domain.user`; the domain is the token's group.
            let cross_domain = format!("{RBAC_TEAM_B}.carol");
            alice
                .batch_execute(&format!("GRANT SELECT ON {table} TO {cross_domain}"))
                .await
                .context("grant: cross-domain SELECT")?;
            let carol =
                connect_as(h, "grant-carol-read", &namespace, "carol", &carol_token)
                    .await?;
            let count: i64 = carol
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await?
                .get(0);
            ensure!(count >= 2, "carol saw {count} rows");
            ensure!(
                sqlstate(&carol, &format!("INSERT INTO {table} VALUES (6, 60)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "a cross-domain grant must be read-only"
            );
            ensure!(
                sqlstate(&alice, &format!("GRANT ALL ON {table} TO {cross_domain}"))
                    .await?
                    == "0A000",
                "a cross-domain write grant must be refused"
            );

            // PUBLIC maps to the domain-wide grant.
            alice
                .batch_execute(&format!("GRANT SELECT ON {table} TO PUBLIC"))
                .await
                .context("grant: SELECT to PUBLIC")?;
            let frank =
                connect_as(h, "grant-frank-read", &namespace, "frank", &frank_token)
                    .await?;
            let count: i64 = frank
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await?
                .get(0);
            ensure!(count >= 2, "frank saw {count} rows through PUBLIC");
            ensure!(
                sqlstate(&frank, &format!("INSERT INTO {table} VALUES (7, 70)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "PUBLIC SELECT must be read-only"
            );
            alice
                .batch_execute(&format!("GRANT ALL ON {table} TO PUBLIC"))
                .await
                .context("grant: ALL to PUBLIC")?;
            let frank =
                connect_as(h, "grant-frank-write", &namespace, "frank", &frank_token)
                    .await?;
            frank
                .batch_execute(&format!("INSERT INTO {table} VALUES (8, 80)"))
                .await
                .context("grant: frank inserts through PUBLIC")?;
            alice
                .batch_execute(&format!(
                    "REVOKE INSERT, UPDATE, DELETE ON {table} FROM PUBLIC"
                ))
                .await
                .context("grant: revoke writes from PUBLIC")?;
            let frank =
                connect_as(h, "grant-frank-ro", &namespace, "frank", &frank_token)
                    .await?;
            ensure!(
                sqlstate(&frank, &format!("INSERT INTO {table} VALUES (9, 90)")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "revoking PUBLIC writes must leave read-only"
            );
            ensure!(
                frank
                    .simple_query(&format!("SELECT * FROM {table}"))
                    .await
                    .is_ok(),
                "revoking PUBLIC writes must keep SELECT"
            );
            alice
                .batch_execute(&format!("REVOKE ALL ON {table} FROM PUBLIC"))
                .await
                .context("grant: revoke all from PUBLIC")?;
            let frank =
                connect_as(h, "grant-frank-none", &namespace, "frank", &frank_token)
                    .await?;
            ensure!(
                sqlstate(&frank, &format!("INSERT INTO {table} VALUES (9, 90)")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "revoking all from PUBLIC must hide the table"
            );
            ensure!(
                frank
                    .simple_query(&format!("SELECT * FROM {table}"))
                    .await
                    .is_err(),
                "revoking all from PUBLIC must hide the table from SELECT"
            );

            // Public tables take no ACL.
            h.create_table_out_of_band(&public_table, "public", "alice")
                .await?;
            ensure!(
                sqlstate(&alice, &format!("GRANT SELECT ON {public_table} TO bob"))
                    .await?
                    == "0A000",
                "public tables must not take ACL rows"
            );

            // Unknown tables look missing.
            ensure!(
                sqlstate(&alice, "GRANT SELECT ON acl_no_such_table TO bob").await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "an unknown table must be 42P01"
            );

            // A domain administrator can manage any table in the domain.
            let admin_user = unique("admin");
            let admin_token = h.token(&admin_user, RBAC_TEAM_A)?;
            let reference = h.connect_reference().await?;
            reference
                .execute(
                    "INSERT INTO public.domain_admin(domain, user_name)
                     VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&RBAC_TEAM_A, &admin_user],
                )
                .await?;
            let admin =
                connect_as(h, "grant-admin", &namespace, &admin_user, &admin_token)
                    .await?;
            admin
                .batch_execute(&format!("GRANT SELECT ON {table} TO bob"))
                .await
                .context("grant: domain admin grants")?;
            reference
                .execute(
                    "DELETE FROM public.domain_admin
                     WHERE domain = $1 AND user_name = $2",
                    &[&RBAC_TEAM_A, &admin_user],
                )
                .await?;

            // Extended protocol: Parse/Describe must accept the statement and
            // Execute must perform it.
            let prepared = alice
                .prepare(&format!("GRANT SELECT ON {table} TO frank"))
                .await
                .context("grant: prepare")?;
            alice
                .execute(&prepared, &[])
                .await
                .context("grant: execute prepared")?;

            // The metadata commits immediately, so a transaction block is
            // refused like DDL.
            alice.batch_execute("BEGIN").await?;
            ensure!(
                sqlstate(&alice, &format!("GRANT SELECT ON {table} TO bob")).await?
                    == "0A000",
                "GRANT inside a transaction must be refused"
            );
            alice.batch_execute("ROLLBACK").await?;

            // A revoke removes the cross-domain grant.
            alice
                .batch_execute(&format!("REVOKE ALL ON {table} FROM {cross_domain}"))
                .await
                .context("grant: revoke cross-domain")?;
            let carol =
                connect_as(h, "grant-carol-revoked", &namespace, "carol", &carol_token)
                    .await?;
            ensure!(
                sqlstate(&carol, &format!("INSERT INTO {table} VALUES (6, 60)")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "a revoked cross-domain grant must hide the table"
            );
            ensure!(
                carol
                    .simple_query(&format!("SELECT * FROM {table}"))
                    .await
                    .is_err(),
                "a revoked cross-domain grant must hide the table from SELECT"
            );

            alice
                .batch_execute(&format!("DROP TABLE {table}"))
                .await
                .context("grant: drop table")?;
            Ok(())
        })
    })
    .await
}

/// `ALL TABLES IN SCHEMA public` grants every non-public table in the current
/// namespace, and a cross-domain grant makes the private namespace visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_tables_grant_contract() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let carol_token = h.token("carol", RBAC_TEAM_B)?;
            let namespace = h.namespace_of(RBAC_TEAM_A)?.to_string();
            let first = unique("all_first");
            let second = unique("all_second");

            let alice = connect_as(h, "grant-all-alice", &namespace, "alice", &alice_token)
                .await?;
            for (table, value) in [(&first, 1), (&second, 2)] {
                alice
                    .batch_execute(&format!(
                        "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
                    ))
                    .await
                    .with_context(|| format!("create {table}"))?;
                alice
                    .batch_execute(&format!(
                        "INSERT INTO {table} VALUES ({value}, {value})"
                    ))
                    .await
                    .with_context(|| format!("insert {table}"))?;
            }

            let cross_domain = format!("{RBAC_TEAM_B}.carol");
            alice
                .batch_execute(&format!(
                    "GRANT SELECT ON ALL TABLES IN SCHEMA public TO {cross_domain}"
                ))
                .await
                .context("grant: all tables cross-domain")?;

            let carol = connect_as(
                h,
                "grant-all-carol",
                &namespace,
                "carol",
                &carol_token,
            )
            .await?;
            for table in [&first, &second] {
                let count: i64 = carol
                    .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                    .await
                    .with_context(|| format!("carol reads {table}"))?
                    .get(0);
                ensure!(count == 1, "carol saw {count} rows in {table}");
                ensure!(
                    sqlstate(&carol, &format!("INSERT INTO {table} VALUES (9, 9)"))
                        .await?
                        == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                    "an ALL TABLES grant must be read-only across domains"
                );
            }

            alice
                .batch_execute(&format!(
                    "REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {cross_domain}"
                ))
                .await
                .context("grant: revoke all tables")?;
            // With the table grants gone, the private namespace itself is no
            // longer visible, so the same-domain database disappears from
            // `pg_database` and a startup connection fails like a missing one.
            let carol = connect_as(
                h,
                "grant-all-carol-revoked",
                &h.lake_namespace,
                "carol",
                &carol_token,
            )
            .await?;
            let listed: i64 = carol
                .query_one(
                    "SELECT count(*) FROM pg_catalog.pg_database WHERE datname = $1",
                    &[&namespace],
                )
                .await?
                .get(0);
            ensure!(
                listed == 0,
                "the revoked private namespace must not be listed"
            );

            alice
                .batch_execute(&format!("DROP TABLE {first}"))
                .await?;
            alice
                .batch_execute(&format!("DROP TABLE {second}"))
                .await?;
            Ok(())
        })
    })
    .await
}

/// A plan prepared before a revoke must not keep reading the table. The ACL
/// refresh marks the view changed and the Execute path replans before running
/// the stale portal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_prepared_statement_replans() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let bob_token = h.token("bob", RBAC_TEAM_A)?;
            let namespace = h.lake_namespace.clone();
            let table = unique("replan");

            let alice =
                connect_as(h, "replan-alice", &namespace, "alice", &alice_token).await?;
            alice
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
                ))
                .await?;
            alice
                .batch_execute(&format!("INSERT INTO {table} VALUES (1, 10)"))
                .await?;
            alice
                .batch_execute(&format!("GRANT SELECT ON {table} TO bob"))
                .await?;

            let bob = connect_as(h, "replan-bob", &namespace, "bob", &bob_token).await?;
            let prepared = bob
                .prepare(&format!("SELECT count(*) FROM {table}"))
                .await
                .context("replan: prepare as bob")?;
            let count: i64 = bob
                .query_one(&prepared, &[])
                .await
                .context("replan: initial execute")?
                .get(0);
            ensure!(count == 1, "bob saw {count} rows, expected 1");

            alice
                .batch_execute(&format!("REVOKE ALL ON {table} FROM bob"))
                .await
                .context("replan: revoke bob")?;

            // The session refreshes its ACL view every five seconds. Wait for
            // the stale plan to become invalid before executing it again.
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
            ensure!(
                bob.query_one(&prepared, &[]).await.is_err(),
                "a prepared statement must not keep reading after its grant is revoked"
            );

            alice.batch_execute(&format!("DROP TABLE {table}")).await?;
            Ok(())
        })
    })
    .await
}

/// `CREATE USER` creates a real PG login role, grants the domain RBAC roles,
/// and records the user in the directory used by token validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_user_contract() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            // A dedicated administrator keeps this fixture from making a
            // shared test user an admin while other suites run in parallel.
            let admin = unique("creator");
            let admin_token = h.token(&admin, RBAC_TEAM_A)?;
            let bob_token = h.token("bob", RBAC_TEAM_A)?;
            let namespace = h.lake_namespace.clone();
            h.ensure_token_user(&admin_token).await?;
            h.ensure_token_user(&bob_token).await?;
            let reference = h.connect_reference().await?;
            reference
                .execute(
                    "INSERT INTO public.domain_admin(domain, user_name)
                     VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&RBAC_TEAM_A, &admin],
                )
                .await
                .context("create-user: make the fixture user a domain admin")?;

            let admin_client =
                connect_as(h, "create-user-admin", &namespace, &admin, &admin_token)
                    .await?;
            let created = unique("created");
            admin_client
                .batch_execute(&format!(
                    "CREATE USER {created} WITH PASSWORD 'created-secret'"
                ))
                .await
                .context("create-user: domain admin creates a user")?;

            let role: bool = reference
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                    &[&created],
                )
                .await?
                .get(0);
            ensure!(role, "the PostgreSQL role was not created");
            let users_role = format!("{RBAC_TEAM_A}_users");
            let row = reference
                .query_one(
                    "SELECT
                       (SELECT EXISTS (SELECT 1 FROM public.lakesoul_user WHERE user_name = $1)),
                       (SELECT EXISTS (SELECT 1 FROM public.domain_user
                                      WHERE domain = $2 AND user_name = $1)),
                       (SELECT EXISTS (SELECT 1 FROM pg_auth_members m
                                       JOIN pg_roles r ON r.oid = m.roleid
                                       JOIN pg_roles u ON u.oid = m.member
                                       WHERE r.rolname = $3 AND u.rolname = $1)),
                       (SELECT EXISTS (SELECT 1 FROM pg_auth_members m
                                       JOIN pg_roles r ON r.oid = m.roleid
                                       JOIN pg_roles u ON u.oid = m.member
                                       WHERE r.rolname = $4 AND u.rolname = $1))",
                    &[&created, &RBAC_TEAM_A, &RBAC_TEAM_A, &users_role],
                )
                .await
                .context("create-user: read directory and memberships")?;
            for (index, label) in [
                "lakesoul_user",
                "domain_user",
                "domain role membership",
                "domain users membership",
            ]
            .iter()
            .enumerate()
            {
                let exists: bool = row.get(index);
                ensure!(exists, "create-user: missing {label}");
            }
            let admins_role = format!("{RBAC_TEAM_A}_admins");
            let admin_grant: bool = reference
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_auth_members m
                                    JOIN pg_roles r ON r.oid = m.roleid
                                    JOIN pg_roles u ON u.oid = m.member
                                    WHERE r.rolname = $2 AND u.rolname = $1)",
                    &[&created, &admins_role],
                )
                .await
                .context("create-user: read admin membership")?
                .get(0);
            ensure!(
                !admin_grant,
                "CREATE USER must never grant the domain admin role"
            );

            // The new user can obtain a token and connect under the same
            // domain; the directory check does not require a pre-existing
            // token-only row.
            let created_token = h.token(&created, RBAC_TEAM_A)?;
            let created_client =
                connect_as(h, "create-user-connect", &namespace, &created, &created_token)
                    .await?;
            created_client
                .batch_execute("SELECT 1")
                .await
                .context("create-user: new user connects")?;

            // The extended protocol uses the same rewrite and hook.
            let prepared_user = unique("prepared");
            let prepared = admin_client
                .prepare(&format!(
                    "CREATE USER {prepared_user} PASSWORD 'prepared-secret'"
                ))
                .await
                .context("create-user: prepare")?;
            admin_client
                .execute(&prepared, &[])
                .await
                .context("create-user: execute prepared")?;
            let prepared_role: bool = reference
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                    &[&prepared_user],
                )
                .await?
                .get(0);
            ensure!(prepared_role, "prepared CREATE USER did not create a role");

            // Token issuance checks the same directory.
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_postgres-lakesoul"))
                .args([
                    "token",
                    "--user",
                    &created,
                    "--group",
                    RBAC_TEAM_A,
                    "--ttl",
                    "5m",
                ])
                .output()
                .context("create-user: run token command")?;
            ensure!(
                output.status.success(),
                "create-user: token command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let token = String::from_utf8(output.stdout)?.trim().to_string();
            let claims = lakesoul_metadata::JwtServer::new(h.meta().get_client_secret())
                .decode_token(&token)
                .context("create-user: decode issued token")?;
            ensure!(
                claims.sub == created && claims.group == RBAC_TEAM_A,
                "create-user: token claims {claims:?}"
            );

            // Ordinary domain users cannot create users.
            let bob = connect_as(h, "create-user-bob", &namespace, "bob", &bob_token).await?;
            ensure!(
                sqlstate(
                    &bob,
                    &format!("CREATE USER {} PASSWORD 'x'", unique("nope"))
                )
                .await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "a non-admin must not create users"
            );

            // Even a public-domain administrator cannot create tenant users
            // through the SQL path; public users are provisioned by operators.
            let public_token = h.token("public_admin", "public")?;
            h.ensure_token_user(&public_token).await?;
            reference
                .execute(
                    "INSERT INTO public.domain_admin(domain, user_name)
                     VALUES ('public', 'public_admin') ON CONFLICT DO NOTHING",
                    &[],
                )
                .await
                .context("create-user: make public_admin a public admin")?;
            let public_admin = connect_as(
                h,
                "create-user-public",
                &namespace,
                "public_admin",
                &public_token,
            )
            .await?;
            ensure!(
                sqlstate(
                    &public_admin,
                    &format!("CREATE USER {} PASSWORD 'x'", unique("nope"))
                )
                .await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "CREATE USER must be refused in the public domain"
            );
            reference
                .execute(
                    "DELETE FROM public.domain_admin
                     WHERE domain = 'public' AND user_name = 'public_admin'",
                    &[],
                )
                .await?;

            // PASSWORD is mandatory and duplicate roles are 42710.
            ensure!(
                sqlstate(&admin_client, "CREATE USER create_user_no_password").await? == "0A000",
                "CREATE USER must require an explicit password"
            );
            ensure!(
                sqlstate(
                    &admin_client,
                    &format!("CREATE USER {created} PASSWORD 'again'")
                )
                .await?
                    == "42710",
                "duplicate CREATE USER must be 42710"
            );
            admin_client
                .batch_execute(&format!(
                    "CREATE USER IF NOT EXISTS {created} PASSWORD 'ignored'"
                ))
                .await
                .context("create-user: IF NOT EXISTS keeps the existing role")?;
            ensure!(
                sqlstate(&admin_client, "CREATE USER create_user_no_password").await? == "0A000",
                "CREATE USER must remain password-only"
            );

            reference
                .execute(
                    "DELETE FROM public.domain_admin
                     WHERE domain = $1 AND user_name = $2",
                    &[&RBAC_TEAM_A, &admin],
                )
                .await?;
            Ok(())
        })
    })
    .await
}

/// Federation objects use the same default as LakeSoul tables: readable in the
/// domain, writable only by the creator or a domain administrator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn federation_acl_contract() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let bob_token = h.token("bob", RBAC_TEAM_A)?;
            let carol_token = h.token("carol", RBAC_TEAM_B)?;
            let namespace = h.lake_namespace.clone();
            let server = unique("federation_server");
            let table = unique("federation_table");

            let alice =
                connect_as(h, "federation-alice", &namespace, "alice", &alice_token)
                    .await?;
            alice
                .batch_execute(&format!(
                    "CREATE SERVER {server} FOREIGN DATA WRAPPER external_file_wrapper"
                ))
                .await
                .context("federation: create server")?;
            alice
                .batch_execute(&format!(
                    "CREATE FOREIGN TABLE {table} (id BIGINT, name TEXT) \
                     SERVER {server} OPTIONS (path 's3://lakesoul-test-bucket/federation/no-such.csv', format 'csv')"
                ))
                .await
                .context("federation: create foreign table")?;

            let bob =
                connect_as(h, "federation-bob", &namespace, "bob", &bob_token).await?;
            ensure!(
                listed_in_catalog(&bob, &table).await?,
                "same-domain user must list the foreign table"
            );
            let carol =
                connect_as(h, "federation-carol", &namespace, "carol", &carol_token)
                    .await?;
            ensure!(
                !listed_in_catalog(&carol, &table).await?,
                "cross-domain user must not list the foreign table"
            );
            ensure!(
                sqlstate(&carol, &format!("SELECT * FROM {table}")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "cross-domain foreign table must look missing"
            );

            // The creator and a team-A space administrator are outside their
            // rights when the connection acts in team B.
            let alice_b_token = h.token("alice", RBAC_TEAM_B)?;
            let alice_b =
                connect_as(h, "federation-alice-b", &namespace, "alice", &alice_b_token)
                    .await?;
            ensure!(
                !listed_in_catalog(&alice_b, &table).await?,
                "a cross-domain creator must not list the foreign table"
            );
            ensure!(
                sqlstate(&alice_b, &format!("SELECT * FROM {table}")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "a cross-domain creator must see the foreign table as missing"
            );

            let federation_admin = unique("federation_admin");
            let federation_admin_token =
                h.token(&federation_admin, RBAC_TEAM_B)?;
            let reference = h.connect_reference().await?;
            reference
                .execute(
                    "INSERT INTO public.domain_admin(domain, user_name)
                     VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    &[&RBAC_TEAM_A, &federation_admin],
                )
                .await
                .context("federation: make the fixture a team-A admin")?;
            let admin_b = connect_as(
                h,
                "federation-admin-b",
                &namespace,
                &federation_admin,
                &federation_admin_token,
            )
            .await?;
            ensure!(
                !listed_in_catalog(&admin_b, &table).await?,
                "a cross-domain administrator must not list the foreign table"
            );
            ensure!(
                sqlstate(&admin_b, &format!("SELECT * FROM {table}")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "a cross-domain administrator must see the foreign table as missing"
            );
            reference
                .execute(
                    "DELETE FROM public.domain_admin
                     WHERE domain = $1 AND user_name = $2",
                    &[&RBAC_TEAM_A, &federation_admin],
                )
                .await?;

            ensure!(
                sqlstate(&bob, &format!("INSERT INTO {table} VALUES (1, 'x')")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "same-domain reader must not write the foreign table"
            );
            ensure!(
                sqlstate(&bob, &format!("DROP FOREIGN TABLE {table}")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "same-domain reader must not drop the foreign table"
            );
            ensure!(
                sqlstate(
                    &bob,
                    &format!(
                        "CREATE FOREIGN TABLE {} (id BIGINT) SERVER {server}",
                        unique("federation_nope")
                    )
                )
                .await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "same-domain reader must not create a table on the server"
            );
            ensure!(
                sqlstate(&bob, &format!("DROP SERVER {server}")).await?
                    == SqlState::INSUFFICIENT_PRIVILEGE.code(),
                "same-domain reader must not drop the server"
            );

            // IF EXISTS suppresses a missing table or server, including one
            // that another domain owns and this connection cannot see.
            bob.batch_execute("DROP FOREIGN TABLE IF EXISTS federation_missing")
                .await?;
            bob.batch_execute("DROP SERVER IF EXISTS federation_missing")
                .await?;
            carol
                .batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {table}"))
                .await
                .context("federation: hidden IF EXISTS table")?;
            carol
                .batch_execute(&format!("DROP SERVER IF EXISTS {server}"))
                .await
                .context("federation: hidden IF EXISTS server")?;

            alice
                .batch_execute(&format!("DROP FOREIGN TABLE {table}"))
                .await
                .context("federation: owner drops foreign table")?;
            alice
                .batch_execute(&format!("DROP SERVER {server}"))
                .await
                .context("federation: owner drops server")?;
            Ok(())
        })
    })
    .await
}

/// A valid token for a user that no domain administrator created is refused
/// before a session is built.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_directory_user_is_rejected() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let user = unique("missing_user");
            let token = h.token(&user, RBAC_TEAM_A)?;
            let config = h.lake_config_as(&h.lake_namespace, &user, Some(&token));
            let error = match h.connect_attempt(&config).await? {
                Ok(_) => bail!("an unlisted user connected"),
                Err(error) => error,
            };
            let db = error
                .as_db_error()
                .context("expected an authentication error")?;
            ensure!(
                db.code().code() == SqlState::INVALID_PASSWORD.code(),
                "unknown user gave {}, expected 28P01",
                db.code().code()
            );
            ensure!(
                !db.message().to_lowercase().contains("directory")
                    && !db.message().to_lowercase().contains("enabled"),
                "the refusal must not reveal the directory state: {}",
                db.message()
            );
            Ok(())
        })
    })
    .await
}

/// Disabling a directory entry takes effect for new connections after the
/// five-second cache, without restarting the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_directory_user_is_rejected() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let user = unique("disabled_user");
            let token = h.token(&user, RBAC_TEAM_A)?;
            h.ensure_token_user(&token).await?;
            connect_as(h, "disabled-first", &h.lake_namespace, &user, &token).await?;

            let reference = h.connect_reference().await?;
            reference
                .execute(
                    "UPDATE public.domain_user SET disabled = true
                     WHERE domain = $1 AND user_name = $2",
                    &[&RBAC_TEAM_A, &user],
                )
                .await
                .context("disabled-user: disable the directory entry")?;
            // The server cached the successful lookup for five seconds.
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;

            let config = h.lake_config_as(&h.lake_namespace, &user, Some(&token));
            let error = match h.connect_attempt(&config).await? {
                Ok(_) => bail!("a disabled user connected"),
                Err(error) => error,
            };
            let db = error
                .as_db_error()
                .context("expected an authentication error")?;
            ensure!(
                db.code().code() == SqlState::INVALID_PASSWORD.code(),
                "disabled user gave {}, expected 28P01",
                db.code().code()
            );

            reference
                .execute(
                    "DELETE FROM public.domain_user
                     WHERE domain = $1 AND user_name = $2",
                    &[&RBAC_TEAM_A, &user],
                )
                .await?;
            reference
                .execute(
                    "DELETE FROM public.lakesoul_user WHERE user_name = $1",
                    &[&user],
                )
                .await?;
            Ok(())
        })
    })
    .await
}

/// Read-side guardrails discovered in the ACL audit: SELECT INTO is refused,
/// hidden tables are 42P01 in SELECT/COPY/DESCRIBE, and information_schema does
/// not list them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_audit_contract() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let carol_token = h.token("carol", RBAC_TEAM_B)?;
            let namespace = h.lake_namespace.clone();
            let table = unique("audit_hidden");

            let alice = connect_as(h, "audit-alice", &namespace, "alice", &alice_token)
                .await?;
            alice
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
                ))
                .await?;
            alice
                .batch_execute(&format!("INSERT INTO {table} VALUES (1, 10)"))
                .await?;

            let carol = connect_as(h, "audit-carol", &namespace, "carol", &carol_token)
                .await?;
            ensure!(
                sqlstate(&carol, &format!("SELECT * FROM {table}")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "hidden SELECT must be 42P01"
            );
            ensure!(
                sqlstate(
                    &carol,
                    &format!("COPY (SELECT * FROM {table}) TO STDOUT")
                )
                .await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "hidden COPY query must be 42P01"
            );
            ensure!(
                sqlstate(&carol, &format!("DESCRIBE {table}")).await?
                    == SqlState::UNDEFINED_TABLE.code(),
                "hidden DESCRIBE must be 42P01"
            );
            let listed: i64 = carol
                .query_one(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = $1",
                    &[&table],
                )
                .await?
                .get(0);
            ensure!(
                listed == 0,
                "information_schema.tables must not list a hidden table"
            );

            ensure!(
                sqlstate(&alice, "SELECT 1 INTO acl_select_into_target").await?
                    == "0A000",
                "SELECT INTO must be refused"
            );
            let extended_error = alice
                .prepare("SELECT 1 INTO acl_select_into_prepared")
                .await
                .expect_err("SELECT INTO must be refused at Parse");
            ensure!(
                extended_error
                    .as_db_error()
                    .is_some_and(|db| db.code().code() == "0A000"),
                "prepared SELECT INTO gave {extended_error}"
            );

            alice
                .batch_execute(&format!("DROP TABLE {table}"))
                .await?;
            Ok(())
        })
    })
    .await
}
