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
                sqlstate(&carol, &format!("SELECT * FROM {table}")).await.is_ok(),
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
