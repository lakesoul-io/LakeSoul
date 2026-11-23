// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Authentication contracts against a real server started with `--jwt-auth`.
//!
//! The password of every connection is a LakeSoul JWT (claims `{sub, group,
//! exp}`, HS256 with the metadata client secret), and its `sub` must name the
//! startup user. Every refusal is `FATAL 28P01` and never says why.
//!
//! Authentication alone hides nothing: a namespace in a domain the token does
//! not carry is still listed and connectable.
//!
//! With `--rbac` as well, the token's `group` confines the connection to its
//! LakeSoul domain plus `public`/`lake-public`: other domains' namespaces are
//! neither listed nor connectable, other domains' tables are `42P01` to every
//! statement and absent from the catalogs, new tables are created in the
//! creator's domain, detached jobs keep their submitter's domain, and the
//! paths that cannot apply the filter (subscriptions, IVM maintenance) are
//! refused with `42501`.

pub mod support;

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use lakesoul_metadata::{Claims, JwtServer};
use tokio_postgres::Client;
use tokio_postgres::error::SqlState;

use support::{Harness, JWT_OTHER_DOMAIN, LAKE_USER, RBAC_TEAM_A, RBAC_TEAM_B};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jwt_authentication_contract() -> Result<()> {
    support::run_jwt_suite(|h| {
        Box::pin(async move {
            jwt_rejects(h).await?;
            auth_only_mode(h).await
        })
    })
    .await
}

/// `postgres-lakesoul token` validates the user directory, so it requires the
/// metadata database in an RBAC deployment. A connection failure must not fall
/// back to minting an unchecked token.
#[test]
fn token_command_requires_metadata_directory() -> Result<()> {
    // Port 1 on loopback: a connection attempt would be refused at once.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_postgres-lakesoul"))
        .args([
            "token",
            "--user",
            "alice",
            "--group",
            RBAC_TEAM_A,
            "--ttl",
            "5m",
        ])
        .env_remove("lakesoul_home")
        .env_remove("LAKESOUL_PG_SECONDARY_URL")
        .env(
            "LAKESOUL_PG_URL",
            "jdbc:postgresql://127.0.0.1:1/offline_metadata",
        )
        .env("LAKESOUL_PG_USERNAME", "offline_user")
        .env("LAKESOUL_PG_PASSWORD", "offline_password")
        .output()
        .context("run postgres-lakesoul token")?;
    ensure!(
        !output.status.success(),
        "token-offline: token issuance must fail when the user directory cannot be read"
    );
    ensure!(
        output.stdout.is_empty(),
        "token-offline: a failed validation must not print a token"
    );
    Ok(())
}

/// A team-A table in the shared `public` namespace.
const OWNED: &str = "rbac_team_a_owned";
/// A team-A table another engine creates while the server runs.
const EXTERNAL: &str = "rbac_team_a_external";
/// A team-A table another engine replaces by a team-B table of the same name.
const REPLACED: &str = "rbac_team_a_replaced";

/// `DROP` authorizes the table it deletes, not the name the catalog snapshot
/// remembers: when another engine drops a team-A table and recreates its
/// name in team B, alice's `DROP` before the next refresh - which the
/// snapshot still allows - must leave team B's table alone.
async fn stale_drop_cannot_reach_a_replacement(
    h: &Harness,
    alice: &Client,
    bob: &Client,
) -> Result<()> {
    // Created through this server, so the snapshot records it as team A.
    alice
        .batch_execute(&format!("CREATE TABLE {REPLACED} (id BIGINT, val BIGINT)"))
        .await
        .context("rbac-stale-drop: create as team A")?;
    h.replace_table_out_of_band(REPLACED, RBAC_TEAM_B, "bob")
        .await?;
    // No wait: the snapshot still says team A (a refresh in between would
    // only hide the name from alice, with the same outcome).
    let (code, message) = server_error(alice, &format!("DROP TABLE {REPLACED}")).await?;
    ensure!(
        code == SqlState::UNDEFINED_TABLE.code(),
        "rbac-stale-drop: alice's DROP gave {code} ({message}), expected 42P01"
    );
    let domains = h.stored_domains(REPLACED).await?;
    ensure!(
        domains == vec![RBAC_TEAM_B.to_string(); 3],
        "rbac-stale-drop: team B's replacement must survive alice's DROP, \
         found domains {domains:?}"
    );
    // The refused DROP refreshed the snapshot: the name is now team B's.
    bob.batch_execute(&format!("DROP TABLE {REPLACED}"))
        .await
        .context("rbac-stale-drop: team B drops its own table")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rbac_domain_enforcement_contract() -> Result<()> {
    support::run_rbac_suite(|h| {
        Box::pin(async move {
            let alice_token = h.token("alice", RBAC_TEAM_A)?;
            let bob_token = h.token("bob", RBAC_TEAM_B)?;
            let alice =
                connect(h, "rbac-alice", &h.lake_namespace, "alice", &alice_token)
                    .await?;
            let bob =
                connect(h, "rbac-bob", &h.lake_namespace, "bob", &bob_token).await?;

            database_visibility(h, &alice, &alice_token).await?;
            create_sets_domain(h, &alice).await?;
            table_visibility(h, &alice, &bob).await?;
            extended_protocol_hides_too(&alice, &bob).await?;
            detached_jobs_keep_the_domain(&alice, &bob).await?;
            jobs_are_confined_to_the_domain(h, &alice).await?;
            unfiltered_paths_are_refused(&alice).await?;
            cross_process_creation(h, &alice, &bob).await?;
            stale_drop_cannot_reach_a_replacement(h, &alice, &bob).await?;

            // One statement per message: DDL is never batched.
            for table in [OWNED, EXTERNAL] {
                alice
                    .batch_execute(&format!("DROP TABLE {table}"))
                    .await
                    .with_context(|| {
                        format!("rbac-owner-drops: the owner drops {table}")
                    })?;
            }
            Ok(())
        })
    })
    .await
}

/// `pg_database` lists the public namespace and the token's own, never
/// another domain's; connecting to another domain's namespace fails exactly
/// like connecting to one that does not exist.
async fn database_visibility(h: &Harness, alice: &Client, token: &str) -> Result<()> {
    let public = h.lake_namespace.as_str();
    let team_a = h.namespace_of(RBAC_TEAM_A)?;
    let team_b = h.namespace_of(RBAC_TEAM_B)?;

    let listed = list_databases(alice, &[public, team_a, team_b]).await?;
    let mut expected = vec![public.to_string(), team_a.to_string()];
    expected.sort();
    ensure!(
        listed == expected,
        "rbac-database-visibility: listed {listed:?}, expected {expected:?}"
    );

    let own = connect(h, "rbac-own-database", team_a, "alice", token).await?;
    let database: String = own
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    ensure!(
        database == team_a,
        "rbac-own-database: connected to {database:?}"
    );

    let hidden = refused_startup(h, team_b, "alice", token).await?;
    let missing_name = format!("{public}_missing");
    let missing = refused_startup(h, &missing_name, "alice", token).await?;
    ensure!(
        hidden.replace(team_b, "<db>") == missing.replace(&missing_name, "<db>"),
        "rbac-hidden-database: a hidden database must fail like a missing one; \
         hidden: {hidden:?}, missing: {missing:?}"
    );
    ensure!(
        hidden.contains("does not exist"),
        "rbac-hidden-database: {hidden:?}"
    );
    Ok(())
}

/// `CREATE TABLE` stores the creator's domain on every metadata row,
/// including the `data_commit_info` and `partition_info` rows its first
/// `INSERT` publishes.
async fn create_sets_domain(h: &Harness, alice: &Client) -> Result<()> {
    alice
        .batch_execute(&format!(
            "CREATE TABLE {OWNED} (id BIGINT PRIMARY KEY, val BIGINT)"
        ))
        .await
        .context("rbac-create-sets-domain: create as team A")?;
    alice
        .batch_execute(&format!("INSERT INTO {OWNED} VALUES (1, 10), (2, 20)"))
        .await
        .context("rbac-create-sets-domain: fill as team A")?;
    let mut domains = h.stored_domains(OWNED).await?;
    domains.extend(h.stored_commit_domains(OWNED).await?);
    ensure!(
        domains.iter().all(|domain| domain == RBAC_TEAM_A),
        "rbac-create-sets-domain: table_info/table_name_id/table_path_id/\
         partition_info/data_commit_info domains are {domains:?}, expected {RBAC_TEAM_A}"
    );
    Ok(())
}

/// Another domain's table in the shared namespace does not exist for bob -
/// nor for a `public` token - while public tables stay readable; nothing bob
/// tries changes any metadata or file.
///
/// "Does not exist" is checked against a table that really does not exist:
/// the same SQLSTATE and the same message but for the name. (A `SELECT` of a
/// missing table is `XX000` naming it, a documented boundary of this server,
/// see `ddl_contract`; DML and DDL say `42P01`.)
async fn table_visibility(h: &Harness, alice: &Client, bob: &Client) -> Result<()> {
    let before = h.authoritative_snapshot().await?;
    for (id, sql) in [
        ("rbac-hidden-select", "SELECT * FROM {t}"),
        ("rbac-hidden-insert", "INSERT INTO {t} VALUES (3, 30)"),
        ("rbac-hidden-update", "UPDATE {t} SET val = 0 WHERE id = 1"),
        (
            "rbac-hidden-alter",
            "ALTER TABLE {t} ADD COLUMN extra BIGINT",
        ),
        ("rbac-hidden-drop", "DROP TABLE {t}"),
    ] {
        expect_like_missing(bob, id, sql, OWNED).await?;
    }
    bob.batch_execute(&format!("DROP TABLE IF EXISTS {OWNED}"))
        .await
        .context("rbac-hidden-drop-if-exists: a hidden table is simply absent")?;
    expect_like_missing(
        &h.lake,
        "rbac-hidden-from-public",
        "SELECT * FROM {t}",
        OWNED,
    )
    .await?;
    ensure!(
        !listed_anywhere(bob, OWNED).await?,
        "rbac-hidden-catalog: bob's pg_class/pg_tables list {OWNED}"
    );
    expect_item_count(bob, "rbac-public-still-readable").await?;

    // The pinned-read table function resolves through the session catalog,
    // so a hidden table stays hidden there too. A far-future timestamp makes
    // the only possible failure the table lookup, not the pinned view.
    let as_of = "2099-01-01T00:00:00+00:00";
    let error = bob
        .query_opt(
            &format!(
                "SELECT * FROM lakesoul_table_scan('{OWNED}', 'timestamp', '{as_of}')"
            ),
            &[],
        )
        .await
        .expect_err("rbac-hidden-time-travel: a hidden table must be refused");
    let (code, message) = match error.as_db_error() {
        Some(db) => (db.code().code().to_string(), db.message().to_string()),
        None => (String::new(), error.to_string()),
    };
    // The catalog may report a hidden table either as an undefined relation
    // (42P01, like a plain SELECT) or as the function's own "not found" error.
    ensure!(
        code == "42P01" || message.contains("not found or not visible"),
        "rbac-hidden-time-travel: unexpected error ({code}): {message}"
    );
    let count: i64 = alice
        .query_one(
            &format!(
                "SELECT count(*) FROM lakesoul_table_scan('{OWNED}', 'timestamp', '{as_of}')"
            ),
            &[],
        )
        .await
        .context("rbac-owner-time-travel")?
        .get(0);
    ensure!(
        count == 2,
        "rbac-owner-time-travel: {count} rows, expected 2"
    );

    let after = h.authoritative_snapshot().await?;
    ensure!(
        before == after,
        "rbac-hidden-no-side-effects: a refused statement changed metadata or files"
    );
    // Names stay unique per namespace across domains, as in PostgreSQL.
    expect_sqlstate(
        bob,
        "rbac-hidden-name-taken",
        &format!("CREATE TABLE {OWNED} (id BIGINT)"),
        &SqlState::DUPLICATE_TABLE,
    )
    .await?;
    ensure!(
        h.stored_domains(OWNED).await? == vec![RBAC_TEAM_A.to_string(); 3],
        "rbac-hidden-name-taken: bob's CREATE changed the owner's rows"
    );

    // The owner still has it all.
    let count: i64 = alice
        .query_one(&format!("SELECT count(*) FROM {OWNED}"), &[])
        .await
        .context("rbac-owner-reads")?
        .get(0);
    ensure!(count == 2, "rbac-owner-reads: {count} rows, expected 2");
    alice
        .batch_execute(&format!("UPDATE {OWNED} SET val = 11 WHERE id = 1"))
        .await
        .context("rbac-owner-updates")?;
    ensure!(
        listed_everywhere(alice, OWNED).await?,
        "rbac-owner-catalog: alice's pg_class/pg_tables must list {OWNED}"
    );
    Ok(())
}

/// Parse of a hidden table fails the same way; the owner's prepared
/// statements are unaffected.
async fn extended_protocol_hides_too(alice: &Client, bob: &Client) -> Result<()> {
    let template = "SELECT val FROM {t} WHERE id = $1";
    let hidden = parse_error(bob, &template.replace("{t}", OWNED)).await?;
    let missing = parse_error(bob, &template.replace("{t}", MISSING)).await?;
    ensure_same_error("rbac-extended-hidden", OWNED, &hidden, &missing)?;
    let statement = alice
        .prepare(&template.replace("{t}", OWNED))
        .await
        .context("rbac-extended-owner")?;
    let val: i64 = alice
        .query_one(&statement, &[&1i64])
        .await
        .context("rbac-extended-owner: execute")?
        .get(0);
    ensure!(val == 11, "rbac-extended-owner: val {val}, expected 11");
    Ok(())
}

/// A detached job runs in its submitter's domain, not in an unconfined one.
async fn detached_jobs_keep_the_domain(alice: &Client, bob: &Client) -> Result<()> {
    let template = "SELECT count(*) AS n FROM {t}";
    let hidden = job_outcome(bob, &template.replace("{t}", OWNED)).await?;
    let missing = job_outcome(bob, &template.replace("{t}", MISSING)).await?;
    ensure!(
        hidden.0 == "failed" && hidden == missing,
        "rbac-job-hidden: bob's job on {OWNED} ended {hidden:?}; on a missing \
         table {missing:?}"
    );
    let sql = template.replace("{t}", OWNED);
    let alices = submit(alice, &sql).await?;
    let state = wait(alice, &alices).await?;
    ensure!(
        state == "succeeded",
        "rbac-job-owner: alice's job ended {state}"
    );
    Ok(())
}

/// A job belongs to its submitter's domain as well as user and database:
/// the same `sub` with another group, in the shared namespace, can neither
/// list, poll, wait on, cancel nor read alice's team-A job - each fails as
/// for a job that does not exist - and its `job_key` names a job of its own.
async fn jobs_are_confined_to_the_domain(h: &Harness, alice: &Client) -> Result<()> {
    const KEY: &str = "rbac-shared-job-key";
    const MISSING_JOB: &str = "00000000-0000-4000-8000-000000000000";
    let alice_b = connect(
        h,
        "rbac-job-same-sub",
        &h.lake_namespace,
        "alice",
        &h.token("alice", RBAC_TEAM_B)?,
    )
    .await?;
    let sql = format!("SELECT count(*) AS n FROM {OWNED}");

    let team_a = submit_with_key(alice, &sql, KEY).await?;
    let state = wait(alice, &team_a).await?;
    ensure!(
        state == "succeeded",
        "rbac-job-same-sub: team A's job ended {state}"
    );

    for (id, probe) in [
        (
            "rbac-job-list",
            "SELECT count(*) FROM lakesoul_queries() WHERE query_id = '{q}'",
        ),
        (
            "rbac-job-status",
            "SELECT count(*) FROM lakesoul_query_status('{q}')",
        ),
    ] {
        let seen: i64 = alice_b
            .query_one(&probe.replace("{q}", &team_a), &[])
            .await
            .with_context(|| format!("{id}: {probe}"))?
            .get(0);
        ensure!(seen == 0, "{id}: team B sees team A's job {team_a}");
    }
    for (id, probe) in [
        (
            "rbac-job-result",
            "SELECT * FROM lakesoul_query_result('{q}')",
        ),
        (
            "rbac-job-result-set",
            "SELECT * FROM lakesoul_query_result_set('{q}', 0)",
        ),
        (
            "rbac-job-result-sets",
            "SELECT * FROM lakesoul_query_result_sets('{q}')",
        ),
        ("rbac-job-wait", "SELECT lakesoul_wait_query('{q}', 0)"),
        ("rbac-job-cancel", "SELECT lakesoul_cancel_query('{q}')"),
    ] {
        let hidden = server_error(&alice_b, &probe.replace("{q}", &team_a)).await?;
        let missing = server_error(&alice_b, &probe.replace("{q}", MISSING_JOB)).await?;
        ensure!(
            (hidden.0.as_str(), hidden.1.replace(&team_a, "<q>"))
                == (missing.0.as_str(), missing.1.replace(MISSING_JOB, "<q>")),
            "{id}: team A's job must fail like a missing one for team B; \
             hidden: {hidden:?}, missing: {missing:?}"
        );
    }

    let team_b = submit_with_key(&alice_b, &sql, KEY).await?;
    ensure!(
        team_b != team_a,
        "rbac-job-key: team B's job_key {KEY:?} deduplicated onto team A's job"
    );
    wait(&alice_b, &team_b).await?;
    let again = submit_with_key(alice, &sql, KEY).await?;
    ensure!(
        again == team_a,
        "rbac-job-key: team A's repeated job_key gave {again}, expected {team_a}"
    );
    let n: i64 = alice
        .query_one(
            &format!("SELECT n FROM lakesoul_query_result('{team_a}')"),
            &[],
        )
        .await
        .context("rbac-job-owner-result")?
        .get(0);
    ensure!(n == 2, "rbac-job-owner-result: team A read {n}, expected 2");
    Ok(())
}

/// Subscriptions and IVM maintenance cannot apply the filter, so they are
/// refused outright.
async fn unfiltered_paths_are_refused(alice: &Client) -> Result<()> {
    expect_sqlstate(
        alice,
        "rbac-subscription-refused",
        "DROP SUBSCRIPTION IF EXISTS rbac_no_such_subscription",
        &SqlState::INSUFFICIENT_PRIVILEGE,
    )
    .await?;
    alice.batch_execute("SET lakesoul.ivm.enabled = on").await?;
    let refused = expect_sqlstate(
        alice,
        "rbac-ivm-refused",
        &format!("INSERT INTO {OWNED} SELECT id + 10, val FROM {OWNED}"),
        &SqlState::INSUFFICIENT_PRIVILEGE,
    )
    .await;
    alice
        .batch_execute("SET lakesoul.ivm.enabled = off")
        .await?;
    refused
}

/// A table another engine creates in team A is resolved for alice at once,
/// listed for her after the next catalog refresh, and never seen by bob.
async fn cross_process_creation(h: &Harness, alice: &Client, bob: &Client) -> Result<()> {
    h.create_table_out_of_band(EXTERNAL, RBAC_TEAM_A, "alice")
        .await?;
    let count: i64 = alice
        .query_one(&format!("SELECT count(*) FROM {EXTERNAL}"), &[])
        .await
        .context("rbac-cross-process-owner: resolve before the refresh")?
        .get(0);
    ensure!(
        count == 0,
        "rbac-cross-process-owner: {count} rows, expected 0"
    );
    expect_like_missing(
        bob,
        "rbac-cross-process-hidden",
        "SELECT * FROM {t}",
        EXTERNAL,
    )
    .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !listed_everywhere(alice, EXTERNAL).await? {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "rbac-cross-process-listed: {EXTERNAL} never appeared in alice's catalog"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ensure!(
        !listed_anywhere(bob, EXTERNAL).await?,
        "rbac-cross-process-hidden: bob's catalog lists {EXTERNAL}"
    );
    expect_like_missing(bob, "rbac-cross-process-drop", "DROP TABLE {t}", EXTERNAL).await
}

/// A table name no suite ever creates.
const MISSING: &str = "rbac_no_such_table";

/// The server's `(SQLSTATE, message)` for `sql`, which must fail.
async fn server_error(client: &Client, sql: &str) -> Result<(String, String)> {
    match client.batch_execute(sql).await {
        Ok(()) => bail!("{sql:?} succeeded"),
        Err(error) => db_error(&error, sql),
    }
}

/// The same for Parse of `sql`.
async fn parse_error(client: &Client, sql: &str) -> Result<(String, String)> {
    match client.prepare(sql).await {
        Ok(_) => bail!("Parse of {sql:?} succeeded"),
        Err(error) => db_error(&error, sql),
    }
}

fn db_error(error: &tokio_postgres::Error, sql: &str) -> Result<(String, String)> {
    let db = error
        .as_db_error()
        .with_context(|| format!("{sql:?}: expected a server error, got {error}"))?;
    Ok((db.code().code().to_string(), db.message().to_string()))
}

/// `template` (with `{t}` for the table) fails on `table` exactly as it
/// fails on [`MISSING`].
async fn expect_like_missing(
    client: &Client,
    id: &str,
    template: &str,
    table: &str,
) -> Result<()> {
    let hidden = server_error(client, &template.replace("{t}", table))
        .await
        .with_context(|| format!("{id}: on {table}"))?;
    let missing = server_error(client, &template.replace("{t}", MISSING))
        .await
        .with_context(|| format!("{id}: on {MISSING}"))?;
    ensure_same_error(id, table, &hidden, &missing)
}

fn ensure_same_error(
    id: &str,
    table: &str,
    hidden: &(String, String),
    missing: &(String, String),
) -> Result<()> {
    ensure!(
        hidden.0 == missing.0
            && hidden.1.replace(table, "<t>") == missing.1.replace(MISSING, "<t>"),
        "{id}: {table} must fail like a missing table; hidden: {hidden:?}, \
         missing: {missing:?}"
    );
    Ok(())
}

/// Submits `sql` as a detached job and returns its final state and SQLSTATE.
async fn job_outcome(client: &Client, sql: &str) -> Result<(String, Option<String>)> {
    let query_id = submit(client, sql).await?;
    let state = wait(client, &query_id).await?;
    let sqlstate: Option<String> = client
        .query_one(
            &format!("SELECT error_sqlstate FROM lakesoul_query_status('{query_id}')"),
            &[],
        )
        .await
        .context("lakesoul_query_status")?
        .get(0);
    Ok((state, sqlstate))
}

/// The names among `names` that `pg_database` lists, sorted.
async fn list_databases(client: &Client, names: &[&str]) -> Result<Vec<String>> {
    let quoted: Vec<String> = names.iter().map(|name| format!("'{name}'")).collect();
    let sql = format!(
        "SELECT datname FROM pg_catalog.pg_database WHERE datname IN ({}) \
         ORDER BY datname",
        quoted.join(", ")
    );
    Ok(client
        .query(&sql, &[])
        .await
        .with_context(|| sql.clone())?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

/// Whether `pg_class` or `pg_tables` lists `table`.
async fn listed_anywhere(client: &Client, table: &str) -> Result<bool> {
    let (in_class, in_tables) = catalog_listing(client, table).await?;
    Ok(in_class || in_tables)
}

/// Whether both `pg_class` and `pg_tables` list `table`. The two are read
/// one after the other, so a catalog refresh in between can make them
/// differ for a moment; a caller waiting for a table polls this.
async fn listed_everywhere(client: &Client, table: &str) -> Result<bool> {
    let (in_class, in_tables) = catalog_listing(client, table).await?;
    Ok(in_class && in_tables)
}

/// Whether `pg_class` and `pg_tables`, in that order, list `table`.
async fn catalog_listing(client: &Client, table: &str) -> Result<(bool, bool)> {
    let in_class = !client
        .query(
            &format!("SELECT relname FROM pg_catalog.pg_class WHERE relname = '{table}'"),
            &[],
        )
        .await
        .context("pg_class")?
        .is_empty();
    let in_tables = !client
        .query(
            &format!(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = '{table}'"
            ),
            &[],
        )
        .await
        .context("pg_tables")?
        .is_empty();
    Ok((in_class, in_tables))
}

/// The startup error of a refused connection.
async fn refused_startup(
    h: &Harness,
    database: &str,
    user: &str,
    token: &str,
) -> Result<String> {
    match h
        .connect_attempt(&h.lake_config_as(database, user, Some(token)))
        .await?
    {
        Ok(_) => bail!("connected to {database:?}, which must not exist for {user}"),
        Err(error) => Ok(error
            .as_db_error()
            .with_context(|| format!("expected a server error, got {error}"))?
            .message()
            .to_string()),
    }
}

async fn expect_sqlstate(
    client: &Client,
    id: &str,
    sql: &str,
    expected: &SqlState,
) -> Result<()> {
    match client.batch_execute(sql).await {
        Ok(()) => bail!("{id}: {sql:?} succeeded, expected {}", expected.code()),
        Err(error) => {
            let db = error
                .as_db_error()
                .with_context(|| format!("{id}: expected a server error, got {error}"))?;
            ensure!(
                db.code() == expected,
                "{id}: {sql:?} gave {} ({}), expected {}",
                db.code().code(),
                db.message(),
                expected.code()
            );
            Ok(())
        }
    }
}

async fn submit(client: &Client, sql: &str) -> Result<String> {
    Ok(client
        .query_one(
            &format!("SELECT lakesoul_submit($lakesoul_job${sql}$lakesoul_job$)"),
            &[],
        )
        .await
        .context("lakesoul_submit")?
        .get(0))
}

async fn submit_with_key(client: &Client, sql: &str, key: &str) -> Result<String> {
    Ok(client
        .query_one(
            &format!(
                "SELECT lakesoul_submit($lakesoul_job${sql}$lakesoul_job$, '{key}')"
            ),
            &[],
        )
        .await
        .context("lakesoul_submit with a job_key")?
        .get(0))
}

async fn wait(client: &Client, query_id: &str) -> Result<String> {
    Ok(client
        .query_one(
            &format!("SELECT lakesoul_wait_query('{query_id}', 30000)"),
            &[],
        )
        .await
        .context("lakesoul_wait_query")?
        .get(0))
}

/// Every bad password is `FATAL 28P01`; a good token connects.
async fn jwt_rejects(h: &Harness) -> Result<()> {
    let database = h.lake_namespace.as_str();

    // The harness's own connection authenticated with a token.
    expect_item_count(&h.lake, "jwt-harness-connection").await?;

    // The server asks for a password, and a client without one gives up
    // before answering: the handshake cannot complete without a token.
    match h
        .connect_attempt(&h.lake_config_as(database, LAKE_USER, None))
        .await?
    {
        Ok(_) => bail!("jwt-no-password: connected without a password"),
        // tokio-postgres reports this as a configuration error whose
        // Display is only "invalid configuration"; the cause is in Debug.
        Err(error) => ensure!(
            error.as_db_error().is_none()
                && format!("{error:?}").contains("password missing"),
            "jwt-no-password: expected the driver to refuse for a missing password, got {error:?}"
        ),
    }

    let foreign_secret = JwtServer::new("not-the-metadata-client-secret")
        .create_token(&Claims {
            sub: LAKE_USER.to_string(),
            group: "public".to_string(),
            exp: usize::try_from(chrono::Utc::now().timestamp() + 3600)?,
        })
        .context("sign a token with a foreign secret")?;
    for (id, password) in [
        ("jwt-empty-password", String::new()),
        ("jwt-garbage-token", "not-a-token".to_string()),
        ("jwt-expired-token", h.expired_token(LAKE_USER, "public")?),
        (
            "jwt-token-for-another-user",
            h.token("someone_else", "public")?,
        ),
        ("jwt-wrong-signature", foreign_secret),
    ] {
        let config = h.lake_config_as(database, LAKE_USER, Some(&password));
        let message = expect_invalid_password(h, id, &config).await?;
        for detail in ["expired", "signature", "someone_else", "token"] {
            ensure!(
                !message.to_lowercase().contains(detail),
                "{id}: the refusal must not say why it failed: {message:?}"
            );
        }
    }

    // A token for another user is valid for that user, and the refusals
    // above left the server serving.
    let token = h.token("alice", "team_a")?;
    let alice = connect(h, "jwt-valid-token", database, "alice", &token).await?;
    expect_item_count(&alice, "jwt-valid-token").await
}

/// `--jwt-auth` without domain enforcement shows every namespace, whatever
/// the token's group.
async fn auth_only_mode(h: &Harness) -> Result<()> {
    let other = h.namespace_of(JWT_OTHER_DOMAIN)?;
    let token = h.token("bob", "a_group_that_owns_nothing")?;
    let bob = connect(h, "auth-only-mode", &h.lake_namespace, "bob", &token).await?;

    let sql = format!(
        "SELECT datname FROM pg_catalog.pg_database \
         WHERE datname IN ('{}', '{other}') ORDER BY datname",
        h.lake_namespace
    );
    let listed: Vec<String> = bob
        .query(&sql, &[])
        .await
        .with_context(|| format!("auth-only-mode: {sql}"))?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let mut expected = vec![h.lake_namespace.clone(), other.to_string()];
    expected.sort();
    ensure!(
        listed == expected,
        "auth-only-mode: pg_database must list the {JWT_OTHER_DOMAIN} namespace too; \
         listed {listed:?}, expected {expected:?}"
    );

    let in_other = connect(h, "auth-only-mode-connect", other, "bob", &token).await?;
    let database: String = in_other
        .query_one("SELECT current_database()::text", &[])
        .await
        .context("auth-only-mode: current_database()")?
        .get(0);
    ensure!(
        database == other,
        "auth-only-mode: connected to {database:?}, expected {other:?}"
    );
    Ok(())
}

async fn connect(
    h: &Harness,
    id: &str,
    database: &str,
    user: &str,
    token: &str,
) -> Result<Client> {
    h.ensure_token_user(token).await?;
    h.connect_attempt(&h.lake_config_as(database, user, Some(token)))
        .await?
        .with_context(|| format!("{id}: a valid token for {user:?} must connect"))
}

/// The refusal's message, after checking it is `FATAL 28P01`.
async fn expect_invalid_password(
    h: &Harness,
    id: &str,
    config: &tokio_postgres::Config,
) -> Result<String> {
    let error = match h.connect_attempt(config).await? {
        Ok(_) => bail!("{id}: the server accepted the connection"),
        Err(error) => error,
    };
    let db = error
        .as_db_error()
        .with_context(|| format!("{id}: expected a server ErrorResponse, got {error}"))?;
    ensure!(
        db.code() == &SqlState::INVALID_PASSWORD && db.severity() == "FATAL",
        "{id}: expected FATAL 28P01, got {} {} ({})",
        db.severity(),
        db.code().code(),
        db.message()
    );
    Ok(db.message().to_string())
}

/// The base fixture `items` holds four rows.
async fn expect_item_count(client: &Client, id: &str) -> Result<()> {
    let count: i64 = client
        .query_one("SELECT count(*) FROM items", &[])
        .await
        .with_context(|| format!("{id}: SELECT count(*) FROM items"))?
        .get(0);
    ensure!(count == 4, "{id}: items has {count} rows, expected 4");
    Ok(())
}
