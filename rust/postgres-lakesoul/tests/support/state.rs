// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use datafusion::arrow::util::display::array_value_to_string;
use tokio_postgres::{Client, Config, NoTls};

use super::{
    Harness, OPERATION_TIMEOUT, StartupState, bounded, configure_session, connect_pg,
    lake_config, management_execute,
};

impl Harness {
    /// Driver tasks remain owned by teardown, including on a suite error or panic.
    pub async fn connect_lake(&self) -> Result<Client> {
        self.connect_database(&self.lake_namespace).await
    }

    /// Select a database at connection startup; never mutate a shared session.
    pub async fn connect_database(&self, database: &str) -> Result<Client> {
        let config = self
            .state
            .authenticated(lake_config(self.lake_addr, database))?;
        self.connect_extra(&config, false).await
    }

    /// The startup configuration for `user` on `database`, with `password`
    /// sent as is and no token added.
    pub fn lake_config_as(
        &self,
        database: &str,
        user: &str,
        password: Option<&str>,
    ) -> Config {
        let mut config = lake_config(self.lake_addr, database);
        config.user(user);
        if let Some(password) = password {
            config.password(password);
        }
        config
    }

    /// One raw handshake: the driver's own error when the server refuses it,
    /// so a test can read the SQLSTATE. The outer error is a timeout.
    pub async fn connect_attempt(
        &self,
        config: &Config,
    ) -> Result<std::result::Result<Client, tokio_postgres::Error>> {
        let attempt = tokio::time::timeout(OPERATION_TIMEOUT, config.connect(NoTls))
            .await
            .context("connection attempt timed out")?;
        Ok(attempt.map(|(client, connection)| {
            self.state.connections.lock().push(tokio::spawn(async move {
                let _ = connection.await;
            }));
            client
        }))
    }

    /// A token for `user` in `group`, signed with the server's secret and
    /// valid for an hour. Only in a [`super::run_jwt_suite`].
    pub fn token(&self, user: &str, group: &str) -> Result<String> {
        super::sign_token(self.jwt()?, user, group, 3600)
    }

    /// Make the directory entry a positive token test needs. The server
    /// checks `domain_user` when it starts with `--rbac`.
    pub async fn ensure_token_user(&self, token: &str) -> Result<()> {
        let claims = self
            .jwt()?
            .decode_token(token)
            .context("decode the harness token")?;
        self.ensure_domain_user(&claims.group, &claims.sub).await
    }

    /// Seed a domain, its role groups (without touching base roles) and one
    /// member. Idempotent, so parallel suites sharing a domain are safe.
    pub async fn ensure_domain_user(&self, domain: &str, user: &str) -> Result<()> {
        let client = self.connect_reference().await?;
        seed_domain_user_on(&client, domain, user).await
    }

    /// The same, expired a day ago: well past the validator's leeway.
    pub fn expired_token(&self, user: &str, group: &str) -> Result<String> {
        super::sign_token(self.jwt()?, user, group, -86_400)
    }

    /// The namespace this suite created in `domain`.
    pub fn namespace_of(&self, domain: &str) -> Result<&str> {
        self.state
            .domain_namespaces
            .iter()
            .find(|(owner, _)| owner == domain)
            .map(|(_, namespace)| namespace.as_str())
            .with_context(|| format!("this suite created no namespace in {domain:?}"))
    }

    /// The `domain` column of `table`'s `table_info`, `table_name_id` and
    /// `table_path_id` rows in the base namespace, read from the metadata
    /// database itself.
    pub async fn stored_domains(&self, table: &str) -> Result<Vec<String>> {
        let client = self.connect_reference().await?;
        let mut domains = Vec::new();
        for (relation, sql) in [
            (
                "table_info",
                "SELECT domain FROM public.table_info \
                 WHERE table_namespace = $1 AND table_name = $2",
            ),
            (
                "table_name_id",
                "SELECT domain FROM public.table_name_id \
                 WHERE table_namespace = $1 AND table_name = $2",
            ),
            (
                // `table_path_id` has no name column: reach it by id.
                "table_path_id",
                "SELECT p.domain FROM public.table_path_id p \
                 JOIN public.table_info t ON t.table_id = p.table_id \
                 WHERE t.table_namespace = $1 AND t.table_name = $2",
            ),
        ] {
            let row = client
                .query_one(sql, &[&self.lake_namespace, &table])
                .await
                .with_context(|| format!("read {relation}.domain of {table}"))?;
            domains.push(row.get(0));
        }
        Ok(domains)
    }

    /// Creates `table` in the base namespace and `domain` the way another
    /// engine would: straight in the metadata database, in one transaction,
    /// so this server learns of it only from metadata. The definition is
    /// `items`'s; the new table has no data.
    pub async fn create_table_out_of_band(
        &self,
        table: &str,
        domain: &str,
        creator: &str,
    ) -> Result<()> {
        self.write_table_out_of_band(table, domain, creator, false)
            .await
    }

    /// Drops `table` from the base namespace and recreates the name in
    /// `domain`, as another engine would, in one metadata transaction: the
    /// name now belongs to a different table, which this server learns of
    /// only at its next catalog refresh. The old table must have no data.
    pub async fn replace_table_out_of_band(
        &self,
        table: &str,
        domain: &str,
        creator: &str,
    ) -> Result<()> {
        self.write_table_out_of_band(table, domain, creator, true)
            .await
    }

    async fn write_table_out_of_band(
        &self,
        table: &str,
        domain: &str,
        creator: &str,
        replace: bool,
    ) -> Result<()> {
        let client = self.connect_reference().await?;
        let table_id = format!("table_{}", uuid::Uuid::new_v4());
        let namespace = &self.lake_namespace;
        // A path of its own: a replacement is a different table.
        let table_path = format!(
            "file://{}/{namespace}/{table}_{}",
            self.warehouse_dir.to_string_lossy(),
            &table_id[6..14]
        );
        // Every value is harness-generated: no quoting concerns.
        let drop_old = if replace {
            format!(
                "DELETE FROM public.table_path_id WHERE table_id IN
                   (SELECT table_id FROM public.table_info
                    WHERE table_namespace = '{namespace}' AND table_name = '{table}');
                 DELETE FROM public.table_name_id
                   WHERE table_namespace = '{namespace}' AND table_name = '{table}';
                 DELETE FROM public.table_info
                   WHERE table_namespace = '{namespace}' AND table_name = '{table}';"
            )
        } else {
            String::new()
        };
        let sql = format!(
            "BEGIN;
             {drop_old}
             INSERT INTO public.table_info
               SELECT (jsonb_populate_record(NULL::public.table_info,
                 to_jsonb(t) || jsonb_build_object(
                   'table_id', '{table_id}', 'table_name', '{table}',
                   'table_path', '{table_path}', 'domain', '{domain}',
                   'creator', '{creator}'))).*
               FROM public.table_info t
               WHERE t.table_namespace = '{namespace}' AND t.table_name = 'items';
             INSERT INTO public.table_name_id (table_name, table_id, table_namespace, domain)
               VALUES ('{table}', '{table_id}', '{namespace}', '{domain}');
             INSERT INTO public.table_path_id (table_path, table_id, table_namespace, domain)
               VALUES ('{table_path}', '{table_id}', '{namespace}', '{domain}');
             COMMIT;"
        );
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("write {table} in {domain} out of band"))?;
        Ok(())
    }

    fn jwt(&self) -> Result<&lakesoul_metadata::JwtServer> {
        self.state
            .jwt
            .as_deref()
            .context("tokens exist only in a JWT suite")
    }

    pub async fn connect_reference(&self) -> Result<Client> {
        self.connect_extra(&self.state.reference_config, true).await
    }

    /// Connect through the real server without applying [`SESSION_SETTINGS`],
    /// so the connection's process-startup defaults stay observable.
    ///
    /// The label matches the harness's primary LakeSoul client, so
    /// `application_name` and every other startup-seeded value are directly
    /// comparable. Used by the time-zone/reset contract to derive the expected
    /// post-`RESET`/`DISCARD` state instead of re-pinning current output.
    pub async fn connect_lake_startup_defaults(&self) -> Result<Client> {
        let config = self
            .state
            .authenticated(lake_config(self.lake_addr, &self.lake_namespace))?;
        let (client, task) = connect_pg(&config, "sql_compat_lake").await?;
        self.state.connections.lock().push(task);
        Ok(client)
    }

    async fn connect_extra(&self, config: &Config, reference: bool) -> Result<Client> {
        let (client, task) = connect_pg(config, "sql_compat_additional").await?;
        self.state.connections.lock().push(task);
        configure_session(&client).await?;
        if reference {
            client
                .batch_execute(&format!(
                    "SET search_path TO {}, pg_catalog",
                    self.reference_schema
                ))
                .await?;
        }
        Ok(client)
    }

    /// No query passes through the read-only service. Include committed and
    /// uncommitted metadata, management-visible values, and exact warehouse
    /// bytes, so a rejected write cannot hide orphan files behind a failed commit.
    pub async fn authoritative_snapshot(&self) -> Result<String> {
        bounded(OPERATION_TIMEOUT, "authoritative no-side-effect snapshot", async {
            let client = self.connect_reference().await?;
            let mut metadata = BTreeMap::new();
            for (table, predicate) in [
                ("namespace", "namespace = $1"),
                ("table_info", "table_namespace = $1"),
                ("table_name_id", "table_namespace = $1"),
                ("table_path_id", "table_namespace = $1"),
                ("partition_info", "table_id IN (SELECT table_id FROM public.table_info WHERE table_namespace = $1)"),
                ("data_commit_info", "table_id IN (SELECT table_id FROM public.table_info WHERE table_namespace = $1)"),
            ] {
                let sql = format!("SELECT row_to_json(t)::text FROM public.{table} t WHERE {predicate} ORDER BY row_to_json(t)::text COLLATE \"C\"");
                let rows = client.query(&sql, &[&self.lake_namespace]).await
                    .map_err(|error| anyhow::anyhow!("authoritative metadata table={table} SQL={sql}: {error:?}"))?;
                metadata.insert(table, rows.iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>());
            }
            let mut data = BTreeMap::new();
            for table in ["items", "empty_items", "pk_items"] {
                let sql = format!("SELECT id, val, label FROM lakesoul.{}.{table} ORDER BY id NULLS LAST, val NULLS LAST, label NULLS LAST", self.lake_namespace);
                let batches = self.state.management.sql(&sql).await?.collect().await?;
                let mut rows = Vec::new();
                for batch in &batches {
                    for row in 0..batch.num_rows() {
                        let values = batch.columns().iter().map(|column| {
                            if column.is_null(row) {
                                Ok(None)
                            } else {
                                array_value_to_string(column, row).map(Some)
                            }
                        }).collect::<std::result::Result<Vec<_>, _>>()?;
                        rows.push(values);
                    }
                }
                data.insert(table, rows);
            }
            let mut files = BTreeMap::new();
            snapshot_files(&self.warehouse_dir, &self.warehouse_dir, &mut files)?;
            Ok::<_, anyhow::Error>(serde_json::to_string(&(metadata, data, files))?)
        }).await
    }

    /// Exact bytes of every regular file under one table's warehouse
    /// directory, keyed by table-relative path.
    ///
    /// A committed update appends a new version; files a statement did not
    /// rewrite or replace must keep their exact bytes, so the caller can
    /// compare this map before and after.
    pub async fn table_file_bytes(
        &self,
        table: &str,
    ) -> Result<BTreeMap<String, Vec<u8>>> {
        let directory = self.warehouse_dir.join(&self.lake_namespace).join(table);
        bounded(OPERATION_TIMEOUT, "table file bytes", async move {
            let mut files = BTreeMap::new();
            if !directory.exists() {
                return Ok(BTreeMap::new());
            }
            snapshot_files(&directory, &directory, &mut files)?;
            Ok::<_, anyhow::Error>(
                files
                    .into_iter()
                    .filter_map(|(path, bytes)| {
                        bytes.map(|bytes| (path.to_string_lossy().into_owned(), bytes))
                    })
                    .collect(),
            )
        })
        .await
    }

    /// A single management commit followed by a primary PostgreSQL metadata
    /// barrier. Never poll a SELECT on the measured LakeSoul connection.
    pub async fn commit_pk_rows(&self, values: &str) -> Result<()> {
        let authority = self.connect_reference().await?;
        let before = self.pk_publication(&authority).await?;
        management_execute(
            &self.state.management,
            &format!(
                "INSERT OR REPLACE INTO lakesoul.{}.pk_items VALUES {values}",
                self.lake_namespace
            ),
        )
        .await?;
        let after = self.pk_publication(&authority).await?;
        ensure!(
            after.0 == before.0 + 1,
            "commit barrier: expected next partition version after {before:?}, got {after:?}"
        );
        ensure!(
            after.1 == before.1 + 1
                && after.2 == i64::from(after.1)
                && after.3 == before.3 + 1,
            "commit barrier: latest snapshot must include every published commit and exactly one new committed data record: before {before:?}, after {after:?}"
        );
        eprintln!(
            "sql-compat management commit barrier: namespace={} version={} snapshot_commits={} committed_records={}",
            self.lake_namespace, after.0, after.1, after.3
        );
        Ok(())
    }

    async fn pk_publication(&self, authority: &Client) -> Result<(i32, i32, i64, i64)> {
        let sql = "WITH target AS (SELECT table_id FROM public.table_info WHERE table_namespace = $1 AND table_name = 'pk_items'), latest AS (SELECT version, snapshot FROM public.partition_info WHERE table_id = (SELECT table_id FROM target) ORDER BY version DESC LIMIT 1) SELECT version, cardinality(snapshot), (SELECT count(*) FROM public.data_commit_info c WHERE c.table_id = (SELECT table_id FROM target) AND c.committed AND c.commit_id = ANY(latest.snapshot)), (SELECT count(*) FROM public.data_commit_info c WHERE c.table_id = (SELECT table_id FROM target) AND c.committed) FROM latest";
        let row = bounded(
            OPERATION_TIMEOUT,
            "primary metadata publication barrier",
            authority.query_one(sql, &[&self.lake_namespace]),
        )
        .await?;
        Ok((
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            row.try_get(3)?,
        ))
    }
}

impl StartupState {
    /// Table names the metadata database still lists for `namespace`.
    ///
    /// Cleanup drops the tables the fixture created, but a suite can also
    /// create (or fail to drop) tables through the server under test. Those
    /// are not in the fixture bookkeeping, so the namespace is enumerated in
    /// metadata instead of trusting the recorded list to be complete.
    pub async fn metadata_table_names(&self, namespace: &str) -> Result<Vec<String>> {
        let (client, task) =
            connect_pg(&self.reference_config, "sql_compat_cleanup").await?;
        self.connections.lock().push(task);
        let rows = bounded(
            OPERATION_TIMEOUT,
            "list metadata tables",
            client.query(
                "SELECT table_name FROM public.table_info WHERE table_namespace = $1 ORDER BY table_name",
                &[&namespace],
            ),
        )
        .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }
}

fn snapshot_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<PathBuf, Option<Vec<u8>>>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("snapshot warehouse {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_dir() {
            files.insert(path.strip_prefix(root)?.to_path_buf(), None);
            snapshot_files(root, &path, files)?;
        } else {
            ensure!(
                kind.is_file(),
                "unexpected non-regular warehouse entry {}",
                path.display()
            );
            files.insert(
                path.strip_prefix(root)?.to_path_buf(),
                Some(std::fs::read(&path)?),
            );
        }
    }
    Ok(())
}

/// Test-only user-directory seeding. The domain role names are test constants
/// or generated UUID names; reject anything that is not a plain identifier so
/// this helper never interpolates arbitrary SQL.
pub(super) async fn seed_domain_user_on(
    client: &Client,
    domain: &str,
    user: &str,
) -> Result<()> {
    let valid = |value: &str| {
        !value.is_empty()
            && value.len() <= 63
            && value.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_alphabetic()
                    || byte == b'_'
                    || (index > 0 && byte.is_ascii_digit())
            })
    };
    ensure!(valid(domain), "invalid test domain {domain:?}");
    ensure!(valid(user), "invalid test user {user:?}");
    let role_sql = if matches!(domain, "public" | "lake-public") {
        String::new()
    } else {
        format!(
            r#"
            do $$
            begin
                begin
                    execute 'create role "{domain}"';
                exception when duplicate_object then null;
                end;
                begin
                    execute 'create role "{domain}_users"';
                exception when duplicate_object then null;
                end;
                begin
                    execute 'create role "{domain}_admins"';
                exception when duplicate_object then null;
                end;
            end $$;
            "#
        )
    };
    client
        .batch_execute(&format!(
            r#"
            insert into public.domain(domain, creator)
            values ('{domain}', 'test-fixture') on conflict (domain) do nothing;
            {role_sql}
            insert into public.lakesoul_user(user_name, created_by_domain)
            values ('{user}', '{domain}') on conflict (user_name) do nothing;
            insert into public.domain_user(domain, user_name, created_by)
            values ('{domain}', '{user}', 'test-fixture')
            on conflict (domain, user_name) do nothing;
            "#
        ))
        .await
        .with_context(|| format!("seed user {user:?} in domain {domain:?}"))
}
