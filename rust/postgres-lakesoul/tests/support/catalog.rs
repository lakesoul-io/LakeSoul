// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Namespace/database fixtures share one server but never share user objects.

use std::path::Path;

use anyhow::{Context, Result, ensure};

use super::{Harness, OPERATION_TIMEOUT, StartupState, bounded, management_execute};

impl Harness {
    pub fn catalog_peer_namespace(&self) -> Result<&str> {
        self.state
            .extra_namespaces
            .first()
            .map(|(namespace, _)| namespace.as_str())
            .context(
                "catalog suite must prepare a second namespace before server startup",
            )
    }
}

impl StartupState {
    pub(super) async fn create_catalog_fixtures(&mut self) -> Result<()> {
        let primary = self.namespace.clone();
        let peer = format!("{primary}_peer");
        management_execute(&self.management, &format!("CREATE SCHEMA lakesoul.{peer}"))
            .await?;
        // Record the empty namespace immediately, including if its first table
        // creation/write fails. The existing teardown owns both namespaces.
        self.extra_namespaces.push((peer.clone(), Vec::new()));
        for (namespace, name, columns, values) in [
            (
                primary.as_str(),
                "mapping_items",
                "id INTEGER, origin TEXT",
                "(101,'alpha'),(102,'alpha')",
            ),
            (primary.as_str(), "only_a", "id INTEGER", "(501)"),
            (
                peer.as_str(),
                "mapping_items",
                "id INTEGER, origin TEXT",
                "(201,'beta'),(202,'beta')",
            ),
            (peer.as_str(), "only_b", "id INTEGER", "(601)"),
        ] {
            let sql = format!(
                "CREATE EXTERNAL TABLE lakesoul.{namespace}.{name} ({columns}) \
                 STORED AS LAKESOUL LOCATION '{}/{namespace}/{name}' \
                 OPTIONS ('file_format' 'parquet')",
                self.warehouse_url.trim_end_matches('/'),
            );
            management_execute(&self.management, &sql).await?;
            if namespace == primary {
                self.created_tables.push(name.to_owned());
            } else {
                self.extra_namespaces
                    .last_mut()
                    .expect("registered peer namespace")
                    .1
                    .push(name.to_owned());
            }
            management_execute(
                &self.management,
                &format!("INSERT INTO lakesoul.{namespace}.{name} VALUES {values}",),
            )
            .await?;
            bounded(OPERATION_TIMEOUT, "prove catalog fixture metadata and files", async {
                let info = self.meta.get_table_info_by_table_name(name, namespace)
                    .await.map_err(|error| anyhow::anyhow!("{error:?}"))?
                    .with_context(|| format!("missing metadata {namespace}.{name}"))?;
                ensure!(info.table_namespace == namespace,
                    "fixture {namespace}.{name} belongs to {}", info.table_namespace);
                let files = self.meta.get_data_files_by_table_name(name, namespace)
                    .await.map_err(|error| anyhow::anyhow!("{error:?}"))?;
                ensure!(!files.is_empty(), "fixture {namespace}.{name} has no published files");
                for file in &files {
                    let path = Path::new(file.strip_prefix("file://").unwrap_or(file));
                    ensure!(path.is_absolute() && path.is_file(), "fixture file missing: {file}");
                    ensure!(path.starts_with(&self.warehouse_dir), "fixture file outside owned warehouse: {file}");
                }
                eprintln!("catalog fixture proof: namespace={namespace} table={name} active_files={} existing_local_parquet=true", files.len());
                Ok::<_, anyhow::Error>(())
            }).await?;
        }
        Ok(())
    }
}
