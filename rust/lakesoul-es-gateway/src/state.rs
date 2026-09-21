// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared gateway state.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

use crate::config::{GatewayConfig, IndexConfig, IndexDefaults};
use crate::error::EsError;
use crate::schema;

/// Resolved runtime of one declared index.
#[derive(Debug, Clone)]
pub struct IndexRuntime {
    pub config: IndexConfig,
    pub namespace: String,
    pub table: String,
    pub path: String,
    pub dim: Option<usize>,
    pub schema: SchemaRef,
}

impl IndexRuntime {
    pub fn hash_bucket_num(&self, defaults: &IndexDefaults) -> usize {
        self.config
            .hash_bucket_num
            .unwrap_or(defaults.hash_bucket_num)
    }

    pub fn tokenizer<'a>(&'a self, defaults: &'a IndexDefaults) -> &'a str {
        self.config
            .tokenizer
            .as_deref()
            .unwrap_or(defaults.tokenizer.as_str())
    }

    pub fn with_positions(&self, defaults: &IndexDefaults) -> bool {
        self.config
            .with_positions
            .unwrap_or(defaults.with_positions)
    }

    pub fn table_ref(&self) -> TableReference {
        TableReference::partial(self.namespace.clone(), self.table.clone())
    }
}

/// Gateway-wide state shared by every handler.
pub struct GatewayState {
    pub config: GatewayConfig,
    pub client: MetaDataClientRef,
    pub session: Arc<SessionContext>,
    pub indexes: HashMap<String, IndexRuntime>,
}

impl GatewayState {
    pub async fn try_new(config: GatewayConfig) -> anyhow::Result<Self> {
        let client = Arc::new(
            MetaDataClient::from_env()
                .await
                .map_err(|error| anyhow::anyhow!("metadata client: {error}"))?,
        );
        let args = core_args(&config);
        let session =
            lakesoul_datafusion::create_lakesoul_session_ctx(client.clone(), &args)
                .map_err(|error| anyhow::anyhow!("datafusion session: {error}"))?;

        let mut indexes = HashMap::new();
        for index in &config.indexes {
            let table = index.table_name().to_string();
            let path = table_path(&config, index, &table);
            let schema = schema::document_schema(
                &index.content_column,
                index.dim.map(|dim| (index.embedding_column.as_str(), dim)),
            );
            indexes.insert(
                index.name.clone(),
                IndexRuntime {
                    config: index.clone(),
                    namespace: config.lakesoul.namespace.clone(),
                    table,
                    path,
                    dim: index.dim,
                    schema,
                },
            );
        }

        // Register every index path's object store on the session runtime so
        // the text-index search can read splits for paths outside the
        // warehouse prefix as well.
        let mut store_config =
            lakesoul_io::config::LakeSoulIOConfigBuilder::new_with_object_store_options(
                args.s3_options(),
            )
            .build();
        for runtime in indexes.values() {
            lakesoul_io::object_store::register_object_store(
                &runtime.path,
                &mut store_config,
                &session.runtime_env(),
            )
            .map_err(|error| {
                anyhow::anyhow!(
                    "failed to register object store for '{}': {error}",
                    runtime.path
                )
            })?;
        }

        Ok(Self {
            config,
            client,
            session,
            indexes,
        })
    }

    /// Look up a declared index.
    pub fn index(&self, name: &str) -> Result<&IndexRuntime, EsError> {
        self.indexes
            .get(name)
            .ok_or_else(|| EsError::index_not_found(name))
    }

    /// Open the LakeSoul table of an index.
    pub async fn lake_soul_table(
        &self,
        runtime: &IndexRuntime,
    ) -> anyhow::Result<LakeSoulTable> {
        LakeSoulTable::for_namespace_and_name(
            &runtime.namespace,
            &runtime.table,
            Some(self.client.clone()),
        )
        .await
        .map_err(|error| anyhow::anyhow!("table '{}': {error}", runtime.table))
    }

    /// Write a record batch through the LakeSoul upsert path; the write
    /// commit synchronously builds the declared indexes, so the rows become
    /// searchable before the request returns.
    pub async fn upsert(
        &self,
        runtime: &IndexRuntime,
        batch: arrow_array::RecordBatch,
    ) -> Result<(), EsError> {
        let table = self
            .lake_soul_table(runtime)
            .await
            .map_err(crate::error::internal)?;
        table
            .execute_upsert(batch)
            .await
            .map_err(crate::error::internal)
    }
}

/// Storage path of a gateway table.
fn table_path(config: &GatewayConfig, index: &IndexConfig, table: &str) -> String {
    if let Some(path) = &index.path {
        return path.clone();
    }
    if let Some(prefix) = &config.lakesoul.warehouse_prefix {
        return format!("{}/{}", prefix.trim_end_matches('/'), table);
    }
    let cwd = std::env::current_dir().unwrap_or_default();
    format!(
        "file://{}/{}/{}",
        cwd.display(),
        config.lakesoul.namespace,
        table
    )
}

/// Translate the gateway configuration into the datafusion session arguments
/// (S3 credentials, warehouse prefix, worker threads).
fn core_args(config: &GatewayConfig) -> CoreArgs {
    CoreArgs {
        warehouse_prefix: config.lakesoul.warehouse_prefix.clone(),
        endpoint: config.lakesoul.endpoint.clone(),
        s3_bucket: config.lakesoul.s3_bucket.clone(),
        s3_access_key: config.lakesoul.s3_access_key.clone(),
        s3_secret_key: config.lakesoul.s3_secret_key.clone(),
        s3_virtual_host_style: config.lakesoul.s3_virtual_host_style,
        worker_threads: 2,
    }
}
