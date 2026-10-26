// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared gateway state.
//!
//! Declared indexes are resolved at startup; when `defaults.dynamic_indexes`
//! is on, an index created by `PUT /{index}` is provisioned lazily from the
//! first written document (its embedding length fixes the vector dimension),
//! and an index whose table already exists is discovered from LakeSoul
//! metadata after a restart.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use arrow_schema::SchemaRef;
use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use lakesoul_common::ser::arrow_java::schema_from_table_info_metadata;
use lakesoul_datafusion::catalog::LakeSoulTableProperty;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_datafusion::text_index::parse_text_index_columns;
use lakesoul_datafusion::vector_index::parse_vector_index_columns;
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use lakesoul_metadata_proto::entity::TableInfo;
use tokio::sync::Mutex;

use crate::config::{GatewayConfig, IndexConfig, IndexDefaults};
use crate::error::EsError;
use crate::schema;

/// Resolved runtime of one index.
#[derive(Debug, Clone)]
pub struct IndexRuntime {
    pub config: IndexConfig,
    pub namespace: String,
    pub table: String,
    pub path: String,
    /// Declared vector columns `(name, dim)` in schema order; empty for a
    /// keyword-only index.
    pub vectors: Vec<(String, usize)>,
    pub schema: SchemaRef,
    /// True for an index created by `PUT /{index}` that has no table yet; the
    /// first write creates the table and fixes the vector dimension.
    pub pending: bool,
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

    /// True when the index declares at least one vector column.
    pub fn has_vectors(&self) -> bool {
        !self.vectors.is_empty()
    }

    /// The dimension of one declared vector column.
    pub fn vector_dim(&self, column: &str) -> Option<usize> {
        self.vectors
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, dim)| *dim)
    }

    /// The primary vector column (the `dim`/`embedding_column` shorthand, or
    /// the first declared column).
    pub fn primary_vector(&self) -> Option<&(String, usize)> {
        self.vectors.first()
    }

    /// True when `column` is a declared vector column.
    pub fn is_vector_column(&self, column: &str) -> bool {
        self.vector_dim(column).is_some()
    }

    pub fn nprobe(&self, defaults: &IndexDefaults) -> usize {
        self.config.nprobe.unwrap_or(defaults.nprobe)
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
    /// Declared indexes plus the dynamically created/discovered ones.
    pub indexes: RwLock<HashMap<String, Arc<IndexRuntime>>>,
    /// Object-store config used to register each index path on the session.
    store_config: Mutex<LakeSoulIOConfig>,
    /// Serializes dynamic provisioning (table creation + cache insert).
    provision_lock: Mutex<()>,
    /// Tables the deferred index maintenance should build, shared with the
    /// maintenance thread.  Declared indexes are added at startup, dynamic
    /// ones when they are provisioned or rediscovered.
    pub index_tables: Arc<std::sync::RwLock<HashSet<String>>>,
    /// Serializes upserts per index.  Concurrent write commits build the same
    /// index shard's secondary index and collide in the metadata catalog
    /// (duplicate index-commit generation / partition-info rows), so a
    /// multi-worker client (WeKnora indexes documents in parallel) must be
    /// let in one commit at a time.
    write_locks: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
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
            let runtime = declared_runtime(&config, index);
            indexes.insert(index.name.clone(), Arc::new(runtime));
        }

        let index_tables: HashSet<String> = indexes
            .values()
            .map(|runtime| runtime.table.clone())
            .collect();
        let state = Self {
            config,
            client,
            session,
            indexes: RwLock::new(indexes),
            index_tables: Arc::new(std::sync::RwLock::new(index_tables)),
            store_config: Mutex::new(
                LakeSoulIOConfigBuilder::new_with_object_store_options(args.s3_options())
                    .build(),
            ),
            provision_lock: Mutex::new(()),
            write_locks: std::sync::Mutex::new(HashMap::new()),
        };

        // Register every known index path's object store on the session
        // runtime so the text-index search can read splits for paths outside
        // the warehouse prefix as well.  Dynamically created indexes are
        // registered when they are provisioned.
        let paths: Vec<String> = state
            .indexes
            .read()
            .expect("index map poisoned")
            .values()
            .map(|runtime| runtime.path.clone())
            .collect();
        for path in paths {
            state.register_path(&path).await?;
        }

        Ok(state)
    }

    /// Look up an index, discovering an auto-provisioned table if needed.
    pub async fn resolve_index(&self, name: &str) -> Result<IndexRuntime, EsError> {
        if let Some(runtime) = self.indexes.read().expect("index map poisoned").get(name)
        {
            return Ok((**runtime).clone());
        }
        if !self.config.defaults.dynamic_indexes {
            return Err(EsError::index_not_found(name));
        }
        if !crate::config::is_valid_index_name(name) {
            return Err(EsError::bad_request(format!("invalid index name '{name}'")));
        }

        let _guard = self.provision_lock.lock().await;
        if let Some(runtime) = self.indexes.read().expect("index map poisoned").get(name)
        {
            return Ok((**runtime).clone());
        }

        // A table provisioned before a restart is discovered from metadata.
        let table = name.to_lowercase();
        if let Some(info) = self
            .client
            .get_table_info_by_table_name(&table, &self.config.lakesoul.namespace)
            .await
            .map_err(|error| {
                EsError::internal(format!("metadata lookup for '{name}': {error}"))
            })?
        {
            let runtime = runtime_from_table_info(&self.config, name, &info)?;
            self.register_path(&runtime.path).await.map_err(|error| {
                EsError::internal(format!("register object store: {error}"))
            })?;
            self.indexes
                .write()
                .expect("index map poisoned")
                .insert(name.to_string(), Arc::new(runtime.clone()));
            self.index_tables
                .write()
                .expect("index table set poisoned")
                .insert(runtime.table.clone());
            return Ok(runtime);
        }

        Err(EsError::index_not_found(name))
    }

    /// `PUT /{index}`: remember an index that has no table yet.  The first
    /// document creates the table (and its vector index) with the dimension
    /// taken from the document's embedding.
    pub async fn create_pending_index(
        &self,
        name: &str,
    ) -> Result<IndexRuntime, EsError> {
        if !self.config.defaults.dynamic_indexes {
            return Err(EsError::index_not_found(name));
        }
        if !crate::config::is_valid_index_name(name) {
            return Err(EsError::bad_request(format!("invalid index name '{name}'")));
        }
        let _guard = self.provision_lock.lock().await;
        if let Some(runtime) = self.indexes.read().expect("index map poisoned").get(name)
        {
            return Ok((**runtime).clone());
        }
        let runtime = pending_runtime(&self.config, name);
        self.indexes
            .write()
            .expect("index map poisoned")
            .insert(name.to_string(), Arc::new(runtime.clone()));
        Ok(runtime)
    }

    /// Create the LakeSoul table behind a pending index.  `dim` is the
    /// embedding length of the first written document; `None` makes the index
    /// keyword-only.
    pub async fn provision_pending_index(
        &self,
        pending: &IndexRuntime,
        dim: Option<usize>,
    ) -> Result<IndexRuntime, EsError> {
        let _guard = self.provision_lock.lock().await;
        if let Some(runtime) = self
            .indexes
            .read()
            .expect("index map poisoned")
            .get(&pending.config.name)
            .filter(|runtime| !runtime.pending)
        {
            return Ok((**runtime).clone());
        }

        let defaults = &self.config.defaults;
        let vector_columns: Vec<(String, usize)> = match dim {
            Some(dim) if dim > 0 => vec![(defaults.embedding_column.clone(), dim)],
            _ => Vec::new(),
        };
        let index = IndexConfig {
            name: pending.config.name.clone(),
            table: Some(pending.table.clone()),
            path: Some(pending.path.clone()),
            dim,
            hash_bucket_num: None,
            tokenizer: None,
            with_positions: None,
            nprobe: None,
            content_column: defaults.content_column.clone(),
            embedding_column: defaults.embedding_column.clone(),
            vectors: Vec::new(),
        };
        let runtime = IndexRuntime {
            config: index,
            namespace: pending.namespace.clone(),
            table: pending.table.clone(),
            path: pending.path.clone(),
            vectors: vector_columns.clone(),
            schema: schema::document_schema(&defaults.content_column, &vector_columns),
            pending: false,
        };
        crate::provision::ensure_table(self, &runtime)
            .await
            .map_err(|error| {
                EsError::internal(format!(
                    "failed to provision index '{}': {error}",
                    runtime.config.name
                ))
            })?;
        self.register_path(&runtime.path).await.map_err(|error| {
            EsError::internal(format!("register object store: {error}"))
        })?;
        self.indexes
            .write()
            .expect("index map poisoned")
            .insert(runtime.config.name.clone(), Arc::new(runtime.clone()));
        self.index_tables
            .write()
            .expect("index table set poisoned")
            .insert(runtime.table.clone());
        tracing::info!(
            index = %runtime.config.name,
            table = %runtime.table,
            dim = ?dim,
            "index provisioned on first write"
        );
        Ok(runtime)
    }

    /// `DELETE /{index}`: drop a dynamically created table.  Declared indexes
    /// are owned by the configuration file and cannot be removed at runtime.
    pub async fn delete_index(&self, name: &str) -> Result<(), EsError> {
        if self.config.indexes.iter().any(|index| index.name == name) {
            return Err(EsError::bad_request(format!(
                "index '{name}' is declared in the gateway configuration and \
                 cannot be deleted at runtime"
            )));
        }
        let runtime = self.resolve_index(name).await?;
        if !runtime.pending {
            self.client
                .drop_table(&runtime.table, &runtime.namespace)
                .await
                .map_err(|error| {
                    EsError::internal(format!(
                        "failed to drop table '{}': {error}",
                        runtime.table
                    ))
                })?;
        }
        self.indexes
            .write()
            .expect("index map poisoned")
            .remove(name);
        Ok(())
    }

    /// Register the object store of `path` on the session runtime.
    async fn register_path(&self, path: &str) -> anyhow::Result<()> {
        let mut store_config = self.store_config.lock().await;
        lakesoul_io::object_store::register_object_store(
            path,
            &mut store_config,
            &self.session.runtime_env(),
        )
        .map_err(|error| {
            anyhow::anyhow!("failed to register object store for '{path}': {error}")
        })?;
        Ok(())
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
        let lock = {
            let mut locks = self.write_locks.lock().expect("write locks poisoned");
            Arc::clone(
                locks
                    .entry(runtime.table.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _guard = lock.lock().await;
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

/// Runtime of an index declared in the configuration file.
fn declared_runtime(config: &GatewayConfig, index: &IndexConfig) -> IndexRuntime {
    // LakeSoul resolves table names case-insensitively (they are folded to
    // lower case), so the gateway creates and looks up the folded name while
    // the Elasticsearch index name keeps its case.
    let table = index.table_name().to_lowercase();
    let vectors: Vec<(String, usize)> = index
        .vector_columns()
        .into_iter()
        .map(|(column, dim)| (column.to_string(), dim))
        .collect();
    IndexRuntime {
        config: index.clone(),
        namespace: config.lakesoul.namespace.clone(),
        path: table_path(config, index, &table),
        table,
        schema: schema::document_schema(&index.content_column, &vectors),
        vectors,
        pending: false,
    }
}

/// Runtime of an index created by `PUT` whose table does not exist yet.
fn pending_runtime(config: &GatewayConfig, name: &str) -> IndexRuntime {
    let index = auto_index_config(config, name);
    let table = index.table_name().to_lowercase();
    let path = table_path(config, &index, &table);
    IndexRuntime {
        schema: schema::document_schema(&index.content_column, &[]),
        config: index,
        namespace: config.lakesoul.namespace.clone(),
        table,
        path,
        vectors: Vec::new(),
        pending: true,
    }
}

/// Index configuration of an index created at runtime: fixed schema, no
/// per-index layout overrides.
fn auto_index_config(config: &GatewayConfig, name: &str) -> IndexConfig {
    IndexConfig {
        name: name.to_string(),
        table: Some(name.to_string()),
        path: None,
        dim: None,
        hash_bucket_num: None,
        tokenizer: None,
        with_positions: None,
        nprobe: None,
        content_column: config.defaults.content_column.clone(),
        embedding_column: config.defaults.embedding_column.clone(),
        vectors: Vec::new(),
    }
}

/// Rebuild the runtime of an existing table from LakeSoul metadata.
fn runtime_from_table_info(
    config: &GatewayConfig,
    name: &str,
    info: &TableInfo,
) -> Result<IndexRuntime, EsError> {
    let schema = schema_from_table_info_metadata(
        &info.table_schema,
        &info.table_schema_arrow_ipc,
        &info.table_schema_arrow_ipc_json_hash,
    )
    .map_err(|error| {
        EsError::internal(format!(
            "decode table '{}' schema: {error}",
            info.table_name
        ))
    })?;
    let properties: LakeSoulTableProperty =
        serde_json::from_str(&info.properties).unwrap_or_default();
    let text_configs = parse_text_index_columns(properties.text_index_columns.as_deref())
        .map_err(|error| {
            EsError::internal(format!(
                "decode text index properties of '{}': {error}",
                info.table_name
            ))
        })?;
    let content_column = text_configs
        .first()
        .map(|config| config.column.clone())
        .unwrap_or_else(|| config.defaults.content_column.clone());
    let vector_configs = parse_vector_index_columns(
        properties.vector_index_columns.as_deref(),
    )
    .map_err(|error| {
        EsError::internal(format!(
            "decode vector index properties of '{}': {error}",
            info.table_name
        ))
    })?;
    let vectors: Vec<(String, usize)> = vector_configs
        .iter()
        .map(|config| (config.column.clone(), config.params.dim))
        .collect();
    let index = IndexConfig {
        name: name.to_string(),
        table: Some(info.table_name.clone()),
        path: Some(info.table_path.clone()),
        dim: vectors.first().map(|(_, dim)| *dim),
        hash_bucket_num: properties
            .hash_bucket_num
            .as_deref()
            .and_then(|value| value.parse().ok()),
        tokenizer: None,
        with_positions: None,
        nprobe: None,
        content_column,
        embedding_column: vectors
            .first()
            .map(|(column, _)| column.clone())
            .unwrap_or_else(|| config.defaults.embedding_column.clone()),
        vectors: Vec::new(),
    };
    Ok(IndexRuntime {
        config: index,
        namespace: info.table_namespace.clone(),
        table: info.table_name.clone(),
        path: info.table_path.clone(),
        vectors,
        schema: Arc::new(schema),
        pending: false,
    })
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
