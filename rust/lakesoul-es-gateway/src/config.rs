// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Gateway configuration (TOML).

use std::collections::HashSet;
use std::path::Path;

use serde::Deserialize;

fn default_listen() -> String {
    "0.0.0.0:9200".to_string()
}

fn default_version() -> String {
    "8.19.6".to_string()
}

fn default_namespace() -> String {
    "default".to_string()
}

fn default_true() -> bool {
    true
}

fn default_hash_bucket_num() -> usize {
    4
}

fn default_tokenizer() -> String {
    "jieba".to_string()
}

fn default_content_column() -> String {
    "content".to_string()
}

fn default_embedding_column() -> String {
    "embedding".to_string()
}

/// Root configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    #[serde(default)]
    pub server: ServerConfig,
    pub lakesoul: LakesoulConfig,
    #[serde(default)]
    pub defaults: IndexDefaults,
    pub indexes: Vec<IndexConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Listen address, e.g. `0.0.0.0:9200`.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Version reported by `GET /`; this decides which WeKnora ES driver
    /// (v7 keyword-only or v8) a DB store binds to.
    #[serde(default = "default_version")]
    pub version: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            version: default_version(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LakesoulConfig {
    /// Namespace holding the gateway's tables.
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Prefix for tables without an explicit `path`
    /// (e.g. `s3://bucket/lakesoul` or `file:///var/lib/lakesoul`).
    pub warehouse_prefix: Option<String>,
    pub endpoint: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    #[serde(default)]
    pub s3_virtual_host_style: bool,
    /// Create/validate the declared tables at startup.
    #[serde(default = "default_true")]
    pub provision_on_start: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexDefaults {
    #[serde(default = "default_hash_bucket_num")]
    pub hash_bucket_num: usize,
    #[serde(default = "default_tokenizer")]
    pub tokenizer: String,
    #[serde(default = "default_true")]
    pub with_positions: bool,
}

impl Default for IndexDefaults {
    fn default() -> Self {
        Self {
            hash_bucket_num: default_hash_bucket_num(),
            tokenizer: default_tokenizer(),
            with_positions: default_true(),
        }
    }
}

/// One Elasticsearch index backed by one LakeSoul table.
#[derive(Debug, Clone, Deserialize)]
pub struct IndexConfig {
    /// Elasticsearch index name (must match the WeKnora store's index_name).
    pub name: String,
    /// LakeSoul table name; defaults to the index name.
    pub table: Option<String>,
    /// Table storage path; defaults to `{warehouse_prefix}/{table}`.
    pub path: Option<String>,
    /// Embedding dimension.  When unset the index is keyword-only.
    pub dim: Option<usize>,
    pub hash_bucket_num: Option<usize>,
    pub tokenizer: Option<String>,
    pub with_positions: Option<bool>,
    #[serde(default = "default_content_column")]
    pub content_column: String,
    #[serde(default = "default_embedding_column")]
    pub embedding_column: String,
}

impl IndexConfig {
    pub fn table_name(&self) -> &str {
        self.table.as_deref().unwrap_or(&self.name)
    }
}

impl GatewayConfig {
    /// Load and validate a TOML configuration file.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path.as_ref())?;
        let config: Self = toml::from_str(&raw)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate index declarations.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.indexes.is_empty() {
            anyhow::bail!("at least one [[indexes]] entry is required");
        }
        let mut names = HashSet::new();
        for index in &self.indexes {
            if !is_valid_index_name(&index.name) {
                anyhow::bail!(
                    "invalid index name '{}': an index name must start with a \
                     letter and contain only letters, digits, '_' and '-'",
                    index.name
                );
            }
            if !names.insert(index.name.clone()) {
                anyhow::bail!("duplicate index declaration '{}'", index.name);
            }
            if let Some(dim) = index.dim
                && dim == 0
            {
                anyhow::bail!("index '{}': dim must be > 0", index.name);
            }
            let buckets = index
                .hash_bucket_num
                .unwrap_or(self.defaults.hash_bucket_num);
            if !(1..=64).contains(&buckets) {
                anyhow::bail!(
                    "index '{}': hash_bucket_num must be between 1 and 64, got {buckets}",
                    index.name
                );
            }
            if index.content_column.trim().is_empty() {
                anyhow::bail!("index '{}': content_column must not be empty", index.name);
            }
            if index.embedding_column.trim().is_empty() {
                anyhow::bail!(
                    "index '{}': embedding_column must not be empty",
                    index.name
                );
            }
        }
        Ok(())
    }
}

/// Elasticsearch index name rules used by the client
/// (`^[a-zA-Z][a-zA-Z0-9_-]{0,127}$`).
pub fn is_valid_index_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() => {}
        _ => return false,
    }
    name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
