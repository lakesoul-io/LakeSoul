//! Central vector-cache configuration.
//!
//! The cache implementation spans three physical tiers (L1 process memory,
//! L2 local SSD, L3 object storage).  Historically each tier and each
//! sub-cache read its own environment variable, which made the public surface
//! hard to reason about.  This module parses the small public surface once and
//! derives all internal sub-budgets:
//!
//! ```text
//! LAKESOUL_VECTOR_CACHE              master switch
//! LAKESOUL_VECTOR_MEMORY_BYTES       L1 total bytes
//! LAKESOUL_CACHE_PATH                L2 root
//! LAKESOUL_CACHE_SIZE                L2 total bytes (0 disables L2)
//! LAKESOUL_VECTOR_LAZY_INDEX         auto / 1 / 0
//! ```
//!
//! Everything else (`block_rows`, per-cache byte budgets, snapshot budgets,
//! index-SSD paths, ...) is derived here or accepted as a deprecated expert
//! override.  The old names keep working for one release; they are not part of
//! the documented public surface.

use std::sync::LazyLock;

use tracing::debug;

/// Public: enable/disable vector caching.
pub const ENV_CACHE: &str = "LAKESOUL_VECTOR_CACHE";
/// Public: L1 memory budget for directory, clusters, blocks and query buffers.
pub const ENV_MEMORY_BYTES: &str = "LAKESOUL_VECTOR_MEMORY_BYTES";
/// Public: L2 root directory.
pub const ENV_DISK_PATH: &str = "LAKESOUL_CACHE_PATH";
/// Public: L2 total byte capacity; `0` disables L2.
pub const ENV_DISK_BYTES: &str = "LAKESOUL_CACHE_SIZE";
/// Public: lazy-cluster mode (`auto`, `1`, `0`).
pub const ENV_LAZY_INDEX: &str = "LAKESOUL_VECTOR_LAZY_INDEX";

const DEFAULT_MEMORY_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_DISK_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_BLOCK_TARGET_BYTES: usize = 256 * 1024;

/// The effective vector-cache configuration for this process.
#[derive(Debug, Clone)]
pub struct VectorCacheConfig {
    enabled: bool,
    memory_bytes: usize,
    disk_bytes: usize,
    disk_path: String,
    lazy_mode: LazyMode,
    cluster_bytes: usize,
    snapshot_bytes: usize,
    block_bytes: usize,
    block_target_bytes: usize,
    block_rows_override: Option<usize>,
    block_source_cache: bool,
    index_ssd_bytes: usize,
    index_ssd_path: String,
    /// True when the unified `LAKESOUL_VECTOR_*` surface (master switch or
    /// memory budget) is in use.  Legacy deployments keep the old per-cache
    /// defaults and the old L2 capacity split.
    unified: bool,
}

/// Lazy-cluster mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LazyMode {
    /// Lazy for shards with many clusters, eager for tiny shards.
    Auto,
    Always,
    Never,
}

impl LazyMode {
    pub fn enabled(self, cluster_count: usize) -> bool {
        match self {
            Self::Auto => cluster_count >= 64,
            Self::Always => true,
            Self::Never => false,
        }
    }
}

impl VectorCacheConfig {
    /// Parse the environment once; later changes are intentionally ignored,
    /// matching the process-wide `LazyLock` caches these values configure.
    pub fn from_env() -> Self {
        let memory = env_bytes(ENV_MEMORY_BYTES)
            .or_else(|| env_bytes("LAKESOUL_INDEX_MEMORY_BYTES"))
            .or_else(|| env_bytes("LAKESOUL_INDEX_CACHE_BYTES"))
            .or_else(|| env_bytes("LAKESOUL_VECTOR_INDEX_CACHE_BYTES"))
            .unwrap_or(DEFAULT_MEMORY_BYTES);
        let legacy_enabled = std::env::var("LAKESOUL_CACHE").ok();
        let unified = std::env::var(ENV_CACHE).is_ok()
            || std::env::var(ENV_MEMORY_BYTES).is_ok();
        let enabled = match std::env::var(ENV_CACHE).ok().as_deref() {
            Some("0") | Some("false") | Some("off") => false,
            Some("1") | Some("true") | Some("on") => true,
            // The old L1 index cache had no master switch and was on by
            // default; `LAKESOUL_CACHE=0` only disabled the generic L2 cache.
            _ => true,
        };
        let explicit_disk = env_bytes(ENV_DISK_BYTES)
            .or_else(|| env_bytes("LAKESOUL_INDEX_SSD_BYTES"));
        // Old deployments only enabled the generic L2 cache when the
        // LAKESOUL_CACHE switch was set; size/path alone were inert.  The
        // unified surface deliberately makes size/path sufficient.
        let legacy_switch_on = matches!(legacy_enabled.as_deref(), Some("1") | Some("true"));
        let disk_configured = if unified {
            explicit_disk.is_some()
                || std::env::var(ENV_DISK_PATH).is_ok()
                || std::env::var("LAKESOUL_INDEX_SSD_DIR").is_ok()
        } else {
            legacy_switch_on
        };
        let disk_bytes = if !enabled
            || !disk_configured
            || matches!(legacy_enabled.as_deref(), Some("0") | Some("false"))
        {
            0
        } else {
            explicit_disk.unwrap_or(DEFAULT_DISK_BYTES)
        };
        let disk_path = std::env::var(ENV_DISK_PATH)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| std::env::var("LAKESOUL_INDEX_SSD_DIR").ok())
            .unwrap_or_else(|| "lakesoul_cache_dir".to_string());
        let lazy_mode = match std::env::var(ENV_LAZY_INDEX).ok().as_deref() {
            Some("0") | Some("false") | Some("off") => LazyMode::Never,
            Some("1") | Some("true") | Some("on") => LazyMode::Always,
            _ => LazyMode::Auto,
        };
        let memory_bytes = if enabled { memory } else { 0 };
        let legacy_index_memory = env_bytes("LAKESOUL_INDEX_MEMORY_BYTES")
            .or_else(|| env_bytes("LAKESOUL_INDEX_CACHE_BYTES"))
            .or_else(|| env_bytes("LAKESOUL_VECTOR_INDEX_CACHE_BYTES"));
        // Legacy deployments keep the historical per-cache defaults:
        // 512 MiB loaded-index cache, 512 MiB snapshot cache, 256 MiB block
        // cache, full generic DiskCache capacity and no index-SSD cache.
        // The unified memory budget only re-partitions when the user opts in
        // with LAKESOUL_VECTOR_MEMORY_BYTES / LAKESOUL_VECTOR_CACHE.
        let cluster_bytes = if !enabled {
            0
        } else {
            expert_bytes("LAKESOUL_VECTOR_CLUSTER_CACHE_BYTES")
            .or(legacy_index_memory)
            .unwrap_or_else(|| {
                if unified {
                    memory_bytes.saturating_mul(6) / 10
                } else {
                    DEFAULT_MEMORY_BYTES
                }
            })
        };
        let snapshot_bytes = if memory_bytes == 0 || !enabled {
            0
        } else {
            expert_bytes("LAKESOUL_VECTOR_SNAPSHOT_CACHE_BYTES").unwrap_or_else(|| {
                if unified {
                    (memory_bytes / 10).clamp(64 * 1024 * 1024, DEFAULT_MEMORY_BYTES)
                } else {
                    DEFAULT_MEMORY_BYTES
                }
            })
        };
        let block_bytes = if !enabled {
            0
        } else {
            expert_bytes("LAKESOUL_VECTOR_BLOCK_CACHE_BYTES").unwrap_or_else(|| {
                if unified {
                    memory_bytes.saturating_mul(3) / 10
                } else {
                    256 * 1024 * 1024
                }
            })
        };
        let block_target_bytes = expert_bytes("LAKESOUL_VECTOR_BLOCK_BYTES")
            .unwrap_or(DEFAULT_BLOCK_TARGET_BYTES);
        let block_rows_override = expert_bytes("LAKESOUL_VECTOR_BLOCK_ROWS");
        let block_source_cache = !std::env::var("LAKESOUL_VECTOR_BLOCK_SOURCE_CACHE")
            .is_ok_and(|value| value == "0");
        // The vector index segment cache is a separate, explicitly sized
        // layer.  It must not carve capacity out of the generic DiskCache:
        // `LAKESOUL_INDEX_SSD_BYTES` stays the opt-in switch (old default 0).
        let index_ssd_bytes = if !enabled {
            0
        } else {
            expert_bytes("LAKESOUL_INDEX_SSD_BYTES").unwrap_or(0)
        };
        // Legacy `LAKESOUL_INDEX_SSD_DIR` was used verbatim; the unified
        // surface puts the index cache under `<LAKESOUL_CACHE_PATH>/index-cache`.
        let index_ssd_path = std::env::var("LAKESOUL_INDEX_SSD_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                if unified {
                    format!("{}/index-cache", disk_path.trim_end_matches('/'))
                } else if std::env::var("LAKESOUL_CACHE_PATH").is_ok() {
                    format!("{}/index-cache", disk_path.trim_end_matches('/'))
                } else {
                    std::env::temp_dir()
                        .join("lakesoul-index-cache")
                        .to_string_lossy()
                        .into_owned()
                }
            });
        warn_deprecated();
        let config = Self {
            enabled,
            memory_bytes,
            disk_bytes,
            disk_path,
            lazy_mode,
            cluster_bytes,
            snapshot_bytes,
            block_bytes,
            block_target_bytes,
            block_rows_override,
            block_source_cache,
            index_ssd_bytes,
            index_ssd_path,
            unified,
        };
        debug!(?config, "vector cache configuration");
        config
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn memory_bytes(&self) -> usize {
        self.memory_bytes
    }

    pub fn disk_enabled(&self) -> bool {
        self.disk_bytes > 0
    }

    pub fn disk_bytes(&self) -> usize {
        self.disk_bytes
    }

    /// Capacity of the generic LakeSoul `DiskCache` (parquet pages, PK index,
    /// decoded blocks).  This deliberately receives the *whole*
    /// `LAKESOUL_CACHE_SIZE` in every mode: a table shared by vector and
    /// non-vector queries must not lose the cache capacity the rest of
    /// LakeSoul was designed for.
    pub fn block_disk_bytes(&self) -> usize {
        self.disk_bytes
    }

    pub fn index_ssd_bytes(&self) -> usize {
        self.index_ssd_bytes
    }

    pub fn index_ssd_path(&self) -> &str {
        &self.index_ssd_path
    }

    pub fn disk_path(&self) -> &str {
        &self.disk_path
    }

    pub fn lazy_mode(&self) -> LazyMode {
        self.lazy_mode
    }

    pub fn cluster_bytes(&self) -> usize {
        self.cluster_bytes
    }

    pub fn snapshot_bytes(&self) -> usize {
        self.snapshot_bytes
    }

    pub fn block_bytes(&self) -> usize {
        self.block_bytes
    }

    pub fn block_target_bytes(&self) -> usize {
        self.block_target_bytes
    }

    pub fn block_source_cache(&self) -> bool {
        self.block_source_cache
    }

    /// Block rows for one vector column.  An explicit expert override wins;
    /// otherwise the block size is capped around `block_target_bytes`.
    pub fn block_rows(&self, row_bytes: usize) -> usize {
        if let Some(rows) = self.block_rows_override {
            return rows.max(1);
        }
        if !self.unified {
            // Historical default: fixed 256 rows per decoded block.
            return 256;
        }
        (self.block_target_bytes / row_bytes.max(1)).clamp(32, 1024)
    }
}

static CONFIG: LazyLock<VectorCacheConfig> = LazyLock::new(VectorCacheConfig::from_env);

/// Process-wide configuration.
pub fn config() -> &'static VectorCacheConfig {
    &CONFIG
}

fn env_bytes(name: &str) -> Option<usize> {
    let value = std::env::var(name).ok()?;
    parse_bytes(&value)
}

fn expert_bytes(name: &str) -> Option<usize> {
    env_bytes(name)
}

fn parse_bytes(value: &str) -> Option<usize> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let (number, multiplier) = if let Some(value) = value.strip_suffix("KiB") {
        (value, 1024usize)
    } else if let Some(value) = value.strip_suffix("MiB") {
        (value, 1024 * 1024)
    } else if let Some(value) = value.strip_suffix("GiB") {
        (value, 1024 * 1024 * 1024)
    } else if let Some(value) = value.strip_suffix("TiB") {
        (value, 1024 * 1024 * 1024 * 1024)
    } else {
        (value, 1)
    };
    number.trim().parse::<usize>().ok().map(|n| n.saturating_mul(multiplier))
}

fn warn_deprecated() {
    const LEGACY: &[&str] = &[
        "LAKESOUL_INDEX_MEMORY_BYTES",
        "LAKESOUL_INDEX_CACHE_BYTES",
        "LAKESOUL_VECTOR_INDEX_CACHE_BYTES",
        "LAKESOUL_VECTOR_CLUSTER_CACHE_BYTES",
        "LAKESOUL_VECTOR_SNAPSHOT_CACHE_BYTES",
        "LAKESOUL_VECTOR_BLOCK_CACHE_BYTES",
        "LAKESOUL_VECTOR_BLOCK_BYTES",
        "LAKESOUL_VECTOR_BLOCK_ROWS",
        "LAKESOUL_VECTOR_BLOCK_SOURCE_CACHE",
        "LAKESOUL_INDEX_SSD_BYTES",
        "LAKESOUL_INDEX_SSD_DIR",
        "LAKESOUL_INDEX_SSD_KIND",
    ];
    for name in LEGACY {
        if std::env::var(name).is_ok() {
            debug!(
                variable = name,
                "deprecated vector-cache variable accepted; prefer \
                 LAKESOUL_VECTOR_CACHE / LAKESOUL_VECTOR_MEMORY_BYTES / \
                 LAKESOUL_CACHE_PATH / LAKESOUL_CACHE_SIZE / \
                 LAKESOUL_VECTOR_LAZY_INDEX"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const ALL: &[&str] = &[
        ENV_CACHE,
        ENV_MEMORY_BYTES,
        ENV_DISK_PATH,
        ENV_DISK_BYTES,
        ENV_LAZY_INDEX,
        "LAKESOUL_CACHE",
        "LAKESOUL_INDEX_MEMORY_BYTES",
        "LAKESOUL_INDEX_CACHE_BYTES",
        "LAKESOUL_VECTOR_INDEX_CACHE_BYTES",
        "LAKESOUL_VECTOR_CLUSTER_CACHE_BYTES",
        "LAKESOUL_VECTOR_SNAPSHOT_CACHE_BYTES",
        "LAKESOUL_VECTOR_BLOCK_CACHE_BYTES",
        "LAKESOUL_VECTOR_BLOCK_BYTES",
        "LAKESOUL_VECTOR_BLOCK_ROWS",
        "LAKESOUL_INDEX_SSD_BYTES",
        "LAKESOUL_INDEX_SSD_DIR",
    ];

    fn clear() {
        for name in ALL {
            // SAFETY: the tests in this module serialize environment access
            // through ENV_LOCK and do not run concurrently with each other.
            unsafe { std::env::remove_var(name) };
        }
    }

    #[test]
    fn legacy_defaults_are_preserved() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear();
        let config = VectorCacheConfig::from_env();
        assert_eq!(config.cluster_bytes(), 512 * 1024 * 1024);
        assert_eq!(config.snapshot_bytes(), 512 * 1024 * 1024);
        assert_eq!(config.block_bytes(), 256 * 1024 * 1024);
        assert_eq!(config.index_ssd_bytes(), 0);
        assert_eq!(config.block_rows(128), 256);
        assert!(!config.disk_enabled());
    }

    #[test]
    fn legacy_index_memory_and_ssd_dir_stay_exact() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear();
        unsafe {
            std::env::set_var("LAKESOUL_INDEX_MEMORY_BYTES", "64MiB");
            std::env::set_var("LAKESOUL_CACHE", "1");
            std::env::set_var("LAKESOUL_CACHE_SIZE", "4GiB");
            std::env::set_var("LAKESOUL_INDEX_SSD_BYTES", "1MiB");
            std::env::set_var("LAKESOUL_INDEX_SSD_DIR", "/tmp/legacy-index");
        }
        let config = VectorCacheConfig::from_env();
        assert_eq!(config.cluster_bytes(), 64 * 1024 * 1024);
        assert_eq!(config.block_disk_bytes(), 4 * 1024 * 1024 * 1024);
        assert_eq!(config.index_ssd_bytes(), 1024 * 1024);
        assert_eq!(config.index_ssd_path(), "/tmp/legacy-index");
        assert_eq!(config.block_rows(128), 256);
    }

    #[test]
    fn legacy_disk_size_without_switch_is_inert() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear();
        unsafe {
            std::env::set_var("LAKESOUL_CACHE_SIZE", "4GiB");
            std::env::set_var("LAKESOUL_CACHE_PATH", "/tmp/legacy-path");
        }
        let config = VectorCacheConfig::from_env();
        assert!(!config.disk_enabled());
        assert_eq!(config.index_ssd_bytes(), 0);
        unsafe {
            std::env::set_var("LAKESOUL_CACHE", "1");
        }
        let config = VectorCacheConfig::from_env();
        assert!(config.disk_enabled());
        assert_eq!(config.block_disk_bytes(), 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn unified_budget_is_split_only_when_opted_in() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear();
        unsafe {
            std::env::set_var(ENV_CACHE, "1");
            std::env::set_var(ENV_MEMORY_BYTES, "1GiB");
            std::env::set_var(ENV_DISK_PATH, "/tmp/unified-cache");
            std::env::set_var(ENV_DISK_BYTES, "8GiB");
        }
        let config = VectorCacheConfig::from_env();
        assert!(config.disk_enabled());
        assert_eq!(config.cluster_bytes(), 1_073_741_824 * 6 / 10);
        assert_eq!(config.snapshot_bytes(), 1_073_741_824 / 10);
        assert_eq!(config.block_bytes(), 1_073_741_824 * 3 / 10);
        assert_eq!(config.block_disk_bytes(), 8 * 1024 * 1024 * 1024);
        assert_eq!(config.index_ssd_bytes(), 0);
        assert_eq!(config.index_ssd_path(), "/tmp/unified-cache/index-cache");
        assert!(config.block_rows(960) > 256);
    }
}
