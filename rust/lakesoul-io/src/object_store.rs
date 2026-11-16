// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::runtime_env::RuntimeEnv;
use object_store::{
    BackoffConfig, ClientOptions, ObjectStore, RetryConfig,
    aws::{AmazonS3, AmazonS3Builder},
};
use rootcause::{bail, report};
use url::{ParseError, Url};

use crate::{
    Result,
    cache::{ReadThroughCache, disk_cache::DiskCache, get_lakesoul_cache},
    config::LakeSoulIOConfig,
    object_store_metrics::MonitoredObjectStore,
};

fn create_s3_store(config: &LakeSoulIOConfig) -> Result<AmazonS3> {
    create_s3_store_from_options(&config.object_store_options)
}

/// Build an S3 store from `fs.s3a.*` configuration options (env-first).
///
/// Used by callers that need an object store outside a DataFusion runtime
/// (e.g. the vector-index auto build after a write).
pub fn create_s3_store_from_options(
    options: &std::collections::HashMap<String, String>,
) -> Result<AmazonS3> {
    // ENV First
    let key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .or_else(|| options.get("fs.s3a.access.key").cloned());
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .or_else(|| options.get("fs.s3a.secret.key").cloned());
    let region = std::env::var("AWS_REGION").ok().or_else(|| {
        std::env::var("AWS_DEFAULT_REGION")
            .ok()
            .or_else(|| options.get("fs.s3a.endpoint.region").cloned())
    });
    let mut endpoint = std::env::var("AWS_ENDPOINT")
        .ok()
        .or_else(|| options.get("fs.s3a.endpoint").cloned());
    let bucket = options.get("fs.s3a.bucket").cloned();
    let virtual_path_style = options.get("fs.s3a.path.style.access").cloned();
    let virtual_path_style = virtual_path_style.is_none_or(|s| s == "true");
    if !virtual_path_style
        && let (Some(endpoint_str), Some(bucket)) = (&endpoint, &bucket)
    {
        // for host style access with endpoint defined, we need to check endpoint contains bucket name
        if !endpoint_str.contains(bucket) {
            let mut endpoint_url = Url::parse(endpoint_str.as_str())?;
            endpoint_url.set_host(Some(&*format!(
                "{}.{}",
                bucket,
                endpoint_url
                    .host_str()
                    .ok_or(report!("endpoint host missing"))?
            )))?;
            let endpoint_s = endpoint_url.to_string();
            endpoint = endpoint_s
                .strip_suffix('/')
                .map(|s| s.to_string())
                .or(Some(endpoint_s));
        }
    }

    if bucket.is_none() {
        bail!("missing fs.s3a.bucket");
    }

    let retry_config = RetryConfig {
        // First attempt plus at most 3 retries.
        max_retries: 3,
        backoff: BackoffConfig {
            base: 2.5,
            max_backoff: Duration::from_secs(20),
            ..Default::default()
        },
        ..Default::default()
    };

    let skip_signature = options
        .get("fs.s3a.s3.signing-algorithm")
        .cloned()
        .is_some_and(|s| s == "NoOpSignerType")
        || (key.as_ref().is_some_and(|k| k == "noop")
            && secret.as_ref().is_some_and(|v| v == "noop"));
    let mut s3_store_builder = AmazonS3Builder::new()
        .with_region(region.unwrap_or_else(|| "us-east-1".to_owned()))
        .with_bucket_name(bucket.unwrap())
        .with_retry(retry_config)
        .with_virtual_hosted_style_request(!virtual_path_style)
        .with_unsigned_payload(true)
        .with_skip_signature(skip_signature)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                // Fail a stuck request quickly instead of letting the Flink
                // task block for minutes; retries are bounded separately.
                .with_connect_timeout(Duration::from_secs(10))
                .with_pool_idle_timeout(Duration::from_secs(600))
                .with_timeout(Duration::from_secs(10)),
        )
        .with_allow_http(true);
    if let (Some(k), Some(s)) = (key, secret)
        && k != "noop"
        && s != "noop"
    {
        s3_store_builder = s3_store_builder
            .with_access_key_id(k)
            .with_secret_access_key(s);
    }
    if let Some(ep) = endpoint {
        s3_store_builder = s3_store_builder.with_endpoint(ep);
    }
    Ok(s3_store_builder.build()?)
}

/// Raw (uncached) inner stores of registered read-through caches.
///
/// The read-through page cache is the right default for table data, but a few
/// call sites issue thousands of small, one-shot range reads where the cache's
/// per-read bookkeeping (metadata `HEAD`, page lookup, blocking local read,
/// stream assembly) costs more than the read itself.  Those call sites take the
/// inner store through [`raw_store_alias`] while every cache identity and
/// namespace keeps being derived from the cached store they were given.
///
/// Entries are weak and keyed by the cached store's allocation (the same
/// identity convention as `cache::store_namespace`), so a session's stores are
/// never pinned by this registry and a freed allocation's entry falls out on
/// the next registration.
static RAW_STORE_ALIASES: OnceLock<Mutex<HashMap<usize, Weak<dyn ObjectStore>>>> =
    OnceLock::new();

fn raw_store_aliases() -> &'static Mutex<HashMap<usize, Weak<dyn ObjectStore>>> {
    RAW_STORE_ALIASES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register `raw` as the uncached inner store of the read-through `cached`
/// store.  Also used by tests.
pub(crate) fn register_raw_store_alias(
    cached: &Arc<dyn ObjectStore>,
    raw: &Arc<dyn ObjectStore>,
) {
    let key = Arc::as_ptr(cached) as *const () as usize;
    let mut aliases = raw_store_aliases()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    aliases.retain(|_, alias| alias.strong_count() > 0);
    aliases.insert(key, Arc::downgrade(raw));
}

/// The uncached store behind a registered read-through cache.
///
/// Returns `store` itself when it was not registered (a local store, a store
/// without the cache, or a freed cache), so a call site can use the result
/// unconditionally as its IO handle.
pub fn raw_store_alias(store: &Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    let key = Arc::as_ptr(store) as *const () as usize;
    raw_store_aliases()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&key)
        .and_then(Weak::upgrade)
        .unwrap_or_else(|| Arc::clone(store))
}

pub fn register_s3_object_store_with_cache(
    url: &Url,
    config: &LakeSoulIOConfig,
    runtime: &RuntimeEnv,
    disk_cache: Arc<DiskCache>,
) -> Result<()> {
    let s3_store = Arc::new(create_s3_store(config)?);
    // Metrics cover the inner store so cache hits are not counted as remote
    // requests; cache-level counters live in `ReadThroughCache`.
    let s3_store: Arc<dyn ObjectStore> =
        Arc::new(MonitoredObjectStore::new(s3_store, "s3"));
    // cache size in bytes, default to 1GB
    // NOTE: all read-through stores share one process-wide stats instance so
    // that the cache metrics (reads/misses/bytes) aggregate correctly.
    // Vector index segments are immutable, read once per cold shard and never
    // revisited: caching them adds a HEAD plus a page-sized local write per
    // object.  Serve them straight from the inner store; data files keep the
    // read-through cache.
    let cache_s3_store: Arc<dyn ObjectStore> = Arc::new(
        ReadThroughCache::new_with_stats(
            Arc::clone(&s3_store),
            disk_cache,
            crate::cache::get_lakesoul_cache_stats_handle(),
        )
        .with_bypass_paths(vec![
            lakesoul_common::index::IndexKind::Vector
                .directory()
                .to_string(),
        ]),
    );
    // The vector exact candidate reader issues many small one-shot range reads
    // per query; it takes the raw store through `raw_store_alias` instead of
    // paying the page cache's per-read bookkeeping.  Everything else keeps
    // reading through `cache_s3_store`.
    register_raw_store_alias(&cache_s3_store, &s3_store);
    runtime.register_object_store(url, cache_s3_store);
    Ok(())
}

/// First check envs for credentials, region and endpoint.
/// Second, check fs.s3a.xxx, to keep compatible with hadoop s3a.
/// If no region is provided, default to us-east-1.
/// Bucket name would be retrieved from file names.
/// Currently only one s3 object store with one bucket is supported.
/// With `LAKESOUL_CACHE` set, reads go through the process-wide local disk
/// cache (`LAKESOUL_CACHE_SIZE`, `LAKESOUL_CACHE_PATH`).
pub fn register_s3_object_store(
    url: &Url,
    config: &LakeSoulIOConfig,
    runtime: &RuntimeEnv,
) -> Result<()> {
    if std::env::var("LAKESOUL_CACHE").is_ok() {
        return register_s3_object_store_with_cache(
            url,
            config,
            runtime,
            get_lakesoul_cache(),
        );
    }
    let s3_store = Arc::new(create_s3_store(config)?);
    let s3_store = Arc::new(MonitoredObjectStore::new(s3_store, "s3"));
    runtime.register_object_store(url, s3_store);
    Ok(())
}

/// Registers an HDFS object store
///
/// # Arguments
///
/// * `url` - The URL of the HDFS object store
/// * `host` - The host of the HDFS object store
/// * `config` - The LakeSoulIOConfig instance
/// * `runtime` - The DataFusion RuntimeEnv instance
pub fn register_hdfs_object_store(
    _url: &Url,
    _host: &str,
    _config: &LakeSoulIOConfig,
    _runtime: &RuntimeEnv,
) -> Result<()> {
    #[cfg(not(feature = "hdfs"))]
    {
        bail!("hdfs support is not enabled");
    }
    #[cfg(feature = "hdfs")]
    {
        let hdfs = crate::hdfs::Hdfs::try_new(_host, _config.clone())?;
        let hdfs = Arc::new(MonitoredObjectStore::new(Arc::new(hdfs), "hdfs"));

        _runtime.register_object_store(_url, hdfs);
        Ok(())
    }
}

/// Try to register object store of this path string, and return normalized path string if
/// this path is local path style, but fs.defaultFS config exists
///
/// # Arguments
///
/// * `path` - The path to register the object store for
/// * `config` - A mutable reference to the LakeSoulIOConfig instance
/// * `runtime` - The DataFusion RuntimeEnv instance
///
/// # Returns
///
/// The normalized path string
pub fn register_object_store(
    path: &str,
    config: &mut LakeSoulIOConfig,
    runtime: &RuntimeEnv,
) -> Result<String> {
    let url = Url::parse(path);
    match url {
        Ok(url) => match url.scheme() {
            "s3" | "s3a" => {
                if runtime
                    .object_store(ObjectStoreUrl::parse(
                        &url[..url::Position::BeforePath],
                    )?)
                    .is_ok()
                {
                    return Ok(path.to_owned());
                }
                if !config.object_store_options.contains_key("fs.s3a.bucket") {
                    config.object_store_options.insert(
                        "fs.s3a.bucket".into(),
                        url.host_str().ok_or(report!("host str missing"))?.into(),
                    );
                }
                register_s3_object_store(&url, config, runtime)?;
                Ok(path.to_owned())
            }
            "hdfs" => {
                if url.has_host() {
                    if runtime
                        .object_store(ObjectStoreUrl::parse(
                            &url[..url::Position::BeforePath],
                        )?)
                        .is_ok()
                    {
                        return Ok(path.to_owned());
                    }
                    register_hdfs_object_store(
                        &url,
                        &url[url::Position::BeforeHost..url::Position::BeforePath],
                        config,
                        runtime,
                    )?;
                    Ok(path.to_owned())
                } else {
                    // defaultFS should have been registered with hdfs,
                    // and we convert hdfs://user/hadoop/file to
                    // hdfs://defaultFS/user/hadoop/file
                    let path = url.path().trim_start_matches('/');
                    let joined_path = [config.default_fs.as_str(), path].join("/");
                    Ok(joined_path)
                }
            }
            // "file" => Ok(path.to_owned()),
            "file" => Ok(path.to_owned()),
            // Support Windows drive letter paths like "c:" or "d:"
            scheme
                if scheme.len() == 1
                    && scheme.chars().next().unwrap().is_ascii_alphabetic() =>
            {
                Ok(format!("file://{}", path))
            }
            _ => bail!("FileSystem is not supported"),
        },
        Err(ParseError::RelativeUrlWithoutBase) => {
            let path = path.trim_start_matches('/');
            if config.default_fs.is_empty() {
                // local filesystem
                Ok(["file://", path].join("/"))
            } else {
                // concat default fs and path
                let joined_path = [config.default_fs.as_str(), path].join("/");
                Ok(joined_path)
            }
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn raw_store_alias_resolves_the_registered_inner_store() {
        let plain: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // A store that was never registered as a read-through cache is its own
        // IO handle; call sites can use the alias unconditionally.
        assert!(Arc::ptr_eq(&raw_store_alias(&plain), &plain));

        let cached: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(Arc::ptr_eq(&raw_store_alias(&cached), &cached));

        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        register_raw_store_alias(&cached, &raw);
        assert!(Arc::ptr_eq(&raw_store_alias(&cached), &raw));
    }

    #[test]
    fn raw_store_alias_does_not_pin_the_inner_store() {
        let cached: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        {
            let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            register_raw_store_alias(&cached, &raw);
            assert!(Arc::ptr_eq(&raw_store_alias(&cached), &raw));
        }
        // The registry holds weak references only: once the cache (its owner)
        // drops the inner store, the lookup falls back to the cached store
        // instead of keeping memory alive.
        assert!(Arc::ptr_eq(&raw_store_alias(&cached), &cached));
    }
}
