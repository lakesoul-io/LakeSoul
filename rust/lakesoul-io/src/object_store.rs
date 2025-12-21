use std::{sync::Arc, time::Duration};

use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::runtime_env::RuntimeEnv;
use object_store::{
    ClientOptions, RetryConfig,
    aws::{AmazonS3, AmazonS3Builder},
};
use rootcause::{bail, report};
use url::{ParseError, Url};

use crate::{
    Result,
    cache::{ReadThroughCache, disk_cache::DiskCache, get_lakesoul_cache},
    config::LakeSoulIOConfig,
};

fn create_s3_store(config: &LakeSoulIOConfig) -> Result<AmazonS3> {
    let key = std::env::var("AWS_ACCESS_KEY_ID").ok().or_else(|| {
        config
            .object_store_options
            .get("fs.s3a.access.key")
            .cloned()
    });
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY").ok().or_else(|| {
        config
            .object_store_options
            .get("fs.s3a.secret.key")
            .cloned()
    });
    let region = std::env::var("AWS_REGION").ok().or_else(|| {
        std::env::var("AWS_DEFAULT_REGION").ok().or_else(|| {
            config
                .object_store_options
                .get("fs.s3a.endpoint.region")
                .cloned()
        })
    });
    let mut endpoint = std::env::var("AWS_ENDPOINT")
        .ok()
        .or_else(|| config.object_store_options.get("fs.s3a.endpoint").cloned());
    let bucket = config.object_store_options.get("fs.s3a.bucket").cloned();
    let virtual_path_style = config
        .object_store_options
        .get("fs.s3a.path.style.access")
        .cloned();
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

    let mut retry_config = RetryConfig::default();
    retry_config.backoff.base = 2.5;
    retry_config.backoff.max_backoff = Duration::from_secs(20);

    let skip_signature = config
        .object_store_options
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
                .with_connect_timeout(Duration::from_secs(30))
                .with_pool_idle_timeout(Duration::from_secs(600))
                .with_timeout(Duration::from_secs(30)),
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

pub fn register_s3_object_store_with_cache(
    url: &Url,
    config: &LakeSoulIOConfig,
    runtime: &RuntimeEnv,
    disk_cache: Arc<DiskCache>,
) -> Result<()> {
    let s3_store = Arc::new(create_s3_store(config)?);
    // cache size in bytes, default to 1GB
    let cache_s3_store = Arc::new(ReadThroughCache::new(s3_store, disk_cache));
    runtime.register_object_store(url, cache_s3_store);
    Ok(())
}

/// First check envs for credentials, region and endpoint.
/// Second, check fs.s3a.xxx, to keep compatible with hadoop s3a.
/// If no region is provided, default to us-east-1.
/// Bucket name would be retrieved from file names.
/// Currently only one s3 object store with one bucket is supported.
pub fn register_s3_object_store(
    url: &Url,
    config: &LakeSoulIOConfig,
    runtime: &RuntimeEnv,
) -> Result<()> {
    let s3_store = Arc::new(create_s3_store(config)?);
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
        let hdfs = Hdfs::try_new(_host, _config.clone())?;

        // add cache if env LAKESOUL_CACHE is set
        // todo
        // if std::env::var("LAKESOUL_CACHE").is_ok() {
        //     // cache size in bytes, default to 1GB
        //     let cache_size = {
        //         match std::env::var("LAKESOUL_CACHE_SIZE") {
        //             Ok(s) => s.parse::<usize>().unwrap_or(1024) * 1024 * 1024,
        //             _ => 1024 * 1024 * 1024,
        //         }
        //     };
        //     let cache = Arc::new(DiskCache::new(cache_size, 4 * 1024 * 1024));
        //     let cache_hdfs_store = Arc::new(ReadThroughCache::new(hdfs, cache));
        //     // register cache store
        //     runtime.register_object_store(url, cache_hdfs_store);
        // } else {
        //     runtime.register_object_store(url, hdfs);
        // }

        _runtime.register_object_store(_url, Arc::new(hdfs));
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
fn register_object_store(
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
                        "fs.s3a.bucket".to_string(),
                        url.host_str()
                            .ok_or(report!("host str missing"))?
                            .to_string(),
                    );
                }
                // add cache if env LAKESOUL_CACHE is set
                // set LAKESOUL_CACHE
                // LAKESOUL_CACHE_SIZE set lakesoul cache size,
                if std::env::var("LAKESOUL_CACHE").is_ok() {
                    register_s3_object_store_with_cache(
                        &url,
                        config,
                        runtime,
                        get_lakesoul_cache(),
                    )?;
                } else {
                    register_s3_object_store(&url, config, runtime)?;
                }
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
