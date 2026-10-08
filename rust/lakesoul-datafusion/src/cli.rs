// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! CLI arguments shared by the LakeSoul coordinator and worker binaries.
//!
//! Locality priority is evaluated per field by Clap itself; no post-parse
//! merging happens:
//!
//! ```text
//! explicit command-line value > environment variable > program default
//! ```
//!
//! In particular an argument is *not* treated as "explicitly set" just
//! because it was passed with the value that happens to equal the default:
//! any explicit command-line value beats the environment variable, and an
//! unset command-line argument lets the environment variable (which in turn
//! beats the default). Bad environment values and configuration conflicts are
//! hard errors, never silent fallbacks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fmt};

use clap::builder::{BoolValueParser, TypedValueParser};
use clap::{ArgAction, Parser, ValueEnum};
use rootcause::bail;
use rootcause::prelude::ResultExt;
use url::Url;

use crate::Result;
use crate::distributed::resolver::{
    KubernetesDiscovery, KubernetesWorkerResolver, StaticWorkerResolver,
};
use crate::distributed::{DistributedOptions, WorkerDiscovery};

pub const DEFAULT_DISCOVERY_POLL_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_DISCOVERY_MAX_STALENESS: Duration = Duration::from_secs(30);
/// Upper bound on the start-up wait for the first Kubernetes discovery sync.
pub const DISCOVERY_STARTUP_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser, Debug, Default, Clone)]
pub struct CoreArgs {
    /// Prefix of LakeSoul datawarehouse
    #[arg(long, env = "LAKESOUL_WAREHOUSE")]
    pub warehouse_prefix: Option<String>,

    #[arg(long, env = "AWS_ENDPOINT")]
    pub endpoint: Option<String>,

    #[arg(long, env = "AWS_BUCKET")]
    pub s3_bucket: Option<String>,

    #[arg(long, env = "AWS_ACCESS_KEY_ID", hide_env = true)]
    pub s3_access_key: Option<String>,

    #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env = true)]
    pub s3_secret_key: Option<String>,

    /// Set-style boolean flag: bare `--s3-virtual-host-style` means `true`,
    /// `--s3-virtual-host-style=false` means an explicit `false` (which beats
    /// the environment variable), and nothing passed on the command line
    /// defers to `AWS_S3_VIRTUAL_HOST_STYLE`.
    #[arg(
        long,
        env = "AWS_S3_VIRTUAL_HOST_STYLE",
        value_parser = BoolValueParser::new(),
        action = ArgAction::Set,
        num_args(0..=1),
        require_equals(true),
        default_value("false"),
        default_missing_value("true")
    )]
    pub s3_virtual_host_style: bool,

    /// tokio worker threads
    #[clap(
        long,
        env = "LAKESOUL_WORKER_THREADS",
        default_value = "2",
        value_parser = clap::value_parser!(u32).range(2..).map(|n| {
            n as usize
        }))
    ]
    pub worker_threads: usize,
}

impl CoreArgs {
    pub fn s3_options(&self) -> HashMap<String, String> {
        let mut options = HashMap::new();
        if let Some(s3_bucket) = &self.s3_bucket {
            options.insert("fs.s3a.bucket".to_string(), s3_bucket.to_string());
        }
        if let Some(endpoint) = &self.endpoint {
            options.insert("fs.s3a.endpoint".to_string(), endpoint.to_string());
        }
        if let Some(s3_access_key) = &self.s3_access_key {
            options.insert("fs.s3a.access.key".to_string(), s3_access_key.to_string());
        }
        if let Some(s3_secret_key) = &self.s3_secret_key {
            options.insert("fs.s3a.secret.key".to_string(), s3_secret_key.to_string());
        }

        // default use virtual path style
        if self.s3_virtual_host_style {
            options.insert("fs.s3a.path.style.access".to_string(), "false".to_string());
        }

        options
    }

    pub fn from_env() -> Self {
        Self {
            warehouse_prefix: env::var("LAKESOUL_WAREHOUSE").ok(),
            endpoint: env::var("AWS_ENDPOINT").ok(),
            s3_bucket: env::var("AWS_BUCKET").ok(),
            s3_access_key: env::var("AWS_ACCESS_KEY_ID").ok(),
            s3_secret_key: env::var("AWS_SECRET_ACCESS_KEY").ok(),
            s3_virtual_host_style: env::var("AWS_S3_VIRTUAL_HOST_STYLE")
                .ok()
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false),
            worker_threads: 2,
        }
    }

    pub fn merge_from_other(&mut self, other: &Self) {
        if let Some(warehouse_prefix) = &other.warehouse_prefix {
            self.warehouse_prefix = Some(warehouse_prefix.clone());
        }
        if let Some(endpoint) = &other.endpoint {
            self.endpoint = Some(endpoint.clone());
        }
        if let Some(s3_access_key) = &other.s3_access_key {
            self.s3_access_key = Some(s3_access_key.clone());
        }
        if let Some(s3_secret_key) = &other.s3_secret_key {
            self.s3_secret_key = Some(s3_secret_key.clone());
        }
        if let Some(bucket) = &other.s3_bucket {
            self.s3_bucket = Some(bucket.clone());
        }
        self.s3_virtual_host_style |= other.s3_virtual_host_style;
        self.worker_threads = other.worker_threads;
    }
}

/// How a coordinator process discovers distributed workers.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
#[clap(rename_all = "lower")]
pub enum DiscoveryMode {
    /// No distributed planning at all (ignore any worker addresses).
    /// Parses (and prints) as `none`, not the variant's literal name.
    #[clap(name = "none")]
    Disabled,
    /// Static, operator-provided URL list.
    Static,
    /// Kubernetes EndpointSlice watching.
    Kubernetes,
}

/// Distributed-execution arguments, flattened into a coordinator's CLI.
///
/// The old form `--distributed-workers [url,...]` keeps working: it implies
/// `discovery = static` when no explicit mode is given.
#[derive(Parser, Debug, Default, Clone)]
pub struct DistributedArgs {
    /// Worker discovery mode: `none`, `static` or `kubernetes`. When omitted,
    /// `static` is inferred from `--distributed-workers`; no mode and no
    /// workers keeps every session single-node.
    #[clap(long, env = "LAKESOUL_DISTRIBUTED_DISCOVERY", value_enum)]
    pub discovery: Option<DiscoveryMode>,

    /// Distributed workers to plan against, comma separated (static mode).
    #[clap(long, value_delimiter = ',', env = "LAKESOUL_DISTRIBUTED_WORKERS")]
    pub distributed_workers: Vec<String>,

    /// Name of the worker Service whose EndpointSlices are watched
    /// (kubernetes mode). Selects slices labelled
    /// `kubernetes.io/service-name=<service>`.
    #[clap(long, env = "LAKESOUL_K8S_WORKER_SERVICE")]
    pub k8s_worker_service: Option<String>,

    /// Namespace holding the worker Service (kubernetes mode).
    #[clap(long, env = "LAKESOUL_K8S_NAMESPACE")]
    pub k8s_namespace: Option<String>,

    /// How often the EndpointSlice list is refreshed, in seconds. Must be
    /// at least 1 (and at most 86400): a zero poll interval would spin-loop
    /// on the Kubernetes API.
    #[clap(
        long,
        env = "LAKESOUL_DISCOVERY_POLL_INTERVAL_SECONDS",
        default_value_t = DEFAULT_DISCOVERY_POLL_INTERVAL.as_secs(),
        value_parser = clap::value_parser!(u64).range(1..=86400)
    )]
    pub discovery_poll_interval_seconds: u64,

    /// How long the last successful discovery snapshot stays eligible after
    /// the last successful refresh, in seconds. Must be at least 1 and at
    /// most 86400.
    #[clap(
        long,
        env = "LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS",
        default_value_t = DEFAULT_DISCOVERY_MAX_STALENESS.as_secs(),
        value_parser = clap::value_parser!(u64).range(1..=86400)
    )]
    pub discovery_max_staleness_seconds: u64,

    /// Fall back to single-node execution when the distributed planner fails
    /// to plan a query, or plans one whose stages cannot be sent to a worker.
    /// If disabled, that failure fails the query instead of silently running
    /// it on the coordinator.
    #[clap(
        long,
        env = "LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL",
        value_parser = BoolValueParser::new(),
        action = ArgAction::Set,
        num_args(0..=1),
        require_equals(true),
        default_value("false"),
        default_missing_value("true")
    )]
    pub distributed_fallback_local: bool,

    /// Overrides the distributed planner's bytes-per-partition scan estimate.
    /// Lower values fan a scan out over more tasks (and therefore workers);
    /// scans whose estimated size stays below the default (16 MiB) per
    /// partition are kept on the coordinator.
    #[clap(long, env = "LAKESOUL_DISTRIBUTED_BYTES_PER_PARTITION")]
    pub distributed_bytes_per_partition: Option<usize>,
}

impl DistributedArgs {
    /// Resolves the discovery backend from the parsed arguments.
    ///
    /// Errors instead of defaulting on every conflict: unknown/absent required
    /// fields and contradicting fields (e.g. `discovery = none` together with
    /// worker addresses, or `discovery = kubernetes` together with static
    /// worker addresses, or an empty Kubernetes service name) fail loudly.
    pub fn resolve_discovery(&self) -> Result<Option<WorkerDiscovery>> {
        let service = self.k8s_worker_service.as_deref().map(str::trim);
        let namespace = self.k8s_namespace.as_deref().map(str::trim);
        let has_workers = !self.distributed_workers.is_empty();

        match self.discovery {
            Some(DiscoveryMode::Disabled) => {
                if has_workers {
                    bail!(
                        "--distributed-workers conflicts with \
                         LAKESOUL_DISTRIBUTED_DISCOVERY=none: 'none' means no \
                         distributed planning at all"
                    );
                }
                if service.is_some() || namespace.is_some() {
                    bail!(
                        "kubernetes service/namespace conflicts with \
                         LAKESOUL_DISTRIBUTED_DISCOVERY=none"
                    );
                }
                Ok(None)
            }
            Some(DiscoveryMode::Static) => {
                if service.is_some() || namespace.is_some() {
                    bail!(
                        "kubernetes service/namespace conflicts with \
                         LAKESOUL_DISTRIBUTED_DISCOVERY=static"
                    );
                }
                // An explicitly empty static list is the documented way to
                // distribute-enable a session with zero workers.
                Ok(Some(WorkerDiscovery::Static(
                    self.distributed_workers.clone(),
                )))
            }
            Some(DiscoveryMode::Kubernetes) => {
                if has_workers {
                    bail!(
                        "--distributed-workers conflicts with \
                         LAKESOUL_DISTRIBUTED_DISCOVERY=kubernetes: kubernetes \
                         discovers workers from EndpointSlices"
                    );
                }
                let (Some(service), Some(namespace)) = (service, namespace) else {
                    bail!(
                        "discovery=kubernetes requires a non-empty worker \
                         Service name (--k8s-worker-service / \
                         LAKESOUL_K8S_WORKER_SERVICE) and namespace \
                         (--k8s-namespace / LAKESOUL_K8S_NAMESPACE)"
                    );
                };
                if service.is_empty() || namespace.is_empty() {
                    bail!(
                        "discovery=kubernetes requires a non-empty worker \
                         Service name (--k8s-worker-service / \
                         LAKESOUL_K8S_WORKER_SERVICE) and namespace \
                         (--k8s-namespace / LAKESOUL_K8S_NAMESPACE)"
                    );
                }
                let poll_interval =
                    Duration::from_secs(self.discovery_poll_interval_seconds);
                let staleness = self.discovery_max_staleness_seconds;
                // K8S API URL

                let host = std::env::var("KUBERNETES_SERVICE_HOST")
                    .context("invalid kubernetes api host")?;
                let port = std::env::var("KUBERNETES_SERVICE_PORT")
                    .context("invalid kubernetes api port")?;
                let raw = format!("https://{host}:{port}");
                let api_url =
                    Url::parse(&raw).context("invalid kubernetes api url `{raw}`")?;

                Ok(Some(WorkerDiscovery::Kubernetes(KubernetesDiscovery::new(
                    service,
                    namespace,
                    api_url,
                    poll_interval,
                    Duration::from_secs(staleness),
                ))))
            }
            None => {
                // Legacy form: `--distributed-workers a,b` (or its env var)
                // without an explicit mode implies static discovery — with
                // the same conflict rules as an explicit `static`.
                if has_workers {
                    if service.is_some() || namespace.is_some() {
                        bail!(
                            "kubernetes service/namespace conflicts with \
                             implied static discovery (--distributed-workers \
                             without an explicit mode): drop one of the two"
                        );
                    }
                    return Ok(Some(WorkerDiscovery::Static(
                        self.distributed_workers.clone(),
                    )));
                }
                if service.is_some() || namespace.is_some() {
                    bail!(
                        "kubernetes service/namespace given without \
                         --distributed-discovery: set \
                         LAKESOUL_DISTRIBUTED_DISCOVERY=kubernetes explicitly"
                    );
                }
                Ok(None)
            }
        }
    }

    /// Starts distributed execution for a coordinator process.
    ///
    /// One resolver (and therefore one discovery loop) per process: the
    /// returned options carry [`WorkerDiscovery::Shared`] so every session of
    /// this process reuses the same resolver instead of starting its own
    /// watcher.
    ///
    /// Kubernetes mode is bounded startup synchronization: waits up to
    /// [`DISCOVERY_STARTUP_SYNC_TIMEOUT`] for the first successful
    /// EndpointSlice refresh and at least one eligible worker, propagation of
    /// the failure (missing token/CA, authorization errors, no ready worker)
    /// to the caller instead of a first-query failure.
    pub async fn resolve_distributed_options(
        &self,
    ) -> Result<Option<DistributedOptions>> {
        let Some(discovery) = self.resolve_discovery()? else {
            return Ok(None);
        };
        let resolver: Arc<dyn datafusion_distributed::WorkerResolver> = match &discovery {
            WorkerDiscovery::Static(urls) => {
                std::sync::Arc::new(StaticWorkerResolver::new(urls.clone())?)
            }
            WorkerDiscovery::Kubernetes(config) => {
                let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
                    config,
                    DISCOVERY_STARTUP_SYNC_TIMEOUT,
                )
                .await?;
                Arc::new(resolver)
            }
            WorkerDiscovery::Shared(_) => {
                bail!("shared worker resolver cannot be configured from the CLI")
            }
        };
        let options = DistributedOptions {
            discovery: WorkerDiscovery::Shared(resolver),
            fallback_to_local: self.distributed_fallback_local,
            ..DistributedOptions::default()
        };
        let options = match self.distributed_bytes_per_partition {
            Some(bytes_per_partition) => DistributedOptions {
                bytes_per_partition: Some(bytes_per_partition),
                ..options
            },
            None => options,
        };
        Ok(Some(options))
    }
}

impl fmt::Display for DiscoveryMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("none"),
            Self::Static => f.write_str("static"),
            Self::Kubernetes => f.write_str("kubernetes"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_options_include_explicit_endpoint() {
        let args = CoreArgs {
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            ..Default::default()
        };

        assert_eq!(
            args.s3_options().get("fs.s3a.endpoint").map(String::as_str),
            Some("http://127.0.0.1:9000")
        );
    }
}
