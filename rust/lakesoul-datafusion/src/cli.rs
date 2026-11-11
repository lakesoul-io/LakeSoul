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
#[cfg(feature = "ivm")]
use lakesoul_ivm::{IvmOptions, PhysicalFormat, StateIndexMode};

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
    /// A scan runs as `ceil(bytes / (this * target_partitions))` tasks, capped
    /// at the number of workers; a scan left with one task runs on the
    /// coordinator. The library default (16 MiB) at the default 4 target
    /// partitions keeps scans up to 64 MiB on the coordinator.
    #[clap(long, env = "LAKESOUL_DISTRIBUTED_BYTES_PER_PARTITION")]
    pub distributed_bytes_per_partition: Option<usize>,

    /// `target_partitions` applied to distributed sessions.  The physical
    /// planner only emits the hash/range repartitions that become distributed
    /// stage boundaries when this is greater than 1, so it is forced to at
    /// least 2.
    #[clap(
        long,
        env = "LAKESOUL_DISTRIBUTED_TARGET_PARTITIONS",
        default_value_t = 4
    )]
    pub distributed_target_partitions: usize,
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
    /// EndpointSlice refresh, propagating its failure (missing token/CA,
    /// authorization errors) to the caller instead of a first-query failure.
    /// Zero eligible workers is not a startup failure.
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
        // `DistributedArgs::default()` leaves the field at 0; the CLI default
        // is 4.  Keep both paths on the library default while still forcing
        // the minimum of 2 that makes distributed stages possible.
        let target_partitions = if self.distributed_target_partitions == 0 {
            4
        } else {
            self.distributed_target_partitions.max(2)
        };
        let options = DistributedOptions {
            discovery: WorkerDiscovery::Shared(resolver),
            fallback_to_local: self.distributed_fallback_local,
            target_partitions,
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

/// Runtime options of the IVM refresh engine.
///
/// The library itself never reads environment variables; a coordinator and
/// every distributed worker build the same [`IvmOptions`] from these flags
/// (directly or through the matching environment variables) and install them
/// in their own process.
#[cfg(feature = "ivm")]
#[derive(Parser, Debug, Clone)]
pub struct IvmArgs {
    /// Number of independent views a chain refresh may run concurrently.
    #[clap(
        long = "ivm-view-parallelism",
        env = "LAKESOUL_IVM_VIEW_PARALLELISM",
        default_value_t = 1
    )]
    pub view_parallelism: usize,

    /// Maximum keyed state streams one lookup plan may expand at once;
    /// 0 leaves the gate unbounded.
    #[clap(
        long = "ivm-max-active-tasks",
        env = "LAKESOUL_IVM_MAX_ACTIVE_TASKS",
        default_value_t = 4
    )]
    pub max_active_tasks: usize,

    /// Maximum number of derived views one statement may materialize.
    #[clap(
        long = "ivm-max-derived-steps",
        env = "LAKESOUL_IVM_MAX_DERIVED_STEPS",
        default_value_t = 64
    )]
    pub max_derived_steps: usize,

    /// Row-level primary-key index policy for key-filtered state reads.
    #[clap(
        long = "ivm-state-index",
        env = "LAKESOUL_IVM_STATE_INDEX",
        value_enum,
        default_value = "auto"
    )]
    pub state_index: IvmStateIndexArg,

    /// Change ratio (percent of estimated live rows) above which a refresh
    /// rebuilds instead of replaying.
    #[clap(long = "ivm-rebuild-ratio", env = "LAKESOUL_IVM_REBUILD_RATIO")]
    pub rebuild_ratio: Option<u64>,

    /// Per-frame budget for materializing a top-k frame; 0 disables it.
    #[clap(
        long = "ivm-topk-materialize-bytes",
        env = "LAKESOUL_IVM_TOPK_MATERIALIZE_BYTES"
    )]
    pub topk_materialize_bytes: Option<usize>,

    /// Physical format of newly created IVM-internal tables.
    #[clap(
        long = "ivm-internal-file-format",
        env = "LAKESOUL_IVM_INTERNAL_FILE_FORMAT",
        value_enum,
        default_value = "vortex"
    )]
    pub internal_file_format: IvmFileFormatArg,

    /// Bounded memory pool for one streaming writer.
    #[clap(long = "ivm-write-pool-size", env = "LAKESOUL_IVM_WRITE_POOL_SIZE")]
    pub write_pool_size: Option<String>,

    /// Spill directory of the writer pool.
    #[clap(long = "ivm-write-pool-dir", env = "LAKESOUL_IVM_WRITE_POOL_DIR")]
    pub write_pool_dir: Option<String>,

    /// Override of the Vortex row-block size for internal tables.
    #[clap(
        long = "ivm-vortex-row-block-size",
        env = "LAKESOUL_IVM_VORTEX_ROW_BLOCK_SIZE"
    )]
    pub vortex_row_block_size: Option<String>,

    /// Override of the Vortex data-block target bytes for internal tables.
    #[clap(
        long = "ivm-vortex-data-block-bytes",
        env = "LAKESOUL_IVM_VORTEX_DATA_BLOCK_BYTES"
    )]
    pub vortex_data_block_bytes: Option<String>,

    /// Materialize keyed lookups on the coordinator instead of embedding
    /// their range-shuffle plan in the distributed refresh plan.
    #[clap(
        long = "ivm-distributed-keyed-local",
        env = "LAKESOUL_IVM_DISTRIBUTED_KEYED_LOCAL",
        value_parser = BoolValueParser::new(),
        action = ArgAction::Set,
        num_args(0..=1),
        require_equals(true),
        default_value("false"),
        default_missing_value("true")
    )]
    pub distributed_keyed_local: bool,

    /// Lease TTL for the per-view refresh lock, in milliseconds.
    #[clap(
        long = "ivm-refresh-lock-ttl-ms",
        env = "LAKESOUL_IVM_REFRESH_LOCK_TTL_MS",
        default_value_t = 1_800_000
    )]
    pub refresh_lock_ttl_ms: u64,
}

#[cfg(feature = "ivm")]
#[derive(ValueEnum, Debug, Clone, Copy, Default)]
#[clap(rename_all = "kebab-case")]
pub enum IvmStateIndexArg {
    /// Index Vortex state, scan Parquet state.
    #[default]
    Auto,
    /// Index every state format.
    Force,
    /// Never use the row-level index.
    Off,
}

#[cfg(feature = "ivm")]
#[derive(ValueEnum, Debug, Clone, Copy, Default)]
#[clap(rename_all = "kebab-case")]
pub enum IvmFileFormatArg {
    Parquet,
    #[default]
    Vortex,
    VortexCompact,
}

#[cfg(feature = "ivm")]
impl Default for IvmArgs {
    fn default() -> Self {
        Self {
            view_parallelism: 1,
            max_active_tasks: 4,
            max_derived_steps: 64,
            state_index: IvmStateIndexArg::Auto,
            rebuild_ratio: None,
            topk_materialize_bytes: None,
            internal_file_format: IvmFileFormatArg::Vortex,
            write_pool_size: None,
            write_pool_dir: None,
            vortex_row_block_size: None,
            vortex_data_block_bytes: None,
            distributed_keyed_local: false,
            refresh_lock_ttl_ms: 1_800_000,
        }
    }
}

#[cfg(feature = "ivm")]
impl IvmArgs {
    /// The behaviour options this process installs.
    pub fn options(&self) -> IvmOptions {
        IvmOptions {
            max_active_tasks: (self.max_active_tasks > 0)
                .then_some(self.max_active_tasks),
            view_parallelism: self.view_parallelism.max(1),
            max_derived_steps: self.max_derived_steps.max(1),
            state_index: match self.state_index {
                IvmStateIndexArg::Auto => StateIndexMode::Auto,
                IvmStateIndexArg::Force => StateIndexMode::Force,
                IvmStateIndexArg::Off => StateIndexMode::Off,
            },
            rebuild_ratio: self.rebuild_ratio,
            topk_materialize_bytes: self.topk_materialize_bytes,
            internal_file_format: match self.internal_file_format {
                IvmFileFormatArg::Parquet => PhysicalFormat::Parquet,
                IvmFileFormatArg::Vortex => PhysicalFormat::Vortex,
                IvmFileFormatArg::VortexCompact => PhysicalFormat::VortexCompact,
            },
            write_pool_size: self.write_pool_size.clone(),
            write_pool_dir: self.write_pool_dir.clone(),
            vortex_row_block_size: self.vortex_row_block_size.clone(),
            vortex_data_block_bytes: self.vortex_data_block_bytes.clone(),
            distributed_keyed_local: self.distributed_keyed_local,
            refresh_lock_ttl_ms: self.refresh_lock_ttl_ms.max(1000),
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

    #[cfg(feature = "ivm")]
    #[test]
    fn ivm_args_map_to_options() {
        let defaults = IvmArgs::default().options();
        assert_eq!(defaults.view_parallelism, 1);
        assert_eq!(defaults.max_active_tasks, Some(4));
        assert_eq!(defaults.max_derived_steps, 64);
        assert!(matches!(defaults.state_index, StateIndexMode::Auto));
        assert!(matches!(
            defaults.internal_file_format,
            PhysicalFormat::Vortex
        ));

        let args = IvmArgs {
            view_parallelism: 2,
            max_active_tasks: 0,
            max_derived_steps: 128,
            state_index: IvmStateIndexArg::Off,
            internal_file_format: IvmFileFormatArg::Parquet,
            ..IvmArgs::default()
        };
        let options = args.options();
        assert_eq!(options.view_parallelism, 2);
        assert_eq!(options.max_active_tasks, None);
        assert_eq!(options.max_derived_steps, 128);
        assert!(matches!(options.state_index, StateIndexMode::Off));
        assert!(matches!(
            options.internal_file_format,
            PhysicalFormat::Parquet
        ));
    }

    #[test]
    fn distributed_target_partitions_are_forced_above_one() {
        let args = DistributedArgs {
            distributed_workers: vec!["http://127.0.0.1:50051".to_string()],
            distributed_target_partitions: 1,
            ..DistributedArgs::default()
        };
        let options = futures::executor::block_on(args.resolve_distributed_options())
            .unwrap()
            .unwrap();
        assert_eq!(options.target_partitions, 2);
    }
}
