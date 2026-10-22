// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Worker discovery for the distributed planner.
//!
//! Both backends implement [`datafusion_distributed::WorkerResolver`], whose
//! `get_urls()` is synchronous and is called several times per query (task
//! counting, then routing right before execution). Both implementations
//! therefore answer from an **in-memory snapshot** only:
//! - [`StaticWorkerResolver`]: a fixed URL list (local development, bare
//!   metal). Building the resolver and [`StaticWorkerResolver::update`] probe
//!   every listed worker once and drop those answering with a different
//!   protocol version; unreachable workers stay listed and fail at dispatch
//!   time as before.
//! - [`KubernetesWorkerResolver`]: a background task polls the Kubernetes
//!   API (EndpointSlice listing refreshed on a fixed interval) and probes each
//!   discovered endpoint's protocol version over the worker gRPC channel.
//!   Only endpoints that are Ready and report
//!   [`crate::distributed::DISTRIBUTED_PROTOCOL_VERSION`] enter the snapshot.
//!   The resolver is created once per process and shared by every session
//!   ([`WorkerDiscovery::Shared`]); snapshot staleness beyond the configured
//!   maximum makes `get_urls()` fail rather than planning against outdated
//!   workers. Empty snapshots are *not* silently planned locally either.
//!
//! No `get_urls()` call ever performs a synchronous Kubernetes API request.
//!
//! Discovery state is exported through the `metrics` facade:
//! - `lakesoul_worker_discovery_eligible_workers`: workers in the published
//!   snapshot (both backends);
//! - `lakesoul_worker_discovery_refresh_failures_total{kind="hard"|"transient"}`:
//!   failed EndpointSlice refreshes;
//! - `lakesoul_worker_discovery_snapshot_age_seconds`: time since the last
//!   successful refresh, updated after every refresh attempt; `+Inf` until the
//!   first success.
//!
//! The Kubernetes series are registered when the resolver starts, so a
//! failure burst right after startup still shows in `increase()`.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion_distributed::{
    ChannelResolver, GetWorkerInfoRequest, WorkerResolver, grpc,
};
use parking_lot::RwLock;
use reqwest::StatusCode;
use rootcause::markers::Cloneable;
use rootcause::prelude::ResultExt;
use rootcause::{Report, bail, report};
use url::Url;

use crate::Result;
use crate::distributed::DISTRIBUTED_PROTOCOL_VERSION;

const ELIGIBLE_WORKERS: &str = "lakesoul_worker_discovery_eligible_workers";
const REFRESH_FAILURES_TOTAL: &str = "lakesoul_worker_discovery_refresh_failures_total";
const SNAPSHOT_AGE_SECONDS: &str = "lakesoul_worker_discovery_snapshot_age_seconds";

fn describe_discovery_metrics() {
    metrics::describe_gauge!(
        ELIGIBLE_WORKERS,
        "Workers in the published discovery snapshot"
    );
    metrics::describe_counter!(
        REFRESH_FAILURES_TOTAL,
        "Failed worker discovery refreshes grouped by kind (hard blocks planning \
         at once, transient keeps the snapshot until it goes stale)"
    );
    metrics::describe_gauge!(
        SNAPSHOT_AGE_SECONDS,
        metrics::Unit::Seconds,
        "Time since the last successful worker discovery refresh, +Inf before \
         the first one"
    );
}

/// Handles of the Kubernetes discovery series, resolved once against the
/// recorder installed when the resolver starts.
struct DiscoveryMetrics {
    eligible: metrics::Gauge,
    hard_failures: metrics::Counter,
    transient_failures: metrics::Counter,
    snapshot_age: metrics::Gauge,
}

impl DiscoveryMetrics {
    /// Registers every series: failure counters at 0, no eligible worker,
    /// and an infinite age until the first successful refresh.
    fn register() -> Self {
        describe_discovery_metrics();
        let this = Self {
            eligible: metrics::gauge!(ELIGIBLE_WORKERS),
            hard_failures: metrics::counter!(REFRESH_FAILURES_TOTAL, "kind" => "hard"),
            transient_failures: metrics::counter!(
                REFRESH_FAILURES_TOTAL,
                "kind" => "transient"
            ),
            snapshot_age: metrics::gauge!(SNAPSHOT_AGE_SECONDS),
        };
        this.eligible.set(0.0);
        this.hard_failures.increment(0);
        this.transient_failures.increment(0);
        this.snapshot_age.set(f64::INFINITY);
        this
    }

    fn record_failure(&self, failure: &DiscoveryFailure) {
        match failure {
            DiscoveryFailure::Hard(_) => self.hard_failures.increment(1),
            DiscoveryFailure::Transient(_) => self.transient_failures.increment(1),
        }
    }

    fn record_age(&self, last_success: Option<Instant>) {
        self.snapshot_age.set(
            last_success.map_or(f64::INFINITY, |at| at.elapsed().as_secs_f64()),
        );
    }
}

/// Publishes the eligible-worker count of a static snapshot.
fn record_static_eligible(snapshot: &WorkerSnapshot) {
    describe_discovery_metrics();
    metrics::gauge!(ELIGIBLE_WORKERS).set(snapshot.urls.len() as f64);
}

/// In-memory snapshot of the currently eligible workers.
#[derive(Debug, Clone, Default)]
pub struct WorkerSnapshot {
    urls: Vec<Url>,
}

impl WorkerSnapshot {
    pub fn new(urls: Vec<Url>) -> Self {
        Self { urls }
    }

    /// The snapshot's worker URLs. This list is replaced wholesale by the
    /// background watcher; treat it as point-in-time.
    pub fn urls(&self) -> &[Url] {
        &self.urls
    }
}

impl WorkerResolver for WorkerSnapshot {
    fn get_urls(&self) -> DFResult<Vec<Url>> {
        Ok(self.urls.clone())
    }
}

/// Where the coordinator discovers workers from.
#[derive(Clone)]
pub enum WorkerDiscovery {
    /// Static URL list for local development and bare-metal clusters.
    Static(Vec<String>),
    /// Kubernetes EndpointSlice watching.
    Kubernetes(KubernetesDiscovery),
    /// An already-built resolver shared by every session of one process.
    ///
    /// Coordinator binaries build the resolver once at start-up (bounded
    /// waiting for the first Kubernetes sync, see
    /// [`KubernetesWorkerResolver::start_and_wait_for_first_sync`]) and
    /// wrap it in here; every session of the process then reuses the same
    /// instance instead of starting its own discovery loop. `get_urls()`
    /// continues to return only in-memory snapshot data.
    Shared(Arc<dyn WorkerResolver>),
}

impl fmt::Debug for WorkerDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(urls) => f.debug_tuple("Static").field(urls).finish(),
            Self::Kubernetes(kubernetes) => {
                f.debug_tuple("Kubernetes").field(kubernetes).finish()
            }
            // Deliberately opaque: the resolver's snapshot changes over time
            // and its inside is not part of the options' contract.
            Self::Shared(_) => f
                .debug_tuple("Shared")
                .field(&"<shared worker resolver>")
                .finish(),
        }
    }
}

/// Resolver over a fixed, operator-provided worker URL list.
///
/// Intended for local development and bare-metal deployments where workers
/// are launched out-of-band. It never performs I/O in `get_urls()`.
///
/// Unlike [`KubernetesWorkerResolver`] there is no background watcher: the
/// configured list is probed synchronously when the resolver is built and
/// on [`StaticWorkerResolver::update`], dropping workers that answer with a
/// different [`DISTRIBUTED_PROTOCOL_VERSION`]. A worker restarted with a
/// different build is therefore re-evaluated the next time a resolver is
/// built or the list is updated.
#[derive(Debug, Clone)]
pub struct StaticWorkerResolver {
    snapshot: Arc<RwLock<WorkerSnapshot>>,
}

impl StaticWorkerResolver {
    /// Builds the resolver from raw URL strings; invalid entries fail here
    /// instead of at query time.
    pub fn new(urls: Vec<String>) -> Result<Self> {
        let snapshot =
            WorkerSnapshot::new(drop_version_mismatched_blocking(parse_urls(urls)?));
        record_static_eligible(&snapshot);
        Ok(Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
        })
    }

    /// Replaces the URL list (e.g. after an operator-side rebalance). The
    /// new list is version-probed like [`StaticWorkerResolver::new`].
    pub fn update(&self, urls: Vec<String>) -> Result<()> {
        let snapshot =
            WorkerSnapshot::new(drop_version_mismatched_blocking(parse_urls(urls)?));
        record_static_eligible(&snapshot);
        *self.snapshot.write() = snapshot;
        Ok(())
    }

    /// The current snapshot (for diagnostics).
    pub fn snapshot(&self) -> WorkerSnapshot {
        self.snapshot.read().clone()
    }
}

impl WorkerResolver for StaticWorkerResolver {
    fn get_urls(&self) -> DFResult<Vec<Url>> {
        Ok(self.snapshot.read().urls.clone())
    }
}

/// Parameters for the Kubernetes EndpointSlice watcher.
#[derive(Debug, Clone)]
pub struct KubernetesDiscovery {
    /// Name of the worker Service. The watcher lists the EndpointSlices of
    /// this Service via the `kubernetes.io/service-name=<service>` label
    /// selector; EndpointSlices do not inherit Pod labels, so relying on some
    /// `app=` label would silently list nothing.
    pub service: String,
    /// Namespace that holds the worker Service and its EndpointSlices.
    pub namespace: String,
    /// API server base URL. Defaults to the in-cluster environment
    /// (`KUBERNETES_SERVICE_HOST`/`KUBERNETES_SERVICE_PORT`).
    api_url: Url,
    /// Bearer token override. When `Some`, this token is fixed: the mounted
    /// service account token rotates, so it is **re-read from the token file
    /// on every request** except when an operator supplies this override.
    pub token: Option<String>,
    /// Mounted token file to read each request from when `token` is `None`.
    /// `None` means the in-cluster default path; tests and non-standard
    /// mounts use this knob instead of environment-variable tricks.
    pub token_path: Option<String>,
    /// How often the EndpointSlice list is refreshed and version probes are
    /// re-checked. Defaults to 5 s. Must be at least 1 s (never zero).
    pub poll_interval: Duration,
    /// How long the last successful snapshot stays eligible after the last
    /// successful refresh. Defaults to 30 s. Once exceeded, new distributed
    /// queries fail until a refresh succeeds again; within the window the
    /// last known-good snapshot keeps serving (a transient API outage must
    /// not drain an otherwise healthy cluster). Must be at least 1 s.
    pub max_staleness: Duration,
}

impl KubernetesDiscovery {
    /// `api_url` overrides the Kubernetes API server base URL; `None` uses
    /// the in-cluster environment (`KUBERNETES_SERVICE_HOST`/`_PORT`).
    pub fn new(
        service: impl Into<String>,
        namespace: impl Into<String>,
        api_url: Url,
        poll_interval: Duration,
        max_staleness: Duration,
    ) -> Self {
        Self {
            service: service.into(),
            namespace: namespace.into(),
            api_url,
            token: None,
            token_path: None,
            poll_interval,
            max_staleness,
        }
    }

    /// The label selector identifying this Service's EndpointSlices.
    pub fn label_selector(&self) -> String {
        format!("kubernetes.io/service-name={}", self.service)
    }

    /// Base URL of the Kubernetes API server, normalized without a trailing
    /// slash. Defaults to the in-cluster environment
    /// (`KUBERNETES_SERVICE_HOST`/`KUBERNETES_SERVICE_PORT`).
    pub(crate) fn api_url(&self) -> &Url {
        &self.api_url
    }

    /// Path of the service account token file to (re-)read on every request.
    pub(crate) fn token_path(&self) -> String {
        self.token_path
            .clone()
            .unwrap_or_else(|| TOKEN_PATH.to_string())
    }
}

const TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const CA_CERT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

/// Reads the service account token file, erroring (never falling back to an
/// anonymous request) when it is missing or unreadable.
pub(crate) fn read_service_account_token(path: &str) -> Result<String> {
    Ok(std::fs::read_to_string(path)
        .map(|token| token.trim().to_string())
        .context(format!(
            "service account token unreadable at {path}; \
                    kubernetes discovery refuses to send anonymous requests, \
                    set LAKESOUL_K8S_WORKER_TOKEN or mount a token ",
        ))?)
}

fn parse_urls(urls: Vec<String>) -> Result<Vec<Url>> {
    urls.iter()
        .map(|url| Ok(Url::parse(url).context(format!("invalid worker url: {url}"))?))
        .collect()
}

/// Resolver backed by a Kubernetes EndpointSlice poll.
///
/// A background task periodically lists the EndpointSlices matching
/// `kubernetes.io/service-name=<service>`, probes each Ready endpoint's
/// protocol version and publishes the matching URLs into the snapshot.
/// `get_urls()` only ever reads the snapshot:
///
/// - never synced → error (the query fails instead of silently planning
///   single-node);
/// - last successful refresh older than `max_staleness` → error, whatever the
///   fresher failures were;
/// - synced but empty (no endpoint currently reports this coordinator's
///   protocol version) → error;
/// - within staleness after a successful refresh → the last snapshot, even if
///   interim refresh attempts failed (a transient API outage must not drain
///   an otherwise healthy pipeline).
///
/// Authorization and configuration errors (401/403, missing token/CA) are
/// surfaced in the error and logs — never presented as a healthy state. A
/// successful refresh yielding candidates but zero eligible workers replaces
/// the snapshot with the empty list immediately: "no ready worker now" is a
/// fact, not a reason to keep routing to workers that have disappeared.
#[derive(Clone, Debug)]
pub struct KubernetesWorkerResolver {
    state: Arc<KubernetesDiscoveryState>,
    max_staleness: Duration,
    description: String,
}

/// State shared between the background refresh loop and every
/// [`KubernetesWorkerResolver`] (and therefore every session) of a process.
#[derive(Debug, Default)]
struct KubernetesDiscoveryState {
    snapshot: RwLock<WorkerSnapshot>,
    status: RwLock<DiscoveryStatus>,
}

/// Visibility into the last refresh outcome; kept small and summary-shaped
/// (no large state machinery): what the logs and start-up errors need.
#[derive(Debug, Default, Clone)]
struct DiscoveryStatus {
    last_success: Option<Instant>,
    last_error: Option<Report<DiscoveryFailure, Cloneable>>,
    /// Set by [`DiscoveryFailure::Hard`] refreshes (authorization, config,
    /// uninterpretable documents). While set, `get_urls` fails immediately
    /// even within the staleness window: these states must not read as
    /// healthy. Cleared by the next successful refresh.
    hard_error: Option<Report<DiscoveryFailure, Cloneable>>,
    candidates: usize,
    eligible: usize,
}

impl KubernetesDiscoveryState {
    fn has_synced(&self) -> bool {
        self.status.read().last_success.is_some()
    }
}

impl KubernetesWorkerResolver {
    /// Starts the polling task and returns the resolver immediately.
    ///
    /// The task terminates when the last resolver-clone state is dropped.
    pub fn start(discovery: &KubernetesDiscovery) -> Result<Self> {
        let client = build_http_client(discovery)?;
        let api_url = discovery.api_url().clone();
        let namespace = discovery.namespace.clone();
        let service_label = discovery.service.clone();
        let token_override = discovery.token.clone();
        let interval = discovery.poll_interval;

        let max_staleness = discovery.max_staleness;
        let state = Arc::new(KubernetesDiscoveryState::default());
        let discovery_metrics = DiscoveryMetrics::register();

        let loop_state = Arc::downgrade(&state);
        let selector = discovery.label_selector();
        let loop_service = service_label.clone();
        let loop_namespace = namespace.clone();
        let loop_token_path = discovery.token_path();
        let description = format!(
            "service={} namespace={} api={api_url}",
            service_label, namespace
        );
        tokio::spawn(async move {
            // The polling loop exit condition is `Upgrade::upgrade` returning
            // None, which happens once every Arc reference to the resolver
            // has been dropped.
            while let Some(handle) = loop_state.upgrade() {
                let status = fetch_endpointslices(
                    &client,
                    &api_url,
                    &loop_namespace,
                    &selector,
                    token_override.as_deref(),
                    &loop_token_path,
                )
                .await;
                match status {
                    Ok(candidates) => {
                        let candidates_count = candidates.len();
                        let eligible = filter_version_matched(candidates).await;
                        *handle.snapshot.write() = WorkerSnapshot::new(eligible.clone());
                        {
                            let mut snapshot_status = handle.status.write();
                            snapshot_status.last_success = Some(Instant::now());
                            snapshot_status.last_error = None;
                            snapshot_status.hard_error = None;
                            snapshot_status.candidates = candidates_count;
                            snapshot_status.eligible = eligible.len();
                        }
                        discovery_metrics.eligible.set(eligible.len() as f64);
                        info!(
                            service = %loop_service,
                            namespace = %loop_namespace,
                            candidates = candidates_count,
                            eligible = eligible.len(),
                            "LakeSoul worker discovery: EndpointSlice refresh ok"
                        );
                    }
                    Err(report) => {
                        // Transient failures keep the last successful
                        // snapshot (staleness decides when it expires);
                        // hard failures additionally block get_urls at
                        // once by recording `hard_error`.
                        let report = report.into_cloneable();
                        warn!(
                            service = %loop_service,
                            namespace = %loop_namespace,
                            error = %report,
                            "LakeSoul worker discovery: EndpointSlice refresh failed"
                        );
                        discovery_metrics.record_failure(report.current_context());
                        let mut handle_status = handle.status.write();
                        if let DiscoveryFailure::Hard(_) = report.current_context() {
                            handle_status.hard_error = Some(report.clone());
                        }
                        handle_status.last_error = Some(report);
                    }
                }
                discovery_metrics.record_age(handle.status.read().last_success);
                drop(handle);
                if loop_state.upgrade().is_none() {
                    break;
                }
                tokio::time::sleep(interval).await;
            }
        });
        Ok(Self {
            state,
            max_staleness,
            description,
        })
    }

    /// Starts the polling task and waits, bounded, for the first successful
    /// refresh.
    ///
    /// Coordinator processes build the resolver at start-up with this instead
    /// of the first PG connection: once serving, the shared resolver is only
    /// read. Authorization errors, missing token/CA and uninterpretable API
    /// responses fail here with the refresh status instead of inside a query.
    /// An empty worker set is a valid start: queries fail with "no eligible
    /// worker" until a worker turns Ready and a later refresh lists it.
    pub async fn start_and_wait_for_first_sync(
        discovery: &KubernetesDiscovery,
        timeout: Duration,
    ) -> Result<Self> {
        let resolver = Self::start(discovery)?;
        let deadline = Instant::now() + timeout;
        loop {
            if resolver.state.has_synced() {
                let (candidates, eligible) = {
                    let status = resolver.state.status.read();
                    (status.candidates, status.eligible)
                };
                if eligible == 0 {
                    warn!(
                        candidates,
                        "LakeSoul worker discovery: starting without an eligible \
                         worker ({}); queries fail until one is Ready",
                        resolver.description
                    );
                }
                return Ok(resolver);
            }
            if Instant::now() >= deadline {
                let last_error = resolver.state.status.read().last_error.clone();
                let summary = format!(
                    "kubernetes worker discovery did not complete an EndpointSlice \
                     refresh within {timeout:?} ({})",
                    resolver.description
                );
                return Err(match last_error {
                    Some(error) => error.context(summary).into_dynamic(),
                    None => report!("{summary}: no refresh attempt finished"),
                });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// The current snapshot (for diagnostics).
    pub fn snapshot(&self) -> WorkerSnapshot {
        self.state.snapshot.read().clone()
    }

    /// Last refresh outcome for diagnostics: (age since last success, error
    /// text, candidate count, eligible count).
    pub fn refresh_status(&self) -> (Option<Duration>, Option<String>, usize, usize) {
        let status = self.state.status.read();
        (
            status.last_success.map(|at| at.elapsed()),
            status.last_error.as_ref().map(ToString::to_string),
            status.candidates,
            status.eligible,
        )
    }
}

impl WorkerResolver for KubernetesWorkerResolver {
    /// Snapshot read only: never a network request, never a Kubernetes API
    /// call.
    fn get_urls(&self) -> DFResult<Vec<Url>> {
        let status = self.state.status.read().clone();
        let Some(last_success) = status.last_success else {
            return Err(DataFusionError::Configuration(format!(
                "kubernetes worker discovery ({}) has not completed a \
                 successful EndpointSlice refresh{}; refusing to plan against \
                 an unknown worker set",
                self.description,
                match status.last_error {
                    Some(error) => format!(" (last refresh error: {error})"),
                    None => String::new(),
                }
            )));
        };
        // A hard refresh failure (authorization, config, uninterpretable
        // document) follows successful syncs too: serving the previous
        // snapshot as if nothing happened would hide the breakage.
        if let Some(hard_error) = &status.hard_error {
            return Err(DataFusionError::Configuration(format!(
                "kubernetes worker discovery ({}) hit an unrecoverable                  refresh failure while holding a valid snapshot: {}",
                self.description, hard_error
            )));
        }
        let age = last_success.elapsed();
        if age > self.max_staleness {
            return Err(DataFusionError::Configuration(format!(
                "kubernetes worker discovery ({}) snapshot is stale: last \
                 successful refresh {:.1?} ago, staleness limit {:.1?}{}; \
                 refusing to plan against possibly outdated workers",
                self.description,
                age,
                self.max_staleness,
                match status.last_error {
                    Some(error) => format!(" (pending refresh failure: {error})"),
                    None => String::new(),
                }
            )));
        }
        let urls = self.state.snapshot.read().urls().to_vec();
        if urls.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "kubernetes worker discovery ({}) found no eligible worker: \
                 the endpoints are not Ready (or not terminating-safe) or \
                 rejected this coordinator's protocol version \
                 {DISTRIBUTED_PROTOCOL_VERSION}; refusing silent single-node \
                 planning",
                self.description
            )));
        }
        Ok(urls)
    }
}

fn build_http_client(discovery: &KubernetesDiscovery) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(concat!("lakesoul-datafusion/", env!("CARGO_PKG_VERSION")));
    // The CA bundle is only needed for HTTPS: a plain-HTTP API server (only
    // ever a test harness) needs no trust anchor. For HTTPS a missing bundle
    // is an explicit configuration error — no anonymous, no system-default
    // trust pool for the in-cluster CA.
    if discovery.api_url().scheme() == "https" {
        let ca_path = CA_CERT_PATH;
        let pem = std::fs::read(&ca_path).context(format!(
            "kubernetes API is HTTPS but the CA bundle is unreadable at {};",
            ca_path
        ))?;
        let cert = reqwest::Certificate::from_pem(&pem).map_err(|err| {
            DataFusionError::Configuration(format!(
                "invalid Kubernetes CA bundle at {ca_path}: {err}"
            ))
        })?;
        builder = builder.add_root_certificate(cert);
    }
    Ok(builder.build().context("building kubernetes client")?)
}

/// A failed EndpointSlice refresh, classified by what it means for the
/// last published snapshot.
#[derive(Debug)]
enum DiscoveryFailure {
    /// Authorization, configuration or protocol incompatibilities (401/403,
    /// unreadable token, a document the parser cannot interpret). Suppresses
    /// the previous snapshot immediately — those states are not "healthy".
    Hard(String),
    /// Transport-level outages (connection refused, timeouts, 5xx). The
    /// last snapshot stays usable until staleness expires it.
    Transient(String),
}

impl fmt::Display for DiscoveryFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hard(message) => write!(f, "{message}"),
            Self::Transient(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for DiscoveryFailure {}

async fn fetch_endpointslices(
    client: &reqwest::Client,
    api_url: &Url,
    namespace: &str,
    selector: &str,
    token_override: Option<&str>,
    token_path: &str,
) -> Result<Vec<Url>, Report<DiscoveryFailure>> {
    let mut url = api_url
        .join(&format!(
            "/apis/discovery.k8s.io/v1/namespaces/{namespace}/endpointslices"
        ))
        .context(DiscoveryFailure::Hard(
            "failed to construct Kubernetes EndpointSlice URL".into(),
        ))?;

    url.query_pairs_mut().append_pair("labelSelector", selector);

    // Bearer token: an explicit override is fixed; otherwise the mounted
    // token is re-read on every request because Kubernetes rotates it.
    // A missing token errors — authentication is never skipped.
    let token = match token_override {
        Some(token) => token.to_owned(),

        None => read_service_account_token(token_path)
            .context(DiscoveryFailure::Hard(
                "failed to read Kubernetes service account token".into(),
            ))
            .attach(format!("token_path={token_path}"))?,
    };

    let response = client
        .get(url.clone())
        .bearer_auth(token)
        .send()
        .await
        .context(DiscoveryFailure::Transient(
            "EndpointSlice request failed".into(),
        ))
        .attach(format!("url={url}"))?;

    let http_status = response.status();

    if !http_status.is_success() {
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                return Err(report!(DiscoveryFailure::Transient(
                    "failed to read unsuccessful EndpointSlice response body".into(),
                ))
                .attach(format!("status={http_status}"))
                .attach(format!("url={url}"))
                .attach(err));
            }
        };

        let body = body.chars().take(200).collect::<String>();

        // Server-side outages and throttling are transient. Other HTTP
        // failures generally indicate authentication, authorization,
        // request/configuration, or protocol problems and suppress the
        // previous snapshot immediately.
        let transient =
            http_status.is_server_error() || http_status == StatusCode::TOO_MANY_REQUESTS;

        let failure = if transient {
            DiscoveryFailure::Transient(format!(
                "EndpointSlice request returned {http_status}"
            ))
        } else {
            DiscoveryFailure::Hard(format!(
                "EndpointSlice request returned {http_status}"
            ))
        };

        let mut report = report!(failure)
            .attach(format!("url={url}"))
            .attach(format!("status={http_status}"));

        if !body.is_empty() {
            report = report.attach(format!("response_body={body}"));
        }

        return Err(report);
    }

    let body = response
        .text()
        .await
        .context(DiscoveryFailure::Transient(
            "failed to read EndpointSliceList response body".into(),
        ))
        .attach(format!("url={url}"))?;

    ready_worker_urls(&body).context(DiscoveryFailure::Hard(
        "failed to interpret EndpointSliceList".into(),
    ))
}

/// The worker gRPC port the slice declares, if any.
///
/// Rules, in order:
/// 1. the TCP port named `grpc` — the documented worker service port; a
///    `grpc` port without a numeric value or outside 1..=65535 is a hard
///    error;
/// 2. no TCP port named `grpc` but exactly one TCP port → that port, for
///    compatibility with slices that declare a single unnamed TCP port
///    (invalid values are a hard error);
/// 3. several TCP ports and none named `grpc` → `None`: ambiguous, the slice
///    cannot be interpreted and is skipped rather than guessed.
///
/// UDP/SCTP ports never count (the worker channel is gRPC over TCP).
fn slice_grpc_port(ports: &[SlicePort]) -> Result<Option<i64>> {
    // Validate numeric values but retain a slice that may still have a grpc port
    let mut named_grpc = None;
    let mut unnamed_tcp = Vec::new();
    for port in ports {
        let is_tcp = port
            .protocol
            .as_deref()
            .unwrap_or("TCP")
            .eq_ignore_ascii_case("TCP");
        if !is_tcp {
            continue;
        }
        if let Some(name) = port.name.as_deref()
            && name == "grpc"
        {
            named_grpc = Some(port.port);
            break;
        }
        unnamed_tcp.push(port.port);
    }
    if let Some(port) = named_grpc {
        return match port {
            Some(value) if valid_port(value) => Ok(Some(value)),
            // Named `grpc` with an out-of-range/missing value means a broken
            // service port declaration, not "no worker here": fail visibly.
            Some(value) => bail!(
                "EndpointSlice declares TCP port named `grpc` with invalid \
                 port value {value}: expected 1..=65535"
            ),
            None => bail!(
                "EndpointSlice declares TCP port named `grpc` without a \
                 numeric port value"
            ),
        };
    }
    if unnamed_tcp.len() == 1 {
        return match unnamed_tcp[0] {
            Some(value) if valid_port(value) => Ok(Some(value)),
            Some(value) => bail!(
                "EndpointSlice declares a single TCP port with invalid value \
                 {value}: expected 1..=65535"
            ),
            None => bail!(
                "EndpointSlice declares a single TCP port without a numeric \
                 port value"
            ),
        };
    }
    Ok(None)
}

/// Builds `http://<address>:<port>`, bracketing IPv6 literals.
fn endpoint_url(address: &str, port: i64) -> Result<Url> {
    let host = if address.contains(':') {
        format!("[{address}]")
    } else {
        address.to_string()
    };
    Url::parse(&format!("http://{host}:{port}"))
        .map_err(|err| report!("invalid worker endpoint address {address:?}: {err}"))
}

/// Extracts worker URLs from an EndpointSliceList JSON document.
///
/// Only endpoints whose `conditions.ready` is `true` (and not terminating)
/// are considered; [`slice_grpc_port`] picks the port. Addresses parse as
/// IPv4 or IPv6 URL hosts; a malformed address is a hard error, and repeated
/// addresses (rolling updates can list one address in several slices) are
/// deduplicated.
pub fn ready_worker_urls(list_json: &str) -> Result<Vec<Url>> {
    let list: EndpointSliceList =
        serde_json::from_str(list_json).context("invalid EndpointSliceList")?;

    let mut urls = Vec::new();
    // A rolling update can list the same address in several EndpointSlices;
    // collapsing them keeps one entry (and one probe) per worker.
    let mut seen = HashSet::new();
    for slice in list.items {
        let Some(port) = slice_grpc_port(&slice.ports)? else {
            continue;
        };
        for endpoint in slice.endpoints {
            let ready = endpoint
                .conditions
                .as_ref()
                .and_then(|conditions| conditions.ready)
                .unwrap_or(true);
            let terminating = endpoint
                .conditions
                .as_ref()
                .and_then(|conditions| conditions.terminating)
                .unwrap_or(false);
            if !ready || terminating {
                continue;
            }
            for address in endpoint.addresses {
                let url = endpoint_url(&address, port)?;
                if seen.insert(url.clone()) {
                    urls.push(url);
                }
            }
        }
    }
    Ok(urls)
}

/// EndpointSliceList JSON node (the list returned by the API server).
#[derive(serde::Deserialize)]
struct EndpointSliceList {
    #[serde(default, deserialize_with = "null_as_empty")]
    items: Vec<EndpointSlice>,
}

/// One EndpointSlice JSON node.
#[derive(serde::Deserialize)]
struct EndpointSlice {
    #[serde(default, deserialize_with = "null_as_empty")]
    endpoints: Vec<SliceEndpoint>,
    #[serde(default, deserialize_with = "null_as_empty")]
    ports: Vec<SlicePort>,
}

/// One endpoint JSON node of a slice.
#[derive(serde::Deserialize)]
struct SliceEndpoint {
    #[serde(default, deserialize_with = "null_as_empty")]
    addresses: Vec<String>,
    #[serde(default)]
    conditions: Option<SliceEndpointConditions>,
}

/// Endpoint readiness conditions, all optional per the API.
#[derive(serde::Deserialize)]
struct SliceEndpointConditions {
    #[serde(default)]
    ready: Option<bool>,
    #[serde(default)]
    terminating: Option<bool>,
}

/// One declared port JSON node of a slice.
#[derive(serde::Deserialize)]
struct SlicePort {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    port: Option<i64>,
    #[serde(default)]
    protocol: Option<String>,
}

/// Reads a JSON list that may be `null` as empty. The API server encodes nil Go
/// slices of fields without `omitempty` as `null`: the placeholder EndpointSlice
/// the controller keeps for a Service without Pods carries
/// `"endpoints":null,"ports":null`.
fn null_as_empty<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Ok(
        <Option<Vec<T>> as serde::Deserialize>::deserialize(deserializer)?
            .unwrap_or_default(),
    )
}

/// Whether `port` is a valid TCP port value.
fn valid_port(port: i64) -> bool {
    (1..=65535).contains(&port)
}

/// Result of probing a worker's advertised protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VersionProbe {
    /// The worker reports [`DISTRIBUTED_PROTOCOL_VERSION`].
    Match,
    /// The worker answered but reports a different protocol version.
    Mismatch,
    /// The worker did not answer within the probe timeout.
    Unreachable,
}

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Timeout for the synchronous probes run while building or updating a
/// [`StaticWorkerResolver`]. Shorter than [`PROBE_TIMEOUT`]: an unreachable
/// worker must not stall session creation noticeably.
const STATIC_PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Probes `candidates` over the worker gRPC channel and pairs every URL
/// with its verdict.
async fn probe_versions(
    candidates: Vec<Url>,
    timeout: Duration,
) -> Vec<(Url, VersionProbe)> {
    futures::future::join_all(candidates.into_iter().map(|url| async move {
        let verdict = tokio::time::timeout(timeout, probe_version(&url))
            .await
            .unwrap_or(VersionProbe::Unreachable);
        (url, verdict)
    }))
    .await
}

/// Queries one worker's advertised protocol version.
async fn probe_version(url: &Url) -> VersionProbe {
    let resolver = grpc::DefaultChannelResolver::default();
    let Ok(mut client) = resolver.get_worker_client_for_url(url).await else {
        return VersionProbe::Unreachable;
    };
    let Ok(info) = client.get_worker_info(GetWorkerInfoRequest {}).await else {
        return VersionProbe::Unreachable;
    };
    if info.version == DISTRIBUTED_PROTOCOL_VERSION {
        VersionProbe::Match
    } else {
        VersionProbe::Mismatch
    }
}

/// Probes `candidates` and keeps only those reporting the expected
/// [`DISTRIBUTED_PROTOCOL_VERSION`].
///
/// Every refresh re-probes rather than caching verdicts: the version request
/// doubles as a liveness check, so a remembered "matches" could admit a worker
/// that has stopped answering.
async fn filter_version_matched(candidates: Vec<Url>) -> Vec<Url> {
    probe_versions(candidates, PROBE_TIMEOUT)
        .await
        .into_iter()
        .filter_map(|(url, verdict)| (verdict == VersionProbe::Match).then_some(url))
        .collect()
}

/// Drops entries of `urls` that answer with a protocol version other than
/// [`DISTRIBUTED_PROTOCOL_VERSION`].
///
/// Unlike [`filter_version_matched`], an unreachable worker is kept: without
/// an answer there is no evidence of incompatibility, and a worker may merely
/// be starting up. Such a worker fails at dispatch time exactly as before.
async fn drop_version_mismatched(urls: Vec<Url>) -> Vec<Url> {
    if urls.is_empty() {
        return urls;
    }
    probe_versions(urls, STATIC_PROBE_TIMEOUT)
        .await
        .into_iter()
        .filter_map(|(url, verdict)| match verdict {
            VersionProbe::Mismatch => {
                warn!(
                    "LakeSoul worker discovery: dropping static worker {url}: \
                     reported protocol version differs from \
                     {DISTRIBUTED_PROTOCOL_VERSION}"
                );
                None
            }
            _ => Some(url),
        })
        .collect()
}

/// Runs `drop_version_mismatched` on a dedicated thread with its own
/// single-threaded Tokio runtime, avoiding nested `block_on` and any
/// dependency on an ambient Tokio runtime.
fn drop_version_mismatched_blocking(urls: Vec<Url>) -> Vec<Url> {
    let probe_urls = urls.clone();
    std::thread::scope(|scope| {
        let handle = scope.spawn(move || -> crate::Result<Vec<Url>> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("building version probe runtime")?;
            Ok(runtime.block_on(drop_version_mismatched(probe_urls)))
        });
        match handle.join() {
            Ok(Ok(filtered)) => filtered,
            // A failed probe must not silently drain the list: keep every
            // URL so routing degrades to the pre-probe behavior (dispatch-
            // time failures) instead of reporting "no ready workers".
            _ => urls,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_resolver_keeps_unreachable_worker() {
        // Nothing listens on 10.0.0.1; without an answer the resolver cannot
        // call the worker incompatible, so it must stay listed (and fail at
        // dispatch time exactly as before version probing).
        let resolver =
            StaticWorkerResolver::new(vec!["http://10.0.0.1:50051".into()]).unwrap();
        assert_eq!(
            resolver.get_urls().unwrap(),
            vec![Url::parse("http://10.0.0.1:50051").unwrap()]
        );
    }

    #[test]
    fn static_resolver_rejects_invalid_url_early() {
        let error = StaticWorkerResolver::new(vec!["not a url".into()]).unwrap_err();
        let message = error.to_string();

        assert!(
            message.contains("invalid worker url") && message.contains("not a url"),
            "expected an invalid worker URL error, got: {message}"
        );
    }

    #[test]
    fn static_resolver_update_replaces_list() {
        let resolver = StaticWorkerResolver::new(vec!["http://a:1".into()]).unwrap();
        resolver
            .update(vec!["http://b:2".into(), "http://c:3".into()])
            .unwrap();
        assert_eq!(resolver.get_urls().unwrap().len(), 2);
    }

    #[test]
    fn empty_snapshot_is_ok_empty() {
        // An empty snapshot must not error here: the distributed planner
        // plans such queries single-node by itself, and the gate's fallback
        // policy only covers failures of that planner.
        let resolver = StaticWorkerResolver::new(Vec::new()).unwrap();
        assert!(resolver.get_urls().unwrap().is_empty());
    }
}
