// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Worker discovery for the distributed planner.
//!
//! Both backends implement [`datafusion_distributed::WorkerResolver`], whose
//! `get_urls()` is synchronous and is called several times per query (task
//! counting, then routing right before execution). Both implementations
//! therefore answer from an **in-memory snapshot** only:
//!
//! - [`StaticWorkerResolver`]: a fixed URL list (local development, bare
//!   metal). Workers are assumed ready; the operator is responsible for
//!   listing only compatible workers.
//! - [`KubernetesWorkerResolver`]: a background task watches the Kubernetes
//!   API (EndpointSlice listing refreshed on a fixed interval) and probes each
//!   discovered endpoint's protocol version over the worker gRPC channel.
//!   Only endpoints that are Ready and report
//!   [`crate::distributed::DISTRIBUTED_PROTOCOL_VERSION`] enter the snapshot.
//!
//! No `get_urls()` call ever performs a synchronous Kubernetes API request.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion_distributed::{
    ChannelResolver, GetWorkerInfoRequest, WorkerResolver, grpc,
};
use parking_lot::RwLock;
use rootcause::prelude::ResultExt;
use rootcause::{bail, report};
use url::Url;

use crate::Result;
use crate::distributed::DISTRIBUTED_PROTOCOL_VERSION;

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
#[derive(Debug, Clone)]
pub enum WorkerDiscovery {
    /// Static URL list for local development and bare-metal clusters.
    Static(Vec<String>),
    /// Kubernetes EndpointSlice watching.
    Kubernetes(KubernetesDiscovery),
}

/// Parameters for the Kubernetes EndpointSlice watcher.
#[derive(Debug, Clone)]
pub struct KubernetesDiscovery {
    /// Namespace that holds the worker EndpointSlices.
    pub namespace: String,
    /// Label selector identifying the EndpointSlices,
    /// e.g. `app=lakesoul-worker`.
    pub label_selector: String,
    /// API server base URL. Defaults to the in-cluster environment
    /// (`KUBERNETES_SERVICE_HOST`/`KUBERNETES_SERVICE_PORT`).
    pub api_url: Option<String>,
    /// Bearer token override. Defaults to the in-cluster service account
    /// token.
    pub token: Option<String>,
    /// CA bundle path override. Defaults to the in-cluster CA.
    pub ca_cert_path: Option<String>,
    /// How often the EndpointSlice list is refreshed and version probes are
    /// re-checked. Defaults to 5 s.
    pub poll_interval: Option<Duration>,
}

impl KubernetesDiscovery {
    pub fn new(namespace: impl Into<String>, label_selector: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            label_selector: label_selector.into(),
            api_url: None,
            token: None,
            ca_cert_path: None,
            poll_interval: None,
        }
    }

    fn api_url(&self) -> String {
        if let Some(url) = &self.api_url {
            return url.trim_end_matches('/').to_string();
        }
        let host = std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_default();
        let port =
            std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        format!("https://{host}:{port}")
    }

    fn token(&self) -> Option<String> {
        self.token.clone().or_else(|| {
            std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token")
                .ok()
                .map(|token| token.trim().to_string())
        })
    }

    fn ca_cert(&self) -> Option<Vec<u8>> {
        let path = self.ca_cert_path.clone().unwrap_or_else(|| {
            "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt".into()
        });
        std::fs::read(path).ok()
    }
}

/// Resolver over a fixed, operator-provided worker URL list.
///
/// Intended for local development and bare-metal deployments where workers
/// are launched out-of-band. It never performs I/O in `get_urls()`.
#[derive(Debug, Clone)]
pub struct StaticWorkerResolver {
    snapshot: Arc<RwLock<WorkerSnapshot>>,
}

impl StaticWorkerResolver {
    /// Builds the resolver from raw URL strings; invalid entries fail here
    /// instead of at query time.
    pub fn new(urls: Vec<String>) -> Result<Self> {
        Ok(Self {
            snapshot: Arc::new(RwLock::new(WorkerSnapshot::new(parse_urls(urls)?))),
        })
    }

    /// Replaces the URL list (e.g. after an operator-side rebalance).
    pub fn update(&self, urls: Vec<String>) -> Result<()> {
        *self.snapshot.write() = WorkerSnapshot::new(parse_urls(urls)?);
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

fn parse_urls(urls: Vec<String>) -> Result<Vec<Url>> {
    urls.iter()
        .map(|url| {
            Url::parse(url).map_err(|err| report!("invalid worker url {url:?}: {err}"))
        })
        .collect()
}

/// Resolver backed by a Kubernetes EndpointSlice watch.
///
/// A background task periodically lists the EndpointSlices matching the
/// configured label selector, probes each Ready endpoint's protocol version
/// and publishes the matching URLs into the snapshot. `get_urls()` only ever
/// reads the snapshot; if the Kubernetes API becomes unreachable the last
/// known-good snapshot is kept.
#[derive(Clone)]
pub struct KubernetesWorkerResolver {
    snapshot: Arc<RwLock<WorkerSnapshot>>,
}

impl KubernetesWorkerResolver {
    /// Starts the watcher task and returns the resolver.
    ///
    /// The watcher terminates when the last snapshot handle is dropped.
    pub fn start(discovery: &KubernetesDiscovery) -> Result<Self> {
        let client = build_http_client(discovery)?;
        let api_url = discovery.api_url();
        let namespace = discovery.namespace.clone();
        let selector = discovery.label_selector.clone();
        let token = discovery.token();
        let interval = discovery.poll_interval.unwrap_or(Duration::from_secs(5));

        let snapshot = Arc::new(RwLock::new(WorkerSnapshot::default()));
        let handle = Arc::downgrade(&snapshot);

        tokio::spawn(async move {
            loop {
                match fetch_endpointslices(
                    &client, &api_url, &namespace, &selector, &token,
                )
                .await
                {
                    Ok(candidates) => {
                        let eligible = probe_versions(candidates).await;
                        let Some(handle) = handle.upgrade() else {
                            // All resolver clones dropped: stop watching.
                            break;
                        };
                        *handle.write() = WorkerSnapshot::new(eligible);
                    }
                    Err(err) => {
                        // Keep the last known-good snapshot; a transient API
                        // outage must not drain an otherwise healthy cluster.
                        warn!(
                            "LakeSoul worker discovery: EndpointSlice refresh failed: {err}"
                        );
                    }
                }
                if handle.upgrade().is_none() {
                    break;
                }
                tokio::time::sleep(interval).await;
            }
        });

        Ok(Self { snapshot })
    }

    /// The current snapshot (for diagnostics).
    pub fn snapshot(&self) -> WorkerSnapshot {
        self.snapshot.read().clone()
    }
}

impl WorkerResolver for KubernetesWorkerResolver {
    fn get_urls(&self) -> DFResult<Vec<Url>> {
        Ok(self.snapshot.read().urls.clone())
    }
}

fn build_http_client(discovery: &KubernetesDiscovery) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(concat!("lakesoul-datafusion/", env!("CARGO_PKG_VERSION")));
    if let Some(ca) = discovery.ca_cert() {
        let cert = reqwest::Certificate::from_pem(&ca).map_err(|err| {
            DataFusionError::Configuration(format!("invalid Kubernetes CA bundle: {err}"))
        })?;
        builder = builder.add_root_certificate(cert);
    }
    Ok(builder.build().context("building kubernetes client")?)
}

async fn fetch_endpointslices(
    client: &reqwest::Client,
    api_url: &str,
    namespace: &str,
    selector: &str,
    token: &Option<String>,
) -> Result<Vec<Url>> {
    let url = format!(
        "{api_url}/apis/discovery.k8s.io/v1/namespaces/{namespace}/endpointslices?labelSelector={selector}"
    );
    let mut request = client.get(&url);
    // The mounted token rotates: re-read it every round unless the operator
    // supplied an explicit one.
    let token = token.clone().or_else(|| {
        std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token")
            .ok()
            .map(|t| t.trim().to_string())
    });
    if let Some(token) = token {
        request = request.bearer_auth(&token);
    }
    let response = request
        .send()
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    let status = response.status();
    if !status.is_success() {
        bail!("EndpointSlice request returned {status}")
    }
    let body = response.text().await?;
    ready_worker_urls(&body)
}

/// Extracts worker URLs from an EndpointSliceList JSON document.
///
/// Only endpoints whose `conditions.ready` is `true` (and not terminating)
/// are considered; the first TCP port declared on the slice is used.
pub fn ready_worker_urls(list_json: &str) -> Result<Vec<Url>> {
    #[derive(serde::Deserialize)]
    struct EndpointSliceList {
        #[serde(default)]
        items: Vec<EndpointSlice>,
    }
    #[derive(serde::Deserialize)]
    struct EndpointSlice {
        #[serde(default)]
        endpoints: Vec<Endpoint>,
        #[serde(default)]
        ports: Vec<SlicePort>,
    }
    #[derive(serde::Deserialize)]
    struct Endpoint {
        #[serde(default)]
        addresses: Vec<String>,
        #[serde(default)]
        conditions: Option<Conditions>,
    }
    #[derive(serde::Deserialize)]
    struct Conditions {
        #[serde(default)]
        ready: Option<bool>,
        #[serde(default)]
        terminating: Option<bool>,
    }
    #[derive(serde::Deserialize)]
    struct SlicePort {
        #[serde(default)]
        port: Option<i64>,
        #[serde(default)]
        protocol: Option<String>,
    }

    let list: EndpointSliceList =
        serde_json::from_str(list_json).context("invalid EndpointSliceList")?;

    let mut urls = Vec::new();
    // A rolling update can list the same address in several EndpointSlices;
    // collapsing them keeps one entry (and one probe) per worker.
    let mut seen = HashSet::new();
    for slice in list.items {
        let port = slice
            .ports
            .iter()
            .filter(|port| {
                port.protocol
                    .as_deref()
                    .unwrap_or("TCP")
                    .eq_ignore_ascii_case("TCP")
            })
            .find_map(|port| port.port.filter(|port| (0..=65535).contains(port)));
        let Some(port) = port else { continue };
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
                if let Ok(url) = Url::parse(&format!("http://{address}:{port}"))
                    && seen.insert(url.clone())
                {
                    urls.push(url);
                }
            }
        }
    }
    Ok(urls)
}

/// Probes `candidates` over the worker gRPC channel and keeps only those
/// reporting the expected [`DISTRIBUTED_PROTOCOL_VERSION`].
///
/// Every refresh re-probes rather than caching verdicts: the version request
/// doubles as a liveness check, so a remembered "matches" could admit a worker
/// that has stopped answering.
async fn probe_versions(candidates: Vec<Url>) -> Vec<Url> {
    futures::future::join_all(candidates.into_iter().map(|url| async move {
        let matched = tokio::time::timeout(PROBE_TIMEOUT, probe_version(&url))
            .await
            .unwrap_or(false);
        matched.then_some(url)
    }))
    .await
    .into_iter()
    .flatten()
    .collect()
}

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

async fn probe_version(url: &Url) -> bool {
    let resolver = grpc::DefaultChannelResolver::default();
    let Ok(mut client) = resolver.get_worker_client_for_url(url).await else {
        return false;
    };
    let Ok(info) = client.get_worker_info(GetWorkerInfoRequest {}).await else {
        return false;
    };
    info.version == DISTRIBUTED_PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_resolver_parses_and_serves_snapshot() {
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
        // An empty snapshot must not error here: the fast-fail policy lives
        // in the planner gate, which distinguishes production from
        // dev-fallback mode.
        let resolver = StaticWorkerResolver::new(Vec::new()).unwrap();
        assert!(resolver.get_urls().unwrap().is_empty());
    }

    fn slice_json(endpoints: &str, ports: &str) -> String {
        format!(r#"{{"items":[{{"endpoints":{endpoints},"ports":{ports}}}]}}"#)
    }

    #[test]
    fn endpoint_slices_ready_only() {
        let json = slice_json(
            r#"[
                {"addresses":["10.0.0.1"],"conditions":{"ready":true,"terminating":false}},
                {"addresses":["10.0.0.2"],"conditions":{"ready":false}},
                {"addresses":["10.0.0.3"],"conditions":{"ready":true,"terminating":true}},
                {"addresses":["10.0.0.4"]}
            ]"#,
            r#"[{"port":50051,"protocol":"TCP"}]"#,
        );
        let urls = ready_worker_urls(&json).unwrap();
        assert_eq!(
            urls,
            vec![
                Url::parse("http://10.0.0.1:50051").unwrap(),
                // Missing conditions default to ready.
                Url::parse("http://10.0.0.4:50051").unwrap(),
            ]
        );
    }

    #[test]
    fn endpoint_slices_collapse_duplicate_addresses() {
        // A rolling update can report the same address in more than one
        // EndpointSlice (and more than once within one); that must not become
        // two workers in the snapshot, which would skew task routing.
        let json = r#"{"items":[
            {"endpoints":[{"addresses":["10.0.0.1","10.0.0.1"]}],"ports":[{"port":50051}]},
            {"endpoints":[{"addresses":["10.0.0.1"]}],"ports":[{"port":50051}]}
        ]}"#;
        assert_eq!(
            ready_worker_urls(json).unwrap(),
            vec![Url::parse("http://10.0.0.1:50051").unwrap()]
        );
    }

    #[test]
    fn endpoint_slices_skip_non_tcp_and_bad_ports() {
        let json = slice_json(
            r#"[{"addresses":["10.0.0.1"],"conditions":{"ready":true}}]"#,
            r#"[{"port":5353,"protocol":"UDP"},{"port":70000},{"port":-1},{"port":8080}]"#,
        );
        let urls = ready_worker_urls(&json).unwrap();
        assert_eq!(urls, vec![Url::parse("http://10.0.0.1:8080").unwrap()]);
    }

    #[test]
    fn endpoint_slices_invalid_json_errors() {
        assert!(ready_worker_urls("{not json").is_err());
    }

    #[tokio::test]
    async fn version_probe_rejects_unreachable() {
        // Nothing listens here; the probe must return false (and never panic).
        let url = Url::parse("http://127.0.0.1:1").unwrap();
        assert!(!probe_version(&url).await);
    }
}
