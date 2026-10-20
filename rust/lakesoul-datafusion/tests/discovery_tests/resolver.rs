// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! EndpointSlice parsing and resolver behavior without a metadata database.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_distributed::{ChannelResolver, Worker, WorkerResolver, grpc};
use lakesoul_datafusion::distributed::resolver::{
    KubernetesDiscovery, KubernetesWorkerResolver, ready_worker_urls,
};
use lakesoul_datafusion::distributed::worker::LakeSoulWorkerSessionBuilder;
use lakesoul_datafusion::distributed::{
    DISTRIBUTED_PROTOCOL_VERSION, DistributedOptions, WorkerDiscovery,
};
use parking_lot::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use url::Url;

/// Builds one EndpointSliceList document: endpoints + a TCP port list for a
/// single slice.
fn slice_json(endpoints: &str, ports: &str) -> String {
    format!(r#"{{"items":[{{"endpoints":{endpoints},"ports":{ports}}}]}}"#)
}

/// The API server's EndpointSliceList for a Service without Pods: the
/// controller's placeholder slice encodes its empty lists as `null`.
const NO_WORKER_SLICES: &str =
    r#"{"items":[{"addressType":"IPv4","endpoints":null,"ports":null}]}"#;

/// Polls `url` with the public channel resolver until the worker's gRPC
/// endpoint answers (used instead of resolver.rs's private `probe_version`).
async fn worker_answers_version_probe(url: &Url) -> bool {
    let resolver = grpc::DefaultChannelResolver::default();
    let Ok(mut client) = resolver.get_worker_client_for_url(url).await else {
        return false;
    };
    matches!(
        client
            .get_worker_info(datafusion_distributed::GetWorkerInfoRequest {})
            .await,
        Ok(_) | Err(_)
    )
}

/// Serves a worker gRPC endpoint reporting `version` on an ephemeral port on
/// its own OS thread and runtime, and waits until a probe gets an answer.
async fn spawn_test_worker(version: &'static str) -> Url {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            listener.set_nonblocking(true).unwrap();
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let worker = Worker::from_session_builder(LakeSoulWorkerSessionBuilder)
                .with_runtime_env(Arc::new(RuntimeEnv::default()))
                .with_version(version);
            tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
    });
    for _ in 0..200 {
        if worker_answers_version_probe(&url).await {
            return url;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("test worker on {url} never answered a version probe");
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
fn endpoint_slices_skip_multi_tcp_without_grpc_name() {
    // Several TCP ports and none named `grpc`: ambiguous, the slice is
    // skipped rather than guessed (previously the first valid TCP port
    // was picked).
    let json = slice_json(
        r#"[{"addresses":["10.0.0.1"],"conditions":{"ready":true}}]"#,
        r#"[{"port":5353,"protocol":"UDP"},{"name":"metrics","port":8080},{"name":"admin","port":8081}]"#,
    );
    assert!(ready_worker_urls(&json).unwrap().is_empty());
}

#[test]
fn endpoint_slices_invalid_port_is_error() {
    // A single TCP port with an invalid numeric value is a misconfigured
    // service, not an empty cluster: hard error.
    let out_of_range = slice_json(
        r#"[{"addresses":["10.0.0.1"],"conditions":{"ready":true}}]"#,
        r#"[{"port":70000}]"#,
    );
    let err = ready_worker_urls(&out_of_range).unwrap_err().to_string();
    assert!(err.contains("TCP port with invalid value"), "got: {err}");

    let named_grpc_out_of_range = slice_json(
        r#"[{"addresses":["10.0.0.1"],"conditions":{"ready":true}}]"#,
        r#"[{"name":"grpc","port":0}]"#,
    );
    let err = ready_worker_urls(&named_grpc_out_of_range)
        .unwrap_err()
        .to_string();
    assert!(err.contains("named `grpc`"), "got: {err}");
}

#[test]
fn endpoint_slices_invalid_address_is_error() {
    let json = slice_json(
        r#"[{"addresses":["10 0.0.1 not a host"],"conditions":{"ready":true}}]"#,
        r#"[{"name":"grpc","port":50051}]"#,
    );
    let err = ready_worker_urls(&json).unwrap_err().to_string();
    assert!(
        err.contains("invalid worker endpoint address"),
        "got: {err}"
    );
}

#[test]
fn grpc_port_beats_other_names_any_order() {
    // `grpc` wins even when `metrics` comes first (a single unnamed port
    // is the compatibility case; the named port rules both when several
    // exist).
    let metrics_first = slice_json(
        r#"[{"addresses":["10.0.0.9"],"conditions":{"ready":true}}]"#,
        r#"[{"name":"metrics","port":19091},{"name":"grpc","port":50051}]"#,
    );
    assert_eq!(
        ready_worker_urls(&metrics_first).unwrap(),
        vec![Url::parse("http://10.0.0.9:50051").unwrap()]
    );
    let grpc_first = slice_json(
        r#"[{"addresses":["10.0.0.9"],"conditions":{"ready":true}}]"#,
        r#"[{"name":"grpc","port":50051},{"name":"metrics","port":19091}]"#,
    );
    assert_eq!(
        ready_worker_urls(&grpc_first).unwrap(),
        vec![Url::parse("http://10.0.0.9:50051").unwrap()]
    );
}

#[test]
fn ipv6_endpoint_addresses_bracketed() {
    let json = r#"{"items":[
            {"endpoints":[{"addresses":["fd00::1","10.0.0.1"]}],"ports":[{"name":"grpc","port":50051}]}
        ]}"#;
    assert_eq!(
        ready_worker_urls(json).unwrap(),
        vec![
            Url::parse("http://[fd00::1]:50051").unwrap(),
            Url::parse("http://10.0.0.1:50051").unwrap(),
        ]
    );
}

#[test]
fn endpoint_slices_invalid_json_errors() {
    assert!(ready_worker_urls("{not json").is_err());
}

// ---- Local mock Kubernetes API (plain HTTP; no cluster, no DB) ----

/// One captured EndpointSlice API request.
#[derive(Debug, Clone)]
struct MockApiRequest {
    path_with_query: String,
    authorization: Option<String>,
}

/// Scripted response for one (or, when the queue empties, all following)
/// EndpointSlice list requests.
#[derive(Debug, Clone)]
struct MockApiResponse {
    status: u16,
    body: String,
}

fn mocked(status: u16, body: String) -> MockApiResponse {
    MockApiResponse { status, body }
}

/// A tiny HTTP/1.1 server acting as the Kubernetes API server: records
/// each GET (path + Authorization header) and plays a scripted sequence
/// of JSON bodies. The last scripted response repeats forever.
struct MockK8sApi {
    base_url: String,
    requests: Arc<Mutex<Vec<MockApiRequest>>>,
    script: Arc<Mutex<VecDeque<MockApiResponse>>>,
}

impl MockK8sApi {
    /// Starts the accept loop on the calling test's runtime; `scripted`
    /// responses are served in order, the last one repeats forever.
    fn start(scripted: Vec<MockApiResponse>) -> Self {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(scripted.into_iter().collect::<VecDeque<_>>()));
        let captured = requests.clone();
        let scripted = script.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                };
                let captured = captured.clone();
                let scripted = scripted.clone();
                tokio::spawn(async move {
                    Self::handle(stream, captured, scripted).await;
                });
            }
        });
        Self {
            base_url,
            requests,
            script,
        }
    }

    async fn handle(
        mut stream: tokio::net::TcpStream,
        captured: Arc<Mutex<Vec<MockApiRequest>>>,
        script: Arc<Mutex<VecDeque<MockApiResponse>>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            match stream.read(&mut chunk).await {
                Ok(read) if read > 0 => buffer.extend_from_slice(&chunk[..read]),
                _ => break,
            }
        }
        let request = String::from_utf8_lossy(&buffer).to_string();
        let mut lines = request.lines();
        let head_line = lines.next().unwrap_or_default().to_string();
        let path = head_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_string();
        let authorization = lines.by_ref().find_map(|line| {
            let mut parts = line.splitn(2, ':');
            let header = parts.next()?.trim().to_ascii_lowercase();
            if header != "authorization" {
                return None;
            }
            let value = parts.next()?.trim().to_string();
            Some(
                value
                    .strip_prefix("Bearer ")
                    .map(|token| token.to_string())
                    .unwrap_or(value),
            )
        });
        captured.lock().push(MockApiRequest {
            path_with_query: path.clone(),
            authorization,
        });
        let (status, body) = {
            let mut script = script.lock();
            match script.pop_front() {
                // Sequences play in order; once the queue is empty the
                // last response repeats forever.
                Some(response) => {
                    if script.is_empty() {
                        script.push_back(response.clone());
                    }
                    (response.status, response.body)
                }
                None => script
                    .back()
                    .map(|response| (response.status, response.body.clone()))
                    .unwrap_or((404, String::new())),
            }
        };
        let reason = match status {
            200 => "OK",
            401 => "Unauthorized",
            403 => "Forbidden",
            500 => "Internal Server Error",
            _ => "Response",
        };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.flush().await;
    }

    /// Replaces the script: `response` is served for the next request and
    /// then repeats.
    fn set_response(&self, response: MockApiResponse) {
        let mut guard = self.script.lock();
        guard.clear();
        guard.push_back(response);
    }

    fn snapshots(&self) -> Vec<MockApiRequest> {
        self.requests.lock().clone()
    }

    fn request_count(&self) -> usize {
        self.requests.lock().len()
    }

    /// Waits until at least `count` requests were served.
    async fn wait_for_requests(&self, count: usize) {
        for _ in 0..600 {
            if self.request_count() >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "mock Kubernetes API served only {} requests, wanted >= {count}",
            self.request_count()
        );
    }
}

/// Bounded wait until the resolver's published snapshot equals `expected` —
/// a request arriving at the mock does not yet mean the resolver has read
/// the response, run the version probes and published the result.
async fn wait_for_snapshot(resolver: &KubernetesWorkerResolver, expected: &[&Url]) {
    for _ in 0..200 {
        let published = resolver.snapshot().urls().to_vec();
        if published.iter().eq(expected.iter().copied()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "snapshot never became {expected:?} (last status: {:?})",
        resolver.refresh_status()
    );
}

/// Discovery config pointed at the mock API, polling fast.
fn mock_discovery(api: &MockK8sApi) -> KubernetesDiscovery {
    KubernetesDiscovery::new(
        "lakesoul-worker",
        "lakesoul-test",
        Url::parse(&api.base_url).unwrap(),
        Duration::from_millis(100),
        Duration::from_secs(30),
    )
}

/// Builds the EndpointSliceList JSON the mock serves for the given
/// `address:port` workers (each in its own slice, port named `grpc`).
fn slice_list_json(workers: &[&str]) -> String {
    let items: Vec<String> = workers
            .iter()
            .map(|worker| {
                // Accepts either "ip:port" or a full URL; the trailing '/'
                // that Url::as_str appends must not end up inside the port.
                let url = Url::parse(worker)
                    .or_else(|_| Url::parse(&format!("http://{worker}")))
                    .unwrap();
                let address = url.host_str().unwrap_or_default().to_string();
                let port = url.port().unwrap_or(50051);
                format!(
                    r#"{{"endpoints":[{{"addresses":["{address}"],"conditions":{{"ready":true,"terminating":false}}}}],"ports":[{{"name":"grpc","port":{port},"protocol":"TCP"}}]}}"#
                )
            })
            .collect();
    format!(r#"{{"items":[{}]}}"#, items.join(","))
}

/// Discovery config with a fixed explicit token override.
fn with_token(mut discovery: KubernetesDiscovery, token: &str) -> KubernetesDiscovery {
    discovery.token = Some(token.to_string());
    discovery
}

#[tokio::test]
async fn discovery_syncs_at_startup_with_service_name_selector() {
    let worker = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    let api = MockK8sApi::start(vec![mocked(200, slice_list_json(&[worker.as_ref()]))]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(
        resolver.get_urls().unwrap(),
        vec![worker.clone()],
        "only the ready, protocol-matching endpoint enters the snapshot"
    );

    let request = api.snapshots().first().unwrap().clone();
    assert!(
        request.path_with_query.contains(
            "/apis/discovery.k8s.io/v1/namespaces/lakesoul-test/endpointslices"
        ),
        "slices are listed in the Service's namespace: {}",
        request.path_with_query
    );
    assert!(
        request
            .path_with_query
            .contains("labelSelector=kubernetes.io%2Fservice-name%3Dlakesoul-worker")
            || request
                .path_with_query
                .contains("labelSelector=kubernetes.io/service-name=lakesoul-worker")
            || request
                .path_with_query
                .contains("labelSelector=kubernetes.io/service-name%3Dlakesoul-worker"),
        "selector must be the service-name label, not a Pod label: {}",
        request.path_with_query
    );
    assert_eq!(request.authorization.as_deref(), Some("token-1"));
}

#[tokio::test]
async fn discovery_token_is_reread_from_file_every_request() {
    let token_file =
        std::env::temp_dir().join(format!("lakesoul-k8s-token-{}", std::process::id()));
    std::fs::write(&token_file, "token-a\n").unwrap();
    let api = MockK8sApi::start(vec![mocked(200, r#"{"items":[]}"#.to_string())]);
    let mut discovery = mock_discovery(&api);
    discovery.token = None;
    discovery.token_path = Some(token_file.to_string_lossy().to_string());
    let resolver = KubernetesWorkerResolver::start(&discovery).unwrap();
    api.wait_for_requests(2).await;

    std::fs::write(&token_file, "token-b\n").unwrap();
    api.wait_for_requests(4).await;
    let snapshots = api.snapshots();
    assert_eq!(snapshots[0].authorization.as_deref(), Some("token-a"));
    assert_eq!(snapshots[1].authorization.as_deref(), Some("token-a"));
    assert!(
        snapshots
            .iter()
            .any(|request| request.authorization.as_deref() == Some("token-b")),
        "the rotated token must be picked up by a later refresh: {snapshots:?}"
    );
    drop(resolver);
}

#[tokio::test]
async fn discovery_missing_token_fails_loudly() {
    let api = MockK8sApi::start(vec![]);
    let mut discovery = mock_discovery(&api);
    discovery.token = None;
    discovery.token_path = Some(
        std::env::temp_dir()
            .join("lakesoul-k8s-token-definitely-missing")
            .to_string_lossy()
            .into_owned(),
    );
    let error = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(2),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("token"),
        "a missing token must be reported, not requested anonymously: {error}"
    );
}

#[tokio::test]
async fn discovery_authorization_error_is_explicit() {
    let api = MockK8sApi::start(vec![mocked(401, r#""#.to_string())]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let error = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(2),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("401"),
        "authorization failures must surface verbatim, not as 'no workers': {error}"
    );
    // And the resolver keeps answering errors, not an empty-but-healthy
    // snapshot: no request ever succeeded.
    let resolver = KubernetesWorkerResolver::start(&discovery).unwrap();
    assert!(resolver.get_urls().is_err());
}

#[tokio::test]
async fn discovery_authorization_failure_after_success_blocks_snapshot() {
    let worker = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    let api = MockK8sApi::start(vec![mocked(200, slice_list_json(&[worker.as_ref()]))]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    wait_for_snapshot(&resolver, &[&worker]).await;

    // RBAC revoked after a working sync: 403 is a hard error — the previous
    // snapshot may not keep answering inside the staleness window.
    api.set_response(mocked(403, String::new()));
    for _ in 0..200 {
        if resolver.get_urls().is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let error = resolver.get_urls().unwrap_err().to_string();
    assert!(
        error.contains("403") && error.contains("unrecoverable"),
        "post-success authorization failure must block the snapshot immediately: {error}"
    );
    // The failure is also the visible refresh status, not a silent healthy
    // state.
    let (_age, last_error, _candidates, _eligible) = resolver.refresh_status();
    assert!(
        last_error.as_deref().unwrap().contains("403"),
        "refresh status must record the authorization failure: {last_error:?}"
    );
}

#[tokio::test]
async fn discovery_transient_api_failure_recovers() {
    let worker = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    let api = MockK8sApi::start(vec![
        mocked(500, String::new()),
        mocked(500, String::new()),
        mocked(200, slice_list_json(&[worker.as_ref()])),
    ]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(resolver.get_urls().unwrap(), vec![worker]);
    let (age, error, _candidates, eligible) = resolver.refresh_status();
    assert!(age.is_some(), "a successful refresh must be recorded");
    assert_eq!(eligible, 1);
    assert!(error.is_none());
}

#[tokio::test]
async fn discovery_empty_snapshot_replaces_workers() {
    let worker = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    let api = MockK8sApi::start(vec![mocked(200, slice_list_json(&[worker.as_ref()]))]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(resolver.get_urls().unwrap(), vec![worker]);

    // The Service loses its workers (scaled to zero): the next successful
    // refresh with zero eligible endpoints must replace the snapshot, not
    // keep routing to workers that disappeared.
    api.set_response(mocked(200, NO_WORKER_SLICES.to_string()));
    api.wait_for_requests(3).await;
    let error = resolver.get_urls().unwrap_err().to_string();
    assert!(
        error.contains("no eligible worker"),
        "an empty successful refresh is a fact, not silent local planning: {error}"
    );
}

#[tokio::test]
async fn discovery_starts_without_workers_and_picks_them_up() {
    // A coordinator may start before any worker is Ready (or while workers
    // are scaled to zero): it starts on the first successful refresh, fails
    // queries with "no eligible worker", and uses a worker as soon as a later
    // refresh lists it, without a restart.
    let api = MockK8sApi::start(vec![mocked(200, NO_WORKER_SLICES.to_string())]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let error = resolver.get_urls().unwrap_err().to_string();
    assert!(
        error.contains("no eligible worker"),
        "an empty start must fail queries, not plan them locally: {error}"
    );

    let worker = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    api.set_response(mocked(200, slice_list_json(&[worker.as_ref()])));
    wait_for_snapshot(&resolver, &[&worker]).await;
    assert_eq!(resolver.get_urls().unwrap(), vec![worker]);
}

#[tokio::test]
async fn discovery_snapshot_goes_stale_after_failed_refreshes() {
    let worker = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    let api = MockK8sApi::start(vec![mocked(200, slice_list_json(&[worker.as_ref()]))]);
    let mut discovery = with_token(mock_discovery(&api), "token-1");
    discovery.max_staleness = Duration::from_millis(400);
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(!&resolver.get_urls().unwrap().is_empty());

    // API outage (5xx; network is down for real deployments) within
    // staleness: the last snapshot still answers, after it new distributed
    // queries fail. Authorization failures (401/403) instead block the
    // snapshot immediately (see discovery_authorization_failure_after…).
    api.set_response(mocked(503, String::new()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let error = resolver.get_urls().unwrap_err().to_string();
    assert!(
        error.contains("stale") && error.contains("503"),
        "staleness must cite the pending transient refresh failure: {error}"
    );
}

#[tokio::test]
async fn discovery_refreshes_addresses_between_polls() {
    let worker_a = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    let worker_b = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    // Only A is served until the first sync; the test then flips the mock to
    // B, so a fast second refresh can never race the first assertion.
    let api = MockK8sApi::start(vec![mocked(200, slice_list_json(&[worker_a.as_ref()]))]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(resolver.get_urls().unwrap(), vec![worker_a]);

    api.set_response(mocked(200, slice_list_json(&[worker_b.as_ref()])));
    // A request at the mock precedes the version probes and the snapshot
    // publish; wait on the snapshot condition, not the request count.
    wait_for_snapshot(&resolver, &[&worker_b]).await;
    assert_eq!(
        resolver.get_urls().unwrap(),
        vec![worker_b],
        "a restarted Pod's new addresses must appear in the snapshot"
    );
}

/// Two sessions built the way the factory does it (once per process,
/// resolver handed to every session) must share ONE resolver instance and
/// therefore one polling loop — measured in mock-API request volume.
#[tokio::test]
async fn sessions_share_one_resolver_and_one_poll_loop() {
    let worker = spawn_test_worker(DISTRIBUTED_PROTOCOL_VERSION).await;
    let api = MockK8sApi::start(vec![mocked(200, slice_list_json(&[worker.as_ref()]))]);
    let discovery = with_token(mock_discovery(&api), "token-1");
    let resolver = KubernetesWorkerResolver::start_and_wait_for_first_sync(
        &discovery,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let options = DistributedOptions {
        discovery: WorkerDiscovery::Shared(Arc::new(resolver)),
        ..DistributedOptions::default()
    };
    // What `LakeSoulSessionFactory::create_session` does per session:
    let session_a =
        lakesoul_datafusion::session::build_worker_resolver(&options.discovery).unwrap();
    let session_b =
        lakesoul_datafusion::session::build_worker_resolver(&options.discovery).unwrap();
    assert!(
        Arc::ptr_eq(&session_a, &session_b),
        "sessions must share the process's single resolver instance"
    );

    // One polling loop: over two poll intervals the mock API sees one
    // loop's worth of requests, not two (an interval ~= 100ms + margins).
    api.wait_for_requests(2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let requests = api.request_count();
    // One loop fires ~3 refreshes in 300 ms at an 100 ms interval. The upper
    // bound is deliberately loose: parallel test runners run CPU-starved,
    // which can squeeze one extra tick in, but a second, session-owned loop
    // would clearly double the volume.
    assert!(
        (2..=6).contains(&requests),
        "request volume {requests} must match ONE polling loop over 300ms, \
             not two sessions each starting their own"
    );
}
