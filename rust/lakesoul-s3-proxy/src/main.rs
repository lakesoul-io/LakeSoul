// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use aws_config::Region;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
    SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use bytes::Bytes;
use hickory_resolver::TokioAsyncResolver;
use http::header::{CONTENT_LENGTH, HOST};
use http::{HeaderValue, Uri};
use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::rbac::verify_permission_by_table_path;
use lazy_static::lazy_static;
use pingora::lb::discovery::ServiceDiscovery;
use pingora::lb::selection::{BackendIter, BackendSelection};
use pingora::lb::{Backend, Backends, Extensions};
use pingora::prelude::*;
use pingora::protocols::l4::socket::SocketAddr;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use prometheus::{IntCounter, register_int_counter};
use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

lazy_static! {
    static ref REQ_COUNTER: IntCounter =
        register_int_counter!("s3proxy_request_num", "request num").unwrap();
    static ref REQ_BYTES: IntCounter =
        register_int_counter!("s3proxy_request_bytes", "request bytes").unwrap();
    static ref RES_BYTES: IntCounter =
        register_int_counter!("s3proxy_response_bytes", "response bytes").unwrap();
}

fn main() {
    let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
    match tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(timer)
        .try_init()
    {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Failed to set logger: {e:?}");
        }
    }
    let mut opt = Opt::default();
    if std::fs::exists("/opt/proxy_conf.yaml").unwrap() {
        info!("Use pingora config file /opt/proxy_conf.yaml");
        opt.conf = Some("/opt/proxy_conf.yaml".to_string());
    }
    let mut proxy_server = Server::new(Some(opt)).unwrap();
    proxy_server.bootstrap();

    let endpoint = std::env::var("AWS_ENDPOINT").expect("need AWS_ENDPOINT env");
    let region = std::env::var("AWS_REGION").expect("need AWS_REGION env");
    let virtual_host =
        std::env::var("AWS_VIRTUAL_HOST").is_ok_and(|v| v.to_lowercase() == "true");
    let (user, group, verify_meta) = match (
        std::env::var("LAKESOUL_CURRENT_USER"),
        std::env::var("LAKESOUL_CURRENT_DOMAIN"),
    ) {
        (Ok(user), Ok(group)) => (user, group, true),
        _ => (String::new(), String::new(), false),
    };

    let verify_client_signature = std::env::var("CLIENT_AWS_SECRET").is_ok()
        && std::env::var("CLIENT_AWS_KEY").is_ok();

    let uri = Uri::from_str(endpoint.as_str()).unwrap();
    let tls: bool = if let Some(scheme) = uri.scheme_str() {
        scheme == "https"
    } else {
        false
    };
    let host = uri.host().unwrap();
    let port = if let Some(port) = uri.port() {
        port.as_u16()
    } else if tls {
        443
    } else {
        80
    };
    info!(
        "endpoint {}, region {}, virtual_host {}, verify rbac {}, verify client {}, tls {}, port {}",
        endpoint, region, virtual_host, verify_meta, verify_client_signature, tls, port
    );

    let mut upstreams = LoadBalancer::from(DnsDiscovery::new(
        host,
        port,
        Arc::new(TokioAsyncResolver::tokio_from_system_conf().unwrap()),
    ));
    upstreams.health_check_frequency = Some(Duration::from_secs(600));
    upstreams.update_frequency = Some(Duration::from_secs(600));
    upstreams.parallel_health_check = true;

    let background_dns_service = background_service("dns resolver", upstreams);
    let lb = background_dns_service.task();

    let background_s3_credentials = background_service(
        "s3 credentials",
        Credentials {
            region,
            identity: ArcSwap::new(Arc::new(Identity::new(0, None))),
            metadata_client: ArcSwapOption::new(None),
            user: user.clone(),
            group,
            verify_meta,
            virtual_host,
        },
    );
    let cred = background_s3_credentials.task();

    let mut lb = http_proxy_service(
        &proxy_server.configuration,
        S3Proxy {
            lb,
            cred,
            host: String::from(host),
            tls,
        },
    );
    lb.add_tcp("0.0.0.0:6188");
    proxy_server.add_service(background_dns_service);
    proxy_server.add_service(background_s3_credentials);
    proxy_server.add_service(lb);

    // add prometheus endpoint
    let mut prometheus_service_http =
        pingora::services::listening::Service::prometheus_http_service();
    prometheus_service_http.add_tcp("0.0.0.0:1234");
    proxy_server.add_service(prometheus_service_http);

    proxy_server.run_forever();
}

pub struct S3Proxy {
    lb: Arc<LoadBalancer<RoundRobin>>,
    cred: Arc<Credentials>,
    host: String,
    tls: bool,
}

pub struct Credentials {
    region: String,
    identity: ArcSwap<Identity>,
    metadata_client: ArcSwapOption<MetaDataClient>,
    user: String,
    group: String,
    verify_meta: bool,
    virtual_host: bool,
}

fn starts_with_any(
    path: &str,
    bucket: &str,
    group: &str,
    prefixes: &'static [&str],
) -> bool {
    prefixes
        .iter()
        .any(|p| path.starts_with(format!("s3://{}/{}/{}", bucket, p, group).as_str()))
}

impl Credentials {
    async fn verify_rbac(
        &self,
        headers: &RequestHeader,
        bucket: &str,
    ) -> Result<(), anyhow::Error> {
        if !self.verify_meta {
            Ok(())
        } else {
            let binding = self.metadata_client.load();
            if let Some(client) = binding.as_ref() {
                let path = parse_table_path(&headers.uri, bucket);
                debug!("Parsed table path {:?}", path);
                if path.starts_with(
                    format!("s3://{}/{}/{}", bucket, self.group, self.user).as_str(),
                ) || starts_with_any(
                    path.as_str(),
                    bucket,
                    self.group.as_str(),
                    &["savepoint", "checkpoint", "resource-manager"],
                ) {
                    return Ok(());
                }
                verify_permission_by_table_path(
                    self.user.as_str(),
                    self.group.as_str(),
                    path.as_str(),
                    client.clone(),
                )
                .await?;
            }
            Ok(())
        }
    }

    fn sign_aws_v4(
        &self,
        headers: &mut RequestHeader,
        host: &String,
        bucket: &str,
    ) -> Result<(), anyhow::Error> {
        let mut signing_settings = SigningSettings::default();
        signing_settings.percent_encoding_mode = PercentEncodingMode::Single;
        signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        let binding = self.identity.load();
        let identity = binding.as_ref();
        let signing_params = v4::SigningParams::builder()
            .identity(identity)
            .region(self.region.as_str())
            .name("s3")
            .time(SystemTime::now())
            .settings(signing_settings)
            .build()?
            .into();

        let mut uri = headers.uri.to_string();
        debug!("original uri {}", uri);

        // for virtual host addressing, we need to replace host header
        // with format bucket-name.endpoint-host before signing
        if self.virtual_host {
            // rewrite host
            let new_host = format!("{}.{}", bucket, host);
            debug!("new host {}", new_host);
            headers.insert_header(HOST, HeaderValue::try_from(new_host)?)?;
            // rewrite path to remove bucket name
            let start = uri.find(bucket).unwrap();
            uri.replace_range(start..(start + bucket.len()), "");
            uri = uri.replace("//", "/");
            debug!("replaced uri {}", uri);
        } else {
            headers.insert_header(HOST, HeaderValue::try_from(host)?)?;
        }

        if let Some(value) = headers.headers.get("x-amz-decoded-content-length") {
            let value: u64 = value.to_str()?.parse()?;
            if value == 0 {
                headers.insert_header(CONTENT_LENGTH, HeaderValue::try_from(0)?)?;
            }
        }

        // construct request for signing by aws_sigv4
        let signable_request = SignableRequest::new(
            headers.method.as_str(),
            &uri,
            headers
                .headers
                .iter()
                .filter_map(|(name, value)| match value.to_str() {
                    Ok(v) => Some((name.as_str(), v)),
                    Err(_) => None,
                }),
            match headers.headers.get("x-amz-content-sha256") {
                Some(value) => {
                    if value == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
                        SignableBody::Precomputed(
                            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".parse()?,
                        )
                    } else {
                        SignableBody::UnsignedPayload
                    }
                }
                None => SignableBody::UnsignedPayload,
            },
        )?;
        let (signing_instructions, _signature) =
            sign(signable_request, &signing_params)?.into_parts();
        let (new_headers, new_query) = signing_instructions.into_parts();
        debug!("new headers {:?}, new query {:?}", new_headers, new_query);

        for header in new_headers.into_iter() {
            let mut value = HeaderValue::from_str(header.value())?;
            value.set_sensitive(header.sensitive());
            headers.insert_header(header.name(), value)?
        }

        if !new_query.is_empty() {
            let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(
                uri.as_str(),
            )?;
            for (name, value) in new_query {
                query.insert(name, &value);
            }
            headers.set_uri(query.build_uri().to_string().parse()?);
        } else {
            headers.set_uri(uri.parse()?);
        }
        debug!("final all headers {:?}", headers);
        Ok(())
    }
}

#[async_trait]
impl BackgroundService for Credentials {
    async fn start(&self, shutdown: ShutdownWatch) {
        if self.verify_meta {
            // create metadata client
            let metadata_client = Arc::new(
                MetaDataClient::from_env()
                    .await
                    .expect("cannot create meta data client"),
            );
            self.metadata_client.store(Some(metadata_client));
        }

        // s3 credential is updated periodically
        let mut now = Instant::now();
        // run update and health check once
        let mut next_update = now;
        loop {
            if *shutdown.borrow() {
                return;
            }
            if next_update <= now {
                // TODO: log err
                let credentials_provider = DefaultCredentialsChain::builder()
                    .region(Region::new(self.region.clone()))
                    .build()
                    .await;
                credentials_provider
                    .provide_credentials()
                    .await
                    .map(|credentials| {
                        info!("new credentials: {credentials:?}");
                        self.identity.swap(Arc::new(Identity::from(credentials)));
                    })
                    .expect("failed to provide credentials from configs");
                next_update = now + Duration::from_secs(60 * 45);
            }
            tokio::time::sleep_until(next_update.into()).await;
            now = Instant::now();
        }
    }
}

#[async_trait]
impl ProxyHttp for S3Proxy {
    /// For this small example, we don't need context storage
    type CTX = ();
    fn new_ctx(&self) {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut (),
    ) -> Result<Box<HttpPeer>> {
        let upstream = self
            .lb
            .select(b"", 256) // hash doesn't matter for round robin
            .unwrap();

        debug!("upstream peer is: {upstream:?}, {0}", self.tls);

        let peer = Box::new(HttpPeer::new(upstream, self.tls, self.host.clone()));
        Ok(peer)
    }

    async fn request_filter(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        REQ_COUNTER.inc();
        let header = session.req_header_mut();
        debug!("request_filter original header: {header:?}");
        let bucket;
        // we need to parse bucket name from uri component
        if let Some(path) = header
            .uri
            .path()
            .split("/")
            .filter(|s| !s.is_empty())
            .next()
        {
            bucket = path.to_string();
        } else {
            let msg = format!("Cannot determine bucket from header {:?}", header);
            error!("{}", msg);
            session
                .respond_error_with_body(400, Bytes::from(msg))
                .await?;
            return Ok(true);
        }

        // verify meta permission
        match self.cred.verify_rbac(header, &bucket).await {
            Err(e) => {
                let msg =
                    format!("Permission denied error {:?}, uri {:?}", e, header.uri);
                error!("{}", msg);
                session
                    .respond_error_with_body(403, Bytes::from(msg))
                    .await?;
                return Ok(true);
            }
            _ => {}
        }

        // signing
        match self.cred.sign_aws_v4(header, &self.host, &bucket) {
            Ok(_) => Ok(false),
            Err(e) => {
                let msg = format!(
                    "Sign aws v4 error {:?}, header {:?}, host {:?}, bucket {:?}",
                    e, header, self.host, bucket
                );
                error!("{}", msg);
                session
                    .respond_error_with_body(500, Bytes::from(msg))
                    .await?;
                Ok(true)
            }
        }
    }

    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        if let Some(bytes) = body {
            debug!(
                "request_body_filter original body length: {}, content: {:?}",
                bytes.len(),
                bytes
            );
            REQ_BYTES.inc_by(bytes.len() as u64)
        }
        Ok(())
    }

    fn response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>>
    where
        Self::CTX: Send + Sync,
    {
        if let Some(bytes) = body {
            RES_BYTES.inc_by(bytes.len() as u64)
        }
        Ok(None)
    }
}

/// Service discovery that resolves domains to Backends with DNS lookup using `hickory_resolver` crate.
///
/// Only IPv4 addresses are used, IPv6 ignored silently.
#[derive(Debug, Clone)]
pub struct DnsDiscovery {
    /// Domain that will be resolved
    pub domain: String,
    // Port used for Backend
    pub port: u16,
    /// Resolver from `hickory_resolver`
    pub resolver: Arc<TokioAsyncResolver>,
    /// Extensions that will be set to backends
    pub extensions: Option<Extensions>,
}

impl DnsDiscovery {
    pub fn new<D: Into<String>>(
        domain: D,
        port: u16,
        resolver: Arc<TokioAsyncResolver>,
    ) -> Self {
        DnsDiscovery {
            domain: domain.into(),
            port,
            resolver,
            extensions: None,
        }
    }

    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = Some(extensions);
        self
    }
}

#[async_trait]
impl ServiceDiscovery for DnsDiscovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        let records = self.resolver.lookup_ip(&self.domain).await.map_err(|err| {
            Error::create(
                Custom("DNS lookup error"),
                ErrorSource::Internal,
                Some(format!("{:?}", self).into()),
                Some(err.into()),
            )
        })?;
        info!("DNS lookup result: {records:?}");

        let result: BTreeSet<_> = records
            .iter()
            .filter_map(|ip| match ip {
                IpAddr::V4(ip) => Some(SocketAddr::Inet(std::net::SocketAddr::V4(
                    SocketAddrV4::new(ip, self.port),
                ))),
                IpAddr::V6(_) => None,
            })
            .map(|addr| Backend {
                addr,
                weight: 1,
                ext: Extensions::new(),
            })
            .collect();

        Ok((result, HashMap::new()))
    }
}

impl From<DnsDiscovery> for Backends {
    fn from(value: DnsDiscovery) -> Self {
        Backends::new(Box::new(value))
    }
}

impl<S> From<DnsDiscovery> for LoadBalancer<S>
where
    S: BackendSelection + 'static,
    S::Iter: BackendIter,
{
    fn from(value: DnsDiscovery) -> Self {
        LoadBalancer::from_backends(value.into())
    }
}

fn assemble_table_path<'a, Iter>(
    split: Iter,
    bucket_name: &str,
    partition_equal: &str,
) -> String
where
    Iter: Iterator<Item = &'a str>,
{
    let mut path = String::with_capacity(256);
    path.push_str("s3://");
    path.push_str(bucket_name);
    split
        .take_while(|s| {
            !(s.ends_with(".parquet")
                || s.starts_with("compact")
                || s.contains(partition_equal))
        })
        .for_each(|s| {
            path.push('/');
            path.push_str(s);
        });
    path
}

fn parse_table_path_from_query(query: &str, bucket_name: &str) -> String {
    let query_parts_iter = query.split("&");
    for query_part in query_parts_iter {
        let mut query_part_iter = query_part.split("=");
        if let Some(key) = query_part_iter.next() {
            if key == "prefix" {
                if let Some(value) = query_part_iter.next() {
                    return assemble_table_path(
                        value.split("%2F").filter(|s| !s.is_empty()),
                        bucket_name,
                        "%3D",
                    );
                }
            }
        }
    }
    format!("s3://{}", bucket_name)
}

fn parse_table_path(uri: &Uri, bucket: &str) -> String {
    let path = uri.path();
    let mut path_parts_iter = path.split("/").filter(|s| !s.is_empty()).peekable();
    // skip bucket name because we already know it
    path_parts_iter.next().unwrap();

    let bucket_name = bucket;
    if let None = path_parts_iter.peek() {
        // a list request without path
        // retrieve path from query string
        let query = uri.query().unwrap_or("");
        if query.is_empty() {
            format!("s3://{}/", bucket)
        } else {
            parse_table_path_from_query(query, bucket_name)
        }
    } else {
        assemble_table_path(path_parts_iter, bucket_name, "%3D")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::pretty::print_batches;
    use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
    use lakesoul_datafusion::LakeSoulQueryPlanner;
    use lakesoul_datafusion::catalog::create_io_config_builder;
    use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
    use lakesoul_datafusion::serialize::arrow_java::ArrowJavaSchema;
    use lakesoul_io::lakesoul_io_config::create_session_context_with_planner;
    use lakesoul_metadata::MetaDataClientRef;
    use proto::proto::entity::TableInfo;

    #[test]
    fn test_parse_table_path() {
        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/test.parquet"),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/default/abc/test.parquet"),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/default/abc"),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/default/abc/"),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/default/abc/test.parquet"),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket/test/default/abc/date=20250221/type=1/test.parquet"
                ),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket/test/default/abc/compact_123456/date=20250221/type=1/test.parquet"
                ),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );

        // list request parse from query
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket?list-type=2&prefix=test%2Fdefault%2Fabc%2Ftest.parquet&delimiter=%2F&encoding-type=url"
                ),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket?list-type=2&prefix=test%2Fdefault%2Fabc&delimiter=%2F&encoding-type=url"
                ),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket?list-type=2&prefix=test%2Fdefault%2Fabc%2F&delimiter=%2F&encoding-type=url"
                ),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket?list-type=2&prefix=test%2Fdefault%2Fabc%2Fdate%3D20250221%2Ftype=1%2Ftest.parquet&delimiter=%2F&encoding-type=url"
                ),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket?list-type=2&prefix=test%2Fdefault%2Fabc%2Ftest.parquet&delimiter=%2F&encoding-type=url"
                ),
                "lakesoul-test-bucket"
            ),
            "s3://lakesoul-test-bucket/test/default/abc"
        );
    }

    async fn create_and_write_table(
        table_name: &str,
        path: &str,
        domain: &str,
        record_batch: RecordBatch,
        meta_data_client: MetaDataClientRef,
    ) -> Result<(String, String), anyhow::Error> {
        let schema = record_batch.schema();
        let table_id = format!("table_{}", uuid::Uuid::new_v4());
        let ti = TableInfo {
            table_id: table_id.clone(),
            table_name: table_name.to_string(),
            table_path: path.to_string(),
            table_schema: serde_json::to_string::<ArrowJavaSchema>(&schema.into())?,
            table_namespace: "default".to_string(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            domain: domain.to_string(),
        };
        meta_data_client.create_table(ti).await?;
        let table = LakeSoulTable::for_namespace_and_name(
            "default",
            table_name,
            Some(meta_data_client.clone()),
        )
        .await?;
        table.execute_upsert(record_batch).await?;
        Ok((table_id, path.to_string()))
    }

    async fn drop_table(
        table_id: &str,
        path: &str,
        meta_data_client: MetaDataClientRef,
    ) -> Result<(), anyhow::Error> {
        meta_data_client
            .delete_table_by_table_id_cascade(table_id, path)
            .await?;
        Ok(())
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values: Vec<Arc<dyn Array>> = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names
            .into_iter()
            .zip(values)
            .map(|(name, array)| (name, array, true))
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn init_s3() {
        let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
        match tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_timer(timer)
            .try_init()
        {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to set logger: {e:?}");
            }
        }
        unsafe {
            std::env::set_var("AWS_ENDPOINT", "http://localhost:9000");
            std::env::set_var("AWS_REGION", "us-east-1");
            std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin1");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin1");
        }
    }

    fn run_server() -> std::thread::JoinHandle<()> {
        unsafe {
            std::env::set_var("LAKESOUL_CURRENT_USER", "lake-iam-001");
            std::env::set_var("LAKESOUL_CURRENT_DOMAIN", "lake-czads");
        }
        std::thread::spawn(move || {
            main();
        })
    }

    fn change_s3_to_proxy() {
        unsafe {
            std::env::set_var("AWS_ENDPOINT", "http://localhost:6188");
        }
    }

    async fn create_table_and_write_suffix(
        suffix: &str,
        metadata_client: MetaDataClientRef,
    ) -> Result<(String, String), anyhow::Error> {
        let table_name = format!("test_rbac_table_{}", suffix);
        let table_path = format!("s3://lakesoul-test-bucket/tmp/table_{}", suffix);
        let doamin = format!("lake-cz{}", suffix);
        let record_batch =
            create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        create_and_write_table(
            &table_name,
            &table_path,
            &doamin,
            record_batch.clone(),
            metadata_client.clone(),
        )
        .await
    }

    async fn insert_table(
        suffix: &str,
        metadata_client: MetaDataClientRef,
    ) -> Result<(), anyhow::Error> {
        let table_name = format!("test_rbac_table_{}", suffix);
        let record_batch =
            create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        let table = LakeSoulTable::for_namespace_and_name(
            "default",
            &table_name,
            Some(metadata_client.clone()),
        )
        .await?;
        table.execute_upsert(record_batch).await?;
        Ok(())
    }

    async fn read_table(
        suffix: &str,
        metadata_client: MetaDataClientRef,
    ) -> Result<(), anyhow::Error> {
        let table_name = format!("test_rbac_table_{}", suffix);
        let builder = create_io_config_builder(
            metadata_client.clone(),
            Some(table_name.as_str()),
            true,
            "default",
            Default::default(),
            Default::default(),
        )
        .await?;
        let sess_ctx = create_session_context_with_planner(
            &mut builder.build(),
            Some(LakeSoulQueryPlanner::new_ref()),
        )?;
        let table = LakeSoulTable::for_namespace_and_name(
            "default",
            &table_name,
            Some(metadata_client.clone()),
        )
        .await?;
        let dataframe = table.to_dataframe(&sess_ctx).await?;
        let results = dataframe.collect().await?;
        print_batches(&results)?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].num_columns(), 2);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[1].num_columns(), 2);
        assert_eq!(results[1].num_rows(), 3);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_s3_rbac() -> Result<(), anyhow::Error> {
        init_s3();
        let metadata_client = Arc::new(MetaDataClient::from_env().await?);

        let (uuid_ads, table_path_ads) =
            create_table_and_write_suffix("ads", metadata_client.clone()).await?;
        let (uuid_dwd, table_path_dwd) =
            create_table_and_write_suffix("dwd", metadata_client.clone()).await?;

        let _thread_handle = run_server();
        tokio::time::sleep(Duration::from_secs(2)).await;
        change_s3_to_proxy();

        // verify read/write table in ads domain success
        insert_table("ads", metadata_client.clone()).await?;
        read_table("ads", metadata_client.clone()).await?;

        // verify read/write table in dwd domain failed
        let err = insert_table("dwd", metadata_client.clone())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403 Forbidden"));
        let err = read_table("dwd", metadata_client.clone())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403 Forbidden"));

        drop_table(&uuid_ads, &table_path_ads, metadata_client.clone()).await?;
        drop_table(&uuid_dwd, &table_path_dwd, metadata_client.clone()).await?;

        // exit self by signal 15
        tokio::time::sleep(Duration::from_secs(1)).await;
        std::process::exit(0);
    }
}
