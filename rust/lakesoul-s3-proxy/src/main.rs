// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::Region;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{sign, PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use hickory_resolver::TokioAsyncResolver;
use http::header::HOST;
use http::{HeaderValue, Uri};
use lakesoul_metadata::rbac::verify_permission_by_table_path;
use lakesoul_metadata::MetaDataClient;
use pingora::lb::discovery::ServiceDiscovery;
use pingora::lb::selection::{BackendIter, BackendSelection};
use pingora::lb::{Backend, Backends, Extensions};
use pingora::prelude::*;
use pingora::protocols::l4::socket::SocketAddr;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info};

fn main() {
    let timer = tracing_subscriber::fmt::time::ChronoLocal::rfc_3339();
    match tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_timer(timer)
        .try_init()
    {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Failed to set logger: {e:?}");
        }
    }

    let mut proxy_server = Server::new(None).unwrap();
    proxy_server.bootstrap();

    let endpoint = std::env::var("AWS_ENDPOINT").expect("need AWS_ENDPOINT env");
    let region = std::env::var("AWS_REGION").expect("need AWS_REGION env");
    let virtual_host = std::env::var("AWS_VIRTUAL_HOST").map_or(false, |v| v.to_lowercase() == "true");
    let (user, group, verify_meta) = match (
        std::env::var("LAKESOUL_PG_USER"),
        std::env::var("LAKESOUL_CURRENT_DOMAIN"),
    ) {
        (Ok(user), Ok(group)) => (user, group, true),
        _ => (String::new(), String::new(), false),
    };
    info!(
        "endpoint {}, region {}, virtual_host {}",
        endpoint, region, virtual_host
    );

    let uri = Uri::from_str(endpoint.as_str()).unwrap();
    let tls: bool = if let Some(scheme) = uri.scheme_str() {
        scheme == "https"
    } else {
        false
    };
    let host = uri.host().unwrap();
    let port = if let Some(port) = uri.port() {
        port.as_u16()
    } else {
        if tls {
            443
        } else {
            80
        }
    };
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
            user,
            group,
            verify_meta,
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
            virtual_host,
        },
    );
    lb.add_tcp("0.0.0.0:6188");
    proxy_server.add_service(background_dns_service);
    proxy_server.add_service(background_s3_credentials);
    proxy_server.add_service(lb);

    proxy_server.run_forever();
}

pub struct S3Proxy {
    lb: Arc<LoadBalancer<RoundRobin>>,
    cred: Arc<Credentials>,
    host: String,
    tls: bool,
    virtual_host: bool,
}

pub struct Credentials {
    region: String,
    identity: ArcSwap<Identity>,
    metadata_client: ArcSwapOption<MetaDataClient>,
    user: String,
    group: String,
    verify_meta: bool,
}

impl Credentials {
    async fn verify_rbac(&self, headers: &RequestHeader, bucket: &Option<&str>) -> Result<(), anyhow::Error> {
        if !self.verify_meta {
            Ok(())
        } else {
            let binding = self.metadata_client.load();
            if let Some(client) = binding.as_ref() {
                verify_permission_by_table_path(
                    self.user.as_str(),
                    self.group.as_str(),
                    parse_table_path(&headers.uri, bucket).as_str(),
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
        bucket: &Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let mut signing_settings = SigningSettings::default();
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

        let uri = headers.uri.to_string();

        // for virtual host addressing, we need to replace host header
        // with format bucket-name.endpoint-host before signing
        if let Some(bucket_name) = bucket {
            let new_host = format!("{}.{}", bucket_name, host);
            info!("new host {}", new_host);
            headers.insert_header(HOST, HeaderValue::try_from(new_host)?)?;
        }

        // construct request for signing by aws_sigv4
        let signable_request = SignableRequest::new(
            headers.method.as_str(),
            &uri,
            headers.headers.iter().filter_map(|(name, value)| match value.to_str() {
                Ok(v) => Some((name.as_str(), v)),
                Err(_) => None,
            }),
            SignableBody::UnsignedPayload,
        )?;
        let (signing_instructions, _signature) = sign(signable_request, &signing_params)?.into_parts();
        let (new_headers, new_query) = signing_instructions.into_parts();
        info!("new headers {:?}, new query {:?}", new_headers, new_query);

        for header in new_headers.into_iter() {
            let mut value = http::HeaderValue::from_str(&header.value())?;
            value.set_sensitive(header.sensitive());
            headers.insert_header(header.name(), value)?
        }
        if !new_query.is_empty() {
            let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(uri.as_str())?;
            for (name, value) in new_query {
                query.insert(name, &value);
            }
            headers.set_uri(query.build_uri().to_string().parse()?);
        }
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
    fn new_ctx(&self) -> () {
        ()
    }

    async fn upstream_peer(&self, _session: &mut Session, _ctx: &mut ()) -> Result<Box<HttpPeer>> {
        let upstream = self
            .lb
            .select(b"", 256) // hash doesn't matter for round robin
            .unwrap();

        debug!("upstream peer is: {upstream:?}");

        let peer = Box::new(HttpPeer::new(upstream, self.tls, self.host.clone()));
        Ok(peer)
    }

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let header = session.req_header_mut();
        info!("request_filter original header: {header:?}");
        let mut bucket = None;
        if self.virtual_host {
            // replace host header
            if let Some(host_header) = header.headers.get(HOST) {
                match host_header.to_str() {
                    Ok(s) => {
                        if let Some(first_dot) = s.find(".") {
                            bucket = Some(s[0..first_dot].to_string());
                        }
                    }
                    Err(_) => {
                        error!("failed to parse host header");
                        session.respond_error(500).await?;
                        return Ok(true);
                    }
                }
            }
        };
        let bucket = bucket.as_ref().map(|b| b.as_str());

        // verify meta permission
        match self.cred.verify_rbac(header, &bucket).await {
            Err(e) => {
                error!("rbac error {:?}", e);
                session.respond_error(403).await?;
                return Ok(true);
            }
            _ => {}
        }

        // signing
        match self.cred.sign_aws_v4(header, &self.host, &bucket) {
            Ok(_) => Ok(false),
            Err(e) => {
                error!("sign error {:?}", e);
                session.respond_error(500).await?;
                Ok(true)
            }
        }
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
    pub fn new<D: Into<String>>(domain: D, port: u16, resolver: Arc<TokioAsyncResolver>) -> Self {
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
                IpAddr::V4(ip) => Some(SocketAddr::Inet(std::net::SocketAddr::V4(SocketAddrV4::new(
                    ip, self.port,
                )))),
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

fn assemble_table_path<'a, Iter>(split: Iter, bucket_name: &str, partition_equal: &str) -> String
where
    Iter: Iterator<Item = &'a str>,
{
    let mut path = String::with_capacity(256);
    path.push_str("s3://");
    path.push_str(bucket_name);
    split
        .take_while(|s| !(s.ends_with(".parquet") || s.contains(partition_equal)))
        .for_each(|s| {
            path.push_str("/");
            path.push_str(s);
        });
    path.push_str("/");
    path
}

fn parse_table_path_from_query(query: &str, bucket_name: &str) -> String {
    let query_parts_iter = query.split("&");
    for query_part in query_parts_iter {
        let mut query_part_iter = query_part.split("=");
        if let Some(key) = query_part_iter.next() {
            if key == "prefix" {
                if let Some(value) = query_part_iter.next() {
                    return assemble_table_path(value.split("%2F"), bucket_name, "%3D");
                }
            }
        }
    }
    format!("s3://{}/", bucket_name)
}

fn parse_table_path(uri: &Uri, bucket: &Option<&str>) -> String {
    let path = uri.path();
    let mut path_parts_iter = path.split("/").filter(|s| !s.is_empty()).peekable();

    let bucket_name = match bucket {
        Some(b) => b,
        None => {
            // get first part of path as bucket name
            path_parts_iter.next().unwrap()
        }
    };
    if let None = path_parts_iter.peek() {
        // a list request without path
        // retrieve path from query string
        let query = uri.query().unwrap_or("");
        if query.is_empty() {
            format!("s3://{}/", bucket.unwrap())
        } else {
            parse_table_path_from_query(query, bucket_name)
        }
    } else {
        assemble_table_path(path_parts_iter, bucket_name, "=")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_path() {
        assert_eq!(
            parse_table_path(&Uri::from_static("/lakesoul-test-bucket/test/test.parquet"), &None),
            "s3://lakesoul-test-bucket/test/"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/default/abc/test.parquet"),
                &None
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/default/abc/test.parquet"),
                &None
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/lakesoul-test-bucket/test/default/abc/date=20250221/type=1/test.parquet"),
                &None
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );

        // virtual host style
        assert_eq!(
            parse_table_path(&Uri::from_static("/test/test.parquet"), &Some("lakesoul-test-bucket")),
            "s3://lakesoul-test-bucket/test/"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/test/default/abc/test.parquet"),
                &Some("lakesoul-test-bucket")
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/test/default/abc/test.parquet"),
                &Some("lakesoul-test-bucket")
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );

        assert_eq!(
            parse_table_path(
                &Uri::from_static("/test/default/abc/date=20250221/type=1/test.parquet"),
                &Some("lakesoul-test-bucket")
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );

        // list request parse from query
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket?list-type=2&prefix=test%2Fdefault%2Fabc%2Ftest.parquet&delimiter=%2F&encoding-type=url"
                ),
                &None
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/lakesoul-test-bucket?list-type=2&prefix=test%2Fdefault%2Fabc%2Fdate%3D20250221%2Ftype=1%2Ftest.parquet&delimiter=%2F&encoding-type=url"
                ),
                &None
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );
        assert_eq!(
            parse_table_path(
                &Uri::from_static(
                    "/?list-type=2&prefix=test%2Fdefault%2Fabc%2Ftest.parquet&delimiter=%2F&encoding-type=url"
                ),
                &Some("lakesoul-test-bucket")
            ),
            "s3://lakesoul-test-bucket/test/default/abc/"
        );
    }
}
