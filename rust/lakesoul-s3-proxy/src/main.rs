// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arc_swap::ArcSwap;
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
use log::{debug, error, info};
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

fn main() {
    env_logger::init();
    let mut proxy_server = Server::new(None).unwrap();
    proxy_server.bootstrap();

    let endpoint = std::env::var("AWS_ENDPOINT").expect("need AWS_ENDPOINT env");
    let region = std::env::var("AWS_REGION").expect("need AWS_REGION env");
    let virtual_host = std::env::var("AWS_VIRTUAL_HOST").map_or(false, |v| v.to_lowercase() == "true");
    info!("endpoint {}, region {}, virtual_host {}", endpoint, region, virtual_host);

    let uri = Uri::from_str(endpoint.as_str()).unwrap();
    let host = uri.host().unwrap();
    let mut upstreams = LoadBalancer::from(DnsDiscovery::new(
        host,
        80,
        Arc::new(TokioAsyncResolver::tokio_from_system_conf().unwrap()),
    ));
    upstreams.health_check_frequency = Some(Duration::from_secs(600));
    upstreams.update_frequency = Some(Duration::from_secs(600));
    upstreams.parallel_health_check = true;

    let background_dns_service = background_service("dns resolver", upstreams);
    let lb = background_dns_service.task();

    let background_s3_credentials = background_service(
        "s3 credentials",
        S3Credentials {
            region,
            identity: ArcSwap::new(Arc::new(Identity::new(0, None))),
        },
    );
    let cred = background_s3_credentials.task();

    let tls: bool = if let Some(scheme) = uri.scheme_str() {
        scheme == "https"
    } else {
        false
    };
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
    cred: Arc<S3Credentials>,
    host: String,
    tls: bool,
    virtual_host: bool,
}

pub struct S3Credentials {
    region: String,
    identity: ArcSwap<Identity>,
}

impl S3Credentials {
    fn sign(&self, headers: &mut RequestHeader, host: &String, virtual_host: bool) -> Result<(), anyhow::Error> {
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
        if virtual_host {
            // replace host header
            if let Some(host_header) = headers.headers.get(HOST) {
                let s = host_header.to_str()?;
                if let Some(first_dot) = s.find(".") {
                    let bucket = &s[0..first_dot];
                    let new_host = format!("{}.{}", bucket, host);
                    info!("new host {}", new_host);
                    headers.insert_header(HOST, HeaderValue::try_from(new_host)?)?;
                }
            }
        }
        let signable_request = SignableRequest::new(
            headers.method.as_str(),
            &uri,
            headers
                .headers
                .iter()
                .filter_map(|(name, value)| {
                    match value.to_str() {
                        Ok(v) => Some((name.as_str(), v)),
                        Err(_) => None,
                    }
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
impl BackgroundService for S3Credentials {
    async fn start(&self, shutdown: ShutdownWatch) {
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
        match self.cred.sign(header, &self.host, self.virtual_host) {
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
