// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::context::S3ProxyContext;
use crate::handler::{HTTPHandler, parse_host_port};
use anyhow::Error;
use arc_swap::ArcSwap;
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
use http::HeaderValue;
use http::header::{CONTENT_LENGTH, HOST};
use pingora::http::RequestHeader;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info};

#[derive(Debug)]
pub struct AWSHandler {
    endpoint: String,
    host: String,
    region: String,
    virtual_host: bool,
    identity: ArcSwap<Identity>,
}

impl AWSHandler {
    pub fn try_new() -> Result<Self, Error> {
        let endpoint = std::env::var("AWS_ENDPOINT")?;
        let region = std::env::var("AWS_REGION")?;
        let virtual_host =
            std::env::var("AWS_VIRTUAL_HOST").is_ok_and(|v| v.to_lowercase() == "true");
        let (host, _) = parse_host_port(endpoint.as_str())?;
        Ok(Self {
            endpoint,
            host,
            region,
            virtual_host,
            identity: ArcSwap::new(Arc::new(Identity::new(0, None))),
        })
    }
}

#[async_trait::async_trait]
impl HTTPHandler for AWSHandler {
    fn handle_request_header(
        &self,
        headers: &mut RequestHeader,
        bucket: &str,
    ) -> Result<(), Error> {
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
            let new_host = format!("{}.{}", bucket, self.host);
            debug!("new host {}", new_host);
            headers.insert_header(HOST, HeaderValue::try_from(new_host)?)?;
            // rewrite path to remove bucket name
            let start = uri.find(bucket).unwrap();
            uri.replace_range(start..(start + bucket.len()), "");
            uri = uri.replace("//", "/");
            debug!("replaced uri {}", uri);
        } else {
            headers.insert_header(HOST, HeaderValue::try_from(self.host.clone())?)?;
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

    async fn refresh_identity(&self) -> Result<(), Error> {
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
            })?;
        Ok(())
    }

    fn get_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn require_request_body_rewrite(
        &self,
        _ctx: &S3ProxyContext,
        _headers: &RequestHeader,
    ) -> bool {
        false
    }

    fn require_response_body_rewrite(
        &self,
        _ctx: &S3ProxyContext,
        _headers: &RequestHeader,
    ) -> bool {
        false
    }

    fn rewrite_request_body(
        &self,
        _headers: &RequestHeader,
        _ctx: &mut S3ProxyContext,
        _body: &mut Option<Bytes>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn rewrite_response_body(
        &self,
        _headers: &RequestHeader,
        _ctx: &mut S3ProxyContext,
        _body: &mut Option<Bytes>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct Contents {
    pub key: String,
    pub last_modified: String,
    pub etag: String,
    pub size: String,
    pub storage_class: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefixes {
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ContentsWrap {
    pub contents: Vec<Contents>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ListBucketResult {
    pub name: String,
    pub prefix: Option<String>,
    pub key_count: u64,
    pub max_keys: u64,
    pub is_truncated: bool,
    pub continuation_token: String,
    pub next_continuation_token: String,
    #[serde(rename = "$value", default)]
    pub contents: Vec<Contents>,
    #[serde(rename = "$value", default)]
    pub common_prefixes: Vec<CommonPrefixes>,
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_list_serde() {
        let l = ListBucketResult {
            name: "".to_string(),
            prefix: "".to_string(),
            key_count: 0,
            max_keys: 0,
            is_truncated: false,
            continuation_token: "".to_string(),
            next_continuation_token: "".to_string(),
            contents: vec![Contents {
                key: "".to_string(),
                last_modified: "".to_string(),
                etag: "".to_string(),
                size: "0".to_string(),
                storage_class: "".to_string(),
            }],
            common_prefixes: vec![],
        };
        let s = quick_xml::se::to_string(&l).unwrap();
        println!("{}", s);
    }
}
