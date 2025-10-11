// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::handler::{parse_host_port, HTTPHandler};
use anyhow::Error;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::Utc;
use http::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE, DATE, HOST, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, RANGE};
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use pingora::Result;
use pingora::http::RequestHeader;
use std::borrow::Cow;
use std::collections::HashMap;
use arrow_array::new_empty_array;
use tokio::sync::Mutex;
use tracing::log::info;
use url::Url;

static CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
static AZURE_VERSION: HeaderValue = HeaderValue::from_static("2023-11-03");
static VERSION: HeaderName = HeaderName::from_static("x-ms-version");
static BLOB_TYPE: HeaderName = HeaderName::from_static("x-ms-blob-type");
static BLOB_TYPE_VALUE: HeaderValue = HeaderValue::from_static("BlockBlob");
const RFC1123_FMT: &str = "%a, %d %h %Y %T GMT";

#[derive(Debug)]
pub struct AzureHandler {
    endpoint: String,
    host: String,
    key: AzureAccessKey,
    account: String,
    multi_part_upload_state: Mutex<HashMap<String, Vec<(usize, String)>>>,
}

impl AzureHandler {
    pub fn try_new() -> Result<Self, Error> {
        let endpoint = std::env::var("AZURE_ENDPOINT")?;
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME")?;
        let key = std::env::var("AZURE_STORAGE_ACCOUNT_KEY")?;
        let az_key = AzureAccessKey::try_new(key.as_str())?;
        let (host, _) = parse_host_port(endpoint.as_str())?;
        Ok(Self {
            endpoint: endpoint.trim_end_matches("/").to_string(),
            host,
            key: az_key,
            account,
            multi_part_upload_state: Mutex::new(HashMap::new()),
        })
    }

    // 1. convert necessary aws headers/url query params to azure's
    // 2. add necessary azure only headers
    // 3. sign using azure shared key and add authorization header
    pub fn process_request_headers(
        &self,
        headers: &mut RequestHeader,
    ) -> Result<(), Error> {
        let s = format!("{}{}", self.endpoint, headers.uri.to_string());
        let url = Url::parse(s.as_str())?;
        // 1.
        rewrite_queries(&url, headers)?;
        rewrite_request_headers(headers)?;
        // 2.
        add_required_headers(headers, &self.host)?;
        // 3.
        sign(&url, headers, self.account.as_str(), &self.key)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl HTTPHandler for AzureHandler {
    fn handle_request_header(
        &self,
        headers: &mut RequestHeader,
        _bucket: &str,
    ) -> Result<(), Error> {
        self.process_request_headers(headers)?;
        Ok(())
    }

    async fn refresh_identity(&self) -> std::result::Result<(), Error> {
        Ok(())
    }

    fn get_endpoint(&self) -> String {
        self.endpoint.clone()
    }
}

// following code are adapted from arrow-rs/object_store/azure

fn hmac_sha256(secret: impl AsRef<[u8]>, bytes: impl AsRef<[u8]>) -> ring::hmac::Tag {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_ref());
    ring::hmac::sign(&key, bytes.as_ref())
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct AzureAccessKey(Vec<u8>);

impl AzureAccessKey {
    /// Create a new [`AzureAccessKey`], checking it for validity
    pub fn try_new(key: &str) -> Result<Self, Error> {
        let key = BASE64_STANDARD.decode(key)?;

        Ok(Self(key))
    }
}

fn sign(
    url: &Url,
    headers: &mut RequestHeader,
    account: &str,
    key: &AzureAccessKey,
) -> Result<(), Error> {
    let signature =
        generate_authorization(&headers.headers, &url, &headers.method, account, key);
    info!("azure signing {}, {}", url, signature);
    headers.insert_header(AUTHORIZATION, signature)?;
    Ok(())
}

fn rewrite_queries(url: &Url, headers: &mut RequestHeader) -> Result<(), Error> {
    if headers.method == Method::HEAD {
        let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(
            &headers.uri.to_string()
        )?;
        query.insert("comp", "metadata");
        headers.set_uri(query.build_uri().to_string().parse()?);
    } else {
        let url.query_pairs()
    }
    Ok(())
}

fn rewrite_request_headers(headers: &mut RequestHeader) -> Result<(), Error> {
    let mut new_headers = HeaderMap::new();
    headers.headers.iter().for_each(|(k, v)| {
        if k.as_str().starts_with("x-amz-meta") {
            new_headers.append(k.as_str().replace("amz", "ms").as_str(), v.clone());
        } else if !k.as_str().starts_with("x-amz-") {
            new_headers.append(k, v.clone());
        }
    });
    headers.headers = new_headers;
    Ok(())
}

fn add_required_headers(headers: &mut RequestHeader, host: &str) -> Result<(), Error> {
    let date = Utc::now();
    let date_str = date.format(RFC1123_FMT).to_string();
    let date_val = HeaderValue::from_str(&date_str)?;
    headers.insert_header(HOST, host)?;
    headers.insert_header(DATE, date_val)?;
    headers.insert_header(VERSION.clone(), AZURE_VERSION.clone())?;
    headers.insert_header(BLOB_TYPE.clone(), BLOB_TYPE_VALUE.clone())?;
    Ok(())
}

fn generate_authorization(
    h: &HeaderMap,
    u: &Url,
    method: &Method,
    account: &str,
    key: &AzureAccessKey,
) -> String {
    let str_to_sign = string_to_sign(h, u, method, account);
    let auth = hmac_sha256(&key.0, str_to_sign);
    format!("SharedKey {}:{}", account, BASE64_STANDARD.encode(auth))
}

fn string_to_sign(h: &HeaderMap, u: &Url, method: &Method, account: &str) -> String {
    // content length must only be specified if != 0
    // this is valid from 2015-02-21
    let content_length = h
        .get(&CONTENT_LENGTH)
        .map(|s| s.to_str())
        .transpose()
        .ok()
        .flatten()
        .filter(|&v| v != "0")
        .unwrap_or_default();
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method.as_ref(),
        add_if_exists(h, &CONTENT_ENCODING),
        add_if_exists(h, &CONTENT_LANGUAGE),
        content_length,
        add_if_exists(h, &CONTENT_MD5),
        add_if_exists(h, &CONTENT_TYPE),
        add_if_exists(h, &DATE),
        add_if_exists(h, &IF_MODIFIED_SINCE),
        add_if_exists(h, &IF_MATCH),
        add_if_exists(h, &IF_NONE_MATCH),
        add_if_exists(h, &IF_UNMODIFIED_SINCE),
        add_if_exists(h, &RANGE),
        canonicalize_header(h),
        canonicalize_resource(account, u)
    )
}

/// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-canonicalized-headers-string>
fn canonicalize_header(headers: &HeaderMap) -> String {
    let mut names = headers
        .iter()
        .filter(|&(k, _)| k.as_str().starts_with("x-ms"))
        .filter_map(|(k, v)| match v.to_str() {
            Ok(v) => Some((k.as_ref(), v)),
            Err(_) => None,
        })
        .collect::<Vec<_>>();
    names.sort_unstable();

    let mut result = String::new();
    for (name, value) in names {
        result.push_str(name);
        result.push(':');
        result.push_str(value);
        result.push('\n');
    }
    result
}

fn canonicalize_resource(account: &str, uri: &Url) -> String {
    let mut can_res: String = String::new();
    can_res.push('/');
    can_res.push_str(account);
    can_res.push_str(uri.path().to_string().as_str());
    can_res.push('\n');

    // query parameters
    let query_pairs = uri.query_pairs();
    {
        let mut qps: Vec<String> = Vec::new();
        for (q, _) in query_pairs {
            if !qps.iter().any(|x| x == &*q) {
                qps.push(q.into_owned());
            }
        }

        qps.sort();

        for qparam in qps {
            // find correct parameter
            let ret = lexy_sort(query_pairs, &qparam);

            can_res = can_res + &qparam.to_lowercase() + ":";

            for (i, item) in ret.iter().enumerate() {
                if i > 0 {
                    can_res.push(',');
                }
                can_res.push_str(item);
            }

            can_res.push('\n');
        }
    };

    can_res[0..can_res.len() - 1].to_owned()
}

fn lexy_sort<'a>(
    vec: impl Iterator<Item = (Cow<'a, str>, Cow<'a, str>)> + 'a,
    query_param: &str,
) -> Vec<Cow<'a, str>> {
    let mut values = vec
        .filter(|(k, _)| *k == query_param)
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn add_if_exists<'a>(h: &'a HeaderMap, key: &HeaderName) -> &'a str {
    h.get(key)
        .map(|s| s.to_str())
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default()
}
