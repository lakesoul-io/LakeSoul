// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::aws::{CommonPrefixes, CompleteMultipartUpload, Contents, ListBucketResult};
use crate::context::S3ProxyContext;
use crate::handler::{HTTPHandler, parse_host_port};
use anyhow::{Error, anyhow};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use chrono::{DateTime, FixedOffset, SecondsFormat, Utc};
use http::header::{
    AUTHORIZATION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE,
    DATE, HOST, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, RANGE,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use pingora::http::{RequestHeader, ResponseHeader};
use pingora::prelude::Session;
use pingora::{InternalError, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::Mutex;
use tracing::log::debug;
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
    multi_part_upload_state: Mutex<HashMap<String, Vec<(u16, String)>>>,
    complete_multipart: Mutex<HashMap<String, Bytes>>,
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
            complete_multipart: Mutex::new(HashMap::new()),
        })
    }

    // 1. convert necessary aws headers/url query params to azure's
    // 2. add necessary azure only headers
    // 3. sign using azure shared key and add authorization header
    pub async fn process_request_headers(
        &self,
        url: &Url,
        ctx: &S3ProxyContext,
        headers: &mut RequestHeader,
    ) -> Result<(), Error> {
        // 1.
        self.rewrite_queries(&url, ctx, headers).await?;
        self.rewrite_request_headers(headers)?;
        // 2.
        self.add_required_headers(headers, ctx, &self.host)?;
        // 3.
        // re-construct url
        let s = format!("{}{}", self.endpoint, headers.uri.to_string());
        let url = Url::parse(s.as_str())?;
        sign(&url, headers, self.account.as_str(), &self.key)?;
        Ok(())
    }

    async fn rewrite_queries(
        &self,
        url: &Url,
        ctx: &S3ProxyContext,
        headers: &mut RequestHeader,
    ) -> Result<(), Error> {
        if ctx.request_query_params.contains_key("list-type") {
            self.convert_list_query(url, headers, &ctx.request_query_params)?;
        } else if ctx.request_query_params.contains_key("partNumber") {
            self.convert_uploadpart_query(url, headers, &ctx.request_query_params)
                .await?;
        } else if ctx.request_query_params.contains_key("uploadId") {
            self.convert_complete_multipart_query(
                url,
                headers,
                &ctx.request_query_params,
            )
            .await?;
        }
        Ok(())
    }

    fn convert_list_query(
        &self,
        _url: &Url,
        headers: &mut RequestHeader,
        query_params: &HashMap<String, String>,
    ) -> Result<(), Error> {
        debug!("converting list query params {:?}", query_params);
        let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(
            &headers.uri.to_string(),
        )?;
        query.clear_params();
        query.insert("comp", "list");
        query.insert("restype", "container");
        // for listing root dir, should not pass prefix params(even if empty) to azure
        query_params.get("prefix").iter().for_each(|p| {
            if !p.is_empty() && !(*p == "/") {
                query.insert("prefix", p);
            }
        });
        query.insert(
            "delimiter",
            query_params.get("delimiter").unwrap_or(&"/".to_string()),
        );
        query_params.get("max-keys").iter().for_each(|max| {
            query.insert("maxresults", max);
        });
        query_params
            .get("continuation-token")
            .iter()
            .for_each(|token| {
                query.insert("marker", token);
            });
        let q = query.build_uri().to_string().parse()?;
        debug!(
            "converting list query params {:?} to uri {:?}",
            query_params, q
        );
        headers.set_uri(q);
        Ok(())
    }

    async fn convert_uploadpart_query(
        &self,
        _url: &Url,
        headers: &mut RequestHeader,
        query_params: &HashMap<String, String>,
    ) -> Result<(), Error> {
        debug!("converting upload part params {:?}", query_params);
        let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(
            &headers.uri.to_string(),
        )?;
        query.clear_params();
        query.insert("comp", "block");
        let part_number = query_params
            .get("partNumber")
            .ok_or(anyhow!("missing part number for uploads"))?;
        let part_number: u16 = part_number.parse()?;
        if part_number > 10000 {
            return Err(anyhow!("part number {} is too large", part_number));
        }
        let upload_id = query_params
            .get("uploadId")
            .ok_or(anyhow!("missing upload ID for uploads"))?;
        {
            let mut multipart_state = self.multi_part_upload_state.lock().await;
            let upload_state = multipart_state
                .get_mut(upload_id)
                .ok_or(anyhow!("missing upload state for upload_id {}", upload_id))?;
            let block_id = if let Some((_, block_id)) =
                upload_state.iter().find(|(p, _)| *p == part_number)
            {
                // use existing block id
                block_id
            } else {
                let idx = u128::from_be_bytes(rand::rng().random());
                let part_idx = format!("{idx:032x}");
                let part_idx = BASE64_STANDARD.encode(&part_idx);
                // create new block id
                upload_state.push((part_number, part_idx.clone()));
                &upload_state.last().unwrap().1
            };
            query.insert("blockId", block_id);
        }

        let q = query.build_uri().to_string().parse()?;
        debug!(
            "converting upload part query params {:?} to uri {:?}",
            query_params, q
        );
        headers.set_uri(q);
        Ok(())
    }

    async fn convert_complete_multipart_query(
        &self,
        _url: &Url,
        headers: &mut RequestHeader,
        query_params: &HashMap<String, String>,
    ) -> Result<(), Error> {
        // change method to put
        headers.set_method(Method::PUT);
        debug!("converting complete part params {:?}", query_params);
        let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(
            &headers.uri.to_string(),
        )?;
        query.clear_params();
        query.insert("comp", "blocklist");
        let q = query.build_uri().to_string().parse()?;
        debug!(
            "converting complete part query params {:?} to uri {:?}",
            query_params, q
        );
        headers.set_uri(q);

        // generate complete multipart request body in advance
        // so that we can change the `Content-Length` header early
        let upload_id = query_params.get("uploadId").unwrap();
        let mut multi_part_state = self.multi_part_upload_state.lock().await;
        let state = multi_part_state
            .get_mut(upload_id)
            .ok_or(anyhow!("cannot find upload ID {:?}", upload_id))?;
        state.sort_unstable_by_key(|(n, _)| *n);
        let block_ids = state
            .iter_mut()
            .map(|(_, block_id)| std::mem::take(block_id))
            .collect();
        let azure_request = BlockList { latest: block_ids };
        let s = quick_xml::se::to_string(&azure_request)?;
        debug!("Converted azure complete multipart upload {:?}", s);
        let bytes = Bytes::from(s);
        let length = bytes.len();
        drop(multi_part_state);
        let mut complete_part_state = self.complete_multipart.lock().await;
        complete_part_state.insert(upload_id.clone(), bytes);
        drop(complete_part_state);
        headers.insert_header("Content-Length", HeaderValue::from(length))?;
        Ok(())
    }

    fn rewrite_request_headers(&self, headers: &mut RequestHeader) -> Result<(), Error> {
        let mut new_headers: HashMap<String, HeaderValue> = HashMap::new();
        headers.as_ref().headers.iter().for_each(|(k, v)| {
            if k.as_str().starts_with("x-amz-meta") {
                let new_header = k.as_str().replace("amz", "ms");
                new_headers.insert(new_header, v.clone());
            }
        });
        new_headers.into_iter().try_for_each(|(k, v)| {
            headers.insert_header(HeaderName::from_str(&k)?, v)?;
            Result::<(), Error>::Ok(())
        })?;
        Ok(())
    }

    fn add_required_headers(
        &self,
        headers: &mut RequestHeader,
        ctx: &S3ProxyContext,
        host: &str,
    ) -> Result<(), Error> {
        let date = Utc::now();
        let date_str = date.format(RFC1123_FMT).to_string();
        let date_val = HeaderValue::from_str(&date_str)?;
        headers.insert_header(HOST, host)?;
        headers.insert_header(DATE, date_val)?;
        headers.insert_header(VERSION.clone(), AZURE_VERSION.clone())?;
        if headers.method == Method::PUT && !ctx.request_query_params.contains_key("uploadId") {
            headers.insert_header(BLOB_TYPE.clone(), BLOB_TYPE_VALUE.clone())?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl HTTPHandler for AzureHandler {
    async fn handle_request_header(
        &self,
        session: &mut Session,
        ctx: &S3ProxyContext,
    ) -> Result<bool, Error> {
        let s = format!("{}{}", self.endpoint, session.req_header().uri.to_string());
        let url = Url::parse(s.as_str())?;
        // first handle CreateMultipartUpload request
        // generate upload id and fake results
        if session.req_header().method == Method::POST
            && ctx.request_query_params.contains_key("uploads")
        {
            let path = url.path();
            // starting from second path segment is the key
            let key = &path[ctx.bucket.len() + 2..];
            // generate a random upload id
            let idx = u128::from_be_bytes(rand::rng().random());
            let upload_id = format!("{idx:032x}");
            let upload_id = BASE64_STANDARD.encode(&upload_id);
            let header = ResponseHeader::build(200, None)?;
            session.set_keepalive(None);
            session
                .write_response_header(Box::new(header), true)
                .await?;
            self.multi_part_upload_state
                .lock()
                .await
                .insert(upload_id.clone(), vec![]);
            let resp_body = InitiateMultipartUploadResult {
                bucket: ctx.bucket.clone(),
                key: key.to_string(),
                upload_id,
            };
            let s = quick_xml::se::to_string(&resp_body)?;
            debug!("generated CreateMultipartUploadResult {:?}", s);
            session
                .write_response_body(Some(Bytes::from(s)), true)
                .await?;
            return Ok(true);
        }

        // handle AbortMultipartUpload
        if session.req_header().method == Method::DELETE
            && ctx.request_query_params.contains_key("uploadId")
        {
            self.multi_part_upload_state
                .lock()
                .await
                .remove(ctx.request_query_params.get("uploadId").unwrap());
            let header = ResponseHeader::build(204, None)?;
            session.set_keepalive(None);
            session
                .write_response_header(Box::new(header), true)
                .await?;
            return Ok(true);
        }

        Ok(false)
    }

    async fn change_upstream_header(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &S3ProxyContext,
    ) -> std::result::Result<(), Error> {
        // handle query rewrite, change headers, and signing
        let s = format!("{}{}", self.endpoint, session.req_header().uri.to_string());
        let url = Url::parse(s.as_str())?;
        self.process_request_headers(&url, ctx, upstream_request)
            .await?;
        Ok(())
    }

    fn handle_response_header(
        &self,
        ctx: &S3ProxyContext,
        headers: &mut ResponseHeader,
    ) -> std::result::Result<(), Error> {
        let etag = headers.headers.get("ETag");
        match etag {
            Some(etag) => {
                let new_etag = etag.to_str()?.trim_matches('"');
                headers.insert_header("ETag", HeaderValue::from_str(new_etag)?)?;
            }
            None => {
                if ctx.request_query_params.get("partNumber").is_some() {
                    // for upload part, we must add ETag header in response header
                    let md5 = headers.headers.get("Content-MD5").cloned();
                    if md5.is_some() {
                        headers.insert_header("ETag", md5.unwrap())?;
                    }
                }
            }
        }
        let last_modified = headers.headers.get("Last-Modified");
        match last_modified {
            Some(last_modified) => {
                let new_last_modified = convert_timestamp(last_modified.to_str()?)?;
                headers.insert_header("Last-Modified", new_last_modified)?;
            }
            None => {}
        }
        Ok(())
    }

    async fn refresh_identity(&self) -> std::result::Result<(), Error> {
        Ok(())
    }

    fn get_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn require_request_body_rewrite(
        &self,
        ctx: &S3ProxyContext,
        headers: &RequestHeader,
    ) -> bool {
        // rewrite request body for CompleteMultipartUpload
        headers.method == Method::POST
            && ctx.request_query_params.contains_key("uploadId")
    }

    fn require_response_body_rewrite(
        &self,
        ctx: &S3ProxyContext,
        headers: &RequestHeader,
    ) -> bool {
        headers.method == Method::GET
            && ctx.request_query_params.contains_key("list-type")
    }

    async fn rewrite_request_body(
        &self,
        _headers: &RequestHeader,
        ctx: &mut S3ProxyContext,
        body: &mut Option<Bytes>,
    ) -> std::result::Result<(), Error> {
        if !ctx.request_query_params.contains_key("partNumber")
            && ctx.request_query_params.contains_key("uploadId")
        {
            let upload_id = ctx.request_query_params.get("uploadId").unwrap();
            debug!("rewrite request_body for uploadId {:?}", upload_id);
            let request_body = String::from_utf8(std::mem::take(&mut ctx.request_body))?;
            let aws_request: CompleteMultipartUpload =
                quick_xml::de::from_str(&request_body)?;

            let mut multipart_state = self.multi_part_upload_state.lock().await;
            let state_len = multipart_state
                .get_mut(upload_id)
                .ok_or(anyhow!("cannot find upload ID {:?}", upload_id))?
                .len();
            // clear state
            multipart_state.remove(upload_id);
            // release lock
            drop(multipart_state);
            if state_len != aws_request.part.len() {
                return Err(anyhow!(
                    "Number of parts mismatched, request {:?}, state {:?}",
                    aws_request.part,
                    state_len
                ));
            }

            let mut complete_state = self.complete_multipart.lock().await;
            let bytes = complete_state.get_mut(upload_id).ok_or(anyhow!(
                "cannot find upload ID in complete part bytes {:?}",
                upload_id
            ))?;
            *body = Some(std::mem::take(bytes));
            // clear state
            complete_state.remove(upload_id);
            // release lock
            drop(complete_state);
        } else {
            *body = Some(Bytes::from(std::mem::take(&mut ctx.request_body)))
        }
        Ok(())
    }

    fn rewrite_response_body(
        &self,
        headers: &RequestHeader,
        ctx: &mut S3ProxyContext,
        body: &mut Option<Bytes>,
    ) -> std::result::Result<(), Error> {
        if headers.method == Method::GET
            && ctx.request_query_params.contains_key("list-type")
        {
            debug!("azure rewrite response body for list");
            // convert azure list result to aws
            let response_body =
                String::from_utf8(std::mem::take(&mut ctx.response_body))?;
            let mut azure_result: EnumerationResults =
                quick_xml::de::from_str(&response_body)?;
            let key_count = azure_result.blobs.blobs.len() as u64;
            let mut aws_result = ListBucketResult {
                name: ctx.bucket.clone(),
                prefix: std::mem::take(&mut azure_result.prefix),
                key_count,
                max_keys: azure_result.max_results.unwrap_or(key_count),
                is_truncated: !azure_result
                    .next_marker
                    .as_ref()
                    .unwrap_or(&String::new())
                    .is_empty(),
                continuation_token: azure_result
                    .marker
                    .as_mut()
                    .map(|marker| std::mem::take(marker))
                    .unwrap_or(String::new()),
                next_continuation_token: azure_result
                    .next_marker
                    .as_mut()
                    .map(|marker| std::mem::take(marker))
                    .unwrap_or(String::new()),
                contents: vec![],
                common_prefixes: vec![],
            };
            azure_result
                .blobs
                .blobs
                .into_iter()
                .try_for_each(|blob| match blob {
                    BlobOrPrefix::Blob(mut blob) => {
                        aws_result.contents.push(Contents {
                            key: std::mem::take(&mut blob.name),
                            last_modified: convert_timestamp(&take_string_from_map(
                                &mut blob.properties,
                                "Last-Modified",
                            ))?,
                            etag: take_string_from_map(&mut blob.properties, "ETag"),
                            size: take_string_from_map(
                                &mut blob.properties,
                                "Content-Length",
                            ),
                            storage_class: "STANDARD".to_string(),
                        });
                        Ok::<(), Error>(())
                    }
                    BlobOrPrefix::BlobPrefix(mut prefix) => {
                        aws_result.common_prefixes.push(CommonPrefixes {
                            prefix: std::mem::take(&mut prefix.name),
                        });
                        Ok(())
                    }
                })?;
            debug!("Converted azure list results {:?}", aws_result);
            let s = quick_xml::se::to_string(&aws_result)?;
            *body = Some(Bytes::from(s));
        } else {
            *body = Some(Bytes::from(std::mem::take(&mut ctx.response_body)))
        }
        Ok(())
    }
}

fn take_string_from_map(map: &mut HashMap<String, String>, key: &str) -> String {
    map.get_mut(key)
        .map_or(String::new(), |v| std::mem::take(v))
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct BlobPrefix {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct Blob {
    pub name: String,
    pub properties: HashMap<String, String>,
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
enum BlobOrPrefix {
    Blob(Blob),
    BlobPrefix(BlobPrefix),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct Blobs {
    #[serde(rename = "$value", default)]
    pub blobs: Vec<BlobOrPrefix>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct EnumerationResults {
    pub prefix: Option<String>,
    pub marker: Option<String>,
    pub max_results: Option<u64>,
    pub delimiter: String,
    pub next_marker: Option<String>,
    pub blobs: Blobs,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct BlockList {
    pub latest: Vec<String>,
}

fn sign(
    url: &Url,
    headers: &mut RequestHeader,
    account: &str,
    key: &AzureAccessKey,
) -> Result<(), Error> {
    let signature =
        generate_authorization(&headers.headers, &url, &headers.method, account, key);
    debug!("azure signing {}, {}", url, signature);
    headers.insert_header(AUTHORIZATION, signature)?;
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

fn convert_timestamp(input: &str) -> Result<String> {
    if input.is_empty() {
        return Ok(String::new());
    }
    let datetime: DateTime<FixedOffset> =
        DateTime::parse_from_rfc2822(input).map_err(|e| {
            pingora::Error::because(InternalError, "handle response body error", e)
        })?;
    Ok(datetime
        .to_utc()
        .to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use crate::azure::{Blob, BlobOrPrefix, BlobPrefix, Blobs, EnumerationResults};

    #[test]
    fn test_list_result_parse() {
        let input = "<?xml version= \"1.0\" encoding= \"utf-8\"?>
<EnumerationResults ServiceEndpoint= \"https://lakeinsight.blob.core.windows.net/\" ContainerName= \"lakeinsight-test\">
    <Prefix>files/</Prefix>
    <Delimiter>/</Delimiter>
    <Blobs>
        <BlobPrefix>
            <Name>files/flink-env/</Name>
        </BlobPrefix>
        <Blob>
            <Name>files/lakesoul-flink-1.17-2.6.0-1.jar</Name>
            <Properties>
                <Creation-Time>Thu, 16 Oct 2025 07:20:10 GMT</Creation-Time>
                <Last-Modified>Fri, 17 Oct 2025 03:32:29 GMT</Last-Modified>
                <Etag>0x8DE0D2DCC95938D</Etag>
                <Content-Length>108604572</Content-Length>
                <Content-Type>application/java-archive</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5 />
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>files/lakesoul-flink-1.20-3.0.0-SNAPSHOT.jar</Name>
            <Properties>
                <Creation-Time>Thu, 16 Oct 2025 07:40:40 GMT</Creation-Time>
                <Last-Modified>Thu, 16 Oct 2025 07:40:40 GMT</Last-Modified>
                <Etag>0x8DE0C874DEDC3D6</Etag>
                <Content-Length>138686786</Content-Length>
                <Content-Type>application/octet-stream</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5 />
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>files/lakesoul-presto-0.29-3.0.0-SNAPSHOT.jar</Name>
            <Properties>
                <Creation-Time>Thu, 16 Oct 2025 07:40:38 GMT</Creation-Time>
                <Last-Modified>Thu, 16 Oct 2025 07:40:38 GMT</Last-Modified>
                <Etag>0x8DE0C874D37AE38</Etag>
                <Content-Length>175048184</Content-Length>
                <Content-Type>application/octet-stream</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5 />
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>files/lakesoul-spark-3.3-2.6.0-1.jar</Name>
            <Properties>
                <Creation-Time>Thu, 16 Oct 2025 07:20:10 GMT</Creation-Time>
                <Last-Modified>Fri, 17 Oct 2025 03:32:28 GMT</Last-Modified>
                <Etag>0x8DE0D2DCC7D2D15</Etag>
                <Content-Length>30522431</Content-Length>
                <Content-Type>application/java-archive</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5 />
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>files/lakesoulPgAssets-1.0-SNAPSHOT.jar</Name>
            <Properties>
                <Creation-Time>Thu, 16 Oct 2025 07:20:10 GMT</Creation-Time>
                <Last-Modified>Fri, 17 Oct 2025 03:32:28 GMT</Last-Modified>
                <Etag>0x8DE0D2DCC6B0745</Etag>
                <Content-Length>20116864</Content-Length>
                <Content-Type>application/java-archive</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5 />
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>files/mc</Name>
            <Properties>
                <Creation-Time>Thu, 16 Oct 2025 07:20:10 GMT</Creation-Time>
                <Last-Modified>Fri, 17 Oct 2025 03:32:29 GMT</Last-Modified>
                <Etag>0x8DE0D2DCC93BF0B</Etag>
                <Content-Length>26681344</Content-Length>
                <Content-Type>application/x-executable</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5 />
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <Blob>
            <Name>files/metrics.properties</Name>
            <Properties>
                <Creation-Time>Thu, 16 Oct 2025 07:20:10 GMT</Creation-Time>
                <Last-Modified>Fri, 17 Oct 2025 03:32:28 GMT</Last-Modified>
                <Etag>0x8DE0D2DCC6D02CF</Etag>
                <Content-Length>548</Content-Length>
                <Content-Type>application/octet-stream</Content-Type>
                <Content-Encoding />
                <Content-Language />
                <Content-CRC64 />
                <Content-MD5>N7rCgIb2kbP6p6EbnSO31A==</Content-MD5>
                <Cache-Control />
                <Content-Disposition />
                <BlobType>BlockBlob</BlobType>
                <AccessTier>Hot</AccessTier>
                <AccessTierInferred>true</AccessTierInferred>
                <LeaseStatus>unlocked</LeaseStatus>
                <LeaseState>available</LeaseState>
                <ServerEncrypted>true</ServerEncrypted>
            </Properties>
            <OrMetadata />
        </Blob>
        <BlobPrefix>
            <Name>files/spark-env/</Name>
        </BlobPrefix>
    </Blobs>
    <NextMarker />
</EnumerationResults>";
        let result: EnumerationResults = quick_xml::de::from_str(input).unwrap();
        println!("{:#?}", result);
        let er = EnumerationResults {
            prefix: Some("a".to_string()),
            marker: Some("marker".to_string()),
            max_results: Some(123),
            delimiter: "/".to_string(),
            next_marker: Some("asdfasdf".to_string()),
            blobs: Blobs {
                blobs: vec![
                    BlobOrPrefix::BlobPrefix(BlobPrefix {
                        name: "blob_prefix".to_string(),
                    }),
                    BlobOrPrefix::Blob(Blob {
                        name: "blob name".to_string(),
                        properties: Default::default(),
                        metadata: None,
                    }),
                ],
            },
        };
        let s = quick_xml::se::to_string(&er).unwrap();
        println!("{}", s);
    }
}
