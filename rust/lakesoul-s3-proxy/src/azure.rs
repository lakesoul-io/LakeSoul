// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::aws::{
    AccessControlList, AccessControlPolicy, CommonPrefixes, CompleteMultipartUpload,
    CompleteMultipartUploadResult, Contents, CopyObjectResult, Delete, DeleteError,
    DeleteResult, Deleted, ListBucketResult, Owner,
};
use crate::context::S3ProxyContext;
use crate::handler::{HTTPHandler, parse_host_port};
use anyhow::{Error, anyhow};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use http::header::{
    AUTHORIZATION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE,
    DATE, ETAG, HOST, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE,
    LAST_MODIFIED, RANGE, TRANSFER_ENCODING,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use pad::PadStr;
use pingora::Result;
use pingora::http::{RequestHeader, ResponseHeader};
use pingora::prelude::Session;
use rand::Rng;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::log::debug;
use url::Url;
use uuid::Uuid;

static CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
static AZURE_VERSION: HeaderValue = HeaderValue::from_static("2020-04-08");
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
    client: reqwest::Client,
    multi_part_upload_state: Mutex<HashMap<String, Vec<(u16, String)>>>,
    batch_delete: Mutex<HashMap<String, Bytes>>,
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
            client: reqwest::Client::new(),
            multi_part_upload_state: Mutex::new(HashMap::new()),
            batch_delete: Mutex::new(HashMap::new()),
        })
    }

    // 1. convert necessary aws headers/url query params to azure's
    // 2. add necessary azure only headers
    // 3. sign using azure shared key and add authorization header
    pub async fn process_request_headers(
        &self,
        url: &Url,
        ctx: &mut S3ProxyContext,
        session: &mut Session,
        headers: &mut RequestHeader,
    ) -> Result<(), Error> {
        // 1.
        self.rewrite_queries(&url, ctx, session, headers).await?;
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
        ctx: &mut S3ProxyContext,
        session: &mut Session,
        headers: &mut RequestHeader,
    ) -> Result<(), Error> {
        if ctx.request_query_params.contains_key("list-type") {
            self.convert_list_query(url, headers, &ctx.request_query_params)?;
        } else if ctx.request_query_params.contains_key("partNumber") {
            self.convert_uploadpart_query(url, headers, &ctx.request_query_params)
                .await?;
        } else if headers.method == Method::DELETE
            && !headers.headers.contains_key("uploadId")
        {
            self.convert_delete_query(session.req_header_mut())?;
        } else if headers.method == Method::POST
            && ctx.request_query_params.contains_key("delete")
        {
            self.convert_batch_delete_query(url, session, headers, ctx)
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
        query_params.get("delimiter").iter().for_each(|p| {
            if !p.is_empty() {
                query.insert("delimiter", p);
            }
        });
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
        aws_request: &CompleteMultipartUpload,
    ) -> Result<Bytes, Error> {
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
        if state.len() != aws_request.part.len() {
            return Err(anyhow!(
                "Number of parts mismatched, request {:?}, state {:?}",
                aws_request.part,
                state.len()
            ));
        }
        state.sort_unstable_by_key(|(n, _)| *n);
        let block_ids = state
            .iter_mut()
            .map(|(_, block_id)| std::mem::take(block_id))
            .collect();
        let azure_request = BlockList { latest: block_ids };
        let s = quick_xml::se::to_string(&azure_request)?;
        debug!("Converted azure complete multipart upload {:?}", s);
        let bytes = Bytes::from(s);
        headers.insert_header(CONTENT_LENGTH, bytes.len())?;
        Ok(bytes)
    }

    fn convert_delete_query(&self, headers: &mut RequestHeader) -> Result<(), Error> {
        debug!("converting delete query params");
        headers.insert_header("x-ms-delete-snapshots", "include")?;
        Ok(())
    }

    async fn convert_batch_delete_query(
        &self,
        _url: &Url,
        session: &mut Session,
        headers: &mut RequestHeader,
        ctx: &mut S3ProxyContext,
    ) -> Result<(), Error> {
        debug!(
            "converting batch delete query params {:?}",
            ctx.request_query_params
        );
        session.enable_retry_buffering();
        loop {
            if session.read_request_body().await?.is_none() {
                break;
            }
        }
        let request_body = session
            .get_retry_buffer()
            .ok_or(anyhow!("cannot get body from session"))?;
        let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(
            &headers.uri.to_string().replace("/?", "?"),
        )?;
        query.clear_params();
        query.insert("comp", "batch");
        query.insert("restype", "container");
        let q = query.build_uri().to_string().parse()?;
        debug!(
            "converting batch delete query params {:?} to uri {:?}",
            ctx.request_query_params, q
        );
        headers.set_uri(q);
        headers.remove_header(&CONTENT_MD5);
        let request_body = String::from_utf8(request_body.to_vec())?;
        debug!("rewrite request_body for batch delete {:?}", request_body);
        let aws_request: Delete = quick_xml::de::from_str(&request_body)?;
        let mut azure_batch = String::new();
        let container_name = headers
            .uri
            .path()
            .split("/")
            .filter(|s| !s.is_empty())
            .next()
            .ok_or(anyhow!("cannot parse container name from {}", headers.uri))?;
        let batch_id = format!("batch_{}", Uuid::new_v4().to_string());
        aws_request
            .object
            .iter()
            .enumerate()
            .try_for_each(|(idx, obj)| {
                if idx > 0 {
                    azure_batch.push_str("\r\n\r\n");
                }
                // genereate subrequest headers
                let path = format!("/{container_name}/{}?recursive=true", obj.key);
                let mut req_header =
                    RequestHeader::build_no_case(Method::DELETE, path.as_bytes(), None)?;

                azure_batch.push_str("--");
                azure_batch.push_str(&batch_id);
                azure_batch.push_str("\r\nContent-Type: application/http\r\n");
                azure_batch.push_str("Content-Transfer-Encoding: binary\r\n");
                azure_batch.push_str(format!("Content-ID: {idx}\r\n\r\n").as_str());

                azure_batch.push_str("DELETE ");
                azure_batch.push_str(&path);
                azure_batch.push_str(" HTTP/1.1\r\n");
                let date = Utc::now();
                let date_str = date.format(RFC1123_FMT).to_string();
                let date_val = HeaderValue::from_str(&date_str)?;
                azure_batch.push_str(DATE.as_str());
                azure_batch.push_str(": ");
                azure_batch.push_str(date_str.as_str());
                req_header.insert_header(DATE, date_val)?;
                azure_batch.push_str("\r\nContent-Length: 0");
                req_header.insert_header("x-ms-delete-snapshots", "include")?;
                azure_batch.push_str("\r\nx-ms-delete-snapshots: include");
                // sign subrequest
                let s = format!("{}{}", self.endpoint, req_header.uri.to_string());
                let url = Url::parse(s.as_str())?;
                sign(&url, &mut req_header, &self.account, &self.key)?;
                azure_batch.push_str("\r\nAuthorization: ");
                azure_batch.push_str(
                    req_header
                        .headers
                        .get(AUTHORIZATION)
                        .ok_or(anyhow!("cannot find authorization header"))?
                        .to_str()?,
                );
                Ok::<(), Error>(())
            })?;
        azure_batch.push_str("\r\n\r\n--");
        azure_batch.push_str(&batch_id);
        azure_batch.push_str("--\r\n");
        debug!("rewrite request_body for batch delete {:?}", azure_batch);
        let bytes = Bytes::from(azure_batch);
        headers.insert_header(CONTENT_LENGTH, bytes.len())?;
        let content_type = format!("multipart/mixed; boundary={}", batch_id);
        headers.insert_header(CONTENT_TYPE, content_type.as_str())?;
        session
            .req_header_mut()
            .insert_header(CONTENT_TYPE, content_type)?;
        self.batch_delete.lock().await.insert(batch_id, bytes);
        ctx.delete_request = Some(aws_request);
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
        if headers.method == Method::PUT {
            headers.remove_header(&LAST_MODIFIED);
            headers.remove_header("Content-MD5");
            if let Some(copy) = headers.headers.get("x-amz-copy-source").cloned() {
                let source = copy.to_str()?;
                let url = if source.starts_with("/") {
                    format!("{}{}", self.endpoint, source)
                } else {
                    format!("{}/{}", self.endpoint, source)
                };
                headers.insert_header("x-ms-copy-source", url)?;
            }
        }
        headers.remove_header(&IF_MATCH);
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
        if headers.method == Method::PUT
            && !ctx.request_query_params.contains_key("uploadId")
        {
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
        ctx: &mut S3ProxyContext,
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

        // handle CopyObject
        // since azure does not return body, we have to do it instead of response_body_filter
        if session.req_header().method == Method::PUT
            && session
                .req_header()
                .headers
                .contains_key("x-amz-copy-source")
        {
            self.rewrite_request_headers(session.req_header_mut())?;
            self.add_required_headers(session.req_header_mut(), &ctx, &self.host)?;
            sign(
                &url,
                session.req_header_mut(),
                self.account.as_str(),
                &self.key,
            )?;
            let resp = self
                .client
                .request(Method::PUT, url.clone())
                .headers(session.req_header().headers.clone())
                .timeout(Duration::from_secs(10))
                .send()
                .await?;
            let code = resp.status().as_u16();
            let mut resp_header = ResponseHeader::build(code, None)?;
            resp.headers().iter().try_for_each(|(k, v)| {
                resp_header.insert_header(k, v)?;
                Ok::<(), Error>(())
            })?;
            resp_header.set_reason_phrase(resp.status().canonical_reason())?;
            self.handle_response_header(ctx, &mut resp_header)?;
            let s = if resp.status().is_success() {
                // write body
                // construct aws copy-object result
                let aws_result = CopyObjectResult {
                    e_tag: resp_header
                        .headers
                        .get(ETAG)
                        .and_then(|etag| etag.to_str().ok())
                        .unwrap_or("")
                        .to_string(),
                    last_modified: ctx
                        .response_headers
                        .get(LAST_MODIFIED.as_str())
                        .and_then(|time| Some(time.as_str()))
                        .unwrap_or("")
                        .to_string(),
                };
                quick_xml::se::to_string(&aws_result)?
            } else {
                String::new()
            };
            if !s.is_empty() {
                resp_header.insert_header(CONTENT_LENGTH, s.as_bytes().len())?;
            } else {
                resp_header.insert_header(CONTENT_LENGTH, 0)?;
            }
            session
                .write_response_header(Box::new(resp_header), true)
                .await?;
            if resp.status().is_success() {
                // write body
                debug!("Converted azure copy object results to aws {:?}", s);
                let bytes = Bytes::from(s);
                session.write_response_body(Some(bytes), true).await?;
            }
            return Ok(true);
        }

        // handle CompleteMultipartUpload
        // Azure does not send body in response for this request
        // so we have to intercept it and request by ourselves
        if session.req_header().method == Method::POST
            && ctx.request_query_params.contains_key("uploadId")
        {
            let body = session
                .read_request_body()
                .await?
                .ok_or(anyhow!("no body found in complete multipart request"))?;
            let request_body = String::from_utf8(body.to_vec())?;
            let aws_request: CompleteMultipartUpload =
                quick_xml::de::from_str(&request_body)?;
            let azure_req_body = self
                .convert_complete_multipart_query(
                    &url,
                    session.req_header_mut(),
                    &ctx.request_query_params,
                    &aws_request,
                )
                .await?;
            self.rewrite_request_headers(session.req_header_mut())?;
            self.add_required_headers(session.req_header_mut(), ctx, &self.host)?;
            let s = format!(
                "{}{}",
                self.endpoint,
                session.req_header_mut().uri.to_string()
            );
            let url = Url::parse(s.as_str())?;
            let path = url.path();
            // starting from second path segment is the key
            let key = &path[ctx.bucket.len() + 2..];
            sign(
                &url,
                session.req_header_mut(),
                self.account.as_str(),
                &self.key,
            )?;
            let resp = self
                .client
                .request(Method::PUT, url.clone())
                .headers(session.req_header().headers.clone())
                .body(azure_req_body)
                .timeout(Duration::from_secs(10))
                .send()
                .await?;
            let code = resp.status().as_u16();
            let mut resp_header = ResponseHeader::build(code, None)?;
            resp.headers().iter().try_for_each(|(k, v)| {
                resp_header.insert_header(k, v)?;
                Ok::<(), Error>(())
            })?;
            resp_header.set_reason_phrase(resp.status().canonical_reason())?;
            self.handle_response_header(ctx, &mut resp_header)?;
            let s = if resp.status().is_success() {
                // build aws response body
                let aws_result = CompleteMultipartUploadResult {
                    location: "".to_string(),
                    bucket: ctx.bucket.clone(),
                    key: key.to_string(),
                    e_tag: resp_header
                        .headers
                        .get(ETAG)
                        .and_then(|etag| etag.to_str().ok())
                        .unwrap_or("")
                        .to_string(),
                };
                quick_xml::se::to_string(&aws_result)?
            } else {
                String::new()
            };

            if !s.is_empty() {
                resp_header.insert_header(CONTENT_LENGTH, s.as_bytes().len())?;
            } else {
                resp_header.insert_header(CONTENT_LENGTH, 0)?;
            }
            session
                .write_response_header(Box::new(resp_header), true)
                .await?;
            if resp.status().is_success() {
                // write body
                debug!(
                    "Converted azure complete multipart upload results to aws {:?}",
                    s
                );
                let bytes = Bytes::from(s);
                session.write_response_body(Some(bytes), true).await?;
            }
            return Ok(true);
        }

        // handle GetBucketAcl
        if session.req_header().method == Method::GET
            && ctx.request_query_params.contains_key("acl")
        {
            let aws_result = AccessControlPolicy {
                owner: Owner {
                    display_name: "lakesoul".to_string(),
                    id: "lakesoul".to_string(),
                },
                access_control: AccessControlList { no_use: vec![] },
            };
            let bytes = quick_xml::se::to_string(&aws_result)?.as_bytes().to_vec();
            let mut resp_header = ResponseHeader::build(200, None)?;
            resp_header.insert_header(CONTENT_TYPE, "text/plain")?;
            resp_header.insert_header(CONTENT_LENGTH, bytes.len())?;
            session
                .write_response_header(Box::new(resp_header), true)
                .await?;
            session
                .write_response_body(Some(Bytes::from(bytes)), true)
                .await?;
            return Ok(true);
        }

        Ok(false)
    }

    async fn change_upstream_header(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut S3ProxyContext,
    ) -> std::result::Result<(), Error> {
        // handle query rewrite, change headers, and signing
        let s = format!("{}{}", self.endpoint, session.req_header().uri.to_string());
        let url = Url::parse(s.as_str())?;
        self.process_request_headers(&url, ctx, session, upstream_request)
            .await?;
        Ok(())
    }

    fn handle_response_header(
        &self,
        ctx: &mut S3ProxyContext,
        headers: &mut ResponseHeader,
    ) -> std::result::Result<(), Error> {
        // if md5 presents in response header, use decoded md5 to replace etag header
        if let Some(md5) = headers.headers.get(&CONTENT_MD5) {
            let md5 = BASE64_STANDARD.decode(md5.as_bytes())?;
            let hex_md5 = hex::encode(md5);
            headers.insert_header(ETAG, hex_md5)?;
        } else if let Some(md5) = headers.headers.get("x-ms-blob-content-md5") {
            let md5 = BASE64_STANDARD.decode(md5.as_bytes())?;
            let hex_md5 = hex::encode(md5);
            headers.insert_header(ETAG, hex_md5)?;
        } else {
            // no md5 but etag only. remove double quotes and starting "0x"
            // and pad string length to nearest power of 2 (required by AWS Java SDK)
            if let Some(etag) = headers.headers.get(ETAG) {
                let new_etag = etag.to_str()?.trim_matches('"').trim_start_matches("0x");
                let padding_len = new_etag.len().next_power_of_two();
                let new_etag = new_etag.pad_to_width_with_char(padding_len, '0');
                ctx.response_headers
                    .insert(ETAG.to_string(), new_etag.to_string());
                headers.insert_header(ETAG, HeaderValue::try_from(new_etag)?)?;
            }
        }
        headers.headers.get(CONTENT_TYPE).iter().try_for_each(|h| {
            ctx.response_headers
                .insert(CONTENT_TYPE.to_string(), h.to_str()?.to_string());
            Ok::<_, Error>(())
        })?;
        headers
            .headers
            .get(LAST_MODIFIED)
            .iter()
            .try_for_each(|h| {
                let time = h.to_str()?;
                ctx.response_headers
                    .insert(LAST_MODIFIED.to_string(), convert_timestamp(time)?);
                Ok::<_, Error>(())
            })?;
        if ctx.require_response_body_rewrite {
            headers.remove_header(&CONTENT_LENGTH);
            headers.insert_header(TRANSFER_ENCODING, "Chunked")?;
        }
        if ctx.request_method == Method::DELETE {
            // for delete request, we just ignore 404
            if headers.status == StatusCode::NOT_FOUND {
                headers.set_status(StatusCode::ACCEPTED)?;
            }
        }
        debug!("Response headers rewritten {:?}", headers);
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
        // rewrite request body for DeleteObjects
        headers.method == Method::POST && ctx.request_query_params.contains_key("delete")
    }

    fn require_response_body_rewrite(
        &self,
        ctx: &S3ProxyContext,
        headers: &RequestHeader,
    ) -> bool {
        (headers.method == Method::GET
            && ctx.request_query_params.contains_key("list-type"))
            || (headers.method == Method::POST
                && ctx.request_query_params.contains_key("delete"))
    }

    async fn rewrite_request_body(
        &self,
        headers: &RequestHeader,
        ctx: &mut S3ProxyContext,
        body: &mut Option<Bytes>,
    ) -> std::result::Result<(), Error> {
        if ctx.request_query_params.contains_key("delete") {
            let request_body = String::from_utf8(std::mem::take(&mut ctx.request_body))?;
            debug!("rewrite request_body for batch delete {:?}", request_body);
            let batch_id = headers
                .headers
                .get(CONTENT_TYPE)
                .and_then(|v| {
                    v.to_str().ok().and_then(|s| {
                        if s.starts_with("multipart/mixed; boundary=batch_")
                            && s.len() == 68
                        {
                            Some(&s[26..])
                        } else {
                            None
                        }
                    })
                })
                .ok_or(anyhow!(
                    "cannot parse batch id name from headers: {headers:?}"
                ))?;
            let azure_batch =
                self.batch_delete
                    .lock()
                    .await
                    .remove(batch_id)
                    .ok_or(anyhow!(
                        "cannot find delete batch id in context {:?}",
                        batch_id
                    ))?;
            debug!(
                "rewrite request_body for batch delete {:?}",
                String::from_utf8_lossy(azure_batch.as_ref())
            );
            *body = Some(Bytes::from(azure_batch));
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
                            last_modified: convert_timestamp(
                                take_string_from_map(
                                    &mut blob.properties,
                                    "Last-Modified",
                                )
                                .as_str(),
                            )?,
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
        } else if headers.method == Method::POST
            && ctx.request_query_params.contains_key("delete")
        {
            let aws_req = ctx
                .delete_request
                .take()
                .ok_or(anyhow!("cannot find aws delete request"))?;
            let response_body =
                String::from_utf8(std::mem::take(&mut ctx.response_body))?;
            let batch_id = ctx
                .response_headers
                .get(CONTENT_TYPE.as_str())
                .and_then(|s| {
                    if s.starts_with("multipart/mixed; boundary=batch") {
                        Some(&s[26..])
                    } else {
                        None
                    }
                })
                .ok_or(anyhow!("cannot find response header content-type"))?;
            let split = response_body
                .split(batch_id)
                .filter(|b| {
                    let s = b.trim_matches(&['-', '\r', '\n', ' ']);
                    !s.is_empty() && s.contains("HTTP/1.1")
                })
                .collect::<Vec<_>>();
            if split.len() != aws_req.object.len() {
                return Err(anyhow!(
                    "batch delete has requests {:?}, response {:?}, size not matched",
                    aws_req.object,
                    split
                ));
            }
            let mut aws_result = DeleteResult {
                deleted: vec![],
                error: vec![],
            };
            let re = Regex::new(r"HTTP/1.1\s+(\d+) (.*)\r\n")?;
            aws_req
                .object
                .into_iter()
                .zip(split.into_iter())
                .try_for_each(|(mut obj, split)| {
                    let key = std::mem::take(&mut obj.key);
                    let caps = re.captures(&split).ok_or(anyhow!(
                        "cannot capture response split pattern {:?}",
                        split
                    ))?;
                    let code = caps
                        .get(1)
                        .ok_or(anyhow!("cannot find http code from {}", split))?
                        .as_str();
                    let reason = caps
                        .get(2)
                        .ok_or(anyhow!("cannot find http reason from {}", split))?
                        .as_str();
                    let code: u16 = code.parse()?;
                    match code {
                        202 => {
                            aws_result.deleted.push(Deleted { key });
                        }
                        400 => {
                            aws_result.error.push(DeleteError {
                                key,
                                code: "InvalidArgument".to_string(),
                                message: "Invalid Argument".to_string(),
                            });
                        }
                        403 => {
                            aws_result.error.push(DeleteError {
                                key,
                                code: "AccessDenied".to_string(),
                                message: "Access Denied".to_string(),
                            });
                        }
                        404 => {
                            aws_result.error.push(DeleteError {
                                key,
                                code: "NoSuchKey".to_string(),
                                message: "The specified key does not exist.".to_string(),
                            });
                        }
                        _ => {
                            let status = StatusCode::from_u16(code)?;
                            aws_result.error.push(DeleteError {
                                key,
                                code: status.to_string(),
                                message: reason.to_string(),
                            });
                        }
                    }
                    Ok::<(), Error>(())
                })?;
            let s = quick_xml::se::to_string(&aws_result)?;
            debug!(
                "Converted azure batch delete results to aws {:?}",
                aws_result
            );
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

fn convert_timestamp(input: &str) -> Result<String, Error> {
    let date = DateTime::parse_from_rfc2822(input)?;
    Ok(date.to_rfc3339_opts(SecondsFormat::Millis, true))
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
    pub delimiter: Option<String>,
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
    debug!("client string to sign: {}", str_to_sign);
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
            delimiter: Some("/".to_string()),
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
