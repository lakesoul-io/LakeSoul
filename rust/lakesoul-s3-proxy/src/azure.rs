// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::aws::{CommonPrefixes, Contents, ListBucketResult};
use crate::context::S3ProxyContext;
use crate::handler::{HTTPHandler, parse_host_port};
use anyhow::Error;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use chrono::Utc;
use http::header::{
    AUTHORIZATION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE,
    DATE, HOST, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, RANGE,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use pingora::Result;
use pingora::http::RequestHeader;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::Mutex;
use tracing::log::{debug, info};
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
        // re-construct url
        let s = format!("{}{}", self.endpoint, headers.uri.to_string());
        let url = Url::parse(s.as_str())?;
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

    fn require_request_body_rewrite(
        &self,
        ctx: &S3ProxyContext,
        headers: &RequestHeader,
    ) -> bool {
        if ctx.request_query_params.contains_key("uploadId") {
            return true;
        }
        return false;
    }

    fn require_response_body_rewrite(
        &self,
        ctx: &S3ProxyContext,
        headers: &RequestHeader,
    ) -> bool {
        headers.method == Method::GET
            && ctx.request_query_params.contains_key("list-type")
    }

    fn rewrite_request_body(
        &self,
        headers: &RequestHeader,
        ctx: &mut S3ProxyContext,
        body: &mut Option<Bytes>,
    ) -> std::result::Result<(), Error> {
        todo!()
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
                .for_each(|mut blob| match blob {
                    BlobOrPrefix::Blob(mut blob) => {
                        aws_result.contents.push(Contents {
                            key: std::mem::take(&mut blob.name),
                            last_modified: take_string_from_map(
                                &mut blob.properties,
                                "Last-Modified",
                            ),
                            etag: take_string_from_map(&mut blob.properties, "ETag"),
                            size: take_string_from_map(
                                &mut blob.properties,
                                "Content-Length",
                            ),
                            storage_class: "STANDARD".to_string(),
                        });
                    }
                    BlobOrPrefix::BlobPrefix(mut prefix) => {
                        aws_result.common_prefixes.push(CommonPrefixes {
                            prefix: std::mem::take(&mut prefix.name),
                        });
                    }
                });
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

fn rewrite_queries(url: &Url, headers: &mut RequestHeader) -> Result<(), Error> {
    if headers.method == Method::HEAD {
        let mut query = aws_smithy_http::query_writer::QueryWriter::new_from_string(
            &headers.uri.to_string(),
        )?;
        query.insert("comp", "metadata");
        headers.set_uri(query.build_uri().to_string().parse()?);
    } else {
        let query_params: HashMap<_, _> = url.query_pairs().collect();
        if query_params.contains_key("list-type") {
            convert_list_query(url, headers, &query_params)?;
        }
    }
    Ok(())
}

fn convert_list_query(
    _url: &Url,
    headers: &mut RequestHeader,
    query_params: &HashMap<Cow<str>, Cow<str>>,
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
        query_params
            .get("delimiter")
            .unwrap_or(&Cow::Borrowed("%2F")),
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

fn rewrite_request_headers(headers: &mut RequestHeader) -> Result<(), Error> {
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

#[cfg(test)]
mod tests {
    use crate::azure::{
        Blob, BlobOrPrefix, BlobPrefix, Blobs,
        EnumerationResults,
    };
    use serde::{Deserialize, Serialize};

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
        /*
        #[derive(Deserialize, Serialize)]
        enum Enum { A, B, C }

        #[derive(Deserialize, Serialize)]
        struct Outer {
            #[serde(rename = "$value")]
            a: Enum,
        }

        #[derive(Deserialize, Serialize)]
        struct AnyName {
            // <A/>, <B/> or <C/>
            #[serde(rename = "$value", default)]
            field: Vec<Enum>,
        }
        let a = AnyName { field: vec![Enum::A, Enum::B] };
        let s = quick_xml::se::to_string(&a).unwrap();
        println!("{}", s);
         */
    }
}
