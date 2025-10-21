// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::context::S3ProxyContext;
use bytes::Bytes;
use http::Uri;
use pingora::http::ResponseHeader;
use pingora::prelude::{RequestHeader, Session};
use std::str::FromStr;

#[async_trait::async_trait]
pub trait HTTPHandler: Send {
    async fn handle_request_header(
        &self,
        session: &mut Session,
        ctx: &S3ProxyContext,
    ) -> Result<bool, anyhow::Error>;

    async fn change_upstream_header(
        &self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut S3ProxyContext,
    ) -> Result<(), anyhow::Error>;

    fn handle_response_header(
        &self,
        ctx: &mut S3ProxyContext,
        headers: &mut ResponseHeader,
    ) -> Result<(), anyhow::Error>;

    async fn refresh_identity(&self) -> Result<(), anyhow::Error>;

    fn get_endpoint(&self) -> String;

    fn require_request_body_rewrite(
        &self,
        ctx: &S3ProxyContext,
        headers: &RequestHeader,
    ) -> bool;

    fn require_response_body_rewrite(
        &self,
        ctx: &S3ProxyContext,
        headers: &RequestHeader,
    ) -> bool;

    async fn rewrite_request_body(
        &self,
        headers: &RequestHeader,
        ctx: &mut S3ProxyContext,
        body: &mut Option<Bytes>,
    ) -> Result<(), anyhow::Error>;

    fn rewrite_response_body(
        &self,
        headers: &RequestHeader,
        ctx: &mut S3ProxyContext,
        body: &mut Option<Bytes>,
    ) -> Result<(), anyhow::Error>;
}

pub fn parse_host_port(endpoint: &str) -> Result<(String, u16), anyhow::Error> {
    let uri = Uri::from_str(endpoint)?;
    let tls: bool = if let Some(scheme) = uri.scheme_str() {
        scheme == "https"
    } else {
        false
    };
    let host = uri.host().ok_or(anyhow::anyhow!("Missing host"))?;
    let port = if let Some(port) = uri.port() {
        port.as_u16()
    } else if tls {
        443
    } else {
        80
    };
    Ok((host.to_string(), port))
}
