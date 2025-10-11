// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use http::Uri;
use pingora::prelude::RequestHeader;
use std::str::FromStr;

#[async_trait::async_trait]
pub trait HTTPHandler: Send {
    fn handle_request_header(
        &self,
        headers: &mut RequestHeader,
        bucket: &str,
    ) -> Result<(), anyhow::Error>;

    async fn refresh_identity(&self) -> Result<(), anyhow::Error>;

    fn get_endpoint(&self) -> String;
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
