// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

pub struct S3ProxyContext {
    pub bucket: String,
    pub request_body: Vec<u8>,
    pub response_body: Vec<u8>,
    pub request_query_params: HashMap<String, String>,
    pub require_request_body_rewrite: bool,
    pub require_response_body_rewrite: bool,
}

impl S3ProxyContext {
    pub fn new() -> Self {
        Self {
            bucket: String::new(),
            response_body: vec![],
            request_body: vec![],
            request_query_params: HashMap::new(),
            require_request_body_rewrite: false,
            require_response_body_rewrite: false,
        }
    }
}
