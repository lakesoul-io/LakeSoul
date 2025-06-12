// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Claims {
    pub sub: String,
    pub group: String,
    pub exp: usize,
}

pub struct JwtServer {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
}

impl JwtServer {
    pub fn new(secret: &str) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
        }
    }

    pub fn create_token(
        &self,
        claims: &Claims,
    ) -> Result<String, jsonwebtoken::errors::Error> {
        info!("claims is: {:?}", claims);
        encode(&Header::default(), &claims, &self.encoding_key)
    }

    pub fn decode_token(
        &self,
        token: &str,
    ) -> Result<Claims, jsonwebtoken::errors::Error> {
        info!("token is: {}", token);
        let data = decode::<Claims>(token, &self.decoding_key, &Validation::default())?;
        Ok(data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwt::JwtServer;
    use chrono::Days;
    use lakesoul_metadata::MetaDataClient;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_verify_token() {
        let metadata_client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let jwt_server = JwtServer::new(metadata_client.get_client_secret().as_str());
        {
            // basic
            let claims = Claims {
                sub: "lake-iam-001".to_string(),
                group: "lake-czods".to_string(),
                exp: chrono::Utc::now()
                    .checked_add_days(Days::new(1))
                    .unwrap()
                    .timestamp() as usize,
            };
            let token = jwt_server.create_token(&claims).unwrap();
            let decoded_claims = jwt_server.decode_token(token.as_str()).unwrap();
            assert_eq!(claims, decoded_claims);
        }
        {
            // expired
            let claims = Claims {
                sub: "lake-iam-001".to_string(),
                group: "lake-czods".to_string(),
                exp: 123412341,
            };
            let token = jwt_server.create_token(&claims).unwrap();
            let decoded_claims = jwt_server.decode_token(token.as_str());
            assert!(decoded_claims.is_err());
        }
    }
}
