use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claims {
    pub sub: String,
    pub group: String,
    pub exp: usize,
}

pub(crate) struct JwtServer {
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

    pub(crate) fn create_token(&self, claims: Claims) -> Result<String, jsonwebtoken::errors::Error> {
        encode(
            &Header::default(),
            &claims,
            &self.encoding_key,
        )
    }

    pub(crate) fn decode_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        let data = decode::<Claims>(
            &token,
            &self.decoding_key,
            &Validation::default(),
        )?;
        Ok(data.claims)
    }
}

