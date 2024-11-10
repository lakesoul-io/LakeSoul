// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{io, num, result, sync::Arc};
use bb8_postgres::bb8::RunError;
use thiserror::Error;

/// Result type for operations that could result in an [LakeSoulMetaDataError]
pub type Result<T, E = LakeSoulMetaDataError> = result::Result<T, E>;

/// Result type for operations that could result in an [LakeSoulMetaDataError] and needs to be shared (wrapped into `Arc`).
pub type SharedResult<T> = result::Result<T, Arc<LakeSoulMetaDataError>>;

/// Error type for generic operations that could result in LakeSoulMetaDataError::External
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

/// DataFusion error
#[derive(Error, Debug)]
pub enum LakeSoulMetaDataError {
    #[error("postgres error: {0}")]
    PostgresError(#[from] tokio_postgres::Error),
    #[error("postgres pool error: {0}")]
    PostgresPoolError(#[from] RunError<tokio_postgres::Error>),
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("serde_json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("parse_int error: {0}")]
    ParseIntError(#[from] num::ParseIntError),
    #[error("parse_url error: {0}")]
    ParseUrlError(#[from] url::ParseError),
    #[error("uuid error: {0}")]
    UuidError(#[from] uuid::Error),
    #[error("prost decode error: {0}")]
    ProstDecodeError(#[from] prost::DecodeError),
    #[error("prost encode error: {0}")]
    ProstEncodeError(#[from] prost::EncodeError),
    #[error(
        "Internal error: {0}\nThis was likely caused by a bug in LakeSoul's \
    code and we would welcome that you file an bug report in our issue tracker"
    )]
    Internal(String),
    #[error("Not found error: {0}")]
    NotFound(String),
    #[error("Other error: {0}")]
    Other(#[from] GenericError),
}

impl From<io::ErrorKind> for LakeSoulMetaDataError {
    fn from(kind: io::ErrorKind) -> Self {
        Self::from(io::Error::from(kind))
    }
}
