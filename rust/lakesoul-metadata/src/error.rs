// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, num, result,
    sync::Arc,
};

/// Result type for operations that could result in an [LakeSoulMetaDataError]
pub type Result<T, E = LakeSoulMetaDataError> = result::Result<T, E>;

/// Result type for operations that could result in an [LakeSoulMetaDataError] and needs to be shared (wrapped into `Arc`).
pub type SharedResult<T> = result::Result<T, Arc<LakeSoulMetaDataError>>;

/// Error type for generic operations that could result in LakeSoulMetaDataError::External
pub type GenericError = Box<dyn Error + Send + Sync>;

/// DataFusion error
#[derive(Debug)]
pub enum LakeSoulMetaDataError {
    PostgresError(tokio_postgres::Error),
    IoError(io::Error),
    SerdeJsonError(serde_json::Error),
    ParseIntError(num::ParseIntError),
    UuidError(uuid::Error),
    ProstDecodeError(prost::DecodeError),
    ProstEncodeError(prost::EncodeError),
    Other(GenericError),
    Internal(String),
}

impl From<tokio_postgres::Error> for LakeSoulMetaDataError {
    fn from(err: tokio_postgres::Error) -> Self {
        Self::PostgresError(err)
    }
}

impl From<io::Error> for LakeSoulMetaDataError {
    fn from(err: io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<io::ErrorKind> for LakeSoulMetaDataError {
    fn from(kind: io::ErrorKind) -> Self {
        Self::from(io::Error::from(kind))
    }
}

impl From<serde_json::Error> for LakeSoulMetaDataError {
    fn from(err: serde_json::Error) -> Self {
        Self::SerdeJsonError(err)
    }
}

impl From<num::ParseIntError> for LakeSoulMetaDataError {
    fn from(err: num::ParseIntError) -> Self {
        Self::ParseIntError(err)
    }
}

impl From<uuid::Error> for LakeSoulMetaDataError {
    fn from(err: uuid::Error) -> Self {
        Self::UuidError(err)
    }
}

impl From<prost::DecodeError> for LakeSoulMetaDataError {
    fn from(err: prost::DecodeError) -> Self {
        Self::ProstDecodeError(err)
    }
}

impl From<prost::EncodeError> for LakeSoulMetaDataError {
    fn from(err: prost::EncodeError) -> Self {
        Self::ProstEncodeError(err)
    }
}

impl From<GenericError> for LakeSoulMetaDataError {
    fn from(err: GenericError) -> Self {
        LakeSoulMetaDataError::Other(err)
    }
}
impl Display for LakeSoulMetaDataError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            LakeSoulMetaDataError::PostgresError(ref desc) => write!(f, "postgres error: {desc}"),
            LakeSoulMetaDataError::IoError(ref desc) => write!(f, "IO error: {desc}"),
            LakeSoulMetaDataError::SerdeJsonError(ref desc) => write!(f, "serde_json error: {desc}"),
            LakeSoulMetaDataError::ParseIntError(ref desc) => write!(f, "parse_int error: {desc}"),
            LakeSoulMetaDataError::UuidError(ref desc) => write!(f, "uuid error: {desc}"),
            LakeSoulMetaDataError::ProstEncodeError(ref desc) => write!(f, "prost encode error: {desc}"),
            LakeSoulMetaDataError::ProstDecodeError(ref desc) => write!(f, "prost decode error: {desc}"),
            LakeSoulMetaDataError::Other(ref desc) => {
                write!(f, "Other error: {desc}")
            }
            LakeSoulMetaDataError::Internal(ref desc) => {
                write!(
                    f,
                    "Internal error: {desc}.\nThis was likely caused by a bug in LakeSoul's \
                    code and we would welcome that you file an bug report in our issue tracker"
                )
            }
        }
    }
}

impl Error for LakeSoulMetaDataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LakeSoulMetaDataError::PostgresError(e) => Some(e),
            LakeSoulMetaDataError::IoError(e) => Some(e),
            LakeSoulMetaDataError::SerdeJsonError(e) => Some(e),
            LakeSoulMetaDataError::ParseIntError(e) => Some(e),
            LakeSoulMetaDataError::UuidError(e) => Some(e),
            LakeSoulMetaDataError::ProstDecodeError(e) => Some(e),
            LakeSoulMetaDataError::ProstEncodeError(e) => Some(e),
            LakeSoulMetaDataError::Other(e) => Some(e.as_ref()),
            LakeSoulMetaDataError::Internal(_) => None,
        }
    }
}

impl From<LakeSoulMetaDataError> for io::Error {
    fn from(e: LakeSoulMetaDataError) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}
