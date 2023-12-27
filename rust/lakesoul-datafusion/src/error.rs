// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{error::Error, fmt::Display, result, sync::Arc};

use lakesoul_io::lakesoul_reader::{ArrowError, DataFusionError};
use lakesoul_metadata::error::LakeSoulMetaDataError;

/// Result type for operations that could result in an [LakeSoulMetaDataError]
pub type Result<T, E = LakeSoulError> = result::Result<T, E>;

/// Result type for operations that could result in an [LakeSoulMetaDataError] and needs to be shared (wrapped into `Arc`).
pub type SharedResult<T> = result::Result<T, Arc<LakeSoulError>>;

/// Error type for generic operations that could result in LakeSoulMetaDataError::External
pub type GenericError = Box<dyn Error + Send + Sync>;

#[derive(Debug)]
pub enum LakeSoulError {
    MetaDataError(LakeSoulMetaDataError),
    DataFusionError(DataFusionError),
    ArrowError(ArrowError),
    SerdeJsonError(serde_json::Error),
    Internal(String),
}

impl From<LakeSoulMetaDataError> for LakeSoulError {
    fn from(err: LakeSoulMetaDataError) -> Self {
        Self::MetaDataError(err)
    }
}

impl From<DataFusionError> for LakeSoulError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err)
    }
}

impl From<ArrowError> for LakeSoulError {
    fn from(err: ArrowError) -> Self {
        Self::ArrowError(err)
    }
}

impl From<serde_json::Error> for LakeSoulError {
    fn from(err: serde_json::Error) -> Self {
        Self::SerdeJsonError(err)
    }
}

impl Display for LakeSoulError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            LakeSoulError::MetaDataError(ref desc) => write!(f, "metadata error: {desc}"),
            LakeSoulError::DataFusionError(ref desc) => write!(f, "DataFusion error: {desc}"),
            LakeSoulError::SerdeJsonError(ref desc) => write!(f, "serde_json error: {desc}"),
            LakeSoulError::ArrowError(ref desc) => write!(f, "arrow error: {desc}"),
            LakeSoulError::Internal(ref desc) => {
                write!(
                    f,
                    "Internal error: {desc}.\nThis was likely caused by a bug in LakeSoul's \
                    code and we would welcome that you file an bug report in our issue tracker"
                )
            }
        }
    }
}

impl Error for LakeSoulError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LakeSoulError::MetaDataError(e) => Some(e),
            LakeSoulError::DataFusionError(e) => Some(e),
            LakeSoulError::SerdeJsonError(e) => Some(e),
            LakeSoulError::ArrowError(e) => Some(e),
            LakeSoulError::Internal(_) => None,
        }
    }
}
