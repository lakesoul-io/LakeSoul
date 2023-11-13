// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{error::Error, sync::Arc, result, fmt::Display};

use lakesoul_io::lakesoul_reader::DataFusionError;
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

impl Display for LakeSoulError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            LakeSoulError::MetaDataError(ref desc) => write!(f, "metadata error: {desc}"),
            LakeSoulError::DataFusionError(ref desc) => write!(f, "DataFusion error: {desc}"),
        }
    }
}

impl Error for LakeSoulError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LakeSoulError::MetaDataError(e) => Some(e),
            LakeSoulError::DataFusionError(e) => Some(e),
        }
    }
}