// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{result, sync::Arc};

use lakesoul_io::lakesoul_reader::{ArrowError, DataFusionError};
use lakesoul_metadata::error::LakeSoulMetaDataError;

/// Result type for operations that could result in an [LakeSoulMetaDataError]
pub type Result<T, E = LakeSoulError> = result::Result<T, E>;

/// Result type for operations that could result in an [LakeSoulMetaDataError] and needs to be shared (wrapped into `Arc`).
pub type SharedResult<T> = result::Result<T, Arc<LakeSoulError>>;

/// Error type for generic operations that could result in LakeSoulMetaDataError::External
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum LakeSoulError {
    #[error("metadata error: {0}")]
    MetaDataError(#[from] LakeSoulMetaDataError),
    #[error("Datafusion error: {0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("arrow error: {0}")]
    ArrowError(#[from] ArrowError),
    #[error("serde_json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("tokio error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("sys time error: {0}")]
    SysTimeError(#[from] std::time::SystemTimeError),
    // #[error("object store path error: {0}")]
    // ObjectStorePathError(#[from] object_store::path::Error),
    // #[error("object store error: {0}")]
    // ObjectStoreError(#[from] object_store::path::Error),
    #[error(
        "Internal error: {0}.\nThis was likely caused by a bug in LakeSoul's \
    code and we would welcome that you file an bug report in our issue tracker"
    )]
    Internal(String),
}
