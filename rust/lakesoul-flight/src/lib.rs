// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The implementation of arrow flight for LakeSoul.

pub mod args;
mod flight_sql_service;
mod jwt;

use datafusion::error::DataFusionError;
pub use flight_sql_service::FlightSqlServiceImpl;
pub use jwt::{Claims, JwtServer};

use lakesoul_datafusion::LakeSoulError;
use lakesoul_metadata::LakeSoulMetaDataError;
use log::error;
use tonic::Status;

macro_rules! impl_error_to_status {
    ($name:ident, $err_type:ty, $status_type:ident) => {
        fn $name(err: $err_type) -> Status {
            error!("Converting error to status: {:?}", err);
            Status::$status_type(format!("{err:?}"))
        }
    };
}

impl_error_to_status!(lakesoul_error_to_status, LakeSoulError, internal);
impl_error_to_status!(datafusion_error_to_status, DataFusionError, internal);
impl_error_to_status!(lakesoul_metadata_error_to_status, LakeSoulMetaDataError, internal);
