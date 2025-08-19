// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The implementation of arrow flight for LakeSoul.

#[macro_use]
extern crate tracing;

pub mod args;
mod flight_sql_service;

use datafusion::error::DataFusionError;
pub use flight_sql_service::FlightSqlServiceImpl;

use lakesoul_datafusion::LakeSoulError;
use lakesoul_metadata::LakeSoulMetaDataError;
use tonic::Status;

mod token_codec;
pub use token_codec::TokenResponse;
mod banner;
pub use banner::BANNER;
pub(crate) use lakesoul_metadata::{Claims, JwtServer};

mod token {
    include!(concat!(env!("OUT_DIR"), "/json.token.TokenServer.rs"));
}
pub use token::token_server_client::TokenServerClient;
pub use token::token_server_server::{TokenServer, TokenServerServer};

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
impl_error_to_status!(
    lakesoul_metadata_error_to_status,
    LakeSoulMetaDataError,
    internal
);
