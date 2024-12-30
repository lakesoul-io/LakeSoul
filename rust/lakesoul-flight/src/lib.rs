mod flight_sql_server;
mod jwt;
mod rbac;
pub mod args;

pub use flight_sql_server::FlightSqlServiceImpl;
use datafusion::error::DataFusionError;
pub use jwt::{JwtServer, Claims};

use lakesoul_metadata::LakeSoulMetaDataError;
use lakesoul_datafusion::LakeSoulError;
use tonic::Status;
use log::error;

macro_rules! impl_error_to_status {
    ($name:ident, $err_type:ty, $status_type:ident) => {
        fn $name(err: $err_type) -> Status {
            error!("Converting error to status: {:?}", err);
            Status::$status_type(format!("{err:?}"))
        }
    }
}

impl_error_to_status!(decode_error_to_status, prost::DecodeError, invalid_argument);
impl_error_to_status!(arrow_error_to_status, arrow::error::ArrowError, internal);
impl_error_to_status!(lakesoul_error_to_status, LakeSoulError, internal);
impl_error_to_status!(datafusion_error_to_status, DataFusionError, internal);
impl_error_to_status!(lakesoul_metadata_error_to_status, LakeSoulMetaDataError, internal);
