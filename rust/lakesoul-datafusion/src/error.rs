use datafusion_common::DataFusionError;
use rootcause::{Report, compat::boxed_error::IntoBoxedError};

pub(crate) fn df_external_err(rep: Report) -> DataFusionError {
    DataFusionError::External(rep.into_boxed_error())
}
