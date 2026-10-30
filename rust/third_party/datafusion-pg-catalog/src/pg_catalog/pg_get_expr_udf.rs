use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion::{
    arrow::datatypes::DataType,
    logical_expr::{ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct PgGetExprUDF {
    signature: Signature,
}

impl PgGetExprUDF {
    pub(crate) fn new() -> PgGetExprUDF {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::UInt32]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::UInt64]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32, DataType::Boolean]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::UInt32, DataType::Boolean]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Boolean]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::UInt64, DataType::Boolean]),
                ],
                Volatility::Stable,
            ),
        }
    }

    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }
}

impl ScalarUDFImpl for PgGetExprUDF {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn name(&self) -> &str {
        "pg_get_expr"
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let expr = &args[0];
        let _oid = &args[1];

        // For now, always return true (full access for current user)
        let mut builder = StringBuilder::new();
        for _ in 0..expr.len() {
            builder.append_value("");
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    }
}

pub fn create_pg_get_expr_udf() -> ScalarUDF {
    PgGetExprUDF::new().into_scalar_udf()
}
