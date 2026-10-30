use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion::{
    arrow::datatypes::DataType,
    logical_expr::{ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct PgHasPrivilegeUDF {
    signature: Signature,
    name: String,
}

impl PgHasPrivilegeUDF {
    pub(crate) fn new(name: &str) -> PgHasPrivilegeUDF {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Stable,
            ),
            name: name.to_owned(),
        }
    }

    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }
}

impl ScalarUDFImpl for PgHasPrivilegeUDF {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;

        let len = args[0].len();

        // For now, always return true (full access for current user)
        let mut builder = BooleanArray::builder(len);
        for _ in 0..len {
            builder.append_value(true);
        }
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    }
}

pub fn create_has_privilege_udf(name: &str) -> ScalarUDF {
    PgHasPrivilegeUDF::new(name).into_scalar_udf()
}
