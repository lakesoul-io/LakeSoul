use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, PrimitiveArray, StringBuilder},
        datatypes::{DataType, Int32Type},
    },
    common::{
        DataFusionError,
        cast::{as_int32_array, as_int64_array},
    },
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
};

pub(crate) fn format_type_impl(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let args = ColumnarValue::values_to_arrays(args)?;
    let type_oids = match &args[0].data_type() {
        DataType::Int32 => as_int32_array(&args[0])?,
        DataType::Int64 => {
            let oid_i64 = as_int64_array(&args[0])?;
            &PrimitiveArray::<Int32Type>::from_iter(oid_i64.iter().map(|v| v.map(|x| x as i32)))
        }
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Unexpected oid type in format_type: {}",
                args[0].data_type()
            )));
        }
    };

    let typemods = match &args[1].data_type() {
        DataType::Int32 => as_int32_array(&args[1])?,
        DataType::Int64 => {
            let oid_i64 = as_int64_array(&args[1])?;
            &PrimitiveArray::<Int32Type>::from_iter(oid_i64.iter().map(|v| v.map(|x| x as i32)))
        }
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Unexpected oid type in format_type: {}",
                args[1].data_type()
            )));
        }
    };

    let mut result = StringBuilder::new();

    for i in 0..type_oids.len() {
        if type_oids.is_null(i) {
            result.append_null();
            continue;
        }

        let type_oid = type_oids.value(i);
        let typemod = if typemods.is_null(i) {
            -1
        } else {
            typemods.value(i)
        };

        let formatted_type = format_postgres_type(type_oid, typemod);
        result.append_value(formatted_type);
    }

    Ok(ColumnarValue::Array(Arc::new(result.finish())))
}

/// Format PostgreSQL type based on OID and type modifier
fn format_postgres_type(type_oid: i32, typemod: i32) -> String {
    match type_oid {
        // Core types
        16 => "boolean".to_string(),
        17 => "bytea".to_string(),
        18 => "\"char\"".to_string(), // Note: quoted to distinguish from char(n)
        19 => "name".to_string(),
        20 => "bigint".to_string(),
        21 => "smallint".to_string(),
        23 => "integer".to_string(),
        24 => "regproc".to_string(),
        25 => "text".to_string(),
        26 => "oid".to_string(),
        27 => "tid".to_string(),
        28 => "xid".to_string(),
        29 => "cid".to_string(),

        // JSON types
        114 => "json".to_string(),
        3802 => "jsonb".to_string(),

        // Numeric types
        700 => "real".to_string(),
        701 => "double precision".to_string(),

        // Character types with length
        1042 => {
            // char(n)
            if typemod > 4 {
                format!("character({})", typemod - 4)
            } else {
                "character".to_string()
            }
        }
        1043 => {
            // varchar(n)
            if typemod > 4 {
                format!("character varying({})", typemod - 4)
            } else {
                "character varying".to_string()
            }
        }

        // Numeric with precision/scale
        1700 => {
            // numeric/decimal
            if typemod >= 4 {
                let precision = ((typemod - 4) >> 16) & 0xffff;
                let scale = (typemod - 4) & 0xffff;
                if scale > 0 {
                    format!("numeric({precision},{scale})")
                } else {
                    format!("numeric({precision})")
                }
            } else {
                "numeric".to_string()
            }
        }

        // Date/Time types
        1082 => "date".to_string(),
        1083 => {
            // time without time zone
            if typemod >= 0 {
                format!("time({typemod}) without time zone")
            } else {
                "time without time zone".to_string()
            }
        }
        1114 => {
            // timestamp without time zone
            if typemod >= 0 {
                format!("timestamp({typemod}) without time zone")
            } else {
                "timestamp without time zone".to_string()
            }
        }
        1184 => {
            // timestamp with time zone
            if typemod >= 0 {
                format!("timestamp({typemod}) with time zone")
            } else {
                "timestamp with time zone".to_string()
            }
        }
        1266 => {
            // time with time zone
            if typemod >= 0 {
                format!("time({typemod}) with time zone")
            } else {
                "time with time zone".to_string()
            }
        }
        1186 => "interval".to_string(),

        // Bit types
        1560 => {
            // bit
            if typemod > 0 {
                format!("bit({typemod})",)
            } else {
                "bit".to_string()
            }
        }
        1562 => {
            // bit varying
            if typemod > 0 {
                format!("bit varying({typemod})")
            } else {
                "bit varying".to_string()
            }
        }

        // UUID
        2950 => "uuid".to_string(),

        // Arrays (append [] to base type)
        oid if oid > 0 => {
            // For array types, we need to look up the base type
            // This is a simplified approach - in practice you'd query pg_types
            if let Some(base_oid) = get_array_base_type(oid) {
                format!("{}[]", format_postgres_type(base_oid, -1))
            } else {
                format!("oid({oid})")
            }
        }

        // Unknown or invalid OID
        _ => format!("oid({type_oid})"),
    }
}

/// Get base type OID for array types
/// This is a simplified mapping - in practice you'd query your pg_types table
fn get_array_base_type(array_oid: i32) -> Option<i32> {
    match array_oid {
        1000 => Some(16),   // _bool -> bool
        1001 => Some(17),   // _bytea -> bytea
        1005 => Some(21),   // _int2 -> int2
        1007 => Some(23),   // _int4 -> int4
        1016 => Some(20),   // _int8 -> int8
        1009 => Some(25),   // _text -> text
        1014 => Some(1042), // _bpchar -> bpchar
        1015 => Some(1043), // _varchar -> varchar
        1021 => Some(700),  // _float4 -> float4
        1022 => Some(701),  // _float8 -> float8
        1231 => Some(1700), // _numeric -> numeric
        1182 => Some(1082), // _date -> date
        1183 => Some(1083), // _time -> time
        1115 => Some(1114), // _timestamp -> timestamp
        1185 => Some(1184), // _timestamptz -> timestamptz
        199 => Some(114),   // _json -> json
        3807 => Some(3802), // _jsonb -> jsonb
        2951 => Some(2950), // _uuid -> uuid
        _ => None,
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct FormatTypeUDF {
    signature: Signature,
}

impl FormatTypeUDF {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int32, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                ],
                Volatility::Stable,
            ),
        }
    }

    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }
}

impl ScalarUDFImpl for FormatTypeUDF {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
    }

    fn name(&self) -> &str {
        "format_type"
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        format_type_impl(&args.args)
    }
}

pub fn create_format_type_udf() -> ScalarUDF {
    FormatTypeUDF::new().into_scalar_udf()
}
