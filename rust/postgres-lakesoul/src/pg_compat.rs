// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shims over the upstream `pg_catalog` functions.
//!
//! Real PostgreSQL declares `pg_relation_is_publishable(regclass)`, and psql
//! sends the relation oid as a *quoted literal*:
//!
//! ```sql
//! WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16495')
//! ```
//!
//! The upstream catalog registers the function for `Int32` only, so that call
//! fails with `type_coercion` and `\d <table>` cannot work from psql. LakeSoul
//! has no publications, so the shim accepts both the integer oid and the
//! quoted form, answering `true` for every row like the upstream
//! implementation does.
//!
//! The shim is registered *after* the upstream `pg_catalog` setup: the
//! function registry is keyed by name, so this registration replaces the
//! oid-only signature instead of coexisting with it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, LargeStringArray, StringArray,
    StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

/// Registers the shims into a connection-local session.
pub fn register_pg_catalog_shims(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(PgRelationIsPublishable));
    context.register_udf(ScalarUDF::from(ToRegtype));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgRelationIsPublishable;

impl ScalarUDFImpl for PgRelationIsPublishable {
    fn name(&self) -> &str {
        "pg_relation_is_publishable"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Utf8]),
                ],
                Volatility::Stable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; args.number_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

/// PostgreSQL's `to_regtype(text)`: resolve a type name to its OID.
///
/// Drivers use it to discover extension types before registering their
/// adapters - `pgvector`'s `psycopg` integration issues
/// `... WHERE t.oid = to_regtype('vector')` from `TypeInfo.fetch`.  DataFusion
/// has no `regtype` function for calls like that, so this shim resolves the
/// names the pgvector clients ask for (`vector`, `bit`, ...) plus the common
/// built-in types, returning `NULL` for unknown names exactly like PostgreSQL
/// does for `to_regtype`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToRegtype;

/// Type-name to OID map for [`ToRegtype`].
///
/// Values are PostgreSQL's built-in type OIDs; `vector` is the OID
/// `datafusion-postgres` advertises in `pg_catalog.pg_type`
/// (`PG_VECTOR_TYPE_OID`).  Unknown names return `NULL` instead of searching
/// `pg_type`: this shim never runs SQL, and the drivers only ask for the types
/// they know how to decode.
static REGTYPE_OIDS: LazyLock<HashMap<&'static str, i32>> = LazyLock::new(|| {
    let mut oids: HashMap<&'static str, i32> = HashMap::from([
        ("bool", 16),
        ("boolean", 16),
        ("bytea", 17),
        ("char", 18),
        ("name", 19),
        ("int8", 20),
        ("bigint", 20),
        ("int2", 21),
        ("smallint", 21),
        ("int4", 23),
        ("int", 23),
        ("integer", 23),
        ("text", 25),
        ("oid", 26),
        ("json", 114),
        ("float4", 700),
        ("real", 700),
        ("float8", 701),
        ("double precision", 701),
        ("varchar", 1043),
        ("character varying", 1043),
        ("date", 1082),
        ("time", 1083),
        ("timestamp", 1114),
        ("timestamp without time zone", 1114),
        ("timestamptz", 1184),
        ("timestamp with time zone", 1184),
        ("interval", 1186),
        ("timetz", 1266),
        ("bit", 1560),
        ("varbit", 1562),
        ("bit varying", 1562),
        ("numeric", 1700),
        ("decimal", 1700),
        ("uuid", 2950),
        ("jsonb", 3802),
    ]);
    oids.insert(
        "vector",
        datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID as i32,
    );
    oids
});

/// Normalize a type name the way PostgreSQL resolves unquoted identifiers:
/// lowercase and ignore whitespace; strip a `pg_catalog.` qualifier and
/// surrounding double quotes.
fn normalize_type_name(name: &str) -> String {
    let trimmed = name.trim().trim_matches('"').to_lowercase();
    let trimmed = trimmed.strip_prefix("pg_catalog.").unwrap_or(&trimmed);
    trimmed.trim().trim_matches('"').to_string()
}

impl ScalarUDFImpl for ToRegtype {
    fn name(&self) -> &str {
        "to_regtype"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Null]),
                ],
                Volatility::Stable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // PostgreSQL's `regtype` is an oid; `pg_type.oid` is Arrow Int32 in
        // the catalog, so a plain Int32 compares and displays correctly here.
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let argument = args
            .args
            .first()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "to_regtype expects one argument".to_string(),
                )
            })?
            .clone();
        let values: Vec<Option<i32>> = match &argument {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Utf8(Some(name))
                | ScalarValue::LargeUtf8(Some(name))
                | ScalarValue::Utf8View(Some(name)) => vec![resolve(name)],
                ScalarValue::Null
                | ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::Utf8View(None) => vec![None],
                other => {
                    return Err(datafusion::error::DataFusionError::Plan(format!(
                        "to_regtype expects text, got {other:?}"
                    )));
                }
            },
            ColumnarValue::Array(array) => {
                let array = array.clone();
                if array.data_type() == &DataType::Null {
                    vec![None; args.number_rows]
                } else {
                    let names = downcast_strings(array.as_ref())?;
                    (0..names.len())
                        .map(|index| names[index].as_deref().and_then(resolve))
                        .collect()
                }
            }
        };
        Ok(ColumnarValue::Array(Arc::new(Int32Array::from(values))))
    }
}

/// Resolve one type name to its OID via [`REGTYPE_OIDS`].
fn resolve(name: &str) -> Option<i32> {
    REGTYPE_OIDS
        .get(normalize_type_name(name).as_str())
        .copied()
}

/// Borrow the UTF-8 values of any Arrow string array `to_regtype` accepts.
fn downcast_strings(array: &dyn Array) -> DFResult<Vec<Option<String>>> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((0..values.len())
            .map(|index| {
                (!values.is_null(index)).then(|| values.value(index).to_string())
            })
            .collect());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((0..values.len())
            .map(|index| {
                (!values.is_null(index)).then(|| values.value(index).to_string())
            })
            .collect());
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok((0..values.len())
            .map(|index| {
                (!values.is_null(index)).then(|| values.value(index).to_string())
            })
            .collect());
    }
    Err(datafusion::error::DataFusionError::Plan(format!(
        "to_regtype expects text, got {:?}",
        array.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_pgvector_and_builtin_type_names() {
        assert_eq!(
            resolve("vector"),
            Some(datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID as i32)
        );
        assert_eq!(resolve("VECTOR"), resolve("vector"));
        assert_eq!(resolve(" pg_catalog.vector "), resolve("vector"));
        assert_eq!(resolve("bit"), Some(1560));
        assert_eq!(resolve("int4"), Some(23));
        assert_eq!(resolve("no_such_type"), None);
    }
}
