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
//!
//! `version()` is also answered here. PostgreSQL clients use it for feature
//! discovery: SQLAlchemy raises during dialect initialization unless the
//! string contains `PostgreSQL <major>.<minor>`. The DataFusion built-in
//! identifies itself as `Apache DataFusion ...`, which is accurate but makes
//! the PostgreSQL dialect unusable. The compatibility string names both
//! engines and deliberately keeps a conservative PostgreSQL baseline; see the
//! compatibility page for the version policy.

use std::sync::Arc;
use std::sync::LazyLock;

use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

/// The PostgreSQL-compatible identity reported by `version()`.
///
/// Keep the PostgreSQL baseline at 14.0: clients use it to gate SQL
/// features, and LakeSoul does not implement the full PostgreSQL surface.
pub fn compatibility_version() -> &'static str {
    static VERSION: LazyLock<String> = LazyLock::new(|| {
        format!(
            "PostgreSQL 14.0 (LakeSoul {} / DataFusion {})",
            env!("CARGO_PKG_VERSION"),
            datafusion::DATAFUSION_VERSION
        )
    });
    VERSION.as_str()
}

/// Registers the shims into a connection-local session.
pub fn register_pg_catalog_shims(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(PgRelationIsPublishable));
    context.register_udf(ScalarUDF::from(PgVersion));
    context.register_udf(ScalarUDF::from(PgGetSerialSequence));
}

/// SQLAlchemy's PostgreSQL reflection asks for the sequence behind a serial
/// column. LakeSoul has no sequences or identity columns, so the honest
/// answer is NULL. Without this function the whole reflection query fails
/// before it can report the table's regular columns.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgGetSerialSequence;

impl ScalarUDFImpl for PgGetSerialSequence {
    fn name(&self) -> &str {
        "pg_get_serial_sequence"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Stable)
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgVersion;

impl ScalarUDFImpl for PgVersion {
    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![], Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            compatibility_version();
            args.number_rows
        ]));
        Ok(ColumnarValue::Array(array))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_postgresql_compatible() {
        let version = compatibility_version();
        assert!(version.starts_with("PostgreSQL 14.0 "), "{version}");
        assert!(version.contains("LakeSoul"), "{version}");
    }

    #[test]
    fn version_udf_has_no_arguments() {
        assert_eq!(PgVersion.name(), "version");
        assert_eq!(
            PgVersion.return_type(&[]).expect("version return type"),
            DataType::Utf8
        );
    }
}
