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

use std::sync::Arc;
use std::sync::LazyLock;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Registers the shims into a connection-local session.
pub fn register_pg_catalog_shims(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(PgRelationIsPublishable));
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
