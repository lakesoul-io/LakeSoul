//! Arrow `Field` metadata helpers for marking Postgres oid / oid-alias columns.
//!
//! Postgres catalog columns like `pg_class.relnamespace` or `pg_attribute.atttypid`
//! are stored as `integer` (Arrow `Int32`) but are semantically *object
//! identifiers* that clients frequently compare against *string* literals
//! (`WHERE relnamespace = 'public'`, `WHERE atttypid = 'int4'`). DataFusion has
//! no notion of "oid"; it silently coerces such comparisons the wrong way and
//! returns empty results (see the `oid_string_coercion` analyzer rule).
//!
//! To fix that, oid / oid-alias columns are annotated here with Field metadata,
//! which survives into the logical plan schema and is readable via
//! [`datafusion::common::ExprSchema::metadata`].

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field};

/// Field metadata key whose value names the oid-alias kind of a column.
///
/// Example: a `relnamespace Int32` column carries
/// `{ "pg.oid_alias": "regnamespace" }`.
pub const OID_ALIAS_KEY: &str = "pg.oid_alias";

/// The supported oid-alias "kinds", stored as the value under [`OID_ALIAS_KEY`].
pub mod kind {
    /// A bare `oid` column. Name strings are *not* resolvable (only numeric).
    pub const OID: &str = "oid";
    /// A `regclass` column (relation oid). Name strings resolve via `pg_class`.
    pub const REGCLASS: &str = "regclass";
    /// A `regnamespace` column (namespace oid). Resolves via `pg_namespace`.
    pub const REGNAMESPACE: &str = "regnamespace";
    /// A `regtype` column (type oid). Numeric-only here (no `pg_type` table).
    pub const REGTYPE: &str = "regtype";
    /// A `regproc` column (function oid). Resolves via `pg_proc`.
    pub const REGPROC: &str = "regproc";
}

/// Every Postgres oid-alias type name this crate recognizes.
///
/// This is the **single source of truth** for:
///  * the `pg.oid_alias` metadata value stamped on Int32 catalog columns (see
///    [`oid_field`]) and embedded in the pg_catalog feather exports, and
///  * the set of oid-alias cast type names accepted by the `PgOidTypePlanner`
///    SQL type planner (so DataFusion's `sql_to_plan` maps them to int4 oids
///    carrying this metadata instead of rejecting them as unsupported types).
///
/// The oid-coercion analyzer rule then reads the metadata (on the column or
/// on the cast's target field) to resolve string comparisons the way Postgres
/// does. The
/// [`kind`] named constants anchor the subset that the dynamic catalog tables
/// use and that have name-lookup support (`regclass`/`regnamespace`/`regproc`);
/// the remainder are numeric-resolution-only.
pub const OID_ALIAS_TYPE_NAMES: &[&str] = &[
    kind::OID,
    kind::REGPROC,
    "regprocedure",
    "regoper",
    "regoperator",
    kind::REGCLASS,
    kind::REGTYPE,
    kind::REGNAMESPACE,
    "regrole",
    "regconfig",
    "regdictionary",
    "regcollation",
];

/// Build an `Int32` [`Field`] for an oid / oid-alias column, annotated with
/// metadata so the oid-coercion analyzer rule can recognize it.
///
/// `kind` should be one of the names in [`OID_ALIAS_TYPE_NAMES`] (e.g.
/// [`kind::REGNAMESPACE`]).
pub fn oid_field(name: impl Into<String>, kind: &str, nullable: bool) -> Field {
    debug_assert!(
        OID_ALIAS_TYPE_NAMES.contains(&kind),
        "unknown oid-alias kind: {kind}"
    );
    Field::new(name, DataType::Int32, nullable).with_metadata(HashMap::from([(
        OID_ALIAS_KEY.to_string(),
        kind.to_string(),
    )]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_field_carries_kind_metadata() {
        let f = oid_field("relnamespace", kind::REGNAMESPACE, false);
        assert_eq!(f.data_type(), &DataType::Int32);
        assert_eq!(
            f.metadata().get(OID_ALIAS_KEY).map(String::as_str),
            Some("regnamespace")
        );
        assert!(!f.is_nullable());

        let nullable = oid_field("reloftype", kind::REGTYPE, true);
        assert!(nullable.is_nullable());
        assert_eq!(
            nullable.metadata().get(OID_ALIAS_KEY).map(String::as_str),
            Some("regtype")
        );
    }
}
