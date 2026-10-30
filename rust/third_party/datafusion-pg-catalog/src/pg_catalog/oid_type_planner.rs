//! A DataFusion [`TypePlanner`] that teaches the SQL planner to accept the
//! Postgres type names DataFusion otherwise rejects as "Unsupported SQL type":
//!
//! # oid-alias types
//!
//! `regclass`, `regproc`, `regtype`, `regnamespace`, `oid`, ... Each is mapped
//! to a plain `Int32` [`Field`] (oids are stored as int4) so the cast plans.
//! Name-string -> oid resolution happens earlier, at the SQL-rewrite layer
//! (`RewriteRegCastToSubquery`); column/reverse casts (`prorettype::regtype`,
//! `c.oid::regclass`) stay in the plan as int4 identity casts.
//!
//! The field is deliberately **metadata-free**. DataFusion (as of 55)
//! derives a logical cast's output field from the *source* field (keeping
//! its `pg.oid_alias` metadata), while a physical `CastExpr` whose target
//! field carries metadata reports that metadata from `return_field()`.
//! Stamping the kind on the cast target therefore makes the two layers
//! disagree -- e.g. `c.oid` is "oid"-kind but `c.oid::regclass` plans as
//! "regclass"-kind -- and trips the physical optimizers' schema-invariant
//! check ("ProjectionPushdown failed. Schema mismatch") as soon as a
//! projection containing the cast is rebuilt. See upstream
//! datafusion#22079 / datafusion#23169 for the divergence.
//!
//! This replaces the former `RemoveOidTypeCast` SQL rewrite: instead of
//! stripping the cast at the AST layer (losing the type), the type is
//! accepted up front.
//!
//! # `pg_catalog`-qualified builtins
//!
//! `pg_catalog.text`, `pg_catalog.int2`, `pg_catalog.int4`, ... sqlparser can
//! only represent a schema-qualified builtin as the catch-all `Custom` type,
//! which DataFusion rejects. This planner maps the canonical pg name to the
//! same Arrow type its unqualified builtin would produce, so casts like
//! `reloftype::pg_catalog.regtype::pg_catalog.text` plan cleanly. Arrays
//! (`pg_catalog.int2[]`) need no special handling: DataFusion recurses into
//! this planner for the array element type. This replaces the cast branch of
//! the former `RemoveQualifier` SQL rewrite.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::sql::sqlparser::ast::{DataType as SQLDataType, ObjectNamePart};

use crate::pg_catalog::oid_field::{self, OID_ALIAS_TYPE_NAMES};

/// Recognize Postgres type names DataFusion rejects and map them to Arrow
/// types/metadata at planning time.
#[derive(Debug, Default)]
pub struct PgOidTypePlanner;

impl PgOidTypePlanner {
    /// Build the int4 oid field for a given kind (`regclass`, `oid`, ...).
    ///
    /// Deliberately metadata-free: DataFusion (as of 55) resolves a logical
    /// cast's output field from the *source* field (`cast_output_field` keeps
    /// the source's `pg.oid_alias` metadata), while a physical `CastExpr`
    /// whose target field carries metadata reports that metadata verbatim
    /// from `return_field()`. Stamping the kind here therefore makes the two
    /// layers disagree (`c.oid` is "oid" but `c.oid::regclass` plans as
    /// "regclass"), which trips the physical optimizers' schema-invariant
    /// check ("ProjectionPushdown failed. Schema mismatch") as soon as a
    /// projection containing the cast is rebuilt. No code reads cast-target
    /// `pg.oid_alias` metadata anymore (the oid-coercion analyzer rule that
    /// did was replaced by `RewriteRegCastToSubquery` in the SQL layer), so
    /// the stamp is pure liability. See upstream datafusion#22079 /
    /// datafusion#23169 for the underlying logical/physical divergence.
    fn oid_field(_kind: &str) -> Arc<Field> {
        Arc::new(Field::new("", DataType::Int32, true))
    }

    /// Lowercase last identifier of a `Custom(...)` type name, if any.
    ///
    /// `regproc` -> `regproc`, `pg_catalog.regclass` -> `regclass`.
    fn custom_type_name(sql_type: &SQLDataType) -> Option<String> {
        let SQLDataType::Custom(name, args) = sql_type else {
            return None;
        };
        if !args.is_empty() {
            return None;
        }
        let last = name.0.last()?;
        let ObjectNamePart::Identifier(ident) = last else {
            return None;
        };
        Some(ident.value.to_lowercase())
    }

    /// The oid-alias kind for a SQL type, if it is one we handle.
    fn kind_for(sql_type: &SQLDataType) -> Option<String> {
        match sql_type {
            SQLDataType::Regclass => Some(oid_field::kind::REGCLASS.to_string()),
            SQLDataType::Custom(_, _) => Self::custom_type_name(sql_type)
                .filter(|n| OID_ALIAS_TYPE_NAMES.contains(&n.as_str())),
            _ => None,
        }
    }

    /// Map a `pg_catalog.<name>` custom type to the Arrow [`DataType`] of its
    /// unqualified builtin.
    ///
    /// Returns `None` unless the type is exactly a two-part
    /// `pg_catalog.<builtin>` with no type arguments, or `<name>` is not a
    /// builtin we know about (in which case it is left for DataFusion to error
    /// on, or for another planner/rule to handle). The oid-alias arm in
    /// [`TypePlanner::plan_type_field`] runs first, so oid-alias names
    /// (`pg_catalog.regclass`, ...) never reach here.
    fn pg_catalog_builtin(sql_type: &SQLDataType) -> Option<DataType> {
        let SQLDataType::Custom(name, args) = sql_type else {
            return None;
        };
        if !args.is_empty() || name.0.len() != 2 {
            return None;
        }
        let mut parts = name.0.iter().filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.as_str()),
            _ => None,
        });
        let (schema, type_name) = match (parts.next(), parts.next()) {
            (Some(s), Some(t)) => (s, t),
            _ => return None,
        };
        if !schema.eq_ignore_ascii_case("pg_catalog") {
            return None;
        }
        builtin_arrow_type(type_name)
    }

    /// Map the pgvector `vector` / `vector(n)` SQL type to an Arrow field.
    ///
    /// `vector(n)` is a fixed-dimension vector and maps to
    /// `FixedSizeList(Float32, n)`; a bare `vector` (no declared dimension)
    /// maps to `List(Float32)`. Both carry the `pg.vector` field metadata that
    /// `arrow-pg` uses to report the pgwire `vector` type and encode values in
    /// the pgvector text format.
    ///
    /// Returns `None` when the type is not a vector we recognize, so the caller
    /// can fall back to the other planners. A `vector` with an unparseable /
    /// non-positive dimension is also left untouched (DataFusion will then
    /// reject the unknown type with its own error).
    #[cfg(feature = "pgvector")]
    fn vector_field(sql_type: &SQLDataType) -> Option<Arc<Field>> {
        if !crate::sql::is_vector_type(sql_type) {
            return None;
        }
        let SQLDataType::Custom(_, modifiers) = sql_type else {
            return None;
        };

        let element = Field::new_list_field(DataType::Float32, true);
        let arrow_type = match modifiers.as_slice() {
            [] => DataType::List(Arc::new(element)),
            [dim] => {
                let dim: i32 = dim.trim().parse().ok()?;
                if dim <= 0 {
                    return None;
                }
                DataType::FixedSizeList(Arc::new(element), dim)
            }
            _ => return None,
        };

        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            arrow_pg::datatypes::PG_VECTOR_KEY.to_string(),
            "vector".to_string(),
        );
        Some(Arc::new(
            Field::new("", arrow_type, true).with_metadata(metadata),
        ))
    }
}

/// Arrow [`DataType`] for a Postgres pg_catalog builtin type name, or `None`
/// if it is not one we map (a user-defined type, or a builtin whose parameters
/// we don't model).
///
/// String types map to `Utf8`, matching DataFusion's default conversion of the
/// unqualified `text`/`varchar`/`char` variants. The
/// `map_string_types_to_utf8view` session option is not consulted because the
/// [`TypePlanner`] trait does not expose session config; if a deployment needs
/// `Utf8View` it can register an additional planner.
fn builtin_arrow_type(name: &str) -> Option<DataType> {
    use DataType::*;
    let dt = match name.to_ascii_lowercase().as_str() {
        // booleans
        "bool" | "boolean" => Boolean,
        // integers
        "int2" | "smallint" => Int16,
        "int4" | "integer" | "int" => Int32,
        "int8" | "bigint" => Int64,
        // floats
        "float4" | "real" => Float32,
        "float8" => Float64,
        // strings -> Utf8 (DataFusion's default for unqualified Text/Varchar)
        "text" | "varchar" | "bpchar" | "char" | "name" => Utf8,
        // bytes
        "bytea" => Binary,
        _ => return None,
    };
    Some(dt)
}

impl TypePlanner for PgOidTypePlanner {
    fn plan_type_field(&self, sql_type: &SQLDataType) -> Result<Option<Arc<Field>>> {
        // 0. pgvector `vector(n)` / `vector` -> FixedSizeList/List of Float32
        //    tagged with `pg.vector` metadata (see the arrow-pg contract).
        #[cfg(feature = "pgvector")]
        if let Some(field) = Self::vector_field(sql_type) {
            return Ok(Some(field));
        }
        // 1. Scalar oid-alias types (regclass, oid, ...) -> int4 with kind
        //    metadata. Arrays of these (`regtype[]`) are handled by DataFusion
        //    recursing into this planner for the element type, so no Array arm
        //    is needed here.
        if let Some(kind) = Self::kind_for(sql_type) {
            return Ok(Some(Self::oid_field(&kind)));
        }
        // 2. pg_catalog-qualified builtins (pg_catalog.text, pg_catalog.int2,
        //    ...). sqlparser represents these as `Custom`, which DataFusion
        //    otherwise rejects; map them to their builtin Arrow type. As above,
        //    `pg_catalog.int2[]` works via element-type recursion.
        if let Some(dt) = Self::pg_catalog_builtin(sql_type) {
            return Ok(Some(Arc::new(Field::new("", dt, true))));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn cast_target_type(sql: &str) -> SQLDataType {
        // SELECT 'x'::<TYPE> AS c  ->  the cast's target DataType
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let stmt = stmts.remove(0);
        use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
        let Statement::Query(q) = stmt else {
            panic!("not a query");
        };
        let SetExpr::Select(sel) = *q.body else {
            panic!("not a select");
        };
        let cast = match &sel.projection[0] {
            datafusion::sql::sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
            datafusion::sql::sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr,
            other => panic!("unexpected projection {other:?}"),
        };
        let datafusion::sql::sqlparser::ast::Expr::Cast { data_type, .. } = cast else {
            panic!("not a cast: {cast:?}");
        };
        data_type.clone()
    }

    #[test]
    fn recognizes_all_oid_alias_type_names() {
        let planner = PgOidTypePlanner;
        for t in [
            "regclass",
            "regproc",
            "regtype",
            "regnamespace",
            "oid",
            "pg_catalog.regclass",
            "pg_catalog.regtype",
            "regrole",
        ] {
            let dt = cast_target_type(&format!("SELECT 'x'::{t} AS c"));
            let field = planner.plan_type_field(&dt).unwrap().unwrap();
            assert_eq!(field.data_type(), &DataType::Int32, "{t}");
            // Deliberately metadata-free: a stamped target field makes the
            // physical cast report metadata the logical layer never produced,
            // tripping the physical optimizers' schema-invariant check (see
            // the `oid_field` doc above).
            assert!(field.metadata().is_empty(), "{t}");
        }
    }

    #[test]
    fn leaves_non_oid_types_alone() {
        let planner = PgOidTypePlanner;
        for t in ["int4", "text", "varchar", "timestamp"] {
            let dt = cast_target_type(&format!("SELECT 'x'::{t} AS c"));
            assert_eq!(planner.plan_type_field(&dt).unwrap(), None, "{t}");
        }
    }

    #[test]
    fn pg_catalog_prefix_normalizes_to_kind() {
        let planner = PgOidTypePlanner;
        // Both bare and pg_catalog-qualified names plan to the same
        // (metadata-free) int4 field.
        for sql in ["regproc", "pg_catalog.regproc"] {
            let dt = cast_target_type(&format!("SELECT 'x'::{sql} AS c"));
            let field = planner.plan_type_field(&dt).unwrap().unwrap();
            assert_eq!(field.data_type(), &DataType::Int32, "{sql}");
            assert!(field.metadata().is_empty(), "{sql}");
        }
    }

    #[test]
    fn pg_catalog_builtins_map_to_arrow_types() {
        let planner = PgOidTypePlanner;
        for (sql, expected) in [
            ("pg_catalog.text", DataType::Utf8),
            ("pg_catalog.varchar", DataType::Utf8),
            ("pg_catalog.bpchar", DataType::Utf8),
            ("pg_catalog.name", DataType::Utf8),
            ("pg_catalog.bool", DataType::Boolean),
            ("pg_catalog.boolean", DataType::Boolean),
            ("pg_catalog.int2", DataType::Int16),
            ("pg_catalog.smallint", DataType::Int16),
            ("pg_catalog.int4", DataType::Int32),
            ("pg_catalog.int8", DataType::Int64),
            ("pg_catalog.bigint", DataType::Int64),
            ("pg_catalog.float4", DataType::Float32),
            ("pg_catalog.real", DataType::Float32),
            ("pg_catalog.float8", DataType::Float64),
            ("pg_catalog.bytea", DataType::Binary),
        ] {
            let dt = cast_target_type(&format!("SELECT 'x'::{sql} AS c"));
            let field = planner
                .plan_type_field(&dt)
                .unwrap()
                .unwrap_or_else(|| panic!("{sql} should be handled by the planner"));
            assert_eq!(field.data_type(), &expected, "{sql}");
            // Metadata-free for every planner-produced field.
            assert!(field.metadata().is_empty(), "{sql}");
        }
    }

    #[test]
    fn pg_catalog_builtin_rejects_non_builtins_and_unknown_schemas() {
        let planner = PgOidTypePlanner;

        // User-defined-looking custom type under pg_catalog is not mapped (left
        // for DataFusion to error on / another rule to handle).
        let dt = cast_target_type("SELECT 'x'::pg_catalog.some_udt AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);

        // A builtin name qualified by a different schema is not mapped either.
        let dt = cast_target_type("SELECT 'x'::public.text AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);

        // `pg_catalog.regclass` is an oid-alias: handled by the oid arm (a
        // plain metadata-free Int32 field), NOT by the builtin arm.
        let dt = cast_target_type("SELECT 'x'::pg_catalog.regclass AS c");
        let field = planner.plan_type_field(&dt).unwrap().unwrap();
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.metadata().is_empty());
    }

    #[test]
    fn pg_catalog_builtin_does_not_need_array_arm() {
        // `pg_catalog.int2[]` parses as Array(Custom(pg_catalog.int2)). The
        // planner only handles the scalar Custom element; DataFusion is the one
        // that recurses into the planner for the element and wraps it into a
        // list. So at the planner level, the Array itself returns None and only
        // its element resolves -- proving no Array arm is required here.
        let planner = PgOidTypePlanner;
        let dt = cast_target_type("SELECT 'x'::pg_catalog.int2[] AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);

        // ...but the element type alone resolves to Int16.
        let dt = cast_target_type("SELECT 'x'::pg_catalog.int2 AS c");
        let field = planner.plan_type_field(&dt).unwrap().unwrap();
        assert_eq!(field.data_type(), &DataType::Int16);
    }

    #[cfg(feature = "pgvector")]
    #[test]
    fn vector_with_dimension_is_fixed_size_list_of_float32() {
        let planner = PgOidTypePlanner;
        let dt = cast_target_type("SELECT 'x'::vector(3) AS c");
        let field = planner.plan_type_field(&dt).unwrap().unwrap();

        let expected =
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Float32, true)), 3);
        assert_eq!(field.data_type(), &expected);
        assert_eq!(
            field
                .metadata()
                .get(arrow_pg::datatypes::PG_VECTOR_KEY)
                .map(String::as_str),
            Some("vector")
        );
    }

    #[cfg(feature = "pgvector")]
    #[test]
    fn bare_vector_is_a_list_of_float32() {
        let planner = PgOidTypePlanner;
        let dt = cast_target_type("SELECT 'x'::vector AS c");
        let field = planner.plan_type_field(&dt).unwrap().unwrap();

        let expected = DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true)));
        assert_eq!(field.data_type(), &expected);
        assert_eq!(
            field
                .metadata()
                .get(arrow_pg::datatypes::PG_VECTOR_KEY)
                .map(String::as_str),
            Some("vector")
        );
    }

    #[cfg(feature = "pgvector")]
    #[test]
    fn vector_with_invalid_dimension_falls_through() {
        let planner = PgOidTypePlanner;
        let dt = cast_target_type("SELECT 'x'::vector(0) AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);

        // A non-vector custom type is untouched.
        let dt = cast_target_type("SELECT 'x'::public.my_type AS c");
        assert_eq!(planner.plan_type_field(&dt).unwrap(), None);
    }
}
