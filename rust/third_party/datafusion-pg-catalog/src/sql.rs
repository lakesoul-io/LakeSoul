mod parser;
pub use parser::PostgresCompatibilityParser;
pub mod rules;

use datafusion::sql::sqlparser::ast::DataType as SQLDataType;

/// True when `data_type` names the pgvector `vector` type (optionally
/// schema-qualified, e.g. `public.vector`).
pub fn is_vector_type(data_type: &SQLDataType) -> bool {
    let SQLDataType::Custom(name, _) = data_type else {
        return false;
    };
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("vector"))
}
