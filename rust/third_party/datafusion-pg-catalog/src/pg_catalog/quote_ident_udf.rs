use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::create_udf;

/// Create a PostgreSQL quote_ident UDF
pub fn create_quote_ident_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let string_array = args[0].as_string::<i32>();

        let mut builder = StringBuilder::new();
        for ident in string_array.iter() {
            if let Some(ident) = ident {
                // PostgreSQL quote_ident implementation:
                // 1. If identifier is already quoted and contains no special chars, return as-is
                // 2. If identifier contains no special chars and is not a reserved word, return as-is
                // 3. Otherwise, wrap in double quotes and escape any internal double quotes
                let quoted = if ident.starts_with('"') && ident.ends_with('"') {
                    // Already quoted, just escape internal quotes
                    ident.replace('"', "\"\"")
                } else if needs_quoting(ident) {
                    // Needs quoting - wrap in quotes and escape internal quotes
                    format!("\"{}\"", ident.replace('"', "\"\""))
                } else {
                    // No quoting needed
                    ident.to_string()
                };
                builder.append_value(&quoted);
            } else {
                builder.append_null();
            }
        }
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "quote_ident",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ParseIdentUDF {
    signature: Signature,
}

impl ParseIdentUDF {
    pub fn new() -> ParseIdentUDF {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Boolean]),
                ],
                Volatility::Stable,
            ),
        }
    }

    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }
}

impl Default for ParseIdentUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ParseIdentUDF {
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn name(&self) -> &str {
        "parse_ident"
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let string_array = args[0].as_string::<i32>();
        let strict_mode = if args.len() > 1 {
            args[1].as_boolean()
        } else {
            &datafusion::arrow::array::BooleanArray::from(vec![false; string_array.len()])
        };

        let mut builder = ListBuilder::new(StringBuilder::new());

        for (i, ident) in string_array.iter().enumerate() {
            if let Some(ident) = ident {
                let strict = strict_mode.value(i);
                match parse_ident_string(ident, strict) {
                    Ok(parts) => {
                        for part in parts {
                            builder.values().append_value(part);
                        }
                        builder.append(true);
                    }
                    Err(_) => {
                        if strict {
                            return Err(datafusion::error::DataFusionError::Execution(format!(
                                "invalid identifier: {}",
                                ident
                            )));
                        } else {
                            // In non-strict mode, return empty array for invalid identifiers
                            builder.append(true);
                        }
                    }
                }
            } else {
                builder.append_null();
            }
        }

        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    }
}

/// Create a PostgreSQL parse_ident UDF
pub fn create_parse_ident_udf() -> ScalarUDF {
    ParseIdentUDF::new().into_scalar_udf()
}

/// Parse an identifier string into its component parts
fn parse_ident_string(ident: &str, strict: bool) -> Result<Vec<String>, &'static str> {
    if ident.is_empty() {
        return Err("empty identifier");
    }

    let mut parts = Vec::new();
    let mut chars = ident.chars().peekable();
    let mut current_part = String::new();
    let mut in_quotes = false;

    while let Some(&c) = chars.peek() {
        match c {
            '"' if !in_quotes => {
                // Start of quoted identifier
                in_quotes = true;
                chars.next(); // consume the quote
            }
            '"' if in_quotes => {
                // Check for escaped quote (double quote)
                chars.next(); // consume first quote
                if let Some(&'"') = chars.peek() {
                    // Escaped quote
                    current_part.push('"');
                    chars.next(); // consume second quote
                } else {
                    // End of quoted identifier
                    in_quotes = false;
                    if !current_part.is_empty() {
                        parts.push(current_part);
                        current_part = String::new();
                    }
                }
            }
            '.' if !in_quotes => {
                // Separator between parts
                chars.next(); // consume the dot
                if !current_part.is_empty() {
                    parts.push(current_part);
                    current_part = String::new();
                } else if strict {
                    return Err("empty identifier part");
                }
            }
            _ => {
                current_part.push(c);
                chars.next();
            }
        }
    }

    // Handle the last part
    if in_quotes {
        return Err("unterminated quoted identifier");
    }

    if !current_part.is_empty() {
        parts.push(current_part);
    } else if ident.ends_with('.') && strict {
        // In strict mode, trailing dot indicates empty identifier part
        return Err("empty identifier part");
    }

    if parts.is_empty() {
        Err("no valid identifier parts")
    } else {
        Ok(parts)
    }
}

/// Check if an identifier needs quoting according to PostgreSQL rules
fn needs_quoting(ident: &str) -> bool {
    if ident.is_empty() {
        return true;
    }

    // Check if identifier starts with a letter or underscore and contains only letters, digits, underscores
    let mut chars = ident.chars();
    if let Some(first_char) = chars.next()
        && !first_char.is_alphabetic()
        && first_char != '_'
    {
        return true;
    }

    // Check remaining characters
    for c in chars {
        if !c.is_alphanumeric() && c != '_' {
            return true;
        }
    }

    // Check if it's a PostgreSQL reserved word
    is_reserved_word(ident)
}

/// Check if identifier is a PostgreSQL reserved word
fn is_reserved_word(word: &str) -> bool {
    let reserved_words = [
        "ALL",
        "ANALYSE",
        "ANALYZE",
        "AND",
        "ANY",
        "ARRAY",
        "AS",
        "ASC",
        "ASYMMETRIC",
        "AUTHORIZATION",
        "BETWEEN",
        "BINARY",
        "BOTH",
        "CASE",
        "CAST",
        "CHECK",
        "COLLATE",
        "COLUMN",
        "CONCURRENTLY",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT_CATALOG",
        "CURRENT_DATE",
        "CURRENT_ROLE",
        "CURRENT_SCHEMA",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DEFAULT",
        "DEFERRABLE",
        "DESC",
        "DISTINCT",
        "DO",
        "ELSE",
        "END",
        "EXCEPT",
        "FALSE",
        "FETCH",
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "GRANT",
        "GROUP",
        "HAVING",
        "ILIKE",
        "IN",
        "INITIALLY",
        "INNER",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "LATERAL",
        "LEADING",
        "LEFT",
        "LIKE",
        "LIMIT",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "NATURAL",
        "NOT",
        "NOTNULL",
        "NULL",
        "OFFSET",
        "ON",
        "ONLY",
        "OR",
        "ORDER",
        "OUTER",
        "OVERLAPS",
        "PLACING",
        "PRIMARY",
        "REFERENCES",
        "RETURNING",
        "RIGHT",
        "SELECT",
        "SESSION_USER",
        "SIMILAR",
        "SOME",
        "SYMMETRIC",
        "TABLE",
        "TABLESAMPLE",
        "THEN",
        "TO",
        "TRAILING",
        "TRUE",
        "UNION",
        "UNIQUE",
        "USER",
        "USING",
        "VARIADIC",
        "VERBOSE",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
    ];

    reserved_words.contains(&word.to_uppercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        // Test the helper functions directly
        assert_eq!(needs_quoting("simple"), false);
        assert_eq!(needs_quoting("_underscore"), false);
        assert_eq!(needs_quoting("with space"), true);
        assert_eq!(needs_quoting("123start"), true);
        assert_eq!(needs_quoting("with-dash"), true);
        assert_eq!(needs_quoting("select"), true);
        assert_eq!(needs_quoting("SELECT"), true);
        assert_eq!(needs_quoting(""), true);

        // Test reserved word detection
        assert_eq!(is_reserved_word("select"), true);
        assert_eq!(is_reserved_word("SELECT"), true);
        assert_eq!(is_reserved_word("not_reserved"), false);
    }

    #[test]
    fn test_parse_ident() {
        // Test basic parsing
        assert_eq!(parse_ident_string("simple", false).unwrap(), vec!["simple"]);
        assert_eq!(
            parse_ident_string("schema.table", false).unwrap(),
            vec!["schema", "table"]
        );
        assert_eq!(
            parse_ident_string("db.schema.table", false).unwrap(),
            vec!["db", "schema", "table"]
        );

        // Test quoted identifiers
        assert_eq!(
            parse_ident_string("\"quoted.ident\"", false).unwrap(),
            vec!["quoted.ident"]
        );
        assert_eq!(
            parse_ident_string("\"schema\".\"table\"", false).unwrap(),
            vec!["schema", "table"]
        );

        // Test escaped quotes
        assert_eq!(
            parse_ident_string("\"quote\"\"test\"", false).unwrap(),
            vec!["quote\"test"]
        );

        // Test mixed quoted and unquoted
        assert_eq!(
            parse_ident_string("schema.\"table.name\"", false).unwrap(),
            vec!["schema", "table.name"]
        );

        // Test edge cases
        assert!(parse_ident_string("", false).is_err());
        assert!(parse_ident_string("unterminated\"", false).is_err());
        assert_eq!(
            parse_ident_string("trailing.", false).unwrap(),
            vec!["trailing"]
        );
        assert_eq!(
            parse_ident_string(".leading", false).unwrap(),
            vec!["leading"]
        );

        // Test strict mode
        assert!(parse_ident_string("trailing.", true).is_err());
        assert!(parse_ident_string(".leading", true).is_err());
        assert!(parse_ident_string("..", true).is_err());
    }
}
