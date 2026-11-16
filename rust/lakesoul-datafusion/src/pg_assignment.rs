// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL assignment-cast rules shared by every SQL front end.
//!
//! `INSERT` and `UPDATE` analyse a value against a target column the same way:
//! an untyped literal takes the column's type, every other expression must be
//! assignable under PostgreSQL's assignment matrix, and an invalid string
//! literal is a conversion error rather than a type mismatch. Both the wire
//! front end and the DataFusion-level update planner use this module so there
//! is exactly one rule set.

use std::fmt;

use arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::sql::sqlparser::ast::{Expr, Value};

/// SQLSTATE `datatype_mismatch`.
pub const DATATYPE_MISMATCH: &str = "42804";
/// SQLSTATE `invalid_text_representation`.
pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";

/// A literal PostgreSQL treats as `unknown` (or a parameter whose type is
/// supplied by the target column), as opposed to a typed expression.
pub enum Literal<'a> {
    Null,
    String(&'a str),
    Number(&'a str),
    Boolean,
    Placeholder,
}

pub fn literal(expr: &Expr) -> Option<Literal<'_>> {
    match expr {
        Expr::Nested(inner) => literal(inner),
        Expr::Value(value) => match &value.value {
            Value::Null => Some(Literal::Null),
            Value::SingleQuotedString(text) => Some(Literal::String(text)),
            Value::Number(text, _) => Some(Literal::Number(text)),
            Value::Boolean(_) => Some(Literal::Boolean),
            Value::Placeholder(_) => Some(Literal::Placeholder),
            _ => None,
        },
        _ => None,
    }
}

/// A rejected assignment, carrying the PostgreSQL SQLSTATE and the
/// user-facing message both front ends report.
#[derive(Debug, Clone)]
pub struct AssignmentError {
    pub sqlstate: &'static str,
    pub message: String,
}

impl fmt::Display for AssignmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AssignmentError {}

/// Check one literal against the target column.
pub fn check_literal(value: Literal<'_>, target: &Field) -> Result<(), AssignmentError> {
    let target_type = target.data_type();
    match value {
        Literal::Null | Literal::Placeholder => Ok(()),
        Literal::String(text) => {
            if is_string(target_type) {
                return Ok(());
            }
            if is_numeric(target_type)
                || is_temporal(target_type)
                || *target_type == DataType::Boolean
                || is_binary(target_type)
            {
                ScalarValue::Utf8(Some(text.to_string()))
                    .cast_to(target_type)
                    .map_err(|_| invalid_text(target, text))?;
            }
            Ok(())
        }
        Literal::Number(text) => {
            if is_numeric(target_type) || is_string(target_type) {
                Ok(())
            } else {
                Err(datatype_mismatch(target, number_type(text)))
            }
        }
        Literal::Boolean => {
            if *target_type == DataType::Boolean || is_string(target_type) {
                Ok(())
            } else {
                Err(datatype_mismatch(target, "boolean"))
            }
        }
    }
}

/// Check a typed expression's source type against the target column.
pub fn ensure_assignment(
    source: &DataType,
    target: &Field,
) -> Result<(), AssignmentError> {
    if *source == DataType::Null {
        // An unresolved parameter takes its type from the target column.
        return Ok(());
    }
    if assignment_allowed(source, target.data_type()) {
        return Ok(());
    }
    Err(datatype_mismatch(target, pg_type_name(source)))
}

pub fn assignment_allowed(source: &DataType, target: &DataType) -> bool {
    // A value already of the column's own type is always assignable. This is
    // what admits complex column types (for example a pgvector `List(Float32)`
    // built by an `ARRAY[...]` expression) that the scalar matrix below does
    // not enumerate. PostgreSQL treats identity assignment as implicit.
    if source == target {
        return true;
    }
    if is_numeric(source) {
        is_numeric(target) || is_string(target)
    } else if *source == DataType::Boolean {
        *target == DataType::Boolean || is_string(target)
    } else if is_string(source) {
        is_string(target)
    } else if is_temporal(source) {
        is_temporal(target) || is_string(target)
    } else if is_binary(source) {
        is_binary(target) || is_string(target)
    } else {
        false
    }
}

pub fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

pub fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
    )
}

pub fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

pub fn pg_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => {
            "smallint"
        }
        DataType::Int32 | DataType::UInt32 => "integer",
        DataType::Int64 | DataType::UInt64 => "bigint",
        DataType::Float16 | DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "text",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, timezone) => match timezone {
            Some(_) => "timestamp with time zone",
            None => "timestamp without time zone",
        },
        DataType::Decimal128(..) | DataType::Decimal256(..) => "numeric",
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "bytea",
        _ => "unknown",
    }
}

/// The PostgreSQL type an untyped numeric literal gets.
pub fn number_type(text: &str) -> &'static str {
    if text.contains(['.', 'e', 'E']) {
        "numeric"
    } else if text.parse::<i32>().is_ok() {
        "integer"
    } else if text.parse::<i64>().is_ok() {
        "bigint"
    } else {
        "numeric"
    }
}

pub fn datatype_mismatch(target: &Field, source: &str) -> AssignmentError {
    AssignmentError {
        sqlstate: DATATYPE_MISMATCH,
        message: format!(
            "column \"{}\" is of type {} but expression is of type {source}",
            target.name(),
            pg_type_name(target.data_type())
        ),
    }
}

pub fn invalid_text(target: &Field, text: &str) -> AssignmentError {
    AssignmentError {
        sqlstate: INVALID_TEXT_REPRESENTATION,
        message: format!(
            "invalid input syntax for type {}: \"{text}\"",
            pg_type_name(target.data_type())
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_complex_types_are_assignable() {
        let vector = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Float32,
            true,
        )));
        assert!(assignment_allowed(&vector, &vector));
        assert!(!assignment_allowed(&DataType::Utf8, &vector));
    }

    #[test]
    fn invalid_string_literal_is_a_conversion_error() {
        let target = Field::new("n", DataType::Int32, false);
        let error = check_literal(Literal::String("nope"), &target).unwrap_err();
        assert_eq!(error.sqlstate, INVALID_TEXT_REPRESENTATION);
        check_literal(Literal::String("5"), &target).unwrap();
    }
}
