// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::ops::Not;
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion::logical_expr::Expr;
use datafusion::prelude::ident;
use datafusion::scalar::ScalarValue;

pub struct Parser {}

impl Parser {
    pub fn parse(filter_str: String, schema: SchemaRef) -> Expr {
        let (op, left, right) = Parser::parse_filter_str(filter_str);
        if op.eq("or") {
            let left_expr = Parser::parse(left, schema.clone());
            let right_expr = Parser::parse(right, schema);
            left_expr.or(right_expr)
        } else if op.eq("and") {
            let left_expr = Parser::parse(left, schema.clone());
            let right_expr = Parser::parse(right, schema);
            left_expr.and(right_expr)
        } else if op.eq("not") {
            let inner = Parser::parse(right, schema);
            Expr::not(inner)
        } else {
            let column = qualified_col_name(left.as_str(), schema.clone());
            match schema.column_with_name(column) {
                None => Expr::Literal(ScalarValue::Boolean(Some(true))),
                Some((_, field)) => {
                    if matches!(field.data_type(), DataType::Struct(_)) {
                        ident(column).is_not_null()
                    } else if right == "null" {
                        match op.as_str() {
                            "eq" => ident(column).is_null(),
                            "noteq" => ident(column).is_not_null(),
                            _ => Expr::Literal(ScalarValue::Boolean(Some(true))),
                        }
                    } else {
                        match op.as_str() {
                            "eq" => {
                                let value = Parser::parse_literal(field, right);
                                ident(column).eq(value)
                            }
                            "noteq" => {
                                let value = Parser::parse_literal(field, right);
                                ident(column).not_eq(value)
                            }
                            "gt" => {
                                let value = Parser::parse_literal(field, right);
                                ident(column).gt(value)
                            }
                            "gteq" => {
                                let value = Parser::parse_literal(field, right);
                                ident(column).gt_eq(value)
                            }
                            "lt" => {
                                let value = Parser::parse_literal(field, right);
                                ident(column).lt(value)
                            }
                            "lteq" => {
                                let value = Parser::parse_literal(field, right);
                                ident(column).lt_eq(value)
                            }

                            _ => Expr::Literal(ScalarValue::Boolean(Some(true))),
                        }
                    }
                }
            }
        }
    }

    fn parse_filter_str(filter: String) -> (String, String, String) {
        let op_offset = filter.find('(').unwrap();
        let (op, filter) = filter.split_at(op_offset);
        if !filter.ends_with(')') {
            panic!("Invalid filter string");
        }
        let filter = &filter[1..filter.len() - 1];
        let mut k: i8 = 0;
        let mut left_offset: usize = 0;
        for (i, ch) in filter.chars().enumerate() {
            match ch {
                '(' => k += 1,
                ')' => k -= 1,
                ',' => {
                    if k == 0 && left_offset == 0 {
                        left_offset = i
                    }
                }
                _ => {}
            }
        }
        if k != 0 {
            panic!("Invalid filter string");
        }
        let (left, right) = filter.split_at(left_offset);
        if op.eq("not") {
            (op.to_string(), left.to_string(), right[0..].to_string())
        } else {
            (op.to_string(), left.to_string(), right[2..].to_string())
        }
    }

    fn parse_literal(field: &Field, value: String) -> Expr {
        let data_type = field.data_type().clone();
        match data_type {
            DataType::Decimal128(precision, scale) => {
                if precision <= 18 {
                    Expr::Literal(ScalarValue::Decimal128(
                        Some(value.parse::<i128>().unwrap()),
                        precision,
                        scale,
                    ))
                } else {
                    let binary_vec = Parser::parse_binary_array(value.as_str()).unwrap();
                    let mut arr = [0u8; 16];
                    for idx in 0..binary_vec.len() {
                        arr[idx + 16 - binary_vec.len()] = binary_vec[idx];
                    }
                    Expr::Literal(ScalarValue::Decimal128(
                        Some(i128::from_be_bytes(arr)),
                        precision,
                        scale,
                    ))
                }
            }
            DataType::Boolean => Expr::Literal(ScalarValue::Boolean(Some(value.parse::<bool>().unwrap()))),
            DataType::Binary => Expr::Literal(ScalarValue::Binary(Parser::parse_binary_array(value.as_str()))),
            DataType::Float32 => Expr::Literal(ScalarValue::Float32(Some(value.parse::<f32>().unwrap()))),
            DataType::Float64 => Expr::Literal(ScalarValue::Float64(Some(value.parse::<f64>().unwrap()))),
            DataType::Int8 => Expr::Literal(ScalarValue::Int8(Some(value.parse::<i8>().unwrap()))),
            DataType::Int16 => Expr::Literal(ScalarValue::Int16(Some(value.parse::<i16>().unwrap()))),
            DataType::Int32 => Expr::Literal(ScalarValue::Int32(Some(value.parse::<i32>().unwrap()))),
            DataType::Int64 => Expr::Literal(ScalarValue::Int64(Some(value.parse::<i64>().unwrap()))),
            DataType::Date32 => Expr::Literal(ScalarValue::Date32(Some(value.parse::<i32>().unwrap()))),
            DataType::Timestamp(_, time_zone) => Expr::Literal(ScalarValue::TimestampMicrosecond(
                Some(value.parse::<i64>().unwrap()),
                time_zone,
            )),
            DataType::Utf8 => {
                let value = value.as_str()[8..value.len() - 2].to_string();
                Expr::Literal(ScalarValue::Utf8(Some(value)))
            }
            _ => Expr::Literal(ScalarValue::Utf8(Some(value))),
        }
    }

    fn parse_binary_array(value: &str) -> Option<Vec<u8>> {
        let left_bracket_pos = value.find('[').unwrap_or(0);
        let right_bracket_pos = value.find(']').unwrap_or(0);
        if left_bracket_pos == 0 {
            None
        } else if left_bracket_pos + 1 == right_bracket_pos {
            Some(Vec::<u8>::new())
        } else {
            Some(
                value[left_bracket_pos + 1..right_bracket_pos]
                    .to_string()
                    .replace(' ', "")
                    .split(',')
                    .collect::<Vec<&str>>()
                    .iter()
                    .map(|s| s.parse::<i16>().unwrap())
                    .map(|s: i16| if s < 0 { (s + 256) as u8 } else { s as u8 })
                    .collect::<Vec<u8>>(),
            )
        }
    }
}

fn qualified_col_name(column: &str, schema: SchemaRef) -> &str {
    if let Ok(_field) = schema.field_with_name(column) {
        return column;
    } else if let Some(dot) = column.find('.') {
        if let Ok(field) = schema.field_with_name(&column[..dot]) {
            if matches!(field.data_type(), DataType::Struct(_)) {
                return &column[..dot];
            }
        }
    }
    column
}

#[cfg(test)]
mod tests {
    use crate::filter::Parser;
    use std::result::Result;

    #[test]
    fn test_filter_parser() -> Result<(), String> {
        let s = String::from("or(lt(a.b.c, 2.0), gt(a.b.c, 3.0))");
        let (op, left, right) = Parser::parse_filter_str(s);
        assert_eq!(op, "or");
        assert_eq!(left, "lt(a.b.c, 2.0)");
        assert_eq!(right, "gt(a.b.c, 3.0)");
        Ok(())
    }
}
