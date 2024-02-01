// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use arrow_schema::{DataType, Field, Fields, SchemaRef};
use datafusion::logical_expr::Expr;
use datafusion::prelude::col;
use datafusion::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use std::ops::Not;
use std::sync::Arc;

pub struct Parser {}

impl Parser {
    pub fn parse(filter_str: String, schema: SchemaRef) -> Result<Expr> {
        let (op, left, right) = Parser::parse_filter_str(filter_str)?;
        let expr = if op.eq("or") {
            let left_expr = Parser::parse(left, schema.clone())?;
            let right_expr = Parser::parse(right, schema)?;
            left_expr.or(right_expr)
        } else if op.eq("and") {
            let left_expr = Parser::parse(left, schema.clone())?;
            let right_expr = Parser::parse(right, schema)?;
            left_expr.and(right_expr)
        } else if op.eq("not") {
            let inner = Parser::parse(right, schema)?;
            Expr::not(inner)
        } else {
            let expr_filed = qualified_expr(left.as_str(), schema.clone());
            if let Some((expr, field)) = expr_filed {
                if right == "null" {
                    match op.as_str() {
                        "eq" => expr.is_null(),
                        "noteq" => expr.is_not_null(),
                        _ => Expr::Literal(ScalarValue::Boolean(Some(true))),
                    }
                } else {
                    let value = Parser::parse_literal(field, right)?;
                    match op.as_str() {
                        "eq" => expr.eq(value),
                        "noteq" => expr.not_eq(value),
                        "gt" => expr.gt(value),
                        "gteq" => expr.gt_eq(value),
                        "lt" => expr.lt(value),
                        "lteq" => expr.lt_eq(value),
                        _ => Expr::Literal(ScalarValue::Boolean(Some(true))),
                    }
                }
            } else {
                Expr::Literal(ScalarValue::Boolean(Some(false)))
            }
        };
        Ok(expr)
    }

    fn parse_filter_str(filter: String) -> Result<(String, String, String)> {
        let op_offset = filter
            .find('(')
            .ok_or(DataFusionError::External(anyhow!("wrong filter str").into()))?;
        let (op, filter) = filter.split_at(op_offset);
        if !filter.ends_with(')') {
            return Err(DataFusionError::External(anyhow!("wrong filter str").into()));
        }
        let filter = &filter[1..filter.len() - 1];
        let mut k: usize = 0;
        let mut left_offset: usize = 0;
        let mut offset_counter: usize = 0;
        for ch in filter.chars() {
            match ch {
                '(' => k += 1,
                ')' => k -= 1,
                ',' => {
                    if k == 0 && left_offset == 0 {
                        left_offset = offset_counter
                    }
                }
                _ => {}
            }
            offset_counter += ch.len_utf8()
        }
        if k != 0 {
            panic!("Invalid filter string");
        }
        let (left, right) = filter.split_at(left_offset);
        let res = if op.eq("not") {
            (op.to_string(), left.trim().to_string(), right[0..].trim().to_string())
        } else {
            (op.to_string(), left.trim().to_string(), right[1..].trim().to_string())
        };
        Ok(res)
    }

    fn parse_literal(field: Arc<Field>, value: String) -> Result<Expr> {
        let data_type = field.data_type().clone();
        let expr = match data_type {
            DataType::Decimal128(precision, scale) => {
                if precision <= 18 {
                    Expr::Literal(ScalarValue::Decimal128(
                        Some(
                            value
                                .parse::<i128>()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?,
                        ),
                        precision,
                        scale,
                    ))
                } else {
                    let binary_vec = Parser::parse_binary_array(value.as_str())?
                        .ok_or(DataFusionError::External(anyhow!("parse binary array failed").into()))?;
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
            DataType::Boolean => Expr::Literal(ScalarValue::Boolean(Some(
                value
                    .parse::<bool>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Binary => Expr::Literal(ScalarValue::Binary(Parser::parse_binary_array(value.as_str())?)),
            DataType::Float32 => Expr::Literal(ScalarValue::Float32(Some(
                value
                    .parse::<f32>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Float64 => Expr::Literal(ScalarValue::Float64(Some(
                value
                    .parse::<f64>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Int8 => Expr::Literal(ScalarValue::Int8(Some(
                value
                    .parse::<i8>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Int16 => Expr::Literal(ScalarValue::Int16(Some(
                value
                    .parse::<i16>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Int32 => Expr::Literal(ScalarValue::Int32(Some(
                value
                    .parse::<i32>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Int64 => Expr::Literal(ScalarValue::Int64(Some(
                value
                    .parse::<i64>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Date32 => Expr::Literal(ScalarValue::Date32(Some(
                value
                    .parse::<i32>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            ))),
            DataType::Timestamp(_, _) => Expr::Literal(ScalarValue::TimestampMicrosecond(
                Some(
                    value
                        .parse::<i64>()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                ),
                Some(crate::constant::LAKESOUL_TIMEZONE.into()),
            )),
            DataType::Utf8 => {
                let value = value.as_str()[8..value.len() - 2].to_string();
                Expr::Literal(ScalarValue::Utf8(Some(value)))
            }
            _ => Expr::Literal(ScalarValue::Utf8(Some(value))),
        };
        Ok(expr)
    }

    fn parse_binary_array(value: &str) -> Result<Option<Vec<u8>>> {
        let left_bracket_pos = value.find('[').unwrap_or(0);
        let right_bracket_pos = value.find(']').unwrap_or(0);
        let res = if left_bracket_pos == 0 {
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
                    .map(|s| s.parse::<i16>())
                    .map(|s| {
                        let s = s.map_err(|e| DataFusionError::External(Box::new(e)))?;
                        if s < 0 {
                            Ok((s + 256) as u8)
                        } else {
                            Ok(s as u8)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        };
        Ok(res)
    }
}

fn qualified_expr(expr_str: &str, schema: SchemaRef) -> Option<(Expr, Arc<Field>)> {
    if let Ok(field) = schema.field_with_name(expr_str) {
        Some((
            col(datafusion::common::Column::new_unqualified(expr_str)),
            Arc::new(field.clone()),
        ))
    } else {
        let mut expr: Option<(Expr, Arc<Field>)> = None;
        let mut root = "".to_owned();
        let mut sub_fields: &Fields = schema.fields();
        for expr_substr in expr_str.split('.') {
            root = if root.is_empty() {
                expr_substr.to_owned()
            } else {
                format!("{}.{}", root, expr_substr)
            };
            if let Some((_, field)) = sub_fields.find(&root) {
                expr = if let Some((folding_exp, _)) = expr {
                    Some((folding_exp.field(field.name()), field.clone()))
                } else {
                    Some((
                        col(datafusion::common::Column::new_unqualified(field.name())),
                        field.clone(),
                    ))
                };
                root = "".to_owned();

                sub_fields = match field.data_type() {
                    DataType::Struct(struct_sub_fields) => &struct_sub_fields,
                    _ => sub_fields,
                };
            }
        }
        expr
    }
}

#[cfg(test)]
mod tests {
    use crate::filter::parser::Parser;
    use std::result::Result;

    #[test]
    fn test_filter_parser() -> Result<(), String> {
        let s = String::from("or(lt(a.b.c, 2.0), gt(a.b.c, 3.0))");
        let (op, left, right) = Parser::parse_filter_str(s).unwrap();
        assert_eq!(op, "or");
        assert_eq!(left, "lt(a.b.c, 2.0)");
        assert_eq!(right, "gt(a.b.c, 3.0)");
        Ok(())
    }
}
