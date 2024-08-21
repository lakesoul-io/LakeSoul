// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::ops::Not;
use std::sync::Arc;

use anyhow::anyhow;
use arrow_schema::{DataType, Field, Fields, SchemaRef};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::{col, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion_common::DataFusionError::{External, Internal};
use datafusion_common::{Column, Result};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::r#type::Nullability;
use datafusion_substrait::substrait::proto::Plan;
use tokio::runtime::Builder;

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
            let expr_filed = qualified_expr(left.as_str(), schema);
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
            .ok_or(External(anyhow!("wrong filter str").into()))?;
        let (op, filter) = filter.split_at(op_offset);
        if !filter.ends_with(')') {
            return Err(External(anyhow!("wrong filter str").into()));
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
                                .map_err(|e| External(Box::new(e)))?,
                        ),
                        precision,
                        scale,
                    ))
                } else {
                    let binary_vec = Parser::parse_binary_array(value.as_str())?
                        .ok_or(External(anyhow!("parse binary array failed").into()))?;
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
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Binary => Expr::Literal(ScalarValue::Binary(Parser::parse_binary_array(value.as_str())?)),
            DataType::Float32 => Expr::Literal(ScalarValue::Float32(Some(
                value
                    .parse::<f32>()
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Float64 => Expr::Literal(ScalarValue::Float64(Some(
                value
                    .parse::<f64>()
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int8 => Expr::Literal(ScalarValue::Int8(Some(
                value
                    .parse::<i8>()
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int16 => Expr::Literal(ScalarValue::Int16(Some(
                value
                    .parse::<i16>()
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int32 => Expr::Literal(ScalarValue::Int32(Some(
                value
                    .parse::<i32>()
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int64 => Expr::Literal(ScalarValue::Int64(Some(
                value
                    .parse::<i64>()
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Date32 => Expr::Literal(ScalarValue::Date32(Some(
                value
                    .parse::<i32>()
                    .map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Timestamp(_, _) => Expr::Literal(ScalarValue::TimestampMicrosecond(
                Some(
                    value
                        .parse::<i64>()
                        .map_err(|e| External(Box::new(e)))?,
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
                        let s = s.map_err(|e| External(Box::new(e)))?;
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

    pub(crate) fn parse_substrait_plan(plan: &Plan) -> Result<Expr> {
        let runtime = Builder::new_current_thread().build().map_err(|e| {
            External(Box::new(e))
        })?;
        runtime.block_on(async {
            let ctx = SessionContext::default();
            let logical_plan = from_substrait_plan(&ctx, plan).await?;
            match logical_plan {
                LogicalPlan::TableScan(scan) => {
                    if scan.filters.len() == 1 {
                        Ok(scan.filters[0].clone())
                    } else {
                        Err(Internal(format!("encountered wrong substrait plan {:?}", plan)))
                    }
                },
                _ => Err(Internal(format!("encountered wrong substrait plan {:?}", plan)))
            }
        })
    }
}

fn qualified_expr(expr_str: &str, schema: SchemaRef) -> Option<(Expr, Arc<Field>)> {
    if let Ok(field) = schema.field_with_name(expr_str) {
        Some((col(Column::new_unqualified(expr_str)), Arc::new(field.clone())))
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
                    Some((col(Column::new_unqualified(field.name())), field.clone()))
                };
                root = "".to_owned();

                sub_fields = match field.data_type() {
                    DataType::Struct(struct_sub_fields) => struct_sub_fields,
                    _ => sub_fields,
                };
            }
        }
        expr
    }
}

fn _from_nullability(nullability: Nullability) -> bool {
    match nullability {
        Nullability::Unspecified => true,
        Nullability::Nullable => true,
        Nullability::Required => false,
    }
}

#[cfg(test)]
mod tests {
    use std::result::Result;

    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use prost::Message;

    use super::*;

    #[test]
    fn test_filter_parser() -> Result<(), String> {
        let s = String::from("or(lt(a.b.c, 2.0), gt(a.b.c, 3.0))");
        let (op, left, right) = Parser::parse_filter_str(s).unwrap();
        assert_eq!(op, "or");
        assert_eq!(left, "lt(a.b.c, 2.0)");
        assert_eq!(right, "gt(a.b.c, 3.0)");
        Ok(())
    }

    #[tokio::test]
    async fn tt() {
        let ctx = SessionContext::new();
        let options = ParquetReadOptions::default();
        let table_path = "/var/folders/_b/qyl87wbn1119cvw8kts6fqtw0000gn/T/lakeSource/type/part-00000-97db3149-f99e-404a-aa9a-2af4ab3f7a44_00000.c000.parquet";
        // let df = ctx.read_parquet(table_path, options).await.unwrap();
        let _ = ctx.register_parquet("type_info", table_path, options).await;
        let test_sql = "select * from type_info";
        let df = ctx.sql(test_sql).await.unwrap();
        let byte_array = [
            10, 30, 8, 1, 18, 26, 47, 102, 117, 110, 99, 116, 105, 111, 110, 115, 95, 99, 111, 109, 112, 97, 114, 105,
            115, 111, 110, 46, 121, 97, 109, 108, 18, 16, 26, 14, 8, 1, 26, 10, 103, 116, 58, 97, 110, 121, 95, 97,
            110, 121, 26, 72, 18, 70, 10, 68, 10, 66, 10, 2, 10, 0, 18, 4, 18, 2, 24, 2, 26, 41, 26, 39, 26, 4, 10, 2,
            16, 1, 34, 22, 26, 20, 18, 18, 10, 14, 10, 12, 10, 10, 98, 5, 109, 111, 110, 101, 121, -112, 3, 1, 34, 0,
            34, 7, 26, 5, 10, 3, 40, -12, 3, 58, 11, 10, 9, 116, 121, 112, 101, 95, 105, 110, 102, 111,
        ];
        let byte_array = unsafe { std::mem::transmute::<&[i8], &[u8]>(&byte_array[..]) };
        let plan = Plan::decode(&byte_array[..]).unwrap();
        let e = Parser::parse_substrait_plan(&plan).unwrap();
        let df = df.filter(e).unwrap();
        let df = df.explain(true, true).unwrap();
        df.show().await.unwrap()
    }
}
