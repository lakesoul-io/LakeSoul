// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::anyhow;
use arrow_schema::{DataType, Field, Fields, IntervalUnit, SchemaRef, TimeUnit};
use datafusion::logical_expr::BinaryExpr;
use datafusion::prelude::{col, not, Expr};
use datafusion::scalar::ScalarValue;
use datafusion_common::{not_impl_err, plan_err, Column, DFSchema, DataFusionError, Result};
use datafusion_substrait::substrait::proto::expression::field_reference::ReferenceType::DirectReference;
use datafusion_substrait::substrait::proto::expression::literal::{
    IntervalCompound, IntervalDayToSecond, IntervalYearToMonth,
};
use datafusion_substrait::substrait::proto::expression::literal::LiteralType;
use datafusion_substrait::substrait::proto::expression::reference_segment::ReferenceType;
use datafusion_substrait::substrait::proto::expression::{Literal, RexType};
use datafusion_substrait::substrait::proto::function_argument::ArgType;
use datafusion_substrait::substrait::proto::read_rel::ReadType;
use datafusion_substrait::substrait::proto::rel::RelType;
use datafusion_substrait::substrait::proto::{plan_rel, r#type, Expression, Plan, Rel, Type};
use log::debug;
use datafusion_substrait::logical_plan::consumer::name_to_op;
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::variation_const::*;

const DEFAULT_TIMEZONE: &str = "UTC";

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
            not(inner)
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

    pub(crate) fn parse_proto(plan: &Plan, df_schema: &DFSchema) -> Result<Expr> {
        let extensions = Extensions::try_from(&plan.extensions)
            .map_err(|e| DataFusionError::Substrait(format!("Failed to parse extensions: {e}")))?;

        match plan.relations.len() {
            1 => match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => Ok(Parser::parse_rel(rel, &extensions, df_schema)?),
                    plan_rel::RelType::Root(root) => Ok(Parser::parse_rel(
                        root.input
                            .as_ref()
                            .ok_or(DataFusionError::Substrait("wrong root".to_string()))?,
                        &extensions,
                        df_schema,
                    )?),
                },
                None => plan_err!("Cannot parse plan relation: None"),
            },
            _ => not_impl_err!(
                "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
                plan.relations.len()
            ),
        }
    }

    fn parse_rel(rel: &Rel, extensions: &Extensions, df_schema: &DFSchema) -> Result<Expr> {
        match &rel.rel_type {
            Some(RelType::Read(read)) => match &read.as_ref().read_type {
                None => {
                    not_impl_err!("unsupported")
                }
                Some(ReadType::NamedTable(_nt)) => {
                    let e = read
                        .filter
                        .as_ref()
                        .ok_or(DataFusionError::Substrait("wrong filter".to_string()))?;
                    Parser::parse_rex(e.as_ref(), df_schema, extensions)
                }
                Some(_) => {
                    not_impl_err!("un supported")
                }
            },
            _ => not_impl_err!("un supported"),
        }
    }

    fn parse_rex(e: &Expression, input_schema: &DFSchema, extensions: &Extensions) -> Result<Expr> {
        match &e.rex_type {
            Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
                Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
                    Some(ReferenceType::MapKey(x)) => match &x.child.as_ref() {
                        Some(_) => not_impl_err!("MapKey is not supported"),
                        None => {
                            let literal = x
                                .map_key
                                .as_ref()
                                .ok_or(DataFusionError::Substrait("can not get map key".into()))?;
                            let sv = from_substrait_literal(literal)?;
                            let field_name = match sv {
                                ScalarValue::Utf8(s) => {
                                    s.ok_or(DataFusionError::Substrait("can not get map key".into()))
                                }
                                _ => not_impl_err!("map key wrong type"),
                            }?;
                            debug!("field name: {}", field_name);
                            let (qualifier, _) = input_schema
                                .qualified_field_with_unqualified_name(&field_name)?;
                            let column = Column::new(qualifier.cloned(), field_name);
                            Ok(Expr::Column(Column {
                                relation: column.relation,
                                name: column.name,
                            }))
                        }
                    },
                    _ => not_impl_err!("Direct reference with types other than MapKey is not supported"),
                },
                _ => not_impl_err!("unsupported field ref type"),
            },
            Some(RexType::ScalarFunction(f)) => {
                let fn_name = extensions.functions.get(&f.function_reference)
                    .ok_or_else(|| DataFusionError::Substrait(format!(
                        "Function not found: function reference = {:?}",
                        f.function_reference
                    )))?;

                let fn_name = substrait_fun_name(fn_name);

                let mut args = vec![];
                for arg in &f.arguments {
                    let arg_expr = match &arg.arg_type {
                        Some(ArgType::Value(e)) => Parser::parse_rex(e, input_schema, extensions),
                        _ => not_impl_err!("Function argument non-Value type not supported"),
                    }?;
                    args.push(arg_expr);
                }

                if let Some(op) = name_to_op(fn_name) {
                    if args.len() != 2 {
                        return not_impl_err!(
                            "Expect two arguments for binary operator {op:?}"
                        );
                    }
                    Ok(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(args[0].clone()),
                        op,
                        right: Box::new(args[1].clone()),
                    }))
                } else {
                    not_impl_err!("Unsupported function name: {fn_name}")
                }
            }
            Some(RexType::Literal(lit)) => {
                let scalar_value = from_substrait_literal(lit)?;
                Ok(Expr::Literal(scalar_value))
            }
            _ => unimplemented!(),
        }
    }
}

fn from_substrait_literal(lit: &Literal) -> Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}"
                )));
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}"
                )));
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}"
                )));
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}"
                )));
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        Some(LiteralType::Timestamp(t)) => ScalarValue::TimestampMicrosecond(Some(*t), None),
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}"
                )));
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeBinary(Some(b.clone())),
            others => {
                return Err(DataFusionError::Substrait(format!(
                    "Unknown type variation reference {others}"
                )));
            }
        },
        Some(LiteralType::FixedBinary(b)) => {
            ScalarValue::FixedSizeBinary(b.len() as _, Some(b.clone()))
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d.value.clone().try_into().or(Err(
                DataFusionError::Substrait("Failed to parse decimal value".to_string()),
            ))?;
            let p = d.precision.try_into().map_err(|e| {
                DataFusionError::Substrait(format!("Failed to parse decimal precision: {e}"))
            })?;
            let s = d.scale.try_into().map_err(|e| {
                DataFusionError::Substrait(format!("Failed to parse decimal scale: {e}"))
            })?;
            ScalarValue::Decimal128(Some(i128::from_le_bytes(value)), p, s)
        }
        Some(LiteralType::IntervalDayToSecond(IntervalDayToSecond {
            days,
            seconds,
            subseconds,
            ..
        })) => ScalarValue::IntervalDayTime(Some(IntervalDayTime {
            days: *days,
            milliseconds: *seconds * 1000 + (*subseconds / 1_000_000) as i32,
        })),
        Some(LiteralType::IntervalYearToMonth(IntervalYearToMonth { years, months })) => {
            ScalarValue::IntervalYearMonth(Some(*years * 12 + months))
        }
        Some(LiteralType::IntervalCompound(IntervalCompound {
            interval_year_to_month,
            interval_day_to_second,
        })) => match (interval_year_to_month, interval_day_to_second) {
            (
                Some(IntervalYearToMonth { years, months }),
                Some(IntervalDayToSecond {
                    days,
                    seconds,
                    subseconds,
                    ..
                }),
            ) => ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                months: *years * 12 + months,
                days: *days,
                nanoseconds: *seconds as i64 * 1_000_000_000 + *subseconds as i64,
            })),
            _ => {
                return Err(DataFusionError::Substrait(
                    "Substrait compound interval missing components".to_string(),
                ))
            }
        },
        Some(LiteralType::Null(ntype)) => from_substrait_null(ntype)?,
        _ => {
            return Err(DataFusionError::Substrait(format!(
                "Unsupported literal_type: {:?}",
                lit.literal_type
            )))
        }
    };

    Ok(scalar_value)
}

fn from_substrait_type(dt: &Type) -> Result<DataType> {
    match &dt.kind {
        Some(s_kind) => match s_kind {
            r#type::Kind::Bool(_) => Ok(DataType::Boolean),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int8),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt8),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int16),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt16),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int32),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt32),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int64),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt64),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::Fp32(_) => Ok(DataType::Float32),
            r#type::Kind::Fp64(_) => Ok(DataType::Float64),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_VARIATION_REF => Ok(DataType::Timestamp(TimeUnit::Second, None)),
                TIMESTAMP_MILLI_TYPE_VARIATION_REF => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
                TIMESTAMP_MICRO_TYPE_VARIATION_REF => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)), 
                TIMESTAMP_NANO_TYPE_VARIATION_REF => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::PrecisionTimestamp(pts) => {
                let unit = match pts.precision {
                    0 => Ok(TimeUnit::Second),
                    3 => Ok(TimeUnit::Millisecond),
                    6 => Ok(TimeUnit::Microsecond),
                    9 => Ok(TimeUnit::Nanosecond),
                    p => Err(DataFusionError::Substrait(format!(
                        "Unsupported Substrait precision {p} for PrecisionTimestamp"
                    ))),
                }?;
                Ok(DataType::Timestamp(unit, None))
            },
            r#type::Kind::PrecisionTimestampTz(pts) => {
                let unit = match pts.precision {
                    0 => Ok(TimeUnit::Second),
                    3 => Ok(TimeUnit::Millisecond), 
                    6 => Ok(TimeUnit::Microsecond),
                    9 => Ok(TimeUnit::Nanosecond),
                    p => Err(DataFusionError::Substrait(format!(
                        "Unsupported Substrait precision {p} for PrecisionTimestampTz"
                    ))),
                }?;
                Ok(DataType::Timestamp(unit, Some(DEFAULT_TIMEZONE.into())))
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF => Ok(DataType::Date32),
                DATE_64_TYPE_VARIATION_REF => Ok(DataType::Date64),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Binary),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeBinary),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::BinaryView),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::FixedBinary(fixed) => Ok(DataType::FixedSizeBinary(fixed.length)),
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeUtf8),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8View),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::Decimal(d) => match d.type_variation_reference {
                DECIMAL_128_TYPE_VARIATION_REF => Ok(DataType::Decimal128(d.precision as u8, d.scale as i8)),
                DECIMAL_256_TYPE_VARIATION_REF => Ok(DataType::Decimal256(d.precision as u8, d.scale as i8)),
                v => Err(DataFusionError::Substrait(format!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ))),
            },
            r#type::Kind::IntervalYear(_) => Ok(DataType::Interval(IntervalUnit::YearMonth)),
            r#type::Kind::IntervalDay(_) => Ok(DataType::Interval(IntervalUnit::DayTime)),
            r#type::Kind::IntervalCompound(_) => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
            r#type::Kind::Varchar(_) => Ok(DataType::Utf8),
            r#type::Kind::FixedChar(_) => Ok(DataType::Utf8),
            _ => Err(DataFusionError::Substrait(format!(
                "Unsupported Substrait type: {s_kind:?}"
            ))),
        },
        None => Err(DataFusionError::Substrait(
            "`None` Substrait kind is not supported".to_string(),
        )),
    }
}

fn from_substrait_null(null_type: &Type) -> Result<ScalarValue> {
    match from_substrait_type(null_type)? {
        DataType::Boolean => Ok(ScalarValue::Boolean(None)),
        DataType::Int8 => Ok(ScalarValue::Int8(None)),
        DataType::Int16 => Ok(ScalarValue::Int16(None)),
        DataType::Int32 => Ok(ScalarValue::Int32(None)),
        DataType::Int64 => Ok(ScalarValue::Int64(None)),
        DataType::UInt8 => Ok(ScalarValue::UInt8(None)),
        DataType::UInt16 => Ok(ScalarValue::UInt16(None)),
        DataType::UInt32 => Ok(ScalarValue::UInt32(None)),
        DataType::UInt64 => Ok(ScalarValue::UInt64(None)),
        DataType::Float32 => Ok(ScalarValue::Float32(None)),
        DataType::Float64 => Ok(ScalarValue::Float64(None)),
        DataType::Utf8 => Ok(ScalarValue::Utf8(None)),
        DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(None)),
        DataType::Binary => Ok(ScalarValue::Binary(None)),
        DataType::LargeBinary => Ok(ScalarValue::LargeBinary(None)),
        DataType::Date32 => Ok(ScalarValue::Date32(None)),
        DataType::Date64 => Ok(ScalarValue::Date64(None)),
        DataType::Decimal128(p, s) => Ok(ScalarValue::Decimal128(None, p, s)),
        dt => Err(DataFusionError::Substrait(format!(
            "Unsupported null type: {dt:?}"
        ))),
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
                    Some((col(format!("{}.{}", folding_exp, field.name())), field.clone()))
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


fn substrait_fun_name(name: &str) -> &str {
    match name.rsplit_once(':') {
        Some((name, _)) => name,
        None => name,
    }
}

#[cfg(test)]
mod tests {
    use std::result::Result;

    use datafusion::{
        logical_expr::{LogicalPlan, TableScan},
        prelude::{ParquetReadOptions, SessionContext},
    };
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

    // #[tokio::test]
    // async fn tt() {
    //     let ctx = SessionContext::new();
    //     let options = ParquetReadOptions::default();
    //     let table_path = "/var/folders/_b/qyl87wbn1119cvw8kts6fqtw0000gn/T/lakeSource/type/part-00000-97db3149-f99e-404a-aa9a-2af4ab3f7a44_00000.c000.parquet";
    //     // let df = ctx.read_parquet(table_path, options).await.unwrap();
    //     let _ = ctx.register_parquet("type_info", table_path, options).await;
    //     let test_sql = "select * from type_info";
    //     let df = ctx.sql(test_sql).await.unwrap();
    //     let byte_array = [
    //         10, 30, 8, 1, 18, 26, 47, 102, 117, 110, 99, 116, 105, 111, 110, 115, 95, 99, 111, 109, 112, 97, 114, 105,
    //         115, 111, 110, 46, 121, 97, 109, 108, 18, 16, 26, 14, 8, 1, 26, 10, 103, 116, 58, 97, 110, 121, 95, 97,
    //         110, 121, 26, 72, 18, 70, 10, 68, 10, 66, 10, 2, 10, 0, 18, 4, 18, 2, 24, 2, 26, 41, 26, 39, 26, 4, 10, 2,
    //         16, 1, 34, 22, 26, 20, 18, 18, 10, 14, 10, 12, 10, 10, 98, 5, 109, 111, 110, 101, 121, -112, 3, 1, 34, 0,
    //         34, 7, 26, 5, 10, 3, 40, -12, 3, 58, 11, 10, 9, 116, 121, 112, 101, 95, 105, 110, 102, 111,
    //     ];
    //     let byte_array = unsafe { std::mem::transmute::<&[i8], &[u8]>(&byte_array[..]) };
    //     let plan = Plan::decode(&byte_array[..]).unwrap();
    //     let e = Parser::parse_proto(&plan, df.schema()).unwrap();
    //     let df = df.filter(e).unwrap();
    //     let df = df.explain(true, true).unwrap();
    //     df.show().await.unwrap()
    // }
}
