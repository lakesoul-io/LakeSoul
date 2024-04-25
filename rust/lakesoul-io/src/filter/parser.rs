// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::ops::Not;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use arrow_schema::{DataType, Field, Fields, SchemaRef};
use async_recursion::async_recursion;
use datafusion::logical_expr::{BinaryExpr, BuiltinScalarFunction, Expr, expr, Like, Operator, ScalarUDF, UserDefinedLogicalNode};
use datafusion::prelude::{col, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion_common::{Column, DataFusionError, DFSchema, not_impl_err, plan_err, Result, substrait_datafusion_err, substrait_err};
use datafusion_substrait::substrait::proto::{FunctionArgument, Plan, plan_rel, r#type, Rel, Type};
use datafusion_substrait::substrait::proto::Expression;
use datafusion_substrait::substrait::proto::expression::{FieldReference, Literal, RexType, ScalarFunction};
use datafusion_substrait::substrait::proto::expression::field_reference::ReferenceType::DirectReference;
use datafusion_substrait::substrait::proto::expression::literal::LiteralType;
use datafusion_substrait::substrait::proto::expression::reference_segment::ReferenceType;
use datafusion_substrait::substrait::proto::extensions::simple_extension_declaration::MappingType;
use datafusion_substrait::substrait::proto::function_argument::ArgType;
use datafusion_substrait::substrait::proto::read_rel::ReadType;
use datafusion_substrait::substrait::proto::rel::RelType;
use datafusion_substrait::variation_const::{DATE_32_TYPE_REF, DATE_64_TYPE_REF, DEFAULT_CONTAINER_TYPE_REF, DEFAULT_TYPE_REF, LARGE_CONTAINER_TYPE_REF, TIMESTAMP_MICRO_TYPE_REF, TIMESTAMP_MILLI_TYPE_REF, TIMESTAMP_NANO_TYPE_REF, TIMESTAMP_SECOND_TYPE_REF, UNSIGNED_INTEGER_TYPE_REF};
use tracing::debug;

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

    pub(crate) async fn parse_proto(ctx: &SessionContext, plan: &Plan, df_schema: &DFSchema) -> Result<Arc<Expr>> {
        /// ex: equal:bool -> equal
        fn delete_type(origin: &str) -> Result<String> {
            debug!("origin func name: {}",origin);
            let out = origin.split_once(":").ok_or(DataFusionError::Internal("wrong func name".into()))?.0;
            debug!("after is {}",out);
            Ok(out.to_string())
        }

        let function_extension_vec = plan
            .extensions
            .iter()
            .map(|e| match &e.mapping_type {
                Some(ext) => match ext {
                    MappingType::ExtensionFunction(ext_f) => {
                        Ok((ext_f.function_anchor, &ext_f.name))
                    }
                    _ => not_impl_err!("Extension type not supported: {ext:?}"),
                },
                None => not_impl_err!("Cannot parse empty extension"),
            })
            .collect::<Result<Vec<(_, _)>>>()?;

        let mut container = vec![];
        for v in function_extension_vec.iter() {
            container.push(delete_type(v.1)?);
        }

        let mut function_extension = HashMap::new();
        for (i, v) in function_extension_vec.iter().enumerate() {
            function_extension.insert(v.0, &container[i]);
        }


        // Parse relations
        match plan.relations.len() {
            1 => match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => Parser::parse_rel(ctx, rel, &function_extension, df_schema).await,
                    plan_rel::RelType::Root(root) => Parser::parse_rel(ctx,
                                                                       root.input
                                                                           .as_ref()
                                                                           .ok_or(DataFusionError::Substrait("wrong root".to_string()))?,
                                                                       &function_extension,
                                                                       df_schema).await,
                },
                None => plan_err!("Cannot parse plan relation: None"),
            },
            _ => not_impl_err!(
                "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
                plan.relations.len()
            ),
        }
    }

    async fn parse_rel(ctx: &SessionContext, rel: &Rel, extensions: &HashMap<u32, &String>, df_schema: &DFSchema) -> Result<Arc<Expr>> {
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
                    parse_rex(ctx, e.as_ref(), df_schema, extensions).await
                }
                Some(_) => {
                    not_impl_err!("un supported")
                }
            },
            _ => not_impl_err!("un supported"),
        }
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
                    DataType::Struct(struct_sub_fields) => &struct_sub_fields,
                    _ => sub_fields,
                };
            }
        }
        expr
    }
}

struct BuiltinExprBuilder {
    expr_name: String,
}

impl BuiltinExprBuilder {
    pub fn try_from_name(name: &str) -> Option<Self> {
        match name {
            "not" | "like" | "ilike" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" | "negative" => Some(Self {
                expr_name: name.to_string(),
            }),
            _ => None,
        }
    }

    pub async fn build(
        self,
        ctx: &SessionContext,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &HashMap<u32, &String>,
    ) -> Result<Arc<Expr>> {
        match self.expr_name.as_str() {
            "like" => {
                Self::build_like_expr(ctx, false, f, input_schema, extensions).await
            }
            "ilike" => {
                Self::build_like_expr(ctx, true, f, input_schema, extensions).await
            }
            "not" | "negative" | "is_null" | "is_not_null" | "is_true" | "is_false"
            | "is_not_true" | "is_not_false" | "is_unknown" | "is_not_unknown" => {
                Self::build_unary_expr(ctx, &self.expr_name, f, input_schema, extensions)
                    .await
            }
            _ => {
                not_impl_err!("Unsupported builtin expression: {}", self.expr_name)
            }
        }
    }

    async fn build_unary_expr(
        ctx: &SessionContext,
        fn_name: &str,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &HashMap<u32, &String>,
    ) -> Result<Arc<Expr>> {
        if f.arguments.len() != 1 {
            return substrait_err!("Expect one argument for {fn_name} expr");
        }
        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for {fn_name} expr");
        };
        let arg = parse_rex(ctx, expr_substrait, input_schema, extensions)
            .await?
            .as_ref()
            .clone();
        let arg = Box::new(arg);

        let expr = match fn_name {
            "not" => Expr::Not(arg),
            "negative" => Expr::Negative(arg),
            "is_null" => Expr::IsNull(arg),
            "is_not_null" => Expr::IsNotNull(arg),
            "is_true" => Expr::IsTrue(arg),
            "is_false" => Expr::IsFalse(arg),
            "is_not_true" => Expr::IsNotTrue(arg),
            "is_not_false" => Expr::IsNotFalse(arg),
            "is_unknown" => Expr::IsUnknown(arg),
            "is_not_unknown" => Expr::IsNotUnknown(arg),
            _ => return not_impl_err!("Unsupported builtin expression: {}", fn_name),
        };

        Ok(Arc::new(expr))
    }

    async fn build_like_expr(
        ctx: &SessionContext,
        case_insensitive: bool,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &HashMap<u32, &String>,
    ) -> Result<Arc<Expr>> {
        let fn_name = if case_insensitive { "ILIKE" } else { "LIKE" };
        if f.arguments.len() != 3 {
            return substrait_err!("Expect three arguments for `{fn_name}` expr");
        }

        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let expr = parse_rex(ctx, expr_substrait, input_schema, extensions)
            .await?
            .as_ref()
            .clone();
        let Some(ArgType::Value(pattern_substrait)) = &f.arguments[1].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let pattern =
            parse_rex(ctx, pattern_substrait, input_schema, extensions)
                .await?
                .as_ref()
                .clone();
        let Some(ArgType::Value(escape_char_substrait)) = &f.arguments[2].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let escape_char_expr =
            parse_rex(ctx, escape_char_substrait, input_schema, extensions)
                .await?
                .as_ref()
                .clone();
        let Expr::Literal(ScalarValue::Utf8(escape_char)) = escape_char_expr else {
            return substrait_err!(
                "Expect Utf8 literal for escape char, but found {escape_char_expr:?}"
            );
        };

        Ok(Arc::new(Expr::Like(Like {
            negated: false,
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            escape_char: escape_char.map(|c| c.chars().next().unwrap()),
            case_insensitive,
        })))
    }
}


#[async_recursion]
async fn parse_rex(ctx: &SessionContext, e: &Expression, input_schema: &DFSchema, extensions: &HashMap<u32, &String>) -> Result<Arc<Expr>> {
    match &e.rex_type {
        Some(RexType::Selection(field_ref)) => Ok(Arc::new(
            from_substrait_field_reference(field_ref, input_schema)?,
        )),
        Some(RexType::ScalarFunction(f)) => {
            let fn_name = extensions.get(&f.function_reference).ok_or_else(|| {
                DataFusionError::NotImplemented(format!(
                    "Aggregated function not found: function reference = {:?}",
                    f.function_reference
                ))
            })?;

            // Convert function arguments from Substrait to DataFusion
            async fn decode_arguments(
                ctx: &SessionContext,
                input_schema: &DFSchema,
                extensions: &HashMap<u32, &String>,
                function_args: &[FunctionArgument],
            ) -> Result<Vec<Expr>> {
                let mut args = Vec::with_capacity(function_args.len());
                for arg in function_args {
                    let arg_expr = match &arg.arg_type {
                        Some(ArgType::Value(e)) => {
                            parse_rex(ctx, e, input_schema, extensions).await
                        }
                        _ => not_impl_err!(
                            "Aggregated function argument non-Value type not supported"
                        ),
                    }?;
                    args.push(arg_expr.as_ref().clone());
                }
                Ok(args)
            }

            let fn_type = scalar_function_type_from_str(ctx, fn_name)?;
            match fn_type {
                ScalarFunctionType::Builtin(fun) => {
                    let args = decode_arguments(
                        ctx,
                        input_schema,
                        extensions,
                        f.arguments.as_slice(),
                    )
                        .await?;
                    Ok(Arc::new(Expr::ScalarFunction(expr::ScalarFunction::new(
                        fun, args,
                    ))))
                }
                ScalarFunctionType::Op(op) => {
                    if f.arguments.len() != 2 {
                        return not_impl_err!(
                            "Expect two arguments for binary operator {op:?}"
                        );
                    }
                    let lhs = &f.arguments[0].arg_type;
                    let rhs = &f.arguments[1].arg_type;

                    match (lhs, rhs) {
                        (Some(ArgType::Value(l)), Some(ArgType::Value(r))) => {
                            Ok(Arc::new(Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(
                                    parse_rex(ctx, l, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                ),
                                op,
                                right: Box::new(
                                    parse_rex(ctx, r, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                ),
                            })))
                        }
                        (l, r) => not_impl_err!(
                            "Invalid arguments for binary expression: {l:?} and {r:?}"
                        ),
                    }
                }
                ScalarFunctionType::Expr(builder) => {
                    builder.build(ctx, f, input_schema, extensions).await
                }
                ScalarFunctionType::Udf(fun) => {
                    let args = decode_arguments(
                        ctx,
                        input_schema,
                        extensions,
                        f.arguments.as_slice(),
                    )
                        .await?;
                    Ok(Arc::new(Expr::ScalarFunction(
                        expr::ScalarFunction::new_udf(fun, args),
                    )))
                }
            }
        }
        Some(RexType::Literal(lit)) => {
            let scalar_value = from_substrait_literal(lit)?;
            Ok(Arc::new(Expr::Literal(scalar_value)))
        }
        _ => unimplemented!()
    }
}


pub(crate) fn from_substrait_literal(lit: &Literal) -> Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        Some(LiteralType::Timestamp(t)) => match lit.type_variation_reference {
            TIMESTAMP_SECOND_TYPE_REF => ScalarValue::TimestampSecond(Some(*t), None),
            TIMESTAMP_MILLI_TYPE_REF => ScalarValue::TimestampMillisecond(Some(*t), None),
            TIMESTAMP_MICRO_TYPE_REF => ScalarValue::TimestampMicrosecond(Some(*t), None),
            TIMESTAMP_NANO_TYPE_REF => ScalarValue::TimestampNanosecond(Some(*t), None),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_REF => ScalarValue::LargeBinary(Some(b.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::FixedBinary(b)) => {
            ScalarValue::FixedSizeBinary(b.len() as _, Some(b.clone()))
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d
                .value
                .clone()
                .try_into()
                .or(substrait_err!("Failed to parse decimal value"))?;
            let p = d.precision.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal precision: {e}")
            })?;
            let s = d.scale.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal scale: {e}")
            })?;
            ScalarValue::Decimal128(
                Some(std::primitive::i128::from_le_bytes(value)),
                p,
                s,
            )
        }
        Some(LiteralType::Null(ntype)) => from_substrait_null(ntype)?,
        _ => return not_impl_err!("Unsupported literal_type: {:?}", lit.literal_type),
    };

    Ok(scalar_value)
}

pub fn name_to_op(name: &str) -> Result<Operator> {
    match name {
        "equal" => Ok(Operator::Eq),
        "not_equal" => Ok(Operator::NotEq),
        "lt" => Ok(Operator::Lt),
        "lte" => Ok(Operator::LtEq),
        "gt" => Ok(Operator::Gt),
        "gte" => Ok(Operator::GtEq),
        "add" => Ok(Operator::Plus),
        "subtract" => Ok(Operator::Minus),
        "multiply" => Ok(Operator::Multiply),
        "divide" => Ok(Operator::Divide),
        "mod" => Ok(Operator::Modulo),
        "and" => Ok(Operator::And),
        "or" => Ok(Operator::Or),
        "is_distinct_from" => Ok(Operator::IsDistinctFrom),
        "is_not_distinct_from" => Ok(Operator::IsNotDistinctFrom),
        "regex_match" => Ok(Operator::RegexMatch),
        "regex_imatch" => Ok(Operator::RegexIMatch),
        "regex_not_match" => Ok(Operator::RegexNotMatch),
        "regex_not_imatch" => Ok(Operator::RegexNotIMatch),
        "bitwise_and" => Ok(Operator::BitwiseAnd),
        "bitwise_or" => Ok(Operator::BitwiseOr),
        "str_concat" => Ok(Operator::StringConcat),
        "at_arrow" => Ok(Operator::AtArrow),
        "arrow_at" => Ok(Operator::ArrowAt),
        "bitwise_xor" => Ok(Operator::BitwiseXor),
        "bitwise_shift_right" => Ok(Operator::BitwiseShiftRight),
        "bitwise_shift_left" => Ok(Operator::BitwiseShiftLeft),
        _ => not_impl_err!("Unsupported function name: {name:?}"),
    }
}

enum ScalarFunctionType {
    Builtin(BuiltinScalarFunction),
    Op(Operator),
    Expr(BuiltinExprBuilder),
    Udf(Arc<ScalarUDF>),
}

fn scalar_function_type_from_str(
    ctx: &SessionContext,
    name: &str,
) -> Result<ScalarFunctionType> {
    let s = ctx.state();
    if let Some(func) = s.scalar_functions().get(name) {
        return Ok(ScalarFunctionType::Udf(func.to_owned()));
    }

    if let Ok(op) = name_to_op(name) {
        return Ok(ScalarFunctionType::Op(op));
    }

    if let Ok(fun) = BuiltinScalarFunction::from_str(name) {
        return Ok(ScalarFunctionType::Builtin(fun));
    }

    if let Some(builder) = BuiltinExprBuilder::try_from_name(name) {
        return Ok(ScalarFunctionType::Expr(builder));
    }

    not_impl_err!("Unsupported function name: {name:?}")
}

fn from_substrait_null(null_type: &Type) -> Result<ScalarValue> {
    if let Some(kind) = &null_type.kind {
        match kind {
            r#type::Kind::Bool(_) => Ok(ScalarValue::Boolean(None)),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int8(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int16(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt16(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int32(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt32(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int64(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(ScalarValue::Float32(None)),
            r#type::Kind::Fp64(_) => Ok(ScalarValue::Float64(None)),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_REF => Ok(ScalarValue::TimestampSecond(None, None)),
                TIMESTAMP_MILLI_TYPE_REF => {
                    Ok(ScalarValue::TimestampMillisecond(None, None))
                }
                TIMESTAMP_MICRO_TYPE_REF => {
                    Ok(ScalarValue::TimestampMicrosecond(None, None))
                }
                TIMESTAMP_NANO_TYPE_REF => {
                    Ok(ScalarValue::TimestampNanosecond(None, None))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_REF => Ok(ScalarValue::Date32(None)),
                DATE_64_TYPE_REF => Ok(ScalarValue::Date64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(ScalarValue::Binary(None)),
                LARGE_CONTAINER_TYPE_REF => Ok(ScalarValue::LargeBinary(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            // FixedBinary is not supported because `None` doesn't have length
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(ScalarValue::Utf8(None)),
                LARGE_CONTAINER_TYPE_REF => Ok(ScalarValue::LargeUtf8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Decimal(d) => Ok(ScalarValue::Decimal128(
                None,
                d.precision as u8,
                d.scale as i8,
            )),
            _ => not_impl_err!("Unsupported Substrait type: {kind:?}"),
        }
    } else {
        not_impl_err!("Null type without kind is not supported")
    }
}

fn from_substrait_field_reference(
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> Result<Expr> {
    match &field_ref.reference_type {
        Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
            Some(ReferenceType::MapKey(x)) => match &x.child.as_ref() {
                Some(_) => not_impl_err!(
                    "Direct reference StructField with child is not supported"
                ),
                None => {
                    // let column = input_schema.field(x.field as usize).qualified_column();
                    // Ok(Expr::Column(Column {
                    //     relation: column.relation,
                    //     name: column.name,
                    // }))
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
                    let column = input_schema
                        .field_with_unqualified_name(&field_name)?
                        .qualified_column();
                    Ok(Expr::Column(Column {
                        relation: column.relation,
                        name: column.name,
                    }))
                }
            },
            _ => not_impl_err!(
                "Direct reference with types other than MapKey is not supported"
            ),
        },
        _ => not_impl_err!("unsupported field ref type"),
    }
}

mod tests {
    use std::result::Result;

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
        // let options = ParquetReadOptions::default();
        // let table_path = "/var/folders/_b/qyl87wbn1119cvw8kts6fqtw0000gn/T/lakeSource/type/part-00000-97db3149-f99e-404a-aa9a-2af4ab3f7a44_00000.c000.parquet";
        // // let df = ctx.read_parquet(table_path, options).await.unwrap();
        // let _ = ctx.register_parquet("type_info", table_path, options).await;
        // let test_sql = "select * from type_info";
        // let df = ctx.sql(test_sql).await.unwrap();
        let byte_array = [
            10, 30, 8, 1, 18, 26, 47, 102, 117, 110, 99, 116, 105, 111, 110, 115, 95, 99, 111, 109, 112, 97, 114, 105,
            115, 111, 110, 46, 121, 97, 109, 108, 18, 16, 26, 14, 8, 1, 26, 10, 103, 116, 58, 97, 110, 121, 95, 97,
            110, 121, 26, 72, 18, 70, 10, 68, 10, 66, 10, 2, 10, 0, 18, 4, 18, 2, 24, 2, 26, 41, 26, 39, 26, 4, 10, 2,
            16, 1, 34, 22, 26, 20, 18, 18, 10, 14, 10, 12, 10, 10, 98, 5, 109, 111, 110, 101, 121, -112, 3, 1, 34, 0,
            34, 7, 26, 5, 10, 3, 40, -12, 3, 58, 11, 10, 9, 116, 121, 112, 101, 95, 105, 110, 102, 111,
        ];
        let byte_array = unsafe { std::mem::transmute::<&[i8], &[u8]>(&byte_array[..]) };
        let plan = Plan::decode(&byte_array[..]).unwrap();
        println!("{:?}", plan);
        // let e = Parser::parse_proto(&ctx, &plan, )).unwrap();
        // println!("{:?}",e);
        // let df = df.filter(e).unwrap();
        // let df = df.explain(true, true).unwrap();
        // df.show().await.unwrap()
    }
}
