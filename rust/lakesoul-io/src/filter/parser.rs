// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::ops::Not;
use std::sync::Arc;

use anyhow::anyhow;
use arrow_schema::{DataType, Field, Fields, SchemaRef};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{SessionContext, col};
use datafusion::scalar::ScalarValue;
use datafusion_common::DataFusionError::{External, Internal};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    DefaultSubstraitConsumer, from_substrait_extended_expr, from_substrait_rex,
};
use datafusion_substrait::substrait::proto::expression::field_reference::ReferenceType;
use datafusion_substrait::substrait::proto::expression::literal::LiteralType;
use datafusion_substrait::substrait::proto::expression::reference_segment::StructField;
use datafusion_substrait::substrait::proto::expression::{
    Literal, RexType, reference_segment,
};
use datafusion_substrait::substrait::proto::function_argument::ArgType;
use datafusion_substrait::substrait::proto::plan_rel::RelType::Root;
use datafusion_substrait::substrait::proto::rel::RelType::Read;
use datafusion_substrait::substrait::proto::r#type::Nullability;
use datafusion_substrait::substrait::proto::{
    Expression, ExtendedExpression, FunctionArgument, Plan, PlanRel, Rel, RelRoot,
};
#[allow(deprecated)]
use datafusion_substrait::variation_const::TIMESTAMP_MICRO_TYPE_VARIATION_REF;
use tokio::runtime::{Builder, Handle};
use tokio::task;

/// The parser for parsing the filter string from Java or Subtrait Plan.
pub struct Parser {}

pub enum FilterContainer {
    RawBuf(Vec<u8>),
    String(String),
    Plan(Plan),
    ExtenedExpr(ExtendedExpression),
}

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
            (
                op.to_string(),
                left.trim().to_string(),
                right[0..].trim().to_string(),
            )
        } else {
            (
                op.to_string(),
                left.trim().to_string(),
                right[1..].trim().to_string(),
            )
        };
        Ok(res)
    }

    fn parse_literal(field: Arc<Field>, value: String) -> Result<Expr> {
        let data_type = field.data_type().clone();
        let expr = match data_type {
            DataType::Decimal128(precision, scale) => {
                if precision <= 18 {
                    Expr::Literal(ScalarValue::Decimal128(
                        Some(value.parse::<i128>().map_err(|e| External(Box::new(e)))?),
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
                value.parse::<bool>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Binary => Expr::Literal(ScalarValue::Binary(
                Parser::parse_binary_array(value.as_str())?,
            )),
            DataType::Float32 => Expr::Literal(ScalarValue::Float32(Some(
                value.parse::<f32>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Float64 => Expr::Literal(ScalarValue::Float64(Some(
                value.parse::<f64>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int8 => Expr::Literal(ScalarValue::Int8(Some(
                value.parse::<i8>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int16 => Expr::Literal(ScalarValue::Int16(Some(
                value.parse::<i16>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int32 => Expr::Literal(ScalarValue::Int32(Some(
                value.parse::<i32>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Int64 => Expr::Literal(ScalarValue::Int64(Some(
                value.parse::<i64>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Date32 => Expr::Literal(ScalarValue::Date32(Some(
                value.parse::<i32>().map_err(|e| External(Box::new(e)))?,
            ))),
            DataType::Timestamp(_, _) => {
                Expr::Literal(ScalarValue::TimestampMicrosecond(
                    Some(value.parse::<i64>().map_err(|e| External(Box::new(e)))?),
                    Some(crate::constant::LAKESOUL_TIMEZONE.into()),
                ))
            }
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

    /// caller may only pass MapKey for field reference,
    /// we need to change it to StructField since from_substrait_field_reference
    fn modify_substrait_argument(
        arguments: &mut Vec<FunctionArgument>,
        df_schema: &DFSchema,
    ) {
        for arg in arguments {
            match &mut arg.arg_type {
                Some(ArgType::Value(Expression {
                    rex_type: Some(RexType::Selection(f)),
                })) => {
                    if let Some(ReferenceType::DirectReference(reference_segment)) =
                        &mut f.reference_type
                    {
                        if let Some(reference_segment::ReferenceType::MapKey(map_key)) =
                            &mut reference_segment.reference_type
                        {
                            if let Some(Literal {
                                literal_type: Some(LiteralType::String(name)),
                                ..
                            }) = &map_key.map_key
                            {
                                if let Some(idx) =
                                    df_schema.index_of_column_by_name(None, name.as_ref())
                                {
                                    reference_segment.reference_type = Some(
                                        reference_segment::ReferenceType::StructField(
                                            Box::new(StructField {
                                                field: idx as i32,
                                                child: None,
                                            }),
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }
                Some(ArgType::Value(Expression {
                    rex_type: Some(RexType::ScalarFunction(f)),
                })) => {
                    Self::modify_substrait_argument(&mut f.arguments, df_schema);
                }
                Some(ArgType::Value(Expression {
                    rex_type: Some(RexType::Literal(literal)),
                })) => match literal.literal_type {
                    Some(LiteralType::Timestamp(_))
                    | Some(LiteralType::TimestampTz(_)) => {
                        // for compatibility with substrait old java version
                        // where type variation ref field is not filled in java
                        #[allow(deprecated)]
                        {
                            literal.type_variation_reference =
                                TIMESTAMP_MICRO_TYPE_VARIATION_REF;
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }

    /// Parse the [`datafusion_substrait::substrait::proto::Plan`] to the [`datafusion::logical_expr::Expr`].
    pub(crate) fn parse_substrait_plan(plan: Plan, df_schema: &DFSchema) -> Result<Expr> {
        let handle = Handle::try_current();
        let closure = async {
            let ctx = SessionContext::default();
            if let Some(PlanRel {
                rel_type:
                    Some(Root(RelRoot {
                        input:
                            Some(Rel {
                                rel_type: Some(Read(mut read_rel)),
                            }),
                        ..
                    })),
            }) = plan.relations.first().cloned()
            {
                if let Some(expression) = &mut read_rel.filter {
                    let extensions = Extensions::try_from(&plan.extensions)?;
                    if let Some(RexType::ScalarFunction(f)) = &mut expression.rex_type {
                        Self::modify_substrait_argument(&mut f.arguments, df_schema);
                    }
                    let state = ctx.state();
                    let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
                    return from_substrait_rex(&consumer, expression, df_schema).await;
                }
            }
            Err(Internal(format!(
                "encountered wrong substrait plan {:?}",
                plan
            )))
        };
        match handle {
            Ok(handle) => task::block_in_place(move || handle.block_on(closure)),
            _ => {
                let runtime = Builder::new_current_thread()
                    .build()
                    .map_err(|e| External(Box::new(e)))?;
                runtime.block_on(closure)
            }
        }
    }

    pub async fn parse_filter_container(
        context: &SessionContext,
        container: FilterContainer,
    ) -> Result<Vec<Expr>> {
        match container {
            FilterContainer::RawBuf(items) => {
                // parse bytes to other types
                todo!()
            }
            FilterContainer::String(_) => todo!(),
            FilterContainer::Plan(plan) => todo!(),
            FilterContainer::ExtenedExpr(extended_expression) => {
                let expr_container =
                    from_substrait_extended_expr(&context.state(), &extended_expression)
                        .await?;
                Ok(expr_container
                    .exprs
                    .into_iter()
                    .map(|(expr, _)| expr)
                    .collect::<Vec<Expr>>())
            }
        }
    }
}

fn qualified_expr(expr_str: &str, schema: SchemaRef) -> Option<(Expr, Arc<Field>)> {
    if let Ok(field) = schema.field_with_name(expr_str) {
        Some((
            col(Column::new_unqualified(expr_str)),
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
