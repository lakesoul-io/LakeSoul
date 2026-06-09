// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::ops::Not;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, SchemaRef};
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{SessionContext, col};
use datafusion::scalar::ScalarValue;
use datafusion_common::DataFusionError::{self, External, Internal};
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
    Expression, ExtendedExpression, FunctionArgument, NamedStruct, Plan, PlanRel, Rel,
    RelRoot,
};
#[allow(deprecated)]
use datafusion_substrait::variation_const::TIMESTAMP_MICRO_TYPE_VARIATION_REF;
use prost::Message;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::report;
use tokio::runtime::{Builder, Handle};
use tokio::task;

/// The parser for parsing the filter string from Java or Subtrait Plan.
pub struct Parser;

pub enum FilterContainer {
    RawBuf(Vec<u8>),
    String(String),
    Plan(Plan),
    ExtenedExpr(ExtendedExpression),
}

impl Parser {
    pub fn parse(filter_str: String, schema: SchemaRef) -> Result<Expr> {
        info!("parsing filter str {}", filter_str);
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
                        _ => Expr::Literal(ScalarValue::Boolean(Some(true)), None),
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
                        _ => Expr::Literal(ScalarValue::Boolean(Some(true)), None),
                    }
                }
            } else {
                Expr::Literal(ScalarValue::Boolean(Some(false)), None)
            }
        };
        Ok(expr)
    }

    fn parse_filter_str(filter: String) -> Result<(String, String, String)> {
        let op_offset = filter
            .find('(')
            .ok_or(External(report!("wrong filter str").into_boxed_error()))?;
        let (op, filter) = filter.split_at(op_offset);
        if !filter.ends_with(')') {
            return Err(External(report!("wrong filter str").into_boxed_error()));
        }
        let filter = &filter[1..filter.len() - 1];
        let mut k: usize = 0;
        let mut left_offset: usize = 0;
        let mut offset_counter: usize = 0;
        for ch in filter.chars() {
            match ch {
                '(' => k += 1,
                ')' => k -= 1,
                ',' if k == 0 && left_offset == 0 => left_offset = offset_counter,
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
                    Expr::Literal(
                        ScalarValue::Decimal128(
                            Some(
                                value
                                    .parse::<i128>()
                                    .map_err(|e| External(Box::new(e)))?,
                            ),
                            precision,
                            scale,
                        ),
                        None,
                    )
                } else {
                    let binary_vec = Parser::parse_binary_array(value.as_str())?.ok_or(
                        External(report!("parse binary array failed").into_boxed_error()),
                    )?;
                    let mut arr = [0u8; 16];
                    for idx in 0..binary_vec.len() {
                        arr[idx + 16 - binary_vec.len()] = binary_vec[idx];
                    }
                    Expr::Literal(
                        ScalarValue::Decimal128(
                            Some(i128::from_be_bytes(arr)),
                            precision,
                            scale,
                        ),
                        None,
                    )
                }
            }
            DataType::Boolean => Expr::Literal(
                ScalarValue::Boolean(Some(
                    value.parse::<bool>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Binary => Expr::Literal(
                ScalarValue::Binary(Parser::parse_binary_array(value.as_str())?),
                None,
            ),
            DataType::Float32 => Expr::Literal(
                ScalarValue::Float32(Some(
                    value.parse::<f32>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Float64 => Expr::Literal(
                ScalarValue::Float64(Some(
                    value.parse::<f64>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Int8 => Expr::Literal(
                ScalarValue::Int8(Some(
                    value.parse::<i8>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Int16 => Expr::Literal(
                ScalarValue::Int16(Some(
                    value.parse::<i16>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Int32 => Expr::Literal(
                ScalarValue::Int32(Some(
                    value.parse::<i32>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Int64 => Expr::Literal(
                ScalarValue::Int64(Some(
                    value.parse::<i64>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Date32 => Expr::Literal(
                ScalarValue::Date32(Some(
                    value.parse::<i32>().map_err(|e| External(Box::new(e)))?,
                )),
                None,
            ),
            DataType::Timestamp(_, _) => Expr::Literal(
                ScalarValue::TimestampMicrosecond(
                    Some(value.parse::<i64>().map_err(|e| External(Box::new(e)))?),
                    Some(crate::constant::LAKESOUL_TIMEZONE.into()),
                ),
                None,
            ),
            DataType::Utf8 => {
                let value = value.as_str()[8..value.len() - 2].to_string();
                Expr::Literal(ScalarValue::Utf8(Some(value)), None)
            }
            _ => Expr::Literal(ScalarValue::Utf8(Some(value)), None),
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
                        && let Some(reference_segment::ReferenceType::MapKey(map_key)) =
                            &mut reference_segment.reference_type
                        && let Some(Literal {
                            literal_type: Some(LiteralType::String(name)),
                            ..
                        }) = &map_key.map_key
                        && let Some(idx) =
                            df_schema.index_of_column_by_name(None, name.as_ref())
                    {
                        reference_segment.reference_type =
                            Some(reference_segment::ReferenceType::StructField(
                                Box::new(StructField {
                                    field: idx as i32,
                                    child: None,
                                }),
                            ));
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
    pub(crate) fn parse_substrait_plan(
        mut plan: Plan,
        df_schema: &DFSchema,
    ) -> Result<Expr> {
        Self::normalize_substrait_plan_schema_names(&mut plan, df_schema);

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
                && let Some(expression) = &mut read_rel.filter
            {
                let extensions = Extensions::try_from(&plan.extensions)?;
                if let Some(RexType::ScalarFunction(f)) = &mut expression.rex_type {
                    Self::modify_substrait_argument(&mut f.arguments, df_schema);
                }
                let state = ctx.state();
                let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
                return from_substrait_rex(&consumer, expression, df_schema).await;
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

    fn normalize_substrait_plan_schema_names(plan: &mut Plan, df_schema: &DFSchema) {
        let names = Self::flatten_schema_names(df_schema);
        for relation in &mut plan.relations {
            if let Some(Root(RelRoot {
                input:
                    Some(Rel {
                        rel_type: Some(Read(read_rel)),
                        ..
                    }),
                ..
            })) = &mut relation.rel_type
                && let Some(base_schema) = &mut read_rel.base_schema
            {
                Self::normalize_named_struct_schema_names(base_schema, &names);
            }
        }
    }

    fn normalize_substrait_extended_expr_schema_names(
        extended_expression: &mut ExtendedExpression,
        df_schema: &DFSchema,
    ) {
        let names = Self::flatten_schema_names(df_schema);
        if let Some(base_schema) = &mut extended_expression.base_schema {
            Self::normalize_named_struct_schema_names(base_schema, &names);
        }
    }

    fn normalize_named_struct_schema_names(
        base_schema: &mut NamedStruct,
        names: &[String],
    ) {
        if !names.is_empty() && base_schema.names.len() < names.len() {
            // PyArrow/DuckDB may serialize only top-level names; DataFusion
            // expects the nested, depth-first list produced by its own producer.
            base_schema.names = names.to_vec();
        }
    }

    fn is_valid_substrait_plan(plan: &Plan) -> bool {
        !plan.relations.is_empty()
    }

    fn is_valid_substrait_extended_expr(
        extended_expression: &ExtendedExpression,
    ) -> bool {
        extended_expression.base_schema.is_some()
            && !extended_expression.referred_expr.is_empty()
    }

    fn flatten_schema_names(df_schema: &DFSchema) -> Vec<String> {
        let mut names = vec![];
        for (idx, field) in df_schema.as_arrow().fields().iter().enumerate() {
            Self::flatten_field_names(field.as_ref(), false, idx, &mut names);
        }
        names
    }

    fn flatten_field_names(
        field: &Field,
        skip_self: bool,
        fallback_idx: usize,
        names: &mut Vec<String>,
    ) {
        if !skip_self {
            names.push(Self::field_name_or_fallback(field, fallback_idx));
        }

        match field.data_type() {
            DataType::Struct(fields) => {
                for (idx, field) in fields.iter().enumerate() {
                    Self::flatten_field_names(field.as_ref(), false, idx, names);
                }
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => {
                Self::flatten_field_names(field.as_ref(), true, fallback_idx, names);
            }
            DataType::FixedSizeList(field, _) => {
                Self::flatten_field_names(field.as_ref(), true, fallback_idx, names);
            }
            DataType::Map(field, _) => {
                if let DataType::Struct(fields) = field.data_type() {
                    for (idx, field) in fields.iter().enumerate() {
                        Self::flatten_field_names(field.as_ref(), true, idx, names);
                    }
                }
            }
            _ => {}
        }
    }

    fn field_name_or_fallback(field: &Field, fallback_idx: usize) -> String {
        if field.name().is_empty() {
            format!("field_{fallback_idx}")
        } else {
            field.name().to_string()
        }
    }

    pub async fn parse_filter_container(
        context: &SessionContext,
        schema: &DFSchema,
        container: FilterContainer,
    ) -> Result<Vec<Expr>> {
        match container {
            FilterContainer::RawBuf(buf) => {
                let c = parse_buf(&buf)?;
                Box::pin(Parser::parse_filter_container(context, schema, c)).await
            }
            FilterContainer::String(s) => {
                let arrow_schema = Arc::new(schema.as_arrow().clone());
                Ok(vec![Parser::parse(s, arrow_schema)?])
            }
            FilterContainer::Plan(plan) => {
                Ok(vec![Parser::parse_substrait_plan(plan, schema)?])
            }
            FilterContainer::ExtenedExpr(mut extended_expression) => {
                Parser::normalize_substrait_extended_expr_schema_names(
                    &mut extended_expression,
                    schema,
                );
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

// never buf -> FilterContainer::RawBuf
fn parse_buf(buf: &[u8]) -> Result<FilterContainer, DataFusionError> {
    if let Ok(expr) = ExtendedExpression::decode(buf)
        && Parser::is_valid_substrait_extended_expr(&expr)
    {
        return Ok(FilterContainer::ExtenedExpr(expr));
    }

    if let Ok(p) = Plan::decode(buf)
        && Parser::is_valid_substrait_plan(&p)
    {
        return Ok(FilterContainer::Plan(p));
    }

    Err(DataFusionError::Substrait(
        "Parse substrait failed. Unsupported filter type".to_string(),
    ))
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
    use super::*;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion_substrait::substrait::proto::ExpressionReference;
    use prost::Message;

    fn flickr_schema() -> Schema {
        Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("image_blob", DataType::Binary, true),
            Field::new("width", DataType::Int32, true),
            Field::new("height", DataType::Int32, true),
            Field::new(
                "captions",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "bboxes",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Arc::new(Field::new(
                            "chain_ids",
                            DataType::List(Arc::new(Field::new_list_field(
                                DataType::Utf8,
                                true,
                            ))),
                            true,
                        )),
                        Arc::new(Field::new("xmin", DataType::Int32, true)),
                        Arc::new(Field::new("ymin", DataType::Int32, true)),
                        Arc::new(Field::new("xmax", DataType::Int32, true)),
                        Arc::new(Field::new("ymax", DataType::Int32, true)),
                    ])),
                    true,
                ))),
                true,
            ),
        ])
    }

    #[test]
    fn flatten_schema_names_includes_nested_struct_fields_under_lists() -> Result<()> {
        let schema = flickr_schema();
        let df_schema = DFSchema::try_from(schema)?;

        assert_eq!(
            Parser::flatten_schema_names(&df_schema),
            vec![
                "filename",
                "image_blob",
                "width",
                "height",
                "captions",
                "bboxes",
                "chain_ids",
                "xmin",
                "ymin",
                "xmax",
                "ymax",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[test]
    fn normalize_extended_expression_schema_names_fills_nested_names() -> Result<()> {
        let schema = flickr_schema();
        let df_schema = DFSchema::try_from(schema)?;
        let mut extended_expression = ExtendedExpression {
            base_schema: Some(NamedStruct {
                names: vec![
                    "filename".to_string(),
                    "image_blob".to_string(),
                    "width".to_string(),
                    "height".to_string(),
                    "captions".to_string(),
                    "bboxes".to_string(),
                ],
                r#struct: None,
            }),
            ..Default::default()
        };

        Parser::normalize_substrait_extended_expr_schema_names(
            &mut extended_expression,
            &df_schema,
        );

        assert_eq!(
            extended_expression.base_schema.unwrap().names,
            Parser::flatten_schema_names(&df_schema)
        );

        Ok(())
    }

    #[test]
    fn parse_buf_prefers_valid_extended_expression_over_plan_decode() -> Result<()> {
        let extended_expression = ExtendedExpression {
            base_schema: Some(NamedStruct {
                names: vec!["filename".to_string()],
                r#struct: None,
            }),
            referred_expr: vec![ExpressionReference::default()],
            ..Default::default()
        };

        match parse_buf(&extended_expression.encode_to_vec())? {
            FilterContainer::ExtenedExpr(_) => Ok(()),
            FilterContainer::Plan(_) => Err(DataFusionError::Substrait(
                "expected ExtendedExpression".to_string(),
            )),
            FilterContainer::RawBuf(_) | FilterContainer::String(_) => Err(
                DataFusionError::Substrait("unexpected filter container".to_string()),
            ),
        }
    }
}
