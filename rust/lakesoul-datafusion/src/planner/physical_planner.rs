// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;

use datafusion::common::{DFSchema, SchemaExt, plan_err, not_impl_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::logical_expr::expr::*;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::expressions::{binary, like};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

use async_trait::async_trait;

use datafusion::logical_expr::{DmlStatement, WriteOp};
use lakesoul_io::helpers::{column_names_to_physical_expr, column_names_to_physical_sort_expr};
use lakesoul_io::repartition::RepartitionByRangeAndHashExec;
use log::{debug, info};

use crate::lakesoul_table::LakeSoulTable;

use datafusion::logical_expr::{binary_expr, Operator};
use datafusion::physical_plan::expressions::{self, Column, GetFieldAccessExpr, GetIndexedFieldExpr};
use datafusion::physical_expr::var_provider::{is_system_variables, VarType};
use datafusion::physical_plan::{functions, udf};

pub struct LakeSoulPhysicalPlanner {
    default_planner: DefaultPhysicalPlanner,
}

impl LakeSoulPhysicalPlanner {
    pub fn new() -> Self {
        Self {
            default_planner: DefaultPhysicalPlanner::default(),
        }
    }
}

#[async_trait]
impl PhysicalPlanner for LakeSoulPhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        info!("create_physical_plan: {:?}", &logical_plan);
        match logical_plan {
            LogicalPlan::Filter(filter) => {
                info!("filter: {:?}", &filter.predicate);
                let physical_input = self.create_physical_plan(&filter.input, session_state).await?;
                let input_schema = physical_input.as_ref().schema();
                let input_dfschema = filter.input.schema();
                info!("input_schema: {:?}", &input_schema);
                info!("input_dfschema: {:?}", &input_dfschema);

                let runtime_expr = self.create_physical_expr(
                    &filter.predicate,
                    input_dfschema,
                    &input_schema,
                    session_state,
                )?;
                Ok(Arc::new(FilterExec::try_new(runtime_expr, physical_input)?))
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::InsertInto,
                input,
                ..
            }) => {
                info!("insert into: {:?}, {:?}", &table_name, &input);
                let name = table_name.table();
                let schema = table_name.schema();
                // let schema = session_state.schema_for_ref(table_name)?;
                let lakesoul_table = LakeSoulTable::for_namespace_and_name(schema.unwrap_or("default"), name)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                match lakesoul_table.as_sink_provider(session_state).await {
                    Ok(provider) => {
                        let physical_input = self.create_physical_plan(input, session_state).await?;

                        if lakesoul_table.primary_keys().is_empty()
                            && !lakesoul_table
                                .schema()
                                .logically_equivalent_names_and_types(&Schema::from(input.schema().as_ref()))
                        {
                            return Err(DataFusionError::Plan(
                                // Return an error if schema of the input query does not match with the table schema.
                                "Inserting query must have the same schema with the table.".to_string(),
                            ));
                        }

                        let physical_input = if !lakesoul_table.primary_keys().is_empty()
                            || !lakesoul_table.range_partitions().is_empty()
                        {
                            let input_schema = physical_input.schema();
                            let input_dfschema = input.as_ref().schema();
                            let sort_expr = column_names_to_physical_sort_expr(
                                [
                                    lakesoul_table.range_partitions().clone(),
                                    lakesoul_table.primary_keys().clone(),
                                ]
                                .concat()
                                .as_slice(),
                                input_dfschema,
                                &input_schema,
                                session_state,
                            )?;
                            let hash_partitioning_expr = column_names_to_physical_expr(
                                lakesoul_table.primary_keys(),
                                input_dfschema,
                                &input_schema,
                                session_state,
                            )?;

                            let hash_partitioning =
                                Partitioning::Hash(hash_partitioning_expr, lakesoul_table.hash_bucket_num());
                            let range_partitioning_expr = column_names_to_physical_expr(
                                lakesoul_table.range_partitions(),
                                input_dfschema,
                                &input_schema,
                                session_state,
                            )?;
                            let sort_exec = Arc::new(SortExec::new(sort_expr, physical_input));
                            Arc::new(RepartitionByRangeAndHashExec::try_new(
                                sort_exec,
                                range_partitioning_expr,
                                hash_partitioning,
                            )?)
                        } else {
                            physical_input
                        };

                        provider.insert_into(session_state, physical_input, false).await
                    }
                    Err(e) => return Err(DataFusionError::External(Box::new(e))),
                }
            }
            LogicalPlan::Statement(statement) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                let name = statement.name();
                
                Err(DataFusionError::NotImplemented(format!(
                    "Unsupported logical plan: Statement({name})"
                )))
            }
            _ => {
                self.default_planner
                    .create_physical_plan(logical_plan, session_state)
                    .await
            }
        }
    }

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `expr`
    ///
    /// `input_schema`: the physical schema for evaluating `expr`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        info!("create_physical_expr: {:?}", &expr);
        create_physical_expr(expr, input_dfschema, input_schema, session_state.execution_props())
    }
}

/// Create a physical expression from a logical expression ([Expr]).
///
/// # Arguments
///
/// * `e` - The logical expression
/// * `input_dfschema` - The DataFusion schema for the input, used to resolve `Column` references
///                      to qualified or unqualified fields by name.
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    if input_schema.fields.len() != input_dfschema.fields().len() {
        return Err(DataFusionError::Plan(
            format!(
                "create_physical_expr expected same number of fields, got \
                     Arrow schema with {}  and DataFusion schema with {}",
                input_schema.fields.len(),
                input_dfschema.fields().len()
            )
        ));
    }
    match e {
        Expr::Placeholder(placeholder) => {
            info!("placeholder: {:?}", &placeholder);
            let schema_type = input_schema.field(placeholder.id.replace("$", "").parse::<usize>().unwrap() - 1).data_type().clone();
            let null_value = ScalarValue::try_from(schema_type)?;
            Ok(Arc::new(Literal::new(null_value)))
        }
        Expr::Alias(Alias { expr, .. }) => Ok(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Column(c) => {
            let idx = input_dfschema.index_of_column(c)?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::ScalarVariable(_, variable_names) => {
            if is_system_variables(variable_names) {
                match execution_props.get_var_provider(VarType::System) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => plan_err!("No system variable provider found"),
                }
            } else {
                match execution_props.get_var_provider(VarType::UserDefined) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => plan_err!("No user defined variable provider found"),
                }
            }
        }
        Expr::IsTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(true))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsNotTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(true))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsNotFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::IsNotUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(
                &binary_op,
                input_dfschema,
                input_schema,
                execution_props,
            )
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // Create physical expressions for left and right operands
            let lhs = create_physical_expr(
                left,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let rhs = create_physical_expr(
                right,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            // Note that the logical planner is responsible
            // for type coercion on the arguments (e.g. if one
            // argument was originally Int32 and one was
            // Int64 they will both be coerced to Int64).
            //
            // There should be no coercion during physical
            // planning.
            binary(lhs, *op, rhs, input_schema)
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if escape_char.is_some() {
                return Err(DataFusionError::Plan(
                    "LIKE does not support escape_char".to_string()
                ));
            }
            let physical_expr = create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let physical_pattern = create_physical_expr(
                pattern,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            like(
                *negated,
                *case_insensitive,
                physical_expr,
                physical_pattern,
                input_schema,
            )
        }
        Expr::Case(case) => {
            let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = &case.expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            let when_expr = case
                .when_then_expr
                .iter()
                .map(|(w, _)| {
                    create_physical_expr(
                        w.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let then_expr = case
                .when_then_expr
                .iter()
                .map(|(_, t)| {
                    create_physical_expr(
                        t.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
                when_expr
                    .iter()
                    .zip(then_expr.iter())
                    .map(|(w, t)| (w.clone(), t.clone()))
                    .collect();
            let else_expr: Option<Arc<dyn PhysicalExpr>> =
                if let Some(e) = &case.else_expr {
                    Some(create_physical_expr(
                        e.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )?)
                } else {
                    None
                };
            Ok(expressions::case(expr, when_then_expr, else_expr)?)
        }
        Expr::Cast(Cast { expr, data_type }) => expressions::cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::TryCast(TryCast { expr, data_type }) => expressions::try_cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::Not(expr) => expressions::not(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Negative(expr) => expressions::negative(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
        ),
        Expr::IsNull(expr) => expressions::is_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::IsNotNull(expr) => expressions::is_not_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::GetIndexedField(GetIndexedField { expr, field }) => {
            let field = match field {
                GetFieldAccess::NamedStructField { name } => {
                    GetFieldAccessExpr::NamedStructField { name: name.clone() }
                }
                GetFieldAccess::ListIndex { key } => GetFieldAccessExpr::ListIndex {
                    key: create_physical_expr(
                        key,
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )?,
                },
                GetFieldAccess::ListRange { start, stop } => {
                    GetFieldAccessExpr::ListRange {
                        start: create_physical_expr(
                            start,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        )?,
                        stop: create_physical_expr(
                            stop,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        )?,
                    }
                }
            };
            Ok(Arc::new(GetIndexedFieldExpr::new(
                create_physical_expr(
                    expr,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?,
                field,
            )))
        }

        Expr::ScalarFunction(ScalarFunction { fun, args }) => {
            let physical_args = args
                .iter()
                .map(|e| {
                    create_physical_expr(e, input_dfschema, input_schema, execution_props)
                })
                .collect::<Result<Vec<_>>>()?;
            functions::create_physical_expr(
                fun,
                &physical_args,
                input_schema,
                execution_props,
            )
        }
        Expr::ScalarUDF(ScalarUDF { fun, args }) => {
            let mut physical_args = vec![];
            for e in args {
                physical_args.push(create_physical_expr(
                    e,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?);
            }
            // udfs with zero params expect null array as input
            if args.is_empty() {
                physical_args.push(Arc::new(Literal::new(ScalarValue::Null)));
            }
            udf::create_physical_expr(fun.clone().as_ref(), &physical_args, input_schema)
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let value_expr = create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let low_expr =
                create_physical_expr(low, input_dfschema, input_schema, execution_props)?;
            let high_expr = create_physical_expr(
                high,
                input_dfschema,
                input_schema,
                execution_props,
            )?;

            // rewrite the between into the two binary operators
            let binary_expr = binary(
                binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
                Operator::And,
                binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
                input_schema,
            );

            if *negated {
                expressions::not(binary_expr?)
            } else {
                binary_expr
            }
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => match expr.as_ref() {
            Expr::Literal(ScalarValue::Utf8(None)) => {
                Ok(expressions::lit(ScalarValue::Boolean(None)))
            }
            _ => {
                let value_expr = create_physical_expr(
                    expr,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?;

                let list_exprs = list
                    .iter()
                    .map(|expr| {
                        create_physical_expr(
                            expr,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                expressions::in_list(value_expr, list_exprs, negated, input_schema)
            }
        },
        other => {
            not_impl_err!("Physical plan does not support logical expression {other:?}")
        }
    }
}
