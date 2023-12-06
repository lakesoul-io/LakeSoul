// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::ops::Deref;
use std::sync::Arc;

use arrow::datatypes::Schema;


use datafusion::common::{DFSchema, SchemaExt};
use datafusion::physical_planner::{PhysicalPlanner, DefaultPhysicalPlanner};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::error::{Result, DataFusionError};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::execution::context::SessionState;

use async_trait::async_trait;

use datafusion::logical_expr::{DmlStatement, WriteOp, LogicalPlanBuilder};

use crate::lakesoul_table::LakeSoulTable;
use crate::lakesoul_table::helpers::create_sort_exprs;


pub struct LakeSoulPhysicalPlanner {
    default_planner : DefaultPhysicalPlanner,
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
        match logical_plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::InsertInto,
                input,
                ..
            }) => {
                let name = table_name.table();
                // let schema = session_state.schema_for_ref(table_name)?;
                let table = LakeSoulTable::for_name(name).await.unwrap();
                match table.as_sink_provider(session_state).await {
                    Ok(provider) => {
                        let builder = LogicalPlanBuilder::from(input.deref().clone());
                        
                        let builder = if table.primary_keys().is_empty() {
                            if !table
                                .schema()
                                .logically_equivalent_names_and_types(&Schema::from(input.schema().as_ref()))
                            {
                                return Err(DataFusionError::Plan(
                                    // Return an error if schema of the input query does not match with the table schema.
                                    format!("Inserting query must have the same schema with the table.")
                                ));
                            }

                            builder
                        } else {
                            let sort_exprs = create_sort_exprs(table.primary_keys());
                            builder.sort(sort_exprs)?
                        };

                        // let pk_exprs = provider.create_pk_exprs();
                        // let builder = if pk_exprs.is_empty() {
                        //     builder
                        // } else {
                            
                        //     builder.repartition(Partitioning::Hash(pk_exprs, 2))?
                        // };

                        let input = builder.build()?;
                        let input_exec = self.create_physical_plan(&input, session_state).await?;
                        provider.insert_into(session_state, input_exec, false).await
                    } 
                    Err(e) => 
                        return Err(DataFusionError::External(Box::new(e)))
                }
            }
            LogicalPlan::Statement(statement) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                let name = statement.name();
                Err(DataFusionError::NotImplemented(
                    format!("Unsupported logical plan: Statement({name})")
                ))
            }
            _ => self.default_planner.create_physical_plan(logical_plan, session_state).await
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
        match expr {
            _ => self.default_planner.create_physical_expr(expr, input_dfschema, input_schema, session_state)
        }
    }
}