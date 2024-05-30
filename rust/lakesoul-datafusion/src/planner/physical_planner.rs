// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;

use datafusion::common::{DFSchema, SchemaExt};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr::PhysicalExpr;

use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

use async_trait::async_trait;

use datafusion::logical_expr::{DmlStatement, WriteOp};
use lakesoul_io::helpers::{column_names_to_physical_expr, column_names_to_physical_sort_expr};
use lakesoul_io::repartition::RepartitionByRangeAndHashExec;

use crate::lakesoul_table::LakeSoulTable;

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
        match logical_plan {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                op: WriteOp::InsertInto,
                input,
                ..
            }) => {
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
        self.default_planner
            .create_physical_expr(expr, input_dfschema, input_schema, session_state)
    }
}
