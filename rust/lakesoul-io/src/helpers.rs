// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow_schema::Schema;
use datafusion::{
    execution::context::SessionState,
    logical_expr::col,
    physical_expr::{create_physical_expr, PhysicalSortExpr},
    physical_plan::Partitioning,
    physical_planner::create_physical_sort_expr,
};
use datafusion_common::{DFSchema, Result};

pub fn create_sort_exprs(
    columns: &[String],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<Vec<PhysicalSortExpr>> {
    columns
        .iter()
        .map(|column| {
            create_physical_sort_expr(
                &col(column).sort(true, true),
                input_dfschema,
                input_schema,
                session_state.execution_props(),
            )
        })
        .collect::<Result<Vec<_>>>()
}

pub fn create_hash_partitioning(
    columns: &[String],
    partitioning_num: usize,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<Partitioning> {
    let runtime_expr = columns
        .iter()
        .map(|column| {
            create_physical_expr(
                &col(column),
                input_dfschema,
                input_schema,
                session_state.execution_props(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Partitioning::Hash(runtime_expr, partitioning_num))
}
