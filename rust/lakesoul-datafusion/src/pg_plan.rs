// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Logical-plan rewrites that preserve PostgreSQL semantic field metadata.
//!
//! A PostgreSQL array column is an Arrow `List` whose element field carries
//! the semantic type (`pg.type`, `pg.typmod`, JSON extension). DataFusion's
//! `VALUES` executor evaluates every row into a `ScalarValue` and builds the
//! output with `ScalarValue::iter_to_array`, which reconstructs a list from the
//! element *data type* only. A `Values` node whose schema carries element
//! metadata therefore cannot produce a matching batch: the evaluated list has
//! lost the metadata.
//!
//! [`cast_values_to_declared_types`] therefore gives the `Values` node physical
//! (metadata-free) column types and puts a projection above it that casts each
//! column back to the declared type. A projection evaluates arrays directly, so
//! the cast's target field — metadata included — reaches the batch. The
//! projection keeps the original column names, so the insert planner's
//! projection above it still resolves.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, TableReference};
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::logical_expr::{Cast, Expr, ExprSchemable, LogicalPlan, Values};

/// Rewrite `Values` nodes so semantic list metadata survives execution.
pub fn cast_values_to_declared_types(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| match node {
        LogicalPlan::Values(values) => cast_values(values).map(Transformed::yes),
        plan => Ok(Transformed::no(plan)),
    })
    .map(|transformed| transformed.data)
}

fn cast_values(values: Values) -> Result<LogicalPlan> {
    let declared: Vec<Arc<Field>> = values
        .schema
        .iter()
        .map(|(_, field)| Arc::clone(field))
        .collect();
    if !declared
        .iter()
        .any(|field| has_list_metadata(field.data_type()))
    {
        return Ok(LogicalPlan::Values(values));
    }

    let physical_fields: Vec<(Option<TableReference>, Arc<Field>)> = values
        .schema
        .iter()
        .map(|(qualifier, field)| {
            (
                qualifier.cloned(),
                Arc::new(strip_list_metadata(field.as_ref())),
            )
        })
        .collect();
    let physical_schema =
        DFSchema::new_with_metadata(physical_fields, values.schema.metadata().clone())?;

    let mut values = values;
    for row in &mut values.values {
        for (index, expression) in row.iter_mut().enumerate() {
            let physical = physical_schema.field(index).data_type().clone();
            if expression.get_type(&physical_schema)? != physical {
                *expression =
                    Expr::Cast(Cast::new(Box::new(expression.clone()), physical));
            }
        }
    }

    let values = LogicalPlan::Values(Values {
        values: values.values,
        schema: Arc::new(physical_schema.clone()),
    });
    let projections = declared
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let column = Expr::Column(Column::new_unqualified(
                physical_schema.field(index).name(),
            ));
            Expr::Cast(Cast::new_from_field(Box::new(column), Arc::clone(field)))
                .alias(field.name())
        })
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(values)
        .project(projections)?
        .build()
}

/// True when the type is a list whose element field still carries metadata.
fn has_list_metadata(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::FixedSizeList(inner, _)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner) => {
            !inner.metadata().is_empty() || has_list_metadata(inner.data_type())
        }
        _ => false,
    }
}

/// Strip the semantic metadata from list element fields, recursively.
fn strip_list_metadata(field: &Field) -> Field {
    Field::new(
        field.name(),
        physical_data_type(field.data_type()),
        field.is_nullable(),
    )
}

fn physical_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(inner) => DataType::List(Arc::new(physical_field(inner))),
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(physical_field(inner)))
        }
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(physical_field(inner)), *size)
        }
        DataType::ListView(inner) => DataType::ListView(Arc::new(physical_field(inner))),
        DataType::LargeListView(inner) => {
            DataType::LargeListView(Arc::new(physical_field(inner)))
        }
        other => other.clone(),
    }
}

fn physical_field(field: &Field) -> Field {
    Field::new(
        field.name(),
        physical_data_type(field.data_type()),
        field.is_nullable(),
    )
}
