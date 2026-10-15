// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow_array::{RecordBatch, StructArray};
use arrow_schema::{DataType, Fields, Schema};
use datafusion_common::{DataFusionError, Result};

/// Stored/configured NOT NULL constraints resolved against a contribution schema.
/// Resolution happens once; validating batches never copies their arrays.
#[derive(Debug)]
pub struct WriteInvariants {
    checks: Vec<NotNullCheck>,
}

#[derive(Debug)]
struct NotNullCheck {
    column: String,
    indices: Vec<usize>,
}

impl WriteInvariants {
    pub fn try_new(table_schema: &Schema, input_schema: &Schema) -> Result<Self> {
        let mut checks = Vec::new();
        Self::collect(
            table_schema.fields(),
            input_schema.fields(),
            "",
            &[],
            &mut checks,
        )?;
        Ok(Self { checks })
    }

    fn collect(
        table_fields: &Fields,
        input_fields: &Fields,
        parent_name: &str,
        parent_indices: &[usize],
        checks: &mut Vec<NotNullCheck>,
    ) -> Result<()> {
        for field in table_fields {
            let name = if parent_name.is_empty() {
                field.name().clone()
            } else {
                format!("{parent_name}.{}", field.name())
            };
            let input_index = input_fields.iter().position(|f| f.name() == field.name());
            let Some(input_index) = input_index else {
                if !field.is_nullable() {
                    return Err(DataFusionError::Plan(format!(
                        "Missing NOT NULL column '{name}'"
                    )));
                }
                // A missing nullable struct makes its required descendants NULL too.
                if let DataType::Struct(children) = field.data_type() {
                    Self::collect(children, &Fields::empty(), &name, &[], checks)?;
                }
                continue;
            };
            let mut indices = parent_indices.to_vec();
            indices.push(input_index);
            if !field.is_nullable() {
                checks.push(NotNullCheck {
                    column: name.clone(),
                    indices: indices.clone(),
                });
            }
            if let DataType::Struct(children) = field.data_type() {
                let DataType::Struct(input_children) =
                    input_fields[input_index].data_type()
                else {
                    return Err(DataFusionError::Plan(format!(
                        "Column '{name}' must be a struct"
                    )));
                };
                Self::collect(children, input_children, &name, &indices, checks)?;
            }
        }
        Ok(())
    }

    pub fn validate(&self, batch: &RecordBatch) -> Result<()> {
        for check in &self.checks {
            let mut array = batch.column(check.indices[0]).as_ref();
            for index in &check.indices[1..] {
                // Spark's GetStructField is NULL when any ancestor is NULL.
                if array.logical_null_count() != 0 {
                    return check.violation();
                }
                let Some(parent) = array.as_any().downcast_ref::<StructArray>() else {
                    return Err(DataFusionError::Execution(format!(
                        "Column '{}' must be a struct",
                        check.column
                    )));
                };
                array = parent.column(*index).as_ref();
            }
            if array.logical_null_count() != 0 {
                return check.violation();
            }
        }
        Ok(())
    }

    /// Check file-schema nullability without treating masked child values as NULL.
    /// Unlike stored Spark constraints, Arrow permits a required child beneath
    /// a NULL struct parent.
    pub fn validate_physical(&self, batch: &RecordBatch) -> Result<()> {
        for check in &self.checks {
            let mut array = batch.column(check.indices[0]).as_ref();
            for index in &check.indices[1..] {
                let Some(parent) = array.as_any().downcast_ref::<StructArray>() else {
                    return Err(DataFusionError::Execution(format!(
                        "Column '{}' must be a struct",
                        check.column
                    )));
                };
                array = parent.column(*index).as_ref();
            }
            let Some(nulls) = array.logical_nulls() else {
                continue;
            };
            if nulls.null_count() == 0 {
                continue;
            }
            if check.indices.len() == 1 {
                return check.violation();
            }
            for row in 0..batch.num_rows() {
                if !nulls.is_null(row) {
                    continue;
                }
                let mut ancestor = batch.column(check.indices[0]).as_ref();
                let mut visible = true;
                for index in &check.indices[1..] {
                    if ancestor.is_null(row) {
                        visible = false;
                        break;
                    }
                    ancestor = ancestor
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("struct path validated above")
                        .column(*index)
                        .as_ref();
                }
                if visible {
                    return check.violation();
                }
            }
        }
        Ok(())
    }
}

impl NotNullCheck {
    fn violation(&self) -> Result<()> {
        Err(DataFusionError::Execution(format!(
            "NOT NULL constraint violated for column '{}'",
            self.column
        )))
    }
}
