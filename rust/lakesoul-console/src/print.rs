// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::io::Write;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::arrow::util::pretty::pretty_format_batches;

use crate::Result;

fn print_batches<W: std::io::Write>(
    writer: &mut W,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    // Filter out any empty batches.
    let batches: Vec<_> = batches
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .cloned()
        .collect();
    if batches.is_empty() {
        return print_empty(writer, schema);
    }

    if print_explain_batches(writer, &schema, &batches)? {
        return Ok(());
    }

    let formatted = pretty_format_batches(&batches)?;
    writeln!(writer, "{formatted}")?;
    Ok(())
}

/// Render DataFusion's `EXPLAIN` / `EXPLAIN ANALYZE` result as its native
/// multi-line plan text instead of embedding it in a bordered table cell.
///
/// DataFusion exposes explain results as `plan_type` and `plan` string
/// columns. The latter contains a tree with line breaks, which is unreadable
/// when rendered through `pretty_format_batches`.
fn print_explain_batches<W: Write>(
    writer: &mut W,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<bool> {
    let plan_type_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == "plan_type");
    let plan_index = schema
        .fields()
        .iter()
        .position(|field| field.name() == "plan");
    let (Some(plan_type_index), Some(plan_index)) = (plan_type_index, plan_index) else {
        return Ok(false);
    };

    for batch in batches {
        let plan_type = batch.column(plan_type_index);
        let plan = batch.column(plan_index);
        for row in 0..batch.num_rows() {
            writeln!(writer, "-- {} --", array_value_to_string(plan_type, row)?)?;
            writeln!(writer, "{}", array_value_to_string(plan, row)?)?;
        }
    }
    Ok(true)
}

/// Print when the result batches contain no rows
fn print_empty<W: std::io::Write>(writer: &mut W, schema: SchemaRef) -> Result<()> {
    // Print column headers for Table format
    if !schema.fields().is_empty() {
        let empty_batch = RecordBatch::new_empty(schema);
        let formatted = pretty_format_batches(&[empty_batch])?;
        writeln!(writer, "{formatted}")?;
    }

    Ok(())
}

// Returns the query execution details formatted
fn get_execution_details_formatted(
    row_count: usize,
    query_start_time: Instant,
) -> String {
    let nrows_shown_msg = String::new();

    format!(
        "{} row(s) fetched. {}\nElapsed {:.3} seconds.\n",
        row_count,
        nrows_shown_msg,
        query_start_time.elapsed().as_secs_f64()
    )
}

#[derive(Default)]
pub struct Printer {
    // not implemented
    _color: bool,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, StringArray};

    use super::*;

    #[test]
    fn explain_output_uses_multiline_plan_format() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "plan_type",
                Arc::new(StringArray::from(vec!["physical_plan"])) as ArrayRef,
            ),
            (
                "plan",
                Arc::new(StringArray::from(vec!["AggregateExec\n  DataSourceExec"]))
                    as ArrayRef,
            ),
        ])
        .unwrap();
        let mut output = Vec::new();

        print_batches(&mut output, batch.schema(), &[batch]).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("-- physical_plan --"));
        assert!(output.contains("AggregateExec\n  DataSourceExec"));
        assert!(!output.contains("| plan_type |"));
    }
}

impl Printer {
    /// Print the batches to stdout using the specified format
    pub fn print_batches(
        &self,
        schema: SchemaRef,
        batches: &[RecordBatch],
        query_start_time: Instant,
        row_count: usize,
    ) -> Result<()> {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        print_batches(&mut writer, schema, batches)?;

        let formatted_exec_details =
            get_execution_details_formatted(row_count, query_start_time);

        writeln!(writer, "{formatted_exec_details}")?;

        Ok(())
    }
}
