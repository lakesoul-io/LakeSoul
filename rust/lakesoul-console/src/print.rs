// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::pretty::pretty_format_batches;
use std::io::Write;
use std::time::Instant;
fn print_batches<W: std::io::Write>(
    writer: &mut W,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> anyhow::Result<()> {
    // filter out any empty batches
    let batches: Vec<_> = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .cloned()
        .collect();
    if batches.is_empty() {
        return print_empty(writer, schema);
    }

    let formatted = pretty_format_batches(&batches)?;
    writeln!(writer, "{formatted}")?;
    Ok(())
}

/// Print when the result batches contain no rows
fn print_empty<W: std::io::Write>(
    writer: &mut W,
    schema: SchemaRef,
) -> anyhow::Result<()> {
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

impl Printer {
    /// Print the batches to stdout using the specified format
    pub fn print_batches(
        &self,
        schema: SchemaRef,
        batches: &[RecordBatch],
        query_start_time: Instant,
        row_count: usize,
    ) -> anyhow::Result<()> {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        print_batches(&mut writer, schema, batches)?;

        let formatted_exec_details =
            get_execution_details_formatted(row_count, query_start_time);

        writeln!(writer, "{formatted_exec_details}")?;

        Ok(())
    }
}
