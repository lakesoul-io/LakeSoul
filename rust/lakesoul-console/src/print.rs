// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Write as _;
use std::io::Write;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::{
    ArrayFormatter, FormatOptions, array_value_to_string,
};
use datafusion::arrow::util::pretty::pretty_format_batches;

use crate::Result;

/// How statement results are written to stdout.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Bordered tables; EXPLAIN plans as plain text.
    #[default]
    Table,
    /// Per statement, a `{"columns":[{"name":..,"type":..}]}` line, then one
    /// JSON array per row of the values as table-format strings (NULL as
    /// `null`).
    Json,
}

fn print_batches<W: Write>(
    writer: &mut W,
    format: OutputFormat,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    match format {
        OutputFormat::Table => print_table(writer, schema, batches),
        OutputFormat::Json => print_json(writer, &schema, batches),
    }
}

/// One line per row, so a reader can split statement output by lines even
/// when values contain line breaks.
fn print_json<W: Write>(
    writer: &mut W,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<()> {
    let columns: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| serde_json::json!({"name": field.name(), "type": field.data_type().to_string()}))
        .collect();
    serde_json::to_writer(&mut *writer, &serde_json::json!({ "columns": columns }))?;
    writeln!(writer)?;
    // The options of `pretty_format_batches`, so values match the table format.
    let options = FormatOptions::default().with_display_error(true);
    let mut cell = String::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let nulls: Vec<_> = batch.columns().iter().map(|c| c.logical_nulls()).collect();
        for row in 0..batch.num_rows() {
            writer.write_all(b"[")?;
            for (index, formatter) in formatters.iter().enumerate() {
                if index > 0 {
                    writer.write_all(b",")?;
                }
                if nulls[index]
                    .as_ref()
                    .is_some_and(|nulls| nulls.is_null(row))
                {
                    writer.write_all(b"null")?;
                } else {
                    cell.clear();
                    write!(cell, "{}", formatter.value(row))?;
                    serde_json::to_writer(&mut *writer, &cell)?;
                }
            }
            writer.write_all(b"]\n")?;
        }
    }
    Ok(())
}

fn print_table<W: Write>(
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

pub struct Printer {
    format: OutputFormat,
}

impl Printer {
    pub fn new(format: OutputFormat) -> Self {
        Self { format }
    }

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

        print_batches(&mut writer, self.format, schema, batches)?;

        let formatted_exec_details =
            get_execution_details_formatted(row_count, query_start_time);

        writeln!(writer, "{formatted_exec_details}")?;

        Ok(())
    }

    pub fn format_batches(
        &self,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<String> {
        let mut buf = Vec::new();
        print_batches(&mut buf, self.format, schema, batches)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Decimal128Array, NullArray, StringArray};

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

        print_batches(&mut output, OutputFormat::Table, batch.schema(), &[batch])
            .unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("-- physical_plan --"));
        assert!(output.contains("AggregateExec\n  DataSourceExec"));
        assert!(!output.contains("| plan_type |"));
    }

    #[test]
    fn json_output_is_one_line_per_row_with_types_and_nulls() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(StringArray::from(vec![Some("a \"b\"\nc"), None])) as ArrayRef,
            ),
            (
                "price",
                Arc::new(
                    Decimal128Array::from(vec![Some(12345), Some(-5)])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            ("nothing", Arc::new(NullArray::new(2)) as ArrayRef),
        ])
        .unwrap();
        let mut output = Vec::new();

        print_batches(&mut output, OutputFormat::Json, batch.schema(), &[batch]).unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            concat!(
                r#"{"columns":[{"name":"name","type":"Utf8"},{"name":"price","type":"Decimal128(15, 2)"},{"name":"nothing","type":"Null"}]}"#,
                "\n",
                r#"["a \"b\"\nc","123.45",null]"#,
                "\n",
                r#"[null,"-0.05",null]"#,
                "\n",
            )
        );
    }

    #[test]
    fn json_output_of_an_empty_result_is_the_column_line() {
        let schema = RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        )])
        .unwrap()
        .schema();
        let mut output = Vec::new();

        print_batches(&mut output, OutputFormat::Json, schema, &[]).unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "{\"columns\":[{\"name\":\"n\",\"type\":\"Utf8\"}]}\n"
        );
    }
}
