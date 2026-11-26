// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `COPY ... TO STDOUT` for the PostgreSQL front end.
//!
//! Export is the mirror of the copy-in path in [`crate::copy_in`]: the server
//! answers with `CopyOutResponse`, streams one `CopyData` frame per row and
//! ends with `CopyDone` and a `COPY <rows>` tag. The rows come straight from
//! the DataFusion plan (so a table copy sees staged writes inside a
//! transaction block), and nothing is collected in memory.
//!
//! TEXT (the default) and CSV are implemented with the same `WITH` options
//! the load path accepts. `COPY ... TO` a file or program and `FORMAT binary`
//! stay explicit `0A000` refusals.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, NaiveDate, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    Date64Array, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, ParamValues, TableReference};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    CopySource, CopyTarget, Ident, ObjectName, Query, Statement, TableObject,
};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{CopyResponse, Response};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::copy::CopyData;
use futures::stream::{self, StreamExt};

use crate::copy_in::{CopyFormat, CopyOptions, parse_options};
use crate::ddl::{feature_not_supported, normalize_column_ident};
use crate::insert::{request_context, target_reference, user_error};

const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";

struct CopyOutRequest {
    sql: String,
    options: CopyOptions,
}

/// Validate a `COPY ... TO STDOUT` without starting it; extended `Parse`
/// runs this so a bad table or column fails before the response.
pub(crate) async fn validate<C>(
    statement: &Statement,
    session_context: &SessionContext,
    client: &C,
) -> PgWireResult<()>
where
    C: ClientInfo + ?Sized,
{
    let request = parse_copy_out(statement, session_context, client).await?;
    session_context
        .sql(&request.sql)
        .await
        .map(|_| ())
        .map_err(|error| PgWireError::ApiError(Box::new(error)))
}

/// Execute a `COPY ... TO STDOUT` and stream its rows.
pub(crate) async fn copy_out<C>(
    statement: &Statement,
    session_context: &SessionContext,
    client: &C,
    params: Option<&ParamValues>,
) -> PgWireResult<Response>
where
    C: ClientInfo + ?Sized,
{
    let request = parse_copy_out(statement, session_context, client).await?;
    let dataframe: DataFrame = session_context
        .sql(&request.sql)
        .await
        .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    let dataframe = match params {
        Some(params) => {
            let plan = dataframe
                .logical_plan()
                .clone()
                .replace_params_with_values(params)
                .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
            session_context
                .execute_logical_plan(plan)
                .await
                .map_err(|error| PgWireError::ApiError(Box::new(error)))?
        }
        None => dataframe,
    };
    let schema: SchemaRef = Arc::new(dataframe.schema().as_arrow().clone());
    let columns = schema.fields().len();
    let batches = dataframe
        .execute_stream()
        .await
        .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    let header_pending = request.options.header;
    let state = CopyOutState {
        options: request.options,
        schema,
        header_pending,
        batch: None,
        row: 0,
    };
    let data_stream = stream::unfold(Some((batches, state)), |state| async move {
        let (mut batches, mut state) = state?;
        loop {
            if let Some(batch) = state.batch.as_ref()
                && state.row < batch.num_rows()
            {
                let mut bytes =
                    match encode_row(batch, state.row, &state.schema, &state.options) {
                        Ok(bytes) => bytes,
                        Err(error) => return Some((Err(error), None)),
                    };
                if state.header_pending {
                    state.header_pending = false;
                    let header = encode_header(&state.schema, &state.options);
                    let mut combined =
                        BytesMut::with_capacity(header.len() + bytes.len());
                    combined.extend_from_slice(&header);
                    combined.extend_from_slice(&bytes);
                    bytes = combined.freeze();
                }
                state.row += 1;
                return Some((Ok(CopyData::new(bytes)), Some((batches, state))));
            }
            state.batch = None;
            state.row = 0;
            match batches.next().await {
                Some(Ok(batch)) => {
                    state.batch = Some(batch);
                }
                Some(Err(error)) => {
                    return Some((Err(PgWireError::ApiError(Box::new(error))), None));
                }
                None => return None,
            }
        }
    });
    Ok(Response::CopyOut(CopyResponse::new(
        0,
        columns,
        data_stream,
    )))
}

/// Parse the statement and resolve the table/column spelling.
async fn parse_copy_out<C>(
    statement: &Statement,
    session_context: &SessionContext,
    client: &C,
) -> PgWireResult<CopyOutRequest>
where
    C: ClientInfo + ?Sized,
{
    let Statement::Copy {
        source,
        to,
        target,
        options,
        legacy_options,
        values,
    } = statement
    else {
        return Err(user_error(
            "XX000",
            "COPY TO hook received a non-COPY statement".to_string(),
        ));
    };
    if !*to {
        return Err(user_error(
            "XX000",
            "COPY TO hook received COPY ... FROM".to_string(),
        ));
    }
    if !values.is_empty() {
        return Err(feature_not_supported(
            "COPY ... TO STDOUT cannot carry inline data".to_string(),
        ));
    }
    match target {
        CopyTarget::Stdout => {}
        CopyTarget::Stdin => {
            return Err(feature_not_supported(
                "COPY ... TO STDIN is not supported".to_string(),
            ));
        }
        CopyTarget::File { .. } | CopyTarget::Program { .. } => {
            return Err(feature_not_supported(
                "COPY ... TO a file or program is not supported; use COPY ... \
                 TO STDOUT"
                    .to_string(),
            ));
        }
    }
    let options = parse_options(options, legacy_options)?;
    if options.format == CopyFormat::Binary {
        return Err(feature_not_supported(
            "COPY ... TO STDOUT with FORMAT binary is not supported".to_string(),
        ));
    }
    let sql = match source {
        CopySource::Table {
            table_name,
            columns,
        } => table_select(table_name, columns, session_context, client).await?,
        CopySource::Query(query) => query_sql(query)?,
    };
    Ok(CopyOutRequest { sql, options })
}

/// `SELECT <columns> FROM <table>` with the resolved table and column
/// spelling, so missing relations and columns report PostgreSQL's codes.
async fn table_select<C>(
    table_name: &ObjectName,
    columns: &[Ident],
    session_context: &SessionContext,
    client: &C,
) -> PgWireResult<String>
where
    C: ClientInfo + ?Sized,
{
    let (_status, database) = request_context(client)?;
    let (table_ref, written) =
        target_reference(&TableObject::TableName(table_name.clone()), &database)?;
    let provider = session_context
        .table_provider(table_ref.clone())
        .await
        .map_err(|error| {
            user_error(
                UNDEFINED_TABLE,
                format!("relation \"{written}\" does not exist: {error}"),
            )
        })?;
    let provider_schema = provider.schema();
    let table_sql = table_reference_sql(&table_ref);
    let column_sql = if columns.is_empty() {
        "*".to_string()
    } else {
        let schema = DFSchema::try_from(provider_schema.clone()).map_err(|error| {
            user_error(
                UNDEFINED_TABLE,
                format!("cannot read the schema of \"{written}\": {error}"),
            )
        })?;
        let mut names = Vec::with_capacity(columns.len());
        for ident in columns {
            let name = normalize_column_ident(ident)?;
            let index = schema.index_of_column_by_name(None, &name).ok_or_else(|| {
                user_error(
                    UNDEFINED_COLUMN,
                    format!("column \"{name}\" of relation \"{written}\" does not exist"),
                )
            })?;
            names.push(quote_ident(provider_schema.field(index).name()));
        }
        names.join(", ")
    };
    Ok(format!("SELECT {column_sql} FROM {table_sql}"))
}

fn query_sql(query: &Query) -> PgWireResult<String> {
    Ok(query.to_string())
}

fn table_reference_sql(table: &TableReference) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = table.catalog() {
        parts.push(quote_ident(catalog));
    }
    if let Some(schema) = table.schema() {
        parts.push(quote_ident(schema));
    }
    parts.push(quote_ident(table.table()));
    parts.join(".")
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

struct CopyOutState {
    options: CopyOptions,
    schema: SchemaRef,
    header_pending: bool,
    batch: Option<RecordBatch>,
    row: usize,
}

fn encode_header(schema: &SchemaRef, options: &CopyOptions) -> Bytes {
    let mut out = BytesMut::new();
    if options.format != CopyFormat::Csv {
        return out.freeze();
    }
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            out.extend_from_slice(&[options.delimiter]);
        }
        encode_csv_field(&mut out, Some(field.name().as_str()), options);
    }
    out.extend_from_slice(b"\n");
    out.freeze()
}

fn encode_row(
    batch: &RecordBatch,
    row: usize,
    schema: &SchemaRef,
    options: &CopyOptions,
) -> PgWireResult<Bytes> {
    let mut out = BytesMut::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            out.extend_from_slice(&[options.delimiter]);
        }
        let value = field_text(batch.column(index), row, field)?;
        match options.format {
            CopyFormat::Text => encode_text_field(&mut out, value.as_deref(), options),
            CopyFormat::Csv => encode_csv_field(&mut out, value.as_deref(), options),
            CopyFormat::Binary => {
                return Err(feature_not_supported(
                    "COPY ... TO STDOUT with FORMAT binary is not supported".to_string(),
                ));
            }
        }
    }
    out.extend_from_slice(b"\n");
    Ok(out.freeze())
}

fn encode_text_field(out: &mut BytesMut, value: Option<&str>, options: &CopyOptions) {
    let Some(value) = value else {
        out.extend_from_slice(&options.null);
        return;
    };
    let mut escaped = BytesMut::with_capacity(value.len());
    for &byte in value.as_bytes() {
        match byte {
            b'\\' => escaped.extend_from_slice(b"\\\\"),
            b'\x08' => escaped.extend_from_slice(b"\\b"),
            b'\x0c' => escaped.extend_from_slice(b"\\f"),
            b'\n' => escaped.extend_from_slice(b"\\n"),
            b'\r' => escaped.extend_from_slice(b"\\r"),
            b'\t' => escaped.extend_from_slice(b"\\t"),
            b'\x0b' => escaped.extend_from_slice(b"\\v"),
            byte if byte == options.delimiter => {
                escaped.extend_from_slice(&[b'\\', byte]);
            }
            byte => escaped.extend_from_slice(&[byte]),
        }
    }
    // A data value that would otherwise read back as the null marker gets a
    // protecting backslash, as PostgreSQL's COPY TO does.
    if !options.null.is_empty() && escaped.as_ref() == options.null.as_slice() {
        out.extend_from_slice(b"\\");
    }
    out.extend_from_slice(&escaped);
}

fn encode_csv_field(out: &mut BytesMut, value: Option<&str>, options: &CopyOptions) {
    let Some(value) = value else {
        out.extend_from_slice(&options.null);
        return;
    };
    let null_string = std::str::from_utf8(&options.null).ok();
    let quote = options.quote as char;
    let escape = options.escape as char;
    let needs_quote = value.is_empty()
        || value.bytes().any(|byte| {
            byte == options.delimiter
                || byte == options.quote
                || byte == b'\n'
                || byte == b'\r'
                || (options.escape != options.quote && byte == options.escape)
        })
        || null_string.is_some_and(|null| !null.is_empty() && value == null);
    if !needs_quote {
        out.extend_from_slice(value.as_bytes());
        return;
    }
    out.extend_from_slice(&[options.quote]);
    for character in value.chars() {
        if character == quote {
            if options.escape == options.quote {
                out.extend_from_slice(&[options.quote]);
                out.extend_from_slice(&[options.quote]);
            } else {
                out.extend_from_slice(&[options.escape, options.quote]);
            }
        } else if character == escape && options.escape != options.quote {
            out.extend_from_slice(&[options.escape, options.escape]);
        } else {
            let mut encoded = [0u8; 4];
            out.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        }
    }
    out.extend_from_slice(&[options.quote]);
}

fn field_text(
    array: &ArrayRef,
    row: usize,
    field: &Field,
) -> PgWireResult<Option<String>> {
    let data_type = field.data_type();
    if let Some(kind) = crate::pg_types::pg_type_name(field) {
        match kind {
            "uuid" => {
                return Ok(Some(downcast::<StringArray>(array).value(row).to_string()));
            }
            "varchar" | "bpchar" => {
                return Ok(Some(downcast::<StringArray>(array).value(row).to_string()));
            }
            "money" if matches!(data_type, DataType::Decimal128(_, _)) => {
                let value = downcast::<Decimal128Array>(array).value(row);
                return Ok(Some(money_text(value)));
            }
            _ => {}
        }
    }
    if array.is_null(row) {
        return Ok(None);
    }
    let text = match data_type {
        DataType::Boolean => {
            if downcast::<BooleanArray>(array).value(row) {
                "t".to_string()
            } else {
                "f".to_string()
            }
        }
        DataType::Int8 => downcast::<Int8Array>(array).value(row).to_string(),
        DataType::Int16 => downcast::<Int16Array>(array).value(row).to_string(),
        DataType::Int32 => downcast::<Int32Array>(array).value(row).to_string(),
        DataType::Int64 => downcast::<Int64Array>(array).value(row).to_string(),
        DataType::UInt8 => downcast::<UInt8Array>(array).value(row).to_string(),
        DataType::UInt16 => downcast::<UInt16Array>(array).value(row).to_string(),
        DataType::UInt32 => downcast::<UInt32Array>(array).value(row).to_string(),
        DataType::UInt64 => downcast::<UInt64Array>(array).value(row).to_string(),
        DataType::Float32 => float_text32(downcast::<Float32Array>(array).value(row)),
        DataType::Float64 => float_text64(downcast::<Float64Array>(array).value(row)),
        DataType::Utf8 => downcast::<StringArray>(array).value(row).to_string(),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array).value(row).to_string(),
        DataType::Utf8View => downcast::<StringViewArray>(array).value(row).to_string(),
        DataType::Binary => bytea_text(downcast::<BinaryArray>(array).value(row)),
        DataType::LargeBinary => {
            bytea_text(downcast::<LargeBinaryArray>(array).value(row))
        }
        DataType::BinaryView => bytea_text(downcast::<BinaryViewArray>(array).value(row)),
        DataType::Time32(unit) => {
            let nanos = match unit {
                TimeUnit::Second => {
                    i64::from(
                        downcast::<datafusion::arrow::array::Time32SecondArray>(array)
                            .value(row),
                    ) * 1_000_000_000
                }
                TimeUnit::Millisecond => {
                    i64::from(
                        downcast::<datafusion::arrow::array::Time32MillisecondArray>(
                            array,
                        )
                        .value(row),
                    ) * 1_000_000
                }
                _ => return Err(out_of_range()),
            };
            time_text(nanos)?
        }
        DataType::Time64(unit) => {
            let nanos = match unit {
                TimeUnit::Microsecond => {
                    downcast::<datafusion::arrow::array::Time64MicrosecondArray>(array)
                        .value(row)
                        .checked_mul(1_000)
                        .ok_or_else(out_of_range)?
                }
                TimeUnit::Nanosecond => {
                    downcast::<datafusion::arrow::array::Time64NanosecondArray>(array)
                        .value(row)
                }
                _ => return Err(out_of_range()),
            };
            time_text(nanos)?
        }
        DataType::Decimal256(_, scale) => {
            let value =
                downcast::<datafusion::arrow::array::Decimal256Array>(array).value(row);
            decimal256_text(value, *scale)?
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let (DataType::List(inner) | DataType::LargeList(inner)) = data_type else {
                unreachable!()
            };
            let values = match data_type {
                DataType::List(_) => {
                    let list = downcast::<datafusion::arrow::array::ListArray>(array);
                    list.value(row)
                }
                DataType::LargeList(_) => {
                    let list =
                        downcast::<datafusion::arrow::array::LargeListArray>(array);
                    list.value(row)
                }
                _ => unreachable!(),
            };
            array_text(&values, inner)?
        }
        DataType::Date32 => date_text(downcast::<Date32Array>(array).value(row))?,
        DataType::Date64 => {
            let millis = downcast::<Date64Array>(array).value(row);
            let timestamp = DateTime::<Utc>::from_timestamp_millis(millis)
                .ok_or_else(|| user_error("22007", "date out of range".to_string()))?;
            timestamp.date_naive().format("%Y-%m-%d").to_string()
        }
        DataType::Timestamp(unit, time_zone) => {
            let micros = match unit {
                TimeUnit::Second => downcast::<TimestampSecondArray>(array)
                    .value(row)
                    .checked_mul(1_000_000)
                    .ok_or_else(out_of_range)?,
                TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)
                    .value(row)
                    .checked_mul(1_000)
                    .ok_or_else(out_of_range)?,
                TimeUnit::Microsecond => {
                    downcast::<TimestampMicrosecondArray>(array).value(row)
                }
                TimeUnit::Nanosecond => {
                    downcast::<TimestampNanosecondArray>(array).value(row) / 1_000
                }
            };
            timestamp_text(micros, time_zone.is_some())?
        }
        DataType::Decimal128(_, scale) => {
            let value = downcast::<Decimal128Array>(array).value(row);
            decimal_text(value, *scale)?
        }
        other => {
            return Err(feature_not_supported(format!(
                "COPY ... TO STDOUT does not support column type {other}"
            )));
        }
    };
    Ok(Some(text))
}

fn time_text(nanos: i64) -> PgWireResult<String> {
    let seconds = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000);
    let time =
        chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, sub as u32)
            .ok_or_else(out_of_range)?;
    let mut text = time.format("%H:%M:%S").to_string();
    if sub != 0 {
        let fraction = format!("{sub:09}");
        text.push('.');
        text.push_str(fraction.trim_end_matches('0'));
    }
    Ok(text)
}

fn money_text(value: i128) -> String {
    let negative = value < 0;
    let cents = value.unsigned_abs();
    let whole = cents / 100;
    let fraction = cents % 100;
    let mut whole = whole.to_string();
    let mut grouped = String::new();
    while whole.len() > 3 {
        let split = whole.len() - 3;
        grouped.insert_str(0, &format!(",{}", &whole[split..]));
        whole.truncate(split);
    }
    grouped.insert_str(0, &whole);
    format!(
        "{}${}.{:02}",
        if negative { "-" } else { "" },
        grouped,
        fraction
    )
}

fn decimal256_text(
    value: datafusion::arrow::datatypes::i256,
    scale: i8,
) -> PgWireResult<String> {
    let negative = value < datafusion::arrow::datatypes::i256::ZERO;
    let digits = if negative {
        (-value).to_string()
    } else {
        value.to_string()
    };
    let mut text = if scale <= 0 {
        digits
    } else {
        let scale = scale as usize;
        let mut digits = digits;
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        text.insert(0, '-');
    }
    Ok(text)
}

fn array_text(array: &ArrayRef, element: &Field) -> PgWireResult<String> {
    let mut out = String::from("{");
    for index in 0..array.len() {
        if index > 0 {
            out.push(',');
        }
        if array.is_null(index) {
            out.push_str("NULL");
            continue;
        }
        let value = field_text(array, index, element)?.unwrap_or_default();
        let needs_quote = value.is_empty()
            || value.trim() != value
            || value.eq_ignore_ascii_case("NULL")
            || value
                .chars()
                .any(|c| matches!(c, ',' | '{' | '}' | '"' | '\\'));
        if needs_quote {
            out.push('"');
            for character in value.chars() {
                if character == '"' || character == '\\' {
                    out.push('\\');
                }
                out.push(character);
            }
            out.push('"');
        } else {
            out.push_str(&value);
        }
    }
    out.push('}');
    Ok(out)
}

fn downcast<T: Array + 'static>(array: &ArrayRef) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .expect("the Arrow array matches its declared type")
}

fn float_text32(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        value.to_string()
    }
}

fn float_text64(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        value.to_string()
    }
}

fn bytea_text(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn date_text(days: i32) -> PgWireResult<String> {
    let date = NaiveDate::from_ymd_opt(1970, 1, 1)
        .expect("the Unix epoch is a date")
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .ok_or_else(|| user_error("22007", "date out of range".to_string()))?;
    Ok(date.format("%Y-%m-%d").to_string())
}

fn timestamp_text(micros: i64, has_time_zone: bool) -> PgWireResult<String> {
    let timestamp = DateTime::<Utc>::from_timestamp_micros(micros)
        .ok_or_else(out_of_range)?
        .naive_utc();
    let mut text = timestamp.format("%Y-%m-%d %H:%M:%S").to_string();
    let sub_micros = timestamp.and_utc().timestamp_subsec_micros();
    if sub_micros != 0 {
        let fraction = format!("{sub_micros:06}");
        text.push('.');
        text.push_str(fraction.trim_end_matches('0'));
    }
    if has_time_zone {
        text.push_str("+00");
    }
    Ok(text)
}

fn decimal_text(value: i128, scale: i8) -> PgWireResult<String> {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let mut text = if scale <= 0 {
        digits
    } else {
        let scale = scale as usize;
        let mut digits = digits;
        if digits.len() <= scale {
            digits = format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits);
        }
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        text.insert(0, '-');
    }
    Ok(text)
}

fn out_of_range() -> PgWireError {
    user_error("22007", "timestamp out of range".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    fn text_options() -> CopyOptions {
        CopyOptions {
            format: CopyFormat::Text,
            delimiter: b'\t',
            null: b"\\N".to_vec(),
            quote: b'"',
            escape: b'"',
            header: false,
        }
    }

    fn csv_options() -> CopyOptions {
        CopyOptions {
            format: CopyFormat::Csv,
            delimiter: b',',
            null: Vec::new(),
            quote: b'"',
            escape: b'"',
            header: true,
        }
    }

    fn batch() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let ids = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let labels = Arc::new(StringArray::from(vec![Some("a\tb"), Some("x,y"), None]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ids, labels]).unwrap();
        (schema, batch)
    }

    #[test]
    fn array_text_quotes_elements_that_need_it() {
        let element = Field::new("item", DataType::Utf8, true);
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a,b"),
            Some("NULL"),
            None,
            Some(" lead"),
            Some("{}"),
            Some("plain"),
        ]));
        assert_eq!(
            array_text(&array, &element).unwrap(),
            r#"{"a,b","NULL",NULL," lead","{}",plain}"#
        );
    }

    #[test]
    fn text_escapes_delimiters_and_nulls() {
        let (schema, batch) = batch();
        let options = text_options();
        let first = encode_row(&batch, 0, &schema, &options).unwrap();
        assert_eq!(first.as_ref(), b"1\ta\\tb\n");
        let second = encode_row(&batch, 1, &schema, &options).unwrap();
        assert_eq!(second.as_ref(), b"2\tx,y\n");
        let third = encode_row(&batch, 2, &schema, &options).unwrap();
        assert_eq!(third.as_ref(), b"\\N\t\\N\n");
    }

    #[test]
    fn csv_quotes_commas_and_nulls() {
        let (schema, batch) = batch();
        let options = csv_options();
        assert_eq!(encode_header(&schema, &options).as_ref(), b"id,label\n");
        let first = encode_row(&batch, 0, &schema, &options).unwrap();
        assert_eq!(first.as_ref(), b"1,a\tb\n");
        let second = encode_row(&batch, 1, &schema, &options).unwrap();
        assert_eq!(second.as_ref(), b"2,\"x,y\"\n");
        let third = encode_row(&batch, 2, &schema, &options).unwrap();
        assert_eq!(third.as_ref(), b",\n");
    }

    #[test]
    fn csv_quotes_a_value_equal_to_a_custom_null() {
        let (schema, batch) = batch();
        let mut options = csv_options();
        options.null = b"NULL".to_vec();
        let third = encode_row(&batch, 2, &schema, &options).unwrap();
        assert_eq!(third.as_ref(), b"NULL,NULL\n");
    }

    #[test]
    fn scalars_use_postgres_output_forms() {
        assert_eq!(bytea_text(&[0xde, 0xad]), "\\xdead");
        assert_eq!(decimal_text(123400, 4).unwrap(), "12.3400");
        assert_eq!(decimal_text(-5, 2).unwrap(), "-0.05");
        assert_eq!(timestamp_text(0, true).unwrap(), "1970-01-01 00:00:00+00");
    }
}
