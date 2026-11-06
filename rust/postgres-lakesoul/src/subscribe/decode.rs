// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL text-format values to Arrow `ScalarValue`s.
//!
//! P0 uses pgoutput's text mode because it is available on every supported
//! server and shares its representation with the initial-snapshot COPY path.
//! Binary mode (and the type-catalog work it needs for enums, domains and
//! composites) is deliberately deferred.

use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use datafusion::common::ScalarValue;
use pg_walstream::ColumnValue;
use rootcause::{bail, report};

use super::source::{PgType, SourceColumn};
use crate::Result;

/// A normalized primary-key value. Unlike `ScalarValue`, this is `Hash + Eq`
/// so it can key the per-batch deduplication map.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    Bool(bool),
    Int(i64),
    Float(u64),
    Decimal(i128, i8),
    Text(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    Timestamp(i64, bool),
}

pub fn parse_value(
    column: &SourceColumn,
    target: &DataType,
    value: &ColumnValue,
) -> Result<ScalarValue> {
    let ColumnValue::Text(bytes) = value else {
        return match value {
            ColumnValue::Null => null_scalar(target),
            ColumnValue::Binary(_) => bail!(
                "column {} arrived in pgoutput binary format; P0 only enables text mode",
                column.name
            ),
            ColumnValue::Text(_) => unreachable!(),
        };
    };
    let text = std::str::from_utf8(bytes).map_err(|error| {
        report!(
            "column {} is not valid UTF-8 in pgoutput: {error}",
            column.name
        )
    })?;

    match &column.pg_type {
        PgType::Bool => {
            let value = match text {
                "t" | "true" => true,
                "f" | "false" => false,
                other => bail!("column {} has invalid bool value {other:?}", column.name),
            };
            Ok(ScalarValue::Boolean(Some(value)))
        }
        PgType::Int2 => Ok(ScalarValue::Int16(Some(parse_number(&column.name, text)?))),
        PgType::Int4 => Ok(ScalarValue::Int32(Some(parse_number(&column.name, text)?))),
        PgType::Int8 => Ok(ScalarValue::Int64(Some(parse_number(&column.name, text)?))),
        PgType::Float4 => {
            Ok(ScalarValue::Float32(Some(parse_float(&column.name, text)?)))
        }
        PgType::Float8 => {
            Ok(ScalarValue::Float64(Some(parse_float(&column.name, text)?)))
        }
        PgType::Numeric { .. } => {
            let DataType::Decimal128(precision, scale) = target else {
                bail!(
                    "column {} maps numeric to {target}; P0 requires decimal128",
                    column.name
                );
            };
            let value = parse_decimal(text, *precision, *scale)
                .map_err(|error| report!("column {}: {error}", column.name))?;
            Ok(ScalarValue::Decimal128(Some(value), *precision, *scale))
        }
        PgType::Text | PgType::Uuid | PgType::Json | PgType::Jsonb => {
            Ok(ScalarValue::Utf8(Some(text.to_string())))
        }
        PgType::Bytea => Ok(ScalarValue::Binary(Some(
            parse_bytea(text)
                .map_err(|error| report!("column {}: {error}", column.name))?,
        ))),
        PgType::Date => {
            let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|error| {
                report!("column {}: invalid date {text:?}: {error}", column.name)
            })?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date");
            let days = date.signed_duration_since(epoch).num_days();
            let days = i32::try_from(days).map_err(|_| {
                report!("column {}: date out of Date32 range", column.name)
            })?;
            Ok(ScalarValue::Date32(Some(days)))
        }
        PgType::Time => {
            let time = parse_time(text)
                .map_err(|error| report!("column {}: {error}", column.name))?;
            let micros = time.num_seconds_from_midnight() as i64 * 1_000_000
                + time.nanosecond() as i64 / 1_000;
            Ok(ScalarValue::Time64Microsecond(Some(micros)))
        }
        PgType::Timestamp => {
            let value = parse_naive_datetime(text)
                .map_err(|error| report!("column {}: {error}", column.name))?;
            let micros = value.and_utc().timestamp_micros();
            Ok(ScalarValue::TimestampMicrosecond(Some(micros), None))
        }
        PgType::TimestampTz => {
            let value = parse_datetime_tz(text)
                .map_err(|error| report!("column {}: {error}", column.name))?;
            let tz = match target {
                DataType::Timestamp(TimeUnit::Microsecond, tz) if tz.is_some() => {
                    tz.clone()
                }
                _ => bail!(
                    "column {} maps timestamptz to {target}; P0 requires \
                     timestamp(microsecond, UTC)",
                    column.name
                ),
            };
            Ok(ScalarValue::TimestampMicrosecond(
                Some(value.timestamp_micros()),
                tz,
            ))
        }
    }
}

pub fn null_scalar(target: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_from(target)
        .map_err(|error| report!("cannot build a NULL for target type {target}: {error}"))
}

pub fn key_of(column: &SourceColumn, value: &ScalarValue) -> Result<KeyValue> {
    let key = match value {
        ScalarValue::Boolean(Some(value)) => KeyValue::Bool(*value),
        ScalarValue::Int16(Some(value)) => KeyValue::Int(*value as i64),
        ScalarValue::Int32(Some(value)) => KeyValue::Int(*value as i64),
        ScalarValue::Int64(Some(value)) => KeyValue::Int(*value),
        ScalarValue::Float32(Some(value)) => KeyValue::Float(f64::from(*value).to_bits()),
        ScalarValue::Float64(Some(value)) => KeyValue::Float(value.to_bits()),
        ScalarValue::Decimal128(Some(value), _, scale) => {
            KeyValue::Decimal(*value, *scale)
        }
        ScalarValue::Utf8(Some(value)) => KeyValue::Text(value.clone()),
        ScalarValue::Binary(Some(value)) => KeyValue::Bytes(value.clone()),
        ScalarValue::Date32(Some(value)) => KeyValue::Date(*value),
        ScalarValue::Time64Microsecond(Some(value)) => KeyValue::Time(*value),
        ScalarValue::TimestampMicrosecond(Some(value), tz) => {
            KeyValue::Timestamp(*value, tz.is_some())
        }
        other => bail!(
            "primary-key column {} has unsupported or NULL value {other:?}",
            column.name
        ),
    };
    Ok(key)
}

fn parse_number<T>(column: &str, text: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    text.parse::<T>()
        .map_err(|error| report!("column {column} has invalid number {text:?}: {error}"))
}

fn parse_float<T>(column: &str, text: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let normalized = match text {
        "Infinity" | "infinity" => "inf",
        "-Infinity" | "-infinity" => "-inf",
        "NaN" | "nan" => "NaN",
        other => other,
    };
    normalized
        .parse::<T>()
        .map_err(|error| report!("column {column} has invalid float {text:?}: {error}"))
}

/// Parse an exact PostgreSQL numeric into a decimal128 mantissa. Rounding is
/// rejected: a mirror must not silently change a value.
fn parse_decimal(text: &str, precision: u8, scale: i8) -> Result<i128> {
    if scale < 0 {
        bail!("negative numeric scale {scale} is not supported in P0");
    }
    let text = text.trim();
    if text.eq_ignore_ascii_case("nan")
        || text.eq_ignore_ascii_case("infinity")
        || text.eq_ignore_ascii_case("-infinity")
    {
        bail!("numeric NaN/Infinity is not supported in P0");
    }
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (integer, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    if integer.is_empty() && fraction.is_empty() {
        bail!("invalid numeric {text:?}");
    }
    if !integer.chars().all(|c| c.is_ascii_digit())
        || !fraction.chars().all(|c| c.is_ascii_digit())
    {
        bail!("invalid numeric {text:?}");
    }
    let scale = scale as usize;
    if fraction.len() > scale {
        bail!(
            "numeric {text:?} has more fractional digits than the target scale {scale}"
        );
    }
    let integer = integer.trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let mut digits = String::with_capacity(integer.len() + scale);
    digits.push_str(integer);
    digits.push_str(fraction);
    for _ in fraction.len()..scale {
        digits.push('0');
    }
    let significant = digits.trim_start_matches('0');
    let significant = if significant.is_empty() {
        "0"
    } else {
        significant
    };
    if significant.len() > precision as usize {
        bail!(
            "numeric {text:?} exceeds the target precision {precision} at scale {scale}"
        );
    }
    let value: i128 = digits
        .parse()
        .map_err(|error| report!("numeric {text:?} does not fit decimal128: {error}"))?;
    Ok(if negative { -value } else { value })
}

fn parse_bytea(text: &str) -> Result<Vec<u8>> {
    let hex = text.strip_prefix("\\x").ok_or_else(|| {
        report!("bytea output is not hex; set bytea_output=hex on the source")
    })?;
    if !hex.len().is_multiple_of(2) {
        bail!("bytea hex value has an odd length");
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for index in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[index..index + 2], 16)
            .map_err(|error| report!("invalid bytea hex: {error}"))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

fn parse_time(text: &str) -> Result<NaiveTime> {
    for format in ["%H:%M:%S%.f", "%H:%M:%S"] {
        if let Ok(value) = NaiveTime::parse_from_str(text, format) {
            return Ok(value);
        }
    }
    bail!("invalid time {text:?}")
}

fn parse_naive_datetime(text: &str) -> Result<NaiveDateTime> {
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(text, format) {
            return Ok(value);
        }
    }
    bail!("invalid timestamp {text:?}")
}

fn parse_datetime_tz(text: &str) -> Result<DateTime<Utc>> {
    for format in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%d %H:%M:%S%z",
    ] {
        if let Ok(value) = DateTime::parse_from_str(text, format) {
            return Ok(value.with_timezone(&Utc));
        }
    }
    bail!("invalid timestamptz {text:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscribe::source::SourceColumn;

    fn column(pg_type: PgType, name: &str) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            type_id: 0,
            type_modifier: -1,
            pg_type,
            type_name: String::new(),
            not_null: true,
        }
    }

    #[test]
    fn decimal_is_exact_and_scale_aware() {
        assert_eq!(parse_decimal("12.3400", 20, 4).unwrap(), 123_400);
        assert_eq!(parse_decimal("-0.0001", 20, 4).unwrap(), -1);
        assert_eq!(parse_decimal("000123.4500", 20, 4).unwrap(), 1_234_500);
        assert_eq!(parse_decimal("0", 20, 4).unwrap(), 0);
        assert!(parse_decimal("1.23456", 20, 4).is_err());
        assert!(parse_decimal("NaN", 20, 4).is_err());
        assert!(parse_decimal("123456789012345678901234567890123456789", 38, 0).is_err());
    }

    #[test]
    fn integer_and_text_values_parse() {
        let int_column = column(PgType::Int8, "id");
        assert_eq!(
            parse_value(&int_column, &DataType::Int64, &ColumnValue::text("42")).unwrap(),
            ScalarValue::Int64(Some(42))
        );
        let text_column = column(PgType::Text, "name");
        assert_eq!(
            parse_value(&text_column, &DataType::Utf8, &ColumnValue::text("alice"))
                .unwrap(),
            ScalarValue::Utf8(Some("alice".into()))
        );
        assert_eq!(
            parse_value(&int_column, &DataType::Int64, &ColumnValue::Null).unwrap(),
            ScalarValue::Int64(None)
        );
    }

    #[test]
    fn temporal_and_binary_values_parse() {
        let date = column(PgType::Date, "d");
        assert!(matches!(
            parse_value(&date, &DataType::Date32, &ColumnValue::text("2026-01-02"))
                .unwrap(),
            ScalarValue::Date32(Some(_))
        ));
        let timestamp = column(PgType::Timestamp, "ts");
        assert!(matches!(
            parse_value(
                &timestamp,
                &DataType::Timestamp(TimeUnit::Microsecond, None),
                &ColumnValue::text("2026-01-02 03:04:05.123456")
            )
            .unwrap(),
            ScalarValue::TimestampMicrosecond(Some(_), None)
        ));
        let timestamptz = column(PgType::TimestampTz, "tstz");
        assert!(matches!(
            parse_value(
                &timestamptz,
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                &ColumnValue::text("2026-01-02 03:04:05.123456+00")
            )
            .unwrap(),
            ScalarValue::TimestampMicrosecond(Some(_), Some(_))
        ));
        let bytea = column(PgType::Bytea, "b");
        assert_eq!(
            parse_value(&bytea, &DataType::Binary, &ColumnValue::text("\\xdeadbeef"))
                .unwrap(),
            ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef]))
        );
    }

    #[test]
    fn keys_are_normalized() {
        let column = column(PgType::Int8, "id");
        assert_eq!(
            key_of(&column, &ScalarValue::Int64(Some(7))).unwrap(),
            KeyValue::Int(7)
        );
        assert!(key_of(&column, &ScalarValue::Int64(None)).is_err());
    }
}
