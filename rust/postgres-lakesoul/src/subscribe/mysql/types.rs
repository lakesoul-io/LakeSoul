// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! MySQL column types and value conversion.
//!
//! The binlog and the snapshot path both hand us `mysql_common::Value`s. A
//! value alone is ambiguous (a `TIME` can exceed 24 hours, a `TIMESTAMP` is an
//! epoch second, `ENUM` is an ordinal), so every conversion also receives the
//! declared column type from `information_schema`.

use std::sync::Arc;

use arrow_schema::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use datafusion::common::ScalarValue;
use mysql_common::binlog::value::BinlogValue;
use mysql_common::value::Value;
use rootcause::{bail, report};

use crate::Result;

/// MySQL's declared column type, normalized enough for CDC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MySqlType {
    TinyInt { unsigned: bool },
    SmallInt { unsigned: bool },
    MediumInt { unsigned: bool },
    Int { unsigned: bool },
    BigInt { unsigned: bool },
    Decimal { precision: u8, scale: u8 },
    Float,
    Double,
    Bit { bits: u16 },
    Date,
    DateTime { precision: u8 },
    Timestamp { precision: u8 },
    Time { precision: u8 },
    Year,
    Char { length: Option<u32> },
    VarChar { length: Option<u32> },
    Text,
    Binary { length: Option<u32> },
    VarBinary { length: Option<u32> },
    Blob,
    Enum { labels: Vec<String> },
    Set { labels: Vec<String> },
    Json,
    Geometry,
    Vector { dimensions: Option<u32> },
    Unsupported(String),
}

impl MySqlType {
    pub fn from_information_schema(
        data_type: &str,
        column_type: &str,
        numeric_precision: Option<u64>,
        numeric_scale: Option<u64>,
        datetime_precision: Option<u64>,
    ) -> Self {
        let data_type = data_type.trim().to_ascii_lowercase();
        let column_type_lc = column_type.trim().to_ascii_lowercase();
        let unsigned = column_type_lc.contains("unsigned");

        match data_type.as_str() {
            "tinyint" => Self::TinyInt { unsigned },
            "smallint" => Self::SmallInt { unsigned },
            "mediumint" => Self::MediumInt { unsigned },
            "int" | "integer" => Self::Int { unsigned },
            "bigint" => Self::BigInt { unsigned },
            "decimal" | "numeric" => {
                let (precision, scale) = decimal_precision_scale(
                    &column_type_lc,
                    numeric_precision,
                    numeric_scale,
                );
                Self::Decimal { precision, scale }
            }
            "float" => Self::Float,
            "double" | "real" => Self::Double,
            "bit" => Self::Bit {
                bits: integer_in_parens(&column_type_lc).unwrap_or(1) as u16,
            },
            "date" => Self::Date,
            "datetime" => Self::DateTime {
                precision: datetime_precision.unwrap_or_else(|| {
                    u64::from(integer_in_parens(&column_type_lc).unwrap_or(0))
                }) as u8,
            },
            "timestamp" => Self::Timestamp {
                precision: datetime_precision.unwrap_or_else(|| {
                    u64::from(integer_in_parens(&column_type_lc).unwrap_or(0))
                }) as u8,
            },
            "time" => Self::Time {
                precision: datetime_precision.unwrap_or_else(|| {
                    u64::from(integer_in_parens(&column_type_lc).unwrap_or(0))
                }) as u8,
            },
            "year" => Self::Year,
            "char" => Self::Char {
                length: integer_in_parens(&column_type_lc),
            },
            "varchar" => Self::VarChar {
                length: integer_in_parens(&column_type_lc),
            },
            "tinytext" | "text" | "mediumtext" | "longtext" => Self::Text,
            "binary" => Self::Binary {
                length: integer_in_parens(&column_type_lc),
            },
            "varbinary" => Self::VarBinary {
                length: integer_in_parens(&column_type_lc),
            },
            "tinyblob" | "blob" | "mediumblob" | "longblob" => Self::Blob,
            "enum" => Self::Enum {
                labels: parse_set_labels(column_type),
            },
            "set" => Self::Set {
                labels: parse_set_labels(column_type),
            },
            "json" => Self::Json,
            "geometry" | "point" | "linestring" | "polygon" | "multipoint"
            | "multilinestring" | "multipolygon" | "geometrycollection" => Self::Geometry,
            "vector" => Self::Vector {
                dimensions: integer_in_parens(&column_type_lc),
            },
            other => Self::Unsupported(format!("{other}: {column_type}")),
        }
    }

    pub fn arrow_type(&self) -> Result<DataType> {
        Ok(match self {
            Self::TinyInt { unsigned: false } => DataType::Int8,
            Self::TinyInt { unsigned: true } => DataType::UInt8,
            Self::SmallInt { unsigned: false } => DataType::Int16,
            Self::SmallInt { unsigned: true } => DataType::UInt16,
            Self::MediumInt { unsigned: false } => DataType::Int32,
            Self::MediumInt { unsigned: true } => DataType::UInt32,
            Self::Int { unsigned: false } => DataType::Int32,
            Self::Int { unsigned: true } => DataType::UInt32,
            Self::BigInt { unsigned: false } => DataType::Int64,
            Self::BigInt { unsigned: true } => DataType::UInt64,
            Self::Decimal { precision, scale } => {
                if *precision > 38 {
                    bail!(
                        "DECIMAL({precision},{scale}) exceeds Decimal128 precision 38; \
                         P0 supports at most 38 digits"
                    );
                }
                DataType::Decimal128(
                    *precision,
                    i8::try_from(*scale)
                        .map_err(|_| report!("DECIMAL scale {scale} exceeds i8"))?,
                )
            }
            Self::Float => DataType::Float32,
            Self::Double => DataType::Float64,
            Self::Bit { .. } => DataType::Binary,
            Self::Date => DataType::Date32,
            Self::DateTime { .. } => DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                None,
            ),
            Self::Timestamp { .. } => DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            ),
            Self::Time { .. } => DataType::Int64,
            Self::Year => DataType::Int16,
            Self::Char { .. } | Self::VarChar { .. } | Self::Text => DataType::Utf8,
            Self::Binary { .. } | Self::VarBinary { .. } | Self::Blob => DataType::Binary,
            Self::Enum { .. } | Self::Set { .. } => DataType::Utf8,
            Self::Json => DataType::Utf8,
            Self::Geometry | Self::Vector { .. } | Self::Unsupported(_) => {
                bail!("MySQL type {self:?} is not supported by the P0 subscriber")
            }
        })
    }
}

fn integer_in_parens(value: &str) -> Option<u32> {
    let start = value.find('(')?;
    let end = value[start + 1..].find(')')? + start + 1;
    value[start + 1..end].trim().parse().ok()
}

fn decimal_precision_scale(
    column_type: &str,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
) -> (u8, u8) {
    if let Some((precision, scale)) = column_type
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .and_then(|(inside, _)| inside.split_once(','))
        && let (Ok(precision), Ok(scale)) =
            (precision.trim().parse::<u8>(), scale.trim().parse::<u8>())
    {
        return (precision, scale);
    }
    (
        numeric_precision.unwrap_or(10).min(u64::from(u8::MAX)) as u8,
        numeric_scale.unwrap_or(0).min(u64::from(u8::MAX)) as u8,
    )
}

/// Parse `enum('a','b')` / `set('a,b','c')` into labels.
fn parse_set_labels(column_type: &str) -> Vec<String> {
    let Some(open) = column_type.find('(') else {
        return Vec::new();
    };
    let Some(close) = column_type.rfind(')') else {
        return Vec::new();
    };
    if close <= open {
        return Vec::new();
    }
    let inside = &column_type[open + 1..close];
    let mut labels = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = inside.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_quote {
            match ch {
                '\'' => {
                    if chars.peek() == Some(&'\'') {
                        current.push('\'');
                        chars.next();
                    } else {
                        in_quote = false;
                    }
                }
                '\\' => {
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                }
                other => current.push(other),
            }
        } else if ch == '\'' {
            in_quote = true;
            current.clear();
        } else if ch == ',' {
            labels.push(std::mem::take(&mut current));
        }
    }
    if in_quote || !current.is_empty() {
        labels.push(current);
    }
    labels
}

/// Convert a snapshot / text-protocol MySQL value into the target Arrow value.
pub fn value_to_scalar(
    value: &Value,
    column_type: &MySqlType,
    charset: Option<&str>,
    target: &DataType,
) -> Result<ScalarValue> {
    if matches!(value, Value::NULL) {
        return super::super::decode::null_scalar(target);
    }

    let scalar = match column_type {
        MySqlType::TinyInt { unsigned } => int_scalar(value, *unsigned, target)?,
        MySqlType::SmallInt { unsigned } => int_scalar(value, *unsigned, target)?,
        MySqlType::MediumInt { unsigned } => int_scalar(value, *unsigned, target)?,
        MySqlType::Int { unsigned } => int_scalar(value, *unsigned, target)?,
        MySqlType::BigInt { unsigned } => int_scalar(value, *unsigned, target)?,
        MySqlType::Year => ScalarValue::Int16(Some(match value {
            Value::Int(v) => {
                i16::try_from(*v).map_err(|_| report!("YEAR {v} out of range"))?
            }
            Value::UInt(v) => {
                i16::try_from(*v).map_err(|_| report!("YEAR {v} out of range"))?
            }
            Value::Bytes(bytes) => decode_text(bytes, charset)?
                .parse::<i16>()
                .map_err(|error| report!("invalid YEAR value: {error}"))?,
            other => bail!("YEAR has unexpected value {other:?}"),
        })),
        MySqlType::Decimal { precision, scale } => {
            let text = bytes_text(value, charset)?;
            let mantissa = parse_decimal(&text, *precision, *scale)?;
            ScalarValue::Decimal128(
                Some(mantissa),
                *precision,
                i8::try_from(*scale)
                    .map_err(|_| report!("DECIMAL scale {scale} exceeds i8"))?,
            )
        }
        MySqlType::Float => ScalarValue::Float32(Some(match value {
            Value::Float(v) => *v,
            Value::Double(v) => *v as f32,
            Value::Int(v) => *v as f32,
            Value::UInt(v) => *v as f32,
            Value::Bytes(bytes) => decode_text(bytes, charset)?
                .parse::<f32>()
                .map_err(|error| report!("invalid FLOAT value: {error}"))?,
            other => bail!("FLOAT has unexpected value {other:?}"),
        })),
        MySqlType::Double => ScalarValue::Float64(Some(match value {
            Value::Double(v) => *v,
            Value::Float(v) => f64::from(*v),
            Value::Int(v) => *v as f64,
            Value::UInt(v) => *v as f64,
            Value::Bytes(bytes) => decode_text(bytes, charset)?
                .parse::<f64>()
                .map_err(|error| report!("invalid DOUBLE value: {error}"))?,
            other => bail!("DOUBLE has unexpected value {other:?}"),
        })),
        MySqlType::Bit { .. } => ScalarValue::Binary(Some(match value {
            Value::Bytes(bytes) => bytes.clone(),
            Value::Int(v) => v.to_be_bytes().to_vec(),
            Value::UInt(v) => v.to_be_bytes().to_vec(),
            other => bail!("BIT has unexpected value {other:?}"),
        })),
        MySqlType::Date => ScalarValue::Date32(Some(date_days(value)?)),
        MySqlType::DateTime { .. } => {
            ScalarValue::TimestampMicrosecond(Some(datetime_micros(value, false)?), None)
        }
        MySqlType::Timestamp { .. } => ScalarValue::TimestampMicrosecond(
            Some(datetime_micros(value, true)?),
            Some(Arc::from("UTC")),
        ),
        MySqlType::Time { .. } => ScalarValue::Int64(Some(time_micros(value)?)),
        MySqlType::Char { .. } | MySqlType::VarChar { .. } | MySqlType::Text => {
            ScalarValue::Utf8(Some(bytes_text(value, charset)?))
        }
        MySqlType::Binary { .. } | MySqlType::VarBinary { .. } | MySqlType::Blob => {
            ScalarValue::Binary(Some(match value {
                Value::Bytes(bytes) => bytes.clone(),
                other => bail!("binary column has unexpected value {other:?}"),
            }))
        }
        MySqlType::Enum { labels } => {
            ScalarValue::Utf8(Some(enum_label(value, labels, charset)?))
        }
        MySqlType::Set { labels } => ScalarValue::Utf8(Some(match value {
            // The snapshot/text protocol returns the selected labels as text.
            // The binlog returns a bitmask, handled in
            // `binlog_value_to_scalar` before this function is reached.
            Value::Bytes(bytes) => decode_text(bytes, charset)?,
            Value::UInt(mask) => labels_from_mask(*mask, labels),
            Value::Int(mask) => labels_from_mask(*mask as u64, labels),
            other => bail!("SET has unexpected value {other:?}"),
        })),
        MySqlType::Json => ScalarValue::Utf8(Some(bytes_text(value, charset)?)),
        MySqlType::Geometry | MySqlType::Vector { .. } | MySqlType::Unsupported(_) => {
            bail!("MySQL type {column_type:?} is not supported by the P0 subscriber")
        }
    };

    cast_if_needed(scalar, target)
}

/// Convert a binlog value. JSON arrives as a parsed JSONB tree rather than a
/// byte string.
pub fn binlog_value_to_scalar(
    value: BinlogValue<'_>,
    column_type: &MySqlType,
    charset: Option<&str>,
    target: &DataType,
) -> Result<ScalarValue> {
    // A binlog SET is a bitmask, not text. Treating the bytes as text happens
    // to work for some masks and silently drops members for others.
    if let (MySqlType::Set { labels }, BinlogValue::Value(Value::Bytes(bytes))) =
        (column_type, &value)
    {
        // MySQL stores a SET bitmask as 1-8 raw little-endian bytes.
        let mask = bytes.iter().enumerate().fold(0u64, |mask, (index, byte)| {
            mask | (u64::from(*byte) << (8 * index))
        });
        return cast_if_needed(
            ScalarValue::Utf8(Some(labels_from_mask(mask, labels))),
            target,
        );
    }
    match value {
        BinlogValue::Value(value) => {
            value_to_scalar(&value, column_type, charset, target)
        }
        BinlogValue::Jsonb(value) => {
            let dom: serde_json::Value = value
                .parse()
                .map_err(|error| report!("decode JSONB value: {error}"))?
                .into();
            let text = serde_json::to_string(&dom)
                .map_err(|error| report!("serialize JSON value: {error}"))?;
            cast_if_needed(ScalarValue::Utf8(Some(text)), target)
        }
        BinlogValue::JsonDiff(_) => bail!(
            "MySQL partial JSON updates (binlog_row_value_options=PARTIAL_JSON) are \
             not supported; clear binlog_row_value_options on the source"
        ),
    }
}

fn cast_if_needed(scalar: ScalarValue, target: &DataType) -> Result<ScalarValue> {
    if scalar.data_type() == *target {
        Ok(scalar)
    } else {
        scalar
            .cast_to(target)
            .map_err(|error| report!("cast MySQL value to {target}: {error}"))
    }
}

fn int_scalar(value: &Value, unsigned: bool, target: &DataType) -> Result<ScalarValue> {
    if unsigned {
        let value = match value {
            Value::UInt(v) => *v,
            Value::Int(v) => u64::try_from(*v)
                .map_err(|_| report!("negative value {v} in unsigned column"))?,
            Value::Bytes(bytes) => decode_text(bytes, None)?
                .parse::<u64>()
                .map_err(|error| report!("invalid unsigned value: {error}"))?,
            other => bail!("integer column has unexpected value {other:?}"),
        };
        Ok(match target {
            DataType::UInt8 => ScalarValue::UInt8(Some(
                u8::try_from(value).map_err(|_| report!("{value} does not fit UInt8"))?,
            )),
            DataType::UInt16 => ScalarValue::UInt16(Some(
                u16::try_from(value)
                    .map_err(|_| report!("{value} does not fit UInt16"))?,
            )),
            DataType::UInt32 => ScalarValue::UInt32(Some(
                u32::try_from(value)
                    .map_err(|_| report!("{value} does not fit UInt32"))?,
            )),
            DataType::UInt64 => ScalarValue::UInt64(Some(value)),
            _ => bail!("unsigned integer mapped to {target}"),
        })
    } else {
        let value = match value {
            Value::Int(v) => *v,
            Value::UInt(v) => i64::try_from(*v)
                .map_err(|_| report!("{v} does not fit signed column"))?,
            Value::Bytes(bytes) => decode_text(bytes, None)?
                .parse::<i64>()
                .map_err(|error| report!("invalid integer value: {error}"))?,
            other => bail!("integer column has unexpected value {other:?}"),
        };
        Ok(match target {
            DataType::Int8 => ScalarValue::Int8(Some(
                i8::try_from(value).map_err(|_| report!("{value} does not fit Int8"))?,
            )),
            DataType::Int16 => ScalarValue::Int16(Some(
                i16::try_from(value)
                    .map_err(|_| report!("{value} does not fit Int16"))?,
            )),
            DataType::Int32 => ScalarValue::Int32(Some(
                i32::try_from(value)
                    .map_err(|_| report!("{value} does not fit Int32"))?,
            )),
            DataType::Int64 => ScalarValue::Int64(Some(value)),
            _ => bail!("signed integer mapped to {target}"),
        })
    }
}

fn date_days(value: &Value) -> Result<i32> {
    let (year, month, day) = match value {
        Value::Date(year, month, day, _, _, _, _) => (*year, *month, *day),
        Value::Bytes(bytes) => {
            let text = decode_text(bytes, None)?;
            let date = NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                .map_err(|error| report!("invalid DATE {text:?}: {error}"))?;
            (date.year() as u16, date.month() as u8, date.day() as u8)
        }
        other => bail!("DATE has unexpected value {other:?}"),
    };
    let date = NaiveDate::from_ymd_opt(i32::from(year), u32::from(month), u32::from(day))
        .ok_or_else(|| report!("invalid DATE {year:04}-{month:02}-{day:02}"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| report!("DATE out of Date32 range"))
}

fn datetime_micros(value: &Value, timestamp: bool) -> Result<i64> {
    let naive = match value {
        Value::Date(year, month, day, hour, minute, second, micros) => {
            NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day))
                .and_then(|date| {
                    date.and_hms_micro_opt(
                        u32::from(*hour),
                        u32::from(*minute),
                        u32::from(*second),
                        *micros,
                    )
                })
                .ok_or_else(|| {
                    report!(
                        "invalid DATETIME {year:04}-{month:02}-{day:02} \
                         {hour:02}:{minute:02}:{second:02}.{micros:06}"
                    )
                })?
        }
        Value::Int(seconds) => chrono::DateTime::from_timestamp(*seconds, 0)
            .ok_or_else(|| report!("invalid epoch timestamp {seconds}"))?
            .naive_utc(),
        Value::Bytes(bytes) => {
            let text = decode_text(bytes, None)?;
            parse_epoch_or_datetime(&text)?
        }
        other => bail!("datetime column has unexpected value {other:?}"),
    };
    let _ = timestamp;
    Ok(naive.and_utc().timestamp_micros())
}

fn parse_epoch_or_datetime(text: &str) -> Result<NaiveDateTime> {
    if let Ok(datetime) = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(datetime);
    }
    if let Ok(datetime) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(datetime);
    }
    let (seconds, micros) = text.split_once('.').unwrap_or((text, "0"));
    let seconds: i64 = seconds
        .parse()
        .map_err(|error| report!("invalid epoch timestamp {text:?}: {error}"))?;
    let micros: i64 = if micros.is_empty() {
        0
    } else {
        format!("{micros:0<6}")[..6]
            .parse()
            .map_err(|error| report!("invalid epoch microseconds {text:?}: {error}"))?
    };
    let total_micros = seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(micros))
        .ok_or_else(|| report!("epoch timestamp {text:?} is out of range"))?;
    chrono::DateTime::from_timestamp_micros(total_micros)
        .map(|value| value.naive_utc())
        .ok_or_else(|| report!("invalid epoch timestamp {text:?}"))
}

fn time_micros(value: &Value) -> Result<i64> {
    match value {
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let magnitude = (i64::from(*days) * 86_400
                + i64::from(*hours) * 3_600
                + i64::from(*minutes) * 60
                + i64::from(*seconds))
                * 1_000_000
                + i64::from(*micros);
            Ok(if *negative { -magnitude } else { magnitude })
        }
        Value::Bytes(bytes) => {
            let text = decode_text(bytes, None)?;
            parse_time_text(&text)
        }
        other => bail!("TIME has unexpected value {other:?}"),
    }
}

fn parse_time_text(text: &str) -> Result<i64> {
    let (negative, text) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (clock, micros) = text.split_once('.').unwrap_or((text, "0"));
    let mut parts = clock.split(':');
    let hours: i64 = parts
        .next()
        .ok_or_else(|| report!("invalid TIME {text:?}"))?
        .parse()
        .map_err(|error| report!("invalid TIME hours: {error}"))?;
    let minutes: i64 = parts
        .next()
        .unwrap_or("0")
        .parse()
        .map_err(|error| report!("invalid TIME minutes: {error}"))?;
    let seconds: i64 = parts
        .next()
        .unwrap_or("0")
        .parse()
        .map_err(|error| report!("invalid TIME seconds: {error}"))?;
    let micros: i64 = if micros.is_empty() {
        0
    } else {
        format!("{micros:0<6}")[..6]
            .parse()
            .map_err(|error| report!("invalid TIME microseconds: {error}"))?
    };
    let value = (hours * 3_600 + minutes * 60 + seconds) * 1_000_000 + micros;
    Ok(if negative { -value } else { value })
}

fn enum_label(value: &Value, labels: &[String], charset: Option<&str>) -> Result<String> {
    match value {
        Value::Int(index) => {
            let index = usize::try_from(*index)
                .map_err(|_| report!("ENUM index {index} is negative"))?;
            labels.get(index.saturating_sub(1)).cloned().ok_or_else(|| {
                report!("ENUM index {index} outside {} labels", labels.len())
            })
        }
        Value::UInt(index) => enum_label(&Value::Int(*index as i64), labels, charset),
        Value::Bytes(bytes) => {
            let label = decode_text(bytes, charset)?;
            if labels.iter().any(|candidate| candidate == &label) || labels.is_empty() {
                Ok(label)
            } else {
                bail!("ENUM value {label:?} is not among the declared labels")
            }
        }
        other => bail!("ENUM has unexpected value {other:?}"),
    }
}

fn labels_from_mask(mask: u64, labels: &[String]) -> String {
    labels
        .iter()
        .enumerate()
        .filter(|(index, _)| mask & (1u64 << index) != 0)
        .map(|(_, label)| label.clone())
        .collect::<Vec<_>>()
        .join(",")
}

fn bytes_text(value: &Value, charset: Option<&str>) -> Result<String> {
    match value {
        Value::Bytes(bytes) => decode_text(bytes, charset),
        Value::Int(value) => Ok(value.to_string()),
        Value::UInt(value) => Ok(value.to_string()),
        Value::Float(value) => Ok(value.to_string()),
        Value::Double(value) => Ok(value.to_string()),
        Value::Date(year, month, day, hour, minute, second, micros) => Ok(format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
        )),
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if *negative { "-" } else { "" };
            Ok(format!(
                "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            ))
        }
        Value::NULL => bail!("unexpected NULL in non-null conversion"),
    }
}

fn decode_text(bytes: &[u8], charset: Option<&str>) -> Result<String> {
    let encoding = charset
        .and_then(|label| encoding_rs::Encoding::for_label(label.as_bytes()))
        .unwrap_or(encoding_rs::UTF_8);
    let (text, _, had_errors) = encoding.decode(bytes);
    if had_errors {
        bail!(
            "MySQL text column is not valid {}: {}",
            encoding.name(),
            String::from_utf8_lossy(bytes)
        );
    }
    Ok(text.into_owned())
}

/// Parse a MySQL decimal string into a Decimal128 mantissa without rounding.
fn parse_decimal(text: &str, precision: u8, scale: u8) -> Result<i128> {
    let text = text.trim();
    if text.eq_ignore_ascii_case("nan")
        || text.eq_ignore_ascii_case("infinity")
        || text.eq_ignore_ascii_case("-infinity")
    {
        bail!("DECIMAL NaN/Infinity is not supported");
    }
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (integer, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    let integer = if integer.is_empty() { "0" } else { integer };
    if !integer.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        bail!("DECIMAL {text:?} contains non-digit characters");
    }
    if fraction.len() > usize::from(scale) {
        bail!("DECIMAL {text:?} has more than {scale} fractional digits");
    }
    let mut digits = String::with_capacity(integer.len() + usize::from(scale));
    digits.push_str(integer);
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n(
        '0',
        usize::from(scale) - fraction.len(),
    ));
    if digits.len() > usize::from(precision) {
        bail!("DECIMAL {text:?} exceeds precision {precision}");
    }
    let value: i128 = digits
        .parse()
        .map_err(|error| report!("parse DECIMAL {text:?}: {error}"))?;
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_column_types() {
        assert_eq!(
            MySqlType::from_information_schema(
                "bigint",
                "bigint unsigned",
                Some(20),
                Some(0),
                None
            ),
            MySqlType::BigInt { unsigned: true }
        );
        assert_eq!(
            MySqlType::from_information_schema(
                "decimal",
                "decimal(20,6)",
                Some(20),
                Some(6),
                None
            ),
            MySqlType::Decimal {
                precision: 20,
                scale: 6
            }
        );
        assert_eq!(
            MySqlType::from_information_schema(
                "enum",
                "enum('a','b''c')",
                None,
                None,
                None
            ),
            MySqlType::Enum {
                labels: vec!["a".into(), "b'c".into()]
            }
        );
    }

    #[test]
    fn parses_decimal_exactly() {
        assert_eq!(parse_decimal("12345.6789", 10, 4).unwrap(), 123456789);
        assert_eq!(parse_decimal("-0.01", 4, 2).unwrap(), -1);
        assert!(parse_decimal("1.234", 4, 2).is_err());
    }

    #[test]
    fn integer_unsigned_keeps_range() {
        let value = Value::UInt(u64::MAX);
        let scalar = value_to_scalar(
            &value,
            &MySqlType::BigInt { unsigned: true },
            None,
            &DataType::UInt64,
        )
        .unwrap();
        assert_eq!(scalar, ScalarValue::UInt64(Some(u64::MAX)));
    }

    #[test]
    fn set_snapshot_text_and_binlog_mask_agree() {
        let labels = vec!["read".into(), "write".into(), "admin".into()];
        let column = MySqlType::Set {
            labels: labels.clone(),
        };
        let snapshot = value_to_scalar(
            &Value::Bytes(b"read,admin".to_vec()),
            &column,
            None,
            &DataType::Utf8,
        )
        .unwrap();
        assert_eq!(snapshot, ScalarValue::Utf8(Some("read,admin".into())));
        let binlog = binlog_value_to_scalar(
            BinlogValue::Value(Value::Bytes(vec![0b101])),
            &column,
            None,
            &DataType::Utf8,
        )
        .unwrap();
        assert_eq!(binlog, ScalarValue::Utf8(Some("read,admin".into())));

        let many = MySqlType::Set {
            labels: (0..10).map(|index| format!("v{index}")).collect(),
        };
        let mask = binlog_value_to_scalar(
            BinlogValue::Value(Value::Bytes(vec![0, 2])),
            &many,
            None,
            &DataType::Utf8,
        )
        .unwrap();
        assert_eq!(mask, ScalarValue::Utf8(Some("v9".into())));
    }

    #[test]
    fn epoch_timestamp_keeps_fractional_microseconds() {
        let value = parse_epoch_or_datetime("1794231000.333000").unwrap();
        assert_eq!(value.and_utc().timestamp_subsec_micros(), 333000);
        let scalar = value_to_scalar(
            &Value::Bytes(b"1794231000.333000".to_vec()),
            &MySqlType::Timestamp { precision: 3 },
            None,
            &DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            ),
        )
        .unwrap();
        match scalar {
            ScalarValue::TimestampMicrosecond(Some(micros), _) => {
                assert_eq!(micros % 1_000_000, 333_000);
            }
            other => panic!("unexpected scalar {other:?}"),
        }
    }

    #[test]
    fn set_mask_maps_labels() {
        let value = Value::UInt(0b101);
        let scalar = value_to_scalar(
            &value,
            &MySqlType::Set {
                labels: vec!["read".into(), "write".into(), "admin".into()],
            },
            None,
            &DataType::Utf8,
        )
        .unwrap();
        assert_eq!(scalar, ScalarValue::Utf8(Some("read,admin".into())));
    }
}
