// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Decode PostgreSQL binary-format values for `COPY ... FROM STDIN`.
//!
//! A binary COPY field carries no OID: the target column's Arrow type (and
//! its semantic `pg.type` metadata) is the source of truth for how the bytes
//! are interpreted. The formats below follow PostgreSQL's `send` functions:
//! big-endian integers, IEEE floats, a length-prefixed base-10000 numeric,
//! days/microseconds relative to 2000-01-01, and a version byte for `jsonb`.
//!
//! Arrays and pgvector values are deliberately out of scope here; they are
//! rejected with `0A000` rather than silently mis-decoded.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion_postgres::pgwire::error::PgWireResult;

use crate::copy_in::{
    CopyColumn, apply_json_kind, invalid_input, parse_decimal, parse_decimal_i256,
};
use crate::ddl::feature_not_supported;
use crate::insert::user_error;

/// Days between 1970-01-01 and 2000-01-01, the epoch PostgreSQL uses for
/// binary `date` and `timestamp` values.
const PG_EPOCH_DAYS: i64 = 10_957;
const PG_EPOCH_MICROS: i64 = PG_EPOCH_DAYS * 86_400 * 1_000_000;

const NUMERIC_RANGE: &str = "22003";

/// Decode one binary field into the target column's `ScalarValue`.
pub(crate) fn decode_binary_value(
    column: &CopyColumn,
    raw: &[u8],
) -> PgWireResult<ScalarValue> {
    let scalar = decode(column, raw)?;
    if scalar.data_type() == column.data_type {
        Ok(scalar)
    } else {
        scalar.cast_to(&column.data_type).map_err(|error| {
            user_error(
                "42804",
                format!(
                    "binary COPY value for column \"{}\" cannot be converted to {}: {error}",
                    column.name, column.data_type
                ),
            )
        })
    }
}

fn decode(column: &CopyColumn, raw: &[u8]) -> PgWireResult<ScalarValue> {
    // Semantic types first: their physical Arrow representation is shared
    // with an ordinary text column, but their binary payload is not text.
    match column.pg_type.as_deref() {
        Some("uuid") => {
            let bytes: [u8; 16] = fixed(column, "uuid", raw)?;
            return Ok(ScalarValue::Utf8(Some(
                uuid::Uuid::from_bytes(bytes).hyphenated().to_string(),
            )));
        }
        Some("money") => {
            let bytes: [u8; 8] = fixed(column, "money", raw)?;
            let cents = i64::from_be_bytes(bytes);
            return Ok(ScalarValue::Decimal128(Some(i128::from(cents)), 19, 2));
        }
        Some("varchar") | Some("bpchar") => {
            let text = binary_text(column, raw)?;
            if let Some(typmod) = column.typmod
                && typmod >= 4
                && text.chars().count() > (typmod - 4) as usize
            {
                return Err(user_error(
                    "22001",
                    format!(
                        "value too long for type {}({})",
                        if column.pg_type.as_deref() == Some("bpchar") {
                            "character"
                        } else {
                            "character varying"
                        },
                        typmod - 4
                    ),
                ));
            }
            return Ok(ScalarValue::Utf8(Some(text)));
        }
        _ => {}
    }

    if let Some(kind) = column.json_kind {
        let text = if kind == "jsonb" {
            let (version, rest) = raw.split_first().ok_or_else(|| {
                user_error(
                    "22P02",
                    format!("empty jsonb binary value in column \"{}\"", column.name),
                )
            })?;
            if *version != 1 {
                return Err(user_error(
                    "22P02",
                    format!(
                        "unsupported jsonb binary version {version} in column \"{}\"",
                        column.name
                    ),
                ));
            }
            std::str::from_utf8(rest)
                .map_err(|_| invalid_input("jsonb", &column.name, rest))?
        } else {
            std::str::from_utf8(raw)
                .map_err(|_| invalid_input("json", &column.name, raw))?
        };
        return Ok(ScalarValue::Utf8(Some(apply_json_kind(
            column,
            text.to_string(),
        )?)));
    }

    match &column.data_type {
        DataType::Boolean => {
            let bytes: [u8; 1] = fixed(column, "boolean", raw)?;
            Ok(ScalarValue::Boolean(Some(bytes[0] != 0)))
        }
        DataType::Int16 => {
            let bytes: [u8; 2] = fixed(column, "smallint", raw)?;
            Ok(ScalarValue::Int16(Some(i16::from_be_bytes(bytes))))
        }
        DataType::Int32 => {
            let bytes: [u8; 4] = fixed(column, "integer", raw)?;
            Ok(ScalarValue::Int32(Some(i32::from_be_bytes(bytes))))
        }
        DataType::Int64 => {
            let bytes: [u8; 8] = fixed(column, "bigint", raw)?;
            Ok(ScalarValue::Int64(Some(i64::from_be_bytes(bytes))))
        }
        DataType::UInt8 => {
            let bytes: [u8; 1] = fixed(column, "oid", raw)?;
            Ok(ScalarValue::UInt8(Some(bytes[0])))
        }
        DataType::UInt16 => {
            let bytes: [u8; 2] = fixed(column, "oid", raw)?;
            Ok(ScalarValue::UInt16(Some(u16::from_be_bytes(bytes))))
        }
        DataType::UInt32 => {
            let bytes: [u8; 4] = fixed(column, "oid", raw)?;
            Ok(ScalarValue::UInt32(Some(u32::from_be_bytes(bytes))))
        }
        DataType::UInt64 => {
            let bytes: [u8; 8] = fixed(column, "oid", raw)?;
            Ok(ScalarValue::UInt64(Some(u64::from_be_bytes(bytes))))
        }
        DataType::Float32 => {
            let bytes: [u8; 4] = fixed(column, "real", raw)?;
            Ok(ScalarValue::Float32(Some(f32::from_be_bytes(bytes))))
        }
        DataType::Float64 => {
            let bytes: [u8; 8] = fixed(column, "double precision", raw)?;
            Ok(ScalarValue::Float64(Some(f64::from_be_bytes(bytes))))
        }
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(binary_text(column, raw)?))),
        DataType::LargeUtf8 => {
            Ok(ScalarValue::LargeUtf8(Some(binary_text(column, raw)?)))
        }
        DataType::Utf8View => Ok(ScalarValue::Utf8View(Some(binary_text(column, raw)?))),
        DataType::Binary => Ok(ScalarValue::Binary(Some(raw.to_vec()))),
        DataType::LargeBinary => Ok(ScalarValue::LargeBinary(Some(raw.to_vec()))),
        DataType::BinaryView => Ok(ScalarValue::BinaryView(Some(raw.to_vec()))),
        DataType::Date32 => {
            let bytes: [u8; 4] = fixed(column, "date", raw)?;
            let days = i32::from_be_bytes(bytes) as i64 + PG_EPOCH_DAYS;
            Ok(ScalarValue::Date32(Some(
                i32::try_from(days).map_err(|_| out_of_range(column, "date"))?,
            )))
        }
        DataType::Date64 => {
            let bytes: [u8; 4] = fixed(column, "date", raw)?;
            let days = i64::from(i32::from_be_bytes(bytes)) + PG_EPOCH_DAYS;
            Ok(ScalarValue::Date64(Some(
                days.checked_mul(86_400_000)
                    .ok_or_else(|| out_of_range(column, "date"))?,
            )))
        }
        DataType::Timestamp(unit, tz) => {
            let bytes: [u8; 8] = fixed(column, "timestamp", raw)?;
            let micros = i64::from_be_bytes(bytes)
                .checked_add(PG_EPOCH_MICROS)
                .ok_or_else(|| out_of_range(column, "timestamp"))?;
            let out_of_range = || out_of_range(column, "timestamp");
            Ok(match unit {
                TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(micros / 1_000_000), tz.clone())
                }
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(micros / 1_000), tz.clone())
                }
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(micros), tz.clone())
                }
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(
                    Some(micros.checked_mul(1_000).ok_or_else(out_of_range)?),
                    tz.clone(),
                ),
            })
        }
        DataType::Time32(unit) => {
            let micros = time_micros(column, raw)?;
            Ok(match unit {
                TimeUnit::Second => {
                    ScalarValue::Time32Second(Some((micros / 1_000_000) as i32))
                }
                TimeUnit::Millisecond => {
                    ScalarValue::Time32Millisecond(Some((micros / 1_000) as i32))
                }
                other => {
                    return Err(feature_not_supported(format!(
                        "binary COPY into column \"{}\" of TIME unit {other:?} is not supported",
                        column.name
                    )));
                }
            })
        }
        DataType::Time64(unit) => {
            let micros = time_micros(column, raw)?;
            Ok(match unit {
                TimeUnit::Microsecond => ScalarValue::Time64Microsecond(Some(micros)),
                TimeUnit::Nanosecond => ScalarValue::Time64Nanosecond(Some(
                    micros
                        .checked_mul(1_000)
                        .ok_or_else(|| out_of_range(column, "time"))?,
                )),
                other => {
                    return Err(feature_not_supported(format!(
                        "binary COPY into column \"{}\" of TIME unit {other:?} is not supported",
                        column.name
                    )));
                }
            })
        }
        DataType::Decimal128(precision, scale) => {
            let text = numeric_binary_to_text(column, raw)?;
            Ok(ScalarValue::Decimal128(
                Some(parse_decimal(
                    text.as_bytes(),
                    *precision,
                    *scale,
                    &column.name,
                )?),
                *precision,
                *scale,
            ))
        }
        DataType::Decimal256(precision, scale) => {
            let text = numeric_binary_to_text(column, raw)?;
            Ok(ScalarValue::Decimal256(
                Some(parse_decimal_i256(
                    text.as_bytes(),
                    &column.name,
                    *precision,
                    *scale,
                )?),
                *precision,
                *scale,
            ))
        }
        other => Err(feature_not_supported(format!(
            "binary COPY into column \"{}\" of type {other} is not supported",
            column.name
        ))),
    }
}

fn fixed<const N: usize>(
    column: &CopyColumn,
    ty: &str,
    raw: &[u8],
) -> PgWireResult<[u8; N]> {
    let bytes: [u8; N] = raw
        .try_into()
        .map_err(|_| invalid_input(ty, &column.name, raw))?;
    Ok(bytes)
}

fn binary_text(column: &CopyColumn, raw: &[u8]) -> PgWireResult<String> {
    String::from_utf8(raw.to_vec()).map_err(|_| invalid_input("text", &column.name, raw))
}

fn time_micros(column: &CopyColumn, raw: &[u8]) -> PgWireResult<i64> {
    let bytes: [u8; 8] = fixed(column, "time", raw)?;
    i64::from_be_bytes(bytes)
        .checked_mul(1_000)
        .ok_or_else(|| out_of_range(column, "time"))
}

fn out_of_range(
    column: &CopyColumn,
    ty: &str,
) -> datafusion_postgres::pgwire::error::PgWireError {
    user_error(
        NUMERIC_RANGE,
        format!("{ty} for column \"{}\" is out of range", column.name),
    )
}

/// PostgreSQL's binary `numeric`: a header (`ndigits`, `weight`, `sign`,
/// `dscale`) followed by `ndigits` base-10000 groups, each a big-endian u16.
/// It is converted to decimal text so the column's precision/scale rules stay
/// in the single existing parser.
fn numeric_binary_to_text(column: &CopyColumn, raw: &[u8]) -> PgWireResult<String> {
    if raw.len() < 8 {
        return Err(invalid_input("numeric", &column.name, raw));
    }
    let ndigits = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    let weight = i16::from_be_bytes([raw[2], raw[3]]) as i32;
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = u16::from_be_bytes([raw[6], raw[7]]) as usize;
    if raw.len() < 8 + ndigits * 2 {
        return Err(invalid_input("numeric", &column.name, raw));
    }
    if !matches!(sign, 0 | 0x4000) {
        return Err(invalid_input("numeric", &column.name, raw));
    }
    let mut digits = String::new();
    for index in 0..ndigits {
        let offset = 8 + index * 2;
        let group = u16::from_be_bytes([raw[offset], raw[offset + 1]]);
        digits.push_str(&format!("{group:04}"));
    }

    let integer_len = (weight + 1) * 4;
    let (integer, fraction) = if integer_len <= 0 {
        (
            "0".to_string(),
            format!("{}{}", "0".repeat((-integer_len) as usize), digits),
        )
    } else if integer_len >= digits.len() as i32 {
        (
            format!(
                "{}{}",
                digits,
                "0".repeat((integer_len as usize) - digits.len())
            ),
            String::new(),
        )
    } else {
        let split = integer_len as usize;
        (digits[..split].to_string(), digits[split..].to_string())
    };
    let mut integer = integer.trim_start_matches('0').to_string();
    if integer.is_empty() {
        integer.push('0');
    }
    let mut fraction = fraction;
    if fraction.len() > dscale {
        fraction.truncate(dscale);
    }
    while fraction.len() < dscale {
        fraction.push('0');
    }
    let mut text = integer;
    if dscale > 0 {
        text.push('.');
        text.push_str(&fraction);
    }
    if sign == 0x4000 && text != "0" {
        text.insert(0, '-');
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion::arrow::datatypes::Field;

    fn column(data_type: DataType) -> CopyColumn {
        CopyColumn {
            name: "c".to_string(),
            data_type,
            json_kind: None,
            pg_type: None,
            typmod: None,
            element: None,
        }
    }

    fn be_i32(value: i32) -> [u8; 4] {
        value.to_be_bytes()
    }

    #[test]
    fn integers_and_floats_decode_from_big_endian() {
        assert_eq!(
            decode_binary_value(&column(DataType::Int16), &7i16.to_be_bytes()).unwrap(),
            ScalarValue::Int16(Some(7))
        );
        assert_eq!(
            decode_binary_value(&column(DataType::Int32), &be_i32(-42)).unwrap(),
            ScalarValue::Int32(Some(-42))
        );
        assert_eq!(
            decode_binary_value(&column(DataType::Int64), &99i64.to_be_bytes()).unwrap(),
            ScalarValue::Int64(Some(99))
        );
        assert_eq!(
            decode_binary_value(&column(DataType::Float64), &1.5f64.to_be_bytes())
                .unwrap(),
            ScalarValue::Float64(Some(1.5))
        );
    }

    #[test]
    fn a_wrong_binary_width_is_invalid_input() {
        assert!(decode_binary_value(&column(DataType::Int32), &[0, 1]).is_err());
    }

    #[test]
    fn dates_and_timestamps_use_the_postgres_epoch() {
        assert_eq!(
            decode_binary_value(&column(DataType::Date32), &be_i32(0)).unwrap(),
            ScalarValue::Date32(Some(10_957))
        );
        let ts = column(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::from("UTC")),
        ));
        assert_eq!(
            decode_binary_value(&ts, &0i64.to_be_bytes()).unwrap(),
            ScalarValue::TimestampMicrosecond(
                Some(PG_EPOCH_MICROS),
                Some(Arc::from("UTC"))
            )
        );
    }

    #[test]
    fn numeric_groups_decode_at_the_column_scale() {
        // 12.34 with weight 0, two groups (0012, 3400), dscale 2.
        let mut raw = Vec::new();
        raw.extend_from_slice(&2u16.to_be_bytes());
        raw.extend_from_slice(&0i16.to_be_bytes());
        raw.extend_from_slice(&0u16.to_be_bytes());
        raw.extend_from_slice(&2u16.to_be_bytes());
        raw.extend_from_slice(&12u16.to_be_bytes());
        raw.extend_from_slice(&3400u16.to_be_bytes());
        let value =
            decode_binary_value(&column(DataType::Decimal128(10, 2)), &raw).unwrap();
        assert_eq!(value, ScalarValue::Decimal128(Some(1234), 10, 2));
    }

    #[test]
    fn uuid_and_money_use_their_semantic_metadata() {
        let mut uuid = column(DataType::Utf8);
        uuid.pg_type = Some("uuid".to_string());
        let id = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            decode_binary_value(&uuid, id.as_bytes()).unwrap(),
            ScalarValue::Utf8(Some(id.to_string()))
        );

        let mut money = column(DataType::Decimal128(19, 2));
        money.pg_type = Some("money".to_string());
        assert_eq!(
            decode_binary_value(&money, &123456i64.to_be_bytes()).unwrap(),
            ScalarValue::Decimal128(Some(123456), 19, 2)
        );
    }

    #[test]
    fn jsonb_requires_a_version_byte() {
        let mut doc = column(DataType::Utf8);
        doc.json_kind = Some("jsonb");
        let raw = [1, b'{', b'"', b'a', b'"', b':', b'1', b'}'];
        assert_eq!(
            decode_binary_value(&doc, &raw).unwrap(),
            ScalarValue::Utf8(Some(r#"{"a":1}"#.to_string()))
        );
        assert!(decode_binary_value(&doc, &raw[1..]).is_err());
    }

    #[test]
    fn arrays_are_rejected() {
        let list = column(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        assert!(decode_binary_value(&list, &[]).is_err());
    }
}
