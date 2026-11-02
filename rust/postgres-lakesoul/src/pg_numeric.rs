// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL `numeric_send` encoding for DECIMAL results.
//!
//! The upstream result encoder turns every Arrow `Decimal128` into a
//! `rust_decimal::Decimal` and lets it write the PostgreSQL binary `numeric`
//! payload. That works for nonzero values, but `rust_decimal` serializes zero
//! with display scale `0` no matter which scale the value carries:
//!
//! ```text
//! if self.is_zero() {
//!     return PostgresDecimal { neg: false, weight: 0, scale: 0, digits: vec![] };
//! }
//! ```
//!
//! PostgreSQL keeps the column's display scale (`dscale`) even when the digit
//! array is empty, so `CAST('0' AS DECIMAL(10,2))` is `00 00 00 00 00 00 00 02`
//! (no digits, weight 0, positive, `dscale` 2) and a client decodes scale `2`,
//! not `0`. The response stream rewrite below re-encodes those zero cells from
//! `(mantissa, scale)` in this crate; nonzero payloads are left byte-identical
//! to the upstream encoder, which already preserves their scale.
//!
//! Only `Decimal128` is supported. A `Decimal256` result (precision above 38)
//! is rejected explicitly instead of being silently truncated or falling back
//! to scale `0`, and so is a scale PostgreSQL cannot put in the 14-bit
//! `dscale` field.

use datafusion::arrow::datatypes::DataType;
use datafusion_postgres::pgwire::api::results::{FieldFormat, QueryResponse, Response};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::data::DataRow;
use futures::StreamExt;

/// PostgreSQL's `NUMERIC_DSCALE_MASK`: the display scale lives in the low
/// 14 bits of the fourth header field.
const NUMERIC_DSCALE_MASK: u32 = 0x3FFF;
/// `NUMERIC_POS` from `numeric.c`.
const NUMERIC_POS: u16 = 0x0000;
/// `NUMERIC_NEG` from `numeric.c`.
const NUMERIC_NEG: u16 = 0x4000;
/// Header size of `numeric_send`: `ndigits`, `weight`, `sign`, `dscale`.
const NUMERIC_HEADER_LEN: usize = 8;

/// Why a DECIMAL value cannot be represented on the PostgreSQL wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumericEncodeError {
    /// The display scale does not fit in PostgreSQL's 14-bit `dscale`.
    ScaleOutOfRange(u32),
    /// The mantissa and scale need more than the digit range this encoder
    /// (and PostgreSQL's `numeric`) can represent.
    OutOfRange,
}

impl NumericEncodeError {
    /// Maps the failure to the SQLSTATE PostgreSQL reports for it.
    fn into_pgwire(self) -> PgWireError {
        let (code, message) = match self {
            Self::ScaleOutOfRange(scale) => (
                "22003",
                format!(
                    "numeric display scale {scale} exceeds PostgreSQL's limit of {NUMERIC_DSCALE_MASK}"
                ),
            ),
            Self::OutOfRange => (
                "22003",
                "decimal value exceeds the digit range PostgreSQL numeric can represent"
                    .to_string(),
            ),
        };
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            code.to_string(),
            message,
        )))
    }
}

/// Encodes a `Decimal128` value as a PostgreSQL binary `numeric` payload,
/// following `numeric_send`: `ndigits`, `weight`, `sign`, `dscale`, then
/// base-10000 groups, most significant first.
///
/// This mirrors the canonical grouping `rust_decimal` uses for nonzero values,
/// so re-encoding such a value reproduces its bytes; the difference is that a
/// zero keeps the requested display scale instead of collapsing to zero.
pub(crate) fn encode_numeric(
    mantissa: i128,
    scale: u32,
) -> Result<Vec<u8>, NumericEncodeError> {
    if scale > NUMERIC_DSCALE_MASK {
        return Err(NumericEncodeError::ScaleOutOfRange(scale));
    }

    let negative = mantissa < 0;
    let magnitude = mantissa.unsigned_abs();

    // PostgreSQL stores zero with no groups but keeps `dscale`; the scale is
    // exactly what the upstream encoder drops.
    if magnitude == 0 {
        let mut payload = Vec::with_capacity(NUMERIC_HEADER_LEN);
        push_header(&mut payload, 0, 0, NUMERIC_POS, scale as u16);
        return Ok(payload);
    }

    // Group from the decimal point: the fractional part is padded to a whole
    // number of base-10000 groups, then the digits are split from the right.
    let fractional_groups = scale.div_ceil(4);
    let pad = fractional_groups * 4 - scale;
    let scaled = magnitude
        .checked_mul(10u128.pow(pad))
        .ok_or(NumericEncodeError::OutOfRange)?;

    let mut groups = Vec::new();
    let mut rest = scaled;
    while rest > 0 {
        groups.push((rest % 10000) as u16);
        rest /= 10000;
    }
    groups.reverse();

    let weight = groups.len() as i32 - fractional_groups as i32 - 1;
    let weight = i16::try_from(weight).map_err(|_| NumericEncodeError::OutOfRange)?;

    // A value with an integer part stores no trailing all-zero fractional
    // groups (rust_decimal canonicalizes the same way). A pure fraction keeps
    // every group: the leading zero groups carry the weight and the scale.
    let mut ndigits = groups.len();
    if weight >= 0 {
        let first_fractional = (weight as usize) + 1;
        while ndigits > first_fractional && groups[ndigits - 1] == 0 {
            ndigits -= 1;
        }
    }

    let ndigits_u16 =
        u16::try_from(ndigits).map_err(|_| NumericEncodeError::OutOfRange)?;
    let mut payload = Vec::with_capacity(NUMERIC_HEADER_LEN + ndigits * 2);
    push_header(
        &mut payload,
        ndigits_u16,
        weight,
        if negative { NUMERIC_NEG } else { NUMERIC_POS },
        scale as u16,
    );
    for group in &groups[..ndigits] {
        payload.extend_from_slice(&group.to_be_bytes());
    }
    Ok(payload)
}

fn push_header(payload: &mut Vec<u8>, ndigits: u16, weight: i16, sign: u16, dscale: u16) {
    payload.extend_from_slice(&ndigits.to_be_bytes());
    payload.extend_from_slice(&weight.to_be_bytes());
    payload.extend_from_slice(&sign.to_be_bytes());
    payload.extend_from_slice(&dscale.to_be_bytes());
}

/// The scale of each DECIMAL result column, in output order. `None` marks a
/// column that is not a `Decimal128`.
///
/// The rewrite only touches columns the response encodes in binary; the scale
/// is needed before that filter because a `Decimal256` column is an error
/// whatever format the client asked for: this server only implements
/// PostgreSQL `numeric` from `Decimal128`, and silently sending truncated
/// digits (or a scale-0 zero) would corrupt the value.
pub(crate) fn decimal_scale_columns(
    arrow_types: &[DataType],
) -> PgWireResult<Vec<Option<u32>>> {
    arrow_types
        .iter()
        .map(|data_type| match data_type {
            DataType::Decimal128(_, scale) => {
                if *scale < 0 {
                    return Err(unsupported_decimal_error(format!(
                        "negative DECIMAL scale {scale} is not supported"
                    )));
                }
                Ok(Some(*scale as u32))
            }
            DataType::Decimal256(_, _) => Err(unsupported_decimal_error(
                "DECIMAL256 results are not supported; use DECIMAL with precision at most 38"
                    .to_string(),
            )),
            _ => Ok(None),
        })
        .collect()
}

fn unsupported_decimal_error(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        message,
    )))
}

/// Rewrites the DECIMAL binary cells of a query response so zero values keep
/// their column scale.
///
/// `arrow_types` is the result schema of the statement's logical plan: it is
/// where the scale lives, because the encoded row only carries the value.
/// A response whose column count does not match the plan (a hook answering
/// with its own columns) is returned untouched, and so is every non-`Query`
/// response.
pub(crate) fn rewrite_decimal_scale(
    response: Response,
    arrow_types: &[DataType],
) -> PgWireResult<Response> {
    let Response::Query(query) = response else {
        return Ok(response);
    };
    if query.row_schema.len() != arrow_types.len() {
        return Ok(Response::Query(query));
    }

    let scales = decimal_scale_columns(arrow_types)?;
    let columns: Vec<Option<u32>> = query
        .row_schema
        .iter()
        .zip(scales)
        .map(|(field, scale)| match scale {
            Some(scale) if field.format() == FieldFormat::Binary => Some(scale),
            _ => None,
        })
        .collect();
    if columns.iter().all(Option::is_none) {
        return Ok(Response::Query(query));
    }

    let QueryResponse {
        command_tag,
        row_schema,
        data_rows,
        ..
    } = query;
    let mut rewritten = QueryResponse::new(
        row_schema,
        Box::pin(
            data_rows.map(move |row| row.and_then(|row| rewrite_row(row, &columns))),
        ),
    );
    rewritten.command_tag = command_tag;
    Ok(Response::Query(rewritten))
}

/// Rewrites the zero DECIMAL cells of one row, leaving every other byte alone.
fn rewrite_row(mut row: DataRow, columns: &[Option<u32>]) -> PgWireResult<DataRow> {
    if columns.iter().all(Option::is_none) || row.field_count as usize != columns.len() {
        return Ok(row);
    }

    let mut offset = 0usize;
    for (index, scale) in columns.iter().enumerate() {
        let length = cell_length(&row.data, offset)?;
        offset += 4;
        match (scale, length) {
            (_, None) => {}
            (Some(scale), Some(length)) => {
                let start = offset;
                let end = offset + length;
                if is_zero_numeric(&row.data[start..end]) {
                    let payload =
                        encode_numeric(0, *scale).map_err(|e| e.into_pgwire())?;
                    // A zero cell is only the header: substituting the whole
                    // payload keeps the row length (and its encoded size) the
                    // same, so no re-buffering is needed.
                    if payload.len() != length {
                        return Err(PgWireError::ApiError(Box::new(
                            std::io::Error::other(format!(
                                "column {index}: zero numeric payload is {length} bytes, expected {}",
                                payload.len()
                            )),
                        )));
                    }
                    row.data[start..end].copy_from_slice(&payload);
                }
            }
            (None, _) => {}
        }
        offset += length.unwrap_or(0);
    }
    Ok(row)
}

/// Length of the cell starting at `offset`, or `None` for SQL NULL.
fn cell_length(data: &[u8], offset: usize) -> PgWireResult<Option<usize>> {
    let header = data.get(offset..offset + 4).ok_or_else(malformed_row)?;
    let length = i32::from_be_bytes(header.try_into().expect("four bytes"));
    if length < 0 {
        return Ok(None);
    }
    let length = length as usize;
    if offset + 4 + length > data.len() {
        return Err(malformed_row());
    }
    Ok(Some(length))
}

/// True when `cell` is a zero `numeric` payload (no digit groups).
fn is_zero_numeric(cell: &[u8]) -> bool {
    if cell.len() != NUMERIC_HEADER_LEN {
        return false;
    }
    u16::from_be_bytes([cell[0], cell[1]]) == 0
}

fn malformed_row() -> PgWireError {
    PgWireError::ApiError(Box::new(std::io::Error::other(
        "malformed DataRow while rewriting DECIMAL scale",
    )))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bytes::BytesMut;
    use rust_decimal::Decimal;
    use tokio_postgres::types::{ToSql, Type};

    use super::encode_numeric;

    /// The bytes the upstream encoder emits for `value` (rust_decimal via
    /// postgres-types). Nonzero values must stay byte-identical to this.
    fn upstream_bytes(value: &str) -> Vec<u8> {
        let decimal = Decimal::from_str(value).expect("fixture parses");
        let mut buffer = BytesMut::new();
        decimal
            .to_sql(&Type::NUMERIC, &mut buffer)
            .expect("decimal encodes");
        buffer.to_vec()
    }

    #[test]
    fn nonzero_values_match_the_upstream_encoder() {
        for value in [
            "1234567.8901",
            "-1234567.8901",
            "-0.01",
            "9999999999999999.9999",
            "-9223372036854775807",
            "0.1",
            "0.01",
            "1",
            "-1",
            "10000",
            "0.0001",
            "12340.56789",
            "1.2",
            "0.00000000000000000001",
        ] {
            let decimal = Decimal::from_str(value).expect("fixture parses");
            let encoded = encode_numeric(decimal.mantissa(), decimal.scale())
                .expect("value is representable");
            assert_eq!(
                encoded,
                upstream_bytes(value),
                "value {value} must keep the upstream bytes"
            );
        }
    }

    #[test]
    fn zero_keeps_the_column_scale() {
        for scale in [0, 2, 3, 4, 20, 28] {
            let encoded = encode_numeric(0, scale).expect("scale is representable");
            assert_eq!(
                encoded,
                vec![0, 0, 0, 0, 0, 0, (scale >> 8) as u8, scale as u8],
                "zero with scale {scale}"
            );
        }
        // The upstream encoder loses the scale of a zero, which is the defect.
        assert_ne!(upstream_bytes("0.00"), encode_numeric(0, 2).unwrap());
    }

    #[test]
    fn scale_outside_the_wire_field_is_rejected() {
        assert!(encode_numeric(0, 0x4000).is_err());
        assert!(encode_numeric(1, u32::MAX).is_err());
    }

    #[test]
    fn negative_mantissa_uses_the_negative_sign() {
        let encoded = encode_numeric(-1, 0).expect("value is representable");
        assert_eq!(&encoded[4..6], &[0x40, 0x00]);
        assert_eq!(encoded[6..8], [0, 0]);
        assert_eq!(encoded[8..], [0, 1]);
        assert_eq!(encoded, upstream_bytes("-1"));
    }
}
