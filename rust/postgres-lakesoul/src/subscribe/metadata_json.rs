// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Lossless, ordered JSON mirrors of LakeSoul metadata's PG text arrays.
//! COPY text unescaping happens before this parser; array and composite
//! escaping are separate layers. SQL NULL is handled by `decode::parse_value`.

use rootcause::{bail, report};
use serde_json::{Value, json};

use super::source::PgType;
use crate::Result;

pub(super) fn array_to_json(text: &str, kind: &PgType) -> Result<String> {
    let elements = fields(text, b'{', b'}', true)?;
    let values = elements
        .into_iter()
        .map(|element| {
            let Some(element) = element else {
                return Ok(Value::Null);
            };
            match kind {
                PgType::UuidArray => {
                    let id = uuid::Uuid::parse_str(&element).map_err(|error| {
                        report!("invalid UUID array element: {error}")
                    })?;
                    Ok(Value::String(id.to_string()))
                }
                PgType::DataFileOpArray => {
                    let parts = fields(&element, b'(', b')', false)?;
                    let [path, file_op, size, file_exist_cols]: [Option<String>; 4] =
                        parts.try_into().map_err(|_| {
                            report!("data_file_op must contain exactly four fields")
                        })?;
                    let size = size
                        .map(|value| value.parse::<i64>())
                        .transpose()
                        .map_err(|error| report!("invalid data_file_op size: {error}"))?;
                    Ok(json!({"path": path, "file_op": file_op, "size": size,
                    "file_exist_cols": file_exist_cols}))
                }
                _ => bail!("not a supported metadata array"),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    serde_json::to_string(&values)
        .map_err(|error| report!("encode metadata JSON: {error}"))
}

/// Read one PG array or composite level, retaining NULL versus quoted empty
/// strings. Nested arrays and explicit dimensions are rejected, never flattened.
fn fields(text: &str, open: u8, close: u8, array: bool) -> Result<Vec<Option<String>>> {
    let bytes = text.as_bytes();
    if bytes.first() != Some(&open) || bytes.last() != Some(&close) {
        bail!("invalid one-dimensional PostgreSQL array/composite framing");
    }
    let end = bytes.len() - 1;
    if end == 1 && array {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    let mut pos = 1;
    loop {
        let quoted = bytes.get(pos) == Some(&b'"');
        let mut value = Vec::new();
        if quoted {
            pos += 1;
            let mut terminated = false;
            while pos < end {
                match bytes[pos] {
                    b'\\' => {
                        pos += 1;
                        if pos >= end {
                            bail!("unterminated PostgreSQL escape")
                        }
                        value.push(bytes[pos]);
                        pos += 1;
                    }
                    b'"' => {
                        pos += 1;
                        if !array && pos < end && bytes[pos] == b'"' {
                            value.push(b'"');
                            pos += 1;
                        } else {
                            terminated = true;
                            break;
                        }
                    }
                    byte => {
                        value.push(byte);
                        pos += 1;
                    }
                }
            }
            if !terminated {
                bail!("unterminated PostgreSQL quoted value")
            }
        } else {
            while pos < end && bytes[pos] != b',' {
                let byte = bytes[pos];
                if byte == b'\\' {
                    pos += 1;
                    if pos >= end {
                        bail!("unterminated PostgreSQL escape")
                    }
                    value.push(bytes[pos]);
                } else {
                    if byte == b'"' || (array && matches!(byte, b'{' | b'}')) {
                        bail!("unexpected quote or nested metadata array");
                    }
                    value.push(byte);
                }
                pos += 1;
            }
        }
        let value = String::from_utf8(value)
            .map_err(|error| report!("invalid UTF-8: {error}"))?;
        let is_null = !quoted
            && if array {
                value.eq_ignore_ascii_case("NULL")
            } else {
                value.is_empty()
            };
        if array && !quoted && value.is_empty() {
            bail!("empty unquoted array element")
        }
        result.push((!is_null).then_some(value));
        if pos == end {
            break;
        }
        if bytes.get(pos) != Some(&b',') {
            bail!("expected PostgreSQL field separator")
        }
        pos += 1;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_order_empty_and_null_are_preserved() {
        let text = "{00000000-0000-0000-0000-000000000002,NULL,00000000-0000-0000-0000-000000000001}";
        let json: Value =
            serde_json::from_str(&array_to_json(text, &PgType::UuidArray).unwrap())
                .unwrap();
        assert_eq!(
            json,
            json!([
                "00000000-0000-0000-0000-000000000002",
                null,
                "00000000-0000-0000-0000-000000000001"
            ])
        );
        assert_eq!(array_to_json("{}", &PgType::UuidArray).unwrap(), "[]");
        assert!(array_to_json("{bad}", &PgType::UuidArray).is_err());
        assert!(array_to_json("{{NULL}}", &PgType::UuidArray).is_err());
    }

    // Build PostgreSQL array quoting separately from record quoting, so both
    // escaping layers (including commas, quotes, Unicode and backslashes) run.
    fn quoted(value: &str) -> String {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    }

    #[test]
    fn composite_array_preserves_values_order_and_nulls() {
        let path = "s3://bucket/分区=a,b/quote\"back\\slash.parquet";
        let record = format!("({},add,123,{})", quoted(path), quoted("id,name"));
        let input = format!("{{{},\"(,del,,\\\"\\\")\",NULL}}", quoted(&record));
        let result: Value = serde_json::from_str(
            &array_to_json(&input, &PgType::DataFileOpArray).unwrap(),
        )
        .unwrap();
        assert_eq!(
            result,
            json!([
                {"path":path,"file_op":"add","size":123,"file_exist_cols":"id,name"},
                {"path":null,"file_op":"del","size":null,"file_exist_cols":""}, null
            ])
        );
    }

    #[test]
    fn malformed_input_is_rejected_instead_of_partially_decoded() {
        for input in [
            "{\"(a,add,1)\"}",
            "{\"(a,add,bad,c)\"}",
            "{\"unterminated}",
            "{NULL,}",
        ] {
            assert!(
                array_to_json(input, &PgType::DataFileOpArray).is_err(),
                "{input}"
            );
        }
    }
}
