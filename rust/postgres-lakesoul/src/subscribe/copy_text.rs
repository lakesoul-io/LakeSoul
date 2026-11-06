// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Streaming decoder for PostgreSQL `COPY ... TO STDOUT` TEXT format.
//!
//! The managed snapshot used to consume rows through `pg_walstream`'s
//! single-reader snapshot helper. The parallel snapshot opens its own
//! `tokio-postgres` connections and `COPY` streams, so it needs the same
//! text-to-`ColumnValue` conversion without buffering the whole result.
//!
//! A COPY TEXT frame may contain a partial row, one row, or many rows. The
//! decoder keeps only the unfinished tail between frames; a completed row is
//! yielded as a slice of the frame buffer.

use bytes::{Bytes, BytesMut};
use pg_walstream::ColumnValue;
use rootcause::{bail, report};

use crate::Result;

/// The two-byte sequence PostgreSQL uses for SQL NULL in COPY TEXT format.
const NULL_MARKER: &[u8] = b"\\N";

/// The `\.` line PostgreSQL uses to terminate `COPY FROM STDIN`. It is never
/// emitted by `COPY TO STDOUT`, but must not be mistaken for data.
const END_OF_DATA_MARKER: &[u8] = b"\\.";

/// Accumulates COPY frames and yields complete, still-escaped rows.
#[derive(Debug, Default)]
pub struct CopyTextDecoder {
    buffer: BytesMut,
}

impl CopyTextDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Hand one `CopyData` payload to the decoder.
    pub fn push(&mut self, chunk: Bytes) {
        if !chunk.is_empty() {
            self.buffer.extend_from_slice(&chunk);
        }
    }

    /// Bytes buffered for the current, not-yet-terminated row (plus any
    /// complete rows not consumed yet). Callers use this to bound a single
    /// oversized TOAST value instead of growing without limit.
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// The next complete line, with its terminator and an optional `\r`
    /// stripped. Returns `None` when more frames are needed.
    pub fn next_line(&mut self) -> Option<Bytes> {
        loop {
            let idx = self.buffer.iter().position(|&byte| byte == b'\n')?;
            let mut line = self.buffer.split_to(idx + 1);
            line.truncate(idx);
            if line.last() == Some(&b'\r') {
                line.truncate(line.len() - 1);
            }
            let line = line.freeze();
            if line == END_OF_DATA_MARKER {
                tracing::debug!("ignoring end-of-data marker in COPY TEXT stream");
                continue;
            }
            return Some(line);
        }
    }

    /// Assert the stream ended on a row boundary.
    pub fn finish(&self) -> Result<()> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            bail!(
                "COPY stream ended mid-row: {} trailing bytes with no row terminator",
                self.buffer.len()
            )
        }
    }
}

/// Split one COPY TEXT line into exactly `column_count` values.
pub fn decode_line(line: &[u8], column_count: usize) -> Result<Vec<ColumnValue>> {
    if column_count == 0 {
        return if line.is_empty() {
            Ok(Vec::new())
        } else {
            Err(report!(
                "COPY row has {} columns, expected 0",
                line.split(|&byte| byte == b'\t').count()
            ))
        };
    }

    let mut values = Vec::with_capacity(column_count);
    for field in line.split(|&byte| byte == b'\t') {
        values.push(decode_field(field));
        if values.len() > column_count {
            bail!("COPY row has more than {column_count} columns");
        }
    }
    if values.len() != column_count {
        bail!(
            "COPY row has {} columns, expected {column_count}",
            values.len()
        );
    }
    Ok(values)
}

fn decode_field(raw: &[u8]) -> ColumnValue {
    if raw == NULL_MARKER {
        ColumnValue::Null
    } else {
        ColumnValue::text_bytes(Bytes::copy_from_slice(&unescape_field(raw)))
    }
}

/// Reverse PostgreSQL's COPY TEXT escaping.
///
/// Supports the escapes PostgreSQL produces: `\b \f \n \r \t \v`, `\\`,
/// 1-3 octal digits, and `\x` followed by 1-2 hex digits. A backslash before
/// any other character is that character, matching PostgreSQL's reader.
fn unescape_field(raw: &[u8]) -> Vec<u8> {
    if !raw.contains(&b'\\') {
        return raw.to_vec();
    }

    let mut out = Vec::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        let byte = raw[index];
        index += 1;
        if byte != b'\\' {
            out.push(byte);
            continue;
        }
        let Some(&escaped) = raw.get(index) else {
            // PostgreSQL drops a trailing backslash; keep it rather than
            // silently losing a byte.
            out.push(b'\\');
            break;
        };
        index += 1;
        match escaped {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'\\' => out.push(b'\\'),
            b'x' => {
                let mut value = 0u8;
                let mut digits = 0;
                while digits < 2 {
                    let Some(&next) = raw.get(index) else {
                        break;
                    };
                    let Some(nibble) = hex_value(next) else {
                        break;
                    };
                    value = value.wrapping_mul(16).wrapping_add(nibble);
                    index += 1;
                    digits += 1;
                }
                if digits == 0 {
                    out.push(b'x');
                } else {
                    out.push(value);
                }
            }
            b'0'..=b'7' => {
                let mut value = u16::from(escaped - b'0');
                let mut digits = 1;
                while digits < 3 {
                    let Some(&next) = raw.get(index) else {
                        break;
                    };
                    if !(b'0'..=b'7').contains(&next) {
                        break;
                    }
                    value = value * 8 + u16::from(next - b'0');
                    index += 1;
                    digits += 1;
                }
                out.push((value & 0xff) as u8);
            }
            other => out.push(other),
        }
    }
    out
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line_of(bytes: &[u8]) -> Bytes {
        Bytes::copy_from_slice(bytes)
    }

    #[test]
    fn decodes_escapes_and_nulls() {
        let values = decode_line(b"1\tAlice\t\\N\t\\\\xDEAD\tline\\nnext", 5).unwrap();
        assert_eq!(values[0].as_str().as_deref(), Some("1"));
        assert_eq!(values[1].as_str().as_deref(), Some("Alice"));
        assert!(values[2].is_null());
        assert_eq!(values[3].as_bytes(), b"\\xDEAD");
        assert_eq!(values[4].as_str().as_deref(), Some("line\nnext"));
    }

    #[test]
    fn decodes_hex_and_octal() {
        let values = decode_line(b"\\x41\\101", 1).unwrap();
        assert_eq!(values[0].as_bytes(), b"AA");
    }

    #[test]
    fn rejects_wrong_column_count() {
        let error = decode_line(b"1\t2", 3).unwrap_err().to_string();
        assert!(error.contains("2 columns"), "{error}");
    }

    #[test]
    fn spans_frames() {
        let mut decoder = CopyTextDecoder::new();
        decoder.push(line_of(b"1\tAli"));
        decoder.push(line_of(b"ce\n2\tBob\n"));
        assert_eq!(decoder.next_line().unwrap().as_ref(), b"1\tAlice");
        assert_eq!(decoder.next_line().unwrap().as_ref(), b"2\tBob");
        assert!(decoder.next_line().is_none());
        decoder.finish().unwrap();
    }

    #[test]
    fn rejects_truncated_stream() {
        let mut decoder = CopyTextDecoder::new();
        decoder.push(line_of(b"1\tAlice"));
        assert!(decoder.next_line().is_none());
        assert!(decoder.finish().is_err());
    }
}
