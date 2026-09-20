// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Blob externalization codec.
//!
//! Binary columns listed in the `blob_columns` option store every value as a
//! tagged byte string:
//!
//! * inline: `0x00 || raw bytes`
//! * external: `0x01 || crc32(u32 LE) || length(u32 LE) || offset(u64 LE) || pack_path`
//!
//! External values live in a side pack file next to the data file
//! (`<data_file>.<column>.blob`), so deleting the data file is enough to
//! clean up its blobs. Readers materialize the raw bytes transparently.

use std::collections::HashMap;

use rootcause::report;

use crate::Result;
use crate::config::OPTION_KEY_BLOB_COLUMNS;

/// Tag for inline (raw) values.
pub const BLOB_TAG_INLINE: u8 = 0x00;
/// Tag for external (pack reference) values.
pub const BLOB_TAG_EXTERNAL: u8 = 0x01;
/// Default inline threshold: values up to 16 KiB stay in the data file.
pub const DEFAULT_INLINE_THRESHOLD: usize = 16 * 1024;
/// Default pack target size (256 MiB); informational for now.
pub const DEFAULT_PACK_TARGET_BYTES: u64 = 256 * 1024 * 1024;

/// How a blob column decides between inline and external storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobMode {
    Auto,
    Inline,
    External,
}

impl BlobMode {
    fn parse(value: &str, column: &str) -> Result<Self> {
        match value {
            "auto" => Ok(Self::Auto),
            "inline" => Ok(Self::Inline),
            "external" => Ok(Self::External),
            other => Err(report!(
                "invalid blob mode {other:?} for column {column:?}; expected auto|inline|external"
            )),
        }
    }
}

/// Resolved policy for one blob column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobPolicy {
    pub mode: BlobMode,
    pub inline_threshold: usize,
    pub pack_target_bytes: u64,
}

impl Default for BlobPolicy {
    fn default() -> Self {
        Self {
            mode: BlobMode::Auto,
            inline_threshold: DEFAULT_INLINE_THRESHOLD,
            pack_target_bytes: DEFAULT_PACK_TARGET_BYTES,
        }
    }
}

impl BlobPolicy {
    /// Whether a value of `length` bytes goes into the pack file.
    pub fn externalizes(&self, length: usize) -> bool {
        match self.mode {
            BlobMode::Inline => false,
            BlobMode::External => true,
            BlobMode::Auto => length > self.inline_threshold,
        }
    }
}

/// Parse the `blob_columns` option into per-column policies.
///
/// The option value is a JSON object, for example:
/// `{"frame": {"mode": "auto", "inline_threshold": 16384}}`.
/// `LAKESOUL_BLOB_DISABLE` (any value except empty/`0`/`false`) disables blob
/// handling entirely.
pub fn parse_blob_policies(
    options: &HashMap<String, String>,
) -> Result<HashMap<String, BlobPolicy>> {
    if blob_disabled(std::env::var("LAKESOUL_BLOB_DISABLE").ok().as_deref()) {
        return Ok(HashMap::new());
    }
    let Some(raw) = options.get(OPTION_KEY_BLOB_COLUMNS) else {
        return Ok(HashMap::new());
    };
    let root: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        report!("invalid {OPTION_KEY_BLOB_COLUMNS} option: {error}").attach(raw.clone())
    })?;
    let serde_json::Value::Object(entries) = root else {
        return Err(report!("{OPTION_KEY_BLOB_COLUMNS} must be a JSON object"));
    };
    let mut policies = HashMap::with_capacity(entries.len());
    for (column, value) in entries {
        policies.insert(column.clone(), parse_policy(&column, &value)?);
    }
    Ok(policies)
}

fn blob_disabled(value: Option<&str>) -> bool {
    matches!(value, Some(other) if !other.is_empty() && other != "0" && other != "false")
}

fn parse_policy(column: &str, value: &serde_json::Value) -> Result<BlobPolicy> {
    let mut policy = BlobPolicy::default();
    let serde_json::Value::Object(fields) = value else {
        return Err(report!(
            "blob policy for column {column:?} must be a JSON object"
        ));
    };
    if let Some(mode) = fields.get("mode") {
        let mode = mode
            .as_str()
            .ok_or_else(|| report!("blob mode for column {column:?} must be a string"))?;
        policy.mode = BlobMode::parse(mode, column)?;
    }
    if let Some(threshold) = fields.get("inline_threshold") {
        policy.inline_threshold = threshold.as_u64().ok_or_else(|| {
            report!("blob inline_threshold for column {column:?} must be an integer")
        })? as usize;
    }
    if let Some(target) = fields.get("pack_target_bytes") {
        policy.pack_target_bytes = target.as_u64().ok_or_else(|| {
            report!("blob pack_target_bytes for column {column:?} must be an integer")
        })?;
    }
    Ok(policy)
}

/// In-memory accumulator for one pack file.
#[derive(Debug, Default)]
pub struct PackBuffer {
    data: Vec<u8>,
}

impl PackBuffer {
    /// Append a value; returns `(offset, length, crc32)`.
    pub fn append(&mut self, value: &[u8]) -> Result<(u64, u32, u32)> {
        let length = u32::try_from(value.len()).map_err(|_| {
            report!(
                "blob value is too large for one pack reference: {} bytes",
                value.len()
            )
        })?;
        let offset = self.data.len() as u64;
        self.data.extend_from_slice(value);
        Ok((offset, length, crc32(value)))
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

/// Result of encoding one value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedValue {
    Inline,
    External { offset: u64, length: u32, crc: u32 },
}

/// Encode one value, spilling to `pack` when the policy says external.
pub fn encode_value(
    value: &[u8],
    policy: &BlobPolicy,
    pack: &mut PackBuffer,
) -> Result<EncodedValue> {
    if !policy.externalizes(value.len()) {
        return Ok(EncodedValue::Inline);
    }
    let (offset, length, crc) = pack.append(value)?;
    Ok(EncodedValue::External {
        offset,
        length,
        crc,
    })
}

/// Tagged bytes for an inline value.
pub fn tagged_inline(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(BLOB_TAG_INLINE);
    out.extend_from_slice(value);
    out
}

/// Tagged bytes for an external reference.
pub fn tagged_external(pack_path: &str, offset: u64, length: u32, crc: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + 4 + 8 + pack_path.len());
    out.push(BLOB_TAG_EXTERNAL);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(pack_path.as_bytes());
    out
}

/// A parsed tagged blob value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaggedValue<'a> {
    Inline(&'a [u8]),
    External {
        crc: u32,
        length: u32,
        offset: u64,
        pack_path: &'a str,
    },
}

/// Parse tagged blob bytes.
pub fn parse_tagged(bytes: &[u8]) -> Result<TaggedValue<'_>> {
    let Some((&tag, rest)) = bytes.split_first() else {
        return Err(report!("empty blob value"));
    };
    match tag {
        BLOB_TAG_INLINE => Ok(TaggedValue::Inline(rest)),
        BLOB_TAG_EXTERNAL => {
            if rest.len() < 16 {
                return Err(report!(
                    "external blob reference is truncated: {} bytes",
                    rest.len()
                ));
            }
            let crc = u32::from_le_bytes(rest[0..4].try_into().unwrap());
            let length = u32::from_le_bytes(rest[4..8].try_into().unwrap());
            let offset = u64::from_le_bytes(rest[8..16].try_into().unwrap());
            let pack_path = std::str::from_utf8(&rest[16..])
                .map_err(|error| report!("blob pack path is not UTF-8: {error}"))?;
            Ok(TaggedValue::External {
                crc,
                length,
                offset,
                pack_path,
            })
        }
        other => Err(report!("unknown blob tag {other:#04x}")),
    }
}

/// Verify a materialized external value against its reference.
pub fn verify_external(value: &[u8], length: u32, crc: u32) -> Result<()> {
    if value.len() != length as usize {
        return Err(report!(
            "blob length mismatch: expected {length}, read {}",
            value.len()
        ));
    }
    let actual = crc32(value);
    if actual != crc {
        return Err(report!(
            "blob crc mismatch: expected {crc:#010x}, got {actual:#010x}"
        ));
    }
    Ok(())
}

/// IEEE CRC32 (kept local to avoid a dependency for now).
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(raw: &str) -> HashMap<String, String> {
        HashMap::from([(OPTION_KEY_BLOB_COLUMNS.to_string(), raw.to_string())])
    }

    #[test]
    fn parses_defaults_and_overrides() {
        let policies = parse_blob_policies(&options(
            r#"{"frame": {"mode": "external"}, "state": {"inline_threshold": 8}}"#,
        ))
        .unwrap();
        assert_eq!(policies.len(), 2);
        assert_eq!(policies["frame"].mode, BlobMode::External);
        assert_eq!(policies["state"].inline_threshold, 8);
        assert_eq!(
            policies["state"].pack_target_bytes,
            DEFAULT_PACK_TARGET_BYTES
        );
    }

    #[test]
    fn rejects_invalid_options() {
        assert!(parse_blob_policies(&options("[1, 2]")).is_err());
        assert!(
            parse_blob_policies(&options(r#"{"frame": {"mode": "sometimes"}}"#)).is_err()
        );
        assert!(parse_blob_policies(&options("not json")).is_err());
        assert!(parse_blob_policies(&options("{}")).unwrap().is_empty());
        assert!(parse_blob_policies(&HashMap::new()).unwrap().is_empty());
    }

    #[test]
    fn auto_mode_spills_large_values_only() {
        let policy = BlobPolicy {
            inline_threshold: 4,
            ..BlobPolicy::default()
        };
        let mut pack = PackBuffer::default();

        let small = encode_value(b"abc", &policy, &mut pack).unwrap();
        assert_eq!(small, EncodedValue::Inline);
        assert!(pack.is_empty());

        let large = encode_value(b"0123456789", &policy, &mut pack).unwrap();
        let EncodedValue::External {
            offset,
            length,
            crc,
        } = large
        else {
            panic!("expected external encoding");
        };
        assert_eq!(offset, 0);
        assert_eq!(length, 10);
        assert_eq!(pack.data(), b"0123456789");
        verify_external(pack.data(), length, crc).unwrap();
    }

    #[test]
    fn tagged_values_roundtrip() {
        let inline = tagged_inline(b"raw");
        assert_eq!(parse_tagged(&inline).unwrap(), TaggedValue::Inline(b"raw"));

        let external =
            tagged_external("file:///tmp/data.vortex.frame.blob", 7, 3, 0xDEAD_BEEF);
        match parse_tagged(&external).unwrap() {
            TaggedValue::External {
                crc,
                length,
                offset,
                pack_path,
            } => {
                assert_eq!(crc, 0xDEAD_BEEF);
                assert_eq!(length, 3);
                assert_eq!(offset, 7);
                assert_eq!(pack_path, "file:///tmp/data.vortex.frame.blob");
            }
            other => panic!("expected external, got {other:?}"),
        }

        assert!(parse_tagged(b"").is_err());
        assert!(parse_tagged(&[BLOB_TAG_EXTERNAL, 1, 2, 3]).is_err());
        assert!(parse_tagged(&[0x7F]).is_err());
    }

    #[test]
    fn verify_external_checks_length_and_crc() {
        let value = b"payload";
        let crc = crc32(value);
        verify_external(value, 7, crc).unwrap();
        assert!(verify_external(value, 6, crc).is_err());
        assert!(verify_external(value, 7, crc ^ 1).is_err());
    }
}
