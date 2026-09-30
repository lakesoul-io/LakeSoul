// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Primary-key index keys.
//!
//! Both the secondary indexes (vector, text) and the row-level primary-key
//! locator address rows by the `arrow-row` encoding of the primary-key
//! columns: the encoding is order-preserving, supports composite keys by
//! concatenating one [`SortField`] per column, and — crucially — the bytes
//! produced by one [`KeyCodec`] can be looked up directly in the locator's
//! per-file key indexes, without decoding.
//!
//! [`KeyLayout`] is the single source of truth for that layout.  Its
//! fingerprint ([`KeyLayout::kind_hash`]) is stored in index headers and in
//! the locator's mmapped index headers, so a schema change (a new primary
//! key type, a different column order) is detected instead of silently
//! returning no rows.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use lakesoul_vector::IndexKey;
use rootcause::report;

/// Format version of the key layout, hashed into the fingerprint.  Bump it
/// when the encoding changes; stale indexes then fail the fingerprint check
/// and are rebuilt instead of misread.
pub(crate) const KEY_LAYOUT_VERSION: u32 = 1;

/// The primary-key columns that form one index key, in key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyLayout {
    columns: Vec<String>,
    types: Vec<DataType>,
}

impl KeyLayout {
    /// A layout over explicit column names and types.
    pub fn new(columns: Vec<String>, types: Vec<DataType>) -> Self {
        Self { columns, types }
    }

    /// Resolve the key columns (and their types) from a data schema.
    pub fn from_schema(schema: &Schema, columns: &[String]) -> crate::Result<Self> {
        let mut types = Vec::with_capacity(columns.len());
        for column in columns {
            let field = schema.field_with_name(column).map_err(|e| {
                report!("index key column '{}' not found in schema: {}", column, e)
            })?;
            types.push(field.data_type().clone());
        }
        Ok(Self {
            columns: columns.to_vec(),
            types,
        })
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn types(&self) -> &[DataType] {
        &self.types
    }

    /// Fingerprint string of this layout (the locator's mmapped index `kind`).
    pub fn kind(&self) -> String {
        key_layout_kind(&self.columns, &self.types)
    }

    /// Hash of [`Self::kind`], as stored in index headers.
    pub fn kind_hash(&self) -> u64 {
        key_layout_kind_hash(&self.columns, &self.types)
    }
}

/// Identifier of one key layout: the pinned columns, their types and the
/// encoding version.
///
/// String and binary types are canonicalised first: `Utf8`/`LargeUtf8`/
/// `Utf8View` (and `Binary`/`LargeBinary`/`BinaryView`) carry the same values
/// through the same arrow-Row encoding, but a data file's inferred schema and
/// the table's declared schema may use different variants.  The fingerprint
/// must not depend on which variant each side happens to see.
pub(crate) fn key_layout_kind(columns: &[String], types: &[DataType]) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    KEY_LAYOUT_VERSION.hash(&mut hasher);
    for (column, data_type) in columns.iter().zip(types) {
        column.hash(&mut hasher);
        format!("{:?}", canonical_key_type(data_type)).hash(&mut hasher);
    }
    format!("pk{:016x}", hasher.finish())
}

/// The canonical fingerprint form of a key type (see [`key_layout_kind`]).
fn canonical_key_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            DataType::Binary
        }
        other => other.clone(),
    }
}

/// Hash of the layout fingerprint.
pub(crate) fn key_layout_kind_hash(columns: &[String], types: &[DataType]) -> u64 {
    use std::hash::{Hash, Hasher};

    let kind = key_layout_kind(columns, types);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    kind.hash(&mut hasher);
    hasher.finish()
}

/// Encodes primary-key columns into [`IndexKey`]s and back.
pub struct KeyCodec {
    layout: KeyLayout,
    converter: RowConverter,
}

impl KeyCodec {
    pub fn new(layout: KeyLayout) -> crate::Result<Self> {
        let fields = layout
            .types
            .iter()
            .cloned()
            .map(SortField::new)
            .collect::<Vec<_>>();
        let converter = RowConverter::new(fields).map_err(|e| {
            report!(
                "primary-key columns {:?} cannot be encoded as index keys: {}",
                layout.columns,
                e
            )
        })?;
        Ok(Self { layout, converter })
    }

    pub fn layout(&self) -> &KeyLayout {
        &self.layout
    }

    /// The layout fingerprint stored in index headers.
    pub fn kind_hash(&self) -> u64 {
        self.layout.kind_hash()
    }

    /// Encode the key columns of a batch, one key per row.
    pub fn encode_batch(&self, batch: &RecordBatch) -> crate::Result<Vec<IndexKey>> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.layout.columns.len());
        for column in &self.layout.columns {
            let array = batch.column_by_name(column).ok_or_else(|| {
                report!("index key column '{}' not found in batch", column)
            })?;
            if array.null_count() > 0 {
                return Err(report!(
                    "index key column '{}' contains nulls; primary keys must not be null",
                    column
                ));
            }
            arrays.push(Arc::clone(array));
        }
        let rows = self.converter.convert_columns(&arrays).map_err(|e| {
            report!("failed to encode primary-key columns as index keys: {}", e)
        })?;
        Ok((0..rows.num_rows())
            .map(|row| IndexKey::from(rows.row(row).data().to_vec()))
            .collect())
    }

    /// Decode keys back into one Arrow array per key column.
    pub fn decode(&self, keys: &[IndexKey]) -> crate::Result<Vec<ArrayRef>> {
        let parser = self.converter.parser();
        self.converter
            .convert_rows(keys.iter().map(|key| parser.parse(key.as_bytes())))
            .map_err(|e| report!("failed to decode index keys: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::Field;

    fn codec() -> KeyCodec {
        KeyCodec::new(KeyLayout::new(
            vec!["id".to_string(), "tenant".to_string()],
            vec![DataType::Int32, DataType::Utf8],
        ))
        .unwrap()
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1, -2, 3])) as ArrayRef),
            (
                "tenant",
                Arc::new(StringArray::from(vec!["a", "bc", "d"])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn composite_keys_round_trip() {
        let codec = codec();
        let keys = codec.encode_batch(&batch()).unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|key| !key.is_empty()));

        let arrays = codec.decode(&keys).unwrap();
        let ids = arrays[0].as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[1, -2, 3]);
        let tenants = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(tenants.value(0), "a");
        assert_eq!(tenants.value(1), "bc");
        assert_eq!(tenants.value(2), "d");
    }

    #[test]
    fn layout_fingerprint_tracks_columns_and_types() {
        let a = KeyLayout::new(vec!["id".to_string()], vec![DataType::Int32]);
        let b = KeyLayout::new(vec!["id".to_string()], vec![DataType::Int64]);
        let c = KeyLayout::new(vec!["other".to_string()], vec![DataType::Int32]);
        assert_ne!(a.kind_hash(), b.kind_hash());
        assert_ne!(a.kind_hash(), c.kind_hash());
        assert_eq!(a.kind_hash(), a.kind_hash());
        assert_eq!(a.kind(), a.kind());
    }

    #[test]
    fn null_keys_are_rejected() {
        let codec = codec();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
            ),
            (
                "tenant",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let err = codec.encode_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("must not be null"), "{err}");
    }

    #[test]
    fn schema_resolution_uses_declared_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tenant", DataType::Utf8, false),
        ]);
        let layout =
            KeyLayout::from_schema(&schema, &["tenant".to_string(), "id".to_string()])
                .unwrap();
        assert_eq!(layout.types(), &[DataType::Utf8, DataType::Int64]);
        let err = KeyLayout::from_schema(&schema, &["missing".to_string()]).unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
    }
}
