// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The fixed document schema of gateway tables.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// Primary key column (internal; the ES client sees it only as `_id`).
pub const PK_COLUMN: &str = "id";
/// Internal CDC change column used for delete tombstones.
pub const CDC_COLUMN: &str = "__cdc_op";
/// Value marking a live row in the CDC column.
pub const CDC_INSERT: &str = "insert";
/// Value marking a delete tombstone in the CDC column.
pub const CDC_DELETE: &str = "delete";

/// Build the fixed schema of a gateway table.
///
/// `content_column` is indexed for full-text search; when a dimension is
/// configured the embedding column is a `FixedSizeList<Float32>` indexed
/// with the IP metric (vectors are L2-normalized on write, so IP is cosine).
pub fn document_schema(
    content_column: &str,
    embedding: Option<(&str, usize)>,
) -> SchemaRef {
    let mut fields = vec![
        Field::new(PK_COLUMN, DataType::UInt64, false),
        Field::new(content_column, DataType::Utf8, true),
        Field::new("source_id", DataType::Utf8, true),
        Field::new("source_type", DataType::Int32, true),
        Field::new("chunk_id", DataType::Utf8, true),
        Field::new("knowledge_id", DataType::Utf8, true),
        Field::new("knowledge_base_id", DataType::Utf8, true),
        Field::new("tag_id", DataType::Utf8, true),
    ];
    if let Some((name, dim)) = embedding {
        fields.push(Field::new(
            name,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ));
    }
    fields.push(Field::new("is_enabled", DataType::Boolean, true));
    fields.push(Field::new("is_recommended", DataType::Boolean, true));
    fields.push(Field::new(CDC_COLUMN, DataType::Utf8, true));
    Arc::new(Schema::new(fields))
}

pub fn pk_field() -> Field {
    Field::new(PK_COLUMN, DataType::UInt64, false)
}

pub fn cdc_field() -> Field {
    Field::new(CDC_COLUMN, DataType::Utf8, true)
}

/// The document fields WeKnora writes into an index.
pub const DOCUMENT_FIELDS: &[&str] = &[
    "content",
    "source_id",
    "source_type",
    "chunk_id",
    "knowledge_id",
    "knowledge_base_id",
    "tag_id",
    "is_enabled",
    "is_recommended",
];
