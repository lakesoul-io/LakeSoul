// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tantivy schema of a text index split.
//!
//! A split has two fields:
//! * the primary key, stored as an indexed `u64` fast field so each hit can
//!   be mapped back to its row without a document store;
//! * the text column, tokenized and indexed, not stored by default (the
//!   original rows live in the parquet data files).

use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, Schema, TextFieldIndexing, TextOptions,
};

use crate::config::TextIndexConfig;

/// Field name of the primary key inside a split.
pub const PK_FIELD: &str = "__lakesoul_pk";

/// Internal Tantivy field name of the indexed text.
///
/// The Arrow column name (which may contain characters Tantivy rejects in
/// field names) is only used to read the column; the index itself always
/// uses this fixed name.
pub const TEXT_FIELD: &str = "text";

/// The resolved fields of a text index schema.
#[derive(Debug, Clone)]
pub struct TextSchema {
    pub schema: Schema,
    pub pk_field: Field,
    pub text_field: Field,
}

impl TextSchema {
    /// Build the schema of a text index split.
    pub fn build(config: &TextIndexConfig) -> Self {
        let mut builder = Schema::builder();
        let pk_field = builder.add_u64_field(PK_FIELD, INDEXED | FAST);
        let mut text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(&config.tokenizer)
                .set_index_option(if config.with_positions {
                    IndexRecordOption::WithFreqsAndPositions
                } else {
                    IndexRecordOption::WithFreqs
                }),
        );
        if config.stored {
            text_options = text_options.set_stored();
        }
        let text_field = builder.add_text_field(TEXT_FIELD, text_options);
        Self {
            schema: builder.build(),
            pk_field,
            text_field,
        }
    }

    /// Resolve the fields of an already built schema.
    pub fn resolve(schema: &Schema) -> tantivy::Result<Self> {
        Ok(Self {
            schema: schema.clone(),
            pk_field: schema.get_field(PK_FIELD)?,
            text_field: schema.get_field(TEXT_FIELD)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_pk_and_text_fields() {
        let config = TextIndexConfig {
            column_name: "body".to_string(),
            tokenizer: "jieba".to_string(),
            with_positions: true,
            stored: false,
        };
        let text_schema = TextSchema::build(&config);
        assert!(text_schema.schema.get_field(PK_FIELD).is_ok());
        assert!(text_schema.schema.get_field(TEXT_FIELD).is_ok());
        assert!(matches!(
            text_schema
                .schema
                .get_field_entry(text_schema.pk_field)
                .field_type(),
            tantivy::schema::FieldType::U64(_)
        ));
    }
}
