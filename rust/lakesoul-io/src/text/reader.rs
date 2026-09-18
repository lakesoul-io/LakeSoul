// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Extracting `(primary key, text)` rows from reader batches.

use arrow_array::{
    Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    UInt64Array,
};
use arrow_schema::DataType;
use rootcause::{bail, report};

use crate::Result;

/// One row's `(primary key, text)` pair; `None` when the primary key is null.
pub type TextValueRow = Option<(u64, Option<String>)>;

/// Row-aligned text values: `None` where the primary key is null.
///
/// Used by exact verification, which must produce one boolean per input row.
pub fn collect_text_values(
    batch: &RecordBatch,
    pk_column: &str,
    text_column: &str,
) -> Result<Vec<TextValueRow>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let pk_array = batch
        .column_by_name(pk_column)
        .ok_or_else(|| report!("primary key column '{}' not found", pk_column))?;
    let text_array = batch
        .column_by_name(text_column)
        .ok_or_else(|| report!("text column '{}' not found", text_column))?;
    let texts = text_values(text_array)?;

    let rows: Vec<TextValueRow> = match pk_array.data_type() {
        DataType::UInt64 => {
            let ids = pk_array.as_any().downcast_ref::<UInt64Array>().unwrap();
            ids.iter()
                .zip(texts)
                .map(|(id, text)| id.map(|id| (id, text)))
                .collect()
        }
        DataType::Int64 => {
            let ids = pk_array.as_any().downcast_ref::<Int64Array>().unwrap();
            ids.iter()
                .zip(texts)
                .map(|(id, text)| id.map(|id| (id as u64, text)))
                .collect()
        }
        other => bail!(
            "text index primary key '{}' must be UInt64 or Int64, got {other}",
            pk_column
        ),
    };
    Ok(rows)
}

/// Documents to index: null/empty primary keys and texts are skipped.
pub fn collect_text_documents(
    batch: &RecordBatch,
    pk_column: &str,
    text_column: &str,
) -> Result<Vec<(u64, String)>> {
    Ok(collect_text_values(batch, pk_column, text_column)?
        .into_iter()
        .flatten()
        .filter_map(|(id, text)| match text {
            Some(text) if !text.is_empty() => Some((id, text)),
            _ => None,
        })
        .collect())
}

fn text_values(array: &arrow_array::ArrayRef) -> Result<Vec<Option<String>>> {
    match array.data_type() {
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok((0..values.len())
                .map(|i| (!values.is_null(i)).then(|| values.value(i).to_string()))
                .collect())
        }
        DataType::LargeUtf8 => {
            let values = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok((0..values.len())
                .map(|i| (!values.is_null(i)).then(|| values.value(i).to_string()))
                .collect())
        }
        DataType::Utf8View => {
            let values = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            Ok((0..values.len())
                .map(|i| (!values.is_null(i)).then(|| values.value(i).to_string()))
                .collect())
        }
        other => {
            bail!("text index column must be Utf8, LargeUtf8 or Utf8View, got {other}")
        }
    }
}
