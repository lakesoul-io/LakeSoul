// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::Result;
use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::v2::batch_range::{
    BatchRange, InProgressPkGroup, InProgressRow,
};
use arrow::array::Array;
use arrow::compute::interleave;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use std::sync::Arc;

/// Precomputed once at merge setup from `fields_map`, reused by every batch.
///
/// `source_col[stream_idx][target_col] = Some(stream_col_idx)` maps each
/// (stream, output column) pair to that stream's source column index.
/// `None` means the stream does not have this column.
pub(crate) struct ColumnMapping {
    pub source_col: Vec<Vec<Option<usize>>>,
}

impl ColumnMapping {
    /// Build from `fields_map[stream_idx][stream_col] = target_col`.
    pub fn from_fields_map(fields_map: &[Vec<usize>], num_target_cols: usize) -> Self {
        let num_streams = fields_map.len();
        let mut source_col = vec![vec![None; num_target_cols]; num_streams];
        for (s, stream_map) in fields_map.iter().enumerate() {
            for (stream_col, &target_col) in stream_map.iter().enumerate() {
                source_col[s][target_col] = Some(stream_col);
            }
        }
        ColumnMapping { source_col }
    }

    /// True if EVERY stream has this target column.
    #[inline]
    fn is_non_partial(&self, target_col: usize) -> bool {
        self.source_col.iter().all(|s| s[target_col].is_some())
    }

    /// True if EVERY target column is non-partial (all streams have all columns).
    #[inline]
    fn all_non_partial(&self) -> bool {
        let n = self.source_col.first().map_or(0, |v| v.len());
        (0..n).all(|c| self.is_non_partial(c))
    }

    /// True if NO stream has this target column.
    #[inline]
    fn is_all_null(&self, target_col: usize) -> bool {
        self.source_col.iter().all(|s| s[target_col].is_none())
    }
}

/// Build a `RecordBatch` from PK groups with three fast-path layers:
///
/// 1. All columns non-partial → original V2 path: single indices array,
///    `interleave` per column at column index `i`, zero overhead.
/// 2. Per-column non-partial → fast path: pre-resolved source column indices,
///    no reverse scan, shared indices array.
/// 3. All-null column → `new_null_array` directly.
pub(crate) fn build_record_batch_from_pk_groups<C: CursorValues>(
    pk_groups: &[InProgressPkGroup],
    ranges: &[&BatchRange<C>],
    schema: &SchemaRef,
    column_mapping: &Arc<ColumnMapping>,
) -> Result<RecordBatch> {
    let num_target_cols = schema.fields().len();
    let num_output_rows = pk_groups.len();
    let num_ranges = ranges.len();
    let source_col = &column_mapping.source_col;

    // Layer 1: global fast path — every stream has every column
    if column_mapping.all_non_partial() {
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(num_output_rows);
        for group in pk_groups {
            let winner = unsafe { group.last().unwrap_unchecked() };
            indices.push((winner.range_idx, winner.row_idx));
        }

        let result = (0..num_target_cols)
            .map(|i| {
                let mut columns: Vec<&dyn Array> = Vec::with_capacity(num_ranges);
                unsafe {
                    for r_idx in 0..num_ranges {
                        let r = ranges.get_unchecked(r_idx);
                        columns.push(r.batch().column(i).as_ref());
                    }
                }
                interleave(&columns, &indices)
            })
            .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;

        return Ok(RecordBatch::try_new(schema.clone(), result)?);
    }

    // Shared indices for all non-partial columns
    let mut fast_indices: Vec<(usize, usize)> = Vec::with_capacity(num_output_rows);
    for group in pk_groups {
        let winner = unsafe { group.last().unwrap_unchecked() };
        fast_indices.push((winner.range_idx, winner.row_idx));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_target_cols);

    // Layer 2 & 3: dispatch per column using precomputed source_col
    unsafe {
        for target_col in 0..num_target_cols {
            if column_mapping.is_all_null(target_col) {
                columns.push(arrow::array::new_null_array(
                    schema.field(target_col).data_type(),
                    num_output_rows,
                ));
                continue;
            }

            if column_mapping.is_non_partial(target_col) {
                let mut source_arrs: Vec<&dyn Array> = Vec::with_capacity(num_ranges);
                for r_idx in 0..num_ranges {
                    let r = ranges.get_unchecked(r_idx);
                    let stream_col_map = source_col.get_unchecked(r.stream_idx());
                    let src_col =
                        (*stream_col_map.get_unchecked(target_col)).unwrap_unchecked();
                    source_arrs.push(r.batch().column(src_col).as_ref());
                }
                columns.push(interleave(&source_arrs, &fast_indices)?);
                continue;
            }

            // Partial column: pre-build source arrays indexed by range_idx,
            // then reverse-scan each PK group to find the last contributing range.
            let mut source_arrs: Vec<&dyn Array> = Vec::with_capacity(num_ranges + 1);

            let null_arr: ArrayRef =
                arrow::array::new_null_array(schema.field(target_col).data_type(), 1);
            source_arrs.push(null_arr.as_ref()); // index 0 = null placeholder

            // range_to_arr_idx[range_idx] = index into source_arrs
            // Default 0 means no source (null). Ranges WITH the column get real indices.
            let mut range_to_arr_idx = vec![0usize; num_ranges];
            for (r_idx, idx) in range_to_arr_idx.iter_mut().enumerate().take(num_ranges) {
                let r = ranges.get_unchecked(r_idx);
                let stream_col_map = source_col.get_unchecked(r.stream_idx());
                let opt = stream_col_map.get_unchecked(target_col);
                if opt.is_some() {
                    let src_col = (*opt).unwrap_unchecked();
                    *idx = source_arrs.len();
                    source_arrs.push(r.batch().column(src_col).as_ref());
                }
            }

            let mut indices: Vec<(usize, usize)> = Vec::with_capacity(num_output_rows);

            for group in pk_groups {
                let mut winner: Option<&InProgressRow> = None;
                for row in group.iter().rev() {
                    let r = ranges.get_unchecked(row.range_idx);
                    let stream_col_map = source_col.get_unchecked(r.stream_idx());
                    if stream_col_map.get_unchecked(target_col).is_some() {
                        winner = Some(row);
                        break;
                    }
                }

                if let Some(w) = winner {
                    let arr_idx = *range_to_arr_idx.get_unchecked(w.range_idx);
                    indices.push((arr_idx, w.row_idx));
                } else {
                    indices.push((0, 0));
                }
            }

            if source_arrs.len() == 1 {
                columns.push(arrow::array::new_null_array(
                    schema.field(target_col).data_type(),
                    num_output_rows,
                ));
            } else {
                columns.push(interleave(&source_arrs, &indices)?);
            }
        }
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
