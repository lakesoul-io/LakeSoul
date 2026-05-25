// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::Result;
use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::v2::batch_range::{
    BatchRange, InProgressPkGroup,
};
use arrow::array::Array;
use arrow::compute::interleave;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use std::collections::HashMap;

/// Build a `RecordBatch` from a list of PK groups and their source ranges,
/// using per-column UseLast semantics for partial schema support.
///
/// For each output column, the function:
/// 1. Scans each PK group's contributors in reverse order to find the last
///    range that has this column
/// 2. Collects unique source array references
/// 3. Uses `interleave` with a null placeholder for columns where no source exists
pub(crate) fn build_record_batch_from_pk_groups<C: CursorValues>(
    pk_groups: &[InProgressPkGroup],
    ranges: &[&BatchRange<C>],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_target_cols = schema.fields().len();
    let num_output_rows = pk_groups.len();
    let mut columns = Vec::with_capacity(num_target_cols);

    for target_col in 0..num_target_cols {
        let mut source_arrs: Vec<&dyn Array> = Vec::new();
        let mut ref_to_arr_idx: HashMap<(usize, usize), usize> = HashMap::new();
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(num_output_rows);

        let null_arr: ArrayRef =
            arrow::array::new_null_array(schema.field(target_col).data_type(), 1);
        source_arrs.push(null_arr.as_ref());

        for group in pk_groups {
            let winner = group
                .iter()
                .rev()
                .find(|row| ranges[row.range_idx].has_target_col(target_col));

            if let Some(winner) = winner {
                let key = (winner.range_idx, winner.row_idx);
                let arr_idx = *ref_to_arr_idx.entry(key).or_insert_with(|| {
                    let src_col = ranges[winner.range_idx]
                        .source_col_for_target(target_col)
                        .unwrap();
                    let idx = source_arrs.len();
                    source_arrs.push(
                        ranges[winner.range_idx]
                            .batch()
                            .column(src_col)
                            .as_ref(),
                    );
                    idx
                });
                indices.push((arr_idx, winner.row_idx));
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
            let col = interleave(&source_arrs, &indices)?;
            columns.push(col);
        }
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
