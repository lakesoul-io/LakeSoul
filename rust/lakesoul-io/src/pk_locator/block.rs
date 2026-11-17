// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Helpers shared by the exact-re-rank decoded-block cache.
//!
//! Exact re-rank needs the stored f32/f64 vector of candidate rows.  The
//! on-demand block cache (see [`super::VectorBlockSource`]) decodes only a
//! small row block from the source data file and caches it in the LakeSoul
//! S3->SSD `DiskCache`; there is no whole-column payload file anymore.
//!
//! This module contains the small format-free helpers: vector element tags,
//! list flattening, raw row-byte to Arrow array conversion and the exact
//! L2/IP scoring loops.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, LargeListArray,
    ListArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef};

/// Element width of a stored float-list column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VectorElem {
    F32,
    F64,
}

impl VectorElem {
    fn of_field(field: &FieldRef) -> Option<Self> {
        match field.data_type() {
            DataType::Float32 => Some(VectorElem::F32),
            DataType::Float64 => Some(VectorElem::F64),
            _ => None,
        }
    }

    pub(crate) fn size(self) -> usize {
        match self {
            VectorElem::F32 => 4,
            VectorElem::F64 => 8,
        }
    }
}

/// The element type of a stored float-list column (`List`, `LargeList` or
/// `FixedSizeList` of `Float32`/`Float64`).
pub(crate) fn vector_elem(data_type: &DataType) -> Option<VectorElem> {
    match data_type {
        DataType::FixedSizeList(field, dim) if *dim > 0 => VectorElem::of_field(field),
        DataType::List(field) | DataType::LargeList(field) => VectorElem::of_field(field),
        _ => None,
    }
}

/// The values of one contiguous row run of a stored vector column.
///
/// List columns are only accepted when every row of the run holds exactly the
/// column's vector length and none of the rows is null: the block layout has a
/// fixed row stride, and the fallback row take covers anything else.
pub(crate) fn chunk_values(
    array: &ArrayRef,
    rows: usize,
    dim: usize,
) -> Option<ArrayRef> {
    match array.data_type() {
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>()?;
            if list.value_length() as usize != dim || list.null_count() != 0 {
                return None;
            }
            let first = list.value_offset(0) as usize;
            let count = rows * dim;
            if first + count > list.values().len() {
                return None;
            }
            Some(list.values().slice(first, count))
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            if list.null_count() != 0 {
                return None;
            }
            let offsets = list.value_offsets();
            let first = *offsets.first()? as usize;
            let last = *offsets.get(rows)? as usize;
            if last.checked_sub(first)? != rows * dim {
                return None;
            }
            Some(list.values().slice(first, rows * dim))
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>()?;
            if list.null_count() != 0 {
                return None;
            }
            let offsets = list.value_offsets();
            let first = *offsets.first()? as usize;
            let last = *offsets.get(rows)? as usize;
            if last.checked_sub(first)? != rows * dim {
                return None;
            }
            Some(list.values().slice(first, rows * dim))
        }
        _ => None,
    }
}

/// Squared L2 distance of two stored vectors, eight lanes at a time.
pub(crate) fn squared_distance_f32(stored: &[f32], query: &[f32]) -> f32 {
    if stored.len() != query.len() {
        return f32::INFINITY;
    }
    let mut lanes = [0.0f32; 8];
    let mut index = 0;
    while index + 8 <= stored.len() {
        for lane in 0..8 {
            let delta = stored[index + lane] - query[index + lane];
            lanes[lane] += delta * delta;
        }
        index += 8;
    }
    let mut sum: f32 = lanes.iter().sum();
    while index < stored.len() {
        let delta = stored[index] - query[index];
        sum += delta * delta;
        index += 1;
    }
    sum
}

/// Negated inner product, eight lanes at a time.
pub(crate) fn negated_dot_f32(stored: &[f32], query: &[f32]) -> f32 {
    if stored.len() != query.len() {
        return f32::NEG_INFINITY;
    }
    let mut lanes = [0.0f32; 8];
    let mut index = 0;
    while index + 8 <= stored.len() {
        for lane in 0..8 {
            lanes[lane] += stored[index + lane] * query[index + lane];
        }
        index += 8;
    }
    let mut sum: f32 = lanes.iter().sum();
    while index < stored.len() {
        sum += stored[index] * query[index];
        index += 1;
    }
    -sum
}

/// Build an Arrow vector-list array from already-decoded row bytes.
pub(crate) fn take_rows_from_bytes(
    field: &FieldRef,
    dim: usize,
    elem: VectorElem,
    rows: &[Arc<[u8]>],
) -> Option<ArrayRef> {
    let row_bytes = dim * elem.size();
    let values_len = rows.len().checked_mul(dim)?;
    let values: ArrayRef = match elem {
        VectorElem::F32 => {
            let mut flat: Vec<f32> = vec![0.0; values_len];
            let dst = unsafe {
                std::slice::from_raw_parts_mut(
                    flat.as_mut_ptr() as *mut u8,
                    values_len * 4,
                )
            };
            for (position, row) in rows.iter().enumerate() {
                if row.len() != row_bytes {
                    return None;
                }
                dst[position * row_bytes..(position + 1) * row_bytes]
                    .copy_from_slice(row);
            }
            Arc::new(Float32Array::from(flat))
        }
        VectorElem::F64 => {
            let mut flat: Vec<f64> = vec![0.0; values_len];
            let dst = unsafe {
                std::slice::from_raw_parts_mut(
                    flat.as_mut_ptr() as *mut u8,
                    values_len * 8,
                )
            };
            for (position, row) in rows.iter().enumerate() {
                if row.len() != row_bytes {
                    return None;
                }
                dst[position * row_bytes..(position + 1) * row_bytes]
                    .copy_from_slice(row);
            }
            Arc::new(Float64Array::from(flat))
        }
    };
    let count = rows.len();
    match field.data_type() {
        DataType::FixedSizeList(item, size) => Some(Arc::new(FixedSizeListArray::new(
            Arc::clone(item),
            *size,
            values,
            None,
        ))),
        DataType::List(item) => {
            let offsets: Vec<i32> = (0..=count).map(|at| (at * dim) as i32).collect();
            Some(Arc::new(ListArray::new(
                Arc::clone(item),
                OffsetBuffer::new(offsets.into()),
                values,
                None,
            )))
        }
        DataType::LargeList(item) => {
            let offsets: Vec<i64> = (0..=count).map(|at| (at * dim) as i64).collect();
            Some(Arc::new(LargeListArray::new(
                Arc::clone(item),
                OffsetBuffer::new(offsets.into()),
                values,
                None,
            )))
        }
        _ => None,
    }
}

/// Score already-decoded row bytes with the exact L2/IP formula.
pub(crate) fn score_rows_from_bytes(
    dim: usize,
    elem: VectorElem,
    rows: &[Arc<[u8]>],
    query: &[f32],
    ascending: bool,
) -> Option<Vec<f32>> {
    if dim != query.len() {
        return None;
    }
    let mut scores = Vec::with_capacity(rows.len());
    let mut stored: Vec<f32> = Vec::with_capacity(dim);
    for row in rows {
        stored.clear();
        match elem {
            VectorElem::F32 => {
                for value in row.as_chunks::<4>().0.iter() {
                    stored.push(f32::from_le_bytes([
                        value[0], value[1], value[2], value[3],
                    ]));
                }
            }
            VectorElem::F64 => {
                for value in row.as_chunks::<8>().0.iter() {
                    stored.push(f64::from_le_bytes([
                        value[0], value[1], value[2], value[3], value[4], value[5],
                        value[6], value[7],
                    ]) as f32);
                }
            }
        }
        if stored.len() != dim {
            return None;
        }
        scores.push(if ascending {
            squared_distance_f32(&stored, query)
        } else {
            negated_dot_f32(&stored, query)
        });
    }
    Some(scores)
}
