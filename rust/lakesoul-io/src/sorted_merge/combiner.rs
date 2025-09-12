// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! This module provides the implementation of the combiner for the sorted merge.
//! It provides two types of combiners:
//! - [`MinHeapSortKeyBatchRangeCombiner`]: a common combiner that uses a min heap to merge the sorted ranges.
//! - [`UseLastRangeCombiner`]: a combiner that uses a loser tree to merge the sorted ranges. UseLastRangeCombiner is used for UseLast MergeOperator.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::constant::{ConstEmptyArray, ConstNullArray};
use crate::sorted_merge::merge_operator::{MergeOperator, MergeResult};
use crate::sorted_merge::sort_key_range::{
    SortKeyArrayRange, SortKeyArrayRangeVec, SortKeyBatchRange, SortKeyBatchRanges,
    SortKeyBatchRangesRef,
};

use super::sort_key_range::UseLastSortKeyBatchRanges;
use crate::sorted_merge::cursor::CursorValues;
use arrow::compute::interleave;
use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, PrimitiveBuilder, StringBuilder,
        make_array as make_arrow_array,
    },
    datatypes::{DataType, Field, SchemaRef},
    error::ArrowError,
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use arrow_array::types::*;
use dary_heap::QuaternaryHeap;
use nohash::BuildNoHashHasher;

/// The combiner for the sorted merge.
#[derive(Debug)]
pub enum RangeCombiner<C: CursorValues> {
    DefaultUseLastRangeCombiner(UseLastRangeCombiner<C>),
    MinHeapSortKeyBatchRangeCombiner(MinHeapSortKeyBatchRangeCombiner<C>),
}

impl<C: CursorValues> RangeCombiner<C> {
    /// Create a new RangeCombiner.
    pub fn new(
        schema: SchemaRef,
        streams_num: usize,
        fields_map: Arc<Vec<Vec<usize>>>,
        target_batch_size: usize,
        merge_operator: Vec<MergeOperator>,
        is_partial_merge: bool,
    ) -> Self {
        if merge_operator.is_empty()
            || merge_operator
                .iter()
                .all(|op| *op == MergeOperator::UseLast)
        {
            RangeCombiner::DefaultUseLastRangeCombiner(UseLastRangeCombiner::new(
                schema,
                streams_num,
                fields_map,
                target_batch_size,
                is_partial_merge,
            ))
        } else {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(
                MinHeapSortKeyBatchRangeCombiner::new(
                    schema,
                    streams_num,
                    fields_map,
                    target_batch_size,
                    merge_operator,
                ),
            )
        }
    }

    /// Push a range into the combiner.
    pub fn push_range(&mut self, range: SortKeyBatchRange<C>) {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(combiner) => {
                combiner.push(range)
            }
            RangeCombiner::DefaultUseLastRangeCombiner(combiner) => combiner.push(range),
        };
    }

    /// Poll a result from the combiner.
    pub fn poll_result(&mut self) -> RangeCombinerResult<C> {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(combiner) => {
                combiner.poll_result()
            }
            RangeCombiner::DefaultUseLastRangeCombiner(combiner) => {
                combiner.poll_result()
            }
        }
    }

    pub fn external_advance(&self) -> bool {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(_) => true,
            RangeCombiner::DefaultUseLastRangeCombiner(_) => false,
        }
    }
}

/// The poll result of the combiner.
#[derive(Debug)]
pub enum RangeCombinerResult<C: CursorValues> {
    /// The combiner is finished.
    None,
    /// An error occurred.
    Err(ArrowError),
    /// A range.
    Range(SortKeyBatchRange<C>),
    /// A record batch.
    RecordBatch(ArrowResult<RecordBatch>),
}

/// The combiner for the sorted merge using a min heap.
/// It is used for the common merge operator.
#[derive(Debug)]
pub struct MinHeapSortKeyBatchRangeCombiner<C: CursorValues> {
    /// The schema of the record batch.
    schema: SchemaRef,

    /// The fields map from source schemas to target schema which vector index = stream_idx.
    fields_map: Arc<Vec<Vec<usize>>>,

    /// The min heap.
    heap: QuaternaryHeap<Reverse<SortKeyBatchRange<C>>>,

    /// The in-progress ranges that accumulate from popping from heap.
    /// When the in-progress ranges are full, we should build a record batch by merging them.
    in_progress: Vec<SortKeyBatchRangesRef<C>>,

    /// The target batch size for generated record batch.
    target_batch_size: usize,

    /// The current sort key range.
    current_sort_key_range: SortKeyBatchRangesRef<C>,

    /// The merge operator.
    merge_operator: Vec<MergeOperator>,

    /// The constant null array to avoid duplicate allocation.
    const_null_array: ConstNullArray,

    /// The constant empty array.
    const_empty_array: ConstEmptyArray,
}

impl<C: CursorValues> MinHeapSortKeyBatchRangeCombiner<C> {
    /// Create a new MinHeapSortKeyBatchRangeCombiner.
    ///
    /// # Arguments
    ///
    /// * `schema`: The schema of the record batch.
    /// * `streams_num`: The number of streams.
    /// * `fields_map`: The fields map from source schemas to target schema which vector index = stream_idx.
    /// * `target_batch_size`: The target batch size for generated record batch.
    /// * `merge_operator`: The merge operator.
    pub fn new(
        schema: SchemaRef,
        streams_num: usize,
        fields_map: Arc<Vec<Vec<usize>>>,
        target_batch_size: usize,
        merge_operator: Vec<MergeOperator>,
    ) -> Self {
        let new_range =
            Arc::new(SortKeyBatchRanges::new(schema.clone(), fields_map.clone()));
        let merge_op = match merge_operator.len() {
            0 => vec![MergeOperator::UseLast; schema.fields().len()],
            _ => merge_operator,
        };
        MinHeapSortKeyBatchRangeCombiner {
            schema,
            fields_map,
            heap: QuaternaryHeap::with_capacity(streams_num),
            in_progress: Vec::with_capacity(target_batch_size),
            target_batch_size,
            current_sort_key_range: new_range,
            merge_operator: merge_op,
            const_null_array: ConstNullArray::new(),
            const_empty_array: ConstEmptyArray::new(),
        }
    }

    pub fn push(&mut self, range: SortKeyBatchRange<C>) {
        self.heap.push(Reverse(range))
    }

    /// if in_progress is full, we should build record batch by merge all ranges in in_progress
    /// otherwise,
    /// if heap is not empty, we should pop from heap and add to in_progress
    /// then return the advanced popped range
    /// if heap is empty,
    /// check if in_progress is empty, if so, return None, which the RangeCombiner is finished
    /// otherwise, build record batch by merge all the remaining ranges in in_progress
    pub fn poll_result(&mut self) -> RangeCombinerResult<C> {
        if self.in_progress.len() == self.target_batch_size {
            RangeCombinerResult::RecordBatch(self.build_record_batch())
        } else {
            match self.heap.pop() {
                Some(Reverse(mut range)) => {
                    if self.current_sort_key_range.match_row(&range) {
                        self.get_mut_current_sort_key_range()
                            .add_range_in_batch(range.clone());
                    } else {
                        self.in_progress.push(self.current_sort_key_range.clone());
                        self.init_current_sort_key_range();
                        self.get_mut_current_sort_key_range()
                            .add_range_in_batch(range.clone());
                    }
                    range.advance();
                    RangeCombinerResult::Range(range)
                }
                None => {
                    if self.current_sort_key_range.is_empty()
                        && self.in_progress.is_empty()
                    {
                        RangeCombinerResult::None
                    } else {
                        if !self.current_sort_key_range.is_empty() {
                            self.in_progress.push(self.current_sort_key_range.clone());
                            self.get_mut_current_sort_key_range().set_batch_range(None);
                        }
                        RangeCombinerResult::RecordBatch(self.build_record_batch())
                    }
                }
            }
        }
    }

    /// Build a record batch by merging the in-progress ranges.
    /// Construct [`Array`] for each column by columnarly merging the in-progress ranges.
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        // construct record batch by columnarly merging
        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, field)| {
                let capacity = self.in_progress.len();
                // collect all array ranges of current column_idx for each row
                let ranges_per_row: Vec<&SortKeyArrayRangeVec> = self
                    .in_progress
                    .iter()
                    .map(|ranges_per_row| ranges_per_row.column(column_idx))
                    .collect::<Vec<_>>();

                // flatten all array ranges of current column_idx for interleave
                let mut flatten_array_ranges = ranges_per_row
                    .iter()
                    .flat_map(|ranges| *ranges)
                    .collect::<Vec<&SortKeyArrayRange>>();

                // For the case that consecutive rows of target array are extended from the same source array
                flatten_array_ranges.dedup_by_key(|range| range.batch_idx);

                let mut batch_idx_to_flatten_array_idx =
                    HashMap::<usize, usize>::with_capacity(16);
                let mut flatten_dedup_arrays: Vec<ArrayRef> = flatten_array_ranges
                    .iter()
                    .enumerate()
                    .map(|(idx, range)| {
                        batch_idx_to_flatten_array_idx.insert(range.batch_idx, idx);
                        range.array()
                    })
                    .collect();

                flatten_dedup_arrays.push(self.const_null_array.get(field.data_type()));

                merge_sort_key_array_ranges(
                    capacity,
                    field,
                    ranges_per_row,
                    &mut flatten_dedup_arrays,
                    &batch_idx_to_flatten_array_idx,
                    unsafe { self.merge_operator.get_unchecked(column_idx) },
                    self.const_empty_array.get(field.data_type()),
                )
            })
            .collect::<ArrowResult<Vec<ArrayRef>>>()?;

        self.in_progress.clear();

        RecordBatch::try_new(self.schema.clone(), columns)
    }

    /// Initialize the current sort key range.
    fn init_current_sort_key_range(&mut self) {
        self.current_sort_key_range = Arc::new(SortKeyBatchRanges::new(
            self.schema.clone(),
            self.fields_map.clone(),
        ));
    }

    /// Get the mutable reference of the current sort key range.
    fn get_mut_current_sort_key_range(&mut self) -> &mut SortKeyBatchRanges<C> {
        Arc::get_mut(&mut self.current_sort_key_range).unwrap()
    }
}

/// Merge the sort key array ranges. We flatten all array ranges of current column_idx for interleave.
///
/// Before interleave, we deduplicate the array ranges by applying MergeOperator on rows from collected ranges.
/// For computed rows by MergeOperator and null rows, we append them to the end of the flatten array.
/// Finally, we interleave the flatten array and return the result.
fn merge_sort_key_array_ranges(
    capacity: usize,
    field: &Field,
    ranges: Vec<&SortKeyArrayRangeVec>,
    flatten_dedup_arrays: &mut Vec<ArrayRef>,
    batch_idx_to_flatten_array_idx: &HashMap<usize, usize>,
    merge_operator: &MergeOperator,
    empty_array: ArrayRef,
) -> ArrowResult<ArrayRef> {
    assert_eq!(ranges.len(), capacity);
    let data_type = (*field.data_type()).clone();
    let mut append_array_data_builder: Box<dyn ArrayBuilder> = match data_type {
        DataType::UInt8 => {
            Box::new(PrimitiveBuilder::<UInt8Type>::with_capacity(capacity))
        }
        DataType::UInt16 => {
            Box::new(PrimitiveBuilder::<UInt16Type>::with_capacity(capacity))
        }
        DataType::UInt32 => {
            Box::new(PrimitiveBuilder::<UInt32Type>::with_capacity(capacity))
        }
        DataType::UInt64 => {
            Box::new(PrimitiveBuilder::<UInt64Type>::with_capacity(capacity))
        }
        DataType::Int8 => Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(capacity)),
        DataType::Int16 => {
            Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(capacity))
        }
        DataType::Int32 => {
            Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(capacity))
        }
        DataType::Int64 => {
            Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(capacity))
        }
        DataType::Float32 => {
            Box::new(PrimitiveBuilder::<Float32Type>::with_capacity(capacity))
        }
        DataType::Float64 => {
            Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(capacity))
        }
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 256)),
        _ => {
            if *merge_operator == MergeOperator::UseLast {
                Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(capacity))
            } else {
                unimplemented!()
            }
        }
    };
    let append_idx = flatten_dedup_arrays.len();
    let null_idx = append_idx - 1;

    // ### build with arrow::compute::interleave ###
    let extend_list: Vec<(usize, usize)> = ranges
        .iter()
        .map(|ranges_per_row| {
            let res = match merge_operator.merge(
                data_type.clone(),
                ranges_per_row,
                &mut append_array_data_builder,
            )? {
                MergeResult::AppendValue(row_idx) => (append_idx, row_idx),
                MergeResult::AppendNull => (null_idx, 0),
                MergeResult::Extend(batch_idx, row_idx) => {
                    (batch_idx_to_flatten_array_idx[&batch_idx], row_idx)
                }
            };
            Ok(res)
        })
        .collect::<ArrowResult<Vec<_>>>()?;

    let append_array = match append_array_data_builder.len() {
        0 => empty_array,
        _ => make_arrow_array(append_array_data_builder.finish().into_data()),
    };

    flatten_dedup_arrays.push(append_array);
    interleave(
        flatten_dedup_arrays
            .iter()
            .map(|array_ref| array_ref.as_ref())
            .collect::<Vec<_>>()
            .as_slice(),
        extend_list.as_slice(),
    )
}

/// The combiner for the sorted merge using a loser tree.
/// It is used for the UseLast merge operator.
#[derive(Debug)]
pub struct UseLastRangeCombiner<C: CursorValues> {
    /// The schema of the record batch.
    schema: SchemaRef,

    /// The fields map from source schemas to target schema which vector index = stream_idx.
    fields_map: Arc<Vec<Vec<usize>>>,

    /// The number of streams.
    streams_num: usize,

    /// A loser tree that always produces the minimum cursor
    ///
    /// Node 0 stores the top winner, Nodes 1..num_streams store
    /// the loser nodes
    ///
    /// This implements a "Tournament Tree" (aka Loser Tree) to keep
    /// track of the current smallest element at the top. When the top
    /// record is taken, the tree structure is not modified, and only
    /// the path from bottom to top is visited, keeping the number of
    /// comparisons close to the theoretical limit of `log(S)`.
    ///
    /// The current implementation uses a vector to store the tree.
    /// Conceptually, it looks like this (assuming 8 streams):
    ///
    /// ```text
    ///     0 (winner)
    ///
    ///     1
    ///    / \
    ///   2   3
    ///  / \ / \
    /// 4  5 6  7
    /// ```
    ///
    /// Where element at index 0 in the vector is the current winner. Element
    /// at index 1 is the root of the loser tree, element at index 2 is the
    /// left child of the root, and element at index 3 is the right child of
    /// the root and so on.
    ///
    /// reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
    loser_tree: Vec<usize>,

    /// The counter of the ranges.
    ranges_counter: usize,

    /// Whether the loser tree has been updated.
    loser_tree_has_updated: bool,

    /// ranges for each input source. `None` means the input is exhausted
    ranges: Vec<Option<SortKeyBatchRange<C>>>,

    /// The in-progress ranges that accumulate from popping from loser tree.
    /// For UseLast merge operator, [`UseLastSortKeyBatchRanges`] is used to collect the in-progress ranges.
    in_progress: Vec<UseLastSortKeyBatchRanges<C>>,

    /// The target batch size for generated record batch.
    target_batch_size: usize,

    /// The current sort key range.
    current_sort_key_range: UseLastSortKeyBatchRanges<C>,

    /// The constant null array to avoid duplicate allocation.
    const_null_array: ConstNullArray,

    /// Whether the merge is partial.
    is_partial_merge: bool,
}

impl<C: CursorValues> UseLastRangeCombiner<C> {
    /// Create a new UseLastRangeCombiner.
    ///
    /// # Arguments
    ///
    /// * `schema`: The schema of the record batch.
    /// * `streams_num`: The number of streams.
    /// * `fields_map`: The fields map from source schemas to target schema which vector index = stream_idx.
    /// * `target_batch_size`: The target batch size for generated record batch.
    /// * `is_partial_merge`: Whether the merge is partial.
    pub fn new(
        schema: SchemaRef,
        streams_num: usize,
        fields_map: Arc<Vec<Vec<usize>>>,
        target_batch_size: usize,
        is_partial_merge: bool,
    ) -> Self {
        Self {
            schema: schema.clone(),
            fields_map: fields_map.clone(),
            streams_num,
            in_progress: Vec::with_capacity(target_batch_size),
            target_batch_size,
            current_sort_key_range: UseLastSortKeyBatchRanges::new(
                schema.fields().len(),
                is_partial_merge,
            ),
            const_null_array: ConstNullArray::new(),
            ranges: (0..streams_num).map(|_| None).collect(),
            loser_tree: vec![],
            ranges_counter: 0,
            loser_tree_has_updated: false,
            is_partial_merge,
        }
    }

    #[inline]
    pub fn push(&mut self, range: SortKeyBatchRange<C>) {
        unsafe {
            let stream_idx = range.stream_idx;
            *self.ranges.get_unchecked_mut(stream_idx) = Some(range);
            self.ranges_counter += 1;
        }
    }

    #[inline]
    pub fn update(&mut self) {
        if self.loser_tree.is_empty() {
            if self.ranges_counter >= self.streams_num {
                self.init_loser_tree();
            }
        } else {
            self.update_loser_tree();
        }
    }

    #[inline]
    pub fn set_current_sort_key_range(
        ranges: &mut UseLastSortKeyBatchRanges<C>,
        range: &SortKeyBatchRange<C>,
        fields_map: &Vec<Vec<usize>>,
    ) {
        ranges.add_range_in_batch(range, fields_map);
    }

    /// If in_progress is full, we should build record batch by merge all ranges in in_progress.
    /// Otherwise,
    /// if loser tree is not empty, we should pop from loser tree and add to in_progress.
    /// then return the advanced popped range.
    /// if loser tree is empty,
    /// check if in_progress is empty, if so, return None, which the RangeCombiner is finished.
    /// otherwise, build record batch by merge all the remaining ranges in in_progress.
    ///
    /// If the merge is full column merge, we use `build_record_batch_full_merge` to build the record batch.
    /// Otherwise, we use `build_record_batch` to build the record batch.
    pub fn poll_result(&mut self) -> RangeCombinerResult<C> {
        if self.ranges_counter < self.streams_num {
            return RangeCombinerResult::Err(ArrowError::InvalidArgumentError(format!(
                "Not all streams have been initialized, ranges_counter: {}, streams_num: {}",
                self.ranges_counter, self.streams_num
            )));
        }
        if self.in_progress.len() == self.target_batch_size {
            RangeCombinerResult::RecordBatch(self.build_record_batch())
        } else {
            if !self.loser_tree_has_updated {
                self.update();
            }
            let winner = self.loser_tree[0];
            if let Some(mut range) = self.ranges[winner].take() {
                self.loser_tree_has_updated = false;
                if self.current_sort_key_range.match_row(&range) {
                    let fields_map = &*self.fields_map;
                    Self::set_current_sort_key_range(
                        &mut self.current_sort_key_range,
                        &range,
                        fields_map,
                    );
                } else {
                    self.push_and_reinit_current_sort_key_range();
                    let fields_map = &*self.fields_map;
                    Self::set_current_sort_key_range(
                        &mut self.current_sort_key_range,
                        &range,
                        fields_map,
                    );
                }
                range.advance();
                RangeCombinerResult::Range(range)
            } else if self.current_sort_key_range.is_empty()
                && self.in_progress.is_empty()
            {
                RangeCombinerResult::None
            } else {
                if !self.current_sort_key_range.is_empty() {
                    self.push_and_reinit_current_sort_key_range();
                    self.current_sort_key_range.set_batch_range(None);
                }
                RangeCombinerResult::RecordBatch(self.build_record_batch())
            }
        }
    }

    /// For full column merge, all columns of each row have same batch idx.
    /// So we only need to build indices once and reuse them for all columns.
    fn build_record_batch_full_merge(&mut self) -> ArrowResult<RecordBatch> {
        let capacity = self.in_progress.len();
        let mut interleave_idx = Vec::<(usize, usize)>::with_capacity(capacity);
        interleave_idx.resize(capacity, (0, 0));
        let mut batch_idx_to_flatten_array_idx =
            HashMap::<usize, usize, BuildNoHashHasher<usize>>::with_capacity_and_hasher(
                capacity * 2,
                BuildNoHashHasher::default(),
            );
        let mut array_count = 1usize;
        let mut flatten_arrays: Vec<Vec<ArrayRef>> =
            Vec::with_capacity(self.schema.fields().len());
        flatten_arrays.resize(self.schema.fields().len(), vec![]);
        for column_idx in 0..self.schema.fields().len() {
            unsafe {
                let flatten_array = flatten_arrays.get_unchecked_mut(column_idx);
                flatten_array.reserve(capacity + 1);
                flatten_array.push(
                    self.const_null_array
                        .get(self.schema.field(column_idx).data_type()),
                );
            }
        }
        for (idx, ranges) in self.in_progress.iter().enumerate() {
            if let Some(range) = ranges.column(0) {
                let batch_idx = range.batch_idx;
                unsafe {
                    match batch_idx_to_flatten_array_idx.get(&batch_idx) {
                        Some(flatten_array_idx) => {
                            *interleave_idx.get_unchecked_mut(idx) =
                                (*flatten_array_idx, range.row_idx)
                        }
                        None => {
                            batch_idx_to_flatten_array_idx.insert(batch_idx, array_count);
                            *interleave_idx.get_unchecked_mut(idx) =
                                (array_count, range.row_idx);
                            array_count += 1;
                            // fill all column arrays for interleaving
                            for column_idx in 0..self.schema.fields().len() {
                                let flatten_array =
                                    flatten_arrays.get_unchecked_mut(column_idx);
                                flatten_array.push(
                                    range.array_ref_by_col(
                                        *self
                                            .fields_map
                                            .get_unchecked(range.stream_idx)
                                            .get_unchecked(column_idx),
                                    ),
                                );
                            }
                        }
                    }
                }
            }
        }
        let columns = flatten_arrays
            .iter()
            .map(|array| {
                interleave(
                    array
                        .iter()
                        .map(|a| a.as_ref())
                        .collect::<Vec<&dyn Array>>()
                        .as_slice(),
                    interleave_idx.as_slice(),
                )
            })
            .collect::<ArrowResult<Vec<ArrayRef>>>()?;

        self.in_progress.clear();

        RecordBatch::try_new(self.schema.clone(), columns)
    }

    /// Build a record batch by merging the in-progress ranges.
    ///
    /// For UseLast merge operator, we still flatten all array ranges of current column_idx for interleave.
    /// However, we only need to collect last index of array ranges for each generated row, rather than compute the merge result.
    /// Finally, we interleave the flatten array and return the result.
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        if !self.is_partial_merge {
            return self.build_record_batch_full_merge();
        }
        // construct record batch by columnarly merging
        let capacity = self.in_progress.len();
        let mut interleave_idx = Vec::<(usize, usize)>::with_capacity(capacity);
        let mut batch_idx_to_flatten_array_idx =
            HashMap::<usize, usize, BuildNoHashHasher<usize>>::with_capacity_and_hasher(
                capacity * 2,
                BuildNoHashHasher::default(),
            );
        // collect all array ranges of current column_idx for each row
        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, field)| {
                let mut flatten_arrays = Vec::with_capacity(capacity + 1);
                flatten_arrays.push(self.const_null_array.get_ref(field.data_type()));
                batch_idx_to_flatten_array_idx.clear();
                interleave_idx.clear();
                interleave_idx.resize(capacity, (0, 0));

                for (idx, range) in self.in_progress.iter().enumerate() {
                    unsafe {
                        let array = range.column(column_idx);
                        if let Some(array) = array {
                            match batch_idx_to_flatten_array_idx.get(&array.batch_idx) {
                                Some(flatten_array_idx) => {
                                    *interleave_idx.get_unchecked_mut(idx) =
                                        (*flatten_array_idx, array.row_idx)
                                }
                                None => {
                                    flatten_arrays.push(array.array_ref());
                                    batch_idx_to_flatten_array_idx.insert(
                                        array.batch_idx,
                                        flatten_arrays.len() - 1,
                                    );
                                    *interleave_idx.get_unchecked_mut(idx) =
                                        (flatten_arrays.len() - 1, array.row_idx);
                                }
                            }
                        }
                    }
                }

                interleave(flatten_arrays.as_slice(), interleave_idx.as_slice())
            })
            .collect::<ArrowResult<Vec<ArrayRef>>>()?;

        self.in_progress.clear();

        RecordBatch::try_new(self.schema.clone(), columns)
    }

    /// Initialize the current sort key range.
    #[inline]
    fn push_and_reinit_current_sort_key_range(&mut self) {
        let new_sort_key_range = UseLastSortKeyBatchRanges::new(
            self.schema.fields().len(),
            self.is_partial_merge,
        );
        let old = std::mem::replace(&mut self.current_sort_key_range, new_sort_key_range);
        self.in_progress.push(old);
    }

    /// Attempts to initialize the loser tree with one value from each
    /// non exhausted input, if possible
    fn init_loser_tree(&mut self) {
        // Init loser tree
        unsafe {
            self.loser_tree.resize(self.streams_num, usize::MAX);
            for i in 0..self.streams_num {
                let mut winner = i;
                let mut cmp_node = self.loser_tree_leaf_node_index(i);
                while cmp_node != 0
                    && *self.loser_tree.get_unchecked(cmp_node) != usize::MAX
                {
                    let challenger = self.loser_tree.get_unchecked(cmp_node);
                    match (
                        &self.ranges.get_unchecked(winner),
                        &self.ranges.get_unchecked(*challenger),
                    ) {
                        // None means the stream is exhausted, always mark None as loser
                        (None, _) => {
                            self.update_winner(cmp_node, &mut winner, *challenger)
                        }
                        (_, None) => (),
                        (Some(ac), Some(bc)) => {
                            if ac.cmp(bc).is_gt() {
                                self.update_winner(cmp_node, &mut winner, *challenger);
                            }
                        }
                    }

                    cmp_node = self.loser_tree_parent_node_index(cmp_node);
                }
                *self.loser_tree.get_unchecked_mut(cmp_node) = winner;
            }
        }
        self.loser_tree_has_updated = true;
    }

    /// Find the leaf node index in the loser tree for the given cursor index
    ///
    /// Note that this is not necessarily a leaf node in the tree, but it can
    /// also be a half-node (a node with only one child). This happens when the
    /// number of cursors/streams is not a power of two. Thus, the loser tree
    /// will be unbalanced, but it will still work correctly.
    ///
    /// For example, with 5 streams, the loser tree will look like this:
    ///
    /// ```text
    ///           0 (winner)
    ///
    ///           1
    ///        /     \
    ///       2       3
    ///     /  \     / \
    ///    4    |   |   |
    ///   / \   |   |   |
    /// -+---+--+---+---+---- Below is not a part of loser tree
    ///  S3 S4 S0   S1  S2
    /// ```
    ///
    /// S0, S1, ... S4 are the streams (read: stream at index 0, stream at
    /// index 1, etc.)
    ///
    /// Zooming in at node 2 in the loser tree as an example, we can see that
    /// it takes as input the next item at (S0) and the loser of (S3, S4).
    ///
    #[inline]
    fn loser_tree_leaf_node_index(&self, cursor_index: usize) -> usize {
        (self.streams_num + cursor_index) / 2
    }

    /// Find the parent node index for the given node index
    #[inline]
    fn loser_tree_parent_node_index(&self, node_idx: usize) -> usize {
        node_idx / 2
    }

    /// Updates the loser tree to reflect the new winner after the previous winner is consumed.
    /// This function adjusts the tree by comparing the current winner with challengers from
    /// other partitions.
    ///
    /// If `enable_round_robin_tie_breaker` is true and a tie occurs at the final level, the
    /// tie-breaker logic will be applied to ensure fair selection among equal elements.
    fn update_loser_tree(&mut self) {
        unsafe {
            let mut winner = *self.loser_tree.get_unchecked(0);
            let mut cmp_node = self.loser_tree_leaf_node_index(winner);

            while cmp_node != 0 {
                let challenger = *self.loser_tree.get_unchecked(cmp_node);
                // None means the stream is exhausted, always mark None as loser
                match (
                    &self.ranges.get_unchecked(winner),
                    &self.ranges.get_unchecked(challenger),
                ) {
                    (None, _) => self.update_winner(cmp_node, &mut winner, challenger),
                    (_, None) => (),
                    (Some(ac), Some(bc)) => {
                        if ac.cmp(bc).is_gt() {
                            self.update_winner(cmp_node, &mut winner, challenger);
                        }
                    }
                }
                cmp_node = self.loser_tree_parent_node_index(cmp_node);
            }
            *self.loser_tree.get_unchecked_mut(0) = winner;
        }
        self.loser_tree_has_updated = true;
    }

    /// Update the winner of the loser tree.
    #[inline]
    fn update_winner(&mut self, cmp_node: usize, winner: &mut usize, challenger: usize) {
        unsafe {
            *self.loser_tree.get_unchecked_mut(cmp_node) = *winner;
            *winner = challenger;
        }
    }
}
