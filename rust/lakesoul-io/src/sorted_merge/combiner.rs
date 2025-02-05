// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Reverse;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::constant::{ConstEmptyArray, ConstNullArray};
use crate::sorted_merge::merge_operator::{MergeOperator, MergeResult};
use crate::sorted_merge::sort_key_range::{
    SortKeyArrayRange, SortKeyBatchRange, SortKeyBatchRanges, SortKeyBatchRangesRef,
};

use arrow::compute::interleave;
use arrow::{
    array::{make_array as make_arrow_array, Array, ArrayBuilder, ArrayRef, PrimitiveBuilder, StringBuilder},
    datatypes::{DataType, Field, SchemaRef},
    error::ArrowError,
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use arrow_array::types::*;
use dary_heap::QuaternaryHeap;
use nohash::BuildNoHashHasher;
use smallvec::SmallVec;

use super::sort_key_range::{UseLastSortKeyBatchRanges, UseLastSortKeyBatchRangesRef};

#[derive(Debug)]
pub enum RangeCombiner {
    DefaultUseLastRangeCombiner(UseLastRangeCombiner),
    MinHeapSortKeyBatchRangeCombiner(MinHeapSortKeyBatchRangeCombiner),
}

impl RangeCombiner {
    pub fn new(
        schema: SchemaRef,
        streams_num: usize,
        fields_map: Arc<Vec<Vec<usize>>>,
        target_batch_size: usize,
        merge_operator: Vec<MergeOperator>,
        is_partial_merge: bool,
    ) -> Self {
        if merge_operator.is_empty() || merge_operator.iter().all(|op| *op == MergeOperator::UseLast) {
            RangeCombiner::DefaultUseLastRangeCombiner(UseLastRangeCombiner::new(
                schema,
                streams_num,
                fields_map,
                target_batch_size,
                is_partial_merge,
            ))
        } else {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(MinHeapSortKeyBatchRangeCombiner::new(
                schema,
                streams_num,
                fields_map,
                target_batch_size,
                merge_operator,
            ))
        }
    }

    pub fn push_range(&mut self, range: SortKeyBatchRange) {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(combiner) => combiner.push(range),
            RangeCombiner::DefaultUseLastRangeCombiner(combiner) => combiner.push(range),
        };
    }

    pub fn poll_result(&mut self) -> RangeCombinerResult {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(combiner) => combiner.poll_result(),
            RangeCombiner::DefaultUseLastRangeCombiner(combiner) => combiner.poll_result(),
        }
    }

    pub fn external_advance(&self) -> bool {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(_) => true,
            RangeCombiner::DefaultUseLastRangeCombiner(_) => false,
        }
    }
}

#[derive(Debug)]
pub enum RangeCombinerResult {
    None,
    Err(ArrowError),
    Range(SortKeyBatchRange),
    RecordBatch(ArrowResult<RecordBatch>),
}

#[derive(Debug)]
pub struct MinHeapSortKeyBatchRangeCombiner {
    schema: SchemaRef,

    // fields_index_map from source schemas to target schema which vector index = stream_idx
    fields_map: Arc<Vec<Vec<usize>>>,

    heap: QuaternaryHeap<Reverse<SortKeyBatchRange>>,
    in_progress: Vec<SortKeyBatchRangesRef>,
    target_batch_size: usize,
    current_sort_key_range: SortKeyBatchRangesRef,
    merge_operator: Vec<MergeOperator>,
    const_null_array: ConstNullArray,
    const_empty_array: ConstEmptyArray,
}

impl MinHeapSortKeyBatchRangeCombiner {
    pub fn new(
        schema: SchemaRef,
        streams_num: usize,
        fields_map: Arc<Vec<Vec<usize>>>,
        target_batch_size: usize,
        merge_operator: Vec<MergeOperator>,
    ) -> Self {
        let new_range = Arc::new(SortKeyBatchRanges::new(schema.clone(), fields_map.clone()));
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

    pub fn push(&mut self, range: SortKeyBatchRange) {
        self.heap.push(Reverse(range))
    }

    /// if in_progress is full, we should build record batch by merge all ranges in in_progress
    /// otherwise, 
    /// if heap is not empty, we should pop from heap and add to in_progress
    /// then return the advanced popped range
    /// if heap is empty, 
    /// check if in_progress is empty, if so, return None, which the RangeCombiner is finished
    /// otherwise, build record batch by merge all the remaining ranges in in_progress
    pub fn poll_result(&mut self) -> RangeCombinerResult {
        if self.in_progress.len() == self.target_batch_size {
            RangeCombinerResult::RecordBatch(self.build_record_batch())
        } else {
            match self.heap.pop() {
                Some(Reverse(mut range)) => {
                    if self.current_sort_key_range.match_row(&range) {
                        self.get_mut_current_sort_key_range().add_range_in_batch(range.clone());
                    } else {
                        self.in_progress.push(self.current_sort_key_range.clone());
                        self.init_current_sort_key_range();
                        self.get_mut_current_sort_key_range().add_range_in_batch(range.clone());
                    }
                    range.advance();
                    RangeCombinerResult::Range(range)
                }
                None => {
                    if self.current_sort_key_range.is_empty() && self.in_progress.is_empty() {
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
                let ranges_per_row: Vec<&SmallVec<[SortKeyArrayRange; 4]>> = self
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

                let mut batch_idx_to_flatten_array_idx = HashMap::<usize, usize>::with_capacity(16);
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

    fn init_current_sort_key_range(&mut self) {
        self.current_sort_key_range = Arc::new(SortKeyBatchRanges::new(self.schema.clone(), self.fields_map.clone()));
    }

    fn get_mut_current_sort_key_range(&mut self) -> &mut SortKeyBatchRanges {
        Arc::make_mut(&mut self.current_sort_key_range)
    }
}

fn merge_sort_key_array_ranges(
    capacity: usize,
    field: &Field,
    ranges: Vec<&SmallVec<[SortKeyArrayRange; 4]>>,
    flatten_dedup_arrays: &mut Vec<ArrayRef>,
    batch_idx_to_flatten_array_idx: &HashMap<usize, usize>,
    merge_operator: &MergeOperator,
    empty_array: ArrayRef,
) -> ArrowResult<ArrayRef> {
    assert_eq!(ranges.len(), capacity);
    let data_type = (*field.data_type()).clone();
    let mut append_array_data_builder: Box<dyn ArrayBuilder> = match data_type {
        DataType::UInt8 => Box::new(PrimitiveBuilder::<UInt8Type>::with_capacity(capacity)),
        DataType::UInt16 => Box::new(PrimitiveBuilder::<UInt16Type>::with_capacity(capacity)),
        DataType::UInt32 => Box::new(PrimitiveBuilder::<UInt32Type>::with_capacity(capacity)),
        DataType::UInt64 => Box::new(PrimitiveBuilder::<UInt64Type>::with_capacity(capacity)),
        DataType::Int8 => Box::new(PrimitiveBuilder::<Int8Type>::with_capacity(capacity)),
        DataType::Int16 => Box::new(PrimitiveBuilder::<Int16Type>::with_capacity(capacity)),
        DataType::Int32 => Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(capacity)),
        DataType::Int64 => Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(capacity)),
        DataType::Float32 => Box::new(PrimitiveBuilder::<Float32Type>::with_capacity(capacity)),
        DataType::Float64 => Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(capacity)),
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
            let res = match merge_operator.merge(data_type.clone(), ranges_per_row, &mut append_array_data_builder)? {
                MergeResult::AppendValue(row_idx) => (append_idx, row_idx),
                MergeResult::AppendNull => (null_idx, 0),
                MergeResult::Extend(batch_idx, row_idx) => (batch_idx_to_flatten_array_idx[&batch_idx], row_idx),
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

#[derive(Debug)]
pub struct UseLastRangeCombiner {
    schema: SchemaRef,
    fields_map: Arc<Vec<Vec<usize>>>,
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

    ranges_counter: usize,

    loser_tree_has_updated: bool,

    /// ranges for each input source. `None` means the input is exhausted
    ranges: Vec<Option<SortKeyBatchRange>>,

    in_progress: Vec<UseLastSortKeyBatchRangesRef>,
    target_batch_size: usize,
    current_sort_key_range: UseLastSortKeyBatchRangesRef,
    const_null_array: ConstNullArray,
    is_partial_merge: bool,
}

impl UseLastRangeCombiner {
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
            current_sort_key_range: Arc::new(UseLastSortKeyBatchRanges::new(schema, fields_map, is_partial_merge)),
            const_null_array: ConstNullArray::new(),
            ranges: (0..streams_num).map(|_| None).collect(),
            loser_tree: vec![],
            ranges_counter: 0,
            loser_tree_has_updated: false,
            is_partial_merge,
        }
    }

    pub fn push(&mut self, range: SortKeyBatchRange) {
        let stream_idx = range.stream_idx;
        self.ranges[stream_idx] = Some(range);
        self.ranges_counter += 1;
    }

    pub fn update(&mut self) {
        if self.loser_tree.is_empty() {
            if self.ranges_counter >= self.streams_num {
                self.init_loser_tree();
            }
        } else {
            self.update_loser_tree();
        }
    }

    /// if in_progress is full, we should build record batch by merge all ranges in in_progress
    /// otherwise, 
    /// if heap is not empty, we should pop from heap and add to in_progress
    /// then return the advanced popped range
    /// if heap is empty, 
    /// check if in_progress is empty, if so, return None, which the RangeCombiner is finished
    /// otherwise, build record batch by merge all the remaining ranges in in_progress
    pub fn poll_result(&mut self) -> RangeCombinerResult {
        if self.ranges_counter < self.streams_num {
            return RangeCombinerResult::Err(ArrowError::InvalidArgumentError(format!("Not all streams have been initialized, ranges_counter: {}, streams_num: {}", self.ranges_counter, self.streams_num)));
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
                    self.get_mut_current_sort_key_range().add_range_in_batch(&range);
                } else {
                    self.in_progress.push(self.current_sort_key_range.clone());
                    self.init_current_sort_key_range();
                    self.get_mut_current_sort_key_range().add_range_in_batch(&range);
                }
                range.advance();
                RangeCombinerResult::Range(range)
            } else {
                if self.current_sort_key_range.is_empty() && self.in_progress.is_empty() {
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

    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
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
                                    *interleave_idx.get_unchecked_mut(idx) = (*flatten_array_idx, array.row_idx)
                                }
                                None => {
                                    if self.is_partial_merge {
                                        flatten_arrays.push(array.array_ref());
                                    } else {
                                        flatten_arrays.push(array.array_ref_by_col(
                                            *self.fields_map.get_unchecked(array.stream_idx)
                                                .get_unchecked(column_idx)
                                        ));
                                    }
                                    batch_idx_to_flatten_array_idx.insert(array.batch_idx, flatten_arrays.len() - 1);
                                    *interleave_idx.get_unchecked_mut(idx) = (flatten_arrays.len() - 1, array.row_idx);
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

    fn get_mut_current_sort_key_range(&mut self) -> &mut UseLastSortKeyBatchRanges {
        Arc::make_mut(&mut self.current_sort_key_range)
    }

    fn init_current_sort_key_range(&mut self) {
        self.current_sort_key_range = Arc::new(UseLastSortKeyBatchRanges::new(
            self.schema.clone(),
            self.fields_map.clone(),
            self.is_partial_merge,
        ));
    }

    /// Attempts to initialize the loser tree with one value from each
    /// non exhausted input, if possible
    fn init_loser_tree(&mut self) {
        // Init loser tree
        self.loser_tree = vec![usize::MAX; self.streams_num];
        for i in 0..self.streams_num {
            let mut winner = i;
            let mut cmp_node = self.loser_tree_leaf_node_index(i);
            while cmp_node != 0 && self.loser_tree[cmp_node] != usize::MAX {
                let challenger = self.loser_tree[cmp_node];
                match (&self.ranges[winner], &self.ranges[challenger]) {
                    // None means the stream is exhausted, always mark None as loser
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
            self.loser_tree[cmp_node] = winner;
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
        let mut winner = self.loser_tree[0];
        let mut cmp_node = self.loser_tree_leaf_node_index(winner);

        while cmp_node != 0 {
            let challenger = self.loser_tree[cmp_node];
            // None means the stream is exhausted, always mark None as loser
            match (&self.ranges[winner], &self.ranges[challenger]) {
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
        self.loser_tree[0] = winner;
        self.loser_tree_has_updated = true;
    }

    #[inline]
    fn update_winner(&mut self, cmp_node: usize, winner: &mut usize, challenger: usize) {
        self.loser_tree[cmp_node] = *winner;
        *winner = challenger;
    }


}
