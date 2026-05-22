// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::v2::batch_range::{
    BatchRange, InProgressPkGroup, InProgressRow,
};
use arrow::array::Array;
use arrow::compute::interleave;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use smallvec::smallvec;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

pub struct LoserTreeRangeMerge<'a, C: CursorValues> {
    ranges: Vec<&'a mut BatchRange<C>>,

    loser_tree: Vec<usize>,

    loser_tree_has_updated: bool,

    pk_groups: Vec<InProgressPkGroup>,

    target_batch_size: usize,

    schema: SchemaRef,
}

impl<'a, C: CursorValues> LoserTreeRangeMerge<'a, C> {
    pub fn new(
        schema: SchemaRef,
        ranges: Vec<&'a mut BatchRange<C>>,
        target_batch_size: usize,
    ) -> Self {
        let len = ranges.len();
        Self {
            ranges,
            loser_tree: Vec::with_capacity(len),
            loser_tree_has_updated: false,
            pk_groups: Vec::with_capacity(target_batch_size + 1),
            target_batch_size,
            schema,
        }
    }

    pub async fn merge(
        &mut self,
        tx: &Sender<crate::Result<RecordBatch>>,
    ) -> crate::Result<()> {
        self.init_loser_tree();
        loop {
            let winner = self.loser_tree[0];
            if !self.ranges[winner].has_more_rows() {
                break;
            }

            let is_same_pk = self.pk_groups.last().map_or(false, |group| {
                let first = &group[0];
                C::eq(
                    self.ranges[first.range_idx].cursor(),
                    first.row_idx,
                    self.ranges[winner].cursor(),
                    self.ranges[winner].begin_row(),
                )
            });

            if is_same_pk {
                self.pk_groups.last_mut().unwrap().push(InProgressRow {
                    range_idx: winner,
                    row_idx: self.ranges[winner].begin_row(),
                });
            } else {
                if self.pk_groups.len() >= self.target_batch_size {
                    let batch = self.flush_pk_groups()?;
                    tx.send(Ok(batch)).await?;
                }
                self.pk_groups.push(smallvec![InProgressRow {
                    range_idx: winner,
                    row_idx: self.ranges[winner].begin_row(),
                }]);
            }

            self.ranges[winner].advance();
            self.update_loser_tree();
        }
        if !self.pk_groups.is_empty() {
            let batch = self.flush_pk_groups()?;
            tx.send(Ok(batch)).await?;
        }
        Ok(())
    }

    fn flush_pk_groups(&mut self) -> crate::Result<RecordBatch> {
        let num_target_cols = self.schema.fields().len();
        let num_output_rows = self.pk_groups.len();
        let mut columns = Vec::with_capacity(num_target_cols);

        for target_col in 0..num_target_cols {
            let mut source_arrs: Vec<&dyn Array> = Vec::new();
            let mut ref_to_arr_idx: HashMap<(usize, usize), usize> = HashMap::new();
            let mut indices: Vec<(usize, usize)> = Vec::with_capacity(num_output_rows);

            let null_arr: ArrayRef = arrow::array::new_null_array(
                self.schema.field(target_col).data_type(),
                1,
            );
            source_arrs.push(null_arr.as_ref());

            for group in &self.pk_groups {
                let winner = group
                    .iter()
                    .rev()
                    .find(|row| self.ranges[row.range_idx].has_target_col(target_col));

                if let Some(winner) = winner {
                    let key = (winner.range_idx, winner.row_idx);
                    let arr_idx = *ref_to_arr_idx.entry(key).or_insert_with(|| {
                        let src_col = self.ranges[winner.range_idx]
                            .source_col_for_target(target_col)
                            .unwrap();
                        let idx = source_arrs.len();
                        source_arrs.push(
                            self.ranges[winner.range_idx]
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
                    self.schema.field(target_col).data_type(),
                    num_output_rows,
                ));
            } else {
                let col = interleave(&source_arrs, &indices)?;
                columns.push(col);
            }
        }

        self.pk_groups.clear();
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    fn init_loser_tree(&mut self) {
        unsafe {
            self.loser_tree.resize(self.ranges.len(), usize::MAX);
            for i in 0..self.ranges.len() {
                let mut winner = i;
                let mut cmp_node = self.loser_tree_leaf_node_index(i);
                while cmp_node != 0
                    && *self.loser_tree.get_unchecked(cmp_node) != usize::MAX
                {
                    let challenger = self.loser_tree.get_unchecked(cmp_node);
                    let winner_range = self.ranges.get_unchecked(winner);
                    let challenger_range = self.ranges.get_unchecked(*challenger);
                    match (
                        winner_range.has_more_rows(),
                        challenger_range.has_more_rows(),
                    ) {
                        (false, _) => {
                            self.update_winner(cmp_node, &mut winner, *challenger)
                        }
                        (_, false) => (),
                        (true, true) => {
                            if winner_range.cmp(challenger_range).is_gt() {
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

    #[inline]
    fn loser_tree_leaf_node_index(&self, cursor_index: usize) -> usize {
        (self.ranges.len() + cursor_index) / 2
    }

    #[inline]
    fn loser_tree_parent_node_index(&self, node_idx: usize) -> usize {
        node_idx / 2
    }

    fn update_loser_tree(&mut self) {
        unsafe {
            let mut winner = *self.loser_tree.get_unchecked(0);
            let mut cmp_node = self.loser_tree_leaf_node_index(winner);

            while cmp_node != 0 {
                let challenger = *self.loser_tree.get_unchecked(cmp_node);
                let winner_range = self.ranges.get_unchecked(winner);
                let challenger_range = self.ranges.get_unchecked(challenger);
                match (
                    winner_range.has_more_rows(),
                    challenger_range.has_more_rows(),
                ) {
                    (false, _) => self.update_winner(cmp_node, &mut winner, challenger),
                    (_, false) => (),
                    (true, true) => {
                        if winner_range.cmp(challenger_range).is_gt() {
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

    #[inline]
    fn update_winner(&mut self, cmp_node: usize, winner: &mut usize, challenger: usize) {
        unsafe {
            *self.loser_tree.get_unchecked_mut(cmp_node) = *winner;
            *winner = challenger;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::merge::sorted::cursor::{ArrayValues, PrimitiveValues};
    use crate::physical_plan::merge::sorted::v2::batch_range::BatchRange;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::RecordBatch;
    use datafusion_common::assert_batches_eq;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryReservation,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch(id_values: Vec<i32>, name_values: Vec<&str>) -> RecordBatch {
        let id_array = Int32Array::from(id_values);
        let name_array = StringArray::from(name_values);

        RecordBatch::try_new(
            create_test_schema(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    fn create_int32_cursor(
        values: Vec<i32>,
        reservation: MemoryReservation,
    ) -> ArrayValues<PrimitiveValues<i32>> {
        use arrow::array::PrimitiveArray;
        use arrow::datatypes::Int32Type;
        use arrow_schema::SortOptions;

        let array = PrimitiveArray::<Int32Type>::from(values);
        let options = SortOptions::default();

        ArrayValues::new(options, &array, reservation)
    }

    async fn collect_merge_results<C: CursorValues + Send + Sync + 'static>(
        mut left: BatchRange<C>,
        mut right: BatchRange<C>,
        target_batch_size: usize,
    ) -> Vec<RecordBatch> {
        let schema = left.batch().schema();
        let (tx, mut rx) = mpsc::channel(10);

        let handle = tokio::spawn(async move {
            let mut merger = LoserTreeRangeMerge::new(
                schema.clone(),
                vec![&mut left, &mut right],
                target_batch_size,
            );
            merger.merge(&tx).await.unwrap();
        });

        let mut results = Vec::new();
        while let Some(result) = rx.recv().await {
            results.push(result.unwrap());
        }
        handle.await.unwrap();
        results
    }

    #[tokio::test]
    async fn test_basic_merge() -> crate::Result<()> {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let schema = create_test_schema();
        let batch1 = create_test_batch(vec![1, 3, 5], vec!["a", "c", "e"]);
        let batch2 = create_test_batch(vec![2, 4, 6], vec!["b", "d", "f"]);

        let cursor1 = create_int32_cursor(vec![1, 3, 5], r1.new_empty());
        let cursor2 = create_int32_cursor(vec![2, 4, 6], r1.new_empty());

        let stream_fields_map = Arc::new(vec![0, 1]);
        let range1 = BatchRange::new(cursor1, batch1, 0, 0, 2, stream_fields_map.clone());
        let range2 = BatchRange::new(cursor2, batch2, 1, 0, 2, stream_fields_map);

        let batches = collect_merge_results(range1, range2, 10).await;

        assert!(!batches.is_empty());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema(), schema);
        assert_batches_eq!(
            &[
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 1  | a    |",
                "| 2  | b    |",
                "| 3  | c    |",
                "| 4  | d    |",
                "| 5  | e    |",
                "| 6  | f    |",
                "+----+------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_equal_values_merge() -> crate::Result<()> {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let schema = create_test_schema();
        let batch1 = create_test_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let batch2 = create_test_batch(vec![1, 2, 3], vec!["x", "y", "z"]);

        let cursor1 = create_int32_cursor(vec![1, 2, 3], r1.new_empty());
        let cursor2 = create_int32_cursor(vec![1, 2, 3], r1.new_empty());

        let stream_fields_map = Arc::new(vec![0, 1]);
        let range1 = BatchRange::new(cursor1, batch1, 0, 0, 2, stream_fields_map.clone());
        let range2 = BatchRange::new(cursor2, batch2, 1, 0, 2, stream_fields_map);

        let batches = collect_merge_results(range1, range2, 10).await;

        assert!(!batches.is_empty());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema(), schema);
        assert_batches_eq!(
            &[
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 1  | x    |",
                "| 2  | y    |",
                "| 3  | z    |",
                "+----+------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_range_with_duplicate_values_merge() -> crate::Result<()> {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let schema = create_test_schema();
        let batch1 = create_test_batch(vec![1, 1, 2], vec!["a", "b", "c"]);
        let batch2 = create_test_batch(vec![2, 4, 4], vec!["x", "y", "z"]);

        let cursor1 = create_int32_cursor(vec![1, 1, 2], r1.new_empty());
        let cursor2 = create_int32_cursor(vec![2, 4, 4], r1.new_empty());

        let stream_fields_map = Arc::new(vec![0, 1]);
        let range1 = BatchRange::new(cursor1, batch1, 0, 0, 2, stream_fields_map.clone());
        let range2 = BatchRange::new(cursor2, batch2, 1, 0, 2, stream_fields_map);

        let batches = collect_merge_results(range1, range2, 1).await;

        assert!(!batches.is_empty());
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].schema(), schema);
        assert_batches_eq!(
            &[
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 1  | b    |",
                "| 2  | x    |",
                "| 4  | z    |",
                "+----+------+",
            ],
            &batches
        );

        Ok(())
    }
}
