use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::v2::batch_range::BatchRange;
use arrow::compute::interleave;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use tokio::sync::mpsc::Sender;

pub struct LoserTreeRangeMerge<'a, C: CursorValues> {
    ranges: Vec<&'a mut BatchRange<C>>,

    loser_tree: Vec<usize>,

    loser_tree_has_updated: bool,

    in_progress_row: Vec<InProgressRow>,

    target_batch_size: usize,

    schema: SchemaRef,
}

struct InProgressRow {
    range_idx: usize,
    row_idx: usize,
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
            in_progress_row: Vec::with_capacity(target_batch_size),
            target_batch_size,
            schema,
        }
    }

    pub async fn merge(
        &mut self,
        tx: Sender<crate::Result<RecordBatch>>,
    ) -> crate::Result<Sender<crate::Result<RecordBatch>>> {
        self.init_loser_tree();
        loop {
            let winner = self.loser_tree[0];
            if !self.ranges[winner].has_more_rows() {
                break;
            }
            // if key is equal, we replace the last row
            if let Some(last_row) = self.in_progress_row.last_mut() {
                if C::eq(
                    &self.ranges[last_row.range_idx].cursor(),
                    last_row.row_idx,
                    &self.ranges[winner].cursor(),
                    self.ranges[winner].begin_row(),
                ) {
                    // replace last row and continue
                    *last_row = InProgressRow {
                        range_idx: winner,
                        row_idx: self.ranges[winner].begin_row(),
                    };
                } else {
                    // this is not first row and not duplicate, first try to output previous rows
                    if self.in_progress_row.len() >= self.target_batch_size {
                        match self.build_record_batch() {
                            Ok(batch) => tx.send(Ok(batch)).await?,
                            Err(e) => {
                                tx.send(Err(e)).await?;
                                return Ok(tx);
                            }
                        }
                    }
                    self.in_progress_row.push(InProgressRow {
                        range_idx: winner,
                        row_idx: self.ranges[winner].begin_row(),
                    });
                }
            } else {
                // first row, we need to continue to see if there's duplicate
                self.in_progress_row.push(InProgressRow {
                    range_idx: winner,
                    row_idx: self.ranges[winner].begin_row(),
                });
            }
            self.ranges[winner].advance();
            self.update_loser_tree();
        }
        // check end
        if !self.in_progress_row.is_empty() {
            match self.build_record_batch() {
                Ok(batch) => tx.send(Ok(batch)).await?,
                Err(e) => {
                    tx.send(Err(e)).await?;
                    return Ok(tx);
                }
            }
        }
        Ok(tx)
    }

    fn build_record_batch(&mut self) -> crate::Result<RecordBatch> {
        let mut indices = Vec::with_capacity(self.in_progress_row.len());
        for row in self.in_progress_row.iter() {
            indices.push((row.range_idx, row.row_idx));
        }
        let column_num = self.ranges[0].batch().num_columns();
        let mut columns = Vec::with_capacity(column_num);
        let result = (0..column_num)
            .into_iter()
            .map(|i| {
                self.ranges.iter().for_each(|range| {
                    columns.push(range.batch().column(i).as_ref());
                });
                let col = interleave(columns.as_slice(), indices.as_slice());
                columns.clear();
                col
            })
            .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;
        self.in_progress_row.clear();
        Ok(RecordBatch::try_new(self.schema.clone(), result)?)
    }

    fn init_loser_tree(&mut self) {
        // Init loser tree
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
                        // None means the stream is exhausted, always mark None as loser
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

    /// Update the winner of the loser tree.
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

    // Helper function to create ArrayValues for testing
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

    // Test the basic functionality of the loser tree merger
    #[tokio::test]
    async fn test_basic_merge() -> crate::Result<()> {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let schema = create_test_schema();
        let batch1 = create_test_batch(vec![1, 3, 5], vec!["a", "c", "e"]);
        let batch2 = create_test_batch(vec![2, 4, 6], vec!["b", "d", "f"]);

        // Create cursors for each batch (using first column as sort key)
        let cursor1 = create_int32_cursor(vec![1, 3, 5], r1.new_empty());
        let cursor2 = create_int32_cursor(vec![2, 4, 6], r1.new_empty());

        // Create BatchRange instances
        let mut range1 = BatchRange::new(cursor1, batch1, 0, 0, 2); // Process all 3 rows
        let mut range2 = BatchRange::new(cursor2, batch2, 1, 0, 2); // Process all 3 rows

        // Create mutable references to ranges
        let ranges = vec![&mut range1, &mut range2];

        // Create the merger
        let mut merger = LoserTreeRangeMerge::new(schema.clone(), ranges, 10);

        // Create channel for receiving batches
        let (tx, mut rx) = mpsc::channel(10);

        // Run the merge operation
        merger.merge(tx).await?;

        // Collect results
        let mut batches = Vec::new();
        while let Some(result) = rx.recv().await {
            if let Ok(batch) = result {
                batches.push(batch);
            }
        }

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

    // Test with equal values to check the equality handling
    #[tokio::test]
    async fn test_equal_values_merge() -> crate::Result<()> {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let schema = create_test_schema();
        let batch1 = create_test_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let batch2 = create_test_batch(vec![1, 2, 3], vec!["x", "y", "z"]);

        // Create cursors for each batch
        let cursor1 = create_int32_cursor(vec![1, 2, 3], r1.new_empty());
        let cursor2 = create_int32_cursor(vec![1, 2, 3], r1.new_empty());

        // Create BatchRange instances
        let mut range1 = BatchRange::new(cursor1, batch1, 0, 0, 2);
        let mut range2 = BatchRange::new(cursor2, batch2, 1, 0, 2);

        let ranges = vec![&mut range1, &mut range2];

        let mut merger = LoserTreeRangeMerge::new(schema.clone(), ranges, 10);

        let (tx, mut rx) = mpsc::channel(10);

        merger.merge(tx).await?;

        let mut batches = Vec::new();
        while let Some(result) = rx.recv().await {
            if let Ok(batch) = result {
                batches.push(batch);
            }
        }

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

    // Test with duplicate values
    #[tokio::test]
    async fn test_range_with_duplicate_values_merge() -> crate::Result<()> {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let r1 = MemoryConsumer::new("r1").register(&pool);
        let schema = create_test_schema();
        let batch1 = create_test_batch(vec![1, 1, 2], vec!["a", "b", "c"]);
        let batch2 = create_test_batch(vec![2, 4, 4], vec!["x", "y", "z"]);

        // Create cursors for each batch
        let cursor1 = create_int32_cursor(vec![1, 1, 2], r1.new_empty());
        let cursor2 = create_int32_cursor(vec![2, 4, 4], r1.new_empty());

        // Create BatchRange instances
        let mut range1 = BatchRange::new(cursor1, batch1, 0, 0, 2);
        let mut range2 = BatchRange::new(cursor2, batch2, 1, 0, 2);

        let ranges = vec![&mut range1, &mut range2];

        let mut merger = LoserTreeRangeMerge::new(schema.clone(), ranges, 1);

        let (tx, mut rx) = mpsc::channel(10);

        merger.merge(tx).await?;

        let mut batches = Vec::new();
        while let Some(result) = rx.recv().await {
            if let Ok(batch) = result {
                batches.push(batch);
            }
        }

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
