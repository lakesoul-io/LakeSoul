use std::sync::Arc;

use arrow::array::{Decimal128Builder, Int64Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rand::{Rng, SeedableRng};

// Generate `num_batches` RecordBatches mimicking TPC-H Q20's partial aggregate result:
// GROUP BY (l_partkey, l_suppkey) -> SUM(l_quantity)
pub fn create_q20_like_batches(
    num_batches: usize,
    num_rows_per_batch: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let seed = 20;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut batches = Vec::with_capacity(num_batches);

    let mut current_partkey = 400000_i64;

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("sum_l_quantity", DataType::Decimal128(25, 2), true),
    ]));

    for _ in 0..num_batches {
        let mut partkey_builder = Int64Builder::new();
        let mut suppkey_builder = Int64Builder::new();
        let mut quantity_builder = Decimal128Builder::new()
            .with_precision_and_scale(25, 2)
            .unwrap();

        for _ in 0..num_rows_per_batch {
            // Occasionally skip a few partkey values to simulate sparsity
            let partkey_jump = if rng.random_bool(0.03) {
                rng.random_range(2..6)
            } else {
                1
            };
            current_partkey += partkey_jump;

            let suppkey = rng.random_range(10_000..99_999);
            let quantity = rng.random_range(500..20_000) as i128;

            partkey_builder.append_value(current_partkey);
            suppkey_builder.append_value(suppkey);
            quantity_builder.append_value(quantity);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(partkey_builder.finish()),
                Arc::new(suppkey_builder.finish()),
                Arc::new(quantity_builder.finish()),
            ],
        )
        .unwrap();

        batches.push(batch);
    }

    (schema, batches)
}
