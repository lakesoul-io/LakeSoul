// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use super::super::StartupState;

pub(super) async fn seed(state: &mut StartupState) -> Result<()> {
    state.create_storage_table(
        "partition_items",
        "part INTEGER NOT NULL, id INTEGER NOT NULL, val INTEGER NOT NULL, PRIMARY KEY (id)",
        &["part"], None,
    ).await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, false),
    ]));
    for (ids, values) in [
        (vec![1, 2, 1, 2], vec![10, 20, 100, 200]),
        (vec![1, 3, 1, 3], vec![11, 30, 101, 300]),
    ] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![0, 0, 1, 1])),
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )?;
        state.write_storage_batch("partition_items", batch).await?;
    }
    state.prove_storage_table("partition_items", 2, 2).await?;
    state.reference_storage_table(
        "partition_items", "part INTEGER NOT NULL, id INTEGER NOT NULL, val INTEGER NOT NULL, PRIMARY KEY (part, id)",
        "(0,1,11),(0,2,20),(0,3,30),(1,1,101),(1,2,200),(1,3,300)",
    ).await
}
