// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! A 1:N lookup step: one base row matches several source rows, the MV merge
//! key carries the matched row's identity column, and a missing match leaves
//! one NULL-padded row.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, JoinKeyExprs,
    LookupChainColumn, LookupChainStep, lookup_chain_mv_schema_for,
    lookup_chain_step_id_column,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

async fn create_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                name.to_string(),
                table_path(dir, name),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

async fn create_bucketed_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    let mut options =
        IvmTableOptions::new(name.to_string(), table_path(dir, name), source_schema())
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_bucket_columns(vec!["k".to_string()]);
    options.hash_bucket_num = "4".to_string();
    runtime.create_table(options).await.unwrap()
}

type ChainRow = (i64, i64, Option<i64>, Option<i64>);

/// The logical `(k, v, bv, cv)` rows of the chain MV.
async fn read_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<ChainRow> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select k, v, bv, cv from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bv = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cv = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                values.value(row),
                (!bv.is_null(row)).then(|| bv.value(row)),
                (!cv.is_null(row)).then(|| cv.value(row)),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

#[test_log::test(tokio::test)]
async fn one_to_many_step_matches_a_full_recompute() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let base = create_source(&runtime, &dir, &format!("ivm_1n_base_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_1n_dim_{suffix}")).await;
    let lookup = create_source(&runtime, &dir, &format!("ivm_1n_lookup_{suffix}")).await;

    let steps = vec![
        LookupChainStep {
            source: 1,
            left: true,
            keys: vec!["g".to_string()],
            right_keys: vec!["g".to_string()],
            key_sources: Vec::new(),
            unique: false,
            key_exprs: JoinKeyExprs::default(),
        },
        LookupChainStep {
            source: 2,
            left: true,
            keys: vec!["k".to_string()],
            right_keys: vec!["k".to_string()],
            key_sources: vec![1],
            unique: true,
            key_exprs: JoinKeyExprs::default(),
        },
    ];
    let columns = vec![
        LookupChainColumn {
            source: 0,
            column: "k".to_string(),
            name: "k".to_string(),
        },
        LookupChainColumn {
            source: 0,
            column: "v".to_string(),
            name: "v".to_string(),
        },
        LookupChainColumn {
            source: 1,
            column: "v".to_string(),
            name: "bv".to_string(),
        },
        LookupChainColumn {
            source: 2,
            column: "v".to_string(),
            name: "cv".to_string(),
        },
    ];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_1n_mv_{suffix}"),
                table_path(&dir, "mv"),
                lookup_chain_mv_schema_for(
                    &[
                        base.schema.clone(),
                        dim.schema.clone(),
                        lookup.schema.clone(),
                    ],
                    &steps,
                    &columns,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string(), lookup_chain_step_id_column(1)]),
        )
        .await
        .unwrap();

    base.append_batch(
        runtime.client(),
        source_batch(&[(1, "x", 10, "insert"), (2, "y", 20, "insert")]),
    )
    .await
    .unwrap();
    dim.append_batch(
        runtime.client(),
        source_batch(&[
            (100, "x", 1, "insert"),
            (101, "x", 2, "insert"),
            (102, "y", 3, "insert"),
        ]),
    )
    .await
    .unwrap();
    lookup
        .append_batch(
            runtime.client(),
            source_batch(&[
                (100, "x", 1000, "insert"),
                (101, "x", 1001, "insert"),
                (102, "y", 1002, "insert"),
            ]),
        )
        .await
        .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_1n_mv_{suffix} \
         SELECT a.k, a.v, b.v AS bv, c.v AS cv \
         FROM ivm_1n_base_{suffix} a \
         LEFT JOIN ivm_1n_dim_{suffix} b ON b.g = a.g \
         LEFT JOIN ivm_1n_lookup_{suffix} c ON c.k = b.k"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![
            (1, 10, Some(1), Some(1000)),
            (1, 10, Some(2), Some(1001)),
            (2, 20, Some(3), Some(1002)),
        ]
    );

    // Deleting one match keeps the other; deleting the last match leaves one
    // NULL-padded row.
    dim.append_batch(
        executor.runtime().client(),
        source_batch(&[(100, "x", 1, "delete"), (101, "x", 2, "delete")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![(1, 10, None, None), (2, 20, Some(3), Some(1002))]
    );

    // Moving the join key changes the match set on both sides.
    dim.append_batch(
        executor.runtime().client(),
        source_batch(&[(102, "x", 3, "update"), (103, "x", 4, "insert")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![
            (1, 10, Some(3), Some(1002)),
            (1, 10, Some(4), None),
            (2, 20, None, None),
        ]
    );

    // A base delete removes its rows; a rebuild agrees with them.
    base.append_batch(
        executor.runtime().client(),
        source_batch(&[(2, "y", 20, "delete")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    let after_delete = vec![(1, 10, Some(3), Some(1002)), (1, 10, Some(4), None)];
    assert_eq!(read_rows(executor.runtime(), &mv).await, after_delete);

    executor
        .execute(&format!(
            "INSERT OVERWRITE ivm_1n_mv_{suffix} \
             SELECT a.k, a.v, b.v AS bv, c.v AS cv \
             FROM ivm_1n_base_{suffix} a \
             LEFT JOIN ivm_1n_dim_{suffix} b ON b.g = a.g \
             LEFT JOIN ivm_1n_lookup_{suffix} c ON c.k = b.k"
        ))
        .await
        .unwrap();
    assert_eq!(read_rows(executor.runtime(), &mv).await, after_delete);
}

/// An INNER 1:N step drops base rows without a match, and a later LEFT step
/// still pads its own missing rows.
#[test_log::test(tokio::test)]
async fn inner_one_to_many_step_drops_unmatched_base_rows() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let base = create_source(&runtime, &dir, &format!("ivm_inner_base_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_inner_dim_{suffix}")).await;
    let lookup =
        create_source(&runtime, &dir, &format!("ivm_inner_lookup_{suffix}")).await;

    let steps = vec![
        LookupChainStep {
            source: 1,
            left: false,
            keys: vec!["g".to_string()],
            right_keys: vec!["g".to_string()],
            key_sources: Vec::new(),
            unique: false,
            key_exprs: JoinKeyExprs::default(),
        },
        LookupChainStep {
            source: 2,
            left: true,
            keys: vec!["k".to_string()],
            right_keys: vec!["k".to_string()],
            key_sources: vec![1],
            unique: true,
            key_exprs: JoinKeyExprs::default(),
        },
    ];
    let columns = vec![
        LookupChainColumn {
            source: 0,
            column: "k".to_string(),
            name: "k".to_string(),
        },
        LookupChainColumn {
            source: 0,
            column: "v".to_string(),
            name: "v".to_string(),
        },
        LookupChainColumn {
            source: 1,
            column: "v".to_string(),
            name: "bv".to_string(),
        },
        LookupChainColumn {
            source: 2,
            column: "v".to_string(),
            name: "cv".to_string(),
        },
    ];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_inner_mv_{suffix}"),
                table_path(&dir, "mv"),
                lookup_chain_mv_schema_for(
                    &[
                        base.schema.clone(),
                        dim.schema.clone(),
                        lookup.schema.clone(),
                    ],
                    &steps,
                    &columns,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string(), lookup_chain_step_id_column(1)]),
        )
        .await
        .unwrap();

    base.append_batch(
        runtime.client(),
        source_batch(&[(1, "x", 10, "insert"), (2, "y", 20, "insert")]),
    )
    .await
    .unwrap();
    dim.append_batch(runtime.client(), source_batch(&[(100, "x", 1, "insert")]))
        .await
        .unwrap();
    lookup
        .append_batch(
            runtime.client(),
            source_batch(&[(100, "x", 1000, "insert")]),
        )
        .await
        .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_inner_mv_{suffix} \
         SELECT a.k, a.v, b.v AS bv, c.v AS cv \
         FROM ivm_inner_base_{suffix} a \
         JOIN ivm_inner_dim_{suffix} b ON b.g = a.g \
         LEFT JOIN ivm_inner_lookup_{suffix} c ON c.k = b.k"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![(1, 10, Some(1), Some(1000))]
    );

    // A second dim row adds a chain row whose lookup value is missing.
    dim.append_batch(
        executor.runtime().client(),
        source_batch(&[(103, "x", 4, "insert")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![(1, 10, Some(1), Some(1000)), (1, 10, Some(4), None)]
    );

    // Removing every match drops the base row entirely.
    dim.append_batch(
        executor.runtime().client(),
        source_batch(&[(100, "x", 1, "delete"), (103, "x", 4, "delete")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert!(read_rows(executor.runtime(), &mv).await.is_empty());

    // A match for the other base row brings it back.
    dim.append_batch(
        executor.runtime().client(),
        source_batch(&[(104, "y", 5, "insert")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![(2, 20, Some(5), None)]
    );
}

/// A unique chain whose dimension keys are the base keys can refresh a
/// dimension-only (or mixed base+dimension) window without scanning the base
/// table or rebuilding the whole chain.
#[test_log::test(tokio::test)]
async fn base_keyed_unique_chain_refreshes_dimensions_incrementally() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let base =
        create_bucketed_source(&runtime, &dir, &format!("ivm_bk_base_{suffix}")).await;
    let dim1 =
        create_bucketed_source(&runtime, &dir, &format!("ivm_bk_dim1_{suffix}")).await;
    let dim2 =
        create_bucketed_source(&runtime, &dir, &format!("ivm_bk_dim2_{suffix}")).await;

    let steps = vec![
        LookupChainStep {
            source: 1,
            left: true,
            keys: vec!["k".to_string()],
            right_keys: vec!["k".to_string()],
            key_sources: Vec::new(),
            unique: true,
            key_exprs: JoinKeyExprs::default(),
        },
        LookupChainStep {
            source: 2,
            left: true,
            keys: vec!["k".to_string()],
            right_keys: vec!["k".to_string()],
            key_sources: Vec::new(),
            unique: true,
            key_exprs: JoinKeyExprs::default(),
        },
    ];
    let columns = vec![
        LookupChainColumn {
            source: 0,
            column: "k".to_string(),
            name: "k".to_string(),
        },
        LookupChainColumn {
            source: 0,
            column: "v".to_string(),
            name: "v".to_string(),
        },
        LookupChainColumn {
            source: 1,
            column: "v".to_string(),
            name: "bv".to_string(),
        },
        LookupChainColumn {
            source: 2,
            column: "v".to_string(),
            name: "cv".to_string(),
        },
    ];
    let mut mv_options = IvmTableOptions::new(
        format!("ivm_bk_mv_{suffix}"),
        table_path(&dir, "mv"),
        lookup_chain_mv_schema_for(
            &[
                base.schema.clone(),
                dim1.schema.clone(),
                dim2.schema.clone(),
            ],
            &steps,
            &columns,
        )
        .unwrap(),
    )
    .with_primary_keys(vec!["k".to_string()])
    .with_bucket_columns(vec!["k".to_string()]);
    mv_options.hash_bucket_num = "4".to_string();
    let mv = runtime.create_table(mv_options).await.unwrap();

    base.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
    )
    .await
    .unwrap();
    dim1.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 100, "insert"), (2, "b", 200, "insert")]),
    )
    .await
    .unwrap();
    dim2.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 1000, "insert"), (2, "b", 2000, "insert")]),
    )
    .await
    .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_bk_mv_{suffix} \
         SELECT a.k, a.v, b.v AS bv, c.v AS cv \
         FROM ivm_bk_base_{suffix} a \
         LEFT JOIN ivm_bk_dim1_{suffix} b ON b.k = a.k \
         LEFT JOIN ivm_bk_dim2_{suffix} c ON c.k = a.k"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![
            (1, 10, Some(100), Some(1000)),
            (2, 20, Some(200), Some(2000)),
        ]
    );

    // Dimension-only windows keep the affected base keys and look up only
    // those rows.
    dim1.append_batch(
        executor.runtime().client(),
        source_batch(&[(1, "a", 101, "insert")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![
            (1, 10, Some(101), Some(1000)),
            (2, 20, Some(200), Some(2000)),
        ]
    );
    dim2.append_batch(
        executor.runtime().client(),
        source_batch(&[(2, "b", 2001, "insert")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![
            (1, 10, Some(101), Some(1000)),
            (2, 20, Some(200), Some(2001)),
        ]
    );

    // A mixed base+dimension window must include both key sets.
    base.append_batch(
        executor.runtime().client(),
        source_batch(&[(1, "a", 11, "insert")]),
    )
    .await
    .unwrap();
    dim1.append_batch(
        executor.runtime().client(),
        source_batch(&[(1, "a", 102, "insert")]),
    )
    .await
    .unwrap();
    dim2.append_batch(
        executor.runtime().client(),
        source_batch(&[(1, "a", 1002, "insert")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![
            (1, 11, Some(102), Some(1002)),
            (2, 20, Some(200), Some(2001)),
        ]
    );

    // A dimension delete updates the one affected base row.
    dim2.append_batch(
        executor.runtime().client(),
        source_batch(&[(2, "b", 2001, "delete")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(executor.runtime(), &mv).await,
        vec![(1, 11, Some(102), Some(1002)), (2, 20, Some(200), None)]
    );

    let incremental = read_rows(executor.runtime(), &mv).await;
    executor
        .execute(&format!(
            "INSERT OVERWRITE ivm_bk_mv_{suffix} \
             SELECT a.k, a.v, b.v AS bv, c.v AS cv \
             FROM ivm_bk_base_{suffix} a \
             LEFT JOIN ivm_bk_dim1_{suffix} b ON b.k = a.k \
             LEFT JOIN ivm_bk_dim2_{suffix} c ON c.k = a.k"
        ))
        .await
        .unwrap();
    assert_eq!(read_rows(executor.runtime(), &mv).await, incremental);
}
