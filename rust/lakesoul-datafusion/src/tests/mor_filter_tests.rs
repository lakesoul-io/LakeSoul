// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul scan filter handling through `LakeSoulTableProvider`.
//!
//! The provider separates predicates by purpose:
//!
//! * range-partition predicates prune whole work units before any file is
//!   opened, independently of the table's primary keys and of the row-level
//!   pushdown option;
//! * row-level/`FileSource` pushdown is enabled or disabled by
//!   `pushdown_filters`, and may only contain predicates that are safe below
//!   the merge (primary-key predicates for merge-on-read tables, any
//!   predicate for append-only tables);
//! * primary-key predicates additionally feed the candidate / row-locator
//!   path, which is independent of `pushdown_filters`;
//! * every other predicate is evaluated by DataFusion's `FilterExec` on the
//!   merged / scanned rows.
//!
//! The merge-on-read invariant is the classic resurrection shape: a table
//! holds `(id=1, v=1)` in an old file and `(id=1, v=100)` in a new file;
//! `WHERE v = 1` must return nothing because the old version is superseded.
//! Pushing `v = 1` below the merge would return `(1,1)`.
//!
//! The native reader pins the same contract in
//! `lakesoul-io/tests/mor_filter_pushdown_test.rs`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use tokio::runtime::Runtime;

use crate::Result;
use crate::cli::CoreArgs;
use crate::lakesoul_table::LakeSoulTable;
use crate::tests::mor_nullability_tests::write_storage_batch;
use crate::tests::{assert_batches_eq, create_table_with_file_format};

/// `part` is the range partition, `id` the primary key of the pk tables,
/// `v` a value column.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

fn batch(parts: &[i32], ids: &[i32], values: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(parts.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(values.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

/// Session whose row-level/`FileSource` pushdown is toggled. Metadata
/// partition pruning is expected to work in both modes.
fn session_with_pushdown(
    client: MetaDataClientRef,
    pushdown_filters: bool,
) -> Result<Arc<SessionContext>> {
    let mut config = crate::create_lakesoul_session_config()?;
    config.options_mut().execution.parquet.pushdown_filters = pushdown_filters;
    crate::create_lakesoul_session_ctx_with_config(client, &CoreArgs::default(), config)
}

/// Seeds two versions of `id = 1` (part 0) and `id = 2` (part 1): the first
/// upsert writes `v = 1` / `v = 2`, the second `v = 100` / `v = 200`.
///
/// The physical format is explicit: an unpinned table follows the default,
/// and these tests mean to exercise one format each.
async fn seed(
    client: MetaDataClientRef,
    table: &str,
    primary_keys: Vec<String>,
    file_format: PhysicalFormat,
) -> Result<()> {
    create_table_with_file_format(
        client,
        table,
        LakeSoulIOConfigBuilder::new()
            .with_schema(schema())
            .with_primary_keys(primary_keys)
            .with_range_partitions(vec!["part".to_string()])
            .build(),
        file_format,
    )
    .await?;
    let table = LakeSoulTable::for_name(table).await?;
    table
        .execute_upsert(batch(&[0, 1], &[1, 2], &[1, 2]))
        .await?;
    table
        .execute_upsert(batch(&[0, 1], &[1, 2], &[100, 200]))
        .await?;
    Ok(())
}

async fn seed_pk(
    client: MetaDataClientRef,
    table: &str,
    file_format: PhysicalFormat,
) -> Result<()> {
    seed(client, table, vec!["id".to_string()], file_format).await
}

async fn seed_append(
    client: MetaDataClientRef,
    table: &str,
    file_format: PhysicalFormat,
) -> Result<()> {
    seed(client, table, vec![], file_format).await
}

fn unique_table(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// The merge-on-read correctness regression: a predicate that only a
/// superseded version matches must never be evaluated before the merge, in
/// either row-level pushdown mode.
#[test]
fn test_non_key_filters_stay_above_merge() {
    Runtime::new()
        .unwrap()
        .block_on(test_non_key_filters_stay_above_merge_inner())
        .unwrap();
}

async fn test_non_key_filters_stay_above_merge_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table = unique_table("mor_filter");
    seed_pk(client.clone(), &table, PhysicalFormat::Parquet).await?;

    for pushdown_filters in [true, false] {
        let ctx = session_with_pushdown(client.clone(), pushdown_filters)?;
        let label = format!("pushdown={pushdown_filters}");

        // A predicate matching only a superseded version must not resurrect it.
        let stale = format!("SELECT * FROM {table} WHERE v = 1");
        assert_eq!(
            row_count(&run_sql(&ctx, &stale).await),
            0,
            "{label}: {stale}"
        );

        // The current version is still returned.
        let current = format!("SELECT * FROM {table} WHERE v = 100");
        assert_batches_eq(
            &format!("{label}: {current}"),
            &[
                "+------+----+-----+",
                "| part | id | v   |",
                "+------+----+-----+",
                "| 0    | 1  | 100 |",
                "+------+----+-----+",
            ],
            &run_sql(&ctx, &current).await,
        );

        // A conjunction must split: `part = 0` prunes the `part = 1` work
        // unit while `v = 1` stays above the merge and drops the superseded
        // row.
        let conjunction = format!("SELECT * FROM {table} WHERE part = 0 AND v = 1");
        assert_eq!(
            row_count(&run_sql(&ctx, &conjunction).await),
            0,
            "{label}: {conjunction}"
        );

        let kept = format!("SELECT * FROM {table} WHERE part = 0 AND v > 0");
        assert_eq!(row_count(&run_sql(&ctx, &kept).await), 1, "{label}: {kept}");

        // A primary-key predicate must resolve to the newest version in both modes.
        let pk = format!("SELECT * FROM {table} WHERE id = 1");
        assert_batches_eq(
            &format!("{label}: {pk}"),
            &[
                "+------+----+-----+",
                "| part | id | v   |",
                "+------+----+-----+",
                "| 0    | 1  | 100 |",
                "+------+----+-----+",
            ],
            &run_sql(&ctx, &pk).await,
        );
    }

    Ok(())
}

/// The PK/no-PK × `pushdown_filters` on/off matrix: a range predicate prunes
/// the work units of unrelated range partitions in every cell, independently
/// of how ranges are packed into merges and of their local lanes; row-level
/// pushdown follows the option.
#[test]
fn test_range_pruning_ignores_row_pushdown_flag() {
    Runtime::new()
        .unwrap()
        .block_on(test_range_pruning_ignores_row_pushdown_flag_inner())
        .unwrap();
}

async fn test_range_pruning_ignores_row_pushdown_flag_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pk_table = unique_table("mor_prune_pk");
    let append_table = unique_table("mor_prune_append");
    seed_pk(client.clone(), &pk_table, PhysicalFormat::Vortex).await?;
    seed_append(client.clone(), &append_table, PhysicalFormat::Vortex).await?;

    for pushdown_filters in [true, false] {
        let ctx = session_with_pushdown(client.clone(), pushdown_filters)?;
        let label = format!("pushdown={pushdown_filters}");

        // The range predicate prunes the three ranges' work units to the
        // `part = 0` one in both modes, not to a particular lane count.
        let sql = format!("SELECT * FROM {pk_table} WHERE part = 0");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 1, "{label}: {sql}");

        // Mixed predicate: pruning by `part`, the unsafe `v > 3` stays above
        // the merge.
        let sql = format!("SELECT * FROM {pk_table} WHERE part = 0 AND v > 3");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 1, "{label}: {sql}");

        // Append-only table: pruning works in both modes.
        let sql = format!("SELECT * FROM {append_table} WHERE part = 0");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 2, "{label}: {sql}");

        // Mixed predicate on the append-only table: `part = 0` prunes, the
        // ordinary predicate keeps its own role. With row-level pushdown off
        // it must be evaluated by the upper `FilterExec`, not by the file
        // source.
        let sql = format!("SELECT * FROM {append_table} WHERE part = 0 AND v > 3");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 1, "{label}: {sql}");

        // Sanity: without a range predicate there is nothing to prune.
        let sql = format!("SELECT * FROM {append_table} WHERE v > 3");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 2, "{label}: {sql}");
    }

    Ok(())
}

#[test]
fn test_mor_non_nullable_value_aggregate() {
    Runtime::new().unwrap().block_on(async {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let table = unique_table("mor_non_nullable_aggregate");
        seed_pk(client.clone(), &table, PhysicalFormat::VortexCompact)
            .await
            .unwrap();
        let storage_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, true),
        ]));
        let update = RecordBatch::try_new(
            storage_schema,
            batch(&[0, 1], &[1, 2], &[100, 200]).columns().to_vec(),
        )
        .unwrap();
        LakeSoulTable::for_name(&table)
            .await
            .unwrap()
            .execute_upsert(update)
            .await
            .unwrap();
        let ctx = session_with_pushdown(client, true).unwrap();
        let df = ctx
            .sql(&format!("SELECT avg(v) AS mean FROM {table}"))
            .await
            .unwrap();
        assert!(df.schema().field(0).is_nullable());
        let batches = df.collect().await.unwrap();
        assert!(batches[0].schema().field(0).is_nullable());
        let mean = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(
            mean.iter().eq([Some(150.0)]),
            "unexpected AVG values: {mean:?}"
        );
    });
}

#[test]
fn test_mor_partial_update_count_respects_nulls() {
    Runtime::new().unwrap().block_on(async {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let table = unique_table("mor_partial_nullability");
        seed_pk(client.clone(), &table, PhysicalFormat::VortexCompact)
            .await
            .unwrap();
        let partial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("part", DataType::Int32, false),
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![0, 0])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, 3])) as ArrayRef,
            ],
        )
        .unwrap();
        // `execute_upsert` enforces the stored NOT NULL on `v` and rejects a
        // contribution without it; commit the sparse file natively, as data
        // written before `v` existed or by another engine would be.
        let handle = LakeSoulTable::for_name(&table).await.unwrap();
        write_storage_batch(
            client.clone(),
            &handle,
            PhysicalFormat::VortexCompact,
            partial,
        )
        .await
        .unwrap();
        let ctx = session_with_pushdown(client, true).unwrap();
        let df = ctx
            .sql(&format!("SELECT id, v FROM {table} ORDER BY id"))
            .await
            .unwrap();
        assert!(
            df.schema()
                .field_with_unqualified_name("v")
                .unwrap()
                .is_nullable()
        );
        let batches = df.collect().await.unwrap();
        assert_batches_eq(
            &table,
            &[
                "+----+-----+",
                "| id | v   |",
                "+----+-----+",
                "| 1  | 100 |",
                "| 2  | 200 |",
                "| 3  |     |",
                "+----+-----+",
            ],
            &batches,
        );
        for batch in &batches {
            assert!(batch.schema().field_with_name("v").unwrap().is_nullable());
        }
        let count =
            run_sql(&ctx, &format!("SELECT count(v) AS present FROM {table}")).await;
        let present = count[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(present.value(0), 2);
        assert!(!count[0].schema().field(0).is_nullable());
    });
}
