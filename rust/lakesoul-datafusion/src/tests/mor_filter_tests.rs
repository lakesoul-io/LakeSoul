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
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::prelude::{SessionContext, col, lit};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::MergeParquetExec;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use tokio::runtime::Runtime;

use crate::Result;
use crate::catalog::LakeSoulProviderOptions;
use crate::cli::CoreArgs;
use crate::lakesoul_table::LakeSoulTable;
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

/// Counts the merge-on-read work units of a plan.
fn merge_work_units(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if node.is::<MergeParquetExec>() {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

/// Counts the scan leaves of a plan. An append-only work unit is a plain
/// file scan, so one leaf is one planned work unit.
fn scan_leaves(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if node.is::<DataSourceExec>() {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

/// File-source predicates of every scan leaf in a plan.
fn leaf_filters(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut filters = vec![];
    plan.apply(|node| {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
            && let Some(filter) = config.file_source().filter()
        {
            filters.push(filter.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    filters
}

/// Whether the plan evaluates something in a `FilterExec` above the scan.
fn has_filter_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.exists(|node| Ok(node.is::<FilterExec>())).unwrap()
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

async fn plan_of(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
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
    // The primary-key predicate reaches the file source on Parquet; a vortex
    // table is served by the row locator instead (`pk_locator_tests`), so the
    // file-source assertion below inspects the Parquet path.
    seed_pk(client.clone(), &table, PhysicalFormat::Parquet).await?;

    for pushdown_filters in [true, false] {
        let ctx = session_with_pushdown(client.clone(), pushdown_filters)?;
        let label = format!("pushdown={pushdown_filters}");

        // A predicate only the superseded version matches must not resurrect
        // it. The value predicate is not a partition predicate, so both work
        // units are planned and it stays above the merge.
        let stale = format!("SELECT * FROM {table} WHERE v = 1");
        assert_eq!(
            row_count(&run_sql(&ctx, &stale).await),
            0,
            "{label}: {stale}"
        );
        let stale_plan = plan_of(&ctx, &stale).await;
        assert_eq!(merge_work_units(&stale_plan), 2, "{label}: {stale}");
        assert!(has_filter_exec(&stale_plan), "{label}: {stale}");
        assert!(
            leaf_filters(&stale_plan)
                .iter()
                .all(|filter| !filter.contains("v@")),
            "{label}: non-key predicate pushed below merge: {:?}",
            leaf_filters(&stale_plan)
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
        let conjunction_plan = plan_of(&ctx, &conjunction).await;
        assert_eq!(
            merge_work_units(&conjunction_plan),
            1,
            "{label}: {conjunction}"
        );
        assert!(
            leaf_filters(&conjunction_plan)
                .iter()
                .all(|filter| !filter.contains("v@")),
            "{label}: value predicate pushed below merge: {:?}",
            leaf_filters(&conjunction_plan)
        );

        let kept = format!("SELECT * FROM {table} WHERE part = 0 AND v > 0");
        assert_eq!(row_count(&run_sql(&ctx, &kept).await), 1, "{label}: {kept}");

        // The primary-key predicate resolves to the newest version. It keeps
        // its file-source pushdown only while row-level pushdown is enabled;
        // the row locator is a separate path and stays available either way.
        let pk = format!("SELECT * FROM {table} WHERE id = 1");
        let pk_leaf_filters = leaf_filters(&plan_of(&ctx, &pk).await);
        if pushdown_filters {
            assert!(
                pk_leaf_filters.iter().any(|filter| filter.contains("id@")),
                "{label}: primary-key predicate must reach the file source: \
                 {pk_leaf_filters:?}"
            );
        } else {
            assert!(
                pk_leaf_filters.is_empty(),
                "{label}: row-level pushdown must be off: {pk_leaf_filters:?}"
            );
        }
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
/// unrelated work units in every cell, while row-level pushdown follows the
/// option.
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

        // Merge-on-read table: the range predicate collapses the three work
        // units to the `part = 0` one in both modes.
        let sql = format!("SELECT * FROM {pk_table} WHERE part = 0");
        let plan = plan_of(&ctx, &sql).await;
        assert_eq!(merge_work_units(&plan), 1, "{label}: {sql}");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 1, "{label}: {sql}");

        // Mixed predicate: pruning by `part`, the unsafe `v > 3` stays above
        // the merge.
        let sql = format!("SELECT * FROM {pk_table} WHERE part = 0 AND v > 3");
        let plan = plan_of(&ctx, &sql).await;
        assert_eq!(merge_work_units(&plan), 1, "{label}: {sql}");
        assert!(has_filter_exec(&plan), "{label}: {sql}");
        assert!(
            leaf_filters(&plan)
                .iter()
                .all(|filter| !filter.contains("v@")),
            "{label}: value predicate pushed below merge: {:?}",
            leaf_filters(&plan)
        );
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 1, "{label}: {sql}");

        // Append-only table: pruning works in both modes.
        let sql = format!("SELECT * FROM {append_table} WHERE part = 0");
        let plan = plan_of(&ctx, &sql).await;
        assert_eq!(scan_leaves(&plan), 1, "{label}: {sql}");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 2, "{label}: {sql}");

        // Mixed predicate on the append-only table: `part = 0` prunes, the
        // ordinary predicate keeps its own role. With row-level pushdown off
        // it must be evaluated by the upper `FilterExec`, not by the file
        // source.
        let sql = format!("SELECT * FROM {append_table} WHERE part = 0 AND v > 3");
        let plan = plan_of(&ctx, &sql).await;
        assert_eq!(scan_leaves(&plan), 1, "{label}: {sql}");
        assert_eq!(row_count(&run_sql(&ctx, &sql).await), 1, "{label}: {sql}");
        if !pushdown_filters {
            assert!(has_filter_exec(&plan), "{label}: {sql}");
        }

        // Sanity: without a range predicate there is nothing to prune.
        let sql = format!("SELECT * FROM {append_table} WHERE v > 3");
        let plan = plan_of(&ctx, &sql).await;
        assert_eq!(scan_leaves(&plan), 2, "{label}: {sql}");
    }

    Ok(())
}

/// The provider itself must not forward an ordinary predicate to the
/// `FileSource` when row-level pushdown is disabled; when it is enabled, the
/// predicate must reach the source. This inspects the plan `scan()` returns,
/// before DataFusion's physical optimizer can re-attach predicates for
/// statistics pruning.
#[test]
fn test_file_source_pushdown_is_gated_by_the_option() {
    Runtime::new()
        .unwrap()
        .block_on(test_file_source_pushdown_is_gated_by_the_option_inner())
        .unwrap();
}

async fn test_file_source_pushdown_is_gated_by_the_option_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table = unique_table("mor_pushdown");
    seed_append(client.clone(), &table, PhysicalFormat::Vortex).await?;
    let table = LakeSoulTable::for_name(&table).await?;

    let value_filter = col("v").gt(lit(3i32));
    let range_filter = col("part").eq(lit(0i32));

    for pushdown_filters in [true, false] {
        let ctx = session_with_pushdown(client.clone(), pushdown_filters)?;
        let label = format!("pushdown={pushdown_filters}");
        let provider = table
            .as_sink_provider(LakeSoulProviderOptions::from_session(&ctx.state()))
            .await?;

        let plan = provider
            .scan(
                &ctx.state(),
                None,
                std::slice::from_ref(&value_filter),
                None,
            )
            .await?;
        let filters = leaf_filters(&plan);
        if pushdown_filters {
            assert!(
                filters.iter().any(|filter| filter.contains("v@")),
                "{label}: ordinary predicate must reach the file source: {filters:?}"
            );
        } else {
            assert!(
                filters.is_empty(),
                "{label}: ordinary predicate forwarded to the file source: {filters:?}"
            );
        }

        // The range predicate prunes in both modes, whether or not it is
        // also row-pushed.
        let plan = provider
            .scan(
                &ctx.state(),
                None,
                std::slice::from_ref(&range_filter),
                None,
            )
            .await?;
        assert_eq!(scan_leaves(&plan), 1, "{label}: range predicate must prune");

        // A direct caller may hand the whole conjunction to `scan`; the
        // provider must split it: `part = 0` prunes, and only an enabled
        // row-level pushdown may forward the predicates to the file source.
        let conjunction = range_filter.clone().and(value_filter.clone());
        let plan = provider
            .scan(&ctx.state(), None, &[conjunction], None)
            .await?;
        assert_eq!(
            scan_leaves(&plan),
            1,
            "{label}: conjunction must still prune"
        );
        let filters = leaf_filters(&plan);
        if pushdown_filters {
            assert!(
                filters.iter().any(|filter| filter.contains("v@")),
                "{label}: enabled row-level pushdown must forward the conjunction: \
                 {filters:?}"
            );
        } else {
            assert!(
                filters.is_empty(),
                "{label}: conjunction forwarded to the file source: {filters:?}"
            );
        }
    }

    Ok(())
}

/// The provider splits a conjunction per conjunct before consulting the file
/// source: the range conjunct prunes the leaf away, and the file-column
/// conjunct must reach the source on its own even though the partition column
/// beside it has no file-schema entry there.
#[test]
fn test_file_source_pushdown_accepts_each_conjunct_separately() {
    Runtime::new()
        .unwrap()
        .block_on(test_file_source_pushdown_accepts_each_conjunct_separately_inner())
        .unwrap();
}

async fn test_file_source_pushdown_accepts_each_conjunct_separately_inner() -> Result<()>
{
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table = unique_table("mor_conjunct_pushdown");
    seed_append(client.clone(), &table, PhysicalFormat::Vortex).await?;
    let table = LakeSoulTable::for_name(&table).await?;

    let value_filter = col("v").gt(lit(3i32));
    let range_filter = col("part").eq(lit(0i32));

    for pushdown_filters in [true, false] {
        let ctx = session_with_pushdown(client.clone(), pushdown_filters)?;
        let label = format!("pushdown={pushdown_filters}");
        let provider = table
            .as_sink_provider(LakeSoulProviderOptions::from_session(&ctx.state()))
            .await?;

        let conjunction = range_filter.clone().and(value_filter.clone());
        let plan = provider
            .scan(&ctx.state(), None, &[conjunction], None)
            .await?;
        assert_eq!(scan_leaves(&plan), 1, "{label}: range conjunct must prune");
        let filters = leaf_filters(&plan);
        if pushdown_filters {
            assert!(
                filters.iter().any(|filter| filter.contains("v@")),
                "{label}: the file-column conjunct must reach the file source on \
                 its own: {filters:?}"
            );
        } else {
            assert!(
                filters.is_empty(),
                "{label}: conjunct forwarded to the file source: {filters:?}"
            );
        }
    }

    Ok(())
}
