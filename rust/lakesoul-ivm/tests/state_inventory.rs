// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Tests for the read-only view-chain state inventory.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    DropViewOptions, InventoryOptions, IvmRuntime, IvmSqlExecutor, IvmTable,
    IvmTableOptions, JoinOutputColumn, JoinSide, StateInventory, min_max_mv_schema,
    multi_agg_mv_schema_for, sum_count_mv_schema_for, wide_keyed_join_view_schema_for,
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
            IvmTableOptions::new(name, table_path(dir, name), source_schema())
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

/// The sum-count MV schema of an aggregate over a (widened) join MV.
fn aggregate_over_join_mv_schema() -> SchemaRef {
    let join = wide_keyed_join_view_schema_for(
        &source_schema(),
        &source_schema(),
        &["k".to_string()],
        &["k".to_string()],
        &["k".to_string()],
        &[
            JoinOutputColumn {
                side: JoinSide::Left,
                column: "g".to_string(),
                name: "g".to_string(),
            },
            JoinOutputColumn {
                side: JoinSide::Right,
                column: "v".to_string(),
                name: "v".to_string(),
            },
        ],
    )
    .unwrap();
    sum_count_mv_schema_for(&join, &["g".to_string()], Some("v")).unwrap()
}

const STATEMENT: &str = "INSERT INTO {mv} SELECT f.g, SUM(d.v) AS sum_v, \
                         COUNT(*) AS count_v FROM {f} f JOIN {d} d \
                         ON f.k = d.k GROUP BY f.g";

#[test_log::test(tokio::test)]
async fn inventory_lists_derived_steps_and_their_footprints() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_inv_f_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_inv_d_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_inv_mv_{suffix}"),
                table_path(&dir, "mv"),
                aggregate_over_join_mv_schema(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = STATEMENT
        .replace("{mv}", &mv.table_name)
        .replace("{f}", &fact.table_name)
        .replace("{d}", &dim.table_name);

    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 20, "insert"),
            (3, "b", 5, "insert"),
        ]),
    )
    .await
    .unwrap();
    dim.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "x", 7, "insert"),
            (2, "y", 100, "insert"),
            (3, "z", 8, "insert"),
        ]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();

    let inventory = runtime.state_inventory(&mv.table_id).await.unwrap();
    assert_eq!(inventory.root_view_id, mv.table_id);
    assert_eq!(inventory.root_table_id, mv.table_id);
    assert_eq!(inventory.root_table_name, mv.table_name);
    assert!(inventory.include_rows);
    assert!(inventory.views.len() >= 2, "{inventory:?}");

    let derived = inventory
        .views
        .iter()
        .find(|view| view.derived)
        .expect("a derived step");
    assert!(derived.table_name.contains("__ivm_step"));
    assert!(derived.bytes > 0, "{derived:?}");
    assert!(derived.files > 0, "{derived:?}");
    assert!(derived.rows.unwrap() > 0, "{derived:?}");

    let root = inventory.views.last().expect("the root view");
    assert_eq!(root.table_id, mv.table_id);
    assert!(root.rows.unwrap() > 0, "{root:?}");
    assert!(
        root.sources
            .iter()
            .any(|source| source.is_view && source.table_id == derived.view_id),
        "{root:?}"
    );
    assert!(inventory.totals.views >= 2);
    assert!(inventory.totals.tables >= 2);
    assert!(inventory.totals.bytes > 0);
    assert!(inventory.totals.rows.unwrap() > 0);
    assert!(
        inventory
            .base_sources
            .iter()
            .any(|source| source.table_id == fact.table_id)
    );

    let text = inventory.render_text();
    assert!(text.contains("IVM state inventory"), "{text}");
    assert!(text.contains("TOP TABLES BY BYTES"), "{text}");
    assert!(text.contains(&mv.table_name), "{text}");

    let json = inventory.to_json_pretty().unwrap();
    let decoded: StateInventory = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.root_view_id, inventory.root_view_id);
    assert_eq!(decoded.totals, inventory.totals);

    let metadata_only = runtime
        .state_inventory_with_options(
            &mv.table_id,
            &InventoryOptions {
                include_rows: false,
                ..InventoryOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(metadata_only.totals.rows.is_none());
    assert!(
        metadata_only
            .views
            .iter()
            .flat_map(|view| &view.tables)
            .all(|table| table.rows.is_none())
    );
    assert!(metadata_only.totals.bytes > 0);

    let by_name = runtime
        .state_inventory_for_table(&mv.namespace, &mv.table_name)
        .await
        .unwrap();
    assert_eq!(by_name.root_view_id, mv.table_id);

    let error = runtime.state_inventory(&fact.table_id).await.unwrap_err();
    assert!(
        error.to_string().contains("not an IVM view"),
        "unexpected error: {error}"
    );

    runtime
        .drop_view(&mv.table_id, &DropViewOptions::default())
        .await
        .unwrap();
    for table in [&fact, &dim, &mv] {
        let _ = runtime
            .client()
            .drop_table(&table.table_name, &table.namespace)
            .await;
    }
}

#[test_log::test(tokio::test)]
async fn inventory_lists_auxiliary_state_tables() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_inv_mm_src_{suffix}"),
                table_path(&dir, "src"),
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("g", DataType::Int64, false),
                    Field::new("v", DataType::Int64, false),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_inv_mm_mv_{suffix}"),
                table_path(&dir, "mv"),
                min_max_mv_schema("g"),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();

    let batch = RecordBatch::try_new(
        source.schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
            Arc::new(StringArray::from(vec!["insert", "insert", "insert"])),
        ],
    )
    .unwrap();
    source.append_batch(runtime.client(), batch).await.unwrap();
    executor
        .execute(&format!(
            "INSERT INTO {} SELECT g, MAX(v) AS value FROM {} GROUP BY g",
            mv.table_name, source.table_name
        ))
        .await
        .unwrap();

    let inventory = runtime.state_inventory(&mv.table_id).await.unwrap();
    let state = inventory
        .views
        .iter()
        .flat_map(|view| &view.tables)
        .find(|table| table.roles.iter().any(|role| role == "state"))
        .unwrap_or_else(|| panic!("a value-count state table in {inventory:#?}"));
    assert!(state.rows.unwrap() > 0, "{state:?}");
    assert!(state.bytes > 0, "{state:?}");
    assert!(inventory.totals.bytes >= state.bytes);

    runtime
        .drop_view(&mv.table_id, &DropViewOptions::default())
        .await
        .unwrap();
    for table in [&source, &mv] {
        let _ = runtime
            .client()
            .drop_table(&table.table_name, &table.namespace)
            .await;
    }
}

fn event_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("doc_no", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn event_batch(rows: &[(&str, &str, &str, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        event_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.4))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.5))),
        ],
    )
    .unwrap()
}

fn event_mv_schema(source_schema: &Schema) -> SchemaRef {
    multi_agg_mv_schema_for(
        source_schema,
        &["region".to_string(), "doc_no".to_string()],
        &[],
        &[
            ("total".to_string(), DataType::Int64),
            ("flag_a".to_string(), DataType::Int64),
            ("flag_b".to_string(), DataType::Int64),
        ],
    )
    .unwrap()
}

fn table_with_role<'a>(
    inventory: &'a StateInventory,
    role: &str,
) -> &'a lakesoul_ivm::TableFootprint {
    inventory
        .views
        .iter()
        .flat_map(|view| &view.tables)
        .find(|table| table.roles.iter().any(|candidate| candidate == role))
        .unwrap_or_else(|| panic!("a {role} table in {inventory:#?}"))
}

async fn create_event_target(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    suffix: &str,
    name: &str,
    source_schema: &Schema,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_inv_ev_{name}_{suffix}"),
                table_path(dir, name),
                event_mv_schema(source_schema),
            )
            .with_primary_keys(vec!["region".to_string(), "doc_no".to_string()]),
        )
        .await
        .unwrap()
}

/// The `SUM + MAX` pattern: `SUM + MAX + MAX` keeps a row-level
/// multi-aggregate state, while rewriting the 0/1 `MAX` flags to `SUM` keeps
/// the compact accumulator state.
#[test_log::test(tokio::test)]
async fn inventory_shows_row_state_versus_compact_multi_agg_state() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_inv_ev_src_{suffix}"),
                table_path(&dir, "src"),
                event_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let max_mv =
        create_event_target(&runtime, &dir, &suffix, "max", &source.schema).await;
    let sum_mv =
        create_event_target(&runtime, &dir, &suffix, "sum", &source.schema).await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();

    source
        .append_batch(
            runtime.client(),
            event_batch(&[
                ("s1", "o1", "x", 1, 100, "insert"),
                ("s1", "o1", "a", 2, 50, "insert"),
                ("s1", "o1", "b", 3, 25, "insert"),
                ("s1", "o2", "x", 4, 200, "insert"),
                ("s2", "o1", "a", 5, 80, "insert"),
                ("s2", "o1", "b", 6, 20, "insert"),
            ]),
        )
        .await
        .unwrap();

    executor
        .execute(&format!(
            "INSERT INTO {} SELECT region, doc_no, \
             SUM(amount) AS total, \
             MAX(CASE WHEN kind = 'a' THEN 1 ELSE 0 END) AS flag_a, \
             MAX(CASE WHEN kind = 'b' THEN 1 ELSE 0 END) AS flag_b \
             FROM {} GROUP BY region, doc_no",
            max_mv.table_name, source.table_name
        ))
        .await
        .unwrap();
    let max_inventory = runtime.state_inventory(&max_mv.table_id).await.unwrap();
    let rows_state = table_with_role(&max_inventory, "multi_agg_rows");
    assert_eq!(rows_state.rows, Some(6), "{rows_state:?}");
    assert!(rows_state.bytes > 0, "{rows_state:?}");

    executor
        .execute(&format!(
            "INSERT INTO {} SELECT region, doc_no, \
             SUM(amount) AS total, \
             SUM(CASE WHEN kind = 'a' THEN 1 ELSE 0 END) AS flag_a, \
             SUM(CASE WHEN kind = 'b' THEN 1 ELSE 0 END) AS flag_b \
             FROM {} GROUP BY region, doc_no",
            sum_mv.table_name, source.table_name
        ))
        .await
        .unwrap();
    let sum_inventory = runtime.state_inventory(&sum_mv.table_id).await.unwrap();
    let compact_state = table_with_role(&sum_inventory, "state");
    assert_eq!(compact_state.rows, Some(3), "{compact_state:?}");
    assert!(
        rows_state.rows.unwrap() > compact_state.rows.unwrap(),
        "row state {rows_state:?} should be larger than compact state {compact_state:?}"
    );

    for mv in [&max_mv, &sum_mv] {
        runtime
            .drop_view(&mv.table_id, &DropViewOptions::default())
            .await
            .unwrap();
    }
    for table in [&source, &max_mv, &sum_mv] {
        let _ = runtime
            .client()
            .drop_table(&table.table_name, &table.namespace)
            .await;
    }
}
