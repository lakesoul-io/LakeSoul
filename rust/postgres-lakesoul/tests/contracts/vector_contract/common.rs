// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, ensure};
use tokio_postgres::{Client, types::Type};

use crate::support::compare;

pub fn rows(cells: &[&[Option<&str>]]) -> compare::Rows {
    cells
        .iter()
        .map(|row| row.iter().map(|cell| cell.map(str::to_owned)).collect())
        .collect()
}

pub async fn assert_query(
    client: &Client,
    id: &str,
    sql: &str,
    columns: &[(&str, Type)],
    expected: &compare::Rows,
    ordered: bool,
) -> Result<()> {
    for binary in [false, true] {
        let protocol = if binary {
            "extended-binary"
        } else {
            "simple-text"
        };
        let context = || {
            format!(
                "case={id} classification=lakesoul-vector SQL={sql} params=[] protocol={protocol} expected_columns={columns:?} ordered={ordered}"
            )
        };
        let actual = if binary {
            compare::query_binary(client, sql).await
        } else {
            compare::simple(client, sql).await
        }
        .with_context(context)?;
        compare::check_columns(&actual, columns).with_context(context)?;
        compare::compare_rows(&actual.rows, expected, ordered).with_context(context)?;
        eprintln!(
            "PASS {} rows={} oid_check=extended-only",
            context(),
            actual.rows.len()
        );
    }
    Ok(())
}

pub async fn assert_ids(
    client: &Client,
    id: &str,
    sql: &str,
    expected: &[i64],
) -> Result<()> {
    let expected = expected
        .iter()
        .map(|id| vec![Some(id.to_string())])
        .collect();
    assert_query(client, id, sql, &[("id", Type::INT8)], &expected, true).await
}

/// Inspect only the final physical plan, not optimizer intermediate plans or
/// logical marker text. Never snapshot the entire EXPLAIN formatting.
pub async fn assert_index_path(
    client: &Client,
    id: &str,
    sql: &str,
    indexed: bool,
) -> Result<()> {
    let explanation = compare::simple(client, &format!("EXPLAIN VERBOSE {sql}"))
        .await
        .with_context(|| format!("case={id} physical EXPLAIN SQL={sql}"))?;
    let physical: Vec<_> = explanation
        .rows
        .iter()
        .filter(|row| row.first().and_then(Option::as_deref) == Some("physical_plan"))
        .filter_map(|row| row.get(1).and_then(Option::as_deref))
        .collect();
    ensure!(
        physical.len() == 1,
        "case={id}: expected one final physical_plan row, actual={:?}",
        explanation.rows
    );
    let plan = physical[0];
    let nodes: Vec<_> = plan
        .lines()
        .filter(|line| line.contains("LakeSoulVectorSearchExec("))
        .collect();
    if indexed {
        ensure!(
            nodes.len() == 1
                && nodes[0].contains("vec=vec")
                && nodes[0].contains("metric=L2"),
            "case={id}: expected one L2 vector index scan for vec, actual physical plan:\n{plan}"
        );
    } else {
        ensure!(
            nodes.is_empty(),
            "case={id}: expected exact fallback, actual physical plan:\n{plan}"
        );
        ensure!(
            // A metadata-only scan leaf reads the same files; it builds the
            // per-file plans on its first poll instead of during planning.
            plan.contains("DataSourceExec")
                || plan.contains("MergeParquetExec")
                || plan.contains("LakeSoulScanExec"),
            "case={id}: exact fallback must read persisted data, actual physical plan:\n{plan}"
        );
    }
    eprintln!(
        "PASS case={id} classification=lakesoul-vector-route indexed={indexed} SQL={sql} physical_node_count={}",
        nodes.len()
    );
    Ok(())
}
