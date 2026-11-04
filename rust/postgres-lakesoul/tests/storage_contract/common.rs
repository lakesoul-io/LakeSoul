// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use tokio_postgres::types::Type;

use crate::support::{Harness, compare};

pub fn rows(cells: &[&[Option<&str>]]) -> compare::Rows {
    cells
        .iter()
        .map(|row| row.iter().map(|cell| cell.map(str::to_owned)).collect())
        .collect()
}

/// PostgreSQL models the independently derived final relation, not LakeSoul's
/// write/merge operation. Every consumer query must agree with that relation
/// and the literal expected rows through both real wire-protocol paths.
pub async fn assert_query(
    h: &Harness,
    id: &str,
    bug: &str,
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
                "case={id} classification=lakesoul-final-relation bug={bug} \
             SQL={sql} params=[] protocol={protocol} expected_columns={columns:?} ordered={ordered}"
            )
        };
        let (actual, reference) = if binary {
            (
                compare::query_binary(&h.lake, sql)
                    .await
                    .with_context(context)?,
                compare::query_binary(&h.reference, sql)
                    .await
                    .with_context(context)?,
            )
        } else {
            (
                compare::simple(&h.lake, sql).await.with_context(context)?,
                compare::simple(&h.reference, sql)
                    .await
                    .with_context(context)?,
            )
        };
        compare::check_columns(&reference, columns).with_context(context)?;
        compare::check_columns(&actual, columns).with_context(context)?;
        compare::compare_rows(&reference.rows, expected, ordered)
            .with_context(|| format!("{}: independent final relation", context()))?;
        compare::compare(&actual, &reference, ordered).with_context(context)?;
        eprintln!(
            "PASS {} rows={} oid_check=extended-only",
            context(),
            actual.rows.len()
        );
    }
    Ok(())
}
