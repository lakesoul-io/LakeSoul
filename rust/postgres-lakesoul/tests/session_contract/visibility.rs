// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use tokio_postgres::types::Type;

use crate::support::{Harness, compare};

/// LakeSoul-specific commit publication; PostgreSQL supplies the READ COMMITTED
/// result oracle, not the metadata barrier. Each reader stays in one transaction.
pub async fn run(h: &Harness) -> Result<()> {
    for (protocol, value, label) in [
        ("simple-text", 199_i64, "visible_simple"),
        ("prepared-binary", 299_i64, "visible_prepared"),
    ] {
        let lake = h.connect_lake().await?;
        let reference = h.connect_reference().await?;
        lake.batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        reference
            .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        let simple_sql =
            "SELECT id, val, label FROM pk_items WHERE id = CAST(1 AS BIGINT)";
        let parameter_sql = "SELECT id, val, label FROM pk_items WHERE id = $1";
        // Parse/Describe both statements before the management commit. Reusing
        // this exact handle is essential: re-prepare could hide a stale plan.
        let statements = if protocol == "prepared-binary" {
            Some((
                lake.prepare_typed(parameter_sql, &[Type::INT8]).await?,
                reference
                    .prepare_typed(parameter_sql, &[Type::INT8])
                    .await?,
            ))
        } else {
            None
        };
        let (before_lake, before_reference) = if let Some((ls, rs)) = &statements {
            (
                compare::binary(ls, &lake.query(ls, &[&1_i64]).await?)?,
                compare::binary(rs, &reference.query(rs, &[&1_i64]).await?)?,
            )
        } else {
            (
                compare::simple(&lake, simple_sql).await?,
                compare::simple(&reference, simple_sql).await?,
            )
        };
        compare::compare(&before_lake, &before_reference, true)?;

        // Commit the independent PG oracle first, avoiding any extra operation
        // between LakeSoul's publication barrier and its measured first SELECT.
        h.reference.execute(
            "INSERT INTO pk_items VALUES (1, $1, $2) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, label = EXCLUDED.label",
            &[&value, &label],
        ).await?;
        h.commit_pk_rows(&format!("(1,{value},'{label}')")).await?;
        let actual = if let Some((ls, _)) = &statements {
            compare::binary(ls, &lake.query(ls, &[&1_i64]).await?)?
        } else {
            compare::simple(&lake, simple_sql).await?
        };
        let expected = if let Some((_, rs)) = &statements {
            compare::binary(rs, &reference.query(rs, &[&1_i64]).await?)?
        } else {
            compare::simple(&reference, simple_sql).await?
        };
        let context = || {
            format!(
                "case=read-committed-first-select contract=LakeSoul-publication SQL={} params={:?} protocol={protocol} expected committed val={value} label={label}; same transaction, no SELECT polling",
                if statements.is_some() {
                    parameter_sql
                } else {
                    simple_sql
                },
                if statements.is_some() {
                    vec!["1"]
                } else {
                    vec![]
                }
            )
        };
        compare::check_columns(
            &actual,
            &[
                ("id", Type::INT8),
                ("val", Type::INT8),
                ("label", Type::TEXT),
            ],
        )
        .with_context(context)?;
        compare::compare(&actual, &expected, true).with_context(context)?;
        compare::compare_rows(
            &actual.rows,
            &vec![vec![
                Some("1".into()),
                Some(value.to_string()),
                Some(label.into()),
            ]],
            true,
        )
        .with_context(context)?;
        lake.batch_execute("ROLLBACK").await?;
        reference.batch_execute("ROLLBACK").await?;
        eprintln!(
            "sql-compat case=read-committed-first-select protocol={protocol}: first SELECT observed committed val={value} inside the original transaction"
        );
    }
    Ok(())
}
