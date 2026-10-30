// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::{Client, Row, SimpleQueryMessage, Statement, types::Type};

pub type Rows = Vec<Vec<Option<String>>>;

#[derive(Debug, PartialEq, Eq)]
pub struct ResultSet {
    pub names: Vec<String>,
    // tokio-postgres intentionally hides simple-query OIDs. Assert those on
    // Describe in the extended path rather than inventing simple metadata.
    pub types: Option<Vec<Type>>,
    pub rows: Rows,
}

pub async fn simple(client: &Client, sql: &str) -> Result<ResultSet> {
    let mut result = ResultSet {
        names: Vec::new(),
        types: None,
        rows: Vec::new(),
    };
    let mut descriptions = 0;
    for message in client.simple_query(sql).await? {
        match message {
            SimpleQueryMessage::RowDescription(columns) => {
                descriptions += 1;
                result.names = columns.iter().map(|c| c.name().to_owned()).collect();
            }
            SimpleQueryMessage::Row(row) => {
                result.rows.push(
                    (0..row.len())
                        .map(|i| row.get(i).map(str::to_owned))
                        .collect(),
                );
            }
            SimpleQueryMessage::CommandComplete(_) => {}
            _ => bail!("unexpected simple-query message"),
        }
    }
    ensure!(
        descriptions == 1,
        "expected one RowDescription, got {descriptions}"
    );
    Ok(result)
}

pub fn binary(statement: &Statement, rows: &[Row]) -> Result<ResultSet> {
    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(row.len());
        for (i, column) in row.columns().iter().enumerate() {
            let value = match *column.type_() {
                Type::BOOL => row
                    .try_get::<_, Option<bool>>(i)?
                    .map(|v| if v { "t" } else { "f" }.to_owned()),
                Type::INT4 => row.try_get::<_, Option<i32>>(i)?.map(|v| v.to_string()),
                Type::INT8 => row.try_get::<_, Option<i64>>(i)?.map(|v| v.to_string()),
                Type::TEXT => row.try_get::<_, Option<String>>(i)?,
                ref other => bail!(
                    "no phase-one result decoder for column {}: {other:?}",
                    column.name()
                ),
            };
            values.push(value);
        }
        decoded.push(values);
    }
    Ok(ResultSet {
        names: statement
            .columns()
            .iter()
            .map(|c| c.name().to_owned())
            .collect(),
        types: Some(
            statement
                .columns()
                .iter()
                .map(|c| c.type_().clone())
                .collect(),
        ),
        rows: decoded,
    })
}

pub fn check_columns(result: &ResultSet, expected: &[(&str, Type)]) -> Result<()> {
    let names: Vec<_> = expected
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect();
    ensure!(
        result.names == names,
        "column names: expected {names:?}, actual {:?}",
        result.names
    );
    if let Some(actual) = &result.types {
        let types: Vec<_> = expected.iter().map(|(_, t)| t.clone()).collect();
        ensure!(
            *actual == types,
            "column types/OIDs: expected {types:?}, actual {actual:?}"
        );
    }
    Ok(())
}

pub fn compare(actual: &ResultSet, expected: &ResultSet, ordered: bool) -> Result<()> {
    ensure!(
        actual.names == expected.names,
        "column names: expected {:?}, actual {:?}",
        expected.names,
        actual.names
    );
    ensure!(
        actual.types == expected.types,
        "column types/OIDs: expected {:?}, actual {:?}",
        expected.types,
        actual.types
    );
    compare_rows(&actual.rows, &expected.rows, ordered)
}

pub fn compare_rows(actual: &Rows, expected: &Rows, ordered: bool) -> Result<()> {
    if ordered {
        return first_difference(actual, expected);
    }
    // Sorting preserves multiplicity, NULL, empty strings and whitespace. No
    // set conversion, global trim, float tolerance or benchmark normalization.
    let mut actual = actual.clone();
    let mut expected = expected.clone();
    actual.sort();
    expected.sort();
    first_difference(&actual, &expected)
}

fn first_difference(actual: &Rows, expected: &Rows) -> Result<()> {
    for (i, (actual_row, expected_row)) in actual.iter().zip(expected).enumerate() {
        ensure!(
            actual_row == expected_row,
            "first differing row {i}: expected {expected_row:?}, actual {actual_row:?}"
        );
    }
    ensure!(
        actual.len() == expected.len(),
        "row count: expected {}, actual {}; first extra/missing row: expected {:?}, actual {:?}",
        expected.len(),
        actual.len(),
        expected.get(actual.len()),
        actual.get(expected.len())
    );
    Ok(())
}

pub async fn query_binary(client: &Client, sql: &str) -> Result<ResultSet> {
    let statement = client
        .prepare(sql)
        .await
        .context("extended Parse/Describe")?;
    let rows = client
        .query(&statement, &[])
        .await
        .context("extended Bind/Execute")?;
    binary(&statement, &rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bag_comparison_preserves_duplicates_null_and_exact_text() {
        let rows = vec![
            vec![None],
            vec![Some(String::new())],
            vec![Some(" β ".into())],
            vec![None],
        ];
        let mut permuted = rows.clone();
        permuted.reverse();
        assert!(compare_rows(&rows, &permuted, false).is_ok());
        assert!(compare_rows(&rows, &permuted, true).is_err());
        for wrong in [
            vec![
                vec![None],
                vec![Some(String::new())],
                vec![Some(" β ".into())],
            ],
            vec![
                vec![None],
                vec![Some(String::new())],
                vec![Some("β".into())],
                vec![None],
            ],
            vec![
                vec![Some(String::new())],
                vec![Some(String::new())],
                vec![Some(" β ".into())],
                vec![None],
            ],
        ] {
            assert!(compare_rows(&rows, &wrong, false).is_err());
        }
    }
}
