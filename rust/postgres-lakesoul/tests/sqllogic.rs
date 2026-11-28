// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! File-based SQL regression through real PostgreSQL sockets. Portable scripts
//! run unchanged on LakeSoul and the reference PostgreSQL; LakeSoul extensions
//! live in a separate directory. Each protocol/execution pair gets its own
//! process/namespace; distributed runs add two Flight workers. Each file gets
//! fresh connections owned by the existing teardown.

#[path = "sqllogic/distributed.rs"]
mod distributed;
#[allow(dead_code)]
pub mod support;

use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use sqllogictest::{
    AsyncDB, ColumnType, Connection, DBOutput, ExpectedError, Normalizer, QueryExpect,
    Record, Runner, SortMode, StatementExpect,
};
use tokio_postgres::{Client, SimpleQueryMessage, types::Type};

use support::compare;

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Simple,
    Extended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Execution {
    Local,
    Distributed,
}

struct Script {
    name: String,
    records: Vec<Record<Column>>,
    count: usize,
}

/// SLT's I/R/T are categories, not an assertion of exact PostgreSQL OIDs.
/// Simple RowDescription hides OIDs in tokio-postgres, so only that path uses
/// Undescribed. Extended queries require the category from their real OIDs.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Column {
    Integer,
    Real,
    Text,
    Undescribed,
}

impl ColumnType for Column {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'I' => Some(Self::Integer),
            'R' => Some(Self::Real),
            'T' => Some(Self::Text),
            _ => None,
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Integer => 'I',
            Self::Real => 'R',
            Self::Text => 'T',
            Self::Undescribed => '?',
        }
    }
}

#[derive(Debug)]
struct SltError(anyhow::Error);

impl fmt::Display for SltError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl std::error::Error for SltError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

struct PgDb<'a> {
    client: &'a Client,
    protocol: Protocol,
}

#[async_trait::async_trait]
impl AsyncDB for PgDb<'_> {
    type Error = SltError;
    type ColumnType = Column;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Column>, SltError> {
        self.execute(sql).await.map_err(SltError)
    }

    // Connection driver tasks belong to the shared harness, including failures.
    async fn shutdown(&mut self) {}

    fn error_sql_state(error: &SltError) -> Option<String> {
        error.0.chain().find_map(|cause| {
            cause
                .downcast_ref::<tokio_postgres::Error>()?
                .as_db_error()
                .map(|error| error.code().code().to_owned())
        })
    }
}

impl PgDb<'_> {
    async fn execute(&self, sql: &str) -> Result<DBOutput<Column>> {
        match self.protocol {
            Protocol::Simple => {
                let mut types = None;
                let mut rows = Vec::new();
                let mut count = 0;
                let mut completions = 0;
                for message in self.client.simple_query(sql).await? {
                    match message {
                        SimpleQueryMessage::RowDescription(columns) => {
                            ensure!(
                                types.is_none(),
                                "SLT requires one result per record"
                            );
                            types = Some(vec![Column::Undescribed; columns.len()]);
                        }
                        SimpleQueryMessage::Row(row) => {
                            rows.push((0..row.len()).map(|i| cell(row.get(i))).collect());
                        }
                        SimpleQueryMessage::CommandComplete(affected) => {
                            completions += 1;
                            count = affected;
                        }
                        _ => bail!("unexpected simple-query message"),
                    }
                }
                ensure!(completions == 1, "SLT requires one SQL command per record");
                Ok(match types {
                    Some(types) => DBOutput::Rows { types, rows },
                    None => DBOutput::StatementComplete(count),
                })
            }
            Protocol::Extended => {
                let statement = self.client.prepare(sql).await?;
                if statement.columns().is_empty() {
                    return Ok(DBOutput::StatementComplete(
                        self.client.execute(&statement, &[]).await?,
                    ));
                }
                let types = statement
                    .columns()
                    .iter()
                    .map(|column| column_type(column.type_()))
                    .collect::<Result<Vec<_>>>()?;
                let rows = self.client.query(&statement, &[]).await?;
                let result = compare::binary(&statement, &rows)?;
                Ok(DBOutput::Rows {
                    types,
                    rows: result
                        .rows
                        .iter()
                        .map(|row| {
                            row.iter().map(|value| cell(value.as_deref())).collect()
                        })
                        .collect(),
                })
            }
        }
    }
}

fn column_type(ty: &Type) -> Result<Column> {
    Ok(match *ty {
        Type::INT2 | Type::INT4 | Type::INT8 => Column::Integer,
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => Column::Real,
        Type::TEXT
        | Type::VARCHAR
        | Type::BPCHAR
        | Type::BOOL
        | Type::DATE
        | Type::TIMESTAMP
        | Type::TIMESTAMPTZ
        | Type::INT4_ARRAY
        | Type::INT8_ARRAY => Column::Text,
        _ => bail!("SLT has no result decoder/category for {ty:?}"),
    })
}

/// Preserve SQL NULL, empty strings and literal marker text as different cells.
/// JSON-quote whitespace, quotes, backslashes and marker-like strings so SLT's
/// row separator never trims/merges user data. Numbers remain exact strings.
fn cell(value: Option<&str>) -> String {
    match value {
        None => "NULL".to_owned(),
        Some("") => "(empty)".to_owned(),
        Some(value)
            if matches!(value, "NULL" | "(empty)")
                || value.chars().any(char::is_whitespace)
                || value.contains(['"', '\\']) =>
        {
            serde_json::to_string(value).expect("serializing a string cannot fail")
        }
        Some(value) => value.to_owned(),
    }
}

fn exact_rows(_: Normalizer, actual: &[Vec<String>], expected: &[String]) -> bool {
    actual
        .iter()
        .map(|row| row.join(" "))
        .eq(expected.iter().cloned())
}

#[allow(clippy::ptr_arg)] // sqllogictest's ColumnTypeValidator signature
fn columns_match(actual: &Vec<Column>, expected: &Vec<Column>) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            *actual == Column::Undescribed || actual == expected
        })
}

/// Keep the correctness gate explicit: no silent skips, retry-until-correct,
/// hash-only answers, shell commands, or unrestricted error expectations.
fn validate(records: &[Record<Column>]) -> Result<usize> {
    let mut count = 0;
    for record in records {
        let (conditions, connection, retry, error) = match record {
            Record::Statement {
                conditions,
                connection,
                retry,
                expected,
                ..
            } => {
                let error = match expected {
                    StatementExpect::Error(error) => Some(error),
                    _ => None,
                };
                (conditions, connection, retry, error)
            }
            Record::Query {
                conditions,
                connection,
                retry,
                expected,
                ..
            } => {
                let error = match expected {
                    QueryExpect::Error(error) => Some(error),
                    QueryExpect::Results { sort_mode, .. } => {
                        ensure!(
                            *sort_mode != Some(SortMode::ValueSort),
                            "use rowsort to preserve row/column associations"
                        );
                        None
                    }
                };
                (conditions, connection, retry, error)
            }
            Record::Comment(_) | Record::Newline => continue,
            _ => bail!("unsupported SLT directive: {record:?}"),
        };
        ensure!(
            conditions.is_empty(),
            "SLT conditions/skips are not allowed"
        );
        ensure!(
            *connection == Connection::Default,
            "use the file's default connection"
        );
        ensure!(retry.is_none(), "SLT retries are not allowed");
        if let Some(error) = error {
            ensure!(
                matches!(error, ExpectedError::SqlState(_)),
                "expected errors must name a SQLSTATE: error (23505)"
            );
        }
        count += 1;
    }
    ensure!(count > 0, "SLT file contains no SQL records");
    Ok(count)
}

fn files_in(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            files.extend(files_in(&path)?);
        } else if path.extension().is_some_and(|ext| ext == "slt") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

async fn check_records(
    client: &Client,
    protocol: Protocol,
    name: &str,
    backend: &str,
    records: &[Record<Column>],
) -> Result<()> {
    let mut runner = Runner::new(|| async { Ok(PgDb { client, protocol }) });
    runner.with_validator(exact_rows);
    runner.with_column_validator(columns_match);
    for record in records {
        runner.run_async(record.clone()).await.with_context(|| {
            format!("SLT file={name} backend={backend} protocol={protocol:?}")
        })?;
    }
    runner.shutdown_async().await;
    Ok(())
}

async fn run(protocol: Protocol, execution: Execution) -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt");
    let filter = std::env::var("LAKESOUL_SLT_FILTER").unwrap_or_default();
    let mut files: Vec<_> = files_in(&root)?
        .into_iter()
        .filter(|path| {
            execution == Execution::Distributed
                || !path.starts_with(root.join("distributed"))
        })
        .filter(|path| {
            path.strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .contains(&filter)
        })
        .collect();
    ensure!(!files.is_empty(), "no SLT files match {filter:?}");
    // Every distributed run proves the worker path, even when a filename filter
    // selects only expressions/VALUES that could otherwise execute locally.
    let proof = root.join(distributed::SCRIPT);
    if execution == Execution::Distributed && !files.contains(&proof) {
        files.insert(0, proof);
    }
    // Parse every selected file before starting a service or mutating metadata.
    let scripts = files
        .iter()
        .map(|path| {
            let name = path.strip_prefix(&root)?.to_string_lossy().into_owned();
            ensure!(
                name.starts_with("pg/")
                    || name.starts_with("lakesoul/")
                    || name.starts_with("distributed/"),
                "SLT files must declare their contract under pg/, lakesoul/ or distributed/: {name}"
            );
            let records = sqllogictest::parse_file::<Column>(path)?;
            let count = validate(&records).with_context(|| name.clone())?;
            Ok(Script { name, records, count })
        })
        .collect::<Result<Vec<_>>>()?;
    match execution {
        Execution::Local => {
            support::run_suite(|h| Box::pin(run_scripts(h, protocol, execution, scripts)))
                .await
        }
        Execution::Distributed => {
            support::run_distributed_insert_suite(|h| {
                Box::pin(run_scripts(h, protocol, execution, scripts))
            })
            .await
        }
    }
}

async fn run_scripts(
    h: &support::Harness,
    protocol: Protocol,
    execution: Execution,
    scripts: Vec<Script>,
) -> Result<()> {
    for Script {
        name,
        records,
        count,
    } in scripts
    {
        let lake = h.connect_lake().await?;
        let reference = h.connect_reference().await?;
        for (backend, client) in [("lakesoul", &lake), ("postgres", &reference)] {
            if backend == "postgres" && name.starts_with("lakesoul/") {
                continue;
            }
            check_records(client, protocol, &name, backend, &records)
                .await
                .with_context(|| format!("SLT execution={execution:?}"))?;
            eprintln!(
                "PASS SLT file={name} backend={backend} protocol={protocol:?} execution={execution:?} records={count}"
            );
        }
        if execution == Execution::Distributed && name == distributed::SCRIPT {
            distributed::prove(h, &lake, protocol).await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqllogic_simple() -> Result<()> {
    run(Protocol::Simple, Execution::Local).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqllogic_extended() -> Result<()> {
    run(Protocol::Extended, Execution::Local).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqllogic_distributed_simple() -> Result<()> {
    run(Protocol::Simple, Execution::Distributed).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqllogic_distributed_extended() -> Result<()> {
    run(Protocol::Extended, Execution::Distributed).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "known defect: simple INSERT of literal NULL returns XX000 instead of 23502"]
async fn simple_literal_null_reports_not_null_sqlstate() -> Result<()> {
    let name = "known_defects/simple_not_null.slt";
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(name);
    let records = sqllogictest::parse_file::<Column>(path)?;
    validate(&records)?;
    support::run_suite(|h| {
        Box::pin(async move {
            // Prove the strict expected code on PostgreSQL before testing LakeSoul.
            check_records(&h.reference, Protocol::Simple, name, "postgres", &records)
                .await?;
            check_records(&h.lake, Protocol::Simple, name, "lakesoul", &records).await
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "known PostgreSQL differences; see tests/upstream/postgresql/README.md"]
async fn upstream_postgresql_known_defects() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/known_defects/upstream");
    run_known_defects(&root, files_in(&root)?, "UPSTREAM").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "known defect: INTERSECT ALL and EXCEPT ALL mishandle multiplicities"]
async fn set_all_multiplicity_matches_postgres() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/known_defects");
    run_known_defects(&root, vec![root.join("set_all.slt")], "SET ALL").await
}

async fn run_known_defects(
    root: &Path,
    paths: Vec<PathBuf>,
    suite: &'static str,
) -> Result<()> {
    let scripts = paths
        .into_iter()
        .map(|path| {
            let name = path.strip_prefix(root)?.to_string_lossy().into_owned();
            let records = sqllogictest::parse_file::<Column>(path)?;
            validate(&records)?;
            Ok((name, records))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(!scripts.is_empty(), "no {suite} defect reproductions");

    let mut failures = Vec::new();
    for execution in [Execution::Local, Execution::Distributed] {
        for protocol in [Protocol::Simple, Protocol::Extended] {
            let scripts = scripts.clone();
            let result = match execution {
                Execution::Local => {
                    support::run_suite(|h| {
                        Box::pin(check_known_defects(
                            h, protocol, execution, scripts, suite,
                        ))
                    })
                    .await
                }
                Execution::Distributed => {
                    support::run_distributed_insert_suite(|h| {
                        Box::pin(check_known_defects(
                            h, protocol, execution, scripts, suite,
                        ))
                    })
                    .await
                }
            };
            if let Err(error) = result {
                failures.push(format!("{error:#}"));
            }
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

async fn check_known_defects(
    h: &support::Harness,
    protocol: Protocol,
    execution: Execution,
    scripts: Vec<(String, Vec<Record<Column>>)>,
    suite: &str,
) -> Result<()> {
    if execution == Execution::Distributed {
        // Retain the ordinary runner's proof of actual worker use.
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/slt")
            .join(distributed::SCRIPT);
        let records = sqllogictest::parse_file::<Column>(path)?;
        let count = validate(&records)?;
        run_scripts(
            h,
            protocol,
            execution,
            vec![Script {
                name: distributed::SCRIPT.to_owned(),
                records,
                count,
            }],
        )
        .await?;
    }
    let mut differences = Vec::new();
    let mut queries = 0;
    for (name, records) in scripts {
        // PostgreSQL must accept every original SQL/answer first.
        check_records(&h.reference, protocol, &name, "postgres", &records).await?;
        eprintln!(
            "PASS SLT reference suite={suite} file={name} protocol={protocol:?} execution={execution:?}"
        );
        for record in &records {
            queries += usize::from(matches!(record, Record::Query { .. }));
            if let Err(error) = check_records(
                &h.lake,
                protocol,
                &name,
                "lakesoul",
                std::slice::from_ref(record),
            )
            .await
            {
                // A fixture failure invalidates subsequent queries.
                if !matches!(record, Record::Query { .. }) {
                    return Err(error);
                }
                differences.push(format!("{error:#}"));
            }
        }
    }
    eprintln!(
        "{suite} REPRO execution={execution:?} protocol={protocol:?} queries={queries} failures={}",
        differences.len()
    );
    // Report all strict query failures, without accepting current wrong answers.
    ensure!(
        differences.is_empty(),
        "{suite} execution={execution:?} protocol={protocol:?}: {} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}

#[test]
fn strict_answers_and_scripts() -> Result<()> {
    ensure!(cell(None) != cell(Some("")));
    ensure!(cell(None) != cell(Some("NULL")));
    ensure!(cell(Some("")) != cell(Some("(empty)")));
    ensure!(cell(Some(" a ")) == "\" a \"");
    let actual = vec![vec!["1".to_owned()], vec!["1".to_owned()]];
    ensure!(!exact_rows(Clone::clone, &actual, &["1".to_owned()]));
    ensure!(!exact_rows(
        Clone::clone,
        &[vec![" a ".to_owned()]],
        &["a".to_owned()]
    ));
    ensure!(!columns_match(&vec![Column::Integer], &vec![Column::Text]));
    for script in [
        "# no SQL records\n",
        "halt\n",
        "statement error\nSELECT 1\n",
        "skipif postgres\nquery I\nSELECT 1\n----\n1\n",
        "query II valuesort\nSELECT 1, 2\n----\n1\n2\n",
    ] {
        let records = sqllogictest::parse::<Column>(script)?;
        ensure!(
            validate(&records).is_err(),
            "accepted non-strict script: {script}"
        );
    }
    Ok(())
}
