// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod support;

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, ensure};
use support::Harness;
use support::tpch::TpchConfig;

fn workload() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("benchmark/workloads/tpch")
}

fn owned_case(h: &Harness, name: &str) -> Result<PathBuf> {
    let directory = h
        .warehouse_dir
        .parent()
        .context("TPC-H temporary root")?
        .join(name);
    std::fs::create_dir(&directory)
        .with_context(|| format!("create {}", directory.display()))?;
    Ok(directory)
}

fn copy_answers(destination: &Path, scale: &str, omit: Option<&str>) -> Result<()> {
    let source = workload().join("answers").join(format!("sf{scale}"));
    for number in 1..=22 {
        let name = format!("q{number:02}");
        if omit != Some(name.as_str()) {
            std::fs::copy(
                source.join(format!("{name}.csv")),
                destination.join(format!("{name}.csv")),
            )
            .with_context(|| format!("copy owned negative reference {name}"))?;
        }
    }
    Ok(())
}

async fn strict_failure_contract(h: &Harness, scale: &str, layout: &str) -> Result<()> {
    let missing = owned_case(h, "missing-answer")?;
    copy_answers(&missing, scale, Some("q22"))?;
    let missing_path = missing.to_str().context("UTF8 answer directory")?;
    let run = h
        .run_tpch_driver(&[
            "--scale",
            scale,
            "--layout",
            layout,
            "--answers-dir",
            missing_path,
        ])
        .await?;
    ensure!(
        run.status.code() == Some(2)
            && run.summary["cause"] == "missing_answer"
            && run.summary["expected"] == 22
            && run.summary["attempted"] == 0,
        "missing q22 answer must fail before execution: {}",
        run.summary
    );

    let wrong = owned_case(h, "wrong-answer")?;
    std::fs::write(wrong.join("q06.csv"), "revenue\n-1\n")
        .context("write wrong owned q06 answer")?;
    let wrong_path = wrong.to_str().context("UTF8 wrong answer directory")?;
    let run = h
        .run_tpch_driver(&[
            "--scale",
            scale,
            "--layout",
            layout,
            "--queries",
            "q06",
            "--answers-dir",
            wrong_path,
        ])
        .await?;
    ensure!(
        run.status.code() == Some(1)
            && run.summary["cause"] == "mismatch"
            && run.summary["completed"] == 1
            && run.summary["matched"] == 0,
        "real numeric mismatch must fail: {}",
        run.summary
    );

    let broken_sql = owned_case(h, "query-error")?;
    std::fs::write(
        broken_sql.join("q06.sql"),
        "SELECT * FROM missing_tpch_contract_table",
    )
    .context("write SQL-error injection")?;
    std::fs::copy(
        workload().join("queries/q22.sql"),
        broken_sql.join("q22.sql"),
    )
    .context("copy q22 after rejected q06")?;
    let broken_path = broken_sql.to_str().context("UTF8 SQL directory")?;
    let run = h
        .run_tpch_driver(&[
            "--scale",
            scale,
            "--layout",
            layout,
            "--queries",
            "q06",
            "q22",
            "--queries-dir",
            broken_path,
        ])
        .await?;
    ensure!(
        run.status.code() == Some(1)
            && run.summary["cause"] == "server_error"
            && run.summary["attempted"] == 2
            && run.summary["matched"] == 1
            && run.summary["queries"][0]["sqlstate"].as_str().is_some(),
        "SQL error must remain failure after the next query succeeds: {}",
        run.summary
    );

    let run = h
        .run_tpch_driver(&[
            "--scale",
            scale,
            "--layout",
            layout,
            "--queries",
            "q01",
            "q22",
            "--query-timeout",
            "0.000000001",
        ])
        .await?;
    ensure!(
        run.status.code() == Some(1)
            && run.summary["category"] == "incomplete"
            && run.summary["cause"] == "timeout"
            && run.summary["expected"] == 2
            && run.summary["attempted"] == 1
            && run.summary["completed"] == 0
            && run.summary["unattempted"] == 1
            && run.summary["matched"] == 0,
        "a timed-out selected query and its unexecuted successor must fail without retry: {}",
        run.summary
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tpch_q01_to_q22_and_strict_failures() -> Result<()> {
    let config = TpchConfig::from_env()?;
    let scale = config.scale;
    let layout = config.layout;
    support::run_tpch_suite(config, move |h| {
        Box::pin(async move {
            h.prove_tpch_remote_execution().await?;
            if std::env::var("LAKESOUL_SQL_COMPAT_FAULT").as_deref() == Ok("expectation")
            {
                let wrong = owned_case(h, "injected-expectation")?;
                std::fs::write(wrong.join("q06.csv"), "revenue\n-1\n")?;
                let run = h
                    .run_tpch_driver(&[
                        "--scale",
                        scale,
                        "--layout",
                        layout,
                        "--queries",
                        "q06",
                        "--answers-dir",
                        wrong.to_str().context("UTF8 fault directory")?,
                    ])
                    .await?;
                ensure!(
                    run.status.success(),
                    "injected wrong reference: {}",
                    run.summary
                );
            }
            for protocol in ["simple", "extended"] {
                let run = h
                    .run_tpch_driver(&[
                        "--scale",
                        scale,
                        "--layout",
                        layout,
                        "--protocol",
                        protocol,
                    ])
                    .await?;
                ensure!(
                    run.status.success()
                        && run.summary["ok"] == true
                        && run.summary["expected"] == 22
                        && run.summary["attempted"] == 22
                        && run.summary["completed"] == 22
                        && run.summary["matched"] == 22,
                    "strict full q01-q22 {protocol} failed: {}",
                    run.summary
                );
            }
            strict_failure_contract(h, scale, layout).await
        })
    })
    .await
}
