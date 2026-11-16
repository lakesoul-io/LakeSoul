// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Real Python PG-wire driver, with the same explicit child reaping as the server.

use std::io::{Read, Seek};
use std::process::{Command, ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context as _, Result};

use super::{ChildGuard, Harness};

pub struct TpchRun {
    pub status: ExitStatus,
    pub summary: serde_json::Value,
}

impl Harness {
    /// Files avoid a pipe deadlock on diagnostics; an owned ChildGuard also
    /// kills and waits if the surrounding harness future is cancelled.
    pub async fn run_tpch_driver(&self, arguments: &[&str]) -> Result<TpchRun> {
        let directory = self
            .warehouse_dir
            .parent()
            .context("TPC-H temporary root")?;
        let mut stdout = tempfile::tempfile_in(directory).context("TPC-H stdout file")?;
        let mut stderr = tempfile::tempfile_in(directory).context("TPC-H stderr file")?;
        let driver =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("benchmark/driver");
        let process = Command::new("python3")
            .args(["-m", "lsbench.strict_tpch", "--json"])
            .args(["--host", &self.lake_addr.ip().to_string()])
            .args(["--port", &self.lake_addr.port().to_string()])
            .args(["--user", "sql_compat", "--database", &self.lake_namespace])
            .args(arguments)
            .current_dir(driver)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("TZ", "UTC")
            .stdin(Stdio::null())
            .stdout(stdout.try_clone().context("clone TPC-H stdout")?)
            .stderr(stderr.try_clone().context("clone TPC-H stderr")?)
            .spawn()
            .context("start python3 -m lsbench.strict_tpch")?;
        let mut child = ChildGuard::new(process);
        let status = tokio::time::timeout(Duration::from_secs(1700), async {
            loop {
                if let Some(status) =
                    child.poll_exit().context("wait for strict TPC-H driver")?
                {
                    return Ok::<_, anyhow::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("strict TPC-H driver exceeded 1700 seconds")??;
        let mut output = String::new();
        let mut errors = String::new();
        stdout.rewind().context("rewind TPC-H stdout")?;
        stdout
            .read_to_string(&mut output)
            .context("read TPC-H stdout")?;
        stderr.rewind().context("rewind TPC-H stderr")?;
        stderr
            .read_to_string(&mut errors)
            .context("read TPC-H stderr")?;
        let summary = serde_json::from_str(&output).with_context(|| {
            format!("strict TPC-H driver returned {status} without a JSON report; stdout={output}; stderr={errors}")
        })?;
        eprintln!(
            "TPC-H driver pid={} reaped=true exit={status}: {summary}",
            child.pid
        );
        if !errors.is_empty() {
            eprintln!("TPC-H driver stderr:\n{errors}");
        }
        Ok(TpchRun { status, summary })
    }
}
