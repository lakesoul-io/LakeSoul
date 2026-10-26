// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Two real Flight workers for the scheduled/manual workload combinations.

use std::net::{SocketAddr, TcpListener as StdTcpListener};

use anyhow::{Context as _, Result, bail, ensure};
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::distributed::{LakeSoulWorkerOptions, spawn_lakesoul_worker};
use lakesoul_datafusion::session::ExecutionMemoryArgs;
use tokio::task::JoinSet;
use tokio_postgres::SimpleQueryMessage;

use super::{CLEANUP_TIMEOUT, CleanupOutcome, Harness, OPERATION_TIMEOUT, bounded};

pub(super) struct Workers {
    addresses: Vec<SocketAddr>,
    tasks: JoinSet<Result<(), String>>,
}

impl Workers {
    pub(super) fn configured() -> Result<Option<Self>> {
        match std::env::var("LAKESOUL_TPCH_WORKERS").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("0") => Ok(None),
            Ok("2") => Ok(Some(Self {
                addresses: Vec::new(),
                tasks: JoinSet::new(),
            })),
            _ => bail!(
                "LAKESOUL_TPCH_WORKERS must be 0 (standalone) or 2 (real Flight workers)"
            ),
        }
    }

    pub(super) async fn start(&mut self, warehouse: &str) -> Result<()> {
        for _ in 0..2 {
            // Keep each listener bound: no reserve/release race for worker RPCs.
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .context("bind TPC-H Flight worker")?;
            self.addresses
                .push(listener.local_addr().context("read worker RPC port")?);
            let options = LakeSoulWorkerOptions {
                core_args: CoreArgs {
                    warehouse_prefix: Some(warehouse.to_owned()),
                    worker_threads: 2,
                    ..CoreArgs::default()
                },
                memory_pool: ExecutionMemoryArgs::default().create_pool(),
            };
            self.tasks.spawn(async move {
                spawn_lakesoul_worker(&options, listener)
                    .await
                    .map_err(|error| error.to_string())
            });
        }
        eprintln!("TPC-H Flight listeners bound: {}", self.urls());
        Ok(())
    }

    pub(super) fn urls(&self) -> String {
        self.addresses
            .iter()
            .map(|address| format!("http://{address}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(super) async fn stop(&mut self, outcome: &mut CleanupOutcome) {
        self.tasks.abort_all();
        let joined = tokio::time::timeout(CLEANUP_TIMEOUT, async {
            while let Some(result) = self.tasks.join_next().await {
                match result {
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => outcome
                        .errors
                        .push(format!("Flight worker panicked: {error}")),
                    Ok(Err(error)) => outcome
                        .errors
                        .push(format!("Flight worker failed: {error}")),
                    Ok(Ok(())) => outcome
                        .errors
                        .push("Flight worker exited before shutdown".into()),
                }
            }
        })
        .await;
        if joined.is_err() {
            outcome
                .errors
                .push("Flight worker shutdown deadline elapsed".into());
        } else {
            outcome.resources.push("Flight worker tasks joined".into());
        }
        for address in &self.addresses {
            let released = StdTcpListener::bind(address).is_ok();
            outcome
                .ports_free
                .push(("Flight worker".into(), address.port(), released));
            if !released {
                outcome
                    .errors
                    .push(format!("Flight worker {address} is still bound"));
            }
        }
    }
}

impl Harness {
    /// A scheduled/manual distributed run must execute remotely, not just
    /// connect a coordinator with worker discovery enabled.
    pub async fn prove_tpch_remote_execution(&self) -> Result<()> {
        if self.state.workers.is_none() {
            return Ok(());
        }
        let sql = "EXPLAIN ANALYZE SELECT l_returnflag, count(*) FROM lineitem GROUP BY l_returnflag";
        let messages = bounded(
            OPERATION_TIMEOUT,
            "TPC-H remote execution proof",
            self.lake.simple_query(sql),
        )
        .await?;
        let mut plan = String::new();
        for message in messages {
            if let SimpleQueryMessage::Row(row) = message {
                for index in 0..row.len() {
                    if let Some(value) = row.get(index) {
                        plan.push_str(value);
                        plan.push('\n');
                    }
                }
            }
        }
        ensure!(
            plan.contains("DistributedExec")
                && plan.contains("NetworkShuffleExec")
                && plan.contains("tasks=2")
                && plan.contains("Plan with Metrics")
                && plan.contains("metrics=[output_rows="),
            "TPC-H must execute a two-task remote scan with runtime metrics; fallback is disabled:\n{plan}"
        );
        eprintln!("TPC-H remote execution proof:\n{plan}");
        Ok(())
    }
}
