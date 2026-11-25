// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! An independently pooled coordinator sharing the suite's namespace/warehouse.

use super::*;

pub(super) struct PeerCoordinator {
    child: ChildGuard,
    pg: SocketAddr,
    metrics: SocketAddr,
}

impl PeerCoordinator {
    pub(super) fn stop(mut self, outcome: &mut CleanupOutcome) {
        match self.child.terminate() {
            Ok(status) => outcome.resources.push(format!(
                "peer child pid {} reaped ({status})",
                self.child.pid
            )),
            Err(error) => outcome.errors.push(format!("peer child cleanup: {error}")),
        }
        for (name, address) in
            [("peer PostgreSQL", self.pg), ("peer metrics", self.metrics)]
        {
            let released = StdTcpListener::bind(address).is_ok();
            outcome
                .ports_free
                .push((name.to_string(), address.port(), released));
            if !released {
                outcome
                    .errors
                    .push(format!("{name} port {} still bound", address.port()));
            }
        }
    }
}

impl Harness {
    /// The peer is registered before readiness checks, so startup failure,
    /// assertion failure and timeout all use the suite's ordinary teardown.
    pub async fn connect_peer_coordinator(&self) -> Result<Client> {
        ensure!(
            self.state.jwt.is_none() && !self.state.rbac,
            "peer fixture requires unauthenticated suite"
        );
        let _startup = STARTUP_GATE.lock().await;
        let pg_listener = StdTcpListener::bind("127.0.0.1:0")?;
        let metrics_listener = StdTcpListener::bind("127.0.0.1:0")?;
        let pg = pg_listener.local_addr()?;
        let metrics = metrics_listener.local_addr()?;
        let log = std::fs::File::create(
            self.warehouse_dir
                .parent()
                .unwrap()
                .join(format!("peer-{}.log", pg.port())),
        )?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_postgres-lakesoul"));
        command
            .args(["--bind", "127.0.0.1", "--port"])
            .arg(pg.port().to_string())
            .arg("--metrics-addr")
            .arg(metrics.to_string())
            .args([
                "--worker-threads",
                "2",
                "--max-result-rows",
                "0",
                "--max-result-bytes",
                "0",
            ])
            .args(["--memory-limit", "0", "--vortex-decode-budget", "0"])
            .arg("--warehouse-prefix")
            .arg(&self.state.warehouse_url)
            .env("TZ", "UTC")
            .env_remove("LAKESOUL_PG_JWT_AUTH")
            .env_remove("LAKESOUL_PG_RBAC")
            .env_remove("LAKESOUL_DISTRIBUTED_WORKERS")
            .env_remove("LAKESOUL_K8S_WORKER_SERVICE")
            .env_remove("LAKESOUL_K8S_NAMESPACE")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        if let Some(workers) = &self.state.workers {
            command
                .args(["--discovery", "static", "--distributed-workers"])
                .arg(workers.urls())
                .args([
                    "--distributed-fallback-local=false",
                    "--distributed-bytes-per-partition",
                    "1",
                ]);
        } else {
            command.args(["--discovery", "none"]);
        }
        drop(pg_listener);
        drop(metrics_listener);
        let child = command.spawn()?;
        let pid = child.id();
        self.state.peers.lock().push(PeerCoordinator {
            child: ChildGuard::new(child),
            pg,
            metrics,
        });
        let config = lake_config(pg, &self.lake_namespace);
        let client = tokio::time::timeout(STARTUP_TIMEOUT, async {
            loop {
                if let Ok(client) = self.connect_extra(&config, false).await {
                    break client;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("peer coordinator readiness timed out")?;
        check_metrics(metrics).await.map_err(anyhow::Error::msg)?;
        eprintln!(
            "peer coordinator ready pid={pid} pg={pg} metrics={metrics} namespace={}",
            self.lake_namespace
        );
        Ok(client)
    }
}
