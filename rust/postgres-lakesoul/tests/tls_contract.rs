// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! TLS listener contracts against the real server.
//!
//! The positive client path (`sslmode=require` through libpq and JDBC) lives
//! in `client-compat`; these cases pin the two properties a server must not
//! get wrong:
//!
//! * a configured certificate that cannot be loaded stops the process instead
//!   of letting datafusion-postgres fall back to cleartext;
//! * `--require-tls` refuses a plaintext startup with `FATAL 28000` before a
//!   session or a connection slot exists.
//!
//! The self-signed material is generated with `openssl`, the same tool the
//! client-compat runner uses, so the test has no certificate fixture to leak
//! or rotate.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use tempfile::TempDir;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);
const EXIT_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn require_tls_refuses_a_plaintext_client() -> Result<()> {
    let workspace = tempfile::tempdir().context("create TLS test directory")?;
    let (cert, key) = generate_certificate(&workspace.path().join("tls"))?;
    let port = reserve_port()?;
    let mut server = start_server(&workspace, port, &cert, &key, true)?;
    wait_until_listening(&mut server, port).await?;

    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("sql_compat")
        .dbname("default")
        .connect_timeout(Duration::from_secs(10));
    let error = match config.connect(tokio_postgres::NoTls).await {
        Ok((client, connection)) => {
            drop(client);
            tokio::spawn(async move {
                let _ = connection.await;
            });
            bail!(
                "a plaintext startup was accepted with --require-tls; server log:\n{}",
                server.log_tail()
            );
        }
        Err(error) => error,
    };
    let database = error
        .as_db_error()
        .with_context(|| format!("expected a server error, got {error:?}"))?;
    ensure!(
        database.code().code() == "28000",
        "expected 28000, got {}: {}",
        database.code().code(),
        database.message()
    );
    ensure!(
        database.severity() == "FATAL",
        "expected FATAL, got {}",
        database.severity()
    );
    ensure!(
        database.message().contains("TLS is required"),
        "unexpected message: {}",
        database.message()
    );
    eprintln!("PASS case=require-tls plaintext=28000");
    Ok(())
}

#[test]
fn broken_certificate_stops_startup_instead_of_downgrading() -> Result<()> {
    let workspace = tempfile::tempdir().context("create TLS test directory")?;
    let tls = workspace.path().join("tls");
    let (_, key) = generate_certificate(&tls)?;
    let bad_cert = tls.join("broken.crt");
    std::fs::write(&bad_cert, "this is not a PEM certificate")
        .context("write the broken certificate")?;

    let port = reserve_port()?;
    let mut server = start_server(&workspace, port, &bad_cert, &key, false)?;
    let status = server.wait_for_exit(EXIT_TIMEOUT)?;
    ensure!(
        !status.success(),
        "a broken certificate must stop the process; log:\n{}",
        server.log_tail()
    );
    let log = server.log_text();
    ensure!(
        log.contains("TLS") || log.to_lowercase().contains("certificate"),
        "the failure must name the TLS material; log:\n{log}"
    );
    eprintln!("PASS case=tls-broken-certificate exit={status}");
    Ok(())
}

#[test]
fn a_key_from_another_certificate_stops_startup() -> Result<()> {
    let workspace = tempfile::tempdir().context("create TLS test directory")?;
    let first = workspace.path().join("first");
    let second = workspace.path().join("second");
    let (first_cert, _) = generate_certificate(&first)?;
    let (_, second_key) = generate_certificate(&second)?;

    let port = reserve_port()?;
    let mut server = start_server(&workspace, port, &first_cert, &second_key, false)?;
    let status = server.wait_for_exit(EXIT_TIMEOUT)?;
    ensure!(
        !status.success(),
        "a key that does not match the certificate must stop the process; log:\n{}",
        server.log_tail()
    );
    let log = server.log_text();
    ensure!(
        log.contains("TLS") || log.to_lowercase().contains("certificate"),
        "the failure must name the TLS material; log:\n{log}"
    );
    eprintln!("PASS case=tls-key-mismatch exit={status}");
    Ok(())
}

/// A server child that is killed when the test ends, including on panic.
struct Server {
    child: Child,
    log: PathBuf,
}

impl Server {
    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn log_tail(&self) -> String {
        let text = self.log_text();
        let lines = text.lines().collect::<Vec<_>>();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().context("poll the server")? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                bail!(
                    "the server did not exit within {timeout:?}; log:\n{}",
                    self.log_tail()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn wait_until_listening(server: &mut Server, port: u16) -> Result<()> {
    let address: SocketAddr = ([127, 0, 0, 1], port).into();
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if let Some(status) = server.child.try_wait().context("poll the server")? {
            bail!(
                "the server exited before listening ({status}); log:\n{}",
                server.log_tail()
            );
        }
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "the server did not listen on {address} within {STARTUP_TIMEOUT:?}; \
                 log:\n{}",
                server.log_tail()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn start_server(
    workspace: &TempDir,
    port: u16,
    cert: &Path,
    key: &Path,
    require_tls: bool,
) -> Result<Server> {
    let log = workspace.path().join("postgres-lakesoul-TLS.log");
    let stdout = std::fs::File::create(&log)
        .with_context(|| format!("create {}", log.display()))?;
    let stderr = stdout.try_clone().context("clone the server log")?;
    let mut command = server_command(workspace, port, cert, key, require_tls);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let child = command
        .spawn()
        .with_context(|| format!("spawn {}", env!("CARGO_BIN_EXE_postgres-lakesoul")))?;
    Ok(Server { child, log })
}

fn server_command(
    workspace: &TempDir,
    port: u16,
    cert: &Path,
    key: &Path,
    require_tls: bool,
) -> Command {
    let warehouse = workspace.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).expect("create the warehouse directory");
    let mut command = Command::new(env!("CARGO_BIN_EXE_postgres-lakesoul"));
    command
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--no-metrics")
        .arg("--discovery")
        .arg("none")
        .arg("--warehouse-prefix")
        .arg(format!("file://{}", warehouse.display()))
        .args([
            "--worker-threads",
            "2",
            "--max-connections",
            "4",
            "--max-queries-per-user",
            "4",
            "--max-result-rows",
            "0",
            "--max-result-bytes",
            "0",
            "--memory-limit",
            "0",
            "--vortex-decode-budget",
            "0",
        ])
        .arg("--tls-cert")
        .arg(cert)
        .arg("--tls-key")
        .arg(key)
        .env("TZ", "UTC")
        .env_remove("LAKESOUL_DISTRIBUTED_WORKERS")
        .env_remove("LAKESOUL_K8S_WORKER_SERVICE")
        .env_remove("LAKESOUL_K8S_NAMESPACE");
    if require_tls {
        command.arg("--require-tls");
    }
    command
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("reserve a port")?;
    let port = listener
        .local_addr()
        .context("read the reserved port")?
        .port();
    drop(listener);
    Ok(port)
}

fn generate_certificate(directory: &Path) -> Result<(PathBuf, PathBuf)> {
    std::fs::create_dir_all(directory)
        .with_context(|| format!("create {}", directory.display()))?;
    let certificate = directory.join("server.crt");
    let key = directory.join("server.key");
    let certificate_text = certificate
        .to_str()
        .context("the certificate path is not UTF-8")?;
    let key_text = key.to_str().context("the key path is not UTF-8")?;
    run_openssl(&[
        "genpkey",
        "-algorithm",
        "RSA",
        "-pkeyopt",
        "rsa_keygen_bits:2048",
        "-out",
        key_text,
    ])?;
    run_openssl(&[
        "req",
        "-new",
        "-x509",
        "-key",
        key_text,
        "-out",
        certificate_text,
        "-days",
        "2",
        "-subj",
        "/CN=localhost",
    ])?;
    Ok((certificate, key))
}

fn run_openssl(arguments: &[&str]) -> Result<()> {
    let output = Command::new("openssl")
        .args(arguments)
        .output()
        .with_context(|| {
            format!("run openssl {arguments:?}; openssl is required for the TLS tests")
        })?;
    ensure!(
        output.status.success(),
        "openssl {arguments:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
