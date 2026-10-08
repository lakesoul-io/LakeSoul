// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! CLI > env > default priority tests, run through a subprocess probe.
//!
//! Clap reads environment variables while parsing; controlling them safely
//! requires re-running the parser in a child process with a private
//! environment (parallel tests must not mutate the global environment). This
//! binary is the probe and the assertions in one: the ignored `cli_probe`
//! test is re-invoked by its sibling tests against this same test executable.

use clap::Parser;
use lakesoul_datafusion::cli::{CoreArgs, DistributedArgs};
use lakesoul_datafusion::distributed::WorkerDiscovery;

#[derive(Parser)]
struct ProbeCli {
    #[command(flatten)]
    core: CoreArgs,
    #[command(flatten)]
    distributed: DistributedArgs,
}

const PROBE_FILTER: &str = "cli_probe";
const PROBE_ARGV: &str = "LAKESOUL_CLI_PROBE_ARGV";
const PROBE_RESULT_PREFIX: &str = "LAKESOUL_CLI_PROBE_RESULT ";

fn probe_result(cli: ProbeCli) -> serde_json::Value {
    let discovery = cli.distributed.resolve_discovery();
    let mut result = serde_json::json!({
        "warehouse": cli.core.warehouse_prefix,
        "endpoint": cli.core.endpoint,
        "bucket": cli.core.s3_bucket,
        "access_set": cli.core.s3_access_key.is_some(),
        "secret_set": cli.core.s3_secret_key.is_some(),
        "virtual_host": cli.core.s3_virtual_host_style,
        "worker_threads": cli.core.worker_threads,
        "workers": cli.distributed.distributed_workers,
        "fallback_local": cli.distributed.distributed_fallback_local,
        "bytes_per_partition": cli.distributed.distributed_bytes_per_partition,
        "poll_seconds": cli.distributed.discovery_poll_interval_seconds,
        "staleness_seconds": cli.distributed.discovery_max_staleness_seconds,
    });
    match discovery {
        Err(err) => result["error"] = serde_json::json!(err.to_string()),
        Ok(discovery) => {
            result["discovery"] = match discovery {
                None => serde_json::json!("single-node"),
                Some(WorkerDiscovery::Static(urls)) => {
                    serde_json::json!({ "static": urls })
                }
                Some(WorkerDiscovery::Kubernetes(discovery)) => serde_json::json!({
                    "kubernetes": {
                        "service": discovery.service,
                        "namespace": discovery.namespace,
                    }
                }),
                Some(WorkerDiscovery::Shared(_)) => serde_json::json!("shared"),
            }
        }
    }
    result
}

#[test]
#[ignore = "subprocess probe target; re-invoked by the priority tests"]
fn cli_probe() {
    let argv: Vec<String> =
        serde_json::from_str(&std::env::var(PROBE_ARGV).expect("probe argv payload"))
            .expect("probe argv json");
    let result = match ProbeCli::try_parse_from(
        std::iter::once("cli-probe".to_string()).chain(argv.clone()),
    ) {
        Ok(cli) => probe_result(cli),
        Err(err) => serde_json::json!({ "error": err.to_string() }),
    };
    println!("{}{}", PROBE_RESULT_PREFIX, result);
}

/// Runs the probe subprocess with a cleared environment plus `envs`.
fn probe(envs: &[(&str, &str)], args: &[&str]) -> serde_json::Value {
    let exe = std::env::current_exe().expect("current test executable");
    let mut command = std::process::Command::new(&exe);
    command.env_clear();
    command.env(PROBE_ARGV, serde_json::to_string(args).unwrap());
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command
        .args([PROBE_FILTER, "--ignored", "--test-threads=1", "--nocapture"])
        .output()
        .expect("probe subprocess ran");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    // libtest interleaves captured output with progress lines; the marker
    // can end up mid-line.
    let line = stdout
        .lines()
        .find_map(|line| line.split_once(PROBE_RESULT_PREFIX).map(|(_, rest)| rest));
    let Some(line) = line else {
        panic!("probe printed no result\nstdout:\n{stdout}\nstderr:\n{stderr}");
    };
    serde_json::from_str(line).expect("probe result is valid JSON")
}

#[test]
fn env_loses_to_explicit_cli_value() {
    // Per-field: an explicitly passed CLI value wins over the environment; a
    // field untouched by CLI uses the env; untouched without env uses the
    // program default.
    let result = probe(
        &[
            ("LAKESOUL_WAREHOUSE", "s3://env-bucket/warehouse"),
            ("AWS_ENDPOINT", "http://env:9000"),
            ("AWS_ACCESS_KEY_ID", "env-access-key"),
            ("AWS_BUCKET", "env-bucket"),
            ("LAKESOUL_DISTRIBUTED_DISCOVERY", "static"),
            ("LAKESOUL_DISTRIBUTED_WORKERS", "http://env-worker:1"),
            ("LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL", "true"),
            ("LAKESOUL_DISTRIBUTED_BYTES_PER_PARTITION", "99"),
            ("LAKESOUL_DISCOVERY_MAX_STALENESS_SECONDS", "77"),
        ],
        &[
            "--warehouse-prefix",
            "s3://cli-bucket/warehouse",
            "--endpoint",
            "http://cli:9000",
            "--s3-bucket",
            "cli-bucket",
            "--distributed-workers",
            "http://cli-worker:1",
            "--discovery-poll-interval-seconds",
            "60",
        ],
    );
    assert_eq!(result["warehouse"], "s3://cli-bucket/warehouse");
    assert_eq!(result["endpoint"], "http://cli:9000");
    assert_eq!(result["bucket"], "cli-bucket");
    assert_eq!(result["discovery"]["static"][0], "http://cli-worker:1");
    // Env-fed, CLI-unset fields keep their env values.
    assert_eq!(result["access_set"], true);
    assert_eq!(result["fallback_local"], true);
    assert_eq!(result["bytes_per_partition"], 99);
    assert_eq!(result["poll_seconds"], 60);
    assert_eq!(result["staleness_seconds"], 77);
}

#[test]
fn env_uses_when_cli_is_silent_and_defaults_when_no_env() {
    let with_env = probe(
        &[
            ("LAKESOUL_DISTRIBUTED_DISCOVERY", "static"),
            ("AWS_S3_VIRTUAL_HOST_STYLE", "true"),
        ],
        &[],
    );
    assert_eq!(with_env["virtual_host"], true);
    assert_eq!(with_env["discovery"]["static"], serde_json::json!([]));
    assert_eq!(with_env["poll_seconds"], 5);

    let defaults = probe(&[], &[]);
    assert_eq!(defaults["virtual_host"], false);
    assert_eq!(defaults["discovery"], "single-node");
    assert_eq!(defaults["poll_seconds"], 5);
    assert_eq!(defaults["staleness_seconds"], 30);
    assert_eq!(defaults["fallback_local"], false);
    assert_eq!(defaults["worker_threads"], 2);
}

#[test]
fn explicitly_false_cli_beats_true_env() {
    let result = probe(
        &[("LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL", "true")],
        &["--distributed-fallback-local=false"],
    );
    eprintln!("DEBUG probe: {result:?}");
    let d = probe(
        &[("LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL", "false")],
        &["--distributed-fallback-local=false"],
    );
    eprintln!("DEBUG2 env=false cli=false: {}", d["fallback_local"]);
    let d = probe(&[], &["--distributed-fallback-local=false"]);
    eprintln!("DEBUG3 no-env cli=false: {}", d["fallback_local"]);
    let d = probe(&[], &["--distributed-fallback-local", "false"]);
    eprintln!("DEBUG4 no-env cli=false split: {}", d["fallback_local"]);
    assert_eq!(
        result["fallback_local"], false,
        "an explicit (default-equal) false must beat env=true"
    );

    // And the converse: an explicit true beats env=false.
    let result = probe(
        &[("LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL", "false")],
        &["--distributed-fallback-local"],
    );
    assert_eq!(result["fallback_local"], true);
}

#[test]
fn bare_flag_still_means_true() {
    let result = probe(
        &[],
        &["--s3-virtual-host-style", "--distributed-fallback-local"],
    );
    assert_eq!(result["virtual_host"], true);
    assert_eq!(result["fallback_local"], true);
}

#[test]
fn invalid_env_or_flag_values_are_errors() {
    // Bad boolean env value: a parse error, not a silent false.
    let result = probe(&[("AWS_S3_VIRTUAL_HOST_STYLE", "maybe")], &[]);
    assert!(
        result["error"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("virtual-host"),
        "a bad virtual-host bool must fail loudly: {result}"
    );

    // Illegal poll interval via CLI.
    let result = probe(&[], &["--discovery-poll-interval-seconds", "0"]);
    assert!(
        result["error"].as_str().unwrap().contains("1..=86400"),
        "a zero poll interval must be rejected loudly: {result}"
    );

    // Mode conflict delivered through the environment only.
    let result = probe(
        &[
            ("LAKESOUL_DISTRIBUTED_DISCOVERY", "kubernetes"),
            ("LAKESOUL_DISTRIBUTED_WORKERS", "http://a:1"),
        ],
        &[],
    );
    assert!(
        result["error"].as_str().unwrap().contains("conflicts"),
        "kubernetes + static workers must fail loudly: {result}"
    );

    // Kubernetes mode missing service/namespace.
    let result = probe(&[("LAKESOUL_DISTRIBUTED_DISCOVERY", "kubernetes")], &[]);
    assert!(
        result["error"].as_str().unwrap().contains("non-empty"),
        "kubernetes without any service name must fail: {result}"
    );
}

#[test]
fn secret_values_are_not_printed_by_the_probe() {
    let secret = "s3-secret-value-8fd1";
    let result = probe(
        &[
            ("AWS_ACCESS_KEY_ID", "s3-access-value-8fd1"),
            ("AWS_SECRET_ACCESS_KEY", secret),
        ],
        &[],
    );
    let plain = result.to_string();
    assert!(
        !plain.contains(secret),
        "secrets must be summarized as a set-flag, never inlined: {plain}"
    );
    assert_eq!(result["secret_set"], true);
    assert_eq!(result["access_set"], true);
}
