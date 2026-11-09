#!/usr/bin/env python3
"""Run the LakeSoul IVM test binaries built with the IVM feature set.

The IVM tests need a feature set and package selection that differ from the
main Rust test steps.  A second `cargo test` with a different target selection
makes Cargo re-unify the dev-dependency features of the whole graph (for
example `tokio/test-util` and `rust_decimal/db-tokio-postgres` come from
`postgres-lakesoul`'s dev-dependencies) and rebuild arrow, datafusion and
vortex.

Build once with the same `--lib --bins --tests` selection as the main test
steps, then run the emitted executables directly.  `--full` runs every
lakesoul-ivm test target for the nightly job; the default runs the core set
used on pull requests.
"""

import json
import os
import subprocess
import sys
import tempfile

IVM_PACKAGE = "lakesoul-ivm"
PLANNER_PACKAGE = "lakesoul-datafusion"
PLANNER_TARGET = "lakesoul_datafusion"
PLANNER_FILTER = "test_distributed_ivm"

# Fixed thread counts for the core set.  sql_oracle creates tables with
# deterministic names, so its tests must not overlap; coordinator_kill and
# metadata_partition are single-test binaries.
CORE = [
    ("IVM lib", IVM_PACKAGE, "lakesoul_ivm", ["--test-threads=4"]),
    ("IVM sql_oracle", IVM_PACKAGE, "sql_oracle", ["--test-threads=1"]),
    ("IVM sqllogic", IVM_PACKAGE, "sqllogic", ["--test-threads=2"]),
    ("IVM distributed_sink", IVM_PACKAGE, "distributed_sink", ["--test-threads=4"]),
    ("IVM coordinator_kill", IVM_PACKAGE, "coordinator_kill", ["--test-threads=1"]),
    ("IVM metadata_partition", IVM_PACKAGE, "metadata_partition", ["--test-threads=1"]),
    (
        "Distributed IVM planner",
        PLANNER_PACKAGE,
        PLANNER_TARGET,
        [PLANNER_FILTER, "--test-threads=1"],
    ),
]
CORE_ARGS = {name: args for _label, _package, name, args in CORE}


def build(artifacts_path):
    packages = os.environ["TEST_PKGS"].split()
    command = [
        "cargo",
        "test",
        "--profile",
        "test-fast",
        *packages,
        "-p",
        IVM_PACKAGE,
        "--features",
        os.environ["IVM_FEATURES"],
        "--lib",
        "--bins",
        "--tests",
        "--no-run",
        "--jobs",
        "4",
        "--message-format=json",
    ]
    print("::group::Build IVM test binaries", flush=True)
    print(" ".join(command), flush=True)
    with open(artifacts_path, "w") as output:
        subprocess.run(command, check=True, stdout=output)
    print("::endgroup::", flush=True)

    artifacts = {}
    with open(artifacts_path) as f:
        for line in f:
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if message.get("reason") != "compiler-artifact":
                continue
            executable = message.get("executable")
            if not executable:
                continue
            target = message.get("target", {})
            key = (
                message.get("package_id", ""),
                target.get("name", ""),
                tuple(target.get("kind", [])),
            )
            artifacts[key] = executable
    return artifacts


def find(artifacts, package, target):
    for (package_id, name, _kind), executable in artifacts.items():
        if package in package_id and name == target:
            return executable
    raise SystemExit(f"missing test executable: {package} {target}")


def run(label, executable, args):
    print(f"::group::{label}", flush=True)
    subprocess.run([executable, *args], check=True)
    print("::endgroup::", flush=True)


def main():
    full = "--full" in sys.argv[1:]
    runner_temp = os.environ.get("RUNNER_TEMP", tempfile.gettempdir())
    artifacts = build(os.path.join(runner_temp, "ivm-artifacts.json"))

    if not full:
        for label, package, target, args in CORE:
            run(label, find(artifacts, package, target), args)
        return

    ran = set()
    for (package_id, name, kind), executable in sorted(
        artifacts.items(), key=lambda item: item[0][1]
    ):
        if IVM_PACKAGE not in package_id or name in ran:
            continue
        if "test" not in kind and "lib" not in kind:
            continue
        ran.add(name)
        run(f"IVM {name}", executable, CORE_ARGS.get(name, []))
    run(
        "Distributed IVM planner",
        find(artifacts, PLANNER_PACKAGE, PLANNER_TARGET),
        [PLANNER_FILTER, "--test-threads=1"],
    )


if __name__ == "__main__":
    main()
