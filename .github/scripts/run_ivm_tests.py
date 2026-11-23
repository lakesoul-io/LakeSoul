#!/usr/bin/env python3
"""Run the LakeSoul IVM tests built with the IVM feature set.

The IVM tests use a package selection that differs from the main Rust test
steps, so a separate `cargo test` invocation must not change the feature
unification of the shared dependency graph.  `rust/third_party/sqllogictest`
keeps its `md-5` dependency aligned with `object_store`; otherwise every
`lakesoul-ivm` test build changes `md-5` and rebuilds object_store, parquet,
vortex and datafusion.

All IVM integration suites are modules of one `ivm_contracts` test binary.
The suites that need their own thread count (or a single process) are selected
by their module-name filter; the rest run together in one process.  The default
selection used on pull requests also builds the main step's PG contract target;
`--full` builds every test target for the nightly job.
"""

import json
import os
import subprocess
import sys
import tempfile

IVM_PACKAGE = "lakesoul-ivm"
IVM_TARGET = "ivm_contracts"
PLANNER_PACKAGE = "lakesoul-datafusion"
PLANNER_TARGET = "lakesoul_datafusion"
PLANNER_FILTER = "test_distributed_ivm"

# Suites that must not share a process/thread pool with the rest.  sql_oracle
# creates tables with deterministic names; coordinator_kill and
# metadata_partition are single-test suites.  sqllogic and distributed_sink
# have their own fixed parallelism.
SPECIAL = [
    ("IVM sql_oracle", "sql_oracle::", ["--test-threads=1"]),
    ("IVM sqllogic", "sqllogic::", ["--test-threads=2"]),
    ("IVM distributed_sink", "distributed_sink::", ["--test-threads=4"]),
    ("IVM coordinator_kill", "coordinator_kill::", ["--test-threads=1"]),
    ("IVM metadata_partition", "metadata_partition::", ["--test-threads=1"]),
]
REST_ARGS = ["--test-threads=4"] + [f"--skip={name[:-2]}" for _l, name, _a in SPECIAL]

# Integration test target of the main "PostgreSQL SQL compatibility" step.
# Building it activates the same dev-dependency features as the main steps
# (tokio/test-util and rust_decimal/db-tokio-postgres come from
# postgres-lakesoul's dev-dependencies).
MAIN_TESTS = ["contracts"]


def build(artifacts_path, full):
    packages = os.environ["TEST_PKGS"].split()
    targets = ["--lib", "--bins", "--tests"] if full else ["--lib", "--bins"]
    if not full:
        for target in MAIN_TESTS + [IVM_TARGET]:
            targets.extend(["--test", target])
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
        *targets,
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
    artifacts = build(os.path.join(runner_temp, "ivm-artifacts.json"), full)

    run(
        "IVM lib",
        find(artifacts, IVM_PACKAGE, "lakesoul_ivm"),
        ["--test-threads=4"],
    )
    merged = find(artifacts, IVM_PACKAGE, IVM_TARGET)
    run("IVM suites", merged, REST_ARGS)
    for label, filt, args in SPECIAL:
        run(label, merged, [filt, *args])
    run(
        "Distributed IVM planner",
        find(artifacts, PLANNER_PACKAGE, PLANNER_TARGET),
        [PLANNER_FILTER, "--test-threads=1"],
    )


if __name__ == "__main__":
    main()
