#!/usr/bin/env python3
"""Isolated, real PostgreSQL smoke; owns and cleans only its own Docker resources."""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import subprocess
import tempfile
import time
import uuid
from pathlib import Path


DRIVER = Path(__file__).resolve().parent


def docker(*args: str, timeout: float = 30, check: bool = True):
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          timeout=timeout, check=check)


def records(path: Path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def samples(path: Path):
    return [row for row in records(path) if "type" not in row]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-image", default="python:3.12-slim")
    args = parser.parse_args()
    suffix = uuid.uuid4().hex[:10]
    network = "lsbench-smoke"
    pg = f"lsbench-smoke-pg-{suffix}"
    names = []
    network_owned = False
    pg_owned = False
    # Keep fixture/output paths out of the working tree, and prevent pycache.
    with tempfile.TemporaryDirectory(prefix="lsbench-smoke-") as directory:
        work = Path(directory)
        (work / "queries").mkdir()
        (work / "answers").mkdir()
        (work / "queries/q01.sql").write_text(
            "SELECT * FROM (VALUES (1.005::numeric, 'a  '::text, DATE '2026-10-16'), "
            "(2.0::numeric, 'b'::text, NULL::date)) AS t(n, label, day)")
        (work / "queries/q06.sql").write_text("SELECT 42 AS engine_specific_label")
        (work / "queries/q11.sql").write_text("SELECT {{SF}}::numeric AS sf")
        # Reverse unordered rows, trim strings, tolerate numeric differences,
        # and deliberately use labels different from the actual query output.
        (work / "answers/q01.csv").write_text("number,text,date\n2,b,\n1,a,2026-10-16\n")
        (work / "answers/q06.csv").write_text("answer\n999\n")
        (work / "mix.json").write_text(json.dumps([
            {"name": "fast", "sql": "SELECT pg_sleep(0.1)", "weight": 2},
            {"name": "slow", "sql": "SELECT pg_sleep(0.4)", "weight": 1}]))
        (work / "timeout-mix.json").write_text(json.dumps([
            {"name": "timeout", "sql": "SELECT pg_sleep(2)", "weight": 1}]))
        (work / "sf-mix.json").write_text(json.dumps([
            {"name": "sf", "sql_file": "queries/q11.sql", "weight": 1}]))

        def runtime(name, command, *, timeout=360):
            container = f"lsbench-smoke-{name}-{suffix}"
            names.append(container)
            return docker("run", "--rm", "--name", container, "--network", network,
                          "-v", f"{DRIVER}:/d:ro", "-v", f"{work}:/work",
                          "-e", "PYTHONPATH=/d", "-e", "PYTHONDONTWRITEBYTECODE=1",
                          args.python_image, "python3", *command, timeout=timeout, check=False)

        def run(name, subcommand, *extra, expect=0):
            process = runtime(name, ["-m", "lsbench", subcommand,
                                    "--host", pg, "--user", "postgres", "--database", "postgres",
                                    "--out", f"/work/{name}.jsonl", "--run-id", name, *extra])
            require(process.returncode == expect,
                    f"{name}: exit={process.returncode}, stdout={process.stdout}, stderr={process.stderr}")
            return work / f"{name}.jsonl"

        try:
            # Failure to create (including an existing user-owned network) must
            # not set the ownership flag and must never trigger its removal.
            docker("network", "create", network)
            network_owned = True
            docker("run", "--detach", "--rm", "--name", pg, "--network", network,
                   "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:14.5")
            pg_owned = True
            for _ in range(60):
                if docker("exec", pg, "pg_isready", "-U", "postgres", check=False).returncode == 0:
                    break
                time.sleep(0.25)
            else:
                raise RuntimeError("PostgreSQL did not become ready")

            help_code = (
                "from lsbench.cli import parser\n"
                "import contextlib, io\n"
                "for command in ([], ['tpch'], ['load'], ['slowclient'], ['cancel'], ['disconnect'], ['connflood']):\n"
                " with contextlib.redirect_stdout(io.StringIO()):\n"
                "  try: parser().parse_args(command + ['--help'])\n"
                "  except SystemExit as exc: assert exc.code == 0\n"
                "print('7 help pages passed')\n")
            help_result = runtime("help", ["-c", help_code])
            require(help_result.returncode == 0, help_result.stderr)

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                slow_future = pool.submit(
                    run, "slow", "slowclient", "--sql",
                    "SELECT repeat('x',1000) FROM generate_series(1,200000)",
                    "--read-rate", "1048576")

                reference = samples(run("tpch", "tpch", "--queries", "q01,q06", "--query-dir", "/work/queries",
                                        "--answers", "/work/answers", "--iterations", "1", "--warmup", "1"))
                require([row["correct"] for row in reference] == [True, False, True, False], reference)
                require([row["iteration"] for row in reference] == [-1, -1, 0, 0], reference)
                extended = samples(run("extended", "tpch", "--queries", "q01,q06", "--query-dir", "/work/queries",
                                       "--answers", "/work/answers", "--protocol", "extended"))
                require([row["correct"] for row in extended] == [True, False], extended)
                pages = samples(run("pages", "tpch", "--queries", "q01", "--query-dir", "/work/queries",
                                    "--answers", "/work/answers", "--protocol", "extended", "--max-rows", "1"))
                require(pages[0]["correct"] is True and pages[0]["rows"] == 2, pages)
                negative = samples(run("negative", "tpch", "--queries", "q01", "--query-dir", "/work/queries",
                                       "--protocol", "extended", "--max-rows", "-2147483648"))
                require(negative[0]["max_rows"] == -2147483648, negative)
                sf = samples(run("sf", "tpch", "--queries", "q11", "--query-dir", "/work/queries", "--sf", "0.1"))
                require(sf[0]["status"] == "ok", sf)
                run("missing-sf", "tpch", "--queries", "q11", "--query-dir", "/work/queries", expect=2)

                closed = run("closed", "load", "--mode", "closed", "--concurrency", "3", "--duration", "10",
                             "--mix", "/work/mix.json", "--protocol", "extended", "--max-rows", "1")
                opened = run("open", "load", "--mode", "open", "--rate", "30", "--max-inflight", "2",
                             "--duration", "10", "--mix", "/work/mix.json", "--connect-per-query")
                require(all(row["status"] == "ok" for row in samples(closed)), "closed query errors")
                require(any(row.get("type") == "tick" for row in records(closed)), "missing closed ticks")
                open_rows = samples(opened)
                require(any(row["status"] == "client_dropped" for row in open_rows), "no overload drops")
                require(all("scheduled_unix" in row for row in open_rows), "missing scheduled arrivals")
                require(all(row["status"] in ("ok", "client_dropped") for row in open_rows), open_rows)
                timeouts = samples(run("timeouts", "load", "--mode", "closed", "--concurrency", "2",
                                       "--duration", "1", "--mix", "/work/timeout-mix.json", "--query-timeout", "0.1"))
                require(timeouts and all(row["status"] == "timeout" and row["sqlstate"] == "57014" for row in timeouts), timeouts)
                sf_load = samples(run("sf-load", "load", "--mode", "closed", "--duration", "0.1",
                                      "--mix", "/work/sf-mix.json", "--sf", "1"))
                require(sf_load and all(row["status"] == "ok" for row in sf_load), sf_load)
                cancelled = samples(run("cancel", "cancel", "--sql", "SELECT pg_sleep(30)", "--after", "0.2"))[0]
                require(cancelled["sqlstate"] == "57014" and cancelled["post_cancel_ok"] is True, cancelled)
                require(cancelled["cancel_to_error_s"] is not None and cancelled["cancel_to_ready_s"] is not None, cancelled)
                for name, extra in (("fin", []), ("rst", ["--rst"])):
                    disconnected = samples(run(name, "disconnect", "--sql", "SELECT pg_sleep(30)", "--after", "0.2", *extra))[0]
                    require(disconnected["status"] == "disconnected" and disconnected["interrupted"], disconnected)
                connections = records(run("connections", "connflood", "--connections", "50", "--rate", "100", "--hold", "0.5"))
                require(connections[-1]["connected"] == 50 and connections[-1]["errors"] == 0, connections[-1])
                short_slow = samples(run("slow-extended", "slowclient", "--sql", "SELECT generate_series(1,2000)",
                                         "--protocol", "extended", "--max-rows", "19", "--read-rate", "max",
                                         "--stall-after-bytes", "1000", "--stall-seconds", "0.2"))[0]
                require(short_slow["rows"] == 2000 and short_slow["status"] == "ok", short_slow)
                slow_file = slow_future.result()
                slow_records = records(slow_file)
                slow_sample = samples(slow_file)[0]
                require(slow_sample["status"] == "ok" and slow_sample["rows"] == 200000, slow_sample)
                tick_rows = [row for row in slow_records if row.get("type") == "tick"]
                rates = [(b["wire_bytes"] - a["wire_bytes"]) / (b["unix"] - a["unix"])
                         for a, b in zip(tick_rows, tick_rows[1:])]
                median_rate = statistics.median(rates)
                require(abs(median_rate / 1048576 - 1) < 0.15, f"Slowclient median bytes/s: {median_rate}")
                require(abs(slow_sample["wire_bytes"] / slow_sample["latency_s"] / 1048576 - 1) < 0.15,
                        f"Slowclient average rate: {slow_sample}")
                print(json.dumps(dict(help_pages=7, tpch_correct=[row["correct"] for row in reference],
                                      extended_rows=pages[0]["rows"], negative_status=negative[0]["status"],
                                      closed_completed=len(samples(closed)), open_arrivals=len(open_rows),
                                      open_dropped=sum(row["status"] == "client_dropped" for row in open_rows),
                                      timeout_samples=len(timeouts), cancel_sqlstate=cancelled["sqlstate"],
                                      post_cancel_ok=cancelled["post_cancel_ok"], connections=connections[-1]["connected"],
                                      slow_rows=slow_sample["rows"], slow_wire_bytes=slow_sample["wire_bytes"],
                                      slow_duration_s=slow_sample["latency_s"], slow_median_bytes_per_s=median_rate), indent=2))
        finally:
            # Never operate on names not created by this invocation.
            for name in names:
                docker("rm", "--force", name, check=False)
            if pg_owned:
                docker("rm", "--force", pg, check=False)
            if network_owned:
                docker("network", "rm", network, check=False)


if __name__ == "__main__":
    main()
