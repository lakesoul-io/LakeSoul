"""TPC-H through `lakesoul-console`: the console process plans and runs the
queries itself (single node, or distributed over `--worker` URLs), with no
PG-wire server in between. One console process runs every iteration, so
in-process caches persist across iterations as in the `tpch` scenario.

The console prints each statement's result table followed by
`N row(s) fetched.` and `Elapsed S seconds.`, and prints errors to stderr
without a summary. A marker `SELECT` before every query names the query that
the following output belongs to; stdout and stderr share one pipe, so the
order of lines is preserved.
"""
from __future__ import annotations

import asyncio
import csv
import re
import sys
import tempfile
import time
from pathlib import Path

MARKER = "lsbench-marker"
MARKER_LINE = re.compile(rf"{MARKER}:(q\d\d):(-?\d+)")
ELAPSED_LINE = re.compile(r"^Elapsed ([0-9.]+) seconds\.$")
FETCHED_LINE = re.compile(r"^(\d+) row\(s\) fetched\.")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def is_text(line: str) -> bool:
    """Neither a result-table line nor the startup banner (box-drawing
    characters only)."""
    return bool(line.strip()) and line[0] not in "+|" and re.search(r"[A-Za-z0-9]", line) is not None


def statement(sql: str) -> str:
    """One `;`-terminated statement. The console runs a statement once a line
    ends with `;`; an unterminated statement at the end of a file is run as
    well, but its error aborts every later file."""
    return sql.rstrip().rstrip(";").rstrip() + ";\n"


def script(database: str, queries: list[tuple[str, str]], warmup: int, iterations: int,
           target_partitions: int | None) -> str:
    if not IDENTIFIER.fullmatch(database):
        raise ValueError(f"--database must be a plain identifier: {database!r}")
    lines = [f"USE {database};\n"]
    if target_partitions is not None:
        # Single-node console sessions default to one partition and ignore
        # the console's --target-partitions flag.
        lines.append(f"SET datafusion.execution.target_partitions = {target_partitions};\n")
    for iteration in range(-warmup, iterations):
        for name, sql in queries:
            lines.append(f"SELECT '{MARKER}:{name}:{iteration}' AS lsbench_marker;\n")
            lines.append(statement(sql))
    return "".join(lines)


def answer_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as stream:
        return sum(1 for _ in csv.reader(stream)) - 1


class Parser:
    """Turns console output lines into per-query outcomes, in order."""

    def __init__(self):
        self.marker: tuple[str, int] | None = None  # marker printed, its summary not yet
        self.pending: tuple[str, int] | None = None  # query running
        self.started = 0.0
        self.rows: int | None = None
        self.errors: list[str] = []
        self.setup_errors: list[str] = []

    def line(self, text: str, now: float) -> dict | None:
        """Feeds one line; returns a finished outcome or None."""
        text = text.rstrip("\n")
        marker = MARKER_LINE.search(text)
        if marker:
            done = self._fail(now, None)
            self.marker = (marker[1], int(marker[2]))
            return done
        elapsed = ELAPSED_LINE.match(text)
        if elapsed:
            if self.marker is not None:
                self.pending, self.marker = self.marker, None
                self.started, self.rows, self.errors = now, None, []
                return None
            if self.pending is None:  # USE / SET
                return None
            name, iteration = self.pending
            self.pending = None
            return dict(query=name, iteration=iteration, status="ok", start_unix=self.started,
                        end_unix=now, latency_s=float(elapsed[1]), rows=self.rows, error=None)
        fetched = FETCHED_LINE.match(text)
        if fetched:
            self.rows = int(fetched[1])
        elif is_text(text):
            (self.errors if self.pending else self.setup_errors).append(text.strip())
        return None

    def finish(self, now: float, exit_code: int) -> dict | None:
        """Outcome of a query cut off by the end of the output, if any."""
        if self.marker is not None and self.pending is None:
            self.pending, self.started, self.errors = self.marker, now, []
            self.marker = None
        return self._fail(now, exit_code)

    def _fail(self, now: float, exit_code: int | None) -> dict | None:
        if self.pending is None:
            return None
        name, iteration = self.pending
        self.pending = None
        error = "\n".join(self.errors) or None
        status = "error"
        if exit_code is not None:
            status = "error" if exit_code == 0 else "disconnected"
            error = "\n".join(filter(None, [error, f"lakesoul-console exited with code {exit_code}"]))
        return dict(query=name, iteration=iteration, status=status, start_unix=self.started,
                    end_unix=now, latency_s=None, rows=None, error=error)


def command(args, script_path: str) -> list[str]:
    cmd = [args.console_bin, "--worker-threads", str(args.worker_threads), "--log-dir", args.log_dir]
    for url in args.worker:
        cmd += ["--worker", url]
    if args.worker:
        if args.target_partitions is not None:
            cmd += ["--target-partitions", str(args.target_partitions)]
        if args.bytes_per_partition is not None:
            cmd += ["--bytes-per-partition", str(args.bytes_per_partition)]
    return cmd + ["--file", script_path]


async def console(args, output) -> None:
    from .cli import sample, tpch_queries

    queries = tpch_queries(args)
    expected = ({name: answer_rows(Path(args.answers) / f"{name}.csv") for name, _ in queries}
                if args.answers else {})
    text = script(args.database, queries, args.warmup, args.iterations,
                  None if args.worker else args.target_partitions)
    planned = (args.warmup + args.iterations) * len(queries)
    with tempfile.NamedTemporaryFile("w", suffix=".sql", prefix="lsbench-console-", encoding="utf-8") as file:
        file.write(text)
        file.flush()
        cmd = command(args, file.name)
        print(f"lsbench: {' '.join(cmd)}", file=sys.stderr, flush=True)
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, limit=1 << 24)
        parser = Parser()
        written = 0

        def write(outcome: dict) -> None:
            nonlocal written
            record = sample(args, outcome["query"], outcome["iteration"], 0)
            record.update(outcome, bytes=None, workers=len(args.worker),
                          target_partitions=args.target_partitions, correct=None, mismatch=None)
            want = expected.get(outcome["query"])
            if want is not None and record["status"] == "ok":
                record["correct"] = record["rows"] == want
                if not record["correct"]:
                    record["mismatch"] = f"Row count: actual={record['rows']}, expected={want}"
            output.write(record)
            written += 1

        while line := await process.stdout.readline():
            decoded = line.decode("utf-8", "replace")
            # Result tables stay out of the pod log; errors and summaries do not.
            if is_text(decoded):
                print(decoded.rstrip("\n"), file=sys.stderr, flush=True)
            if (outcome := parser.line(decoded, time.time())) is not None:
                write(outcome)
        exit_code = await process.wait()
        if (outcome := parser.finish(time.time(), exit_code)) is not None:
            write(outcome)
    if written < planned:
        output.write(dict(type="aborted", run_id=args.run_id, scenario=args.command,
                          reason=f"lakesoul-console exited with code {exit_code} after "
                                 f"{written} of {planned} queries",
                          setup_errors=parser.setup_errors))
