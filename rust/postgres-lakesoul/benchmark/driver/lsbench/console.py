"""TPC-H through `lakesoul-console`: the console process plans and runs the
queries itself (single node, or distributed over `--worker` URLs), with no
PG-wire server in between. One console process runs every iteration, so
in-process caches persist across iterations as in the `tpch` scenario.

The console runs with `--format json`: per statement a
`{"columns":[{"name":..,"type":..}]}` line and one JSON array of display
strings per row, then `N row(s) fetched.` and `Elapsed S seconds.`; errors go
to stderr without a summary. A marker `SELECT` before every query names the
query that the following output belongs to; stdout and stderr share one pipe,
so the order of lines is preserved.

The console also samples its own memory (`--memory-sample-file`): process
RSS, jemalloc's allocated/active/resident bytes and the execution pool's
reservation, taken together at each sample. Every query record carries the
samples taken while it ran as `memory` (see `MemoryTimeline`).
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import tempfile
import time
from pathlib import Path

from .answers import compare
from .wire import Result

MARKER = "lsbench-marker"
MARKER_LINE = re.compile(rf"{MARKER}:(q\d\d):(-?\d+)")
ELAPSED_LINE = re.compile(r"^Elapsed ([0-9.]+) seconds\.$")
FETCHED_LINE = re.compile(r"^(\d+) row\(s\) fetched\.")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
NUMERIC_TYPE = re.compile(r"(Int|UInt|Float|Decimal)\d")


def is_text(line: str) -> bool:
    """Neither result output (JSON lines, or table lines of the default
    format) nor the startup banner (box-drawing characters only)."""
    return (
        bool(line.strip())
        and line[0] not in "+|[{"
        and re.search(r"[A-Za-z0-9]", line) is not None
    )


def statement(sql: str) -> str:
    """One `;`-terminated statement. The console runs a statement once a line
    ends with `;`; an unterminated statement at the end of a file is run as
    well, but its error aborts every later file."""
    return sql.rstrip().rstrip(";").rstrip() + ";\n"


def script(
    database: str,
    queries: list[tuple[str, str]],
    warmup: int,
    iterations: int,
    target_partitions: int | None,
) -> str:
    if not IDENTIFIER.fullmatch(database):
        raise ValueError(f"--database must be a plain identifier: {database!r}")
    lines = [f"USE {database};\n"]
    if target_partitions is not None:
        # Single-node console sessions default to one partition and ignore
        # the console's --target-partitions flag.
        lines.append(
            f"SET datafusion.execution.target_partitions = {target_partitions};\n"
        )
    for iteration in range(-warmup, iterations):
        for name, sql in queries:
            lines.append(f"SELECT '{MARKER}:{name}:{iteration}' AS lsbench_marker;\n")
            lines.append(statement(sql))
    return "".join(lines)


class Parser:
    """Turns console output lines into per-query outcomes, in order. With
    `keep_values`, an ok outcome carries the result as `columns`, `types` and
    `values`."""

    def __init__(self, keep_values: bool = False):
        self.keep_values = keep_values
        self.marker: tuple[str, int] | None = (
            None  # marker printed, its summary not yet
        )
        self.pending: tuple[str, int] | None = None  # query running
        self.started = 0.0
        self.rows: int | None = None
        self.columns: list[str] = []
        self.types: list[str] = []
        self.values: list[tuple[str | None, ...]] = []
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
                self.columns, self.types, self.values = [], [], []
                return None
            if self.pending is None:  # USE / SET
                return None
            name, iteration = self.pending
            self.pending = None
            outcome = dict(
                query=name,
                iteration=iteration,
                status="ok",
                start_unix=self.started,
                end_unix=now,
                latency_s=float(elapsed[1]),
                rows=self.rows,
                error=None,
            )
            if self.keep_values:
                outcome.update(
                    columns=self.columns, types=self.types, values=self.values
                )
            return outcome
        if self.pending is not None and text[:1] in ("{", "["):
            try:
                decoded = json.loads(text)
            except ValueError:
                decoded = None
            if isinstance(decoded, dict) and "columns" in decoded:
                self.columns = [c["name"] for c in decoded["columns"]]
                self.types = [c["type"] for c in decoded["columns"]]
                return None
            if isinstance(decoded, list):
                if self.keep_values:
                    self.values.append(tuple(decoded))
                return None
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
            error = "\n".join(
                filter(None, [error, f"lakesoul-console exited with code {exit_code}"])
            )
        return dict(
            query=name,
            iteration=iteration,
            status=status,
            start_unix=self.started,
            end_unix=now,
            latency_s=None,
            rows=None,
            error=error,
        )


MEMORY_KEYS = (
    "rss_bytes",
    "rss_anon_bytes",
    "jemalloc_allocated_bytes",
    "jemalloc_active_bytes",
    "jemalloc_resident_bytes",
    "pool_reserved_bytes",
)


class MemoryTimeline:
    """Follows the console's memory sample file (one JSON object per line,
    `ts` in Unix seconds) and summarizes the samples of a time window."""

    def __init__(self, path: str):
        self.path = path
        self.offset = 0
        self.partial = ""
        self.samples: list[dict] = []

    def poll(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                f.seek(self.offset)
                chunk = f.read()
                self.offset = f.tell()
        except FileNotFoundError:
            return
        lines = (self.partial + chunk).split("\n")
        self.partial = lines.pop()  # unterminated: the sampler is mid-write
        for line in lines:
            try:
                sample = json.loads(line)
            except ValueError:
                continue
            if isinstance(sample, dict) and isinstance(sample.get("ts"), (int, float)):
                self.samples.append(sample)

    def window(self, start: float, end: float) -> dict | None:
        """Per-view peaks over the samples in [start, end], and the whole
        sample at the RssAnon peak, where the views are comparable; None
        without samples. Samples before `start` are dropped: queries run in
        order, so no later window needs them."""
        self.poll()
        self.samples = [s for s in self.samples if s["ts"] >= start]
        inside = [s for s in self.samples if s["ts"] <= end]
        if not inside:
            return None

        def peak(key: str) -> int | None:
            values = [s[key] for s in inside if isinstance(s.get(key), int)]
            return max(values) if values else None

        at_peak = max(inside, key=lambda s: s.get("rss_anon_bytes") or 0)
        return dict(
            samples=len(inside),
            peak={k: peak(k) for k in MEMORY_KEYS},
            at_rss_anon_peak={k: at_peak.get(k) for k in ("ts", *MEMORY_KEYS)},
        )


def check(outcome: dict, answer: Path, sql: str) -> tuple[bool, str | None]:
    """`answers.compare` with the rules of the `tpch` scenario; numeric
    tolerance applies to integer, float and decimal columns."""
    oids = [1700 if NUMERIC_TYPE.match(t) else 25 for t in outcome["types"]]
    result = Result(
        columns=outcome["columns"],
        type_oids=oids,
        values=outcome["values"],
        rows=len(outcome["values"]),
    )
    return compare(result, answer, sql)


def command(args, script_path: str, memory_path: str) -> list[str]:
    cmd = [
        args.console_bin,
        "--format",
        "json",
        "--worker-threads",
        str(args.worker_threads),
        "--log-dir",
        args.log_dir,
        "--memory-sample-file",
        memory_path,
        "--memory-sample-interval-ms",
        str(args.memory_sample_interval_ms),
    ]
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
    sql = dict(queries)
    text = script(
        args.database,
        queries,
        args.warmup,
        args.iterations,
        None if args.worker else args.target_partitions,
    )
    planned = (args.warmup + args.iterations) * len(queries)
    with (
        tempfile.NamedTemporaryFile(
            "w", suffix=".sql", prefix="lsbench-console-", encoding="utf-8"
        ) as file,
        tempfile.TemporaryDirectory(prefix="lsbench-console-") as scratch,
    ):
        file.write(text)
        file.flush()
        memory = MemoryTimeline(str(Path(scratch) / "memory.jsonl"))
        cmd = command(args, file.name, memory.path)
        print(f"lsbench: {' '.join(cmd)}", file=sys.stderr, flush=True)
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1 << 24,
        )
        parser = Parser(keep_values=bool(args.answers))
        written = 0

        def write(outcome: dict) -> None:
            nonlocal written
            result = {
                k: outcome.pop(k)
                for k in ("columns", "types", "values")
                if k in outcome
            }
            record = sample(args, outcome["query"], outcome["iteration"], 0)
            record.update(
                outcome,
                bytes=None,
                workers=len(args.worker),
                target_partitions=args.target_partitions,
                correct=None,
                mismatch=None,
                memory=memory.window(outcome["start_unix"], outcome["end_unix"]),
            )
            if args.answers and record["status"] == "ok":
                record["correct"], record["mismatch"] = check(
                    result,
                    Path(args.answers) / f"{outcome['query']}.csv",
                    sql[outcome["query"]],
                )
            output.write(record)
            written += 1

        while line := await process.stdout.readline():
            decoded = line.decode("utf-8", "replace")
            # Result rows stay out of the pod log; errors and summaries do not.
            if is_text(decoded):
                print(decoded.rstrip("\n"), file=sys.stderr, flush=True)
            if (outcome := parser.line(decoded, time.time())) is not None:
                write(outcome)
        exit_code = await process.wait()
        if (outcome := parser.finish(time.time(), exit_code)) is not None:
            write(outcome)
    if written < planned:
        output.write(
            dict(
                type="aborted",
                run_id=args.run_id,
                scenario=args.command,
                reason=f"lakesoul-console exited with code {exit_code} after "
                f"{written} of {planned} queries",
                setup_errors=parser.setup_errors,
            )
        )
