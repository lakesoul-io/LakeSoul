"""Strict TPC-H gate over the real PostgreSQL-wire service.

This is the workload-level TPC-H regression runner, not a general SQL
compatibility comparator. It reuses the checked-in TPC-H queries and the
DuckDB reference CSVs together with their documented numeric tolerance and
``ORDER BY`` tie rules. Its strictness is about execution accounting: every
selected query must reach a complete PostgreSQL response, compare as expected,
and be present in the final report, or the process exits nonzero.

The timing/sampling driver in :mod:`lsbench.cli` is deliberately unchanged.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import re
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Sequence

from .answers import compare
from .wire import Client, PGError, Result

BENCHMARK_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_QUERIES_DIR = BENCHMARK_ROOT / "workloads" / "tpch" / "queries"
DEFAULT_ANSWERS_ROOT = BENCHMARK_ROOT / "workloads" / "tpch" / "answers"

SCALES = ("0.1", "1", "10")
LAYOUTS = ("pk", "nopk", "pkmonth")
PROTOCOLS = ("simple", "extended")
# One canonical name per query; the CLI validates an explicit --queries list
# against the same set instead of silently skipping an unknown or missing file.
DEFAULT_QUERIES = tuple(f"q{number:02d}" for number in range(1, 23))

GENERATOR = "tpchgen-rs@46f3dca"
REFERENCE_ANSWERS = "DuckDB 1.5.6 over tpchgen-rs Parquet"
COMPARISON = (
    "numeric absolute <= 0.01 or relative <= 1e-6; ORDER BY ties compared "
    "per equal-sort-key group, multiset otherwise"
)

# Fixed workload execution width, independent of server defaults and of the
# standalone/distributed mode under test.
TARGET_PARTITIONS = 4

# Session setup is not a TPC-H query. A deliberately tiny --query-timeout
# negative fixture must expire on the selected query, not while applying the
# session environment, so setup keeps its own lower bound.
SESSION_TIMEOUT_S = 30.0

# Deterministic session environment. `statement_timeout` is appended with the
# requested client-side timeout so the server and client bound the same query.
SESSION_SETTINGS = (
    "SET TIME ZONE 'UTC'",
    "SET DateStyle = 'ISO, YMD'",
    "SET client_encoding = 'UTF8'",
    f"SET datafusion.execution.target_partitions = {TARGET_PARTITIONS}",
)
COMMAND = re.compile(r"SELECT (\d+)\Z", re.IGNORECASE)
TEMPLATE = re.compile(r"\{\{[^{}]*\}\}")


@dataclass(frozen=True)
class Problem:
    """A stable preflight cause, separate from its human-readable detail."""

    code: str
    detail: str
    query: str | None = None
    path: str | None = None

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "query": self.query,
            "path": self.path,
            "detail": self.detail,
        }

    def __str__(self) -> str:
        return self.detail


class PreflightError(Exception):
    """One or more selected query/answer inputs are missing or malformed."""

    def __init__(self, problems: Sequence[Problem]):
        self.problems = tuple(problems)
        super().__init__("; ".join(str(problem) for problem in self.problems))


@dataclass(frozen=True)
class QuerySpec:
    name: str
    sql: str
    answer: Path


@dataclass
class Outcome:
    name: str
    status: str
    stage: str | None = None
    reason: str | None = None
    sqlstate: str | None = None
    rows: int = 0
    elapsed_ms: float | None = None
    attempted: bool = False
    complete: bool = False

    def as_dict(self) -> dict:
        return dict(
            name=self.name,
            status=self.status,
            stage=self.stage,
            category=(stage_category(self.stage) if self.status != "PASS" else None),
            reason=self.reason,
            sqlstate=self.sqlstate,
            rows=self.rows,
            elapsed_ms=self.elapsed_ms,
            attempted=self.attempted,
            complete=self.complete,
        )


# Coarse negative categories for the final summary. Detailed `stage` values are
# kept per query so callers do not have to infer the wire-level reason.
STAGE_CATEGORIES = {
    "server_error": "query",
    "protocol": "query",
    "transport": "incomplete",
    "command": "query",
    "shape": "query",
    "accounting": "query",
    "client": "incomplete",
    "mismatch": "compare",
    "answer": "compare",
    "timeout": "incomplete",
    "incomplete": "incomplete",
    "unattempted": "incomplete",
}


def stage_category(stage: str | None) -> str | None:
    return STAGE_CATEGORIES.get(stage) if stage is not None else None


def select_queries(requested: Sequence[str] | None) -> list[str]:
    """Canonical selected query names; ``None`` means the complete suite."""
    if requested is None:
        return list(DEFAULT_QUERIES)
    names = list(requested)
    if not names:
        raise ValueError("query selection is empty")
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"duplicate query selection: {', '.join(duplicates)}")
    unknown = sorted(set(names) - set(DEFAULT_QUERIES))
    if unknown:
        raise ValueError(f"unknown query selection: {', '.join(unknown)}")
    return sorted(names)


def scale_literal(scale: str) -> str:
    """A finite PostgreSQL float literal for the Q11 template substitution."""
    try:
        value = Decimal(scale)
    except InvalidOperation as exc:
        raise ValueError(f"invalid scale factor {scale!r}") from exc
    if not value.is_finite():
        raise ValueError(f"scale factor is not finite: {scale!r}")
    try:
        literal = repr(float(value))
        finite = math.isfinite(float(value))
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"scale factor is not representable: {scale!r}") from exc
    if not finite or literal.lower() in ("nan", "inf", "-inf"):
        raise ValueError(f"scale factor is not a finite float: {scale!r}")
    return literal


def render_scale(sql: str, scale: str) -> str:
    """Replace ``{{SF}}`` with an exact float literal; no other rewrite."""
    rendered = sql.replace("{{SF}}", scale_literal(scale))
    leftover = TEMPLATE.search(rendered)
    if leftover is not None:
        raise ValueError(f"unrendered template placeholder {leftover.group(0)!r}")
    return rendered


def answer_problem(path: Path, name: str) -> Problem | None:
    """Structural RFC4180 check; comparison itself stays in answers.compare."""
    if not path.is_file():
        return Problem("missing_answer", f"missing answer {path}", name, str(path))
    try:
        with path.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.reader(stream, strict=True))
    except (OSError, UnicodeError, csv.Error) as exc:
        return Problem(
            "unreadable_answer", f"unreadable answer {path}: {exc}", name, str(path)
        )
    if not rows:
        return Problem("empty_answer", f"empty answer {path}", name, str(path))
    width = len(rows[0])
    if width == 0:
        return Problem(
            "empty_answer_header", f"empty answer header {path}", name, str(path)
        )
    for number, row in enumerate(rows[1:], start=2):
        if len(row) != width:
            return Problem(
                "malformed_answer",
                f"malformed answer {path}: row {number} has {len(row)} "
                f"fields, header has {width}",
                name,
                str(path),
            )
    return None


def build_suite(
    queries_dir: Path, answers_dir: Path, names: Sequence[str], scale: str
) -> list[QuerySpec]:
    """Read and validate every selected query and answer before connecting."""
    scale_literal(scale)  # Reject a bad scale once, not per selected query.
    problems: list[Problem] = []
    specs: list[QuerySpec] = []
    for name in names:
        query_path = queries_dir / f"{name}.sql"
        answer_path = answers_dir / f"{name}.csv"
        sql: str | None = None
        try:
            sql = query_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            problems.append(
                Problem(
                    "missing_query",
                    f"missing query {query_path}",
                    name,
                    str(query_path),
                )
            )
        except (OSError, UnicodeError) as exc:
            problems.append(
                Problem(
                    "unreadable_query",
                    f"unreadable query {query_path}: {exc}",
                    name,
                    str(query_path),
                )
            )
        if sql is not None:
            if not sql.strip():
                problems.append(
                    Problem(
                        "empty_query",
                        f"empty query {query_path}",
                        name,
                        str(query_path),
                    )
                )
                sql = None
            else:
                try:
                    sql = render_scale(sql, scale)
                except ValueError as exc:
                    problems.append(
                        Problem(
                            "render_query",
                            f"cannot render {query_path}: {exc}",
                            name,
                            str(query_path),
                        )
                    )
                    sql = None
        reference_problem = answer_problem(answer_path, name)
        if reference_problem is not None:
            problems.append(reference_problem)
        if sql is not None and reference_problem is None:
            specs.append(QuerySpec(name=name, sql=sql, answer=answer_path))
    if problems:
        raise PreflightError(problems)
    return specs


def protocol_problem(result: Result) -> tuple[str, str] | None:
    """First reason a returned result is not a complete SELECT response."""
    if result.sqlstate == "57014":
        detail = result.error or "statement cancelled"
        return "timeout", f"{detail} (SQLSTATE {result.sqlstate})"
    if result.error is not None or result.sqlstate is not None:
        detail = result.error or "server returned an error without a message"
        if result.sqlstate:
            detail = f"{detail} (SQLSTATE {result.sqlstate})"
        return "server_error", detail
    if result.ready_at is None:
        return "incomplete", "ReadyForQuery was not received"
    if result.command is None:
        return "incomplete", "CommandComplete was not received"
    tag = COMMAND.fullmatch(result.command)
    if tag is None:
        return "command", f"unexpected command tag {result.command!r}"
    if result.rowdesc_s is None:
        return "shape", "RowDescription was not received"
    if not result.columns:
        return "shape", "result has no columns"
    if len(result.type_oids) != len(result.columns):
        return (
            "shape",
            f"{len(result.type_oids)} type OIDs for {len(result.columns)} columns",
        )
    if result.rows != len(result.values):
        return (
            "accounting",
            f"wire counted {result.rows} rows but collected {len(result.values)}",
        )
    for number, row in enumerate(result.values, start=1):
        if len(row) != len(result.columns):
            return (
                "shape",
                f"row {number} has {len(row)} fields for {len(result.columns)} columns",
            )
    if int(tag.group(1)) != result.rows:
        return (
            "accounting",
            f"CommandComplete reports {tag.group(1)} rows, wire counted {result.rows}",
        )
    return None


def judge(spec: QuerySpec, result: Result, elapsed_ms: float | None = None) -> Outcome:
    """Classify one complete execution against its checked-in reference."""
    problem = protocol_problem(result)
    if problem is not None:
        stage, reason = problem
        return Outcome(
            name=spec.name,
            status="FAIL",
            stage=stage,
            reason=reason,
            sqlstate=result.sqlstate,
            rows=result.rows,
            elapsed_ms=elapsed_ms,
            attempted=True,
            complete=result.ready_at is not None,
        )
    try:
        matched, reason = compare(result, spec.answer, spec.sql)
    except (OSError, UnicodeError, csv.Error) as exc:
        return Outcome(
            name=spec.name,
            status="FAIL",
            stage="answer",
            reason=f"cannot read {spec.answer}: {exc}",
            rows=result.rows,
            elapsed_ms=elapsed_ms,
            attempted=True,
            complete=True,
        )
    if not matched:
        return Outcome(
            name=spec.name,
            status="FAIL",
            stage="mismatch",
            reason=reason or "result did not match the reference CSV",
            rows=result.rows,
            elapsed_ms=elapsed_ms,
            attempted=True,
            complete=True,
        )
    return Outcome(
        name=spec.name,
        status="PASS",
        rows=result.rows,
        elapsed_ms=elapsed_ms,
        attempted=True,
        complete=True,
    )


def summarize(
    scale: str,
    layout: str,
    protocol: str,
    expected_names: Sequence[str],
    outcomes: Sequence[Outcome],
    *,
    error: str | None = None,
    problems: Sequence[Problem | str] = (),
    category: str | None = None,
    cause: str | None = None,
) -> dict:
    """Explicit accounting for the selected set; ``ok`` is the exit decision."""
    problems = tuple(problems)
    expected = len(expected_names)
    attempted = sum(1 for outcome in outcomes if outcome.attempted)
    completed = sum(1 for outcome in outcomes if outcome.complete)
    matched = sum(1 for outcome in outcomes if outcome.status == "PASS")
    identities_match = (
        expected > 0
        and len(set(expected_names)) == expected
        and sorted(outcome.name for outcome in outcomes) == sorted(expected_names)
    )
    problem_dicts = [
        problem.as_dict()
        if isinstance(problem, Problem)
        else {"code": "preflight", "query": None, "path": None, "detail": str(problem)}
        for problem in problems
    ]
    if category is None:
        if problems:
            category = "preflight"
        elif error is not None:
            category = "setup"
    failure_categories = {category} if category is not None else set()
    causes = {problem["code"] for problem in problem_dicts}
    if cause is not None:
        causes.add(cause)
    for outcome in outcomes:
        outcome_category = stage_category(outcome.stage)
        if outcome.status != "PASS" and outcome_category is not None:
            failure_categories.add(outcome_category)
        if outcome.status != "PASS" and outcome.stage is not None:
            causes.add(outcome.stage)
    if cause is None:
        cause = problem_dicts[0]["code"] if problem_dicts else None
    if cause is None:
        for outcome in outcomes:
            if outcome.status != "PASS" and outcome.stage is not None:
                cause = outcome.stage
                break
    if not identities_match and error is None and not problems:
        failure_categories.add("incomplete")
        causes.add("selection_accounting")
        if cause is None:
            cause = "selection_accounting"
        error = "execution records do not cover the selected queries exactly once"
    primary = category
    if primary is None:
        for candidate in ("incomplete", "query", "compare"):
            if candidate in failure_categories:
                primary = candidate
                break
    summary = dict(
        suite="tpch-strict",
        scale=scale,
        layout=layout,
        protocol=protocol,
        generator=GENERATOR,
        reference=REFERENCE_ANSWERS,
        comparison=COMPARISON,
        target_partitions=TARGET_PARTITIONS,
        expected=expected,
        attempted=attempted,
        completed=completed,
        matched=matched,
        failed=attempted - matched,
        unmatched=expected - matched,
        unattempted=expected - attempted,
        ok=(
            identities_match
            and error is None
            and not problems
            and expected == attempted == completed == matched
        ),
        category=primary,
        cause=cause,
        causes=sorted(causes),
        failure_categories=sorted(failure_categories),
        error=error,
        problems=problem_dicts,
        queries=[outcome.as_dict() for outcome in outcomes],
    )
    return summary


def flatten(value: object, limit: int = 500) -> str:
    text = " ".join(str(value).split())
    return text if len(text) <= limit else text[:limit] + "..."


def format_outcome(outcome: Outcome) -> str:
    if outcome.status == "PASS":
        head = f"PASS {outcome.name} rows={outcome.rows}"
    else:
        head = (
            f"{outcome.status} {outcome.name} "
            f"category={stage_category(outcome.stage) or 'unknown'} "
            f"stage={outcome.stage or 'unknown'} "
            f"sqlstate={outcome.sqlstate or '-'} rows={outcome.rows}"
        )
    if outcome.elapsed_ms is not None:
        head += f" elapsed_ms={outcome.elapsed_ms:.3f}"
    if outcome.reason:
        head += f" reason={flatten(outcome.reason)}"
    return head


def print_summary(summary: dict, json_only: bool) -> None:
    rendered = json.dumps(summary, sort_keys=True)
    if json_only:
        print(rendered, flush=True)
        return
    print(
        f"TPC-H strict: sf={summary['scale']} layout={summary['layout']} "
        f"protocol={summary['protocol']} generator={GENERATOR} "
        f"target_partitions={summary['target_partitions']}",
        flush=True,
    )
    print(
        f"expected={summary['expected']} attempted={summary['attempted']} "
        f"completed={summary['completed']} matched={summary['matched']} "
        f"failed={summary['failed']} unmatched={summary['unmatched']} "
        f"unattempted={summary['unattempted']}",
        flush=True,
    )
    category = summary["category"] or "-"
    categories = ",".join(summary["failure_categories"]) or "-"
    print(
        f"RESULT {'PASS' if summary['ok'] else 'FAIL'} "
        f"category={category} failure_categories={categories}",
        flush=True,
    )
    print(f"SUMMARY_JSON {rendered}", flush=True)


def positive_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return number


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="python3 -m lsbench.strict_tpch",
        description=(
            "Strict TPC-H gate: execute checked-in queries over a real "
            "LakeSoul PostgreSQL-wire service and fail on any mismatch, "
            "server error, missing input, or incomplete execution."
        ),
    )
    result.add_argument("--host", default="127.0.0.1")
    result.add_argument("--port", type=int, required=True)
    result.add_argument("--user", default="sql_compat")
    result.add_argument(
        "--database",
        required=True,
        help="existing LakeSoul namespace containing the TPC-H tables",
    )
    result.add_argument(
        "--scale",
        choices=SCALES,
        default="0.1",
        help="TPC-H scale factor; selects answers/sf<scale> by default",
    )
    result.add_argument(
        "--layout",
        choices=LAYOUTS,
        default="pk",
        help="fixture layout reported in the summary",
    )
    result.add_argument("--protocol", choices=PROTOCOLS, default="simple")
    result.add_argument(
        "--queries",
        nargs="+",
        choices=DEFAULT_QUERIES,
        metavar="qNN",
        help="explicit selected set; default is all q01..q22",
    )
    result.add_argument(
        "--queries-dir",
        type=Path,
        default=None,
        help=f"SQL directory (default: {DEFAULT_QUERIES_DIR})",
    )
    result.add_argument(
        "--answers-dir",
        type=Path,
        default=None,
        help="CSV directory (default: answers/sf<scale>)",
    )
    result.add_argument(
        "--query-timeout",
        dest="query_timeout",
        type=positive_float,
        default=60.0,
        help="per-query timeout in seconds (default: 60)",
    )
    result.add_argument(
        "--connect-timeout",
        type=positive_float,
        default=10.0,
        help="connection establishment timeout in seconds",
    )
    result.add_argument(
        "--json", action="store_true", help="emit only the final JSON summary on stdout"
    )
    return result


def resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    queries_dir = (
        args.queries_dir if args.queries_dir is not None else DEFAULT_QUERIES_DIR
    )
    answers_dir = (
        args.answers_dir
        if args.answers_dir is not None
        else DEFAULT_ANSWERS_ROOT / f"sf{args.scale}"
    )
    return queries_dir, answers_dir


def timeout_millis(timeout: float) -> int:
    return max(1, int(round(timeout * 1000)))


def session_problem(statement: str, result: Result) -> str | None:
    if result.error is not None or result.sqlstate is not None:
        detail = result.error or "server returned an error without a message"
        if result.sqlstate:
            detail = f"{detail} (SQLSTATE {result.sqlstate})"
        return f"{statement}: {detail}"
    if result.ready_at is None:
        return f"{statement}: ReadyForQuery was not received"
    if result.command is None or result.command.upper() != "SET":
        return f"{statement}: unexpected command tag {result.command!r}"
    return None


async def apply_session(client: Client, query_timeout: float) -> str | None:
    statements = SESSION_SETTINGS + (
        f"SET statement_timeout = '{timeout_millis(query_timeout)}ms'",
    )
    deadline = max(query_timeout, SESSION_TIMEOUT_S)
    for statement in statements:
        async with asyncio.timeout(deadline):
            result = await client.query(statement, collect=True, protocol="simple")
        problem = session_problem(statement, result)
        if problem is not None:
            return problem
    return None


async def execute_one(
    client: Client, spec: QuerySpec, protocol: str, query_timeout: float
) -> tuple[Outcome, bool]:
    """Execute once; the bool reports whether the connection remains usable."""
    started = time.monotonic()
    try:
        async with asyncio.timeout(query_timeout):
            result = await client.query(spec.sql, collect=True, protocol=protocol)
    except TimeoutError:
        elapsed_ms = (time.monotonic() - started) * 1000
        return (
            Outcome(
                name=spec.name,
                status="FAIL",
                stage="timeout",
                reason=f"no complete response within {query_timeout:g}s",
                elapsed_ms=elapsed_ms,
                attempted=True,
                complete=False,
            ),
            False,
        )
    except PGError as exc:
        elapsed_ms = (time.monotonic() - started) * 1000
        return (
            Outcome(
                name=spec.name,
                status="FAIL",
                stage="protocol",
                reason=flatten(exc),
                sqlstate=exc.sqlstate,
                elapsed_ms=elapsed_ms,
                attempted=True,
                complete=False,
            ),
            False,
        )
    except (ConnectionError, OSError) as exc:
        elapsed_ms = (time.monotonic() - started) * 1000
        return (
            Outcome(
                name=spec.name,
                status="FAIL",
                stage="transport",
                reason=flatten(exc),
                elapsed_ms=elapsed_ms,
                attempted=True,
                complete=False,
            ),
            False,
        )
    except Exception as exc:  # Unexpected client failure: stop the sequence.
        elapsed_ms = (time.monotonic() - started) * 1000
        return (
            Outcome(
                name=spec.name,
                status="FAIL",
                stage="client",
                reason=flatten(f"{type(exc).__name__}: {exc}"),
                elapsed_ms=elapsed_ms,
                attempted=True,
                complete=False,
            ),
            False,
        )
    elapsed_ms = (time.monotonic() - started) * 1000
    return judge(spec, result, elapsed_ms), True


async def execute_suite(
    args: argparse.Namespace, specs: Sequence[QuerySpec], emit: Callable[[str], None]
) -> tuple[list[Outcome], str | None, str | None]:
    """One connection, one pass, no retries/reconnects/sampling."""
    client = Client(
        args.host,
        args.port,
        args.user,
        args.database,
        connect_timeout=args.connect_timeout,
    )
    healthy = True
    try:
        try:
            await client.connect()
        except Exception as exc:
            healthy = False
            detail = flatten(exc)
            if getattr(exc, "sqlstate", None):
                detail += f" (SQLSTATE {exc.sqlstate})"
            return [], f"connect failed: {detail}", "connect_failed"
        try:
            problem = await apply_session(client, args.query_timeout)
        except Exception as exc:
            healthy = False
            detail = flatten(exc)
            if getattr(exc, "sqlstate", None):
                detail += f" (SQLSTATE {exc.sqlstate})"
            return [], f"session setup failed: {detail}", "session_failed"
        if problem is not None:
            return [], f"session setup failed: {problem}", "session_failed"
        outcomes: list[Outcome] = []
        for spec in specs:
            if not healthy:
                outcome = Outcome(
                    name=spec.name,
                    status="SKIPPED",
                    stage="unattempted",
                    reason="an earlier query left the connection unusable",
                )
                outcomes.append(outcome)
                emit(format_outcome(outcome))
                continue
            outcome, healthy = await execute_one(
                client, spec, args.protocol, args.query_timeout
            )
            outcomes.append(outcome)
            emit(format_outcome(outcome))
        return outcomes, None, None
    finally:
        if client.sock is not None:
            if healthy:
                try:
                    await client.terminate()
                except Exception:
                    client.close(abrupt=True)
            else:
                client.close(abrupt=True)


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    json_only = args.json

    def emit(text: str) -> None:
        if not json_only:
            print(text, flush=True)

    try:
        names = select_queries(args.queries)
    except ValueError as exc:
        print(f"strict TPC-H: {exc}", file=sys.stderr)
        summary = summarize(
            args.scale,
            args.layout,
            args.protocol,
            [],
            [],
            error=f"invalid query selection: {flatten(exc)}",
            category="preflight",
            cause="invalid_selection",
        )
        print_summary(summary, json_only)
        return 2

    queries_dir, answers_dir = resolve_paths(args)
    try:
        specs = build_suite(queries_dir, answers_dir, names, args.scale)
    except ValueError as exc:
        print(f"strict TPC-H: {exc}", file=sys.stderr)
        summary = summarize(
            args.scale,
            args.layout,
            args.protocol,
            names,
            [],
            error=f"invalid configuration: {flatten(exc)}",
            category="preflight",
            cause="invalid_configuration",
        )
        print_summary(summary, json_only)
        return 2
    except PreflightError as exc:
        for problem in exc.problems:
            print(f"PREFLIGHT {problem}", file=sys.stderr)
        summary = summarize(
            args.scale,
            args.layout,
            args.protocol,
            names,
            [],
            error="preflight failed",
            problems=exc.problems,
            category="preflight",
        )
        print_summary(summary, json_only)
        return 2

    outcomes, error, cause = asyncio.run(execute_suite(args, specs, emit))
    if error is not None:
        print(f"ERROR {flatten(error)}", file=sys.stderr)
    summary = summarize(
        args.scale,
        args.layout,
        args.protocol,
        names,
        outcomes,
        error=error,
        cause=cause,
        category="setup" if error is not None else None,
    )
    print_summary(summary, json_only)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
