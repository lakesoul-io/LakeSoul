"""Async benchmark scenarios and JSONL command-line interface."""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import random
import re
import sys
import tempfile
import time
import uuid
from pathlib import Path

from .answers import compare
from .wire import Client, PGError, ReadPolicy, Result

# Closed-loop pause after a `disconnected` sample (server unreachable).
RECONNECT_BACKOFF_S = 1.0


class Output:
    def __init__(self, path: str, run_id: str):
        self.run_id = run_id
        self.stream = sys.stdout if path == "-" else open(path, "a", encoding="utf-8", buffering=1)

    def write(self, record: dict) -> None:
        self.stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
        self.stream.flush()

    def close(self) -> None:
        if self.stream is not sys.stdout:
            self.stream.close()


def sample(args, name: str, iteration: int, client_id: int, **extra) -> dict:
    return dict(run_id=args.run_id, scenario=args.command, query=name,
                iteration=iteration, client_id=client_id, start_unix=time.time(),
                end_unix=None, latency_s=None, rowdesc_s=None, first_row_s=None,
                rows=0, bytes=0, status="ok", sqlstate=None, error=None, **extra)


def finish(record: dict, started: float, result: Result | None = None) -> dict:
    record["end_unix"] = time.time()
    record["latency_s"] = time.monotonic() - started
    if result is not None:
        for key in ("rows", "bytes", "rowdesc_s", "first_row_s", "sqlstate", "error"):
            record[key] = getattr(result, key)
        if result.error and record["status"] == "ok":
            record["status"] = "cancelled" if result.sqlstate == "57014" else "error"
    return record


def tag(args, sql: str, name: str, iteration: int, client_id: int) -> str:
    def safe(value):
        return re.sub(r"[^A-Za-z0-9_.:-]", "_", str(value))
    return (f"/* lsbench run={safe(args.run_id)} scenario={safe(args.command)} "
            f"q={safe(name)} i={iteration} c={client_id} */\n{sql}")


def client(args, **kwargs) -> Client:
    return Client(args.host, args.port, args.user, args.database, args.connect_timeout, **kwargs)


async def execute(args, connection: Client, name: str, sql: str, iteration: int,
                  client_id: int, *, collect: bool = False,
                  scheduled_unix: float | None = None) -> tuple[dict, Result | None]:
    record = sample(args, name, iteration, client_id)
    if scheduled_unix is not None:
        record["scheduled_unix"] = scheduled_unix
    started = time.monotonic()
    result = None
    query_task = None
    try:
        if connection.sock is None:
            await connection.connect()
        query_task = asyncio.create_task(connection.query(
            tag(args, sql, name, iteration, client_id), collect=collect,
            protocol=getattr(args, "protocol", "simple"), max_rows=getattr(args, "max_rows", 0),
            started=started))
        timeout = getattr(args, "query_timeout", 0)
        if timeout > 0:
            done, _ = await asyncio.wait({query_task}, timeout=timeout)
            if not done:
                record["status"] = "timeout"
                record["error"] = f"Query exceeded {timeout:g} seconds"
                try:
                    await connection.cancel()
                    await asyncio.wait_for(asyncio.shield(query_task), 5)
                except (OSError, PGError, TimeoutError, ConnectionError) as exc:
                    record["cancel_error"] = str(exc)
                    connection.close(abrupt=True)
                    query_task.cancel()
                if not query_task.cancelled() and query_task.done():
                    result = query_task.result()
                else:
                    await asyncio.gather(query_task, return_exceptions=True)
            else:
                result = query_task.result()
        else:
            result = await query_task
    except (OSError, PGError, TimeoutError, ConnectionError) as exc:
        record["status"] = "disconnected" if isinstance(exc, (ConnectionError, OSError)) else "error"
        record["sqlstate"] = getattr(exc, "sqlstate", None)
        record["error"] = str(exc) or type(exc).__name__
        connection.close(abrupt=True)
    finally:
        if query_task is not None and not query_task.done():
            query_task.cancel()
            await asyncio.gather(query_task, return_exceptions=True)
    result = result or (connection.result if query_task is not None else None)
    # Preserve client-side timeout/transport errors if the server sent none.
    prior_error, prior_state = record["error"], record["sqlstate"]
    finish(record, started, result)
    record["error"] = record["error"] or prior_error
    record["sqlstate"] = record["sqlstate"] or prior_state
    return record, result


def sql_argument(args) -> str:
    return args.sql if args.sql is not None else Path(args.sql_file).read_text(encoding="utf-8")


def substitute_sf(sql: str, sf: float | None) -> str:
    if "{{SF}}" not in sql:
        return sql
    if sf is None:
        raise ValueError("SQL contains {{SF}}; specify --sf")
    return sql.replace("{{SF}}", str(float(sf)))


def tpch_queries(args) -> list[tuple[str, str]]:
    """`--queries` as (name, SQL) pairs with `{{SF}}` substituted; validates
    `--answers` up front so a missing file fails before any query runs."""
    names = [f"q{i:02}" for i in range(1, 23)] if args.queries == "all" else args.queries.split(",")
    if not names or any(not re.fullmatch(r"q(?:0[1-9]|1[0-9]|2[0-2])", name) for name in names):
        raise ValueError("--queries must be all or comma-separated q01..q22")
    queries = [(name, substitute_sf((Path(args.query_dir) / f"{name}.sql").read_text(encoding="utf-8"), args.sf))
               for name in names]
    if args.answers:
        for name in names:
            if not (Path(args.answers) / f"{name}.csv").is_file():
                raise ValueError(f"Missing answer: {Path(args.answers) / (name + '.csv')}")
    return queries


async def tpch(args, output: Output) -> None:
    if args.answers and not args.fetch_result:
        raise ValueError("--answers requires --fetch-result")
    queries = tpch_queries(args)
    connection = client(args)
    outages = 0
    try:
        for iteration in range(-args.warmup, args.iterations):
            for name, sql in queries:
                record, result = await execute(args, connection, name, sql, iteration, 0,
                                               collect=args.fetch_result)
                record.update(correct=None, mismatch=None, protocol=args.protocol, max_rows=args.max_rows)
                if args.answers and record["status"] == "ok" and result is not None:
                    record["correct"], record["mismatch"] = compare(result, Path(args.answers) / f"{name}.csv", sql)
                output.write(record)
                if record["status"] == "disconnected":
                    outages += 1
                    if outages >= args.max_outages:
                        # Every further query would crash the server again
                        # (CrashLoopBackOff); stop instead of driving the loop.
                        output.write(dict(type="aborted", run_id=args.run_id, scenario=args.command,
                                          reason=f"{outages} dropped queries", after_query=name,
                                          iteration=iteration))
                        return
                    if args.recover_timeout > 0:
                        await wait_for_server(args, output, name, iteration)
    finally:
        await connection.terminate()


async def wait_for_server(args, output: Output, after: str, iteration: int) -> None:
    """After the server dropped a query (restart, OOM kill), wait until it
    accepts connections again so the next query measures the service rather
    than the outage, and record how long the outage lasted."""
    started, unix = time.monotonic(), time.time()
    recovered = False
    while time.monotonic() - started < args.recover_timeout:
        probe = client(args)
        try:
            await probe.connect()
            recovered = True
            break
        except (OSError, PGError, TimeoutError, ConnectionError):
            await asyncio.sleep(2)
        finally:
            await probe.terminate()
    output.write(dict(type="recovery", run_id=args.run_id, scenario=args.command, after_query=after,
                      iteration=iteration, start_unix=unix, recovered=recovered,
                      outage_s=time.monotonic() - started))


def read_mix(path: str, sf: float | None) -> list[dict]:
    file = Path(path)
    data = json.loads(file.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError("Mix must be a nonempty JSON array")
    for item in data:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise ValueError("Each mix entry must have a string name")
        if ("sql" in item) == ("sql_file" in item):
            raise ValueError("Each mix entry requires exactly one of sql or sql_file")
        if "sql_file" in item:
            item["sql"] = (file.parent / item["sql_file"]).read_text(encoding="utf-8")
        if not isinstance(item["sql"], str):
            raise ValueError("Mix SQL must be a string")
        item["sql"] = substitute_sf(item["sql"], sf)
        item["weight"] = float(item.get("weight", 1))
        if not math.isfinite(item["weight"]) or item["weight"] <= 0:
            raise ValueError("Mix weights must be finite and positive")
    return data


async def ticks(args, output: Output, stop: asyncio.Event, state: dict,
                connection: Client | None = None) -> None:
    while True:
        try:
            await asyncio.wait_for(stop.wait(), 1)
            return
        except TimeoutError:
            record = dict(type="tick", run_id=args.run_id, scenario=args.command,
                          unix=time.time(), **state)
            if connection is not None:
                record["wire_bytes"] = connection.policy.received
                record["rows"] = connection.result.rows if connection.result else 0
                record["bytes"] = connection.result.bytes if connection.result else 0
            output.write(record)


async def load(args, output: Output) -> None:
    mix = read_mix(args.mix, args.sf)
    weights = [entry["weight"] for entry in mix]
    rng = random.Random(args.seed)
    state = dict(inflight=0, completed=0, errors=0, dropped=0)
    stop = asyncio.Event()
    ticker = asyncio.create_task(ticks(args, output, stop, state))
    started, unix = time.monotonic(), time.time()
    deadline = started + args.duration
    clients: dict[int, Client] = {}
    active: set[asyncio.Task] = set()

    halted = False

    def halt(record):
        nonlocal deadline, halted
        halted, deadline = True, time.monotonic()
        output.write(dict(type="aborted", run_id=args.run_id, scenario=args.command,
                          reason="server outage", after_query=record["query"],
                          client_id=record["client_id"]))

    async def one(client_id, iteration, entry, scheduled=None):
        state["inflight"] += 1
        if client_id not in clients:
            clients[client_id] = client(args)
        connection = clients[client_id]
        try:
            record, _ = await execute(args, connection, entry["name"], entry["sql"],
                                      iteration, client_id, scheduled_unix=scheduled)
            record.update(protocol=args.protocol, max_rows=args.max_rows, mode=args.mode)
            output.write(record)
            state["completed"] += 1
            state["errors"] += record["status"] != "ok"
            if record["status"] == "disconnected" and args.stop_on_outage and not halted:
                # Server gone (OOM kill, crash): every further query would hit the
                # restarting pod again (CrashLoopBackOff), so end the run here.
                halt(record)
            return record
        finally:
            state["inflight"] -= 1
            if args.connect_per_query:
                await connection.terminate()

    async def closed_worker(client_id):
        local = random.Random(args.seed + client_id)
        iteration = 0
        while time.monotonic() < deadline:
            entry = local.choices(mix, weights=weights, k=1)[0]
            record = await one(client_id, iteration, entry)
            if record["status"] == "disconnected":
                # Server gone (restart, OOM kill): back off like a real client
                # instead of recording thousands of instant connect failures.
                await asyncio.sleep(RECONNECT_BACKOFF_S)
            iteration += 1

    try:
        if args.mode == "closed":
            await asyncio.gather(*(closed_worker(i) for i in range(args.concurrency)))
        else:
            available = list(range(args.max_inflight))
            arrival, iteration = 0.0, 0

            async def open_worker(slot, index, entry, scheduled):
                try:
                    await one(slot, index, entry, scheduled)
                finally:
                    available.append(slot)

            while not halted:
                arrival += rng.expovariate(args.rate)
                if arrival >= args.duration:
                    break
                await asyncio.sleep(max(0, started + arrival - time.monotonic()))
                entry = rng.choices(mix, weights=weights, k=1)[0]
                if available:
                    task = asyncio.create_task(open_worker(available.pop(), iteration, entry, unix + arrival))
                    active.add(task)
                    task.add_done_callback(active.discard)
                else:
                    record = sample(args, entry["name"], iteration, -1,
                                    scheduled_unix=unix + arrival)
                    record.update(status="client_dropped", error="max-inflight reached",
                                  protocol=args.protocol, max_rows=args.max_rows, mode=args.mode)
                    output.write(finish(record, time.monotonic()))
                    state["dropped"] += 1
                iteration += 1
            await asyncio.sleep(max(0, deadline - time.monotonic()))
            if active:
                await asyncio.gather(*active)
    finally:
        if active:
            for task in active:
                task.cancel()
            await asyncio.gather(*active, return_exceptions=True)
        stop.set()
        await ticker
        await asyncio.gather(*(connection.terminate() for connection in clients.values()))
        output.write(dict(type="summary", run_id=args.run_id, scenario=args.command,
                          duration_s=time.monotonic() - started, **state))


async def slowclient(args, output: Output) -> None:
    sql = sql_argument(args)
    connection = client(args, rcvbuf=args.rcvbuf)
    stop = asyncio.Event()
    ticker = None
    try:
        # Exclude handshake bytes and time from the throttled-consumption budget.
        try:
            await connection.connect()
        except (OSError, PGError, TimeoutError) as exc:
            record = sample(args, "slowclient", 0, 0)
            record.update(status="error", error=str(exc), sqlstate=getattr(exc, "sqlstate", None))
            output.write(finish(record, time.monotonic()))
            return
        for statement in args.setup or []:
            result = await connection.query(statement)
            if result.error is not None:
                record = sample(args, "slowclient", 0, 0)
                record.update(status="error", error=f"setup {statement!r}: {result.error}",
                              sqlstate=result.sqlstate)
                output.write(finish(record, time.monotonic()))
                return
        connection.policy = ReadPolicy(args.read_rate, args.stall_after_bytes, args.stall_seconds)
        ticker = asyncio.create_task(ticks(args, output, stop, {}, connection))
        record, _ = await execute(args, connection, "slowclient", sql, 0, 0)
        record.update(wire_bytes=connection.policy.received, read_rate=args.read_rate,
                      rcvbuf=args.rcvbuf, protocol=args.protocol, max_rows=args.max_rows)
        output.write(record)
    finally:
        stop.set()
        if ticker:
            await ticker
        await connection.terminate()


async def interruption(args, output: Output) -> None:
    connection = client(args)
    record = sample(args, args.command, 0, 0)
    started = time.monotonic()
    task = None
    result = None
    try:
        await connection.connect()
        task = asyncio.create_task(connection.query(tag(args, args.sql, args.command, 0, 0), started=started))
        done, _ = await asyncio.wait({task}, timeout=args.after)
        record["after_s"] = args.after
        record["interrupted"] = not done
        if not done:
            action_at = time.monotonic()
            record["action_unix"] = time.time()
            if args.command == "cancel":
                await connection.cancel()
                result = await asyncio.wait_for(asyncio.shield(task), 10)
                record["cancel_to_error_s"] = (result.error_at - action_at) if result.error_at else None
                record["cancel_to_ready_s"] = (result.ready_at - action_at) if result.ready_at else None
            else:
                connection.close(abrupt=True, rst=args.rst)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                result = connection.result
                record.update(status="disconnected", rst=args.rst)
        else:
            result = task.result()
        finish(record, started, result)
        if args.command == "cancel":
            # The benchmark query's elapsed time must not include this probe.
            probe = await connection.query(tag(args, "SELECT 1", "post_cancel", 0, 0), collect=True)
            record["post_cancel_ok"] = probe.error is None and probe.values == [("1",)]
    except (OSError, PGError, TimeoutError, ConnectionError) as exc:
        record.update(status="error", error=str(exc), sqlstate=getattr(exc, "sqlstate", None))
        if args.command == "cancel":
            record["post_cancel_ok"] = False
        finish(record, started, connection.result)
        record["error"] = record["error"] or str(exc)
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await connection.terminate()
    output.write(record)


async def connflood(args, output: Output) -> None:
    connections: list[Client] = []
    state = dict(attempted=0, connected=0, errors=0)
    started = time.monotonic()

    async def connect_one(index):
        connection = client(args)
        connections.append(connection)
        record = sample(args, "connect", 0, index)
        begin = time.monotonic()
        try:
            await connection.connect()
            state["connected"] += 1
        except (OSError, PGError, TimeoutError) as exc:
            record.update(status="error", sqlstate=getattr(exc, "sqlstate", None), error=str(exc))
            state["errors"] += 1
            connection.close(abrupt=True)
        state["attempted"] += 1
        output.write(finish(record, begin))

    tasks = []
    try:
        for index in range(args.connections):
            await asyncio.sleep(max(0, started + index / args.rate - time.monotonic()))
            tasks.append(asyncio.create_task(connect_one(index)))
        await asyncio.gather(*tasks)
        # Hold the entire successfully admitted population for at least --hold.
        await asyncio.sleep(args.hold)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(connection.terminate() for connection in connections))
    output.write(dict(type="summary", run_id=args.run_id, scenario=args.command,
                      duration_s=time.monotonic() - started, **state))


def positive(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and > 0")
    return number


def nonnegative(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be finite and >= 0")
    return number


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return number


def nonnegative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return number


def int32(value: str) -> int:
    number = int(value)
    if not -(2**31) <= number < 2**31:
        raise argparse.ArgumentTypeError("must fit a signed i32")
    return number


def read_rate(value: str) -> float | None:
    return None if value == "max" else positive(value)


def add_protocol(parser) -> None:
    parser.add_argument("--protocol", choices=("simple", "extended"), default="simple")
    parser.add_argument("--max-rows", type=int32, default=0,
                        help="extended Execute row limit; signed i32 sent verbatim (default: 0)")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="lsbench", description="Stdlib PG-wire benchmark (trust authentication, no TLS)")
    commands = result.add_subparsers(dest="command", required=True)
    for name, help_text in (("tpch", "Sequential TPC-H queries and reference checks"),
                            ("load", "Closed-loop or Poisson open-loop load"),
                            ("slowclient", "Controlled socket consumption and stalls"),
                            ("cancel", "Cancel a query and verify session reuse"),
                            ("disconnect", "Abruptly disconnect during a query"),
                            ("connflood", "Ramp and hold concurrent connections")):
        sub = commands.add_parser(name, help=help_text)
        sub.add_argument("--host", default="127.0.0.1")
        sub.add_argument("--port", type=positive_int, default=5432)
        sub.add_argument("--user", default="lsbench")
        sub.add_argument("--database", required=True, help="existing LakeSoul namespace / PostgreSQL database")
        sub.add_argument("--out", required=True, help="append JSONL to this file; - for stdout")
        sub.add_argument("--run-id", default=uuid.uuid4().hex)
        sub.add_argument("--connect-timeout", type=positive, default=10)
        if name in ("tpch", "load", "slowclient"):
            add_protocol(sub)
            sub.add_argument("--query-timeout", type=nonnegative, default=30 if name == "load" else 0,
                             help="seconds before CancelRequest; 0 disables timeout")
        if name in ("tpch", "load"):
            sub.add_argument("--sf", type=positive, help="replace {{SF}} in SQL with a float literal")
        if name == "tpch":
            sub.add_argument("--queries", default="all")
            sub.add_argument("--query-dir", default=str(Path(__file__).resolve().parents[2] / "workloads/tpch/queries"))
            sub.add_argument("--iterations", type=positive_int, default=1)
            sub.add_argument("--warmup", type=nonnegative_int, default=0)
            sub.add_argument("--answers")
            sub.add_argument("--recover-timeout", type=nonnegative, default=180,
                             help="after a dropped connection wait up to this long for the server "
                                  "before the next query (0: do not wait)")
            sub.add_argument("--max-outages", type=positive_int, default=3,
                             help="stop after this many dropped queries (server crash loop)")
            sub.add_argument("--fetch-result", action=argparse.BooleanOptionalAction, default=True,
                             help="retain text rows for correctness checking; results are always drained")
        elif name == "load":
            sub.add_argument("--mode", choices=("closed", "open"), required=True)
            sub.add_argument("--concurrency", type=positive_int, default=1)
            sub.add_argument("--rate", type=positive, default=1)
            sub.add_argument("--max-inflight", type=positive_int, default=256)
            sub.add_argument("--duration", type=positive, required=True)
            sub.add_argument("--mix", required=True)
            sub.add_argument("--connect-per-query", action="store_true")
            sub.add_argument("--stop-on-outage", action=argparse.BooleanOptionalAction, default=True,
                             help="end the run at the first dropped connection (server crash) "
                                  "instead of driving a CrashLoopBackOff")
            sub.add_argument("--seed", type=int, default=1)
        elif name == "slowclient":
            source = sub.add_mutually_exclusive_group(required=True)
            source.add_argument("--sql")
            source.add_argument("--sql-file")
            sub.add_argument("--rcvbuf", type=positive_int, default=65536)
            sub.add_argument("--read-rate", type=read_rate, default=None, metavar="BYTES_PER_S|max")
            sub.add_argument("--stall-after-bytes", type=nonnegative_int)
            sub.add_argument("--stall-seconds", type=nonnegative, default=0)
            sub.add_argument("--setup", action="append", metavar="SQL",
                             help="statement run on the connection before the throttled query (repeatable)")
        elif name in ("cancel", "disconnect"):
            sub.add_argument("--sql", required=True)
            sub.add_argument("--after", type=nonnegative, default=1)
            if name == "disconnect":
                sub.add_argument("--rst", action="store_true")
        elif name == "connflood":
            sub.add_argument("--connections", type=positive_int, required=True)
            sub.add_argument("--rate", type=positive, default=10)
            sub.add_argument("--hold", type=nonnegative, default=10)
    console = commands.add_parser("console", help="Sequential TPC-H through lakesoul-console (no PG wire)")
    console.add_argument("--database", required=True, help="existing LakeSoul namespace (USE)")
    console.add_argument("--out", required=True, help="append JSONL to this file; - for stdout")
    console.add_argument("--run-id", default=uuid.uuid4().hex)
    console.add_argument("--sf", type=positive, help="replace {{SF}} in SQL with a float literal")
    console.add_argument("--queries", default="all")
    console.add_argument("--query-dir", default=str(Path(__file__).resolve().parents[2] / "workloads/tpch/queries"))
    console.add_argument("--iterations", type=positive_int, default=1)
    console.add_argument("--warmup", type=nonnegative_int, default=0)
    console.add_argument("--answers", help="answer directory; only row counts are compared")
    console.add_argument("--console-bin", default="lakesoul-console")
    console.add_argument("--worker-threads", type=positive_int, required=True,
                         help="console tokio threads; match the CPU limit (the console default is 2)")
    console.add_argument("--target-partitions", type=positive_int,
                         help="single node: SET datafusion.execution.target_partitions; "
                              "with --worker: the console's --target-partitions")
    console.add_argument("--worker", action="append", default=[], metavar="URL",
                         help="worker gRPC URL (repeatable); none runs on a single node")
    console.add_argument("--bytes-per-partition", type=positive_int,
                         help="with --worker: bytes per distributed file-scan task")
    console.add_argument("--log-dir", default=tempfile.gettempdir(), help="console tracing log directory")
    return result


async def dispatch(args, output: Output) -> None:
    from .console import console

    handler = {"tpch": tpch, "load": load, "slowclient": slowclient, "console": console,
               "cancel": interruption, "disconnect": interruption, "connflood": connflood}[args.command]
    await handler(args, output)


def main() -> int:
    arguments = parser()
    args = arguments.parse_args()
    if getattr(args, "port", 0) > 65535:
        arguments.error("--port must be <= 65535")
    output = None
    try:
        output = Output(args.out, args.run_id)
        asyncio.run(dispatch(args, output))
        return 0
    except (OSError, ValueError, PGError) as exc:
        print(f"lsbench: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    finally:
        if output:
            output.close()
