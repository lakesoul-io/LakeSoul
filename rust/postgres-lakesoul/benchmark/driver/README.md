# lsbench: controlled PG-wire benchmark driver

Requires Python 3.12 and its standard library only. No libpq, asyncpg, SSL,
password authentication, or pip installation is used. The LakeSoul coordinator
uses trust authentication; use an existing LakeSoul namespace as `--database`.
The service is read-only, so generate/load benchmark tables separately.

From the repository root:

```sh
export PYTHONPATH="$PWD/rust/postgres-lakesoul/benchmark/driver"
python3 -m lsbench --help
python3 -m lsbench tpch --help
```

All subcommands accept `--host` (default `127.0.0.1`), `--port` (5432),
`--user` (`lsbench`), required `--database` and `--out`, optional `--run-id`
(default random UUID hex), and `--connect-timeout` (10 seconds). Options follow
 the subcommand. `--out -` writes JSONL to stdout; a file is appended and flushed
for every record. Use a different run ID/output file for independent runs.
Every SQL statement has a leading sanitized correlation comment:

```sql
/* lsbench run=<run_id> scenario=<scenario> q=<query> i=<iteration> c=<client_id> */
```

## Sequential TPC-H and correctness

```sh
python3 -m lsbench tpch --host coordinator --database tpch_sf1 \
  --out tpch.jsonl --run-id sf1-cold --queries q01,q06,q11 --sf 1 \
  --iterations 3 --warmup 1 \
  --answers rust/postgres-lakesoul/benchmark/workloads/tpch/answers/sf1
```

`--queries all` (default) runs q01 through q22. `--query-dir` defaults to the
sibling `workloads/tpch/queries` directory. Each iteration runs the listed
queries sequentially, using one persistent session. Warmup iterations are
`-K, ..., -1`; measured iterations start at zero. `--sf` substitutes `{{SF}}`
in SQL with a floating-point literal (for example, `1.0`). SQL containing this
placeholder without `--sf` fails before connecting.

`--answers DIR` reads RFC4180 `qNN.csv` files with a header. Columns match by
position, not header labels; column and row counts must agree. NULL is an
empty CSV field (therefore indistinguishable from an empty string). Strings
are exact after trimming trailing ASCII spaces; dates compare as ISO text.
Numeric PostgreSQL types use absolute tolerance <= 0.01 **or** relative error
<= 1e-6 against the reference value. Comparison is order-sensitive only when
SQL contains a top-level `ORDER BY`; otherwise it compares multisets, including
duplicate rows. Comments, strings, quoted identifiers and nested subqueries
do not falsely trigger ordered comparison. The driver records `correct` and
`mismatch`; mismatches do not abort the remaining queries or change the
process exit code.

`--fetch-result` is on by default and retains text values for comparison.
`--no-fetch-result` reduces client memory when answers are not requested. It
still drains all rows, so server execution and backpressure are not bypassed.
Result values are not copied into JSONL.

## TPC-H through lakesoul-console

```sh
python3 -m lsbench console --database tpch_sf1 --out console.jsonl --sf 1 \
  --iterations 3 --warmup 1 --worker-threads 4 --target-partitions 4 \
  --answers rust/postgres-lakesoul/benchmark/workloads/tpch/answers/sf1
```

`console` does not use PG wire. It writes one statement file (`USE`, an
optional `SET datafusion.execution.target_partitions`, then a marker `SELECT`
before every query) and runs it with a single `lakesoul-console` process
(`--console-bin`, default from `PATH`), so caches persist across iterations
as in `tpch`. The console needs the metadata and warehouse environment
(`LAKESOUL_PG_URL`, `LAKESOUL_WAREHOUSE`, `AWS_*`). `latency_s` is the
console's own `Elapsed` time (millisecond resolution, planning plus
execution plus printing); `rows` comes from its `N row(s) fetched.` line.
`--answers` checks row counts only.

`--worker URL` (repeatable) runs distributed; `--target-partitions` and
`--bytes-per-partition` then go to the console's flags. Without workers,
`--target-partitions` becomes the `SET`, because single-node sessions default
to one partition. A failed query is recorded with `status: "error"` and the
run continues. If the console dies, the running query is `disconnected` and a
`type: "aborted"` record follows.

## Closed-loop and open-loop load

Create a mix JSON file, for example:

```json
[
  {"name": "q01", "sql_file": "queries/q01.sql", "weight": 1},
  {"name": "probe", "sql": "SELECT 1", "weight": 3}
]
```

Relative SQL file paths resolve relative to the mix file, not the current
working directory. Each entry needs exactly one of `sql`/`sql_file`. Weights
must be finite and positive. `--sf` works for both forms, with fail-fast missing
placeholder validation. `--seed` (default 1) controls weighted selections and
Poisson arrivals.

```sh
python3 -m lsbench load --host coordinator --database tpch_sf1 \
  --out closed.jsonl --mode closed --concurrency 16 --duration 120 \
  --mix mix.json --sf 1 --query-timeout 60
python3 -m lsbench load --host coordinator --database tpch_sf1 \
  --out open.jsonl --mode open --rate 20 --max-inflight 64 --duration 120 \
  --mix mix.json --sf 1 --query-timeout 30
```

Closed-loop clients issue another query only after completing their previous
one. Open-loop arrivals have exponentially distributed interarrival times.
The cap bounds active client tasks/connections, not a hidden queue: excess
arrivals produce `status: "client_dropped"`, `client_id: -1` samples. Successful
open-loop admissions record `scheduled_unix`, so scheduling delay is
`start_unix - scheduled_unix`. Connections persist per client/slot by default;
`--connect-per-query` instead includes a new handshake on each sample.

`--duration` limits arrivals, not completion; admitted work is drained after
the interval. `--query-timeout` defaults to 30 seconds for load (zero disables).
On timeout the client sends a separate CancelRequest, allows five seconds to
drain ErrorResponse/ReadyForQuery, and closes the session if draining fails.
A timed-out sample remains `timeout` even when SQLSTATE is `57014`. Tick records
continue while admitted work drains.

## Slow consumers and stalls

```sh
python3 -m lsbench slowclient --host coordinator --database tpch_sf1 \
  --out slow.jsonl --sql 'SELECT * FROM lineitem' \
  --rcvbuf 65536 --read-rate 1048576 \
  --stall-after-bytes 8388608 --stall-seconds 20
```

`--sql-file` is an alternative to `--sql`. `--read-rate max` (default) disables
throttling. A token bucket gates actual socket reads, with at most a 16 KiB
burst: there is no eager asyncio stream reader buffering results behind the
limiter. Startup is unthrottled and excluded from byte counters. The read rate
and stall threshold count all subsequent PostgreSQL wire bytes, whereas the
sample's `bytes` counts only DataRow payloads. Linux may double the requested
SO_RCVBUF value. The optional stall happens once, exactly at the cumulative
wire-byte threshold. Tick records expose `wire_bytes`, DataRow `bytes` and
`rows`; take differences over tick timestamps to measure consumption rate.
`--query-timeout` defaults to zero for slowclient and tpch.

## Simple versus extended protocol

`tpch`, `load`, and `slowclient` accept `--protocol simple|extended` (simple is
the default). Simple uses `Query` and drains through ReadyForQuery. Extended
uses unnamed Parse/Bind/Describe(portal)/Execute with no parameters and text
result columns. `--max-rows` defaults to 0 and accepts every signed i32,
including negative robustness-test values, which are sent verbatim.

For `max_rows <= 0`, Execute is followed immediately by Sync. For positive
page limits, Flush is used between Execute pages; PortalSuspended causes
another Execute, and CommandComplete/ErrorResponse causes Sync. Syncing an
implicit transaction while suspended would destroy the PostgreSQL portal,
so the driver deliberately does **not** Sync between pages. The coordinator
may buffer a complete extended fetch (notably max_rows=0), unlike simple
streaming; report the protocol and page limit with benchmark results.

## Cancellation and abrupt disconnects

```sh
python3 -m lsbench cancel --host coordinator --database tpch_sf1 \
  --out cancel.jsonl --sql 'SELECT * FROM lineitem' --after 2
python3 -m lsbench disconnect --host coordinator --database tpch_sf1 \
  --out disconnect.jsonl --sql 'SELECT * FROM lineitem' --after 2 --rst
```

CancelRequest uses BackendKeyData on a separate connection. The driver
records cancellation-to-ErrorResponse and cancellation-to-ReadyForQuery
latencies and then executes tagged `SELECT 1` on the **same** session;
`post_cancel_ok` proves reuse. If a query finishes before `--after`,
`interrupted` is false and no cancellation/disconnect is sent. Disconnect
closes without Terminate; `--rst` requests SO_LINGER(1,0) instead of FIN.
Ordinary session shutdown sends Terminate.

## Connection population

```sh
python3 -m lsbench connflood --host coordinator --database tpch_sf1 \
  --out connections.jsonl --connections 500 --rate 25 --hold 60
```

Connections are ramped at the requested rate without queries. Each handshake
emits an outcome sample (including startup SQLSTATE/errors). After all
attempts finish, every successfully admitted connection stays open for
`--hold` seconds, then receives Terminate. A summary reports attempted,
connected, errors and elapsed duration.

## JSONL schema

Query/connection samples have no `type` discriminator and always include:

| Field | Meaning |
| --- | --- |
| `run_id`, `scenario`, `query` | Correlation identifiers; query is a name, not SQL |
| `iteration`, `client_id` | Zero-based measured iteration/client; negative warmups, -1 dropped client |
| `start_unix`, `end_unix` | Wall-clock epoch seconds |
| `latency_s` | Monotonic elapsed time, including a new connection when needed |
| `rowdesc_s`, `first_row_s` | Seconds from sample start; null if not observed |
| `rows`, `bytes` | Complete DataRows and DataRow payload bytes (including field count/lengths, excluding 5-byte message framing) |
| `status` | `ok`, `error`, `cancelled`, `timeout`, `disconnected`, or `client_dropped` |
| `sqlstate`, `error` | Server SQLSTATE/message, or client-side error; otherwise null |

Scenario-specific fields include `correct`, `mismatch`, `protocol`, `max_rows`,
`mode`, `scheduled_unix`, `cancel_error`, `wire_bytes`, `read_rate`, `rcvbuf`,
`after_s`, `interrupted`, `action_unix`, `cancel_to_error_s`,
`cancel_to_ready_s`, `post_cancel_ok`, and `rst`. Nullable observations remain
null instead of zero. Query time is measured until ReadyForQuery; cancellation
probes and graceful shutdown are excluded. The row counters on an interrupted
query include only fully decoded rows, even if partial wire data was received.

Once per second, load emits `type: "tick"`, `unix`, `inflight`, `completed`,
`errors`, `dropped`; slowclient ticks include wire/DataRow bytes and rows.
Load and connflood also emit `type: "summary"`. All ticks and summaries contain
`run_id` and `scenario`, but intentionally are not query samples. Load errors
count completed non-ok requests; dropped arrivals have a separate counter.
CLI/configuration errors exit 2, interruption exits 130; query outcomes and
correctness are reported in JSONL rather than encoded in the process status.

## Isolated PostgreSQL smoke

`python3 smoke.py` creates its own `lsbench-smoke` Docker network, PostgreSQL
14.5 container with trust authentication, and Python 3.12 driver containers.
It never publishes a host port or changes an existing container. An already
existing network with that name causes a safe failure rather than reuse or
removal. Pull `python:3.12-slim` first if needed; `--python-image IMAGE` permits
a locally available image containing Python 3.12. The smoke covers every
command/help page, correctness pass/fail, warmup, closed/open load, timeout,
1 MiB/s large-result consumption, cancellation/reuse, FIN/RST, 50 connections,
and extended execution including paged fetches. All containers/network and
temporary fixtures are cleaned up, even on failure. The slow-client run takes
about 193 seconds by design (200,000 approximately 1 KiB rows).
