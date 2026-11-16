---
sidebar_position: 8
sidebar_label: Detached queries
---

# Detached queries

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

A long ETL or a full-table aggregation often outlives the client connection
that started it. PostgreSQL's asynchronous command API does not help here:
`PQsendQuery` only makes the client's I/O non-blocking, and the result stream
stays bound to the `PGconn`; PostgreSQL has no query handle that can be used
from another connection.

`postgres-lakesoul` adds one at the SQL layer instead. A **detached query** is
a script submitted once; it returns a durable query id and then runs
independently of the connection. Any later connection can poll it, read its
results, or cancel it.

## Submit

```sql
SELECT lakesoul_submit(
  $job$
    SET lakesoul.ivm.enabled = on;
    INSERT INTO silver.orders
      SELECT * FROM bronze.orders WHERE dt = '2026-11-09';
    SELECT count(*) AS written FROM silver.orders;
  $job$,
  'orders-2026-11-09',   -- optional idempotency key
  7200000,               -- optional timeout in milliseconds
  true                   -- retry after a server restart (default true)
) AS query_id;
```

A script is one or more `SELECT`, `INSERT ... SELECT` and `SET` statements. A
`SET` affects every statement after it in the same job session, exactly like a
psql script: `SET search_path`, `SET TIME ZONE`, `SET statement_timeout`,
`SET lakesoul.ivm.enabled` and ordinary DataFusion options are supported.
Transaction control, DDL, `SET LOCAL`/`SET GLOBAL` and `SET ROLE` are refused.

The second argument is an idempotency key. Submitting the same SQL with the
same key while the first job still exists returns the existing query id instead
of starting a second job. It is also how a client recovers when the submit
response is lost: the key is chosen by the client, so it can look the job up
after reconnecting. A key from a previous server run cannot be reused; submit
with a new key instead.

The submit call itself is a control action. It may be issued inside a
transaction block (drivers such as psycopg and SQLAlchemy start one implicitly),
but the job cannot be rolled back with that transaction.

## Poll

```sql
SELECT query_id, state, attempt, rows_produced, result_sets,
       error_sqlstate, error_message
FROM lakesoul_query_status('7c9e8f3a-...');

SELECT * FROM lakesoul_queries() WHERE state = 'running';

-- Wait for a terminal state (does not cancel the job on timeout).
SELECT lakesoul_wait_query('7c9e8f3a-...', 60000);
```

States are `queued`, `running`, `succeeded`, `failed`, `canceled` and `lost`.
A running job survives a server restart by **re-running from scratch**: the
server re-queues its own unfinished jobs, increments `attempt` when a runner
claims one, and gives up as `lost` after `retry_on_restart` attempts. A
completed result does not need a re-run: its Parquet files and schemas are
recorded in the metadata database and remain readable for the 24 hour result
window after a restart.

Because a re-run starts from the beginning, an `INSERT ... SELECT` is
**at-least-once** if the previous attempt had already committed before the
crash. Pass `retry_on_restart => false` (or the fourth positional argument
`false`) for a write that must not be repeated; use an upsert, IVM, or a
partition overwrite when the target is not idempotent.

Use `lakesoul_queries()` when the query id is a client-side parameter:

```sql
SELECT state FROM lakesoul_queries() WHERE query_id = $1;
```

`lakesoul_query_status(...)`, `lakesoul_query_result(...)` and the other result
functions require a literal query id so they can build their result schema
while planning. A UUID is safe to inline.

## Results

An `INSERT ... SELECT` writes the target table with the write path's usual
single commit; it produces no row result. Its status reports the last command
tag and the number of written rows.

Each `SELECT` in the script produces its own result set. A result set is one
Parquet file under
`<warehouse>/_lakesoul_jobs/<query_id>/attempt-<n>/stmt-<i>/part-0.parquet`.
Read the **last** result set with:

```sql
SELECT * FROM lakesoul_query_result('7c9e8f3a-...');
SELECT * FROM lakesoul_query_result('7c9e8f3a-...', 100000, 10000);
```

The second and third arguments are an offset and a limit. The default limit is
1,000,000 rows.

A script with several `SELECT`s exposes every set:

```sql
SELECT * FROM lakesoul_query_result_sets('7c9e8f3a-...');
-- set_index | statement_index | rows | bytes | expires_at

SELECT * FROM lakesoul_query_result_set('7c9e8f3a-...', 0);
SELECT * FROM lakesoul_query_result_set('7c9e8f3a-...', 1, 100000, 10000);
```

`lakesoul_query_result` returns the last set, which is the usual psql-like
convenience. The whole script's spool is capped at 1 GiB by default; a larger
result fails with SQLSTATE `54000` and a hint to write the rows into a table
instead. Results are kept for 24 hours and job rows for 3 days by default.

A hard crash can leave an incomplete multipart part under the job prefix. The
completed result files are removed by the retention sweep; an object-store
lifecycle rule should abort incomplete multipart uploads.

## Cancel

```sql
SELECT lakesoul_cancel_query('7c9e8f3a-...');
```

Cancellation is cooperative, like the wire path's `CancelRequest`: a query
checks the token between batches. A streaming query stops promptly; a
CPU-bound operator stops when it next yields. `psql`'s Ctrl-C only cancels the
polling statement, never the detached job.

## Limits

- A job runs with the submitting user's identity, database, `search_path`, time
  zone and IVM switch. `statement_timeout` is not inherited; the script may
  `SET` it.
- `INSERT` still refuses a transaction block inside the job; the job's own
  statements are autocommit.
- The server-wide concurrency limit defaults to 4 running jobs. There is no
  per-user limit yet.
- The queue is in the server process. Each pg-wire cluster owns the jobs it
  accepted; set `--job-cluster-id` to a stable value when its listen address
  can change across restarts. Completed results may be read from any process
  that shares the warehouse.

## Configuration

| Flag | Environment | Default |
| --- | --- | --- |
| `--max-jobs` | `LAKESOUL_PG_MAX_JOBS` | `4` (`0` = unlimited) |
| `--job-result-max-mib` | `LAKESOUL_PG_JOB_RESULT_MAX_MIB` | `1024` |
| `--job-result-ttl-hours` | `LAKESOUL_PG_JOB_RESULT_TTL_HOURS` | `24` |
| `--job-record-ttl-hours` | `LAKESOUL_PG_JOB_RECORD_TTL_HOURS` | `72` |
| `--job-cluster-id` | `LAKESOUL_PG_JOB_CLUSTER_ID` | `<bind>:<port>` |
