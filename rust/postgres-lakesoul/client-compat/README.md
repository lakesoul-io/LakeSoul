# PostgreSQL client compatibility baseline

This directory contains the first real-client baseline for the
`postgres-lakesoul` PostgreSQL wire front end. It complements the Rust
`sql_compat` / `session_contract` / `insert_contract` / `update_contract` /
`upsert_contract` / `transaction_contract` suites, which use
`tokio-postgres`; those prove wire and SQL semantics, while these tests prove
that ordinary PostgreSQL driver workflows can execute.

## What it covers

| Client | Path |
| --- | --- |
| `psycopg2` | connect, SELECT, parameterized types including `bytea`, `executemany`, `execute_values`, UPSERT, transaction rollback/commit, duplicate-key SQLSTATE, `information_schema`/`pg_catalog`, `psql \d` |
| `psycopg3` | the same paths through its extended protocol and batch implementation |
| `SQLAlchemy` | `SELECT`/write through the `psycopg2` dialect, plus reflection of columns and the primary key |
| pgjdbc | connect, DDL, prepared batch `addBatch`/`executeBatch`, prepared SELECT, transaction rollback, `DatabaseMetaData`, `setFetchSize`, UPSERT |

The suite asserts **explicit refusal** for currently unsupported paths:

- server-side named cursors (`DECLARE`/`FETCH`): `0A000`
- `COPY ... FROM STDIN`: `0A000`
- `INSERT ... RETURNING`: `0A000`

If one of these starts working, the test fails and the compatibility contract
must be updated rather than silently widening the claim.

## Run it

The metadata PostgreSQL must be initialized with the repository schema and
migrations. The default local test database is `lakesoul_test`.

```sh
# Local debug binary
cargo -q build -p postgres-lakesoul
./rust/postgres-lakesoul/client-compat/run.sh
```

Set `POSTGRES_LAKESOUL_BIN` when the binary lives elsewhere, and
`LAKESOUL_PG_URL` / `LAKESOUL_PG_USERNAME` / `LAKESOUL_PG_PASSWORD` to point at
a non-default metadata database. `uv` is used to create a disposable Python
environment when available; otherwise install `requirements.txt` yourself.
The JDBC half requires Maven and JDK 11 or newer.

The runner starts its own server on a free loopback port and a temporary
warehouse, then removes both on exit.

## Current local baseline

The first run used:

- psycopg2 2.9.13, psycopg 3.3.9, SQLAlchemy 2.0.57
- pgjdbc 42.7.7 (pinned in `jdbc/pom.xml`)

Result: **all Python tests pass**; the pgjdbc workflow also passes without a
recorded gap. The tests already cover:

- connect and `SELECT`, including the default `sslmode=prefer` fallback;
- parameterized values, `numeric`, `date`, `timestamptz`, `boolean`, `bytea`;
- `executemany`, psycopg2 `execute_values`, and pgjdbc `addBatch`/`executeBatch`;
- `ON CONFLICT ... DO UPDATE`;
- transaction rollback and commit (including staged writes);
- duplicate-key `23505` and connection recovery;
- `information_schema`/`pg_catalog` table and column discovery, including psql `\d`;
- SQLAlchemy reflection of columns and primary keys through the `psycopg2` dialect;
- pgjdbc `DatabaseMetaData.getTables`/`getColumns`/`getPrimaryKeys`, `setFetchSize`, and UPSERT;
- Metabase-style startup `SET`/`SHOW` statements.

The remaining known gaps are:

| Gap | Detail |
| --- | --- |
| `COPY FROM STDIN` | explicitly refused with `0A000`; this is the main blocker for Airbyte/Singer-style loaders |
| server-side named cursors | psycopg3 reaches the explicit `0A000`; psycopg2 first probes the missing `pg_catalog.pg_cursors` and fails with `XX000` |
| `INSERT ... RETURNING` | explicitly refused with `0A000` |

SQLAlchemy reflection is certified through the `psycopg2` dialect. The
`psycopg3` dialect starts a nested transaction (`SAVEPOINT`) during reflection,
which LakeSoul does not support yet.

`version()` now returns a PostgreSQL-compatible baseline string
(`PostgreSQL 14.0 (LakeSoul ... / DataFusion ...)`) so SQLAlchemy can
initialize its PostgreSQL dialect. This is feature-discovery compatibility,
not a claim of PostgreSQL 14 parity.

## Interpreting a run

The test output prints the exact `psycopg2`, `psycopg` and `SQLAlchemy`
versions and the JDBC driver is pinned in `jdbc/pom.xml`. A result is only a
certification for those builds. The matrix should grow with tool-specific
queries captured from real BI/import tools; do not infer JDBC or psycopg
compatibility from the `tokio-postgres` contract suites.
