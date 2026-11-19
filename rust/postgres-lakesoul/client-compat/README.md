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
| `psycopg2` | connect, SELECT, parameterized types including `bytea` and `jsonb`, `jsonb` operators (`->`, `->>`, `?`), `executemany`, `execute_values`, `COPY ... FROM STDIN` and `COPY ... TO STDOUT` (TEXT/CSV, `copy_expert`/`copy_from`), UPSERT, transaction rollback/commit, duplicate-key SQLSTATE, `information_schema`/`pg_catalog`, `SHOW`/`SHOW ALL`, `current_setting`/`set_config`, `pg_settings`, `pg_roles`, `to_char`, `psql \d`/`\dt`/`\l`/`\dn`, named cursors |
| `psycopg3` | the same paths through its extended protocol and batch implementation, including `cursor.copy()`/`write_row`, CopyFail aborts and named cursors |
| `SQLAlchemy` | `SELECT`/write through the `psycopg2` dialect, plus reflection of columns and the primary key |
| pgjdbc | connect, DDL, prepared batch `addBatch`/`executeBatch`, prepared `jsonb` SELECT/INSERT, prepared SELECT, `CopyManager.copyIn` (TEXT/CSV), transaction rollback, `DatabaseMetaData`, `setFetchSize`, UPSERT |

The suite asserts **explicit refusal** for currently unsupported paths:

- `COPY ... TO` a file or program and `COPY ... FROM` a file: `0A000`
- a `COPY` that shares a multi-statement simple-query batch: `0A000`

Named cursors are forward-only and non-holdable. `WITH HOLD`, `SCROLL`, binary
cursors and backward fetches are explicitly refused instead of being silently
downgraded. `INSERT ... RETURNING` currently covers `INSERT ... VALUES` with
`*`, column references and aliases; expressions, `INSERT ... SELECT`,
`ON CONFLICT` and `UPDATE ... RETURNING` remain explicit `0A000` gaps.

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
- forward-only named cursors through psycopg2 and psycopg3, and the `pg_cursors` view;
- `SHOW`, `SHOW ALL`, `current_setting`/`set_config`, `pg_settings`, `pg_roles`
  and PostgreSQL `to_char` date patterns, including the psql `\dt`/`\l`/`\dn`
  metadata commands;
- `COPY ... FROM STDIN` and `COPY ... TO STDOUT` in TEXT and CSV, including
  NULL, escaped newlines, quoted commas/newlines, a CSV header, an omitted
  column list, one-commit atomicity, transaction rollback, driver-side
  encoders (`copy_from`, `write_row`, `CopyManager.copyIn`/`copyOut`), psql
  `\copy TO` and CopyFail aborts;
- `INSERT ... VALUES ... RETURNING` for `*`, column references and aliases, including SQLAlchemy Core;
- SQLAlchemy reflection of columns and primary keys through the `psycopg2` dialect;
- pgjdbc `DatabaseMetaData.getTables`/`getColumns`/`getPrimaryKeys`, `setFetchSize`, and UPSERT;
- Metabase-style startup `SET`/`SHOW` statements.

The remaining known gaps are:

| Gap | Detail |
| --- | --- |
| `pg_dump --data-only` | still blocked by its REPEATABLE READ transaction and `OPERATOR(pg_catalog.=)` syntax; `COPY TO` itself is supported |
| `COPY ... TO` a file/program | explicitly refused with `0A000`; use `COPY ... TO STDOUT` |
| `psql \du` | the role list query uses `ARRAY(subquery)`, which the pinned planner cannot execute; `pg_roles` itself answers |
| `current_user, session_user` | both work separately; one projection selecting both is limited by the parser's `current_user -> session_user` rewrite |
| numeric `to_char` | explicit error; date/time patterns are implemented |

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
