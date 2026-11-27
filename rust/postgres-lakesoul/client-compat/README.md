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
| `psycopg2` | connect, SELECT, parameterized types including `bytea` and `jsonb`, `jsonb` operators (`->`, `->>`, `?`), `DELETE`/`TRUNCATE` (including `RETURNING`), `executemany`, `execute_values`, `COPY ... FROM STDIN` (TEXT/CSV/binary) and `COPY ... TO STDOUT` (TEXT/CSV, `copy_expert`/`copy_from`), UPSERT, transaction rollback/commit, duplicate-key SQLSTATE, `information_schema`/`pg_catalog`, `SHOW`/`SHOW ALL`, `current_setting`/`set_config`, `pg_settings`, `pg_roles`, `to_char`, `psql \d`/`\dt`/`\l`/`\dn`, named cursors |
| `psycopg3` | the same paths through its extended protocol and batch implementation, including `cursor.copy()`/`write_row`, CopyFail aborts and named cursors |
| `SQLAlchemy` | `SELECT`/write through the `psycopg2` dialect, plus reflection of columns and the primary key |
| `test_types.py` | psycopg2 + psycopg3 semantic types: `uuid`, `uuid[]`, `money`, `numeric(60,10)`/`numeric(76,0)`, `varchar(n)`/`char(n)`, `time`/`timestamp(p)`/`timestamptz`, arrays of int/text/numeric/varchar/money, OIDs and `pg_attribute` metadata, plus explicit refusals |
| `test_batch_insert.py` | the vector.dev sink shape `INSERT INTO t SELECT * FROM jsonb_populate_recordset(NULL::t, $1)` (plain, `DO NOTHING`, `DO UPDATE`, NULL values and `RETURNING`) and large multi-row `VALUES`, through both drivers; `BEGIN … COMMIT` folds several JSON inserts into one metadata commit |
| `test_pgvector.py` | pgvector type discovery, `Vector` parameters and `<->`/`<#>`/`<=>` distance, vector column round trip, `format_type`, SQLAlchemy `Vector` reflection/select, and `CREATE TABLE`/`CREATE INDEX` DDL (including `vector_index_columns` declarations, refused index forms and SQLAlchemy index creation) |
| pgjdbc | connect, DDL, prepared batch `addBatch`/`executeBatch`, prepared `jsonb` SELECT/INSERT, prepared `DELETE`/`TRUNCATE`, prepared SELECT, `CopyManager.copyIn` (TEXT/CSV/binary), transaction rollback, `DatabaseMetaData`, `setFetchSize`, UPSERT; semantic types and `ParameterMetaData` in `JdbcTypeSmoke`, pgvector type/query and `CREATE TABLE`/`CREATE INDEX` DDL via `com.pgvector:pgvector` in `JdbcVectorSmoke` |

Time travel (`lakesoul_table_scan(table, mode, value)`) is also covered through
both drivers: `test_time_travel.py` pins a scan to a `timestamp` between two
commits and checks that a `WHERE` clause applies to the pinned rows; the Rust
contract covers `tag` and `snapshot_id`.

The suite asserts **explicit refusal** for currently unsupported paths:

- `COPY ... TO` a file or program and `COPY ... FROM` a file: `0A000`
- a `COPY` that shares a multi-statement simple-query batch: `0A000`

Named cursors are forward-only and non-holdable. `WITH HOLD`, `SCROLL`, binary
cursors and backward fetches are explicitly refused instead of being silently
downgraded. `INSERT ... RETURNING` covers `INSERT ... VALUES`,
`INSERT ... SELECT` and the JSON-batch shape, including full-row `ON CONFLICT
DO UPDATE`; `UPDATE ... RETURNING` covers the new rows. `*` column references
and aliases are supported; expressions and `ON CONFLICT DO NOTHING RETURNING`
remain explicit `0A000` gaps.

If one of these starts working, the test fails and the compatibility contract
must be updated rather than silently widening the claim.

## Run it

The metadata PostgreSQL must be initialized with the repository schema and
migrations. The default local test database is `lakesoul_test`.

```sh
# Same profile CI uses; the runner needs both binaries
cargo -q build --profile test-fast -p postgres-lakesoul -p lakesoul-console
./rust/postgres-lakesoul/client-compat/run.sh
```

Set `POSTGRES_LAKESOUL_BIN` when the binary lives elsewhere, and
`LAKESOUL_PG_URL` / `LAKESOUL_PG_USERNAME` / `LAKESOUL_PG_PASSWORD` to point at
a non-default metadata database. `uv` is used to create a disposable Python
environment when available; otherwise install `requirements.txt` yourself.
The JDBC half requires Maven and JDK 11 or newer.

The runner starts its own server on a free loopback port and a temporary
warehouse, then removes both on exit. It generates a self-signed certificate
with `openssl` and starts the server with `--tls-cert`, `--tls-key` and
`--require-tls`; every client in the baseline connects with
`PGSSLMODE=require`. `test_tls.py` additionally asserts that a plaintext
`sslmode=disable` connection is refused with `28000`.

## Current local baseline

The first run used:

- psycopg2 2.9.13, psycopg 3.3.9, SQLAlchemy 2.0.57
- pgjdbc 42.7.7 (pinned in `jdbc/pom.xml`)

The extended type/pgvector baseline adds `pgvector` (Python) 0.5.x and
`com.pgvector:pgvector` 0.1.6 (pinned in `jdbc/pom.xml`).

Result: **all Python tests pass**; the pgjdbc workflow also passes without a
recorded gap. The tests already cover:

- connect and `SELECT` over TLS (`sslmode=prefer` and `sslmode=require`), with
  `sslmode=disable` refused by the server;
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
- `COPY ... FROM STDIN` in TEXT, CSV and binary, and `COPY ... TO STDOUT` in
  TEXT and CSV, including
  NULL, escaped newlines, quoted commas/newlines, a CSV header, an omitted
  column list, one-commit atomicity, transaction rollback, driver-side
  encoders (`copy_from`, `write_row`, `CopyManager.copyIn`/`copyOut`), psql
  `\copy TO` and CopyFail aborts;
- `INSERT ... VALUES ... RETURNING`, `INSERT ... SELECT ... RETURNING` and
  `UPDATE ... RETURNING` for `*`, column references and aliases, including SQLAlchemy Core;
- SQLAlchemy reflection of columns and primary keys through the `psycopg2` dialect;
- pgjdbc `DatabaseMetaData.getTables`/`getColumns`/`getPrimaryKeys`, `setFetchSize`, and UPSERT;
- Metabase-style startup `SET`/`SHOW` statements.

## High-volume insert guidance

For log/iot ingestion, send rows in batches rather than one statement per row:

- The JSONB `jsonb_populate_recordset` shape used by vector.dev's PostgreSQL
  sink is planned by a dedicated fast path: the array is decoded into one
  `VALUES` batch, and the client is acknowledged only after publication.
  `DO NOTHING` and `DO UPDATE` keep the same semantics as ordinary `INSERT`.
  ParameterDescription reports `$1` as `jsonb`, so clients can bind the array
  without sending a type hint.
- A large `INSERT INTO t VALUES (...), (...), …` follows the same path.
- Several writes inside `BEGIN … COMMIT` are published by one
  `commit_transaction`, which reduces metadata round trips.
- Long-running writes should still rely on the Spark compaction service to
  merge files across batches.

File count is **not** one per batch: the write sink opens one writer per
`(hash bucket, range partition descriptor)`. With `hash_bucket_num = 1` and no
range columns a batch is one file; with four buckets it is four; a
range-partitioned table writes one file per touched partition, and publishes
one metadata commit per partition. The
`insert_batch_file_count_contract` test pins these numbers for both the VALUES
and JSON shapes.

Single-frontend measurements (local disk, no workers, 1000 rows per statement):

| Shape | Clients | Rows/s | p50 ACK | Result |
| --- | --- | --- | --- | --- |
| VALUES / JSON | 1 | 13k–16k | ~75–80 ms | all commits succeed |
| VALUES / JSON | 8, same partition | 3k–6k | ~200–310 ms | most commits fail with `40001` |
| VALUES, distinct partitions | 8 | ~18k | ~200 ms | all commits succeed |

Concurrent writes to the same `(table, partition)` use optimistic CAS. A plain
INSERT or UPDATE/DELETE still returns SQLSTATE `40001` on a stale version and
must be retried by the client. An `ON CONFLICT DO UPDATE` (upsert) instead
retries its commit inside the metadata client: it re-reads the current partition
version and regenerates the snapshot without recomputing or re-uploading the
rows. Compaction, append and merge commits that landed in between are rebased
into the new snapshot; an intervening update or delete is reported as a
conflict. A writer still needs to retry when the five-attempt budget is
exhausted under heavy contention, or shard across partitions/tables.

The remaining known gaps are:

| Gap | Detail |
| --- | --- |
| `pg_dump --data-only` | still blocked by its REPEATABLE READ transaction and `OPERATOR(pg_catalog.=)` syntax; `COPY TO` itself is supported |
| `COPY ... TO` a file/program | explicitly refused with `0A000`; use `COPY ... TO STDOUT` |
| `psql \du` | the role list query uses `ARRAY(subquery)`, which the pinned planner cannot execute; `pg_roles` itself answers |
| `current_user, session_user` | both work separately; one projection selecting both is limited by the parser's `current_user -> session_user` rewrite |
| numeric `to_char` | explicit error; date/time patterns are implemented |
| psycopg2 `register_vector` | call with `arrays=False`: LakeSoul reports `vector` but not the `_vector` array type that the adapter's default registration expects |
| psycopg2 standalone large numeric predicate | an unspecified-OID `Decimal` parameter is inferred as `float8` outside a typed INSERT and loses precision; psycopg3 binds the numeric OID and stays exact |
| `money[]` in psycopg2 | no built-in array loader; the driver returns PostgreSQL's array text form (`{$3.25}`) instead of a Python list |
| array subscript results | `arr[1]` is reported as text by the wire layer; cast with `::text` when comparing across both drivers |
| binary array parameters | an over-long `varchar(n)` element sent as a bound binary array is not rejected; only SQL array literals are checked element by element |
| SQLAlchemy DDL transactions | DDL is refused inside a transaction block; use an autocommit connection (`isolation_level="AUTOCOMMIT"`) for `CREATE TABLE`/`CREATE INDEX`. The psycopg3 SQLAlchemy dialect also issues nested transactions during type discovery and is not certified |

SQLAlchemy reflection is certified through the `psycopg2` dialect. The
`psycopg3` dialect's reflection uses `SAVEPOINT`, which is now supported, but
the dialect is not certified yet because other reflection queries remain gaps.

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
