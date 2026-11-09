# PostgreSQL foreign tables (ADBC)

LakeSoul's PostgreSQL front end can expose a table from another database as a
foreign table in the current LakeSoul namespace. Queries read the remote table
through an [ADBC](https://arrow.apache.org/adbc/) driver and can join it with
local LakeSoul tables in one DataFusion plan. `INSERT INTO` writes through the
same driver.

The endpoint stores servers, user mappings and foreign tables in the
`federation` metadata schema. A query plan contains only the server key;
credentials stay in metadata and are resolved by the coordinator or a
distributed worker. Projection, filter and limit pushdown use the SQL dialect
recorded on the server.

## Install an ADBC driver

Install the driver for each source database with
[`dbc`](https://docs.columnar.tech/dbc/):

```bash
dbc install postgresql
dbc install mysql
dbc install sqlite
```

Drivers are installed into the user driver directory and are loaded by name.
`ADBC_DRIVER_PATH` can point at another driver location. Every
`lakesoul-worker` that may execute a foreign scan needs the same driver
installed.

## Create a server and user mapping

`adbc_fdw` is the generic wrapper. `postgres_fdw` remains available as a
compatibility alias and defaults to the `postgresql` ADBC driver.

```sql
CREATE EXTENSION adbc_fdw;

CREATE SERVER app_source
FOREIGN DATA WRAPPER adbc_fdw
OPTIONS (
  driver 'postgresql',
  uri 'postgresql://postgres.example.com:5432/app'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER app_source
OPTIONS (
  user 'lakesoul_reader',
  password '...'
);
```

The dialect is fixed when the server is created. It is inferred from the ADBC
driver, or set explicitly with the `dialect` option:

| Driver | Default dialect |
|---|---|
| `postgresql` | `postgres` |
| `mysql` | `mysql` |
| `sqlite` | `sqlite` |
| `duckdb` | `duckdb` |
| `bigquery` | `bigquery` |
| `snowflake` | `snowflake` |
| `trino` | `generic` |
| other | required `dialect` option |

Supported `dialect` values are `postgres`, `mysql`, `sqlite`, `duckdb`,
`bigquery`, `snowflake` and `generic`.

```sql
CREATE SERVER app_mysql
FOREIGN DATA WRAPPER adbc_fdw
OPTIONS (
  driver 'mysql',
  uri 'mysql://mysql.example.com:3306/app',
  dialect 'mysql'
);
```

`uri` is the portable way to configure a connection. PostgreSQL servers may
instead use `host`, `port`, `dbname` and the user mapping's `user`/`password`
options. Any option that is not reserved by the FDW is forwarded to the ADBC
driver as a native database option.

The password is stored in the metadata PostgreSQL. Use a dedicated
least-privilege reader and protect the metadata database. Encryption of these
options is planned separately.

## Create a foreign table

```sql
CREATE FOREIGN TABLE public.remote_users (
  id      bigint,
  name    text,
  amount  numeric(20,4)
)
SERVER app_source
OPTIONS (
  schema_name 'public',
  table_name  'users'
);
```

The column list uses the same PostgreSQL-to-Arrow mapping as `CREATE TABLE`.
It must match the remote table's column names and compatible types. The
foreign table can be queried directly or joined with LakeSoul tables:

```sql
SELECT l.id, l.name, r.amount
FROM local_users l
JOIN remote_users r USING (id);
```

## Write to a foreign table

Writes are append-only. To make retries and repeated IVM deliveries idempotent,
declare the remote key and a conflict policy on the foreign table:

```sql
CREATE FOREIGN TABLE public.remote_users (
  id      bigint,
  name    text,
  amount  numeric(20,4)
)
SERVER app_source
OPTIONS (
  schema_name 'public',
  table_name  'users',
  primary_key 'id',
  on_conflict 'update'       -- or 'nothing'
);
```

Then a normal insert runs on the data workers:

```sql
INSERT INTO remote_users VALUES (1, 'alice', 10.0), (2, 'bob', 20.0);
```

`on_conflict 'update'` generates `ON CONFLICT (id) DO UPDATE` on PostgreSQL and
SQLite, or `ON DUPLICATE KEY UPDATE` on MySQL. `on_conflict 'nothing'` ignores
an existing key instead. Re-running the same statement therefore converges
instead of duplicating rows. `write_batch_size` (default 1024) bounds how many
rows go into one generated statement. With `on_conflict 'update'`, the source
must not contain the same conflict key twice in one statement; PostgreSQL
rejects that as a command that would affect one row twice, so deduplicate the
source first.

Writes use at-least-once delivery with per-batch commits, not a global
transaction. If a task fails, batches that already committed remain; retrying
the statement is safe only when the target has the declared unique key and the
conflict policy is `update` or `nothing`. A plain append has no idempotency
guarantee.

The write stage runs on workers, not on the coordinator: its input is hash
repartitioned and the row-count summary goes back to the coordinator. At least
two write tasks are used when workers are configured, including a single-worker
deployment. Without a distributed worker resolver, the write runs locally.

## Cleanup

```sql
DROP FOREIGN TABLE public.remote_users;
DROP USER MAPPING FOR CURRENT_USER SERVER app_source;
DROP SERVER app_source CASCADE;
```

Dropping a server without `CASCADE` is refused while foreign tables depend on
it. `DROP FOREIGN TABLE` removes only the metadata; the remote table is never
dropped.

## Distributed workers

A single foreign scan is one partition. When the distributed planner routes a
stage to a worker, the worker needs the same server definitions. Start each
`lakesoul-worker` with the metadata connection and a warehouse:

```bash
lakesoul-worker \
  --warehouse-prefix s3://bucket/warehouse \
  --port 50051
```

with `LAKESOUL_PG_URL`, `LAKESOUL_PG_USERNAME` and `LAKESOUL_PG_PASSWORD`
pointing at the metadata PostgreSQL. The worker loads all federation user
mappings at startup and registers their ADBC pools by the server and the
user-mapping key (the query user, or `PUBLIC` for a shared mapping). Create
servers and user mappings before starting workers, or restart workers after a
change; the registry is not polled.

The coordinator generates the remote SQL with the dialect stored on the
server; a worker only executes the serialized SQL. A foreign scan is one
partition, so a plan whose stages are all single-task runs on the coordinator,
because `datafusion-distributed` removes a network boundary that would connect
two single-task stages. When the scan is part of a larger plan with a multi-task
stage - for example a join with a LakeSoul table scanned over several tasks -
the scan stage is sent to a worker and the worker resolves the
server/user-mapping key from its own federation registry. Parallel remote
scanning (several tasks per foreign table) is a later optimization.

Foreign writes take the opposite shape: the write stage is hash repartitioned
and always runs on workers when workers are configured. Each task commits its
own batches; there is no global transaction across tasks.

## Current limits

- Only `INSERT` is implemented; `UPDATE` and `DELETE` on a foreign table are
  not implemented.
- `INSERT ... ON CONFLICT`, `INSERT OVERWRITE` and `RETURNING` are not
  supported. Configure `primary_key` and `on_conflict` on the foreign table
  instead.
- Writes are at-least-once with per-batch commits. There is no global
  transaction across worker tasks; a failed statement can leave already
  committed batches. Idempotency requires a unique key and `on_conflict`.
- SQLite permits only one writer at a time. Parallel write tasks can fail with
  `database is locked`; use a server database for parallel writes.
- `IMPORT FOREIGN SCHEMA` is not implemented yet; create each foreign table
  explicitly.
- Supported DDL is `CREATE/DROP SERVER`, `CREATE/DROP USER MAPPING`,
  `CREATE/DROP FOREIGN TABLE` and `CREATE/DROP EXTENSION adbc_fdw` (or
  `postgres_fdw`).
- `ALTER SERVER`, `ALTER FOREIGN TABLE`, `GRANT` and FDW catalog views are not
  implemented.
- The foreign table's declared schema is the type contract. A source type that
  cannot be represented by the endpoint's Arrow/PostgreSQL types needs an
  explicit cast or a compatible declared column.
- A query reads each foreign table through its own remote connection; two
  foreign tables in one query do not share a remote transaction snapshot.
