---
sidebar_position: 2
---

# Quick start: the PostgreSQL endpoint

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

This page starts the LakeSoul engine, exposes it over the PostgreSQL wire
protocol, and runs a first `CREATE TABLE` / `INSERT` / `SELECT` from `psql`.

## Prerequisites

- A PostgreSQL 14+ database for LakeSoul metadata. The quick start uses Docker.
- A Rust toolchain and the native build dependencies (`protoc`, Clang, `flatc`).
  See [Development environment](../development/environment.md) if `cargo build`
  fails.
- `psql` on the machine where you connect.

## 1. Start the metadata database

LakeSoul stores table and commit metadata in PostgreSQL. This database is **not**
the endpoint users connect to; it is the metadata store.

```bash
docker run -d --name lakesoul-pg -p 5432:5432 \
  -e POSTGRES_USER=lakesoul_test \
  -e POSTGRES_PASSWORD=lakesoul_test \
  -e POSTGRES_DB=lakesoul_test \
  postgres:14.5

docker cp script/meta_init.sql lakesoul-pg:/
docker exec -i lakesoul-pg sh -c \
  "PGPASSWORD=lakesoul_test psql -U lakesoul_test -f meta_init.sql"
```

The schema creates the `default` namespace. If you already have a PostgreSQL
instance, run `script/meta_init.sql` against it instead.

## 2. Start the endpoint

From the repository root:

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix file:///tmp/lakesoul/warehouse
```

The endpoint listens on `127.0.0.1:5433` and uses the default metadata
connection (`lakesoul_test` / `lakesoul_test` on `127.0.0.1:5432`). Override it
with environment variables when your metadata database is elsewhere:

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```

`--warehouse-prefix` is required for `CREATE TABLE`: it is the location new
table data is written to. It can be a local path (`file:///...`) or an object
store prefix (`s3://bucket/warehouse`).

## 3. Connect

In PostgreSQL, a database maps to a LakeSoul namespace. The quick start uses the
`default` namespace created by `meta_init.sql`:

```bash
psql "postgresql://lakesoul_test:lakesoul_test@127.0.0.1:5433/default"
```

Any PostgreSQL client works: `psql`, JDBC, `pgx`, DBeaver, or a BI tool.

## 4. Create, insert, query

```sql
CREATE TABLE orders (
  id     bigint primary key,
  region text,
  amount numeric(20,4)
) WITH (hash_bucket_num = 8);

INSERT INTO orders VALUES (1, 'APAC', 42.00)
ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount;

SELECT region, sum(amount) FROM orders GROUP BY 1;
```

`hash_bucket_num` controls the hash partitioning of a primary-key table.
`numeric` requires an explicit precision and scale; `numeric` without them is
rejected.

`psql \d orders` works, and so do `pg_catalog` queries.

## 5. Next steps

- [DDL](../engine/postgres/ddl.md) — table options, types, partitioning and the
  clauses LakeSoul refuses.
- [Insert and upsert](../engine/postgres/insert-and-upsert.md) — `ON CONFLICT`
  semantics, duplicate keys, and the write path.
- [Incremental SQL](../engine/incremental-sql/getting-started.md) — maintain a
  target from the table's commit log with `INSERT ... SELECT`.
- [Vector search](../engine/vector-search/overview.md) — pgvector-style SQL over
  lake-resident ANN indexes.

## Object storage

Pass object store credentials when the warehouse prefix is on S3 or an
S3-compatible store:

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix s3://lakesoul-test-bucket/warehouse \
  --endpoint http://127.0.0.1:9000 \
  --s3-bucket lakesoul-test-bucket \
  --s3-access-key rustfsadmin \
  --s3-secret-key rustfsadmin
```

Add `--s3-virtual-host-style` for stores that require virtual-hosted requests.

## Troubleshooting

- **`FATAL: database ... does not exist`** — connect to an existing namespace.
  The quick start uses `default`; create additional namespaces with the
  management API or the Python catalog.
- **`CREATE TABLE needs a configured warehouse`** — start the server with
  `--warehouse-prefix`.
- **Connection refused** — check `--bind` and `--port`; the default bind address
  is `127.0.0.1`.
