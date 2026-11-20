---
sidebar_position: 2
---

# DDL

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

The PostgreSQL front end supports `CREATE TABLE` and `DROP TABLE`. The table is
created through the LakeSoul catalog; `CREATE TABLE` needs a warehouse location
configured on the server (`--warehouse-prefix`).

## CREATE TABLE

```sql
CREATE TABLE orders (
  id     bigint primary key,
  day    date not null,
  region text not null,
  amount numeric(20,4)
) PARTITIONED BY (day)
  WITH (hash_bucket_num = 8, file_format = 'vortex-compact');
```

- A **primary key** can be declared as a column constraint (`id bigint primary
  key`) or a table constraint (`PRIMARY KEY (id)`).
- `PARTITIONED BY (col, ...)` uses existing columns and declares range
  partitions. Partition columns must be declared in the table and should be
  `NOT NULL`.
- `WITH (...)` configures the table. See the options below.

`PARTITIONED BY` is the LakeSoul/Hive form: it names existing columns. PostgreSQL's
`PARTITION BY` clause is not accepted.

## Table options

| Option | Values | Meaning |
| --- | --- | --- |
| `hash_bucket_num` | `1..=32768` | Hash buckets for a primary-key table; default `4` |
| `file_format` | `parquet`, `vortex`, `vortex-compact` | Physical file format; default `vortex-compact` |
| `append_only` | `true`, `false` | Append-only table; no primary-key merge |
| `use_cdc` | `true`, `false` | CDC table with a change column; required for deletes and incremental SQL targets |
| `lakesoul_cdc_change_column` | column name | Change column for CDC; defaults to `rowKinds` |
| `vector_index_columns` | JSON array | Vector index declarations, e.g. `'[{"column":"embedding","dim":8}]'` |
| `text_index_columns` | JSON array | Text index declarations, e.g. `'[{"column":"body"}]'` |
| `index_maintenance` | `inline`, `deferred` | `deferred` commits data only and leaves index builds to the background runner |

An unknown option is rejected with `0A000` rather than ignored.

`use_cdc = true` requires a change column. Declare `rowKinds`, or name another column with `lakesoul_cdc_change_column`. Without it, deletes cannot be represented, and an incremental SQL target cannot retract rows.

## Types

`CREATE TABLE` accepts a fixed set of column types:

| PostgreSQL type | Stored as |
| --- | --- |
| `smallint`, `int2` | `Int16` |
| `integer`, `int`, `int4` | `Int32` |
| `bigint`, `int8` | `Int64` |
| `real`, `float4` | `Float32` |
| `double precision`, `float8` | `Float64` |
| `boolean` | `Boolean` |
| `text`, `varchar`, `char` | `Utf8`; `varchar(n)`/`char(n)` keep their length in `pg.typmod` |
| `uuid` | `Utf8` with a `pg.type=uuid` marker |
| `INT[]`, `TEXT[]`, ... | `List<element>`; one-dimensional arrays of supported scalar types |
| `json` | `Utf8` with the `arrow.json` extension |
| `jsonb` | `Utf8` with `arrow.json` and `pg.json` markers |
| `date` | `Date32` |
| `time(p)` | `Time32` for `p=0/3`, `Time64` for `p=6` |
| `timestamp(p)` | `Timestamp` for `p=0/3/6`; default `6` |
| `timestamptz(p)` | UTC `Timestamp` for `p=0/3/6` |
| `bytea` | Binary |
| `numeric(p, s)`, `decimal(p, s)` | `Decimal128` for `p <= 38`, `Decimal256` for `p <= 76` |
| `money` | `Decimal128(19,2)` with a `pg.type=money` marker |
| `vector(n)` | `FixedSizeList(Float32, n)` with the `pg.vector` marker |
| `vector` | `List(Float32)` with the `pg.vector` marker |

`numeric` and `decimal` require an explicit precision and scale; bare `numeric`
is rejected, as is precision above 76. `time(p)`/`timestamp(p)` accept only
`p = 0, 3, 6`; other precisions are rejected. `interval` and `inet` columns are
rejected (interval expressions still work). Types outside this table are
rejected with `0A000`.

`INSERT ... VALUES` accepts the pgvector text form for a vector column, either
bare (`'[1,2,3]'`) or cast (`'[1,2,3]'::vector(3)`).

See [Data types](../../reference/data-types.md) for the full mapping and the
query-side type behaviour.

## Vector and text indexes

A vector index requires a primary key and a `vector(n)` (or `LIST` of float)
column:

```sql
CREATE TABLE docs (
  id        bigint primary key,
  body      text,
  embedding vector(8)
) WITH (vector_index_columns = '[{"column":"embedding","dim":8}]',
        text_index_columns   = '[{"column":"body"}]');
```

`CREATE INDEX` declares an index on an existing table. LakeSoul has one
vector-index implementation, so `USING hnsw` and `USING ivfflat` are accepted
and normalized to it; `WITH (lists = ..., m = ..., ef_construction = ...)` is
accepted for compatibility and ignored. Text indexes use `USING gin`:

```sql
CREATE INDEX docs_embedding_idx ON docs USING hnsw (embedding)
  WITH (m = 16, ef_construction = 64);
CREATE INDEX docs_body_idx ON docs USING gin (body);
```

Only one column per statement is supported, without expressions, `INCLUDE` or
a partial `WHERE`. Index names are accepted for client compatibility but are
not stored: LakeSoul identifies an index by `(table, column)`, so
`DROP INDEX name` and `REINDEX INDEX name` are not available.

`CREATE INDEX` builds the index over existing rows unless the table has
`index_maintenance = 'deferred'`, in which case the declaration is stored and
the background runner builds it. `ALTER TABLE ... SET (...)` only declares or
replaces index properties; run `REINDEX TABLE` to build them:

```sql
ALTER TABLE docs SET (index_maintenance = 'deferred');
REINDEX TABLE docs;
SELECT lakesoul_rebuild_index('docs', 'all');  -- all | vector | text
```

With `index_maintenance = 'deferred'`, writes commit data only. A coordinator
started with `--index-maintenance-executor local` supervises a
`--index-maintenance-runner` child; Helm runs it as a separate Deployment. The
runner holds a lease in `maintenance.lease`, so several servers can run without
duplicating the work. Vector search on a not-yet-built deferred declaration
falls back to the exact ordered scan; text search reports that the index is not
built until the runner has built it.

## DROP TABLE

```sql
DROP TABLE orders;
```

`DROP TABLE IF EXISTS` is supported. Dropping a table removes its catalog entry;
data files follow the normal cleanup policy.

## Refused clauses

The front end refuses, rather than silently ignoring:

- `CREATE TABLE AS`, `CREATE TEMPORARY TABLE`, `CREATE EXTERNAL TABLE`,
  `CREATE OR REPLACE TABLE`
- `LIKE`, `CLONE`, `INHERITS`, `PARTITION OF`, `FOR VALUES`
- `UNIQUE`, `CHECK` and `FOREIGN KEY` constraints, and column defaults
- `STORED AS` / `LOCATION` (use `WITH (file_format = ...)` and the server
  warehouse instead)
- `PRIMARY KEY` as a clause outside the supported column/table constraint forms
- `CREATE INDEX ... USING btree/hash/gist/brin`, multi-column or expression
  indexes, partial indexes and `INCLUDE`
- `DROP INDEX` and `REINDEX INDEX/SCHEMA/DATABASE`; use the per-column
  `lakesoul_drop_vector_index` function and `REINDEX TABLE`

Each refusal returns SQLSTATE `0A000` with a message naming the clause.

## Next

- [Insert and upsert](insert-and-upsert.md)
- [Data types](../../reference/data-types.md)
- [Physical file formats](../../operations/physical-file-formats.md)
