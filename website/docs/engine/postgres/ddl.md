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

An unknown option is rejected with `0A000` rather than ignored.

Vector/text index declarations (`vector_index_columns`, `text_index_columns`,
`index_maintenance`) are documented in [Vector and text index DDL](../index-ddl.md).

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

`numeric` and `decimal` require an explicit precision and scale; bare `numeric`
is rejected, as is precision above 76. `time(p)`/`timestamp(p)` accept only
`p = 0, 3, 6`; other precisions are rejected. `interval` and `inet` columns are
rejected (interval expressions still work). Types outside this table are
rejected with `0A000`.

The pgvector `vector(n)` / `vector` column types and the accepted vector-literal
forms are documented in [Vector and text index DDL](../index-ddl.md).

See [Data types](../../reference/data-types.md) for the full mapping and the
query-side type behaviour.

## Vector and text indexes

`vector(n)` columns, `vector_index_columns`, `text_index_columns`,
`CREATE INDEX`, `REINDEX TABLE` and deferred index maintenance are documented
in [Vector and text index DDL](../index-ddl.md).

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

Each refusal returns SQLSTATE `0A000` with a message naming the clause.
Index-specific refusals are listed in
[Vector and text index DDL](../index-ddl.md#refused-forms).

## Next

- [Vector and text index DDL](../index-ddl.md)
- [Insert and upsert](insert-and-upsert.md)
- [Data types](../../reference/data-types.md)
- [Physical file formats](../../operations/physical-file-formats.md)
