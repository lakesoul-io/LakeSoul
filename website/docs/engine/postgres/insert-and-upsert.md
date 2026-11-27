---
sidebar_position: 3
---

# Insert and upsert

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

The PostgreSQL front end supports `INSERT ... VALUES` and `INSERT ... SELECT`.
Both write through the native LakeSoul sink and commit immediately.

## Plain insert

```sql
INSERT INTO orders (id, day, region, amount)
VALUES (1, DATE '2026-01-01', 'APAC', 42.00);
```

On a primary-key table, a plain insert is **insert-only**: a duplicate key is
rejected with SQLSTATE `23505`. Use `ON CONFLICT` to upsert.

An `INSERT` that omits a column writes `NULL` to it. It is not a sparse upsert;
list every column whose value should be preserved.

## Upsert

```sql
INSERT INTO orders (id, day, region, amount)
VALUES (1, DATE '2026-01-01', 'APAC', 45.00)
ON CONFLICT (id) DO UPDATE
SET day = EXCLUDED.day,
    region = EXCLUDED.region,
    amount = EXCLUDED.amount;
```

Two conflict actions are supported:

| Clause | Behaviour |
| --- | --- |
| `ON CONFLICT (key) DO NOTHING` | Drops rows that conflict with an existing key |
| `ON CONFLICT (key) DO UPDATE SET col = EXCLUDED.col` | Replaces the row; every non-key column must be assigned |

`DO UPDATE` only accepts assignments of the form `column = EXCLUDED.column`,
and the assignment list must cover **every non-key column**. The `INSERT` must
name every table column, or omit the column list. An expression over the old row
(`SET amount = orders.amount + EXCLUDED.amount`) would require read-modify-write
and is refused.

`ON CONFLICT` is not supported on a range-partitioned table: LakeSoul unique keys
are partition-local.

## Insert from a query

```sql
INSERT INTO daily_orders (day, region, amount)
SELECT day, region, sum(amount)
FROM orders
GROUP BY 1, 2;
```

An `INSERT ... SELECT` can upsert as well, with the same column rules:

```sql
INSERT INTO orders (id, day, region, amount)
SELECT id, day, region, amount
FROM staging_orders
ON CONFLICT (id) DO UPDATE
SET day = EXCLUDED.day,
    region = EXCLUDED.region,
    amount = EXCLUDED.amount;
```

Without incremental SQL this is a normal insert. With it enabled for the session,
the same statement maintains the target from the source changelog. See
[Incremental SQL](../incremental-sql/overview.md).

## Transactions

Plain `INSERT` and `ON CONFLICT DO NOTHING` inside an explicit transaction
block are staged and published by `COMMIT`; `ROLLBACK` removes them. The general
`ON CONFLICT ... DO UPDATE` path still uses its own publication path and is
refused inside a block with `0A000`. Writes inside a multi-statement simple-query
batch are also refused.

`BEGIN` / `COMMIT` are accepted for reads and for the staged insert paths; put
general upserts in autocommit statements.

## Errors

| SQLSTATE | Meaning |
| --- | --- |
| `23505` | Duplicate key from a plain insert |
| `40001` | Lost a write race with a concurrent writer; retry the statement |
| `42601` | Source and target columns do not line up |
| `42804` | An expression is not assignment-castable to the target column |
| `22P02` | A string literal does not convert to the target type |
| `0A000` | `RETURNING` with `ON CONFLICT DO NOTHING`, a `RETURNING` expression, `ON CONFLICT` on a range-partitioned or CDC table, an assignment list that misses a non-key column, or a general `DO UPDATE` inside a transaction block |

`INSERT ... VALUES ... RETURNING`, `INSERT ... SELECT ... RETURNING` and
`UPDATE ... RETURNING` and `INSERT ... ON CONFLICT DO UPDATE ... RETURNING`
return the rows the write consumes. `RETURNING *`, column references and column
aliases are supported; expressions and `ON CONFLICT DO NOTHING ... RETURNING`
are not supported yet. `RETURNING` on foreign and external-file tables is also
refused. A prepared write whose parameters appear
in a `SELECT` source or an `UPDATE` assignment must give them explicit types
(`$1::integer`) or use the driver's typed-prepare API; only `INSERT ... VALUES`
infers parameter types from the target columns. A volatile source expression
would be evaluated twice (once for the returned rows, once for the write) and
is therefore refused.

## Index maintenance

If the table declares vector or full-text indexes, the index is updated after the
metadata commit. If the index build fails, the statement returns an error but the
inserted rows are already committed and visible.

## DELETE and TRUNCATE

See [DELETE and TRUNCATE](delete-and-truncate.md) for the row representations,
the partition fast path and the transaction boundaries.

## Write a JSON batch

Log shippers such as vector.dev bind one batch as a single JSONB parameter
and send this statement shape:

```sql
INSERT INTO orders
SELECT * FROM jsonb_populate_recordset(NULL::orders, $1)
ON CONFLICT (id) DO UPDATE
SET day = EXCLUDED.day,
    region = EXCLUDED.region,
    amount = EXCLUDED.amount;
```

`$1` is a JSON **array of objects**, one object per row. Keys map to column
names (a lowercase fallback is applied); a missing key writes `NULL`. A JSON
array of arrays is also accepted and maps values positionally to the columns
named in the `INSERT` column list. Both `DO NOTHING` and `DO UPDATE` conflict
actions behave the same as for a `VALUES` insert.

```json
[
  {"id": 1, "day": "2026-01-01", "region": "APAC", "amount": 42.00},
  {"id": 2, "day": "2026-01-02", "region": "EMEA", "amount": 51.50}
]
```

The function is recognised only in the `INSERT ... SELECT * FROM
jsonb_populate_recordset(NULL::table, $n)` shape and only with a JSONB scalar
parameter. It is not a general table function for arbitrary `SELECT`s;
`RETURNING` is supported on this shape (including `DO UPDATE`); it is projected
from the rewritten rows. ParameterDescription reports the placeholder as
`jsonb`, so clients do not have to supply a type hint when binding the array.
Invalid JSON and a positional row of the wrong width are rejected with
`22P02`/`42601`.

## Bulk write guidance

Each `INSERT` or `COPY` statement is one commit and is acknowledged after the
metadata commit. To ingest high volumes through the PG wire endpoint:

- Send **batches, not single rows**. Put many rows in one `VALUES` list, bind a
  JSON array as above, or use [`COPY ... FROM STDIN`](copy.md). For vector.dev's
  PostgreSQL sink, raise the sink's batch size so one statement carries more
  rows.
- Group several writes inside `BEGIN ... COMMIT` when the statement shape
  allows it; staged writes publish together at `COMMIT` and reduce metadata
  round trips. General `DO UPDATE` still has to run in autocommit.
- A batch produces one file per **hash bucket and range partition** it touches,
  not one file per batch. Choose `hash_bucket_num` and range partitions for
  the read parallelism you need; avoid an unnecessarily large bucket count for
  write-heavy tables.
- Concurrent writers to the **same table and partition** serialize on that
  partition's metadata version. A plain `INSERT`/`UPDATE` that loses the race
  returns `40001` and must be retried by the client. `ON CONFLICT DO UPDATE`
  retries its publication in the server: compatible commits (compaction,
  append, merge) are rebased onto the new snapshot, while an intervening
  update or delete is reported as a conflict.
- If many clients write concurrently, spread them across partitions or tables
  so they do not all claim the same version slot. The server's commit retry
  budget is bounded, so clients should still handle a final `40001` by
  retrying the statement.
- Long-running ingestion still relies on the background compaction service to
  merge files across batches.

## Next

- [Transactions and catalog](transactions-and-catalog.md)
- [COPY](copy.md)
- [Incremental SQL](../incremental-sql/getting-started.md)
- [Vector search](../vector-search/quickstart.md)
