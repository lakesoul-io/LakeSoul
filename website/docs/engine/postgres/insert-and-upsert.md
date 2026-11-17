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
| `0A000` | `RETURNING` with `ON CONFLICT` or a non-`VALUES` source, `ON CONFLICT` on a range-partitioned or CDC table, an assignment list that misses a non-key column, or a general `DO UPDATE` inside a transaction block |

`INSERT ... VALUES ... RETURNING` returns the inserted rows. `RETURNING *`,
column references and column aliases are supported; expressions, `INSERT ...
SELECT ... RETURNING`, `ON CONFLICT ... RETURNING` and `UPDATE ... RETURNING`
are not supported yet. `RETURNING` on foreign and external-file tables is also
refused.

## Index maintenance

If the table declares vector or full-text indexes, the index is updated after the
metadata commit. If the index build fails, the statement returns an error but the
inserted rows are already committed and visible.

## Next

- [Transactions and catalog](transactions-and-catalog.md)
- [Incremental SQL](../incremental-sql/getting-started.md)
- [Vector search](../vector-search/quickstart.md)
