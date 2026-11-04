---
sidebar_position: 3
sidebar_label: 写入与 Upsert
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
ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount;
```

Two conflict actions are supported:

| Clause | Behaviour |
| --- | --- |
| `ON CONFLICT (key) DO NOTHING` | Drops rows that conflict with an existing key |
| `ON CONFLICT (key) DO UPDATE SET col = EXCLUDED.col` | Replaces the whole row with the incoming row |

`DO UPDATE` only accepts assignments of the form `column = EXCLUDED.column`.
An expression over the old row (`SET amount = orders.amount + EXCLUDED.amount`)
would require read-modify-write and is refused.

## Insert from a query

```sql
INSERT INTO daily_orders (day, region, amount)
SELECT day, region, sum(amount)
FROM orders
GROUP BY 1, 2;
```

Without incremental SQL this is a normal insert. With it enabled for the session,
the same statement maintains the target from the source changelog. See
[Incremental SQL](../incremental-sql/overview.md).

## Transactions

Writes are refused inside an explicit transaction block and inside a
multi-statement simple-query batch. LakeSoul commits immediately, so a later
`ROLLBACK` or a failing statement in the batch could not undo the rows.

`BEGIN` / `COMMIT` are accepted for read-only work; put writes in autocommit
statements.

## Errors

| SQLSTATE | Meaning |
| --- | --- |
| `23505` | Duplicate key from a plain insert |
| `40001` | Lost a write race with a concurrent writer; retry the statement |
| `42601` | Source and target columns do not line up |
| `42804` | An expression is not assignment-castable to the target column |
| `22P02` | A string literal does not convert to the target type |
| `0A000` | `RETURNING`, `ON CONFLICT` expression assignments, or a write in a transaction block |

`RETURNING` is not supported: the sink commits after the rows are written and
there is no row stream to return.

## Index maintenance

If the table declares vector or full-text indexes, the index is updated after the
metadata commit. If the index build fails, the statement returns an error but the
inserted rows are already committed and visible.

## Next

- [Transactions and catalog](transactions-and-catalog.md)
- [Incremental SQL](../incremental-sql/getting-started.md)
- [Vector search](../vector-search/overview.md)
