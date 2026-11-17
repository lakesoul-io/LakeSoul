---
sidebar_position: 3.5
---

# COPY FROM STDIN

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`COPY ... FROM STDIN` loads rows in bulk through the PostgreSQL copy
sub-protocol. It is the path importers and dump/restore tools use instead of
many single-row `INSERT`s: Airbyte and Singer targets, `pg_dump`/`pg_restore`,
`psql \copy`, pgjdbc `CopyManager`, psycopg2 `copy_expert`/`copy_from` and
psycopg3 `cursor.copy()`.

```sql
COPY orders (id, day, region, amount) FROM STDIN;
```

The client streams the rows, ends the copy, and the server answers
`COPY <rows>`.

## Formats

TEXT (the default) and CSV are implemented:

```sql
COPY orders FROM STDIN;
COPY orders FROM STDIN WITH (FORMAT csv, HEADER true);
COPY orders FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL 'NULL');
```

| Option | TEXT | CSV |
| --- | --- | --- |
| `FORMAT text` / `FORMAT csv` | yes | yes |
| `DELIMITER` | one byte, default tab | one byte, default comma |
| `NULL` | default `\N` | default unquoted empty field |
| `HEADER` | refused (`42601`) | skips the first record |
| `QUOTE`, `ESCAPE` | refused (`42601`) | one byte, default `"` |
| `ENCODING` | `UTF8` only | `UTF8` only |

TEXT values follow PostgreSQL's COPY escaping: `\b`, `\f`, `\n`, `\r`, `\t`,
`\v`, `\\`, octal and `\x` sequences, and a backslash before the delimiter
protects it. A line of `\.` ends the data stream.

CSV values may be quoted, and a quoted field can contain the delimiter,
newlines and doubled quotes; `\r\n` record terminators are accepted. Only an
unquoted empty field is NULL, so `""` is the empty string. With a custom
`NULL` string, a quoted field with that text is still data.

Column values are converted by the target column's PostgreSQL input rules:
`boolean`, `smallint`/`integer`/`bigint`, `real`/`double precision`,
`numeric(p,s)` (rounded to the column scale), `text`, `bytea` (hex `\x...` or
the legacy escape form), `date`, `timestamp`, `timestamptz`, plus the other
types `CREATE TABLE` accepts. An unparsable value reports `22P02` (`22007` for
date/time) instead of writing a different value.

## Semantics

- One `COPY` is one LakeSoul commit. A duplicate key (`23505`), a NOT NULL
  violation (`23502`) or a malformed row (`22P02`/`22P04`) publishes **none**
  of the rows, not even the rows before the failing one.
- Inside `BEGIN`/`COMMIT`, a copy is staged like an `INSERT` and published by
  `COMMIT`; `ROLLBACK` removes it. Other connections see nothing before the
  commit.
- Without a column list, the stream must carry every table column in table
  order. An explicit list may omit columns: they are written as NULL (or the
  column default, when the table declares one).
- An empty copy writes nothing and returns `COPY 0`.
- A statement cannot share a simple-query batch with a COPY. The batch is
  refused with `0A000` before it runs, because the copy sub-protocol owns the
  connection until `CopyDone`.
- The client can abort with CopyFail; the buffered rows are discarded and the
  statement reports `57014`. The connection stays usable.
- Rows are buffered in memory until `CopyDone`, because one statement must
  become one commit. Split very large loads into several `COPY` statements.

## Not supported

These are refused explicitly instead of being approximated:

- `COPY ... TO STDOUT`
- `COPY ... FROM` a file or program
- `COPY (query) ...`
- `FORMAT binary`
- `FREEZE`, `FORCE_QUOTE`, `FORCE_NOT_NULL`, `FORCE_NULL`
- data embedded in the statement text (`COPY t FROM STDIN; ...`)

## Clients

| Client | Entry point |
| --- | --- |
| psycopg2 | `cursor.copy_expert(...)`, `cursor.copy_from(...)` |
| psycopg3 | `cursor.copy(...)` with `copy.write` / `copy.write_row` |
| pgjdbc | `connection.unwrap(PGConnection.class).getCopyAPI().copyIn(...)` |
| psql | `\copy orders FROM 'orders.csv' WITH (FORMAT csv, HEADER true)` |

```python
with cursor.copy(
    "COPY orders (id, amount) FROM STDIN WITH (FORMAT csv)"
) as copy:
    copy.write("1,42.00\n")
    copy.write("2,7.50\n")
```
