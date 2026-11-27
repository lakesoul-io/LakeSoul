---
sidebar_position: 3.5
---

# COPY

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

The PostgreSQL copy sub-protocol moves rows in bulk in both directions.
`COPY ... FROM STDIN` is the path importers use instead of many single-row
`INSERT`s; `COPY ... TO STDOUT` is the export path used by report exports and
data movers. Clients include Airbyte and Singer, `psql \copy`, pgjdbc
`CopyManager`, psycopg2 `copy_expert`/`copy_from` and psycopg3
`cursor.copy()`.

```sql
COPY orders (id, day, region, amount) FROM STDIN;
```

The client streams the rows, ends the copy, and the server answers
`COPY <rows>`.

## Formats

TEXT (the default), CSV and binary are implemented for the import direction:

```sql
COPY orders FROM STDIN;
COPY orders FROM STDIN WITH (FORMAT csv, HEADER true);
COPY orders FROM STDIN WITH (FORMAT binary);
COPY orders FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL 'NULL');
```

| Option | TEXT | CSV | Binary (import) |
| --- | --- | --- | --- |
| `FORMAT text` / `FORMAT csv` / `FORMAT binary` | yes | yes | yes |
| `DELIMITER` | one byte, default tab | one byte, default comma | refused (`42601`) |
| `NULL` | default `\N` | default unquoted empty field | `-1` field length |
| `HEADER` | refused (`42601`) | import: skips the first record; export: writes column names | refused (`42601`) |
| `QUOTE`, `ESCAPE` | refused (`42601`) | one byte, default `"` | refused (`42601`) |
| `ENCODING` | `UTF8` only | `UTF8` only | `UTF8` only |

TEXT values follow PostgreSQL's COPY escaping: `\b`, `\f`, `\n`, `\r`, `\t`,
`\v`, `\\`, octal and `\x` sequences, and a backslash before the delimiter
protects it. A line of `\.` ends the data stream.

CSV values may be quoted, and a quoted field can contain the delimiter,
newlines and doubled quotes; `\r\n` record terminators are accepted. Only an
unquoted empty field is NULL, so `""` is the empty string. With a custom
`NULL` string, a quoted field with that text is still data.

Binary import follows PostgreSQL's `PGCOPY` stream format: a 19-byte header
(`PGCOPY\n\xff\r\n\0`, zero flags, zero extension length), then one `int16`
field count and one `int32` length plus payload per field, ending with an
`int16` `-1` field count. A field length of `-1` is NULL. Any piece may be
split across `CopyData` frames. The supported binary value types are the core
scalars: `boolean`, `smallint`, `integer`, `bigint`, `real`, `double
precision`, `numeric`, `text`/`varchar`/`char`, `bytea`, `uuid`, `money`,
`date`, `time`, `timestamp`, `timestamptz`, `json` and `jsonb`. Binary arrays
and pgvector values are refused with `0A000`. `COPY ... TO STDOUT WITH
(FORMAT binary)` is also refused; export supports TEXT and CSV only.

Column values are converted by the target column's PostgreSQL input rules:
`boolean`, `smallint`/`integer`/`bigint`, `real`/`double precision`,
`numeric(p,s)` (rounded to the column scale), `text`, `varchar(n)`, `char(n)`,
`uuid`, `money`, arrays, `bytea` (hex `\x...` or the legacy escape form),
`date`, `time(p)`, `timestamp(p)`, `timestamptz(p)`, plus the other types
`CREATE TABLE` accepts. An unparsable value reports `22P02` (`22007` for
date/time) instead of writing a different value.

## Semantics

- One `COPY ... FROM STDIN` is one LakeSoul commit. A duplicate key (`23505`),
  a NOT NULL violation (`23502`) or a malformed row (`22P02`/`22P04`)
  publishes **none** of the rows, not even the rows before the failing one.
- Inside `BEGIN`/`COMMIT`, a copy is staged like an `INSERT` and published by
  `COMMIT`; `ROLLBACK` removes it. Other connections see nothing before the
  commit.
- Without a column list, the stream must carry every table column in table
  order. An explicit list may omit columns: they are written as NULL (or the
  column default, when the table declares one).
- An empty copy writes nothing and returns `COPY 0`.
- A COPY (either direction) cannot share a simple-query batch; the batch is
  refused with `0A000` before it runs.
- The client can abort with CopyFail; the buffered rows are discarded and the
  statement reports `57014`. The connection stays usable.
- Rows are buffered in memory until `CopyDone`, because one statement must
  become one commit. Split very large loads into several `COPY` statements.

## Export: COPY TO STDOUT

```sql
COPY orders TO STDOUT;
COPY orders (id, amount) TO STDOUT WITH (FORMAT csv, HEADER true);
COPY (SELECT id, amount FROM orders WHERE amount > 0 ORDER BY id)
  TO STDOUT WITH (FORMAT csv);
```

The server streams one `CopyData` row at a time and ends with `COPY <rows>`.
`HEADER` is written together with the first data row; an export with zero rows
carries no header. TEXT and CSV use the same options as the import direction. The rows come from
the query plan, so a table copy sees the block's staged writes inside a
transaction, and a query copy supports `ORDER BY`, filters and joins. A table
copy without `ORDER BY` has no guaranteed row order.

## Not supported

These are refused explicitly instead of being approximated:

- `COPY ... TO` a file or program, and `COPY ... FROM` a file or program
- `FORMAT binary` on export, and binary arrays or vector values on import
- text/CSV-only options (`DELIMITER`, `NULL`, `HEADER`, `QUOTE`, `ESCAPE`)
  combined with `FORMAT binary`
- `FREEZE`, `FORCE_QUOTE`, `FORCE_NOT_NULL`, `FORCE_NULL`
- data embedded in the statement text (`COPY t FROM STDIN; ...`)

## Clients

| Client | Entry point |
| --- | --- |
| psycopg2 | `cursor.copy_expert(...)`, `cursor.copy_from(...)`; `copy_expert` for export |
| psycopg3 | `cursor.copy(...)` with `copy.write` / `copy.write_row` / `copy.read` |
| pgjdbc | `connection.unwrap(PGConnection.class).getCopyAPI().copyIn(...)` / `copyOut(...)` |
| psql | `\copy orders FROM 'orders.csv' ...` / `\copy orders TO 'orders.csv' ...` |

```python
with cursor.copy(
    "COPY orders (id, amount) FROM STDIN WITH (FORMAT csv)"
) as copy:
    copy.write("1,42.00\n")
    copy.write("2,7.50\n")

with cursor.copy(
    "COPY orders (id, amount) TO STDOUT WITH (FORMAT csv)"
) as copy:
    for chunk in copy:
        print(chunk)
```
