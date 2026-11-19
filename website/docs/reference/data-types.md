---
sidebar_position: 2
---

# Data types

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Column types in DDL

`CREATE TABLE` maps PostgreSQL types to LakeSoul's Arrow schema:

| PostgreSQL type | Stored as | Notes |
| --- | --- | --- |
| `smallint`, `int2` | `Int16` | |
| `integer`, `int`, `int4` | `Int32` | |
| `bigint`, `int8` | `Int64` | Required for primary-key columns of indexed tables |
| `real`, `float4` | `Float32` | |
| `double precision`, `float8` | `Float64` | |
| `float(p)` | `Float32` for `p <= 24`, `Float64` for `p <= 53` | Precision only |
| `boolean`, `bool` | `Boolean` | |
| `text`, `varchar`, `char` | `Utf8` | |
| `json` | `Utf8` + `arrow.json` | Original JSON text is preserved |
| `jsonb` | `Utf8` + `arrow.json` + `pg.json` | Canonical JSON text; duplicates and key order are normalized |
| `date` | `Date32` | |
| `timestamp` | Microsecond timestamp, no time zone | `timestamp(6)` only |
| `timestamptz` | Microsecond timestamp in UTC | |
| `bytea` | Binary | |
| `numeric(p, s)`, `decimal(p, s)` | `Decimal128(p, s)` | `1 <= p <= 38`, `0 <= s <= p` |

`numeric` without a precision and scale is rejected. `timestamp(p)` accepts only
`p = 6`; other precisions are rejected rather than silently rounded.

## Types that are not column types

`uuid`, `interval`, `time`, `inet`, `money`, enums and array column types are
not part of the DDL mapping. A table with such a column is rejected with
`0A000`.

## JSON columns

`json` and `jsonb` are stored as UTF-8 text with the Arrow canonical
`arrow.json` extension. The PostgreSQL front end additionally stores a
`pg.json` marker so it reports the original OID (114 for `json`, 3802 for
`jsonb`) on `SELECT` and through `pg_attribute`.

- `json` keeps the document exactly as written.
- `jsonb` is parsed and canonicalized when it enters through the PostgreSQL
  front end (parameters, `COPY ... FROM STDIN`, subscription writes): duplicate
  object keys keep their last value, keys are sorted, and insignificant
  whitespace is removed.
- `->`, `->>`, `#>`, `#>>` and `?` are available over both types.
- Spark 3.5 and Flink 1.20 see these columns as ordinary strings, so the same
  table remains readable and writable outside PostgreSQL.

`jsonb` equality and ordering in LakeSoul are string equality and ordering over
the canonical text, not PostgreSQL's binary jsonb comparison. Comparison
operators such as `@>` are not implemented yet.

Arrays are still usable in expressions and results (`ARRAY[...]`, `INTEGER[]`,
1-based indexing), and the wire protocol decodes them. They are not a supported
column type.

## Query-side behaviour

- **NULL semantics** follow SQL three-valued logic: `NULL` is distinct from the
  empty string, `COUNT(*)` counts rows while `COUNT(col)` skips NULLs.
- **Decimals** are exact. The engine returns `NUMERIC` with the column's scale;
  `DECIMAL`/`NUMERIC` values are not routed through floating point.
- **Timestamps** are microsecond precision; `timestamptz` is normalised to UTC.
- **Integer arithmetic** follows the column width; division and remainder follow
  SQL semantics for the operand types.

## Known boundaries

A few type differences are intentional and documented rather than hidden:

| Case | LakeSoul | PostgreSQL |
| --- | --- | --- |
| `COALESCE(integer_col, -1)` | `INT8` | `INT4` |
| `rank()` / `row_number()` without a cast | `NUMERIC` | `INT8` |
| `numeric` precision | Up to 38 digits | Wider `NUMERIC` |
| `timestamp` precision | Microseconds | Configurable |

Values match; the OIDs may differ. Cast explicitly in a query if a client depends
on the PostgreSQL type.

## Next

- [DDL](../engine/postgres/ddl.md)
- [Compatibility](../engine/postgres/compatibility.md)
- [SQL errors](sql-errors.md)
