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
| `text`, `varchar`, `char` | `Utf8` | `varchar(n)`/`char(n)` keep their length in `pg.typmod` and enforce it on writes |
| `uuid` | `Utf8` + `pg.type=uuid` | Canonical lowercase hyphenated text; UUID OID on the wire |
| one-dimensional array, e.g. `INT[]` | `List<element>` | Element type must be a supported scalar type; multidimensional arrays and array primary keys/partitions are rejected |
| `json` | `Utf8` + `arrow.json` | Original JSON text is preserved |
| `jsonb` | `Utf8` + `arrow.json` + `pg.json` | Canonical JSON text; duplicates and key order are normalized |
| `date` | `Date32` | |
| `time(p)` | Arrow `Time32`/`Time64` | `p` 0, 3 or 6; other precisions are rejected |
| `timestamp(p)` | Arrow `Timestamp` | `p` 0, 3 or 6; default is 6. Fractional digits beyond the unit are truncated |
| `timestamptz(p)` | Arrow `Timestamp` in UTC | Same precision rules |
| `bytea` | Binary | |
| `numeric(p, s)`, `decimal(p, s)` | `Decimal128` for `p <= 38`, `Decimal256` for `39 <= p <= 76` | `1 <= p <= 76`, `0 <= s <= p` |
| `money` | `Decimal128(19,2)` + `pg.type=money` | `MONEY` OID on the wire |

`numeric` without a precision and scale is rejected; precision above 76 is
rejected rather than silently truncated. `interval` and `inet` columns are not
supported: Arrow has an interval type but the Parquet/Vortex writers cannot
persist it, and Arrow has no inet type. Interval expressions still work.

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

Arrays are column types as well as expressions (`ARRAY[...]`, `INTEGER[]`,
1-based indexing), and the wire protocol encodes and decodes them. A supported
array column is one-dimensional and uses a supported scalar element type.

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
| `numeric` precision | Up to 76 digits (`Decimal256`) | Wider `NUMERIC` |
| `timestamp` precision | Seconds, milliseconds or microseconds (0/3/6) | Configurable 0-6 |
| `char(n)` padding | Length is enforced, trailing-space padding is not emulated | Blank-padded and comparison-insensitive |

Values match; the OIDs may differ. Cast explicitly in a query if a client depends
on the PostgreSQL type.

## Next

- [DDL](../engine/postgres/ddl.md)
- [Compatibility](../engine/postgres/compatibility.md)
- [SQL errors](sql-errors.md)
