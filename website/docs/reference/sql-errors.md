---
sidebar_position: 4
---

# SQL errors

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

The PostgreSQL front end returns standard SQLSTATE codes where the behaviour
maps cleanly, and documents the boundaries where it does not.

## Codes

| SQLSTATE | Name | Typical cause |
| --- | --- | --- |
| `0A000` | feature_not_supported | `UPDATE`, `DELETE`, `COPY`, `RETURNING`, unsupported DDL clause, write in a transaction block |
| `21000` | cardinality_violation | A scalar subquery returned more than one row |
| `22007` | invalid_datetime_format | A timestamp or date literal could not be parsed |
| `22023` | invalid_parameter_value | Invalid `SET` value or table option |
| `22P02` | invalid_text_representation | A string literal does not convert to the target type |
| `23505` | unique_violation | Duplicate primary key on a plain insert |
| `25P02` | in_failed_sql_transaction | A statement after a failed transaction, until `ROLLBACK` |
| `40001` | serialization_failure | Lost a write race; retry the statement |
| `42601` | syntax_error | SQL syntax error, or source/target column mismatch |
| `42701` | duplicate_column | A column is declared twice, including in `PARTITIONED BY` |
| `42703` | undefined_column | Unknown column |
| `42804` | datatype_mismatch | An expression is not assignment-castable |
| `42P01` | undefined_table | Unknown table |
| `42P07` | duplicate_table | Table already exists |
| `42P16` | invalid_table_definition | Invalid primary key or table definition |
| `3F000` | invalid_schema_name | Invalid schema in a qualified name |
| `57014` | query_canceled | `statement_timeout` or a cancel request |
| `XX000` | internal_error | Engine error, or an unknown database at startup |

## Mapping boundaries

These are documented rather than silently different:

| Case | LakeSoul | PostgreSQL |
| --- | --- | --- |
| Invalid integer conversion | `XX000` | `22P02` |
| Unknown database at startup | `FATAL XX000` | Different message and code |
| `rank()` / `row_number()` type | `NUMERIC` | `INT8` |
| `COALESCE(integer_col, -1)` type | `INT8` | `INT4` |

Match on the SQLSTATE where the table above says it matches; parse the message
only as a fallback.

## Retrying

- `40001` is retryable: the statement lost a race with a concurrent writer.
  Retry it once the other transaction has committed.
- `23505` is not retryable as-is: the key already exists. Use `ON CONFLICT` if an
  upsert is intended.
- `57014` is a cancellation, not a data error; the connection remains usable.

## Next

- [Compatibility](../engine/postgres/compatibility.md)
- [Insert and upsert](../engine/postgres/insert-and-upsert.md)
- [Data types](data-types.md)
