# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
from typing import Any

import pyarrow as pa


def normalize_table(table: pa.Table, expected_schema: pa.Schema) -> pa.Table:
    arrays = []
    for field in expected_schema:
        if field.name not in table.schema.names:
            raise AssertionError(f"missing column {field.name!r} in table {table.schema}")
        column = table[field.name]
        if not column.type.equals(field.type):
            column = column.cast(field.type)
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=expected_schema).combine_chunks()


def table_summary(
    table: pa.Table,
    expected_schema: pa.Schema,
    partition_by: tuple[str, ...] = (),
) -> dict[str, Any]:
    normalized = normalize_table(table, expected_schema)
    rows = [_canonical_row(row, normalized.schema.names) for row in normalized.to_pylist()]
    row_lines = sorted(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows)
    return {
        "schema": schema_fingerprint(normalized.schema),
        "row_count": normalized.num_rows,
        "row_hash": hashlib.sha256("\n".join(row_lines).encode("utf-8")).hexdigest(),
        "partition_summary": partition_summary(normalized, partition_by),
    }


def schema_fingerprint(schema: pa.Schema) -> dict[str, Any]:
    fields = [
        {
            "name": field.name,
            "type": _logical_type(field.type),
            "nullable": field.nullable,
        }
        for field in schema
    ]
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    return {
        "fields": fields,
        "hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
    }


def partition_summary(table: pa.Table, partition_by: tuple[str, ...]) -> dict[str, int]:
    if not partition_by:
        return {"-5": table.num_rows}
    counts: dict[str, int] = {}
    for row in table.to_pylist():
        key = ",".join(f"{col}={_canonical_value(row[col])}" for col in partition_by)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def assert_table_matches(
    actual: pa.Table,
    expected: pa.Table,
    partition_by: tuple[str, ...] = (),
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected_summary = table_summary(expected, expected.schema, partition_by)
    actual_summary = table_summary(actual, expected.schema, partition_by)
    if actual_summary != expected_summary:
        raise AssertionError(
            "table mismatch:\n"
            f"expected={json.dumps(expected_summary, indent=2, sort_keys=True)}\n"
            f"actual={json.dumps(actual_summary, indent=2, sort_keys=True)}"
        )
    return actual_summary, expected_summary


def _canonical_row(row: dict[str, Any], names: list[str]) -> dict[str, Any]:
    return {name: _canonical_value(row.get(name)) for name in names}


def _canonical_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, float):
        return repr(value)
    return value


def _logical_type(data_type: pa.DataType) -> str:
    if pa.types.is_large_string(data_type) or pa.types.is_string(data_type):
        return "string"
    if pa.types.is_int8(data_type):
        return "int8"
    if pa.types.is_int16(data_type):
        return "int16"
    if pa.types.is_int32(data_type):
        return "int32"
    if pa.types.is_int64(data_type):
        return "int64"
    if pa.types.is_float32(data_type):
        return "float32"
    if pa.types.is_float64(data_type):
        return "float64"
    if pa.types.is_boolean(data_type):
        return "bool"
    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return "date"
    if pa.types.is_timestamp(data_type):
        return "timestamp"
    if pa.types.is_decimal(data_type):
        return f"decimal({data_type.precision},{data_type.scale})"
    return str(data_type)
