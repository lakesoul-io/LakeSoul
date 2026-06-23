# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class CaseSpec:
    name: str
    schema: pa.Schema
    batches: tuple[pa.Table, ...]
    partition_by: tuple[str, ...] = ()
    primary_keys: tuple[str, ...] = ()
    hash_bucket_num: int = 1
    read_columns: tuple[str, ...] | None = None
    read_partition_filter: dict[str, Any] | None = None

    @property
    def expected_table(self) -> pa.Table:
        table = self._final_table()
        if self.read_partition_filter:
            rows = [
                row
                for row in table.to_pylist()
                if all(row.get(key) == value for key, value in self.read_partition_filter.items())
            ]
            table = _table_from_rows(rows, table.schema)
        if self.read_columns is not None:
            table = table.select(list(self.read_columns))
        return table

    @property
    def read_schema(self) -> pa.Schema:
        return self.expected_table.schema

    def _final_table(self) -> pa.Table:
        if self.primary_keys:
            rows_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
            for batch in self.batches:
                for row in batch.to_pylist():
                    key = tuple(row[col] for col in self.primary_keys)
                    rows_by_key[key] = row
            return _table_from_rows(list(rows_by_key.values()), self.schema)
        return pa.concat_tables(self.batches, promote_options="default")


def _table_from_rows(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    return pa.Table.from_pylist(rows, schema=schema)


def _table(schema: pa.Schema, rows: list[dict[str, Any]]) -> pa.Table:
    return _table_from_rows(rows, schema)


BASIC_APPEND_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("label", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("active", pa.bool_()),
        pa.field("note", pa.string()),
    ]
)

PARTITIONED_APPEND_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("part", pa.string(), nullable=False),
        pa.field("value", pa.int32()),
        pa.field("tag", pa.string()),
    ]
)

PK_UPSERT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("value", pa.int32()),
    ]
)

SCHEMA_TYPES_SCHEMA = pa.schema(
    [
        pa.field("type_id", pa.int32(), nullable=False),
        pa.field("long_value", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("flag", pa.bool_()),
        pa.field("ratio", pa.float64()),
        pa.field("event_date", pa.date32()),
        pa.field("event_ts", pa.timestamp("us")),
    ]
)

CASES: dict[str, CaseSpec] = {
    "basic_append": CaseSpec(
        name="basic_append",
        schema=BASIC_APPEND_SCHEMA,
        batches=(
            _table(
                BASIC_APPEND_SCHEMA,
                [
                    {
                        "id": 1,
                        "label": "alpha",
                        "score": 1.5,
                        "active": True,
                        "note": "first",
                    },
                    {
                        "id": 2,
                        "label": "beta",
                        "score": None,
                        "active": False,
                        "note": None,
                    },
                ],
            ),
            _table(
                BASIC_APPEND_SCHEMA,
                [
                    {
                        "id": 3,
                        "label": "gamma",
                        "score": -7.25,
                        "active": None,
                        "note": "last",
                    }
                ],
            ),
        ),
    ),
    "partitioned_append": CaseSpec(
        name="partitioned_append",
        schema=PARTITIONED_APPEND_SCHEMA,
        batches=(
            _table(
                PARTITIONED_APPEND_SCHEMA,
                [
                    {"id": 1, "part": "north", "value": 10, "tag": "a"},
                    {"id": 2, "part": "south", "value": 20, "tag": "b"},
                ],
            ),
            _table(
                PARTITIONED_APPEND_SCHEMA,
                [
                    {"id": 3, "part": "north", "value": 30, "tag": None},
                    {"id": 4, "part": "east", "value": 40, "tag": "d"},
                ],
            ),
        ),
        partition_by=("part",),
        read_partition_filter={"part": "north"},
        read_columns=("id", "part", "value"),
    ),
    "pk_upsert": CaseSpec(
        name="pk_upsert",
        schema=PK_UPSERT_SCHEMA,
        batches=(
            _table(
                PK_UPSERT_SCHEMA,
                [
                    {"id": 1, "name": "alice", "value": 10},
                    {"id": 2, "name": "bob", "value": 20},
                    {"id": 3, "name": "carol", "value": 30},
                ],
            ),
            _table(
                PK_UPSERT_SCHEMA,
                [
                    {"id": 2, "name": "bob-updated", "value": 200},
                    {"id": 4, "name": "dave", "value": 40},
                ],
            ),
        ),
        primary_keys=("id",),
        hash_bucket_num=2,
    ),
    "schema_types": CaseSpec(
        name="schema_types",
        schema=SCHEMA_TYPES_SCHEMA,
        batches=(
            _table(
                SCHEMA_TYPES_SCHEMA,
                [
                    {
                        "type_id": 1,
                        "long_value": 10_000_000_001,
                        "name": "typed-a",
                        "flag": True,
                        "ratio": 3.5,
                        "event_date": dt.date(2024, 1, 2),
                        "event_ts": dt.datetime(2024, 1, 2, 3, 4, 5, 123456),
                    },
                    {
                        "type_id": 2,
                        "long_value": None,
                        "name": "typed-b",
                        "flag": False,
                        "ratio": None,
                        "event_date": dt.date(2024, 2, 3),
                        "event_ts": dt.datetime(2024, 2, 3, 4, 5, 6),
                    },
                ],
            ),
        ),
    ),
}

SMOKE_CASES = ("basic_append", "partitioned_append", "pk_upsert", "schema_types")
FULL_CASES = tuple(CASES)
