# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import json
from collections.abc import Mapping
from pathlib import Path

import pyarrow

from lakesoul._lib._metadata import _create_table, drop_table


def _partitions_to_metadata_str(
    schema: pyarrow.Schema, partitions: Mapping[str, list[str]] | None
) -> str:
    partitions = partitions or {}
    schema_names = set(schema.names)
    range_partitions: list[str] = partitions.get("range", [])
    hash_partitions: list[str] = partitions.get("hash", [])

    for col in range_partitions + hash_partitions:
        if col not in schema_names:
            raise ValueError(f"partition column not in schema: {col}")

    return f"{','.join(range_partitions)};{','.join(hash_partitions)}"


def create_table(
    table_name: str,
    *,
    namespace: str = "default",
    table_path: str | Path,
    table_schema: pyarrow.Schema,
    properties: Mapping[str, str] | None = None,
    partitions: Mapping[str, list[str]] | None = None,
    domain: str = "public",
) -> None:
    _create_table(
        table_name,
        namespace,
        str(table_path),
        table_schema,
        json.dumps(dict(properties or {}), separators=(",", ":")),
        _partitions_to_metadata_str(table_schema, partitions),
        domain,
    )


__all__ = ["create_table", "drop_table"]
