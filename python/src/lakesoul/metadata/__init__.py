# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import json
from collections.abc import Mapping
from pathlib import Path

import pyarrow

from lakesoul._lib._metadata import _NativeMetadataClient, _create_table
from lakesoul._lib._metadata import drop_table as _drop_table
from lakesoul.metadata.meta_ops import list_namespaces, list_tables


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
    _client: _NativeMetadataClient | None = None,
) -> None:
    args = (
        table_name,
        namespace,
        str(table_path),
        table_schema,
        json.dumps(dict(properties or {}), separators=(",", ":")),
        _partitions_to_metadata_str(table_schema, partitions),
        domain,
    )
    if _client is None:
        _create_table(*args)
    else:
        _client.create_table(*args)


def drop_table(
    table_name: str,
    namespace: str = "default",
    *,
    _client: _NativeMetadataClient | None = None,
) -> None:
    if _client is None:
        _drop_table(table_name, namespace)
    else:
        _client.drop_table(table_name, namespace)


__all__ = ["create_table", "drop_table", "list_namespaces", "list_tables"]
