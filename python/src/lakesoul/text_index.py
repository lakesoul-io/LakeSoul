# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""LakeSoul text index orchestration.

This module is the text kind's thin layer on top of
:mod:`lakesoul.index`: it maps the text configuration onto the native
Tantivy split builder and exposes the user-facing build entry points.

Usage::

    from lakesoul.text_index import build_partition_text_index

    build_partition_text_index(
        table_name="my_table",
        namespace="default",
        partition_desc="range=2024-01-01",
        text_column="body",
        tokenizer="jieba",
        store_config={"type": "s3", "bucket": "...", ...},
    )
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from . import index as _index
from .index import (
    IndexKindSpec,
    ShardInfo,
    build_partition_index,
    build_table_index,
    extract_bucket_id as _extract_bucket_id,
    group_files_by_shard as _group_files_by_shard,
    rename_summary_key as _rename_summary_key,
)

# Re-exported for compatibility with earlier imports.
__all__ = [
    "ShardInfo",
    "build_partition_text_index",
    "build_table_text_index",
]


def _build_shard(
    store_config: Any,
    file_paths: list[str],
    pk_column: str,
    config: Any,
    rebuild: bool,
) -> str:
    """Build one text index shard through the native PyO3 binding."""
    from ._lib.text import build_shard_text_index, rebuild_shard_text_index

    builder = rebuild_shard_text_index if rebuild else build_shard_text_index
    return builder(
        store_config=store_config,
        file_paths=list(file_paths),
        pk_column=pk_column,
        text_column=config["column"],
        tokenizer=config.get("tokenizer", "jieba"),
        with_positions=config.get("with_positions", True),
        stored=config.get("stored", False),
    )


def _validate(
    configs: Any,
    schema: pa.Schema,
    primary_keys: Any,
) -> None:
    """Validate a table can support the configured text indexes.

    Mirrors the native builder's requirements: an Int64/UInt64 primary key
    column (index hits are mapped back through it) and Utf8/LargeUtf8/
    Utf8View text columns whose tokenizer is supported.
    """
    from ._lib.text import text_supported_tokenizers

    if not configs:
        return
    if not primary_keys:
        raise ValueError(
            "a text index requires an id column: pass primary_keys=[...] "
            "when creating a table with text_index (the index maps search "
            "results to primary key values)"
        )
    pk_column = primary_keys[0]
    pk_index = schema.get_field_index(pk_column)
    if pk_index < 0:
        raise ValueError(
            f"text index primary key '{pk_column}' not found in table schema "
            f"(columns: {list(schema.names)})"
        )
    pk_type = schema.field(pk_index).type
    if not (pa.types.is_uint64(pk_type) or pa.types.is_int64(pk_type)):
        raise ValueError(
            f"text index primary key '{pk_column}' must be UInt64 or Int64, "
            f"got {pk_type}"
        )
    supported = text_supported_tokenizers()
    for cfg in configs:
        column = cfg["column"]
        tokenizer = cfg.get("tokenizer", "jieba")
        if tokenizer not in supported:
            raise ValueError(
                f"text index column '{column}' uses unsupported tokenizer "
                f"'{tokenizer}'; supported: {', '.join(supported)}"
            )
        index = schema.get_field_index(column)
        if index < 0:
            raise ValueError(
                f"text index column '{column}' not found in table schema "
                f"(columns: {list(schema.names)})"
            )
        dtype = schema.field(index).type
        if not (
            pa.types.is_string(dtype)
            or pa.types.is_large_string(dtype)
            or pa.types.is_string_view(dtype)
        ):
            raise ValueError(
                f"text index column '{column}' must be Utf8, LargeUtf8 or "
                f"Utf8View, got {dtype}"
            )


def _parse(raw: str) -> list[dict[str, Any]]:
    from ._lib.text import parse_text_index_configs

    return list(parse_text_index_configs(raw))


TEXT_KIND_SPEC = IndexKindSpec(
    name="text",
    property_key="text_index_columns",
    parse_configs=_parse,
    validate=_validate,
    build_shard=_build_shard,
)
_index.register_index_kind(TEXT_KIND_SPEC)


def _text_config(
    text_column: str,
    tokenizer: str,
    with_positions: bool,
    stored: bool,
) -> dict[str, Any]:
    return {
        "column": text_column,
        "tokenizer": tokenizer,
        "with_positions": with_positions,
        "stored": stored,
    }


def _as_text_result(result: dict[str, Any]) -> dict[str, Any]:
    """Rename the generic ``column`` summary key for text callers."""
    return _rename_summary_key(result, "text_column")


def build_partition_text_index(
    table_name: str,
    namespace: str,
    partition_desc: str,
    text_column: str,
    tokenizer: str = "jieba",
    with_positions: bool = True,
    stored: bool = False,
    store_config: dict[str, Any] | None = None,
    rebuild: bool = False,
) -> dict[str, Any]:
    """Build (or update, or rebuild) the text index for a single partition
    of a table.

    It:

    1. Looks up table metadata (schema, PK columns, table path) from PG.
    2. Queries the latest partition version's data files.
    3. Groups files by hash bucket.
    4. Calls the native Tantivy builder for each (partition, bucket) shard.

    Args:
        table_name: LakeSoul table name.
        namespace: LakeSoul namespace (default ``"default"``).
        partition_desc: Partition descriptor string, e.g. ``"range=2024-01-01"``.
        text_column: Name of the text column in the Arrow schema.
        tokenizer: Tokenizer registered on the index (``"jieba"``,
            ``"default"``, ``"en_stem"``, ``"whitespace"``, ``"raw"``).
        with_positions: Index term positions so phrase queries work.
        stored: Also store the original text in the index.
        store_config: Dict with S3/local storage credentials; if not
            provided, reads ``LAKESOUL_OBJECT_STORE_*`` env vars.
        rebuild: When true, every shard is rebuilt from scratch (all active
            data files re-read into a fresh split, published as a new index
            generation) instead of receiving an incremental delta split.

    Returns:
        Dict with summary::

            {
              "status": "ok",
              "shards_total": 4,
              "shards_succeeded": 4,
              "table_path": "s3://bucket/table",
              "text_column": "body",
              "partition_desc": "range=2024-01-01",
            }

    Raises:
        RuntimeError: If any shard's index build fails.
    """
    result = build_partition_index(
        "text",
        table_name=table_name,
        namespace=namespace,
        partition_desc=partition_desc,
        config=_text_config(text_column, tokenizer, with_positions, stored),
        store_config=store_config,
        rebuild=rebuild,
    )
    return _as_text_result(result)


def build_table_text_index(
    table_name: str,
    namespace: str,
    text_column: str,
    tokenizer: str = "jieba",
    with_positions: bool = True,
    stored: bool = False,
    store_config: dict[str, Any] | None = None,
    rebuild: bool = False,
) -> dict[str, Any]:
    """Build (or rebuild) the text index for ALL partitions of a table.

    Convenience wrapper around :func:`build_partition_text_index` that
    iterates over all existing partitions of the table.

    Returns:
        Dict with per-partition results.
    """
    result = build_table_index(
        "text",
        table_name=table_name,
        namespace=namespace,
        configs=[_text_config(text_column, tokenizer, with_positions, stored)],
        store_config=store_config,
        rebuild=rebuild,
    )
    return _as_text_result(result)
