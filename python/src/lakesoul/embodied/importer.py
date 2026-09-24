# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Shared helpers for the embodied dataset importers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from lakesoul.catalog import LakeSoulCatalog, TableNotFoundError


@dataclass(frozen=True)
class ImportSummary:
    table: str
    path: str
    episodes: int
    rows: int
    video_frames: int
    columns: tuple[str, ...]
    tables: tuple[str, ...] = ()


def sanitize(name: str) -> str:
    """Turn a dotted feature key into a LakeSoul-safe column name."""
    sanitized = "".join(
        character if character.isalnum() or character == "_" else "_"
        for character in name
    )
    return sanitized.strip("_") or "column"


def resolve_names(columns: list[str]) -> list[str]:
    """Sanitize column names and reject collisions."""
    resolved = [sanitize(column) for column in columns]
    if len(set(resolved)) != len(resolved):
        raise ValueError(f"columns collide after sanitizing: {columns}")
    return resolved


def prepare_table(
    catalog: LakeSoulCatalog,
    table: str,
    namespace: str,
    overwrite: bool,
) -> None:
    """Drop the target table when it exists and ``overwrite`` is set."""
    try:
        catalog.table(table, namespace=namespace)
    except TableNotFoundError:
        return
    if not overwrite:
        raise ValueError(f"table {table!r} already exists; pass overwrite=True")
    catalog.drop_table(table, namespace=namespace, if_exists=True)


def sibling_path(path: str | Path, suffix: str) -> str:
    """Return the storage path of a side table next to ``path``."""
    return f"{str(path).rstrip('/')}{suffix}"


def filter_properties(
    properties: Mapping[str, str] | None, schema: pa.Schema
) -> dict[str, str] | None:
    """Keep ``blob_columns`` entries whose column exists in ``schema``."""
    if not properties:
        return dict(properties) if properties is not None else None
    filtered = dict(properties)
    raw = filtered.get("blob_columns")
    if raw:
        parsed = json.loads(raw) if isinstance(raw, str) else dict(raw)
        parsed = {
            column: policy
            for column, policy in parsed.items()
            if column in schema.names
        }
        if parsed:
            filtered["blob_columns"] = json.dumps(parsed)
        else:
            filtered.pop("blob_columns")
    return filtered


__all__ = [
    "ImportSummary",
    "filter_properties",
    "prepare_table",
    "resolve_names",
    "sanitize",
    "sibling_path",
]
