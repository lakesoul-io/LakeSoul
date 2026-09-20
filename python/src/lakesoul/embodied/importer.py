# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Shared helpers for the embodied dataset importers."""

from __future__ import annotations

from dataclasses import dataclass

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


__all__ = [
    "ImportSummary",
    "prepare_table",
    "resolve_names",
    "sanitize",
]
