# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog, TableNotFoundError
from lakesoul.catalog import MANIFEST_TABLE_SUFFIX

SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False)])


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _make_table(catalog: LakeSoulCatalog, tmp_path: Path, name: str):
    return catalog.create_table(name, path=(tmp_path / name).as_uri(), schema=SCHEMA)


def _samples(anchors: list[int]) -> pa.Table:
    return pa.table(
        {
            "episode_id": pa.array(["ep000000"] * len(anchors), type=pa.string()),
            "anchor": pa.array(anchors, type=pa.int64()),
        }
    )


def test_create_list_read_drop_manifest(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("manifest")
    table = _make_table(catalog, tmp_path, name)

    try:
        info = catalog.create_manifest(table, "eval", _samples([0, 1]))
        assert info.manifest == "eval"
        assert info.rows == 2
        assert info.snapshot_id > 0

        sibling = catalog.table(f"{name}{MANIFEST_TABLE_SUFFIX}")
        assert sibling.partition_by == ("manifest",)

        summaries = catalog.list_manifests(table)
        assert [(item.manifest, item.rows) for item in summaries] == [("eval", 2)]
        assert summaries[0].snapshot_id == info.snapshot_id

        rows = catalog.read_manifest(table, "eval")
        assert rows.num_rows == 2
        params = json.loads(rows.column("params")[0].as_py())
        assert params["order_by"] == "frame_index"
        assert params["episode_column"] == "episode_id"

        ranked = pa.table(
            {
                "episode_id": pa.array(["ep000000", "ep000001"], type=pa.string()),
                "anchor": pa.array([5, 7], type=pa.int64()),
                "rank": pa.array([1, 0], type=pa.int64()),
            }
        )
        catalog.create_manifest(table, "train", ranked, params={"seed": 7})
        summaries = catalog.list_manifests(table)
        assert [item.manifest for item in summaries] == ["eval", "train"]
        train = catalog.read_manifest(table, "train")
        assert train.column("rank").to_pylist() == [1, 0]
        assert json.loads(train.column("params")[0].as_py())["seed"] == 7

        assert catalog.drop_manifest(table, "eval") is True
        assert [item.manifest for item in catalog.list_manifests(table)] == ["train"]
        assert catalog.drop_manifest(table, "eval") is False
        with pytest.raises(ValueError, match="unknown manifest"):
            catalog.read_manifest(table, "eval")
    finally:
        catalog.drop_table(name, if_exists=True)


def test_create_manifest_validation(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("manifest_invalid")
    table = _make_table(catalog, tmp_path, name)

    try:
        with pytest.raises(ValueError, match="manifest names"):
            catalog.create_manifest(table, "bad/name", _samples([0]))

        with pytest.raises(ValueError, match="must contain"):
            catalog.create_manifest(table, "missing", pa.table({"anchor": [1]}))

        with pytest.raises(ValueError, match="anchor must be an integer"):
            catalog.create_manifest(
                table,
                "float-anchor",
                pa.table(
                    {
                        "episode_id": pa.array(["ep"], type=pa.string()),
                        "anchor": pa.array([0.5], type=pa.float64()),
                    }
                ),
            )

        with pytest.raises(ValueError, match="duplicate"):
            catalog.create_manifest(
                table,
                "dup",
                pa.table(
                    {
                        "episode_id": pa.array(["ep", "ep"], type=pa.string()),
                        "anchor": pa.array([1, 1], type=pa.int64()),
                    }
                ),
            )

        with pytest.raises(ValueError, match="rank values must be unique"):
            catalog.create_manifest(
                table,
                "rank",
                pa.table(
                    {
                        "episode_id": pa.array(["ep", "ep"], type=pa.string()),
                        "anchor": pa.array([1, 2], type=pa.int64()),
                        "rank": pa.array([0, 0], type=pa.int64()),
                    }
                ),
            )

        with pytest.raises(ValueError, match="must not be empty"):
            catalog.create_manifest(
                table,
                "empty",
                pa.table(
                    {
                        "episode_id": pa.array([], type=pa.string()),
                        "anchor": pa.array([], type=pa.int64()),
                    }
                ),
            )

        with pytest.raises(ValueError, match="unknown snapshot"):
            catalog.create_manifest(
                table, "missing-snap", _samples([0]), snapshot=999999
            )
    finally:
        catalog.drop_table(name, if_exists=True)


def test_drop_snapshot_refuses_manifest(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("manifest_pin")
    table = _make_table(catalog, tmp_path, name)

    try:
        info = catalog.create_manifest(table, "eval", _samples([0]))
        with pytest.raises(ValueError, match="referenced by manifest"):
            catalog.drop_snapshot(table, info.snapshot_id)

        assert catalog.drop_manifest(table, "eval")
        assert catalog.drop_snapshot(table, info.snapshot_id)
    finally:
        catalog.drop_table(name, if_exists=True)


def test_create_manifest_overwrite(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("manifest_overwrite")
    table = _make_table(catalog, tmp_path, name)

    try:
        first = catalog.create_manifest(table, "eval", _samples([0]))
        second = catalog.create_manifest(
            table, "eval", _samples([1, 2]), overwrite=True
        )
        assert second.rows == 2
        assert second.snapshot_id != first.snapshot_id
        summaries = catalog.list_manifests(table)
        assert [(item.manifest, item.rows) for item in summaries] == [("eval", 2)]
    finally:
        catalog.drop_table(name, if_exists=True)


def test_drop_table_cascades_manifest(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("manifest_cascade")
    table = _make_table(catalog, tmp_path, name)

    catalog.create_manifest(table, "eval", _samples([0]))
    catalog.table(f"{name}{MANIFEST_TABLE_SUFFIX}")
    catalog.drop_table(name)
    with pytest.raises(TableNotFoundError):
        catalog.table(f"{name}{MANIFEST_TABLE_SUFFIX}")
