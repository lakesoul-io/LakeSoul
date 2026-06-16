# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from lakesoul.ray import LakeSoulDatasink

write_module = import_module("lakesoul.ray.write_lakesoul")


def _patch_table(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    schema: pa.Schema,
    *,
    properties: str = '{"hashBucketNum":"1"}',
    partitions: str = ";",
) -> None:
    table_info = SimpleNamespace(
        table_path=path.as_uri(),
        properties=properties,
        partitions=partitions,
    )
    monkeypatch.setattr(
        write_module,
        "get_table_info_by_name",
        lambda table_name, namespace: table_info,
    )
    monkeypatch.setattr(
        write_module,
        "get_arrow_schema_by_table_name",
        lambda **kwargs: schema,
    )


def test_datasink_writes_arrow_blocks_and_commits_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    _patch_table(monkeypatch, tmp_path / "table", table.schema)
    committed: list[tuple[str, str, list[tuple[str, str, int, list[str]]]]] = []
    monkeypatch.setattr(
        write_module,
        "commit_data_files",
        lambda table_name, namespace, files: committed.append(
            (table_name, namespace, files)
        ),
    )

    datasink = LakeSoulDatasink("target")
    task_result = datasink.write([table.slice(0, 1), table.slice(1, 1)], None)
    datasink.on_write_complete(SimpleNamespace(write_returns=[task_result]))

    assert task_result.row_count == 2
    assert len(task_result.files) == 1
    assert task_result.files[0].partition == "-5"
    assert pq.read_table(task_result.files[0].path).to_pydict() == table.to_pydict()
    assert committed == [
        (
            "target",
            "default",
            [
                (
                    task_result.files[0].partition,
                    task_result.files[0].path,
                    task_result.files[0].size,
                    list(task_result.files[0].existing_columns),
                )
            ],
        )
    ]

    committed.clear()
    datasink.on_write_complete([task_result])
    assert len(committed) == 1


def test_datasink_uses_table_partition_and_vortex_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = pa.table({"id": [1], "part": ["a"]})
    _patch_table(
        monkeypatch,
        tmp_path / "table",
        table.schema,
        properties='{"hashBucketNum":"4","file_format":"vortex"}',
        partitions="part;id",
    )

    datasink = LakeSoulDatasink("target")
    task_result = datasink.write([table], None)

    assert not datasink.supports_distributed_writes
    assert task_result.files[0].partition == "part=a"
    assert task_result.files[0].path.endswith(".vortex")


def test_datasink_rejects_schema_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = pa.schema([pa.field("id", pa.int64())])
    _patch_table(monkeypatch, tmp_path / "table", expected)
    datasink = LakeSoulDatasink("target")

    with pytest.raises(ValueError, match="schema does not match"):
        datasink.write([pa.table({"other": [1]})], None)


def test_empty_write_does_not_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    _patch_table(monkeypatch, tmp_path / "table", schema)
    committed = False

    def commit(*args: object) -> None:
        nonlocal committed
        committed = True

    monkeypatch.setattr(write_module, "commit_data_files", commit)
    datasink = LakeSoulDatasink("target")
    result = datasink.write([pa.Table.from_batches([], schema=schema)], None)
    datasink.on_write_complete(SimpleNamespace(write_returns=[result]))

    assert result.files == ()
    assert not committed


def test_dataset_method_is_registered() -> None:
    assert ray.data.Dataset.write_lakesoul is write_module.write_lakesoul  # type: ignore[unresolved-attribute]


def test_real_write(
    ray_session: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del ray_session
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    committed: list[list[tuple[str, str, int, list[str]]]] = []
    dataset = ray.data.from_arrow(table)
    dataset.write_lakesoul("target", concurrency=1)  # type: ignore[unresolved-attribute]

    assert len(committed) == 1
    assert len(committed[0]) == 1
    assert committed[0][0][2] > 0


def test_dataset_write_lakesoul_end_to_end(
    ray_session: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del ray_session
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    _patch_table(monkeypatch, tmp_path / "table", table.schema)
    committed: list[list[tuple[str, str, int, list[str]]]] = []
    monkeypatch.setattr(
        write_module,
        "commit_data_files",
        lambda table_name, namespace, files: committed.append(files),
    )

    dataset = ray.data.from_arrow(table)
    dataset.write_lakesoul("target", concurrency=1)  # type: ignore[unresolved-attribute]

    assert len(committed) == 1
    assert len(committed[0]) == 1
    assert committed[0][0][2] > 0
