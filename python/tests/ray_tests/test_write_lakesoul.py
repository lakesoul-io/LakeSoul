# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import pickle
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from lakesoul.catalog import TableWriteConfig
from lakesoul.ray import LakeSoulDatasink

from lakesoul.ray import write_lakesoul as write_lakesoul_fn


def _fake_table(
    path: Path,
    schema: pa.Schema,
    *,
    namespace: str = "analytics",
    primary_keys: tuple[str, ...] = (),
    partition_by: tuple[str, ...] = (),
    hash_bucket_num: int = 1,
    format: str = "vortex-compact",
    object_store_options: dict[str, str] | None = None,
):
    committed: list[tuple[str, str, list[tuple[str, str, int, list[str]]]]] = []

    def write_config(format: str = format):
        return TableWriteConfig(
            table_name="target",
            namespace=namespace,
            path=path.as_uri(),
            schema=schema,
            primary_keys=primary_keys,
            partition_by=partition_by,
            hash_bucket_num=hash_bucket_num,
            format=format,  # type: ignore[arg-type]
        )

    class FakeCatalog:
        def __init__(self) -> None:
            self._client = SimpleNamespace(
                commit_data_files=lambda table_name, namespace, files: committed.append(
                    (table_name, namespace, files)
                )
            )
            self._object_store_options = object_store_options or {}

        def _merge_object_store_options(self, overrides):
            merged = dict(self._object_store_options)
            merged.update(dict(overrides or {}))
            return merged

    table = SimpleNamespace(
        name="target",
        namespace=namespace,
        catalog=FakeCatalog(),
        write_config=write_config,
    )
    return table, committed


def test_datasink_writes_arrow_blocks_and_commits_once(tmp_path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    table_handle, committed = _fake_table(
        tmp_path / "table",
        table.schema,
        format="parquet",
    )

    datasink = LakeSoulDatasink(table_handle, format="parquet")
    task_result = datasink.write([table.slice(0, 1), table.slice(1, 1)], None)
    datasink.on_write_complete(SimpleNamespace(write_returns=[task_result]))

    assert task_result.row_count == 2
    assert len(task_result.files) == 1
    assert task_result.files[0].partition == "-5"
    assert pq.read_table(task_result.files[0].path).to_pydict() == table.to_pydict()
    assert committed == [
        (
            "target",
            "analytics",
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


def test_datasink_defaults_to_vortex_compact(tmp_path: Path) -> None:
    table = pa.table({"id": [1], "value": ["a"]})
    table_handle, _ = _fake_table(tmp_path / "table", table.schema)

    datasink = LakeSoulDatasink(table_handle)
    task_result = datasink.write([table], None)

    assert len(task_result.files) == 1
    assert task_result.files[0].path.endswith(".vortex")
    assert task_result.files[0].other_info["physical_format"] == "vortex-compact"


def test_datasink_write_result_is_pickleable_before_commit(tmp_path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    table_handle, committed = _fake_table(tmp_path / "table", table.schema)

    datasink = LakeSoulDatasink(table_handle)
    task_result = datasink.write([table], None)
    restored = pickle.loads(pickle.dumps(task_result))
    datasink.on_write_complete(SimpleNamespace(write_returns=[restored]))

    assert restored == task_result
    assert committed == [
        (
            "target",
            "analytics",
            [
                (
                    restored.files[0].partition,
                    restored.files[0].path,
                    restored.files[0].size,
                    list(restored.files[0].existing_columns),
                )
            ],
        )
    ]


def test_datasink_uses_table_partition_and_vortex_config(tmp_path: Path) -> None:
    table = pa.table({"id": [1], "part": ["a"]})
    table_handle, _ = _fake_table(
        tmp_path / "table",
        table.schema,
        primary_keys=("id",),
        partition_by=("part",),
        hash_bucket_num=4,
        format="vortex",
    )

    datasink = LakeSoulDatasink(table_handle, format="vortex")
    task_result = datasink.write([table], None)

    assert not datasink.supports_distributed_writes
    assert task_result.files[0].partition == "part=a"
    assert task_result.files[0].path.endswith(".vortex")


def test_datasink_rejects_schema_mismatch(tmp_path: Path) -> None:
    expected = pa.schema([pa.field("id", pa.int64())])
    table_handle, _ = _fake_table(tmp_path / "table", expected)
    datasink = LakeSoulDatasink(table_handle)

    with pytest.raises(ValueError, match="schema does not match"):
        datasink.write([pa.table({"other": [1]})], None)


def test_empty_write_does_not_commit(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    table_handle, committed = _fake_table(tmp_path / "table", schema)
    datasink = LakeSoulDatasink(table_handle)
    result = datasink.write([pa.Table.from_batches([], schema=schema)], None)
    datasink.on_write_complete(SimpleNamespace(write_returns=[result]))

    assert result.files == ()
    assert committed == []


def test_empty_write_result_is_pickleable(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    table_handle, committed = _fake_table(tmp_path / "table", schema)
    datasink = LakeSoulDatasink(table_handle)

    result = datasink.write([pa.Table.from_batches([], schema=schema)], None)
    restored = pickle.loads(pickle.dumps(result))
    datasink.on_write_complete(SimpleNamespace(write_returns=[restored]))

    assert restored == result
    assert restored.files == ()
    assert committed == []


def test_datasink_serialization_does_not_capture_table_handle(tmp_path: Path) -> None:
    table = pa.table({"id": [1]})
    table_handle, _ = _fake_table(tmp_path / "table", table.schema)
    datasink = LakeSoulDatasink(table_handle)

    state = datasink.__getstate__()

    assert state["_table_handle"] is None
    assert state["_table"].table_name == "target"


def test_dataset_method_is_registered() -> None:
    assert (
        ray.data.Dataset.write_lakesoul  # type: ignore[attr-defined]
        is write_lakesoul_fn
    )
