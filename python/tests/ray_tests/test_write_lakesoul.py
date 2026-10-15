# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import pickle
from types import SimpleNamespace

from uuid import uuid4
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from lakesoul import LakeSoulCatalog
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


def test_datasink_ignores_no_column_empty_blocks_and_commits_rows(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.string()),
        ]
    )
    catalog = LakeSoulCatalog.from_env()
    name = f"ray_empty_partitions_{uuid4().hex}"
    table = catalog.create_table(
        name,
        path=(tmp_path / name).as_uri(),
        schema=schema,
        primary_keys=("id",),
        hash_bucket_num=1,
    )
    data = pa.table({"id": [1, 2], "value": ["a", "b"]}, schema=schema)
    try:
        datasink = LakeSoulDatasink(table, format="parquet")
        result = datasink.write(
            [
                pa.table({}),
                data.slice(0, 1),
                pa.table({}),
                data.slice(1, 1),
                pa.table({}),
            ],
            None,
        )
        datasink.on_write_complete(SimpleNamespace(write_returns=[result]))

        assert result.row_count == 2
        assert (
            table.scan().to_arrow_table().sort_by("id").to_pydict() == data.to_pydict()
        )
        files = [file for partition in table.scan_plan() for file in partition.files]
        with pytest.raises(ValueError, match="non-nullable"):
            datasink.write(
                [pa.table({}), pa.table({"id": pa.array([None], type=pa.int64())})],
                None,
            )
        assert [
            file for partition in table.scan_plan() for file in partition.files
        ] == files
        assert (
            table.scan().to_arrow_table().sort_by("id").to_pydict() == data.to_pydict()
        )
    finally:
        catalog.drop_table(name, if_exists=True)


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


@pytest.mark.parametrize("format", ["parquet", "vortex", "vortex-compact"])
def test_datasink_writes_sparse_stored_schema_without_padding(
    tmp_path: Path, format: str
) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("required", pa.int64(), nullable=False),
            pa.field("omitted", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )
    table_handle, committed = _fake_table(
        tmp_path / "sparse", schema, primary_keys=("id",)
    )
    datasink = LakeSoulDatasink(table_handle, format=format)
    block = pa.table(
        {
            "id": [1, 2],
            "required": [10, 20],
            "value": pa.array([None, 7], type=pa.int64()),
        }
    )
    result = datasink.write([block.slice(0, 1), block.slice(1, 1)], None)
    datasink.on_write_complete(SimpleNamespace(write_returns=[result]))

    assert result.row_count == 2
    assert all("omitted" not in file.existing_columns for file in result.files)
    assert len(committed) == 1
    if format == "parquet":
        written = pa.concat_tables([pq.read_table(file.path) for file in result.files])
        assert written.schema.names == ["id", "required", "value"]
        assert written.sort_by("id").to_pydict() == block.to_pydict()


@pytest.mark.parametrize(
    "values",
    [
        {"part": ["north"], "required": [7]},
        {"id": [1], "required": [7]},
        {"id": [1], "part": ["north"]},
        {"id": [None], "part": ["north"], "required": [7]},
        {"id": [1], "part": ["north"], "required": [None]},
    ],
)
def test_datasink_rejects_stored_constraint_violations_without_committing(
    tmp_path: Path, values
) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string()),
            pa.field("required", pa.int64(), nullable=False),
            pa.field("optional", pa.string()),
        ]
    )
    table_handle, committed = _fake_table(
        tmp_path / "invalid",
        schema,
        primary_keys=("id",),
        partition_by=("part",),
    )
    arrays = {
        name: pa.array(
            value, type=pa.int64() if name in {"id", "required"} else pa.string()
        )
        for name, value in values.items()
    }
    datasink = LakeSoulDatasink(table_handle)
    with pytest.raises(ValueError, match="missing required|non-nullable"):
        datasink.write([pa.table(arrays)], None)
    assert committed == []


@pytest.mark.parametrize("format", ["parquet", "vortex", "vortex-compact"])
def test_ray_sparse_upsert_preserves_omitted_values_and_rejects_invalid_writes(
    tmp_path: Path, ray_session, format: str
) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = f"ray_sparse_{uuid4().hex}"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("required", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
        primary_keys=("id",),
        hash_bucket_num=2,
    )
    try:
        table.write_ray(
            ray.data.from_arrow(
                pa.table(
                    {
                        "id": [1, 2],
                        "required": [1, 2],
                        "label": ["keep", "other"],
                        "value": [10, 20],
                    }
                )
            ),
            format=format,
        )
        table.write_ray(
            ray.data.from_arrow(
                pa.table({"id": [1, 3], "required": [11, 3], "value": [100, 30]})
            ),
            format=format,
        )
        expected = [
            {"id": 1, "required": 11, "label": "keep", "value": 100},
            {"id": 2, "required": 2, "label": "other", "value": 20},
            {"id": 3, "required": 3, "label": None, "value": 30},
        ]
        assert table.scan().to_arrow_table().sort_by("id").to_pylist() == expected
        table.write_ray(
            ray.data.from_arrow(
                pa.table(
                    {
                        "id": [1],
                        "required": [12],
                        "label": pa.array([None], type=pa.string()),
                    }
                )
            ),
            format=format,
        )
        expected[0] = {"id": 1, "required": 12, "label": None, "value": 100}
        assert table.scan().to_arrow_table().sort_by("id").to_pylist() == expected
        files = [file for partition in table.scan_plan() for file in partition.files]

        with pytest.raises(ValueError, match="missing required"):
            table.write_ray(ray.data.from_arrow(pa.table({"id": [1]})), format=format)
        with pytest.raises(
            (ValueError, ray.exceptions.RayTaskError), match="non-nullable"
        ):
            table.write_ray(
                ray.data.from_arrow(
                    pa.table({"id": [1], "required": pa.array([None], type=pa.int64())})
                ),
                format=format,
            )
        assert [
            file for partition in table.scan_plan() for file in partition.files
        ] == files
        assert table.scan().to_arrow_table().sort_by("id").to_pylist() == expected
    finally:
        catalog.drop_table(table_name, if_exists=True)


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
