# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest
from daft.io.sink import WriteResult as DaftWriteResult
from daft.recordbatch import MicroPartition

from lakesoul.catalog import TableWriteConfig
from lakesoul.daft.sink import LakeSoulDataSink, _normalize_table
from lakesoul.io import FileInfo, WriteResult


def _write_result(*, path: str, rows: int, size: int = 10) -> WriteResult:
    file_info = FileInfo(
        partition="-5",
        path=path,
        size=size,
        existing_columns=("id",),
        row_count=rows,
        other_info={},
    )
    return WriteResult(
        files=(file_info,),
        partitions={"-5": (file_info,)},
        row_count=rows,
    )


def _fake_table(
    schema: pa.Schema,
    *,
    primary_keys: tuple[str, ...] = (),
    partition_by: tuple[str, ...] = (),
):
    committed: list[WriteResult] = []

    class FakeCatalog:
        def _merge_object_store_options(self, overrides):
            merged = {"region": "test"}
            merged.update(dict(overrides or {}))
            return merged

        def _commit_write_result(self, table, result):
            committed.append(result)

    table = SimpleNamespace(
        name="target",
        namespace="analytics",
        catalog=FakeCatalog(),
        write_config=lambda format="vortex-compact": TableWriteConfig(
            table_name="target",
            namespace="analytics",
            path="s3://bucket/target",
            schema=schema,
            primary_keys=primary_keys,
            partition_by=partition_by,
            hash_bucket_num=1,
            format=format,
        ),
    )
    return table, committed


def _micropartition(values: list[int]) -> MicroPartition:
    return MicroPartition.from_pydict({"id": pa.array(values, type=pa.int64())})


def test_normalize_table_accepts_daft_arrow_representation_changes() -> None:
    expected = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
            pa.field("payload", pa.binary()),
        ]
    )
    actual = pa.Table.from_arrays(
        [
            pa.array([1, 2], type=pa.int64()),
            pa.array(["alpha", "beta"], type=pa.large_string()),
            pa.array([b"a", b"b"], type=pa.large_binary()),
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("label", pa.large_string()),
                pa.field("payload", pa.large_binary()),
            ]
        ),
    )

    normalized = _normalize_table(actual, expected)

    assert normalized.schema == expected
    assert normalized.to_pydict() == actual.to_pydict()


@pytest.mark.parametrize("list_type", [pa.list_, pa.large_list])
def test_normalize_table_accepts_fixed_size_vectors(list_type) -> None:
    expected = pa.schema([pa.field("state", pa.list_(pa.float32(), 14))])
    actual = pa.table(
        {"state": pa.array([list(range(14)), None], type=list_type(pa.float32()))}
    )

    normalized = _normalize_table(actual, expected)

    assert normalized.schema == expected
    assert normalized.to_pydict() == actual.to_pydict()


@pytest.mark.parametrize("length", [13, 15])
def test_normalize_table_rejects_wrong_vector_length(length: int) -> None:
    expected = pa.schema([pa.field("state", pa.list_(pa.float32(), 14))])
    actual = pa.table(
        {"state": pa.array([list(range(length))], type=pa.large_list(pa.float32()))}
    )

    with pytest.raises(pa.ArrowInvalid):
        _normalize_table(actual, expected)


def test_normalize_table_rejects_vector_element_type_change() -> None:
    expected = pa.schema([pa.field("state", pa.list_(pa.float32(), 14))])
    actual = pa.table(
        {"state": pa.array([list(range(14))], type=pa.large_list(pa.int64()))}
    )

    with pytest.raises(ValueError, match="incompatible column"):
        _normalize_table(actual, expected)


def test_normalize_table_rejects_null_in_non_nullable_column() -> None:
    expected = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    actual = pa.table({"id": pa.array([1, None], type=pa.int64())})

    with pytest.raises(ValueError, match="non-nullable"):
        _normalize_table(actual, expected)


def test_normalize_table_rejects_incompatible_type() -> None:
    expected = pa.schema([pa.field("id", pa.int64())])
    actual = pa.table({"id": pa.array(["1"], type=pa.string())})

    with pytest.raises(ValueError, match="incompatible column"):
        _normalize_table(actual, expected)


def test_normalize_table_preserves_sparse_pk_columns_and_explicit_null() -> None:
    expected = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string()),
            pa.field("omitted", pa.string()),
            pa.field("value", pa.string()),
        ]
    )
    actual = pa.table(
        {
            "value": pa.array([None], type=pa.large_string()),
            "part": pa.array(["north"], type=pa.large_string()),
            "id": [1],
        }
    )
    normalized = _normalize_table(
        actual, expected, primary_keys=("id",), partition_by=("part",)
    )
    assert normalized.schema == pa.schema(
        [expected.field(name) for name in ("id", "part", "value")]
    )
    assert normalized.to_pydict() == {"id": [1], "part": ["north"], "value": [None]}


@pytest.mark.parametrize(
    "values",
    [
        {"id": [1], "required": [7]},
        {"id": [1], "part": ["north"]},
        {"id": [None], "part": ["north"], "required": [7]},
        {"id": [1], "part": ["north"], "required": [None]},
    ],
)
def test_sink_checks_stored_constraints_before_opening_writer(
    monkeypatch, values
) -> None:
    import lakesoul.daft.sink as sink_module

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string()),
            pa.field("required", pa.int64(), nullable=False),
            pa.field("optional", pa.string()),
        ]
    )
    table, committed = _fake_table(schema, primary_keys=("id",), partition_by=("part",))
    arrays = {
        name: pa.array(
            value, type=pa.int64() if name in {"id", "required"} else pa.string()
        )
        for name, value in values.items()
    }

    def unexpected_writer(config):
        raise AssertionError("invalid input must not create a writer")

    monkeypatch.setattr(sink_module, "Writer", unexpected_writer)
    sink = LakeSoulDataSink(table)
    with pytest.raises(ValueError, match="missing required|non-nullable"):
        list(sink.write(iter([MicroPartition.from_pydict(arrays)])))
    assert committed == []


def test_sink_uses_one_writer_per_micropartition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lakesoul.daft.sink as sink_module

    table, _ = _fake_table(pa.schema([pa.field("id", pa.int64())]))
    writers = []

    class FakeWriter:
        def __init__(self, config) -> None:
            self.config = config
            self.tables = []
            self.aborted = False
            writers.append(self)

        def write(self, arrow_table) -> int:
            self.tables.append(arrow_table)
            return arrow_table.num_rows

        def finish(self) -> WriteResult:
            return _write_result(
                path="s3://bucket/target/part-0.parquet",
                rows=sum(arrow_table.num_rows for arrow_table in self.tables),
            )

        def abort(self) -> None:
            self.aborted = True

    monkeypatch.setattr(sink_module, "Writer", FakeWriter)
    sink = LakeSoulDataSink(table, max_file_size=128 * 1024 * 1024)

    outputs = list(sink.write(iter([_micropartition([1, 2]), _micropartition([3])])))

    assert len(writers) == 2
    assert [len(writer.tables) for writer in writers] == [1, 1]
    assert writers[0].config.max_file_size == 128 * 1024 * 1024
    assert writers[1].config.max_file_size == 128 * 1024 * 1024
    assert [output.rows_written for output in outputs] == [2, 1]
    assert [output.result.row_count for output in outputs] == [2, 1]


def test_sink_aborts_micropartition_writer_when_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lakesoul.daft.sink as sink_module

    table, _ = _fake_table(pa.schema([pa.field("id", pa.int64())]))
    writers = []

    class FakeWriter:
        def __init__(self, config) -> None:
            self.aborted = False
            writers.append(self)

        def write(self, arrow_table) -> int:
            raise RuntimeError("write failed")

        def finish(self) -> WriteResult:
            raise AssertionError("finish must not be called")

        def abort(self) -> None:
            self.aborted = True

    monkeypatch.setattr(sink_module, "Writer", FakeWriter)
    sink = LakeSoulDataSink(table)
    with pytest.raises(RuntimeError, match="write failed"):
        list(sink.write(iter([_micropartition([1])])))

    assert writers[0].aborted


def test_sink_preserves_write_error_when_abort_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lakesoul.daft.sink as sink_module

    table, _ = _fake_table(pa.schema([pa.field("id", pa.int64())]))

    class FakeWriter:
        def __init__(self, config) -> None:
            pass

        def write(self, arrow_table) -> int:
            raise RuntimeError("write failed")

        def finish(self) -> WriteResult:
            raise AssertionError("finish must not be called")

        def abort(self) -> None:
            raise RuntimeError("abort failed")

    monkeypatch.setattr(sink_module, "Writer", FakeWriter)
    sink = LakeSoulDataSink(table)

    with pytest.raises(RuntimeError, match="write failed"):
        list(sink.write(iter([_micropartition([1])])))


def test_sink_finalize_merges_task_results_and_commits_once() -> None:
    table, committed = _fake_table(pa.schema([pa.field("id", pa.int64())]))
    sink = LakeSoulDataSink(table)
    first = _write_result(path="s3://bucket/target/part-0.parquet", rows=2, size=20)
    second = _write_result(path="s3://bucket/target/part-1.parquet", rows=3, size=30)

    output = sink.finalize(
        [
            DaftWriteResult(first, bytes_written=20, rows_written=2),
            DaftWriteResult(second, bytes_written=30, rows_written=3),
        ]
    )

    assert output.to_pydict() == {
        "files_written": [2],
        "rows_written": [5],
        "bytes_written": [50],
    }
    assert len(committed) == 1
    assert committed[0].files == first.files + second.files
    assert committed[0].row_count == 5
    assert sink.result == committed[0]


def test_sink_finalize_empty_write_does_not_commit() -> None:
    table, committed = _fake_table(pa.schema([pa.field("id", pa.int64())]))
    sink = LakeSoulDataSink(table)

    output = sink.finalize([])

    assert output.to_pydict() == {
        "files_written": [0],
        "rows_written": [0],
        "bytes_written": [0],
    }
    assert committed == []
    assert sink.result == WriteResult(files=(), partitions={}, row_count=0)
