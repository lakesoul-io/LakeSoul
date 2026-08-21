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


def _fake_table(schema: pa.Schema):
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
            primary_keys=(),
            partition_by=(),
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
