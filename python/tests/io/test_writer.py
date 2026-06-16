# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakesoul.io import IOConfig, Writer


def _local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    assert parsed.scheme == "file"
    return Path(unquote(parsed.path))


def _batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "value": pa.array(["a", "b"]),
        }
    )


def test_writer_accepts_arrow_inputs_and_finishes_once(tmp_path: Path) -> None:
    batch = _batch()
    table = pa.Table.from_batches([batch])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    config = IOConfig(path=tmp_path / "data", schema=batch.schema, batch_size=1)

    with Writer(config) as writer:
        assert writer.write(batch) == 2
        assert writer.write(table) == 2
        assert writer.write(reader) == 2

    result = writer.result
    assert result is not None
    assert result is writer.finish()
    assert result.row_count == 6
    assert len(result.files) == 1
    assert result.files[0].row_count == 6
    assert result.files[0].size > 0

    output_path = _local_path(result.files[0].path)
    assert output_path.suffix == ".parquet"
    assert pq.read_table(output_path).num_rows == 6

    with pytest.raises(RuntimeError, match="closed"):
        writer.write(batch)


def test_writer_dynamic_partitions(tmp_path: Path) -> None:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "part": pa.array(["a", "b", "a"]),
        }
    )
    config = IOConfig(
        path=tmp_path / "partitioned",
        schema=table.schema,
        partition_by=["part"],
    )

    writer = Writer(config)
    assert writer.write(table) == 3
    result = writer.finish()

    assert result.row_count == 3
    assert set(result.partitions) == {"part=a", "part=b"}
    assert sum(len(files) for files in result.partitions.values()) == 2
    for file_info in result.files:
        output_path = _local_path(file_info.path)
        assert output_path.exists()
        assert output_path.parent.name in {"part=a", "part=b"}
        assert pq.read_schema(output_path).names == ["id"]


def test_writer_aborts_on_context_error(tmp_path: Path) -> None:
    batch = _batch()
    writer = Writer(IOConfig(path=tmp_path / "aborted", schema=batch.schema))

    with pytest.raises(ValueError, match="stop"):
        with writer:
            writer.write(batch)
            raise ValueError("stop")

    assert writer.closed
    assert writer.result is None
    with pytest.raises(RuntimeError, match="aborted"):
        writer.finish()


def test_writer_rejects_schema_mismatch(tmp_path: Path) -> None:
    batch = _batch()
    writer = Writer(IOConfig(path=tmp_path / "data", schema=batch.schema))
    other = pa.record_batch({"id": pa.array([1], type=pa.int64())})

    with pytest.raises(ValueError, match="input schema"):
        writer.write(other)
    writer.abort()


def test_writer_vortex_output(tmp_path: Path) -> None:
    batch = _batch()
    writer = Writer(
        IOConfig(path=tmp_path / "vortex", schema=batch.schema, format="vortex")
    )

    writer.write(batch)
    result = writer.finish()

    assert result.row_count == 2
    assert len(result.files) == 1
    output_path = _local_path(result.files[0].path)
    assert output_path.suffix == ".vortex"
    assert output_path.exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"batch_size": 0}, "batch_size"),
        ({"thread_num": -1}, "thread_num"),
        ({"thread_num": False}, "thread_num"),
        ({"hash_bucket_num": 0}, "hash_bucket_num"),
        ({"format": "csv"}, "format"),
        ({"partition_by": ["missing"]}, "column not in schema"),
    ],
)
def test_writer_config_validation(
    tmp_path: Path, kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        IOConfig(path=tmp_path / "data", schema=_batch().schema, **kwargs)
