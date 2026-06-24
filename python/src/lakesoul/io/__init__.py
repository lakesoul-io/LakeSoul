# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import os
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Literal

import pyarrow as pa

from lakesoul._lib._writer import _NativeFileInfo, _NativeWriter

__all__ = ["FileInfo", "IOConfig", "WriteResult", "Writer"]


@dataclass(slots=True)
class IOConfig:
    """Configuration for writing Arrow data to LakeSoul data files."""

    path: str | os.PathLike[str]
    schema: pa.Schema
    format: Literal["parquet", "vortex"] = "parquet"
    primary_keys: Sequence[str] = ()
    partition_by: Sequence[str] = ()
    hash_bucket_num: int = 1
    batch_size: int = 8192
    thread_num: int | None = None
    max_file_size: int | None = None
    max_row_group_size: int = 250_000
    object_store_options: Mapping[str, str] = field(default_factory=dict)
    options: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_config(self)


@dataclass(frozen=True, slots=True)
class FileInfo:
    """A data file produced by a completed Writer."""

    partition: str
    path: str
    size: int
    existing_columns: tuple[str, ...]
    row_count: int
    other_info: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Files produced by a completed Writer."""

    files: tuple[FileInfo, ...]
    partitions: Mapping[str, tuple[FileInfo, ...]]
    row_count: int

    @classmethod
    def _from_native(cls, outputs: list[_NativeFileInfo]) -> WriteResult:
        files = tuple(
            FileInfo(
                partition=output.partition,
                path=output.path,
                size=output.size,
                existing_columns=tuple(output.existing_columns),
                row_count=output.row_count,
                other_info=MappingProxyType(dict(output.other_info)),
            )
            for output in outputs
        )
        grouped: dict[str, list[FileInfo]] = {}
        for file_info in files:
            grouped.setdefault(file_info.partition, []).append(file_info)
        partitions = MappingProxyType(
            {
                partition: tuple(partition_files)
                for partition, partition_files in grouped.items()
            }
        )
        return cls(
            files=files,
            partitions=partitions,
            row_count=sum(file_info.row_count for file_info in files),
        )


class Writer:
    """Synchronous writer for LakeSoul data files."""

    def __init__(self, config: IOConfig) -> None:
        if not isinstance(config, IOConfig):
            raise TypeError("config must be an IOConfig")
        _validate_config(config)

        self._schema = config.schema
        self._batch_size = config.batch_size
        self._native = _NativeWriter(
            path=_normalize_path(config.path),
            schema=config.schema,
            format=config.format.lower(),
            primary_keys=list(config.primary_keys),
            partition_by=list(config.partition_by),
            hash_bucket_num=config.hash_bucket_num,
            batch_size=config.batch_size,
            thread_num=_resolve_thread_num(config.thread_num),
            max_file_size=config.max_file_size,
            max_row_group_size=config.max_row_group_size,
            object_store_options=dict(config.object_store_options),
            options=dict(config.options),
        )
        self._result: WriteResult | None = None
        self._aborted = False

    @property
    def result(self) -> WriteResult | None:
        return self._result

    @property
    def closed(self) -> bool:
        return self._native.closed

    def write(self, data: pa.RecordBatch | pa.Table | pa.RecordBatchReader) -> int:
        """Write Arrow data and return the number of accepted rows."""
        if self.closed:
            raise RuntimeError("writer is closed")

        if isinstance(data, pa.RecordBatch):
            return self._write_batch(data)
        if isinstance(data, pa.Table):
            self._validate_schema(data.schema)
            return sum(
                self._write_batch(batch)
                for batch in data.to_batches(max_chunksize=self._batch_size)
            )
        if isinstance(data, pa.RecordBatchReader):
            self._validate_schema(data.schema)
            return sum(self._write_batch(batch) for batch in data)
        raise TypeError(
            "data must be a pyarrow.RecordBatch, Table, or RecordBatchReader"
        )

    def finish(self) -> WriteResult:
        """Flush pending data, close the writer, and return written files."""
        if self._result is not None:
            return self._result
        if self._aborted:
            raise RuntimeError("writer has been aborted")
        self._result = WriteResult._from_native(self._native.finish())
        return self._result

    def abort(self) -> None:
        """Abort pending writes and close the writer."""
        if self.closed:
            return
        self._native.abort()
        self._aborted = True

    def __enter__(self) -> Writer:
        if self.closed:
            raise RuntimeError("writer is closed")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        if exc_type is None:
            self.finish()
        else:
            try:
                self.abort()
            except Exception as abort_error:
                warnings.warn(
                    f"failed to abort LakeSoul writer: {abort_error}",
                    RuntimeWarning,
                    stacklevel=2,
                )
        return False

    def __del__(self) -> None:
        native = getattr(self, "_native", None)
        if native is None or native.closed:
            return
        warnings.warn(
            "unclosed LakeSoul Writer; aborting pending writes",
            ResourceWarning,
            stacklevel=2,
        )
        try:
            native.abort()
        except Exception:
            pass

    def _write_batch(self, batch: pa.RecordBatch) -> int:
        self._validate_schema(batch.schema)
        return self._native.write(batch)

    def _validate_schema(self, schema: pa.Schema) -> None:
        if not schema.equals(self._schema):
            raise ValueError(
                f"input schema does not match writer schema:\n"
                f"expected: {self._schema}\nactual: {schema}"
            )


def _validate_config(config: IOConfig) -> None:
    try:
        path = os.fspath(config.path)
    except TypeError as error:
        raise TypeError("path must be a string or os.PathLike") from error
    if not isinstance(path, str) or not path:
        raise ValueError("path must not be empty")
    if not isinstance(config.schema, pa.Schema):
        raise TypeError("schema must be a pyarrow.Schema")

    if not isinstance(config.format, str):
        raise TypeError("format must be a string")
    format_name = config.format.lower()
    if format_name not in {"parquet", "vortex"}:
        raise ValueError("format must be 'parquet' or 'vortex'")

    primary_keys = _validate_columns("primary_keys", config.primary_keys)
    partition_by = _validate_columns("partition_by", config.partition_by)
    schema_names = set(config.schema.names)
    for column in (*primary_keys, *partition_by):
        if column not in schema_names:
            raise ValueError(f"column not in schema: {column}")

    _validate_positive_int("hash_bucket_num", config.hash_bucket_num)
    _validate_positive_int("batch_size", config.batch_size)
    _validate_positive_int("max_row_group_size", config.max_row_group_size)
    if config.thread_num is not None:
        if isinstance(config.thread_num, bool) or not isinstance(
            config.thread_num, int
        ):
            raise ValueError("thread_num must be None, zero, or a positive integer")
        if config.thread_num < 0:
            raise ValueError("thread_num must be None, zero, or a positive integer")
    if config.max_file_size is not None:
        _validate_positive_int("max_file_size", config.max_file_size)
    _validate_string_mapping("object_store_options", config.object_store_options)
    _validate_string_mapping("options", config.options)


def _validate_columns(name: str, columns: Sequence[str]) -> tuple[str, ...]:
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence):
        raise TypeError(f"{name} must be a sequence of strings")
    values = tuple(columns)
    if any(not isinstance(column, str) or not column for column in values):
        raise TypeError(f"{name} must contain non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must not contain duplicate columns")
    return values


def _validate_positive_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _validate_string_mapping(name: str, values: Mapping[str, str]) -> None:
    if not isinstance(values, Mapping):
        raise TypeError(f"{name} must be a mapping of strings")
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in values.items()
    ):
        raise TypeError(f"{name} must contain only string keys and values")


def _resolve_thread_num(thread_num: int | None) -> int:
    if thread_num is None or thread_num == 0:
        return os.cpu_count() or 1
    return thread_num


def _normalize_path(path: str | os.PathLike[str]) -> str:
    value = os.fspath(path)
    if "://" not in value:
        value = Path(value).expanduser().resolve().as_uri()
    if value != "file:///":
        value = value.rstrip("/")
    return value
