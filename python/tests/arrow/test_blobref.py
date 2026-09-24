# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pyarrow.fs as pa_fs
import pytest

from lakesoul.blob import (
    BLOBREF_SUFFIX,
    blob_pack_path,
    blobref_path,
    read_blobref,
    write_blobref,
)


def test_blob_pack_path() -> None:
    assert (
        blob_pack_path("s3://bucket/table", "frame", "abc")
        == "s3://bucket/table/_blob/frame/abc.blob"
    )
    assert blob_pack_path("s3://bucket/table/", "frame", "abc") == (
        "s3://bucket/table/_blob/frame/abc.blob"
    )
    generated = blob_pack_path("s3://bucket/table", "frame")
    assert generated.startswith("s3://bucket/table/_blob/frame/")
    assert generated.endswith(".blob")


def test_blobref_path() -> None:
    assert blobref_path("s3://bucket/table/part-0.parquet") == (
        "s3://bucket/table/part-0.parquet" + BLOBREF_SUFFIX
    )


def test_blobref_roundtrip(tmp_path) -> None:
    filesystem = pa_fs.LocalFileSystem()
    data_file = str(tmp_path / "part-0.parquet")
    packs = [
        "s3://bucket/table/_blob/frame/b.blob",
        "s3://bucket/table/_blob/frame/a.blob",
    ]

    assert write_blobref(data_file, packs, filesystem=filesystem) == (
        data_file + BLOBREF_SUFFIX
    )
    assert read_blobref(data_file, filesystem=filesystem) == sorted(set(packs))
    assert (tmp_path / ("part-0.parquet" + BLOBREF_SUFFIX)).exists()


def test_blobref_missing_sidecar(tmp_path) -> None:
    filesystem = pa_fs.LocalFileSystem()
    assert read_blobref(str(tmp_path / "part-0.parquet"), filesystem=filesystem) is None


def test_blobref_rejects_unknown_version(tmp_path) -> None:
    filesystem = pa_fs.LocalFileSystem()
    data_file = str(tmp_path / "part-0.parquet")
    sidecar = tmp_path / ("part-0.parquet" + BLOBREF_SUFFIX)
    sidecar.write_text('{"version": 99, "packs": []}')
    with pytest.raises(ValueError, match="unsupported blobref version"):
        read_blobref(data_file, filesystem=filesystem)
