# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Zero-copy access to columns stored with the blob layout.

By default the native reader materializes tagged blob values by reading their
pack file up front. A scan created with
``scan.options(reader_options={"blob_materialize": "false"})`` keeps the tagged
representation instead, and :class:`BlobRef` turns each value into inline bytes
or a range read against its pack file.

Example:
    >>> from lakesoul import BlobRef
    >>> scan = catalog.scan("video").options(
    ...     reader_options={"blob_materialize": "false"}
    ... )
    >>> for value in scan.to_arrow_table().column("frame").to_pylist():
    ...     blob = BlobRef.parse(value)
    ...     first_kib = blob.read(0, 1024)
"""

from __future__ import annotations

import json
import struct
import uuid
import zlib
from dataclasses import dataclass
from urllib.parse import urlparse

import pyarrow as pa

BLOB_TAG_INLINE = 0x00
BLOB_TAG_EXTERNAL = 0x01

BLOB_DIR = "_blob"
BLOBREF_SUFFIX = ".blobref"
BLOBREF_VERSION = 1

_REF_PREFIX = struct.Struct("<IIQ")


def blob_pack_path(table_path: str, column: str, pack_id: str | None = None) -> str:
    """Return the immutable pack path for a column.

    Packs are shared between data files, so the name only carries a random id.
    """
    return (
        f"{table_path.rstrip('/')}/{BLOB_DIR}/{column}/"
        f"{pack_id or uuid.uuid4().hex}.blob"
    )


def blobref_path(data_file: str) -> str:
    """Return the sidecar path that belongs to a data file."""
    return f"{data_file}{BLOBREF_SUFFIX}"


def write_blobref(
    data_file: str,
    packs: list[str],
    filesystem: pa.fs.FileSystem | None = None,
) -> str:
    """Write the ``.blobref`` sidecar next to a data file and return its path."""
    payload = json.dumps(
        {"version": BLOBREF_VERSION, "packs": sorted(set(packs))},
        separators=(",", ":"),
    ).encode("utf-8")
    resolved, path = _resolve(data_file, filesystem)
    with resolved.open_output_stream(blobref_path(path)) as stream:
        stream.write(payload)
    return blobref_path(data_file)


def read_blobref(
    data_file: str,
    filesystem: pa.fs.FileSystem | None = None,
) -> list[str] | None:
    """Read the packs referenced by a data file, or ``None`` when absent."""
    resolved, path = _resolve(data_file, filesystem)
    sidecar = blobref_path(path)
    try:
        with resolved.open_input_file(sidecar) as stream:
            payload = json.loads(stream.read().decode("utf-8"))
    except FileNotFoundError:
        return None
    if payload.get("version") != BLOBREF_VERSION:
        raise ValueError(f"unsupported blobref version in {sidecar}")
    return list(payload.get("packs", []))


def _resolve(
    uri: str, filesystem: pa.fs.FileSystem | None
) -> tuple[pa.fs.FileSystem, str]:
    if filesystem is not None:
        parsed = urlparse(uri)
        return filesystem, f"{parsed.netloc}/{parsed.path.lstrip('/')}"
    return _open_filesystem(uri)


@dataclass(frozen=True)
class BlobRef:
    """A single blob value in its tagged binary representation.

    ``inline`` holds the bytes for values stored in the data file; external
    values carry the pack file location and the byte range instead.
    """

    inline: bytes | None = None
    pack: str | None = None
    offset: int = 0
    length: int = 0
    crc32: int = 0

    @classmethod
    def parse(cls, value: bytes | bytearray | memoryview) -> BlobRef:
        """Decode a tagged blob value as written by the native writer."""
        raw = bytes(value)
        if not raw:
            raise ValueError("empty blob value")
        tag, payload = raw[0], raw[1:]
        if tag == BLOB_TAG_INLINE:
            return cls(inline=payload)
        if tag == BLOB_TAG_EXTERNAL:
            if len(payload) <= _REF_PREFIX.size:
                raise ValueError("truncated external blob reference")
            crc32, length, offset = _REF_PREFIX.unpack_from(payload)
            return cls(
                pack=payload[_REF_PREFIX.size :].decode("utf-8"),
                offset=offset,
                length=length,
                crc32=crc32,
            )
        raise ValueError(f"unknown blob tag {tag:#x}")

    @property
    def is_inline(self) -> bool:
        """Whether the value lives in the data file."""
        return self.inline is not None

    @property
    def size(self) -> int:
        """Payload length in bytes."""
        return len(self.inline) if self.inline is not None else self.length

    def read(
        self,
        offset: int = 0,
        size: int | None = None,
        filesystem: pa.fs.FileSystem | None = None,
    ) -> bytes:
        """Read a byte range of the value, fetching only what is needed.

        A full read of an external value verifies its CRC32. Pass
        ``filesystem`` to reuse an already configured filesystem (for example
        an S3 filesystem built from the table's object store options) instead
        of resolving the pack URI from the environment.
        """
        if offset < 0:
            raise ValueError("offset must not be negative")
        if self.inline is not None:
            return (
                self.inline[offset:]
                if size is None
                else self.inline[offset : offset + size]
            )
        if self.pack is None:
            raise ValueError("blob reference has neither inline bytes nor a pack path")
        remaining = self.length - offset
        if remaining < 0:
            raise ValueError(f"offset {offset} exceeds blob size {self.length}")
        if size is None or size > remaining:
            size = remaining
        data = _read_range(self.pack, self.offset + offset, size, filesystem)
        if offset == 0 and size == self.length:
            actual = zlib.crc32(data) & 0xFFFFFFFF
            if actual != self.crc32:
                raise ValueError(
                    f"blob checksum mismatch for {self.pack}: "
                    f"expected {self.crc32:#010x}, got {actual:#010x}"
                )
        return data

    def materialize(self, filesystem: pa.fs.FileSystem | None = None) -> bytes:
        """Read the whole value (equivalent to ``read()``)."""
        return self.read(filesystem=filesystem)


def materialize_blob(
    value: bytes | bytearray | memoryview,
    filesystem: pa.fs.FileSystem | None = None,
) -> bytes:
    """Materialize a tagged blob value, inline or external."""
    return BlobRef.parse(value).read(filesystem=filesystem)


def _open_filesystem(uri: str) -> tuple[pa.fs.FileSystem, str]:
    try:
        return pa.fs.FileSystem.from_uri(uri)
    except (pa.ArrowInvalid, ValueError):
        return pa.fs.LocalFileSystem(), uri


def _read_range(
    uri: str,
    offset: int,
    size: int,
    filesystem: pa.fs.FileSystem | None = None,
) -> bytes:
    if filesystem is None:
        filesystem, path = _open_filesystem(uri)
    else:
        parsed = urlparse(uri)
        path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"
    with filesystem.open_input_file(path) as stream:
        stream.seek(offset)
        data = stream.read(size)
    if len(data) != size:
        raise ValueError(
            f"short blob read from {uri}: wanted {size} bytes, got {len(data)}"
        )
    return data
