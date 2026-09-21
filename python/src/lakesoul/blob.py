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

import struct
import zlib
from dataclasses import dataclass

import pyarrow as pa

BLOB_TAG_INLINE = 0x00
BLOB_TAG_EXTERNAL = 0x01

_REF_PREFIX = struct.Struct("<IIQ")


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

    def read(self, offset: int = 0, size: int | None = None) -> bytes:
        """Read a byte range of the value, fetching only what is needed.

        A full read of an external value verifies its CRC32.
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
        data = _read_range(self.pack, self.offset + offset, size)
        if offset == 0 and size == self.length:
            actual = zlib.crc32(data) & 0xFFFFFFFF
            if actual != self.crc32:
                raise ValueError(
                    f"blob checksum mismatch for {self.pack}: "
                    f"expected {self.crc32:#010x}, got {actual:#010x}"
                )
        return data

    def materialize(self) -> bytes:
        """Read the whole value (equivalent to ``read()``)."""
        return self.read()


def materialize_blob(value: bytes | bytearray | memoryview) -> bytes:
    """Materialize a tagged blob value, inline or external."""
    return BlobRef.parse(value).read()


def _open_filesystem(uri: str) -> tuple[pa.fs.FileSystem, str]:
    try:
        return pa.fs.FileSystem.from_uri(uri)
    except (pa.ArrowInvalid, ValueError):
        return pa.fs.LocalFileSystem(), uri


def _read_range(uri: str, offset: int, size: int) -> bytes:
    filesystem, path = _open_filesystem(uri)
    with filesystem.open_input_file(path) as stream:
        stream.seek(offset)
        data = stream.read(size)
    if len(data) != size:
        raise ValueError(
            f"short blob read from {uri}: wanted {size} bytes, got {len(data)}"
        )
    return data
