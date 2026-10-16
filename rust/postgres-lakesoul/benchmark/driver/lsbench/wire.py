"""Small PostgreSQL v3 client with no read-ahead or third-party dependencies."""
from __future__ import annotations

import asyncio
import socket
import struct
import time
from dataclasses import dataclass, field
from typing import Any


class PGError(Exception):
    def __init__(self, message: str, sqlstate: str | None = None):
        super().__init__(message)
        self.sqlstate = sqlstate


def error_fields(payload: bytes) -> dict[str, str]:
    fields = {}
    pos = 0
    while pos < len(payload) and payload[pos]:
        tag = chr(payload[pos])
        end = payload.index(0, pos + 1)
        fields[tag] = payload[pos + 1:end].decode("utf-8", "replace")
        pos = end + 1
    return fields


def frame(kind: bytes, payload: bytes = b"") -> bytes:
    return kind + struct.pack("!I", len(payload) + 4) + payload


@dataclass
class Result:
    columns: list[str] = field(default_factory=list)
    type_oids: list[int] = field(default_factory=list)
    values: list[tuple[str | None, ...]] = field(default_factory=list)
    rows: int = 0
    bytes: int = 0
    rowdesc_s: float | None = None
    first_row_s: float | None = None
    sqlstate: str | None = None
    error: str | None = None
    error_at: float | None = None
    ready_at: float | None = None
    command: str | None = None


class ReadPolicy:
    """Bound socket reads before recv, not after an eager asyncio stream buffer.

    A one-read token bucket permits at most 16 KiB burst. Stalling is triggered
    by cumulative wire bytes after startup, including protocol headers.
    """
    def __init__(self, rate: float | None = None, stall_after: int | None = None,
                 stall_seconds: float = 0):
        self.rate = rate
        self.stall_after = stall_after
        self.stall_seconds = stall_seconds
        self.received = 0
        self.stalled = False
        self.tokens = 0.0
        self.updated = time.monotonic()

    async def allowance(self, requested: int) -> int:
        if (self.stall_after is not None and not self.stalled
                and self.received >= self.stall_after):
            self.stalled = True
            await asyncio.sleep(self.stall_seconds)
            # Stalling must not accumulate a burst of catch-up tokens.
            self.updated = time.monotonic()
            self.tokens = 0
        size = min(requested, 16384)
        if self.stall_after is not None and not self.stalled:
            size = min(size, self.stall_after - self.received)
        if self.rate is None:
            return size
        while True:
            now = time.monotonic()
            self.tokens = min(16384.0, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            # Batch reads instead of waking once per byte at low rates.
            target = min(size, max(1, int(self.rate / 20)))
            if self.tokens >= target:
                allowed = min(size, int(self.tokens))
                self.tokens -= allowed
                return allowed
            await asyncio.sleep((target - self.tokens) / self.rate)


class Client:
    def __init__(self, host: str, port: int, user: str, database: str,
                 connect_timeout: float = 10, rcvbuf: int | None = None):
        self.host, self.port = host, port
        self.user, self.database = user, database
        self.connect_timeout = connect_timeout
        self.rcvbuf = rcvbuf
        self.sock: socket.socket | None = None
        self.backend_key: tuple[int, int] | None = None
        self.parameters: dict[str, str] = {}
        self.policy = ReadPolicy()
        self.result: Result | None = None

    async def connect(self) -> None:
        async with asyncio.timeout(self.connect_timeout):
            infos = await asyncio.get_running_loop().getaddrinfo(
                self.host, self.port, type=socket.SOCK_STREAM)
            last_error: OSError | None = None
            for family, kind, proto, _, address in infos:
                sock = socket.socket(family, kind, proto)
                sock.setblocking(False)
                if self.rcvbuf is not None:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.rcvbuf)
                try:
                    await asyncio.get_running_loop().sock_connect(sock, address)
                except OSError as exc:
                    sock.close()
                    last_error = exc
                    continue
                except BaseException:
                    sock.close()
                    raise
                self.sock = sock
                break
            if self.sock is None:
                raise last_error or OSError("No address for PostgreSQL host")
            try:
                params = (f"user\0{self.user}\0database\0{self.database}\0"
                          "client_encoding\0UTF8\0application_name\0lsbench\0\0").encode()
                payload = struct.pack("!I", 196608) + params
                await self.send(struct.pack("!I", len(payload) + 4) + payload)
                while True:
                    kind, payload = await self.message()
                    if kind == b"R":
                        method, = struct.unpack_from("!I", payload)
                        if method != 0:
                            raise PGError(f"Authentication method {method} unsupported; use trust authentication")
                    elif kind == b"S":
                        key, value, _ = payload.split(b"\0", 2)
                        self.parameters[key.decode()] = value.decode()
                    elif kind == b"K":
                        self.backend_key = struct.unpack("!II", payload)
                    elif kind == b"E":
                        fields = error_fields(payload)
                        raise PGError(fields.get("M", "Startup failed"), fields.get("C"))
                    elif kind == b"Z":
                        return
                    elif kind not in (b"N", b"v"):
                        raise PGError(f"Unexpected startup message {kind!r}")
            except BaseException:
                self.close(abrupt=True)
                raise

    async def send(self, data: bytes) -> None:
        if self.sock is None:
            raise ConnectionError("PostgreSQL connection is closed")
        await asyncio.get_running_loop().sock_sendall(self.sock, data)

    async def exact(self, count: int) -> bytes:
        data = bytearray()
        while len(data) < count:
            size = await self.policy.allowance(count - len(data))
            if self.sock is None:
                raise ConnectionError("PostgreSQL connection is closed")
            chunk = await asyncio.get_running_loop().sock_recv(self.sock, size)
            if not chunk:
                raise ConnectionError("PostgreSQL closed the connection")
            if self.policy.rate is not None:
                self.policy.tokens += size - len(chunk)
            self.policy.received += len(chunk)
            data.extend(chunk)
        return bytes(data)

    async def message(self) -> tuple[bytes, bytes]:
        header = await self.exact(5)
        length, = struct.unpack_from("!I", header, 1)
        if length < 4 or length > 256 * 1024 * 1024:
            raise PGError(f"Invalid PostgreSQL message length {length}")
        return header[:1], await self.exact(length - 4)

    async def query(self, sql: str, *, collect: bool = False,
                    protocol: str = "simple", max_rows: int = 0,
                    started: float | None = None) -> Result:
        result = self.result = Result()
        started = time.monotonic() if started is None else started
        paging = protocol == "extended" and max_rows > 0
        if protocol == "simple":
            await self.send(frame(b"Q", sql.encode() + b"\0"))
        else:
            parse = frame(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!h", 0))
            bind = frame(b"B", b"\0\0" + struct.pack("!hhh", 0, 0, 0))
            describe = frame(b"D", b"P\0")
            execute = frame(b"E", b"\0" + struct.pack("!i", max_rows))
            await self.send(parse + bind + describe + execute + frame(b"H" if paging else b"S"))
        synced = not paging
        while True:
            kind, payload = await self.message()
            now = time.monotonic()
            if kind == b"T":
                if result.rowdesc_s is None:
                    result.rowdesc_s = now - started
                count, = struct.unpack_from("!H", payload)
                pos = 2
                for _ in range(count):
                    end = payload.index(0, pos)
                    result.columns.append(payload[pos:end].decode("utf-8"))
                    pos = end + 1
                    _, _, oid, _, _, fmt = struct.unpack_from("!IhIhih", payload, pos)
                    if fmt != 0:
                        raise PGError("Server returned binary results despite text request")
                    result.type_oids.append(oid)
                    pos += 18
            elif kind == b"D":
                if result.first_row_s is None:
                    result.first_row_s = now - started
                result.rows += 1
                result.bytes += len(payload)
                if collect:
                    count, = struct.unpack_from("!H", payload)
                    pos = 2
                    row = []
                    for _ in range(count):
                        length, = struct.unpack_from("!i", payload, pos)
                        pos += 4
                        if length == -1:
                            row.append(None)
                        else:
                            row.append(payload[pos:pos + length].decode("utf-8"))
                            pos += length
                    result.values.append(tuple(row))
            elif kind == b"C":
                result.command = payload.rstrip(b"\0").decode("utf-8", "replace")
                if not synced:
                    await self.send(frame(b"S"))
                    synced = True
            elif kind == b"E":
                fields = error_fields(payload)
                result.sqlstate = fields.get("C")
                result.error = fields.get("M", "PostgreSQL error")
                result.error_at = now
                if not synced:
                    await self.send(frame(b"S"))
                    synced = True
            elif kind == b"s":  # PortalSuspended: no Sync until the portal is drained.
                await self.send(frame(b"E", b"\0" + struct.pack("!i", max_rows)) + frame(b"H"))
            elif kind == b"Z":
                result.ready_at = now
                return result
            elif kind in (b"N", b"A", b"1", b"2", b"3", b"n", b"I"):
                continue
            elif kind == b"S":
                key, value, _ = payload.split(b"\0", 2)
                self.parameters[key.decode()] = value.decode()
            else:
                raise PGError(f"Unexpected query message {kind!r}")

    async def cancel(self) -> None:
        if self.backend_key is None:
            raise PGError("Server did not provide BackendKeyData")
        async with asyncio.timeout(self.connect_timeout):
            reader, writer = await asyncio.open_connection(self.host, self.port)
            try:
                writer.write(struct.pack("!IIII", 16, 80877102, *self.backend_key))
                await writer.drain()
                # PostgreSQL closes this connection without replying.
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

    async def terminate(self) -> None:
        try:
            if self.sock is not None:
                await self.send(frame(b"X"))
        except (OSError, ConnectionError):
            pass
        finally:
            self.close()

    def close(self, *, abrupt: bool = False, rst: bool = False) -> None:
        if self.sock is not None:
            if rst:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            self.sock.close()
            self.sock = None
