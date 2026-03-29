"""RPC wire protocol for Turbo Transfer filesystem operations.

Header (16 bytes):
  magic:          4 bytes  b"TURB"
  flags:          1 byte   (bit 0: compressed, bit 1: response, bit 2: error)
  op:             1 byte   (FsOp enum)
  request_id:     4 bytes  (big-endian uint32)
  reserved:       2 bytes
  payload_length: 4 bytes  (big-endian uint32)
"""

from __future__ import annotations

import struct
from enum import IntEnum

import msgpack

MAGIC = b"TURB"
HEADER_SIZE = 16
HEADER_FMT = "!4sBBIHI"  # magic, flags, op, request_id, reserved, payload_length
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB


class Flags:
    COMPRESSED = 0x01
    RESPONSE = 0x02
    ERROR = 0x04


class FsOp(IntEnum):
    HELLO = 0x01

    GETATTR = 0x10
    READDIR = 0x11
    STATFS = 0x13

    OPEN = 0x20
    READ = 0x21
    WRITE = 0x22
    RELEASE = 0x23
    CREATE = 0x24
    TRUNCATE = 0x25
    FSYNC = 0x26

    UNLINK = 0x30
    MKDIR = 0x31
    RMDIR = 0x32
    RENAME = 0x33
    SYMLINK = 0x34
    READLINK = 0x35

    SETATTR = 0x40


class ProtocolError(Exception):
    pass


def encode_request(op: FsOp, request_id: int, payload: dict, compress: bool = False) -> bytes:
    from . import compression

    data = msgpack.packb(payload, use_bin_type=True)
    flags = 0
    if compress and len(data) > 4096:
        data = compression.compress(data)
        flags |= Flags.COMPRESSED
    header = struct.pack(HEADER_FMT, MAGIC, flags, op, request_id, 0, len(data))
    return header + data


def encode_response(op: FsOp, request_id: int, payload: dict, error: bool = False, compress: bool = False) -> bytes:
    from . import compression

    data = msgpack.packb(payload, use_bin_type=True)
    flags = Flags.RESPONSE
    if error:
        flags |= Flags.ERROR
    if compress and len(data) > 4096:
        data = compression.compress(data)
        flags |= Flags.COMPRESSED
    header = struct.pack(HEADER_FMT, MAGIC, flags, op, request_id, 0, len(data))
    return header + data


def decode_header(data: bytes) -> tuple[int, FsOp, int, int]:
    """Returns (flags, op, request_id, payload_length)."""
    if len(data) < HEADER_SIZE:
        raise ProtocolError(f"Header too short: {len(data)}")
    magic, flags, op, request_id, _, payload_length = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])
    if magic != MAGIC:
        raise ProtocolError(f"Bad magic: {magic!r}")
    return flags, FsOp(op), request_id, payload_length


def decode_payload(flags: int, data: bytes) -> dict:
    from . import compression

    if flags & Flags.COMPRESSED:
        data = compression.decompress(data)
    return msgpack.unpackb(data, raw=False)


def recv_message(sock) -> tuple[int, FsOp, int, dict]:
    """Blocking receive. Returns (flags, op, request_id, payload).

    Reads from a socket, not asyncio.
    """
    header_buf = _recv_exact(sock, HEADER_SIZE)
    flags, op, request_id, payload_length = decode_header(header_buf)
    payload_buf = _recv_exact(sock, payload_length) if payload_length > 0 else b""
    payload = decode_payload(flags, payload_buf) if payload_buf else {}
    return flags, op, request_id, payload


def _recv_exact(sock, n: int) -> bytes:
    """Read exactly n bytes from a socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed")
        buf.extend(chunk)
    return bytes(buf)


async def async_recv_message(reader) -> tuple[int, FsOp, int, dict]:
    """Async receive for the server side."""
    header_buf = await reader.readexactly(HEADER_SIZE)
    flags, op, request_id, payload_length = decode_header(header_buf)
    payload_buf = await reader.readexactly(payload_length) if payload_length > 0 else b""
    payload = decode_payload(flags, payload_buf) if payload_buf else {}
    return flags, op, request_id, payload
