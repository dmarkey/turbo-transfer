"""Tests for the RPC wire protocol."""

import pytest

from turbo_transfer.protocol import (
    Flags,
    FsOp,
    HEADER_SIZE,
    ProtocolError,
    decode_header,
    decode_payload,
    encode_request,
    encode_response,
)


class TestEncodeDecodeRequest:
    def test_basic_request(self):
        msg = encode_request(FsOp.GETATTR, 42, {"path": "/foo"})
        assert msg[:4] == b"TURB"
        flags, op, req_id, payload_len = decode_header(msg[:HEADER_SIZE])
        assert op == FsOp.GETATTR
        assert req_id == 42
        assert not (flags & Flags.RESPONSE)
        payload = decode_payload(flags, msg[HEADER_SIZE:])
        assert payload["path"] == "/foo"

    def test_compressed_request(self):
        # Large payload triggers compression
        big = {"data": b"\x00" * 10000}
        msg = encode_request(FsOp.WRITE, 1, big, compress=True)
        flags, op, req_id, payload_len = decode_header(msg[:HEADER_SIZE])
        assert flags & Flags.COMPRESSED
        payload = decode_payload(flags, msg[HEADER_SIZE:])
        assert payload["data"] == b"\x00" * 10000

    def test_small_payload_not_compressed(self):
        msg = encode_request(FsOp.GETATTR, 1, {"path": "/"}, compress=True)
        flags, _, _, _ = decode_header(msg[:HEADER_SIZE])
        assert not (flags & Flags.COMPRESSED)


class TestEncodeDecodeResponse:
    def test_success_response(self):
        msg = encode_response(FsOp.GETATTR, 42, {"st_mode": 0o100644, "st_size": 100})
        flags, op, req_id, _ = decode_header(msg[:HEADER_SIZE])
        assert flags & Flags.RESPONSE
        assert not (flags & Flags.ERROR)
        payload = decode_payload(flags, msg[HEADER_SIZE:])
        assert payload["st_mode"] == 0o100644

    def test_error_response(self):
        msg = encode_response(FsOp.GETATTR, 42, {"errno": 2, "msg": "Not found"}, error=True)
        flags, op, req_id, _ = decode_header(msg[:HEADER_SIZE])
        assert flags & Flags.RESPONSE
        assert flags & Flags.ERROR
        payload = decode_payload(flags, msg[HEADER_SIZE:])
        assert payload["errno"] == 2


class TestDecodeHeader:
    def test_bad_magic(self):
        with pytest.raises(ProtocolError, match="Bad magic"):
            decode_header(b"BAAD" + b"\x00" * 12)

    def test_short_header(self):
        with pytest.raises(ProtocolError, match="too short"):
            decode_header(b"TURB\x00")


class TestFsOpEnum:
    def test_all_ops_unique(self):
        values = [op.value for op in FsOp]
        assert len(values) == len(set(values))
