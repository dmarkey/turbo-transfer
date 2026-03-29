"""Tests for RPC over loopback — full client/server integration without FUSE."""

import asyncio
import os
import tempfile
import threading
import time
from pathlib import Path

import pytest

from turbo_transfer.fileserver import FileServer, _handle_client, TRANSFER_PORT
from turbo_transfer.protocol import FsOp
from turbo_transfer.rpc import ConnectionPool, RpcError, rpc_call


@pytest.fixture
def server_root():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "test.txt").write_text("Hello from RPC")
        (root / "bigfile.bin").write_bytes(os.urandom(2 * 1024 * 1024))
        (root / "subdir").mkdir()
        (root / "subdir" / "child.txt").write_text("child")
        yield root


@pytest.fixture
def loopback_server(server_root):
    """Start a file server on loopback in a background thread."""
    fs = FileServer(server_root)

    async def run():
        server = await asyncio.start_server(
            lambda r, w: _handle_client(r, w, fs),
            "127.0.0.1",
            0,  # random port
        )
        port = server.sockets[0].getsockname()[1]
        return server, port

    loop = asyncio.new_event_loop()
    server, port = loop.run_until_complete(run())

    thread = threading.Thread(target=lambda: loop.run_until_complete(server.serve_forever()), daemon=True)
    thread.start()

    yield port

    server.close()
    loop.call_soon_threadsafe(loop.stop)


@pytest.fixture
def pool(loopback_server):
    """ConnectionPool pointing at the loopback server."""
    import socket

    port = loopback_server

    class IPv4Pool(ConnectionPool):
        """Test pool that connects via IPv4 loopback."""
        def _create_connection(self):
            from turbo_transfer.rpc import RpcConnection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.connect(("127.0.0.1", port))
            return RpcConnection(sock)

    p = IPv4Pool("::1", "0", port=port, size=4)
    yield p
    p.close_all()


class TestRpcHello:
    def test_hello(self, pool):
        result = rpc_call(pool, FsOp.HELLO, {})
        assert "hostname" in result
        assert "root" in result


class TestRpcGetattr:
    def test_file(self, pool):
        result = rpc_call(pool, FsOp.GETATTR, {"path": "/test.txt"})
        assert result["st_size"] == len("Hello from RPC")

    def test_not_found(self, pool):
        with pytest.raises(RpcError) as exc_info:
            rpc_call(pool, FsOp.GETATTR, {"path": "/nope"})
        assert exc_info.value.errno == 2  # ENOENT


class TestRpcReadWrite:
    def test_read_file(self, pool):
        r = rpc_call(pool, FsOp.OPEN, {"path": "/test.txt", "flags": os.O_RDONLY})
        fh = r["fh"]
        data = rpc_call(pool, FsOp.READ, {"fh": fh, "size": 100, "offset": 0})
        assert data["data"] == b"Hello from RPC"
        rpc_call(pool, FsOp.RELEASE, {"fh": fh})

    def test_read_large_file(self, pool):
        r = rpc_call(pool, FsOp.OPEN, {"path": "/bigfile.bin", "flags": os.O_RDONLY})
        fh = r["fh"]
        # Read in chunks
        all_data = b""
        offset = 0
        while True:
            chunk = rpc_call(pool, FsOp.READ, {"fh": fh, "size": 1024 * 1024, "offset": offset})
            if not chunk["data"]:
                break
            all_data += chunk["data"]
            offset += len(chunk["data"])
        assert len(all_data) == 2 * 1024 * 1024
        rpc_call(pool, FsOp.RELEASE, {"fh": fh})

    def test_write_new_file(self, pool, server_root):
        r = rpc_call(pool, FsOp.CREATE, {"path": "/written.txt", "mode": 0o644})
        fh = r["fh"]
        rpc_call(pool, FsOp.WRITE, {"fh": fh, "data": b"Written via RPC", "offset": 0})
        rpc_call(pool, FsOp.RELEASE, {"fh": fh})
        assert (server_root / "written.txt").read_text() == "Written via RPC"


class TestRpcReaddir:
    def test_list_root(self, pool):
        result = rpc_call(pool, FsOp.READDIR, {"path": "/"})
        names = [e["name"] for e in result["entries"]]
        assert "test.txt" in names
        assert "subdir" in names

    def test_entries_have_stat(self, pool):
        result = rpc_call(pool, FsOp.READDIR, {"path": "/"})
        for entry in result["entries"]:
            assert "st_mode" in entry


class TestRpcConcurrency:
    def test_parallel_requests(self, pool):
        """Multiple threads making RPC calls concurrently."""
        results = [None] * 8
        errors = []

        def do_request(i):
            try:
                r = rpc_call(pool, FsOp.GETATTR, {"path": "/test.txt"})
                results[i] = r["st_size"]
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=do_request, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Errors: {errors}"
        assert all(r == len("Hello from RPC") for r in results)
