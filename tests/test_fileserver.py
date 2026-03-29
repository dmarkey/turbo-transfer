"""Tests for the file server — direct handler calls without network."""

import os
import tempfile
from pathlib import Path

import pytest

from turbo_transfer.fileserver import FileServer
from turbo_transfer.protocol import FsOp


@pytest.fixture
def server_root():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "hello.txt").write_text("Hello World")
        (root / "subdir").mkdir()
        (root / "subdir" / "nested.txt").write_text("Nested file")
        yield root


@pytest.fixture
def server(server_root):
    return FileServer(server_root)


class TestGetattr:
    def test_file(self, server):
        result = server.handle(FsOp.GETATTR, {"path": "/hello.txt"})
        assert result["st_size"] == 11
        assert result["st_mode"] & 0o100000  # regular file

    def test_dir(self, server):
        result = server.handle(FsOp.GETATTR, {"path": "/"})
        assert result["st_mode"] & 0o040000  # directory

    def test_not_found(self, server):
        with pytest.raises(OSError):
            server.handle(FsOp.GETATTR, {"path": "/nope"})


class TestReaddir:
    def test_root(self, server):
        result = server.handle(FsOp.READDIR, {"path": "/"})
        names = [e["name"] for e in result["entries"]]
        assert "hello.txt" in names
        assert "subdir" in names

    def test_subdir(self, server):
        result = server.handle(FsOp.READDIR, {"path": "/subdir"})
        names = [e["name"] for e in result["entries"]]
        assert "nested.txt" in names


class TestFileOps:
    def test_read(self, server):
        r = server.handle(FsOp.OPEN, {"path": "/hello.txt", "flags": os.O_RDONLY})
        fh = r["fh"]
        data = server.handle(FsOp.READ, {"fh": fh, "size": 100, "offset": 0})
        assert data["data"] == b"Hello World"
        server.handle(FsOp.RELEASE, {"fh": fh})

    def test_write(self, server, server_root):
        r = server.handle(FsOp.CREATE, {"path": "/new.txt", "mode": 0o644})
        fh = r["fh"]
        w = server.handle(FsOp.WRITE, {"fh": fh, "data": b"Test data", "offset": 0})
        assert w["written"] == 9
        server.handle(FsOp.RELEASE, {"fh": fh})
        assert (server_root / "new.txt").read_text() == "Test data"

    def test_truncate(self, server, server_root):
        server.handle(FsOp.TRUNCATE, {"path": "/hello.txt", "length": 5})
        assert (server_root / "hello.txt").read_text() == "Hello"


class TestDirOps:
    def test_mkdir_rmdir(self, server, server_root):
        server.handle(FsOp.MKDIR, {"path": "/newdir", "mode": 0o755})
        assert (server_root / "newdir").is_dir()
        server.handle(FsOp.RMDIR, {"path": "/newdir"})
        assert not (server_root / "newdir").exists()

    def test_unlink(self, server, server_root):
        server.handle(FsOp.UNLINK, {"path": "/hello.txt"})
        assert not (server_root / "hello.txt").exists()

    def test_rename(self, server, server_root):
        server.handle(FsOp.RENAME, {"old": "/hello.txt", "new": "/renamed.txt"})
        assert not (server_root / "hello.txt").exists()
        assert (server_root / "renamed.txt").read_text() == "Hello World"


class TestPathSecurity:
    def test_traversal_blocked(self, server):
        with pytest.raises(PermissionError, match="traversal"):
            server.handle(FsOp.GETATTR, {"path": "/../../../etc/passwd"})


class TestStatfs:
    def test_statfs(self, server):
        result = server.handle(FsOp.STATFS, {"path": "/"})
        assert "f_bsize" in result
        assert "f_blocks" in result


class TestSymlink:
    def test_symlink_readlink(self, server, server_root):
        server.handle(FsOp.SYMLINK, {"target": "hello.txt", "path": "/link.txt"})
        assert (server_root / "link.txt").is_symlink()
        result = server.handle(FsOp.READLINK, {"path": "/link.txt"})
        assert result["target"] == "hello.txt"
