"""File server — serves local filesystem operations over RPC."""

from __future__ import annotations

import asyncio
import errno
import os
import platform
import socket
import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rich.console import Console

from .protocol import (
    Flags,
    FsOp,
    async_recv_message,
    encode_response,
    CHUNK_SIZE,
)

TRANSFER_PORT = 9876
SOCKET_BUF_SIZE = 4 * 1024 * 1024
console = Console()


class FileServer:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self._open_files: dict[int, int] = {}  # fh -> os file descriptor
        self._next_fh = 0
        self._executor = ThreadPoolExecutor(max_workers=16)

    def _resolve(self, path: str, follow_symlinks: bool = True) -> str:
        """Resolve RPC path to local path. Prevent traversal."""
        joined = self.root / path.lstrip("/")
        if follow_symlinks:
            resolved = joined.resolve()
        else:
            # Resolve parent but not the final component (for symlink ops)
            resolved = joined.parent.resolve() / joined.name
        if not str(resolved).startswith(str(self.root)):
            raise PermissionError("Path traversal blocked")
        return str(resolved)

    def _stat_to_dict(self, st: os.stat_result) -> dict:
        return {
            "st_mode": st.st_mode,
            "st_ino": st.st_ino,
            "st_nlink": st.st_nlink,
            "st_uid": st.st_uid,
            "st_gid": st.st_gid,
            "st_size": st.st_size,
            "st_atime": st.st_atime,
            "st_mtime": st.st_mtime,
            "st_ctime": st.st_ctime,
        }

    def handle(self, op: FsOp, payload: dict) -> dict:
        """Dispatch an RPC request. Runs in a thread."""
        handler = _HANDLERS.get(op)
        if handler is None:
            raise OSError(errno.ENOSYS, f"Unsupported operation: {op}")
        return handler(self, payload)

    def _hello(self, p: dict) -> dict:
        return {"hostname": platform.node(), "root": str(self.root)}

    def _getattr(self, p: dict) -> dict:
        path = self._resolve(p["path"], follow_symlinks=False)
        st = os.lstat(path)
        return self._stat_to_dict(st)

    def _readdir(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        entries = []
        for name in os.listdir(path):
            full = os.path.join(path, name)
            try:
                st = os.lstat(full)
                entries.append({"name": name, **self._stat_to_dict(st)})
            except OSError:
                entries.append({"name": name, "st_mode": 0})
        return {"entries": entries}

    def _statfs(self, p: dict) -> dict:
        path = self._resolve(p.get("path", "/"))
        st = os.statvfs(path)
        return {
            "f_bsize": st.f_bsize,
            "f_frsize": st.f_frsize,
            "f_blocks": st.f_blocks,
            "f_bfree": st.f_bfree,
            "f_bavail": st.f_bavail,
            "f_files": st.f_files,
            "f_ffree": st.f_ffree,
            "f_favail": getattr(st, "f_favail", st.f_ffree),
            "f_namemax": st.f_namemax,
        }

    def _open(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        flags = p.get("flags", os.O_RDONLY)
        fd = os.open(path, flags)
        fh = self._next_fh
        self._next_fh += 1
        self._open_files[fh] = fd
        return {"fh": fh}

    def _read(self, p: dict) -> dict:
        fd = self._open_files.get(p["fh"])
        if fd is None:
            raise OSError(errno.EBADF, "Bad file handle")
        data = os.pread(fd, p["size"], p["offset"])
        return {"data": data}

    def _write(self, p: dict) -> dict:
        fd = self._open_files.get(p["fh"])
        if fd is None:
            raise OSError(errno.EBADF, "Bad file handle")
        written = os.pwrite(fd, p["data"], p["offset"])
        return {"written": written}

    def _release(self, p: dict) -> dict:
        fd = self._open_files.pop(p["fh"], None)
        if fd is not None:
            os.close(fd)
        return {}

    def _create(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        mode = p.get("mode", 0o644)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        fh = self._next_fh
        self._next_fh += 1
        self._open_files[fh] = fd
        st = os.fstat(fd)
        return {"fh": fh, **self._stat_to_dict(st)}

    def _truncate(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        os.truncate(path, p["length"])
        return {}

    def _fsync(self, p: dict) -> dict:
        fd = self._open_files.get(p["fh"])
        if fd is not None:
            os.fsync(fd)
        return {}

    def _unlink(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        os.unlink(path)
        return {}

    def _mkdir(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        os.mkdir(path, p.get("mode", 0o755))
        return {}

    def _rmdir(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        os.rmdir(path)
        return {}

    def _rename(self, p: dict) -> dict:
        old = self._resolve(p["old"])
        new = self._resolve(p["new"])
        os.rename(old, new)
        return {}

    def _symlink(self, p: dict) -> dict:
        target = p["target"]
        path = self._resolve(p["path"])
        os.symlink(target, path)
        return {}

    def _readlink(self, p: dict) -> dict:
        path = self._resolve(p["path"], follow_symlinks=False)
        target = os.readlink(path)
        return {"target": target}

    def _setattr(self, p: dict) -> dict:
        path = self._resolve(p["path"])
        if "st_mode" in p:
            os.chmod(path, p["st_mode"])
        if "st_uid" in p or "st_gid" in p:
            uid = p.get("st_uid", -1)
            gid = p.get("st_gid", -1)
            os.chown(path, uid, gid)
        if "st_size" in p:
            os.truncate(path, p["st_size"])
        if "st_atime" in p and "st_mtime" in p:
            os.utime(path, (p["st_atime"], p["st_mtime"]))
        st = os.lstat(path)
        return self._stat_to_dict(st)


_HANDLERS = {
    FsOp.HELLO: FileServer._hello,
    FsOp.GETATTR: FileServer._getattr,
    FsOp.READDIR: FileServer._readdir,
    FsOp.STATFS: FileServer._statfs,
    FsOp.OPEN: FileServer._open,
    FsOp.READ: FileServer._read,
    FsOp.WRITE: FileServer._write,
    FsOp.RELEASE: FileServer._release,
    FsOp.CREATE: FileServer._create,
    FsOp.TRUNCATE: FileServer._truncate,
    FsOp.FSYNC: FileServer._fsync,
    FsOp.UNLINK: FileServer._unlink,
    FsOp.MKDIR: FileServer._mkdir,
    FsOp.RMDIR: FileServer._rmdir,
    FsOp.RENAME: FileServer._rename,
    FsOp.SYMLINK: FileServer._symlink,
    FsOp.READLINK: FileServer._readlink,
    FsOp.SETATTR: FileServer._setattr,
}


async def _handle_client(reader, writer, server: FileServer):
    peer = writer.get_extra_info("peername")
    console.print(f"[green]Client connected: {peer[0]}[/green]")

    sock = writer.get_extra_info("socket")
    if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF_SIZE)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF_SIZE)
        except OSError:
            pass

    loop = asyncio.get_event_loop()

    try:
        while True:
            try:
                flags, op, request_id, payload = await async_recv_message(reader)
            except (asyncio.IncompleteReadError, ConnectionError):
                break

            try:
                result = await loop.run_in_executor(server._executor, server.handle, op, payload)
                compress = op == FsOp.READ and len(result.get("data", b"")) > 4096
                resp = encode_response(op, request_id, result, compress=compress)
            except OSError as e:
                resp = encode_response(
                    op, request_id,
                    {"errno": e.errno or errno.EIO, "msg": str(e)},
                    error=True,
                )

            writer.write(resp)
            await writer.drain()
    except Exception as e:
        console.print(f"[red]Client error: {e}[/red]")
    finally:
        writer.close()
        console.print(f"[dim]Client disconnected: {peer[0]}[/dim]")


async def start_server(root: str, bind_addr: str = "::"):
    root_path = Path(root).resolve()
    if not root_path.is_dir():
        console.print(f"[red]Not a directory: {root_path}[/red]")
        return

    server_obj = FileServer(root_path)

    tcp_server = await asyncio.start_server(
        lambda r, w: _handle_client(r, w, server_obj),
        bind_addr,
        TRANSFER_PORT,
        family=socket.AF_INET6,
    )

    for sock in tcp_server.sockets:
        try:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        except OSError:
            pass

    console.print(f"[green]Serving [bold]{root_path}[/bold] on port {TRANSFER_PORT}[/green]")
    console.print("[dim]Waiting for connections...[/dim]")

    async with tcp_server:
        await tcp_server.serve_forever()
