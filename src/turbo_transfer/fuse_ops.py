"""FUSE filesystem that proxies all operations to a remote file server via RPC."""

from __future__ import annotations

import errno
import os
import stat
import time

from refuse.high import FUSE, FuseOSError, Operations

from .cache import AttrCache, DirCache
from .protocol import FsOp
from .rpc import ConnectionPool, RpcError, rpc_call


class TurboFS(Operations):
    """FUSE operations backed by a remote file server."""

    def __init__(self, pool: ConnectionPool):
        self._pool = pool
        self._attr_cache = AttrCache(ttl=1.0)
        self._dir_cache = DirCache(ttl=2.0)

    def _rpc(self, op: FsOp, payload: dict, compress: bool = False) -> dict:
        try:
            return rpc_call(self._pool, op, payload, compress=compress)
        except RpcError as e:
            raise FuseOSError(e.errno)

    # -- Metadata --

    def getattr(self, path, fh=None):
        cached = self._attr_cache.get(path)
        if cached:
            return cached
        result = self._rpc(FsOp.GETATTR, {"path": path})
        self._attr_cache.put(path, result)
        return result

    def readdir(self, path, fh):
        cached = self._dir_cache.get(path)
        if cached is not None:
            entries = cached
        else:
            result = self._rpc(FsOp.READDIR, {"path": path})
            entries = result["entries"]
            self._dir_cache.put(path, entries)
            # Populate attr cache from readdir results
            for e in entries:
                child_path = os.path.join(path, e["name"]).replace("//", "/")
                self._attr_cache.put(child_path, {
                    k: v for k, v in e.items() if k != "name"
                })

        for e in entries:
            yield e["name"], {k: v for k, v in e.items() if k != "name"}, 0
        # Also yield . and ..
        # refuse/fusepy expect these or handles them — entries from server already exclude them

    def readlink(self, path):
        result = self._rpc(FsOp.READLINK, {"path": path})
        return result["target"]

    def statfs(self, path):
        return self._rpc(FsOp.STATFS, {"path": path})

    # -- File operations --

    def open(self, path, flags):
        result = self._rpc(FsOp.OPEN, {"path": path, "flags": flags})
        return result["fh"]

    def read(self, path, size, offset, fh):
        result = self._rpc(FsOp.READ, {"fh": fh, "size": size, "offset": offset})
        return result["data"]

    def write(self, path, data, offset, fh):
        result = self._rpc(FsOp.WRITE, {"fh": fh, "data": data, "offset": offset}, compress=True)
        return result["written"]

    def release(self, path, fh):
        self._rpc(FsOp.RELEASE, {"fh": fh})
        self._attr_cache.invalidate(path)

    def create(self, path, mode, fi=None):
        result = self._rpc(FsOp.CREATE, {"path": path, "mode": mode})
        self._attr_cache.invalidate(path)
        parent = os.path.dirname(path)
        self._dir_cache.invalidate(parent)
        return result["fh"]

    def truncate(self, path, length, fh=None):
        self._rpc(FsOp.TRUNCATE, {"path": path, "length": length})
        self._attr_cache.invalidate(path)

    def fsync(self, path, datasync, fh):
        self._rpc(FsOp.FSYNC, {"fh": fh})

    def flush(self, path, fh):
        # fsync on flush to ensure data is written
        self._rpc(FsOp.FSYNC, {"fh": fh})

    # -- Directory operations --

    def mkdir(self, path, mode):
        self._rpc(FsOp.MKDIR, {"path": path, "mode": mode})
        parent = os.path.dirname(path)
        self._dir_cache.invalidate(parent)

    def rmdir(self, path):
        self._rpc(FsOp.RMDIR, {"path": path})
        self._attr_cache.invalidate(path)
        parent = os.path.dirname(path)
        self._dir_cache.invalidate(parent)

    def unlink(self, path):
        self._rpc(FsOp.UNLINK, {"path": path})
        self._attr_cache.invalidate(path)
        parent = os.path.dirname(path)
        self._dir_cache.invalidate(parent)

    def rename(self, old, new):
        self._rpc(FsOp.RENAME, {"old": old, "new": new})
        self._attr_cache.invalidate(old)
        self._attr_cache.invalidate(new)
        self._dir_cache.invalidate(os.path.dirname(old))
        self._dir_cache.invalidate(os.path.dirname(new))

    def symlink(self, target, source):
        # FUSE calls symlink(target, source) where source is the new link path
        self._rpc(FsOp.SYMLINK, {"target": target, "path": source})
        parent = os.path.dirname(source)
        self._dir_cache.invalidate(parent)

    # -- Attribute operations --

    def chmod(self, path, mode):
        self._rpc(FsOp.SETATTR, {"path": path, "st_mode": mode})
        self._attr_cache.invalidate(path)

    def chown(self, path, uid, gid):
        self._rpc(FsOp.SETATTR, {"path": path, "st_uid": uid, "st_gid": gid})
        self._attr_cache.invalidate(path)

    def utimens(self, path, times=None):
        if times:
            atime, mtime = times
        else:
            now = time.time()
            atime = mtime = now
        self._rpc(FsOp.SETATTR, {"path": path, "st_atime": atime, "st_mtime": mtime})
        self._attr_cache.invalidate(path)

    def destroy(self, path):
        self._pool.close_all()


def mount_remote(pool: ConnectionPool, mountpoint: str, foreground: bool = True):
    """Mount the remote filesystem at mountpoint."""
    os.makedirs(mountpoint, exist_ok=True)
    fs = TurboFS(pool)
    FUSE(
        fs,
        mountpoint,
        foreground=foreground,
        nothreads=False,
        allow_other=False,
        # Performance options
        big_writes=True,
        max_read=1048576,    # 1 MB
        max_write=1048576,   # 1 MB
    )
