"""Caching layer for FUSE operations."""

from __future__ import annotations

import threading
import time
from cachetools import TTLCache


class AttrCache:
    """Thread-safe attribute cache with TTL."""

    def __init__(self, ttl: float = 1.0, maxsize: int = 10000):
        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self._lock = threading.Lock()

    def get(self, path: str) -> dict | None:
        with self._lock:
            return self._cache.get(path)

    def put(self, path: str, attrs: dict):
        with self._lock:
            self._cache[path] = attrs

    def invalidate(self, path: str):
        with self._lock:
            self._cache.pop(path, None)

    def invalidate_prefix(self, prefix: str):
        """Invalidate all entries under a directory."""
        with self._lock:
            keys = [k for k in self._cache if k.startswith(prefix)]
            for k in keys:
                del self._cache[k]

    def clear(self):
        with self._lock:
            self._cache.clear()


class DirCache:
    """Thread-safe directory listing cache with TTL."""

    def __init__(self, ttl: float = 2.0, maxsize: int = 1000):
        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self._lock = threading.Lock()

    def get(self, path: str) -> list[dict] | None:
        with self._lock:
            return self._cache.get(path)

    def put(self, path: str, entries: list[dict]):
        with self._lock:
            self._cache[path] = entries

    def invalidate(self, path: str):
        with self._lock:
            self._cache.pop(path, None)

    def clear(self):
        with self._lock:
            self._cache.clear()
