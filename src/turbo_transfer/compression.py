"""LZ4 compression wrapper for Turbo Transfer."""

from __future__ import annotations

import lz4.frame


def compress(data: bytes) -> bytes:
    return lz4.frame.compress(data, compression_level=0)  # fastest


def decompress(data: bytes) -> bytes:
    return lz4.frame.decompress(data)
