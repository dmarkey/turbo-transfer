"""Thread-safe RPC connection pool for FUSE client → file server communication."""

from __future__ import annotations

import errno
import queue
import socket
import threading

from .protocol import (
    Flags,
    FsOp,
    ProtocolError,
    decode_header,
    decode_payload,
    encode_request,
    recv_message,
    HEADER_SIZE,
)

SOCKET_BUF_SIZE = 4 * 1024 * 1024  # 4 MB
DEFAULT_PORT = 9876


class RpcError(Exception):
    """Remote filesystem error with errno."""

    def __init__(self, err: int, msg: str = ""):
        self.errno = err
        self.msg = msg
        super().__init__(f"[errno {err}] {msg}")


class RpcConnection:
    """A single RPC connection to the file server. NOT thread-safe — use one per thread."""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._lock = threading.Lock()
        self._request_id = 0

    def request(self, op: FsOp, payload: dict, compress: bool = False) -> dict:
        """Send request, block until response, return payload dict. Raises RpcError on remote error."""
        with self._lock:
            req_id = self._request_id
            self._request_id += 1

        msg = encode_request(op, req_id, payload, compress=compress)
        self._sock.sendall(msg)

        flags, resp_op, resp_id, resp_payload = recv_message(self._sock)

        if not (flags & Flags.RESPONSE):
            raise ProtocolError("Expected response, got request")

        if flags & Flags.ERROR:
            raise RpcError(resp_payload.get("errno", errno.EIO), resp_payload.get("msg", ""))

        return resp_payload

    def close(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()


class ConnectionPool:
    """Thread-safe pool of RPC connections to a peer."""

    def __init__(self, peer_ip: str, scope_id: str, port: int = DEFAULT_PORT, size: int = 8):
        self._peer_ip = peer_ip
        self._scope_id = scope_id
        self._port = port
        self._pool: queue.Queue[RpcConnection] = queue.Queue(maxsize=size)
        self._size = size
        self._created = 0
        self._create_lock = threading.Lock()

    def _create_connection(self) -> RpcConnection:
        addr_info = socket.getaddrinfo(
            f"{self._peer_ip}%{self._scope_id}",
            self._port,
            socket.AF_INET6,
            socket.SOCK_STREAM,
        )
        if not addr_info:
            raise ConnectionError(f"Cannot resolve {self._peer_ip}%{self._scope_id}")

        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF_SIZE)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF_SIZE)
        except OSError:
            pass
        sock.connect(addr_info[0][4])
        return RpcConnection(sock)

    def get(self) -> RpcConnection:
        """Get a connection from the pool (blocking if needed)."""
        # Try to get an existing idle connection
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            pass

        # Create a new one if under limit
        with self._create_lock:
            if self._created < self._size:
                self._created += 1
                return self._create_connection()

        # All connections busy — wait for one to be returned
        return self._pool.get(timeout=30)

    def put(self, conn: RpcConnection):
        """Return a connection to the pool."""
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            conn.close()

    def close_all(self):
        """Drain and close all connections."""
        while True:
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break


def rpc_call(pool: ConnectionPool, op: FsOp, payload: dict, compress: bool = False) -> dict:
    """Convenience: get connection, make RPC call, return connection to pool."""
    conn = pool.get()
    try:
        result = conn.request(op, payload, compress=compress)
        pool.put(conn)
        return result
    except (ConnectionError, ProtocolError, OSError):
        # Connection broken — don't return it to pool, create fresh on next call
        try:
            conn.close()
        except Exception:
            pass
        with pool._create_lock:
            pool._created -= 1
        raise RpcError(errno.EIO, "Connection lost")
