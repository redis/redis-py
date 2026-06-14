"""Tests for the RESP parser recursion depth guard (issue #4116).

A malicious or misbehaving server can send deeply nested aggregate replies
that exhaust the Python call stack.  The parsers now bound recursion depth
via ``_MAX_PARSE_DEPTH`` to prevent this.
"""

from unittest.mock import MagicMock

import pytest

from redis._parsers.resp2 import _MAX_PARSE_DEPTH as _MAX_DEPTH_RESP2
from redis._parsers.resp2 import _RESP2Parser
from redis._parsers.resp3 import _MAX_PARSE_DEPTH as _MAX_DEPTH_RESP3
from redis._parsers.resp3 import _RESP3Parser
from redis._parsers.socket import SocketBuffer
from redis.exceptions import InvalidResponse


class _FakeSocket:
    """Minimal socket stand-in that yields pre-built RESP bytes."""

    def __init__(self, payload: bytes):
        self._payload = payload

    def recv(self, n: int) -> bytes:
        if not self._payload:
            return b""
        chunk = self._payload[:n]
        self._payload = self._payload[n:]
        return chunk

    def settimeout(self, _timeout: float) -> None:
        pass


def _make_buf(payload: bytes) -> SocketBuffer:
    """Create a SocketBuffer backed by a fake socket with the given payload."""
    return SocketBuffer(
        _FakeSocket(payload), socket_read_size=65536, socket_timeout=5.0
    )


def _make_parser(cls, payload: bytes):
    """Create a parser with buffer and encoder wired up."""
    parser = cls(65536)
    parser._buffer = _make_buf(payload)
    # Wire up a mock encoder so decode doesn't fail
    parser.encoder = MagicMock()
    parser.encoder.decode = lambda x: x.decode("utf-8") if isinstance(x, bytes) else x
    return parser


def _make_nested_arrays(depth: int) -> bytes:
    """Build ``depth`` nested single-element RESP arrays terminating in +OK."""
    return (b"*1\r\n" * depth) + b"+OK\r\n"


class TestRESP2DepthGuard:
    """Verify _RESP2Parser rejects excessively nested aggregate replies."""

    def test_depth_exceeded_raises(self):
        """Exceeding _MAX_PARSE_DEPTH raises InvalidResponse."""
        depth = _MAX_DEPTH_RESP2 + 5
        parser = _make_parser(_RESP2Parser, _make_nested_arrays(depth))

        with pytest.raises(InvalidResponse, match="nesting depth"):
            parser._read_response()

    def test_depth_exactly_at_limit_raises(self):
        """Responses nested exactly at the limit still raise."""
        depth = _MAX_DEPTH_RESP2 + 1
        parser = _make_parser(_RESP2Parser, _make_nested_arrays(depth))

        with pytest.raises(InvalidResponse, match="nesting depth"):
            parser._read_response()

    def test_shallow_nesting_accepted(self):
        """A shallow nested response (within limit) parses without error."""
        depth = 10
        parser = _make_parser(_RESP2Parser, _make_nested_arrays(depth))

        result = parser._read_response()
        for _ in range(depth):
            assert isinstance(result, list)
            result = result[0]
        assert result == "OK"

    def test_depth_guard_message_includes_limit(self):
        """The error message includes the configured depth limit."""
        depth = _MAX_DEPTH_RESP2 + 1
        parser = _make_parser(_RESP2Parser, _make_nested_arrays(depth))

        with pytest.raises(InvalidResponse, match=str(_MAX_DEPTH_RESP2)):
            parser._read_response()


class TestRESP3DepthGuard:
    """Verify _RESP3Parser rejects excessively nested aggregate replies."""

    def test_depth_exceeded_raises(self):
        """Exceeding _MAX_PARSE_DEPTH raises InvalidResponse."""
        depth = _MAX_DEPTH_RESP3 + 5
        parser = _make_parser(_RESP3Parser, _make_nested_arrays(depth))

        with pytest.raises(InvalidResponse, match="nesting depth"):
            parser._read_response()

    def test_shallow_nesting_accepted(self):
        """A shallow nested response (within limit) parses without error."""
        depth = 10
        parser = _make_parser(_RESP3Parser, _make_nested_arrays(depth))

        result = parser._read_response()
        for _ in range(depth):
            assert isinstance(result, list)
            result = result[0]
        assert result == "OK"

    def test_depth_guard_message_includes_limit(self):
        """The error message includes the configured depth limit."""
        depth = _MAX_DEPTH_RESP3 + 1
        parser = _make_parser(_RESP3Parser, _make_nested_arrays(depth))

        with pytest.raises(InvalidResponse, match=str(_MAX_DEPTH_RESP3)):
            parser._read_response()
