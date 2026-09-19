"""
Tests for the recursion depth guard in RESP parsers.

Verifies that deeply nested aggregate replies raise InvalidResponse
instead of RecursionError, preventing client-side DoS (issue #4116).
"""

import pytest

from redis._parsers.resp2 import _AsyncRESP2Parser, _RESP2Parser
from redis._parsers.resp3 import _AsyncRESP3Parser, _RESP3Parser
from redis._parsers.socket import SocketBuffer
from redis.exceptions import InvalidResponse


class FakeSocket:
    """Minimal socket-like object for feeding raw bytes to SocketBuffer."""

    def __init__(self, payload: bytes):
        self.payload = payload

    def recv(self, n: int) -> bytes:
        if not self.payload:
            return b""
        chunk = self.payload[:n]
        self.payload = self.payload[n:]
        return chunk

    def settimeout(self, _timeout: float) -> None:
        pass


def make_deeply_nested_payload(depth: int) -> bytes:
    """Build a RESP2 payload of `depth` nested arrays terminated by +OK."""
    return (b"*1\r\n" * depth) + b"+OK\r\n"


class TestRESP2RecursionDepthGuard:
    """Test that _RESP2Parser raises InvalidResponse on deeply nested replies."""

    def test_normal_nesting_succeeds(self):
        """A modestly nested response should parse without error."""
        depth = 10
        payload = make_deeply_nested_payload(depth)
        parser = _RESP2Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        result = parser._read_response(disable_decoding=True)
        # The result is a deeply nested list containing "OK"
        current = result
        for _ in range(depth):
            assert isinstance(current, list)
            assert len(current) == 1
            current = current[0]
        assert current == b"OK"

    def test_depth_at_limit_succeeds(self):
        """A response exactly at MAX_NESTING_DEPTH should still parse."""
        depth = _RESP2Parser.MAX_NESTING_DEPTH
        payload = make_deeply_nested_payload(depth)
        parser = _RESP2Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        result = parser._read_response(disable_decoding=True)
        current = result
        for _ in range(depth):
            assert isinstance(current, list)
            current = current[0]
        assert current == b"OK"

    def test_depth_exceeding_limit_raises(self):
        """A response exceeding MAX_NESTING_DEPTH must raise InvalidResponse."""
        depth = _RESP2Parser.MAX_NESTING_DEPTH + 1
        payload = make_deeply_nested_payload(depth)
        parser = _RESP2Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        with pytest.raises(InvalidResponse, match="nesting depth exceeded"):
            parser._read_response(disable_decoding=True)

    def test_depth_much_beyond_limit_raises(self):
        """A massively nested payload must raise InvalidResponse, not RecursionError."""
        depth = 5000
        payload = make_deeply_nested_payload(depth)
        parser = _RESP2Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        with pytest.raises(InvalidResponse, match="nesting depth exceeded"):
            parser._read_response(disable_decoding=True)


class TestRESP3RecursionDepthGuard:
    """Test that _RESP3Parser raises InvalidResponse on deeply nested replies."""

    def _make_resp3_deeply_nested_array(self, depth: int) -> bytes:
        """Build a RESP3 payload of `depth` nested arrays terminated by +OK."""
        return (b"*1\r\n" * depth) + b"+OK\r\n"

    def test_resp3_normal_nesting_succeeds(self):
        depth = 10
        payload = self._make_resp3_deeply_nested_array(depth)
        parser = _RESP3Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        result = parser._read_response(disable_decoding=True)
        current = result
        for _ in range(depth):
            assert isinstance(current, list)
            assert len(current) == 1
            current = current[0]
        assert current == b"OK"

    def test_resp3_depth_exceeding_limit_raises(self):
        depth = _RESP3Parser.MAX_NESTING_DEPTH + 1
        payload = self._make_resp3_deeply_nested_array(depth)
        parser = _RESP3Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        with pytest.raises(InvalidResponse, match="nesting depth exceeded"):
            parser._read_response(disable_decoding=True)

    def test_resp3_deeply_nested_set_raises(self):
        """RESP3 set type (~) should also be guarded."""
        depth = _RESP3Parser.MAX_NESTING_DEPTH + 1
        payload = (b"~1\r\n" * depth) + b"+OK\r\n"
        parser = _RESP3Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        with pytest.raises(InvalidResponse, match="nesting depth exceeded"):
            parser._read_response(disable_decoding=True)

    def test_resp3_deeply_nested_map_raises(self):
        """RESP3 map type (%) should also be guarded."""
        # A map needs key+value, so nesting via map entries
        # Each map has 1 entry: key is a simple string, value is another map
        depth = _RESP3Parser.MAX_NESTING_DEPTH + 1
        # Build nested maps: each %1\r\n$1\r\nk\r\n -> next level
        payload = (b"%1\r\n$1\r\nk\r\n" * depth) + b"+OK\r\n"
        parser = _RESP3Parser(65536)
        parser._buffer = SocketBuffer(
            FakeSocket(payload), socket_read_size=65536, socket_timeout=1
        )
        with pytest.raises(InvalidResponse, match="nesting depth exceeded"):
            parser._read_response(disable_decoding=True)


@pytest.mark.asyncio
class TestAsyncRESP2RecursionDepthGuard:
    """Test that _AsyncRESP2Parser raises InvalidResponse on deeply nested replies."""

    def _build_async_parser(self, payload: bytes) -> _AsyncRESP2Parser:
        """Build an async parser with a mock stream."""
        import asyncio

        parser = _AsyncRESP2Parser(65536)

        class MockStreamReader:
            def __init__(self, data: bytes):
                self._data = data
                self._pos = 0

            async def readline(self) -> bytes:
                end = self._data.find(b"\r\n", self._pos)
                if end < 0:
                    result = self._data[self._pos :]
                    self._pos = len(self._data)
                    return result + b"\r\n"
                result = self._data[self._pos : end + 2]
                self._pos = end + 2
                return result

            async def readexactly(self, n: int) -> bytes:
                result = self._data[self._pos : self._pos + n]
                self._pos += n
                return result

            @property
            def _buffer(self):
                return self._data[self._pos :]

            def at_eof(self) -> bool:
                return self._pos >= len(self._data)

        parser._stream = MockStreamReader(payload)
        parser.encoder = None  # Not needed for disable_decoding=True
        parser._connected = True
        return parser

    @pytest.mark.asyncio
    async def test_async_normal_nesting_succeeds(self):
        depth = 10
        payload = make_deeply_nested_payload(depth)
        parser = self._build_async_parser(payload)
        result = await parser._read_response(disable_decoding=True)
        current = result
        for _ in range(depth):
            assert isinstance(current, list)
            assert len(current) == 1
            current = current[0]
        assert current == b"OK"

    @pytest.mark.asyncio
    async def test_async_depth_exceeding_limit_raises(self):
        depth = _AsyncRESP2Parser.MAX_NESTING_DEPTH + 1
        payload = make_deeply_nested_payload(depth)
        parser = self._build_async_parser(payload)
        with pytest.raises(InvalidResponse, match="nesting depth exceeded"):
            await parser._read_response(disable_decoding=True)


@pytest.mark.asyncio
class TestAsyncRESP3RecursionDepthGuard:
    """Test that _AsyncRESP3Parser raises InvalidResponse on deeply nested replies."""

    def _build_async_parser(self, payload: bytes) -> _AsyncRESP3Parser:
        parser = _AsyncRESP3Parser(65536)

        class MockStreamReader:
            def __init__(self, data: bytes):
                self._data = data
                self._pos = 0

            async def readline(self) -> bytes:
                end = self._data.find(b"\r\n", self._pos)
                if end < 0:
                    result = self._data[self._pos :]
                    self._pos = len(self._data)
                    return result + b"\r\n"
                result = self._data[self._pos : end + 2]
                self._pos = end + 2
                return result

            async def readexactly(self, n: int) -> bytes:
                result = self._data[self._pos : self._pos + n]
                self._pos += n
                return result

            @property
            def _buffer(self):
                return self._data[self._pos :]

            def at_eof(self) -> bool:
                return self._pos >= len(self._data)

        parser._stream = MockStreamReader(payload)
        parser.encoder = type("FakeEncoder", (), {"decode": lambda self, x: x})()
        parser._connected = True
        return parser

    @pytest.mark.asyncio
    async def test_async_resp3_normal_nesting_succeeds(self):
        depth = 10
        payload = make_deeply_nested_payload(depth)
        parser = self._build_async_parser(payload)
        result = await parser._read_response(disable_decoding=True)
        current = result
        for _ in range(depth):
            assert isinstance(current, list)
            assert len(current) == 1
            current = current[0]
        assert current == b"OK"

    @pytest.mark.asyncio
    async def test_async_resp3_depth_exceeding_limit_raises(self):
        depth = _AsyncRESP3Parser.MAX_NESTING_DEPTH + 1
        payload = make_deeply_nested_payload(depth)
        parser = self._build_async_parser(payload)
        with pytest.raises(InvalidResponse, match="nesting depth exceeded"):
            await parser._read_response(disable_decoding=True)
