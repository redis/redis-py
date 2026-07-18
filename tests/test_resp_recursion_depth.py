"""
Tests for recursion depth limits in RESP2 and RESP3 parsers.

Regression tests for https://github.com/redis/redis-py/issues/4116
Ensures deeply nested responses raise InvalidResponse instead of RecursionError.
"""
from unittest import mock

import pytest

from redis._parsers.resp2 import _RESP2Parser, _AsyncRESP2Parser
from redis._parsers.resp3 import _RESP3Parser, _AsyncRESP3Parser
from redis.exceptions import InvalidResponse


def _make_sync_resp2_parser():
    parser = _RESP2Parser.__new__(_RESP2Parser)
    parser._buffer = mock.MagicMock()
    parser.encoder = mock.MagicMock()
    parser.encoder.decode = lambda x: x.decode("utf-8", errors="replace") if isinstance(x, bytes) else x
    return parser


def _make_sync_resp3_parser():
    parser = _RESP3Parser.__new__(_RESP3Parser)
    parser._buffer = mock.MagicMock()
    parser.encoder = mock.MagicMock()
    parser.encoder.decode = lambda x: x.decode("utf-8", errors="replace") if isinstance(x, bytes) else x
    parser.pubsub_push_handler_func = lambda x: x
    parser.node_moving_push_handler_func = None
    parser.maintenance_push_handler_func = None
    parser.oss_cluster_maint_push_handler_func = None
    parser.invalidation_push_handler_func = None
    return parser


class TestRESP2SyncDepthLimit:
    def test_deeply_nested_array_raises_invalid_response(self):
        """Malicious server sending deeply nested *1\\r\\n arrays."""
        depth = 600  # exceeds _MAX_RESP_DEPTH = 512
        lines = [b"*1\r\n"] * depth + [b"+OK\r\n"]

        parser = _make_sync_resp2_parser()
        call_count = [0]

        def mock_readline(timeout=None):
            idx = call_count[0]
            call_count[0] += 1
            return lines[idx]

        parser._buffer.readline = mock_readline

        with pytest.raises(InvalidResponse, match="nesting depth exceeds"):
            parser.read_response()

    def test_shallow_array_parses_ok(self):
        """Legitimate shallow arrays should parse normally."""
        parser = _make_sync_resp2_parser()
        # readline returns without \r\n (transport strips it)
        lines = [b"*2\r\n", b"+foo", b"+bar"]

        call_count = [0]

        def mock_readline(timeout=None):
            idx = call_count[0]
            call_count[0] += 1
            return lines[idx]

        parser._buffer.readline = mock_readline
        result = parser.read_response()
        assert result == ["foo", "bar"]


class TestRESP3SyncDepthLimit:
    def test_deeply_nested_array_raises_invalid_response(self):
        """RESP3 deeply nested array."""
        depth = 600
        lines = [b"*1\r\n"] * depth + [b"+OK\r\n"]

        parser = _make_sync_resp3_parser()
        call_count = [0]

        def mock_readline(timeout=None):
            idx = call_count[0]
            call_count[0] += 1
            return lines[idx]

        parser._buffer.readline = mock_readline

        with pytest.raises(InvalidResponse, match="nesting depth exceeds"):
            parser.read_response()

    def test_deeply_nested_set_raises_invalid_response(self):
        """RESP3 deeply nested set (~1\\r\\n)."""
        depth = 600
        lines = [b"~1\r\n"] * depth + [b"+OK\r\n"]

        parser = _make_sync_resp3_parser()
        call_count = [0]

        def mock_readline(timeout=None):
            idx = call_count[0]
            call_count[0] += 1
            return lines[idx]

        parser._buffer.readline = mock_readline

        with pytest.raises(InvalidResponse, match="nesting depth exceeds"):
            parser.read_response()

    def test_deeply_nested_map_raises_invalid_response(self):
        """RESP3 deeply nested map (%1\\r\\n key val).

        Each map level consumes 3 reads: the %1\\r\\n header, one recursive
        key read, and one recursive val read.  For N nesting levels we need
        N header lines + 1 leaf key + N val lines = 2N + 1 total lines.
        """
        depth = 550  # well above _MAX_RESP_DEPTH = 512
        lines = [b"%1\r\n"] * depth + [b"+k"] + [b"+v"] * depth

        parser = _make_sync_resp3_parser()
        call_count = [0]

        def mock_readline(timeout=None):
            idx = call_count[0]
            call_count[0] += 1
            return lines[idx]

        parser._buffer.readline = mock_readline

        with pytest.raises(InvalidResponse, match="nesting depth exceeds"):
            parser.read_response()


@pytest.mark.asyncio
class TestRESP2AsyncDepthLimit:
    async def test_deeply_nested_array_raises_invalid_response(self):
        """Async RESP2 deeply nested array."""
        depth = 600
        lines = [b"*1\r\n"] * depth + [b"+OK"]

        parser = _AsyncRESP2Parser.__new__(_AsyncRESP2Parser)
        parser._stream = mock.MagicMock()
        parser.encoder = mock.MagicMock()
        parser.encoder.decode = lambda x: x.decode("utf-8", errors="replace") if isinstance(x, bytes) else x
        parser._stream_closed = False

        call_count = [0]

        async def mock_readline():
            idx = call_count[0]
            call_count[0] += 1
            return lines[idx]

        parser._readline = mock_readline

        with pytest.raises(InvalidResponse, match="nesting depth exceeds"):
            await parser._read_response()


@pytest.mark.asyncio
class TestRESP3AsyncDepthLimit:
    async def test_deeply_nested_array_raises_invalid_response(self):
        """Async RESP3 deeply nested array."""
        depth = 600
        lines = [b"*1\r\n"] * depth + [b"+OK"]

        parser = _AsyncRESP3Parser.__new__(_AsyncRESP3Parser)
        parser._stream = mock.MagicMock()
        parser.encoder = mock.MagicMock()
        parser.encoder.decode = lambda x: x.decode("utf-8", errors="replace") if isinstance(x, bytes) else x
        parser._stream_closed = False
        parser.pubsub_push_handler_func = mock.AsyncMock(return_value=None)
        parser.invalidation_push_handler_func = None

        call_count = [0]

        async def mock_readline():
            idx = call_count[0]
            call_count[0] += 1
            return lines[idx]

        parser._readline = mock_readline

        with pytest.raises(InvalidResponse, match="nesting depth exceeds"):
            await parser._read_response()
