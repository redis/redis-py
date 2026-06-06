"""Tests for per-read socket_timeout semantics on async connections.

Issue: redis/redis-py#3454 — socket_timeout on async connection should apply
per individual socket read, matching the sync client behavior, rather than to
the entire response.
"""

import asyncio

import pytest

from redis._parsers import _AsyncRESP2Parser, _AsyncRESP3Parser
from redis.asyncio.connection import Connection
from redis.utils import HIREDIS_AVAILABLE

if HIREDIS_AVAILABLE:
    from redis._parsers import _AsyncHiredisParser
    from redis._parsers.hiredis import NOT_ENOUGH_DATA


class SlowChunkStream:
    """Mock StreamReader that returns data one chunk at a time with delays."""

    def __init__(self, chunks, delay_between_chunks):
        self._chunks = list(chunks)
        self._delay = delay_between_chunks
        self._buffer = b""
        self._pos = 0
        self._chunk_index = 0

    def at_eof(self):
        return self._chunk_index >= len(self._chunks) and self._pos >= len(self._buffer)

    async def read(self, _want):
        if self._pos >= len(self._buffer):
            if self._chunk_index >= len(self._chunks):
                return b""
            if self._delay:
                await asyncio.sleep(self._delay)
            self._buffer = self._chunks[self._chunk_index]
            self._chunk_index += 1
            self._pos = 0
        result = self._buffer[self._pos :]
        self._pos += len(result)
        return result

    async def readline(self):
        if self._pos >= len(self._buffer):
            if self._chunk_index >= len(self._chunks):
                return b""
            if self._delay:
                await asyncio.sleep(self._delay)
            self._buffer = self._chunks[self._chunk_index]
            self._chunk_index += 1
            self._pos = 0
        nl = self._buffer.find(b"\n", self._pos)
        if nl < 0:
            result = self._buffer[self._pos :]
            self._pos = len(self._buffer)
            return result
        result = self._buffer[self._pos : nl + 1]
        self._pos = nl + 1
        return result

    async def readexactly(self, length):
        result = bytearray()
        while len(result) < length:
            if self._pos >= len(self._buffer):
                if self._chunk_index >= len(self._chunks):
                    raise asyncio.IncompleteReadError(bytes(result), length)
                if self._delay:
                    await asyncio.sleep(self._delay)
                self._buffer = self._chunks[self._chunk_index]
                self._chunk_index += 1
                self._pos = 0
            take = min(length - len(result), len(self._buffer) - self._pos)
            result.extend(self._buffer[self._pos : self._pos + take])
            self._pos += take
        return bytes(result)


class _DummyEncoder:
    decode_responses = False
    encoding = "utf-8"
    encoding_errors = "strict"

    def decode(self, value):
        if isinstance(value, bytes):
            return value.decode(self.encoding, self.encoding_errors)
        if isinstance(value, list):
            return [self.decode(v) for v in value]
        return value


def _make_resp2_parser(stream, read_size=4096):
    parser = _AsyncRESP2Parser(socket_read_size=read_size)
    parser._stream = stream
    parser._connected = True
    parser.encoder = _DummyEncoder()
    return parser


def _make_resp3_parser(stream, read_size=4096):
    parser = _AsyncRESP3Parser(socket_read_size=read_size)
    parser._stream = stream
    parser._connected = True
    parser.encoder = _DummyEncoder()
    return parser


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_per_read_timeout_allows_slow_multi_chunk_response(factory):
    """
    A response that takes longer than the timeout in total, but where each
    individual socket read completes quickly, must succeed under per-read
    timeout semantics.
    """
    # Bulk string payload split across several chunks with 0.05s delay each.
    payload = b"hello world this is a moderately large bulk string value"
    chunks = [
        b"$" + str(len(payload)).encode() + b"\r\n",
        payload[:10],
        payload[10:25],
        payload[25:40],
        payload[40:] + b"\r\n",
    ]
    stream = SlowChunkStream(chunks, delay_between_chunks=0.05)
    parser = factory(stream)

    # Total elapsed will be ~0.2s, but each read is only 0.05s.
    # With per-read semantics a 0.1s timeout should allow it.
    response = await parser.read_response(timeout=0.1)
    assert response == payload.decode()


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_per_read_timeout_fails_when_single_read_exceeds_timeout(factory):
    """
    If an individual socket read itself exceeds the timeout, the parser must
    raise a timeout error.
    """
    chunks = [b"$5\r\n", b"hello", b"\r\n"]
    # 0.3s delay per chunk means the second read will exceed a 0.1s timeout.
    stream = SlowChunkStream(chunks, delay_between_chunks=0.3)
    parser = factory(stream)

    with pytest.raises(asyncio.TimeoutError):
        await parser.read_response(timeout=0.1)


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_per_read_timeout_propagates_through_nested_arrays(factory):
    """
    Nested RESP arrays must keep the per-read timeout on every recursive
    _readline/_read call.
    """
    # *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n split into many chunks
    chunks = [
        b"*2\r\n",
        b"$5\r\n",
        b"hello",
        b"\r\n$5\r\n",
        b"world",
        b"\r\n",
    ]
    stream = SlowChunkStream(chunks, delay_between_chunks=0.04)
    parser = factory(stream)

    # Total ~0.24s but each read 0.04s; 0.1s per-read timeout should pass.
    response = await parser.read_response(timeout=0.1)
    assert response == ["hello", "world"]


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_no_timeout_when_sentinel_default(factory):
    """When no timeout is supplied (SENTINEL default), reads must not time out."""
    chunks = [b"+OK\r\n"]
    stream = SlowChunkStream(chunks, delay_between_chunks=0.1)
    parser = factory(stream)

    from redis.utils import SENTINEL

    response = await parser.read_response(timeout=SENTINEL)
    assert response == "OK"


@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
async def test_hiredis_per_read_timeout_allows_slow_multi_chunk_response():
    """
    The hiredis async parser must also apply timeout per read_from_socket call,
    not across the entire read_response loop.
    """
    import hiredis

    payload = b"hello world this is a moderately large bulk string value"
    chunks = [
        b"$" + str(len(payload)).encode() + b"\r\n",
        payload[:10],
        payload[10:25],
        payload[25:40],
        payload[40:] + b"\r\n",
    ]
    stream = SlowChunkStream(chunks, delay_between_chunks=0.05)

    parser = _AsyncHiredisParser(socket_read_size=4096)
    parser._stream = stream
    parser._connected = True
    parser._reader = hiredis.Reader(
        protocolError=Exception,
        replyError=Exception,
        notEnoughData=NOT_ENOUGH_DATA,
    )

    response = await parser.read_response(timeout=0.1)
    assert response == payload


@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
async def test_hiredis_per_read_timeout_fails_when_chunk_too_slow():
    """Hiredis parser must raise when a single read_from_socket exceeds timeout."""
    import hiredis

    chunks = [b"$5\r\n", b"hello", b"\r\n"]
    stream = SlowChunkStream(chunks, delay_between_chunks=0.3)

    parser = _AsyncHiredisParser(socket_read_size=4096)
    parser._stream = stream
    parser._connected = True
    parser._reader = hiredis.Reader(
        protocolError=Exception,
        replyError=Exception,
        notEnoughData=NOT_ENOUGH_DATA,
    )

    with pytest.raises(asyncio.TimeoutError):
        await parser.read_response(timeout=0.1)


@pytest.mark.parametrize("protocol", [2, 3])
async def test_connection_passes_timeout_to_parser(protocol):
    """
    Connection.read_response must pass its timeout through to the parser
    rather than wrapping the parser call in an outer timeout context.
    """
    conn = Connection(protocol=protocol, socket_timeout=0.05)
    # Ensure parser is the Python-backed one for this test.
    from redis._parsers import _AsyncRESP2Parser, _AsyncRESP3Parser

    expected_class = _AsyncRESP2Parser if protocol == 2 else _AsyncRESP3Parser
    assert isinstance(conn._parser, expected_class)

    # Patch the parser to record the timeout it receives.
    recorded = {}

    async def fake_read_response(*args, **kwargs):
        recorded["timeout"] = kwargs.get("timeout")
        return "OK"

    conn._parser.read_response = fake_read_response
    response = await conn.read_response(timeout=0.42)
    assert response == "OK"
    assert recorded["timeout"] == 0.42
