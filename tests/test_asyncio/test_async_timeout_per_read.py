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


class HookedStream:
    """Mock StreamReader that fires a hook after every socket read.

    Used to simulate the maintenance-notification machinery pushing a new
    per-read timeout onto the parser while a read_response is in progress.
    """

    def __init__(self, chunks_with_delays, on_read):
        self._steps = list(chunks_with_delays)
        self._on_read = on_read
        self._buffer = b""
        self._pos = 0

    def at_eof(self):
        return not self._steps and self._pos >= len(self._buffer)

    async def read(self, _want):
        if self._pos >= len(self._buffer):
            if not self._steps:
                return b""
            delay, data = self._steps.pop(0)
            if delay:
                await asyncio.sleep(delay)
            self._buffer = data
            self._pos = 0
            self._on_read()
        result = self._buffer[self._pos :]
        self._pos += len(result)
        return result


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
    Connection.read_response must hand the socket_timeout fallback to the
    parser per read rather than wrapping the parser call in an outer timeout
    context, while explicit caller timeouts keep their single-deadline
    wrapper.
    """
    conn = Connection(protocol=protocol, socket_timeout=0.05)
    assert conn._parser is not None

    # Patch the parser to record the timeout it receives.
    recorded = {}

    async def fake_read_response(*args, **kwargs):
        recorded["timeout"] = kwargs.get("timeout")
        return "OK"

    conn._parser.read_response = fake_read_response

    # timeout=None falls back to socket_timeout: the parser gets the
    # per-read window.
    response = await conn.read_response(timeout=None)
    assert response == "OK"
    assert recorded["timeout"] == 0.05

    # An explicit caller timeout keeps the whole-response wrapper, so the
    # parser receives no timeout of its own.
    recorded["timeout"] = "unset"
    response = await conn.read_response(timeout=0.42)
    assert response == "OK"
    assert recorded["timeout"] is None


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_relaxed_timeout_reaches_reads_started_after_update(factory):
    """
    When the maintenance machinery relaxes the per-read timeout while a
    read_response is in progress, socket reads that START after the update
    must use the relaxed deadline, not the one captured at entry (#4177).
    """
    payload = b"x" * 50
    holder = {}

    def relax():
        holder["parser"]._socket_timeout = 0.5

    steps = [(0, b"$50\r\n"), (0, payload[:10]), (0.2, payload[10:] + b"\r\n")]
    stream = HookedStream(steps, on_read=relax)
    parser = factory(stream)
    holder["parser"] = parser

    # The entry timeout (0.05) would abort the 0.2s final read; the relaxed
    # 0.5 deadline pushed after the first read must cover it instead.
    response = await parser.read_response(timeout=0.05)
    assert response == payload.decode()


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_stored_timeout_overrides_timeout_captured_at_entry(factory):
    """
    The stored maintenance timeout wins over the timeout argument captured
    when read_response was entered, in both directions.
    """
    payload = b"x" * 50
    holder = {}

    def tighten():
        holder["parser"]._socket_timeout = 0.05

    steps = [(0, b"$50\r\n"), (0, payload[:10]), (0.2, payload[10:] + b"\r\n")]
    stream = HookedStream(steps, on_read=tighten)
    parser = factory(stream)
    holder["parser"] = parser

    # Entry timeout is a generous 0.5s, but the tightened 0.05 deadline
    # pushed after the first read must abort the 0.2s final read.
    with pytest.raises(asyncio.TimeoutError):
        await parser.read_response(timeout=0.5)


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_stored_timeout_none_makes_later_reads_block(factory):
    """A stored None (relaxed with no deadline) makes later reads block."""
    payload = b"x" * 50
    holder = {}

    def relax_to_blocking():
        holder["parser"]._socket_timeout = None

    steps = [(0, b"$50\r\n"), (0, payload[:10]), (0.2, payload[10:] + b"\r\n")]
    stream = HookedStream(steps, on_read=relax_to_blocking)
    parser = factory(stream)
    holder["parser"] = parser

    response = await parser.read_response(timeout=0.05)
    assert response == payload.decode()


@pytest.mark.parametrize(
    "parser_class, protocol",
    [(_AsyncRESP2Parser, 2), (_AsyncRESP3Parser, 3)],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_restore_bounds_in_flight_blocking_read(parser_class, protocol):
    """
    A maintenance relax stores None (blocking) on the parser. A read that
    starts relaxed must still expose its in-flight timeout context so a
    later restore can bound it; otherwise the Python parser hangs past the
    restored socket_timeout until the server sends data. The hiredis parser
    already wraps blocking reads for this reason.
    """
    never = asyncio.Event()

    class HangingStream:
        _buffer = b""

        def at_eof(self):
            return False

        async def read(self, _want):
            await never.wait()
            return b""

    conn = Connection(protocol=protocol, socket_timeout=0.05, parser_class=parser_class)
    parser = conn._parser
    parser._stream = HangingStream()
    parser._connected = True
    parser.encoder = _DummyEncoder()
    # The maintenance machinery relaxed this connection to blocking before
    # the read started.
    parser._socket_timeout = None

    task = asyncio.create_task(parser.read_response(timeout=0.05))
    try:
        for _ in range(100):
            if parser._active_read_timeout is not None:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("blocking read never exposed an in-flight timeout context")

        # Maintenance ends: restore the connection socket_timeout. The
        # in-flight relaxed read must be retightened and abort.
        conn.update_current_socket_timeout(-1)
        done, _pending = await asyncio.wait({task}, timeout=2.0)
        assert task in done, "restored socket_timeout did not bound the in-flight read"
        with pytest.raises(asyncio.TimeoutError):
            task.result()
    finally:
        never.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("protocol", [2, 3])
async def test_update_current_socket_timeout_pushes_to_parser(protocol):
    """
    Connection.update_current_socket_timeout must push the new deadline
    onto the parser so reads started after the update pick it up, matching
    the sync client's update_parser_timeout.
    """
    conn = Connection(protocol=protocol, socket_timeout=0.05)
    parser = conn._parser

    # Relax: parser sees the relaxed deadline.
    conn.update_current_socket_timeout(0.2)
    assert parser._socket_timeout == 0.2

    # Relax to blocking: parser reads must block.
    conn.update_current_socket_timeout(None)
    assert parser._socket_timeout is None

    # Restore: -1 resolves to the connection's current socket_timeout.
    conn.update_current_socket_timeout(-1)
    assert parser._socket_timeout == 0.05


@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
async def test_hiredis_relaxed_timeout_reaches_reads_started_after_update():
    """The hiredis parser must also honor a relaxed deadline on later reads."""
    import hiredis

    payload = b"x" * 50
    holder = {}

    def relax():
        holder["parser"]._socket_timeout = 0.5

    steps = [(0, b"$50\r\n"), (0, payload[:10]), (0.2, payload[10:] + b"\r\n")]
    stream = HookedStream(steps, on_read=relax)

    parser = _AsyncHiredisParser(socket_read_size=4096)
    parser._stream = stream
    parser._connected = True
    parser._reader = hiredis.Reader(
        protocolError=Exception,
        replyError=Exception,
        notEnoughData=NOT_ENOUGH_DATA,
    )
    holder["parser"] = parser

    response = await parser.read_response(timeout=0.05)
    assert response == payload


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_readline_terminator_straddling_chunks(factory):
    """A '\\r\\n' split across two socket reads must still terminate the line."""
    stream = SlowChunkStream([b"+OK\r", b"\n"], delay_between_chunks=0)
    parser = factory(stream)
    assert await parser.read_response() == "OK"


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_bulk_read_across_many_small_chunks(factory):
    """
    Bulk replies arriving in many small chunks must parse correctly while
    the parser accumulates chunks without repeatedly copying the buffer.
    """
    payload = bytes(65 + (i % 26) for i in range(4096))
    chunks = (
        [b"$4096\r\n"]
        + [payload[i : i + 64] for i in range(0, len(payload), 64)]
        + [b"\r\n"]
    )
    stream = SlowChunkStream(chunks, delay_between_chunks=0)
    parser = factory(stream, read_size=64)
    assert await parser.read_response() == payload.decode()


class StallStream:
    """Mock StreamReader that blocks forever once its queued chunks run out.

    Simulates a server that stops sending mid-reply: the queued chunks are
    delivered, then every read stalls until the per-read timeout cancels it.
    """

    def __init__(self, chunks):
        self._chunks = list(chunks)
        self._buffer = b""
        self._pos = 0

    def feed(self, chunk):
        self._chunks.append(chunk)

    def at_eof(self):
        return not self._chunks and self._pos >= len(self._buffer)

    async def read(self, _want):
        if self._pos >= len(self._buffer):
            if not self._chunks:
                await asyncio.sleep(60)
                return b""
            self._buffer = self._chunks.pop(0)
            self._pos = 0
        result = self._buffer[self._pos :]
        self._pos += len(result)
        return result


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_timeout_preserves_partially_consumed_bulk(factory):
    """
    A per-read timeout firing mid-payload must not drop the bytes already
    consumed from the stream. Explicit caller timeouts make read_response
    return None without disconnecting, so a retry on the same connection
    must reparse the complete reply instead of desynchronizing.
    """
    payload = b"helloworld"
    stream = StallStream([b"$10\r\n", payload[:5]])
    parser = factory(stream)

    with pytest.raises(asyncio.TimeoutError):
        await parser.read_response(timeout=0.05)

    stream.feed(payload[5:] + b"\r\n")
    assert await parser.read_response(timeout=1) == payload.decode()


@pytest.mark.parametrize(
    "factory",
    [_make_resp2_parser, _make_resp3_parser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser"],
)
async def test_timeout_preserves_partially_consumed_line(factory):
    """Same preservation contract for a line split across the timeout."""
    stream = StallStream([b"+hel", b"lo"])
    parser = factory(stream)

    with pytest.raises(asyncio.TimeoutError):
        await parser.read_response(timeout=0.05)

    stream.feed(b" world\r\n")
    assert await parser.read_response(timeout=1) == "hello world"
