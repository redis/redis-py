"""socket_timeout applies per socket read in the async client (#3454).

The sync client applies socket_timeout to every individual recv(), so a
large response whose chunks keep arriving within the timeout does not
fail. The async client used to wrap the whole read_response() in a single
deadline, timing out slow-but-healthy responses whose total transfer time
exceeded socket_timeout. These tests drive a real Connection and parser
over an in-memory StreamReader, so no server is needed.
"""

import asyncio
import math
import time
from unittest import mock

import pytest

from redis._parsers.hiredis import _AsyncHiredisParser
from redis._parsers.resp2 import _AsyncRESP2Parser
from redis._parsers.resp3 import _AsyncRESP3Parser
from redis.asyncio.connection import Connection
from redis.exceptions import TimeoutError
from redis.utils import HIREDIS_AVAILABLE

SOCKET_TIMEOUT = 0.3
CHUNK_GAP = 0.08  # every gap between pieces is well below SOCKET_TIMEOUT
READ_SIZE = 16384
PAYLOAD = b"x" * 200_000
BULK_WIRE = b"$%d\r\n%s\r\n" % (len(PAYLOAD), PAYLOAD)

requires_hiredis = pytest.mark.skipif(
    not HIREDIS_AVAILABLE, reason="hiredis is not installed"
)


def make_connection(parser, protocol, socket_timeout=SOCKET_TIMEOUT):
    conn = Connection(
        socket_timeout=socket_timeout,
        socket_read_size=READ_SIZE,
        protocol=protocol,
    )
    conn._parser = parser
    reader = asyncio.StreamReader()
    conn._reader = reader
    conn._writer = mock.Mock()
    parser.on_connect(conn)
    conn.disconnect = mock.AsyncMock()
    return conn, reader


def make_resp2_connection(socket_timeout=SOCKET_TIMEOUT):
    return make_connection(_AsyncRESP2Parser(READ_SIZE), 2, socket_timeout)


def make_resp3_connection(socket_timeout=SOCKET_TIMEOUT):
    return make_connection(_AsyncRESP3Parser(READ_SIZE), 3, socket_timeout)


@requires_hiredis
def make_hiredis_connection(socket_timeout=SOCKET_TIMEOUT):
    return make_connection(_AsyncHiredisParser(READ_SIZE), 2, socket_timeout)


CONNECTION_FACTORIES = [
    pytest.param(make_resp2_connection, id="resp2"),
    pytest.param(make_resp3_connection, id="resp3"),
    pytest.param(
        make_hiredis_connection,
        id="hiredis",
        marks=requires_hiredis,
    ),
]


async def feed_in_pieces(reader, data, piece=READ_SIZE, gap=CHUNK_GAP):
    """Feed data in pieces, waiting `gap` between pieces.

    No single gap exceeds socket_timeout, but the whole transfer takes
    several times longer than socket_timeout.
    """
    for i in range(0, len(data), piece):
        reader.feed_data(data[i : i + piece])
        await asyncio.sleep(gap)
    reader.feed_eof()


@pytest.mark.parametrize("factory", CONNECTION_FACTORIES)
async def test_socket_timeout_is_per_read_not_per_response(factory):
    # Regression test for #3454: a large response delivered in pieces, each
    # arriving promptly, must be read in full even though the total time
    # exceeds socket_timeout. Before the fix, read_response() raised
    # TimeoutError after SOCKET_TIMEOUT seconds.
    conn, reader = factory()
    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE))

    start = time.monotonic()
    response = await conn.read_response()
    elapsed = time.monotonic() - start
    await feeder

    assert response == PAYLOAD
    # prove the transfer really took longer than one socket_timeout window
    assert elapsed > SOCKET_TIMEOUT


@pytest.mark.parametrize("factory", CONNECTION_FACTORIES)
async def test_socket_timeout_still_fires_when_socket_stalls(factory):
    # The per-read deadline must still fire: a response that starts and
    # then stalls for longer than socket_timeout raises TimeoutError.
    conn, reader = factory()
    reader.feed_data(b"$5\r\nhe")

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        await conn.read_response()
    elapsed = time.monotonic() - start

    assert elapsed < SOCKET_TIMEOUT * 4
    conn.disconnect.assert_awaited()


async def test_socket_timeout_per_line_for_aggregate_replies():
    # An aggregate reply made of many small elements exercises the line
    # reads: each element gets a fresh socket_timeout window.
    conn, reader = make_resp3_connection()
    elements = [b"$5\r\nhello\r\n"] * 30
    wire = b"*%d\r\n" % len(elements) + b"".join(elements)
    feeder = asyncio.create_task(
        feed_in_pieces(reader, wire, piece=len(elements[0]) * 2, gap=0.05)
    )

    response = await conn.read_response()
    await feeder

    assert response == [b"hello"] * 30


async def test_maintenance_relax_reaches_reads_of_in_flight_response():
    # Maintenance notifications relax the socket timeout mid-response via
    # update_current_socket_timeout(). With per-read deadlines the relaxed
    # value must reach the reads of the in-flight response: after the
    # relax, a gap longer than the original socket_timeout is tolerated.
    conn, reader = make_resp2_connection(socket_timeout=0.25)
    wire = BULK_WIRE
    reader.feed_data(wire[:100])

    async def feed_rest_late():
        await asyncio.sleep(0.5)  # gap > original socket_timeout of 0.25
        reader.feed_data(wire[100:])
        reader.feed_eof()

    read_task = asyncio.create_task(conn.read_response())
    feeder = asyncio.create_task(feed_rest_late())
    await asyncio.sleep(0.1)  # the response is now in flight
    conn.update_current_socket_timeout(2.0)

    response = await read_task
    await feeder

    assert response == PAYLOAD


async def test_explicit_timeout_still_bounds_the_whole_read():
    # An explicit caller timeout keeps its historical semantics: it bounds
    # the whole read and returns None when it expires, even though the
    # socket never stalls for a full socket_timeout.
    conn, reader = make_resp2_connection()
    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE))

    start = time.monotonic()
    response = await conn.read_response(timeout=SOCKET_TIMEOUT / 2)
    elapsed = time.monotonic() - start
    feeder.cancel()

    assert response is None
    assert elapsed < SOCKET_TIMEOUT * 2
    conn.disconnect.assert_not_awaited()


async def test_blocking_read_ignores_socket_timeout():
    # PubSub-style blocking reads (timeout=math.inf) must not be cut short
    # by socket_timeout, however long the response takes in total.
    conn, reader = make_resp2_connection()
    feeder = asyncio.create_task(feed_in_pieces(reader, BULK_WIRE))

    response = await conn.read_response(timeout=math.inf)
    await feeder

    assert response == PAYLOAD


async def test_send_packed_command_times_out_when_writer_stalls():
    # The write side already applies socket_timeout per send operation: a
    # writer whose drain() stalls raises TimeoutError instead of hanging.
    conn = Connection(socket_timeout=0.2, health_check_interval=0)
    writer = mock.Mock()
    writer.transport.is_closing.return_value = False

    async def stalled_drain():
        await asyncio.sleep(10)

    writer.drain = stalled_drain
    conn._reader = mock.Mock()
    conn._writer = writer
    conn.disconnect = mock.AsyncMock()

    start = time.monotonic()
    with pytest.raises(TimeoutError, match="Timeout writing to socket"):
        await conn.send_packed_command(b"PING", check_health=False)
    elapsed = time.monotonic() - start

    assert elapsed < 2
    conn.disconnect.assert_awaited()


async def test_send_packed_command_within_socket_timeout_succeeds():
    # A slow but responsive writer (drain completes within socket_timeout)
    # must not be timed out.
    conn = Connection(socket_timeout=1, health_check_interval=0)
    writer = mock.Mock()
    writer.transport.is_closing.return_value = False

    async def slow_drain():
        await asyncio.sleep(0.05)

    writer.drain = slow_drain
    conn._reader = mock.Mock()
    conn._writer = writer

    await conn.send_packed_command(b"PING", check_health=False)

    writer.writelines.assert_called_once_with([b"PING"])
