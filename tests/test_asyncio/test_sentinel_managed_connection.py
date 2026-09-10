import socket
from unittest import mock

import pytest
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import SentinelManagedConnection
from redis.backoff import NoBackoff

pytestmark = pytest.mark.asyncio


@pytest.mark.fixed_client
async def test_connect_retry_on_timeout_error(connect_args):
    """Test that the _connect function is retried in case of a timeout"""
    connection_pool = mock.AsyncMock()
    connection_pool.get_master_address = mock.AsyncMock(
        return_value=(connect_args["host"], connect_args["port"])
    )
    conn = SentinelManagedConnection(
        retry_on_timeout=True,
        retry=Retry(NoBackoff(), 3),
        connection_pool=connection_pool,
    )
    origin_connect = conn._connect
    conn._connect = mock.AsyncMock()

    async def mock_connect():
        # connect only on the last retry
        if conn._connect.call_count <= 2:
            raise socket.timeout
        else:
            return await origin_connect()

    conn._connect.side_effect = mock_connect
    await conn.connect()
    assert conn._connect.call_count == 3
    assert connection_pool.get_master_address.call_count == 3
    await conn.disconnect()


@pytest.mark.fixed_client
async def test_read_response_disconnects_on_base_exception(connect_args):
    """
    Mirror of the sync test: a BaseException at the socket read leaves the reply
    unread, so the connection must be closed rather than reused. See #1128.
    """
    connection_pool = mock.AsyncMock()
    connection_pool.get_master_address = mock.AsyncMock(
        return_value=(connect_args["host"], connect_args["port"])
    )
    connection_pool.is_master = True
    connection_pool.check_connection = False
    conn = SentinelManagedConnection(connection_pool=connection_pool)
    await conn.connect()
    try:
        await conn.send_command("PING")
        with mock.patch.object(
            conn, "_read_response_from_parser", side_effect=KeyboardInterrupt
        ):
            with pytest.raises(KeyboardInterrupt):
                await conn.read_response()
        assert conn._reader is None
    finally:
        await conn.disconnect()
