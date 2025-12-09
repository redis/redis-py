import socket
from unittest import mock

import pytest

from redis.asyncio import Connection
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import SentinelManagedConnection
from redis.backoff import NoBackoff

pytestmark = pytest.mark.asyncio


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
    original_super_connect = Connection._connect.__get__(conn, Connection)

    with mock.patch.object(Connection, "_connect", new_callable=mock.AsyncMock) as mock_super_connect:
        async def side_effect(*args, **kwargs):
            if mock_super_connect.await_count <= 2:
                raise socket.timeout()
            return await original_super_connect(*args, **kwargs)

        mock_super_connect.side_effect = side_effect

        await conn.connect()
        assert mock_super_connect.await_count == 3
        assert connection_pool.get_master_address.call_count == 3
        await conn.disconnect()