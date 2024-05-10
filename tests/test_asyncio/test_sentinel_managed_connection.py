import socket
from typing import Tuple
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
)
from redis.backoff import NoBackoff

from .compat import mock

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
    await conn.disconnect()


class SentinelManagedConnectionMock(SentinelManagedConnection):
    async def connect_to_address(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    async def can_read_destructive(self) -> bool:
        # Mock this function to always return False.
        # Trrue means there's still data to be read and hence we can't reconnect
        # to this connection yet
        return False


class SentinelManagedConnectionMockForReplicaMode(SentinelManagedConnectionMock):
    async def connect(self) -> None:
        """
        This simulates the behavior of connect when
        :py:class:`~redis.SentinelConnectionPool`
        is in replica mode.
        It'll call rotate_slaves and connect to the next replica.
        """
        import random
        import time

        self.host = f"host-{random.randint(0, 10)}"
        self.port = time.time()


class SentinelManagedConnectionMockForMasterMode(SentinelManagedConnectionMock):
    async def connect_to(self, address: Tuple[str, int]) -> None:
        """
        This simulates the behavior of connect_to when
        :py:class:`~redis.SentinelConnectionPool`
        is in master mode.
        It'll try to connect to master. In this mock class,
        it'll just set the host and port without actually connecting.
        """
        self.host, self.port = address


@pytest_asyncio.fixture()
async def connection_pool_replica_mock() -> SentinelConnectionPool:
    sentinel_manager = Sentinel([["master", 400]])
    # Give a random slave
    sentinel_manager.discover_slaves = AsyncMock(return_value=["replica", 5000])
    # Create connection pool with our mock connection object
    connection_pool = SentinelConnectionPool(
        "usasm",
        sentinel_manager,
        is_master=False,
        connection_class=SentinelManagedConnectionMockForReplicaMode,
    )
    return connection_pool


@pytest_asyncio.fixture()
async def connection_pool_master_mock() -> SentinelConnectionPool:
    sentinel_manager = Sentinel([["master", 400]])
    # Give a random slave
    sentinel_manager.discover_master = AsyncMock(return_value=["replica", 5000])
    # Create connection pool with our mock connection object
    connection_pool = SentinelConnectionPool(
        "usasm",
        sentinel_manager,
        is_master=True,
        connection_class=SentinelManagedConnectionMockForMasterMode,
    )
    return connection_pool


def same_address(
    connection_1: SentinelManagedConnection,
    connection_2: SentinelManagedConnection,
) -> bool:
    return bool(
        connection_1.host == connection_2.host
        and connection_1.port == connection_2.port
    )


async def test_connects_to_same_address_if_same_id_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``_iter_req_id`` is the same
    when we are in replica mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = await connection_pool_replica_mock.get_connection(
        "ANY", _iter_req_id=1
    )
    assert same_address(
        await connection_pool_replica_mock.get_connection("ANY", _iter_req_id=1),
        connection_for_req_1,
    )


async def test_connects_to_same_conn_object_if_same_id_and_conn_released_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection object is the same if the ``_iter_req_id`` is the same
    when we are in replica mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`
    and if we release the connection back to the connection pool before
    trying to connect again.
    """
    connection_for_req_1 = await connection_pool_replica_mock.get_connection(
        "ANY", _iter_req_id=1
    )
    await connection_pool_replica_mock.release(connection_for_req_1)
    assert (
        await connection_pool_replica_mock.get_connection("ANY", _iter_req_id=1)
        == connection_for_req_1
    )


async def test_connects_to_diff_address_if_no_iter_req_id_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection object is different if no _iter_req_id is supplied.
    In reality, they can be the same, but in this case, we're not
    releasing the connection to the pool so they should always be different.
    """
    connection_for_req_1 = await connection_pool_replica_mock.get_connection(
        "ANY", _iter_req_id=1
    )
    connection_for_random_req = await connection_pool_replica_mock.get_connection(
        "ANYY"
    )
    assert not same_address(connection_for_random_req, connection_for_req_1)
    assert not same_address(
        await connection_pool_replica_mock.get_connection("ANY_COMMAND"),
        connection_for_random_req,
    )
    assert not same_address(
        await connection_pool_replica_mock.get_connection("ANY_COMMAND"),
        connection_for_req_1,
    )


async def test_connects_to_same_address_if_same_iter_req_id_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``_iter_req_id`` is the same
    when we are in master mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = await connection_pool_master_mock.get_connection(
        "ANY", _iter_req_id=1
    )
    assert same_address(
        await connection_pool_master_mock.get_connection("ANY", _iter_req_id=1),
        connection_for_req_1,
    )


async def test_connects_to_same_conn_object_if_same_iter_req_id_and_released_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``_iter_req_id`` is the same
    when we are in master mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`
    and if we release the connection back to the connection pool before
    trying to connect again.
    """
    connection_for_req_1 = await connection_pool_master_mock.get_connection(
        "ANY", _iter_req_id=1
    )
    assert same_address(
        await connection_pool_master_mock.get_connection("ANY", _iter_req_id=1),
        connection_for_req_1,
    )


async def test_connects_to_same_address_if_no_iter_req_id_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that connection address is always the same regardless if
    there's an ``iter_req_id`` or not
    when we are in master mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = await connection_pool_master_mock.get_connection(
        "ANY", _iter_req_id=1
    )
    connection_for_random_req = await connection_pool_master_mock.get_connection("ANYY")
    assert same_address(connection_for_random_req, connection_for_req_1)
    assert same_address(
        await connection_pool_master_mock.get_connection("ANY_COMMAND"),
        connection_for_random_req,
    )

    assert same_address(
        await connection_pool_master_mock.get_connection("ANY_COMMAND"),
        connection_for_req_1,
    )
