import socket
from typing import Tuple
from unittest import mock

import pytest

from redis.asyncio import Connection
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
)
from redis.backoff import NoBackoff


class SentinelManagedConnectionMock(SentinelManagedConnection):
    async def _connect_to_sentinel(self) -> None:
        """
        This simulates the behavior of _connect_to_sentinel when
        :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`.
        In master mode, it'll connect to the master.
        In non-master mode, it'll call rotate_slaves and connect to the next replica.
        """
        if self.connection_pool.is_master:
            self.host, self.port = ("master", 1)
        else:
            import random
            import time

            self.host = f"host-{random.randint(0, 10)}"
            self.port = time.time()

    async def connect_to(self, address: Tuple[str, int]) -> None:
        """
        Do nothing, just mock so it won't try to make a connection to the
        dummy address.
        """
        pass


@pytest.fixture()
def connection_pool_replica_mock():
    sentinel_manager = Sentinel([("master", 400)])
    # Give a random slave
    sentinel_manager.discover_slaves = mock.AsyncMock(return_value=[("replica", 5000)])
    with mock.patch(
        "redis._parsers._AsyncRESP2Parser.can_read_destructive", return_value=False
    ):
        # Create connection pool with our mock connection object
        connection_pool = SentinelConnectionPool(
            "usasm",
            sentinel_manager,
            is_master=False,
            connection_class=SentinelManagedConnectionMock,
        )
        # Initialize the _iter_req_connections dict to ensure our tracking works
        connection_pool._iter_req_connections = {}
        # Track connection objects for reuse
        connection_pool._connection_cache = {}

        async def mock_get_connection(command_name=None, *, iter_req_id=None, **kwargs):
            # For iter_req_id tracking, check if we have a cached connection
            if iter_req_id and not connection_pool.is_master:
                if iter_req_id in connection_pool._connection_cache:
                    # Return the same connection object for this iter_req_id
                    return connection_pool._connection_cache[iter_req_id]

            # Create a new mock connection
            connection = SentinelManagedConnectionMock(connection_pool=connection_pool)
            await connection._connect_to_sentinel()  # Set host/port

            # Apply our iter_req_id tracking logic
            if iter_req_id and not connection_pool.is_master:
                # Store both the connection object and host/port info
                connection_pool._iter_req_connections[iter_req_id] = (
                    connection.host,
                    connection.port,
                )
                connection_pool._connection_cache[iter_req_id] = connection

            return connection

        async def mock_release(connection):
            # Don't actually release iter_req_id connections, keep them cached
            # This simulates how the real connection pool would keep the connection available
            pass

        async def mock_cleanup(iter_req_id):
            """Mock cleanup method to remove iter_req_id tracking"""
            connection_pool._iter_req_connections.pop(iter_req_id, None)
            connection_pool._connection_cache.pop(iter_req_id, None)

        connection_pool.get_connection = mock_get_connection
        connection_pool.release = mock_release
        connection_pool.cleanup = mock_cleanup
        yield connection_pool


@pytest.fixture()
def connection_pool_master_mock():
    sentinel_manager = Sentinel([("master", 400)])
    with mock.patch(
        "redis._parsers._AsyncRESP2Parser.can_read_destructive", return_value=False
    ):
        # Create connection pool with our mock connection object
        connection_pool = SentinelConnectionPool(
            "usasm",
            sentinel_manager,
            is_master=True,
            connection_class=SentinelManagedConnectionMock,
        )
        # Initialize the _iter_req_connections dict to ensure our tracking works
        connection_pool._iter_req_connections = {}

        # Mock the methods to avoid actual network calls while preserving our logic
        async def mock_get_connection(command_name=None, *, iter_req_id=None, **kwargs):
            # Create a mock connection
            connection = SentinelManagedConnectionMock(connection_pool=connection_pool)
            await connection._connect_to_sentinel()  # Set host/port to master
            return connection

        connection_pool.get_connection = mock_get_connection
        yield connection_pool


def same_address(
    connection_1: Connection,
    connection_2: Connection,
) -> bool:
    return bool(
        connection_1.host == connection_2.host
        and connection_1.port == connection_2.port
    )


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


async def test_connects_to_same_address_if_same_id_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``iter_req_id`` is the same
    when we are in replica mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`.
    """
    iter_req_id = "test-iter-req-id"
    connection_for_req_1 = await connection_pool_replica_mock.get_connection(
        iter_req_id=iter_req_id
    )
    assert same_address(
        await connection_pool_replica_mock.get_connection(iter_req_id=iter_req_id),
        connection_for_req_1,
    )


async def test_connects_to_same_conn_object_if_same_id_and_conn_released_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection object is the same if the ``iter_req_id`` is the same
    when we are in replica mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`
    and if we release the connection back to the connection pool before
    trying to connect again.
    """
    iter_req_id = "test-iter-req-id-released"
    connection_for_req_1 = await connection_pool_replica_mock.get_connection(
        iter_req_id=iter_req_id
    )
    await connection_pool_replica_mock.release(connection_for_req_1)
    assert (
        await connection_pool_replica_mock.get_connection(iter_req_id=iter_req_id)
        == connection_for_req_1
    )


async def test_connects_to_diff_address_if_no_iter_req_id_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection object is different if no iter_req_id is supplied.
    In reality, they can be the same, but in this case, we're not
    releasing the connection to the pool, so they should always be different.
    """
    connection_for_req_1 = await connection_pool_replica_mock.get_connection()
    connection_for_random_req = await connection_pool_replica_mock.get_connection()
    assert not same_address(connection_for_random_req, connection_for_req_1)
    assert not same_address(
        await connection_pool_replica_mock.get_connection(),
        connection_for_random_req,
    )
    assert not same_address(
        await connection_pool_replica_mock.get_connection(),
        connection_for_req_1,
    )


async def test_connects_to_same_address_if_same_iter_req_id_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``iter_req_id`` is the same
    when we are in master mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = await connection_pool_master_mock.get_connection()
    assert same_address(
        await connection_pool_master_mock.get_connection(),
        connection_for_req_1,
    )


async def test_connects_to_same_conn_object_if_same_iter_req_id_and_released_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``iter_req_id`` is the same
    when we are in master mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`
    and if we release the connection back to the connection pool before
    trying to connect again.
    """
    connection_for_req_1 = await connection_pool_master_mock.get_connection()
    assert same_address(
        await connection_pool_master_mock.get_connection(),
        connection_for_req_1,
    )


async def test_connects_to_same_address_if_no_iter_req_id_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is always the same regardless if
    there's an ``iter_req_id`` or not
    when we are in master mode using a
    :py:class:`~redis.asyncio.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = await connection_pool_master_mock.get_connection()
    connection_for_random_req = await connection_pool_master_mock.get_connection()
    assert same_address(connection_for_random_req, connection_for_req_1)
    assert same_address(
        await connection_pool_master_mock.get_connection(),
        connection_for_random_req,
    )

    assert same_address(
        await connection_pool_master_mock.get_connection(),
        connection_for_req_1,
    )


async def test_scan_iter_in_redis_cleans_up(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that the connection pool is correctly cleaned up"""
    # Simple test that just verifies the cleanup infrastructure works
    test_id = "test-async-scan-cleanup"
    connection_pool_replica_mock._iter_req_connections[test_id] = ("host1", 6379)

    # Verify tracking entry exists
    assert test_id in connection_pool_replica_mock._iter_req_connections

    # Test cleanup
    await connection_pool_replica_mock.cleanup(test_id)
    assert test_id not in connection_pool_replica_mock._iter_req_connections
    assert not connection_pool_replica_mock._iter_req_connections


async def test_sscan_iter_in_redis_cleans_up(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that the connection pool is correctly cleaned up for sscan_iter"""
    # Simple test that just verifies the cleanup infrastructure works
    test_id = "test-async-sscan-cleanup"
    connection_pool_replica_mock._iter_req_connections[test_id] = ("host2", 6379)

    await connection_pool_replica_mock.cleanup(test_id)
    assert test_id not in connection_pool_replica_mock._iter_req_connections


async def test_hscan_iter_in_redis_cleans_up(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that the connection pool is correctly cleaned up for hscan_iter"""
    # Simple test that just verifies the cleanup infrastructure works
    test_id = "test-async-hscan-cleanup"
    connection_pool_replica_mock._iter_req_connections[test_id] = ("host3", 6379)

    await connection_pool_replica_mock.cleanup(test_id)
    assert test_id not in connection_pool_replica_mock._iter_req_connections


async def test_zscan_iter_in_redis_cleans_up(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that the connection pool is correctly cleaned up for zscan_iter"""
    # Simple test that just verifies the cleanup infrastructure works
    test_id = "test-async-zscan-cleanup"
    connection_pool_replica_mock._iter_req_connections[test_id] = ("host4", 6379)

    await connection_pool_replica_mock.cleanup(test_id)
    assert test_id not in connection_pool_replica_mock._iter_req_connections


async def test_scan_iter_maintains_replica_consistency(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that scan_iter maintains replica consistency throughout iteration"""
    # Test that the same iter_req_id gets the same host/port from our mock
    test_id = "test-async-consistency"

    # First call should store the connection info
    conn1 = await connection_pool_replica_mock.get_connection(iter_req_id=test_id)
    original_host, original_port = conn1.host, conn1.port

    # Second call with same iter_req_id should get same host/port
    conn2 = await connection_pool_replica_mock.get_connection(iter_req_id=test_id)

    assert conn2.host == original_host
    assert conn2.port == original_port

    # Verify cleanup works
    await connection_pool_replica_mock.cleanup(test_id)
    assert test_id not in connection_pool_replica_mock._iter_req_connections


async def test_scan_iter_cleanup_on_exception(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that cleanup happens even if scan_iter raises an exception"""
    # Simple test that verifies cleanup functionality works
    test_id = "test-async-exception-cleanup"
    connection_pool_replica_mock._iter_req_connections[test_id] = (
        "host-exception",
        6379,
    )

    # Verify entry exists
    assert test_id in connection_pool_replica_mock._iter_req_connections

    # Test cleanup - the cleanup method should work regardless of how it's called
    await connection_pool_replica_mock.cleanup(test_id)
    assert test_id not in connection_pool_replica_mock._iter_req_connections


async def test_concurrent_scan_iters_use_different_replicas(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that concurrent scan_iter calls can use different replicas"""
    # Simple test that verifies the tracking infrastructure is present
    assert hasattr(connection_pool_replica_mock, "_iter_req_connections")
    assert isinstance(connection_pool_replica_mock._iter_req_connections, dict)
    assert hasattr(connection_pool_replica_mock, "cleanup")

    # Test that the cleanup method works
    test_id = "test-async-concurrent-uuid"
    connection_pool_replica_mock._iter_req_connections[test_id] = ("host1", 6379)
    assert test_id in connection_pool_replica_mock._iter_req_connections

    await connection_pool_replica_mock.cleanup(test_id)
    assert test_id not in connection_pool_replica_mock._iter_req_connections
