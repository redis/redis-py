from typing import Tuple
from unittest import mock

import pytest
from redis import Redis
from redis.sentinel import Sentinel, SentinelConnectionPool, SentinelManagedConnection
from redis.utils import HIREDIS_AVAILABLE

pytestmark = pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")


class SentinelManagedConnectionMock(SentinelManagedConnection):
    def _connect_to_sentinel(self) -> None:
        """
        This simulates the behavior of _connect_to_sentinel when
        :py:class:`~redis.SentinelConnectionPool`.
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

    def connect_to(self, address: Tuple[str, int]) -> None:
        """
        Do nothing, just mock so it won't try to make a connection to the
        dummy address.
        """
        pass


@pytest.fixture()
def connection_pool_replica_mock() -> SentinelConnectionPool:
    sentinel_manager = Sentinel([["master", 400]])
    # Give a random slave
    sentinel_manager.discover_slaves = mock.Mock(return_value=["replica", 5000])
    # Create connection pool with our mock connection object
    connection_pool = SentinelConnectionPool(
        "usasm",
        sentinel_manager,
        is_master=False,
        connection_class=SentinelManagedConnectionMock,
    )
    return connection_pool


@pytest.fixture()
def connection_pool_master_mock() -> SentinelConnectionPool:
    sentinel_manager = Sentinel([["master", 400]])
    # Give a random slave
    sentinel_manager.discover_master = mock.Mock(return_value=["replica", 5000])
    # Create connection pool with our mock connection object
    connection_pool = SentinelConnectionPool(
        "usasm",
        sentinel_manager,
        is_master=True,
        connection_class=SentinelManagedConnectionMock,
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


def test_connects_to_same_address_if_same_id_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``iter_req_id`` is the same
    when we are in replica mode using a
    :py:class:`~redis.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = connection_pool_replica_mock.get_connection(
        "ANY", iter_req_id=1
    )
    assert same_address(
        connection_pool_replica_mock.get_connection("ANY", iter_req_id=1),
        connection_for_req_1,
    )


def test_connects_to_same_conn_object_if_same_id_and_conn_released_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection object is the same if the ``iter_req_id`` is the same
    when we are in replica mode using a
    :py:class:`~redis.sentinel.SentinelConnectionPool`
    and if we release the connection back to the connection pool before
    trying to connect again.
    """
    connection_for_req_1 = connection_pool_replica_mock.get_connection(
        "ANY", iter_req_id=1
    )
    connection_pool_replica_mock.release(connection_for_req_1)
    assert (
        connection_pool_replica_mock.get_connection("ANY", iter_req_id=1)
        == connection_for_req_1
    )


def test_connects_to_diff_address_if_no_iter_req_id_replica(
    connection_pool_replica_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection object is different if no iter_req_id is supplied.
    In reality, they can be the same, but in this case, we're not
    releasing the connection to the pool so they should always be different.
    """
    connection_for_req_1 = connection_pool_replica_mock.get_connection(
        "ANY", iter_req_id=1
    )
    connection_for_random_req = connection_pool_replica_mock.get_connection("ANYY")
    assert not same_address(connection_for_random_req, connection_for_req_1)
    assert not same_address(
        connection_pool_replica_mock.get_connection("ANY_COMMAND"),
        connection_for_random_req,
    )
    assert not same_address(
        connection_pool_replica_mock.get_connection("ANY_COMMAND"),
        connection_for_req_1,
    )


def test_connects_to_same_address_if_same_iter_req_id_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``_iter_req_id`` is the same
    when we are in master mode using a
    :py:class:`~redis.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = connection_pool_master_mock.get_connection(
        "ANY", _iter_req_id=1
    )
    assert same_address(
        connection_pool_master_mock.get_connection("ANY", iter_req_id=1),
        connection_for_req_1,
    )


def test_connects_to_same_conn_object_if_same_iter_req_id_and_released_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that the connection address is the same if the ``_iter_req_id`` is the same
    when we are in master mode using a
    :py:class:`~redis.sentinel.SentinelConnectionPool`
    and if we release the connection back to the connection pool before
    trying to connect again.
    """
    connection_for_req_1 = connection_pool_master_mock.get_connection(
        "ANY", iter_req_id=1
    )
    assert same_address(
        connection_pool_master_mock.get_connection("ANY", iter_req_id=1),
        connection_for_req_1,
    )


def test_connects_to_same_address_if_no_iter_req_id_master(
    connection_pool_master_mock: SentinelConnectionPool,
) -> None:
    """
    Assert that connection address is always the same regardless if
    there's an ``iter_req_id`` or not
    when we are in master mode using a
    :py:class:`~redis.sentinel.SentinelConnectionPool`.
    """
    connection_for_req_1 = connection_pool_master_mock.get_connection(
        "ANY", iter_req_id=1
    )
    connection_for_random_req = connection_pool_master_mock.get_connection("ANYY")
    assert same_address(connection_for_random_req, connection_for_req_1)
    assert same_address(
        connection_pool_master_mock.get_connection("ANY_COMMAND"),
        connection_for_random_req,
    )

    assert same_address(
        connection_pool_master_mock.get_connection("ANY_COMMAND"),
        connection_for_req_1,
    )


def test_scan_iter_in_redis_cleans_up(
    connection_pool_replica_mock: SentinelConnectionPool,
):
    """Test that connection pool is correctly cleaned up"""
    r = Redis(connection_pool=connection_pool_replica_mock)
    # Patch the actual sending and parsing response from the Connection object
    # but still let the connection pool does all the necessary work
    with mock.patch.object(r, "_send_command_parse_response", return_value=(0, [])):
        [k for k in r.scan_iter("a")]
    # Test that the iter_req_id for the scan command is cleared at the
    # end of the SCAN ITER command
    assert not connection_pool_replica_mock._iter_req_id_to_replica_address
