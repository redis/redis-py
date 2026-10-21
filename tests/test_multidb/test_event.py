from unittest.mock import MagicMock

import pytest

from redis.client import Redis
from redis.multidb.event import (
    ActiveDatabaseChanged,
    CloseConnectionOnActiveDatabaseChanged,
)


@pytest.mark.fixed_client
class TestCloseConnectionOnActiveDatabaseChanged:
    def test_standalone_keeps_in_use_connections(self):
        old_database = MagicMock()
        old_database.client = MagicMock(spec=Redis)
        pool = old_database.client.connection_pool = MagicMock()

        CloseConnectionOnActiveDatabaseChanged().listen(
            ActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.close.assert_not_called()
        pool.update_active_connections_for_reconnect.assert_called_once_with()
        pool.disconnect.assert_called_once_with(inuse_connections=False)

    def test_cluster_keeps_in_use_connections(self):
        old_database = MagicMock()
        nodes = [MagicMock(), MagicMock()]
        old_database.client.nodes_manager.nodes_cache = {
            f"node{i}": node for i, node in enumerate(nodes)
        }

        CloseConnectionOnActiveDatabaseChanged().listen(
            ActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.close.assert_not_called()
        for node in nodes:
            pool = node.redis_connection.connection_pool
            pool.update_active_connections_for_reconnect.assert_called_once_with()
            pool.disconnect.assert_called_once_with(inuse_connections=False)
