from unittest.mock import AsyncMock, MagicMock

import pytest

from redis.asyncio import Redis
from redis.asyncio.multidb.client import PubSub
from redis.asyncio.multidb.event import (
    AsyncActiveDatabaseChanged,
    CloseConnectionOnActiveDatabaseChanged,
    ResubscribeOnActiveDatabaseChanged,
)


@pytest.mark.fixed_client
class TestResubscribeOnActiveDatabaseChanged:
    @pytest.mark.asyncio
    async def test_resubscribes_channels_patterns_and_shard_channels(self):
        old_pubsub = MagicMock()
        old_pubsub.channels = {b"channel": None}
        old_pubsub.patterns = {b"pattern*": None}
        old_pubsub.shard_channels = {b"shard-channel": None}
        old_pubsub.aclose = AsyncMock()
        new_pubsub = MagicMock()
        new_pubsub.on_connect = AsyncMock()
        new_database = MagicMock()
        new_database.client.pubsub.return_value = new_pubsub
        command_executor = MagicMock()
        command_executor.active_pubsub = old_pubsub

        await ResubscribeOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(
                MagicMock(),
                new_database,
                command_executor,
                ignore_subscribe_messages=True,
            )
        )

        new_database.client.pubsub.assert_called_once_with(
            ignore_subscribe_messages=True
        )
        assert new_pubsub.channels == {b"channel": None}
        assert new_pubsub.patterns == {b"pattern*": None}
        assert new_pubsub.shard_channels == {b"shard-channel": None}
        new_pubsub.on_connect.assert_awaited_once_with(None)
        assert command_executor.active_pubsub is new_pubsub
        old_pubsub.aclose.assert_awaited_once()


@pytest.mark.fixed_client
class TestCloseConnectionOnActiveDatabaseChanged:
    @pytest.mark.asyncio
    async def test_standalone_keeps_in_use_connections(self):
        old_database = MagicMock()
        old_database.client = MagicMock(spec=Redis)
        pool = old_database.client.connection_pool = MagicMock()
        pool.update_active_connections_for_reconnect = AsyncMock()
        pool.disconnect = AsyncMock()

        await CloseConnectionOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.aclose.assert_not_called()
        pool.update_active_connections_for_reconnect.assert_awaited_once_with()
        pool.disconnect.assert_awaited_once_with(inuse_connections=False)

    @pytest.mark.asyncio
    async def test_cluster_keeps_in_use_connections(self):
        old_database = MagicMock()
        nodes = [MagicMock(), MagicMock()]
        for node in nodes:
            node.disconnect_free_connections = AsyncMock()
        old_database.client.nodes_manager.nodes_cache = {
            f"node{i}": node for i, node in enumerate(nodes)
        }

        await CloseConnectionOnActiveDatabaseChanged().listen(
            AsyncActiveDatabaseChanged(old_database, MagicMock(), MagicMock())
        )

        old_database.client.aclose.assert_not_called()
        for node in nodes:
            node.update_active_connections_for_reconnect.assert_called_once_with()
            node.disconnect_free_connections.assert_awaited_once_with()


@pytest.mark.fixed_client
class TestMultiDbPubSubShardedMethods:
    @staticmethod
    def _pubsub():
        client = MagicMock()
        client.command_executor.execute_pubsub_method = AsyncMock(return_value="ok")
        return PubSub(client), client.command_executor.execute_pubsub_method

    @pytest.mark.asyncio
    async def test_ssubscribe(self):
        pubsub, execute = self._pubsub()
        handler = MagicMock()

        assert await pubsub.ssubscribe("a", b=handler) == "ok"
        execute.assert_awaited_once_with("ssubscribe", "a", b=handler)

    @pytest.mark.asyncio
    async def test_sunsubscribe(self):
        pubsub, execute = self._pubsub()

        assert await pubsub.sunsubscribe("a", "b") == "ok"
        execute.assert_awaited_once_with("sunsubscribe", "a", "b")

    @pytest.mark.asyncio
    async def test_get_sharded_message(self):
        pubsub, execute = self._pubsub()

        assert (
            await pubsub.get_sharded_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            == "ok"
        )
        execute.assert_awaited_once_with(
            "get_sharded_message", ignore_subscribe_messages=True, timeout=1.0
        )
