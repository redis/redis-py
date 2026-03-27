"""
Tests for async Redis keyspace notifications support.
"""

import asyncio

import pytest
from unittest.mock import AsyncMock, MagicMock, Mock

from redis.asyncio.keyspace_notifications import (
    AsyncClusterKeyspaceNotifications,
    AsyncKeyspaceNotifications,
    _ClusterNodePoolAdapter,
)
from redis.exceptions import ConnectionError
from redis.keyspace_notifications import (
    EventType,
    KeyspaceChannel,
)


class TestAsyncStandaloneKeyspaceNotificationsMocked:
    """
    Mock-based unit tests for AsyncKeyspaceNotifications (standalone Redis).
    """

    def _create_mock_redis(self):
        """Create a mock async Redis client with pubsub."""
        redis_client = Mock()
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = AsyncMock(return_value=None)
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.psubscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.punsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()
        redis_client.pubsub = Mock(return_value=mock_pubsub)
        return redis_client, mock_pubsub

    @pytest.mark.asyncio
    async def test_subscribe_exact_channel(self):
        """Test subscribing to an exact keyspace channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        # Verify subscribe was called with kwargs (exact channel, not pattern)
        mock_pubsub.subscribe.assert_called_once()
        assert "__keyspace@0__:mykey" in mock_pubsub.subscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_pattern_channel(self):
        """Test subscribing to a pattern keyspace channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("user:*", db=0)
        await notifications.subscribe(channel)

        # Verify psubscribe was called with kwargs (pattern subscription)
        mock_pubsub.psubscribe.assert_called_once()
        assert "__keyspace@0__:user:*" in mock_pubsub.psubscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_keyevent(self):
        """Test subscribing to a keyevent channel."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.subscribe_keyevent(EventType.SET, db=0)

        # KeyeventChannel with "set" is exact, not pattern
        mock_pubsub.subscribe.assert_called_once()
        assert "__keyevent@0__:set" in mock_pubsub.subscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_keyspace_convenience(self):
        """Test the subscribe_keyspace convenience method."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.subscribe_keyspace("cache:*", db=0)

        # Pattern should use psubscribe with kwargs
        mock_pubsub.psubscribe.assert_called_once()
        assert "__keyspace@0__:cache:*" in mock_pubsub.psubscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """Test unsubscribing from channels."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)

        # Subscribe to exact and pattern channels
        exact_channel = KeyspaceChannel("mykey", db=0)
        pattern_channel = KeyspaceChannel("user:*", db=0)
        await notifications.subscribe(exact_channel)
        await notifications.subscribe(pattern_channel)

        # Unsubscribe
        await notifications.unsubscribe(exact_channel)
        await notifications.unsubscribe(pattern_channel)

        mock_pubsub.unsubscribe.assert_called_once()
        mock_pubsub.punsubscribe.assert_called_once()

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_get_message_returns_notification(self):
        """Test that get_message returns KeyNotification objects."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a keyspace message
        mock_pubsub.get_message.return_value = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == "set"
        assert notification.database == 0

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_get_message_with_pattern(self):
        """Test get_message with pattern subscription."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a pmessage
        mock_pubsub.get_message.return_value = {
            "type": "pmessage",
            "pattern": b"__keyspace@0__:user:*",
            "channel": b"__keyspace@0__:user:123",
            "data": b"set",
        }

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.subscribe(KeyspaceChannel("user:*", db=0))

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "user:123"
        assert notification.event_type == "set"

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_get_message_returns_none_when_closed(self):
        """Test that get_message returns None when notifications are closed."""
        redis_client, mock_pubsub = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        await notifications.aclose()

        notification = await notifications.get_message(timeout=1.0)
        assert notification is None

    @pytest.mark.asyncio
    async def test_handler_callback(self):
        """Test that handlers are called with KeyNotification objects."""
        redis_client, mock_pubsub = self._create_mock_redis()

        received = []

        def handler(notification):
            received.append(notification)

        # Message to be delivered
        test_message = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        # Track the wrapped handler that will be registered
        registered_handlers = {}

        async def capture_subscribe(**kwargs):
            registered_handlers.update(kwargs)

        mock_pubsub.subscribe.side_effect = capture_subscribe

        # When get_message is called, simulate pubsub behavior:
        # call the handler and return None (like real pubsub does)
        async def mock_get_message(**_kwargs):
            channel_key = "__keyspace@0__:mykey"
            if channel_key in registered_handlers and registered_handlers[channel_key]:
                registered_handlers[channel_key](test_message)
                return None  # Handler consumed the message
            return test_message

        mock_pubsub.get_message.side_effect = mock_get_message

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel, handler=handler)

        # get_message should return None because handler consumed it
        result = await notifications.get_message(timeout=1.0)
        assert result is None

        # Handler should have been called with KeyNotification
        assert len(received) == 1
        assert received[0].key == "mykey"
        assert received[0].event_type == "set"

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_async_handler_callback(self):
        """Test that async handlers are properly awaited."""
        redis_client, mock_pubsub = self._create_mock_redis()

        received = []

        async def async_handler(notification):
            await asyncio.sleep(0)  # Simulate async work
            received.append(notification)

        # Message to be delivered
        test_message = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        # Track the wrapped handler that will be registered
        registered_handlers = {}

        async def capture_subscribe(**kwargs):
            registered_handlers.update(kwargs)

        mock_pubsub.subscribe.side_effect = capture_subscribe

        # When get_message is called, simulate pubsub behavior:
        # call the handler and return None (like real pubsub does)
        async def mock_get_message(**_kwargs):
            channel_key = "__keyspace@0__:mykey"
            if channel_key in registered_handlers and registered_handlers[channel_key]:
                handler = registered_handlers[channel_key]
                result = handler(test_message)
                # Await if the handler is async
                if hasattr(result, "__await__"):
                    await result
                return None  # Handler consumed the message
            return test_message

        mock_pubsub.get_message.side_effect = mock_get_message

        notifications = AsyncKeyspaceNotifications(redis_client)
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel, handler=async_handler)

        # get_message should return None because handler consumed it
        result = await notifications.get_message(timeout=1.0)
        assert result is None

        # Handler should have been called with KeyNotification
        assert len(received) == 1
        assert received[0].key == "mykey"
        assert received[0].event_type == "set"

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test AsyncKeyspaceNotifications as async context manager."""
        redis_client, mock_pubsub = self._create_mock_redis()

        async with AsyncKeyspaceNotifications(redis_client) as notifications:
            await notifications.subscribe(KeyspaceChannel("mykey"))
            assert notifications.subscribed

        # Should be closed after exiting context
        mock_pubsub.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribed_property(self):
        """Test the subscribed property."""
        redis_client, _ = self._create_mock_redis()

        notifications = AsyncKeyspaceNotifications(redis_client)
        assert not notifications.subscribed

        await notifications.subscribe(KeyspaceChannel("mykey"))
        assert notifications.subscribed

        await notifications.unsubscribe(KeyspaceChannel("mykey"))
        assert not notifications.subscribed

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_key_prefix_filtering(self):
        """Test that key_prefix filters notifications."""
        redis_client, mock_pubsub = self._create_mock_redis()

        # Setup mock to return a notification for a key without the prefix
        mock_pubsub.get_message.return_value = {
            "type": "message",
            "channel": b"__keyspace@0__:other:key",
            "data": b"set",
            "pattern": None,
        }

        notifications = AsyncKeyspaceNotifications(redis_client, key_prefix="myapp:")
        await notifications.subscribe(KeyspaceChannel("*"))

        # Should be filtered out (returns None)
        notification = await notifications.get_message(timeout=0.1)
        assert notification is None

        await notifications.aclose()


class TestAsyncClusterKeyspaceNotificationsMocked:
    """
    Mock-based unit tests for AsyncClusterKeyspaceNotifications.
    """

    def _create_mock_node(self, name, host, port, server_type="primary"):
        """Create a mock ClusterNode."""
        node = Mock()
        node.name = name
        node.host = host
        node.port = port
        node.server_type = server_type

        # Create a mock pubsub
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = AsyncMock(return_value=None)
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.psubscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.punsubscribe = AsyncMock()
        mock_pubsub.aclose = AsyncMock()

        return node, mock_pubsub

    def _create_mock_cluster(self, nodes, pubsubs_by_node):
        """Create a mock async RedisCluster with the given nodes."""
        cluster = Mock()
        # Filter to only primary nodes
        primary_nodes = [n for n in nodes if n.server_type == "primary"]
        cluster.get_nodes = Mock(return_value=nodes)
        cluster.get_primaries = Mock(return_value=primary_nodes)

        # Mock connection_kwargs for creating standalone clients
        cluster.connection_kwargs = {}

        # Mock nodes_manager
        nodes_manager = Mock()
        cluster.nodes_manager = nodes_manager

        return cluster, pubsubs_by_node

    @pytest.mark.asyncio
    async def test_subscribe_exact_channel_on_all_nodes(self):
        """Test subscribing to an exact channel creates subscriptions on all nodes."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Manually inject the mock pubsubs
        notifications._node_pubsubs = pubsubs_by_node.copy()

        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        # Verify subscribe was called on both pubsubs
        pubsub1.subscribe.assert_called_once()
        pubsub2.subscribe.assert_called_once()

        assert "__keyspace@0__:mykey" in pubsub1.subscribe.call_args.kwargs
        assert "__keyspace@0__:mykey" in pubsub2.subscribe.call_args.kwargs

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_subscribe_pattern_on_all_nodes(self):
        """Test subscribing to a pattern creates psubscriptions on all nodes."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Manually inject the mock pubsubs
        notifications._node_pubsubs = pubsubs_by_node.copy()

        channel = KeyspaceChannel("user:*", db=0)
        await notifications.subscribe(channel)

        # Verify psubscribe was called on both pubsubs (patterns use psubscribe)
        pubsub1.psubscribe.assert_called_once()
        pubsub2.psubscribe.assert_called_once()

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_receives_notification_from_any_node(self):
        """Test that notifications can be received from any primary node."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)
        notifications._node_pubsubs = pubsubs_by_node.copy()

        # Subscribe first
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)

        # --- Notification from node1 ---
        msg_from_node1 = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }
        pubsub1.get_message.return_value = msg_from_node1
        pubsub2.get_message.return_value = None

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == EventType.SET

        # --- Notification from node2 ---
        pubsub1.get_message.return_value = None
        msg_from_node2 = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"del",
            "pattern": None,
        }
        pubsub2.get_message.return_value = msg_from_node2

        notification = await notifications.get_message(timeout=1.0)

        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == EventType.DEL

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test AsyncClusterKeyspaceNotifications as async context manager."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        pubsubs_by_node = {node1.name: pubsub1}
        cluster, _ = self._create_mock_cluster([node1], pubsubs_by_node)

        async with AsyncClusterKeyspaceNotifications(cluster) as notifications:
            # Manually inject the mock pubsub for testing
            notifications._node_pubsubs = pubsubs_by_node.copy()

            await notifications.subscribe(KeyspaceChannel("mykey"))
            assert notifications.subscribed

        # Should have closed pubsub
        pubsub1.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribed_property(self):
        """Test the subscribed property for cluster notifications."""
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        pubsubs_by_node = {node1.name: pubsub1}
        cluster, _ = self._create_mock_cluster([node1], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)
        notifications._node_pubsubs = pubsubs_by_node.copy()

        assert not notifications.subscribed

        await notifications.subscribe(KeyspaceChannel("mykey"))
        assert notifications.subscribed

        await notifications.unsubscribe(KeyspaceChannel("mykey"))
        assert not notifications.subscribed

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_ensure_node_pubsub_uses_adapter(self):
        """
        Test that _ensure_node_pubsub creates a PubSub backed by a
        _ClusterNodePoolAdapter wrapping the ClusterNode, instead of
        creating a standalone Redis client.
        """
        node, _ = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        # Set up connection_kwargs as ClusterNode does
        node.connection_kwargs = {
            "host": "127.0.0.1",
            "port": 7000,
            "password": "secret",
        }
        node.acquire_connection = Mock(return_value=MagicMock())
        node.release = Mock()

        cluster = Mock()
        cluster.get_primaries = Mock(return_value=[node])
        cluster.connection_kwargs = {}

        notifications = AsyncClusterKeyspaceNotifications(cluster)
        pubsub = await notifications._ensure_node_pubsub(node)

        # The PubSub's connection_pool should be a _ClusterNodePoolAdapter
        assert isinstance(pubsub.connection_pool, _ClusterNodePoolAdapter)
        # The adapter should wrap the original node
        assert pubsub.connection_pool._node is node
        # The adapter should expose the node's connection_kwargs
        assert pubsub.connection_pool.connection_kwargs is node.connection_kwargs

        # Calling again returns the same PubSub (cached)
        pubsub2 = await notifications._ensure_node_pubsub(node)
        assert pubsub2 is pubsub

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_refresh_subscriptions_recovers_broken_connections(self):
        """
        Test that refresh_subscriptions detects and re-creates broken pubsub connections.

        When an existing primary node's pubsub connection breaks (e.g., transient
        network issue) but the node remains a primary, the broken pubsub should be
        re-created during refresh.
        """
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Manually inject the mock pubsubs
        notifications._node_pubsubs = pubsubs_by_node.copy()

        # Subscribe first
        channel = KeyspaceChannel("mykey", db=0)
        await notifications.subscribe(channel)
        notifications._subscribed_channels = {"__keyspace@0__:mykey": None}

        # Simulate broken connection on node1 by setting is_connected to False
        mock_connection = Mock()
        mock_connection.is_connected = False
        pubsub1.connection = mock_connection

        # node2 has a working connection
        mock_connection2 = Mock()
        mock_connection2.is_connected = True
        pubsub2.connection = mock_connection2

        # Create a new pubsub for the re-creation
        new_pubsub1 = MagicMock()
        new_pubsub1.get_message = AsyncMock(return_value=None)
        new_pubsub1.subscribe = AsyncMock()
        new_pubsub1.psubscribe = AsyncMock()
        new_pubsub1.aclose = AsyncMock()

        # Mock the _ensure_node_pubsub to return new pubsub for node1
        original_ensure = notifications._ensure_node_pubsub

        async def mock_ensure(node):
            if node.name == node1.name:
                notifications._node_pubsubs[node.name] = new_pubsub1
                return new_pubsub1
            return await original_ensure(node)

        notifications._ensure_node_pubsub = mock_ensure

        # Call refresh_subscriptions
        await notifications.refresh_subscriptions()

        # Verify node1's pubsub was replaced
        assert notifications._node_pubsubs[node1.name] is new_pubsub1

        # Verify the new pubsub was subscribed
        new_pubsub1.subscribe.assert_called()

        # node2 should remain unchanged (connection was working)
        assert notifications._node_pubsubs[node2.name] is pubsub2

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_is_pubsub_connected_returns_false_for_broken_connection(self):
        """
        Test that _is_pubsub_connected correctly detects broken connections.
        """
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)

        pubsubs_by_node = {node1.name: pubsub1}
        cluster, _ = self._create_mock_cluster([node1], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Test with None connection
        pubsub1.connection = None
        assert notifications._is_pubsub_connected(pubsub1) is False

        # Test with connection that is not connected
        mock_connection = Mock()
        mock_connection.is_connected = False
        pubsub1.connection = mock_connection
        assert notifications._is_pubsub_connected(pubsub1) is False

        # Test with connected connection
        mock_connection.is_connected = True
        assert notifications._is_pubsub_connected(pubsub1) is True

        await notifications.aclose()

    @pytest.mark.asyncio
    async def test_node_failure_during_pattern_subscribe_does_not_lose_patterns(self):
        """
        Test that a node which fails during the pattern subscribe call and
        is re-created during the exact-channel subscribe call still receives
        both the patterns and the exact channels.

        Regression test for the bug where _execute_subscribe called
        _subscribe_to_all_nodes twice sequentially.  A node cleaned up
        during the first (pattern) call would be treated as "new" in the
        second (exact-channel) call and caught up from
        _subscribed_patterns — which had not been updated yet, so the
        patterns were silently lost.
        """
        node1, pubsub1 = self._create_mock_node("127.0.0.1:7000", "127.0.0.1", 7000)
        node2, pubsub2 = self._create_mock_node("127.0.0.1:7001", "127.0.0.1", 7001)

        pubsubs_by_node = {node1.name: pubsub1, node2.name: pubsub2}
        cluster, _ = self._create_mock_cluster([node1, node2], pubsubs_by_node)

        notifications = AsyncClusterKeyspaceNotifications(cluster)

        # Mock _ensure_node_pubsub to return our mock pubsubs and
        # populate _node_pubsubs like the real implementation does
        async def mock_ensure(node):
            notifications._node_pubsubs[node.name] = pubsubs_by_node[node.name]
            return pubsubs_by_node[node.name]

        notifications._ensure_node_pubsub = mock_ensure

        # Make node2's pubsub raise on the first psubscribe call
        # (simulating a transient failure during pattern subscription)
        pubsub2.psubscribe.side_effect = ConnectionError("connection lost")

        # Subscribe with both a pattern and an exact channel in one call
        pattern = KeyspaceChannel("user:*")  # pattern → psubscribe
        exact = KeyspaceChannel("mykey")  # exact   → subscribe
        await notifications.subscribe(pattern, exact)

        # node1 should have received both psubscribe and subscribe
        pubsub1.psubscribe.assert_called_once()
        pubsub1.subscribe.assert_called_once()

        # node2 failed during psubscribe, so it should have been cleaned
        # up entirely — it must NOT appear in _node_pubsubs with only the
        # exact channel subscribed.
        assert node2.name not in notifications._node_pubsubs

        # The tracking state should still record both subscriptions so that
        # refresh_subscriptions can fully re-subscribe node2 later.
        assert "__keyspace@0__:user:*" in notifications._subscribed_patterns
        assert "__keyspace@0__:mykey" in notifications._subscribed_channels

        await notifications.aclose()
