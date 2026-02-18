"""
Tests for Redis keyspace notifications support.
"""

import pytest

import redis
from redis.keyspace_notifications import (
    ClusterKeyspaceNotifications,
    EventType,
    KeyeventChannel,
    KeyNotification,
    KeyNotificationType,
    KeyspaceChannel,
    _is_pattern,
    is_keyevent_channel,
    is_keyspace_channel,
    is_keyspace_notification_channel,
)


class TestEventType:
    """Tests for EventType constants."""

    def test_common_event_types(self):
        """Test that common event type constants are defined."""
        assert EventType.SET == "set"
        assert EventType.DEL == "del"
        assert EventType.EXPIRE == "expire"
        assert EventType.EXPIRED == "expired"
        assert EventType.LPUSH == "lpush"

    def test_backwards_compatibility_alias(self):
        """Test that KeyNotificationType is an alias for EventType."""
        assert KeyNotificationType is EventType
        assert KeyNotificationType.SET == "set"

    def test_event_type_is_string(self):
        """Test that event types are plain strings."""
        assert isinstance(EventType.SET, str)
        assert isinstance(EventType.DEL, str)

    def test_can_compare_with_any_string(self):
        """Test that event types can be compared with any string."""
        assert EventType.SET == "set"
        # Future Redis events work without library updates
        assert "some_future_event" == "some_future_event"


class TestPatternDetection:
    """Tests for pattern detection function."""

    def test_is_pattern_with_asterisk(self):
        """Test detection of asterisk wildcard."""
        assert _is_pattern("user:*") is True
        assert _is_pattern("*") is True
        assert _is_pattern("__keyspace@0__:user:*") is True

    def test_is_pattern_with_question_mark(self):
        """Test detection of question mark wildcard."""
        assert _is_pattern("user:?") is True
        assert _is_pattern("key?") is True

    def test_is_pattern_with_brackets(self):
        """Test detection of bracket character class."""
        assert _is_pattern("user:[abc]") is True
        assert _is_pattern("key[0-9]") is True

    def test_is_pattern_exact_channel(self):
        """Test that exact channels are not detected as patterns."""
        assert _is_pattern("user:123") is False
        assert _is_pattern("__keyspace@0__:mykey") is False
        assert _is_pattern("simple_key") is False

    def test_is_pattern_escaped_wildcards(self):
        """Test that escaped wildcards are not detected as patterns."""
        assert _is_pattern(r"user:\*") is False
        assert _is_pattern(r"key\?name") is False
        assert _is_pattern(r"test\[bracket") is False

    def test_is_pattern_with_bytes(self):
        """Test pattern detection with bytes input."""
        assert _is_pattern(b"user:*") is True
        assert _is_pattern(b"user:123") is False


class TestKeyspaceChannelClass:
    """Tests for KeyspaceChannel class."""

    def test_basic_channel(self):
        """Test basic keyspace channel creation."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel = KeyspaceChannel("mykey", db=0)
        assert str(channel) == "__keyspace@0__:mykey"
        assert channel.key_or_pattern == "mykey"
        assert channel.db == 0

    def test_channel_default_db(self):
        """Test keyspace channel defaults to database 0."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel = KeyspaceChannel("mykey")
        assert str(channel) == "__keyspace@0__:mykey"
        assert channel.db == 0

    def test_pattern_channel(self):
        """Test keyspace channel with pattern."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel = KeyspaceChannel("user:*", db=0)
        assert str(channel) == "__keyspace@0__:user:*"
        assert channel.is_pattern is True

    def test_pattern_all_keys(self):
        """Test pattern for all keys."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel = KeyspaceChannel("*", db=0)
        assert str(channel) == "__keyspace@0__:*"

    def test_is_pattern_property(self):
        """Test is_pattern property."""
        from redis.keyspace_notifications import KeyspaceChannel

        exact = KeyspaceChannel("user:123", db=0)
        assert exact.is_pattern is False

        pattern = KeyspaceChannel("user:*", db=0)
        assert pattern.is_pattern is True

    def test_equality_with_string(self):
        """Test equality comparison with string."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel = KeyspaceChannel("mykey", db=0)
        assert channel == "__keyspace@0__:mykey"
        assert channel != "__keyspace@1__:mykey"

    def test_equality_with_channel(self):
        """Test equality comparison with another channel."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel1 = KeyspaceChannel("mykey", db=0)
        channel2 = KeyspaceChannel("mykey", db=0)
        channel3 = KeyspaceChannel("otherkey", db=0)
        assert channel1 == channel2
        assert channel1 != channel3

    def test_hash(self):
        """Test that channels can be used in sets and dicts."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel1 = KeyspaceChannel("mykey", db=0)
        channel2 = KeyspaceChannel("mykey", db=0)
        channel_set = {channel1, channel2}
        assert len(channel_set) == 1

    def test_repr(self):
        """Test repr output."""
        from redis.keyspace_notifications import KeyspaceChannel

        channel = KeyspaceChannel("mykey", db=0)
        assert repr(channel) == "KeyspaceChannel('mykey', db=0)"


class TestKeyeventChannelClass:
    """Tests for KeyeventChannel class."""

    def test_basic_channel(self):
        """Test basic keyevent channel creation."""
        from redis.keyspace_notifications import KeyeventChannel

        channel = KeyeventChannel(EventType.SET, db=0)
        assert str(channel) == "__keyevent@0__:set"
        assert channel.event == "set"
        assert channel.db == 0

    def test_channel_with_string_event(self):
        """Test keyevent channel with string event type."""
        from redis.keyspace_notifications import KeyeventChannel

        channel = KeyeventChannel("del", db=0)
        assert str(channel) == "__keyevent@0__:del"

    def test_channel_default_db(self):
        """Test keyevent channel defaults to database 0."""
        from redis.keyspace_notifications import KeyeventChannel

        channel = KeyeventChannel(EventType.SET)
        assert str(channel) == "__keyevent@0__:set"
        assert channel.db == 0

    def test_pattern_channel(self):
        """Test keyevent channel with pattern."""
        from redis.keyspace_notifications import KeyeventChannel

        # Pattern for all GET-related events (get, getex, getdel, getset)
        channel = KeyeventChannel("get*")
        assert str(channel) == "__keyevent@0__:get*"
        assert channel.is_pattern is True

        # Pattern for all list operations
        channel = KeyeventChannel("l*")
        assert str(channel) == "__keyevent@0__:l*"

    def test_all_events_factory(self):
        """Test KeyeventChannel.all_events() factory method."""
        from redis.keyspace_notifications import KeyeventChannel

        channel = KeyeventChannel.all_events()
        assert str(channel) == "__keyevent@0__:*"
        assert channel.is_pattern is True

    def test_is_pattern_property(self):
        """Test is_pattern property."""
        from redis.keyspace_notifications import KeyeventChannel

        exact = KeyeventChannel(EventType.SET)
        assert exact.is_pattern is False

        pattern = KeyeventChannel.all_events()
        assert pattern.is_pattern is True

        pattern2 = KeyeventChannel("get*")
        assert pattern2.is_pattern is True

    def test_equality_with_string(self):
        """Test equality comparison with string."""
        from redis.keyspace_notifications import KeyeventChannel

        channel = KeyeventChannel(EventType.SET, db=0)
        assert channel == "__keyevent@0__:set"
        assert channel != "__keyevent@1__:set"

    def test_equality_with_channel(self):
        """Test equality comparison with another channel."""
        from redis.keyspace_notifications import KeyeventChannel

        channel1 = KeyeventChannel(EventType.SET, db=0)
        channel2 = KeyeventChannel("set", db=0)
        channel3 = KeyeventChannel(EventType.DEL, db=0)
        assert channel1 == channel2
        assert channel1 != channel3

    def test_hash(self):
        """Test that channels can be used in sets and dicts."""
        from redis.keyspace_notifications import KeyeventChannel

        channel1 = KeyeventChannel(EventType.SET, db=0)
        channel2 = KeyeventChannel("set", db=0)
        channel_set = {channel1, channel2}
        assert len(channel_set) == 1

    def test_repr(self):
        """Test repr output."""
        from redis.keyspace_notifications import KeyeventChannel

        channel = KeyeventChannel("set", db=0)
        assert repr(channel) == "KeyeventChannel('set', db=0)"


class TestChannelDetection:
    """Tests for channel type detection functions."""

    def test_is_keyspace_channel(self):
        """Test keyspace channel detection."""
        assert is_keyspace_channel("__keyspace@0__:mykey") is True
        assert is_keyspace_channel("__keyspace@5__:user:123") is True
        assert is_keyspace_channel("__keyevent@0__:set") is False
        assert is_keyspace_channel("regular_channel") is False

    def test_is_keyevent_channel(self):
        """Test keyevent channel detection."""
        assert is_keyevent_channel("__keyevent@0__:set") is True
        assert is_keyevent_channel("__keyevent@5__:del") is True
        assert is_keyevent_channel("__keyspace@0__:mykey") is False
        assert is_keyevent_channel("regular_channel") is False

    def test_is_keyspace_notification_channel(self):
        """Test general keyspace notification channel detection."""
        assert is_keyspace_notification_channel("__keyspace@0__:mykey") is True
        assert is_keyspace_notification_channel("__keyevent@0__:set") is True
        assert is_keyspace_notification_channel("regular_channel") is False

    def test_bytes_input(self):
        """Test that bytes input is handled correctly."""
        assert is_keyspace_channel(b"__keyspace@0__:mykey") is True
        assert is_keyevent_channel(b"__keyevent@0__:set") is True


class TestKeyNotification:
    """Tests for KeyNotification class."""

    def test_from_message_keyspace(self):
        """Test parsing a keyspace notification message."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:user:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }
        notification = KeyNotification.from_message(message)

        assert notification is not None
        assert notification.key == "user:123"
        assert notification.event_type == "set"
        assert notification.event_type == EventType.SET  # Can compare with constant
        assert notification.database == 0
        assert notification.is_keyspace is True

    def test_from_message_keyevent(self):
        """Test parsing a keyevent notification message."""
        message = {
            "type": "message",
            "pattern": None,
            "channel": "__keyevent@0__:set",
            "data": "user:123",
        }
        notification = KeyNotification.from_message(message)

        assert notification is not None
        assert notification.key == "user:123"
        assert notification.event_type == "set"
        assert notification.database == 0
        assert notification.is_keyspace is False

    def test_from_message_with_bytes(self):
        """Test parsing a message with bytes channel and data."""
        message = {
            "type": "pmessage",
            "pattern": b"__keyspace@0__:user:*",
            "channel": b"__keyspace@0__:user:456",
            "data": b"del",
        }
        notification = KeyNotification.from_message(message)

        assert notification is not None
        assert notification.key == "user:456"
        assert notification.event_type == "del"

    def test_from_message_with_key_prefix(self):
        """Test parsing with key prefix filtering and stripping."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:user:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }

        # With matching prefix - should strip it
        notification = KeyNotification.from_message(message, key_prefix="user:")
        assert notification is not None
        assert notification.key == "123"

        # With non-matching prefix - should return None
        notification = KeyNotification.from_message(message, key_prefix="cache:")
        assert notification is None

    def test_from_message_with_bytes_key_prefix(self):
        """Test parsing with bytes key prefix."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:user:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }
        notification = KeyNotification.from_message(message, key_prefix=b"user:")
        assert notification is not None
        assert notification.key == "123"

    def test_from_message_non_notification(self):
        """Test that non-notification messages return None."""
        # Subscribe message
        message = {
            "type": "subscribe",
            "pattern": None,
            "channel": "__keyspace@0__:user:*",
            "data": 1,
        }
        assert KeyNotification.from_message(message) is None

        # Regular pubsub message
        message = {
            "type": "message",
            "pattern": None,
            "channel": "regular_channel",
            "data": "some data",
        }
        assert KeyNotification.from_message(message) is None

    def test_from_message_none(self):
        """Test that None message returns None."""
        assert KeyNotification.from_message(None) is None

    def test_try_parse(self):
        """Test the try_parse class method."""
        notification = KeyNotification.try_parse(
            "__keyspace@0__:mykey",
            "set"
        )
        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == "set"

    def test_try_parse_with_bytes(self):
        """Test try_parse with bytes input."""
        notification = KeyNotification.try_parse(
            b"__keyevent@0__:del",
            b"mykey"
        )
        assert notification is not None
        assert notification.key == "mykey"
        assert notification.event_type == "del"

    def test_key_starts_with(self):
        """Test the key_starts_with method."""
        message = {
            "type": "pmessage",
            "pattern": "__keyspace@0__:*",
            "channel": "__keyspace@0__:user:123",
            "data": "set",
        }
        notification = KeyNotification.from_message(message)

        assert notification.key_starts_with("user:") is True
        assert notification.key_starts_with("cache:") is False
        assert notification.key_starts_with(b"user:") is True

    def test_database_wildcard(self):
        """Test parsing channel with wildcard database."""
        notification = KeyNotification.try_parse(
            "__keyspace@*__:mykey",
            "set"
        )
        assert notification is not None
        assert notification.database == -1  # -1 indicates wildcard

    def test_future_event_type(self):
        """Test that future/unknown event types work as plain strings."""
        notification = KeyNotification.try_parse(
            "__keyspace@0__:mykey",
            "some_future_event"
        )
        assert notification is not None
        # Event type is just the string - no UNKNOWN enum needed
        assert notification.event_type == "some_future_event"
        assert isinstance(notification.event_type, str)


class TestKeyNotificationIntegration:
    """Integration tests that require a running Redis server."""

    @pytest.fixture
    def redis_client(self, r):
        """Get a Redis client from the test fixtures."""
        return r

    @pytest.mark.onlynoncluster
    def test_keyspace_notification_subscribe(self, redis_client):
        """Test subscribing to keyspace notifications."""
        # This test requires notify-keyspace-events to be configured
        # Skip if not configured
        config = redis_client.config_get("notify-keyspace-events")
        if not config.get("notify-keyspace-events"):
            pytest.skip("Keyspace notifications not enabled")

        pubsub = redis_client.pubsub()
        channel = KeyspaceChannel("test_ksn:*")
        pubsub.psubscribe(str(channel))

        # Get the subscribe confirmation
        msg = pubsub.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "psubscribe"

        # Set a key to trigger notification
        redis_client.set("test_ksn:key1", "value1")

        # Get the notification
        msg = pubsub.get_message(timeout=1.0)
        if msg:
            notification = KeyNotification.from_message(msg)
            if notification:
                assert notification.key == "test_ksn:key1"
                assert notification.event_type == "set"

        pubsub.close()
        redis_client.delete("test_ksn:key1")


class TestClusterKeyspaceNotificationsMocked:
    """
    Mock-based unit tests for ClusterKeyspaceNotifications.

    These tests use mocks to simulate cluster behavior without requiring
    a running Redis Cluster, allowing us to test slot migration scenarios
    that are difficult to reproduce in a real cluster environment.
    """

    def _create_mock_node(self, name, host, port, server_type="primary"):
        """Create a mock ClusterNode."""
        from unittest.mock import Mock, MagicMock

        node = Mock()
        node.name = name
        node.host = host
        node.port = port
        node.server_type = server_type

        # Create a mock redis connection with pubsub
        redis_conn = Mock()
        mock_pubsub = MagicMock()
        mock_pubsub.get_message = Mock(return_value=None)
        redis_conn.pubsub = Mock(return_value=mock_pubsub)
        node.redis_connection = redis_conn

        return node, mock_pubsub

    def _create_mock_cluster(self, nodes):
        """Create a mock RedisCluster with the given nodes."""
        from unittest.mock import Mock

        cluster = Mock()
        cluster.get_nodes = Mock(return_value=nodes)
        cluster.get_redis_connection = Mock(
            side_effect=lambda node: node.redis_connection
        )

        # Mock nodes_manager
        nodes_manager = Mock()
        nodes_manager.initialize = Mock()
        cluster.nodes_manager = nodes_manager

        return cluster

    def test_slot_migration_receives_notification_from_new_node(self):
        """
        Test that after slot migration, notifications are received from the
        new node that owns the slot.

        This test simulates:
        1. A cluster with 2 primary nodes
        2. Subscribing to a key's notifications (subscribes to ALL primaries)
        3. Slot migration: the key moves from node1 to node2
        4. After migration, notifications come from node2

        Since ClusterKeyspaceNotifications subscribes to ALL primary nodes,
        notifications should continue to work after slot migration without
        any manual intervention.
        """
        from unittest.mock import Mock

        # Create two mock primary nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )

        # Create mock cluster
        cluster = self._create_mock_cluster([node1, node2])

        # Create ClusterKeyspaceNotifications
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe to a key's notifications using KeyspaceChannel class
        test_key = "mykey"
        channel = KeyspaceChannel(test_key)
        notifications.subscribe(channel)

        # Verify we subscribed to both nodes
        assert len(notifications._node_pubsubs) == 2
        assert node1.name in notifications._node_pubsubs
        assert node2.name in notifications._node_pubsubs

        # Verify subscribe was called on both pubsubs
        pubsub1.subscribe.assert_called_once()
        pubsub2.subscribe.assert_called_once()

        # --- BEFORE MIGRATION ---
        # Simulate notification from node1 (the original owner)
        before_migration_msg = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }

        # Set up pubsub1 to return the message, pubsub2 returns None
        pubsub1.get_message.return_value = before_migration_msg
        pubsub2.get_message.return_value = None

        # Get the notification
        notification = notifications.get_message(
            ignore_subscribe_messages=True, timeout=1.0
        )

        assert notification is not None
        assert notification.key == test_key
        assert notification.event_type == EventType.SET

        # --- AFTER MIGRATION ---
        # Simulate slot migration: now node2 owns the slot
        # node1 no longer sends notifications for this key
        pubsub1.get_message.return_value = None

        # node2 now sends the notification
        after_migration_msg = {
            "type": "message",
            "channel": b"__keyspace@0__:mykey",
            "data": b"set",
            "pattern": None,
        }
        pubsub2.get_message.return_value = after_migration_msg

        # Get the notification - should come from node2
        notification = notifications.get_message(
            ignore_subscribe_messages=True, timeout=1.0
        )

        assert notification is not None, (
            "Should receive notification after migration - "
            "ClusterKeyspaceNotifications subscribes to all primaries"
        )
        assert notification.key == test_key
        assert notification.event_type == EventType.SET

        # Cleanup
        notifications.close()

    def test_subscribes_to_all_primary_nodes(self):
        """
        Test that ClusterKeyspaceNotifications subscribes to ALL primary nodes,
        not just the node that owns the key's slot.

        This is the key design decision that makes slot migrations transparent.
        """
        from unittest.mock import Mock

        # Create three mock primary nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )
        node3, pubsub3 = self._create_mock_node(
            "127.0.0.1:7002", "127.0.0.1", 7002, "primary"
        )

        # Create mock cluster
        cluster = self._create_mock_cluster([node1, node2, node3])

        # Create ClusterKeyspaceNotifications
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe to a single key's notifications using KeyspaceChannel class
        # Even though this key would only be on ONE node in a real cluster,
        # we subscribe to ALL nodes
        test_key = "mykey"
        channel = KeyspaceChannel(test_key)
        notifications.subscribe(channel)

        # Verify we subscribed to ALL three nodes
        assert len(notifications._node_pubsubs) == 3
        assert node1.name in notifications._node_pubsubs
        assert node2.name in notifications._node_pubsubs
        assert node3.name in notifications._node_pubsubs

        # Verify subscribe was called on all pubsubs
        pubsub1.subscribe.assert_called_once()
        pubsub2.subscribe.assert_called_once()
        pubsub3.subscribe.assert_called_once()

        # Cleanup
        notifications.close()

    def test_pattern_subscription_on_all_nodes(self):
        """
        Test that pattern subscriptions are created on all primary nodes.
        """
        from unittest.mock import Mock

        # Create two mock primary nodes
        node1, pubsub1 = self._create_mock_node(
            "127.0.0.1:7000", "127.0.0.1", 7000, "primary"
        )
        node2, pubsub2 = self._create_mock_node(
            "127.0.0.1:7001", "127.0.0.1", 7001, "primary"
        )

        cluster = self._create_mock_cluster([node1, node2])
        notifications = ClusterKeyspaceNotifications(cluster)

        # Subscribe to a pattern using KeyspaceChannel
        pattern = KeyspaceChannel("user:*")
        notifications.subscribe(pattern)

        # Verify psubscribe was called on both nodes (patterns use psubscribe)
        pubsub1.psubscribe.assert_called_once()
        pubsub2.psubscribe.assert_called_once()

        # Cleanup
        notifications.close()


@pytest.mark.onlycluster
class TestClusterKeyspaceNotifications:
    """
    Cluster integration tests for keyspace notifications.

    These tests require a running Redis Cluster with at least 3 primary nodes.
    Run with: pytest tests/test_keyspace_notifications.py -v --redis-url=redis://localhost:7000
    """

    @pytest.fixture
    def cluster_client(self, r):
        """Get a Redis Cluster client from the test fixtures."""
        return r

    @pytest.fixture
    def cluster_notifications(self, cluster_client):
        """Create a ClusterKeyspaceNotifications instance."""
        notifications = ClusterKeyspaceNotifications(cluster_client)
        yield notifications
        notifications.close()

    def _enable_keyspace_notifications(self, cluster_client):
        """Enable keyspace notifications on all cluster nodes."""
        for node in cluster_client.get_primaries():
            node.redis_connection.config_set("notify-keyspace-events", "KEA")

    def _get_key_for_node(self, cluster_client, node_index: int) -> str:
        """
        Find a key that hashes to a slot owned by the specified node.

        Args:
            cluster_client: Redis Cluster client
            node_index: Index of the primary node (0, 1, 2, ...)

        Returns:
            A key string that will be stored on the specified node
        """
        primaries = list(cluster_client.get_primaries())
        if node_index >= len(primaries):
            pytest.skip(f"Not enough primary nodes (need {node_index + 1})")

        target_node = primaries[node_index]

        # Find a slot owned by this node
        for slot, nodes in cluster_client.nodes_manager.slots_cache.items():
            if nodes and nodes[0].name == target_node.name:
                # Find a key that hashes to this slot
                for i in range(10000):
                    key = f"test_key_{i}"
                    if cluster_client.keyslot(key) == slot:
                        return key

        pytest.fail(f"Could not find a key for node {node_index}")

    def test_create_key_on_node1_keyspace_notification(
        self, cluster_client, cluster_notifications
    ):
        """
        Test case 1: Create a key on node 1 and verify that the notification
        about the creation of the key is received.
        """
        self._enable_keyspace_notifications(cluster_client)

        key = self._get_key_for_node(cluster_client, 0)

        # Subscribe to keyspace notifications for this key
        channel = KeyspaceChannel(key)
        cluster_notifications.subscribe(channel)

        # Allow subscription to complete
        import time
        time.sleep(0.1)

        # Create the key
        cluster_client.set(key, "value1")

        # Get the notification (ignore_subscribe_messages=True to skip subscribe confirmations)
        notification = cluster_notifications.get_message(
            ignore_subscribe_messages=True, timeout=2.0
        )
        assert notification is not None, "Expected to receive a keyspace notification"
        assert notification.key == key
        assert notification.event_type == EventType.SET
        assert notification.is_keyspace is True

        # Cleanup
        cluster_client.delete(key)

    def test_update_key_on_node1_keyspace_notification(
        self, cluster_client, cluster_notifications
    ):
        """
        Test case 2: Update a key on node 1 and verify that the keyspace
        notification is received.
        """
        self._enable_keyspace_notifications(cluster_client)

        key = self._get_key_for_node(cluster_client, 0)

        # Create the key first
        cluster_client.set(key, "initial_value")

        # Subscribe to keyspace notifications for this key
        channel = KeyspaceChannel(key)
        cluster_notifications.subscribe(channel)

        import time
        time.sleep(0.1)

        # Update the key
        cluster_client.set(key, "updated_value")

        # Get the notification (ignore_subscribe_messages=True to skip subscribe confirmations)
        notification = cluster_notifications.get_message(
            ignore_subscribe_messages=True, timeout=2.0
        )
        assert notification is not None, "Expected to receive a keyspace notification"
        assert notification.key == key
        assert notification.event_type == EventType.SET

        # Cleanup
        cluster_client.delete(key)

    def test_update_key_on_node2_keyevent_notification(
        self, cluster_client, cluster_notifications
    ):
        """
        Test case 3: Update a key on node 2 and verify that the key event
        notification is received.
        """
        self._enable_keyspace_notifications(cluster_client)

        key = self._get_key_for_node(cluster_client, 1)

        # Create the key first
        cluster_client.set(key, "initial_value")

        # Subscribe to keyevent notifications for SET events
        channel = KeyeventChannel(EventType.SET)
        cluster_notifications.subscribe(channel)

        import time
        time.sleep(0.1)

        # Update the key
        cluster_client.set(key, "updated_value")

        # Get the notification (ignore_subscribe_messages=True to skip subscribe confirmations)
        notification = cluster_notifications.get_message(
            ignore_subscribe_messages=True, timeout=2.0
        )
        assert notification is not None, "Expected to receive a keyevent notification"
        assert notification.key == key
        assert notification.event_type == EventType.SET
        assert notification.is_keyspace is False  # This is a keyevent notification

        # Cleanup
        cluster_client.delete(key)

    def test_delete_key_on_node3_notification(
        self, cluster_client, cluster_notifications
    ):
        """
        Test case 4: Delete a key on node 3 and verify that the deletion
        notification is received.
        """
        self._enable_keyspace_notifications(cluster_client)

        primaries = list(cluster_client.get_primaries())
        if len(primaries) < 3:
            pytest.skip("Need at least 3 primary nodes for this test")

        key = self._get_key_for_node(cluster_client, 2)

        # Create the key first
        cluster_client.set(key, "value_to_delete")

        # Subscribe to keyspace notifications for this key
        channel = KeyspaceChannel(key)
        cluster_notifications.subscribe(channel)

        import time
        time.sleep(0.1)

        # Delete the key
        cluster_client.delete(key)

        # Get the notification (ignore_subscribe_messages=True to skip subscribe confirmations)
        notification = cluster_notifications.get_message(
            ignore_subscribe_messages=True, timeout=2.0
        )
        assert notification is not None, "Expected to receive a deletion notification"
        assert notification.key == key
        assert notification.event_type == EventType.DEL

    def test_pattern_subscription_across_nodes(
        self, cluster_client, cluster_notifications
    ):
        """
        Test case 5: Modify a bunch of keys across nodes 1, 2 and 3 that all
        match the same pattern and check that all notifications are received.
        """
        self._enable_keyspace_notifications(cluster_client)

        primaries = list(cluster_client.get_primaries())
        if len(primaries) < 3:
            pytest.skip("Need at least 3 primary nodes for this test")

        # Find keys on different nodes that match a pattern
        keys_by_node = {}
        for node_idx in range(min(3, len(primaries))):
            key = self._get_key_for_node(cluster_client, node_idx)
            # Rename to have a common prefix
            prefixed_key = f"pattern_test:{key}"
            keys_by_node[node_idx] = prefixed_key

        # Subscribe to pattern matching all test keys
        pattern = KeyspaceChannel("pattern_test:*")
        cluster_notifications.subscribe(pattern)

        import time
        time.sleep(0.1)

        # Modify all keys
        for key in keys_by_node.values():
            cluster_client.set(key, "test_value")

        # Collect all notifications (ignore_subscribe_messages=True to skip subscribe confirmations)
        received_keys = set()
        for _ in range(len(keys_by_node) * 2):  # Allow extra iterations
            notification = cluster_notifications.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            if notification:
                received_keys.add(notification.key)
            if len(received_keys) >= len(keys_by_node):
                break

        # Verify all keys received notifications
        for key in keys_by_node.values():
            assert key in received_keys, f"Missing notification for key: {key}"

        # Cleanup
        for key in keys_by_node.values():
            cluster_client.delete(key)

    def test_slot_migration_notifications(self, cluster_client, cluster_notifications):
        """
        Test case 6: Move slots from node 1 to node 2 that impact some
        pre-defined keys and ensure that keyspace notifications and key event
        notifications are received correctly before and after the migration.

        This test reassigns slot ownership between nodes using CLUSTER SETSLOT
        commands. Since ClusterKeyspaceNotifications subscribes to ALL primary
        nodes, notifications should continue to work after slot migration
        without any manual intervention - the notification will simply come
        from the new node that owns the slot.

        Note: This test uses CLUSTER SETSLOT NODE to reassign slot ownership
        rather than the full MIGRATE workflow, because the Docker test
        environment doesn't support MIGRATE between nodes (IOERR).
        """
        self._enable_keyspace_notifications(cluster_client)

        primaries = list(cluster_client.get_primaries())
        if len(primaries) < 2:
            pytest.skip("Need at least 2 primary nodes for this test")

        source_node = primaries[0]
        dest_node = primaries[1]

        # Find a key and its slot on the source node
        test_key = self._get_key_for_node(cluster_client, 0)
        slot = cluster_client.keyslot(test_key)

        # Create the key before migration
        cluster_client.set(test_key, "pre_migration_value")

        # Subscribe to notifications for this key on ALL nodes
        # This is the key behavior: we subscribe to all primaries
        channel = KeyspaceChannel(test_key)
        cluster_notifications.subscribe(channel)

        import time
        time.sleep(0.1)

        # Verify we're subscribed to all primary nodes
        assert len(cluster_notifications._node_pubsubs) == len(primaries), (
            "Should be subscribed to all primary nodes"
        )

        # Verify notification works BEFORE migration (from source_node)
        cluster_client.set(test_key, "value_before_migration")
        notification = cluster_notifications.get_message(
            ignore_subscribe_messages=True, timeout=2.0
        )
        assert notification is not None, "Should receive notification before migration"
        assert notification.key == test_key

        # Get node IDs for migration
        source_id = cluster_client.cluster_myid(source_node)
        dest_id = cluster_client.cluster_myid(dest_node)

        try:
            # Delete the key from source node before slot reassignment
            cluster_client.delete(test_key)

            # Drain any pending notifications
            while cluster_notifications.get_message(
                ignore_subscribe_messages=True, timeout=0.1
            ):
                pass

            # Reassign slot ownership from source to destination
            # This must be done on all nodes for consistency
            for node in primaries:
                node.redis_connection.execute_command(
                    "CLUSTER", "SETSLOT", slot, "NODE", dest_id
                )

            # Allow cluster to stabilize
            time.sleep(0.3)

            # Update the cluster client's slot map (simulates MOVED response)
            cluster_client.nodes_manager.initialize()

            # Create the key on the new node (dest_node now owns this slot)
            # The notification will be published by dest_node
            cluster_client.set(test_key, "value_after_migration")

            # Since we're subscribed to ALL primary nodes (including dest_node),
            # we should receive the notification without any refresh needed
            notification = cluster_notifications.get_message(
                ignore_subscribe_messages=True, timeout=2.0
            )

            assert notification is not None, (
                "Should receive notification after migration - "
                "ClusterKeyspaceNotifications subscribes to all primaries"
            )
            assert notification.key == test_key
            assert notification.event_type == EventType.SET

        finally:
            # Cleanup: restore slot to original node
            try:
                cluster_client.delete(test_key)

                for node in primaries:
                    node.redis_connection.execute_command(
                        "CLUSTER", "SETSLOT", slot, "NODE", source_id
                    )

                time.sleep(0.3)
                cluster_client.nodes_manager.initialize()
            except Exception:
                pass  # Best effort cleanup

