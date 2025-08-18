import threading
from unittest.mock import Mock, patch, MagicMock
import pytest

from redis.maintenance_events import (
    MaintenanceEvent,
    NodeMovingEvent,
    NodeMigratingEvent,
    NodeMigratedEvent,
    NodeFailingOverEvent,
    NodeFailedOverEvent,
    MaintenanceEventsConfig,
    MaintenanceEventPoolHandler,
    MaintenanceEventConnectionHandler,
    MaintenanceState,
)


class TestMaintenanceEvent:
    """Test the base MaintenanceEvent class functionality through concrete subclasses."""

    def test_abstract_class_cannot_be_instantiated(self):
        """Test that MaintenanceEvent cannot be instantiated directly."""
        with patch("time.monotonic", return_value=1000):
            with pytest.raises(TypeError):
                MaintenanceEvent(id=1, ttl=10)  # type: ignore

    def test_init_through_subclass(self):
        """Test MaintenanceEvent initialization through concrete subclass."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMovingEvent(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )
            assert event.id == 1
            assert event.ttl == 10
            assert event.creation_time == 1000
            assert event.expire_at == 1010

    @pytest.mark.parametrize(
        ("current_time", "expected_expired_state"),
        [
            (1005, False),
            (1015, True),
        ],
    )
    def test_is_expired(self, current_time, expected_expired_state):
        """Test is_expired returns False for non-expired event."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMovingEvent(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )

        with patch("time.monotonic", return_value=current_time):
            assert event.is_expired() == expected_expired_state

    def test_is_expired_exact_boundary(self):
        """Test is_expired at exact expiration boundary."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMovingEvent(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )

        with patch("time.monotonic", return_value=1010):  # Exactly at expiration
            assert not event.is_expired()

        with patch("time.monotonic", return_value=1011):  # 1 second past expiration
            assert event.is_expired()


class TestNodeMovingEvent:
    """Test the NodeMovingEvent class."""

    def test_init(self):
        """Test NodeMovingEvent initialization."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMovingEvent(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )
            assert event.id == 1
            assert event.new_node_host == "localhost"
            assert event.new_node_port == 6379
            assert event.ttl == 10
            assert event.creation_time == 1000

    def test_repr(self):
        """Test NodeMovingEvent string representation."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMovingEvent(
                id=1, new_node_host="localhost", new_node_port=6379, ttl=10
            )

        with patch("time.monotonic", return_value=1005):  # 5 seconds later
            repr_str = repr(event)
            assert "NodeMovingEvent" in repr_str
            assert "id=1" in repr_str
            assert "new_node_host='localhost'" in repr_str
            assert "new_node_port=6379" in repr_str
            assert "ttl=10" in repr_str
            assert "remaining=5.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_same_id_host_port(self):
        """Test equality for events with same id, host, and port."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=20
        )  # Different TTL
        assert event1 == event2

    def test_equality_same_id_different_host(self):
        """Test inequality for events with same id but different host."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="host1", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=1, new_node_host="host2", new_node_port=6379, ttl=10
        )
        assert event1 != event2

    def test_equality_same_id_different_port(self):
        """Test inequality for events with same id but different port."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6380, ttl=10
        )
        assert event1 != event2

    def test_equality_different_id(self):
        """Test inequality for events with different id."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=2, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        assert event1 != event2

    def test_equality_different_type(self):
        """Test inequality for events of different types."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMigratingEvent(id=1, ttl=10)
        assert event1 != event2

    def test_hash_same_id_host_port(self):
        """Test hash consistency for events with same id, host, and port."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=20
        )  # Different TTL
        assert hash(event1) == hash(event2)

    def test_hash_different_host(self):
        """Test hash difference for events with different host."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="host1", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=1, new_node_host="host2", new_node_port=6379, ttl=10
        )
        assert hash(event1) != hash(event2)

    def test_hash_different_port(self):
        """Test hash difference for events with different port."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6380, ttl=10
        )
        assert hash(event1) != hash(event2)

    def test_hash_different_id(self):
        """Test hash difference for events with different id."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=2, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        assert hash(event1) != hash(event2)

    def test_set_functionality(self):
        """Test that events can be used in sets correctly."""
        event1 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        event2 = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=20
        )  # Same id, host, port - should be considered the same
        event3 = NodeMovingEvent(
            id=1, new_node_host="host2", new_node_port=6380, ttl=10
        )  # Same id but different host/port - should be different
        event4 = NodeMovingEvent(
            id=2, new_node_host="localhost", new_node_port=6379, ttl=10
        )  # Different id - should be different

        event_set = {event1, event2, event3, event4}
        assert len(event_set) == 3  # event1 and event2 should be considered the same


class TestNodeMigratingEvent:
    """Test the NodeMigratingEvent class."""

    def test_init(self):
        """Test NodeMigratingEvent initialization."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMigratingEvent(id=1, ttl=5)
            assert event.id == 1
            assert event.ttl == 5
            assert event.creation_time == 1000

    def test_repr(self):
        """Test NodeMigratingEvent string representation."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMigratingEvent(id=1, ttl=5)

        with patch("time.monotonic", return_value=1002):  # 2 seconds later
            repr_str = repr(event)
            assert "NodeMigratingEvent" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=3.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeMigratingEvent."""
        event1 = NodeMigratingEvent(id=1, ttl=5)
        event2 = NodeMigratingEvent(id=1, ttl=10)  # Same id, different ttl
        event3 = NodeMigratingEvent(id=2, ttl=5)  # Different id

        assert event1 == event2
        assert event1 != event3
        assert hash(event1) == hash(event2)
        assert hash(event1) != hash(event3)


class TestNodeMigratedEvent:
    """Test the NodeMigratedEvent class."""

    def test_init(self):
        """Test NodeMigratedEvent initialization."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMigratedEvent(id=1)
            assert event.id == 1
            assert event.ttl == NodeMigratedEvent.DEFAULT_TTL
            assert event.creation_time == 1000

    def test_default_ttl(self):
        """Test that DEFAULT_TTL is used correctly."""
        assert NodeMigratedEvent.DEFAULT_TTL == 5
        event = NodeMigratedEvent(id=1)
        assert event.ttl == 5

    def test_repr(self):
        """Test NodeMigratedEvent string representation."""
        with patch("time.monotonic", return_value=1000):
            event = NodeMigratedEvent(id=1)

        with patch("time.monotonic", return_value=1001):  # 1 second later
            repr_str = repr(event)
            assert "NodeMigratedEvent" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=4.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeMigratedEvent."""
        event1 = NodeMigratedEvent(id=1)
        event2 = NodeMigratedEvent(id=1)  # Same id
        event3 = NodeMigratedEvent(id=2)  # Different id

        assert event1 == event2
        assert event1 != event3
        assert hash(event1) == hash(event2)
        assert hash(event1) != hash(event3)


class TestNodeFailingOverEvent:
    """Test the NodeFailingOverEvent class."""

    def test_init(self):
        """Test NodeFailingOverEvent initialization."""
        with patch("time.monotonic", return_value=1000):
            event = NodeFailingOverEvent(id=1, ttl=5)
            assert event.id == 1
            assert event.ttl == 5
            assert event.creation_time == 1000

    def test_repr(self):
        """Test NodeFailingOverEvent string representation."""
        with patch("time.monotonic", return_value=1000):
            event = NodeFailingOverEvent(id=1, ttl=5)

        with patch("time.monotonic", return_value=1002):  # 2 seconds later
            repr_str = repr(event)
            assert "NodeFailingOverEvent" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=3.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeFailingOverEvent."""
        event1 = NodeFailingOverEvent(id=1, ttl=5)
        event2 = NodeFailingOverEvent(id=1, ttl=10)  # Same id, different ttl
        event3 = NodeFailingOverEvent(id=2, ttl=5)  # Different id

        assert event1 == event2
        assert event1 != event3
        assert hash(event1) == hash(event2)
        assert hash(event1) != hash(event3)


class TestNodeFailedOverEvent:
    """Test the NodeFailedOverEvent class."""

    def test_init(self):
        """Test NodeFailedOverEvent initialization."""
        with patch("time.monotonic", return_value=1000):
            event = NodeFailedOverEvent(id=1)
            assert event.id == 1
            assert event.ttl == NodeFailedOverEvent.DEFAULT_TTL
            assert event.creation_time == 1000

    def test_default_ttl(self):
        """Test that DEFAULT_TTL is used correctly."""
        assert NodeFailedOverEvent.DEFAULT_TTL == 5
        event = NodeFailedOverEvent(id=1)
        assert event.ttl == 5

    def test_repr(self):
        """Test NodeFailedOverEvent string representation."""
        with patch("time.monotonic", return_value=1000):
            event = NodeFailedOverEvent(id=1)

        with patch("time.monotonic", return_value=1001):  # 1 second later
            repr_str = repr(event)
            assert "NodeFailedOverEvent" in repr_str
            assert "id=1" in repr_str
            assert "ttl=5" in repr_str
            assert "remaining=4.0s" in repr_str
            assert "expired=False" in repr_str

    def test_equality_and_hash(self):
        """Test equality and hash for NodeFailedOverEvent."""
        event1 = NodeFailedOverEvent(id=1)
        event2 = NodeFailedOverEvent(id=1)  # Same id
        event3 = NodeFailedOverEvent(id=2)  # Different id

        assert event1 == event2
        assert event1 != event3
        assert hash(event1) == hash(event2)
        assert hash(event1) != hash(event3)


class TestMaintenanceEventsConfig:
    """Test the MaintenanceEventsConfig class."""

    def test_init_defaults(self):
        """Test MaintenanceEventsConfig initialization with defaults."""
        config = MaintenanceEventsConfig()
        assert config.enabled is False
        assert config.proactive_reconnect is True
        assert config.relax_timeout == 20

    def test_init_custom_values(self):
        """Test MaintenanceEventsConfig initialization with custom values."""
        config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=False, relax_timeout=30
        )
        assert config.enabled is True
        assert config.proactive_reconnect is False
        assert config.relax_timeout == 30

    def test_repr(self):
        """Test MaintenanceEventsConfig string representation."""
        config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=False, relax_timeout=30
        )
        repr_str = repr(config)
        assert "MaintenanceEventsConfig" in repr_str
        assert "enabled=True" in repr_str
        assert "proactive_reconnect=False" in repr_str
        assert "relax_timeout=30" in repr_str

    def test_is_relax_timeouts_enabled_true(self):
        """Test is_relax_timeouts_enabled returns True for positive timeout."""
        config = MaintenanceEventsConfig(relax_timeout=20)
        assert config.is_relax_timeouts_enabled() is True

    def test_is_relax_timeouts_enabled_false(self):
        """Test is_relax_timeouts_enabled returns False for -1 timeout."""
        config = MaintenanceEventsConfig(relax_timeout=-1)
        assert config.is_relax_timeouts_enabled() is False

    def test_is_relax_timeouts_enabled_zero(self):
        """Test is_relax_timeouts_enabled returns True for zero timeout."""
        config = MaintenanceEventsConfig(relax_timeout=0)
        assert config.is_relax_timeouts_enabled() is True

    def test_is_relax_timeouts_enabled_none(self):
        """Test is_relax_timeouts_enabled returns True for None timeout."""
        config = MaintenanceEventsConfig(relax_timeout=None)
        assert config.is_relax_timeouts_enabled() is True

    def test_relax_timeout_none_is_saved_as_none(self):
        """Test that None value for relax_timeout is saved as None."""
        config = MaintenanceEventsConfig(relax_timeout=None)
        assert config.relax_timeout is None


class TestMaintenanceEventPoolHandler:
    """Test the MaintenanceEventPoolHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_pool = Mock()
        self.mock_pool._lock = MagicMock()
        self.mock_pool._lock.__enter__.return_value = None
        self.mock_pool._lock.__exit__.return_value = None
        self.config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=True, relax_timeout=20
        )
        self.handler = MaintenanceEventPoolHandler(self.mock_pool, self.config)

    def test_init(self):
        """Test MaintenanceEventPoolHandler initialization."""
        assert self.handler.pool == self.mock_pool
        assert self.handler.config == self.config
        assert isinstance(self.handler._processed_events, set)
        assert isinstance(self.handler._lock, type(threading.RLock()))

    def test_remove_expired_notifications(self):
        """Test removal of expired notifications."""
        with patch("time.monotonic", return_value=1000):
            event1 = NodeMovingEvent(
                id=1, new_node_host="host1", new_node_port=6379, ttl=10
            )
            event2 = NodeMovingEvent(
                id=2, new_node_host="host2", new_node_port=6380, ttl=5
            )
            self.handler._processed_events.add(event1)
            self.handler._processed_events.add(event2)

        # Move time forward but not enough to expire event2 (expires at 1005)
        with patch("time.monotonic", return_value=1003):
            self.handler.remove_expired_notifications()
            assert event1 in self.handler._processed_events
            assert event2 in self.handler._processed_events  # Not expired yet

        # Move time forward to expire event2 but not event1
        with patch("time.monotonic", return_value=1006):
            self.handler.remove_expired_notifications()
            assert event1 in self.handler._processed_events
            assert event2 not in self.handler._processed_events  # Now expired

    def test_handle_event_node_moving(self):
        """Test handling of NodeMovingEvent."""
        event = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        with patch.object(self.handler, "handle_node_moving_event") as mock_handle:
            self.handler.handle_event(event)
            mock_handle.assert_called_once_with(event)

    def test_handle_event_unknown_type(self):
        """Test handling of unknown event type."""
        event = NodeMigratingEvent(id=1, ttl=5)  # Not handled by pool handler

        result = self.handler.handle_event(event)
        assert result is None

    def test_handle_node_moving_event_disabled_config(self):
        """Test node moving event handling when both features are disabled."""
        config = MaintenanceEventsConfig(proactive_reconnect=False, relax_timeout=-1)
        handler = MaintenanceEventPoolHandler(self.mock_pool, config)
        event = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        result = handler.handle_node_moving_event(event)
        assert result is None
        assert event not in handler._processed_events

    def test_handle_node_moving_event_already_processed(self):
        """Test node moving event handling when event already processed."""
        event = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        self.handler._processed_events.add(event)

        result = self.handler.handle_node_moving_event(event)
        assert result is None

    def test_handle_node_moving_event_success(self):
        """Test successful node moving event handling."""
        event = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        with (
            patch("threading.Timer") as mock_timer,
            patch("time.monotonic", return_value=1000),
        ):
            self.handler.handle_node_moving_event(event)

            # Verify timer was started
            mock_timer.assert_called_once_with(
                event.ttl, self.handler.handle_node_moved_event, args=(event,)
            )
            mock_timer.return_value.start.assert_called_once()

            # Verify event was added to processed set
            assert event in self.handler._processed_events

            # Verify pool methods were called
            self.mock_pool.update_connections_settings.assert_called_once()

    def test_handle_node_moved_event(self):
        """Test handling of node moved event (cleanup)."""
        event = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )
        self.mock_pool.connection_kwargs = {"host": "localhost"}
        self.handler.handle_node_moved_event(event)

        # Verify cleanup methods were called
        self.mock_pool.update_connections_settings.assert_called_once()


class TestMaintenanceEventConnectionHandler:
    """Test the MaintenanceEventConnectionHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.config = MaintenanceEventsConfig(enabled=True, relax_timeout=20)
        self.handler = MaintenanceEventConnectionHandler(
            self.mock_connection, self.config
        )

    def test_init(self):
        """Test MaintenanceEventConnectionHandler initialization."""
        assert self.handler.connection == self.mock_connection
        assert self.handler.config == self.config

    def test_handle_event_migrating(self):
        """Test handling of NodeMigratingEvent."""
        event = NodeMigratingEvent(id=1, ttl=5)

        with patch.object(
            self.handler, "handle_maintenance_start_event"
        ) as mock_handle:
            self.handler.handle_event(event)
            mock_handle.assert_called_once_with(MaintenanceState.MIGRATING)

    def test_handle_event_migrated(self):
        """Test handling of NodeMigratedEvent."""
        event = NodeMigratedEvent(id=1)

        with patch.object(
            self.handler, "handle_maintenance_completed_event"
        ) as mock_handle:
            self.handler.handle_event(event)
            mock_handle.assert_called_once_with()

    def test_handle_event_failing_over(self):
        """Test handling of NodeFailingOverEvent."""
        event = NodeFailingOverEvent(id=1, ttl=5)

        with patch.object(
            self.handler, "handle_maintenance_start_event"
        ) as mock_handle:
            self.handler.handle_event(event)
            mock_handle.assert_called_once_with(MaintenanceState.FAILING_OVER)

    def test_handle_event_failed_over(self):
        """Test handling of NodeFailedOverEvent."""
        event = NodeFailedOverEvent(id=1)

        with patch.object(
            self.handler, "handle_maintenance_completed_event"
        ) as mock_handle:
            self.handler.handle_event(event)
            mock_handle.assert_called_once_with()

    def test_handle_event_unknown_type(self):
        """Test handling of unknown event type."""
        event = NodeMovingEvent(
            id=1, new_node_host="localhost", new_node_port=6379, ttl=10
        )

        result = self.handler.handle_event(event)
        assert result is None

    def test_handle_maintenance_start_event_disabled(self):
        """Test maintenance start event handling when relax timeouts are disabled."""
        config = MaintenanceEventsConfig(relax_timeout=-1)
        handler = MaintenanceEventConnectionHandler(self.mock_connection, config)

        result = handler.handle_maintenance_start_event(MaintenanceState.MIGRATING)
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_start_event_moving_state(self):
        """Test maintenance start event handling when connection is in MOVING state."""
        self.mock_connection.maintenance_state = MaintenanceState.MOVING

        result = self.handler.handle_maintenance_start_event(MaintenanceState.MIGRATING)
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_start_event_migrating_success(self):
        """Test successful maintenance start event handling for migrating."""
        self.mock_connection.maintenance_state = MaintenanceState.NONE

        self.handler.handle_maintenance_start_event(MaintenanceState.MIGRATING)

        assert self.mock_connection.maintenance_state == MaintenanceState.MIGRATING
        self.mock_connection.update_current_socket_timeout.assert_called_once_with(20)
        self.mock_connection.set_tmp_settings.assert_called_once_with(
            tmp_relax_timeout=20
        )

    def test_handle_maintenance_start_event_failing_over_success(self):
        """Test successful maintenance start event handling for failing over."""
        self.mock_connection.maintenance_state = MaintenanceState.NONE

        self.handler.handle_maintenance_start_event(MaintenanceState.FAILING_OVER)

        assert self.mock_connection.maintenance_state == MaintenanceState.FAILING_OVER
        self.mock_connection.update_current_socket_timeout.assert_called_once_with(20)
        self.mock_connection.set_tmp_settings.assert_called_once_with(
            tmp_relax_timeout=20
        )

    def test_handle_maintenance_completed_event_disabled(self):
        """Test maintenance completed event handling when relax timeouts are disabled."""
        config = MaintenanceEventsConfig(relax_timeout=-1)
        handler = MaintenanceEventConnectionHandler(self.mock_connection, config)

        result = handler.handle_maintenance_completed_event()
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_completed_event_moving_state(self):
        """Test maintenance completed event handling when connection is in MOVING state."""
        self.mock_connection.maintenance_state = MaintenanceState.MOVING

        result = self.handler.handle_maintenance_completed_event()
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_completed_event_success(self):
        """Test successful maintenance completed event handling."""
        self.mock_connection.maintenance_state = MaintenanceState.MIGRATING

        self.handler.handle_maintenance_completed_event()

        assert self.mock_connection.maintenance_state == MaintenanceState.NONE

        self.mock_connection.update_current_socket_timeout.assert_called_once_with(-1)
        self.mock_connection.reset_tmp_settings.assert_called_once_with(
            reset_relax_timeout=True
        )
