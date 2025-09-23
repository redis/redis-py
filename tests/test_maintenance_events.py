import threading
from unittest.mock import Mock, call, patch, MagicMock
import pytest

from redis.connection import ConnectionInterface

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
    EndpointType,
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

    def test_equality_none_id_none_port(self):
        """Test equality for events with same id and host and port - None."""
        event1 = NodeMovingEvent(id=1, new_node_host=None, new_node_port=None, ttl=10)
        event2 = NodeMovingEvent(
            id=1, new_node_host=None, new_node_port=None, ttl=20
        )  # Different TTL
        assert event1 == event2

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
        assert config.enabled is True
        assert config.proactive_reconnect is True
        assert config.relaxed_timeout == 10

    def test_init_custom_values(self):
        """Test MaintenanceEventsConfig initialization with custom values."""
        config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=False, relaxed_timeout=30
        )
        assert config.enabled is True
        assert config.proactive_reconnect is False
        assert config.relaxed_timeout == 30

    def test_repr(self):
        """Test MaintenanceEventsConfig string representation."""
        config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=False, relaxed_timeout=30
        )
        repr_str = repr(config)
        assert "MaintenanceEventsConfig" in repr_str
        assert "enabled=True" in repr_str
        assert "proactive_reconnect=False" in repr_str
        assert "relaxed_timeout=30" in repr_str

    def test_is_relaxed_timeouts_enabled_true(self):
        """Test is_relaxed_timeouts_enabled returns True for positive timeout."""
        config = MaintenanceEventsConfig(relaxed_timeout=20)
        assert config.is_relaxed_timeouts_enabled() is True

    def test_is_relaxed_timeouts_enabled_false(self):
        """Test is_relaxed_timeouts_enabled returns False for -1 timeout."""
        config = MaintenanceEventsConfig(relaxed_timeout=-1)
        assert config.is_relaxed_timeouts_enabled() is False

    def test_is_relaxed_timeouts_enabled_zero(self):
        """Test is_relaxed_timeouts_enabled returns True for zero timeout."""
        config = MaintenanceEventsConfig(relaxed_timeout=0)
        assert config.is_relaxed_timeouts_enabled() is True

    def test_is_relaxed_timeouts_enabled_none(self):
        """Test is_relaxed_timeouts_enabled returns True for None timeout."""
        config = MaintenanceEventsConfig(relaxed_timeout=None)
        assert config.is_relaxed_timeouts_enabled() is True

    def test_relaxed_timeout_none_is_saved_as_none(self):
        """Test that None value for relaxed_timeout is saved as None."""
        config = MaintenanceEventsConfig(relaxed_timeout=None)
        assert config.relaxed_timeout is None


class TestMaintenanceEventPoolHandler:
    """Test the MaintenanceEventPoolHandler class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_pool = Mock()
        self.mock_pool._lock = MagicMock()
        self.mock_pool._lock.__enter__.return_value = None
        self.mock_pool._lock.__exit__.return_value = None
        self.config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=20
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
        config = MaintenanceEventsConfig(proactive_reconnect=False, relaxed_timeout=-1)
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

    def test_handle_node_moving_event_with_no_host_and_port(self):
        """Test successful node moving event handling."""
        event = NodeMovingEvent(id=1, new_node_host=None, new_node_port=None, ttl=2)

        with (
            patch("threading.Timer") as mock_timer,
            patch("time.monotonic", return_value=1000),
        ):
            self.handler.handle_node_moving_event(event)

            # Verify timer was started
            mock_timer.assert_has_calls(
                [
                    call(
                        event.ttl / 2,
                        self.handler.run_proactive_reconnect,
                        args=(None,),
                    ),
                    call().start(),
                    call(
                        event.ttl, self.handler.handle_node_moved_event, args=(event,)
                    ),
                    call().start(),
                ]
            )

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
        self.config = MaintenanceEventsConfig(enabled=True, relaxed_timeout=20)
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
            mock_handle.assert_called_once_with(MaintenanceState.MAINTENANCE)

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
            mock_handle.assert_called_once_with(MaintenanceState.MAINTENANCE)

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
        """Test maintenance start event handling when relaxed timeouts are disabled."""
        config = MaintenanceEventsConfig(relaxed_timeout=-1)
        handler = MaintenanceEventConnectionHandler(self.mock_connection, config)

        result = handler.handle_maintenance_start_event(MaintenanceState.MAINTENANCE)

        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_start_event_moving_state(self):
        """Test maintenance start event handling when connection is in MOVING state."""
        self.mock_connection.maintenance_state = MaintenanceState.MOVING

        result = self.handler.handle_maintenance_start_event(
            MaintenanceState.MAINTENANCE
        )
        assert result is None
        self.mock_connection.update_current_socket_timeout.assert_not_called()

    def test_handle_maintenance_start_event_success(self):
        """Test successful maintenance start event handling for migrating."""
        self.mock_connection.maintenance_state = MaintenanceState.NONE

        self.handler.handle_maintenance_start_event(MaintenanceState.MAINTENANCE)

        assert self.mock_connection.maintenance_state == MaintenanceState.MAINTENANCE
        self.mock_connection.update_current_socket_timeout.assert_called_once_with(20)
        self.mock_connection.set_tmp_settings.assert_called_once_with(
            tmp_relaxed_timeout=20
        )

    def test_handle_maintenance_completed_event_disabled(self):
        """Test maintenance completed event handling when relaxed timeouts are disabled."""
        config = MaintenanceEventsConfig(relaxed_timeout=-1)
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
        self.mock_connection.maintenance_state = MaintenanceState.MAINTENANCE

        self.handler.handle_maintenance_completed_event()

        assert self.mock_connection.maintenance_state == MaintenanceState.NONE

        self.mock_connection.update_current_socket_timeout.assert_called_once_with(-1)
        self.mock_connection.reset_tmp_settings.assert_called_once_with(
            reset_relaxed_timeout=True
        )


class TestEndpointType:
    """Test the EndpointType class functionality."""

    def test_endpoint_type_constants(self):
        """Test that the EndpointType constants are correct."""
        assert EndpointType.INTERNAL_IP.value == "internal-ip"
        assert EndpointType.INTERNAL_FQDN.value == "internal-fqdn"
        assert EndpointType.EXTERNAL_IP.value == "external-ip"
        assert EndpointType.EXTERNAL_FQDN.value == "external-fqdn"
        assert EndpointType.NONE.value == "none"


class TestMaintenanceEventsConfigEndpointType:
    """Test MaintenanceEventsConfig endpoint type functionality."""

    def setup_method(self):
        """Set up common mock classes for all tests."""

        class MockSocket:
            def __init__(self, resolved_ip):
                self.resolved_ip = resolved_ip

            def getpeername(self):
                return (self.resolved_ip, 6379)

        class MockConnection(ConnectionInterface):
            def __init__(self, host, resolved_ip=None, is_ssl=False):
                self.host = host
                self.port = 6379
                self._sock = MockSocket(resolved_ip) if resolved_ip else None
                self.__class__.__name__ = "SSLConnection" if is_ssl else "Connection"

            def get_resolved_ip(self):
                # Call the actual method from AbstractConnection
                from redis.connection import AbstractConnection

                return AbstractConnection.get_resolved_ip(self)  # type: ignore

        self.MockSocket = MockSocket
        self.MockConnection = MockConnection

    def test_config_validation_valid_endpoint_types(self):
        """Test that MaintenanceEventsConfig accepts valid endpoint types."""
        for endpoint_type in EndpointType:
            config = MaintenanceEventsConfig(endpoint_type=endpoint_type)
            assert config.endpoint_type == endpoint_type

    def test_config_validation_none_endpoint_type(self):
        """Test that MaintenanceEventsConfig accepts None as endpoint type."""
        config = MaintenanceEventsConfig(endpoint_type=None)
        assert config.endpoint_type is None

    def test_endpoint_type_detection_ip_addresses(self):
        """Test endpoint type detection for IP addresses."""
        config = MaintenanceEventsConfig()

        # Test private IPv4 addresses
        conn1 = self.MockConnection("192.168.1.1", resolved_ip="192.168.1.1")
        assert (
            config.get_endpoint_type("192.168.1.1", conn1) == EndpointType.INTERNAL_IP
        )

        # Test public IPv4 addresses
        conn2 = self.MockConnection("8.8.8.8", resolved_ip="8.8.8.8")
        assert config.get_endpoint_type("8.8.8.8", conn2) == EndpointType.EXTERNAL_IP

        # Test IPv6 loopback
        conn3 = self.MockConnection("::1")
        assert config.get_endpoint_type("::1", conn3) == EndpointType.INTERNAL_IP

        # Test IPv6 public address
        conn4 = self.MockConnection("2001:4860:4860::8888")
        assert (
            config.get_endpoint_type("2001:4860:4860::8888", conn4)
            == EndpointType.EXTERNAL_IP
        )

    def test_endpoint_type_detection_fqdn_with_resolved_ip(self):
        """Test endpoint type detection for FQDNs with resolved IP addresses."""
        config = MaintenanceEventsConfig()

        # Test FQDN resolving to private IP
        conn1 = self.MockConnection(
            "redis.internal.company.com", resolved_ip="192.168.1.1"
        )
        assert (
            config.get_endpoint_type("redis.internal.company.com", conn1)
            == EndpointType.INTERNAL_FQDN
        )

        # Test FQDN resolving to public IP
        conn2 = self.MockConnection("db123.redis.com", resolved_ip="8.8.8.8")
        assert (
            config.get_endpoint_type("db123.redis.com", conn2)
            == EndpointType.EXTERNAL_FQDN
        )

        # Test internal FQDN resolving to public IP (should use resolved IP)
        conn3 = self.MockConnection(
            "redis.internal.company.com", resolved_ip="10.8.8.8"
        )
        assert (
            config.get_endpoint_type("redis.internal.company.com", conn3)
            == EndpointType.INTERNAL_FQDN
        )

        # Test FQDN with TLS
        conn4 = self.MockConnection(
            "redis.internal.company.com", resolved_ip="192.168.1.1", is_ssl=True
        )
        assert (
            config.get_endpoint_type("redis.internal.company.com", conn4)
            == EndpointType.INTERNAL_FQDN
        )

        conn5 = self.MockConnection(
            "db123.redis.com", resolved_ip="8.8.8.8", is_ssl=True
        )
        assert (
            config.get_endpoint_type("db123.redis.com", conn5)
            == EndpointType.EXTERNAL_FQDN
        )

    def test_endpoint_type_detection_fqdn_heuristics(self):
        """Test endpoint type detection using FQDN heuristics when no resolved IP is available."""
        config = MaintenanceEventsConfig()

        # Test localhost (should be internal)
        conn1 = self.MockConnection("localhost")
        assert (
            config.get_endpoint_type("localhost", conn1) == EndpointType.INTERNAL_FQDN
        )

        # Test .local domain (should be internal)
        conn2 = self.MockConnection("server.local")
        assert (
            config.get_endpoint_type("server.local", conn2)
            == EndpointType.INTERNAL_FQDN
        )

        # Test public domain (should be external)
        conn3 = self.MockConnection("example.com")
        assert (
            config.get_endpoint_type("example.com", conn3) == EndpointType.EXTERNAL_FQDN
        )

    def test_endpoint_type_override(self):
        """Test that configured endpoint_type overrides detection."""

        # Test with endpoint_type set to NONE
        config = MaintenanceEventsConfig(endpoint_type=EndpointType.NONE)
        conn = self.MockConnection("localhost")

        assert config.get_endpoint_type("localhost", conn) == EndpointType.NONE

        # Test with endpoint_type set to EXTERNAL_IP
        config = MaintenanceEventsConfig(endpoint_type=EndpointType.EXTERNAL_IP)
        assert config.get_endpoint_type("localhost", conn) == EndpointType.EXTERNAL_IP
