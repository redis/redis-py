import enum
import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Union

from redis.typing import Number


class MaintenanceState(enum.Enum):
    NONE = "none"
    MOVING = "moving"
    MIGRATING = "migrating"
    FAILING_OVER = "failing_over"



if TYPE_CHECKING:
    from redis.connection import (
        BlockingConnectionPool,
        ConnectionInterface,
        ConnectionPool,
    )


class MaintenanceEvent(ABC):
    """
    Base class for maintenance events sent through push messages by Redis server.

    This class provides common functionality for all maintenance events including
    unique identification and TTL (Time-To-Live) functionality.

    Attributes:
        id (int): Unique identifier for this event
        ttl (int): Time-to-live in seconds for this notification
        creation_time (float): Timestamp when the notification was created/read
    """

    def __init__(self, id: int, ttl: int):
        """
        Initialize a new MaintenanceEvent with unique ID and TTL functionality.

        Args:
            id (int): Unique identifier for this event
            ttl (int): Time-to-live in seconds for this notification
        """
        self.id = id
        self.ttl = ttl
        self.creation_time = time.monotonic()
        self.expire_at = self.creation_time + self.ttl

    def is_expired(self) -> bool:
        """
        Check if this event has expired based on its TTL
        and creation time.

        Returns:
            bool: True if the event has expired, False otherwise
        """
        return time.monotonic() > (self.creation_time + self.ttl)

    @abstractmethod
    def __repr__(self) -> str:
        """
        Return a string representation of the maintenance event.

        This method must be implemented by all concrete subclasses.

        Returns:
            str: String representation of the event
        """
        pass

    @abstractmethod
    def __eq__(self, other) -> bool:
        """
        Compare two maintenance events for equality.

        This method must be implemented by all concrete subclasses.
        Events are typically considered equal if they have the same id
        and are of the same type.

        Args:
            other: The other object to compare with

        Returns:
            bool: True if the events are equal, False otherwise
        """
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """
        Return a hash value for the maintenance event.

        This method must be implemented by all concrete subclasses to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value for the event
        """
        pass


class NodeMovingEvent(MaintenanceEvent):
    """
    This event is received when a node is replaced with a new node
    during cluster rebalancing or maintenance operations.
    """

    def __init__(self, id: int, new_node_host: str, new_node_port: int, ttl: int):
        """
        Initialize a new NodeMovingEvent.

        Args:
            id (int): Unique identifier for this event
            new_node_host (str): Hostname or IP address of the new replacement node
            new_node_port (int): Port number of the new replacement node
            ttl (int): Time-to-live in seconds for this notification
        """
        super().__init__(id, ttl)
        self.new_node_host = new_node_host
        self.new_node_port = new_node_port

    def __repr__(self) -> str:
        expiry_time = self.expire_at
        remaining = max(0, expiry_time - time.monotonic())

        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, "
            f"new_node_host='{self.new_node_host}', "
            f"new_node_port={self.new_node_port}, "
            f"ttl={self.ttl}, "
            f"creation_time={self.creation_time}, "
            f"expires_at={expiry_time}, "
            f"remaining={remaining:.1f}s, "
            f"expired={self.is_expired()}"
            f")"
        )

    def __eq__(self, other) -> bool:
        """
        Two NodeMovingEvent events are considered equal if they have the same
        id, new_node_host, and new_node_port.
        """
        if not isinstance(other, NodeMovingEvent):
            return False
        return (
            self.id == other.id
            and self.new_node_host == other.new_node_host
            and self.new_node_port == other.new_node_port
        )

    def __hash__(self) -> int:
        """
        Return a hash value for the event to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on event type, id, new_node_host, and new_node_port
        """
        return hash((self.__class__, self.id, self.new_node_host, self.new_node_port))


class NodeMigratingEvent(MaintenanceEvent):
    """
    Event for when a Redis cluster node is in the process of migrating slots.

    This event is received when a node starts migrating its slots to another node
    during cluster rebalancing or maintenance operations.

    Args:
        id (int): Unique identifier for this event
        ttl (int): Time-to-live in seconds for this notification
    """

    def __init__(self, id: int, ttl: int):
        super().__init__(id, ttl)

    def __repr__(self) -> str:
        expiry_time = self.creation_time + self.ttl
        remaining = max(0, expiry_time - time.monotonic())
        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, "
            f"ttl={self.ttl}, "
            f"creation_time={self.creation_time}, "
            f"expires_at={expiry_time}, "
            f"remaining={remaining:.1f}s, "
            f"expired={self.is_expired()}"
            f")"
        )

    def __eq__(self, other) -> bool:
        """
        Two NodeMigratingEvent events are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeMigratingEvent):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the event to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on event type and id
        """
        return hash((self.__class__, self.id))


class NodeMigratedEvent(MaintenanceEvent):
    """
    Event for when a Redis cluster node has completed migrating slots.

    This event is received when a node has finished migrating all its slots
    to other nodes during cluster rebalancing or maintenance operations.

    Args:
        id (int): Unique identifier for this event
    """

    DEFAULT_TTL = 5

    def __init__(self, id: int):
        super().__init__(id, NodeMigratedEvent.DEFAULT_TTL)

    def __repr__(self) -> str:
        expiry_time = self.creation_time + self.ttl
        remaining = max(0, expiry_time - time.monotonic())
        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, "
            f"ttl={self.ttl}, "
            f"creation_time={self.creation_time}, "
            f"expires_at={expiry_time}, "
            f"remaining={remaining:.1f}s, "
            f"expired={self.is_expired()}"
            f")"
        )

    def __eq__(self, other) -> bool:
        """
        Two NodeMigratedEvent events are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeMigratedEvent):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the event to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on event type and id
        """
        return hash((self.__class__, self.id))


class NodeFailingOverEvent(MaintenanceEvent):
    """
    Event for when a Redis cluster node is in the process of failing over.

    This event is received when a node starts a failover process during
    cluster maintenance operations or when handling node failures.

    Args:
        id (int): Unique identifier for this event
        ttl (int): Time-to-live in seconds for this notification
    """

    def __init__(self, id: int, ttl: int):
        super().__init__(id, ttl)

    def __repr__(self) -> str:
        expiry_time = self.creation_time + self.ttl
        remaining = max(0, expiry_time - time.monotonic())
        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, "
            f"ttl={self.ttl}, "
            f"creation_time={self.creation_time}, "
            f"expires_at={expiry_time}, "
            f"remaining={remaining:.1f}s, "
            f"expired={self.is_expired()}"
            f")"
        )

    def __eq__(self, other) -> bool:
        """
        Two NodeFailingOverEvent events are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeFailingOverEvent):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the event to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on event type and id
        """
        return hash((self.__class__, self.id))


class NodeFailedOverEvent(MaintenanceEvent):
    """
    Event for when a Redis cluster node has completed a failover.

    This event is received when a node has finished the failover process
    during cluster maintenance operations or after handling node failures.

    Args:
        id (int): Unique identifier for this event
    """

    DEFAULT_TTL = 5

    def __init__(self, id: int):
        super().__init__(id, NodeFailedOverEvent.DEFAULT_TTL)

    def __repr__(self) -> str:
        expiry_time = self.creation_time + self.ttl
        remaining = max(0, expiry_time - time.monotonic())
        return (
            f"{self.__class__.__name__}("
            f"id={self.id}, "
            f"ttl={self.ttl}, "
            f"creation_time={self.creation_time}, "
            f"expires_at={expiry_time}, "
            f"remaining={remaining:.1f}s, "
            f"expired={self.is_expired()}"
            f")"
        )

    def __eq__(self, other) -> bool:
        """
        Two NodeFailedOverEvent events are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeFailedOverEvent):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the event to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on event type and id
        """
        return hash((self.__class__, self.id))


class MaintenanceEventsConfig:
    """
    Configuration class for maintenance events handling behaviour. Events are received through
    push notifications.

    This class defines how the Redis client should react to different push notifications
    such as node moving, migrations, etc. in a Redis cluster.

    """

    def __init__(
        self,
        enabled: bool = False,
        proactive_reconnect: bool = True,
        relax_timeout: Optional[Number] = 20,
    ):
        """
        Initialize a new MaintenanceEventsConfig.

        Args:
            enabled (bool): Whether to enable maintenance events handling.
                Defaults to False.
            proactive_reconnect (bool): Whether to proactively reconnect when a node is replaced.
                Defaults to True.
            relax_timeout (Number): The relax timeout to use for the connection during maintenance.
                If -1 is provided - the relax timeout is disabled. Defaults to 20.

        """
        self.enabled = enabled
        self.relax_timeout = relax_timeout
        self.proactive_reconnect = proactive_reconnect

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"enabled={self.enabled}, "
            f"proactive_reconnect={self.proactive_reconnect}, "
            f"relax_timeout={self.relax_timeout}, "
            f")"
        )

    def is_relax_timeouts_enabled(self) -> bool:
        """
        Check if the relax_timeout is enabled. The '-1' value is used to disable the relax_timeout.
        If relax_timeout is set to None, it will make the operation blocking
        and waiting until any response is received.

        Returns:
            True if the relax_timeout is enabled, False otherwise.
        """
        return self.relax_timeout != -1


class MaintenanceEventPoolHandler:
    def __init__(
        self,
        pool: Union["ConnectionPool", "BlockingConnectionPool"],
        config: MaintenanceEventsConfig,
    ) -> None:
        self.pool = pool
        self.config = config
        self._processed_events = set()
        self._lock = threading.RLock()
        self.connection = None

    def set_connection(self, connection: "ConnectionInterface"):
        self.connection = connection

    def remove_expired_notifications(self):
        with self._lock:
            for notification in tuple(self._processed_events):
                if notification.is_expired():
                    self._processed_events.remove(notification)

    def handle_event(self, notification: MaintenanceEvent):
        self.remove_expired_notifications()

        if isinstance(notification, NodeMovingEvent):
            return self.handle_node_moving_event(notification)
        else:
            logging.error(f"Unhandled notification type: {notification}")

    def handle_node_moving_event(self, event: NodeMovingEvent):
        if (
            not self.config.proactive_reconnect
            and not self.config.is_relax_timeouts_enabled()
        ):
            return
        with self._lock:
            if event in self._processed_events:
                # nothing to do in the connection pool handling
                # the event has already been handled or is expired
                # just return
                return

            with self.pool._lock:
                if (
                    self.config.proactive_reconnect
                    or self.config.is_relax_timeouts_enabled()
                ):
                    moving_address_src = (
                        self.connection.getpeername() if self.connection else None
                    )

                    if getattr(self.pool, "set_in_maintenance", False):
                        self.pool.set_in_maintenance(True)

                    # Update connection settings for all connections
                    self.pool.update_connections_settings(
                        state=MaintenanceState.MOVING,
                        relax_timeout=self.config.relax_timeout,
                        matching_address=moving_address_src,
                        address_type_to_match="connected",
                        include_free_connections=True,
                    )

                    if self.config.proactive_reconnect:
                        # take care for the active connections in the pool
                        # mark them for reconnect after they complete the current command
                        self.pool.update_active_connections_for_reconnect(
                            tmp_host_address=event.new_node_host,
                            tmp_relax_timeout=self.config.relax_timeout,
                            moving_address_src=moving_address_src,
                        )
                        # take care for the inactive connections in the pool
                        # delete them and create new ones
                        self.pool.disconnect_and_reconfigure_free_connections(
                            tmp_host_address=event.new_node_host,
                            tmp_relax_timeout=self.config.relax_timeout,
                            moving_address_src=moving_address_src,
                        )

                    # Update config for new connections:
                    # Set state to MOVING
                    # update host
                    # if relax timeouts are enabled - update timeouts
                    kwargs: dict = {
                        "maintenance_state": MaintenanceState.MOVING,
                        "host": event.new_node_host,
                    }
                    if self.config.is_relax_timeouts_enabled():
                        kwargs.update(
                            {
                                "socket_timeout": self.config.relax_timeout,
                                "socket_connect_timeout": self.config.relax_timeout,
                            }
                        )
                    self.pool.update_connection_kwargs(**kwargs)

                    if getattr(self.pool, "set_in_maintenance", False):
                        self.pool.set_in_maintenance(False)

            threading.Timer(
                event.ttl, self.handle_node_moved_event, args=(event,)
            ).start()

            self._processed_events.add(event)

    def handle_node_moved_event(self, event: NodeMovingEvent):
        with self._lock:
            # if the current host in kwargs is not matching the event
            # it means there has been a new moving event after this one
            # and we don't need to revert the kwargs
            if self.pool.connection_kwargs.get("host") == event.new_node_host:
                orig_host = self.pool.connection_kwargs.get("orig_host_address")
                orig_socket_timeout = self.pool.connection_kwargs.get(
                    "orig_socket_timeout"
                )
                orig_connect_timeout = self.pool.connection_kwargs.get(
                    "orig_socket_connect_timeout"
                )
                kwargs: dict = {
                    "maintenance_state": MaintenanceState.NONE,
                    "host": orig_host,
                    "socket_timeout": orig_socket_timeout,
                    "socket_connect_timeout": orig_connect_timeout,
                }
                self.pool.update_connection_kwargs(**kwargs)

            with self.pool._lock:
                moving_address = event.new_node_host
                reset_relax_timeout = self.config.is_relax_timeouts_enabled()
                reset_host_address = self.config.proactive_reconnect

                self.pool.update_connections_settings(
                    relax_timeout=-1,
                    state=MaintenanceState.NONE,
                    matching_address=moving_address,
                    address_type_to_match="configured",
                    reset_relax_timeout=reset_relax_timeout,
                    reset_host_address=reset_host_address,
                    include_free_connections=True,
                )


class MaintenanceEventConnectionHandler:
    def __init__(
        self, connection: "ConnectionInterface", config: MaintenanceEventsConfig
    ) -> None:
        self.connection = connection
        self.config = config

    def handle_event(self, event: MaintenanceEvent):
        if isinstance(event, NodeMigratingEvent):
            return self.handle_maintenance_start_event(MaintenanceState.MIGRATING)
        elif isinstance(event, NodeMigratedEvent):
            return self.handle_maintenance_completed_event()
        elif isinstance(event, NodeFailingOverEvent):
            return self.handle_maintenance_start_event(MaintenanceState.FAILING_OVER)
        elif isinstance(event, NodeFailedOverEvent):
            return self.handle_maintenance_completed_event()
        else:
            logging.error(f"Unhandled event type: {event}")

    def handle_maintenance_start_event(self, maintenance_state: MaintenanceState):
        if (
            self.connection.maintenance_state == MaintenanceState.MOVING
            or not self.config.is_relax_timeouts_enabled()
        ):
            return

        self.connection.maintenance_state = maintenance_state
        self.connection.set_tmp_settings(tmp_relax_timeout=self.config.relax_timeout)
        # extend the timeout for all created connections
        self.connection.update_current_socket_timeout(self.config.relax_timeout)


    def handle_maintenance_completed_event(self):
        # Only reset timeouts if state is not MOVING and relax timeouts are enabled
        if (
            self.connection.maintenance_state == MaintenanceState.MOVING
            or not self.config.is_relax_timeouts_enabled()
        ):
            return
        self.connection.reset_tmp_settings(reset_relax_timeout=True)
        # Maintenance completed - reset the connection
        # timeouts by providing -1 as the relax timeout
        self.connection.update_current_socket_timeout(-1)
        self.connection.maintenance_state = MaintenanceState.NONE
