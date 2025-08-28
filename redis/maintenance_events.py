import enum
import ipaddress
import logging
import re
import threading
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Union

from redis.typing import Number


class MaintenanceState(enum.Enum):
    NONE = "none"
    MOVING = "moving"
    MAINTENANCE = "maintenance"


class EndpointType(enum.Enum):
    """Valid endpoint types used in CLIENT MAINT_NOTIFICATIONS command."""

    INTERNAL_IP = "internal-ip"
    INTERNAL_FQDN = "internal-fqdn"
    EXTERNAL_IP = "external-ip"
    EXTERNAL_FQDN = "external-fqdn"
    NONE = "none"

    def __str__(self):
        """Return the string value of the enum."""
        return self.value


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

    def __init__(
        self,
        id: int,
        new_node_host: Optional[str],
        new_node_port: Optional[int],
        ttl: int,
    ):
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
            int: Hash value based on event type class name, id,
            new_node_host and new_node_port
        """
        try:
            node_port = int(self.new_node_port) if self.new_node_port else None
        except ValueError:
            node_port = 0

        return hash(
            (
                self.__class__.__name__,
                int(self.id),
                str(self.new_node_host),
                node_port,
            )
        )


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
        return hash((self.__class__.__name__, int(self.id)))


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
        return hash((self.__class__.__name__, int(self.id)))


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
        return hash((self.__class__.__name__, int(self.id)))


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
        return hash((self.__class__.__name__, int(self.id)))


def _is_private_fqdn(host: str) -> bool:
    """
    Determine if an FQDN is likely to be internal/private.

    This uses heuristics based on RFC 952 and RFC 1123 standards:
    - .local domains (RFC 6762 - Multicast DNS)
    - .internal domains (common internal convention)
    - Single-label hostnames (no dots)
    - Common internal TLDs

    Args:
        host (str): The FQDN to check

    Returns:
        bool: True if the FQDN appears to be internal/private
    """
    host_lower = host.lower().rstrip(".")

    # Single-label hostnames (no dots) are typically internal
    if "." not in host_lower:
        return True

    # Common internal/private domain patterns
    internal_patterns = [
        r"\.local$",  # mDNS/Bonjour domains
        r"\.internal$",  # Common internal convention
        r"\.corp$",  # Corporate domains
        r"\.lan$",  # Local area network
        r"\.intranet$",  # Intranet domains
        r"\.private$",  # Private domains
    ]

    for pattern in internal_patterns:
        if re.search(pattern, host_lower):
            return True

    # If none of the internal patterns match, assume it's external
    return False


class MaintenanceEventsConfig:
    """
    Configuration class for maintenance events handling behaviour. Events are received through
    push notifications.

    This class defines how the Redis client should react to different push notifications
    such as node moving, migrations, etc. in a Redis cluster.

    """

    def __init__(
        self,
        enabled: bool = True,
        proactive_reconnect: bool = True,
        relax_timeout: Optional[Number] = 10,
        endpoint_type: Optional[EndpointType] = None,
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
            endpoint_type (Optional[EndpointType]): Override for the endpoint type to use in CLIENT MAINT_NOTIFICATIONS.
                If None, the endpoint type will be automatically determined based on the host and TLS configuration.
                Defaults to None.

        Raises:
            ValueError: If endpoint_type is provided but is not a valid endpoint type.
        """
        self.enabled = enabled
        self.relax_timeout = relax_timeout
        self.proactive_reconnect = proactive_reconnect
        self.endpoint_type = endpoint_type

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"enabled={self.enabled}, "
            f"proactive_reconnect={self.proactive_reconnect}, "
            f"relax_timeout={self.relax_timeout}, "
            f"endpoint_type={self.endpoint_type!r}"
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

    def get_endpoint_type(
        self, host: str, connection: "ConnectionInterface"
    ) -> EndpointType:
        """
        Determine the appropriate endpoint type for CLIENT MAINT_NOTIFICATIONS command.

        Logic:
        1. If endpoint_type is explicitly set, use it
        2. Otherwise, check the original host from connection.host:
           - If host is an IP address, use it directly to determine internal-ip vs external-ip
           - If host is an FQDN, get the resolved IP to determine internal-fqdn vs external-fqdn

        Args:
            host: User provided hostname to analyze
            connection: The connection object to analyze for endpoint type determination

        Returns:
        """

        # If endpoint_type is explicitly set, use it
        if self.endpoint_type is not None:
            return self.endpoint_type

        # Check if the host is an IP address
        try:
            ip_addr = ipaddress.ip_address(host)
            # Host is an IP address - use it directly
            is_private = ip_addr.is_private
            return EndpointType.INTERNAL_IP if is_private else EndpointType.EXTERNAL_IP
        except ValueError:
            # Host is an FQDN - need to check resolved IP to determine internal vs external
            pass

        # Host is an FQDN, get the resolved IP to determine if it's internal or external
        resolved_ip = connection.get_resolved_ip()

        if resolved_ip:
            try:
                ip_addr = ipaddress.ip_address(resolved_ip)
                is_private = ip_addr.is_private
                # Use FQDN types since the original host was an FQDN
                return (
                    EndpointType.INTERNAL_FQDN
                    if is_private
                    else EndpointType.EXTERNAL_FQDN
                )
            except ValueError:
                # This shouldn't happen since we got the IP from the socket, but fallback
                pass

        # Final fallback: use heuristics on the FQDN itself
        is_private = _is_private_fqdn(host)
        return EndpointType.INTERNAL_FQDN if is_private else EndpointType.EXTERNAL_FQDN


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
                    # Get the current connected address - if any
                    # This is the address that is being moved
                    # and we need to handle only connections
                    # connected to the same address
                    moving_address_src = (
                        self.connection.getpeername() if self.connection else None
                    )

                    if getattr(self.pool, "set_in_maintenance", False):
                        # Set pool in maintenance mode - executed only if
                        # BlockingConnectionPool is used
                        self.pool.set_in_maintenance(True)

                    # Update maintenance state, timeout and optionally host address
                    # connection settings for matching connections
                    self.pool.update_connections_settings(
                        state=MaintenanceState.MOVING,
                        maintenance_event_hash=hash(event),
                        relax_timeout=self.config.relax_timeout,
                        host_address=event.new_node_host,
                        matching_address=moving_address_src,
                        matching_pattern="connected_address",
                        update_event_hash=True,
                        include_free_connections=True,
                    )

                    if self.config.proactive_reconnect:
                        if event.new_node_host is not None:
                            self.run_proactive_reconnect(moving_address_src)
                        else:
                            threading.Timer(
                                event.ttl / 2,
                                self.run_proactive_reconnect,
                                args=(moving_address_src,),
                            ).start()

                    # Update config for new connections:
                    # Set state to MOVING
                    # update host
                    # if relax timeouts are enabled - update timeouts
                    kwargs: dict = {
                        "maintenance_state": MaintenanceState.MOVING,
                        "maintenance_event_hash": hash(event),
                    }
                    if event.new_node_host is not None:
                        # the host is not updated if the new node host is None
                        # this happens when the MOVING push notification does not contain
                        # the new node host - in this case we only update the timeouts
                        kwargs.update(
                            {
                                "host": event.new_node_host,
                            }
                        )
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

    def run_proactive_reconnect(self, moving_address_src: Optional[str] = None):
        """
        Run proactive reconnect for the pool.
        Active connections are marked for reconnect after they complete the current command.
        Inactive connections are disconnected and will be connected on next use.
        """
        with self._lock:
            with self.pool._lock:
                # take care for the active connections in the pool
                # mark them for reconnect after they complete the current command
                self.pool.update_active_connections_for_reconnect(
                    moving_address_src=moving_address_src,
                )
                # take care for the inactive connections in the pool
                # delete them and create new ones
                self.pool.disconnect_free_connections(
                    moving_address_src=moving_address_src,
                )

    def handle_node_moved_event(self, event: NodeMovingEvent):
        """
        Handle the cleanup after a node moving event expires.
        """
        event_hash = hash(event)

        with self._lock:
            # if the current maintenance_event_hash in kwargs is not matching the event
            # it means there has been a new moving event after this one
            # and we don't need to revert the kwargs yet
            if self.pool.connection_kwargs.get("maintenance_event_hash") == event_hash:
                orig_host = self.pool.connection_kwargs.get("orig_host_address")
                orig_socket_timeout = self.pool.connection_kwargs.get(
                    "orig_socket_timeout"
                )
                orig_connect_timeout = self.pool.connection_kwargs.get(
                    "orig_socket_connect_timeout"
                )
                kwargs: dict = {
                    "maintenance_state": MaintenanceState.NONE,
                    "maintenance_event_hash": None,
                    "host": orig_host,
                    "socket_timeout": orig_socket_timeout,
                    "socket_connect_timeout": orig_connect_timeout,
                }
                self.pool.update_connection_kwargs(**kwargs)

            with self.pool._lock:
                reset_relax_timeout = self.config.is_relax_timeouts_enabled()
                reset_host_address = self.config.proactive_reconnect

                self.pool.update_connections_settings(
                    relax_timeout=-1,
                    state=MaintenanceState.NONE,
                    maintenance_event_hash=None,
                    matching_event_hash=event_hash,
                    matching_pattern="event_hash",
                    update_event_hash=True,
                    reset_relax_timeout=reset_relax_timeout,
                    reset_host_address=reset_host_address,
                    include_free_connections=True,
                )


class MaintenanceEventConnectionHandler:
    # 1 = "starting maintenance" events, 0 = "completed maintenance" events
    _EVENT_TYPES: dict[type["MaintenanceEvent"], int] = {
        NodeMigratingEvent: 1,
        NodeFailingOverEvent: 1,
        NodeMigratedEvent: 0,
        NodeFailedOverEvent: 0,
    }

    def __init__(
        self, connection: "ConnectionInterface", config: MaintenanceEventsConfig
    ) -> None:
        self.connection = connection
        self.config = config

    def handle_event(self, event: MaintenanceEvent):
        # get the event type by checking its class in the _EVENT_TYPES dict
        event_type = self._EVENT_TYPES.get(event.__class__, None)

        if event_type is None:
            logging.error(f"Unhandled event type: {event}")
            return

        if event_type:
            self.handle_maintenance_start_event(MaintenanceState.MAINTENANCE)
        else:
            self.handle_maintenance_completed_event()

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
