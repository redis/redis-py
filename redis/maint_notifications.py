import enum
import ipaddress
import logging
import re
import threading
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal, Optional, Union

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


class MaintenanceNotification(ABC):
    """
    Base class for maintenance notifications sent through push messages by Redis server.

    This class provides common functionality for all maintenance notifications including
    unique identification and TTL (Time-To-Live) functionality.

    Attributes:
        id (int): Unique identifier for this notification
        ttl (int): Time-to-live in seconds for this notification
        creation_time (float): Timestamp when the notification was created/read
    """

    def __init__(self, id: int, ttl: int):
        """
        Initialize a new MaintenanceNotification with unique ID and TTL functionality.

        Args:
            id (int): Unique identifier for this notification
            ttl (int): Time-to-live in seconds for this notification
        """
        self.id = id
        self.ttl = ttl
        self.creation_time = time.monotonic()
        self.expire_at = self.creation_time + self.ttl

    def is_expired(self) -> bool:
        """
        Check if this notification has expired based on its TTL
        and creation time.

        Returns:
            bool: True if the notification has expired, False otherwise
        """
        return time.monotonic() > (self.creation_time + self.ttl)

    @abstractmethod
    def __repr__(self) -> str:
        """
        Return a string representation of the maintenance notification.

        This method must be implemented by all concrete subclasses.

        Returns:
            str: String representation of the notification
        """
        pass

    @abstractmethod
    def __eq__(self, other) -> bool:
        """
        Compare two maintenance notifications for equality.

        This method must be implemented by all concrete subclasses.
        Notifications are typically considered equal if they have the same id
        and are of the same type.

        Args:
            other: The other object to compare with

        Returns:
            bool: True if the notifications are equal, False otherwise
        """
        pass

    @abstractmethod
    def __hash__(self) -> int:
        """
        Return a hash value for the maintenance notification.

        This method must be implemented by all concrete subclasses to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value for the notification
        """
        pass


class NodeMovingNotification(MaintenanceNotification):
    """
    This notification is received when a node is replaced with a new node
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
        Initialize a new NodeMovingNotification.

        Args:
            id (int): Unique identifier for this notification
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
        Two NodeMovingNotification notifications are considered equal if they have the same
        id, new_node_host, and new_node_port.
        """
        if not isinstance(other, NodeMovingNotification):
            return False
        return (
            self.id == other.id
            and self.new_node_host == other.new_node_host
            and self.new_node_port == other.new_node_port
        )

    def __hash__(self) -> int:
        """
        Return a hash value for the notification to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on notification type class name, id,
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


class NodeMigratingNotification(MaintenanceNotification):
    """
    Notification for when a Redis cluster node is in the process of migrating slots.

    This notification is received when a node starts migrating its slots to another node
    during cluster rebalancing or maintenance operations.

    Args:
        id (int): Unique identifier for this notification
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
        Two NodeMigratingNotification notifications are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeMigratingNotification):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the notification to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on notification type and id
        """
        return hash((self.__class__.__name__, int(self.id)))


class NodeMigratedNotification(MaintenanceNotification):
    """
    Notification for when a Redis cluster node has completed migrating slots.

    This notification is received when a node has finished migrating all its slots
    to other nodes during cluster rebalancing or maintenance operations.

    Args:
        id (int): Unique identifier for this notification
    """

    DEFAULT_TTL = 5

    def __init__(self, id: int):
        super().__init__(id, NodeMigratedNotification.DEFAULT_TTL)

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
        Two NodeMigratedNotification notifications are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeMigratedNotification):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the notification to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on notification type and id
        """
        return hash((self.__class__.__name__, int(self.id)))


class NodeFailingOverNotification(MaintenanceNotification):
    """
    Notification for when a Redis cluster node is in the process of failing over.

    This notification is received when a node starts a failover process during
    cluster maintenance operations or when handling node failures.

    Args:
        id (int): Unique identifier for this notification
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
        Two NodeFailingOverNotification notifications are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeFailingOverNotification):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the notification to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on notification type and id
        """
        return hash((self.__class__.__name__, int(self.id)))


class NodeFailedOverNotification(MaintenanceNotification):
    """
    Notification for when a Redis cluster node has completed a failover.

    This notification is received when a node has finished the failover process
    during cluster maintenance operations or after handling node failures.

    Args:
        id (int): Unique identifier for this notification
    """

    DEFAULT_TTL = 5

    def __init__(self, id: int):
        super().__init__(id, NodeFailedOverNotification.DEFAULT_TTL)

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
        Two NodeFailedOverNotification notifications are considered equal if they have the same
        id and are of the same type.
        """
        if not isinstance(other, NodeFailedOverNotification):
            return False
        return self.id == other.id and type(self) is type(other)

    def __hash__(self) -> int:
        """
        Return a hash value for the notification to allow
        instances to be used in sets and as dictionary keys.

        Returns:
            int: Hash value based on notification type and id
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


class MaintNotificationsConfig:
    """
    Configuration class for maintenance notifications handling behaviour. Notifications are received through
    push notifications.

    This class defines how the Redis client should react to different push notifications
    such as node moving, migrations, etc. in a Redis cluster.

    """

    def __init__(
        self,
        enabled: Union[bool, Literal["auto"]] = "auto",
        proactive_reconnect: bool = True,
        relaxed_timeout: Optional[Number] = 10,
        endpoint_type: Optional[EndpointType] = None,
    ):
        """
        Initialize a new MaintNotificationsConfig.

        Args:
            enabled (bool | "auto"): Controls maintenance notifications handling behavior.
                - True: The CLIENT MAINT_NOTIFICATIONS command must succeed during connection setup,
                otherwise a ResponseError is raised.
                - "auto": The CLIENT MAINT_NOTIFICATIONS command is attempted but failures are
                gracefully handled - a warning is logged and normal operation continues.
                - False: Maintenance notifications are completely disabled.
                Defaults to "auto".
            proactive_reconnect (bool): Whether to proactively reconnect when a node is replaced.
                Defaults to True.
            relaxed_timeout (Number): The relaxed timeout to use for the connection during maintenance.
                If -1 is provided - the relaxed timeout is disabled. Defaults to 20.
            endpoint_type (Optional[EndpointType]): Override for the endpoint type to use in CLIENT MAINT_NOTIFICATIONS.
                If None, the endpoint type will be automatically determined based on the host and TLS configuration.
                Defaults to None.

        Raises:
            ValueError: If endpoint_type is provided but is not a valid endpoint type.
        """
        self.enabled = enabled
        self.relaxed_timeout = relaxed_timeout
        self.proactive_reconnect = proactive_reconnect
        self.endpoint_type = endpoint_type

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"enabled={self.enabled}, "
            f"proactive_reconnect={self.proactive_reconnect}, "
            f"relaxed_timeout={self.relaxed_timeout}, "
            f"endpoint_type={self.endpoint_type!r}"
            f")"
        )

    def is_relaxed_timeouts_enabled(self) -> bool:
        """
        Check if the relaxed_timeout is enabled. The '-1' value is used to disable the relaxed_timeout.
        If relaxed_timeout is set to None, it will make the operation blocking
        and waiting until any response is received.

        Returns:
            True if the relaxed_timeout is enabled, False otherwise.
        """
        return self.relaxed_timeout != -1

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


class MaintNotificationsPoolHandler:
    def __init__(
        self,
        pool: Union["ConnectionPool", "BlockingConnectionPool"],
        config: MaintNotificationsConfig,
    ) -> None:
        self.pool = pool
        self.config = config
        self._processed_notifications = set()
        self._lock = threading.RLock()
        self.connection = None

    def set_connection(self, connection: "ConnectionInterface"):
        self.connection = connection

    def remove_expired_notifications(self):
        with self._lock:
            for notification in tuple(self._processed_notifications):
                if notification.is_expired():
                    self._processed_notifications.remove(notification)

    def handle_notification(self, notification: MaintenanceNotification):
        self.remove_expired_notifications()

        if isinstance(notification, NodeMovingNotification):
            return self.handle_node_moving_notification(notification)
        else:
            logging.error(f"Unhandled notification type: {notification}")

    def handle_node_moving_notification(self, notification: NodeMovingNotification):
        if (
            not self.config.proactive_reconnect
            and not self.config.is_relaxed_timeouts_enabled()
        ):
            return
        with self._lock:
            if notification in self._processed_notifications:
                # nothing to do in the connection pool handling
                # the notification has already been handled or is expired
                # just return
                return

            with self.pool._lock:
                if (
                    self.config.proactive_reconnect
                    or self.config.is_relaxed_timeouts_enabled()
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
                        maintenance_notification_hash=hash(notification),
                        relaxed_timeout=self.config.relaxed_timeout,
                        host_address=notification.new_node_host,
                        matching_address=moving_address_src,
                        matching_pattern="connected_address",
                        update_notification_hash=True,
                        include_free_connections=True,
                    )

                    if self.config.proactive_reconnect:
                        if notification.new_node_host is not None:
                            self.run_proactive_reconnect(moving_address_src)
                        else:
                            threading.Timer(
                                notification.ttl / 2,
                                self.run_proactive_reconnect,
                                args=(moving_address_src,),
                            ).start()

                    # Update config for new connections:
                    # Set state to MOVING
                    # update host
                    # if relax timeouts are enabled - update timeouts
                    kwargs: dict = {
                        "maintenance_state": MaintenanceState.MOVING,
                        "maintenance_notification_hash": hash(notification),
                    }
                    if notification.new_node_host is not None:
                        # the host is not updated if the new node host is None
                        # this happens when the MOVING push notification does not contain
                        # the new node host - in this case we only update the timeouts
                        kwargs.update(
                            {
                                "host": notification.new_node_host,
                            }
                        )
                    if self.config.is_relaxed_timeouts_enabled():
                        kwargs.update(
                            {
                                "socket_timeout": self.config.relaxed_timeout,
                                "socket_connect_timeout": self.config.relaxed_timeout,
                            }
                        )
                    self.pool.update_connection_kwargs(**kwargs)

                    if getattr(self.pool, "set_in_maintenance", False):
                        self.pool.set_in_maintenance(False)

            threading.Timer(
                notification.ttl,
                self.handle_node_moved_notification,
                args=(notification,),
            ).start()

            self._processed_notifications.add(notification)

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

    def handle_node_moved_notification(self, notification: NodeMovingNotification):
        """
        Handle the cleanup after a node moving notification expires.
        """
        notification_hash = hash(notification)

        with self._lock:
            # if the current maintenance_notification_hash in kwargs is not matching the notification
            # it means there has been a new moving notification after this one
            # and we don't need to revert the kwargs yet
            if (
                self.pool.connection_kwargs.get("maintenance_notification_hash")
                == notification_hash
            ):
                orig_host = self.pool.connection_kwargs.get("orig_host_address")
                orig_socket_timeout = self.pool.connection_kwargs.get(
                    "orig_socket_timeout"
                )
                orig_connect_timeout = self.pool.connection_kwargs.get(
                    "orig_socket_connect_timeout"
                )
                kwargs: dict = {
                    "maintenance_state": MaintenanceState.NONE,
                    "maintenance_notification_hash": None,
                    "host": orig_host,
                    "socket_timeout": orig_socket_timeout,
                    "socket_connect_timeout": orig_connect_timeout,
                }
                self.pool.update_connection_kwargs(**kwargs)

            with self.pool._lock:
                reset_relaxed_timeout = self.config.is_relaxed_timeouts_enabled()
                reset_host_address = self.config.proactive_reconnect

                self.pool.update_connections_settings(
                    relaxed_timeout=-1,
                    state=MaintenanceState.NONE,
                    maintenance_notification_hash=None,
                    matching_notification_hash=notification_hash,
                    matching_pattern="notification_hash",
                    update_notification_hash=True,
                    reset_relaxed_timeout=reset_relaxed_timeout,
                    reset_host_address=reset_host_address,
                    include_free_connections=True,
                )


class MaintNotificationsConnectionHandler:
    # 1 = "starting maintenance" notifications, 0 = "completed maintenance" notifications
    _NOTIFICATION_TYPES: dict[type["MaintenanceNotification"], int] = {
        NodeMigratingNotification: 1,
        NodeFailingOverNotification: 1,
        NodeMigratedNotification: 0,
        NodeFailedOverNotification: 0,
    }

    def __init__(
        self, connection: "ConnectionInterface", config: MaintNotificationsConfig
    ) -> None:
        self.connection = connection
        self.config = config

    def handle_notification(self, notification: MaintenanceNotification):
        # get the notification type by checking its class in the _NOTIFICATION_TYPES dict
        notification_type = self._NOTIFICATION_TYPES.get(notification.__class__, None)

        if notification_type is None:
            logging.error(f"Unhandled notification type: {notification}")
            return

        if notification_type:
            self.handle_maintenance_start_notification(MaintenanceState.MAINTENANCE)
        else:
            self.handle_maintenance_completed_notification()

    def handle_maintenance_start_notification(
        self, maintenance_state: MaintenanceState
    ):
        if (
            self.connection.maintenance_state == MaintenanceState.MOVING
            or not self.config.is_relaxed_timeouts_enabled()
        ):
            return

        self.connection.maintenance_state = maintenance_state
        self.connection.set_tmp_settings(
            tmp_relaxed_timeout=self.config.relaxed_timeout
        )
        # extend the timeout for all created connections
        self.connection.update_current_socket_timeout(self.config.relaxed_timeout)

    def handle_maintenance_completed_notification(self):
        # Only reset timeouts if state is not MOVING and relaxed timeouts are enabled
        if (
            self.connection.maintenance_state == MaintenanceState.MOVING
            or not self.config.is_relaxed_timeouts_enabled()
        ):
            return
        self.connection.reset_tmp_settings(reset_relaxed_timeout=True)
        # Maintenance completed - reset the connection
        # timeouts by providing -1 as the relaxed timeout
        self.connection.update_current_socket_timeout(-1)
        self.connection.maintenance_state = MaintenanceState.NONE
