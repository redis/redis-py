"""
Redis Keyspace Notifications support for redis-py.

This module provides utilities for subscribing to and parsing Redis keyspace
notifications. Keyspace notifications allow clients to receive events when
keys are modified in Redis.

For more information, see:
https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/

Standalone Redis Example:
    >>> from redis import Redis
    >>> from redis.keyspace_notifications import (
    ...     KeyNotification,
    ...     KeyspaceChannel,
    ... )
    >>>
    >>> r = Redis()
    >>> pubsub = r.pubsub()
    >>>
    >>> # Subscribe to keyspace events for keys starting with "user:"
    >>> channel = KeyspaceChannel("user:*")
    >>> pubsub.psubscribe(str(channel))
    >>>
    >>> for message in pubsub.listen():
    ...     notification = KeyNotification.from_message(message)
    ...     if notification:
    ...         print(f"Key: {notification.key}, Event: {notification.event_type}")

Redis Cluster Example:
    >>> from redis.cluster import RedisCluster
    >>> from redis.keyspace_notifications import (
    ...     ClusterKeyspaceNotifications,
    ...     KeyspaceChannel,
    ...     EventType,
    ... )
    >>>
    >>> rc = RedisCluster(host="localhost", port=7000)
    >>> ksn = ClusterKeyspaceNotifications(rc)
    >>>
    >>> # Subscribe using Channel class (patterns auto-detected)
    >>> channel = KeyspaceChannel("user:*")
    >>> ksn.subscribe(channel)
    >>>
    >>> # Or use convenience methods for specific event types
    >>> ksn.subscribe_keyevent(EventType.SET)
    >>>
    >>> for notification in ksn.listen():
    ...     print(f"Key: {notification.key}, Event: {notification.event_type}")
"""

import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from redis.exceptions import (
    AskError,
    ConnectionError,
    MovedError,
    RedisError,
    TimeoutError,
)

# Type alias for channel arguments - can be a string, bytes, or Channel object
# This is defined here and the actual types are added after class definitions
ChannelT = Union[str, bytes, "KeyspaceChannel", "KeyeventChannel"]


# =============================================================================
# Event Type Constants
# =============================================================================
# These are common Redis keyspace notification event types provided for
# convenience. You can use any string as an event type - these constants
# are not exhaustive and Redis may add new events in future versions.


class EventType:
    """
    Common Redis keyspace notification event type constants.

    These are provided for convenience and IDE autocomplete. You can use
    any string as an event type - new Redis events will work without
    needing library updates.

    Example:
        >>> from redis.keyspace_notifications import EventType, keyevent_channel
        >>> channel = keyevent_channel(EventType.SET, db=0)
        >>> # Or just use a string directly:
        >>> channel = keyevent_channel("set", db=0)
    """

    # String commands
    SET = "set"
    SETEX = "setex"
    SETNX = "setnx"
    SETRANGE = "setrange"
    INCR = "incr"
    INCRBY = "incrby"
    INCRBYFLOAT = "incrbyfloat"
    DECR = "decr"
    DECRBY = "decrby"
    APPEND = "append"

    # Generic commands
    DEL = "del"
    UNLINK = "unlink"
    RENAME = "rename"
    RENAME_FROM = "rename_from"
    RENAME_TO = "rename_to"
    COPY_TO = "copy_to"
    MOVE = "move"
    RESTORE = "restore"

    # Expiration events
    EXPIRE = "expire"
    EXPIREAT = "expireat"
    PEXPIRE = "pexpire"
    PEXPIREAT = "pexpireat"
    EXPIRED = "expired"
    PERSIST = "persist"

    # Eviction events
    EVICTED = "evicted"

    # List commands
    LPUSH = "lpush"
    RPUSH = "rpush"
    LPOP = "lpop"
    RPOP = "rpop"
    LINSERT = "linsert"
    LSET = "lset"
    LTRIM = "ltrim"
    LMOVE = "lmove"
    BLPOP = "blpop"
    BRPOP = "brpop"
    BLMOVE = "blmove"

    # Set commands
    SADD = "sadd"
    SREM = "srem"
    SPOP = "spop"
    SMOVE = "smove"
    SINTERSTORE = "sinterstore"
    SUNIONSTORE = "sunionstore"
    SDIFFSTORE = "sdiffstore"

    # Sorted set commands
    ZADD = "zadd"
    ZINCRBY = "zincrby"
    ZREM = "zrem"
    ZREMRANGEBYRANK = "zremrangebyrank"
    ZREMRANGEBYSCORE = "zremrangebyscore"
    ZREMRANGEBYLEX = "zremrangebylex"
    ZPOPMIN = "zpopmin"
    ZPOPMAX = "zpopmax"
    BZPOPMIN = "bzpopmin"
    BZPOPMAX = "bzpopmax"
    ZINTERSTORE = "zinterstore"
    ZUNIONSTORE = "zunionstore"
    ZDIFFSTORE = "zdiffstore"
    ZRANGESTORE = "zrangestore"

    # Hash commands
    HSET = "hset"
    HSETNX = "hsetnx"
    HDEL = "hdel"
    HINCRBY = "hincrby"
    HINCRBYFLOAT = "hincrbyfloat"

    # Stream commands
    XADD = "xadd"
    XTRIM = "xtrim"
    XDEL = "xdel"
    XGROUP_CREATE = "xgroup-create"
    XGROUP_CREATECONSUMER = "xgroup-createconsumer"
    XGROUP_DELCONSUMER = "xgroup-delconsumer"
    XGROUP_DESTROY = "xgroup-destroy"
    XGROUP_SETID = "xgroup-setid"
    XSETID = "xsetid"
    XCLAIM = "xclaim"
    XAUTOCLAIM = "xautoclaim"
    XREADGROUP = "xreadgroup"

    # Other
    NEW = "new"  # Key created (when tracking new keys)
    SORTSTORE = "sortstore"
    GETEX = "getex"
    GETDEL = "getdel"
    SETIFGT = "setifgt"
    SETIFLT = "setiflt"
    SETIFEQ = "setifeq"
    SETIFNE = "setifne"


# Backwards compatibility alias
KeyNotificationType = EventType


# Channel prefixes used by Redis keyspace notifications
KEYSPACE_PREFIX = "__keyspace@"
KEYEVENT_PREFIX = "__keyevent@"

# Regex patterns for parsing keyspace/keyevent channels
# Pattern: __keyspace@<db>__:<key> or __keyevent@<db>__:<event>
_KEYSPACE_PATTERN = re.compile(r"^__keyspace@(\d+|\*)__:(.+)$")
_KEYEVENT_PATTERN = re.compile(r"^__keyevent@(\d+|\*)__:(.+)$")


@dataclass
class KeyNotification:
    """
    Represents a parsed Redis keyspace or keyevent notification.

    This class provides convenient access to the notification details
    like key, event type, and database number.

    Attributes:
        key: The Redis key that was affected (for keyspace notifications)
             or the key name from the message data (for keyevent notifications)
        event_type: The type of operation that occurred (e.g., "set", "del").
                   This is a plain string, so new Redis events work automatically.
                   Compare against EventType constants or any string.
        database: The database number where the event occurred
        channel: The original channel name
        is_keyspace: True if this is a keyspace notification, False for keyevent
    """

    key: str
    event_type: str
    database: int
    channel: str
    is_keyspace: bool

    @classmethod
    def from_message(
        cls,
        message: Dict[str, Any],
        key_prefix: Optional[Union[str, bytes]] = None,
    ) -> Optional["KeyNotification"]:
        """
        Parse a pub/sub message into a KeyNotification.

        Args:
            message: A pub/sub message dict with 'channel', 'data', and 'type' keys
            key_prefix: Optional prefix to filter and strip from keys.
                       If provided, only notifications for keys starting with
                       this prefix will be returned, and the prefix will be
                       stripped from the key.

        Returns:
            A KeyNotification if the message is a valid keyspace/keyevent
            notification, None otherwise.

        Example:
            >>> message = {
            ...     'type': 'pmessage',
            ...     'pattern': '__keyspace@0__:user:*',
            ...     'channel': '__keyspace@0__:user:123',
            ...     'data': 'set'
            ... }
            >>> notification = KeyNotification.from_message(message)
            >>> notification.key
            'user:123'
            >>> notification.event_type
            'set'
        """
        if message is None:
            return None

        msg_type = message.get("type")
        if msg_type not in ("message", "pmessage"):
            return None

        channel = message.get("channel")
        data = message.get("data")

        if channel is None or data is None:
            return None

        # Convert bytes to string if needed
        if isinstance(channel, bytes):
            channel = channel.decode("utf-8", errors="replace")
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")

        return cls._parse(channel, data, key_prefix)

    @classmethod
    def try_parse(
        cls,
        channel: Union[str, bytes],
        data: Union[str, bytes],
        key_prefix: Optional[Union[str, bytes]] = None,
    ) -> Optional["KeyNotification"]:
        """
        Try to parse a channel and data into a KeyNotification.

        This is a lower-level method that takes the channel and data directly,
        useful when working with callback-based subscription handlers.

        Args:
            channel: The channel name (e.g., "__keyspace@0__:mykey")
            data: The message data (event type for keyspace, key for keyevent)
            key_prefix: Optional prefix to filter and strip from keys

        Returns:
            A KeyNotification if valid, None otherwise.
        """
        if isinstance(channel, bytes):
            channel = channel.decode("utf-8", errors="replace")
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")

        return cls._parse(channel, data, key_prefix)

    @classmethod
    def _parse(
        cls,
        channel: str,
        data: str,
        key_prefix: Optional[Union[str, bytes]] = None,
    ) -> Optional["KeyNotification"]:
        """Internal parsing logic."""
        # Normalize key_prefix
        if isinstance(key_prefix, bytes):
            key_prefix = key_prefix.decode("utf-8", errors="replace")

        # Try keyspace pattern first: __keyspace@<db>__:<key>
        match = _KEYSPACE_PATTERN.match(channel)
        if match:
            db_str, key = match.groups()
            database = int(db_str) if db_str != "*" else -1
            event_type = data  # For keyspace, the data is the event type

            # Apply key prefix filter
            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=True,
            )

        # Try keyevent pattern: __keyevent@<db>__:<event>
        match = _KEYEVENT_PATTERN.match(channel)
        if match:
            db_str, event_type = match.groups()
            database = int(db_str) if db_str != "*" else -1
            key = data  # For keyevent, the data is the key

            # Apply key prefix filter
            if key_prefix:
                if not key.startswith(key_prefix):
                    return None
                key = key[len(key_prefix) :]

            return cls(
                key=key,
                event_type=event_type,
                database=database,
                channel=channel,
                is_keyspace=False,
            )

        return None

    def key_starts_with(self, prefix: Union[str, bytes]) -> bool:
        """Check if the key starts with the given prefix."""
        if isinstance(prefix, bytes):
            prefix = prefix.decode("utf-8", errors="replace")
        return self.key.startswith(prefix)


# =============================================================================
# Channel Classes
# =============================================================================


class KeyspaceChannel:
    """
    Represents a keyspace notification channel for subscribing to events on keys.

    Keyspace notifications publish the event type (e.g., "set", "del") as the message
    when a key matching the pattern is modified.

    This class can be used directly with subscribe()/psubscribe() as it implements
    __str__ to return the channel string.

    Attributes:
        key_or_pattern: The key or pattern to monitor
        db: The database number (None means all databases)
        is_pattern: Whether this channel contains wildcards

    Examples:
        >>> channel = KeyspaceChannel("user:123", db=0)
        >>> str(channel)
        '__keyspace@0__:user:123'

        >>> channel = KeyspaceChannel.pattern("user:", db=0)
        >>> str(channel)
        '__keyspace@0__:user:*'

        >>> # Use directly with pubsub
        >>> pubsub.subscribe(channel)
    """

    def __init__(self, key_or_pattern: str, db: int = 0):
        """
        Create a keyspace notification channel.

        Args:
            key_or_pattern: The key or pattern to monitor. Use '*' for wildcards.
            db: The database number. Defaults to 0 (the only database in Redis Cluster).
        """
        self.key_or_pattern = key_or_pattern
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return f"{KEYSPACE_PREFIX}{self.db}__:{self.key_or_pattern}"

    @property
    def is_pattern(self) -> bool:
        """Check if this channel contains wildcards and should use psubscribe."""
        return _is_pattern(self._channel_str)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return f"KeyspaceChannel({self.key_or_pattern!r}, db={self.db})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, KeyspaceChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


class KeyeventChannel:
    """
    Represents a keyevent notification channel for subscribing to event types.

    Keyevent notifications publish the key name as the message when the specified
    event type occurs on any key.

    This class can be used directly with subscribe()/psubscribe() as it implements
    __str__ to return the channel string.

    Attributes:
        event: The event type to monitor
        db: The database number (None means all databases)
        is_pattern: Whether this channel contains wildcards

    Examples:
        >>> channel = KeyeventChannel(EventType.SET, db=0)
        >>> str(channel)
        '__keyevent@0__:set'

        >>> channel = KeyeventChannel.all_events(db=0)
        >>> str(channel)
        '__keyevent@0__:*'

        >>> # Use directly with pubsub
        >>> pubsub.subscribe(channel)
    """

    def __init__(self, event: str, db: int = 0):
        """
        Create a keyevent notification channel.

        Args:
            event: The event type to monitor (e.g., EventType.SET or "set")
            db: The database number. Defaults to 0 (the only database in Redis Cluster).
        """
        self.event = event
        self.db = db
        self._channel_str = self._build_channel_string()

    def _build_channel_string(self) -> str:
        return f"{KEYEVENT_PREFIX}{self.db}__:{self.event}"

    @property
    def is_pattern(self) -> bool:
        """Check if this channel contains wildcards and should use psubscribe."""
        return _is_pattern(self._channel_str)

    @classmethod
    def all_events(cls, db: int = 0) -> "KeyeventChannel":
        """
        Create a keyevent pattern for subscribing to all event types.

        This is equivalent to KeyeventChannel("*").

        Args:
            db: The database number. Defaults to 0 (the only database in Redis Cluster).

        Returns:
            A KeyeventChannel configured to receive all events.

        Examples:
            >>> channel = KeyeventChannel.all_events()
            >>> str(channel)
            '__keyevent@0__:*'
        """
        return cls("*", db=db)

    def __str__(self) -> str:
        return self._channel_str

    def __repr__(self) -> str:
        return f"KeyeventChannel({self.event!r}, db={self.db})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, KeyeventChannel):
            return self._channel_str == other._channel_str
        if isinstance(other, str):
            return self._channel_str == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._channel_str)


def is_keyspace_channel(channel: Union[str, bytes]) -> bool:
    """Check if a channel is a keyspace notification channel."""
    if isinstance(channel, bytes):
        channel = channel.decode("utf-8", errors="replace")
    return channel.startswith(KEYSPACE_PREFIX)


def is_keyevent_channel(channel: Union[str, bytes]) -> bool:
    """Check if a channel is a keyevent notification channel."""
    if isinstance(channel, bytes):
        channel = channel.decode("utf-8", errors="replace")
    return channel.startswith(KEYEVENT_PREFIX)


def is_keyspace_notification_channel(channel: Union[str, bytes]) -> bool:
    """Check if a channel is any type of keyspace notification channel."""
    return is_keyspace_channel(channel) or is_keyevent_channel(channel)


def _is_pattern(channel: Union[str, bytes, "KeyspaceChannel", "KeyeventChannel"]) -> bool:
    """
    Check if a channel string contains glob-style pattern characters.

    Redis uses glob-style patterns for psubscribe:
    - * matches any sequence of characters
    - ? matches any single character
    - [...] matches any character in the brackets

    Args:
        channel: The channel string to check. Can be a string, bytes,
                 or a KeyspaceChannel/KeyeventChannel object.

    Returns:
        True if the channel contains pattern characters, False otherwise.
    """
    # Handle Channel objects that have _channel_str attribute
    # (KeyspaceChannel, KeyeventChannel)
    if hasattr(channel, "_channel_str"):
        channel = channel._channel_str
    if isinstance(channel, bytes):
        channel = channel.decode("utf-8", errors="replace")
    # Check for unescaped glob pattern characters
    # We look for *, ?, or [ that are not escaped with backslash
    i = 0
    while i < len(channel):
        char = channel[i]
        if char == "\\":
            # Skip escaped character
            i += 2
            continue
        if char in ("*", "?", "["):
            return True
        i += 1
    return False


# =============================================================================
# Cluster-Aware Keyspace Notification Manager
# =============================================================================


class ClusterKeyspaceNotifications:
    """
    Manages keyspace notification subscriptions across all nodes in a Redis Cluster.

    In Redis Cluster, keyspace notifications are NOT broadcast between nodes.
    Each node only emits notifications for keys it owns. This class automatically
    subscribes to all primary nodes in the cluster and handles topology changes.

    Example usage:
        >>> from redis.cluster import RedisCluster
        >>> from redis.keyspace_notifications import (
        ...     ClusterKeyspaceNotifications,
        ...     KeyNotificationType,
        ... )
        >>>
        >>> rc = RedisCluster(host="localhost", port=7000)
        >>> ksn = ClusterKeyspaceNotifications(rc)
        >>>
        >>> # Subscribe to keyspace events for keys matching "user:*"
        >>> ksn.subscribe_keyspace("user:*", db=0)
        >>>
        >>> # Or subscribe to all SET events
        >>> ksn.subscribe_keyevent(KeyNotificationType.SET, db=0)
        >>>
        >>> # With a handler callback
        >>> def handle_notification(notification):
        ...     print(f"Key {notification.key} was modified: {notification.event_type}")
        >>> ksn.subscribe_keyspace("cache:*", db=0, handler=handle_notification)
        >>>
        >>> # Start listening (blocking)
        >>> for notification in ksn.listen():
        ...     print(f"{notification.key}: {notification.event_type}")
        >>>
        >>> # Or use get_message for non-blocking
        >>> notification = ksn.get_message()
    """

    def __init__(
        self,
        redis_cluster,
        key_prefix: Optional[Union[str, bytes]] = None,
        ignore_subscribe_messages: bool = True,
    ):
        """
        Initialize the cluster keyspace notification manager.

        Args:
            redis_cluster: A RedisCluster instance
            key_prefix: Optional prefix to filter and strip from keys in notifications
            ignore_subscribe_messages: If True, subscribe/unsubscribe confirmations
                                      are not returned by get_message/listen
        """
        self.cluster = redis_cluster
        self.key_prefix = key_prefix
        self.ignore_subscribe_messages = ignore_subscribe_messages

        # Track subscriptions per node
        self._node_pubsubs: Dict[str, Any] = {}
        self._subscribed_patterns: Dict[str, Any] = {}  # pattern -> handler
        self._subscribed_channels: Dict[str, Any] = {}  # channel -> handler
        self._closed = False

        # Generator for round-robin message retrieval
        self._pubsub_iter = None

        # Debounce refresh to avoid excessive topology checks
        self._last_refresh_time: float = 0.0
        self._refresh_cooldown_seconds: float = 1.0

        # Track known primary nodes for automatic topology change detection
        self._known_primary_names: set = set()

        # Periodic topology check interval (in seconds)
        self._topology_check_interval: float = 1.0
        self._last_topology_check: float = 0.0

    def _get_all_primary_nodes(self):
        """Get all primary nodes in the cluster."""
        return [
            node
            for node in self.cluster.get_nodes()
            if node.server_type == "primary"
        ]

    def _ensure_node_pubsub(self, node) -> Any:
        """Get or create a PubSub instance for a node."""
        if node.name not in self._node_pubsubs:
            redis_conn = self.cluster.get_redis_connection(node)
            pubsub = redis_conn.pubsub(
                ignore_subscribe_messages=self.ignore_subscribe_messages
            )
            self._node_pubsubs[node.name] = pubsub
        return self._node_pubsubs[node.name]

    def _subscribe_to_all_nodes(
        self, channels: Dict[str, Any], use_psubscribe: bool
    ):
        """Subscribe to patterns/channels on all primary nodes."""
        primaries = self._get_all_primary_nodes()

        # Track known primaries for topology change detection
        self._known_primary_names = {node.name for node in primaries}

        # Initialize topology check timer to avoid immediate re-check
        self._last_topology_check = time.monotonic()

        for node in primaries:
            pubsub = self._ensure_node_pubsub(node)
            if use_psubscribe:
                pubsub.psubscribe(**channels)
            else:
                pubsub.subscribe(**channels)

    def _unsubscribe_from_all_nodes(
        self, channels: List[str], use_punsubscribe: bool
    ):
        """Unsubscribe from patterns/channels on all nodes."""
        for pubsub in self._node_pubsubs.values():
            if use_punsubscribe:
                pubsub.punsubscribe(*channels)
            else:
                pubsub.unsubscribe(*channels)

    def subscribe(
        self,
        *channels: ChannelT,
        handler: Optional[Callable[[KeyNotification], None]] = None,
    ):
        """
        Subscribe to keyspace notification channels on all cluster nodes.

        Automatically detects whether each channel is a pattern (contains
        wildcards like *, ?, [) or an exact channel name and uses the
        appropriate Redis subscribe command internally.

        Args:
            *channels: Channels to subscribe to. Can be strings, KeyspaceChannel,
                      or KeyeventChannel objects. Patterns are auto-detected.
            handler: Optional callback function that receives KeyNotification
                    objects. If provided, notifications are passed to the handler
                    instead of being returned by get_message()/listen().

        Example:
            >>> # Using Channel classes (recommended)
            >>> ksn.subscribe(KeyspaceChannel("user:123", db=0))
            >>> ksn.subscribe(KeyspaceChannel.pattern("user:", db=0))
            >>>
            >>> # Using strings
            >>> ksn.subscribe("__keyspace@0__:user:123")
            >>> ksn.subscribe("__keyspace@0__:user:*")
            >>>
            >>> # With handler
            >>> ksn.subscribe(KeyspaceChannel.pattern("cache:", db=0), handler=my_handler)
        """
        patterns = {}
        exact_channels = {}

        for channel in channels:
            # Convert Channel objects to strings for use as dict keys
            channel_str = str(channel) if hasattr(channel, "_channel_str") else channel
            if _is_pattern(channel):
                patterns[channel_str] = handler
            else:
                exact_channels[channel_str] = handler

        if patterns:
            self._subscribed_patterns.update(patterns)
            self._subscribe_to_all_nodes(patterns, use_psubscribe=True)

        if exact_channels:
            self._subscribed_channels.update(exact_channels)
            self._subscribe_to_all_nodes(exact_channels, use_psubscribe=False)

    def unsubscribe(self, *channels: ChannelT):
        """
        Unsubscribe from keyspace notification channels on all nodes.

        Automatically detects whether each channel is a pattern or exact
        channel and uses the appropriate Redis unsubscribe command.

        Args:
            *channels: Channels to unsubscribe from. Can be strings,
                      KeyspaceChannel, or KeyeventChannel objects.
        """
        patterns = []
        exact_channels = []

        for channel in channels:
            # Convert Channel objects to strings
            channel_str = str(channel) if hasattr(channel, "_channel_str") else channel
            if _is_pattern(channel):
                self._subscribed_patterns.pop(channel_str, None)
                patterns.append(channel_str)
            else:
                self._subscribed_channels.pop(channel_str, None)
                exact_channels.append(channel_str)

        if patterns:
            self._unsubscribe_from_all_nodes(patterns, use_punsubscribe=True)

        if exact_channels:
            self._unsubscribe_from_all_nodes(exact_channels, use_punsubscribe=False)

    def subscribe_keyspace(
        self,
        key_or_pattern: str,
        db: Optional[int] = None,
        handler: Optional[Callable[[KeyNotification], None]] = None,
    ):
        """
        Subscribe to keyspace notifications for specific keys.

        This is a convenience method that constructs the appropriate keyspace
        channel and subscribes to it.

        Args:
            key_or_pattern: The key or pattern to monitor. Use '*' for wildcards.
            db: The database number. If None, uses '*' to match all databases.
            handler: Optional callback for notifications.

        Example:
            >>> # Monitor a specific key
            >>> ksn.subscribe_keyspace("user:123", db=0)
            >>>
            >>> # Monitor all keys with a prefix
            >>> ksn.subscribe_keyspace("user:*", db=0)
            >>>
            >>> # Monitor across all databases
            >>> ksn.subscribe_keyspace("session:*")
        """
        channel = keyspace_channel(key_or_pattern, db=db)
        self.subscribe(channel, handler=handler)

    def subscribe_keyevent(
        self,
        event: str,
        db: Optional[int] = None,
        handler: Optional[Callable[[KeyNotification], None]] = None,
    ):
        """
        Subscribe to keyevent notifications for specific event types.

        This is a convenience method that constructs the appropriate keyevent
        channel and subscribes to it.

        Args:
            event: The event type to monitor (e.g., EventType.SET or "set")
            db: The database number. If None, uses '*' to match all databases.
            handler: Optional callback for notifications.

        Example:
            >>> # Monitor all SET events
            >>> ksn.subscribe_keyevent(EventType.SET, db=0)
            >>>
            >>> # Monitor all DEL events across all databases
            >>> ksn.subscribe_keyevent("del")
            >>>
            >>> # Monitor all EXPIRED events with a handler
            >>> ksn.subscribe_keyevent(
            ...     EventType.EXPIRED,
            ...     handler=lambda n: print(f"Key expired: {n.key}")
            ... )
        """
        channel = keyevent_channel(event, db=db)
        self.subscribe(channel, handler=handler)

    def _create_pubsub_iterator(self):
        """Create a round-robin iterator over all node pubsubs."""
        while True:
            pubsubs = list(self._node_pubsubs.values())
            if not pubsubs:
                return
            yield from pubsubs

    def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float = 0.0,
    ) -> Optional[KeyNotification]:
        """
        Get the next keyspace notification if one is available.

        This method polls all node pubsubs in round-robin fashion until
        a message is received or the timeout expires.
        If a connection error or topology change (MOVED/ASK) occurs,
        subscriptions are automatically refreshed.

        Args:
            ignore_subscribe_messages: If True, skip subscribe/unsubscribe messages
            timeout: Total time to wait for a message (distributed across all nodes)

        Returns:
            A KeyNotification if a notification is available, None otherwise.
        """
        if self._closed:
            return None

        if self._pubsub_iter is None:
            self._pubsub_iter = self._create_pubsub_iterator()

        total_nodes = len(self._node_pubsubs)
        if total_nodes == 0:
            return None

        # Calculate per-node timeout for each poll
        # Use a small timeout per node to allow round-robin polling
        per_node_timeout = min(0.1, timeout / max(total_nodes, 1))

        start_time = time.monotonic()
        end_time = start_time + timeout

        while True:
            # Check if we've exceeded the total timeout
            if time.monotonic() >= end_time:
                return None

            # Periodically check for topology changes
            # This detects slot migrations that don't cause connection errors
            self._check_topology_changed()

            try:
                pubsub = next(self._pubsub_iter)
            except StopIteration:
                self._pubsub_iter = self._create_pubsub_iterator()
                continue

            try:
                message = pubsub.get_message(
                    ignore_subscribe_messages=ignore_subscribe_messages,
                    timeout=per_node_timeout,
                )
            except (MovedError, AskError):
                # Topology change - refresh subscriptions and continue
                self._refresh_subscriptions_on_error()
                continue
            except (ConnectionError, TimeoutError, RedisError):
                # Connection error - refresh subscriptions and continue
                self._refresh_subscriptions_on_error()
                continue

            if message is not None:
                notification = KeyNotification.from_message(
                    message, key_prefix=self.key_prefix
                )
                if notification is not None:
                    # Call handler if registered
                    handler = self._subscribed_patterns.get(
                        message.get("pattern")
                    ) or self._subscribed_channels.get(message.get("channel"))
                    if handler:
                        handler(notification)
                        # Continue polling for more messages
                        continue
                    return notification
                # If not a keyspace notification, continue checking other nodes

    def listen(self):
        """
        Listen for keyspace notifications from all cluster nodes.

        This is a generator that yields KeyNotification objects as they arrive.
        It blocks until a notification is received.

        Yields:
            KeyNotification objects for each keyspace/keyevent notification.

        Example:
            >>> for notification in ksn.listen():
            ...     print(f"{notification.key}: {notification.event_type}")
        """
        while not self._closed and self._node_pubsubs:
            notification = self.get_message(timeout=1.0)
            if notification is not None:
                yield notification

    def _refresh_subscriptions_on_error(self):
        """
        Refresh subscriptions after a connection error, with debouncing.

        This is called automatically when a connection error occurs during
        get_message(). It uses a cooldown to avoid excessive topology checks.
        """
        now = time.monotonic()
        if now - self._last_refresh_time < self._refresh_cooldown_seconds:
            return  # Skip refresh if within cooldown period

        self._last_refresh_time = now
        self._pubsub_iter = None  # Reset iterator after topology change

        try:
            self.refresh_subscriptions()
        except Exception:
            # Ignore errors during refresh - will retry on next error
            pass

    def _check_topology_changed(self) -> bool:
        """
        Check if the cluster topology has changed since last check.

        This detects when nodes have been added or removed from the cluster,
        which can happen during slot migrations or failovers.

        Returns:
            True if topology changed and subscriptions were refreshed.
        """
        now = time.monotonic()
        if now - self._last_topology_check < self._topology_check_interval:
            return False  # Skip check if within interval

        self._last_topology_check = now

        # Refresh the cluster's view of the topology
        try:
            self.cluster.nodes_manager.initialize()
        except Exception:
            return False

        # Get current primary node names
        current_primaries = {node.name for node in self._get_all_primary_nodes()}

        # Check if topology changed
        if current_primaries != self._known_primary_names:
            self._known_primary_names = current_primaries
            self._pubsub_iter = None  # Reset iterator
            self.refresh_subscriptions()
            return True

        return False

    def refresh_subscriptions(self):
        """
        Refresh subscriptions after a topology change.

        This method is called automatically when topology changes are detected
        or when connection errors occur. You can also call it manually if needed.

        This method:
        1. Discovers any new primary nodes and subscribes them
        2. Removes pubsubs for nodes that are no longer primaries
        """
        current_primaries = {node.name: node for node in self._get_all_primary_nodes()}

        # Remove pubsubs for nodes that are no longer primaries
        removed_nodes = set(self._node_pubsubs.keys()) - set(current_primaries.keys())
        for node_name in removed_nodes:
            pubsub = self._node_pubsubs.pop(node_name, None)
            if pubsub:
                try:
                    pubsub.close()
                except Exception:
                    pass

        # Subscribe new nodes to existing patterns/channels
        new_nodes = set(current_primaries.keys()) - set(self._node_pubsubs.keys())
        for node_name in new_nodes:
            node = current_primaries[node_name]
            pubsub = self._ensure_node_pubsub(node)

            if self._subscribed_patterns:
                pubsub.psubscribe(**self._subscribed_patterns)
            if self._subscribed_channels:
                pubsub.subscribe(**self._subscribed_channels)

    def close(self):
        """Close all pubsub connections and clean up resources."""
        self._closed = True
        for pubsub in self._node_pubsubs.values():
            try:
                pubsub.close()
            except Exception:
                pass
        self._node_pubsubs.clear()
        self._subscribed_patterns.clear()
        self._subscribed_channels.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @property
    def subscribed(self) -> bool:
        """Check if there are any active subscriptions."""
        return bool(self._subscribed_patterns or self._subscribed_channels)

