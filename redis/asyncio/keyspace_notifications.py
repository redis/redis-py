"""
Async Redis Keyspace Notifications support for redis-py.

This module provides async utilities for subscribing to and parsing Redis
keyspace notifications.

Standalone Redis Example:
    >>> from redis.asyncio import Redis
    >>> from redis.asyncio.keyspace_notifications import (
    ...     AsyncKeyspaceNotifications,
    ... )
    >>> from redis.keyspace_notifications import KeyspaceChannel, EventType
    >>>
    >>> async def main():
    ...     async with Redis() as r:
    ...         async with AsyncKeyspaceNotifications(r) as ksn:
    ...             channel = KeyspaceChannel("user:*")
    ...             await ksn.subscribe(channel)
    ...             async for notification in ksn.listen():
    ...                 print(f"Key: {notification.key}, Event: {notification.event_type}")

Redis Cluster Example:
    >>> from redis.asyncio.cluster import RedisCluster
    >>> from redis.asyncio.keyspace_notifications import (
    ...     AsyncClusterKeyspaceNotifications,
    ... )
    >>> from redis.keyspace_notifications import KeyspaceChannel, EventType
    >>>
    >>> async def main():
    ...     async with RedisCluster(host="localhost", port=7000) as rc:
    ...         async with AsyncClusterKeyspaceNotifications(rc) as ksn:
    ...             channel = KeyspaceChannel("user:*")
    ...             await ksn.subscribe(channel)
    ...             async for notification in ksn.listen():
    ...                 print(f"Key: {notification.key}, Event: {notification.event_type}")
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from redis.asyncio.client import PubSub, Redis
from redis.asyncio.cluster import ClusterNode, RedisCluster, _ClusterNodePoolAdapter
from redis.exceptions import (
    ConnectionError,
    RedisError,
    TimeoutError,
)
from redis.keyspace_notifications import (
    ChannelT,
    KeyeventChannel,
    KeyNotification,
    KeyspaceChannel,
    SubkeyeventChannel,
    SubkeyspaceChannel,
    SubkeyspaceeventChannel,
    SubkeyspaceitemChannel,
    _is_pattern,
)
from redis.utils import safe_str

logger = logging.getLogger(__name__)


# Type alias for handlers that can be sync or async
AsyncHandlerT = Callable[[KeyNotification], None | Awaitable[None]]


# =============================================================================
# Async Interface for Keyspace Notifications
# =============================================================================


class AsyncKeyspaceNotificationsInterface(ABC):
    """
    Async interface for keyspace notification managers.

    This interface provides a consistent async API for both standalone
    (AsyncKeyspaceNotifications) and cluster (AsyncClusterKeyspaceNotifications)
    implementations.
    """

    @abstractmethod
    async def subscribe(
        self,
        *channels: ChannelT,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to keyspace notification channels."""
        pass

    @abstractmethod
    async def unsubscribe(self, *channels: ChannelT):
        """Unsubscribe from keyspace notification channels."""
        pass

    @abstractmethod
    async def subscribe_keyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to keyspace notifications for specific keys."""
        pass

    @abstractmethod
    async def subscribe_keyevent(
        self,
        event: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to keyevent notifications for specific event types."""
        pass

    @abstractmethod
    async def subscribe_subkeyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspace notifications for specific keys."""
        pass

    @abstractmethod
    async def subscribe_subkeyevent(
        self,
        event: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyevent notifications for specific event types."""
        pass

    @abstractmethod
    async def subscribe_subkeyspaceitem(
        self,
        key_or_pattern: str,
        subkey_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspaceitem notifications for a specific subkey."""
        pass

    @abstractmethod
    async def subscribe_subkeyspaceevent(
        self,
        event: str,
        key_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspaceevent notifications for an event on a key."""
        pass

    @abstractmethod
    async def get_message(
        self,
        ignore_subscribe_messages: bool | None = None,
        timeout: float = 0.0,
    ) -> KeyNotification | None:
        """Get the next keyspace notification if one is available."""
        pass

    @abstractmethod
    def listen(self) -> AsyncIterator[KeyNotification]:
        """Listen for keyspace notifications."""
        pass

    @abstractmethod
    async def aclose(self):
        """Close the notification manager and clean up resources."""
        pass

    @abstractmethod
    async def __aenter__(self):
        pass

    @abstractmethod
    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        pass

    @property
    @abstractmethod
    def subscribed(self) -> bool:
        """Check if there are any active subscriptions and not closed."""
        pass

    @abstractmethod
    async def run(
        self,
        poll_timeout: float = 1.0,
        exception_handler: Callable[
            [BaseException, AsyncKeyspaceNotificationsInterface],
            None | Awaitable[None],
        ]
        | None = None,
    ) -> None:
        """
        Run the notification loop as a coroutine.

        This is the async equivalent of run_in_thread() for sync notifications.
        Use asyncio.create_task() to run in the background.

        The exception_handler can be either a sync or async function.
        """
        pass


# =============================================================================
# Abstract Base Class for Async Keyspace Notifications
# =============================================================================


class AbstractAsyncKeyspaceNotifications(AsyncKeyspaceNotificationsInterface):
    """
    Abstract base class for async keyspace notification managers.

    Provides shared implementation for subscribe/unsubscribe logic.
    Subclasses must implement:
    - _execute_subscribe: Execute the subscribe operation
    - _execute_unsubscribe: Execute the unsubscribe operation
    - get_message: Get the next notification
    - listen: Async generator for notifications
    - aclose: Clean up resources
    """

    def __init__(
        self,
        key_prefix: str | bytes | None = None,
        ignore_subscribe_messages: bool = True,
    ):
        """
        Initialize the base async keyspace notification manager.

        Args:
            key_prefix: Optional prefix to filter and strip from keys in notifications
            ignore_subscribe_messages: If True, subscribe/unsubscribe confirmations
                                      are not returned by get_message/listen
        """
        self.key_prefix = key_prefix
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self._closed = False

    async def subscribe(
        self,
        *channels: ChannelT,
        handler: AsyncHandlerT | None = None,
    ):
        """
        Subscribe to keyspace notification channels.

        Automatically detects whether each channel is a pattern (contains
        wildcards like *, ?, [) or an exact channel name and uses the
        appropriate Redis subscribe command internally.

        The handler can be either a sync or async function.  Note that a
        **sync** handler will be called directly on the event loop thread,
        so it must not perform blocking I/O or long-running computation —
        prefer an ``async`` handler whenever possible.
        """
        # Wrap the handler to convert raw messages to KeyNotification objects
        wrapped_handler: Callable | None = None
        if handler is not None:
            key_prefix = self.key_prefix
            is_async_handler = inspect.iscoroutinefunction(handler)

            if is_async_handler:
                # We've verified handler is async, so the result is awaitable
                async_handler = handler

                async def _async_wrap_handler(message):
                    notification = KeyNotification.from_message(
                        message, key_prefix=key_prefix
                    )
                    if notification is not None:
                        await async_handler(notification)

                wrapped_handler = _async_wrap_handler
            else:

                def _sync_wrap_handler(message):
                    notification = KeyNotification.from_message(
                        message, key_prefix=key_prefix
                    )
                    if notification is not None:
                        handler(notification)

                wrapped_handler = _sync_wrap_handler

        patterns = {}
        exact_channels = {}

        for channel in channels:
            if hasattr(channel, "_channel_str"):
                channel_str = str(channel)
            else:
                channel_str = safe_str(channel)
            if _is_pattern(channel):
                patterns[channel_str] = wrapped_handler
            else:
                exact_channels[channel_str] = wrapped_handler

        # Delegate to subclass implementation first.  For standalone Redis
        # this raises on failure, keeping tracking state clean.  For cluster
        # implementations the operation is best-effort (partial failures are
        # logged, not raised) so tracking state is always updated afterwards.
        await self._execute_subscribe(patterns, exact_channels)
        self._track_subscribe(patterns, exact_channels)

    @abstractmethod
    async def _execute_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Execute the subscribe operation."""
        pass

    async def unsubscribe(self, *channels: ChannelT):
        """Unsubscribe from keyspace notification channels."""
        patterns = []
        exact_channels = []

        for channel in channels:
            if hasattr(channel, "_channel_str"):
                channel_str = str(channel)
            else:
                channel_str = safe_str(channel)
            if _is_pattern(channel):
                patterns.append(channel_str)
            else:
                exact_channels.append(channel_str)

        # Delegate to subclass implementation first.  For standalone Redis
        # this raises on failure, keeping tracking state intact.  For cluster
        # implementations the operation is best-effort (partial failures are
        # logged, not raised) so tracking state is always removed afterwards
        # — this is intentional: the user asked to unsubscribe, so
        # refresh_subscriptions should not re-subscribe these channels.
        await self._execute_unsubscribe(patterns, exact_channels)
        self._untrack_subscribe(patterns, exact_channels)

    @abstractmethod
    async def _execute_unsubscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Execute the unsubscribe operation."""
        pass

    def _track_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Track newly subscribed patterns/channels.

        Override in subclasses that need to maintain their own subscription
        registry (e.g. cluster implementations that must re-subscribe
        new/failed-over nodes).  The default is a no-op because standalone
        implementations delegate tracking to the underlying PubSub object.
        """

    def _untrack_subscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Remove patterns/channels from the subscription registry.

        Override in subclasses that maintain their own subscription registry.
        The default is a no-op.
        """

    async def subscribe_keyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to keyspace notifications for specific keys."""
        channel = KeyspaceChannel(key_or_pattern, db=db)
        await self.subscribe(channel, handler=handler)

    async def subscribe_keyevent(
        self,
        event: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to keyevent notifications for specific event types."""
        channel = KeyeventChannel(event, db=db)
        await self.subscribe(channel, handler=handler)

    async def subscribe_subkeyspace(
        self,
        key_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspace notifications for specific keys."""
        channel = SubkeyspaceChannel(key_or_pattern, db=db)
        await self.subscribe(channel, handler=handler)

    async def subscribe_subkeyevent(
        self,
        event: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyevent notifications for specific event types."""
        channel = SubkeyeventChannel(event, db=db)
        await self.subscribe(channel, handler=handler)

    async def subscribe_subkeyspaceitem(
        self,
        key_or_pattern: str,
        subkey_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspaceitem notifications for a specific subkey."""
        channel = SubkeyspaceitemChannel(key_or_pattern, subkey_or_pattern, db=db)
        await self.subscribe(channel, handler=handler)

    async def subscribe_subkeyspaceevent(
        self,
        event: str,
        key_or_pattern: str,
        db: int = 0,
        handler: AsyncHandlerT | None = None,
    ):
        """Subscribe to subkeyspaceevent notifications for an event on a key."""
        channel = SubkeyspaceeventChannel(event, key_or_pattern, db=db)
        await self.subscribe(channel, handler=handler)

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        await self.aclose()
        return False

    async def run(
        self,
        poll_timeout: float = 1.0,
        exception_handler: Callable[
            [BaseException, AsyncKeyspaceNotificationsInterface],
            None | Awaitable[None],
        ]
        | None = None,
    ) -> None:
        """
        Run the notification loop as a coroutine.

        This continuously polls for notifications and triggers handlers.
        Use asyncio.create_task() to run in the background.

        Args:
            poll_timeout: Timeout in seconds for each get_message call.
            exception_handler: Optional callback for handling exceptions.
                              Can be sync or async.
        """
        while self.subscribed:
            try:
                await self.get_message(timeout=poll_timeout)
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                if exception_handler is not None:
                    result = exception_handler(e, self)
                    if inspect.isawaitable(result):
                        await result
                else:
                    raise


# =============================================================================
# Standalone Async Keyspace Notification Manager
# =============================================================================


class AsyncKeyspaceNotifications(AbstractAsyncKeyspaceNotifications):
    """
    Manages keyspace notification subscriptions for standalone async Redis.

    For standalone Redis, keyspace notifications work with a single PubSub
    connection. This class wraps that connection and provides:
    - Automatic pattern vs exact channel detection
    - KeyNotification parsing with optional key_prefix filtering
    - Convenience methods for keyspace and keyevent subscriptions
    - Context manager and run() coroutine support
    """

    def __init__(
        self,
        redis_client: Redis,
        key_prefix: str | bytes | None = None,
        ignore_subscribe_messages: bool = True,
    ):
        """
        Initialize the standalone async keyspace notification manager.

        Note: Keyspace notifications must be enabled on the Redis server via
        the ``notify-keyspace-events`` configuration option.

        Args:
            redis_client: An async Redis client instance
            key_prefix: Optional prefix to filter and strip from keys in notifications
            ignore_subscribe_messages: If True, subscribe/unsubscribe confirmations
                                      are not returned by get_message/listen
        """
        super().__init__(key_prefix, ignore_subscribe_messages)
        self.redis = redis_client
        # Create PubSub with ignore_subscribe_messages=False so per-call arg works
        self._pubsub: PubSub = redis_client.pubsub(ignore_subscribe_messages=False)

    async def _execute_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Execute subscribe on the single pubsub connection."""
        if patterns:
            await self._pubsub.psubscribe(**patterns)
        if exact_channels:
            await self._pubsub.subscribe(**exact_channels)

    async def _execute_unsubscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Execute unsubscribe on the single pubsub connection."""
        if patterns:
            await self._pubsub.punsubscribe(*patterns)
        if exact_channels:
            await self._pubsub.unsubscribe(*exact_channels)

    async def get_message(
        self,
        ignore_subscribe_messages: bool | None = None,
        timeout: float = 0.0,
    ) -> KeyNotification | None:
        """
        Get the next keyspace notification if one is available.

        Args:
            ignore_subscribe_messages: If True, skip subscribe/unsubscribe messages.
                                      Defaults to the value set in __init__ (True).
            timeout: Time to wait for a message.

        Returns:
            A KeyNotification if a notification is available and no handler
            was registered for the channel, None otherwise.
        """
        if ignore_subscribe_messages is None:
            ignore_subscribe_messages = self.ignore_subscribe_messages

        if self._closed:
            return None

        message = await self._pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages,
            timeout=timeout,
        )

        if message is not None:
            return KeyNotification.from_message(message, key_prefix=self.key_prefix)

        return None

    async def listen(self) -> AsyncIterator[KeyNotification]:
        """
        Listen for keyspace notifications.

        This is an async generator that yields KeyNotification objects as they arrive.

        Yields:
            KeyNotification objects for each keyspace/keyevent notification.

        Example:
            >>> async for notification in ksn.listen():
            ...     print(f"{notification.key}: {notification.event_type}")
        """
        while self.subscribed:
            notification = await self.get_message(timeout=1.0)
            if notification is not None:
                yield notification

    @property
    def subscribed(self) -> bool:
        """Check if there are any active subscriptions and not closed."""
        return not self._closed and self._pubsub.subscribed

    async def aclose(self):
        """Close the pubsub connection and clean up resources."""
        self._closed = True
        try:
            await self._pubsub.aclose()
        except Exception:
            pass


# =============================================================================
# Cluster-Aware Async Keyspace Notification Manager
# =============================================================================


class AsyncClusterKeyspaceNotifications(AbstractAsyncKeyspaceNotifications):
    """
    Manages keyspace notification subscriptions across all nodes in an async Redis Cluster.

    In Redis Cluster, keyspace notifications are NOT broadcast between nodes.
    Each node only emits notifications for keys it owns. This class automatically
    subscribes to all primary nodes in the cluster and handles topology changes.
    """

    def __init__(
        self,
        redis_cluster: RedisCluster,
        key_prefix: str | bytes | None = None,
        ignore_subscribe_messages: bool = True,
    ):
        """
        Initialize the async cluster keyspace notification manager.

        Note: Keyspace notifications must be enabled on all Redis cluster nodes via
        the ``notify-keyspace-events`` configuration option.

        Args:
            redis_cluster: An async RedisCluster instance
            key_prefix: Optional prefix to filter and strip from keys in notifications
            ignore_subscribe_messages: If True, subscribe/unsubscribe confirmations
                                      are not returned by get_message/listen
        """
        super().__init__(key_prefix, ignore_subscribe_messages)
        self.cluster = redis_cluster

        # Canonical subscription registry: pattern/channel -> wrapped handler.
        # In cluster mode there are multiple PubSub objects (one per node), so
        # this is the single source of truth used to (re-)subscribe new or
        # failed-over nodes.
        self._subscribed_patterns: dict[str, Any] = {}
        self._subscribed_channels: dict[str, Any] = {}

        # Track subscriptions per node
        self._node_pubsubs: dict[str, PubSub] = {}

        # Lock for topology refresh operations
        self._refresh_lock = asyncio.Lock()

        # Current pubsub index for round-robin polling
        self._poll_index = 0

    @property
    def subscribed(self) -> bool:
        """Check if there are any active subscriptions and not closed."""
        return not self._closed and bool(
            self._subscribed_patterns or self._subscribed_channels
        )

    def _track_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Track newly subscribed patterns/channels in the cluster registry."""
        if patterns:
            self._subscribed_patterns.update(patterns)
        if exact_channels:
            self._subscribed_channels.update(exact_channels)

    def _untrack_subscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Remove patterns/channels from the cluster registry."""
        for p in patterns:
            self._subscribed_patterns.pop(p, None)
        for c in exact_channels:
            self._subscribed_channels.pop(c, None)

    def _get_all_primary_nodes(self) -> list[ClusterNode]:
        """Get all primary nodes in the cluster."""
        return self.cluster.get_primaries()

    async def _ensure_node_pubsub(self, node: ClusterNode) -> PubSub:
        """Get or create a PubSub instance for a node.

        Uses a :class:`_ClusterNodePoolAdapter` to borrow a connection
        from the node's existing pool.  When the ``PubSub`` is closed
        the connection is disconnected and returned to the node,
        ensuring no subscribed socket is left in the free queue.
        """
        if node.name not in self._node_pubsubs:
            pool_adapter = _ClusterNodePoolAdapter(node)
            pubsub = PubSub(
                connection_pool=pool_adapter,  # type: ignore[arg-type]
                ignore_subscribe_messages=False,
            )
            self._node_pubsubs[node.name] = pubsub
        return self._node_pubsubs[node.name]

    async def _cleanup_node(self, node_name: str) -> None:
        """Remove and close a node's PubSub.

        ``PubSub.aclose()`` disconnects the connection and releases it
        back to the underlying :class:`ClusterNode` via the adapter.
        """
        pubsub = self._node_pubsubs.pop(node_name, None)
        if pubsub:
            try:
                await pubsub.aclose()
            except Exception:
                pass

    async def _execute_subscribe(
        self, patterns: dict[str, Any], exact_channels: dict[str, Any]
    ) -> None:
        """Execute subscribe on all cluster nodes.

        Patterns and exact channels are subscribed in a single pass over
        nodes so that a mid-batch node failure cannot create a
        partially-caught-up replacement.  If a node fails during this
        call it is removed from ``_node_pubsubs`` and will be fully
        re-subscribed on the next ``refresh_subscriptions`` cycle.

        If a newly discovered node is encountered (not yet in
        ``_node_pubsubs``), it is also subscribed to all *previously*
        tracked patterns/channels so it doesn't miss notifications for
        subscriptions that were established before this node joined.
        """
        if not patterns and not exact_channels:
            return

        failed_nodes: list[str] = []
        for node in self._get_all_primary_nodes():
            is_new_node = node.name not in self._node_pubsubs
            pubsub = await self._ensure_node_pubsub(node)
            try:
                # If this is a brand-new node, catch it up on existing
                # subscriptions before adding the new channels.
                if is_new_node:
                    if self._subscribed_patterns:
                        await pubsub.psubscribe(**self._subscribed_patterns)
                    if self._subscribed_channels:
                        await pubsub.subscribe(**self._subscribed_channels)

                if patterns:
                    await pubsub.psubscribe(**patterns)
                if exact_channels:
                    await pubsub.subscribe(**exact_channels)
            except Exception:
                # Remove the broken pubsub and its connection pool
                # so refresh_subscriptions can re-create both later.
                await self._cleanup_node(node.name)
                failed_nodes.append(node.name)

        if failed_nodes:
            logger.warning(
                "Failed to subscribe on cluster nodes: %s. "
                "These nodes will be retried on the next refresh cycle.",
                ", ".join(failed_nodes),
            )

    async def _execute_unsubscribe(
        self, patterns: list[str], exact_channels: list[str]
    ) -> None:
        """Execute unsubscribe on all cluster nodes."""
        if patterns:
            await self._unsubscribe_from_all_nodes(patterns, use_punsubscribe=True)
        if exact_channels:
            await self._unsubscribe_from_all_nodes(
                exact_channels, use_punsubscribe=False
            )

    async def _unsubscribe_from_all_nodes(
        self, channels: list[str], use_punsubscribe: bool
    ):
        """Unsubscribe from patterns/channels on all nodes.

        Best-effort: tries every node so that a single broken connection
        does not prevent the remaining nodes from being unsubscribed.
        Broken pubsubs are cleaned up; the tracking state is still removed
        by the caller, so ``refresh_subscriptions`` will *not* re-subscribe
        these channels on replacement nodes.
        """
        failed_nodes: list[str] = []
        for node_name in list(self._node_pubsubs.keys()):
            pubsub = self._node_pubsubs.get(node_name)
            if pubsub is None:
                continue
            try:
                if use_punsubscribe:
                    await pubsub.punsubscribe(*channels)
                else:
                    await pubsub.unsubscribe(*channels)
            except Exception:
                await self._cleanup_node(node_name)
                failed_nodes.append(node_name)

        if failed_nodes:
            logger.warning(
                "Failed to unsubscribe on cluster nodes: %s. "
                "These nodes will be re-created on the next refresh cycle.",
                ", ".join(failed_nodes),
            )

    async def get_message(
        self,
        ignore_subscribe_messages: bool | None = None,
        timeout: float = 0.0,
    ) -> KeyNotification | None:
        """
        Get the next keyspace notification if one is available.

        This method polls all node pubsubs in round-robin fashion until
        a message is received or the timeout expires.
        If a connection error occurs, subscriptions are automatically refreshed.

        Args:
            ignore_subscribe_messages: If True, skip subscribe/unsubscribe messages.
                                      Defaults to the value set in __init__ (True).
            timeout: Total time to wait for a message (distributed across all nodes)

        Returns:
            A KeyNotification if a notification is available, None otherwise.
        """
        if self._closed:
            return None

        total_nodes = len(self._node_pubsubs)
        if total_nodes == 0:
            # Sleep for the requested timeout so callers that loop
            # (run(), listen) don't spin the CPU when all node
            # connections have been cleaned up.
            if timeout > 0:
                await asyncio.sleep(timeout)
            return None

        if ignore_subscribe_messages is None:
            ignore_subscribe_messages = self.ignore_subscribe_messages

        # Handle timeout=0 as a single non-blocking poll over all pubsubs
        if timeout == 0.0:
            return await self._poll_all_nodes_once(ignore_subscribe_messages)

        # Calculate per-node timeout for each poll
        per_node_timeout = min(0.1, timeout / max(total_nodes, 1))

        start_time = time.monotonic()
        end_time = start_time + timeout

        while True:
            if time.monotonic() >= end_time:
                return None

            pubsubs = list(self._node_pubsubs.values())
            if not pubsubs:
                return None

            # Round-robin polling
            self._poll_index = self._poll_index % len(pubsubs)
            pubsub = pubsubs[self._poll_index]
            self._poll_index += 1

            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=ignore_subscribe_messages,
                    timeout=per_node_timeout,
                )
            except (ConnectionError, TimeoutError, RedisError):
                await self._refresh_subscriptions_on_error()
                continue

            if message is not None:
                notification = KeyNotification.from_message(
                    message, key_prefix=self.key_prefix
                )
                if notification is not None:
                    return notification

    async def _poll_all_nodes_once(
        self, ignore_subscribe_messages: bool
    ) -> KeyNotification | None:
        """
        Perform a single non-blocking poll over all node pubsubs.

        Returns:
            A KeyNotification if one is available, None otherwise.
        """
        had_error = False
        for pubsub in list(self._node_pubsubs.values()):
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=ignore_subscribe_messages,
                    timeout=0.0,
                )
            except (ConnectionError, TimeoutError, RedisError):
                # Record the error but continue polling remaining healthy
                # nodes so that already-buffered notifications are not lost.
                had_error = True
                continue

            if message is not None:
                notification = KeyNotification.from_message(
                    message, key_prefix=self.key_prefix
                )
                if notification is not None:
                    # Refresh before returning if any node had an error,
                    # so the next poll cycle has fresh state.
                    if had_error:
                        await self._refresh_subscriptions_on_error()
                    return notification

        # Refresh after polling all nodes if any had errors
        if had_error:
            await self._refresh_subscriptions_on_error()
        return None

    async def listen(self) -> AsyncIterator[KeyNotification]:
        """
        Listen for keyspace notifications from all cluster nodes.

        This is an async generator that yields KeyNotification objects as they arrive.

        Yields:
            KeyNotification objects for each keyspace/keyevent notification.

        Example:
            >>> async for notification in ksn.listen():
            ...     print(f"{notification.key}: {notification.event_type}")
        """
        while self.subscribed:
            notification = await self.get_message(timeout=1.0)
            if notification is not None:
                yield notification

    async def _refresh_subscriptions_on_error(self):
        """
        Refresh subscriptions after a connection error.

        This is called automatically when a connection error occurs during
        get_message(). It checks if nodes changed before refreshing.
        """
        self._poll_index = 0  # Reset round-robin index

        try:
            await self.refresh_subscriptions()
        except Exception:
            logger.warning(
                "Failed to refresh cluster subscriptions, will retry on next error",
                exc_info=True,
            )

    def _is_pubsub_connected(self, pubsub) -> bool:
        """Check if a pubsub connection is still alive."""
        try:
            conn = pubsub.connection
            if conn is None:
                return False
            # Async connections use is_connected property
            return conn.is_connected
        except Exception:
            return False

    async def refresh_subscriptions(self):
        """
        Refresh subscriptions after a topology change.

        This method is called automatically when topology changes are detected
        or when connection errors occur. You can also call it manually if needed.

        This method:
        1. Discovers any new primary nodes and subscribes them
        2. Removes pubsubs for nodes that are no longer primaries
        3. Re-creates broken pubsub connections for existing nodes
        """
        async with self._refresh_lock:
            current_primaries = {
                node.name: node for node in self._get_all_primary_nodes()
            }

            # Remove pubsubs for nodes that are no longer primaries
            removed_nodes = set(self._node_pubsubs.keys()) - set(
                current_primaries.keys()
            )
            for node_name in removed_nodes:
                await self._cleanup_node(node_name)

            # Detect broken connections for existing nodes and remove them
            # so they get re-created below
            existing_nodes = set(self._node_pubsubs.keys()) & set(
                current_primaries.keys()
            )
            for node_name in existing_nodes:
                pubsub = self._node_pubsubs.get(node_name)
                if pubsub and not self._is_pubsub_connected(pubsub):
                    # Connection is broken, remove it so it gets re-created
                    await self._cleanup_node(node_name)

            # Subscribe new nodes (and nodes with broken connections)
            # to existing patterns/channels
            new_nodes = set(current_primaries.keys()) - set(self._node_pubsubs.keys())
            failed_nodes: list[str] = []
            for node_name in new_nodes:
                node = current_primaries[node_name]
                pubsub = await self._ensure_node_pubsub(node)

                try:
                    if self._subscribed_patterns:
                        await pubsub.psubscribe(**self._subscribed_patterns)
                    if self._subscribed_channels:
                        await pubsub.subscribe(**self._subscribed_channels)
                except Exception:
                    # Subscription failed - remove from dict so retry is possible
                    await self._cleanup_node(node_name)
                    failed_nodes.append(node_name)

            # Raise after attempting all nodes so we don't skip any
            if failed_nodes:
                raise ConnectionError(
                    f"Failed to subscribe to cluster nodes: {', '.join(failed_nodes)}"
                )

    async def aclose(self):
        """Close all pubsub connections and clean up resources."""
        self._closed = True
        for node_name in list(self._node_pubsubs.keys()):
            await self._cleanup_node(node_name)
        self._subscribed_patterns.clear()
        self._subscribed_channels.clear()
