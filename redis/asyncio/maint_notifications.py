import asyncio
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from redis.asyncio.observability.recorder import (
    record_connection_handoff,
    record_connection_relaxed_timeout,
    record_maint_notification_count,
)
from redis.maint_notifications import (
    MaintenanceNotification,
    MaintenanceState,
    MaintNotificationsConfig,
    NodeMovingNotification,
    OSSNodeMigratedNotification,
    _get_maintenance_notification_name,
    _get_maintenance_notification_type,
    _should_skip_connection_timeout_update,
)
from redis.observability.attributes import get_pool_name

if TYPE_CHECKING:
    from redis.asyncio.cluster import RedisCluster
    from redis.asyncio.connection import AsyncMaintNotificationsAbstractConnection

logger = logging.getLogger(__name__)

_ScheduledCallback = Callable[..., Awaitable[None]]


def add_debug_log_for_notification(
    connection: object,
    notification: str | MaintenanceNotification,
) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return

    socket_address = None
    try:
        writer = getattr(connection, "_writer", None)
        socket_name = writer.get_extra_info("sockname") if writer else None
        socket_address = (
            socket_name[1] if socket_name and len(socket_name) > 1 else None
        )
    except (AttributeError, OSError, TypeError):
        pass

    resolved_ip = None
    try:
        get_resolved_ip = getattr(connection, "get_resolved_ip", None)
        if callable(get_resolved_ip):
            resolved_ip = get_resolved_ip()
    except (AttributeError, OSError):
        pass

    logger.debug(
        f"Handling maintenance notification: {notification}, "
        f"with connection: {connection}, connected to ip {resolved_ip}, "
        f"local socket port: {socket_address}",
    )


class AsyncMaintNotificationsPoolHandler:
    def __init__(
        self,
        pool: Any,
        config: MaintNotificationsConfig,
    ) -> None:
        self.pool = pool
        self.config = config
        self._processed_notifications: set[MaintenanceNotification] = set()
        self._scheduled_tasks: set[asyncio.Task[None]] = set()
        self._lock = asyncio.Lock()
        self.connection: Any | None = None

    def set_connection(
        self, connection: "AsyncMaintNotificationsAbstractConnection"
    ) -> None:
        self.connection = connection

    def get_handler_for_connection(self) -> "AsyncMaintNotificationsPoolHandler":
        # Copy all data that should be shared between connections, while each
        # connection gets its own handler instance and current connection state.
        copy = AsyncMaintNotificationsPoolHandler(self.pool, self.config)
        copy._processed_notifications = self._processed_notifications
        copy._scheduled_tasks = self._scheduled_tasks
        copy._lock = self._lock
        copy.connection = None
        return copy

    async def remove_expired_notifications(self) -> None:
        async with self._lock:
            for notification in tuple(self._processed_notifications):
                if notification.is_expired():
                    self._processed_notifications.remove(notification)

    async def handle_notification(self, notification: MaintenanceNotification) -> None:
        await self.remove_expired_notifications()

        if isinstance(notification, NodeMovingNotification):
            await self.handle_node_moving_notification(notification)
        else:
            logger.error(f"Unhandled notification type: {notification}")

    async def handle_node_moving_notification(
        self, notification: NodeMovingNotification
    ) -> None:
        if (
            not self.config.proactive_reconnect
            and not self.config.is_relaxed_timeouts_enabled()
        ):
            return

        async with self._lock:
            if notification in self._processed_notifications:
                # nothing to do in the connection pool handling
                # the notification has already been handled or is expired
                # just return
                return
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Handling node MOVING notification: {notification}, "
                    f"with connection: {self.connection}, connected to ip "
                    f"{self.connection.get_resolved_ip() if self.connection else None}"
                )
            # Get the current connected address - if any
            # This is the address that is being moved
            # and we need to handle only connections
            # connected to the same address
            moving_address_src = (
                self.connection.getpeername() if self.connection else None
            )

            # The async pool owns the active/free connection collections and
            # asyncio.Lock is not reentrant, so the whole MOVING pool mutation
            # has to be one pool-owned atomic operation. The handler still owns
            # the notification policy and passes the already-decided inputs.
            await self.pool.apply_moving_notification(
                notification=notification,
                config=self.config,
                moving_address_src=moving_address_src,
                run_proactive_reconnect=(
                    self.config.proactive_reconnect
                    and notification.new_node_host is not None
                ),
            )

            if self.config.proactive_reconnect and notification.new_node_host is None:
                self._schedule(
                    notification.ttl / 2,
                    self.run_proactive_reconnect,
                    moving_address_src,
                )

            self._schedule(
                notification.ttl,
                self.handle_node_moved_notification,
                notification,
            )

            await record_connection_handoff(
                pool_name=get_pool_name(self.pool),
            )

            self._processed_notifications.add(notification)

    async def run_proactive_reconnect(
        self, moving_address_src: str | None = None
    ) -> None:
        """
        Run proactive reconnect for the pool.
        Active connections are marked for reconnect after they complete the current command.
        Inactive connections are disconnected and will be connected on next use.
        """
        async with self._lock:
            # This delayed reconnect must be atomic for the same reason as the
            # initial MOVING mutation: a connection can move between active and
            # free lists while another task is acquiring or releasing it.
            await self.pool.run_proactive_reconnect(
                moving_address_src=moving_address_src,
            )

    async def handle_node_moved_notification(
        self, notification: NodeMovingNotification
    ) -> None:
        """
        Handle the cleanup after a node moving notification expires.
        """
        notification_hash = hash(notification)

        async with self._lock:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"Reverting temporary changes related to notification: {notification}, "
                    f"with connection: {self.connection}, connected to ip "
                    f"{self.connection.get_resolved_ip() if self.connection else None}"
                )
            reset_relaxed_timeout = self.config.is_relaxed_timeouts_enabled()
            reset_host_address = self.config.proactive_reconnect

            # Cleanup has to reset future connection kwargs and existing
            # matching connections together under the pool lock. Splitting it
            # lets an acquire/release interleave and leaves stale MOVING state.
            await self.pool.cleanup_moving_notification(
                notification_hash=notification_hash,
                reset_relaxed_timeout=reset_relaxed_timeout,
                reset_host_address=reset_host_address,
            )

    def _schedule(
        self,
        delay: float,
        callback: _ScheduledCallback,
        *args: Any,
    ) -> None:
        # Record the absolute deadline now so that any lag between create_task
        # and the task's first execution slice does not push the fire time out.
        deadline = asyncio.get_running_loop().time() + delay
        task = asyncio.create_task(self._run_after(deadline, callback, *args))
        self._scheduled_tasks.add(task)
        task.add_done_callback(self._scheduled_tasks.discard)
        task.add_done_callback(self._log_scheduled_task_result)

    async def _run_after(
        self,
        deadline: float,
        callback: _ScheduledCallback,
        *args: Any,
    ) -> None:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining > 0:
            await asyncio.sleep(remaining)
        await callback(*args)

    async def cancel_scheduled_tasks(self) -> None:
        if not self._scheduled_tasks:
            return
        tasks = tuple(self._scheduled_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _log_scheduled_task_result(task: asyncio.Task[None]) -> None:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc:
            logger.error(
                "Error handling scheduled maintenance notification", exc_info=exc
            )


class AsyncMaintNotificationsConnectionHandler:
    def __init__(
        self,
        connection: "AsyncMaintNotificationsAbstractConnection",
        config: MaintNotificationsConfig,
    ) -> None:
        self.connection = connection
        self.config = config

    def _get_pool_name(self) -> str:
        """
        Get the pool name from the connection's pool handler.
        Falls back to connection representation if pool is not available.
        """
        pool_handler = getattr(
            self.connection, "_maint_notifications_pool_handler", None
        )
        if pool_handler and getattr(pool_handler, "pool", None):
            return get_pool_name(pool_handler.pool)
        # Fallback for standalone connections without a pool
        return repr(self.connection)

    async def handle_notification(self, notification: MaintenanceNotification) -> None:
        # 1 for start, 0 for end notification type, None for unknown.
        notification_type = _get_maintenance_notification_type(notification)
        maint_notification = _get_maintenance_notification_name(notification)

        await record_maint_notification_count(
            server_address=self.connection.host,
            server_port=self.connection.port,
            network_peer_address=self.connection.host,
            network_peer_port=self.connection.port,
            maint_notification=maint_notification,
        )

        if notification_type is None:
            logger.error(f"Unhandled notification type: {notification}")
            return

        if notification_type:
            await self.handle_maintenance_start_notification(
                MaintenanceState.MAINTENANCE, notification
            )
        else:
            await self.handle_maintenance_completed_notification(
                notification=notification
            )

    async def handle_maintenance_start_notification(
        self,
        maintenance_state: MaintenanceState,
        notification: MaintenanceNotification,
    ) -> None:
        add_debug_log_for_notification(self.connection, notification)

        if _should_skip_connection_timeout_update(
            self.connection.maintenance_state, self.config
        ):
            return

        self.connection.maintenance_state = maintenance_state
        self.connection.set_tmp_settings(
            tmp_relaxed_timeout=self.config.relaxed_timeout
        )
        self.connection.update_current_socket_timeout(self.config.relaxed_timeout)

        maint_notification = _get_maintenance_notification_name(notification)
        await record_connection_relaxed_timeout(
            connection_name=self._get_pool_name(),
            maint_notification=maint_notification,
            relaxed=True,
        )

    async def handle_maintenance_completed_notification(self, **kwargs: Any) -> None:
        # Only reset timeouts if state is not MOVING and relaxed timeouts are enabled
        if _should_skip_connection_timeout_update(
            self.connection.maintenance_state, self.config
        ):
            return

        notification = None
        if kwargs.get("notification"):
            notification = kwargs["notification"]
        add_debug_log_for_notification(
            self.connection, notification if notification else "MAINTENANCE_COMPLETED"
        )
        self.connection.reset_tmp_settings(reset_relaxed_timeout=True)
        # Maintenance completed - reset the connection
        # timeouts by providing -1 as the relaxed timeout
        self.connection.update_current_socket_timeout(-1)
        self.connection.maintenance_state = MaintenanceState.NONE
        # reset the sets that keep track of received start maint
        # notifications and skipped end maint notifications
        self.connection.reset_received_notifications()

        if notification:
            maint_notification = _get_maintenance_notification_name(notification)
            await record_connection_relaxed_timeout(
                connection_name=self._get_pool_name(),
                maint_notification=maint_notification,
                relaxed=False,
            )


class AsyncOSSMaintNotificationsHandler:
    """
    Cluster-wide handler for OSS (open-source) cluster maintenance notifications.

    Reacts to SMIGRATED (slot migration completed) push notifications and
    triggers topology re-initialization via nodes_manager.initialize().

    Lock discipline: acquires _lock to check/set _in_progress, then releases it
    before await initialize() to prevent deadlock — initialize() may dispatch
    commands whose responses contain new push notifications that re-enter
    handle_notification, and asyncio.Lock is non-reentrant.
    """

    def __init__(
        self,
        cluster_client: "RedisCluster",
        config: MaintNotificationsConfig,
    ) -> None:
        self.cluster_client = cluster_client
        self.config = config
        self._processed_notifications: set[MaintenanceNotification] = set()
        self._in_progress: set[MaintenanceNotification] = set()
        self._lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task] = set()

    def get_handler_for_connection(self) -> "AsyncOSSMaintNotificationsHandler":
        # Copy all data that should be shared between connections
        # but each connection should have its own handler
        # since each connection can be in a different state
        copy = AsyncOSSMaintNotificationsHandler(self.cluster_client, self.config)
        copy._processed_notifications = self._processed_notifications
        copy._in_progress = self._in_progress
        copy._lock = self._lock
        copy._background_tasks = self._background_tasks
        return copy

    async def remove_expired_notifications(self) -> None:
        async with self._lock:
            for n in tuple(self._processed_notifications):
                if n.is_expired():
                    self._processed_notifications.remove(n)

    async def handle_notification(self, notification: MaintenanceNotification) -> None:
        # Schedule as a background task so the parser's read path is not blocked.
        # This also breaks the inline call chain that would cause deadlock: any push
        # notification arriving while the task holds _lock during initialize() becomes
        # a new task that simply waits for the lock instead of deadlocking.
        task = asyncio.get_running_loop().create_task(
            self._do_handle_notification(notification)
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _do_handle_notification(
        self, notification: MaintenanceNotification
    ) -> None:
        if isinstance(notification, OSSNodeMigratedNotification):
            await self.handle_oss_maintenance_completed_notification(notification)
        else:
            logger.error(f"Unhandled notification type: {notification}")

    async def handle_oss_maintenance_completed_notification(
        self, notification: OSSNodeMigratedNotification
    ) -> None:
        await self.remove_expired_notifications()

        async with self._lock:
            if (
                notification in self._in_progress
                or notification in self._processed_notifications
            ):
                # we are already handling this notification or it has already been processed
                # we should skip in_progress notification since when we reinitialize the cluster
                # we execute a CLUSTER SLOTS command that can use a different connection
                # that has also has the notification and we don't want to
                # process the same notification twice
                return
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Handling SMIGRATED notification: {notification}")
            self._in_progress.add(notification)

            try:
                # Extract the information about the src and destination nodes that are
                # affected by the maintenance. nodes_to_slots_mapping structure:
                # {
                #     "src_host:port": [
                #         {"dest_host:port": "slot_range"},
                #         ...
                #     ],
                #     ...
                # }
                additional_startup_nodes_info = []
                affected_nodes = set()
                for (
                    src_address,
                    dest_mappings,
                ) in notification.nodes_to_slots_mapping.items():
                    src_host, src_port = src_address.split(":")
                    src_node = self.cluster_client.nodes_manager.get_node(
                        host=src_host, port=int(src_port)
                    )
                    if src_node is not None:
                        affected_nodes.add(src_node)
                    for dest_mapping in dest_mappings:
                        for dest_address in dest_mapping.keys():
                            dest_host, dest_port = dest_address.split(":")
                            additional_startup_nodes_info.append(
                                (dest_host, int(dest_port))
                            )
                # Updates the cluster slots cache with the new slots mapping
                # This will also update the nodes cache with the new nodes mapping
                await self.cluster_client.nodes_manager.initialize(
                    additional_startup_nodes_info=additional_startup_nodes_info,
                )

                all_nodes = set(affected_nodes)
                all_nodes = all_nodes.union(
                    self.cluster_client.nodes_manager.nodes_cache.values()
                )
                for current_node in all_nodes:
                    handoff_recorded = False
                    if current_node in affected_nodes:
                        # mark for reconnect all in-use connections to the node — this
                        # forces them to disconnect after completing their current command
                        free_set = set(current_node._free)
                        for conn in current_node._connections:
                            if conn not in free_set:
                                add_debug_log_for_notification(
                                    conn, "SMIGRATED - mark for reconnect"
                                )
                                conn.mark_for_reconnect()
                        await record_connection_handoff(
                            pool_name=f"{current_node.host}:{current_node.port}"
                        )
                        handoff_recorded = True
                    else:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"SMIGRATED: Node {current_node.name} not affected "
                                f"by maintenance, skipping mark for reconnect"
                            )
                    if (
                        current_node
                        not in self.cluster_client.nodes_manager.nodes_cache.values()
                    ):
                        task = asyncio.get_running_loop().create_task(
                            current_node.disconnect_free_connections()
                        )
                        self._background_tasks.add(task)
                        task.add_done_callback(self._background_tasks.discard)
                        if not handoff_recorded:
                            await record_connection_handoff(
                                pool_name=f"{current_node.host}:{current_node.port}"
                            )

                self._processed_notifications.add(notification)

            finally:
                self._in_progress.discard(notification)
