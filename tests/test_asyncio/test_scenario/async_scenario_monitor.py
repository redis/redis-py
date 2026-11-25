"""The async counterparts of the client monitor and the client-side fault hooks in
tests/test_scenario/scenario_monitor.py: the sampler is a task on the event loop,
and the instrumentation wraps the asyncio connection and nodes manager classes.
"""

import asyncio
from typing import Callable

from redis.asyncio import RedisCluster
from redis.asyncio.cluster import NodesManager
from redis.asyncio.connection import AbstractConnection, Connection
from redis.exceptions import AuthenticationError
from tests.test_scenario.scenario_monitor import (
    AUTH_REJECTED_MESSAGE,
    MONITOR_INTERVAL_SECONDS,
    AuthOutage,
    ClientMonitor,
    ClientSnapshot,
    _mapped_slots,
    _now,
    _primaries,
    record,
    unattributed_events,
)


class AsyncClientMonitor(ClientMonitor):
    """The async counterpart of ClientMonitor: the sampler is a task."""

    def __init__(self, snapshot: Callable[[], ClientSnapshot]):
        super().__init__(snapshot)
        self._task: asyncio.Task | None = None

    def start(self):
        self.observe()
        self._task = asyncio.create_task(self._sample(), name="client_monitor")

    async def _sample(self):
        while not self._stop.is_set():
            await asyncio.sleep(MONITOR_INTERVAL_SECONDS)
            self.observe()

    async def stop(self):
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None


def instrument(monkeypatch):
    """Install the hooks that feed the monitors; see ClientMonitor."""
    unattributed_events.clear()
    original_connect = Connection._connect
    original_connect_check_health = AbstractConnection.connect_check_health
    original_initialize = NodesManager.initialize
    original_disconnect = AbstractConnection.disconnect

    async def _connect(self):
        address = f"{self.host}:{self.port}"
        record("dial", address)
        try:
            return await original_connect(self)
        except BaseException as e:
            record("dial_failure", address, e)
            raise

    async def connect_check_health(self, *args, **kwargs):
        try:
            return await original_connect_check_health(self, *args, **kwargs)
        except BaseException as e:
            record("connect_failure", f"{self.host}:{self.port}", e)
            raise

    async def disconnect(self, *args, **kwargs):
        if self.is_connected:
            record("drop", f"{self.host}:{self.port}")
        return await original_disconnect(self, *args, **kwargs)

    async def initialize(self, *args, **kwargs):
        record("refresh")
        try:
            return await original_initialize(self, *args, **kwargs)
        except BaseException as e:
            record("refresh_failure", error=e)
            raise

    monkeypatch.setattr(Connection, "_connect", _connect)
    monkeypatch.setattr(
        AbstractConnection, "connect_check_health", connect_check_health
    )
    monkeypatch.setattr(AbstractConnection, "disconnect", disconnect)
    monkeypatch.setattr(NodesManager, "initialize", initialize)


def inject_auth_outage(monkeypatch, outage: AuthOutage):
    original_handshake = AbstractConnection.on_connect_check_health

    async def on_connect_check_health(self, *args, **kwargs):
        if outage.rejects(f"{self.host}:{self.port}"):
            raise AuthenticationError(AUTH_REJECTED_MESSAGE)
        return await original_handshake(self, *args, **kwargs)

    monkeypatch.setattr(
        AbstractConnection, "on_connect_check_health", on_connect_check_health
    )


def _client_connections(client: RedisCluster) -> list:
    """Every connection the client's nodes and startup nodes hold."""
    # A startup node is often the same object as a node of the topology.
    nodes = {
        id(node): node
        for node in [
            *client.nodes_manager.nodes_cache.values(),
            *client.nodes_manager.startup_nodes.values(),
        ]
    }
    return [conn for node in nodes.values() for conn in node._connections]


def _open_sockets(client: RedisCluster) -> int:
    return sum(1 for conn in _client_connections(client) if conn.is_connected)


def _connections_to(client: RedisCluster, address: str) -> int:
    node = client.get_node(node_name=address)
    return 0 if node is None else len(node._connections)


def _snapshot(client: RedisCluster) -> ClientSnapshot:
    nodes = list(client.nodes_manager.nodes_cache.items())
    return ClientSnapshot(
        _now(),
        frozenset(_primaries(client)),
        {name: len(node._connections) for name, node in nodes},
        {
            name: sum(1 for conn in node._connections if conn.is_connected)
            for name, node in nodes
        },
        _open_sockets(client),
        _mapped_slots(client),
    )
