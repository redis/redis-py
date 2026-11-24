"""The timeline, the client monitor and the client-side fault hooks of the cluster
scenario tests.

The timeline records the operations the main thread runs. The monitor records every
new connection, failed connect and topology refresh of the client, and samples its
primaries and connections per node every MONITOR_INTERVAL_SECONDS. The async
scenario tests reuse all of it, with their own sampler and instrumentation in
tests/test_asyncio/test_scenario/async_scenario_monitor.py.
"""

from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
import logging
import threading
import time
from dataclasses import dataclass, field
from threading import Thread
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

try:
    import psutil
except ImportError:
    # Only the cross-check of the process's sockets needs it. It is listed in
    # tests/test_scenario/requirements.txt.
    psutil = None

from redis import RedisCluster
from redis.asyncio import RedisCluster as AsyncRedisCluster
from redis.cluster import NodesManager
from redis.connection import AbstractConnection, Connection
from redis.exceptions import AuthenticationError

if TYPE_CHECKING:
    from tests.test_scenario.scenario_workloads import CommandError


# How often the monitor samples the client's primaries and connections. New
# connections and refreshes are recorded as they happen, not sampled.
MONITOR_INTERVAL_SECONDS = 0.1
# Seconds around each operation during which connection and timeout errors are
# allowed.
TRANSITION_MARGIN = 5
# How long the returning node rejects AUTH in the AUTH outage scenario. The drill's
# node rejected it for about 50 seconds.
AUTH_OUTAGE_SECONDS = 20
AUTH_REJECTED_MESSAGE = "invalid username-password pair"


def _now() -> float:
    return time.monotonic()


# The timeline: the scenario's operations, run by the main thread.


@dataclass
class Operation:
    """One maintenance operation of a scenario.

    started and done bracket the fault injector call. settled is when the client
    had caught up with the operation, if the scenario waited for that.
    """

    name: str
    started: float
    done: Optional[float] = None
    settled: Optional[float] = None

    @property
    def end(self) -> float:
        if self.settled is not None:
            return self.settled
        return self.done if self.done is not None else _now()


@dataclass
class Timeline:
    started: float = field(default_factory=_now)
    warmed_up: float = 0.0
    operations: List[Operation] = field(default_factory=list)
    steady_started: float = 0.0
    steady_done: float = 0.0

    @contextmanager
    def operation(self, name: str) -> Iterator[Operation]:
        operation = Operation(name, _now())
        self.operations.append(operation)
        logging.info(f"Operation {name} started")
        yield operation
        operation.done = _now()
        logging.info(
            f"Operation {name} done in {operation.done - operation.started:.1f}s"
        )

    @property
    def settled(self) -> float:
        """When the client had caught up with the last operation."""
        return self.operations[-1].end

    def transition_windows(self) -> List[tuple]:
        return [
            (op.started - TRANSITION_MARGIN, op.end + TRANSITION_MARGIN)
            for op in self.operations
        ]

    def in_transition(self, at: float) -> bool:
        return any(start <= at <= end for start, end in self.transition_windows())

    def operation_windows(self) -> List[tuple]:
        """(name, start, end) of each operation up to the start of the next one."""
        starts = [op.started for op in self.operations[1:]] + [self.steady_started]
        return [(op.name, op.started, end) for op, end in zip(self.operations, starts)]

    def phases(self) -> List[tuple]:
        """(name, start, end) of every phase, for the summary."""
        phases = [("warm-up", self.started, self.warmed_up)]
        starts = [op.started for op in self.operations[1:]] + [self.steady_started]
        for op, next_start in zip(self.operations, starts):
            phases.append((op.name, op.started, op.end))
            phases.append((f"after {op.name}", op.end, next_start))
        phases.append(("steady", self.steady_started, self.steady_done))
        return phases


# The monitor: what the client does while the app runs.


@dataclass
class _Event:
    at: float
    kind: str
    address: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class ClientSnapshot:
    at: float
    primaries: FrozenSet[str]
    # node name -> connections the node's pool holds, and how many of them are
    # connected
    connections: Dict[str, int]
    sockets: Dict[str, int]
    # connected sockets of every node and startup node pool
    open_sockets: int
    # slots the client's slot map routes to a node
    mapped_slots: int


@dataclass(frozen=True)
class TopologyChange:
    at: float
    added: FrozenSet[str]
    removed: FrozenSet[str]


class ClientMonitor:
    """What the client does while the app runs.

    Two sources feed it. The hooks installed by instrument() record every TCP dial
    (Connection._connect), every failed connect including the handshake
    (connect_check_health), every close of a connected socket (disconnect) and
    every topology refresh (NodesManager.initialize) of the client. A sampler takes
    a ClientSnapshot of the client every MONITOR_INTERVAL_SECONDS and records when
    its primaries change.

    The hooks are installed on the classes, so they see every client of the
    process. They record into the monitor that current_monitor names, which the
    threads or tasks working for this monitor's client set with bound_to().
    """

    def __init__(self, snapshot: Callable[[], ClientSnapshot]):
        self._snapshot = snapshot
        self._lock = threading.Lock()
        self._events: List[_Event] = []
        self.snapshots: List[ClientSnapshot] = []
        self.topology_changes: List[TopologyChange] = []
        self._stop = threading.Event()
        self._sampler: Optional[Thread] = None

    def record(self, kind: str, address: Optional[str] = None, error=None):
        event = _Event(
            _now(), kind, address, None if error is None else type(error).__name__
        )
        with self._lock:
            self._events.append(event)

    def observe(self):
        """Take a snapshot of the client and record a change of its primaries."""
        snapshot = self._snapshot()
        previous = self.snapshots[-1] if self.snapshots else None
        self.snapshots.append(snapshot)
        if previous is None or snapshot.primaries == previous.primaries:
            return
        change = TopologyChange(
            snapshot.at,
            snapshot.primaries - previous.primaries,
            previous.primaries - snapshot.primaries,
        )
        self.topology_changes.append(change)
        logging.info(
            f"Client topology changed: added {sorted(change.added)}, "
            f"removed {sorted(change.removed)}"
        )

    def start(self):
        self.observe()
        self._sampler = Thread(target=self._sample, name="client_monitor", daemon=True)
        self._sampler.start()

    def _sample(self):
        while not self._stop.wait(MONITOR_INTERVAL_SECONDS):
            self.observe()

    def stop(self):
        self._stop.set()
        if self._sampler is not None:
            self._sampler.join()
            self._sampler = None

    def events(
        self,
        kind: str,
        start: float = 0.0,
        end: float = float("inf"),
        addresses: Optional[Set[str]] = None,
    ) -> List[_Event]:
        with self._lock:
            return [
                e
                for e in self._events
                if e.kind == kind
                and start <= e.at <= end
                and (addresses is None or e.address in addresses)
            ]

    def count(self, kind: str, **kwargs) -> int:
        return len(self.events(kind, **kwargs))

    def peak_second(self, kind: str, start: float, end: float) -> Tuple[int, float]:
        """The most events of kind in one second between start and end, and when
        that second starts."""
        buckets: Dict[int, int] = defaultdict(int)
        for event in self.events(kind, start=start, end=end):
            buckets[int(event.at - start)] += 1
        if not buckets:
            return 0, start
        second, count = max(buckets.items(), key=lambda item: item[1])
        return count, start + second

    def by_address(self, kind: str, start: float, end: float) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for event in self.events(kind, start=start, end=end):
            counts[event.address or ""] += 1
        return dict(counts)

    def stable_nodes(self, start: float, end: float) -> Set[str]:
        """The nodes that were primaries in every snapshot between start and end."""
        snapshots = [s for s in self.snapshots if start <= s.at <= end]
        if not snapshots:
            return set()
        return set(frozenset.intersection(*(s.primaries for s in snapshots)))

    def changes_between(self, start: float, end: float) -> List[TopologyChange]:
        return [c for c in self.topology_changes if start <= c.at <= end]


# The monitor of the client the current thread or task works for. Each client of a
# scenario has its own monitor, and the class-level hooks of instrument() record
# into the one named here. A new thread starts with no value; a new task inherits
# the value of the task that created it, so the client's own background tasks are
# counted for it too.
current_monitor: ContextVar[Optional[ClientMonitor]] = ContextVar(
    "current_monitor", default=None
)
# Hook events no monitor was bound for, by kind. instrument() resets it.
unattributed_events: Dict[str, int] = defaultdict(int)


@contextmanager
def bound_to(monitor: ClientMonitor) -> Iterator[None]:
    """Record the hook events of the current thread or task into monitor."""
    token = current_monitor.set(monitor)
    try:
        yield
    finally:
        current_monitor.reset(token)


def record(kind: str, address: Optional[str] = None, error=None):
    """Record a hook event into the monitor of the current thread or task."""
    monitor = current_monitor.get()
    if monitor is None:
        unattributed_events[kind] += 1
        return
    monitor.record(kind, address, error)


def instrument(monkeypatch):
    """Install the hooks that feed the monitors; see ClientMonitor."""
    unattributed_events.clear()
    original_connect = Connection._connect
    original_connect_check_health = AbstractConnection.connect_check_health
    original_initialize = NodesManager.initialize
    original_disconnect = AbstractConnection.disconnect

    def _connect(self):
        address = f"{self.host}:{self.port}"
        record("dial", address)
        try:
            return original_connect(self)
        except BaseException as e:
            record("dial_failure", address, e)
            raise

    def connect_check_health(self, *args, **kwargs):
        try:
            return original_connect_check_health(self, *args, **kwargs)
        except BaseException as e:
            record("connect_failure", f"{self.host}:{self.port}", e)
            raise

    def disconnect(self, *args, **kwargs):
        if self._sock is not None:
            record("drop", f"{self.host}:{self.port}")
        return original_disconnect(self, *args, **kwargs)

    def initialize(self, *args, **kwargs):
        record("refresh")
        try:
            return original_initialize(self, *args, **kwargs)
        except BaseException as e:
            record("refresh_failure", error=e)
            raise

    monkeypatch.setattr(Connection, "_connect", _connect)
    monkeypatch.setattr(
        AbstractConnection, "connect_check_health", connect_check_health
    )
    monkeypatch.setattr(AbstractConnection, "disconnect", disconnect)
    monkeypatch.setattr(NodesManager, "initialize", initialize)


def process_sockets_to(port: int) -> Optional[int]:
    """Established TCP sockets of this process to the database port.

    Counts what the operating system sees, so it also finds sockets that left the
    client's pools without being closed. None when psutil is not installed.
    """
    if psutil is None:
        return None
    process = psutil.Process()
    connections = getattr(process, "net_connections", process.connections)("tcp")
    return sum(
        1
        for c in connections
        if c.raddr and c.raddr.port == port and c.status == psutil.CONN_ESTABLISHED
    )


# Client-side faults the scenarios inject.


class AuthOutage:
    """Rejects AUTH on one node for a while, to simulation problematic server behaviour.
    This will let us validate reasonable library behaviour with faulty server.

    Nothing is rejected until arm() names the node. The outage then starts with the
    first handshake to that node and lasts AUTH_OUTAGE_SECONDS: the node accepts
    connections again, but not the database's credentials yet. The TCP connect
    succeeds and the handshake raises AuthenticationError, so the client cleans up
    the connection as it would after a real rejection.
    """

    def __init__(self, duration: float = AUTH_OUTAGE_SECONDS):
        self.duration = duration
        self.node: Optional[str] = None
        self.started: Optional[float] = None
        self.rejections = 0
        self._lock = threading.Lock()

    def arm(self, node: str):
        self.node = node

    @property
    def ended(self) -> Optional[float]:
        return None if self.started is None else self.started + self.duration

    def over(self) -> bool:
        ended = self.ended
        return ended is not None and _now() >= ended

    def rejects(self, address: str) -> bool:
        if address != self.node:
            return False
        with self._lock:
            if self.started is None:
                self.started = _now()
                logging.info(f"AUTH outage on {address} for {self.duration}s")
            if _now() - self.started >= self.duration:
                return False
            self.rejections += 1
            return True

    def expected(self, error: "CommandError") -> bool:
        """Whether the app's error is one the outage causes."""
        ended = self.ended
        return (
            ended is not None
            and error.error_type == AuthenticationError.__name__
            and self.started <= error.at <= ended + TRANSITION_MARGIN
        )


def inject_auth_outage(monkeypatch, outage: AuthOutage):
    original_handshake = AbstractConnection.on_connect_check_health

    def on_connect_check_health(self, *args, **kwargs):
        if outage.rejects(f"{self.host}:{self.port}"):
            raise AuthenticationError(AUTH_REJECTED_MESSAGE)
        return original_handshake(self, *args, **kwargs)

    monkeypatch.setattr(
        AbstractConnection, "on_connect_check_health", on_connect_check_health
    )


# What the client holds: its topology and its connections.


def _primaries(client: RedisCluster | AsyncRedisCluster) -> Set[str]:
    return {node.name for node in client.get_primaries()}


def _slot_owners(client: RedisCluster | AsyncRedisCluster) -> Set[str]:
    return {
        nodes[0].name for nodes in client.nodes_manager.slots_cache.values() if nodes
    }


def _mapped_slots(client: RedisCluster | AsyncRedisCluster) -> int:
    return sum(1 for nodes in client.nodes_manager.slots_cache.values() if nodes)


def _pool_connections(node) -> list:
    if node.redis_connection is None:
        return []
    pool = node.redis_connection.connection_pool
    with pool._lock:
        return [*pool._get_free_connections(), *pool._get_in_use_connections()]


def _client_connections(client: RedisCluster) -> list:
    """Every connection the client's node and startup node pools hold."""
    # A startup node is often the same object as a node of the topology.
    nodes = {
        id(node): node
        for node in [
            *client.nodes_manager.nodes_cache.values(),
            *client.nodes_manager.startup_nodes.values(),
        ]
    }
    return [conn for node in nodes.values() for conn in _pool_connections(node)]


def _open_sockets(client: RedisCluster) -> int:
    return sum(1 for conn in _client_connections(client) if conn._sock is not None)


def _connections_to(client: RedisCluster, address: str) -> int:
    node = client.get_node(node_name=address)
    return 0 if node is None else len(_pool_connections(node))


def _snapshot(client: RedisCluster) -> ClientSnapshot:
    pools = {
        name: _pool_connections(node)
        for name, node in list(client.nodes_manager.nodes_cache.items())
    }
    return ClientSnapshot(
        _now(),
        frozenset(_primaries(client)),
        {name: len(conns) for name, conns in pools.items()},
        {
            name: sum(1 for conn in conns if conn._sock is not None)
            for name, conns in pools.items()
        },
        _open_sockets(client),
        _mapped_slots(client),
    )
