"""Thresholds, validations and the scenario base class of the maintenance operation
tests with maintenance notifications disabled
(tests/test_scenario/test_maint_operations_sch_off.py, which describes the scenario
pattern and the checks).

The validations work on the monitor, the timeline and the workload stats, so the
async suite shares them. The async base class is in
tests/test_asyncio/test_scenario/async_maint_operations_helpers.py.
"""

import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Literal

import pytest

from redis import RedisCluster
from redis.asyncio import RedisCluster as AsyncRedisCluster
from redis.crc import REDIS_CLUSTER_HASH_SLOTS
from redis.exceptions import AuthenticationError
from tests.test_scenario.conftest import (
    _FAULT_INJECTOR_CLIENT_OSS_API,
    get_cluster_client_maint_notifications,
)
from tests.test_scenario.fault_injector_client import (
    FaultInjectorClient,
    SlotMigrateEffects,
)
from tests.test_scenario.maint_notifications_helpers import generate_params
from tests.test_scenario.scenario_monitor import (
    AUTH_OUTAGE_SECONDS,
    TRANSITION_MARGIN,
    AuthOutage,
    ClientMonitor,
    ClientSnapshot,
    Operation,
    Timeline,
    _Event,
    _mapped_slots,
    _now,
    _open_sockets,
    _primaries,
    _slot_owners,
    _snapshot,
    bound_to,
    instrument,
    process_sockets_to,
    unattributed_events,
)
from tests.test_scenario.scenario_workloads import (
    WORKLOAD_CLASSES,
    WORKLOADS,
    CommandError,
    ScenarioApp,
    WorkloadStats,
    keys_on_every_primary,
)
from tests.test_scenario.test_maint_notifications import TestPushNotificationsBase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S:%f",
)


# Load before the first operation, so every node's pool is warm.
WARMUP_SECONDS = 15
# How much longer the warm-up waits for every worker to have its connection to
# every primary. A consumer of the blocking workload spends most of its time
# blocked on one node, so its pools can take longer to fill.
WARMUP_EXTRA_SECONDS = 20
# Load after the last operation has settled, during which nothing may still change.
STEADY_SECONDS = 30
# How long after an operation the client may take to route to the new topology.
RECOVERY_TIMEOUT = 60
# How long the client must open no connection to a returning node for its
# workers' reconnects to count as done; see reconnects_settled.
RECONNECT_QUIET_SECONDS = 10
WORKERS = 10
# Starting thresholds, to be tightened from the numbers every run logs.
MAX_TRANSITION_ERROR_RATIO = 0.005
# A node that keeps its shards needs no new connection; this covers a worker that
# happens to need one more connection to it while the load shifts.
STABLE_NODE_DIAL_SLACK = WORKERS
# A node that joins starts with no connections, and each worker needs at most one.
JOINING_NODE_MAX_DIALS = 2 * WORKERS + 5
MAX_DIALS_PER_SECOND = 2 * WORKERS + 5
MAX_STEADY_DIALS = 2
# The default topology_refresh_min_interval of the cluster clients.
TOPOLOGY_REFRESH_MIN_INTERVAL = 1.0
REFRESH_SLACK = 2
# The connections the client holds. Each worker needs at most one connection per
# node, so the client holds about WORKERS per node once it is warm; the warm-up
# total is the baseline the whole scenario is held to.
NODE_CONNECTION_SLACK = 2
TOTAL_CONNECTION_SLACK = 5
# How much the held connections may move in the steady window.
STEADY_CONNECTION_DRIFT = 2
# New connections after the warm-up beyond the connections lost in that time.
CHURN_SLACK = 5
MIN_RECOVERED_THROUGHPUT_RATIO = 0.8

# The workloads every scenario runs at the same time, each on a client of its own.
# Set SCENARIO_WORKLOADS to a comma-separated subset of WORKLOADS to run fewer, for
# example SCENARIO_WORKLOADS=blocking to debug one.
SCENARIO_WORKLOADS = [
    name.strip()
    for name in os.environ.get("SCENARIO_WORKLOADS", ",".join(WORKLOADS)).split(",")
    if name.strip()
]
if not SCENARIO_WORKLOADS or set(SCENARIO_WORKLOADS) - set(WORKLOADS):
    raise ValueError(
        f"SCENARIO_WORKLOADS={os.environ.get('SCENARIO_WORKLOADS')!r} must name some "
        f"of {WORKLOADS}"
    )


# The node maintenance and failback scenario.
# Time the node stays in maintenance under load. The drill held it for about ten
# minutes; what matters here is that the client settles on the reduced topology.
HOLD_SECONDS = 30
# Budget for one node_maintenance step: the shard moves plus the fault injector's
# wait for the database to serve again.
NODE_MAINTENANCE_OP_TIMEOUT = 600
NODE_MAINTENANCE_ACTIVE_TIMEOUT = 300
FAILBACK_TEST_TIMEOUT = (
    WARMUP_SECONDS
    + WARMUP_EXTRA_SECONDS
    + 2 * NODE_MAINTENANCE_OP_TIMEOUT
    + HOLD_SECONDS
    + AUTH_OUTAGE_SECONDS
    + RECOVERY_TIMEOUT
    + STEADY_SECONDS
    + 180
)


# The single operation scenario: the operations it runs, named as the fault
# injector's triggers, and how long the app keeps running on the topology the
# operation left.
SINGLE_OPERATIONS = ["maintenance_mode"]
OPERATE_SECONDS = 60
SINGLE_OPERATION_TEST_TIMEOUT = (
    WARMUP_SECONDS
    + WARMUP_EXTRA_SECONDS
    + NODE_MAINTENANCE_OP_TIMEOUT
    + RECOVERY_TIMEOUT
    + OPERATE_SECONDS
    + STEADY_SECONDS
    + 180
)


def _node_maintenance_db_configs() -> list:
    """The database shapes of the node maintenance scenarios, as pytest params.

    IP endpoints, as the drill's client saw them, in plain TCP and TLS: the drill's
    application used TLS, which makes every reconnect a TLS handshake. Hostname
    endpoints and mTLS do not change the reconnect logic under test, and every run
    of this suite is several minutes long. The database is replicated, as
    production databases are: the maintenance then fails the node's masters over to
    their replicas before it moves the shards away.
    """
    configs = []
    for _, _, db_config, _ in generate_params(
        _FAULT_INJECTOR_CLIENT_OSS_API,
        [SlotMigrateEffects.REMOVE],
        include_tls=True,
        triggers=["maintenance_mode"],
    ):
        if db_config.get("oss_cluster_api_preferred_endpoint_type") != "ip":
            continue
        if db_config.get("enforce_client_authentication") == "enabled":
            continue
        tls = db_config.get("tls_mode") == "enabled"
        configs.append(
            pytest.param(
                {**db_config, "replication": True}, id="tls" if tls else "no_tls"
            )
        )
    return configs


NODE_MAINTENANCE_DB_CONFIGS = _node_maintenance_db_configs()


# The validations shared by every scenario, for both stacks.


def log_summary(
    workload: str, monitor: ClientMonitor, stats: WorkloadStats, timeline: Timeline
):
    for name, start, end in timeline.phases():
        logging.info(
            f"[{workload}] [{name}] {end - start:.1f}s: "
            f"dials {monitor.by_address('dial', start, end)}, "
            f"dial failures {monitor.count('dial_failure', start=start, end=end)}, "
            f"connect failures {monitor.count('connect_failure', start=start, end=end)}, "
            f"refreshes {monitor.count('refresh', start=start, end=end)} "
            f"({monitor.count('refresh_failure', start=start, end=end)} failed), "
            f"topology changes {len(monitor.changes_between(start, end))}, "
            f"commands {stats.commands_between(start, end)}, "
            f"errors {stats.errors_between(start, end)}"
        )
    logging.info(
        f"[{workload}] Topology changes: "
        + "; ".join(
            f"+{sorted(c.added)} -{sorted(c.removed)}" for c in monitor.topology_changes
        )
    )


def validate_errors(
    stats: WorkloadStats,
    timeline: Timeline,
    expected: Callable[[CommandError], bool] | None = None,
):
    """Check the app's errors.

    expected marks the errors a scenario causes on purpose. They are left out of
    every check here, and the scenario validates them itself.
    """
    errors = [e for e in stats.errors if expected is None or not expected(e)]
    # RedisClusterUnreachableError is not a RedisError, so it lands here too.
    never_allowed = [e for e in errors if not e.is_redis_error or not e.is_transient]
    assert not never_allowed, (
        f"{len(never_allowed)} error(s) that must never happen: "
        + "; ".join(
            f"{e.command} {e.error_type}: {e.message}" for e in never_allowed[:5]
        )
    )

    outside = [e for e in errors if not timeline.in_transition(e.at)]
    assert not outside, f"{len(outside)} error(s) outside the operations: " + "; ".join(
        f"{e.command} {e.error_type}: {e.message}" for e in outside[:5]
    )

    for start, end in timeline.transition_windows():
        in_window = sum(1 for e in errors if start <= e.at <= end)
        commands = stats.commands_between(start, end)
        assert in_window <= MAX_TRANSITION_ERROR_RATIO * max(commands, 1), (
            f"{in_window} errors in {commands} commands during an operation, more "
            f"than {MAX_TRANSITION_ERROR_RATIO:.1%}"
        )


def phases_at(timeline: Timeline, at: float) -> list[str]:
    """The phases of the timeline the one second that starts at at overlaps."""
    return [
        name for name, start, end in timeline.phases() if start < at + 1 and at <= end
    ]


def second_activity(monitor: ClientMonitor, at: float) -> str:
    """What the client did in the one second that starts at at, per node."""
    end = at + 1
    return (
        f"new connections {monitor.by_address('dial', at, end)}, dropped "
        f"{monitor.by_address('drop', at, end)}, failed connects "
        f"{monitor.by_address('connect_failure', at, end)}, failed dials "
        f"{monitor.by_address('dial_failure', at, end)}, refreshes "
        f"{monitor.count('refresh', start=at, end=end)}"
    )


def validate_connections(
    monitor: ClientMonitor,
    timeline: Timeline,
    expected_dials: Callable[[_Event], bool] | None = None,
):
    """Bound the new connections and refreshes.

    expected_dials marks the new connections a scenario causes on purpose. They
    are left out of the peak per second, and the scenario validates them itself.
    """
    after_warmup = timeline.warmed_up
    end = timeline.steady_done

    # The nodes that keep their shards must keep their connections. In the drill a
    # failed connect to a returning node tore down the healthy nodes' connections
    # too.
    stable = monitor.stable_nodes(after_warmup, end)
    stable_dials = monitor.by_address("dial", after_warmup, end)
    for address in stable:
        assert stable_dials.get(address, 0) <= STABLE_NODE_DIAL_SLACK, (
            f"{stable_dials.get(address, 0)} new connections to node {address}, "
            f"which kept its shards (at most {STABLE_NODE_DIAL_SLACK})"
        )

    peak, peak_at = monitor.peak_second("dial", after_warmup, end, expected_dials)
    assert peak <= MAX_DIALS_PER_SECOND, (
        f"{peak} new connections in one second (at most {MAX_DIALS_PER_SECOND}), "
        f"at {peak_at - timeline.started:.1f}s, in "
        f"{', '.join(phases_at(timeline, peak_at)) or 'no phase'}: "
        + second_activity(monitor, peak_at)
    )

    failed_after_settled = monitor.count(
        "connect_failure", start=timeline.settled, end=end
    ) + monitor.count("dial_failure", start=timeline.settled, end=end)
    assert failed_after_settled == 0, (
        f"{failed_after_settled} failed connects after the client recovered"
    )

    steady_dials = monitor.count(
        "dial", start=timeline.steady_started, end=timeline.steady_done
    )
    assert steady_dials <= MAX_STEADY_DIALS, (
        f"{steady_dials} new connections in the steady window "
        f"(at most {MAX_STEADY_DIALS})"
    )
    steady_refreshes = monitor.count(
        "refresh", start=timeline.steady_started, end=timeline.steady_done
    )
    assert steady_refreshes == 0, (
        f"{steady_refreshes} topology refreshes in the steady window"
    )
    steady_changes = monitor.changes_between(
        timeline.steady_started, timeline.steady_done
    )
    assert not steady_changes, (
        f"Topology changes in the steady window: {steady_changes}"
    )

    for name, start, window_end in timeline.operation_windows():
        refreshes = monitor.count("refresh", start=start, end=window_end)
        limit = (window_end - start) / TOPOLOGY_REFRESH_MIN_INTERVAL + REFRESH_SLACK
        assert refreshes <= limit, (
            f"{refreshes} topology refreshes during {name} (at most {limit:.0f})"
        )


def reconnects_settled(monitor: ClientMonitor, node: str) -> bool:
    """Whether the client stopped opening connections to node: none for the last
    RECONNECT_QUIET_SECONDS.

    A worker reconnects to a node that returned only when it next sends the node
    a command, so the reconnects trickle in after the node is back. The failback
    has settled once they have stopped: they then come before the steady window,
    not in it. A client holds only as many connections to a node as its workers
    use at the same time, so no count of them tells that they are done.
    """
    return not monitor.count(
        "dial", start=_now() - RECONNECT_QUIET_SECONDS, addresses={node}
    )


def missing_connections(snapshot: ClientSnapshot, primaries: frozenset[str]) -> int:
    """The connections the workers still lack to primaries in snapshot.

    Each worker needs one connection to every primary, as it has a key on each.
    """
    return sum(max(0, WORKERS - snapshot.sockets.get(p, 0)) for p in primaries)


def validate_held_connections(monitor: ClientMonitor, timeline: Timeline):
    """The client never holds more connections than the app needs.

    The baseline is what the client held at the end of the warm-up. From then on,
    in every snapshot, the client holds at most the baseline plus slack in total,
    and at most WORKERS plus slack per node. In the steady window it is back at
    the baseline, less what it held to the primaries that are no longer in its
    topology, and stays there. And every new connection after the warm-up
    replaces one that was lost: a client that opens connections without losing
    any keeps more than it needs, whatever the timing.

    The warm-up can end before every worker has its connection to every primary:
    a worker connects to a node only when it sends the node a command, and a
    consumer of the blocking workload spends most of its time blocked on one node.
    The connections the workers still lacked then (missing_connections) come
    later without replacing any, so each limit above allows them too.
    """
    baseline_snapshots = [s for s in monitor.snapshots if s.at <= timeline.warmed_up]
    assert baseline_snapshots, "No snapshot of the client before the warm-up ended"
    warm = baseline_snapshots[-1]
    baseline = warm.open_sockets
    run = [
        s
        for s in monitor.snapshots
        if timeline.warmed_up < s.at <= timeline.steady_done
    ]
    steady = [s for s in run if s.at >= timeline.steady_started]
    # A node that left the topology for good, as a node left in maintenance does,
    # takes the connections the client held to it along.
    removed = warm.primaries - steady[-1].primaries
    steady_baseline = baseline - sum(warm.sockets.get(n, 0) for n in removed)
    missing = missing_connections(warm, warm.primaries)
    steady_missing = missing_connections(warm, warm.primaries - removed)
    new = monitor.count(
        "dial", start=timeline.warmed_up, end=timeline.steady_done
    ) - monitor.count(
        "dial_failure", start=timeline.warmed_up, end=timeline.steady_done
    )
    lost = monitor.count("drop", start=timeline.warmed_up, end=timeline.steady_done)
    peak = max(run, key=lambda s: s.open_sockets)
    logging.info(
        f"Held connections: {baseline} after the warm-up ({steady_baseline} without "
        f"the removed nodes {sorted(removed)}), {missing} short of what the "
        f"workers need, peak {peak.open_sockets}, steady "
        f"{min(s.open_sockets for s in steady)}-"
        f"{max(s.open_sockets for s in steady)}; after the warm-up {new} new and "
        f"{lost} lost"
    )

    total_limit = baseline + missing + TOTAL_CONNECTION_SLACK
    node_limit = WORKERS + NODE_CONNECTION_SLACK
    for snapshot in run:
        assert snapshot.open_sockets <= total_limit, (
            f"The client held {snapshot.open_sockets} connections at "
            f"{snapshot.at - timeline.started:.1f}s, more than the "
            f"{baseline} it held after the warm-up, plus the {missing} the workers "
            f"still lacked, plus {TOTAL_CONNECTION_SLACK}"
        )
        crowded = {n: c for n, c in snapshot.sockets.items() if c > node_limit}
        assert not crowded, (
            f"The client held more than {node_limit} connections to a node at "
            f"{snapshot.at - timeline.started:.1f}s: {crowded}"
        )

    low = min(s.open_sockets for s in steady)
    high = max(s.open_sockets for s in steady)
    assert steady_baseline - TOTAL_CONNECTION_SLACK <= low and (
        high <= steady_baseline + steady_missing + TOTAL_CONNECTION_SLACK
    ), (
        f"The client held {low}-{high} connections in the steady window, not the "
        f"{steady_baseline} it held after the warm-up to the nodes still in its "
        f"topology, plus up to the {steady_missing} the workers still lacked to "
        f"them"
    )
    assert high - low <= STEADY_CONNECTION_DRIFT, (
        f"The held connections still moved between {low} and {high} in the "
        f"steady window"
    )

    assert new <= lost + missing + CHURN_SLACK, (
        f"{new} new connections after the warm-up, but only {lost} were lost and "
        f"the workers lacked {missing}: the client opened {new - lost - missing} "
        f"more than it needed"
    )


def validate_process_sockets(
    held: int, port: int, when: str, clients: int = 1
) -> int | None:
    """The process has no sockets to the database beyond the clients' pools.

    Compares what the operating system sees with held, the open sockets in the
    pools of all the process's clients, of which there are clients. Skipped, and
    None returned, without psutil.
    """
    sockets = process_sockets_to(port)
    if sockets is None:
        logging.info(f"psutil is not installed; not checking the sockets {when}")
        return None
    logging.info(f"Sockets to port {port} {when}: {sockets}, in the pools: {held}")
    assert abs(sockets - held) <= NODE_CONNECTION_SLACK * clients, (
        f"The process has {sockets} sockets to port {port} {when}, but the clients' "
        f"pools hold {held}: sockets leaked out of the pools"
    )
    return sockets


def validate_slot_coverage(monitor: ClientMonitor, timeline: Timeline):
    """The client's slot map routes every slot in every snapshot after the warm-up.

    A refresh that keeps a partial map, even briefly, leaves commands with no node
    to go to.
    """
    for snapshot in monitor.snapshots:
        if not timeline.warmed_up < snapshot.at <= timeline.steady_done:
            continue
        assert snapshot.mapped_slots == REDIS_CLUSTER_HASH_SLOTS, (
            f"The client mapped {snapshot.mapped_slots} of {REDIS_CLUSTER_HASH_SLOTS} "
            f"slots at {snapshot.at - timeline.started:.1f}s"
        )


def validate_final_state(monitor: ClientMonitor):
    """The client ends the scenario routing every slot to a connected primary.

    Takes the monitor's last snapshot, so call it after the monitor has stopped.
    """
    final = monitor.snapshots[-1]
    disconnected = sorted(p for p in final.primaries if not final.sockets.get(p))
    assert not disconnected, (
        f"The client holds no connected socket to primaries {disconnected} at the end"
    )


def validate_slots(client: RedisCluster | AsyncRedisCluster):
    mapped = _mapped_slots(client)
    assert mapped == REDIS_CLUSTER_HASH_SLOTS, (
        f"The client maps {mapped} of {REDIS_CLUSTER_HASH_SLOTS} slots at the end"
    )
    owners = _slot_owners(client)
    primaries = _primaries(client)
    assert owners == primaries, (
        f"Slot owners {sorted(owners)} differ from the client's primaries "
        f"{sorted(primaries)} at the end"
    )


def validate_notifications_disabled(client: RedisCluster | AsyncRedisCluster):
    """The client runs without maintenance notifications, as the scenarios need."""
    config = client.maint_notifications_config
    assert not (config and config.enabled), (
        "The client has maintenance notifications enabled"
    )
    assert client._oss_cluster_maint_notifications_handler is None, (
        "The client has a maintenance notifications handler"
    )


def validate_joining_node(monitor: ClientMonitor, since: float, end: float, node: str):
    """A node that joins the topology gets only the connections the app needs."""
    dials = monitor.count("dial", start=since, end=end, addresses={node})
    assert dials <= JOINING_NODE_MAX_DIALS, (
        f"{dials} new connections to the joining node {node} "
        f"(at most {JOINING_NODE_MAX_DIALS})"
    )


def throughput_status(stats: WorkloadStats, baseline: float) -> str:
    return (
        f"throughput {stats.recent_throughput():.0f}/s against a baseline of "
        f"{baseline:.0f}/s"
    )


def recovery_status(client, stats: WorkloadStats, baseline: float, node: str) -> str:
    return (
        f"node {node} owns slots: {node in _slot_owners(client)}, "
        + throughput_status(stats, baseline)
    )


def validate_failback_topology(
    monitor: ClientMonitor, timeline: Timeline, enter: Operation, node: str
):
    """The client drops the node once and takes it back once, without flapping."""
    changes = monitor.changes_between(timeline.warmed_up, timeline.steady_done)
    assert [(c.added, c.removed) for c in changes] == [
        (frozenset(), frozenset({node})),
        (frozenset({node}), frozenset()),
    ], f"Expected node {node} to be removed and then added once, got {changes}"
    removed, added = changes
    assert removed.at >= enter.started, (
        f"Node {node} left the client's topology before the maintenance started"
    )
    exit_started = timeline.operations[-1].started
    assert removed.at <= exit_started <= added.at, (
        f"Node {node} rejoined the client's topology before the failback started"
    )


def validate_auth_outage(
    monitor: ClientMonitor, stats: WorkloadStats, outage: AuthOutage, end: float
):
    """The client rides out the AUTH outage without a reconnect loop.

    The outage really happened for the client, the rejected handshakes stay within
    what the app's commands need, and once AUTH works again the node gets only the
    connections the app needs. Each command the app routes to the node while it
    rejects AUTH dials it once and fails, so the app's command rate sets the dials
    to it per second, and validate_connections leaves them out
    (AuthOutage.expected_dial). A reconnect loop shows instead as more rejected
    handshakes than failed commands. A topology refresh may try the node too and
    fail over to the next one, so the handshakes a refresh makes are not counted.
    """
    started, ended = outage.started(monitor), outage.ended(monitor)
    assert started is not None and ended is not None, (
        f"The returning node {outage.node} never got a handshake from the client, "
        f"so the AUTH outage never started for it"
    )
    # In the drill a refresh failed on the rejected AUTH first ("Topology refresh
    # failed, keeping the current one"), and the client then reported the healthy
    # cluster as unreachable. One node rejecting AUTH must not fail a refresh.
    failed_refreshes = monitor.events(
        "refresh_failure", start=started, end=ended + TRANSITION_MARGIN
    )
    assert not failed_refreshes, (
        f"{len(failed_refreshes)} topology refresh(es) failed during the AUTH outage: "
        + ", ".join(sorted({e.error or "" for e in failed_refreshes}))
    )
    failed_connects = monitor.events(
        "connect_failure", start=started, end=ended, addresses={outage.node}
    )
    # A failed dial fails the connect too, but only a rejected AUTH is the outage's
    rejections = [e for e in failed_connects if e.error == AuthenticationError.__name__]
    rejected = [e for e in rejections if not e.in_refresh]
    auth_errors = sum(1 for e in stats.errors if outage.expected_for(monitor, e))
    peak, peak_at = monitor.peak_second("dial", started, ended)
    logging.info(
        f"AUTH outage on {outage.node} for {monitor.name}: "
        f"{outage.rejections(monitor)} rejected handshakes, {len(rejected)} of "
        f"commands and {len(rejections) - len(rejected)} of refreshes, "
        f"{len(failed_connects) - len(rejections)} other failed connects, peak "
        f"{peak} new connections per second, {auth_errors} commands failed with "
        f"AuthenticationError; rejected by caller: {outage.callers(monitor)}"
    )
    assert len(rejected) <= auth_errors, (
        f"{len(rejected)} rejected handshakes to {outage.node} during the AUTH "
        f"outage, outside topology refreshes, but only {auth_errors} commands failed "
        f"with AuthenticationError: the client redials the node on its own. Rejected "
        f"handshakes by caller: {outage.callers(monitor)}. Peak {peak} new "
        f"connections per second, {peak_at - started:.1f}s into it: "
        + second_activity(monitor, peak_at)
    )
    validate_joining_node(monitor, ended, end, outage.node)


@dataclass
class Expectation:
    """What the client must end up seeing after an operation.

    reached(client) tells when the client has caught up with the operation,
    status(client) describes the client for a failure message, and validate runs
    on the monitor and the timeline after the scenario.
    """

    reached: Callable[[Any], bool]
    status: Callable[[Any], str]
    validate: Callable[[ClientMonitor, Timeline], None]


def node_removed_expectation(before: set[str]) -> Expectation:
    """The operation takes exactly one node out of the client's topology."""

    def reached(client) -> bool:
        primaries = _primaries(client)
        return len(primaries) == len(before) - 1 and primaries < before

    def status(client) -> str:
        return f"primaries {sorted(_primaries(client))}, before {sorted(before)}"

    def validate(monitor: ClientMonitor, timeline: Timeline):
        changes = monitor.changes_between(timeline.warmed_up, timeline.steady_done)
        assert len(changes) == 1 and not changes[0].added, (
            f"Expected one node to leave the client's topology once, got {changes}"
        )
        assert len(changes[0].removed) == 1 and changes[0].removed < before, (
            f"Expected exactly one of {sorted(before)} to leave, got {changes[0]}"
        )
        final = monitor.snapshots[-1].primaries
        assert final == before - changes[0].removed, (
            f"The client ends with primaries {sorted(final)}, expected "
            f"{sorted(before - changes[0].removed)}"
        )

    return Expectation(reached, status, validate)


@dataclass
class WorkloadRun:
    """One workload of a scenario, on a client and with a monitor of its own."""

    name: str
    client: RedisCluster
    monitor: ClientMonitor
    app: ScenarioApp
    baseline: float = 0.0


def collect_failure(failures: list[str], workload: str, check: Callable[[], None]):
    """Run check and add its assertion failure, labeled with workload, to failures.

    The scenario validates every workload, and reports all their failures at once.
    """
    try:
        check()
    except AssertionError as e:
        failures.append(f"[{workload}] {e}")


class ClusterMaintScenarioBase(TestPushNotificationsBase):
    """Runs a scenario: the app in the background, operations in the main thread.

    The app runs every workload of SCENARIO_WORKLOADS at the same time, each on a
    client and with a monitor of its own, so each client is validated on its own.
    A scenario calls _start_app, runs its operations on self._timeline, then calls
    _finish, which runs the steady window, the shared validations and the close.
    """

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self, fault_injector_client_oss_api: FaultInjectorClient):
        self._fault_injector = fault_injector_client_oss_api
        self._bdb_name = None
        self._endpoint_config = None
        self._runs: list[WorkloadRun] = []
        self._timeline = Timeline()
        self._db_port = 0
        self._nodes_in_maintenance: set[int] = set()

        yield

        logging.info("Starting cleanup...")
        try:
            for run in self._runs:
                run.app.stop()
            for run in self._runs:
                run.monitor.stop()
            for run in self._runs:
                run.client.close()
            self._runs = []
        finally:
            # The cluster is shared with the later tests, so it is restored even
            # when stopping the app or closing a client fails.
            if self._bdb_name:
                self.delete_prev_db(self._fault_injector, self._bdb_name)
            for node_id in list(self._nodes_in_maintenance):
                # A scenario that ends with a node in maintenance, or fails before
                # its failback, leaves the node evacuated. The database is gone by
                # now, so take the node out of maintenance without moving shards
                # back; later tests get the whole cluster.
                try:
                    self._node_maintenance("exit", node_id, restore_shards=False)
                except (Exception, pytest.fail.Exception) as e:
                    logging.error(
                        f"Failed to take node {node_id} out of maintenance: {e}"
                    )
            logging.info("Cleanup finished")

    def _start_app(
        self,
        monkeypatch,
        db_config: dict[str, Any],
        dynamic_startup_nodes: bool,
    ) -> list[WorkloadRun]:
        """Create a new database and a client per workload, start the apps and the
        monitors, and warm up.

        Returns the workload runs. Each client serves its workload for the whole
        scenario.
        """
        # A database of its own: a unique name and a port RE picks, so nothing an
        # earlier test left behind affects this one.
        db_config = {
            **db_config,
            "name": f"{db_config['name']}-{uuid.uuid4().hex[:6]}",
        }
        db_config.pop("port", None)
        self._bdb_name = db_config["name"]
        self._endpoint_config = self.create_db(self._fault_injector, db_config)
        # Every new connection, also to a node that returns from maintenance, must
        # pass AUTH, as in the drill. The fault injector gives every database it
        # creates a password.
        assert self._endpoint_config.get("password"), (
            "The database has no password, so the client never sends AUTH"
        )
        self._db_port = self._endpoint_config["raw_endpoints"][0]["port"]

        # The hooks go in first, so each monitor also sees its client's first
        # connects and refresh.
        instrument(monkeypatch)
        for name in SCENARIO_WORKLOADS:
            self._runs.append(self._new_run(name, dynamic_startup_nodes))

        self._timeline = Timeline()
        for run in self._runs:
            run.monitor.start()
            run.app.start()
        time.sleep(WARMUP_SECONDS)
        self._wait_for_warm_pools()
        self._timeline.warmed_up = _now()
        for run in self._runs:
            run.baseline = run.app.stats.recent_throughput()
            logging.info(
                f"[{run.name}] Warmed up: primaries "
                f"{sorted(_primaries(run.client))}, {run.baseline:.0f} ops/s"
            )
        self._validate_process_sockets("after the warm-up")
        return self._runs

    def _wait_for_warm_pools(self):
        """Wait, at most WARMUP_EXTRA_SECONDS, until every worker has its
        connection to every primary, so the warm-up baseline is what the app needs.
        """
        deadline = _now() + WARMUP_EXTRA_SECONDS
        started = _now()
        while True:
            missing = {}
            for run in self._runs:
                snapshot = _snapshot(run.client)
                missing[run.name] = missing_connections(snapshot, snapshot.primaries)
            if not any(missing.values()) or _now() >= deadline:
                break
            time.sleep(1)
        if _now() > started + 1 or any(missing.values()):
            logging.info(
                f"Warm-up extended by {_now() - started:.0f}s; connections the "
                f"workers still lack: {missing}"
            )

    def _new_run(self, workload: str, dynamic_startup_nodes: bool) -> WorkloadRun:
        monitor = ClientMonitor(lambda: _snapshot(client), workload)
        with bound_to(monitor):
            client = get_cluster_client_maint_notifications(
                endpoints_config=self._endpoint_config,
                enable_maintenance_notifications=False,
                dynamic_startup_nodes=dynamic_startup_nodes,
            )
        validate_notifications_disabled(client)
        app = ScenarioApp(
            client,
            WORKLOAD_CLASSES[workload](),
            [
                keys_on_every_primary(client, prefix=f"maint_{workload}_{index}")
                for index in range(WORKERS)
            ],
            monitor,
        )
        return WorkloadRun(workload, client, monitor, app)

    def _common_primaries(self) -> set[str]:
        """The primaries every client sees; they must all see the same."""
        primaries = {run.name: _primaries(run.client) for run in self._runs}
        common = next(iter(primaries.values()))
        assert all(p == common for p in primaries.values()), (
            f"The clients see different primaries: {primaries}"
        )
        return common

    def _validate_process_sockets(self, when: str):
        validate_process_sockets(
            sum(_open_sockets(run.client) for run in self._runs),
            self._db_port,
            when,
            clients=len(self._runs),
        )

    def _wait_for_recovery(
        self,
        condition: Callable[[RedisCluster], bool],
        status: Callable[[WorkloadRun], str],
    ) -> float:
        """Wait until condition(client) holds and the throughput is back, for every
        workload.

        Returns the time it happened. status(run) describes a workload's state for
        the failure message.
        """
        deadline = _now() + RECOVERY_TIMEOUT
        while True:
            pending = [
                run
                for run in self._runs
                if not (
                    condition(run.client)
                    and run.app.stats.recent_throughput()
                    >= MIN_RECOVERED_THROUGHPUT_RATIO * run.baseline
                )
            ]
            if not pending:
                return _now()
            if _now() >= deadline:
                break
            time.sleep(1)
        pytest.fail(
            f"No recovery within {RECOVERY_TIMEOUT}s after "
            f"{self._timeline.operations[-1].name}: "
            + "; ".join(f"[{run.name}] {status(run)}" for run in pending)
        )

    def _finish(
        self,
        expected_errors: Callable[[CommandError], bool] | None = None,
        validate_run: Callable[[WorkloadRun], None] | None = None,
        expected_dials: Callable[[_Event], bool] | None = None,
    ):
        """Run the steady window, stop the apps, validate and close the clients.

        expected_errors marks the app errors the scenario causes on purpose; see
        validate_errors. expected_dials marks its new connections; see
        validate_connections. validate_run(run) adds the scenario's own checks of
        each workload. Every workload is validated, and all the failures are reported
        together.
        """
        timeline, runs = self._timeline, self._runs
        timeline.steady_started = _now()
        time.sleep(STEADY_SECONDS)
        timeline.steady_done = _now()
        for run in runs:
            run.app.stop()
        for run in runs:
            run.monitor.stop()

        failures: list[str] = []
        for run in runs:
            log_summary(run.name, run.monitor, run.app.stats, timeline)

            def check(run=run):
                with bound_to(run.monitor):
                    validate_errors(run.app.stats, timeline, expected_errors)
                    run.app.validate()
                    validate_connections(run.monitor, timeline, expected_dials)
                    validate_held_connections(run.monitor, timeline)
                    validate_slot_coverage(run.monitor, timeline)
                    validate_final_state(run.monitor)
                    validate_slots(run.client)
                    assert run.client.ping(target_nodes=RedisCluster.PRIMARIES), (
                        "A primary did not answer PING at the end"
                    )
                if validate_run is not None:
                    validate_run(run)

            collect_failure(failures, run.name, check)
        collect_failure(
            failures, "all", lambda: self._validate_process_sockets("at the end")
        )

        for run in runs:
            with bound_to(run.monitor):
                run.client.close()
            open_after_close = _open_sockets(run.client)
            if open_after_close:
                failures.append(
                    f"[{run.name}] {open_after_close} socket(s) still open after "
                    f"close()"
                )
        self._runs = []
        process_after_close = process_sockets_to(self._db_port)
        if process_after_close:
            failures.append(
                f"[all] The process still has {process_after_close} sockets to port "
                f"{self._db_port} after close()"
            )
        if unattributed_events:
            failures.append(
                f"[all] Hook events no client was bound for, so no monitor counted "
                f"them: {dict(unattributed_events)}"
            )
        if failures:
            pytest.fail("\n".join(failures))

    def _track_failed_enter(self, action_id: str):
        """Hand the node of a failed 'enter' to the cleanup.

        An 'enter' that fails may have put its node in maintenance already. The
        fault injector names that node in the output of the failed action.
        """
        try:
            status = self._fault_injector.get_action_status(action_id)
        except Exception as e:
            logging.error(f"Failed to read the node of the failed maintenance: {e}")
            return
        node_id = (status.get("output") or {}).get("node_id")
        if node_id is not None:
            self._nodes_in_maintenance.add(node_id)

    def _node_maintenance(
        self,
        mode: Literal["enter", "exit"],
        node_id: int | None = None,
        restore_shards: bool = True,
    ) -> dict:
        action_id = self._fault_injector.node_maintenance(
            self._endpoint_config,
            mode,
            node_id=node_id,
            active_timeout=NODE_MAINTENANCE_ACTIVE_TIMEOUT,
            restore_shards=restore_shards,
        )
        try:
            result = self._fault_injector.get_operation_result(
                action_id, timeout=NODE_MAINTENANCE_OP_TIMEOUT
            )
        except pytest.fail.Exception:
            if mode == "enter":
                self._track_failed_enter(action_id)
            raise
        logging.info(f"node_maintenance {mode} result: {result}")
        output = result.get("output", {})
        if mode == "enter":
            self._nodes_in_maintenance.add(output["node_id"])
        else:
            self._nodes_in_maintenance.discard(node_id)
        return output
