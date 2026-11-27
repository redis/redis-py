"""Unit tests of the validations and the monitor of the cluster scenario tests.

These need no Redis server: they feed the validations recorded events, snapshots
and stats directly.
"""

from types import SimpleNamespace

import pytest

from tests.test_scenario.maint_operations_helpers import (
    RECONNECT_QUIET_SECONDS,
    WORKERS,
    missing_connections,
    reconnects_settled,
    validate_auth_outage,
    validate_held_connections,
    validate_notifications_disabled,
)
from tests.test_scenario.scenario_monitor import (
    AuthOutage,
    ClientMonitor,
    ClientSnapshot,
    Timeline,
    _Event,
    _now,
    bound_to,
    record,
    refreshing,
)
from tests.test_scenario.scenario_workloads import (
    CommandError,
    QueueStats,
    WorkloadStats,
    validate_queue,
)

PRIMARIES = frozenset({"n1:1", "n2:1", "n3:1", "n4:1", "n5:1"})


def _monitor(name: str = "client") -> ClientMonitor:
    return ClientMonitor(lambda: None, name)


def _snapshot(at: float, sockets: dict[str, int]) -> ClientSnapshot:
    return ClientSnapshot(
        at,
        PRIMARIES,
        dict(sockets),
        dict(sockets),
        sum(sockets.values()),
        16384,
    )


def _timeline() -> Timeline:
    timeline = Timeline(started=0.0)
    timeline.warmed_up = 10.0
    timeline.steady_started = 90.0
    timeline.steady_done = 100.0
    return timeline


def _held(monitor: ClientMonitor, warm: dict[str, int], later: dict[str, int]):
    """A warm-up snapshot of warm, then later from just after it to the end."""
    monitor.snapshots.append(_snapshot(9.0, warm))
    for at in range(11, 101):
        monitor.snapshots.append(_snapshot(float(at), later))


class TestHeldConnections:
    def test_missing_connections_counts_what_the_workers_lack(self):
        snapshot = _snapshot(0.0, {"n1:1": 9, "n2:1": 9, "n3:1": 7, "n4:1": 7})
        # n5 has none
        assert missing_connections(snapshot, PRIMARIES) == 1 + 1 + 3 + 3 + WORKERS

    def test_reconnects_settle_once_no_new_connection_came_for_a_while(self):
        monitor = _monitor()
        quiet = RECONNECT_QUIET_SECONDS
        monitor._events.append(_Event(_now() - quiet - 1, "dial", "n1:1"))
        assert reconnects_settled(monitor, "n1:1")

        monitor._events.append(_Event(_now() - 1, "dial", "n1:1"))
        assert not reconnects_settled(monitor, "n1:1")
        # Connections to other nodes do not count
        assert reconnects_settled(monitor, "n2:1")

    def test_connections_the_warm_up_did_not_make_yet_are_allowed(self):
        monitor = _monitor()
        warm = {"n1:1": 9, "n2:1": 9, "n3:1": 7, "n4:1": 7, "n5:1": 10}
        _held(monitor, warm, {p: WORKERS for p in PRIMARIES})
        for at in range(20, 28):
            monitor._events.append(_Event(float(at), "dial", "n3:1"))

        validate_held_connections(monitor, _timeline())

    def test_more_connections_than_the_workers_need_fail(self):
        monitor = _monitor()
        warm = {p: WORKERS for p in PRIMARIES}
        later = {**warm, "n1:1": WORKERS + 2, "n2:1": WORKERS + 2, "n3:1": 13}
        _held(monitor, warm, later)

        with pytest.raises(AssertionError, match="more than the 50 it held"):
            validate_held_connections(monitor, _timeline())

    def test_new_connections_beyond_the_lost_and_the_lacking_fail(self):
        monitor = _monitor()
        warm = {p: WORKERS for p in PRIMARIES}
        _held(monitor, warm, warm)
        # Replaced 6 times over without losing any
        for at in range(20, 26):
            monitor._events.append(_Event(float(at), "dial", "n1:1"))

        with pytest.raises(AssertionError, match="opened 6 more than it needed"):
            validate_held_connections(monitor, _timeline())


class TestQueueValidation:
    def test_a_duplicate_is_reported_with_its_push_and_pops(self):
        monitor = _monitor()
        monitor._events.append(_Event(10.5, "drop", "n1:1"))
        queues = QueueStats()
        queues.pushed["q"] = {"0-1", "0-2"}
        queues.push_spans["q"] = {"0-1": (10.0, 11.0), "0-2": (20.0, 20.1)}
        queues.popped["q"] = ["0-1", "0-1", "0-2"]
        queues.pop_spans["q"] = [
            ("0-1", 12.0, 12.2),
            ("0-1", 13.0, 13.2),
            ("0-2", 21.0, 21.5),
        ]

        problem = validate_queue("q", [], queues, monitor, since=0.0)

        assert problem.startswith("q: a value was popped or queued twice")
        assert (
            "0-1: popped 2x, still queued 0x; RPUSH acknowledged at 10.00-11.00s"
            in (problem)
        )
        assert "drops {'n1:1': 1}" in problem
        assert "BLPOP at 12.00-12.20s" in problem
        assert "BLPOP at 13.00-13.20s" in problem
        assert "0-2:" not in problem

    def test_a_value_popped_and_still_queued_is_reported(self):
        queues = QueueStats()
        queues.pushed["q"] = {"0-1"}
        queues.popped["q"] = ["0-1"]

        problem = validate_queue("q", ["0-1"], queues)

        assert "0-1: popped 1x, still queued 1x; RPUSH acknowledged" in problem

    def test_a_queue_without_duplicates_passes(self):
        queues = QueueStats()
        queues.pushed["q"] = {"0-1", "0-2"}
        queues.popped["q"] = ["0-1"]

        assert validate_queue("q", ["0-2"], queues) is None


class TestAuthOutage:
    def test_each_client_gets_its_own_outage(self):
        first, second = _monitor("first"), _monitor("second")
        outage = AuthOutage(duration=20)
        outage.arm("n1:1")
        with bound_to(first):
            assert outage.rejects("n1:1")
            assert not outage.rejects("n2:1")
        # The first client's outage is long over when the second connects
        outage._started[first] -= 30
        with bound_to(first):
            assert not outage.rejects("n1:1")
        with bound_to(second):
            assert outage.rejects("n1:1")

        assert outage.over(first) and not outage.over(second)
        assert outage.rejections(first) == 1 and outage.rejections(second) == 1
        assert outage.callers(second) == {"outside redis-py": 1}

    def test_expected_errors_and_dials_follow_the_bound_client(self):
        first, second = _monitor("first"), _monitor("second")
        outage = AuthOutage(duration=20)
        outage.arm("n1:1")
        outage._started[first] = 100.0
        error = CommandError(110.0, "AuthenticationError", "", "GET", True, False)
        dial = _Event(110.0, "dial", "n1:1")

        with bound_to(first):
            assert outage.expected(error) and outage.expected_dial(dial)
        with bound_to(second):
            assert not outage.expected(error) and not outage.expected_dial(dial)


class TestAuthOutageValidation:
    @staticmethod
    def _outage(monitor: ClientMonitor) -> AuthOutage:
        outage = AuthOutage(duration=20)
        outage.arm("n1:1")
        outage._started[monitor] = 100.0
        outage._rejections[monitor] = 3
        outage._callers[monitor]["round:12 > cluster.execute_command"] = 3
        return outage

    @staticmethod
    def _stats(*error_times: float) -> WorkloadStats:
        stats = WorkloadStats()
        stats.errors = [
            CommandError(at, "AuthenticationError", "", "GET", True, False)
            for at in error_times
        ]
        return stats

    def test_handshakes_of_refreshes_are_not_counted(self):
        monitor = _monitor()
        monitor._events += [
            _Event(101.0, "connect_failure", "n1:1", "AuthenticationError"),
            _Event(102.0, "connect_failure", "n1:1", "AuthenticationError", True),
        ]

        validate_auth_outage(monitor, self._stats(101.0), self._outage(monitor), 200.0)

    def test_failed_dials_are_not_rejected_handshakes(self):
        monitor = _monitor()
        monitor._events += [
            _Event(101.0, "connect_failure", "n1:1", "AuthenticationError"),
            _Event(102.0, "connect_failure", "n1:1", "ConnectionError"),
        ]

        validate_auth_outage(monitor, self._stats(101.0), self._outage(monitor), 200.0)

    def test_more_rejected_handshakes_than_failed_commands_fail(self):
        monitor = _monitor()
        monitor._events += [
            _Event(101.0, "connect_failure", "n1:1", "AuthenticationError"),
            _Event(102.0, "connect_failure", "n1:1", "AuthenticationError"),
        ]

        with pytest.raises(AssertionError, match="Rejected handshakes by caller"):
            validate_auth_outage(
                monitor, self._stats(101.0), self._outage(monitor), 200.0
            )

    def test_a_client_that_never_reached_the_node_fails(self):
        monitor = _monitor()
        outage = AuthOutage()
        outage.arm("n1:1")

        with pytest.raises(AssertionError, match="never started for it"):
            validate_auth_outage(monitor, self._stats(), outage, 200.0)


class TestMonitor:
    def test_events_a_refresh_records_are_marked(self):
        monitor = _monitor()
        with bound_to(monitor):
            record("connect_failure", "n1:1")
            with refreshing():
                record("connect_failure", "n1:1")

        assert [e.in_refresh for e in monitor.events("connect_failure")] == [
            False,
            True,
        ]
        assert all(e.at <= _now() for e in monitor.events("connect_failure"))


class TestNotificationsDisabled:
    def test_a_client_without_notifications_passes(self):
        client = SimpleNamespace(
            maint_notifications_config=SimpleNamespace(enabled=False),
            _oss_cluster_maint_notifications_handler=None,
        )
        validate_notifications_disabled(client)

    def test_a_client_with_notifications_fails(self):
        client = SimpleNamespace(
            maint_notifications_config=SimpleNamespace(enabled=True),
            _oss_cluster_maint_notifications_handler=object(),
        )
        with pytest.raises(AssertionError, match="notifications enabled"):
            validate_notifications_disabled(client)
