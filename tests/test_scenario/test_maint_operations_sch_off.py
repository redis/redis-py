"""Cluster client behavior during maintenance operations, with maintenance
notifications (Smart Client Handoffs) disabled.

The client gets no maintenance notifications, so it learns about every topology
change only from MOVED replies and connection errors, as a client of a deployment
without SCH does. Every scenario follows the same pattern:

- The app, a set of background workers, runs its workloads across all shards for
  the whole scenario, the way a production application does. Every scenario runs
  all the workloads at the same time (SCENARIO_WORKLOADS, by default every one of
  WORKLOADS): regular data operations (simple), the drill's application command
  mix (mixed), and blocking commands (blocking), against a plain TCP and a TLS
  database. Each workload has a client instance of its own, which its workers
  share, which serves the whole scenario and which must end it in a good state.
- The main thread runs the scenario's maintenance operations through the fault
  injector, one after another, and records each on the timeline.
- Each client has a monitor of its own, which collects what the client does while
  the app runs: every new connection, failed connect and topology refresh, and,
  sampled every 0.1 seconds, the client's primaries and its connections per node.

After a steady window with no operation, the shared validations check four things
for each client, and report the failures of all the clients together:

- No connection storm. New connections and topology refreshes stay bounded, the
  nodes that keep their shards keep their connections, and the client stops
  connecting once the topology has settled.
- No unexpected errors. Errors that report a healthy cluster as down
  (RedisClusterUnreachableError), handshake failures (AuthenticationError), a close
  timeout of a replaced socket, and any non-Redis error fail the test anywhere. A
  connection or timeout error is allowed only while an operation is in progress,
  and only in a small share of the commands. Counters show whether a write ran
  twice.
- Recovery. The client routes to the new topology within a bounded time, and the
  throughput comes back.
- A good final state. The client maps every slot, holds connections to every
  primary, every primary answers, and close() leaves no socket open.

Each scenario adds its own checks on top. The scenarios:

- Node maintenance and failback. The master shards of one node move to the other
  nodes, and after a hold they move back. This replays a node maintenance drill
  against Redis Enterprise, run through the fault injector's node_maintenance
  action.
- A single operation (SINGLE_OPERATIONS). The operation runs once and is not
  undone, and the app keeps running for OPERATE_SECONDS on the topology it left.
  Today the only operation is maintenance_mode: a node goes into maintenance and
  stays there until the cleanup takes it out, after the database is deleted.
- Node maintenance and failback with AUTH rejected by the returning node. The same
  operations, but for AUTH_OUTAGE_SECONDS after the returning node first accepts a
  connection from a client, every handshake of that client to it fails with
  AuthenticationError, as the drill's returning node rejected the database's
  credentials. Each client gets the whole outage, from its own first connection.
  The rejection comes from a test hook on the handshake, not from the server.
"""

import logging
import time
from typing import Any

import pytest

from tests.test_scenario.maint_operations_helpers import (
    FAILBACK_TEST_TIMEOUT,
    HOLD_SECONDS,
    NODE_MAINTENANCE_DB_CONFIGS,
    OPERATE_SECONDS,
    SINGLE_OPERATION_TEST_TIMEOUT,
    SINGLE_OPERATIONS,
    ClusterMaintScenarioBase,
    WorkloadRun,
    Expectation,
    node_removed_expectation,
    reconnects_settled,
    recovery_status,
    validate_auth_outage,
    validate_failback_topology,
    validate_joining_node,
)
from tests.test_scenario.scenario_monitor import (
    AuthOutage,
    _connections_to,
    _primaries,
    _slot_owners,
    inject_auth_outage,
)


class TestClusterNodeMaintenanceFailbackSchOff(ClusterMaintScenarioBase):
    """Node maintenance and failback against a cluster client with SCH disabled."""

    def _run_failback(
        self,
        monkeypatch,
        db_config: dict[str, Any],
        dynamic_startup_nodes: bool,
        outage: AuthOutage | None = None,
    ):
        """Run the maintenance and the failback, and validate both.

        With outage, the returning node rejects AUTH for a while after it first
        accepts a connection, for each client from its own first connection.
        """
        if outage is not None:
            inject_auth_outage(monkeypatch, outage)
        runs = self._start_app(monkeypatch, db_config, dynamic_startup_nodes)
        timeline = self._timeline
        before = self._common_primaries()

        with timeline.operation("maintenance") as enter:
            node_id = self._node_maintenance("enter")["node_id"]

        time.sleep(HOLD_SECONDS)
        removed: set[str] = set()
        for run in runs:
            during = _primaries(run.client)
            dropped = before - during
            logging.info(
                f"[{run.name}] During maintenance: primaries {sorted(during)}, "
                f"removed {dropped}"
            )
            assert len(dropped) == 1, (
                f"[{run.name}] Expected the client to drop exactly the evacuated "
                f"node's endpoint, before={sorted(before)}, during={sorted(during)}"
            )
            removed |= dropped
        assert len(removed) == 1, (
            f"The clients dropped different nodes: {sorted(removed)}"
        )
        returning = removed.pop()

        if outage is not None:
            outage.arm(returning)
        with timeline.operation("failback") as failback:
            self._node_maintenance("exit", node_id)
        # The AUTH outage of each client starts with its own first connection to
        # the node, so each client recovers after its own outage. It has
        # recovered once its workers stopped reconnecting to the node, so the
        # steady window does not see the reconnects.
        monitors = {id(run.client): run.monitor for run in runs}
        failback.settled = self._wait_for_recovery(
            lambda c: (
                (outage is None or outage.over(monitors[id(c)]))
                and returning in _slot_owners(c)
                and _connections_to(c, returning) > 0
                and reconnects_settled(monitors[id(c)], returning)
            ),
            lambda run: recovery_status(
                run.client, run.app.stats, run.baseline, returning
            ),
        )

        def validate_run(run: WorkloadRun):
            final = run.monitor.snapshots[-1].primaries
            assert final == before, (
                f"The client ends with primaries {sorted(final)}, expected the ones "
                f"before the maintenance {sorted(before)}"
            )
            validate_failback_topology(run.monitor, timeline, enter, returning)
            if outage is None:
                validate_joining_node(
                    run.monitor, failback.started, timeline.steady_done, returning
                )
            else:
                validate_auth_outage(
                    run.monitor, run.app.stats, outage, timeline.steady_done
                )

        if outage is None:
            self._finish(validate_run=validate_run)
        else:
            self._finish(outage.expected, validate_run, outage.expected_dial)

    @pytest.mark.timeout(FAILBACK_TEST_TIMEOUT)
    @pytest.mark.parametrize("dynamic_startup_nodes", [False, True])
    @pytest.mark.parametrize("db_config", NODE_MAINTENANCE_DB_CONFIGS)
    def test_failback_without_connection_storm(
        self,
        monkeypatch,
        db_config: dict[str, Any],
        dynamic_startup_nodes: bool,
    ):
        """A node maintenance and its failback under load cause no connection storm.

        dynamic_startup_nodes=False is the drill's setup: the seed endpoint is the
        only startup node, so a refresh has to fall back to the nodes of the current
        topology when the seed and the returning node do not answer.
        """
        self._run_failback(monkeypatch, db_config, dynamic_startup_nodes)

    @pytest.mark.timeout(FAILBACK_TEST_TIMEOUT)
    @pytest.mark.parametrize("dynamic_startup_nodes", [False, True])
    @pytest.mark.parametrize("db_config", NODE_MAINTENANCE_DB_CONFIGS)
    def test_failback_with_auth_errors_on_returning_node(
        self,
        monkeypatch,
        db_config: dict[str, Any],
        dynamic_startup_nodes: bool,
    ):
        """A returning node that rejects AUTH for a while causes no connection storm.

        The commands routed to the returning node fail with AuthenticationError
        while it rejects AUTH. The client must not report the healthy cluster as
        down, must keep its connections to the other nodes, must not redial the
        returning node in a loop, and must recover once AUTH works again.
        """
        self._run_failback(
            monkeypatch, db_config, dynamic_startup_nodes, outage=AuthOutage()
        )


class TestClusterSingleOperationSchOff(ClusterMaintScenarioBase):
    """One maintenance operation, then a while on the topology it left, SCH disabled.

    Every operation is an _operate_<name> method that triggers it through the fault
    injector and returns the Expectation the client must meet. The operation is not
    undone: a node left in maintenance is taken out of it in the cleanup, after the
    database is deleted.
    """

    def _operate_maintenance_mode(self, before: set[str]) -> Expectation:
        # The node stays in maintenance; _nodes_in_maintenance hands it to the
        # cleanup.
        self._node_maintenance("enter")
        return node_removed_expectation(before)

    @pytest.mark.timeout(SINGLE_OPERATION_TEST_TIMEOUT)
    @pytest.mark.parametrize("dynamic_startup_nodes", [False, True])
    @pytest.mark.parametrize("db_config", NODE_MAINTENANCE_DB_CONFIGS)
    @pytest.mark.parametrize("operation", SINGLE_OPERATIONS)
    def test_operation_without_connection_storm(
        self,
        monkeypatch,
        operation: str,
        db_config: dict[str, Any],
        dynamic_startup_nodes: bool,
    ):
        """One operation under load causes no connection storm, and the client
        keeps serving on the topology the operation left."""
        runs = self._start_app(monkeypatch, db_config, dynamic_startup_nodes)
        timeline = self._timeline
        before = self._common_primaries()

        with timeline.operation(operation) as op:
            expectation = getattr(self, f"_operate_{operation}")(before)
        op.settled = self._wait_for_recovery(
            expectation.reached, lambda run: expectation.status(run.client)
        )
        for run in runs:
            logging.info(
                f"[{run.name}] After {operation}: {expectation.status(run.client)}"
            )
        logging.info(f"Operating for {OPERATE_SECONDS}s")
        time.sleep(OPERATE_SECONDS)

        self._finish(
            validate_run=lambda run: expectation.validate(run.monitor, timeline)
        )
