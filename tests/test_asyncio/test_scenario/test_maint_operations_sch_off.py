"""Async cluster client behavior during maintenance operations, with maintenance
notifications (Smart Client Handoffs) disabled.

The async counterpart of tests/test_scenario/test_maint_operations_sch_off.py, which
describes the scenario pattern, the checks and the scenarios. The app's workers are
tasks and the monitor samples from a task on the same event loop; the timeline, the
thresholds and the validations are shared with the sync suite.
"""

import asyncio
import logging
from typing import Any

import pytest

from tests.test_asyncio.test_scenario.async_maint_operations_helpers import (
    AsyncClusterMaintScenarioBase,
    AsyncWorkloadRun,
)
from tests.test_asyncio.test_scenario.async_scenario_monitor import (
    _connections_to,
    inject_auth_outage,
)
from tests.test_scenario.maint_operations_helpers import (
    FAILBACK_TEST_TIMEOUT,
    HOLD_SECONDS,
    NODE_MAINTENANCE_DB_CONFIGS,
    OPERATE_SECONDS,
    SINGLE_OPERATION_TEST_TIMEOUT,
    SINGLE_OPERATIONS,
    Expectation,
    node_removed_expectation,
    reconnects_settled,
    recovery_status,
    throughput_status,
    validate_auth_outage,
    validate_failback_topology,
    validate_joining_node,
)
from tests.test_scenario.scenario_monitor import AuthOutage, _primaries, _slot_owners


class TestAsyncClusterNodeMaintenanceFailbackSchOff(AsyncClusterMaintScenarioBase):
    """Node maintenance and failback against an async cluster client with SCH disabled."""

    async def _run_failback(
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
        runs = await self._start_app(monkeypatch, db_config, dynamic_startup_nodes)
        timeline = self._timeline
        before = self._common_primaries()

        with timeline.operation("maintenance") as enter:
            node_id = (await self._node_maintenance("enter"))["node_id"]

        await asyncio.sleep(HOLD_SECONDS)
        removed: set[str] = set()
        for run in runs:
            during = _primaries(run.client)
            dropped = before - during
            logging.info(
                f"[{run.name}] During maintenance: primaries {sorted(during)}, "
                f"removed {dropped}"
            )
            if len(dropped) != 1:
                # The scenario stops here, before _finish logs what the
                # clients did.
                self._log_summaries()
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
            await self._node_maintenance("exit", node_id)
        # The AUTH outage of each client starts with its own first connection to
        # the node, so each client recovers after its own outage. It has
        # recovered once its workers stopped reconnecting to the node, so the
        # steady window does not see the reconnects.
        monitors = {id(run.client): run.monitor for run in runs}
        failback.settled = await self._wait_for_recovery(
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

        def validate_run(run: AsyncWorkloadRun):
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
            await self._finish(validate_run=validate_run)
        else:
            await self._finish(outage.expected, validate_run, outage.expected_dial)

    @pytest.mark.timeout(FAILBACK_TEST_TIMEOUT)
    @pytest.mark.parametrize("dynamic_startup_nodes", [False, True])
    @pytest.mark.parametrize("db_config", NODE_MAINTENANCE_DB_CONFIGS)
    async def test_failback_without_connection_storm(
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
        await self._run_failback(monkeypatch, db_config, dynamic_startup_nodes)

    @pytest.mark.timeout(FAILBACK_TEST_TIMEOUT)
    @pytest.mark.parametrize("dynamic_startup_nodes", [False, True])
    @pytest.mark.parametrize("db_config", NODE_MAINTENANCE_DB_CONFIGS)
    async def test_failback_with_auth_errors_on_returning_node(
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
        await self._run_failback(
            monkeypatch, db_config, dynamic_startup_nodes, outage=AuthOutage()
        )


class TestAsyncClusterSingleOperationSchOff(AsyncClusterMaintScenarioBase):
    """One maintenance operation, then a while on the topology it left, SCH disabled.

    The async counterpart of TestClusterSingleOperationSchOff in the sync suite.
    """

    async def _operate_maintenance_mode(self, before: set[str]) -> Expectation:
        # The node stays in maintenance; _nodes_in_maintenance hands it to the
        # cleanup.
        await self._node_maintenance("enter")
        return node_removed_expectation(before)

    @pytest.mark.timeout(SINGLE_OPERATION_TEST_TIMEOUT)
    @pytest.mark.parametrize("dynamic_startup_nodes", [False, True])
    @pytest.mark.parametrize("db_config", NODE_MAINTENANCE_DB_CONFIGS)
    @pytest.mark.parametrize("operation", SINGLE_OPERATIONS)
    async def test_operation_without_connection_storm(
        self,
        monkeypatch,
        operation: str,
        db_config: dict[str, Any],
        dynamic_startup_nodes: bool,
    ):
        """One operation under load causes no connection storm, and the client
        keeps serving on the topology the operation left."""
        runs = await self._start_app(monkeypatch, db_config, dynamic_startup_nodes)
        timeline = self._timeline
        before = self._common_primaries()

        with timeline.operation(operation) as op:
            expectation = await getattr(self, f"_operate_{operation}")(before)
        op.settled = await self._wait_for_recovery(
            expectation.reached,
            lambda run: (
                f"{expectation.status(run.client)}, "
                + throughput_status(run.app.stats, run.baseline)
            ),
        )
        for run in runs:
            logging.info(
                f"[{run.name}] After {operation}: {expectation.status(run.client)}"
            )
        logging.info(f"Operating for {OPERATE_SECONDS}s")
        await asyncio.sleep(OPERATE_SECONDS)

        await self._finish(
            validate_run=lambda run: expectation.validate(run.monitor, timeline)
        )
