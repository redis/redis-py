"""The async scenario base class of the maintenance operation tests with
maintenance notifications disabled. The thresholds and the validations are shared
with the sync suite, in tests/test_scenario/maint_operations_helpers.py.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal

import pytest
import pytest_asyncio

from redis.asyncio import RedisCluster
from tests.test_asyncio.test_scenario.async_fault_injector_client import (
    AsyncFaultInjectorClient,
)
from tests.test_asyncio.test_scenario.async_scenario_monitor import (
    AsyncClientMonitor,
    _open_sockets,
    _snapshot,
    instrument,
)
from tests.test_asyncio.test_scenario.async_scenario_workloads import (
    ASYNC_WORKLOAD_CLASSES,
    AsyncScenarioApp,
)
from tests.test_asyncio.test_scenario.conftest import (
    get_async_cluster_client_maint_notifications,
)
from tests.test_asyncio.test_scenario.test_maint_notifications import (
    TestAsyncPushNotificationsBase,
)
from tests.test_scenario.maint_operations_helpers import (
    MIN_RECOVERED_THROUGHPUT_RATIO,
    NODE_MAINTENANCE_ACTIVE_TIMEOUT,
    NODE_MAINTENANCE_OP_TIMEOUT,
    RECOVERY_TIMEOUT,
    SCENARIO_WORKLOADS,
    STEADY_SECONDS,
    WARMUP_EXTRA_SECONDS,
    WARMUP_SECONDS,
    WORKERS,
    log_summary,
    missing_connections,
    validate_connections,
    validate_errors,
    validate_final_state,
    validate_held_connections,
    validate_notifications_disabled,
    validate_process_sockets,
    validate_slot_coverage,
    validate_slots,
)
from tests.test_scenario.scenario_monitor import (
    Timeline,
    _Event,
    _now,
    _primaries,
    bound_to,
    process_sockets_to,
    unattributed_events,
)
from tests.test_scenario.scenario_workloads import CommandError, keys_on_every_primary


@dataclass
class AsyncWorkloadRun:
    """The async counterpart of WorkloadRun in the sync suite."""

    name: str
    client: RedisCluster
    monitor: AsyncClientMonitor
    app: AsyncScenarioApp
    baseline: float = 0.0


async def collect_async_failure(
    failures: list[str], workload: str, check: Callable[[], Awaitable[None]]
):
    """The async counterpart of collect_failure in the sync suite."""
    try:
        await check()
    except AssertionError as e:
        failures.append(f"[{workload}] {e}")


class AsyncClusterMaintScenarioBase(TestAsyncPushNotificationsBase):
    """Runs a scenario: the app in the background, operations in the main task.

    The async counterpart of ClusterMaintScenarioBase in the sync suite.
    """

    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_cleanup(
        self, fault_injector_client_oss_api: AsyncFaultInjectorClient
    ):
        self._fault_injector = fault_injector_client_oss_api
        self._bdb_name = None
        self._endpoint_config = None
        self._runs: list[AsyncWorkloadRun] = []
        self._timeline = Timeline()
        self._db_port = 0
        self._nodes_in_maintenance: set[int] = set()

        yield

        logging.info("Starting cleanup...")
        try:
            for run in self._runs:
                await run.app.stop()
            for run in self._runs:
                await run.monitor.stop()
            for run in self._runs:
                await run.client.aclose()
            self._runs = []
        finally:
            # The cluster is shared with the later tests, so it is restored even
            # when stopping the app or closing a client fails.
            if self._bdb_name:
                await self.delete_prev_db(self._fault_injector, self._bdb_name)
            for node_id in list(self._nodes_in_maintenance):
                # A scenario that ends with a node in maintenance, or fails before
                # its failback, leaves the node evacuated. The database is gone by
                # now, so take the node out of maintenance without moving shards
                # back; later tests get the whole cluster.
                try:
                    await self._node_maintenance("exit", node_id, restore_shards=False)
                except (Exception, pytest.fail.Exception) as e:
                    logging.error(
                        f"Failed to take node {node_id} out of maintenance: {e}"
                    )
            logging.info("Cleanup finished")

    async def _start_app(
        self,
        monkeypatch,
        db_config: dict[str, Any],
        dynamic_startup_nodes: bool,
    ) -> list[AsyncWorkloadRun]:
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
        self._endpoint_config = await self.create_db(self._fault_injector, db_config)
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
            self._runs.append(await self._new_run(name, dynamic_startup_nodes))

        self._timeline = Timeline()
        for run in self._runs:
            run.monitor.start()
            run.app.start()
        await asyncio.sleep(WARMUP_SECONDS)
        await self._wait_for_warm_pools()
        self._timeline.warmed_up = _now()
        for run in self._runs:
            run.baseline = run.app.stats.recent_throughput()
            logging.info(
                f"[{run.name}] Warmed up: primaries "
                f"{sorted(_primaries(run.client))}, {run.baseline:.0f} ops/s"
            )
        self._validate_process_sockets("after the warm-up")
        return self._runs

    async def _wait_for_warm_pools(self):
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
            await asyncio.sleep(1)
        if _now() > started + 1 or any(missing.values()):
            logging.info(
                f"Warm-up extended by {_now() - started:.0f}s; connections the "
                f"workers still lack: {missing}"
            )

    async def _new_run(
        self, workload: str, dynamic_startup_nodes: bool
    ) -> AsyncWorkloadRun:
        monitor = AsyncClientMonitor(lambda: _snapshot(client), workload)
        with bound_to(monitor):
            client = get_async_cluster_client_maint_notifications(
                endpoints_config=self._endpoint_config,
                enable_maintenance_notifications=False,
                dynamic_startup_nodes=dynamic_startup_nodes,
            )
            # The async cluster discovers its topology lazily.
            await client.initialize()
        validate_notifications_disabled(client)
        app = AsyncScenarioApp(
            client,
            ASYNC_WORKLOAD_CLASSES[workload](),
            [
                keys_on_every_primary(client, prefix=f"maint_{workload}_{index}")
                for index in range(WORKERS)
            ],
            monitor,
        )
        return AsyncWorkloadRun(workload, client, monitor, app)

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

    def _log_summaries(self):
        """Log what every client did so far, for a scenario that fails before
        _finish logs it."""
        for run in self._runs:
            log_summary(run.name, run.monitor, run.app.stats, self._timeline)

    async def _wait_for_recovery(
        self,
        condition: Callable[[RedisCluster], bool],
        status: Callable[[AsyncWorkloadRun], str],
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
            await asyncio.sleep(1)
        self._log_summaries()
        pytest.fail(
            f"No recovery within {RECOVERY_TIMEOUT}s after "
            f"{self._timeline.operations[-1].name}: "
            + "; ".join(f"[{run.name}] {status(run)}" for run in pending)
        )

    async def _finish(
        self,
        expected_errors: Callable[[CommandError], bool] | None = None,
        validate_run: Callable[[AsyncWorkloadRun], None] | None = None,
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
        await asyncio.sleep(STEADY_SECONDS)
        timeline.steady_done = _now()
        for run in runs:
            await run.app.stop()
        for run in runs:
            await run.monitor.stop()

        failures: list[str] = []
        for run in runs:
            log_summary(run.name, run.monitor, run.app.stats, timeline)

            async def check(run=run):
                with bound_to(run.monitor):
                    validate_errors(run.app.stats, timeline, expected_errors)
                    await run.app.validate()
                    validate_connections(run.monitor, timeline, expected_dials)
                    validate_held_connections(run.monitor, timeline)
                    validate_slot_coverage(run.monitor, timeline)
                    validate_final_state(run.monitor)
                    validate_slots(run.client)
                    assert await run.client.ping(target_nodes=RedisCluster.PRIMARIES), (
                        "A primary did not answer PING at the end"
                    )
                if validate_run is not None:
                    validate_run(run)

            await collect_async_failure(failures, run.name, check)

        async def check_process_sockets():
            self._validate_process_sockets("at the end")

        await collect_async_failure(failures, "all", check_process_sockets)

        for run in runs:
            with bound_to(run.monitor):
                await run.client.aclose()
            open_after_close = _open_sockets(run.client)
            if open_after_close:
                failures.append(
                    f"[{run.name}] {open_after_close} socket(s) still open after "
                    f"aclose()"
                )
        self._runs = []
        # The transports close their sockets on the next loop iterations.
        process_after_close = process_sockets_to(self._db_port)
        deadline = _now() + 1
        while process_after_close and _now() < deadline:
            await asyncio.sleep(0.05)
            process_after_close = process_sockets_to(self._db_port)
        if process_after_close:
            failures.append(
                f"[all] The process still has {process_after_close} sockets to port "
                f"{self._db_port} after aclose()"
            )
        if unattributed_events:
            failures.append(
                f"[all] Hook events no client was bound for, so no monitor counted "
                f"them: {dict(unattributed_events)}"
            )
        if failures:
            pytest.fail("\n".join(failures))

    async def _track_failed_enter(self, action_id: str):
        """Hand the node of a failed 'enter' to the cleanup.

        An 'enter' that fails may have put its node in maintenance already. The
        fault injector names that node in the output of the failed action.
        """
        try:
            status = await self._fault_injector.get_action_status(action_id)
        except Exception as e:
            logging.error(f"Failed to read the node of the failed maintenance: {e}")
            return
        node_id = (status.get("output") or {}).get("node_id")
        if node_id is not None:
            self._nodes_in_maintenance.add(node_id)

    async def _node_maintenance(
        self,
        mode: Literal["enter", "exit"],
        node_id: int | None = None,
        restore_shards: bool = True,
    ) -> dict:
        action_id = await self._fault_injector.node_maintenance(
            self._endpoint_config,
            mode,
            node_id=node_id,
            active_timeout=NODE_MAINTENANCE_ACTIVE_TIMEOUT,
            restore_shards=restore_shards,
        )
        try:
            result = await self._fault_injector.get_operation_result(
                action_id, timeout=NODE_MAINTENANCE_OP_TIMEOUT
            )
        except pytest.fail.Exception:
            if mode == "enter":
                await self._track_failed_enter(action_id)
            raise
        logging.info(f"node_maintenance {mode} result: {result}")
        output = result.get("output", {})
        if mode == "enter":
            self._nodes_in_maintenance.add(output["node_id"])
        else:
            self._nodes_in_maintenance.discard(node_id)
        return output
