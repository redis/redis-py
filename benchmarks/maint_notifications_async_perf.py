"""Performance / event-loop analysis harness for async cluster maintenance notifications.

Two flows, selectable via --flow:

  throughput      N concurrent tasks run SET/GET against the cluster for a fixed
                  duration, optionally triggering a slot-migration mid-run. Reports
                  throughput, per-command latency percentiles, errors, and - most
                  importantly - event-loop stalls (the symptom we are chasing).

  notifications   Acquire one connection per node, trigger a migration effect, and
                  watch the SMIGRATING/SMIGRATED handling + per-connection state
                  transitions. Useful for profiling the notification-handling path.

Both flows run an event-loop-lag monitor in the background so we can see exactly
when and for how long the loop freezes.

It needs a Redis Enterprise cluster with a fault injector, the same setup as the
scenario tests: the fault-injector URL, the database it creates and the effect it
triggers are command-line arguments. The script reuses the existing
scenario-test helpers rather than reimplementing them.

Run from the repo root, e.g.:

    python -m benchmarks.maint_notifications_async_perf --flow throughput --maint on \\
        --fault-injector-url http://127.0.0.1:20324
    python -m benchmarks.maint_notifications_async_perf --flow throughput --maint off
    python -m benchmarks.maint_notifications_async_perf --flow notifications

The fault-injector URL defaults to the FAULT_INJECTION_API_URL environment
variable, the one the scenario tests read.
"""

import argparse
import asyncio
import logging
import os
import time

from redis.maint_notifications import MaintenanceState

# --- Reused scenario-test helpers (do not reimplement) -----------------------
from tests.test_asyncio.test_scenario.async_fault_injector_client import (
    AsyncREFaultInjector,
)
from tests.test_asyncio.test_scenario.conftest import (
    get_async_cluster_client_maint_notifications,
)
from tests.test_scenario.fault_injector_client import SlotMigrateEffects
from tests.test_scenario.maint_notifications_helpers import KeyGenerationHelpers

# =============================================================================
# Environment defaults, each overridable from the command line.
# =============================================================================

# Fault-injector REST endpoint, read the same way as the scenario tests.
DEFAULT_FAULT_INJECTOR_URL = os.getenv(
    "FAULT_INJECTION_API_URL", "http://127.0.0.1:20324"
)

# Database to create for the run. Mirrors the slot-shuffle / external-ip config
# used by the scenario tests. --db-name, --db-port and --shards override the
# matching keys.
DEFAULT_BDB_CONFIG: dict = {
    "memory_size": 134217728,
    "eviction_policy": "volatile-lru",
    "sharding": True,
    "oss_cluster": True,
    "proxy_policy": "all-master-shards",
    "shard_key_regex": [
        {"regex": ".*\\{(?<tag>.*)\\}.*"},
        {"regex": "(?<tag>.*)"},
    ],
    "name": "perf-maint-async",
    "port": 13580,
    "shards_count": 10,
    "shards_placement": "sparse",
    "replication": False,
    "oss_cluster_api_preferred_ip_type": "external",
    "oss_cluster_api_preferred_endpoint_type": "ip",
}

# Default migration effect/trigger to fire mid-run.
DEFAULT_EFFECT = SlotMigrateEffects.SLOT_SHUFFLE
DEFAULT_TRIGGER = "migrate"
# How long to wait for the fault injector's operation. It blocks on a
# slot-shuffle reshard for up to 300s, the SLOT_SHUFFLE_TIMEOUT of the
# scenario tests; a shorter wait fails slow but successful migrations.
DEFAULT_OP_TIMEOUT = 300

# Client connection knobs.
DEFAULT_SOCKET_TIMEOUT = 2.0
DEFAULT_SOCKET_CONNECT_TIMEOUT = 2.0

logging.basicConfig(
    level=logging.INFO,
    # datefmt goes to time.strftime, which has no %f; msecs adds the subsecond
    format="%(asctime)s.%(msecs)03d %(levelname)s %(threadName)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("maint_perf")


def _apply_redis_log_level(level: int):
    """Set the redis maint/cluster logger levels. Running these at DEBUG mimics
    the scenario-test invocation (--log-cli-level=DEBUG) and lets us measure
    whether synchronous debug logging during the notification flood stalls the
    single event loop."""
    logging.getLogger("redis.asyncio.maint_notifications").setLevel(level)
    logging.getLogger("redis.asyncio.cluster").setLevel(level)


# =============================================================================
# Event-loop lag monitor - the core diagnostic.
# =============================================================================


class LoopLagMonitor:
    """Background task that measures how late the loop services a fixed-interval
    sleep. A large lag means something blocked the single event loop."""

    def __init__(self, interval: float = 0.05, stall_threshold: float = 0.25):
        self.interval = interval
        self.stall_threshold = stall_threshold
        self.samples: list[float] = []
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def _run(self):
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            t0 = loop.time()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass
            lag = loop.time() - t0 - self.interval
            if lag < 0:
                lag = 0.0
            self.samples.append(lag)
            if lag >= self.stall_threshold:
                logger.warning(f">>> EVENT-LOOP STALL: {lag:.3f}s")

    def start(self):
        self._task = asyncio.get_running_loop().create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    def report(self):
        if not self.samples:
            logger.info("Loop-lag: no samples")
            return
        worst = max(self.samples)
        over = [s for s in self.samples if s >= self.stall_threshold]
        logger.info(
            f"Loop-lag: samples={len(self.samples)} worst={worst:.3f}s "
            f"stalls(>={self.stall_threshold}s)={len(over)} "
            f"total_stalled={sum(over):.3f}s"
        )


# =============================================================================
# Shared setup / teardown.
# =============================================================================


async def setup_cluster(
    fault_injector: AsyncREFaultInjector,
    bdb_config: dict,
    maint_enabled: bool,
    socket_timeout: float,
    socket_connect_timeout: float,
) -> tuple:
    """Create the DB and return (client, endpoints_config)."""
    # Delete any leftover DB with the same name.
    try:
        existing = await fault_injector.find_database_id_by_name(bdb_config["name"])
        if existing:
            logger.info(f"Deleting leftover DB id={existing}")
            await fault_injector.delete_database(existing)
    except Exception as e:
        logger.warning(f"Pre-cleanup skipped: {e}")

    logger.info(f"Creating DB '{bdb_config['name']}' ...")
    endpoints_config = await fault_injector.create_database(bdb_config)
    logger.info(f"DB created: bdb_id={endpoints_config.get('bdb_id')}")

    client = get_async_cluster_client_maint_notifications(
        endpoints_config=endpoints_config,
        enable_maintenance_notifications=maint_enabled,
        disable_retries=True,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
    )
    try:
        await client.initialize()
    except BaseException:
        # the caller never gets the client or the DB to clean up
        await teardown(client, fault_injector, endpoints_config, keep_db=False)
        raise
    logger.info(
        f"Cluster initialized; maint_notifications={'on' if maint_enabled else 'off'}; "
        f"nodes={[n.name for n in client.get_nodes()]}"
    )
    return client, endpoints_config


async def teardown(client, fault_injector, endpoints_config, keep_db: bool):
    try:
        if client is not None:
            await client.aclose()
    except Exception as e:
        logger.warning(f"Client close error: {e}")
    if not keep_db and endpoints_config:
        try:
            await fault_injector.delete_database(endpoints_config["bdb_id"])
            logger.info("DB deleted")
        except Exception as e:
            logger.warning(f"DB delete error: {e}")


async def trigger_migration(
    fault_injector: AsyncREFaultInjector,
    endpoints_config: dict,
    effect: SlotMigrateEffects,
    trigger: str,
    timeout: int,
):
    logger.info(f"Triggering effect={effect.value} trigger={trigger} ...")
    action_id = await fault_injector.trigger_effect(
        endpoint_config=endpoints_config,
        effect_name=effect,
        trigger_name=trigger,
    )
    result = await fault_injector.get_operation_result(action_id, timeout=timeout)
    logger.info(f"Effect completed: {result.get('status')}")


# =============================================================================
# Flow 1: throughput under concurrent load (optionally with a migration).
# =============================================================================


async def run_throughput_flow(args):
    fault_injector = AsyncREFaultInjector(args.fault_injector_url)
    client = None
    endpoints_config = None
    monitor = LoopLagMonitor(stall_threshold=args.stall_threshold)
    try:
        client, endpoints_config = await setup_cluster(
            fault_injector,
            args.bdb_config,
            maint_enabled=args.maint == "on",
            socket_timeout=args.socket_timeout,
            socket_connect_timeout=args.connect_timeout,
        )

        keys = KeyGenerationHelpers.generate_keys_for_all_shards(
            args.bdb_config["shards_count"], prefix="perf"
        )

        # Warm up the per-node pools sequentially so the concurrent phase does not
        # cold-connect every node at once.
        if args.warmup:
            logger.info("Warming up pools (sequential pass)...")
            for key in keys:
                await client.set(key, "v")
                await client.get(key)

        # Pre-generate each worker's key set BEFORE starting the concurrent load.
        # generate_keys_for_all_shards brute-forces CRC16 (~16k iterations/key) and
        # is fully synchronous; doing it inside the workers blocks the event loop
        # during the connection-establishment burst. Compute it up front instead.
        if args.keys_per_worker:
            t0 = time.perf_counter()
            worker_keys_by_name = {
                f"w{i}": KeyGenerationHelpers.generate_keys_for_all_shards(
                    args.bdb_config["shards_count"], prefix=f"w{i}_perf"
                )
                for i in range(args.tasks)
            }
            logger.info(
                f"Pre-generated per-worker keys in {time.perf_counter() - t0:.2f}s"
            )
        else:
            worker_keys_by_name = {f"w{i}": keys for i in range(args.tasks)}

        monitor.start()

        stats = {"ok": 0, "err": 0}
        latencies: list[float] = []
        errors: list[str] = []
        stop = asyncio.Event()

        async def timed(name: str, expected, command, *command_args):
            # each command is timed and counted on its own, so a SET that
            # succeeds before its GET fails still counts
            t0 = time.perf_counter()
            try:
                reply = await command(*command_args)
                elapsed = time.perf_counter() - t0
                # every SET writes "v", so a GET that returns anything else is
                # a broken read (misrouted or stale), not a successful one
                if reply != expected:
                    raise ValueError(f"unexpected reply {reply!r:.80}")
                latencies.append(elapsed)
                stats["ok"] += 1
            except Exception as e:
                stats["err"] += 1
                msg = f"{type(e).__name__}: {e}"
                errors.append(msg)
                logger.error(f"[{name}] command failed: {msg}")

        async def worker(name: str):
            worker_keys = worker_keys_by_name[name]
            while not stop.is_set():
                for key in worker_keys:
                    if stop.is_set():
                        break
                    await timed(name, True, client.set, key, "v")
                    await timed(name, b"v", client.get, key)
                await asyncio.sleep(0.001)

        workers = [
            asyncio.create_task(worker(f"w{i}"), name=f"w{i}")
            for i in range(args.tasks)
        ]

        # Let the load stabilize, then optionally trigger a migration mid-run.
        await asyncio.sleep(args.ramp)
        if args.trigger_migration:
            await trigger_migration(
                fault_injector,
                endpoints_config,
                args.effect,
                args.trigger,
                args.op_timeout,
            )

        await asyncio.sleep(args.duration)
        stop.set()
        await asyncio.gather(*workers, return_exceptions=True)

        await monitor.stop()

        # Report.
        latencies.sort()
        logger.info("=" * 60)
        logger.info(f"THROUGHPUT FLOW (maint={args.maint}, tasks={args.tasks})")
        logger.info(f"  ok_commands={stats['ok']} errors={stats['err']}")
        if latencies:
            p50 = latencies[len(latencies) // 2]
            p99 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))]
            logger.info(
                f"  latency p50={p50 * 1000:.1f}ms p99={p99 * 1000:.1f}ms "
                f"max={latencies[-1] * 1000:.1f}ms"
            )
        monitor.report()
        if errors:
            uniq: dict[str, int] = {}
            for e in errors:
                uniq[e] = uniq.get(e, 0) + 1
            logger.info(f"  error breakdown: {uniq}")
        logger.info("=" * 60)
    finally:
        await teardown(client, fault_injector, endpoints_config, args.keep_db)


# =============================================================================
# Flow 2: notification handling (acquire conns, trigger, observe state).
# =============================================================================


def _node_connections(node) -> list:
    return list(node._connections)


async def run_notifications_flow(args):
    fault_injector = AsyncREFaultInjector(args.fault_injector_url)
    client = None
    endpoints_config = None
    monitor = LoopLagMonitor(stall_threshold=args.stall_threshold)
    try:
        client, endpoints_config = await setup_cluster(
            fault_injector,
            args.bdb_config,
            maint_enabled=True,  # this flow only makes sense with maint on
            socket_timeout=args.socket_timeout,
            socket_connect_timeout=args.connect_timeout,
        )

        # Acquire and connect one connection per node so notifications land on them.
        held = {}
        for node in client.nodes_manager.nodes_cache.values():
            conn = node.acquire_connection()
            await conn.connect()
            held[node] = conn
        logger.info(f"Acquired 1 connection on each of {len(held)} nodes")

        monitor.start()

        # Trigger the migration in the background and poll connection state.
        trigger_task = asyncio.create_task(
            trigger_migration(
                fault_injector,
                endpoints_config,
                args.effect,
                args.trigger,
                args.op_timeout,
            )
        )

        deadline = time.time() + args.duration
        seen_maintenance = set()
        # keep draining until the migration completes, or its last pushes stay
        # buffered and the report shows a stale state
        while time.time() < deadline or not trigger_task.done():
            for node, conn in held.items():
                try:
                    if await conn.can_read():
                        await conn.read_response(push_request=True)
                except Exception as e:
                    logger.error(f"read error on {node.name}: {e}")
                state = conn.maintenance_state
                if (
                    state == MaintenanceState.MAINTENANCE
                    and node.name not in seen_maintenance
                ):
                    seen_maintenance.add(node.name)
                    logger.info(
                        f"[{node.name}] -> MAINTENANCE "
                        f"socket_timeout={conn.socket_timeout} "
                        f"should_reconnect={conn.should_reconnect()}"
                    )
            await asyncio.sleep(0.1)

        await trigger_task
        await monitor.stop()

        logger.info("=" * 60)
        logger.info("NOTIFICATIONS FLOW")
        logger.info(f"  nodes that entered MAINTENANCE: {sorted(seen_maintenance)}")
        for node, conn in held.items():
            logger.info(
                f"  {node.name}: state={conn.maintenance_state} "
                f"socket_timeout={conn.socket_timeout} "
                f"should_reconnect={conn.should_reconnect()}"
            )
        monitor.report()
        logger.info("=" * 60)

        for node, conn in held.items():
            node.release(conn)
    finally:
        await teardown(client, fault_injector, endpoints_config, args.keep_db)


# =============================================================================
# CLI.
# =============================================================================


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--flow", choices=["throughput", "notifications"], required=True)
    p.add_argument(
        "--maint",
        choices=["on", "off"],
        default="on",
        help="Enable maintenance notifications (throughput flow only).",
    )
    p.add_argument("--tasks", type=int, default=10, help="Concurrent workers.")
    p.add_argument("--ramp", type=float, default=3.0, help="Seconds before trigger.")
    p.add_argument("--duration", type=float, default=40.0, help="Seconds of load.")
    p.add_argument(
        "--no-trigger-migration",
        dest="trigger_migration",
        action="store_false",
        help="Run the load without firing a migration (baseline).",
    )
    p.add_argument("--no-warmup", dest="warmup", action="store_false")
    p.add_argument("--socket-timeout", type=float, default=DEFAULT_SOCKET_TIMEOUT)
    p.add_argument(
        "--connect-timeout", type=float, default=DEFAULT_SOCKET_CONNECT_TIMEOUT
    )
    p.add_argument("--stall-threshold", type=float, default=0.25)
    p.add_argument(
        "--asyncio-debug",
        action="store_true",
        help="Enable asyncio debug mode to log slow (blocking) callbacks by name.",
    )
    p.add_argument("--slow-callback-duration", type=float, default=0.1)
    p.add_argument(
        "--keys-per-worker",
        action="store_true",
        help="Give each worker its own distinct key set (pre-generated up front).",
    )
    p.add_argument("--keep-db", action="store_true", help="Do not delete the DB.")
    p.add_argument(
        "--fault-injector-url",
        default=DEFAULT_FAULT_INJECTOR_URL,
        help="Fault-injector REST endpoint (default: $FAULT_INJECTION_API_URL).",
    )
    p.add_argument("--db-name", default=DEFAULT_BDB_CONFIG["name"])
    p.add_argument("--db-port", type=int, default=DEFAULT_BDB_CONFIG["port"])
    p.add_argument("--shards", type=int, default=DEFAULT_BDB_CONFIG["shards_count"])
    p.add_argument(
        "--effect",
        choices=[e.value for e in SlotMigrateEffects],
        default=DEFAULT_EFFECT.value,
        help="Slot-migration effect to trigger.",
    )
    p.add_argument("--trigger", default=DEFAULT_TRIGGER)
    p.add_argument(
        "--op-timeout",
        type=int,
        default=DEFAULT_OP_TIMEOUT,
        help="Seconds to wait for the fault injector's operation.",
    )
    p.add_argument(
        "--redis-log-level",
        choices=["INFO", "DEBUG"],
        default="INFO",
        help="Level for redis maint/cluster loggers. DEBUG mimics the scenario "
        "tests and exercises the synchronous debug-logging path.",
    )
    p.set_defaults(trigger_migration=True, warmup=True)
    args = p.parse_args()
    args.effect = SlotMigrateEffects(args.effect)
    args.bdb_config = {
        **DEFAULT_BDB_CONFIG,
        "name": args.db_name,
        "port": args.db_port,
        "shards_count": args.shards,
    }
    return args


async def main():
    args = parse_args()
    _apply_redis_log_level(getattr(logging, args.redis_log_level))
    if args.asyncio_debug:
        # asyncio debug mode logs any callback/coroutine step that runs longer
        # than slow_callback_duration, naming the offending function - this is
        # how we identify what synchronous work is blocking the event loop.
        loop = asyncio.get_running_loop()
        loop.set_debug(True)
        loop.slow_callback_duration = args.slow_callback_duration
        logging.getLogger("asyncio").setLevel(logging.WARNING)
    start = time.perf_counter()
    try:
        if args.flow == "throughput":
            await run_throughput_flow(args)
        else:
            await run_notifications_flow(args)
    finally:
        logger.info(
            f"Execution time ({args.flow} flow, including DB create/delete): "
            f"{time.perf_counter() - start:.1f}s"
        )


if __name__ == "__main__":
    asyncio.run(main())
