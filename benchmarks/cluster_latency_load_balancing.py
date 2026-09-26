"""Compare round-robin and latency-based Redis Cluster read routing.

The benchmark never adds delay inside the client. Use ``--degrade-command`` to
pause a Redis replica or add network latency, and ``--recover-command`` to undo
that external change.
"""

import argparse
import json
import shlex
import subprocess
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from redis.cluster import (
    REPLICA,
    ClusterNode,
    LoadBalancingStrategy,
    RedisCluster,
)
from redis.commands import READ_COMMANDS
from redis.exceptions import RedisError
from redis.typing import EncodableT


class TrackingRedisCluster(RedisCluster):
    """Record the initially routed node for each read attempt."""

    def __init__(self, *args, **kwargs):
        self._selection_lock = threading.Lock()
        self._selections: list[tuple[float, str]] = []
        super().__init__(*args, **kwargs)

    def _execute_command(self, target_node: ClusterNode, *args: EncodableT, **kwargs):
        if args[0] in READ_COMMANDS:
            with self._selection_lock:
                self._selections.append((time.monotonic(), target_node.name))
        return super()._execute_command(target_node, *args, **kwargs)

    def drain_selections(self) -> list[tuple[float, str]]:
        with self._selection_lock:
            selections = self._selections
            self._selections = []
        return selections


def _percentile(sorted_samples: list[float], quantile: float) -> float:
    if not sorted_samples:
        return 0.0
    position = (len(sorted_samples) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(sorted_samples) - 1)
    fraction = position - lower
    return (
        sorted_samples[lower]
        + (sorted_samples[upper] - sorted_samples[lower]) * fraction
    )


def summarize(
    latencies: list[float], selected_nodes: list[str], errors: int
) -> dict[str, object]:
    """Return deterministic latency and routing statistics."""
    sorted_latencies = sorted(latencies)
    node_counts = Counter(selected_nodes)
    selection_count = len(selected_nodes)
    return {
        "attempts": selection_count,
        "successful_requests": len(latencies),
        "errors": errors,
        "p50_ms": round(_percentile(sorted_latencies, 0.50) * 1000, 3),
        "p95_ms": round(_percentile(sorted_latencies, 0.95) * 1000, 3),
        "p99_ms": round(_percentile(sorted_latencies, 0.99) * 1000, 3),
        "node_share": {
            node: round(count / selection_count, 4)
            for node, count in sorted(node_counts.items())
        }
        if selection_count
        else {},
    }


def run_phase(
    clients: dict[str, TrackingRedisCluster],
    key: str,
    duration: float,
    workers_per_strategy: int,
) -> tuple[dict[str, dict[str, object]], dict[str, list[tuple[float, str]]]]:
    for client in clients.values():
        client.drain_selections()

    deadline = time.monotonic() + duration

    def worker(client: TrackingRedisCluster) -> tuple[list[float], int]:
        latencies = []
        errors = 0
        while time.monotonic() < deadline:
            started_at = time.monotonic()
            try:
                client.get(key)
            except RedisError:
                errors += 1
            else:
                latencies.append(time.monotonic() - started_at)
        return latencies, errors

    outcomes: dict[str, list] = {name: [] for name in clients}
    with ThreadPoolExecutor(max_workers=workers_per_strategy * len(clients)) as pool:
        futures = [
            (name, pool.submit(worker, client))
            for name, client in clients.items()
            for _ in range(workers_per_strategy)
        ]
        for name, future in futures:
            outcomes[name].append(future.result())

    selections = {name: client.drain_selections() for name, client in clients.items()}
    summaries = {}
    for name, worker_outcomes in outcomes.items():
        latencies = [
            latency
            for worker_latencies, _ in worker_outcomes
            for latency in worker_latencies
        ]
        errors = sum(worker_errors for _, worker_errors in worker_outcomes)
        summaries[name] = summarize(
            latencies,
            [node for _, node in selections[name]],
            errors,
        )
    return summaries, selections


def apply_external_change(command: str | None, prompt: str) -> None:
    if command:
        subprocess.run(shlex.split(command), check=True)
    else:
        input(f"{prompt}\nPress Enter when complete: ")


def wait_for_replication(
    client: TrackingRedisCluster,
    nodes: list[ClusterNode],
    key: str,
    value: str,
    timeout: float = 10.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if all(node.redis_connection.get(key) == value.encode() for node in nodes):
                return
        except RedisError:
            pass
        time.sleep(0.05)
    raise RuntimeError("test key did not replicate to every eligible node")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="redis://localhost:16379/0")
    parser.add_argument("--key", default="{latency-benchmark}:key")
    parser.add_argument("--degraded-node", required=True, help="Replica as host:port")
    parser.add_argument("--degrade-command")
    parser.add_argument("--recover-command")
    parser.add_argument("--warmup-seconds", type=float, default=5.0)
    parser.add_argument("--phase-seconds", type=float, default=15.0)
    parser.add_argument("--recovery-seconds", type=float, default=35.0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--socket-timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    clients = {
        "round_robin": TrackingRedisCluster.from_url(
            args.url,
            load_balancing_strategy=LoadBalancingStrategy.ROUND_ROBIN,
            socket_timeout=args.socket_timeout,
        ),
        "latency_based": TrackingRedisCluster.from_url(
            args.url,
            load_balancing_strategy=LoadBalancingStrategy.LATENCY_BASED,
            socket_timeout=args.socket_timeout,
        ),
    }

    try:
        latency_client = clients["latency_based"]
        slot = latency_client.determine_slot("GET", args.key)
        nodes = latency_client.nodes_manager.slots_cache[slot]
        degraded_node = next(
            (node for node in nodes if node.name == args.degraded_node), None
        )
        if degraded_node is None or degraded_node.server_type != REPLICA:
            eligible = ", ".join(f"{node.name} ({node.server_type})" for node in nodes)
            raise ValueError(
                f"--degraded-node must be a replica for {args.key}; eligible: {eligible}"
            )

        value = "redis-py-latency-benchmark"
        latency_client.set(args.key, value)
        wait_for_replication(latency_client, nodes, args.key, value)

        run_phase(clients, args.key, args.warmup_seconds, args.workers)
        healthy, _ = run_phase(clients, args.key, args.phase_seconds, args.workers)

        apply_external_change(
            args.degrade_command,
            f"Externally degrade replica {args.degraded_node}",
        )
        try:
            degraded, _ = run_phase(clients, args.key, args.phase_seconds, args.workers)
        finally:
            apply_external_change(
                args.recover_command,
                f"Restore replica {args.degraded_node}",
            )

        recovery_started_at = time.monotonic()
        recovered, recovery_selections = run_phase(
            clients, args.key, args.recovery_seconds, args.workers
        )
        reentry_times = [
            selected_at - recovery_started_at
            for selected_at, node_name in recovery_selections["latency_based"]
            if node_name == args.degraded_node
        ]
        recovered["latency_based"]["degraded_node_reentry_seconds"] = (
            round(min(reentry_times), 3) if reentry_times else None
        )

        print(
            json.dumps(
                {
                    "degraded_node": args.degraded_node,
                    "healthy": healthy,
                    "degraded": degraded,
                    "recovered": recovered,
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        for client in clients.values():
            client.close()


if __name__ == "__main__":
    main()
