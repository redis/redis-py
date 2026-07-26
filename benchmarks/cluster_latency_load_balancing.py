"""Compare round-robin and latency-aware reads on a Redis cluster.

The benchmark injects delay into one replica's client-side command path. This
keeps the setup small and repeatable while exercising the same async
``RedisCluster`` selection and measurement paths used in production.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import time
from collections import Counter
from dataclasses import dataclass, field

from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.cluster import LoadBalancingStrategy


@dataclass
class Measurements:
    counts: Counter[str] = field(default_factory=Counter)
    latencies: list[float] = field(default_factory=list)

    @property
    def p99_ms(self) -> float:
        if not self.latencies:
            return 0.0
        ordered = sorted(self.latencies)
        index = min(len(ordered) - 1, math.ceil(len(ordered) * 0.99) - 1)
        return ordered[index] * 1000


async def run_mode(
    host: str,
    port: int,
    key: str,
    requests: int,
    concurrency: int,
    delay_ms: float,
    strategy: LoadBalancingStrategy,
    delayed_node_name: str | None,
) -> tuple[str, Measurements, str]:
    client = await RedisCluster(
        host=host,
        port=port,
        load_balancing_strategy=strategy,
    )
    slot_nodes = client.nodes_manager.slots_cache[client.keyslot(key)]
    replicas = slot_nodes[1:]
    if not replicas:
        await client.aclose()
        raise RuntimeError("the selected key slot must have at least one replica")

    delayed_node = next(
        (node for node in replicas if node.name == delayed_node_name),
        replicas[-1] if delayed_node_name is None else None,
    )
    if delayed_node is None:
        await client.aclose()
        raise ValueError(
            f"node {delayed_node_name!r} is not a replica for key slot "
            f"{client.keyslot(key)}"
        )

    measurements = Measurements()
    original_execute_command = ClusterNode.execute_command

    async def delayed_execute_command(node, *args, **kwargs):
        started = time.perf_counter()
        if args and args[0] == "GET":
            measurements.counts[node.name] += 1
            if node.name == delayed_node.name:
                await asyncio.sleep(delay_ms / 1000)
        try:
            return await original_execute_command(node, *args, **kwargs)
        finally:
            if args and args[0] == "GET":
                measurements.latencies.append(time.perf_counter() - started)

    ClusterNode.execute_command = delayed_execute_command
    try:
        requests_per_worker, remainder = divmod(requests, concurrency)

        async def worker(worker_index: int) -> None:
            worker_requests = requests_per_worker + (worker_index < remainder)
            for _ in range(worker_requests):
                await client.get(key)

        await asyncio.gather(*(worker(i) for i in range(concurrency)))
    finally:
        ClusterNode.execute_command = original_execute_command
        await client.aclose()

    return strategy.value, measurements, delayed_node.name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--key", default="{latency-benchmark}:key")
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--delay-ms", type=float, default=10.0)
    parser.add_argument("--delayed-node")
    return parser.parse_args()


async def main(args: argparse.Namespace) -> None:
    if args.requests < args.concurrency:
        raise ValueError("--requests must be at least --concurrency")

    for strategy in (
        LoadBalancingStrategy.ROUND_ROBIN,
        LoadBalancingStrategy.LATENCY_BASED,
    ):
        name, measurements, delayed_node = await run_mode(
            host=args.host,
            port=args.port,
            key=args.key,
            requests=args.requests,
            concurrency=args.concurrency,
            delay_ms=args.delay_ms,
            strategy=strategy,
            delayed_node_name=args.delayed_node,
        )
        total = sum(measurements.counts.values())
        delayed_share = measurements.counts[delayed_node] / total * 100
        print(
            f"strategy={name:<14} delayed_node={delayed_node:<24} "
            f"delayed_share={delayed_share:6.2f}% p99={measurements.p99_ms:8.3f}ms"
        )


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
