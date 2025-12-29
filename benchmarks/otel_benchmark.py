#!/usr/bin/env python3
"""
OpenTelemetry Benchmark for redis-py.

This module provides benchmarking infrastructure to measure the performance impact
of OpenTelemetry instrumentation on redis-py operations.

Run one scenario at a time:
    python -m benchmarks.otel_benchmark --scenario baseline --baseline-tag v5.2.1
    python -m benchmarks.otel_benchmark --scenario otel_disabled
    python -m benchmarks.otel_benchmark --scenario otel_noop
    python -m benchmarks.otel_benchmark --scenario otel_enabled_http
    python -m benchmarks.otel_benchmark --scenario otel_enabled_grpc
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, Any


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""
    scenario: str
    duration_seconds: float
    total_operations: int
    operations_per_second: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    errors: int = 0
    first_error: Optional[str] = None
    metadata: Dict = field(default_factory=dict)


@dataclass
class LoadGeneratorConfig:
    """Configuration for the load generator."""
    duration_seconds: float = 30.0
    value_size_bytes: int = 100
    key_prefix: str = "otel_bench"
    warmup_seconds: float = 2.0
    redis_host: str = "localhost"
    redis_port: int = 6379


class LoadGenerator:
    """
    Generates SET/GET load against Redis for benchmarking.
    Performs alternating SET and GET operations and collects latency metrics.
    """

    def __init__(self, client: Any, config: LoadGeneratorConfig):
        self.client = client
        self.config = config
        self.latencies: List[float] = []
        self.errors: int = 0
        self.first_error: Optional[str] = None
        self._value = "x" * config.value_size_bytes
        self._key_counter = 0

    def _get_key(self) -> str:
        """Generate a key for the current operation."""
        key = f"{self.config.key_prefix}:{self._key_counter % 1000}"
        self._key_counter += 1
        return key

    def _run_operation(self) -> float:
        """Run a single SET+GET operation pair and return latency in ms."""
        key = self._get_key()
        start = time.perf_counter()
        try:
            self.client.set(key, self._value)
            self.client.get(key)
        except Exception as e:
            if self.first_error is None:
                self.first_error = str(e)
            self.errors += 1
            return 0.0
        end = time.perf_counter()
        return (end - start) * 1000  # Convert to milliseconds

    def warmup(self) -> None:
        """Run warmup operations to stabilize connections."""
        print(f"  Warming up for {self.config.warmup_seconds}s...")
        end_time = time.monotonic() + self.config.warmup_seconds
        while time.monotonic() < end_time:
            self._run_operation()
        self.latencies.clear()
        self.errors = 0
        self.first_error = None
        self._key_counter = 0

    def run(self) -> BenchmarkResult:
        """Run the load generator for the configured duration."""
        self.latencies = []
        self.errors = 0

        print(f"  Running load for {self.config.duration_seconds}s...")
        start_time = time.monotonic()
        end_time = start_time + self.config.duration_seconds

        while time.monotonic() < end_time:
            latency = self._run_operation()
            if latency > 0:
                self.latencies.append(latency)

        actual_duration = time.monotonic() - start_time
        return self._calculate_results(actual_duration)

    def _calculate_results(self, duration: float) -> BenchmarkResult:
        """Calculate benchmark results from collected latencies."""
        if not self.latencies:
            return BenchmarkResult(
                scenario="unknown", duration_seconds=duration, total_operations=0,
                operations_per_second=0, avg_latency_ms=0, p50_latency_ms=0,
                p95_latency_ms=0, p99_latency_ms=0, min_latency_ms=0,
                max_latency_ms=0, errors=self.errors, first_error=self.first_error,
            )

        sorted_latencies = sorted(self.latencies)
        total_ops = len(self.latencies) * 2  # Each iteration does SET + GET

        return BenchmarkResult(
            scenario="unknown",
            duration_seconds=duration,
            total_operations=total_ops,
            operations_per_second=total_ops / duration,
            avg_latency_ms=statistics.mean(self.latencies),
            p50_latency_ms=sorted_latencies[len(sorted_latencies) // 2],
            p95_latency_ms=sorted_latencies[int(len(sorted_latencies) * 0.95)],
            p99_latency_ms=sorted_latencies[int(len(sorted_latencies) * 0.99)],
            min_latency_ms=min(self.latencies),
            max_latency_ms=max(self.latencies),
            errors=self.errors,
            first_error=self.first_error,
        )


def print_result(result: BenchmarkResult) -> None:
    """Print a single benchmark result."""
    print("\n" + "=" * 60)
    print(f"BENCHMARK RESULT: {result.scenario}")
    print("=" * 60)
    print(f"  Duration:     {result.duration_seconds:.2f}s")
    print(f"  Operations:   {result.total_operations:,}")
    print(f"  Ops/sec:      {result.operations_per_second:,.0f}")
    print(f"  Avg latency:  {result.avg_latency_ms:.3f}ms")
    print(f"  P50 latency:  {result.p50_latency_ms:.3f}ms")
    print(f"  P95 latency:  {result.p95_latency_ms:.3f}ms")
    print(f"  P99 latency:  {result.p99_latency_ms:.3f}ms")
    print(f"  Min latency:  {result.min_latency_ms:.3f}ms")
    print(f"  Max latency:  {result.max_latency_ms:.3f}ms")
    print(f"  Errors:       {result.errors}")
    if result.first_error:
        print(f"  First error:  {result.first_error}")
    if result.metadata.get("description"):
        print(f"  Description:  {result.metadata['description']}")
    print("=" * 60)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Benchmark OTel instrumentation overhead in redis-py. Run one scenario at a time."
    )
    parser.add_argument(
        "--scenario", type=str, required=True,
        choices=["baseline", "otel_disabled", "otel_noop", "otel_inmemory", "otel_enabled_http", "otel_enabled_grpc"],
        help="Scenario to run (required)"
    )
    parser.add_argument(
        "--baseline-tag", type=str, default=None,
        help="Git tag to use for baseline scenario (required when --scenario baseline)"
    )
    parser.add_argument(
        "--duration", type=float, default=30.0,
        help="Duration of benchmark in seconds (default: 30)"
    )
    parser.add_argument(
        "--warmup", type=float, default=5.0,
        help="Warmup duration in seconds (default: 5)"
    )
    parser.add_argument(
        "--value-size", type=int, default=100,
        help="Size of values in bytes (default: 100)"
    )
    parser.add_argument(
        "--host", type=str, default="localhost",
        help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=6379,
        help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output result as JSON"
    )
    return parser.parse_args()


def _clear_redis_modules() -> None:
    """Remove all redis.* modules from sys.modules to allow fresh import."""
    to_remove = [key for key in sys.modules if key == "redis" or key.startswith("redis.")]
    for key in to_remove:
        del sys.modules[key]


def run_baseline_scenario(tag: str, config: LoadGeneratorConfig) -> Optional[BenchmarkResult]:
    """
    Run benchmark against a baseline git tag.

    This clones the repo at the specified tag, manipulates sys.path to import
    the old redis-py version, and runs the benchmark in the same process.
    """
    repo_root = Path(__file__).parent.parent

    with TemporaryDirectory() as tmpdir:
        print(f"  Cloning repository at tag {tag}...")
        try:
            subprocess.run(
                ["git", "clone", "--depth", "1", "--branch", tag, str(repo_root), tmpdir],
                check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            print(f"  ERROR: Failed to clone tag {tag}: {e.stderr}")
            return None

        # Save original sys.path and modules state
        original_path = sys.path.copy()

        try:
            # Clear any existing redis imports and prepend cloned directory
            _clear_redis_modules()
            sys.path.insert(0, tmpdir)

            # Import redis from the cloned directory
            import redis as baseline_redis

            print(f"  Using redis from: {baseline_redis.__file__}")

            client = baseline_redis.Redis(host=config.redis_host, port=config.redis_port, decode_responses=True)
            try:
                generator = LoadGenerator(client, config)
                generator.warmup()
                result = generator.run()
                result.scenario = "baseline"
                result.metadata["tag"] = tag
                result.metadata["description"] = "Baseline without OTel code"
                return result
            finally:
                client.close()

        finally:
            # Restore original sys.path and clear redis modules again
            sys.path[:] = original_path
            _clear_redis_modules()


def run_scenario(scenario: str, config: LoadGeneratorConfig) -> BenchmarkResult:
    """
    Run a single benchmark scenario.
    """
    import redis
    client = redis.Redis(host=config.redis_host, port=config.redis_port, decode_responses=True)

    try:
        if scenario == "otel_disabled":
            description = "OTel not initialized"

        elif scenario == "otel_noop":
            from opentelemetry import metrics
            from opentelemetry.metrics import NoOpMeterProvider
            metrics.set_meter_provider(NoOpMeterProvider())

            from redis.observability.providers import get_observability_instance
            from redis.observability.config import OTelConfig
            otel = get_observability_instance()
            otel.init(OTelConfig())
            description = "OTel with NoOpMeterProvider"

        elif scenario == "otel_inmemory":
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import InMemoryMetricReader

            reader = InMemoryMetricReader()
            provider = MeterProvider(metric_readers=[reader])
            metrics.set_meter_provider(provider)

            from redis.observability.providers import get_observability_instance
            from redis.observability.config import OTelConfig
            otel = get_observability_instance()
            otel.init(OTelConfig())
            description = "OTel with InMemoryMetricReader"

        elif scenario == "otel_enabled_http":
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

            # HTTP exporter - host configurable via OTEL_COLLECTOR_HOST env var,
            # default is localhost (port 4318)
            host = os.environ.get("OTEL_COLLECTOR_HOST", "localhost")
            endpoint = f"http://{host}:4318/v1/metrics"
            exporter = OTLPMetricExporter(endpoint=endpoint)
            reader = PeriodicExportingMetricReader(exporter, export_interval_millis=10000)
            provider = MeterProvider(metric_readers=[reader])
            metrics.set_meter_provider(provider)

            from redis.observability.providers import get_observability_instance
            from redis.observability.config import OTelConfig
            otel = get_observability_instance()
            otel.init(OTelConfig())
            description = f"OTel with PeriodicExportingMetricReader (HTTP) -> {endpoint}"

        elif scenario == "otel_enabled_grpc":
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

            # gRPC exporter - host configurable via OTEL_COLLECTOR_HOST env var,
            # default is localhost (port 4317)
            host = os.environ.get("OTEL_COLLECTOR_HOST", "localhost")
            endpoint = f"{host}:4317"
            exporter = OTLPMetricExporter(endpoint=endpoint)
            reader = PeriodicExportingMetricReader(exporter, export_interval_millis=10000)
            provider = MeterProvider(metric_readers=[reader])
            metrics.set_meter_provider(provider)

            from redis.observability.providers import get_observability_instance
            from redis.observability.config import OTelConfig
            otel = get_observability_instance()
            otel.init(OTelConfig())
            description = f"OTel with PeriodicExportingMetricReader (gRPC) -> {endpoint}"
        else:
            raise ValueError(f"Unknown scenario: {scenario}")

        generator = LoadGenerator(client, config)
        generator.warmup()
        result = generator.run()
        result.scenario = scenario
        result.metadata["description"] = description
        return result

    finally:
        client.close()


def main() -> int:
    """Main entry point for the benchmark."""
    args = parse_args()

    # Validate baseline scenario requires --baseline-tag
    if args.scenario == "baseline" and not args.baseline_tag:
        print("ERROR: --baseline-tag is required when --scenario baseline")
        return 1

    print("=" * 60)
    print(f"OTel Benchmark: {args.scenario}")
    if args.baseline_tag:
        print(f"Baseline tag: {args.baseline_tag}")
    print("=" * 60)
    print(f"Duration: {args.duration}s")
    print(f"Warmup: {args.warmup}s")
    print(f"Value size: {args.value_size} bytes")
    print(f"Redis: {args.host}:{args.port}")

    config = LoadGeneratorConfig(
        duration_seconds=args.duration,
        warmup_seconds=args.warmup,
        value_size_bytes=args.value_size,
        redis_host=args.host,
        redis_port=args.port,
    )

    if args.scenario == "baseline":
        result = run_baseline_scenario(tag=args.baseline_tag, config=config)
        if result is None:
            print("ERROR: Baseline benchmark failed")
            return 1
    else:
        result = run_scenario(args.scenario, config)

    if args.json:
        print(json.dumps(asdict(result), indent=2))
    else:
        print_result(result)

    return 0


if __name__ == "__main__":
    sys.exit(main())
