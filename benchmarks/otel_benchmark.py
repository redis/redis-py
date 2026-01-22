#!/usr/bin/env python3
"""
OpenTelemetry Benchmark for redis-py.

This module provides benchmarking infrastructure to measure the performance impact
of OpenTelemetry instrumentation on redis-py operations.

The benchmark uses a comprehensive load generator that exercises all Redis operations
(commands, pubsub, streaming, CSC, connections) in each iteration. This ensures
consistent test conditions when comparing different OTel configurations.

Run one scenario at a time:
    python -m benchmarks.otel_benchmark --scenario baseline --baseline-tag v5.2.1
    python -m benchmarks.otel_benchmark --scenario otel_disabled
    python -m benchmarks.otel_benchmark --scenario otel_noop
    python -m benchmarks.otel_benchmark --scenario otel_inmemory
    python -m benchmarks.otel_benchmark --scenario otel_enabled_http
    python -m benchmarks.otel_benchmark --scenario otel_enabled_grpc

Specify which OTel metric groups to enable:
    python -m benchmarks.otel_benchmark --scenario otel_enabled_http --metric-groups command,pubsub
    python -m benchmarks.otel_benchmark --scenario otel_enabled_http --metric-groups all
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
    warmup_seconds: float = 5.0
    redis_host: str = "localhost"
    redis_port: int = 6379


class ComprehensiveLoadGenerator:
    """
    Comprehensive load generator that exercises all Redis operations.

    Each iteration performs:
    - Command operations (SET/GET) - triggers COMMAND metrics
    - PubSub operations (PUBLISH) - triggers PUBSUB metrics
    - Streaming operations (XADD/XREAD) - triggers STREAMING metrics
    - Connection pool operations - triggers CONNECTION metrics

    CSC (Client-Side Caching) requires RESP3 protocol and is tested separately
    when available.

    This ensures consistent test conditions across all OTel configurations.
    """

    def __init__(self, config: LoadGeneratorConfig, redis_module: Any = None):
        """
        Initialize the comprehensive load generator.

        Args:
            config: Load generator configuration
            redis_module: Optional redis module to use (for baseline testing with
                         a different redis-py version). If None, imports redis normally.
        """
        self.config = config
        self.redis_module = redis_module
        self.latencies: List[float] = []
        self.errors: int = 0
        self.first_error: Optional[str] = None
        self._value = "x" * config.value_size_bytes
        self._key_counter = 0
        self._message_counter = 0

        # Resources (initialized in setup)
        self.client = None
        self.pubsub_publisher = None
        self.pubsub = None
        self.stream_name = f"{config.key_prefix}:stream"
        self.pubsub_channel = f"{config.key_prefix}:channel"
        self.consumer_group = "benchmark_group"
        self.consumer_name = "benchmark_consumer"

        # CSC client (optional, requires RESP3)
        self.csc_client = None

    def _get_redis_module(self) -> Any:
        """Get the redis module to use."""
        if self.redis_module is not None:
            return self.redis_module
        import redis
        return redis

    def _get_key(self) -> str:
        """Generate a key for the current operation."""
        key = f"{self.config.key_prefix}:{self._key_counter % 1000}"
        self._key_counter += 1
        return key

    def setup(self) -> None:
        """Set up all Redis resources."""
        redis = self._get_redis_module()

        # Main client for commands
        self.client = redis.Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            decode_responses=True
        )

        # PubSub publisher (separate connection)
        self.pubsub_publisher = redis.Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            decode_responses=True
        )

        # PubSub subscriber
        subscriber = redis.Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            decode_responses=True
        )
        self.pubsub = subscriber.pubsub()
        self.pubsub.subscribe(self.pubsub_channel)
        # Consume subscription confirmation
        self.pubsub.get_message(timeout=1.0)

        # Create stream and consumer group
        try:
            self.client.xgroup_create(
                self.stream_name,
                self.consumer_group,
                id="0",
                mkstream=True
            )
        except Exception:
            # Group may already exist
            pass

        # Try to set up CSC client (requires RESP3, may not be available)
        try:
            from redis.cache import CacheConfig
            self.csc_client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                decode_responses=True,
                protocol=3,
                cache_config=CacheConfig(max_size=1000)
            )
            # Test that CSC works
            self.csc_client.ping()
        except Exception:
            # CSC not available (old redis-py version or server doesn't support RESP3)
            self.csc_client = None

    def teardown(self) -> None:
        """Clean up all Redis resources."""
        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.close()
            except Exception:
                pass

        if self.pubsub_publisher:
            try:
                self.pubsub_publisher.close()
            except Exception:
                pass

        if self.client:
            try:
                self.client.delete(self.stream_name)
            except Exception:
                pass
            try:
                self.client.close()
            except Exception:
                pass

        if self.csc_client:
            try:
                self.csc_client.close()
            except Exception:
                pass

    def _run_operation(self) -> float:
        """
        Run a comprehensive operation cycle and return latency in ms.

        Each cycle includes:
        1. SET + GET (command metrics)
        2. PUBLISH + get_message (pubsub metrics)
        3. XADD + XREADGROUP + XACK (streaming metrics)
        4. CSC GET operations if available (csc metrics)
        """
        key = self._get_key()
        start = time.perf_counter()

        try:
            # 1. Command operations (SET/GET)
            self.client.set(key, self._value)
            self.client.get(key)

            # 2. PubSub operations
            self._message_counter += 1
            self.pubsub_publisher.publish(self.pubsub_channel, f"msg:{self._message_counter}")
            self.pubsub.get_message(timeout=0.001)  # Non-blocking receive

            # 3. Streaming operations
            entry_id = self.client.xadd(
                self.stream_name,
                {"data": self._value[:50]},  # Smaller payload for streams
                maxlen=1000
            )
            messages = self.client.xreadgroup(
                self.consumer_group,
                self.consumer_name,
                {self.stream_name: ">"},
                count=1,
                block=1  # 1ms timeout
            )
            if messages:
                for stream_name, entries in messages:
                    for eid, _ in entries:
                        self.client.xack(self.stream_name, self.consumer_group, eid)

            # 4. CSC operations (if available)
            if self.csc_client:
                csc_key = f"{self.config.key_prefix}:csc:{self._key_counter % 100}"
                self.csc_client.set(csc_key, self._value)
                self.csc_client.get(csc_key)  # Cache miss
                self.csc_client.get(csc_key)  # Cache hit

        except Exception as e:
            if self.first_error is None:
                self.first_error = str(e)
            self.errors += 1

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
        self._message_counter = 0

    def run(self) -> BenchmarkResult:
        """Run the load generator for the configured duration."""
        self.latencies = []
        self.errors = 0
        self.first_error = None

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
        # Count operations per cycle:
        # - 2 command ops (SET + GET)
        # - 2 pubsub ops (PUBLISH + get_message)
        # - 3 stream ops (XADD + XREADGROUP + XACK)
        # - 3 CSC ops if available (SET + 2x GET)
        ops_per_cycle = 7 + (3 if self.csc_client else 0)
        total_ops = len(self.latencies) * ops_per_cycle

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


def print_result(result: BenchmarkResult, iterations: int = 1) -> None:
    """Print a single benchmark result."""
    print("\n" + "=" * 60)
    print(f"BENCHMARK RESULT: {result.scenario}")
    print("=" * 60)
    if iterations > 1:
        print(f"  Iterations:   {iterations} (averaged)")
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


def average_results(results: List[BenchmarkResult]) -> BenchmarkResult:
    """Average multiple benchmark results into a single result."""
    if not results:
        raise ValueError("Cannot average empty results list")
    if len(results) == 1:
        return results[0]

    n = len(results)
    # Find first error from any iteration
    first_error = None
    for r in results:
        if r.first_error:
            first_error = r.first_error
            break

    return BenchmarkResult(
        scenario=results[0].scenario,
        duration_seconds=sum(r.duration_seconds for r in results) / n,
        total_operations=int(sum(r.total_operations for r in results) / n),
        operations_per_second=sum(r.operations_per_second for r in results) / n,
        avg_latency_ms=sum(r.avg_latency_ms for r in results) / n,
        p50_latency_ms=sum(r.p50_latency_ms for r in results) / n,
        p95_latency_ms=sum(r.p95_latency_ms for r in results) / n,
        p99_latency_ms=sum(r.p99_latency_ms for r in results) / n,
        min_latency_ms=min(r.min_latency_ms for r in results),
        max_latency_ms=max(r.max_latency_ms for r in results),
        errors=sum(r.errors for r in results),
        first_error=first_error,
        metadata=results[0].metadata,
    )


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
    parser.add_argument(
        "--iterations", type=int, default=5,
        help="Number of iterations to run (default: 5). Final result is averaged."
    )
    parser.add_argument(
        "--metric-groups", type=str, default=None,
        help=(
            "Comma-separated list of metric groups to enable. "
            "Options: command, pubsub, streaming, csc, connection_basic, connection_advanced, resiliency, all. "
            "Default: resiliency,connection_basic. "
            "Example: --metric-groups command,pubsub,connection_basic"
        )
    )
    parser.add_argument(
        "--export-interval", type=int, default=10000,
        help="OTel metric export interval in milliseconds (default: 10000)"
    )
    return parser.parse_args()


def _clear_redis_modules() -> None:
    """Remove all redis.* modules from sys.modules to allow fresh import."""
    to_remove = [key for key in sys.modules if key == "redis" or key.startswith("redis.")]
    for key in to_remove:
        del sys.modules[key]


def run_baseline_scenario(tag: str, config: LoadGeneratorConfig) -> Optional[BenchmarkResult]:
    """
    Run benchmark against a baseline git tag using ComprehensiveLoadGenerator.

    This clones the repo at the specified tag, manipulates sys.path to import
    the old redis-py version, and runs the benchmark using the same comprehensive
    load generator for consistent comparison with other scenarios.

    Returns:
        BenchmarkResult or None on failure
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

            # Create comprehensive generator with baseline redis module
            generator = ComprehensiveLoadGenerator(config, redis_module=baseline_redis)
            generator.setup()
            try:
                generator.warmup()
                result = generator.run()
                result.scenario = "baseline"
                result.metadata["tag"] = tag
                result.metadata["description"] = "Baseline without OTel code"
                return result
            finally:
                generator.teardown()

        finally:
            # Restore original sys.path and clear redis modules again
            sys.path[:] = original_path
            _clear_redis_modules()


def _get_metric_groups_for_benchmark(metric_group_names: Optional[List[str]]) -> Optional[List[Any]]:
    """
    Convert metric group names to MetricGroup enum values.

    Args:
        metric_group_names: List of metric group names (command, pubsub, streaming, csc,
                           connection_basic, connection_advanced, resiliency, all)

    Returns:
        List of MetricGroup enum values, or None for defaults
    """
    if not metric_group_names:
        return None

    from redis.observability.config import MetricGroup

    name_to_group = {
        "command": MetricGroup.COMMAND,
        "pubsub": MetricGroup.PUBSUB,
        "streaming": MetricGroup.STREAMING,
        "csc": MetricGroup.CSC,
        "connection_basic": MetricGroup.CONNECTION_BASIC,
        "connection_advanced": MetricGroup.CONNECTION_ADVANCED,
        "resiliency": MetricGroup.RESILIENCY,
    }

    # Handle "all" - return all metric groups
    if "all" in metric_group_names:
        return list(name_to_group.values())

    groups = []
    for name in metric_group_names:
        if name in name_to_group:
            groups.append(name_to_group[name])

    return groups if groups else None


def setup_scenario(
    scenario: str,
    metric_group_names: Optional[List[str]] = None,
    export_interval_millis: int = 10000
) -> str:
    """
    Set up OTel for a scenario. Returns the description.
    This should only be called once per process.

    Args:
        scenario: The scenario name
        metric_group_names: List of metric group names to enable
        export_interval_millis: Export interval in milliseconds for PeriodicExportingMetricReader
    """
    if scenario == "otel_disabled":
        return "OTel not initialized"

    # Determine which metric groups to enable
    metric_groups = _get_metric_groups_for_benchmark(metric_group_names)
    groups_desc = ""
    if metric_group_names:
        groups_desc = f" [groups: {', '.join(metric_group_names)}]"

    if scenario == "otel_noop":
        from opentelemetry import metrics
        from opentelemetry.metrics import NoOpMeterProvider
        metrics.set_meter_provider(NoOpMeterProvider())

        from redis.observability.providers import get_observability_instance
        from redis.observability.config import OTelConfig
        otel = get_observability_instance()
        if metric_groups:
            otel.init(OTelConfig(metric_groups=metric_groups))
        else:
            otel.init(OTelConfig())
        return f"OTel with NoOpMeterProvider{groups_desc}"

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
        if metric_groups:
            otel.init(OTelConfig(metric_groups=metric_groups))
        else:
            otel.init(OTelConfig())
        return f"OTel with InMemoryMetricReader{groups_desc}"

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
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=export_interval_millis)
        provider = MeterProvider(metric_readers=[reader])
        metrics.set_meter_provider(provider)

        from redis.observability.providers import get_observability_instance
        from redis.observability.config import OTelConfig
        otel = get_observability_instance()
        if metric_groups:
            otel.init(OTelConfig(metric_groups=metric_groups))
        else:
            otel.init(OTelConfig())
        return f"OTel with PeriodicExportingMetricReader (HTTP) -> {endpoint} [export: {export_interval_millis}ms]{groups_desc}"

    elif scenario == "otel_enabled_grpc":
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        # gRPC exporter - host configurable via OTEL_COLLECTOR_HOST env var,
        # default is localhost (port 4317)
        host = os.environ.get("OTEL_COLLECTOR_HOST", "localhost")
        endpoint = f"{host}:4317"
        exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=export_interval_millis)
        provider = MeterProvider(metric_readers=[reader])
        metrics.set_meter_provider(provider)

        from redis.observability.providers import get_observability_instance
        from redis.observability.config import OTelConfig
        otel = get_observability_instance()
        if metric_groups:
            otel.init(OTelConfig(metric_groups=metric_groups))
        else:
            otel.init(OTelConfig())
        return f"OTel with PeriodicExportingMetricReader (gRPC) -> {endpoint} [export: {export_interval_millis}ms]{groups_desc}"

    else:
        raise ValueError(f"Unknown scenario: {scenario}")


def run_iteration(scenario: str, config: LoadGeneratorConfig, description: str) -> BenchmarkResult:
    """
    Run a single benchmark iteration using the ComprehensiveLoadGenerator.

    Args:
        scenario: The scenario name
        config: Load generator configuration
        description: Description of the scenario

    Returns:
        BenchmarkResult from the comprehensive load generator
    """
    generator = ComprehensiveLoadGenerator(config)
    generator.setup()
    try:
        generator.warmup()
        result = generator.run()
        result.scenario = scenario
        result.metadata["description"] = description
        return result
    finally:
        generator.teardown()


def main() -> int:
    """Main entry point for the benchmark."""
    args = parse_args()

    # Validate baseline scenario requires --baseline-tag
    if args.scenario == "baseline" and not args.baseline_tag:
        print("ERROR: --baseline-tag is required when --scenario baseline")
        return 1

    # Parse metric groups
    metric_group_names: Optional[List[str]] = None
    if args.metric_groups:
        metric_group_names = [g.strip().lower() for g in args.metric_groups.split(",")]
        # Validate metric group names
        valid_groups = {"command", "pubsub", "streaming", "csc", "connection_basic", "connection_advanced", "resiliency", "all"}
        invalid_groups = [g for g in metric_group_names if g not in valid_groups]
        if invalid_groups:
            print(f"ERROR: Invalid metric groups: {', '.join(invalid_groups)}")
            print(f"Valid options: {', '.join(sorted(valid_groups))}")
            return 1

    print("=" * 60)
    print(f"OTel Benchmark: {args.scenario}")
    if args.baseline_tag:
        print(f"Baseline tag: {args.baseline_tag}")
    if metric_group_names:
        print(f"Metric groups: {', '.join(metric_group_names)}")
    print("=" * 60)
    print(f"Duration: {args.duration}s per iteration")
    print(f"Warmup: {args.warmup}s per iteration")
    print(f"Iterations: {args.iterations}")
    print(f"Value size: {args.value_size} bytes")
    print(f"Redis: {args.host}:{args.port}")

    config = LoadGeneratorConfig(
        duration_seconds=args.duration,
        warmup_seconds=args.warmup,
        value_size_bytes=args.value_size,
        redis_host=args.host,
        redis_port=args.port,
    )

    # Set up OTel once (for non-baseline scenarios)
    description = ""
    if args.scenario != "baseline":
        print("\nSetting up OTel...")
        description = setup_scenario(
            args.scenario,
            metric_group_names=metric_group_names,
            export_interval_millis=args.export_interval
        )
        print(f"  {description}")

    # Run benchmark iterations
    results: List[BenchmarkResult] = []
    for i in range(args.iterations):
        print(f"\n--- Iteration {i + 1}/{args.iterations} ---")

        if args.scenario == "baseline":
            result = run_baseline_scenario(tag=args.baseline_tag, config=config)
            if result is None:
                print("ERROR: Baseline benchmark failed")
                return 1
        else:
            result = run_iteration(args.scenario, config, description)

        results.append(result)
        print(f"  Ops/sec: {result.operations_per_second:,.0f}")

    # Average results across all iterations
    final_result = average_results(results)

    # Output results
    if args.json:
        print(json.dumps(asdict(final_result), indent=2))
    else:
        print_result(final_result, iterations=args.iterations)

    return 0


if __name__ == "__main__":
    sys.exit(main())
