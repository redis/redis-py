# Benchmarks

This directory contains local benchmark scripts for redis-py. They are not part
of the normal CI test matrix.

## Client-Side Caching Benchmarks

`client_side_caching.py` measures redis-py sync client-side caching performance
with focused GET/SET workloads. The benchmark supports standalone Redis and
Redis Cluster, accepts server address and credentials as inputs, and emits
throughput and latency percentiles as text and JSON.

Client-side caching requires RESP3 and Redis 7.4 or later. Run the commands
below from the repository root.

The benchmark uses RESP3 by default for both `--client-side-cache on` and
`--client-side-cache off` so the comparison isolates caching rather than RESP2
versus RESP3. Use the same `--seed`, key range, value size, clients, duration,
and workload settings for each pair of runs. Each `--clients` value creates one
Redis client and one worker thread; client objects are not shared between worker
threads. The benchmark exits non-zero if Redis operations fail unless
`--allow-errors` is set.

Install the regular development requirements:

```shell
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r dev_requirements.txt
```

Run a standalone read-heavy comparison:

```shell
python -m benchmarks.client_side_caching \
    --mode standalone \
    --host localhost \
    --port 6379 \
    --username default \
    --password "$REDIS_PASSWORD" \
    --client-side-cache off \
    --workload read-heavy \
    --duration 60 \
    --seed 1 \
    --output-json csc-off-read-heavy.json

python -m benchmarks.client_side_caching \
    --mode standalone \
    --host localhost \
    --port 6379 \
    --username default \
    --password "$REDIS_PASSWORD" \
    --client-side-cache on \
    --workload read-heavy \
    --duration 60 \
    --seed 1 \
    --output-json csc-on-read-heavy.json
```

Run the write-heavy pair by changing `--workload` and output file names:

```shell
python -m benchmarks.client_side_caching \
    --mode standalone \
    --host localhost \
    --port 6379 \
    --username default \
    --password "$REDIS_PASSWORD" \
    --client-side-cache off \
    --workload write-heavy \
    --duration 60 \
    --seed 1 \
    --output-json csc-off-write-heavy.json

python -m benchmarks.client_side_caching \
    --mode standalone \
    --host localhost \
    --port 6379 \
    --username default \
    --password "$REDIS_PASSWORD" \
    --client-side-cache on \
    --workload write-heavy \
    --duration 60 \
    --seed 1 \
    --output-json csc-on-write-heavy.json
```

Run against Redis Cluster by using `--mode cluster` and either `--host/--port`
for one startup node or `--cluster-nodes` for multiple startup nodes:

```shell
python -m benchmarks.client_side_caching \
    --mode cluster \
    --cluster-nodes "node1.example.com:6379,node2.example.com:6379,node3.example.com:6379" \
    --username default \
    --password "$REDIS_PASSWORD" \
    --client-side-cache on \
    --workload read-heavy \
    --duration 60 \
    --seed 1 \
    --output-json cluster-csc-on-read-heavy.json
```

Run the matching cluster baseline with the same options and
`--client-side-cache off`.

The JSON summary contains the fields most useful for comparison:

- `throughput_ops_per_sec`
- `avg_latency_ms`
- `p50_latency_ms`
- `p95_latency_ms`
- `p99_latency_ms`
- `total_operations`
- `read_operations`
- `write_operations`
- `clients`
- `errors`
- `warmup_errors`
- `total_errors`
- `cache_sizes`
- `metadata.seed`

For a quick setup check, use a short duration and small key range:

```shell
python -m benchmarks.client_side_caching \
    --client-side-cache on \
    --workload read-heavy \
    --duration 3 \
    --warmup 0 \
    --key-range 100
```

## OpenTelemetry Benchmarks

`otel_benchmark.py` measures redis-py operation throughput and latency with
different OpenTelemetry configurations:

- `baseline`: redis-py from a git tag, without the OpenTelemetry code path.
- `otel_disabled`: current working tree with observability not initialized.
- `otel_noop`: observability initialized with a no-op meter provider.
- `otel_inmemory`: observability initialized with an in-memory metric reader.
- `otel_enabled_http`: metrics exported over OTLP/HTTP.
- `otel_enabled_grpc`: metrics exported over OTLP/gRPC.

Run the commands below from the repository root.

The regular development requirements install the OTLP/HTTP exporter only. Install
the gRPC exporter separately when you want to run the `otel_enabled_grpc`
scenario:

```shell
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r dev_requirements.txt
python -m pip install -r benchmarks/requirements.txt
```

Start Redis before running any scenario. The OpenTelemetry example stack also
starts an OTLP collector on ports `4317` and `4318`, which is useful for the HTTP
and gRPC exporter scenarios:

```shell
docker compose -f docs/examples/opentelemetry/docker-compose.yml up -d
```

Choose one way to run the benchmark and use the matching examples below.

### Run as a module

If you prefer to run the benchmark with `python -m`, use these commands.

Run one scenario at a time:

```shell
python -m benchmarks.otel_benchmark --scenario otel_disabled
python -m benchmarks.otel_benchmark --scenario otel_noop
python -m benchmarks.otel_benchmark --scenario otel_inmemory
python -m benchmarks.otel_benchmark --scenario otel_enabled_http
python -m benchmarks.otel_benchmark --scenario otel_enabled_grpc
```

For a shorter smoke run while checking setup, lower the duration and iteration
count:

```shell
python -m benchmarks.otel_benchmark --scenario otel_enabled_grpc --duration 10 --warmup 2 --iterations 1
```

Run the baseline scenario with a redis-py release tag:

```shell
python -m benchmarks.otel_benchmark --scenario baseline --baseline-tag v5.2.1
```

Use `--async` to run the async client variant:

```shell
python -m benchmarks.otel_benchmark --scenario otel_enabled_http --async
```

By default the benchmark sends exporter traffic to `localhost`. Set
`OTEL_COLLECTOR_HOST` when the collector is on another host:

```shell
OTEL_COLLECTOR_HOST=collector.example.com python -m benchmarks.otel_benchmark --scenario otel_enabled_grpc
```

### Run the file directly

If you prefer to run the Python file directly, use these commands instead.

Run one scenario at a time:

```shell
python benchmarks/otel_benchmark.py --scenario otel_disabled
python benchmarks/otel_benchmark.py --scenario otel_noop
python benchmarks/otel_benchmark.py --scenario otel_inmemory
python benchmarks/otel_benchmark.py --scenario otel_enabled_http
python benchmarks/otel_benchmark.py --scenario otel_enabled_grpc
```

For a shorter smoke run while checking setup, lower the duration and iteration
count:

```shell
python benchmarks/otel_benchmark.py --scenario otel_enabled_grpc --duration 10 --warmup 2 --iterations 1
```

Run the baseline scenario with a redis-py release tag:

```shell
python benchmarks/otel_benchmark.py --scenario baseline --baseline-tag v5.2.1
```

Use `--async` to run the async client variant:

```shell
python benchmarks/otel_benchmark.py --scenario otel_enabled_http --async
```

By default the benchmark sends exporter traffic to `localhost`. Set
`OTEL_COLLECTOR_HOST` when the collector is on another host:

```shell
OTEL_COLLECTOR_HOST=collector.example.com python benchmarks/otel_benchmark.py --scenario otel_enabled_grpc
```

Stop the example stack when finished:

```shell
docker compose -f docs/examples/opentelemetry/docker-compose.yml down
```
