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

## Read-Path Benchmarks

`measure_read_path/` compares the read path (the parsers, the connections and
the connection pools) of the sync and async clients, standalone and cluster,
across redis-py checkouts. Use it for any change to the parsers, connections
or pools: every command goes through this path.

The standalone configs run against a local Redis server on 6379; the cluster
configs need the cluster that `invoke devenv` starts on 16379. Only keys
under `bench_read_path:` are written and deleted, in db 13 on the standalone
server and db 0 on the cluster. Do not run it at the same time as a test
suite or another benchmark: they compete for the server and the CPU.

Prepare one worktree per tree. For a pull request, `prepare_worktrees.sh`
creates or refreshes `../redis-py-bench-master` (current master),
`../redis-py-bench-base` (the PR's merge-base, which isolates the PR's own
effect) and `../redis-py-bench-pr` (the PR head):

```shell
benchmarks/measure_read_path/prepare_worktrees.sh <PR number>
```

Run the tools from the repository root. Each tree runs in its own
interpreter with `-I`, and the worker checks that `redis` was imported from
that tree, so an editable install of another checkout cannot be measured by
mistake.

```shell
TREES="master=../redis-py-bench-master base=../redis-py-bench-base pr=../redis-py-bench-pr"

# Timing: median time per operation, noise, deltas, execution times
python -m benchmarks.measure_read_path.run_bench --trees $TREES --compare pr:base pr:master

# Timeouts armed per operation (diagnostic, not timed)
python -m benchmarks.measure_read_path.run_bench --trees $TREES --count-arms

# Peak traced memory per operation
python -m benchmarks.measure_read_path.mem_probe --trees $TREES

# Parser only, no network: every parser, sync and async, reading from memory
python -m benchmarks.measure_read_path.parser_micro --trees $TREES
```

`run_bench` covers the pure-Python parser and hiredis, each on RESP3 and RESP2,
with the default `socket_timeout=5` and with `socket_timeout=None`. `--client
async sync` and `--topology standalone cluster` pick the clients and
topologies; the cluster configs connect to `--cluster-port` (default 16379,
the cluster `invoke devenv` starts). `--pool` picks the connection pool
class, `connection` (`ConnectionPool`, the default) or `blocking`
(`BlockingConnectionPool`); the async cluster client manages its connections
per node and has no pool class. Add `--uvloop` to repeat the async configs
under uvloop. Every combination of these runs, and each worker checks that its
connections use the requested parser before timing. Each round runs every
tree once per config, in a shuffled order. A delta is flagged only above the
allowed regression and above twice the larger noise (relative IQR across
rounds) of the two trees; see "Regression gate" below. Every scenario checks
its reply, so a broken read path cannot pass as a fast one.

| scenario | read-path shape |
|---|---|
| `get_small_seq`, `set_small_seq` | one short reply per round trip |
| `pipeline_1k_get` | 1000 replies already buffered |
| `lrange_10k`, `hgetall_1k` | one aggregate reply, many line and bulk reads |
| `get_1mb`, `get_10mb` | one bulk that spans many stream reads |
| `concurrent_get` | 64 tasks on a 64-connection pool |
| `pool_checkout` | `get_connection()` + `release()`, no round trip |

Round-trip scenarios are bound by network latency, so parser overhead shows
best in the aggregate, pipeline and parser-only numbers. `parser_micro` and
`mem_probe` also cover the sync stack. `parser_micro` times the pure-Python
RESP2 and RESP3 parsers and the hiredis parser, sync and async, reading a
10k-element array (many line reads) and a 10 MB bulk (about 160 socket
reads), each attached through its own `on_connect()` to an in-memory stream
or socket. `mem_probe` runs its scenarios through the async and the sync
client. It uses tracemalloc, which does not see hiredis' C reader buffer, so
the hiredis rows understate its peak. The sync pure-Python peak for a 10 MB
GET moves by up to about 3.5% between runs of the same code, because it
depends on how the reply arrives in socket reads.

Every tool reports how long each test took, and writes JSON and Markdown
results under `benchmarks/measure_read_path/results/` (ignored by git), with the
tree SHAs in the file name. Use `--output-dir` to write them elsewhere.

### Regression gate

`run_bench`, `mem_probe` and `parser_micro` end with a regression summary for
every `--compare candidate:baseline` pair: whether the gate passed, and for
every regressed or inconclusive cell the baseline and candidate numbers and
the delta. A cell regresses when the candidate is worse by more than
`--allowed-regression` percent (default 5) and by more than twice the noise
measured for either tree. The noise is the relative IQR of a tree's per-round
results. The noise rule may raise the limit only up to `--max-limit` percent
(default 10): a cell that would need a higher limit is inconclusive, because it
is too noisy to catch a regression that size, and it fails the gate instead of
passing unseen. With `--fail-on-regression` the tool exits 1 on a regression
or an inconclusive cell, and `--summary FILE` appends the summary to a file
such as `$GITHUB_STEP_SUMMARY`. `run_bench` gives regressed and inconclusive
cells `--recheck-rounds` more rounds (default 3) before the verdict. The
timeout counts from `--count-arms` are informational and never fail a run.

```shell
python -m benchmarks.measure_read_path.parser_micro --trees $TREES \
    --compare pr:master --fail-on-regression
```

The `Benchmarks` workflow (`.github/workflows/benchmarks.yaml`) runs these
gates on request, not on every pull request:

- by hand, from Actions > Benchmarks > Run workflow, with a base ref (default
  `master`), a ref under test (a branch, tag, SHA or
  `refs/pull/<number>/head`) and the allowed regression (default 3%);
- on a pull request labeled `with-benchmarks`, when the label is added and on
  every push while it stays. The tree under test is GitHub's merge of the PR
  onto its target branch, and the base is `master`.

`ci_matrix.py` generates the jobs: one each for peak memory and the
parser-only benchmark, and one `run_bench` job per config, every client,
topology, pool class, parser, protocol and socket timeout combination (58
jobs). Each job measures both trees on the same runner, with the tools from
the workflow's own commit, so the ref under test does not need the
benchmarks. The workflow is informational: keep it out of the required
status checks, so a regression shows as a failed check without blocking the
merge.

## Maintenance Notifications Harness

`maint_notifications_async_perf.py` runs the async cluster client under load
while a fault injector triggers a slot migration, and reports throughput,
latency percentiles, errors and event-loop stalls. It needs the same Redis
Enterprise and fault-injector setup as the scenario tests, and it creates and
deletes its own database. It runs in the CAE pipeline with the scenario tests,
not in the `Benchmarks` workflow.

```shell
python -m benchmarks.maint_notifications_async_perf --flow throughput --maint on \
    --fault-injector-url http://127.0.0.1:20324
python -m benchmarks.maint_notifications_async_perf --flow throughput --maint off
python -m benchmarks.maint_notifications_async_perf --flow notifications --effect slot-shuffle
```

`--fault-injector-url` defaults to `FAULT_INJECTION_API_URL`, like the
scenario tests. `--db-name`, `--db-port`, `--shards`, `--effect` and
`--trigger` set the database and the migration. Run with `--help` for the
load options.

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
