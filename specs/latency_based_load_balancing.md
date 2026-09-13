# Latency-Based Cluster Read Balancing

## Objective

Add an opt-in `LoadBalancingStrategy.LATENCY_BASED` for Redis Cluster reads.
The strategy samples two eligible nodes and selects the lower score:

```text
aged peak EWMA * (in-flight requests + 1)
```

The unsuffixed strategy includes the primary and replicas, matching `RANDOM`
and `ROUND_ROBIN`. Existing strategies remain unchanged.

## State and observations

Each node stores a generation, in-flight count, ordinary EWMA, peak EWMA, and
last-observation time. The load balancer also stores a slow cluster baseline.

Only successful reads update latency. All commands update in-flight load while
the strategy is active, so writes make a busy primary less attractive without
mixing write latency into the read estimator. Failures, redirects, timeouts,
cancellation, and pipeline batches release in-flight state without recording a
latency sample.

For successful latency `r`, node EWMA alpha `0.2`, baseline alpha `0.05`, elapsed
time `dt`, and decay period `10s`:

```text
d = 0 if dt >= 30 else exp(-dt / 10)
aged_ewma = baseline + (ewma - baseline) * d
aged_peak = aged_ewma + max(0, peak - ewma) * d
new_ewma = 0.2 * r + 0.8 * aged_ewma
new_peak = max(r, new_ewma, aged_peak)
new_baseline = 0.05 * new_ewma + 0.95 * baseline
```

Cold nodes use the baseline, initialized to `1ms`. The first successful node
EWMA updates that value through the same baseline equation; it does not replace
the baseline. Measurements expire after three decay periods (`30s`) and then
score at the baseline. This finite evidence lifetime is necessary because pure
exponential decay only approaches the baseline: with one primary and one
replica, strict minimum selection could otherwise starve a recovered node
forever.

## Lifecycle

`start_request(node_name)` increments load and returns a token containing the
node generation. `finish_request(token, latency=None)` always releases the
matching generation and observes latency only when provided. Stale tokens are
ignored after a node is removed and later recreated.

Topology refresh reconciles active names only after successful discovery. It
preserves surviving state and prunes departed nodes. Ordinary round-robin reset
does not erase latency history.

## Timing boundary

A sample begins immediately before connection acquisition and ends after a
successful parsed response. It includes pool wait, successful reconnect,
network, server, parsing, and callbacks. It excludes routing, retry backoff,
failed attempts, and ASK migration attempts. Sync and async use this same
client-observed boundary.

## Pipelines

Each grouped node batch counts as one in-flight operation while executing but
does not update latency. A transactional pipeline likewise counts as one
in-flight operation on its owning node, and immediate watched commands use one
attempt apiece. Batch duration is not comparable across different command
counts or sync pipeline ordering. Commands retried individually use the normal
lifecycle and may record a successful read sample.

## Tradeoffs

- Errors do not penalize latency; existing retry and topology logic owns hard
  failures.
- End-to-end attempt timing intentionally includes client pool pressure.
- The fixed decay constants avoid new public configuration and require benchmark
  validation.
- Expiring evidence at `30s` creates a small score discontinuity in exchange for
  bounded recovery exploration.
- The traffic-weighted baseline favors nodes serving real traffic; a per-node
  median is deferred unless evidence shows it is needed.
- In-flight accounting begins immediately after selection rather than reserving
  atomically inside routing, avoiding a broad routing return-type change.
- Pipeline-only workloads receive concurrency balancing but do not learn
  historical latency.

## Merge evidence

The implementation requires deterministic unit tests for state, failures,
topology generations, sync/async parity, and pipelines, plus a real local-cluster
benchmark with externally induced replica degradation and recovery.
