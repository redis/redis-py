from benchmarks.cluster_latency_load_balancing import summarize


def test_summarize_reports_latency_percentiles_and_node_share():
    summary = summarize(
        latencies=[0.001, 0.002, 0.003, 0.004],
        selected_nodes=[
            "replica-a",
            "replica-a",
            "replica-a",
            "primary",
            "replica-b",
            "replica-b",
        ],
        errors=2,
    )

    assert summary == {
        "attempts": 6,
        "successful_requests": 4,
        "errors": 2,
        "p50_ms": 2.5,
        "p95_ms": 3.85,
        "p99_ms": 3.97,
        "node_share": {
            "primary": 0.1667,
            "replica-a": 0.5,
            "replica-b": 0.3333,
        },
    }
