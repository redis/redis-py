"""Peak Python memory per operation on the read path, sync and async.

Runs one operation per scenario under tracemalloc and reports the peak
traced allocation above the starting point, in MiB and as a multiple of the
payload. Each config runs the same scenarios through the async client and
through the sync client. tracemalloc slows the run, so these numbers are for
memory only; use run_bench.py for timing. Memory allocated inside hiredis' C
reader is not traced, so the hiredis rows understate its peak.

Run from the repository root:

  python -m benchmarks.measure_read_path.mem_probe \\
      --trees base=../redis-py-bench-base pr=../redis-py-bench-pr
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import tracemalloc

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import gate  # noqa: E402
import run_bench  # noqa: E402
import worker  # noqa: E402

MIB = 1024 * 1024
K_MGET = [worker.PREFIX + f"mget1m:{i}" for i in range(10)]
GRID = [
    (client, parser, protocol, "5")
    for client in ("async", "sync")
    for parser, protocol in (
        ("python", 3),
        ("python", 2),
        ("hiredis", 3),
        ("hiredis", 2),
    )
]


def ops(r):
    """(name, operation, payload bytes, reply check) per scenario."""
    one_mb, ten_mb = len(worker.BIG_1M), len(worker.BIG_10M)
    return [
        (
            "get_1mb",
            lambda: r.get(worker.K_1M),
            one_mb,
            lambda v: len(v) == one_mb,
        ),
        (
            "get_10mb",
            lambda: r.get(worker.K_10M),
            ten_mb,
            lambda v: len(v) == ten_mb,
        ),
        (
            "mget_10x1mb",
            lambda: r.mget(K_MGET),
            10 * one_mb,
            lambda v: [len(x) for x in v] == [one_mb] * 10,
        ),
        (
            "lrange_10k",
            lambda: r.lrange(worker.K_LIST, 0, -1),
            None,
            lambda v: len(v) == worker.LIST_LEN,
        ),
    ]


async def prepare(args, redis, teardown=False):
    """Write the scenario keys, or delete them, through an async client."""
    args = argparse.Namespace(**{**vars(args), "client": "async"})
    r = worker.make_client(redis, args, max_connections=1)
    try:
        if teardown:
            await worker.teardown(r)
        else:
            await worker.setup(r)
            for k in K_MGET:
                await r.set(k, worker.BIG_1M)
    finally:
        await r.aclose()


def make_sync_client(redis, args):
    connection_pool = redis.ConnectionPool(
        host=args.host,
        port=args.port,
        db=args.db,
        protocol=args.protocol,
        parser_class=worker.parser_class(args),
        socket_timeout=worker.socket_timeout(args),
        max_connections=1,
    )
    return redis.Redis(connection_pool=connection_pool)


def result(peaks, payload, start_wall):
    peak = min(peaks)  # least-disturbed run
    return {
        "peak_mib": round(peak / MIB, 2),
        "peak_x_payload": round(peak / payload, 2) if payload else None,
        "wall_s": round(time.perf_counter() - start_wall, 2),
    }


async def measure_async(args, redis):
    r = worker.make_client(redis, args, max_connections=1)
    try:
        out = {}
        for name, op, payload, check in ops(r):
            start_wall = time.perf_counter()
            assert check(await op()), name  # also warms the connection
            peaks = []
            for _ in range(args.repeats):
                tracemalloc.start()
                start, _ = tracemalloc.get_traced_memory()
                reply = await op()
                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                assert check(reply), name
                del reply
                peaks.append(peak - start)
            out[name] = result(peaks, payload, start_wall)
        return out
    finally:
        await r.aclose()


def measure_sync(args, redis):
    r = make_sync_client(redis, args)
    try:
        out = {}
        for name, op, payload, check in ops(r):
            start_wall = time.perf_counter()
            assert check(op()), name  # also warms the connection
            peaks = []
            for _ in range(args.repeats):
                tracemalloc.start()
                start, _ = tracemalloc.get_traced_memory()
                reply = op()
                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                assert check(reply), name
                del reply
                peaks.append(peak - start)
            out[name] = result(peaks, payload, start_wall)
        return out
    finally:
        r.close()


def child(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--tree", required=True)
    p.add_argument("--client", choices=["async", "sync"], default="async")
    p.add_argument("--parser", default="python")
    p.add_argument("--protocol", type=int, default=3)
    p.add_argument("--socket-timeout", default="5")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=13)
    p.add_argument("--repeats", type=int, default=3)
    args = p.parse_args(argv)
    redis = worker.import_redis(args.tree)
    asyncio.run(prepare(args, redis))
    try:
        if args.client == "sync":
            out = measure_sync(args, redis)
        else:
            out = asyncio.run(measure_async(args, redis))
    finally:
        asyncio.run(prepare(args, redis, teardown=True))
    print(json.dumps(out))


def main():
    # no abbreviations: the child's --tree must not match --trees
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p.add_argument("--trees", nargs="+")
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=13)
    p.add_argument("--output-dir", default=os.path.join(HERE, "results"))
    gate.add_gate_args(p)
    args, rest = p.parse_known_args()
    if "--child" in rest:
        rest.remove("--child")
        return child(rest)
    if not args.trees:
        p.error("--trees is required")
    trees = {k: os.path.abspath(v) for k, v in (t.split("=", 1) for t in args.trees)}
    pairs = gate.compare_pairs(args, trees)
    heads = {k: run_bench.git_head(v) for k, v in trees.items()}
    rows = [
        "| config | scenario | " + " | ".join(trees) + " |",
        "|" + "---|" * (2 + len(trees)),
    ]
    times = [
        "| config | tree | seconds |",
        "|---|---|---|",
    ]
    results = {}
    total_start = time.perf_counter()
    for client, parser, protocol, timeout in GRID:
        cfg = f"{client}/{parser}/resp{protocol}/timeout={timeout}"
        res = {}
        for label, path in trees.items():
            cmd = [
                args.python,
                "-I",
                os.path.abspath(__file__),
                "--child",
                "--tree",
                path,
                "--client",
                client,
                "--parser",
                parser,
                "--protocol",
                str(protocol),
                "--socket-timeout",
                timeout,
                "--host",
                args.host,
                "--port",
                str(args.port),
                "--db",
                str(args.db),
            ]
            start = time.perf_counter()
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if proc.returncode != 0:
                raise SystemExit(proc.stderr)
            times.append(f"| {cfg} | {label} | {time.perf_counter() - start:.1f} |")
            res[label] = json.loads(proc.stdout.strip().splitlines()[-1])
        results[cfg] = res
        for sc in next(iter(res.values())):
            cells = []
            for t in trees:
                m = res[t][sc]
                x = f", {m['peak_x_payload']}x" if m["peak_x_payload"] else ""
                cells.append(f"{m['peak_mib']} MiB{x}")
            rows.append(f"| {cfg} | {sc} | " + " | ".join(cells) + " |")
    total = time.perf_counter() - total_start
    text = (
        "Peak traced memory per operation (x = multiple of the payload)\n\n"
        "Trees: "
        + ", ".join(f"`{k}`={heads[k]}" for k in trees)
        + "\n\n"
        + "\n".join(rows)
        + f"\n\n## Execution times\n\nWhole probe: {total:.0f}s\n\n"
        + "\n".join(times)
    )
    print(text)
    # peak memory is nearly deterministic (the least-disturbed of the
    # repeats), so no noise term: only the allowed percentage applies
    summaries = [
        gate.render(
            "Read path, peak traced memory",
            gate.evaluate(
                [
                    {
                        "config": cfg,
                        "scenario": sc,
                        "old": res[old][sc]["peak_mib"],
                        "new": res[new][sc]["peak_mib"],
                    }
                    for cfg, res in results.items()
                    for sc in res[old]
                ],
                args.allowed_regression,
                args.max_limit,
            ),
            new,
            old,
            args.allowed_regression,
            "MiB per operation",
            args.max_limit,
        )
        for new, old in pairs
    ]
    run_bench.save(
        args,
        heads,
        {
            "memory": results,
            "report": text + "\n\n" + "\n\n".join(s for s, _ in summaries),
        },
        suffix="memory",
    )
    gate.finish(args, summaries)


if __name__ == "__main__":
    main()
