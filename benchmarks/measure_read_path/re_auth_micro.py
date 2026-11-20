"""Connection pool re-authentication benchmark against the standalone server.

A streaming credential provider renews its token every so often, and the pool
then sends AUTH with the new token: right away on every free connection, and
on the next release of every connection that is in use. That work runs on the
release path of every command and under the pool lock, so it is timed here on
its own. Three scenarios, each run on the sync and the async pool:

- release (sync and async): get_connection() and release() with no AUTH
  pending, which is what every command pays. It times the release path
  itself, including the re-authentication listener that runs on it.
- deferred_auth_release (sync and async): the same, with a token pending on
  the connection, so the release sends the AUTH and reads its reply. It times
  the deferred AUTH a renewal leaves on a connection that was in use.
- refresh_20 (sync and async): re_auth_callback() on a pool with 20 free,
  connected connections. It times one token renewal, which holds the pool
  lock for all of it, so other threads or tasks wait this long to check a
  connection out or return one.

Every scenario authenticates as an ACL user the benchmark creates and deletes,
with a token the server accepts, so each tree runs the same AUTH commands.
Before timing, it checks that the renewal leaves every connection connected
and with no AUTH pending. Each tree runs in its own interpreter.

Run from the repository root, with the standalone server that `invoke devenv`
starts:

  python -m benchmarks.measure_read_path.re_auth_micro \\
      --trees base=../redis-py-bench-base pr=../redis-py-bench-pr
"""

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import gate  # noqa: E402
import run_bench  # noqa: E402
import worker  # noqa: E402

# operations per round; a round of any scenario takes tens of milliseconds
OPS = {"release": 5_000, "deferred_auth_release": 500, "refresh_20": 20}
FREE_CONNECTIONS = 20
USER = "bench_re_auth"
PASSWORD = "bench_re_auth_password"


def ns_per_op(samples, ops):
    return statistics.median(samples) / ops


def time_sync(fn, ops, rounds):
    samples = []
    # the first round is a warmup
    for i in range(rounds + 1):
        start = time.perf_counter_ns()
        for _ in range(ops):
            fn()
        if i:
            samples.append(time.perf_counter_ns() - start)
    return ns_per_op(samples, ops)


async def time_async(fn, ops, rounds):
    samples = []
    # the first round is a warmup
    for i in range(rounds + 1):
        start = time.perf_counter_ns()
        for _ in range(ops):
            await fn()
        if i:
            samples.append(time.perf_counter_ns() - start)
    return ns_per_op(samples, ops)


def timed(fn):
    start = time.perf_counter()
    ns = fn()
    return {
        "ns_per_op": round(ns, 1),
        "wall_s": round(time.perf_counter() - start, 2),
    }


async def timed_async(fn):
    start = time.perf_counter()
    ns = await fn()
    return {
        "ns_per_op": round(ns, 1),
        "wall_s": round(time.perf_counter() - start, 2),
    }


def token():
    from redis.auth.token import SimpleToken

    return SimpleToken(PASSWORD, -1, time.time() * 1000, {"oid": USER})


def pool_kwargs(args) -> dict:
    return dict(host=args.host, port=args.port, username=USER, password=PASSWORD)


def sync_cells(args, rounds):
    import redis

    pool = redis.ConnectionPool(**pool_kwargs(args))
    renewal = token()

    def release():
        pool.release(pool.get_connection())

    def deferred_auth_release():
        connection = pool.get_connection()
        connection.set_re_auth_token(renewal)
        pool.release(connection)

    try:
        free = [pool.get_connection() for _ in range(FREE_CONNECTIONS)]
        for connection in free:
            pool.release(connection)
        pool.re_auth_callback(renewal)
        assert all(c.is_connected and c._re_auth_token is None for c in free)
        deferred_auth_release()
        assert all(c._re_auth_token is None for c in free)
        return {
            "release": timed(lambda: time_sync(release, OPS["release"], rounds)),
            "deferred_auth_release": timed(
                lambda: time_sync(
                    deferred_auth_release, OPS["deferred_auth_release"], rounds
                )
            ),
            "refresh_20": timed(
                lambda: time_sync(
                    lambda: pool.re_auth_callback(renewal),
                    OPS["refresh_20"],
                    rounds,
                )
            ),
        }
    finally:
        pool.disconnect()


async def async_cells(args, rounds):
    import redis.asyncio

    pool = redis.asyncio.ConnectionPool(**pool_kwargs(args))
    renewal = token()

    async def release():
        await pool.release(await pool.get_connection())

    async def deferred_auth_release():
        connection = await pool.get_connection()
        connection.set_re_auth_token(renewal)
        await pool.release(connection)

    try:
        free = [await pool.get_connection() for _ in range(FREE_CONNECTIONS)]
        for connection in free:
            await pool.release(connection)
        await pool.re_auth_callback(renewal)
        assert all(c.is_connected and c._re_auth_token is None for c in free)
        await deferred_auth_release()
        assert all(c._re_auth_token is None for c in free)
        return {
            "release": await timed_async(
                lambda: time_async(release, OPS["release"], rounds)
            ),
            "deferred_auth_release": await timed_async(
                lambda: time_async(
                    deferred_auth_release, OPS["deferred_auth_release"], rounds
                )
            ),
            "refresh_20": await timed_async(
                lambda: time_async(
                    lambda: pool.re_auth_callback(renewal),
                    OPS["refresh_20"],
                    rounds,
                )
            ),
        }
    finally:
        await pool.disconnect()


def child(args):
    worker.import_redis(args.tree)
    import redis

    admin = redis.Redis(host=args.host, port=args.port)
    admin.acl_setuser(
        USER,
        enabled=True,
        passwords=[f"+{PASSWORD}"],
        commands=["+@all"],
        keys=["*"],
    )
    try:
        out = {
            "sync": sync_cells(args, args.rounds),
            "async": asyncio.run(async_cells(args, args.rounds)),
        }
    finally:
        admin.acl_deluser(USER)
        admin.close()
    print(json.dumps(out))


def main():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    p.add_argument("--trees", nargs="+")
    p.add_argument("--tree", help=argparse.SUPPRESS)
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--rounds", type=int, default=10)
    p.add_argument(
        "--passes",
        type=int,
        default=5,
        help="runs per tree; their spread sets the uncertainty of a delta",
    )
    p.add_argument(
        "--output-dir",
        default=os.path.join(HERE, "results"),
        help="where the JSON and Markdown results are written",
    )
    gate.add_gate_args(p)
    args = p.parse_args()
    if args.tree:
        return child(args)
    if not args.trees:
        p.error("--trees is required")
    trees = {k: os.path.abspath(v) for k, v in (t.split("=", 1) for t in args.trees)}
    pairs = gate.compare_pairs(args, trees)
    # passes[tree] -> list of {config: {scenario: result}}
    passes = {t: [] for t in trees}
    total_start = time.perf_counter()
    for i in range(args.passes):
        for label in run_bench.tree_order(trees, i):
            path = trees[label]
            cmd = [
                args.python,
                "-I",
                os.path.abspath(__file__),
                "--tree",
                path,
                "--host",
                args.host,
                "--port",
                str(args.port),
                "--rounds",
                str(args.rounds),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if proc.returncode != 0:
                raise SystemExit(proc.stderr)
            passes[label].append(json.loads(proc.stdout.strip().splitlines()[-1]))
    first = passes[next(iter(trees))][0]
    cells = [(cfg, scenario) for cfg in first for scenario in first[cfg]]
    heads = {k: run_bench.git_head(v) for k, v in trees.items()}

    def values(tree, cfg, scenario, key="ns_per_op"):
        return [r[cfg][scenario][key] for r in passes[tree]]

    lines = [
        "Pool re-authentication, nanoseconds per operation "
        f"(median of {args.passes} passes)",
        "",
        "Trees: " + ", ".join(f"`{k}`={heads[k]}" for k in trees),
        "",
        "| config | scenario | " + " | ".join(trees) + " |",
        "|" + "---|" * (2 + len(trees)),
    ]
    for cfg, scenario in cells:
        row = [f"{statistics.median(values(t, cfg, scenario)):.1f} ns" for t in trees]
        lines.append(f"| {cfg} | {scenario} | " + " | ".join(row) + " |")
    lines += [
        "",
        "## Execution times",
        "",
        f"Whole run: "
        f"{time.perf_counter() - total_start:.0f}s. Median seconds per "
        "config and scenario run:",
        "",
    ]
    for cfg, scenario in cells:
        row = [
            f"{t}={statistics.median(values(t, cfg, scenario, 'wall_s')):.2f}s"
            for t in trees
        ]
        lines.append(f"- {cfg} {scenario}: " + ", ".join(row))
    text = "\n".join(lines)
    print(text)

    summaries = [
        gate.render(
            "Pool re-authentication",
            gate.evaluate(
                [
                    {
                        "config": cfg,
                        "scenario": scenario,
                        "old_samples": values(old, cfg, scenario),
                        "new_samples": values(new, cfg, scenario),
                    }
                    for cfg, scenario in cells
                ],
                args.allowed_regression,
                args.max_limit,
            ),
            new,
            old,
            args.allowed_regression,
            "nanoseconds per operation",
            args.max_limit,
            inconclusive_fails=not args.warn_inconclusive,
        )
        for new, old in pairs
    ]
    run_bench.save(
        args,
        heads,
        {
            "passes": passes,
            "report": text + "\n\n" + "\n\n".join(s for s, _, _ in summaries),
        },
        suffix="re_auth",
    )
    gate.finish(args, summaries)


if __name__ == "__main__":
    main()
