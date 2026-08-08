# Network-level verification of the fix for redis/redis-py#3741

This branch reproduces, at the real TCP level (not a mock), the bug
described in [redis/redis-py#3741](https://github.com/redis/redis-py/issues/3741)
and verifies that [PR #3863](https://github.com/redis/redis-py/pull/3863)
fixes it.

## Summary

- `proxy.py` — a small async TCP proxy that sits between a redis-py client
  and a real Redis server. It forces a genuine TCP RST (via `SO_LINGER`,
  linger=0) on the first N connection attempts it accepts, then passes
  every connection after that straight through to the real backend.
- `demo_proxy_repro.py` — connects a `redis.asyncio.connection.Connection`
  through that proxy, using the retry configuration from the original bug
  report (`Retry(ExponentialBackoff(cap=10, base=1), 25)`).

Because the RST is a real kernel-level TCP event (not a Python-level
exception injection), this reproduces the exact class of failure the
original reporter saw in production
(`ConnectionResetError: [Errno 104] Connection reset by peer`
mid-handshake).

## Results

**On `742b13bd`** (the commit immediately before #3863 merged):
```
$ python demo_proxy_repro.py --port 6500
connecting...
FAILED TO CONNECT: ConnectionError: ...
```
Proxy log shows exactly **one** connection attempt, then nothing — the
handshake write (`CLIENT SETINFO LIB-NAME`; default protocol is RESP2 so
no `HELLO` is sent) failed once and was never retried, despite 25 retries
being configured.

**On `master`** (after #3863 / commit `4a6c2c0f`):
```
$ python demo_proxy_repro.py --port 6500
connecting...
CONNECTED: True
PING response: b'PONG'
```
Proxy log shows two forced resets followed by a successful pass-through —
`connect()` retried the full connect+handshake flow and recovered.

## How to run it yourself

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e . && pip install pytest pytest-asyncio mock

# start a throwaway real Redis
redis-server --port 6398 --daemonize yes --pidfile redis-test.pid --logfile redis-test.log

# terminal 1: the proxy
python proxy.py --listen-port 6500 --backend-port 6398 --reset-count 2

# terminal 2: pre-fix
git checkout 742b13bd && pip install -e . --no-deps
python demo_proxy_repro.py --port 6500   # expect: FAILED TO CONNECT after 1 attempt

# restart the proxy in terminal 1 (resets its counter), then in terminal 2:
git checkout master && pip install -e . --no-deps
python demo_proxy_repro.py --port 6500   # expect: CONNECTED: True, PONG
```

## Why this needed care to get right

Two things will silently give you a false pass if you're not careful:

1. **Don't let the raw socket connect fail** — if there's nothing listening
   at all, the connect-level retry (which now wraps everything, post-#3863)
   will retry the TCP connect itself with real exponential backoff sleeps,
   making the test hang rather than fail fast. Point the client at a proxy
   that completes the TCP handshake before resetting, not at a closed port.

2. **Don't set `health_check_interval`** in the demo client config. If set,
   `check_health()` sends a `PING` before any real handshake command, and
   that `PING` already goes through its own independent
   `retry.call_with_retry` — a mechanism that predates #3863 and was never
   broken. That pre-existing retry loop will recover from resets on *both*
   the pre-fix and post-fix commit, making them indistinguishable and
   masking the actual bug/fix being tested.
