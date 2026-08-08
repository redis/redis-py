"""
Point a redis-py async Connection at the proxy (proxy.py) instead of
directly at Redis, using the reporter's exact retry configuration from
https://github.com/redis/redis-py/issues/3741, and see whether it
survives the proxy's simulated resets.

Usage:
    python demo_proxy_repro.py --port 6500
"""
import argparse
import asyncio

from redis.asyncio.connection import Connection
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    conn = Connection(
        port=args.port,
        socket_keepalive=True,
        socket_connect_timeout=15,
        socket_timeout=5,
        retry=Retry(ExponentialBackoff(cap=10, base=1), 25),
        retry_on_error=[RedisConnectionError, ConnectionResetError],
        # NOTE: health_check_interval is intentionally NOT set here.
        # If it were, check_health()'s PING would be the first write on a
        # fresh connection, and PING failures are recovered by their own
        # independent retry.call_with_retry() -- a mechanism that predates
        # PR #3863 and was never broken. That masks the actual bug: it
        # makes pre-fix and post-fix code behave identically against this
        # proxy, because the pre-existing PING retry does all the
        # recovering instead of the connect()-level retry PR #3863 added.
        # Leaving it unset means the first write on a fresh connection is
        # the real (and, pre-fix, genuinely unprotected) handshake command.
    )

    print("connecting...", flush=True)
    try:
        await conn.connect()
    except Exception as e:
        print(f"FAILED TO CONNECT: {type(e).__name__}: {e}", flush=True)
        return

    print("CONNECTED:", conn.is_connected, flush=True)
    pong = await conn.retry.call_with_retry(
        lambda: _ping(conn), lambda e: conn.disconnect()
    )
    print("PING response:", pong, flush=True)
    await conn.disconnect()


async def _ping(conn):
    await conn.send_command("PING")
    return await conn.read_response()


if __name__ == "__main__":
    asyncio.run(main())
