"""
Repro for https://github.com/redis/redis-py/issues/3741

Reporter's config (async redis-om on top of redis-py):
    socket_keepalive=True
    socket_connect_timeout=15
    socket_timeout=5
    retry=Retry(ExponentialBackoff(cap=10, base=1), 25)
    retry_on_error=[ConnectionError, TimeoutError, ConnectionResetError]
    health_check_interval=5

Observed error (from debug logs in the issue):
    ConnectionResetError: [Errno 104] Connection reset by peer
raised while the client was (re)establishing a connection -- i.e. during
the handshake, not while writing the user's actual command.

petyaslavova's diagnosis: get_connection() opens a new socket and runs
handshake commands (AUTH / HELLO / CLIENT SETINFO etc.) to configure it;
failures during that handshake step were NOT covered by the retry policy,
even though the client was configured with 25 retries.

petyaslavova's claim to verify: PR #3863 wrapped the whole
connect+handshake flow in `retry.call_with_retry`, so this should now
retry per the configured policy instead of raising on the first reset.
"""

from errno import ECONNRESET
from unittest.mock import patch

import pytest
from redis.asyncio.connection import Connection
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff, NoBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
import asyncio
from errno import ECONNRESET
from unittest.mock import patch


RESETS_BEFORE_SUCCESS = 2


@pytest.mark.fixed_client
async def test_3741_handshake_reset_retries_using_reporter_config():
    """
    Simulate a ConnectionResetError happening mid-handshake, using the
    reporter's own retry configuration. Before PR #3863, this raised
    immediately on the first reset. After the fix, it should retry and
    succeed once the transient failure clears.
    """
    # NOTE: health_check_interval is set (matching the reporter's config),
    # which means check_health() fires a PING before the first handshake
    # command. That PING already goes through its own independent
    # `retry.call_with_retry` (see Connection.check_health) -- a mechanism
    # that predates PR #3863 and was never broken. To isolate the actual
    # bug (handshake commands like HELLO/AUTH not being retried), we must
    # only fail the HELLO write specifically, not just "the first N writes".
    resets_raised = {"n": 0}
    real_writelines = None

    def flaky_writelines(self, data):
        chunk = data[0] if isinstance(data, (list, tuple)) else data
        # default protocol is RESP2, so no HELLO is sent -- the first
        # handshake command actually written is CLIENT SETINFO LIB-NAME
        is_handshake_cmd = b"LIB-NAME" in chunk
        if is_handshake_cmd and resets_raised["n"] < RESETS_BEFORE_SUCCESS:
            resets_raised["n"] += 1
            raise ConnectionResetError(ECONNRESET, "Connection reset by peer")
        return real_writelines(self, data)

    import asyncio

    real_writelines = asyncio.StreamWriter.writelines

    with patch.object(
        asyncio.StreamWriter, "writelines", new=flaky_writelines
    ):
        conn = Connection(
            port=6399,
            socket_keepalive=True,
            socket_connect_timeout=15,
            socket_timeout=5,
            retry=Retry(ExponentialBackoff(cap=10, base=1), 25),
            retry_on_error=[RedisConnectionError, ConnectionResetError],
            health_check_interval=5,
        )
        await conn.connect()

        # If the handshake retry gap from #3741 were still open, the very
        # first ConnectionResetError would have propagated out of connect()
        # and this line would never be reached.
        assert conn.is_connected
        assert resets_raised["n"] == RESETS_BEFORE_SUCCESS, (
            "expected exactly 2 handshake resets before recovery, got "
            f"{resets_raised['n']}"
        )
        await conn.disconnect()


# (bytes that identify the target handshake command on the wire,
#  extra Connection kwargs needed to make that command actually get sent)
HANDSHAKE_FAULT_CASES = [
    pytest.param(b"HELLO", {"protocol": 3}, id="hello"),
    pytest.param(b"SELECT", {"db": 1}, id="select"),
    pytest.param(b"LIB-NAME", {}, id="client-setinfo"),
]


@pytest.mark.fixed_client
@pytest.mark.parametrize("trigger_bytes, extra_conn_kwargs", HANDSHAKE_FAULT_CASES)
async def test_3741_handshake_reset_retries_parametrized(
    trigger_bytes, extra_conn_kwargs
):
    """
    Parametrized #3741 repro: inject a ConnectionResetError on each
    handshake command in turn (HELLO, AUTH, SELECT, CLIENT SETINFO) and
    confirm the connection retries and recovers no matter which handshake
    step is the one that gets reset.
    """
    resets_raised = {"n": 0}
    real_writelines = asyncio.StreamWriter.writelines

    def flaky_writelines(self, data):
        chunk = data[0] if isinstance(data, (list, tuple)) else data
        if trigger_bytes in chunk and resets_raised["n"] < RESETS_BEFORE_SUCCESS:
            resets_raised["n"] += 1
            raise ConnectionResetError(ECONNRESET, "Connection reset by peer")
        return real_writelines(self, data)

    with patch.object(asyncio.StreamWriter, "writelines", new=flaky_writelines):
        conn = Connection(
            port=6399,
            socket_keepalive=True,
            socket_connect_timeout=15,
            socket_timeout=5,
            retry=Retry(ExponentialBackoff(cap=10, base=1), 25),
            retry_on_error=[RedisConnectionError, ConnectionResetError],
            health_check_interval=5,
            **extra_conn_kwargs,
        )
        await conn.connect()

        assert conn.is_connected
        assert resets_raised["n"] == RESETS_BEFORE_SUCCESS, (
            f"expected exactly {RESETS_BEFORE_SUCCESS} resets on "
            f"{trigger_bytes!r} before recovery, got {resets_raised['n']}"
        )
        await conn.disconnect()


@pytest.mark.fixed_client
async def test_3741_handshake_reset_exhausts_configured_retries():
    """
    Sanity check on the other edge: if the reset never clears, the client
    should still eventually give up with a clean ConnectionError, having
    made the number of attempts the retry policy allows (not just 1).
    """
    with patch.object(
        __import__("asyncio").StreamWriter, "writelines"
    ) as writelines:
        writelines.side_effect = ConnectionResetError(
            ECONNRESET, "Connection reset by peer"
        )
        conn = Connection(
            port=6399,
            retry=Retry(NoBackoff(), 4),
            retry_on_error=[RedisConnectionError, ConnectionResetError],
        )
        with pytest.raises(RedisConnectionError):
            await conn.connect()

        assert writelines.call_count == 5
