"""Async integration tests for the HIMPORT command family against a real server.

Mirrors ``tests/test_himport_integration.py``. Runs against standalone (single
and pooled) under ``invoke standalone-tests`` and cluster under
``invoke cluster-tests``. HIMPORT was introduced in Redis 8.9.0, so the whole
module is gated on that server version.
"""

import contextlib
from unittest import mock

import pytest
import pytest_asyncio

import redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from tests.conftest import skip_if_server_version_lt

SCHEMAS = {"shared": ["name", "email", "age"]}

# HIMPORT (Hinted Hash Templates) landed in Redis 8.9.0; gate the whole module.
pytestmark = skip_if_server_version_lt("8.9.0")


def _himport_verb(parts):
    """Return the HIMPORT subcommand for a command's leading tokens, or ``None``.

    Handles both wire forms: the joined ``"HIMPORT SET"`` produced by
    ``execute_command`` and the split ``("HIMPORT", "SET")`` produced by the
    command builders used in the bundled path.
    """
    joined = " ".join(str(p) for p in list(parts)[:2])
    for verb in ("PREPARE", "SET", "DISCARDALL", "DISCARD"):
        if joined.startswith(f"HIMPORT {verb}"):
            return verb
    return None


@contextlib.asynccontextmanager
async def track_himport_wire(conn):
    """Record how each HIMPORT write reached the socket, in order.

    Wraps ``conn``'s write primitives (calling through to the real ones, so the
    exchange still happens for real) and yields a list of markers:

    - ``"PREPARE+SET"`` — lazy PREPARE bundled with the SET in one packed write
    - ``"SET"``         — bare SET (fieldset already prepared on this connection)
    - ``"PREPARE"`` / ``"DISCARD"`` — a standalone command
    - ``"DISCARD*N"``   — a deferred-discard reconcile of ``N`` fieldsets
    """
    events = []
    real_pack = conn.pack_commands
    real_send = conn.send_command

    def pack(commands, *args, **kwargs):
        verbs = [_himport_verb(cmd) for cmd in commands]
        if "PREPARE" in verbs:
            events.append("PREPARE+SET")
        elif verbs and all(v == "DISCARD" for v in verbs):
            events.append(f"DISCARD*{len(verbs)}")
        return real_pack(commands, *args, **kwargs)

    async def send(*args, **kwargs):
        verb = _himport_verb(args)
        if verb is not None:
            events.append(verb)
        return await real_send(*args, **kwargs)

    with (
        mock.patch.object(conn, "pack_commands", new=pack),
        mock.patch.object(conn, "send_command", new=send),
    ):
        yield events


@pytest_asyncio.fixture()
async def hr(create_redis):
    """A client (single/pooled standalone, or cluster) with a declared HIMPORT schema."""
    client = await create_redis()
    # Populate the registry directly (mirrors what init schemas did) without an eager
    # PREPARE, so lazy-bundling wire assertions in the tests still hold.
    client.himport_registry.prepare("shared", SCHEMAS["shared"])
    return client


async def test_runtime_prepare_without_init_schemas(create_redis):
    """A client that declares nothing up front can still declare + use fieldsets."""
    client = await create_redis()
    await client.himport_prepare("ronly", ["a", "b"])
    await client.delete("r:{u}:1")
    await client.himport_set("r:{u}:1", "ronly", ["1", "2"])
    assert await client.hget("r:{u}:1", "b") == b"2"


@pytest.mark.onlynoncluster
async def test_disconnect_clears_prepared_state(create_redis):
    """A disconnect drops the server session; prepared state is cleared and the
    next himport_set transparently re-prepares."""
    client = await create_redis(single_connection_client=True)
    client.himport_registry.prepare("shared", SCHEMAS["shared"])
    conn = client.connection
    async with track_himport_wire(conn) as events:
        # First use of the fieldset bundles PREPARE with SET; the second reuses
        # the already-prepared connection and sends bare.
        await client.himport_set("d:{u}:1", "shared", ["a", "a@x", "1"])
        await client.himport_set("d:{u}:x", "shared", ["c", "c@x", "3"])
    assert events == ["PREPARE+SET", "SET"]
    assert "shared" in conn._himport_prepared
    await conn.disconnect()
    assert conn._himport_prepared == {}
    # The reconnected socket has no session state, so the next set re-bundles.
    async with track_himport_wire(conn) as events:
        await client.himport_set("d:{u}:2", "shared", ["b", "b@x", "2"])
    assert events == ["PREPARE+SET"]
    assert await client.hget("d:{u}:2", "name") == b"b"


@pytest.mark.onlynoncluster
async def test_discard_on_disconnected_single_connection(create_redis):
    """Discarding on a single-connection client whose socket is not connected
    updates the registry without reconnecting or issuing a server DISCARD (a fresh
    connection re-prepares lazily on the next himport_set)."""
    client = await create_redis(single_connection_client=True)
    await client.himport_prepare("tmp", ["a", "b"])
    await client.himport_set("z:{u}:1", "tmp", ["1", "2"])
    conn = client.connection
    assert "tmp" in conn._himport_prepared
    await conn.disconnect()
    assert conn.is_connected is False
    # While disconnected the branch must skip the wire DISCARD entirely: spy the
    # internal command to prove it is never issued, and confirm the socket stays
    # closed (a DISCARD would have reconnected it).
    with mock.patch.object(client, "himport_discard_internal") as discard_wire:
        assert await client.himport_discard("tmp") == 1
        discard_wire.assert_not_called()
    assert conn.is_connected is False
    assert "tmp" not in client.himport_registry
    # A later himport_set still works: the prepare (while still disconnected)
    # sends nothing, and the set reconnects and bundles PREPARE lazily.
    async with track_himport_wire(conn) as events:
        await client.himport_prepare("tmp2", ["c", "d"])
        await client.himport_set("z:{u}:2", "tmp2", ["3", "4"])
    assert events == ["PREPARE+SET"]
    assert await client.hget("z:{u}:2", "d") == b"4"


@pytest.mark.onlynoncluster
async def test_himport_set_retries_and_reprepares(create_redis):
    """himport_set uses the normal command path, so a retryable error triggers the
    standard reconnect-and-retry: the dropped socket clears the prepared state and
    the retry re-prepares the fieldset before the SET."""
    client = await create_redis(
        single_connection_client=True,
        retry=Retry(NoBackoff(), 1),
    )
    client.himport_registry.prepare("shared", SCHEMAS["shared"])
    # Warm the pinned connection so "shared" is already prepared on it.
    await client.himport_set("h:{u}:warm", "shared", ["w", "w@x", "0"])
    conn = client.connection
    real_parse = client.parse_response
    injected = {"done": False}

    async def flaky(connection, command_name, **options):
        # Drop the connection once, while reading the first SET reply.
        if command_name == "HIMPORT SET" and not injected["done"]:
            injected["done"] = True
            raise redis.ConnectionError("simulated drop")
        return await real_parse(connection, command_name, **options)

    with mock.patch.object(client, "parse_response", side_effect=flaky):
        assert await client.himport_set("h:{u}:r", "shared", ["a", "a@x", "1"]) is True
    assert injected["done"] is True  # the failure actually fired
    # The command still succeeded and the reconnected socket was re-prepared.
    assert await client.hget("h:{u}:r", "email") == b"a@x"
    assert "shared" in conn._himport_prepared


async def test_prepare_set_readback(hr):
    await hr.himport_set("h:{u}:1", "shared", ["alice", "alice@x", "25"])
    await hr.himport_set("h:{u}:2", "shared", ["bob", "bob@x", "30"])
    assert await hr.hget("h:{u}:1", "name") == b"alice"
    assert await hr.hget("h:{u}:2", "email") == b"bob@x"
    assert await hr.hgetall("h:{u}:1") == {
        b"name": b"alice",
        b"email": b"alice@x",
        b"age": b"25",
    }


async def test_positional_value_mapping(hr):
    await hr.himport_set("h:{u}:p", "shared", ["x", "y", "99"])
    assert await hr.hget("h:{u}:p", "age") == b"99"


async def test_create_or_replace(hr):
    await hr.himport_set("h:{u}:r", "shared", ["a", "a@x", "1"])
    await hr.himport_set("h:{u}:r", "shared", ["b", "b@x", "2"])
    assert await hr.hget("h:{u}:r", "name") == b"b"
    assert await hr.hget("h:{u}:r", "age") == b"2"


async def test_wrongtype_on_non_hash_key(hr):
    await hr.set("h:{u}:str", "notahash")
    with pytest.raises(redis.ResponseError):
        await hr.himport_set("h:{u}:str", "shared", ["a", "a@x", "1"])


async def test_value_count_mismatch_propagates(hr):
    with pytest.raises(redis.ResponseError):
        await hr.himport_set("h:{u}:bad", "shared", ["only-one-value"])


async def test_runtime_prepare_then_set(hr):
    assert await hr.himport_prepare("ord", ["a", "b", "c"]) is True
    await hr.himport_set("h:{u}:o", "ord", ["va", "vb", "vc"])
    assert await hr.hget("h:{u}:o", "b") == b"vb"


async def test_discard_runtime(hr):
    await hr.himport_prepare("tmp", ["f1", "f2"])
    await hr.himport_set("h:{u}:t", "tmp", ["1", "2"])
    assert await hr.himport_discard("tmp") == 1
    assert await hr.himport_discard("tmp") == 0
    assert await hr.hget("h:{u}:t", "f1") == b"1"


async def test_pool_churn_lazy_prepare(hr):
    n = 20
    for i in range(n):
        await hr.himport_set(f"h:{{u}}:c{i}", "shared", [f"n{i}", f"e{i}@x", str(i)])
    for i in range(n):
        assert await hr.hget(f"h:{{u}}:c{i}", "name") == f"n{i}".encode()


async def test_pipeline_non_transaction(hr):
    # A pipeline bypasses the per-command lazy prepare; the fieldset is prepared on
    # the pipeline connection as a pre-flight before the batch.
    pipe = hr.pipeline(transaction=False)
    pipe.himport_set("h:{u}:np1", "shared", ["a", "a@x", "1"])
    pipe.himport_set("h:{u}:np2", "shared", ["b", "b@x", "2"])
    pipe.hget("h:{u}:np1", "name")
    result = await pipe.execute()
    assert not any(isinstance(r, Exception) for r in result)
    assert result[-1] == b"a"
    assert await hr.hget("h:{u}:np2", "email") == b"b@x"


async def test_pipeline_transaction(hr):
    # Same fieldset inside MULTI/EXEC; PREPARE is pre-flighted before MULTI.
    pipe = hr.pipeline(transaction=True)
    pipe.himport_set("h:{u}:tp1", "shared", ["c", "c@x", "3"])
    pipe.himport_set("h:{u}:tp2", "shared", ["d", "d@x", "4"])
    pipe.hget("h:{u}:tp1", "age")
    result = await pipe.execute()
    assert not any(isinstance(r, Exception) for r in result)
    assert result[-1] == b"3"
    assert await hr.hget("h:{u}:tp2", "name") == b"d"


async def test_pipeline_runtime_prepared_fieldset(hr):
    # A fieldset declared at runtime is also pre-flighted for the pipeline.
    await hr.himport_prepare("pl", ["x", "y"])
    pipe = hr.pipeline(transaction=False)
    pipe.himport_set("h:{u}:pl1", "pl", ["1", "2"])
    result = await pipe.execute()
    assert not any(isinstance(r, Exception) for r in result)
    assert await hr.hget("h:{u}:pl1", "y") == b"2"


@pytest.mark.onlynoncluster
async def test_himport_set_recovers_when_fieldset_lost_midconnection(create_redis):
    """If the server drops a prepared fieldset mid-connection (e.g. RESET or
    maxmemory-clients eviction) without dropping the socket, the next himport_set
    re-prepares on the same connection and retries the SET once — no reconnect."""
    client = await create_redis(single_connection_client=True)
    client.himport_registry.prepare("shared", SCHEMAS["shared"])
    await client.himport_set("rst:{u}:1", "shared", ["a", "a@x", "1"])
    conn = client.connection
    assert "shared" in conn._himport_prepared
    # Drop the fieldset server-side on this exact socket without touching the
    # client's tracking, simulating a mid-connection loss.
    await client.execute_command("HIMPORT", "DISCARD", "shared")
    assert "shared" in conn._himport_prepared  # client still believes it exists
    assert conn.is_connected is True
    async with track_himport_wire(conn) as events:
        assert (
            await client.himport_set("rst:{u}:2", "shared", ["b", "b@x", "2"]) is True
        )
    # The bare SET fails with NoSuchFieldsetError, then a PREPARE+SET recovers it.
    assert events == ["SET", "PREPARE+SET"]
    assert conn.is_connected is True  # recovery never reconnected
    assert await client.hget("rst:{u}:2", "name") == b"b"


@pytest.mark.onlycluster
async def test_cluster_multi_slot(hr):
    keys = [f"h:{{s{i}}}:k" for i in range(8)]
    for i, k in enumerate(keys):
        await hr.himport_set(k, "shared", [f"n{i}", f"e{i}@x", str(i)])
    for i, k in enumerate(keys):
        assert await hr.hget(k, "name") == f"n{i}".encode()
