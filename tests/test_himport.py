import asyncio
import inspect
import threading
from unittest import mock

import pytest

import redis.asyncio.cluster as async_cluster_mod
import redis.cluster as cluster_mod
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
from redis.connection import BlockingConnectionPool, ConnectionPool
from redis._parsers.base import BaseParser
from redis.exceptions import DataError, NoSuchFieldsetError, ResponseError
from redis.himport import HIMPORT_SET, FieldsetOrigin, HImportConfig, HImportFieldset
from redis.sentinel import Sentinel, SentinelConnectionPool


@pytest.mark.fixed_client
class TestHImportConfig:
    # -- construction -----------------------------------------------------

    def test_empty_by_default(self):
        cfg = HImportConfig()
        assert len(cfg) == 0
        assert cfg.names() == []
        assert cfg.items() == []
        assert list(cfg) == []
        assert cfg.get("missing") is None
        assert "missing" not in cfg

    def test_none_schemas_is_empty(self):
        assert len(HImportConfig(None)) == 0

    def test_constructor_entries_are_init(self):
        cfg = HImportConfig({"shared": ["name", "email", "age"]})
        fs = cfg.get("shared")
        assert isinstance(fs, HImportFieldset)
        assert fs.name == "shared"
        assert fs.fields == ("name", "email", "age")
        assert fs.origin is FieldsetOrigin.INIT
        assert "shared" in cfg
        assert cfg.names() == ["shared"]

    def test_multiple_constructor_entries(self):
        cfg = HImportConfig({"a": ["x"], "b": ["y", "z"]})
        assert len(cfg) == 2
        assert {name for name, _ in cfg.items()} == {"a", "b"}
        assert all(fs.origin is FieldsetOrigin.INIT for _, fs in cfg.items())

    # -- field-order fidelity --------------------------------------------

    def test_field_order_preserved(self):
        cfg = HImportConfig()
        cfg.prepare("fs", ["c", "a", "b"])
        assert cfg.get("fs").fields == ("c", "a", "b")

    def test_duplicates_not_deduplicated(self):
        # The server rejects duplicate field names; the client must not silently
        # deduplicate and hide that error.
        cfg = HImportConfig()
        cfg.prepare("fs", ["a", "a", "b"])
        assert cfg.get("fs").fields == ("a", "a", "b")

    def test_accepts_various_iterables(self):
        cfg = HImportConfig()
        cfg.prepare("tuple", ("a", "b"))
        cfg.prepare("gen", (f"f{i}" for i in range(3)))
        assert cfg.get("tuple").fields == ("a", "b")
        assert cfg.get("gen").fields == ("f0", "f1", "f2")

    def test_empty_string_names_and_fields_allowed(self):
        # Empty strings are valid fieldset names and field names per the HLD;
        # the client must not reject them locally.
        cfg = HImportConfig({"": [""]})
        assert cfg.get("").fields == ("",)
        cfg.prepare("fs", ["", "x", ""])
        assert cfg.get("fs").fields == ("", "x", "")

    # -- validation -------------------------------------------------------

    def test_empty_field_list_rejected(self):
        cfg = HImportConfig()
        with pytest.raises(DataError):
            cfg.prepare("fs", [])

    def test_empty_field_list_rejected_in_constructor(self):
        with pytest.raises(DataError):
            HImportConfig({"fs": []})

    @pytest.mark.parametrize("bad", ["name", b"name"])
    def test_string_fields_rejected(self, bad):
        # A bare string would iterate character-by-character; reject it rather
        # than silently register single-character fields.
        cfg = HImportConfig()
        with pytest.raises(DataError):
            cfg.prepare("fs", bad)

    # -- versioning -------------------------------------------------------

    def test_versions_are_monotonic(self):
        cfg = HImportConfig()
        v1 = cfg.prepare("a", ["x"]).version
        v2 = cfg.prepare("b", ["y"]).version
        assert v2 > v1

    def test_replace_bumps_version_and_updates_fields(self):
        cfg = HImportConfig()
        first = cfg.prepare("fs", ["a"])
        second = cfg.prepare("fs", ["a", "b"])
        assert second.version > first.version
        assert cfg.get("fs").fields == ("a", "b")
        assert len(cfg) == 1

    def test_readd_after_discard_gets_fresh_version(self):
        cfg = HImportConfig()
        first = cfg.prepare("fs", ["a"])
        assert cfg.discard("fs") is True
        reAdded = cfg.prepare("fs", ["a"])
        assert reAdded.version > first.version

    # -- revision (mutation clock) ---------------------------------------

    def test_revision_starts_at_zero(self):
        assert HImportConfig().revision == 0

    def test_revision_advances_on_prepare(self):
        cfg = HImportConfig()
        before = cfg.revision
        cfg.prepare("fs", ["a"])
        assert cfg.revision > before

    def test_revision_advances_on_discard(self):
        cfg = HImportConfig()
        cfg.prepare("fs", ["a"])
        before = cfg.revision
        cfg.discard("fs")
        assert cfg.revision > before

    def test_revision_advances_on_discard_all(self):
        cfg = HImportConfig()
        cfg.prepare("a", ["x"])
        cfg.prepare("b", ["y"])
        before = cfg.revision
        cfg.discard_all()
        assert cfg.revision > before

    def test_revision_unchanged_on_noop_discard(self):
        cfg = HImportConfig()
        before = cfg.revision
        assert cfg.discard("missing") is False
        assert cfg.revision == before

    def test_revision_unchanged_on_empty_discard_all(self):
        cfg = HImportConfig()
        before = cfg.revision
        assert cfg.discard_all() == 0
        assert cfg.revision == before

    def test_revision_unchanged_when_discard_init_raises(self):
        cfg = HImportConfig({"fs": ["a"]})
        before = cfg.revision
        with pytest.raises(DataError):
            cfg.discard("fs")
        assert cfg.revision == before

    # -- names_to_discard -------------------------------------------------

    def test_names_to_discard_returns_removed_names(self):
        cfg = HImportConfig()
        cfg.prepare("a", ["x"])
        cfg.prepare("b", ["y"])
        cfg.discard("a")
        # Connection had prepared "a" and "b"; only "a" is now gone.
        assert cfg.names_to_discard(["a", "b"]) == ["a"]

    def test_names_to_discard_empty_when_all_registered(self):
        cfg = HImportConfig({"a": ["x"], "b": ["y"]})
        assert cfg.names_to_discard(["a", "b"]) == []

    def test_names_to_discard_preserves_input_order(self):
        cfg = HImportConfig()
        assert cfg.names_to_discard(["c", "a", "b"]) == ["c", "a", "b"]

    def test_names_to_discard_ignores_readd(self):
        # A name discarded then re-declared is still registered, so it is not
        # flagged for discard (the version bump handles re-prepare instead).
        cfg = HImportConfig()
        cfg.prepare("fs", ["a"])
        cfg.discard("fs")
        cfg.prepare("fs", ["a", "b"])
        assert cfg.names_to_discard(["fs"]) == []

    # -- origin semantics -------------------------------------------------

    def test_prepare_new_name_is_runtime(self):
        cfg = HImportConfig()
        assert cfg.prepare("fs", ["a"]).origin is FieldsetOrigin.RUNTIME

    def test_reprepare_preserves_init_origin(self):
        # Re-preparing an init fieldset must keep it INIT so discard protection
        # cannot be bypassed.
        cfg = HImportConfig({"fs": ["a"]})
        updated = cfg.prepare("fs", ["a", "b"])
        assert updated.origin is FieldsetOrigin.INIT
        with pytest.raises(DataError):
            cfg.discard("fs")

    def test_reprepare_preserves_runtime_origin(self):
        cfg = HImportConfig()
        cfg.prepare("fs", ["a"])
        assert cfg.prepare("fs", ["a", "b"]).origin is FieldsetOrigin.RUNTIME

    # -- discard ----------------------------------------------------------

    def test_discard_runtime_removes_and_returns_true(self):
        cfg = HImportConfig()
        cfg.prepare("fs", ["a"])
        assert cfg.discard("fs") is True
        assert "fs" not in cfg
        assert len(cfg) == 0

    def test_discard_unknown_returns_false(self):
        cfg = HImportConfig()
        assert cfg.discard("missing") is False

    def test_discard_init_raises_and_keeps_entry(self):
        cfg = HImportConfig({"fs": ["a"]})
        with pytest.raises(DataError):
            cfg.discard("fs")
        assert "fs" in cfg

    # -- discard_all ------------------------------------------------------

    def test_discard_all_removes_runtime_and_returns_count(self):
        cfg = HImportConfig()
        cfg.prepare("a", ["x"])
        cfg.prepare("b", ["y"])
        assert cfg.discard_all() == 2
        assert len(cfg) == 0

    def test_discard_all_on_empty_returns_zero(self):
        assert HImportConfig().discard_all() == 0

    def test_discard_all_rejected_when_init_present(self):
        cfg = HImportConfig({"init": ["a"]})
        cfg.prepare("runtime", ["b"])
        with pytest.raises(DataError):
            cfg.discard_all()
        # Registry left untouched by the rejection.
        assert len(cfg) == 2
        assert "init" in cfg
        assert "runtime" in cfg

    # -- read-only access / immutability ----------------------------------

    def test_fieldset_is_immutable(self):
        cfg = HImportConfig({"fs": ["a"]})
        fs = cfg.get("fs")
        with pytest.raises(Exception):
            fs.fields = ("b",)

    def test_items_snapshot_does_not_mutate_registry(self):
        cfg = HImportConfig({"fs": ["a"]})
        items = cfg.items()
        items.clear()
        assert "fs" in cfg

    def test_repr_lists_entries(self):
        cfg = HImportConfig({"fs": ["a", "b"]})
        text = repr(cfg)
        assert "fs" in text
        assert "INIT" in text

    def test_concurrent_prepares_do_not_lose_revision_bumps(self):
        # A sync Redis instance (and its shared HImportConfig) is routinely used
        # across threads. Each prepare of a distinct name must advance the revision
        # exactly once; the mutation lock keeps the read-modify-write from racing and
        # collapsing bumps, which would let a connection skip a reconcile.
        cfg = HImportConfig()
        count = 200
        barrier = threading.Barrier(count)

        def worker(i):
            barrier.wait()  # maximize contention on the mutation path
            cfg.prepare(f"fs{i}", ["a"])

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(cfg) == count
        assert cfg.revision == count  # no bump lost to a race


class _RecordingConn:
    """Minimal stand-in for a connection carrying HIMPORT state.

    ``pack_commands`` / ``send_packed_command`` are no-ops (the client's
    ``parse_response`` is mocked to supply the replies), so the batched
    DISCARD/PREPARE read loops can be exercised without a real socket.
    """

    def __init__(self, cfg, prepared=None):
        self.himport_config = cfg
        self._himport_prepared = dict(prepared or {})
        self._himport_reconciled_revision = 0

    def pack_commands(self, commands):
        return list(commands)

    def send_packed_command(self, packed, **kwargs):
        pass


class _AsyncRecordingConn(_RecordingConn):
    async def send_packed_command(self, packed, **kwargs):
        pass


class _CapturingConn(_RecordingConn):
    """Recording connection that captures the wire writes in order.

    ``writes`` records ``("packed", [cmd, ...])`` for each ``send_packed_command``
    and ``("cmd", (args...))`` for each ``send_command`` so tests can assert the
    exact command ordering (e.g. that ASKING is packed immediately before the SET).
    """

    def __init__(self, cfg, prepared=None):
        super().__init__(cfg, prepared)
        self.writes = []

    def send_packed_command(self, packed, **kwargs):
        self.writes.append(("packed", list(packed)))

    def send_command(self, *args, **kwargs):
        self.writes.append(("cmd", args))


class _AsyncCapturingConn(_CapturingConn):
    async def send_packed_command(self, packed, **kwargs):
        self.writes.append(("packed", list(packed)))

    async def send_command(self, *args, **kwargs):
        self.writes.append(("cmd", args))


@pytest.mark.fixed_client
class TestHImportBatchedReadDrain:
    """A per-command ResponseError inside a packed DISCARD/PREPARE batch must not
    leave replies unread — that would desync the pooled connection. Each loop drains
    every reply, marks only the ones that succeeded, then surfaces the first error.
    """

    def test_reconcile_discards_drains_all_replies_on_error(self):
        # prepare+discard advances the revision so the connection reconciles; it has
        # three stale prepared fieldsets, so three DISCARD replies must be read even
        # though the second one errors.
        cfg = HImportConfig()
        cfg.prepare("a", ["f"])
        cfg.discard("a")
        conn = _RecordingConn(cfg, {"a": 1, "b": 1, "c": 1})
        client = Redis()
        replies = [True, ResponseError("boom"), True]
        with mock.patch.object(client, "parse_response", side_effect=replies) as pr:
            with pytest.raises(ResponseError, match="boom"):
                client._himport_reconcile_discards(conn)
        assert pr.call_count == 3  # every packed DISCARD reply drained
        assert conn._himport_prepared == {}  # every stale name dropped regardless

    def test_prepare_pipeline_drains_all_replies_on_error(self):
        cfg = HImportConfig({"a": ["f"], "b": ["f"]})
        conn = _RecordingConn(cfg)
        conn._himport_reconciled_revision = cfg.revision  # no discard to reconcile
        commands = [
            (("HIMPORT SET", "k1", "a", "v"), {}),
            (("HIMPORT SET", "k2", "b", "v"), {}),
        ]
        assert commands[0][0][0] == HIMPORT_SET  # guards the wire-token assumption
        pipe = Redis().pipeline()
        replies = [True, ResponseError("boom")]
        with mock.patch.object(pipe, "parse_response", side_effect=replies) as pr:
            with pytest.raises(ResponseError, match="boom"):
                pipe._himport_prepare_pipeline(conn, commands)
        assert pr.call_count == 2  # both PREPARE replies drained
        assert "a" in conn._himport_prepared  # first PREPARE succeeded, marked
        assert "b" not in conn._himport_prepared  # second errored, not marked

    def test_async_reconcile_discards_drains_all_replies_on_error(self):
        async def run():
            cfg = HImportConfig()
            cfg.prepare("a", ["f"])
            cfg.discard("a")
            conn = _AsyncRecordingConn(cfg, {"a": 1, "b": 1, "c": 1})
            client = AsyncRedis()
            client.parse_response = mock.AsyncMock(
                side_effect=[True, ResponseError("boom"), True]
            )
            with pytest.raises(ResponseError, match="boom"):
                await client._himport_reconcile_discards(conn)
            assert client.parse_response.call_count == 3
            assert conn._himport_prepared == {}

        asyncio.run(run())

    def test_async_prepare_pipeline_drains_all_replies_on_error(self):
        async def run():
            cfg = HImportConfig({"a": ["f"], "b": ["f"]})
            conn = _AsyncRecordingConn(cfg)
            conn._himport_reconciled_revision = cfg.revision
            commands = [
                (("HIMPORT SET", "k1", "a", "v"), {}),
                (("HIMPORT SET", "k2", "b", "v"), {}),
            ]
            pipe = AsyncRedis().pipeline()
            pipe.parse_response = mock.AsyncMock(
                side_effect=[True, ResponseError("boom")]
            )
            with pytest.raises(ResponseError, match="boom"):
                await pipe._himport_prepare_pipeline(conn, commands)
            assert pipe.parse_response.call_count == 2
            assert "a" in conn._himport_prepared
            assert "b" not in conn._himport_prepared

        asyncio.run(run())


PREPARE_FS_AB = ("HIMPORT", "PREPARE", "fs", "a", "b")
ASKING = ("ASKING",)
SET_K_FS = ("HIMPORT", "SET", "k", "fs", "v")


DISCARD_GONE = ("HIMPORT", "DISCARD", "gone")


@pytest.mark.fixed_client
class TestHImportAskRedirectSync:
    """An ASK-redirected ``HIMPORT SET`` must carry the ASKING allowance on the
    same connection, immediately before the SET.

    Redis clears the per-command ASKING flag after the next command, so any
    session command (deferred DISCARD, lazy PREPARE) sent between ASKING and the
    SET would consume the allowance and leave the slot-scoped SET to fail or
    redirect again during migration. On the sync cluster the dispatch loop runs
    ``RedisCluster._himport_prepare_for_asking`` (reconcile + lazy PREPARE) before
    sending ASKING, so ASKING lands right before the bare SET the node then issues.
    ``_himport_prepare_for_asking`` ignores ``self``; call it unbound for the test.
    """

    def test_prepare_for_asking_sends_bare_prepare(self):
        cfg = HImportConfig({"fs": ["a", "b"]})
        conn = _CapturingConn(cfg)
        conn._himport_reconciled_revision = cfg.revision  # nothing to reconcile
        redis_node = Redis()
        with mock.patch.object(redis_node, "parse_response", side_effect=["OK"]) as pr:
            cluster_mod.RedisCluster._himport_prepare_for_asking(
                None, redis_node, conn, "fs"
            )
        # A single bare PREPARE (the caller sends ASKING itself, right after).
        assert conn.writes == [("cmd", PREPARE_FS_AB)]
        assert conn._himport_prepared["fs"] == cfg.get("fs").version
        assert pr.call_count == 1

    def test_prepare_for_asking_noop_when_already_prepared(self):
        cfg = HImportConfig({"fs": ["a"]})
        fieldset = cfg.get("fs")
        conn = _CapturingConn(cfg, prepared={"fs": fieldset.version})
        conn._himport_reconciled_revision = cfg.revision
        redis_node = Redis()
        cluster_mod.RedisCluster._himport_prepare_for_asking(
            None, redis_node, conn, "fs"
        )
        assert conn.writes == []  # nothing sent before ASKING

    def test_prepare_for_asking_reconciles_discards_first(self):
        # A stale prepared fieldset is DISCARDed (session command, no allowance
        # needed) ahead of everything; the already-prepared target needs no PREPARE.
        cfg = HImportConfig({"fs": ["a"]})
        cfg.prepare("gone", ["x"])
        cfg.discard("gone")
        fieldset = cfg.get("fs")
        conn = _CapturingConn(cfg, prepared={"gone": 1, "fs": fieldset.version})
        conn._himport_reconciled_revision = 0  # force a reconcile pass
        redis_node = Redis()
        with mock.patch.object(redis_node, "parse_response", side_effect=[1]):
            cluster_mod.RedisCluster._himport_prepare_for_asking(
                None, redis_node, conn, "fs"
            )
        assert conn.writes == [("packed", [DISCARD_GONE])]
        assert "gone" not in conn._himport_prepared


@pytest.mark.fixed_client
class TestHImportAskRedirectAsyncCluster:
    """The async cluster owns its own HIMPORT executor on ``ClusterNode``; there
    the ASKING allowance is folded into the SET's own packed write (the ASKING is
    not sent as a separately pooled command), so ASKING lands immediately before
    the SET. The methods only touch ``self.parse_response`` /
    ``self._himport_reconcile_discards``; a stub ``self`` exercises the real code.
    """

    def test_prepare_and_set_packs_prepare_asking_set(self):
        async def run():
            cfg = HImportConfig({"fs": ["a", "b"]})
            conn = _AsyncCapturingConn(cfg)
            fieldset = cfg.get("fs")
            node = mock.Mock()
            node.parse_response = mock.AsyncMock(side_effect=["OK", "OK", 1])
            resp = await async_cluster_mod.ClusterNode._himport_prepare_and_set(
                node, conn, "k", "fs", ["v"], fieldset, asking=True
            )
            assert resp == 1
            assert conn.writes == [("packed", [PREPARE_FS_AB, ASKING, SET_K_FS])]
            assert conn._himport_prepared["fs"] == fieldset.version
            assert node.parse_response.call_count == 3

        asyncio.run(run())

    def test_prepare_and_set_without_asking_keeps_bundle(self):
        # Regression guard: without an ASK redirect the batch stays [PREPARE, SET].
        async def run():
            cfg = HImportConfig({"fs": ["a", "b"]})
            conn = _AsyncCapturingConn(cfg)
            fieldset = cfg.get("fs")
            node = mock.Mock()
            node.parse_response = mock.AsyncMock(side_effect=["OK", 1])
            await async_cluster_mod.ClusterNode._himport_prepare_and_set(
                node, conn, "k", "fs", ["v"], fieldset
            )
            assert conn.writes == [("packed", [PREPARE_FS_AB, SET_K_FS])]

        asyncio.run(run())

    def test_bare_set_packs_asking_then_set(self):
        async def run():
            cfg = HImportConfig({"fs": ["a"]})
            fieldset = cfg.get("fs")
            conn = _AsyncCapturingConn(cfg, prepared={"fs": fieldset.version})
            conn._himport_reconciled_revision = cfg.revision
            node = mock.Mock()
            node._himport_reconcile_discards = mock.AsyncMock()
            node.parse_response = mock.AsyncMock(side_effect=["OK", 1])
            resp = await async_cluster_mod.ClusterNode._himport_execute_set(
                node, conn, "k", "fs", ["v"], asking=True
            )
            assert resp == 1
            assert conn.writes == [("packed", [ASKING, SET_K_FS])]

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportSingleConnectionPrepareMaterializesFields:
    """On a single-connection client the immediate wire PREPARE must send the
    fully materialized fields even when the caller passes a one-shot iterable.

    ``himport_config.prepare`` consumes the iterable into a tuple, so re-passing
    the original ``fields`` to the wire call would forward an exhausted iterator
    and PREPARE zero fields. The wire call must reuse the stored
    ``HImportFieldset.fields`` returned by ``prepare`` instead.
    """

    def test_generator_fields_reach_wire_prepare(self):
        client = Redis()
        client._single_connection_client = True
        conn = mock.Mock()
        conn.is_connected = True
        conn._himport_prepared = {}
        client.connection = conn
        with mock.patch.object(client, "himport_prepare_internal") as internal:
            client.himport_prepare("fs", (f"f{i}" for i in range(3)))
        internal.assert_called_once_with("fs", ("f0", "f1", "f2"))
        assert conn._himport_prepared["fs"] == client.himport_config.get("fs").version

    def test_async_generator_fields_reach_wire_prepare(self):
        async def run():
            client = AsyncRedis()
            client.single_connection_client = True
            client.initialize = mock.AsyncMock(return_value=client)
            conn = mock.Mock()
            conn.is_connected = True
            conn._himport_prepared = {}
            client.connection = conn
            client.himport_prepare_internal = mock.AsyncMock()
            await client.himport_prepare("fs", (f"f{i}" for i in range(3)))
            client.himport_prepare_internal.assert_awaited_once_with(
                "fs", ("f0", "f1", "f2")
            )
            assert (
                conn._himport_prepared["fs"] == client.himport_config.get("fs").version
            )

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportPropagationSync:
    """Data propagation: client -> pool -> connection (standalone + sentinel)."""

    def test_pool_builds_config_from_schemas(self):
        pool = ConnectionPool(himport_schemas={"shared": ["a", "b"]})
        assert isinstance(pool.himport_config, HImportConfig)
        assert pool.himport_config.get("shared").fields == ("a", "b")
        conn = pool.make_connection()
        # Same shared object reaches the connection, plus empty per-conn state.
        assert conn.himport_config is pool.himport_config
        assert conn._himport_prepared == {}
        assert conn._himport_reconciled_revision == 0

    def test_pool_accepts_prebuilt_config_internally(self):
        # Internal channel (used by the cluster to share one object): a pre-built
        # himport_config passed via connection_kwargs is used as-is, not rebuilt.
        cfg = HImportConfig({"x": ["1"]})
        pool = ConnectionPool(himport_config=cfg)
        assert pool.himport_config is cfg
        assert pool.make_connection().himport_config is cfg

    def test_pool_empty_config_when_unconfigured(self):
        # A config always exists (empty) so runtime himport_prepare has a single
        # shared object to mutate; the same object reaches connections.
        pool = ConnectionPool()
        assert isinstance(pool.himport_config, HImportConfig)
        assert len(pool.himport_config) == 0
        assert pool.make_connection().himport_config is pool.himport_config

    def test_blocking_pool_resolves(self):
        pool = BlockingConnectionPool(himport_schemas={"s": ["a"]})
        assert pool.himport_config.get("s").fields == ("a",)
        assert pool.make_connection().himport_config is pool.himport_config

    def test_redis_property_and_propagation(self):
        r = Redis(himport_schemas={"shared": ["name", "email"]})
        assert r.himport_config is r.connection_pool.himport_config
        assert r.himport_config.get("shared").fields == ("name", "email")

    def test_redis_unix_socket_propagates_schemas(self):
        # Regression: himport_schemas must propagate for Unix-socket clients, not
        # just TCP. Construction does not connect, so no socket is needed.
        r = Redis(
            unix_socket_path="/tmp/does-not-connect.sock",
            himport_schemas={"shared": ["name", "email"]},
        )
        assert r.himport_config is r.connection_pool.himport_config
        assert r.himport_config.get("shared").fields == ("name", "email")

    def test_redis_unconfigured_is_empty(self):
        cfg = Redis().himport_config
        assert isinstance(cfg, HImportConfig)
        assert len(cfg) == 0

    def test_redis_has_no_public_himport_config_param(self):
        # himport_config (the object) is internal-only; the sole public knob is the
        # schemas dict, so the client never holds a caller-supplied mutable config.
        assert "himport_config" not in inspect.signature(Redis.__init__).parameters

    def test_sentinel_pool_resolves_via_connection_kwargs(self):
        # Sentinel needs no edits: himport_schemas rides in connection_kwargs and
        # the base pool (via SentinelConnectionPool) resolves it.
        s = Sentinel([("127.0.0.1", 26379)], himport_schemas={"s": ["a"]})
        assert s.connection_kwargs.get("himport_schemas") == {"s": ["a"]}
        pool = SentinelConnectionPool("mymaster", s, himport_schemas={"s": ["a"]})
        assert pool.himport_config.get("s").fields == ("a",)


@pytest.mark.fixed_client
class TestHImportPropagationAsync:
    """Async mirror of the standalone propagation checks."""

    def test_async_pool_and_client(self):
        async def run():
            pool = AsyncConnectionPool(himport_schemas={"s": ["a", "b"]})
            assert pool.himport_config.get("s").fields == ("a", "b")
            conn = pool.make_connection()
            assert conn.himport_config is pool.himport_config
            assert conn._himport_prepared == {}
            assert conn._himport_reconciled_revision == 0

            r = AsyncRedis(himport_schemas={"s": ["a"]})
            assert r.himport_config is r.connection_pool.himport_config
            assert r.himport_config.get("s").fields == ("a",)

            assert len(AsyncRedis().himport_config) == 0

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportSetValueValidation:
    """himport_set must reject bare string-like value iterables (str/bytes/
    bytearray/memoryview): splatting them sends single chars/bytes as separate
    positional values instead of one value. The guard runs before any connection
    use, so no server is needed. Mirrors HImportConfig's field-list guard."""

    @pytest.mark.parametrize("bad", ["abc", b"x", bytearray(b"x"), memoryview(b"x")])
    def test_sync_rejects_string_like_values(self, bad):
        with pytest.raises(DataError, match="must be a collection of values"):
            Redis().himport_set("k", "fs", bad)

    @pytest.mark.parametrize("bad", ["abc", b"x", bytearray(b"x"), memoryview(b"x")])
    def test_async_rejects_string_like_values(self, bad):
        async def run():
            with pytest.raises(DataError, match="must be a collection of values"):
                await AsyncRedis().himport_set("k", "fs", bad)

        asyncio.run(run())


@pytest.mark.fixed_client
class TestHImportClusterWiring:
    """Cluster wiring guards (offline). Full topology behavior is covered by the
    cluster suite; here we assert the plumbing that makes propagation possible."""

    def test_nodes_manager_accepts_shared_config(self):
        # The one shared config is threaded to nodes via NodesManager (and injected
        # onto each node pool), not through connection_kwargs.
        params = inspect.signature(cluster_mod.NodesManager.__init__).parameters
        assert "himport_config" in params

    def test_sync_cluster_single_public_param(self):
        params = inspect.signature(cluster_mod.RedisCluster.__init__).parameters
        assert "himport_schemas" in params
        assert "himport_config" not in params

    def test_async_cluster_single_public_param(self):
        params = inspect.signature(async_cluster_mod.RedisCluster.__init__).parameters
        assert "himport_schemas" in params
        assert "himport_config" not in params

    def test_async_cluster_has_himport_slot(self):
        # Private backing attribute lives in __slots__; the public name is a property.
        assert "_himport_config" in async_cluster_mod.RedisCluster.__slots__

    def test_himport_config_is_read_only_property(self):
        # All client classes expose himport_config as a read-only property (no setter),
        # so it cannot be rebound to desync the shared registry.
        from redis import Redis, RedisCluster
        from redis.asyncio import Redis as AsyncRedis
        from redis.asyncio import RedisCluster as AsyncRedisCluster

        for cls in (Redis, RedisCluster, AsyncRedis, AsyncRedisCluster):
            attr = inspect.getattr_static(cls, "himport_config")
            assert isinstance(attr, property), cls
            assert attr.fset is None, cls  # read-only: no setter


@pytest.mark.fixed_client
class TestNoSuchFieldsetErrorMapping:
    """The parser maps the server's "no such fieldset" reply to the dedicated
    NoSuchFieldsetError so the HIMPORT SET recovery path can catch it by type.

    The server reply is a fixed message with no fieldset name appended (verified
    against the server: always exactly ``ERR no such fieldset``), so the mapping is
    an exact match in ``EXCEPTION_CLASSES``, like the other ERR-message entries.
    """

    def test_maps_no_such_fieldset_reply(self):
        # Exact wire reply the server sends for an unprepared fieldset.
        exc = BaseParser.parse_error("ERR no such fieldset")
        assert isinstance(exc, NoSuchFieldsetError)
        assert isinstance(exc, ResponseError)  # remains a ResponseError subtype
        assert exc.status_code == "ERR"

    def test_unrelated_err_stays_generic(self):
        exc = BaseParser.parse_error("ERR something else went wrong")
        assert type(exc) is ResponseError
        assert not isinstance(exc, NoSuchFieldsetError)
