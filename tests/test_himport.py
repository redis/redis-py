import asyncio
import inspect

import pytest

import redis.asyncio.cluster as async_cluster_mod
import redis.cluster as cluster_mod
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
from redis.connection import BlockingConnectionPool, ConnectionPool
from redis.exceptions import DataError
from redis.himport import FieldsetOrigin, HImportConfig, HImportFieldset
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

    def test_pool_none_when_unconfigured(self):
        pool = ConnectionPool()
        assert pool.himport_config is None
        assert pool.make_connection().himport_config is None

    def test_blocking_pool_resolves(self):
        pool = BlockingConnectionPool(himport_schemas={"s": ["a"]})
        assert pool.himport_config.get("s").fields == ("a",)
        assert pool.make_connection().himport_config is pool.himport_config

    def test_redis_property_and_propagation(self):
        r = Redis(himport_schemas={"shared": ["name", "email"]})
        assert r.himport_config is r.connection_pool.himport_config
        assert r.himport_config.get("shared").fields == ("name", "email")

    def test_redis_unconfigured_is_none(self):
        assert Redis().himport_config is None

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

            assert AsyncRedis().himport_config is None

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
        assert "himport_config" in async_cluster_mod.RedisCluster.__slots__
