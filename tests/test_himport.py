import pytest

from redis.exceptions import DataError
from redis.himport import FieldsetOrigin, HImportConfig, HImportFieldset


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
