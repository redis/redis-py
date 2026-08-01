"""Unit tests for the pure response-shaping helpers in
``redis._parsers.helpers``.

``pairs_to_dict`` and ``pairs_to_dict_typed`` turn a flat ``[k1, v1, k2, v2]``
reply into a dict and are used all over the response callbacks, but they were
only exercised indirectly through integration tests. These cover their
behavior (decoding flags, odd-length input, type coercion and its fallback)
without needing a live server.
"""

from redis._parsers.helpers import pairs_to_dict, pairs_to_dict_typed


class TestPairsToDict:
    def test_none_returns_empty_dict(self):
        assert pairs_to_dict(None) == {}

    def test_empty_list_returns_empty_dict(self):
        assert pairs_to_dict([]) == {}

    def test_basic_pairs_preserve_types(self):
        # Without any decode flag, keys and values are passed through as-is.
        assert pairs_to_dict([b"a", b"1", b"b", b"2"]) == {b"a": b"1", b"b": b"2"}

    def test_decode_keys_only(self):
        # Keys are decoded to str; values are left untouched.
        assert pairs_to_dict([b"a", b"1"], decode_keys=True) == {"a": b"1"}

    def test_decode_string_values_only(self):
        # Values are decoded to str; keys are left untouched.
        assert pairs_to_dict([b"a", b"1"], decode_string_values=True) == {b"a": "1"}

    def test_decode_keys_and_values(self):
        result = pairs_to_dict(
            [b"a", b"1", b"b", b"2"], decode_keys=True, decode_string_values=True
        )
        assert result == {"a": "1", "b": "2"}

    def test_odd_length_drops_unpaired_trailing_item(self):
        # zip() stops at the shorter side, so a dangling key with no value is
        # dropped rather than raising.
        assert pairs_to_dict([b"a", b"1", b"b"]) == {b"a": b"1"}

    def test_odd_length_with_decode_flags(self):
        assert pairs_to_dict([b"a", b"1", b"b"], decode_keys=True) == {"a": b"1"}

    def test_non_bytes_values_pass_through_when_decoding(self):
        # str_if_bytes only decodes bytes; ints and strs are returned unchanged.
        assert pairs_to_dict([b"n", 5], decode_string_values=True) == {b"n": 5}

    def test_duplicate_keys_last_value_wins(self):
        assert pairs_to_dict([b"a", b"1", b"a", b"2"]) == {b"a": b"2"}


class TestPairsToDictTyped:
    def test_empty_list_returns_empty_dict(self):
        assert pairs_to_dict_typed([], {}) == {}

    def test_coerces_value_for_known_key(self):
        result = pairs_to_dict_typed(["count", "5"], {"count": int})
        assert result == {"count": 5}
        assert isinstance(result["count"], int)

    def test_uncoercible_value_falls_back_to_original(self):
        # int("abc") raises, so the raw value is kept instead of blowing up.
        assert pairs_to_dict_typed(["count", "abc"], {"count": int}) == {"count": "abc"}

    def test_unknown_key_is_left_unchanged(self):
        assert pairs_to_dict_typed(["name", "redis"], {"count": int}) == {
            "name": "redis"
        }

    def test_mixed_typed_and_untyped_keys(self):
        result = pairs_to_dict_typed(
            ["count", "5", "ratio", "1.5", "name", "redis"],
            {"count": int, "ratio": float},
        )
        assert result == {"count": 5, "ratio": 1.5, "name": "redis"}

    def test_odd_length_drops_unpaired_trailing_item(self):
        assert pairs_to_dict_typed(["count", "5", "name"], {"count": int}) == {
            "count": 5
        }
