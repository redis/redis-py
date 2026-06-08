"""Unit tests for :class:`redis.commands.search.result.Result`.

These tests exercise the pure-Python parsing logic and do not require a
running Redis server, so they live in the ``fixed_client`` test group.
"""

import pytest

from redis.commands.search.result import Result


@pytest.mark.fixed_client
class TestResultFromResp3:
    def test_str_keys_are_parsed(self):
        """Baseline: a response with native ``str`` keys parses correctly."""
        res = {
            "total_results": 2,
            "results": [
                {
                    "id": "doc1",
                    "score": 1.5,
                    "extra_attributes": {"field1": "value1"},
                },
                {
                    "id": "doc2",
                    "score": 0.5,
                    "extra_attributes": {"field1": "value2"},
                },
            ],
            "warning": ["a warning"],
        }

        result = Result.from_resp3(res, with_scores=True)

        assert result.total == 2
        assert result.warnings == ["a warning"]
        assert [d.id for d in result.docs] == ["doc1", "doc2"]
        assert [d.score for d in result.docs] == [1.5, 0.5]
        assert result.docs[0].field1 == "value1"

    def test_bytes_keys_are_parsed(self):
        """Regression test for #4107.

        On a RESP3 connection opened with ``decode_responses=False`` the map
        keys returned by the server are ``bytes``.  Before the fix
        :meth:`Result.from_resp3` looked up ``"total_results"``/``"results"``
        as ``str`` keys, missed them entirely and returned an empty result.
        """
        res = {
            b"total_results": 2,
            b"results": [
                {
                    b"id": b"doc1",
                    b"score": 1.5,
                    b"extra_attributes": {b"field1": b"value1"},
                },
                {
                    b"id": b"doc2",
                    b"score": 0.5,
                    b"extra_attributes": {b"field1": b"value2"},
                },
            ],
            b"warning": [b"a warning"],
        }

        result = Result.from_resp3(res, with_scores=True)

        assert result.total == 2
        assert result.warnings == ["a warning"]
        assert [d.id for d in result.docs] == ["doc1", "doc2"]
        assert [d.score for d in result.docs] == [1.5, 0.5]
        assert result.docs[0].field1 == "value1"
        assert result.docs[1].field1 == "value2"

    def test_mixed_str_and_bytes_keys(self):
        """Outer ``bytes`` keys with inner ``str`` keys (and vice-versa) parse.

        Different parser layers (hiredis vs. the pure-python parser, RESP2 vs.
        RESP3) can produce mixed maps in practice, so each level is normalised
        independently.
        """
        res = {
            b"total_results": 1,
            b"results": [
                {"id": "doc1", "extra_attributes": {"f": "v"}},
            ],
            b"warning": [],
        }

        result = Result.from_resp3(res)

        assert result.total == 1
        assert result.docs[0].id == "doc1"
        assert result.docs[0].f == "v"

    def test_empty_or_none_response(self):
        """``None`` and an empty dict both yield an empty result."""
        for empty in (None, {}, {b"total_results": 0, b"results": []}):
            r = Result.from_resp3(empty)
            assert r.total == 0
            assert r.docs == []
            assert r.warnings == []
