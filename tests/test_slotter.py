import sys

import pytest

from redis.encoder import Encoder
from redis.exceptions import DataError
from redis.slotter import SPEEDUPS, KeySlotter


class TestCKeySlotter:
    @pytest.mark.skipif(not SPEEDUPS, reason="KeySlotter speedups are not installed")
    @pytest.mark.parametrize("errors", ("strict", "replace", "ignore"))
    @pytest.mark.parametrize("encoding", ("utf-8", "utf-16"))
    def test_ckey_slot(self, encoding: str, errors: str) -> None:
        encoder = Encoder(
            encoding=encoding, encoding_errors=errors, decode_responses=True
        )
        pyks = KeySlotter(encoder=encoder, speedups=False)
        assert not pyks.speedups
        cks = KeySlotter(encoder=encoder, speedups=True)
        assert cks.speedups

        for key in [
            "test",
            b"test",
            "test" * 1000,
            b"test" * 1000,
            1,
            int("1" * 1000),
            sys.maxsize,
            1.1,
            1 / 3,
            0.2 + 0.1,
            float("1" * 1000),
            sys.float_info.max,
        ]:
            assert pyks.key_slot(key) == cks.key_slot(key)

        for key in [None, False, True]:
            with pytest.raises(DataError):
                pyks.key_slot(key)
            with pytest.raises(DataError):
                cks.key_slot(key)

        for key in [memoryview(b"abc")]:
            with pytest.raises(AttributeError):
                pyks.key_slot(key)
            with pytest.raises(AttributeError):
                cks.key_slot(key)
