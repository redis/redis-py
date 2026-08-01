"""Server-free unit tests for ZADD client-side option validation.

ZADD's mutually-exclusive-option checks raise ``DataError`` while building the
command arguments, before any connection is used, so these run without a
running Redis server.
"""

import pytest

import redis
from redis.exceptions import DataError


@pytest.fixture
def client():
    # Lazy client: no socket is opened unless a command is actually sent, and
    # these invalid-option combos raise before that happens.
    return redis.Redis()


class TestZaddOptionValidation:
    def test_nx_with_gt_reports_the_real_options(self, client):
        with pytest.raises(DataError) as excinfo:
            client.zadd("key", {"m": 1}, nx=True, gt=True)
        msg = str(excinfo.value)
        assert msg == "Only one of 'nx', 'gt', or 'lt' may be defined."
        # Regression guard: the message used to name a non-existent 'gr' option.
        assert "'gr'" not in msg

    def test_nx_with_lt_reports_the_real_options(self, client):
        with pytest.raises(DataError) as excinfo:
            client.zadd("key", {"m": 1}, nx=True, lt=True)
        assert str(excinfo.value) == "Only one of 'nx', 'gt', or 'lt' may be defined."

    def test_nx_and_xx_still_rejected(self, client):
        with pytest.raises(DataError, match="either 'nx' or 'xx', not both"):
            client.zadd("key", {"m": 1}, nx=True, xx=True)

    def test_gt_and_lt_still_rejected(self, client):
        with pytest.raises(DataError, match="either 'gt' or 'lt', not both"):
            client.zadd("key", {"m": 1}, gt=True, lt=True)
