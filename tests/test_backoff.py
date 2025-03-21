from unittest.mock import Mock

import pytest

from redis.backoff import ExponentialWithJitterBackoff


def test_exponential_with_jitter_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_random = Mock(side_effect=[0.25, 0.5, 0.75, 1.0, 0.9])
    monkeypatch.setattr("random.random", mock_random)

    bo = ExponentialWithJitterBackoff(cap=5, base=1)

    assert bo.compute(0) == 0.25  # min(5, 0.25*2^0)
    assert bo.compute(1) == 1.0  # min(5, 0.5*2^1)
    assert bo.compute(2) == 3.0  # min(5, 0.75*2^2)
    assert bo.compute(3) == 5.0  # min(5, 1*2^3)
    assert bo.compute(4) == 5.0  # min(5, 0.9*2^4)
