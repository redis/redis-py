"""Tests for JWT signature verification in JWToken."""
from datetime import datetime, timezone

import pytest

jwt = pytest.importorskip("jwt")

from redis.auth.token import JWToken


def test_jwt_token_rejects_forged_token_with_wrong_key():
    """A token signed with one key must be rejected when verified with another."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")

    with pytest.raises(jwt.InvalidSignatureError):
        JWToken(encoded, key="wrong_secret", algorithms=["HS256"])


def test_jwt_token_accepts_valid_token_with_correct_key():
    """A token signed and verified with the same key is accepted."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

    assert jwt_token.try_get("key") == "value"
