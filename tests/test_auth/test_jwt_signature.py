"""Tests for JWT signature verification in JWToken."""
from datetime import datetime, timezone

import pytest

from redis.auth.token import JWToken

jwt = pytest.importorskip("jwt")


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


def test_jwt_token_old_initialization():
    """Backward-compatible initialization with token only."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded)

    assert jwt_token.try_get("key") == "value"


def test_jwt_token_key_and_algorithms():
    """Initialization with key and algorithms verifies signature."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

    assert jwt_token.try_get("key") == "value"


def test_jwt_token_algorithms_only():
    """Initialization with algorithms only (no key) skips verification."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded, algorithms=["HS256"])

    assert jwt_token.try_get("key") == "value"


def test_jwt_token_key_without_algorithms_raises():
    """Initialization with key but no algorithms should raise ValueError."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")

    with pytest.raises(ValueError, match="algorithms must be provided"):
        JWToken(encoded, key="secret")


def test_jwt_token_legacy_never_expires_sentinel():
    """Legacy initialization handles exp=-1 sentinel correctly."""
    token = {
        "exp": -1,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded)

    assert not jwt_token.is_expired()
    assert jwt_token.ttl() == -1


def test_jwt_token_verified_never_expires_sentinel():
    """Verified initialization handles exp=-1 sentinel correctly."""
    token = {
        "exp": -1,
        "iat": datetime.now(timezone.utc).timestamp(),
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

    assert not jwt_token.is_expired()
    assert jwt_token.ttl() == -1


def test_jwt_token_legacy_with_audience_claim():
    """Legacy initialization skips aud verification."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "aud": "some-audience",
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded)

    assert jwt_token.try_get("key") == "value"


def test_jwt_token_verified_with_audience_claim():
    """Verified initialization skips aud verification."""
    token = {
        "exp": datetime.now(timezone.utc).timestamp() + 100,
        "aud": "some-audience",
        "key": "value",
    }
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

    assert jwt_token.try_get("key") == "value"
