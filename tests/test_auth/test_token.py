from datetime import datetime, timezone

import pytest
from redis.auth.err import InvalidTokenSchemaErr
from redis.auth.token import JWToken, SimpleToken


class TestToken:
    def test_simple_token(self):
        token = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"key": "value"},
        )

        assert token.ttl() == pytest.approx(1000, 10)
        assert token.is_expired() is False
        assert token.try_get("key") == "value"
        assert token.get_value() == "value"
        assert token.get_expires_at_ms() == pytest.approx(
            (datetime.now(timezone.utc).timestamp() * 1000) + 100, 10
        )
        assert token.get_received_at_ms() == pytest.approx(
            (datetime.now(timezone.utc).timestamp() * 1000), 10
        )

        token = SimpleToken(
            "value",
            -1,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"key": "value"},
        )

        assert token.ttl() == -1
        assert token.is_expired() is False
        assert token.get_expires_at_ms() == -1

    def test_jwt_token(self):
        jwt = pytest.importorskip("jwt")

        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded)

        assert jwt_token.ttl() == pytest.approx(100000, 10)
        assert jwt_token.is_expired() is False
        assert jwt_token.try_get("key") == "value"
        assert jwt_token.get_value() == encoded
        assert jwt_token.get_expires_at_ms() == pytest.approx(
            (datetime.now(timezone.utc).timestamp() * 1000) + 100000, 10
        )
        assert jwt_token.get_received_at_ms() == pytest.approx(
            (datetime.now(timezone.utc).timestamp() * 1000), 10
        )

        token = {
            "exp": -1,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded)

        assert jwt_token.ttl() == -1
        assert jwt_token.is_expired() is False
        assert jwt_token.get_expires_at_ms() == -1000

        with pytest.raises(InvalidTokenSchemaErr):
            token = {"key": "value"}
            encoded = jwt.encode(token, "secret", algorithm="HS256")
            JWToken(encoded)
