import base64
import json
from datetime import datetime, timezone

import pytest
from redis.auth.err import InvalidTokenSchemaErr
from redis.auth.token import JWToken, SimpleToken


def _encode_without_alg_header(payload: dict) -> str:
    """Build a JWT whose header omits ``alg``.

    ``jwt.encode`` always writes ``alg``, so the segments are assembled by
    hand. The signature is arbitrary because the unverified path never checks
    it.
    """

    def segment(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    return ".".join(
        (
            segment(json.dumps({"typ": "JWT"}).encode()),
            segment(json.dumps(payload).encode()),
            segment(b"unverified"),
        )
    )


def _bracketed_call(call):
    """Call ``call`` between two clock reads, returning (before, result, after).

    ``ttl()`` and ``JWToken.get_received_at_ms()`` read the clock when invoked,
    so the only exact statement about their result is that it falls inside the
    window bracketing the call. That holds however slow the CI runner is, where
    a fixed tolerance either goes stale or has to be so wide it asserts nothing.
    """
    before = datetime.now(timezone.utc).timestamp() * 1000
    result = call()
    after = datetime.now(timezone.utc).timestamp() * 1000
    return before, result, after


def _spy_on_decode(monkeypatch, jwt) -> dict:
    """Record the keyword arguments JWToken passes to ``jwt.decode``.

    Dicts are snapshotted at call time because PyJWT writes into the options
    mapping it is given, and these assertions are about what was passed.
    """
    captured = {}
    original_decode = jwt.decode

    def spy(*args, **kwargs):
        captured.update(
            {k: dict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        )
        return original_decode(*args, **kwargs)

    monkeypatch.setattr(jwt, "decode", spy)
    return captured


@pytest.mark.fixed_client
class TestToken:
    def test_simple_token(self):
        # An hour, so that a stalled runner cannot expire the token mid-test.
        # The previous 1s window was inside what a contended CI machine can
        # lose between two statements, while pytest's timeout is 30s.
        expires_at = (datetime.now(timezone.utc).timestamp() * 1000) + 3_600_000
        received_at = datetime.now(timezone.utc).timestamp() * 1000
        token = SimpleToken("value", expires_at, received_at, {"key": "value"})

        before, ttl, after = _bracketed_call(token.ttl)

        assert expires_at - after <= ttl <= expires_at - before
        assert token.is_expired() is False
        assert token.try_get("key") == "value"
        assert token.get_value() == "value"
        assert token.get_expires_at_ms() == expires_at
        assert token.get_received_at_ms() == received_at

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

        exp = datetime.now(timezone.utc).timestamp() + 100
        token = {
            "exp": exp,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded)

        before, ttl, after = _bracketed_call(jwt_token.ttl)

        assert exp * 1000 - after <= ttl <= exp * 1000 - before
        assert jwt_token.is_expired() is False
        assert jwt_token.try_get("key") == "value"
        assert jwt_token.get_value() == encoded
        assert jwt_token.get_expires_at_ms() == exp * 1000

        before, received_at, after = _bracketed_call(jwt_token.get_received_at_ms)

        assert before <= received_at <= after

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

    def test_jwt_token_with_audience(self):
        jwt = pytest.importorskip("jwt")

        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "aud": "test-audience",
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

        assert jwt_token.try_get("aud") == "test-audience"
        assert jwt_token.try_get("key") == "value"
        assert jwt_token.is_expired() is False

    def test_jwt_token_with_nbf(self):
        jwt = pytest.importorskip("jwt")

        # nbf must be in the future to prove that verification skips it.
        nbf = datetime.now(timezone.utc).timestamp() + 100
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "nbf": nbf,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

        assert jwt_token.try_get("nbf") == nbf
        assert jwt_token.try_get("key") == "value"
        assert jwt_token.is_expired() is False

    def test_jwt_token_with_future_iat(self):
        jwt = pytest.importorskip("jwt")

        # iat must be in the future to prove that verification skips it
        # (a slightly future iat can occur under clock skew).
        iat = datetime.now(timezone.utc).timestamp() + 60
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": iat,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

        assert jwt_token.try_get("iat") == iat
        assert jwt_token.try_get("key") == "value"
        assert jwt_token.is_expired() is False

    def test_jwt_token_verified_expired_token(self):
        jwt = pytest.importorskip("jwt")

        # Expiration is owned by redis-py via is_expired()/ttl(), not PyJWT:
        # an expired token must still construct with the signature verified.
        token = {
            "exp": datetime.now(timezone.utc).timestamp() - 100,
            "iat": datetime.now(timezone.utc).timestamp() - 200,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

        assert jwt_token.try_get("key") == "value"
        assert jwt_token.is_expired() is True

    def test_jwt_token_verified_never_expires_sentinel(self):
        jwt = pytest.importorskip("jwt")

        token = {
            "exp": -1,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

        assert jwt_token.is_expired() is False
        assert jwt_token.ttl() == -1

    def test_jwt_token_accepts_valid_token_with_correct_key(self):
        jwt = pytest.importorskip("jwt")

        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

        assert jwt_token.try_get("key") == "value"

    def test_jwt_token_rejects_forged_token_with_wrong_key(self):
        jwt = pytest.importorskip("jwt")

        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        with pytest.raises(jwt.InvalidSignatureError):
            JWToken(encoded, key="wrong_secret", algorithms=["HS256"])

    def test_jwt_token_rejects_algorithm_outside_allow_list(self):
        jwt = pytest.importorskip("jwt")

        # Restricting the algorithm is the only thing `algorithms` does: a
        # token signed with one outside the list must be rejected even though
        # the key is correct.
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        with pytest.raises(jwt.InvalidAlgorithmError):
            JWToken(encoded, key="secret", algorithms=["HS512"])

    def test_jwt_token_key_without_algorithms_raises(self):
        jwt = pytest.importorskip("jwt")

        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        with pytest.raises(ValueError, match="algorithms must be provided"):
            JWToken(encoded, key="secret")

    def test_jwt_token_verified_pins_claim_validation_off(self, monkeypatch):
        jwt = pytest.importorskip("jwt")

        # Every claim check is named explicitly so that a PyJWT upgrade cannot
        # change which tokens are accepted. PyJWT applies its own default for
        # any option left unnamed, and those defaults have changed between
        # releases (verify_sub and verify_jti arrived in 2.10, both on).
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        captured = _spy_on_decode(monkeypatch, jwt)
        JWToken(encoded, key="secret", algorithms=["HS256"])

        assert captured["options"] == {
            "verify_exp": False,
            "verify_nbf": False,
            "verify_iat": False,
            "verify_aud": False,
            "verify_iss": False,
            "verify_sub": False,
            "verify_jti": False,
        }

    def test_jwt_token_pins_every_claim_check_pyjwt_offers(self):
        jwt = pytest.importorskip("jwt")

        # Pinning only works for options that are named. Anything PyJWT adds
        # later would fall back to its own default and change which tokens are
        # accepted on a PyJWT upgrade, so a new claim check must fail here and
        # be triaged deliberately rather than arrive silently.
        pyjwt_claim_checks = {
            option
            for option in jwt.PyJWT().options
            if option.startswith("verify_") and option != "verify_signature"
        }

        assert pyjwt_claim_checks <= set(JWToken._PINNED_DECODE_OPTIONS)

    def test_jwt_token_verified_accepts_non_string_sub(self):
        jwt = pytest.importorskip("jwt")

        # A non-string `sub` is rejected by PyJWT >= 2.10 defaults. redis-py
        # never reads `sub`, and the unverified path accepts it, so pinning
        # keeps both paths aligned.
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "sub": 12345,
            "jti": 678,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        jwt_token = JWToken(encoded, key="secret", algorithms=["HS256"])

        assert jwt_token.try_get("sub") == 12345
        assert jwt_token.try_get("key") == "value"

    def test_jwt_token_decode_options_can_enable_claim_validation(self):
        jwt = pytest.importorskip("jwt")

        # A caller that wants a pinned-off check back turns it on per claim.
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "nbf": datetime.now(timezone.utc).timestamp() + 100,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        # Accepted with the pinned defaults...
        pinned = JWToken(encoded, key="secret", algorithms=["HS256"])
        assert pinned.try_get("key") == "value"

        # ...and rejected once the caller asks for nbf validation.
        with pytest.raises(jwt.ImmatureSignatureError):
            JWToken(
                encoded,
                key="secret",
                algorithms=["HS256"],
                decode_options={"verify_nbf": True},
            )

    @pytest.mark.parametrize("forbidden", ["verify_signature", "verify_exp"])
    def test_jwt_token_decode_options_reject_forbidden_keys(self, forbidden):
        jwt = pytest.importorskip("jwt")

        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        with pytest.raises(ValueError, match="cannot be overridden"):
            JWToken(
                encoded,
                key="secret",
                algorithms=["HS256"],
                decode_options={forbidden: True},
            )

    def test_jwt_token_decode_options_without_key_raises(self):
        jwt = pytest.importorskip("jwt")

        # Claim validation without signature verification would validate
        # claims that are not authenticated, so it is rejected instead of
        # being silently ignored.
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        with pytest.raises(ValueError, match="together with key"):
            JWToken(encoded, decode_options={"verify_nbf": True})

    def test_jwt_token_decode_options_are_not_mutated(self):
        jwt = pytest.importorskip("jwt")

        # PyJWT writes into the options mapping it is given.
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")
        decode_options = {"verify_nbf": False}

        JWToken(
            encoded, key="secret", algorithms=["HS256"], decode_options=decode_options
        )

        assert decode_options == {"verify_nbf": False}

    def test_jwt_token_verified_missing_required_field(self):
        jwt = pytest.importorskip("jwt")

        # Schema validation runs after both decode paths, so a verified token
        # without `exp` must fail the same way an unverified one does.
        with pytest.raises(InvalidTokenSchemaErr):
            encoded = jwt.encode({"key": "value"}, "secret", algorithm="HS256")
            JWToken(encoded, key="secret", algorithms=["HS256"])

    def test_jwt_token_unverified_does_not_forward_algorithms(self, monkeypatch):
        jwt = pytest.importorskip("jwt")

        # Nothing is verified without a key, so PyJWT never consults the list.
        # It is not derived from the token header either, because that header
        # is attacker-controlled (RFC 8725 section 2.1).
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS384")

        captured = _spy_on_decode(monkeypatch, jwt)
        jwt_token = JWToken(encoded)

        assert "algorithms" not in captured
        assert captured["options"] == {"verify_signature": False}
        assert jwt_token.try_get("key") == "value"

    def test_jwt_token_unverified_ignores_given_algorithms(self, monkeypatch):
        jwt = pytest.importorskip("jwt")

        # A caller-supplied list is accepted but never forwarded without a key.
        token = {
            "exp": datetime.now(timezone.utc).timestamp() + 100,
            "iat": datetime.now(timezone.utc).timestamp(),
            "key": "value",
        }
        encoded = jwt.encode(token, "secret", algorithm="HS256")

        captured = _spy_on_decode(monkeypatch, jwt)
        jwt_token = JWToken(encoded, algorithms=["HS512"])

        assert "algorithms" not in captured
        assert jwt_token.try_get("key") == "value"

    def test_jwt_token_unverified_header_without_alg(self):
        jwt = pytest.importorskip("jwt")

        # The header is not inspected on this path, so a missing alg cannot
        # raise. Guards against reintroducing a header lookup here.
        encoded = _encode_without_alg_header(
            {
                "exp": datetime.now(timezone.utc).timestamp() + 100,
                "key": "value",
            }
        )

        assert "alg" not in jwt.get_unverified_header(encoded)

        jwt_token = JWToken(encoded)

        assert jwt_token.try_get("key") == "value"
        assert jwt_token.is_expired() is False
