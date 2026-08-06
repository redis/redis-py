from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from redis.auth.err import InvalidTokenSchemaErr

if TYPE_CHECKING:
    from jwt import PyJWK
    from jwt.algorithms import AllowedPublicKeys


class TokenInterface(ABC):
    @abstractmethod
    def is_expired(self) -> bool:
        pass

    @abstractmethod
    def ttl(self) -> float:
        pass

    @abstractmethod
    def try_get(self, key: str) -> str:
        pass

    @abstractmethod
    def get_value(self) -> str:
        pass

    @abstractmethod
    def get_expires_at_ms(self) -> float:
        pass

    @abstractmethod
    def get_received_at_ms(self) -> float:
        pass


class TokenResponse:
    def __init__(self, token: TokenInterface):
        self._token = token

    def get_token(self) -> TokenInterface:
        return self._token

    def get_ttl_ms(self) -> float:
        return self._token.get_expires_at_ms() - self._token.get_received_at_ms()


class SimpleToken(TokenInterface):
    def __init__(
        self, value: str, expires_at_ms: float, received_at_ms: float, claims: dict
    ) -> None:
        self.value = value
        self.expires_at = expires_at_ms
        self.received_at = received_at_ms
        self.claims = claims

    def ttl(self) -> float:
        if self.expires_at == -1:
            return -1

        return self.expires_at - (datetime.now(timezone.utc).timestamp() * 1000)

    def is_expired(self) -> bool:
        if self.expires_at == -1:
            return False

        return self.ttl() <= 0

    def try_get(self, key: str) -> str:
        return self.claims.get(key)

    def get_value(self) -> str:
        return self.value

    def get_expires_at_ms(self) -> float:
        return self.expires_at

    def get_received_at_ms(self) -> float:
        return self.received_at


class JWToken(TokenInterface):
    REQUIRED_FIELDS = {"exp"}

    # Decode options the caller may not override, because this class depends on
    # them. Signature verification is decided by whether a key was supplied,
    # and expiration is owned by is_expired()/ttl(), including the exp == -1
    # never-expiring sentinel.
    FORBIDDEN_DECODE_OPTIONS = frozenset({"verify_signature", "verify_exp"})

    # Claim validation pinned on the verified path. Every claim check PyJWT
    # offers is named explicitly so that upgrading PyJWT cannot change which
    # tokens this class accepts: unnamed options fall back to PyJWT's defaults,
    # which have changed between releases (verify_sub and verify_jti arrived in
    # 2.10, both defaulting to on). Callers opt back in through decode_options.
    _PINNED_DECODE_OPTIONS = {
        "verify_exp": False,
        "verify_nbf": False,
        "verify_iat": False,
        "verify_aud": False,
        "verify_iss": False,
        "verify_sub": False,
        "verify_jti": False,
    }

    def __init__(
        self,
        token: str,
        key: "AllowedPublicKeys | PyJWK | str | bytes | None" = None,
        algorithms: list[str] | None = None,
        decode_options: dict[str, Any] | None = None,
    ):
        """Initialize a JWT token wrapper.

        Args:
            token: The encoded JWT string.
            key: The key used to verify the token signature. Anything PyJWT
                accepts: a shared secret, a PEM-encoded key, a public key
                object, or a ``PyJWK``. If None, signature verification is
                skipped (backward compatibility).
            algorithms: A list of allowed algorithms. Required when key is
                provided. Ignored when key is None, and not forwarded to
                PyJWT at all: without signature verification PyJWT never
                consults the list, so it cannot restrict anything.
            decode_options: Claim validation to enable or disable on top of
                the pinned defaults, forwarded to PyJWT's ``options``. Only
                meaningful together with ``key``. The keys in
                ``FORBIDDEN_DECODE_OPTIONS`` cannot be set.

        Raises:
            ImportError: If the PyJWT library is not installed.
            ValueError: If key is provided but algorithms is None, if
                decode_options is provided without key, or if decode_options
                contains a forbidden key.
            InvalidTokenSchemaErr: If required fields are missing.

        Warning:
            Passing ``algorithms`` without ``key`` does **not** verify
            anything. Without a key the token is decoded with the signature
            unchecked, so every claim is attacker-controlled and must be
            treated as untrusted input. Only ``key`` turns verification on.

            When you do pass ``key``, hard-code ``algorithms`` or configure it
            next to the key, and never derive it from the token's own header.
            Trusting the header's ``alg`` enables algorithm-confusion attacks,
            such as an RSA public key being accepted as an HMAC secret
            (RFC 8725 section 2.1). Do not mix symmetric and asymmetric
            algorithms in one list, because they interpret ``key``
            differently.

            Enabling ``verify_aud`` or ``verify_iss`` through
            ``decode_options`` cannot work on its own: PyJWT needs an
            ``audience`` or ``issuer`` value to compare against, and this
            class does not forward one. With ``verify_aud`` on and no
            audience, PyJWT rejects every token that carries an ``aud``
            claim.

        Note:
            Signature verification is opt-in via ``key``. The old
            initialization path (key=None) is still available for backward
            compatibility and disables signature verification.

            By default no claim is enforced by PyJWT on either path: redis-py
            owns claim semantics, expiration in particular, via
            ``is_expired()`` and ``ttl()`` including the ``exp == -1``
            never-expiring sentinel. On the verified path every claim check is
            pinned off explicitly rather than left to PyJWT's defaults, so
            upgrading PyJWT cannot change which tokens are accepted. Callers
            that want stricter validation enable it per claim through
            ``decode_options``.
        """
        try:
            import jwt
        except ImportError as ie:
            raise ImportError(
                f"The PyJWT library is required for {self.__class__.__name__}.",
            ) from ie
        self._value = token
        if key is not None:
            if algorithms is None:
                raise ValueError("algorithms must be provided when key is specified")
            self._decoded = jwt.decode(
                self._value,
                key,
                algorithms=algorithms,
                options=self._build_decode_options(decode_options),
            )
        else:
            if decode_options is not None:
                raise ValueError("decode_options must be used together with key")
            # No algorithms are forwarded here, not even when the caller
            # supplied some. With verify_signature disabled PyJWT never reads
            # them: the list is only consulted under that flag, by the
            # "algorithms is required" guard and by the allow-list check inside
            # _verify_signature. Passing a list would therefore be inert, and
            # deriving one from the token's own header - as this path did
            # before signature verification became available - would mean
            # feeding PyJWT attacker-controlled data for no benefit. PyJWT
            # warns against that exact pattern (RFC 8725 section 2.1).
            self._decoded = jwt.decode(
                self._value,
                options={"verify_signature": False},
            )
        self._validate_token()

    @classmethod
    def _build_decode_options(
        cls, decode_options: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Merge caller overrides over the pinned claim-validation defaults.

        Returns a new dict every time: PyJWT writes into the mapping it is
        given, so neither the class attribute nor the caller's dict may be
        forwarded directly.
        """
        options = dict(cls._PINNED_DECODE_OPTIONS)

        if decode_options:
            forbidden = cls.FORBIDDEN_DECODE_OPTIONS.intersection(decode_options)
            if forbidden:
                raise ValueError(
                    "These decode options cannot be overridden: "
                    f"{', '.join(sorted(forbidden))}"
                )
            options.update(decode_options)

        return options

    def is_expired(self) -> bool:
        exp = self._decoded["exp"]
        if exp == -1:
            return False

        return (
            self._decoded["exp"] * 1000 <= datetime.now(timezone.utc).timestamp() * 1000
        )

    def ttl(self) -> float:
        exp = self._decoded["exp"]
        if exp == -1:
            return -1

        return (
            self._decoded["exp"] * 1000 - datetime.now(timezone.utc).timestamp() * 1000
        )

    def try_get(self, key: str) -> str:
        return self._decoded.get(key)

    def get_value(self) -> str:
        return self._value

    def get_expires_at_ms(self) -> float:
        return float(self._decoded["exp"] * 1000)

    def get_received_at_ms(self) -> float:
        return datetime.now(timezone.utc).timestamp() * 1000

    def _validate_token(self):
        actual_fields = {x for x in self._decoded.keys()}

        if len(self.REQUIRED_FIELDS - actual_fields) != 0:
            raise InvalidTokenSchemaErr(self.REQUIRED_FIELDS - actual_fields)
