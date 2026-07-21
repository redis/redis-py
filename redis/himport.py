"""HIMPORT client-side configuration for redis-py.

`HIMPORT` lets a client register an ordered list of hash field names once per
connection (a *fieldset*) and then create many hashes by sending only values.
Because a fieldset is server-side session state bound to a single physical
connection, redis-py keeps a client-level registry of the fieldsets the
application has declared and prepares them lazily, per connection, on first use
by ``himport_set``.

This module holds only that registry. It is pure in-memory state with no I/O and
no ``asyncio`` primitives, so a single class is shared by both the sync and async
clients.

Example::

    >>> from redis.himport import HImportConfig, FieldsetOrigin
    >>> cfg = HImportConfig({"account_data": ["name", "email", "age"]})
    >>> cfg.get("account_data").fields
    ('name', 'email', 'age')
    >>> cfg.get("account_data").origin is FieldsetOrigin.INIT
    True
"""

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from enum import Enum

from redis.exceptions import DataError


class FieldsetOrigin(Enum):
    """How a fieldset entered the HIMPORT config.

    ``INIT`` fieldsets are declared in the client constructor and are treated as
    permanent for the client's lifetime — they cannot be discarded. ``RUNTIME``
    fieldsets are added later via ``himport_prepare`` and may be discarded.
    """

    INIT = "INIT"
    RUNTIME = "RUNTIME"


@dataclass(frozen=True)
class HImportFieldset:
    """An immutable HIMPORT fieldset entry.

    Attributes:
        name: Fieldset name used by ``HIMPORT SET`` / ``HIMPORT DISCARD``.
        fields: Ordered field names, exactly as supplied by the caller. They are
            never reordered or deduplicated (HLD R.2): the server canonicalizes
            field order internally and rejects duplicate field names, so the
            client only preserves the caller's positional order.
        origin: Whether the fieldset was declared at client init or at runtime.
        version: Monotonic stamp bumped each time the fieldset is (re)declared.
            Connections compare the version they last prepared against this value
            to detect a stale prepared state; the stamp is drawn from the config's
            mutation clock (:attr:`HImportConfig.revision`), which never repeats,
            so discarding and re-declaring the same name yields a fresh version
            rather than a colliding one. This is the prepare-side signal; the
            discard-side counterpart is the clock advancing on removal, since a
            removed fieldset leaves no entry to stamp.
    """

    name: str
    fields: tuple[str, ...]
    origin: FieldsetOrigin
    version: int


class HImportConfig:
    """Client-level registry of HIMPORT fieldsets.

    Pure in-memory state, shared by the sync and async clients. It is mutated only
    through the client's ``himport_prepare`` / ``himport_discard`` /
    ``himport_discard_all`` methods and exposed read-only through the client's
    ``himport_config`` property.

    Args:
        schemas: Optional mapping of ``fieldset_name -> ordered field names`` to
            register at construction time. These entries are given
            :attr:`FieldsetOrigin.INIT` and cannot later be discarded.
    """

    def __init__(self, schemas: Mapping[str, Iterable[str]] | None = None) -> None:
        self._fieldsets: dict[str, HImportFieldset] = {}
        # Monotonic mutation clock. It advances on every registry change (prepare
        # and discard). prepare stamps the new entry with the advanced value as its
        # per-fieldset version; discard has no surviving entry to stamp, so the
        # advance itself is the signal that a removal happened. Because the clock
        # only ever increases, a re-declared name never reuses a stamp, while
        # comparing per-fieldset versions still avoids forcing unrelated fieldsets
        # to re-prepare.
        self._revision: int = 0
        if schemas:
            for name, fields in schemas.items():
                self._set(name, fields, FieldsetOrigin.INIT)

    # -- internal helpers -------------------------------------------------

    def _advance(self) -> int:
        self._revision += 1
        return self._revision

    def _set(
        self, name: str, fields: Iterable[str], origin: FieldsetOrigin
    ) -> HImportFieldset:
        # A bare str/bytes is iterable character-by-character; that is almost
        # certainly a caller mistake and would silently register single-character
        # "fields", so reject it as invalid local API usage.
        if isinstance(fields, (str, bytes)):
            raise DataError(
                "HIMPORT fields must be a collection of field names, not a string"
            )
        field_tuple = tuple(fields)
        if not field_tuple:
            raise DataError("HIMPORT fieldset must have at least one field")
        fieldset = HImportFieldset(
            name=name,
            fields=field_tuple,
            origin=origin,
            version=self._advance(),
        )
        self._fieldsets[name] = fieldset
        return fieldset

    # -- mutation ---------------------------------------------------------

    def prepare(self, name: str, fields: Iterable[str]) -> HImportFieldset:
        """Add or replace a fieldset, bumping its version, and return the entry.

        A new name is registered as :attr:`FieldsetOrigin.RUNTIME`. Re-declaring
        an existing name preserves its current origin — in particular an
        ``INIT`` fieldset stays ``INIT`` (and therefore undiscardable) when
        re-prepared, so discard protection cannot be bypassed by re-preparing.
        Field order is preserved verbatim; nothing is reordered or deduplicated.
        """
        existing = self._fieldsets.get(name)
        origin = existing.origin if existing is not None else FieldsetOrigin.RUNTIME
        return self._set(name, fields, origin)

    def discard(self, name: str) -> bool:
        """Remove a runtime fieldset from the registry.

        Returns ``True`` if a fieldset was removed, ``False`` if ``name`` was not
        registered. Raises :class:`~redis.exceptions.DataError` if the named
        fieldset was declared at client init (:attr:`FieldsetOrigin.INIT`).
        Advances :attr:`revision` when a fieldset is actually removed.
        """
        existing = self._fieldsets.get(name)
        if existing is None:
            return False
        if existing.origin is FieldsetOrigin.INIT:
            raise DataError(
                f"cannot discard HIMPORT fieldset {name!r} declared at client init"
            )
        del self._fieldsets[name]
        self._advance()
        return True

    def discard_all(self) -> int:
        """Remove all runtime fieldsets and return the number removed.

        Raises :class:`~redis.exceptions.DataError` if any init-declared fieldset
        exists, leaving the registry unchanged: init fieldsets cannot be
        discarded, so ``discard_all`` is rejected whenever one is present.
        Advances :attr:`revision` when at least one fieldset is removed.
        """
        init_names = [
            name
            for name, fieldset in self._fieldsets.items()
            if fieldset.origin is FieldsetOrigin.INIT
        ]
        if init_names:
            raise DataError(
                "cannot discard all HIMPORT fieldsets while init-declared "
                "fieldsets exist: " + ", ".join(repr(n) for n in init_names)
            )
        count = len(self._fieldsets)
        if count:
            self._fieldsets.clear()
            self._advance()
        return count

    # -- read-only access -------------------------------------------------

    @property
    def revision(self) -> int:
        """Monotonic mutation clock, advanced on every registry change.

        It is the discard-side counterpart to per-fieldset
        :attr:`HImportFieldset.version`. A connection records the revision it last
        reconciled against; when it differs, a discard (or prepare) has occurred
        since, so the connection recomputes which of its prepared fieldsets are no
        longer registered — see :meth:`names_to_discard` — and discards those when
        it is released back to the pool.
        """
        return self._revision

    def names_to_discard(self, prepared_names: Iterable[str]) -> list[str]:
        """Return which of ``prepared_names`` are no longer registered.

        Given the fieldset names a connection has prepared on the server, this is
        the set that must be sent ``HIMPORT DISCARD`` (typically when the
        connection is released), because they have been removed from the registry.
        """
        return [name for name in prepared_names if name not in self._fieldsets]

    def get(self, name: str) -> HImportFieldset | None:
        """Return the fieldset registered under ``name``, or ``None``."""
        return self._fieldsets.get(name)

    def names(self) -> list[str]:
        """Return the registered fieldset names."""
        return list(self._fieldsets)

    def items(self) -> list[tuple[str, HImportFieldset]]:
        """Return a snapshot of ``(name, fieldset)`` pairs."""
        return list(self._fieldsets.items())

    def __contains__(self, name: object) -> bool:
        return name in self._fieldsets

    def __len__(self) -> int:
        return len(self._fieldsets)

    def __iter__(self) -> Iterator[str]:
        return iter(self._fieldsets)

    def __repr__(self) -> str:
        entries = ", ".join(
            f"{name}={fieldset.origin.value}:{list(fieldset.fields)}"
            for name, fieldset in self._fieldsets.items()
        )
        return f"{self.__class__.__name__}({entries})"
