"""HIMPORT client-side fieldset registry for redis-py.

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

    >>> from redis.himport import HImportRegistry
    >>> registry = HImportRegistry()
    >>> registry.prepare("account_data", ["name", "email", "age"])
    >>> registry.get("account_data").fields
    ('name', 'email', 'age')
"""

import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from redis.exceptions import DataError
from redis.typing import EncodableT, FieldT, KeyT

# ---------------------------------------------------------------------------
# Wire command tokens
# ---------------------------------------------------------------------------
# Centralised so the sync/async clients, the cluster clients and the
# CoreCommands mixin never repeat the literal HIMPORT strings. ``HIMPORT_*`` are
# the full command names passed to ``execute_command`` and registered as
# response callbacks (the request packer splits the space). The
# ``himport_*_command`` builders return the positional wire args used when a
# connection packs commands directly (lazy PREPARE bundled with SET, deferred
# DISCARD reconcile, etc.).

_HIMPORT = "HIMPORT"
_PREPARE = "PREPARE"
_SET = "SET"
_DISCARD = "DISCARD"
_DISCARDALL = "DISCARDALL"

HIMPORT_PREPARE = f"{_HIMPORT} {_PREPARE}"
HIMPORT_SET = f"{_HIMPORT} {_SET}"
HIMPORT_DISCARD = f"{_HIMPORT} {_DISCARD}"
HIMPORT_DISCARDALL = f"{_HIMPORT} {_DISCARDALL}"


def himport_prepare_command(fieldset_name: str, fields: Iterable[FieldT]) -> tuple:
    """Positional wire args for ``HIMPORT PREPARE fieldset_name field [field ...]``."""
    return (_HIMPORT, _PREPARE, fieldset_name, *fields)


def himport_set_command(
    key: KeyT, fieldset_name: str, values: Iterable[EncodableT]
) -> tuple:
    """Positional wire args for ``HIMPORT SET key fieldset_name value [value ...]``."""
    return (_HIMPORT, _SET, key, fieldset_name, *values)


def himport_discard_command(fieldset_name: str) -> tuple:
    """Positional wire args for ``HIMPORT DISCARD fieldset_name``."""
    return (_HIMPORT, _DISCARD, fieldset_name)


@dataclass(frozen=True)
class HImportFieldset:
    """An immutable HIMPORT fieldset entry.

    Attributes:
        name: Fieldset name used by ``HIMPORT SET`` / ``HIMPORT DISCARD``.
        fields: Ordered field names, exactly as supplied by the caller. They are
            never reordered or deduplicated (HLD R.2): the server canonicalizes
            field order internally and rejects duplicate field names, so the
            client only preserves the caller's positional order.
        version: Monotonic stamp bumped each time the fieldset is (re)declared.
            Connections compare the version they last prepared against this value
            to detect a stale prepared state; the stamp is drawn from the registry's
            mutation clock (:attr:`HImportRegistry.revision`), which never repeats,
            so discarding and re-declaring the same name yields a fresh version
            rather than a colliding one. This is the prepare-side signal; the
            discard-side counterpart is the clock advancing on removal, since a
            removed fieldset leaves no entry to stamp.
    """

    name: str
    fields: tuple[FieldT, ...]
    version: int


class HImportRegistry:
    """Client-level registry of HIMPORT fieldsets.

    Pure in-memory state, shared by the sync and async clients. It is mutated only
    through the client's ``himport_prepare`` / ``himport_discard`` /
    ``himport_discard_all`` methods and exposed read-only through the client's
    ``himport_registry`` property. Mutations are serialized under a lock so the
    revision bump and dict change stay consistent when a sync ``Redis`` instance is
    shared across threads. Reads that iterate or snapshot the registry take the same
    lock, so a concurrent mutation cannot make them observe a torn view or raise
    ``dictionary changed size during iteration``; single-key/scalar reads (``get``,
    ``__contains__``, ``__len__``, :attr:`revision`) are atomic and stay lock-free.
    The async client runs single-threaded and never contends.

    The registry always starts empty; fieldsets are declared at runtime through the
    client's ``himport_prepare`` method.
    """

    def __init__(self) -> None:
        self._fieldsets: dict[str, HImportFieldset] = {}
        # Serializes mutations (prepare/discard/discard_all) so the revision bump and
        # the dict change are applied atomically under concurrent access from a
        # thread-shared sync client, and guards the reads that iterate/snapshot the
        # dict so they never see a torn view or a mid-iteration resize. Held only
        # across synchronous, non-blocking bodies, never across I/O, so it is harmless
        # for the single-threaded async client.
        self._lock = threading.Lock()
        # Monotonic mutation clock. It advances on every registry change (prepare
        # and discard). prepare stamps the new entry with the advanced value as its
        # per-fieldset version; discard has no surviving entry to stamp, so the
        # advance itself is the signal that a removal happened. Because the clock
        # only ever increases, a re-declared name never reuses a stamp, while
        # comparing per-fieldset versions still avoids forcing unrelated fieldsets
        # to re-prepare.
        self._revision: int = 0

    # -- internal helpers -------------------------------------------------
    # ``_advance`` and ``_set`` assume ``_lock`` is already held (they run under the
    # public mutation methods); they never acquire the lock themselves.

    def _advance(self) -> int:
        self._revision += 1
        return self._revision

    def _set(self, name: str, fields: Iterable[FieldT]) -> HImportFieldset:
        # A bare single field name (str/bytes/bytearray/memoryview) is itself
        # iterable element-by-element; that is almost certainly a caller mistake and
        # would silently register single-character/single-byte "fields" (e.g.
        # memoryview(b"id") -> field names 105, 100), so reject it as invalid local
        # API usage. int/float are not iterable, so tuple() below rejects them.
        if isinstance(fields, (str, bytes, bytearray, memoryview)):
            raise DataError(
                "HIMPORT fields must be a collection of field names, "
                "not a single string or binary value"
            )
        field_tuple = tuple(fields)
        if not field_tuple:
            raise DataError("HIMPORT fieldset must have at least one field")
        fieldset = HImportFieldset(
            name=name,
            fields=field_tuple,
            version=self._advance(),
        )
        self._fieldsets[name] = fieldset
        return fieldset

    # -- mutation ---------------------------------------------------------

    def prepare(self, name: str, fields: Iterable[FieldT]) -> HImportFieldset:
        """Add or replace a fieldset, bumping its version, and return the entry.

        Re-declaring an existing name replaces its fields and bumps its version.
        Field order is preserved verbatim; nothing is reordered or deduplicated.
        """
        with self._lock:
            return self._set(name, fields)

    def discard(self, name: str) -> bool:
        """Remove a fieldset from the registry.

        Returns ``True`` if a fieldset was removed, ``False`` if ``name`` was not
        registered. Advances :attr:`revision` when a fieldset is actually removed.
        """
        with self._lock:
            if name not in self._fieldsets:
                return False
            del self._fieldsets[name]
            self._advance()
            return True

    def discard_all(self) -> int:
        """Remove all fieldsets and return the number removed.

        Advances :attr:`revision` when at least one fieldset is removed.
        """
        with self._lock:
            count = len(self._fieldsets)
            if count:
                self._fieldsets.clear()
                self._advance()
            return count

    # -- read-only access -------------------------------------------------
    # Reads that iterate or build a snapshot of the dict take ``_lock`` so a
    # concurrent mutation cannot resize it mid-iteration or expose a torn view;
    # single-key/scalar reads below (get/__contains__/__len__/revision) are atomic
    # and deliberately stay lock-free.

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
        # Hold the lock across the comprehension so every membership test sees one
        # consistent snapshot; ``prepared_names`` is an external iterable of plain
        # strings and never calls back into the registry.
        with self._lock:
            return [name for name in prepared_names if name not in self._fieldsets]

    def get(self, name: str) -> HImportFieldset | None:
        """Return the fieldset registered under ``name``, or ``None``."""
        return self._fieldsets.get(name)

    def names(self) -> list[str]:
        """Return the registered fieldset names."""
        with self._lock:
            return list(self._fieldsets)

    def items(self) -> list[tuple[str, HImportFieldset]]:
        """Return a snapshot of ``(name, fieldset)`` pairs."""
        with self._lock:
            return list(self._fieldsets.items())

    def __contains__(self, name: object) -> bool:
        return name in self._fieldsets

    def __len__(self) -> int:
        return len(self._fieldsets)

    def __iter__(self) -> Iterator[str]:
        # Snapshot under the lock and iterate the copy, so the lock is never held
        # across caller consumption and a concurrent mutation cannot resize the
        # underlying dict mid-iteration.
        with self._lock:
            return iter(list(self._fieldsets))

    def __repr__(self) -> str:
        with self._lock:
            entries = list(self._fieldsets.items())
        body = ", ".join(
            f"{name}={list(fieldset.fields)}" for name, fieldset in entries
        )
        return f"{self.__class__.__name__}({body})"
