"""Shared HIMPORT wire-execution helpers for the synchronous clients.

The PREPARE / SET / DISCARD packed-write drain loops, per-connection version
bookkeeping, and ``NoSuchFieldsetError`` re-prepare-and-retry are identical for
the standalone (:class:`redis.Redis`) and cluster (:class:`redis.RedisCluster`)
sync clients, differing only in (a) which object provides ``parse_response`` and
(b) the cluster-only ASK-redirect handling. These functions take that object as
``node`` and an ``asking`` flag (``False`` -- and a no-op -- for standalone), so
both clients share one implementation instead of copy-pasting the logic. The
per-class ``_himport_*`` methods are thin delegators to these functions.

The async mirror lives in :mod:`redis.asyncio._himport_exec`; the two are kept
separate on purpose (the project maintains parallel sync/async stacks by hand).
"""

from redis.exceptions import NoSuchFieldsetError, ResponseError
from redis.himport import (
    HIMPORT_DISCARD,
    HIMPORT_PREPARE,
    HIMPORT_SET,
    HImportRegistry,
    himport_discard_command,
    himport_prepare_command,
    himport_set_command,
)


def reconcile_discards(node, conn):
    """DISCARD, on ``conn``, any prepared fieldset removed from the registry.

    Runs at most once per registry mutation: the connection records the registry
    ``revision`` it last reconciled against, so unchanged registries are a no-op.
    ``node`` supplies ``parse_response`` (the standalone client itself, or the
    owning node's client in cluster mode).
    """
    registry = conn.himport_registry
    if registry is None or conn._himport_reconciled_revision == registry.revision:
        return
    # Snapshot the revision *before* computing ``stale`` so the value stamped at
    # the end is never newer than the registry state ``stale`` reflects. A
    # concurrent ``himport_discard`` (thread-shared sync client) that lands after
    # this point only leaves the connection marked behind the live revision, so
    # the next reconcile re-runs and catches it. Re-reading ``registry.revision``
    # at the end instead would stamp a discard this connection never sent.
    reconciled_to = registry.revision
    stale = registry.names_to_discard(list(conn._himport_prepared))
    if stale:
        conn.send_packed_command(
            conn.pack_commands([himport_discard_command(n) for n in stale])
        )
        # One reply per packed DISCARD must be read regardless of a per-command
        # ResponseError, otherwise the unread replies desync the pooled socket.
        # Drain every reply, then surface the first error (ConnectionError is not
        # caught: it tears the socket down, so no desync is possible).
        first_error = None
        for n in stale:
            try:
                node.parse_response(conn, HIMPORT_DISCARD)
            except ResponseError as e:
                first_error = first_error or e
            conn._himport_prepared.pop(n, None)
        if first_error is not None:
            raise first_error
    conn._himport_reconciled_revision = reconciled_to


def prepare_and_set(node, conn, key, fieldset_name, values, fieldset, asking=False):
    """PREPARE ``fieldset`` bundled with the SET on ``conn`` (one packed write).

    When ``asking`` is set (an ASK-redirected cluster SET) the batch becomes
    ``[PREPARE, ASKING, SET]`` so the per-command ASKING allowance falls
    immediately before the SET -- the only slot-scoped command. PREPARE is a
    connection-session command the ASKING flag does not gate, so placing it
    before ASKING is safe. Every reply is drained even on a per-command error so
    the packed replies never desync the pooled socket.
    """
    commands = [himport_prepare_command(fieldset_name, fieldset.fields)]
    if asking:
        commands.append(("ASKING",))
    commands.append(himport_set_command(key, fieldset_name, values))
    conn.send_packed_command(conn.pack_commands(commands))
    prep_error = ask_error = set_error = None
    set_resp = None
    try:
        node.parse_response(conn, HIMPORT_PREPARE)
    except ResponseError as e:
        prep_error = e
    if asking:
        try:
            node.parse_response(conn, "ASKING")
        except ResponseError as e:
            ask_error = e
    try:
        set_resp = node.parse_response(conn, HIMPORT_SET)
    except ResponseError as e:
        set_error = e

    if prep_error:
        raise prep_error  # PREPARE failure is the root cause
    else:
        conn._himport_prepared[fieldset_name] = fieldset.version

    if ask_error:
        raise ask_error
    if set_error:
        raise set_error
    return set_resp


def execute_set(node, conn, key, fieldset_name, values, asking=False):
    """Execute an ``HIMPORT SET`` on ``conn`` with the required session setup.

    Reconciles deferred discards, lazily bundles PREPARE with the SET on first
    use of a fieldset, and recovers once from a mid-connection fieldset loss
    (``NoSuchFieldsetError``) by re-PREPARE-and-retry. When ``asking`` is set the
    ASKING allowance is folded into the SET's own packed write so it immediately
    precedes the (slot-scoped) SET; the session setup runs first, since those are
    connection-session commands the flag does not gate.
    """
    reconcile_discards(node, conn)

    registry = conn.himport_registry
    fieldset = registry.get(fieldset_name) if registry is not None else None
    # Lazy PREPARE bundled with SET on first use of this fieldset.
    if (
        fieldset is not None
        and conn._himport_prepared.get(fieldset_name) != fieldset.version
    ):
        return prepare_and_set(
            node, conn, key, fieldset_name, values, fieldset, asking=asking
        )

    # Believed already prepared (or an unregistered fieldset): bare SET, with
    # ASKING packed immediately before it when this is an ASK redirect.
    if asking:
        conn.send_packed_command(
            conn.pack_commands(
                [("ASKING",), himport_set_command(key, fieldset_name, values)]
            )
        )
        try:
            node.parse_response(conn, "ASKING")
        except ResponseError as ask_error:
            # ASKING and SET were one packed write, so the SET reply is still
            # queued. Drain it before surfacing the ASKING error, otherwise the
            # connection returns to the pool with an unread reply and desyncs the
            # next borrower.
            try:
                node.parse_response(conn, HIMPORT_SET)
            except ResponseError:
                pass
            raise ask_error
    else:
        conn.send_command(*himport_set_command(key, fieldset_name, values))
    try:
        return node.parse_response(conn, HIMPORT_SET)
    except NoSuchFieldsetError:
        # Server dropped the fieldset mid-connection without dropping the socket
        # (e.g. RESET / maxmemory-clients eviction): re-PREPARE on this healthy
        # connection and retry the SET once rather than reconnecting. Only for
        # registry-backed fieldsets; manual/unregistered usage propagates.
        if fieldset is None:
            raise
        conn._himport_prepared.pop(fieldset_name, None)
        return prepare_and_set(
            node, conn, key, fieldset_name, values, fieldset, asking=asking
        )


def prepare_pipeline(node, conn, command_arg_lists):
    """Pre-flight ``conn`` for a pipeline batch containing ``HIMPORT SET``s.

    The packed pipeline write bypasses the per-command lazy-PREPARE path, so the
    fieldsets referenced by the buffered SETs must be PREPAREd on ``conn`` first.
    Reconciles deferred discards, then PREPAREs every distinct registered fieldset
    the batch references that this connection has not already prepared, in one
    packed write. ``command_arg_lists`` is the batch's per-command positional-arg
    sequences (the caller extracts them from its own command representation).
    No-op when the batch has no registry-backed ``HIMPORT SET``.
    """
    # A pipeline may run on a connection type that never carries real HIMPORT
    # state (e.g. mocked test doubles); only proceed for a real config.
    registry = getattr(conn, "himport_registry", None)
    if not isinstance(registry, HImportRegistry):
        return
    reconcile_discards(node, conn)
    to_prepare = []
    seen = set()
    for args in command_arg_lists:
        if not args or args[0] != HIMPORT_SET or len(args) < 3:
            continue
        fieldset_name = args[2]
        if fieldset_name in seen:
            continue
        seen.add(fieldset_name)
        fieldset = registry.get(fieldset_name)
        if (
            fieldset is not None
            and conn._himport_prepared.get(fieldset_name) != fieldset.version
        ):
            to_prepare.append(fieldset)
    if not to_prepare:
        return
    conn.send_packed_command(
        conn.pack_commands(
            [himport_prepare_command(fs.name, fs.fields) for fs in to_prepare]
        )
    )
    # One reply per packed PREPARE must be read regardless of a per-command
    # ResponseError, otherwise the unread replies desync the socket before the
    # buffered batch is even sent. Drain every reply (marking only the ones that
    # succeeded), then surface the first error as the root cause.
    first_error = None
    for fs in to_prepare:
        try:
            node.parse_response(conn, HIMPORT_PREPARE)
        except ResponseError as e:
            first_error = first_error or e
            continue
        conn._himport_prepared[fs.name] = fs.version
    if first_error is not None:
        raise first_error
