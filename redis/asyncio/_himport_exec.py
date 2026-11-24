"""Shared HIMPORT wire-execution helpers for the asynchronous clients.

Async mirror of :mod:`redis._himport_exec`. The PREPARE / SET / DISCARD
packed-write drain loops, per-connection version bookkeeping, and
``NoSuchFieldsetError`` re-prepare-and-retry are identical for the standalone
(:class:`redis.asyncio.Redis`) and cluster (async ``ClusterNode``) clients,
differing only in (a) which object provides ``parse_response`` and (b) the
cluster-only ASK-redirect handling. These coroutines take that object as
``node`` and an ``asking`` flag (``False`` -- and a no-op -- for standalone), so
both clients share one implementation. The per-class ``_himport_*`` methods are
thin delegators to these coroutines.

The sync version lives in :mod:`redis._himport_exec`; the two are kept separate
on purpose (the project maintains parallel sync/async stacks by hand).
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
    parse_himport_set_args,
)


async def reconcile_discards(node, conn):
    """DISCARD, on ``conn``, any prepared fieldset removed from the registry.

    Runs at most once per registry mutation: the connection records the registry
    ``revision`` it last reconciled against, so unchanged registries are a no-op.
    ``node`` supplies ``parse_response`` (the standalone client itself, or the
    owning ``ClusterNode`` in cluster mode).
    """
    registry = conn.himport_registry
    if registry is None or conn._himport_reconciled_revision == registry.revision:
        return
    # Snapshot the revision *before* computing ``stale`` so the value stamped at
    # the end is never newer than the registry state ``stale`` reflects. A
    # concurrent ``himport_discard`` (or another task while this awaits the
    # DISCARD replies) that lands after this point only leaves the connection
    # marked behind the live revision, so the next reconcile re-runs and catches
    # it. Re-reading ``registry.revision`` at the end instead would stamp a
    # discard this connection never sent.
    reconciled_to = registry.revision
    stale = registry.names_to_discard(list(conn._himport_prepared))
    if stale:
        await conn.send_packed_command(
            conn.pack_commands([himport_discard_command(n) for n in stale])
        )
        # One reply per packed DISCARD must be read regardless of a per-command
        # ResponseError, otherwise the unread replies desync the pooled socket.
        # Drain every reply, then surface the first error (ConnectionError is not
        # caught: it tears the socket down, so no desync is possible).
        first_error = None
        for n in stale:
            try:
                await node.parse_response(conn, HIMPORT_DISCARD)
            except ResponseError as e:
                first_error = first_error or e
            conn._himport_prepared.pop(n, None)
        if first_error is not None:
            raise first_error
    conn._himport_reconciled_revision = reconciled_to


async def prepare_and_set(
    node, conn, key, fieldset_name, values, fieldset, asking=False
):
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
    await conn.send_packed_command(conn.pack_commands(commands))
    prep_error = ask_error = set_error = None
    set_resp = None
    try:
        await node.parse_response(conn, HIMPORT_PREPARE)
    except ResponseError as e:
        prep_error = e
    if asking:
        try:
            await node.parse_response(conn, "ASKING")
        except ResponseError as e:
            ask_error = e
    try:
        set_resp = await node.parse_response(conn, HIMPORT_SET)
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


async def execute_set(node, conn, key, fieldset_name, values, asking=False):
    """Execute an ``HIMPORT SET`` on ``conn`` with the required session setup.

    Reconciles deferred discards, lazily bundles PREPARE with the SET on first
    use of a fieldset, and recovers once from a mid-connection fieldset loss
    (``NoSuchFieldsetError``) by re-PREPARE-and-retry. When ``asking`` is set the
    ASKING allowance is folded into the SET's own packed write so it immediately
    precedes the (slot-scoped) SET; the session setup runs first, since those are
    connection-session commands the flag does not gate.
    """
    await reconcile_discards(node, conn)

    registry = conn.himport_registry
    fieldset = registry.get(fieldset_name) if registry is not None else None
    # Lazy PREPARE bundled with SET on first use of this fieldset.
    if (
        fieldset is not None
        and conn._himport_prepared.get(fieldset_name) != fieldset.version
    ):
        return await prepare_and_set(
            node, conn, key, fieldset_name, values, fieldset, asking=asking
        )

    # Believed already prepared (or an unregistered fieldset): bare SET, with
    # ASKING packed immediately before it when this is an ASK redirect.
    if asking:
        await conn.send_packed_command(
            conn.pack_commands(
                [("ASKING",), himport_set_command(key, fieldset_name, values)]
            )
        )
        try:
            await node.parse_response(conn, "ASKING")
        except ResponseError as ask_error:
            # ASKING and SET were one packed write, so the SET reply is still
            # queued. Drain it before surfacing the ASKING error, otherwise the
            # connection returns to the pool with an unread reply and desyncs the
            # next borrower.
            try:
                await node.parse_response(conn, HIMPORT_SET)
            except ResponseError:
                pass
            raise ask_error
    else:
        await conn.send_command(*himport_set_command(key, fieldset_name, values))
    try:
        return await node.parse_response(conn, HIMPORT_SET)
    except NoSuchFieldsetError:
        # Server dropped the fieldset mid-connection without dropping the socket
        # (e.g. RESET / maxmemory-clients eviction): re-PREPARE on this healthy
        # connection and retry the SET once rather than reconnecting. Only for
        # registry-backed fieldsets; manual/unregistered usage propagates.
        if fieldset is None:
            raise
        conn._himport_prepared.pop(fieldset_name, None)
        return await prepare_and_set(
            node, conn, key, fieldset_name, values, fieldset, asking=asking
        )


async def prepare_pipeline(node, conn, command_arg_lists):
    """Pre-flight ``conn`` for a pipeline batch containing ``HIMPORT SET``s.

    The packed pipeline write bypasses the per-command lazy-PREPARE path, so the
    fieldsets referenced by the buffered SETs must be PREPAREd on ``conn`` first.
    Reconciles deferred discards, then PREPAREs every distinct registered fieldset
    the batch references that this connection has not already prepared, in one
    packed write. ``command_arg_lists`` is the batch's per-command positional-arg
    sequences (the caller extracts them from its own command representation).
    No-op when the batch has no registry-backed ``HIMPORT SET``.
    """
    # Selection (registry check, deferred-discard reconcile, scan/dedup/version)
    # is shared with pipeline_prepares. This path differs only in that it sends the
    # PREPAREs as their own packed exchange -- rather than folding them into a
    # queued write -- then drains their replies and raises the first error.
    to_prepare = await pipeline_prepares(node, conn, command_arg_lists)
    if not to_prepare:
        return
    await conn.send_packed_command(
        conn.pack_commands(prepare_wire_commands(to_prepare))
    )
    # Every reply must be drained even on a per-command error, or the unread
    # replies desync the socket before the buffered batch is sent; then raise.
    first_error = await drain_pipeline_prepares(node, conn, to_prepare)
    if first_error is not None:
        raise first_error


async def pipeline_prepares(node, conn, command_arg_lists):
    """Return the fieldsets that must be PREPAREd on ``conn`` for this batch.

    Like :func:`prepare_pipeline`, but does **not** send the PREPAREs: the caller
    folds them into the same packed write as the queued commands (see the pipeline
    executors), so the first pipeline use of a fieldset on a fresh or reconnected
    connection stays a single round trip instead of a separate PREPARE exchange
    followed by the batch. Deferred-discard reconciliation is still performed here,
    but it only touches the socket when discards are actually pending (rare); the
    common warm-up cost -- the first-use PREPARE -- is what gets folded. Returns an
    empty list when the batch references no not-yet-prepared registered fieldset,
    or when ``conn`` carries no real HIMPORT registry.
    """
    registry = getattr(conn, "himport_registry", None)
    if not isinstance(registry, HImportRegistry):
        return []
    await reconcile_discards(node, conn)
    to_prepare = []
    seen = set()
    for args in command_arg_lists:
        parsed = parse_himport_set_args(args)
        if parsed is None:
            continue
        fieldset_name = parsed[1]
        if fieldset_name in seen:
            continue
        seen.add(fieldset_name)
        fieldset = registry.get(fieldset_name)
        if (
            fieldset is not None
            and conn._himport_prepared.get(fieldset_name) != fieldset.version
        ):
            to_prepare.append(fieldset)
    return to_prepare


def prepare_wire_commands(fieldsets):
    """The leading ``HIMPORT PREPARE`` wire commands the caller folds into a batch."""
    return [himport_prepare_command(fs.name, fs.fields) for fs in fieldsets]


async def drain_pipeline_prepares(node, conn, fieldsets):
    """Drain the ``len(fieldsets)`` leading PREPARE replies of a folded pipeline
    write, marking each fieldset prepared on success.

    Returns the first ``ResponseError`` (or ``None``). The caller must still drain
    the queued command replies and only then surface this error: every reply on
    the wire has to be read before raising, or the pooled socket desyncs.
    """
    first_error = None
    for fs in fieldsets:
        try:
            await node.parse_response(conn, HIMPORT_PREPARE)
        except ResponseError as e:
            first_error = first_error or e
            continue
        conn._himport_prepared[fs.name] = fs.version
    return first_error
