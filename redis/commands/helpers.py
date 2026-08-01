import copy
import random
import string
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
)

import redis
from redis.typing import ChannelT, PubSubHandler, Subscription


def list_or_args(keys: Any, args: Iterable[Any] | None) -> List[Any]:
    # returns a single new list combining keys and args
    try:
        iter(keys)
        # a string or bytes-like instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (bytes, str, bytearray, memoryview)):
            keys = [keys]
        else:
            keys = list(keys)
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


def parse_pubsub_subscriptions(
    args: tuple[Any, ...], kwargs: Mapping[str, PubSubHandler]
) -> dict[ChannelT, PubSubHandler | None]:
    parsed_args = list_or_args(args[0], args[1:]) if args else []
    subscriptions: dict[ChannelT, PubSubHandler | None] = {}
    for arg in parsed_args:
        if isinstance(arg, Subscription):
            subscriptions[arg.name] = arg.handler
        else:
            subscriptions[arg] = None
    subscriptions.update(kwargs)
    return subscriptions


def pubsub_subscription_args(
    subscriptions: Mapping[ChannelT, PubSubHandler | None],
) -> list[ChannelT | Subscription]:
    return [
        channel if handler is None else Subscription(channel, handler)
        for channel, handler in subscriptions.items()
    ]


def nativestr(x):
    """Return the decoded binary string, or a string, depending on type."""
    r = x.decode("utf-8", "replace") if isinstance(x, bytes) else x
    if r == "null":
        return
    return r


def delist(x):
    """Given a list of binaries, return the stringified version."""
    if x is None:
        return x
    return [nativestr(obj) for obj in x]


def parse_to_list(response):
    """Optimistically parse the response to a list."""
    res = []

    special_values = {"infinity", "nan", "-infinity"}

    if response is None:
        return res

    for item in response:
        if item is None:
            res.append(None)
            continue
        if isinstance(item, float):
            res.append(item)
            continue
        try:
            item_str = nativestr(item)
        except TypeError:
            res.append(None)
            continue

        if isinstance(item_str, str) and item_str.lower() in special_values:
            res.append(item_str)  # Keep as string
        else:
            try:
                res.append(int(item))
            except (ValueError, OverflowError, TypeError):
                try:
                    res.append(float(item))
                except (ValueError, TypeError):
                    res.append(item_str)

    return res


def random_string(length=10):
    """
    Returns a random N character long string.
    """
    return "".join(  # nosec
        random.choice(string.ascii_lowercase) for x in range(length)
    )


def decode_dict_keys(obj):
    """Decode the keys of the given dictionary with utf-8."""
    newobj = copy.copy(obj)
    for k in obj.keys():
        if isinstance(k, bytes):
            newobj[k.decode("utf-8")] = newobj[k]
            newobj.pop(k)
    return newobj


def get_protocol_version(client):
    if isinstance(client, redis.Redis) or isinstance(client, redis.asyncio.Redis):
        return client.connection_pool.connection_kwargs.get("protocol")
    elif isinstance(client, redis.cluster.AbstractRedisCluster):
        return client.nodes_manager.connection_kwargs.get("protocol")


def get_legacy_responses(client):
    """Return the user-supplied ``legacy_responses`` flag for ``client``.

    Defaults to ``True`` when the flag is not present in the client's
    ``connection_kwargs``. Mirrors :func:`get_protocol_version` so module
    command bases can read both the protocol and the response-shape
    selection from the same place.
    """
    if isinstance(client, redis.Redis) or isinstance(client, redis.asyncio.Redis):
        return client.connection_pool.connection_kwargs.get("legacy_responses", True)
    elif isinstance(client, redis.cluster.AbstractRedisCluster):
        return client.nodes_manager.connection_kwargs.get("legacy_responses", True)
    return True


def apply_module_callbacks(
    user_protocol: Optional[int],
    legacy_responses: bool,
    *,
    common: Dict[str, Callable[..., Any]],
    resp2: Dict[str, Callable[..., Any]],
    resp3: Dict[str, Callable[..., Any]],
    resp2_unified: Optional[Dict[str, Callable[..., Any]]] = None,
    resp3_unified: Optional[Dict[str, Callable[..., Any]]] = None,
    resp3_to_resp2_legacy: Optional[Dict[str, Callable[..., Any]]] = None,
) -> Dict[str, Callable[..., Any]]:
    """Return the merged module-callback dict for the given (protocol,
    legacy_responses) combination.

    Mirrors the selection used by
    :func:`redis._parsers.response_callbacks.get_response_callbacks` for
    the core callbacks: ``common`` is overlaid with the protocol-specific
    dict matching ``user_protocol`` and ``legacy_responses``.
    ``resp2_unified`` defaults to ``resp2``, ``resp3_unified`` to ``resp3``,
    and ``resp3_to_resp2_legacy`` to an empty dict.
    """
    callbacks: Dict[str, Callable[..., Any]] = dict(common)
    if legacy_responses:
        if user_protocol is None:
            callbacks.update(resp3_to_resp2_legacy or {})
        elif user_protocol in (3, "3"):
            callbacks.update(resp3)
        else:
            callbacks.update(resp2)
    else:
        if user_protocol is None or user_protocol in (3, "3"):
            callbacks.update(resp3_unified if resp3_unified is not None else resp3)
        else:
            callbacks.update(resp2_unified if resp2_unified is not None else resp2)
    return callbacks


def at_most_one_value_set(iterable: Iterable[Any]):
    """
    Checks that at most one of the values in the iterable is truthy.

    Args:
        iterable: An iterable of values to check.

    Returns:
        True if at most one value is truthy, False otherwise.

    Raises:
        Might raise an error if the values in iterable are not boolean-compatible.
        For example if the type of the values implement
        __len__ or __bool__ methods and they raise an error.
    """
    values = (bool(x) for x in iterable)
    return sum(values) <= 1


def normalize_function_lib_code(code: Any) -> Any:
    """
    Normalize Redis function library source for FUNCTION LOAD.

    Redis requires the payload to start with a shebang (``#!``) and to contain a
    real newline after the shebang line. Callers often copy examples from
    redis-cli / the docs that differ from what the server accepts:

    - Multi-line triple-quoted strings with leading whitespace before ``#!``
      (server error: ``Missing library metadata``).
    - redis-cli double-quoted style where the shebang separator is a
      two-character ``\\n`` escape rather than a real newline (server error:
      ``Invalid library metadata``).
    - A shebang terminator using CRLF, LFCR, or lone CR line endings.

    Expands only the first shebang-line terminator escape, chosen by earliest
    position among a real newline and redis-cli-style ``\\r\\n`` / ``\\n``.
    The Lua body, including its physical line endings and escapes, is left
    unchanged for exact source round trips.

    Returns the same type as ``code`` for ``str`` / ``bytes`` /
    ``bytearray``; other types are returned unchanged so the encoder can raise
    as usual. Unchanged binary inputs are returned by identity.
    """
    if isinstance(code, (bytes, bytearray, memoryview)):
        return _normalize_function_lib_code_binary(code)

    if isinstance(code, str):
        return _normalize_function_lib_code_text(code)

    return code


def _find_function_lib_bytes(
    data: bytes | bytearray | memoryview,
    pattern: bytes,
    start: int = 0,
    end: int | None = None,
) -> int:
    if isinstance(data, (bytes, bytearray)):
        return data.find(pattern, start, len(data) if end is None else end)

    stop = len(data) if end is None else end
    last = stop - len(pattern)
    for pos in range(start, last + 1):
        if all(data[pos + offset] == value for offset, value in enumerate(pattern)):
            return pos
    return -1


def _normalize_function_lib_code_binary(
    code: bytes | bytearray | memoryview,
) -> bytes | bytearray | memoryview:
    if isinstance(code, memoryview):
        try:
            view = code.cast("B")
        except TypeError:
            # Strided/non-contiguous views cannot be cast. Match the command
            # encoder's behavior and copy only this unsupported view shape.
            code = bytes(code)
            view = code
    else:
        view = code

    start = 0
    whitespace = b" \t\n\r\v\f"
    while start < len(view) and view[start] in whitespace:
        start += 1

    real_lf = _find_function_lib_bytes(view, b"\n", start)
    real_cr = _find_function_lib_bytes(view, b"\r", start)
    real_nl = min((pos for pos in (real_lf, real_cr) if pos >= 0), default=-1)
    header_end = real_nl if real_nl >= 0 else len(view)
    esc_crlf = _find_function_lib_bytes(view, b"\\r\\n", start, header_end)
    esc_lf = _find_function_lib_bytes(view, b"\\n", start, header_end)
    escaped = min((pos for pos in (esc_crlf, esc_lf) if pos >= 0), default=-1)

    valid_lf = real_lf >= 0 and (real_cr < 0 or real_lf < real_cr)
    if start == 0 and escaped < 0 and (valid_lf or real_nl < 0):
        return code

    if escaped >= 0:
        prefix_end = escaped
        terminator_end = escaped + (4 if escaped == esc_crlf else 2)
        # An escaped LF followed by CR is one LFCR shebang terminator.
        if terminator_end < len(view) and view[terminator_end] == ord("\r"):
            terminator_end += 1
    elif real_nl >= 0:
        prefix_end = real_nl
        terminator_end = real_nl + 1
        if terminator_end < len(view) and (
            (view[real_nl] == ord("\r") and view[terminator_end] == ord("\n"))
            or (view[real_nl] == ord("\n") and view[terminator_end] == ord("\r"))
        ):
            terminator_end += 1
    else:
        normalized = bytes(view[start:])
        return bytearray(normalized) if isinstance(code, bytearray) else normalized

    # Normalize only the shebang terminator. The Lua body is preserved byte-for-byte
    # for FUNCTION LIST WITHCODE round trips, hashes, and audit comparisons.
    normalized = bytes(view[start:prefix_end]) + b"\n" + bytes(view[terminator_end:])
    if isinstance(code, bytearray):
        return bytearray(normalized)
    return normalized


def _normalize_function_lib_code_text(text: str) -> str:
    # Only leading whitespace prevents Redis from finding the shebang. Preserve
    # the right-hand side exactly for FUNCTION LIST WITHCODE round trips.
    text = text.lstrip()

    real_lf = text.find("\n")
    real_cr = text.find("\r")
    real_nl = min((pos for pos in (real_lf, real_cr) if pos >= 0), default=-1)
    header_end = real_nl if real_nl >= 0 else len(text)
    esc_crlf = text.find("\\r\\n", 0, header_end)
    esc_lf = text.find("\\n", 0, header_end)
    escaped = min((pos for pos in (esc_crlf, esc_lf) if pos >= 0), default=-1)

    if escaped >= 0:
        terminator_end = escaped + (4 if escaped == esc_crlf else 2)
        if terminator_end < len(text) and text[terminator_end] == "\r":
            terminator_end += 1
        return text[:escaped] + "\n" + text[terminator_end:]

    valid_lf = real_lf >= 0 and (real_cr < 0 or real_lf < real_cr)
    if real_nl < 0 or valid_lf:
        return text

    terminator_end = real_nl + 1
    if terminator_end < len(text) and text[terminator_end] == "\n":
        terminator_end += 1
    return text[:real_nl] + "\n" + text[terminator_end:]
