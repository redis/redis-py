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

    - Multi-line triple-quoted strings with a leading newline or indentation
      before ``#!`` (server error: ``Missing library metadata``).
    - redis-cli double-quoted style where the shebang separator is a
      two-character ``\\n`` escape rather than a real newline (server error:
      ``Invalid library metadata``).
    - Windows CRLF (``\\r\\n``) line endings.

    Expands only the first shebang-line terminator escape, chosen by earliest
    position among a real newline and redis-cli-style ``\\r\\n`` / ``\\n``.
    Later escapes (e.g. Lua string ``\\n``) are left unchanged. Binary payloads
    are normalized via latin-1 so non-UTF-8 body bytes still get framing fixes.

    Returns the same type as ``code`` for ``str`` / ``bytes`` / ``bytearray``;
    other types are returned unchanged so the encoder can raise as usual.
    """
    if isinstance(code, (bytes, bytearray, memoryview)):
        # latin-1 is a 1:1 byte mapping; do not require UTF-8 for framing.
        raw = bytes(code)
        normalized = _normalize_function_lib_code_text(raw.decode("latin-1"))
        return normalized.encode("latin-1")

    if isinstance(code, str):
        return _normalize_function_lib_code_text(code)

    return code


def _normalize_function_lib_code_text(text: str) -> str:
    # Drop leading/trailing whitespace so the shebang is at offset 0.
    text = text.strip()
    # Normalize common line-ending variants to LF (what Redis's parser scans for).
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # End of the shebang line is the earliest of: a real newline, or a
    # redis-cli-style "\r\n" / "\n" escape. Expand that escape when it wins
    # (even if real newlines appear later in a multiline Lua body).
    real_nl = text.find("\n")
    esc_crlf = text.find("\\r\\n")
    esc_lf = text.find("\\n")

    candidates: List[tuple[int, str, int]] = []
    if real_nl >= 0:
        candidates.append((real_nl, "real", 1))
    if esc_crlf >= 0:
        candidates.append((esc_crlf, "esc_crlf", 4))
    if esc_lf >= 0:
        candidates.append((esc_lf, "esc_lf", 2))

    if not candidates:
        return text

    pos, kind, length = min(candidates, key=lambda item: item[0])
    if kind == "real":
        return text
    return text[:pos] + "\n" + text[pos + length :]
